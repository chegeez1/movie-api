from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, Response, FileResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from contextlib import asynccontextmanager
from datetime import datetime
from collections import deque
from typing import Optional
import asyncio
import os
import subprocess
import sys
import threading
import time
import re
import json
import uuid
import urllib.parse
import urllib.request
import httpx

from chege_scraper import ChegeScraper, _is_video_url

_PREFIX = "/movies-api"

_CDN_DOMAINS = (
    "pbcdnw.aoneroom.com",
    "pbcdn.aoneroom.com",
    "macdn.aoneroom.com",
)

_STRIP_KEYS = {
    "source", "uploaded_by", "uploadBy",
    "player_domain", "url_format", "note",
}

def _rewrite(obj, proxy_base: str):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in _STRIP_KEYS:
                continue
            out[k] = _rewrite(v, proxy_base)
        return out
    if isinstance(obj, list):
        return [_rewrite(item, proxy_base) for item in obj]
    if isinstance(obj, str) and obj.startswith("https://") and any(d in obj for d in _CDN_DOMAINS):
        return f"{proxy_base}img?url={urllib.parse.quote(obj, safe='')}"
    return obj


# ── Request log ring-buffer (last 500 requests) ──────────────────────────────
_REQUEST_LOG: deque = deque(maxlen=500)
_SKIP_LOG_PATHS = {"/health", "/admin/requests"}


class StripPrefixMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.scope.get("path", "")
        if path.startswith(_PREFIX):
            request.scope["path"] = path[len(_PREFIX):] or "/"
            raw = request.scope.get("raw_path", b"")
            if raw.startswith(_PREFIX.encode()):
                request.scope["raw_path"] = raw[len(_PREFIX):] or b"/"
        return await call_next(request)


class RequestLogMiddleware(BaseHTTPMiddleware):
    """Records every request (post-prefix-strip) into the ring buffer."""
    async def dispatch(self, request: Request, call_next):
        start  = time.time()
        path   = request.scope.get("path", "")
        method = request.method
        response = await call_next(request)
        duration_ms = round((time.time() - start) * 1000)
        if path not in _SKIP_LOG_PATHS:
            ip = (
                request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                or (request.client.host if request.client else "unknown")
            )
            _REQUEST_LOG.append({
                "ts":          datetime.utcnow().isoformat() + "Z",
                "method":      method,
                "path":        path,
                "status":      response.status_code,
                "duration_ms": duration_ms,
                "ip":          ip,
            })
        return response


class SanitizeResponseMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        ct = response.headers.get("content-type", "")
        if "application/json" not in ct:
            return response
        body = b""
        async for chunk in response.body_iterator:
            body += chunk
        try:
            scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
            host = request.headers.get("x-forwarded-host",
                   request.headers.get("host", request.url.netloc))
            proxy_base = f"{scheme}://{host}{_PREFIX}/"
            data = json.loads(body)
            data = _rewrite(data, proxy_base)
            body = json.dumps(data).encode()
        except Exception:
            pass
        headers = dict(response.headers)
        headers["content-length"] = str(len(body))
        return Response(
            content=body,
            status_code=response.status_code,
            headers=headers,
            media_type="application/json",
        )


app = FastAPI(
    title="Chege Movie API",
    description="A movie API powered by MovieBox data. Created by Chege.",
    version="3.0.0",
    root_path=_PREFIX,
)

app.add_middleware(SanitizeResponseMiddleware)
app.add_middleware(RequestLogMiddleware)
app.add_middleware(StripPrefixMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _start_bulk_and_schedule(max_pages: int = 500, concurrency: int = 8) -> None:
    """
    Start the bulk downloader if not already running, then after it finishes
    wait 6 hours and re-run automatically to catch newly added movies.
    Call this once at startup; it self-perpetuates forever.
    """
    global _bulk_active, _bulk_stop
    if _bulk_active:
        return
    _bulk_stop = False

    def _run_loop():
        while True:
            _bulk_download_all(max_pages=max_pages, include_series=True, concurrency=concurrency)
            print("[bulk-dl] Scan complete. Sleeping 6 h before next scan for new movies.", file=sys.stderr)
            # Clear queued set so re-scan can detect titles added since last run
            _auto_queued.clear()
            time.sleep(6 * 3600)

    threading.Thread(target=_run_loop, daemon=True).start()


@app.on_event("startup")
async def startup_event():
    threading.Thread(target=_warm_cache, daemon=True).start()
    # Auto-start bulk library builder — downloads every MovieBox title to VPS disk,
    # then re-scans every 6 hours to catch newly added movies.
    threading.Thread(target=_start_bulk_and_schedule, daemon=True).start()


scraper = ChegeScraper()

_cache: dict = {}
CACHE_TTL   = 3600
CACHE_STALE = 7200
_refresh_lock: set = set()

_server_cache: dict = {}
SERVER_CACHE_TTL = 600  # 10 minutes — probes are fast but network conditions change

# Video URL cache — populated automatically when users watch via the proxy player
# key: "subjectId:ep:season:resolution"  value: {url, type, ts}
_video_url_cache: dict = {}

# ── VPS-disk download / local library ────────────────────────────────────────
_DOWNLOAD_DIR = "/opt/movie-downloads"
_LIBRARY_INDEX_FILE = os.path.join(_DOWNLOAD_DIR, ".library_index.json")
# job_id → {status, progress, filepath, filename, size_mb, error, ts}
_download_jobs: dict = {}
# Keys already auto-queued so we don't re-fire on every page refresh
_auto_queued:   set  = set()
# Titles where Torrentio returned zero streams — skip on future scans
_torrent_no_streams: set = set()
# Limit concurrent background downloads — 8 parallel max (bulk)
_dl_semaphore = threading.Semaphore(8)
# Separate priority slots for user-triggered downloads — always available, not shared with bulk
_priority_semaphore = threading.Semaphore(3)
# Limit concurrent Playwright browser sessions — only 1 at a time to avoid crashes
_playwright_semaphore = threading.Semaphore(1)

# ── Persistent library index (detail_path → movie metadata) ──────────────────
# Survives API restarts; written to disk after every successful download.
_lib_index: dict  = {}          # keyed by detail_path
_lib_index_lock = threading.Lock()

def _load_lib_index() -> None:
    """Load library index from disk into memory on startup."""
    global _lib_index
    try:
        if os.path.isfile(_LIBRARY_INDEX_FILE):
            with open(_LIBRARY_INDEX_FILE, "r") as f:
                _lib_index = json.load(f)
            print(f"[lib-index] Loaded {len(_lib_index)} entries from disk", file=sys.stderr)
    except Exception as exc:
        print(f"[lib-index] Load failed: {exc}", file=sys.stderr)
        _lib_index = {}

def _save_lib_entry(detail_path: str, stream: dict, size_mb: float = 0) -> None:
    """Persist a single movie/series entry to the library index."""
    global _lib_index
    entry = {
        "id":          stream.get("id", ""),
        "detail_path": detail_path,
        "title":       stream.get("title", ""),
        "type":        "series" if stream.get("is_series") else "movie",
        "year":        (stream.get("release_date") or "")[:4],
        "imdb_rating": stream.get("imdb_rating"),
        "poster_url":  stream.get("cover_url"),
        "size_mb":     round(size_mb, 1),
    }
    with _lib_index_lock:
        _lib_index[detail_path] = entry
        try:
            os.makedirs(_DOWNLOAD_DIR, exist_ok=True)
            tmp = _LIBRARY_INDEX_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(_lib_index, f)
            os.replace(tmp, _LIBRARY_INDEX_FILE)
        except Exception as exc:
            print(f"[lib-index] Save failed: {exc}", file=sys.stderr)

# Load index at import time
_load_lib_index()

# ── Bulk library builder state ────────────────────────────────────────────────
_bulk_active  = False          # True while bulk builder is running
_bulk_stop    = False          # Set to True to request graceful stop
_bulk_stats: dict = {
    "seen": 0, "queued": 0, "skipped": 0, "errors": 0,
    "page": 0, "source": "", "started_at": None, "finished_at": None,
}


def _ensure_download_dir() -> None:
    os.makedirs(_DOWNLOAD_DIR, exist_ok=True)


# Bulk download year gate — skip titles released before this year to save disk space
_BULK_MIN_YEAR = 2010

def _bulk_process_item(item: dict) -> None:
    """Try to queue a download for a single scraped item (movie or episode 1 of series)."""
    global _bulk_stats
    import sys

    detail_path = item.get("detail_path") or item.get("detailPath", "")
    if not detail_path:
        _bulk_stats["skipped"] += 1
        return

    # Year gate: skip anything older than _BULK_MIN_YEAR to save storage
    raw_date = item.get("release_date") or item.get("releaseDate") or ""
    try:
        year = int(str(raw_date)[:4])
    except (ValueError, TypeError):
        year = 9999  # unknown year → allow through
    if year < _BULK_MIN_YEAR:
        _bulk_stats["skipped"] += 1
        return

    _bulk_stats["seen"] += 1

    try:
        stream = _cached(
            f"stream:{detail_path}",
            scraper.get_stream_info,
            detail_path=detail_path,
        )
        if not stream:
            _bulk_stats["errors"] += 1
            return
    except Exception as exc:
        print(f"[bulk-dl] stream_info error — {detail_path}: {exc}", file=sys.stderr)
        _bulk_stats["errors"] += 1
        return

    safe_title = _safe_name(stream.get("title", "unknown"))[:60]
    is_series  = stream.get("is_series", False)
    ep, season = (1, 1) if is_series else (1, 0)

    if existing := _find_local_file(safe_title, ep, season):
        _bulk_stats["skipped"] += 1
        # Backfill library index for files that already exist on disk
        if detail_path not in _lib_index:
            try:
                sz = os.path.getsize(existing) / 1_048_576
            except OSError:
                sz = 0
            _save_lib_entry(detail_path, stream, sz)
        return

    # skip_playwright=True: bulk mode skips the slow 60s Playwright fallback
    _trigger_auto_download(stream, ep=ep, season=season, skip_playwright=True)
    _bulk_stats["queued"] += 1


def _bulk_download_all(
    max_pages: int = 200,
    include_series: bool = True,
    concurrency: int = 3,
) -> None:
    """
    Background thread — scrapes ALL content from MovieBox and downloads everything to VPS disk.
    Sources iterated in order:
      1. All movies via subject/filter (paginated, subject_type=1)
      2. All series via subject/filter (paginated, subject_type=2)  [if include_series]
      3. Ranking chart (multiple pages)
      4. Trending (multiple pages)
    Each item calls _trigger_auto_download() which fires its own daemon thread.
    The _dl_semaphore limits parallel yt-dlp processes to `concurrency`.
    """
    global _bulk_active, _bulk_stop, _bulk_stats, _dl_semaphore
    import sys

    _dl_semaphore = threading.Semaphore(concurrency)
    _bulk_active  = True
    _bulk_stop    = False
    _bulk_stats.update({
        "seen": 0, "queued": 0, "skipped": 0, "errors": 0,
        "page": 0, "source": "", "started_at": time.time(), "finished_at": None,
    })

    print(f"[bulk-dl] Starting — max_pages={max_pages} series={include_series} concurrency={concurrency}", file=sys.stderr)

    def _scrape_pages(source_name: str, fetch_fn, *args, **kwargs):
        """Iterate pages from a paginated scraper fn until exhausted or stopped."""
        for page in range(1, max_pages + 1):
            if _bulk_stop:
                return
            _bulk_stats["page"]   = page
            _bulk_stats["source"] = source_name
            try:
                result = fetch_fn(*args, page=page, per_page=30, **kwargs)
            except Exception as exc:
                print(f"[bulk-dl] {source_name} page {page} error: {exc}", file=sys.stderr)
                break
            items = result.get("items", [])
            if not items:
                break
            # Sort newest-first within each page so latest content downloads first
            items = sorted(
                items,
                key=lambda x: (x.get("release_date") or x.get("releaseDate") or "0000"),
                reverse=True,
            )
            for item in items:
                if _bulk_stop:
                    return
                if not include_series and item.get("is_series"):
                    _bulk_stats["skipped"] += 1
                    continue
                _bulk_process_item(item)
                time.sleep(0.1)   # light throttle — don't hammer the origin
            if not result.get("pager", {}).get("hasMore", False):
                print(f"[bulk-dl] {source_name} exhausted at page {page}", file=sys.stderr)
                break
            time.sleep(0.3)   # pause between pages

    try:
        # ── 1. All movies ────────────────────────────────────────────────────
        _scrape_pages("movies", scraper.get_movies, subject_type=1)

        # ── 2. All series (ep 1 s1 only) ────────────────────────────────────
        if include_series and not _bulk_stop:
            _scrape_pages("series", scraper.get_movies, subject_type=2)

        # ── 3. Ranking chart (catches popular titles the filter might miss) ──
        if not _bulk_stop:
            _scrape_pages("ranking", scraper.get_ranking)

        # ── 4. Trending ──────────────────────────────────────────────────────
        if not _bulk_stop:
            _scrape_pages("trending", scraper.get_trending)

    finally:
        _bulk_stats["finished_at"] = time.time()
        _bulk_active = False
        print(f"[bulk-dl] Finished — {_bulk_stats}", file=sys.stderr)


def _safe_name(text: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", text)


def _job_filepath(safe_title: str, ep: int, season: int, resolution: int):
    filename = f"{safe_title}_s{season:02d}e{ep:02d}_{resolution}p.mp4"
    filepath = os.path.join(_DOWNLOAD_DIR, filename)
    return filepath, filename


_MIN_REAL_FILE_BYTES = 50 * 1024 * 1024  # 50 MB — trailers/previews are always smaller


def _find_local_file(safe_title: str, ep: int, season: int) -> Optional[str]:
    """
    Return path to the best matching local file for a title/ep/season,
    or None. Prefers 1080p → 720p → any resolution.
    Files under 50 MB are treated as trailers/corrupt downloads and ignored.

    Season normalisation: the frontend passes season=0 as default for series,
    but bulk downloads store series as season=1 (s01e01).  When season=0 finds
    nothing we automatically retry with season=1 so those files are visible.
    """
    def _search(s: int, e: int) -> Optional[str]:
        if not os.path.isdir(_DOWNLOAD_DIR):
            return None
        # Pattern 1 (simple):      From_s01e01_1080p.mp4
        # Pattern 2 (season range): From_S1-S4_s01e01_1080p.mp4
        ep_tag   = f"_s{s:02d}e{e:02d}_"
        prefix   = f"{safe_title}{ep_tag}"   # pattern 1
        # Fast-path: exact resolution candidates for both patterns
        for res in ("1080p", "720p", "480p", "360p"):
            for ext in (".mp4", ".mkv"):
                for p in (f"{safe_title}{ep_tag}{res}{ext}",):
                    candidate = os.path.join(_DOWNLOAD_DIR, p)
                    if os.path.exists(candidate) and os.path.getsize(candidate) >= _MIN_REAL_FILE_BYTES:
                        return candidate
        try:
            for fname in os.listdir(_DOWNLOAD_DIR):
                if not fname.lower().endswith((".mp4", ".mkv", ".webm", ".m4v", ".avi")):
                    continue
                # Pattern 1: Title_s01e01_*
                # Pattern 2: Title_<anything>_s01e01_*  (season range in name)
                if (fname.startswith(prefix) or
                        (fname.startswith(safe_title + "_") and ep_tag in fname)):
                    fpath = os.path.join(_DOWNLOAD_DIR, fname)
                    if os.path.getsize(fpath) >= _MIN_REAL_FILE_BYTES:
                        return fpath
        except OSError:
            pass
        return None

    result = _search(season, ep)
    if result:
        return result
    # season=0 is the frontend default; bulk stores series as season=1 — try both
    if season == 0:
        return _search(1, ep)
    if season == 1:
        return _search(0, ep)
    return None


def _download_direct_mp4(job_id: str, url: str, filepath: str, headers: Optional[dict] = None) -> bool:
    """
    Download a direct MP4/video URL to disk using wget (fast, reliable, resumes).
    Returns True on success.
    """
    import sys
    _ensure_download_dir()
    hdrs = headers or {}
    cmd = [
        "wget", "-q", "--show-progress", "--progress=dot:giga",
        "-c",          # resume partial downloads
        "--timeout=30", "--tries=3",
        "-O", filepath,
    ]
    for k, v in hdrs.items():
        cmd += ["--header", f"{k}: {v}"]
    cmd.append(url)

    print(f"[dl-job] {job_id} wget {url[:80]}", file=sys.stderr)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:  # type: ignore[union-attr]
            pass   # just drain — wget progress goes to stderr which we merged
        proc.wait()
        if proc.returncode == 0 and os.path.exists(filepath) and os.path.getsize(filepath) > 1_048_576:
            return True
        print(f"[dl-job] {job_id} wget rc={proc.returncode}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print(f"[dl-job] {job_id} wget not found, falling back to yt-dlp", file=sys.stderr)
        return False
    except Exception as exc:
        print(f"[dl-job] {job_id} wget error: {exc}", file=sys.stderr)
        return False


def _download_via_ytdlp(job_id: str, url: str, filepath: str) -> tuple[bool, str]:
    """
    Download any URL (embed page or direct HLS/MP4) using yt-dlp.
    Returns (success, error_message).
    """
    import sys
    _ensure_download_dir()
    cmd = [
        "yt-dlp",
        "--no-check-certificate",
        "--no-playlist",
        "--newline",
        "--geo-bypass",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "--add-headers", "Referer:https://www.google.com",
        "-o", filepath,
        url,
    ]
    print(f"[dl-job] {job_id} yt-dlp {url[:80]}", file=sys.stderr)
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,   # separate stderr to capture real errors
            text=True,
        )
        # Drain stdout for progress updates
        stdout_lines = []
        for line in proc.stdout:  # type: ignore[union-attr]
            line = line.strip()
            stdout_lines.append(line)
            if "[download]" in line and "%" in line:
                try:
                    pct = float(line.split("%")[0].strip().split()[-1])
                    if job_id in _download_jobs:
                        _download_jobs[job_id]["progress"] = round(min(pct, 99.9), 1)
                except Exception:
                    pass
        stderr_out = proc.stderr.read().strip()  # type: ignore[union-attr]
        proc.wait()

        # Check for a real output file
        actual = filepath
        # yt-dlp may add extension — check for .mp4 / .mkv / .webm variants
        if not os.path.exists(actual):
            for ext in (".mp4", ".mkv", ".webm", ".m4v"):
                if os.path.exists(filepath + ext):
                    try:
                        os.rename(filepath + ext, actual)
                    except OSError:
                        actual = filepath + ext
                    break

        if proc.returncode == 0 and os.path.exists(actual) and os.path.getsize(actual) > 1_048_576:
            return True, ""

        # Build a useful error — first meaningful line from stderr
        err_lines = [l for l in stderr_out.splitlines() if l.strip() and "WARNING" not in l]
        err_summary = err_lines[-1][:120] if err_lines else f"exit code {proc.returncode}"
        print(f"[dl-job] {job_id} yt-dlp FAIL: {err_summary}", file=sys.stderr)
        return False, err_summary
    except FileNotFoundError:
        return False, "yt-dlp not installed — run: pip install -U yt-dlp --break-system-packages"
    except Exception as exc:
        return False, str(exc)


def _run_download_job(
    job_id: str,
    source_url: str,
    filepath: str,
    filename: str,
    is_embed: bool,
) -> None:
    """
    Execute a single download job.  Job must already be registered in _download_jobs.
    Strategy:
      - direct MP4/m3u8 URL  → wget first (fast), fall back to yt-dlp
      - embed page URL        → yt-dlp only (needs extraction)
    """
    import sys
    _download_jobs[job_id]["status"] = "downloading"
    print(f"[dl-job] {job_id} START embed={is_embed} — {source_url[:80]}", file=sys.stderr)

    success = False
    err_msg = ""

    if not is_embed:
        # Direct URL — try wget first, then yt-dlp
        success = _download_direct_mp4(job_id, source_url, filepath, headers={
            "Referer": "https://netfilm.world/",
            "Origin": "https://netfilm.world",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        })
        if not success:
            success, err_msg = _download_via_ytdlp(job_id, source_url, filepath)
    else:
        # Embed page — yt-dlp only
        success, err_msg = _download_via_ytdlp(job_id, source_url, filepath)

    if success:
        # Verify size gate: >500 KB = real content (trailers are 1-30 MB; error pages are <50 KB)
        size = os.path.getsize(filepath)
        if size < 512_000:
            os.remove(filepath)
            _download_jobs[job_id].update({"status": "error", "error": f"File too small ({size//1024} KB) — likely an error page"})
            print(f"[dl-job] {job_id} SIZE-GATE: {size//1024} KB — deleted", file=sys.stderr)
        else:
            size_mb = size // 1_048_576
            print(f"[dl-job] {job_id} DONE — {size_mb} MB", file=sys.stderr)
            _download_jobs[job_id].update({"status": "ready", "progress": 100, "size_mb": size_mb})
            _bulk_stats["downloaded"] = _bulk_stats.get("downloaded", 0) + 1
    else:
        print(f"[dl-job] {job_id} FAILED — {err_msg}", file=sys.stderr)
        _download_jobs[job_id].update({"status": "error", "error": err_msg or "Download failed"})
        _bulk_stats["dl_errors"] = _bulk_stats.get("dl_errors", 0) + 1


def _embed_urls_for(imdb_id: str, is_series: bool, ep: int, season: int) -> list[tuple[str, str]]:
    """
    Return ordered list of (embed_url, source_name) to try with yt-dlp.
    Ordered from most likely to work → least.
    """
    s = max(season, 1)
    if is_series:
        return [
            (f"https://vidsrc.me/embed/tv?imdb={imdb_id}&season={s}&episode={ep}", "vidsrc.me"),
            (f"https://vidsrc.xyz/embed/tv/{imdb_id}?season={s}&episode={ep}",     "vidsrc.xyz"),
            (f"https://2embed.cc/embedtv/{imdb_id}&s={s}&e={ep}",                  "2embed"),
            (f"https://vidsrc.to/embed/tv/{imdb_id}/{s}/{ep}",                     "vidsrc.to"),
        ]
    else:
        return [
            (f"https://vidsrc.me/embed/movie?imdb={imdb_id}",    "vidsrc.me"),
            (f"https://vidsrc.xyz/embed/movie/{imdb_id}",         "vidsrc.xyz"),
            (f"https://2embed.cc/embed/{imdb_id}",                "2embed"),
            (f"https://vidsrc.to/embed/movie/{imdb_id}",          "vidsrc.to"),
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Torrentio / aria2c torrent-based download pipeline
# Uses https://torrentio.strem.fun (public Stremio addon) to get magnet links
# for any IMDB ID, then downloads via aria2c.  No authentication required.
# ─────────────────────────────────────────────────────────────────────────────

_TORRENTIO_BASE = "https://torrentio.strem.fun"
_TORRENT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://explodie.org:6969/announce",
    "udp://tracker.dler.org:6969/announce",
]


_DISK_PAUSE_GB = 20   # stop new downloads when free space drops below this


def _free_disk_gb() -> float:
    """Return free GB on the download partition."""
    try:
        st = os.statvfs(_DOWNLOAD_DIR if os.path.isdir(_DOWNLOAD_DIR) else "/")
        return (st.f_bavail * st.f_frsize) / 1_073_741_824
    except OSError:
        return 999.0


def _parse_size_gb(stream: dict) -> float:
    """Extract file size in GB from a Torrentio stream's title field (💾 N.NN GB)."""
    title = stream.get("title", "")
    m = re.search(r"\U0001f4be\s*([\d.]+)\s*gb", title, re.IGNORECASE)
    if m:
        return float(m.group(1))
    # Also try MB
    m2 = re.search(r"\U0001f4be\s*([\d.]+)\s*mb", title, re.IGNORECASE)
    if m2:
        return float(m2.group(1)) / 1024
    return 99.0   # unknown size — treat as large


def _torrentio_best_stream(imdb_id: str, is_series: bool, ep: int, season: int) -> Optional[dict]:
    """
    Query Torrentio and return the best available torrent stream dict.
    Prefers 720p (space-efficient) → 1080p → 480p; caps at 2.5 GB per file.
    Returns None if no streams found or API unreachable.
    """
    import sys
    if not imdb_id or not imdb_id.startswith("tt"):
        return None

    if is_series:
        s = max(season, 1)
        path = f"stream/series/{imdb_id}%3A{s}%3A{ep}.json"
    else:
        path = f"stream/movie/{imdb_id}.json"

    url = f"{_TORRENTIO_BASE}/{path}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
    except Exception as exc:
        print(f"[torrentio] API error for {imdb_id}: {exc}", file=sys.stderr)
        return None

    streams = data.get("streams", [])
    if not streams:
        print(f"[torrentio] No streams for {imdb_id}", file=sys.stderr)
        _torrent_no_streams.add(imdb_id)   # remember — skip on future scans
        return None

    print(f"[torrentio] {len(streams)} streams for {imdb_id}", file=sys.stderr)

    def _quality_rank(s: dict) -> tuple:
        name  = (s.get("name", "") + " " + s.get("title", "")).lower()
        # Extract seeder count from title (emoji 👤 N)
        seeders = 0
        m = re.search(r"\U0001f464\s*(\d+)", s.get("title", ""))
        if m:
            seeders = int(m.group(1))
        # Extract size in GB from title (emoji 💾 N.NN GB)
        size_gb = 99.0
        sm = re.search(r"\U0001f4be\s*([\d.]+)\s*gb", s.get("title", ""), re.IGNORECASE)
        if sm:
            size_gb = float(sm.group(1))
        # Quality tier: prefer 720p (good quality, half the size of 1080p)
        # 0=720p, 1=1080p, 2=480p, 3=4k, 4=other
        if "720p" in name or "720" in name:
            tier = 0
        elif "1080p" in name or "1080" in name:
            tier = 1
        elif "480p" in name or "480" in name:
            tier = 2
        elif "2160p" in name or "4k" in name or "uhd" in name:
            tier = 3
        else:
            tier = 4
        # Within tier: prefer WEB-DL/WEBRip > BluRay > other; prefer more seeders; prefer smaller
        source_bonus = 0 if any(k in name for k in ("web-dl","webrip","web dl")) else (1 if "bluray" in name or "blu-ray" in name else 2)
        return (tier, source_bonus, -seeders, size_gb)

    # Filter: must have a valid infoHash
    valid = [s for s in streams if s.get("infoHash")]
    if not valid:
        return None

    # Prefer streams under 2.5 GB to conserve disk space; fall back to smallest if all larger
    _MAX_SIZE_GB = 2.5
    under_cap = [s for s in valid if _parse_size_gb(s) <= _MAX_SIZE_GB]
    pool = under_cap if under_cap else sorted(valid, key=_parse_size_gb)

    best = sorted(pool, key=_quality_rank)[0]
    q_name = best.get("name", "").replace("\n", " ")
    print(f"[torrentio] Best: {q_name} — {best.get('infoHash','')[:16]}...", file=sys.stderr)
    return best


def _download_via_torrent(
    job_id: str,
    imdb_id: str,
    is_series: bool,
    ep: int,
    season: int,
    filepath: str,
    filename: str,
) -> bool:
    """
    Download a movie/episode via torrent using aria2c.
    1. Queries Torrentio for the best magnet link.
    2. Runs aria2c to download to a temp dir.
    3. Finds the largest video file in the result.
    4. Remuxes .mkv→.mp4 using ffmpeg (no re-encode, fast).
    5. Moves final file to `filepath`.
    Returns True on success.
    """
    import sys, shutil, glob
    _ensure_download_dir()

    stream = _torrentio_best_stream(imdb_id, is_series, ep, season)
    if not stream:
        return False

    info_hash = stream["infoHash"]
    dn        = stream.get("behaviorHints", {}).get("filename") or stream.get("name", info_hash)
    file_idx  = stream.get("fileIdx")  # 0-based; None means single-file torrent

    # Build magnet link with public trackers for faster peer discovery
    magnet = (
        f"magnet:?xt=urn:btih:{info_hash}"
        f"&dn={urllib.parse.quote(str(dn))}"
        + "".join(f"&tr={urllib.parse.quote(t)}" for t in _TORRENT_TRACKERS)
    )

    tmp_dir = f"/tmp/chege_torrent_{job_id}"
    os.makedirs(tmp_dir, exist_ok=True)

    cmd = [
        "aria2c",
        "--seed-time=0",            # never seed
        "--file-allocation=none",   # skip pre-allocation for speed
        "--max-connection-per-server=8",
        "--split=16",               # 16 parallel chunks per file
        "--min-split-size=5M",
        "--enable-dht=true",
        "--enable-peer-exchange=true",
        "--bt-enable-lpd=true",
        "--bt-max-peers=100",       # more peers = faster on hot torrents
        "--bt-request-peer-speed-limit=0",  # no per-peer limit
        "--max-overall-download-limit=0",   # unlimited download speed
        f"--dir={tmp_dir}",
        "--console-log-level=warn",
        "--summary-interval=60",
        "--timeout=120",
        "--connect-timeout=30",
        "--retry-wait=5",
        "--max-tries=3",
        "--bt-stop-timeout=600",    # give up on stalled torrent after 10 min
    ]
    if file_idx is not None:
        cmd.append(f"--select-file={file_idx + 1}")  # aria2c is 1-based

    cmd.append(magnet)

    print(f"[torrent] {job_id} aria2c infoHash={info_hash[:16]} fileIdx={file_idx}", file=sys.stderr)
    if job_id in _download_jobs:
        _download_jobs[job_id]["source"] = "torrentio"
        _download_jobs[job_id]["status"] = "downloading"

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in (proc.stdout or []):
            line = line.strip()
            # Parse aria2c progress lines: "[#XXXXXX 1GiB/4GiB(25%) CN:5 DL:2MiB ...]"
            m = re.search(r"\((\d+)%\)", line)
            if m and job_id in _download_jobs:
                _download_jobs[job_id]["progress"] = int(m.group(1))
        proc.wait()
    except Exception as exc:
        print(f"[torrent] {job_id} aria2c error: {exc}", file=sys.stderr)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return False

    # Find largest video file in the temp dir (recursive)
    VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".webm", ".m4v", ".mov")
    candidates = []
    for root, _, files in os.walk(tmp_dir):
        for f in files:
            if f.lower().endswith(VIDEO_EXTS):
                fp = os.path.join(root, f)
                try:
                    candidates.append((os.path.getsize(fp), fp))
                except OSError:
                    pass

    if not candidates:
        print(f"[torrent] {job_id} no video files found in {tmp_dir}", file=sys.stderr)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return False

    candidates.sort(reverse=True)
    src_path = candidates[0][1]
    src_size = candidates[0][0]
    print(f"[torrent] {job_id} found {src_path} ({src_size // 1_048_576} MB)", file=sys.stderr)

    if src_size < _MIN_REAL_FILE_BYTES:
        print(f"[torrent] {job_id} file too small ({src_size // 1_048_576} MB) — skipping", file=sys.stderr)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return False

    # Remux .mkv → .mp4 using ffmpeg (stream copy, no re-encode)
    if src_path.lower().endswith(".mkv"):
        out_path = filepath if filepath.endswith(".mp4") else filepath.rsplit(".", 1)[0] + ".mp4"
        print(f"[torrent] {job_id} remuxing MKV → MP4", file=sys.stderr)
        try:
            remux = subprocess.run(
                ["ffmpeg", "-y", "-i", src_path,
                 "-c", "copy", "-movflags", "+faststart",
                 out_path],
                capture_output=True, timeout=3600,
            )
            if remux.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 1_048_576:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                size_mb = os.path.getsize(out_path) // 1_048_576
                if job_id in _download_jobs:
                    _download_jobs[job_id].update({"status": "ready", "progress": 100, "size_mb": size_mb})
                    _bulk_stats["downloaded"] = _bulk_stats.get("downloaded", 0) + 1
                print(f"[torrent] {job_id} DONE (remuxed) — {size_mb} MB", file=sys.stderr)
                return True
            else:
                err = remux.stderr.decode("utf-8", "ignore")[-200:]
                print(f"[torrent] {job_id} ffmpeg failed: {err}", file=sys.stderr)
                # Fall through: try moving the .mkv directly
        except Exception as exc:
            print(f"[torrent] {job_id} remux exception: {exc}", file=sys.stderr)
            # Fall through: move .mkv as-is

    # Move file to final destination (rename to .mp4 even if .mkv — browser handles it)
    final = filepath if filepath.endswith((".mp4", ".mkv")) else filepath
    if src_path.lower().endswith(".mkv") and not final.endswith(".mkv"):
        final = final.rsplit(".", 1)[0] + ".mkv"
    try:
        shutil.move(src_path, final)
    except Exception as exc:
        print(f"[torrent] {job_id} move error: {exc}", file=sys.stderr)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return False

    shutil.rmtree(tmp_dir, ignore_errors=True)
    size_mb = os.path.getsize(final) // 1_048_576
    if job_id in _download_jobs:
        _download_jobs[job_id].update({"status": "ready", "progress": 100, "size_mb": size_mb})
        _bulk_stats["downloaded"] = _bulk_stats.get("downloaded", 0) + 1
    print(f"[torrent] {job_id} DONE — {size_mb} MB ({os.path.basename(final)})", file=sys.stderr)
    return True


def _playwright_warm_cache(subject_id: str, detail_path: str, ep: int, season: int, resolution: int) -> Optional[str]:
    """
    Load the player page with headless Chromium and intercept ALL network responses.

    Why response interception (not proxy cache):
      The netfilm.world player JS calls h5-api.aoneroom.com DIRECTLY — those requests
      never pass through our proxy, so proxy capture never fires. Playwright sits in the
      middle of the browser's network stack and can see every response regardless of origin.

    Requires: pip install playwright --break-system-packages && playwright install chromium
    """
    import sys, json as _json
    # Systemd service may have a stripped PATH — force the site-packages where playwright lives
    for _sp in ("/usr/local/lib/python3.12/dist-packages", "/usr/lib/python3/dist-packages"):
        if _sp not in sys.path:
            sys.path.insert(0, _sp)
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except ImportError as _ie:
        print(f"[playwright] ImportError: {_ie} — sys.path={sys.path[:4]}", file=sys.stderr)
        return None

    vkey = f"{subject_id}:{ep}:{season}:{resolution}"
    prior = _video_url_cache.get(vkey)
    if prior and time.time() - prior.get("ts", 0) < VIDEO_URL_TTL:
        return prior["url"]

    # Load netfilm.world DIRECTLY over HTTPS — Firebase auth silently fails over plain HTTP
    # (proxy is HTTP localhost which breaks Firebase's secure context requirement)
    page_url = (
        f"https://netfilm.world/movies/{detail_path}"
        f"?id={subject_id}&ep={ep}&resolution={resolution}"
    )
    if season:
        page_url += f"&se={season}"

    print(f"[playwright] Loading direct: {page_url[:120]}", file=sys.stderr)
    captured: list = []

    def _on_response(response):
        """Intercept every network response the browser receives."""
        try:
            url = response.url
            # Never capture localhost/proxy URLs — those are our own rewritten pages, not real CDN streams
            if "localhost" in url or "127.0.0.1" in url:
                return
            # Direct video URL in the request itself (CDN hit)
            # Explicitly skip images — webp/jpg/png/gif pass _is_video_url on aoneroom CDN domains
            _IMG_EXTS = ('.webp', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.bmp')
            if any(url.lower().split("?")[0].endswith(e) for e in _IMG_EXTS):
                return
            if _is_video_url(url) and url not in [c.get("url") for c in captured]:
                vtype = "m3u8" if ".m3u8" in url else "mp4"
                print(f"[playwright] CDN direct: {url}", file=sys.stderr)
                captured.append({"url": url, "type": vtype})
                return
            # JSON API response — look for video URL inside the body
            ct = response.headers.get("content-type", "")
            if "application/json" in ct and response.status == 200:
                # Skip ad/config endpoints — they embed a generic placeholder clip, not real content
                _AD_SKIP = ("ad/get-config", "ad-config", "advertisement", "/ads/", "adscenes")
                if any(skip in url.lower() for skip in _AD_SKIP):
                    return
                try:
                    data = response.json()
                    print(f"[playwright] JSON from {url[:80]}: keys={list(data.keys()) if isinstance(data, dict) else type(data).__name__}", file=sys.stderr)
                    result = scraper._extract_video_url_from_json(data)
                    if result:
                        found_url = result.get("url", "")
                        # Double-check: reject localhost/proxy URLs from JSON bodies too
                        if "localhost" in found_url or "127.0.0.1" in found_url:
                            print(f"[playwright] Skipping proxy URL from JSON: {found_url[:80]}", file=sys.stderr)
                            return
                        if found_url not in [c.get("url") for c in captured]:
                            print(f"[playwright] Video URL from JSON: {found_url}", file=sys.stderr)
                            captured.append(result)
                            # NOTE: do NOT write to _video_url_cache here — wait until best URL is chosen
                except Exception as je:
                    print(f"[playwright] JSON parse error {url[:60]}: {je}", file=sys.stderr)
        except Exception:
            pass

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage",
                      "--disable-setuid-sandbox"],
            )
            page = browser.new_page()
            page.on("response", _on_response)

            try:
                page.goto(page_url, timeout=30_000, wait_until="domcontentloaded")
            except Exception as nav_err:
                print(f"[playwright] nav error (continuing): {nav_err}", file=sys.stderr)

            # Let Firebase + player JS initialize
            page.wait_for_timeout(3000)

            # Click the play button — the player won't request the video URL until playback starts
            play_selectors = [
                "video",
                ".play-btn", ".play-button", ".btn-play",
                "[class*='play']",
                ".vjs-big-play-button",
                ".plyr__control--overlaid",
                ".jw-display-icon-container",
                ".mejs__overlay-play",
                "button[aria-label*='play' i]",
                "button[title*='play' i]",
            ]
            clicked = False
            for sel in play_selectors:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        el.click()
                        print(f"[playwright] Clicked: {sel}", file=sys.stderr)
                        clicked = True
                        break
                except Exception:
                    pass
            if not clicked:
                # Last resort: click body and dispatch a tap event
                try:
                    page.evaluate("document.body.click()")
                    page.evaluate("document.dispatchEvent(new MouseEvent('click', {bubbles:true}))")
                    # Also try forcing video play via JS
                    page.evaluate("""
                        const v = document.querySelector('video');
                        if (v) { v.muted = true; v.play().catch(()=>{}); }
                    """)
                    print(f"[playwright] Used JS click/play fallback", file=sys.stderr)
                except Exception:
                    pass

            # Wait up to 25s for a video URL to appear
            for _ in range(25):
                page.wait_for_timeout(1000)
                if captured:
                    break

            browser.close()
    except Exception as exc:
        print(f"[playwright] Error: {exc}", file=sys.stderr)
        return None

    if captured:
        # Filter to only real video streams — drop images, API endpoint URLs, etc.
        _VIDEO_SIGS = (".mp4", ".m3u8", ".ts", ".webm")
        _VIDEO_CDNS = ("pbcdnw.aoneroom.com", "pbcdn.aoneroom.com", "macdn.aoneroom.com", "cdn.aoneroom.com")
        real = [
            c for c in captured
            if any(sig in c.get("url", "").lower() for sig in _VIDEO_SIGS)
            or any(cdn in c.get("url", "") for cdn in _VIDEO_CDNS)
        ]
        if not real:
            print(f"[playwright] captured {len(captured)} URLs but none are real video streams", file=sys.stderr)
            for c in captured:
                print(f"[playwright]  rejected: {c.get('url','')[:100]}", file=sys.stderr)
        else:
            # Prefer m3u8, then best mp4 quality (hd > sd > ld)
            best = next((c for c in real if c.get("type") == "m3u8"), None)
            if not best:
                def _quality_rank(c):
                    u = c.get("url", "").lower()
                    if "-hd." in u or "1080" in u or "720" in u: return 0
                    if "-sd." in u or "480" in u or "360" in u: return 1
                    return 2  # -ld or unknown
                best = sorted(real, key=_quality_rank)[0]
            url = best.get("url", "")
            if url:
                print(f"[playwright] SUCCESS: {url}", file=sys.stderr)
                _video_url_cache[vkey] = {**best, "ts": time.time()}
                return url

    print(f"[playwright] No URL captured for {detail_path}", file=sys.stderr)
    return None


def _resolve_direct_source(stream: dict, ep: int, season: int, resolution: int, skip_playwright: bool = False) -> tuple[Optional[str], str]:
    """
    Try all direct sources in order. Returns (url, source_name) or (None, "").

    Order:
      stream_meta  → passive_cache → BWM → aoneroom API
      → Playwright (loads local proxy, triggers JS, proxy captures CDN URL)

    skip_playwright=True skips the slow Playwright step (used during bulk mode).
    """
    import sys
    subject_id  = stream.get("id", "")
    title       = stream.get("title", "")
    detail_path = stream.get("detail_path", "")

    # 0. Direct CDN URLs already embedded in stream metadata
    found = stream.get("_found_video_urls", [])
    if found:
        matches = [u for u in found if u.get("ep") == ep and u.get("season") == season]
        if not matches:
            matches = found
        if matches:
            best = min(matches, key=lambda u: abs(u.get("resolution", 0) - resolution))
            url = best.get("url", "")
            if _is_video_url(url):
                print(f"[resolve] stream_meta hit: {url[:80]}", file=sys.stderr)
                return url, "stream_meta"

    # 1. Passive capture cache (populated when users stream OR by playwright below)
    if subject_id:
        vkey = f"{subject_id}:{ep}:{season}:{resolution}"
        cached = _video_url_cache.get(vkey)
        if cached and time.time() - cached.get("ts", 0) < VIDEO_URL_TTL:
            return cached["url"], "passive_cache"

    # 2. BWM direct MP4 (third-party index of direct download URLs)
    if subject_id:
        try:
            bwm = scraper.get_video_sources_bwm(subject_id, ep=ep, season=season, title=title)
            if bwm:
                res_map = {"1080p": 1080, "720p": 720, "480p": 480, "360p": 360}
                best = min(bwm, key=lambda s: abs(res_map.get(s["quality"], 0) - resolution))
                url = best["url"]
                if _is_video_url(url):
                    return url, f"bwm_{best['quality']}"
        except Exception as exc:
            print(f"[resolve] BWM: {exc}", file=sys.stderr)

    # 3. Aoneroom API (direct — works for some titles when auth passes)
    if subject_id:
        try:
            ao = scraper.get_video_url(subject_id, ep=ep, season=season, resolution=resolution)
            if ao and _is_video_url(ao.get("url", "")):
                return ao["url"], "aoneroom"
        except Exception as exc:
            print(f"[resolve] aoneroom: {exc}", file=sys.stderr)

    # NOTE: step 3b (detail_trailer fallback) was removed — it returned trailer
    # clips (1-30 MB) instead of full movie files, causing 1048/1049 downloads
    # to be trailers. Never use trailer URLs as a download source.

    # 4. Playwright — loads the player page, intercepts JSON, captures CDN URL
    #    Semaphore ensures only one browser at a time to prevent crashes.
    #    Skipped during bulk mode (skip_playwright=True) to avoid 60s timeouts.
    if subject_id and detail_path and not skip_playwright:
        with _playwright_semaphore:
            url = _playwright_warm_cache(subject_id, detail_path, ep, season, resolution)
        if url and _is_video_url(url):
            return url, "playwright"

    return None, ""


def _auto_download_worker(stream: dict, ep: int, season: int, resolution: int = 1080, skip_playwright: bool = False, priority: bool = False) -> None:
    """
    Background worker: resolve source → download to VPS disk.
    Job is registered in _download_jobs IMMEDIATELY so it's always visible in /library-status.
    priority=True uses the _priority_semaphore (user-triggered) so it never waits behind 8 bulk jobs.
    skip_playwright=True skips the slow Playwright step (used during bulk downloads).
    """
    import sys

    safe_title = _safe_name(stream.get("title", "unknown"))[:60]
    imdb_id    = stream.get("imdb_id", "")
    is_series  = stream.get("is_series", False)
    filepath, filename = _job_filepath(safe_title, ep, season, resolution)

    # Pre-register job BEFORE acquiring semaphore — always visible in status
    job_id = f"auto_{safe_title}_{ep}_{season}"
    _download_jobs[job_id] = {
        "status": "pending", "progress": 0,
        "filepath": filepath, "filename": filename,
        "title": stream.get("title", ""),
        "ts": time.time(), "auto": True,
    }

    # Early exit: already on disk
    if existing := _find_local_file(safe_title, ep, season):
        _download_jobs[job_id]["status"] = "skipped_exists"
        detail_path = stream.get("detail_path", "")
        if detail_path and detail_path not in _lib_index:
            try:
                sz = os.path.getsize(existing) / 1_048_576
            except OSError:
                sz = 0
            _save_lib_entry(detail_path, stream, sz)
        return

    # Guard: pause if disk is nearly full
    free_gb = _free_disk_gb()
    if free_gb < _DISK_PAUSE_GB:
        _download_jobs[job_id].update({
            "status": "paused_disk_full",
            "error": f"Disk low: {free_gb:.1f} GB free (need {_DISK_PAUSE_GB} GB)",
        })
        print(f"[auto-dl] DISK LOW ({free_gb:.1f} GB free) — pausing {filename}", file=sys.stderr)
        return

    # Wait for a download slot — priority jobs use a separate semaphore so they
    # never queue behind 8 bulk downloads
    _sem = _priority_semaphore if priority else _dl_semaphore
    _download_jobs[job_id]["status"] = "waiting"
    with _sem:
        # Re-check inside lock
        if existing := _find_local_file(safe_title, ep, season):
            _download_jobs[job_id]["status"] = "skipped_exists"
            detail_path = stream.get("detail_path", "")
            if detail_path and detail_path not in _lib_index:
                try:
                    sz = os.path.getsize(existing) / 1_048_576
                except OSError:
                    sz = 0
                _save_lib_entry(detail_path, stream, sz)
            return

        # ── Step 0: Torrentio / aria2c (primary source — no auth required) ──────
        # Resolve IMDB ID first — get_stream_info does a lookup but bulk mode
        # calls _trigger_auto_download with the light search result which may
        # have imdb_id=None.  Attempt scraper lookup if missing.
        effective_imdb = imdb_id
        if not effective_imdb:
            title = stream.get("title", "")
            year  = (stream.get("release_date") or "")[:4]
            try:
                effective_imdb = scraper.lookup_imdb_id(title, year or None, is_series) or ""
                if effective_imdb:
                    stream["imdb_id"] = effective_imdb
                    print(f"[auto-dl] Resolved IMDB: {title} → {effective_imdb}", file=sys.stderr)
            except Exception as exc:
                print(f"[auto-dl] IMDB lookup failed for {title}: {exc}", file=sys.stderr)

        if effective_imdb and effective_imdb in _torrent_no_streams:
            print(f"[auto-dl] Skipping {filename} — Torrentio previously returned 0 streams for {effective_imdb}", file=sys.stderr)
            effective_imdb = ""   # fall through to direct sources

        if effective_imdb:
            torrent_ok = _download_via_torrent(
                job_id, effective_imdb, is_series, ep, season, filepath, filename
            )
            if torrent_ok:
                detail_path = stream.get("detail_path", "")
                if detail_path:
                    _save_lib_entry(detail_path, stream, _download_jobs[job_id].get("size_mb", 0))
                return
            print(f"[auto-dl] Torrent failed for {filename}, trying direct sources", file=sys.stderr)

        # ── Step 1: try direct sources (BWM / cache / aoneroom / playwright) ──
        direct_url, src_name = _resolve_direct_source(stream, ep, season, resolution, skip_playwright=skip_playwright)
        if direct_url:
            _download_jobs[job_id]["source"] = src_name
            print(f"[auto-dl] {filename} via {src_name} (direct)", file=sys.stderr)
            _run_download_job(job_id, direct_url, filepath, filename, is_embed=False)
            if _download_jobs[job_id]["status"] == "ready":
                detail_path = stream.get("detail_path", "")
                if detail_path:
                    _save_lib_entry(detail_path, stream, _download_jobs[job_id].get("size_mb", 0))
                return
            print(f"[auto-dl] Direct source failed", file=sys.stderr)

        # All sources exhausted
        subj = stream.get("id", "")
        _download_jobs[job_id].update({
            "status": "error",
            "error": f"No source found for {stream.get('title','')} (imdb={effective_imdb or 'none'}, subj={subj})",
        })
        _bulk_stats["dl_errors"] = _bulk_stats.get("dl_errors", 0) + 1
        print(f"[auto-dl] {filename} — all sources failed (imdb={effective_imdb or 'none'})", file=sys.stderr)


VIDEO_URL_TTL = 7200  # 2 hours


def _cached(key: str, fn, *args, **kwargs):
    entry = _cache.get(key)
    age = time.time() - entry["ts"] if entry else float("inf")
    if entry and age < CACHE_TTL:
        return entry["data"]
    if entry and age < CACHE_STALE:
        if key not in _refresh_lock:
            _refresh_lock.add(key)
            def _refresh():
                try:
                    result = fn(*args, **kwargs)
                    if result is not None:
                        _cache[key] = {"data": result, "ts": time.time()}
                finally:
                    _refresh_lock.discard(key)
            threading.Thread(target=_refresh, daemon=True).start()
        return entry["data"]
    result = fn(*args, **kwargs)
    if result is not None:
        _cache[key] = {"data": result, "ts": time.time()}
    return result


def _warm_cache():
    def _store(key, fn, **kwargs):
        try:
            if key not in _cache:
                result = fn(**kwargs)
                if result is not None:
                    _cache[key] = {"data": result, "ts": time.time()}
        except Exception:
            pass

    _store("home",                             scraper.get_home)
    _store("trending:1:20",                    scraper.get_trending,  page=1, per_page=20)
    _store("movies:1:48:None:None:None:None",  scraper.get_movies,    page=1, per_page=48)
    _store("movies:1:48:1:None:None:None",     scraper.get_movies,    page=1, per_page=48, subject_type=1)
    _store("movies:1:48:2:None:None:None",     scraper.get_movies,    page=1, per_page=48, subject_type=2)

    # Pre-warm stream data (IMDB lookup included) for banner + trending titles
    banner_paths: list[str] = []
    home = _cache.get("home", {}).get("data") or {}
    if home.get("banner"):
        for item in home["banner"].get("items", [])[:8]:
            dp = item.get("detailPath")
            if dp:
                banner_paths.append(dp)
                _store(f"stream:{dp}", scraper.get_stream_info, detail_path=dp)

    # After stream data is ready, pre-warm server availability for those titles
    if banner_paths:
        threading.Thread(
            target=lambda: asyncio.run(_warm_server_checks(banner_paths)),
            daemon=True,
        ).start()


async def _warm_server_checks(detail_paths: list[str]):
    """Pre-probe embed servers for a list of detail paths (background task)."""
    for dp in detail_paths:
        try:
            stream = _cache.get(f"stream:{dp}", {}).get("data") or {}
            imdb = stream.get("imdb_id")
            if not imdb:
                continue
            media = "tv" if stream.get("is_series") else "movie"
            await _get_ranked_servers(imdb, media, 1, 1)
        except Exception:
            pass


def now():
    return datetime.utcnow().isoformat() + "Z"


# ─────────────────────────────────────────────────────────────────────────────
# Embed server checking helpers
# ─────────────────────────────────────────────────────────────────────────────

_SERVER_TIMEOUT = 6

def _build_embed_urls(imdb_id: str, media_type: str, season: int, episode: int) -> list[dict]:
    t = media_type.lower()
    is_tv = t in ("tv", "series")
    if is_tv:
        return [
            {"label": "Server 1", "url": f"https://vidsrc.to/embed/tv/{imdb_id}/{season}/{episode}"},
            {"label": "Server 2", "url": f"https://vidsrc.me/embed/tv?imdb={imdb_id}&season={season}&episode={episode}"},
            {"label": "Server 3", "url": f"https://player.autoembed.cc/embed/tv/{imdb_id}/{season}/{episode}"},
        ]
    return [
        {"label": "Server 1", "url": f"https://vidsrc.to/embed/movie/{imdb_id}"},
        {"label": "Server 2", "url": f"https://vidsrc.me/embed/movie?imdb={imdb_id}"},
        {"label": "Server 3", "url": f"https://player.autoembed.cc/embed/movie/{imdb_id}"},
    ]


async def _probe(client: httpx.AsyncClient, server: dict) -> dict:
    t0 = time.monotonic()
    try:
        r = await client.head(
            server["url"],
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        ok = r.status_code < 500
    except Exception:
        ok = False
    latency = int((time.monotonic() - t0) * 1000)
    return {**server, "ok": ok, "latency_ms": latency}


async def _get_ranked_servers(imdb_id: str, media_type: str, season: int, episode: int) -> dict:
    """Return ranked server list — cached for SERVER_CACHE_TTL seconds."""
    # For series, cache per IMDB ID only (server availability doesn't change per episode)
    is_tv = media_type.lower() in ("tv", "series")
    cache_key = f"srv:{imdb_id}:{media_type}" if is_tv else f"srv:{imdb_id}"
    entry = _server_cache.get(cache_key)
    if entry and time.time() - entry["ts"] < SERVER_CACHE_TTL:
        # Re-build correct episode URLs from cached ok/latency results but with current ep/season
        cached = entry["data"]
        if is_tv:
            fresh_urls = {s["label"]: s for s in _build_embed_urls(imdb_id, media_type, season, episode)}
            servers = [
                {**s, "url": fresh_urls[s["label"]]["url"]}
                for s in cached["servers"]
                if s["label"] in fresh_urls
            ]
            return {**cached, "servers": servers, "cached": True}
        return {**cached, "cached": True}

    candidates = _build_embed_urls(imdb_id, media_type, season, episode)
    async with httpx.AsyncClient(timeout=_SERVER_TIMEOUT) as client:
        results = await asyncio.gather(*[_probe(client, s) for s in candidates])

    working = sorted([r for r in results if r["ok"]],     key=lambda x: x["latency_ms"])
    failed  = sorted([r for r in results if not r["ok"]], key=lambda x: x["latency_ms"])
    ranked  = working + failed

    data = {
        "servers": [
            {"label": s["label"], "url": s["url"], "ok": s["ok"], "latency_ms": s["latency_ms"]}
            for s in ranked
        ],
        "working_count": len(working),
        "cached": False,
    }
    _server_cache[cache_key] = {"data": data, "ts": time.time()}
    return data


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    docs_path = os.path.join(os.path.dirname(__file__), "docs.html")
    if os.path.exists(docs_path):
        return FileResponse(docs_path, media_type="text/html")
    return {
        "api": "Chege Movie API",
        "version": "3.0.0",
        "docs": "/docs",
    }


@app.get("/img")
async def proxy_image(url: str = Query(...)):
    parsed = urllib.parse.urlparse(url)
    if not any(parsed.netloc.endswith(d) for d in _CDN_DOMAINS):
        raise HTTPException(status_code=403, detail="Domain not allowed")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "image/*,*/*",
            })
        return Response(
            content=r.content,
            media_type=r.headers.get("content-type", "image/jpeg"),
            headers={"Cache-Control": "public, max-age=86400", "Access-Control-Allow-Origin": "*"},
        )
    except Exception:
        raise HTTPException(status_code=502, detail="Failed to fetch image")


@app.get("/home")
async def get_home():
    result = _cached("home", scraper.get_home)
    return {
        "status": 200, "success": True, "creator": "Chege tech",
        "results": {
            "banner": result.get("banner"),
            "platformList": result.get("platformList", []),
            "operatingList": result.get("operatingList", []),
        },
    }


@app.get("/genres")
async def get_genres():
    """Return all known genres available on the platform."""
    genres = [
        "Action", "Adventure", "Animation", "Anime", "Biography", "Comedy",
        "Crime", "Documentary", "Drama", "Fantasy", "History", "Horror",
        "K-Drama", "Music", "Mystery", "Romance", "Sci-Fi", "Sport",
        "Thriller", "War", "Western",
    ]
    return {"status": "success", "timestamp": now(), "data": {"genres": genres}}


@app.get("/movies")
async def get_movies(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    type: Optional[str] = Query(None),
    genre: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    country: Optional[str] = Query(None),
):
    subject_type = None
    if type == "movie":   subject_type = 1
    elif type == "series": subject_type = 2

    cache_key = f"movies:{page}:{limit}:{subject_type}:{genre}:{year}:{country}"
    result = _cached(cache_key, scraper.get_movies,
                     page=page, per_page=limit,
                     subject_type=subject_type, genre=genre, country=country, year=year)

    pager = result.get("pager", {})
    return {
        "status": "success", "timestamp": now(),
        "data": {
            "movies": result["items"],
            "total": pager.get("totalCount", len(result["items"])),
            "page": page, "limit": limit,
            "has_more": pager.get("hasMore", False),
        },
    }


@app.get("/movies/top-rated")
async def get_top_rated(
    limit: int = Query(20, ge=1, le=100),
    type: Optional[str] = Query(None, description="movie | series"),
    min_rating: float = Query(7.5, ge=0, le=10),
):
    """Return highly-rated titles sorted by IMDB score."""
    subject_type = None
    if type == "movie":    subject_type = 1
    elif type == "series": subject_type = 2

    cache_key = f"movies:1:100:{subject_type}:None:None:None"
    result = _cached(cache_key, scraper.get_movies,
                     page=1, per_page=100, subject_type=subject_type)

    items = result.get("items", [])
    rated = [
        m for m in items
        if m.get("imdb_rating") and float(m["imdb_rating"]) >= min_rating
    ]
    rated.sort(key=lambda x: float(x.get("imdb_rating") or 0), reverse=True)
    return {
        "status": "success", "timestamp": now(),
        "data": {"movies": rated[:limit], "total": len(rated)},
    }


@app.get("/movies/new-releases")
async def get_new_releases(
    limit: int = Query(20, ge=1, le=100),
    type: Optional[str] = Query(None, description="movie | series"),
):
    """Return recently released titles (this year + last year), sorted by date."""
    from datetime import datetime as _dt
    current_year = _dt.utcnow().year
    subject_type = None
    if type == "movie":    subject_type = 1
    elif type == "series": subject_type = 2

    cy_key = f"movies:1:50:{subject_type}:None:{current_year}:None"
    py_key = f"movies:1:50:{subject_type}:None:{current_year - 1}:None"

    cy = _cached(cy_key, scraper.get_movies, page=1, per_page=50, subject_type=subject_type, year=current_year)
    py = _cached(py_key, scraper.get_movies, page=1, per_page=50, subject_type=subject_type, year=current_year - 1)

    combined = list(cy.get("items", [])) + list(py.get("items", []))
    combined.sort(key=lambda x: x.get("release_date") or "", reverse=True)
    combined = combined[:limit]
    return {
        "status": "success", "timestamp": now(),
        "data": {"movies": combined, "total": len(combined), "years": [current_year, current_year - 1]},
    }


@app.get("/movies/similar/{detail_path:path}")
async def get_similar(
    detail_path: str,
    limit: int = Query(12, ge=1, le=50),
):
    """Return similar titles based on genre matching."""
    cache_key = f"stream:{detail_path}"
    stream = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not stream:
        raise HTTPException(status_code=404, detail="Title not found")

    subject_type = 2 if stream.get("is_series") else 1
    genre_str    = stream.get("genre") or ""
    genres       = [g.strip() for g in genre_str.split(",") if g.strip()]

    if genres:
        primary_genre = genres[0]
        g_key  = f"movies:1:60:{subject_type}:{primary_genre}:None:None"
        result = _cached(g_key, scraper.get_movies, page=1, per_page=60,
                         subject_type=subject_type, genre=primary_genre)
        items  = [m for m in result.get("items", []) if m.get("detail_path") != detail_path]

        # Score by genre overlap so best matches come first
        genre_set = set(genres)
        def _score(m):
            m_genres = set(g.strip() for g in (m.get("genre") or "").split(",") if g.strip())
            m_genres |= set(m.get("genres") or [])
            return len(m_genres & genre_set)

        items.sort(key=_score, reverse=True)
        return {
            "status": "success", "timestamp": now(),
            "data": {"similar": items[:limit], "based_on": genres, "total": len(items)},
        }

    # Fallback: use existing related endpoint
    rel_key = f"related:{stream['id']}:1:{limit}"
    result  = _cached(rel_key, scraper.get_related, subject_id=stream["id"], page=1, per_page=limit)
    return {
        "status": "success", "timestamp": now(),
        "data": {"similar": result.get("items", [])[:limit], "based_on": [], "total": len(result.get("items", []))},
    }


@app.get("/imdb/lookup")
async def imdb_lookup(
    title: str  = Query(..., min_length=1),
    year:  Optional[str] = Query(None),
    type:  str  = Query("movie"),
):
    """Resolve a title + year to an IMDB ID using the scraper's lookup."""
    is_series   = type in ("series", "tv", "show")
    cache_key   = f"imdb:{title.lower()}:{year}:{type}"
    imdb_id     = _cached(cache_key, scraper.lookup_imdb_id, title=title, year=year, is_series=is_series)
    if not imdb_id:
        raise HTTPException(status_code=404, detail="IMDB ID not found for this title")
    return {
        "status": "success", "timestamp": now(),
        "data": {"imdb_id": imdb_id, "title": title, "year": year, "type": type},
    }


@app.get("/movies/search")
async def search_movies(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    cache_key = f"search:{q}:{page}:{limit}"
    result = _cached(cache_key, scraper.search, keyword=q, page=page, per_page=limit)
    pager = result.get("pager", {})
    return {
        "status": "success", "timestamp": now(),
        "data": {
            "query": q,
            "results": result["items"],
            "counts": result.get("counts", []),
            "total": pager.get("totalCount", len(result["items"])),
            "page": page, "limit": limit,
            "has_more": pager.get("hasMore", False),
        },
    }


@app.get("/movies/featured")
async def get_featured(limit: int = Query(20, ge=1, le=50)):
    """Daily curated mix: top-rated + trending + new releases, shuffled with a date seed."""
    import random
    from datetime import datetime as _dt
    current_year = _dt.utcnow().year

    tr  = _cached("trending:1:20",                               scraper.get_trending, page=1, per_page=20)
    top = _cached("movies:1:100:None:None:None:None",            scraper.get_movies,   page=1, per_page=100)
    ny  = _cached(f"movies:1:50:None:None:{current_year}:None",  scraper.get_movies,   page=1, per_page=50, year=current_year)

    seen: set = set()
    pool: list = []

    for item in tr.get("items", []):
        if item["detail_path"] not in seen:
            seen.add(item["detail_path"])
            pool.append({**item, "_src": "trending"})

    for item in sorted(top.get("items", []), key=lambda x: float(x.get("imdb_rating") or 0), reverse=True)[:30]:
        if item["detail_path"] not in seen and float(item.get("imdb_rating") or 0) >= 7.0:
            seen.add(item["detail_path"])
            pool.append({**item, "_src": "top_rated"})

    for item in ny.get("items", []):
        if item["detail_path"] not in seen:
            seen.add(item["detail_path"])
            pool.append({**item, "_src": "new"})

    day_seed = int(_dt.utcnow().strftime("%Y%m%d"))
    rng = random.Random(day_seed)
    rng.shuffle(pool)

    return {
        "status": "success", "timestamp": now(),
        "data": {"movies": pool[:limit], "total": len(pool)},
    }


@app.get("/movies/suggest")
async def search_suggest(q: str = Query(..., min_length=1)):
    result = scraper.search_suggest(q)
    return {"status": "success", "timestamp": now(), "data": {"query": q, "suggestions": result}}


@app.get("/movie/{movie_id}")
async def get_movie(movie_id: str):
    result = scraper.get_by_id(movie_id)
    if not result:
        raise HTTPException(status_code=404, detail="Movie not found")
    return {"status": "success", "timestamp": now(), "data": result}


@app.get("/trending")
async def get_trending(page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=100)):
    cache_key = f"trending:{page}:{limit}"
    result = _cached(cache_key, scraper.get_trending, page=page, per_page=limit)
    pager = result.get("pager", {})
    return {
        "status": "success", "timestamp": now(),
        "data": {"movies": result["items"], "page": page, "limit": limit,
                 "has_more": pager.get("hasMore", False)},
    }


@app.get("/recent")
async def get_recent(page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=100)):
    cache_key = f"recent:{page}:{limit}"
    result = _cached(cache_key, scraper.get_movies, page=page, per_page=limit)
    pager = result.get("pager", {})
    return {
        "status": "success", "timestamp": now(),
        "data": {"movies": result["items"], "page": page, "limit": limit,
                 "has_more": pager.get("hasMore", False)},
    }


@app.get("/ranking")
async def get_ranking(
    id: str = Query("", description="Ranking list ID — leave blank for default"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """Return a ranked chart. Leave `id` blank to get the default/top chart."""
    cache_key = f"ranking:{id}:{page}:{limit}"
    result = _cached(cache_key, scraper.get_ranking, ranking_id=id, page=page, per_page=limit)
    pager = result.get("pager", {})
    return {
        "status": "success", "timestamp": now(),
        "data": {"movies": result["items"], "page": page, "limit": limit,
                 "has_more": pager.get("hasMore", False)},
    }


@app.get("/popular-searches")
async def popular_searches():
    result = _cached("popular_searches", scraper.get_popular_searches)
    return {"status": "success", "timestamp": now(), "data": {"searches": result}}


def _trigger_auto_download(result: dict, ep: int = 1, season: int = 0, skip_playwright: bool = False, priority: bool = False) -> None:
    """
    Fire-and-forget: auto-download this title to VPS disk if not already present.
    Key is (imdb_id or title, ep, season) — only queued once per server lifetime.
    priority=True: user-triggered, uses priority semaphore (won't wait behind 8 bulk jobs).
    skip_playwright=True is used by bulk mode to avoid 60s Playwright timeouts.
    """
    import sys
    is_series = result.get("is_series", False)
    # For series: queue ep=1, season=1 (normalise season=0 → 1 to match bulk filenames).
    # For movies: ep=1, season=0.
    _ep     = ep if is_series else 1
    _season = (max(season, 1) if is_series else 0)
    akey = f"{result.get('imdb_id') or result.get('id') or result.get('title', '')}:{_ep}:{_season}"
    # Priority requests always re-queue even if already in bulk queue
    if not priority and akey in _auto_queued:
        return
    safe_title = _safe_name(result.get("title", "unknown"))[:60]
    if _find_local_file(safe_title, _ep, _season):
        _auto_queued.add(akey)
        return
    _auto_queued.add(akey)
    tag = "[priority-dl]" if priority else "[auto-dl]"
    print(f"{tag} Queuing download: {result.get('title')} s{_season}e{_ep}", file=sys.stderr)
    threading.Thread(
        target=_auto_download_worker,
        args=(result, _ep, _season),
        kwargs={"skip_playwright": skip_playwright, "priority": priority},
        daemon=True,
    ).start()


@app.get("/stream-video/{detail_path:path}")
async def stream_video(
    request: Request,
    detail_path: str,
    ep: int = Query(1),
    season: int = Query(0),
):
    """
    Stream a locally-downloaded MP4 via X-Accel-Redirect.
    FastAPI only resolves the filename; nginx serves the bytes directly from disk
    using sendfile — no Python byte streaming, full kernel-speed throughput.
    """
    cache_key = f"stream:{detail_path}"
    stream = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not stream:
        raise HTTPException(status_code=404, detail="Title not found")

    safe_title = _safe_name(stream.get("title", detail_path))[:60]
    local_path  = _find_local_file(safe_title, ep, season)
    if not local_path:
        raise HTTPException(status_code=404, detail="File not on server yet")

    filename = os.path.basename(local_path)
    # X-Accel-Redirect tells nginx to serve /_video_files/<filename> directly from disk.
    # The /_video_files/ internal location maps to /opt/movie-downloads/.
    # nginx handles Range requests, Content-Length, and sendfile automatically.
    from fastapi.responses import Response
    return Response(
        status_code=200,
        headers={
            "X-Accel-Redirect":    f"/_video_files/{filename}",
            "X-Accel-Buffering":   "yes",
            "Content-Type":        "video/mp4",
            "Accept-Ranges":       "bytes",
            "Cache-Control":       "no-transform",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "Content-Range, Accept-Ranges, Content-Length",
        },
    )


@app.get("/stream/{detail_path:path}")
async def get_stream(
    detail_path: str,
    ep: int = Query(1),
    season: int = Query(0),
):
    cache_key = f"stream:{detail_path}"
    result = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not result:
        raise HTTPException(status_code=404, detail="Stream info not found")
    # Auto-download in background — non-blocking
    _trigger_auto_download(result, ep=ep, season=season)
    return {"status": "success", "timestamp": now(), "data": result}


@app.get("/play/{detail_path:path}")
async def play(
    detail_path: str,
    ep: int = Query(1, ge=1, description="Episode number"),
    season: int = Query(1, ge=1, description="Season number"),
):
    """
    Combined endpoint: stream metadata + ranked embed servers in one request.
    Stream data is cached (1h). Server probes are cached (10 min).
    Use this instead of calling /stream/ + /servers separately.
    """
    cache_key = f"stream:{detail_path}"
    stream = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not stream:
        raise HTTPException(status_code=404, detail="Title not found")

    servers_data = None
    if stream.get("imdb_id"):
        media_type = "tv" if stream["is_series"] else "movie"
        servers_data = await _get_ranked_servers(stream["imdb_id"], media_type, season, ep)

    return {
        "status": "success", "timestamp": now(),
        "data": {"stream": stream, "servers": servers_data},
    }


@app.get("/servers")
async def check_servers(
    imdb_id: str = Query(...),
    type: str = Query("movie"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
):
    data = await _get_ranked_servers(imdb_id, type, season, episode)
    return {
        "status": "success", "timestamp": now(),
        "data": {"imdb_id": imdb_id, **data},
    }


@app.get("/related/{subject_id}")
async def get_related(
    subject_id: str,
    page: int = Query(1, ge=1),
    limit: int = Query(12, ge=1, le=50),
):
    cache_key = f"related:{subject_id}:{page}:{limit}"
    result = _cached(cache_key, scraper.get_related, subject_id=subject_id, page=page, per_page=limit)
    pager = result.get("pager", {})
    return {
        "status": "success", "timestamp": now(),
        "data": {"related": result["items"], "page": page, "limit": limit,
                 "has_more": pager.get("hasMore", False)},
    }


@app.post("/prepare-download/{detail_path:path}")
async def prepare_download(
    detail_path: str,
    ep: int = Query(1),
    season: int = Query(0),
    resolution: int = Query(1080),
):
    """
    Queue a VPS-disk download. Returns immediately with a job_id.
    Client polls /download-job/{job_id} for progress, then GETs /download-file/{job_id}.
    """
    import sys

    cache_key = f"stream:{detail_path}"
    stream = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not stream:
        raise HTTPException(status_code=404, detail="Title not found")

    safe_title  = _safe_name(stream.get("title", detail_path))[:60]
    subject_id  = stream.get("id", "")
    imdb_id     = stream.get("imdb_id", "")
    is_series   = stream.get("is_series", False)
    detail_path_escaped = stream.get("detail_path", detail_path)

    filepath, filename = _job_filepath(safe_title, ep, season, resolution)

    # ── Already on disk ───────────────────────────────────────────────────────
    if os.path.exists(filepath) and os.path.getsize(filepath) > 50 * 1_048_576:
        job_id = f"cached_{safe_title}_{ep}_{season}_{resolution}"
        _download_jobs[job_id] = {
            "status": "ready", "progress": 100,
            "filepath": filepath, "filename": filename,
            "size_mb": os.path.getsize(filepath) // 1_048_576,
            "ts": time.time(),
        }
        print(f"[prepare-dl] Serving cached file: {filepath}", file=sys.stderr)
        return {"job_id": job_id, "status": "ready", "cached": True}

    # ── Active job for same file ──────────────────────────────────────────────
    for jid, job in _download_jobs.items():
        if job.get("filepath") == filepath and job["status"] in ("queued", "downloading"):
            return {"job_id": jid, "status": job["status"]}

    # ── Determine source URL ──────────────────────────────────────────────────
    source_url = None
    is_embed   = False

    # 1. Passive capture cache (user already watched → URL is live)
    if subject_id:
        vkey   = f"{subject_id}:{ep}:{season}:{resolution}"
        cached = _video_url_cache.get(vkey)
        if cached and time.time() - cached.get("ts", 0) < VIDEO_URL_TTL:
            source_url = cached["url"]
            print(f"[prepare-dl] Source: passive-cache", file=sys.stderr)

    # 2. BWM direct MP4
    if not source_url and subject_id:
        bwm = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: scraper.get_video_sources_bwm(
                subject_id, ep=ep, season=season, title=stream.get("title", safe_title)
            ),
        )
        if bwm:
            res_map = {"1080p": 1080, "720p": 720, "480p": 480, "360p": 360}
            best = min(bwm, key=lambda s: abs(res_map.get(s["quality"], 0) - resolution))
            source_url = best["url"]
            print(f"[prepare-dl] Source: bwm ({best['quality']})", file=sys.stderr)

    # 3. Torrentio / aria2c (no auth, no geo-block, real 1080p)
    if not source_url and imdb_id and imdb_id.startswith("tt"):
        job_id = str(uuid.uuid4())[:8]
        _download_jobs[job_id] = {
            "status": "queued", "progress": 0,
            "filepath": filepath, "filename": filename,
            "ts": time.time(),
        }
        def _torrent_worker():
            ok = _download_via_torrent(job_id, imdb_id, is_series, ep, season, filepath, filename)
            if not ok:
                _download_jobs[job_id].update({"status": "error", "error": "Torrent download failed"})
        threading.Thread(target=_torrent_worker, daemon=True).start()
        print(f"[prepare-dl] Source: torrentio (imdb={imdb_id})", file=sys.stderr)
        return {"job_id": job_id, "status": "queued"}

    # 4. vidsrc.to embed — yt-dlp will extract + download in one shot
    if not source_url and imdb_id and imdb_id.startswith("tt"):
        s = max(season, 1)
        if is_series:
            source_url = f"https://vidsrc.to/embed/tv/{imdb_id}/{s}/{ep}"
        else:
            source_url = f"https://vidsrc.to/embed/movie/{imdb_id}"
        is_embed = True
        print(f"[prepare-dl] Source: vidsrc.to embed", file=sys.stderr)

    if not source_url:
        raise HTTPException(status_code=404, detail="No download source found for this title.")

    # ── Start background download ─────────────────────────────────────────────
    job_id = str(uuid.uuid4())[:8]
    _download_jobs[job_id] = {
        "status": "queued", "progress": 0,
        "filepath": filepath, "filename": filename,
        "ts": time.time(),
    }
    threading.Thread(
        target=_run_download_job,
        args=(job_id, source_url, filepath, filename, is_embed),
        daemon=True,
    ).start()
    return {"job_id": job_id, "status": "queued"}


@app.get("/download-job/{job_id}")
async def download_job_status(job_id: str):
    """Poll this to track progress of a VPS-disk download job."""
    job = _download_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "job_id":    job_id,
        "status":    job["status"],
        "progress":  round(job.get("progress", 0), 1),
        "filename":  job.get("filename", ""),
        "size_mb":   job.get("size_mb"),
        "error":     job.get("error"),
    }


@app.get("/download-file/{job_id}")
async def download_file_by_job(job_id: str):
    """Serve a completed VPS-disk download directly to the browser."""
    job = _download_jobs.get(job_id)
    if not job or job["status"] != "ready":
        raise HTTPException(status_code=404, detail="File not ready yet")
    filepath = job["filepath"]
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File missing from disk")
    filename = job["filename"]
    return Response(
        status_code=200,
        headers={
            "X-Accel-Redirect":    f"/_video_files/{os.path.basename(filepath)}",
            "X-Accel-Buffering":   "yes",
            "Content-Type":        "application/octet-stream",
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Accept-Ranges":       "bytes",
        },
    )


@app.post("/build-library")
async def start_build_library(
    max_pages: int   = Query(500,  description="Max pages to scrape per source"),
    series:    bool  = Query(True, description="Also download series (ep1 s1)"),
    concurrency: int = Query(3,    description="Parallel yt-dlp downloads"),
):
    """Start the bulk MovieBox → VPS downloader in the background."""
    global _bulk_active, _bulk_stop
    if _bulk_active:
        return {"status": "already_running", "stats": _bulk_stats}
    _bulk_stop = False
    threading.Thread(
        target=_bulk_download_all,
        args=(max_pages, series, concurrency),
        daemon=True,
    ).start()
    return {"status": "started", "max_pages": max_pages, "series": series, "concurrency": concurrency}


@app.delete("/build-library")
async def stop_build_library():
    """Gracefully stop the bulk downloader (active yt-dlp jobs finish, no new ones start)."""
    global _bulk_stop
    _bulk_stop = True
    return {"status": "stopping", "stats": _bulk_stats}


@app.get("/library-status")
async def library_status():
    """Overall stats: disk library + active downloads + bulk builder progress."""
    _ensure_download_dir()

    # Count files on disk
    total_files  = 0
    total_bytes  = 0
    if os.path.isdir(_DOWNLOAD_DIR):
        for fname in os.listdir(_DOWNLOAD_DIR):
            if fname.endswith(".mp4"):
                fpath = os.path.join(_DOWNLOAD_DIR, fname)
                try:
                    total_bytes += os.path.getsize(fpath)
                    total_files += 1
                except OSError:
                    pass

    all_jobs = list(_download_jobs.values())

    # Active / in-flight
    active_jobs = [
        {
            "title":    job.get("title", job.get("filename", "")),
            "status":   job["status"],
            "progress": job.get("progress", 0),
            "source":   job.get("source", ""),
        }
        for job in all_jobs
        if job["status"] in ("pending", "waiting", "queued", "downloading")
    ]

    # Last 10 errors — most useful for debugging
    error_jobs = [
        {
            "title":  job.get("title", job.get("filename", "")),
            "error":  job.get("error", ""),
            "source": job.get("source", ""),
        }
        for job in all_jobs
        if job["status"] == "error"
    ][-10:]

    elapsed = None
    if _bulk_stats.get("started_at"):
        end = _bulk_stats.get("finished_at") or time.time()
        elapsed = round(end - _bulk_stats["started_at"])

    by_status: dict = {}
    for j in all_jobs:
        s = j["status"]
        by_status[s] = by_status.get(s, 0) + 1

    return {
        "library": {
            "total_files": total_files,
            "total_gb":    round(total_bytes / 1_073_741_824, 2),
            "path":        _DOWNLOAD_DIR,
        },
        "bulk_builder": {
            "active":         _bulk_active,
            "stop_requested": _bulk_stop,
            "elapsed_sec":    elapsed,
            "stats":          _bulk_stats,
        },
        "jobs_by_status":   by_status,
        "active_downloads": active_jobs,
        "last_errors":      error_jobs,
    }


@app.get("/local-library")
async def local_library():
    """List all movies/episodes already downloaded to VPS disk."""
    import sys
    _ensure_download_dir()
    files = []
    try:
        for fname in sorted(os.listdir(_DOWNLOAD_DIR)):
            if not fname.endswith(".mp4"):
                continue
            fpath = os.path.join(_DOWNLOAD_DIR, fname)
            try:
                size = os.path.getsize(fpath)
                mtime = os.path.getmtime(fpath)
            except OSError:
                continue
            # Parse filename: {title}_s{season}e{ep}_{res}p.mp4
            stem = fname[:-4]
            title = stem
            res = ""
            ep_info = {}
            # Extract resolution
            if "_" in stem:
                parts = stem.rsplit("_", 1)
                if parts[1].endswith("p") and parts[1][:-1].isdigit():
                    res = parts[1]
                    stem = parts[0]
            # Extract episode/season
            import re as _re
            m = _re.search(r"_s(\d+)e(\d+)$", stem)
            if m:
                ep_info = {"season": int(m.group(1)), "ep": int(m.group(2))}
                title = stem[: m.start()].replace("_", " ").strip()
            files.append({
                "filename":  fname,
                "title":     title,
                "resolution": res,
                "size_mb":   round(size / 1_048_576, 1),
                "downloaded_at": mtime,
                **ep_info,
            })
    except Exception as exc:
        print(f"[local-library] Error: {exc}", file=sys.stderr)
    return {
        "status": "success",
        "count": len(files),
        "total_gb": round(sum(f["size_mb"] for f in files) / 1024, 2),
        "files": files,
    }


@app.get("/local-library-movies")
async def local_library_movies(limit: int = Query(500)):
    """
    Return downloaded titles as Movie objects.
    Primary source: persistent library index (_lib_index, survives restarts).
    Fallback: cross-reference in-memory stream cache (catches titles from this session).
    """
    import re as _re
    _ensure_download_dir()

    # Build safe_title → size_mb map from disk (for total count + size updates)
    downloaded: dict[str, float] = {}
    if os.path.isdir(_DOWNLOAD_DIR):
        for fname in os.listdir(_DOWNLOAD_DIR):
            if not fname.endswith(".mp4"):
                continue
            stem = fname[:-4]
            p = stem.rsplit("_", 1)
            if len(p) == 2 and p[1].endswith("p") and p[1][:-1].isdigit():
                stem = p[0]
            m = _re.search(r"_s\d{2}e\d{2}$", stem)
            if m:
                stem = stem[: m.start()]
            try:
                sz = os.path.getsize(os.path.join(_DOWNLOAD_DIR, fname)) / 1_048_576
            except OSError:
                sz = 0
            downloaded[stem] = sz

    movies: dict[str, dict] = {}  # detail_path → movie entry

    # 1) Load from persistent index (most entries, survives restarts)
    with _lib_index_lock:
        index_snapshot = dict(_lib_index)
    for detail_path, entry in index_snapshot.items():
        title = entry.get("title", "")
        safe  = _safe_name(title)[:60]
        if safe in downloaded:
            movies[detail_path] = dict(entry)
            movies[detail_path]["size_mb"] = round(downloaded.get(safe, 0), 1)

    # 2) Supplement with in-memory stream cache (new downloads this session)
    for key, entry in list(_cache.items()):
        if not key.startswith("stream:"):
            continue
        s = (entry.get("data") or {})
        if not s:
            continue
        detail_path = s.get("detail_path", "") or key[7:]
        if not detail_path or detail_path in movies:
            continue
        title = s.get("title", "")
        safe  = _safe_name(title)[:60]
        if safe in downloaded:
            movies[detail_path] = {
                "id":          s.get("id", ""),
                "detail_path": detail_path,
                "title":       title,
                "type":        "series" if s.get("is_series") else "movie",
                "year":        (s.get("release_date") or "")[:4],
                "imdb_rating": s.get("imdb_rating"),
                "poster_url":  s.get("cover_url"),
                "size_mb":     round(downloaded.get(safe, 0), 1),
            }

    sorted_movies = sorted(movies.values(), key=lambda x: x.get("title", "").lower())
    return {
        "status": "success",
        "count":  len(sorted_movies),
        "total":  len(downloaded),
        "data":   {"movies": sorted_movies[:limit]},
    }


@app.get("/download-info/{detail_path:path}")
async def download_info(
    detail_path: str,
    ep: int = Query(1),
    season: int = Query(0),
    resolution: int = Query(1080),
):
    """Fast check whether a download is available. Returns JSON, never streams."""
    cache_key = f"stream:{detail_path}"
    stream = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not stream:
        raise HTTPException(status_code=404, detail="Title not found")

    safe_title = _safe_name(stream.get("title", detail_path))[:60]
    subject_id = stream.get("id", "")

    # 0. VPS local library — instant hit if already downloaded
    local_path = _find_local_file(safe_title, ep, season)
    if local_path:
        fname = os.path.basename(local_path)
        size_mb = os.path.getsize(local_path) // 1_048_576
        return {
            "available": True,
            "source": "local",
            "filename": fname,
            "size_mb": size_mb,
            "type": "mp4",
            "needs_conversion": False,
            "local": True,
        }

    # Not on disk yet — queue as PRIORITY so it jumps ahead of bulk background downloads
    _trigger_auto_download(stream, ep=ep, season=season, skip_playwright=True, priority=True)

    # 1. BWM / GiftedTech — direct MP4 URLs, multiple qualities, no conversion needed
    if subject_id:
        bwm_sources = scraper.get_video_sources_bwm(
            subject_id, ep=ep, season=season, title=stream.get("title", safe_title)
        )
        if bwm_sources:
            # Pick best quality matching the requested resolution for the legacy single-url fields
            res_map = {"1080p": 1080, "720p": 720, "480p": 480, "360p": 360}
            best = min(bwm_sources, key=lambda s: abs(res_map.get(s["quality"], 0) - resolution))
            return {
                "available": True,
                "sources": bwm_sources,          # full multi-quality list for the UI
                "url": best["url"],              # best-match for legacy /download flow
                "type": "mp4",
                "filename": best["filename"],
                "needs_conversion": False,
                "source": "bwm",
            }

    def _resp(url: str, src_type: str, fname: str, **extra):
        """Build a download-info response. HLS sources include stream_url for direct IDM use."""
        is_hls = src_type == "m3u8" or ".m3u8" in url
        r = {
            "available": True,
            "url": url,
            "type": src_type,
            "filename": fname,
            "needs_conversion": is_hls,
            **extra,
        }
        if is_hls:
            r["stream_url"] = url   # raw m3u8 — IDM/VLC can grab this directly at full speed
        return r

    # 2. Passive-capture cache (populated when anyone watches via the proxy player)
    if subject_id:
        vkey = f"{subject_id}:{ep}:{season}:{resolution}"
        cached = _video_url_cache.get(vkey)
        if cached and time.time() - cached.get("ts", 0) < VIDEO_URL_TTL:
            return _resp(cached["url"], cached["type"],
                         f"{safe_title}_{resolution}p.mp4", source="cache")

    # 3. yt-dlp → vidsrc.to (works for any title that streams on Server 1)
    imdb_id  = stream.get("imdb_id", "")
    is_series = stream.get("is_series", False)
    if imdb_id:
        ytdlp_info = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: scraper.get_video_url_from_ytdlp(
                imdb_id, is_series, ep=ep, season=max(season, 1) if is_series else season
            )
        )
        if ytdlp_info:
            return _resp(ytdlp_info["url"], ytdlp_info["type"],
                         f"{safe_title}_{resolution}p.mp4", source="ytdlp")

    # 4. Video URLs already extracted from the resource data (zero extra requests)
    for u in stream.get("_found_video_urls") or []:
        if u.get("ep") == ep and (not season or u.get("season") == season):
            if abs(u.get("resolution", 0) - resolution) <= 360:
                src_type = "m3u8" if ".m3u8" in u["url"] else "mp4"
                return _resp(u["url"], src_type,
                             f"{safe_title}_{u.get('resolution', resolution)}p.mp4",
                             source="resource")

    # 5. Probe netfilm.world (player API + HTML parse)
    if subject_id:
        nf_info = scraper.get_video_url_from_netfilm(
            subject_id, detail_path=detail_path, ep=ep, season=season, resolution=resolution
        )
        if nf_info:
            return _resp(nf_info["url"], nf_info["type"],
                         f"{safe_title}_{resolution}p.mp4", source="netfilm")

    # 5. Probe aoneroom video API endpoints directly
    if subject_id:
        ao_info = scraper.get_video_url(subject_id, ep=ep, season=season, resolution=resolution)
        if ao_info:
            return _resp(ao_info["url"], ao_info["type"],
                         f"{safe_title}_{resolution}p.mp4", source="aoneroom")

    # 6. Trailer fallback
    trailer = stream.get("trailer") or {}
    turl = trailer.get("url")
    is_series = stream.get("is_series", False)
    if turl:
        if is_series:
            # For series, don't silently serve a trailer — tell user to watch first
            return {
                "available": True,
                "url": turl,
                "type": "mp4",
                "filename": f"{safe_title}_trailer.mp4",
                "needs_conversion": False,
                "is_trailer": True,
                "watch_first": True,
                "source": "trailer",
            }
        return {
            "available": True,
            "url": turl,
            "type": "mp4",
            "filename": f"{safe_title}_trailer.mp4",
            "needs_conversion": False,
            "is_trailer": True,
            "source": "trailer",
        }

    return {
        "available": False,
        "detail": "No downloadable file found for this title.",
    }


@app.get("/debug/paths")
async def debug_paths():
    """List every stream path currently in the cache — use one of these with /debug/resource."""
    paths = [
        key.removeprefix("stream:")
        for key in _cache
        if key.startswith("stream:")
    ]
    return {"cached_stream_paths": sorted(paths), "count": len(paths)}


@app.get("/debug/resource/{detail_path:path}")
async def debug_resource(detail_path: str):
    """Return raw resource + subject data for this title (helps diagnose video URL fields)."""
    data = scraper._get(
        "/wefeed-h5api-bff/detail",
        params={"detailPath": detail_path},
    )
    if not data:
        raise HTTPException(status_code=404, detail="Not found")
    inner = data.get("data", {})
    resource = inner.get("resource", {})
    subject = inner.get("subject", {})
    seasons_raw = resource.get("seasons") or []
    # Return first resolution object of each season so we can see all fields
    sample_resolutions = []
    for s in seasons_raw[:2]:
        for r in (s.get("resolutions") or [])[:3]:
            sample_resolutions.append({"season": s.get("se"), **r})
    return {
        "resource_keys": list(resource.keys()),
        "subject_keys": list(subject.keys()),
        "sample_resolutions": sample_resolutions,
        "trailer": subject.get("trailer"),
    }


@app.get("/download/{detail_path:path}")
async def download_movie(
    detail_path: str,
    ep: int = Query(1),
    season: int = Query(0),
    resolution: int = Query(1080),
):
    cache_key = f"stream:{detail_path}"
    stream = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not stream:
        raise HTTPException(status_code=404, detail="Title not found")

    safe_title = _safe_name(stream.get("title", detail_path))[:60]
    subject_id  = stream.get("id", "")

    import sys
    from chege_scraper import _is_video_url

    video_info = None
    dl_source  = "none"

    # 0a. VPS local library — serve from disk via X-Accel-Redirect (nginx sendfile, fastest)
    local_path = _find_local_file(safe_title, ep, season)
    if local_path:
        local_name = os.path.basename(local_path)
        print(f"[download] Serving from VPS disk via X-Accel-Redirect: {local_name}", file=sys.stderr)
        return Response(
            status_code=200,
            headers={
                "X-Accel-Redirect":    f"/_video_files/{local_name}",
                "X-Accel-Buffering":   "yes",
                "Content-Type":        "application/octet-stream",
                "Content-Disposition": f'attachment; filename="{local_name}"',
                "Accept-Ranges":       "bytes",
            },
        )

    # 0. BWM / GiftedTech — direct MP4 URLs; proxy through our server so mobile gets a real download
    if subject_id:
        bwm_sources = scraper.get_video_sources_bwm(
            subject_id, ep=ep, season=season, title=stream.get("title", safe_title)
        )
        if bwm_sources:
            res_map = {"1080p": 1080, "720p": 720, "480p": 480, "360p": 360}
            best = min(bwm_sources, key=lambda s: abs(res_map.get(s["quality"], 0) - resolution))
            print(f"[download] source=bwm quality={best['quality']} url={best['url'][:100]}", file=sys.stderr)
            bwm_url  = best["url"]
            bwm_name = best["filename"]
            async def stream_bwm():
                dl_headers = {
                    "Referer":    "https://moviebox.ac",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                }
                async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                    async with client.stream("GET", bwm_url, headers=dl_headers) as r:
                        async for chunk in r.aiter_bytes(65536):
                            yield chunk
            return StreamingResponse(
                stream_bwm(),
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f'attachment; filename="{bwm_name}"',
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "no-store",
                },
            )

    # 1. Check the passive-capture cache (populated when anyone watches via the proxy player)
    if subject_id:
        vkey = f"{subject_id}:{ep}:{season}:{resolution}"
        cached = _video_url_cache.get(vkey)
        if cached and time.time() - cached.get("ts", 0) < VIDEO_URL_TTL:
            curl = cached["url"]
            if _is_video_url(curl):
                video_info = {"url": curl, "type": cached["type"]}
                dl_source  = "cache"
            else:
                print(f"[download] REJECTED cached URL (not video): {curl[:100]}", file=sys.stderr)
                del _video_url_cache[vkey]   # evict the bad entry

    # 2. yt-dlp → vidsrc.to — works for any title that streams on Server 1
    imdb_id   = stream.get("imdb_id", "")
    is_series = stream.get("is_series", False)
    if not video_info and imdb_id:
        ytdlp_info = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: scraper.get_video_url_from_ytdlp(
                imdb_id, is_series, ep=ep, season=max(season, 1) if is_series else season
            )
        )
        if ytdlp_info and _is_video_url(ytdlp_info.get("url", "")):
            video_info = ytdlp_info
            dl_source  = "ytdlp"
        elif ytdlp_info:
            print(f"[download] REJECTED ytdlp URL: {ytdlp_info.get('url','')[:100]}", file=sys.stderr)

    # 3. Probe netfilm.world directly (player API + HTML parsing)
    if not video_info and subject_id:
        nf = scraper.get_video_url_from_netfilm(
            subject_id, detail_path=detail_path, ep=ep, season=season, resolution=resolution
        )
        if nf and _is_video_url(nf.get("url", "")):
            video_info = nf
            dl_source  = "netfilm"
        elif nf:
            print(f"[download] REJECTED netfilm URL: {nf.get('url','')[:100]}", file=sys.stderr)

    # 4. Try aoneroom API endpoints directly
    if not video_info and subject_id:
        ao = scraper.get_video_url(subject_id, ep=ep, season=season, resolution=resolution)
        if ao and _is_video_url(ao.get("url", "")):
            video_info = ao
            dl_source  = "aoneroom"
        elif ao:
            print(f"[download] REJECTED aoneroom URL: {ao.get('url','')[:100]}", file=sys.stderr)

    # ── Size-gate: reject anything under 50 MB — it's a trailer/promo ────────
    _MIN_DL_BYTES = 50 * 1024 * 1024  # 50 MB
    if video_info:
        check_url = video_info["url"]
        is_check_hls = ".m3u8" in check_url
        if not is_check_hls:  # can't Content-Length an m3u8 playlist
            try:
                import urllib.request as _ur
                head_req = _ur.Request(check_url, method="HEAD")
                head_req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                head_req.add_header("Referer", "https://moviebox.ac")
                with _ur.urlopen(head_req, timeout=8) as hr:
                    cl = hr.headers.get("Content-Length") or "0"
                    size = int(cl)
                if size and size < _MIN_DL_BYTES:
                    print(f"[download] SIZE-GATE: {size//1024//1024} MB < 50 MB — "
                          f"source={dl_source} — rejecting trailer", file=sys.stderr)
                    video_info = None
                    dl_source  = "none"
            except Exception as e:
                print(f"[download] size-check failed ({e}) — accepting URL anyway", file=sys.stderr)

    if not video_info:
        raise HTTPException(
            status_code=404,
            detail="No full-length download source found for this title. "
                   "Watch the episode on Server 2 first, then try downloading.",
        )

    video_url = video_info["url"]
    is_hls    = video_info.get("type") == "m3u8" or ".m3u8" in video_url
    filename  = f"{safe_title}_{resolution}p.mp4"
    print(f"[download] source={dl_source} hls={is_hls} url={video_url[:100]}", file=sys.stderr)

    if is_hls:
        # Convert HLS → MP4 on-the-fly via ffmpeg (stream remux, no re-encoding).
        # -movflags frag_keyframe+empty_moov+faststart makes it a fragmented MP4
        # so the browser starts receiving/showing progress immediately.
        async def stream_hls_as_mp4():
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-headers",
                "Referer: https://moviebox.ac\r\n"
                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n",
                "-i", video_url,
                "-c", "copy",
                "-f", "mp4",
                "-movflags", "frag_keyframe+empty_moov+faststart",
                "pipe:1",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                while True:
                    chunk = await proc.stdout.read(65536)
                    if not chunk:
                        break
                    yield chunk
            finally:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass

        return StreamingResponse(
            stream_hls_as_mp4(),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-store",
            },
        )

    # Direct MP4 — proxy through our server
    async def stream_mp4():
        dl_headers = {
            "Referer":    "https://moviebox.ac",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
            async with client.stream("GET", video_url, headers=dl_headers) as r:
                async for chunk in r.aiter_bytes(65536):
                    yield chunk

    return StreamingResponse(
        stream_mp4(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-store",
        },
    )


_PLAYER_UPSTREAM   = "https://netfilm.world"
_PLAYER_PROXY_PATH = f"{_PREFIX}/proxy/player"

# Ad/tracker domains to block at the proxy level (return empty 204)
_AD_DOMAINS = {
    "voluum.com", "adcash.com", "adcashdsp.com", "chatmate.tv",
    "popads.net", "popcash.net", "adsterra.com", "propellerads.com",
    "hilltopads.net", "exoclick.com", "trafficjunky.com", "juicyads.com",
    "plugrush.com", "ero-advertising.com", "adspyglass.com",
    "rotator.adcash.com", "track.voluum.com",
    "googletagmanager.com", "googlesyndication.com", "doubleclick.net",
    "adservice.google.com", "amazon-adsystem.com",
    "v2006.com", "adex.com", "afu.php",
    "mgid.com", "revcontent.com", "outbrain.com", "taboola.com",
    "bidvertiser.com", "zedo.com", "undertone.com", "smartadserver.com",
    "adnxs.com", "adsrvr.org", "rubiconproject.com", "openx.net",
    "pubmatic.com", "criteo.com", "media.net",
    "phiglerdail.net", "phigler", "clickadu.com", "trafficshop.com",
    "adpushup.com", "monetag.com", "richpush.co", "pushground.com",
    "evadav.com", "adoperator.com", "trafficker.com",
}

# Keyword patterns to match ad domains/paths in proxied requests
_AD_URL_KEYWORDS = [
    "v2006.com", "adex.com", "afu.php", "voluum", "adcash", "chatmate",
    "popads", "popcash", "adsterra", "propellerads", "hilltopads",
    "exoclick", "trafficjunky", "juicyads", "plugrush", "mgid",
    "googlesyndication", "doubleclick", "adservice.google",
    "phiglerdail", "phigler", "clickadu", "monetag", "richpush",
]

# Script tag src patterns to strip from proxied HTML
_AD_SCRIPT_PATTERNS = [
    "voluum", "adcash", "v2006", "adex\\.com", "afu\\.php",
    "popads", "popcash", "adsterra", "propellerads",
    "hilltopads", "exoclick", "trafficjunky", "juicyads", "plugrush",
    "googlesyndication", "doubleclick", "adservice\\.google",
    "mgid", "revcontent", "outbrain", "taboola",
]

# JS code patterns to scrub from proxied JavaScript/HTML (redirect patterns)
_AD_JS_PATTERNS = [
    r'window\.open\s*\([^)]*(?:v2006|adcash|voluum|chatmate|popads|adsterra|propellerads|hilltopads|exoclick|afu\.php)[^)]*\)',
    r'(?:location|window\.location)\s*(?:\.href|\.assign|\.replace)?\s*=\s*["\'][^"\']*(?:v2006|adcash|voluum|chatmate|popads|adsterra)[^"\']*["\']',
    r'document\.createElement\s*\(\s*["\']script["\']\s*\)[^;]*(?:v2006|adcash|voluum|afu\.php)',
]

_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    "content-encoding", "content-length",
    "x-frame-options", "content-security-policy", "content-security-policy-report-only",
}


@app.api_route("/proxy/player/{path:path}", methods=["GET", "HEAD", "OPTIONS"])
async def proxy_player(request: Request, path: str = ""):
    qs = str(request.url.query)

    # Block requests to known ad/tracker domains routed through our proxy
    combined = path + "?" + qs
    for kw in _AD_URL_KEYWORDS:
        if kw in combined:
            return Response(status_code=204, headers={"Access-Control-Allow-Origin": "*"})

    upstream_url = f"{_PLAYER_UPSTREAM}/{path}" + (f"?{qs}" if qs else "")
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host   = request.headers.get("x-forwarded-host",
             request.headers.get("host", "localhost"))
    proxy_base = f"{scheme}://{host}{_PLAYER_PROXY_PATH}"

    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP and k.lower() != "host"
    }
    fwd_headers.update({
        "Host": "netfilm.world",
        "Referer": _PLAYER_UPSTREAM + "/",
        "Origin": _PLAYER_UPSTREAM,
    })

    try:
        async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
            r = await client.get(upstream_url, headers=fwd_headers)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Player proxy error: {e}")

    ct   = r.headers.get("content-type", "")
    body = r.content

    # Passively capture video URLs from JSON API responses that pass through the proxy
    if "application/json" in ct:
        try:
            import json as _json
            from urllib.parse import parse_qs
            jdata = _json.loads(body)
            result = scraper._extract_video_url_from_json(jdata)
            if result:
                qs_params = parse_qs(qs)
                subject_id = (qs_params.get("id") or qs_params.get("subjectId") or [""])[0]
                ep_val     = (qs_params.get("ep") or ["1"])[0]
                se_val     = (qs_params.get("se") or ["0"])[0]
                res_val    = (qs_params.get("resolution") or ["1080"])[0]
                if subject_id:
                    vkey = f"{subject_id}:{ep_val}:{se_val}:{res_val}"
                    _video_url_cache[vkey] = {**result, "ts": time.time()}
                    import sys
                    print(f"[proxy-capture] Cached video URL for {vkey}: {result['url'][:80]}", file=sys.stderr)
        except Exception:
            pass

    if "text/html" in ct or "javascript" in ct or "text/css" in ct:
        import re as _re
        text = r.text
        text = text.replace("https://netfilm.world", proxy_base)
        text = text.replace("http://netfilm.world",  proxy_base)

        # Scrub known ad redirect code from ALL text content (HTML + JS)
        for js_pat in _AD_JS_PATTERNS:
            text = _re.sub(js_pat, "void 0", text, flags=_re.IGNORECASE | _re.DOTALL)

        if "text/html" in ct:
            # Strip entire <script> tags whose src points to ad networks
            for pat in _AD_SCRIPT_PATTERNS:
                text = _re.sub(
                    r'<script[^>]+src=["\'][^"\']*' + pat + r'[^"\']*["\'][^>]*>.*?</script>',
                    '', text, flags=_re.IGNORECASE | _re.DOTALL
                )
                text = _re.sub(
                    r'<script[^>]+src=["\'][^"\']*' + pat + r'[^"\']*["\'][^>]*/?>',
                    '', text, flags=_re.IGNORECASE
                )

            # Full ad-host list used by injected JS
            _AD_HOSTS_JS = (
                "'v2006','afu.php','adex','voluum','adcash','chatmate',"
                "'popads','popcash','adsterra','propellerads','hilltopads',"
                "'exoclick','trafficjunky','juicyads','plugrush','mgid',"
                "'revcontent','outbrain','taboola','phiglerdail','phigler',"
                "'clickadu','monetag','richpush','pushground','evadav',"
                "'bidvertiser','zedo','adspyglass','ero-advertising',"
                "'smartadserver','adnxs','adsrvr','rubiconproject'"
            )

            # Injected FIRST — before any page script runs
            ad_block_script = (
                "<script>"
                # Kill ALL window.open — player has zero reason to open new tabs
                "window.open=function(){return null;};"
                # Also try to kill it on parent/top frames (cross-origin will throw, caught below)
                "try{window.top.open=function(){return null;};}catch(e){}"
                "try{window.parent.open=function(){return null;};}catch(e){}"
                "(function(){"
                "var AD=[" + _AD_HOSTS_JS + "];"
                "function isAd(u){return AD.some(function(d){return String(u).indexOf(d)>=0;});}"
                # Walk up the DOM tree and check if any ancestor is an ad anchor
                "function adAncestorHref(el){"
                "for(var i=0;i<8&&el;i++,el=el.parentElement){"
                "var h=(el.href)||(el.getAttribute&&el.getAttribute('href'))||'';"
                "if(h&&isAd(h))return h;"
                "if(el.tagName==='A'&&el.target&&el.target!=='_self'){"
                "var hh=el.href||el.getAttribute('href')||'';"
                "if(hh)return hh;"  # block any _blank anchor, even non-ad
                "}"
                "}"
                "return null;"
                "}"
                # --- location.assign / replace / href ---
                "var _loc=window.location;"
                "try{"
                "var _assign=_loc.assign.bind(_loc);"
                "var _replace=_loc.replace.bind(_loc);"
                "Object.defineProperty(window.location,'assign',{value:function(u){if(!isAd(u))_assign(u);}});"
                "Object.defineProperty(window.location,'replace',{value:function(u){if(!isAd(u))_replace(u);}});"
                "}catch(e){}"
                "try{"
                "var desc=Object.getOwnPropertyDescriptor(window.location,'href');"
                "if(desc&&desc.set){var origSet=desc.set;"
                "Object.defineProperty(window.location,'href',{get:desc.get,set:function(v){if(!isAd(v))origSet.call(window.location,v);}});}"
                "}catch(e){}"
                # --- HTMLAnchorElement.prototype.click override ---
                "try{"
                "var _origAClick=HTMLAnchorElement.prototype.click;"
                "HTMLAnchorElement.prototype.click=function(){"
                "var h=this.href||this.getAttribute('href')||'';"
                "if(isAd(h))return;"
                "if(this.target&&this.target!=='_self'){this.removeAttribute('target');}"
                "_origAClick.call(this);"
                "};"
                "}catch(e){}"
                # --- document.createElement wrapper: patch <a> elements at creation time ---
                "try{"
                "var _origCE=document.createElement.bind(document);"
                "document.createElement=function(tag){"
                "var el=_origCE(tag);"
                "if(typeof tag==='string'&&tag.toLowerCase()==='a'){"
                "var _elClick=el.click.bind(el);"
                "el.click=function(){"
                "var h=this.href||this.getAttribute('href')||'';"
                "if(isAd(h))return;"
                "if(this.target&&this.target!=='_self')this.removeAttribute('target');"
                "_elClick();"
                "};"
                "}"
                "return el;"
                "};"
                "}catch(e){}"
                # --- Capture-phase event blocker: click, mousedown, pointerdown, auxclick ---
                # mousedown/pointerdown fires BEFORE click — catches "on-first-click popunder"
                # auxclick = middle-click → new tab (bypasses window.open!)
                "function _blkEvt(e){"
                "if(adAncestorHref(e.target)){e.stopImmediatePropagation();e.preventDefault();}"
                "}"
                "['click','mousedown','pointerdown','auxclick'].forEach(function(ev){"
                "document.addEventListener(ev,_blkEvt,true);"
                "});"
                "document.addEventListener('touchstart',function(e){"
                "if(adAncestorHref(e.target)){e.stopImmediatePropagation();e.preventDefault();}"
                "},{capture:true,passive:false});"
                # --- Intercept dispatchEvent to catch synthetic events on ad anchors ---
                "try{"
                "var _origDE=EventTarget.prototype.dispatchEvent;"
                "EventTarget.prototype.dispatchEvent=function(ev){"
                "if(ev&&(ev.type==='click'||ev.type==='mousedown')&&this&&this.tagName==='A'){"
                "var h=this.href||this.getAttribute('href')||'';"
                "if(isAd(h))return false;"
                "if(this.target&&this.target!=='_self')return false;"
                "}"
                "return _origDE.call(this,ev);"
                "};"
                "}catch(e){}"
                # --- Strip target=_blank from all anchors, existing + future ---
                "function _stripTargets(root){"
                "try{"
                "(root.querySelectorAll?root.querySelectorAll('a[target]'):[]).forEach(function(a){"
                "a.removeAttribute('target');"
                "});"
                "}catch(e){}}"
                "_stripTargets(document);"
                "try{"
                "var _mo=new MutationObserver(function(muts){"
                "muts.forEach(function(m){"
                "m.addedNodes.forEach(function(n){"
                "if(!n||n.nodeType!==1)return;"
                "if(n.tagName==='A')n.removeAttribute('target');"
                "else _stripTargets(n);"
                "});"
                "});"
                "});"
                "_mo.observe(document.documentElement,{childList:true,subtree:true});"
                "}catch(e){}"
                # --- Kill form.submit() to ad URLs ---
                "try{"
                "var _origSubmit=HTMLFormElement.prototype.submit;"
                "HTMLFormElement.prototype.submit=function(){"
                "var a=this.action||'';"
                "if(isAd(a))return;"
                "_origSubmit.call(this);"
                "};"
                "}catch(e){}"
                # --- setTimeout/setInterval wrapper (prevents delayed popup tricks) ---
                "var _sT=window.setTimeout,_sI=window.setInterval;"
                "function wrapFn(fn){"
                "if(typeof fn!=='function')return fn;"
                "return function(){try{fn.apply(this,arguments);}catch(e){}}"
                "}"
                "window.setTimeout=function(fn,d){return _sT(wrapFn(fn),d);};"
                "window.setInterval=function(fn,d){return _sI(wrapFn(fn),d);};"
                "})();"
                "</script>"
            )

            fix_script = (
                "<script>"
                "(function(){"
                "var p=window.location.pathname;"
                "var px='/movies-api/proxy/player';"
                "if(p.startsWith(px)){"
                "var rest=p.slice(px.length);"
                "history.replaceState(null,'',rest+window.location.search+window.location.hash);"
                "}"
                "})();"
                "(function(){"
                "function fixPlayer(){"
                "var v=document.querySelector('video');"
                "if(!v)return false;"
                "var el=v;"
                "while(el.parentElement&&el.parentElement.id!=='app'&&el.parentElement!==document.body){el=el.parentElement;}"
                "el.style.cssText='position:fixed!important;top:0!important;left:0!important;width:100%!important;height:100%!important;z-index:50!important;overflow:hidden!important;background:#000!important';"
                "if(el.parentElement){Array.from(el.parentElement.children).forEach(function(c){if(c!==el){c.style.setProperty('display','none','important');}});}"
                "document.querySelectorAll('iframe').forEach(function(f){f.style.setProperty('display','none','important');});"
                "return true;"
                "}"
                "var t=0;"
                "var iv=setInterval(function(){if(fixPlayer()||++t>40)clearInterval(iv);},250);"
                "var ob=new MutationObserver(fixPlayer);"
                "ob.observe(document.documentElement,{childList:true,subtree:true});"
                "})();"
                "</script>"
            )
            # Video URL capture script — intercepts fetch/XHR in the player page
            # and reports video URLs back to /report-video so /download can use them.
            video_capture_script = (
                "<script>"
                "(function(){"
                "var sp=new URLSearchParams(window.location.search);"
                "var sid=sp.get('id')||sp.get('subjectId')||'';"
                "var ep=sp.get('ep')||'1';"
                "var se=sp.get('se')||'0';"
                "var res=sp.get('resolution')||'1080';"
                "if(!sid)return;"
                "var _done=false;"
                "var _NON=['.js','.css','.html','.json','.png','.jpg','.ico','.woff','.woff2','.ttf','.map','.svg','.gif','.xml'];"
                "function isVid(u){"
                "if(!u||typeof u!=='string'||u.indexOf('http')!==0)return false;"
                "var low=u.toLowerCase().split('?')[0];"
                "if(_NON.some(function(e){return low.slice(-e.length)===e;}))return false;"
                "if(low.indexOf('.m3u8')>=0||low.indexOf('.mp4')>=0||low.indexOf('.webm')>=0)return true;"
                "if(['pbcdnw.aoneroom.com','pbcdn.aoneroom.com','macdn.aoneroom.com'].some(function(d){return u.indexOf(d)>=0;}))return true;"
                "return false;"
                "}"
                "function findVid(obj,depth){"
                "if(!obj||depth>6)return null;"
                "if(typeof obj==='string')return isVid(obj)?obj:null;"
                "if(Array.isArray(obj)){for(var i=0;i<Math.min(obj.length,8);i++){var r=findVid(obj[i],depth+1);if(r)return r;}return null;}"
                "if(typeof obj==='object'){"
                "var keys=['url','videoUrl','playUrl','hlsUrl','m3u8Url','source','address','streamUrl','mediaUrl','fileUrl','videoAddress'];"
                "for(var i=0;i<keys.length;i++){var v=obj[keys[i]];if(isVid(v))return v;}"
                "var vals=Object.values(obj);"
                "for(var i=0;i<vals.length;i++){if(vals[i]&&typeof vals[i]==='object'){var r=findVid(vals[i],depth+1);if(r)return r;}}"
                "}"
                "return null;"
                "}"
                "function report(url){"
                "if(_done)return;_done=true;"
                "var t=url.indexOf('.m3u8')>=0?'m3u8':'mp4';"
                "try{"
                "fetch('/movies-api/report-video',{"
                "method:'POST',"
                "headers:{'Content-Type':'application/json'},"
                "body:JSON.stringify({subject_id:sid,ep:ep,season:se,resolution:res,url:url,type:t})"
                "});"
                "}catch(e){}"
                "}"
                # Intercept fetch — catch both JSON bodies AND direct .m3u8/.mp4 request URLs
                "var _oF=window.fetch;"
                "window.fetch=function(url,opts){"
                "var us=typeof url==='string'?url:(url&&url.url)||'';"
                "if(!_done&&isVid(us))report(us);"
                "return _oF.call(this,url,opts).then(function(resp){"
                "if(_done)return resp;"
                "var ct=resp.headers.get('content-type')||'';"
                "if(ct.indexOf('mpegurl')>=0||ct.indexOf('m3u8')>=0){report(us);}"
                "else if(ct.indexOf('json')>=0){resp.clone().json().then(function(d){var v=findVid(d,0);if(v)report(v);}).catch(function(){});}"
                "return resp;"
                "});"
                "};"
                # Intercept XHR — catch both JSON bodies AND direct .m3u8/.mp4 request URLs
                "var _oO=XMLHttpRequest.prototype.open,_oS=XMLHttpRequest.prototype.send;"
                "XMLHttpRequest.prototype.open=function(m,u){this._vu=u;if(!_done&&isVid(u))report(u);return _oO.apply(this,arguments);};"
                "XMLHttpRequest.prototype.send=function(){"
                "var x=this;"
                "x.addEventListener('load',function(){"
                "if(_done)return;"
                "var ct=x.getResponseHeader('content-type')||'';"
                "if(ct.indexOf('mpegurl')>=0||ct.indexOf('m3u8')>=0){report(x._vu||'');}"
                "else if(ct.indexOf('json')>=0&&x.responseText){"
                "try{var d=JSON.parse(x.responseText);var v=findVid(d,0);if(v)report(v);}catch(e){}"
                "}"
                "});"
                "return _oS.apply(this,arguments);"
                "};"
                # Also watch for video/source elements appearing in DOM
                "var _vob=new MutationObserver(function(muts){"
                "muts.forEach(function(m){"
                "m.addedNodes.forEach(function(n){"
                "if(!n||n.nodeType!==1)return;"
                "var srcs=[];"
                "if(n.tagName==='VIDEO'||n.tagName==='SOURCE')srcs.push(n.src||n.getAttribute('src'));"
                "if(n.querySelectorAll)n.querySelectorAll('video,source').forEach(function(el){srcs.push(el.src||el.getAttribute('src'));});"
                "srcs.forEach(function(s){if(isVid(s))report(s);});"
                "});"
                "});"
                "});"
                "_vob.observe(document.documentElement,{childList:true,subtree:true});"
                "})();"
                "</script>"
            )

            # Inject ad-block script at the VERY TOP of <head> so it runs before any page script
            text = text.replace("<head>", "<head>" + ad_block_script + video_capture_script, 1)
            if "<head>" not in text:
                text = text.replace("<html", "<head>" + ad_block_script + video_capture_script + "</head><html", 1)
            text = text.replace("</body>", fix_script + "</body>", 1)

        body = text.encode("utf-8")

    resp_headers = {k: v for k, v in r.headers.items() if k.lower() not in _HOP_BY_HOP}
    resp_headers["content-length"]          = str(len(body))
    resp_headers["access-control-allow-origin"] = "*"

    # Browser-enforced: block all navigation and popups from the player frame
    if "text/html" in ct:
        resp_headers["content-security-policy"] = (
            "navigate-to 'self' https://movieapi.jchege.tech https://movies.jchege.tech "
            "https://netfilm.world https://h5-api.aoneroom.com https://pbcdnw.aoneroom.com "
            "https://pbcdn.aoneroom.com; "
            "form-action 'self';"
        )
        # Deny popup permission for this frame AND all nested sub-frames
        resp_headers["permissions-policy"] = "popups=()"

    return Response(content=body, status_code=r.status_code,
                    headers=resp_headers, media_type=ct)


@app.post("/report-video")
async def report_video(request: Request):
    """Browser player reports a discovered video URL so /download can use it."""
    import sys
    from chege_scraper import _is_video_url
    try:
        payload    = await request.json()
        url        = payload.get("url", "")
        subject_id = str(payload.get("subject_id", ""))
        ep         = str(payload.get("ep", "1"))
        season     = str(payload.get("season", "0"))
        resolution = str(payload.get("resolution", "1080"))
        vtype      = payload.get("type", "mp4")
        if not subject_id or not _is_video_url(url):
            return {"ok": False, "reason": "invalid url or missing subject_id"}
        vkey = f"{subject_id}:{ep}:{season}:{resolution}"
        _video_url_cache[vkey] = {"url": url, "type": vtype, "ts": time.time()}
        print(f"[report-video] Cached {vkey}: {url[:100]}", file=sys.stderr)
        return {"ok": True, "cached": vkey}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


MOOD_MAP: dict = {
    "feel-good":  ["Comedy", "Animation", "Family"],
    "action":     ["Action", "Adventure"],
    "dark":       ["Crime", "Thriller", "Mystery"],
    "romance":    ["Romance", "Drama"],
    "thrilling":  ["Thriller", "Mystery", "Crime"],
    "funny":      ["Comedy"],
    "fantasy":    ["Fantasy", "Sci-Fi", "Animation"],
    "family":     ["Family", "Animation"],
    "scary":      ["Horror", "Thriller"],
    "inspiring":  ["Biography", "Drama", "Documentary"],
    "anime":      ["Anime"],
    "k-drama":    ["K-Drama"],
}


@app.get("/movies/by-genre/{genre}")
async def movies_by_genre(
    genre: str,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    type: Optional[str] = Query(None),
):
    """Return paginated movies for a specific genre."""
    subject_type: Optional[int] = None
    if type == "movie":
        subject_type = 1
    elif type in ("series", "tv"):
        subject_type = 2
    cache_key = f"movies:{page}:{limit}:{subject_type}:{genre}:None:None"
    result = _cached(cache_key, scraper.get_movies, page=page, per_page=limit,
                     subject_type=subject_type, genre=genre)
    pager = result.get("pager", {})
    return {
        "status": "success", "timestamp": now(),
        "data": {
            "movies": result["items"],
            "genre": genre,
            "page": page,
            "limit": limit,
            "has_more": pager.get("hasMore", False),
            "total": pager.get("totalCount", len(result["items"])),
        },
    }


@app.get("/movies/mood/{mood}")
async def movies_by_mood(
    mood: str,
    limit: int = Query(20, ge=1, le=60),
):
    """Return a shuffled mix of titles matching a mood (maps mood → genres)."""
    mood = mood.lower().strip()
    genre_list = MOOD_MAP.get(mood)
    if not genre_list:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown mood '{mood}'. Available: {sorted(MOOD_MAP.keys())}",
        )

    seen: set = set()
    pool: list = []
    for genre in genre_list:
        cache_key = f"movies:1:50:None:{genre}:None:None"
        result = _cached(cache_key, scraper.get_movies, page=1, per_page=50, genre=genre)
        for item in result.get("items", []):
            if item["detail_path"] not in seen:
                seen.add(item["detail_path"])
                pool.append({**item, "_mood_genre": genre})

    import random as _rand
    _rand.Random(int(time.time() // 3600)).shuffle(pool)   # rotates every hour
    return {
        "status": "success", "timestamp": now(),
        "data": {"movies": pool[:limit], "mood": mood, "genres": genre_list, "total": len(pool)},
    }


@app.get("/movies/random")
async def random_movie(type: Optional[str] = Query(None)):
    """Return a single randomly selected movie or series."""
    import random as _rand
    subject_type: Optional[int] = None
    if type == "movie":   subject_type = 1
    elif type in ("series", "tv"): subject_type = 2

    page = _rand.randint(1, 8)
    cache_key = f"movies:{page}:50:{subject_type}:None:None:None"
    result = _cached(cache_key, scraper.get_movies, page=page, per_page=50, subject_type=subject_type)
    items = result.get("items", [])
    if not items:
        fb = _cached("movies:1:50:None:None:None:None", scraper.get_movies, page=1, per_page=50)
        items = fb.get("items", [])
    if not items:
        raise HTTPException(status_code=404, detail="No movies found")
    return {
        "status": "success", "timestamp": now(),
        "data": {"movie": _rand.choice(items)},
    }


@app.get("/admin/stats")
async def admin_stats():
    """Admin: summary of cache + server state."""
    now_ts = time.time()
    movie_keys   = list(_cache.keys())
    server_keys  = list(_server_cache.keys())

    entries = []
    for k, v in _cache.items():
        age = int(now_ts - v["ts"])
        entries.append({"key": k, "age_secs": age, "fresh": age < CACHE_TTL, "stale": age >= CACHE_TTL})

    return {
        "status": "ok",
        "version": "3.0.0",
        "cache": {
            "movie_entries": len(movie_keys),
            "server_entries": len(server_keys),
            "ttl_secs": CACHE_TTL,
            "stale_secs": CACHE_STALE,
            "server_ttl_secs": SERVER_CACHE_TTL,
        },
        "endpoints_count": 22,
        "timestamp": now(),
    }


@app.get("/admin/cache")
async def admin_cache_list():
    """Admin: list all cache entries with metadata."""
    now_ts = time.time()
    entries = []
    for k, v in _cache.items():
        age = int(now_ts - v["ts"])
        data = v["data"]
        # Estimate item count from data shape
        item_count = None
        if isinstance(data, dict):
            item_count = (
                len(data.get("items", [])) or
                len(data.get("movies", [])) or
                len(data.get("trending", [])) or
                len(data.get("banner", {}).get("items", []) if isinstance(data.get("banner"), dict) else []) or
                None
            )
        entries.append({
            "key": k,
            "age_secs": age,
            "fresh": age < CACHE_TTL,
            "stale": age >= CACHE_TTL and age < CACHE_STALE,
            "expired": age >= CACHE_STALE,
            "item_count": item_count,
        })
    entries.sort(key=lambda x: x["age_secs"])

    server_entries = []
    for k, v in _server_cache.items():
        age = int(now_ts - v["ts"])
        d   = v["data"]
        server_entries.append({
            "key": k,
            "age_secs": age,
            "working_count": d.get("working_count", 0),
            "server_count": len(d.get("servers", [])),
            "cached": d.get("cached", False),
        })

    return {
        "status": "ok",
        "timestamp": now(),
        "movie_cache": entries,
        "server_cache": server_entries,
    }


@app.post("/admin/cache/clear")
async def admin_cache_clear(target: str = Query("all", description="all | movie | server")):
    """Admin: clear cache entries."""
    cleared = 0
    if target in ("all", "movie"):
        cleared += len(_cache)
        _cache.clear()
    if target in ("all", "server"):
        cleared += len(_server_cache)
        _server_cache.clear()
    return {"status": "ok", "cleared": cleared, "target": target, "timestamp": now()}


@app.delete("/admin/cache/{key:path}")
async def admin_cache_delete(key: str):
    """Admin: delete a specific cache entry by key."""
    removed = False
    if key in _cache:
        del _cache[key]
        removed = True
    elif key in _server_cache:
        del _server_cache[key]
        removed = True
    if not removed:
        raise HTTPException(status_code=404, detail=f"Cache key not found: {key}")
    return {"status": "ok", "deleted": key, "timestamp": now()}


@app.get("/admin/endpoints")
async def admin_endpoints():
    """Admin: list all registered routes."""
    routes = []
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            routes.append({
                "path": route.path,
                "methods": sorted(route.methods or []),
                "name": route.name,
                "summary": getattr(route, "summary", None) or (route.endpoint.__doc__ or "").strip().split("\n")[0] if hasattr(route, "endpoint") and route.endpoint.__doc__ else None,
            })
    return {"status": "ok", "count": len(routes), "routes": routes, "timestamp": now()}


@app.get("/admin/requests")
async def admin_requests(limit: int = Query(default=200, le=500)):
    """Return the most recent API requests from the in-memory ring buffer."""
    entries = list(reversed(list(_REQUEST_LOG)))[:limit]
    return {
        "status":    "ok",
        "total":     len(_REQUEST_LOG),
        "returned":  len(entries),
        "requests":  entries,
        "timestamp": now(),
    }


@app.get("/health")
async def health():
    server_cache_entries = len(_server_cache)
    movie_cache_entries  = len(_cache)
    cached_servers = {
        k.replace("srv:", ""): {
            "working": v["data"].get("working_count", 0),
            "age_secs": int(time.time() - v["ts"]),
        }
        for k, v in _server_cache.items()
    }
    return {
        "status": "healthy",
        "api": "Chege Movie API",
        "version": "3.0.0",
        "cache": {
            "movie_entries": movie_cache_entries,
            "server_entries": server_cache_entries,
            "server_cache_ttl_secs": SERVER_CACHE_TTL,
            "cached_servers": cached_servers,
        },
        "timestamp": now(),
    }


@app.get("/chege/hello")
async def chege_hello():
    return {"message": "Chege's Movie API v3 is running! Created by Chege", "timestamp": now()}
