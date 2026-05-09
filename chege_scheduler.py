"""
chege_scheduler.py — Auto-download latest movies + auto-cleanup old ones.

Run standalone:  python3 chege_scheduler.py
Or via PM2:      pm2 start chege_scheduler.py --name movie-scheduler --interpreter python3

What it does:
  - Every 6 hours: fetches trending + latest movies and triggers downloads for any
    not yet on disk.
  - Every hour: if free disk < DISK_LOW_GB, deletes oldest files until free again.
"""

import os
import sys
import time
import json
import shutil
import threading
import requests
import re

# ── Config ────────────────────────────────────────────────────────────────────
DOWNLOAD_DIR   = "/opt/movie-downloads"
API_BASE       = "http://127.0.0.1:8000"   # local API
FETCH_INTERVAL = 6 * 3600                   # how often to fetch new movies (seconds)
CLEAN_INTERVAL = 3600                        # how often to run cleanup (seconds)
DISK_LOW_GB    = 20.0                        # start deleting when free disk drops below this
DISK_TARGET_GB = 40.0                        # keep deleting until free disk reaches this
MAX_PAGES      = 5                           # pages of trending/latest to fetch per run
MIN_FILE_MB    = 50                          # ignore files smaller than this (trailers)


# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[scheduler {ts}] {msg}", file=sys.stderr, flush=True)


def free_disk_gb() -> float:
    try:
        s = shutil.disk_usage(DOWNLOAD_DIR)
        return s.free / 1_073_741_824
    except Exception:
        return 999.0


def safe_name(title: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", title)[:60]


def is_on_disk(title: str, ep: int = 1, season: int = 0) -> bool:
    """Quick check — mirrors _find_local_file logic from main.py."""
    if not os.path.isdir(DOWNLOAD_DIR):
        return False
    safe = safe_name(title)
    ep_tag = f"_s{season:02d}e{ep:02d}_"
    for fname in os.listdir(DOWNLOAD_DIR):
        if fname.startswith(safe) and ep_tag in fname and fname.endswith((".mp4", ".mkv")):
            fpath = os.path.join(DOWNLOAD_DIR, fname)
            try:
                if os.path.getsize(fpath) >= MIN_FILE_MB * 1_048_576:
                    return True
            except OSError:
                pass
    # Retry with season=1 if season=0 found nothing
    if season == 0:
        return is_on_disk(title, ep, season=1)
    return False


def trigger_download(detail_path: str, ep: int = 1, season: int = 0) -> bool:
    """Hit the API's /movie-status endpoint which queues a priority download."""
    try:
        params = {"ep": ep, "season": season}
        r = requests.get(
            f"{API_BASE}/movie-status/{detail_path}",
            params=params,
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        log(f"trigger_download error ({detail_path}): {e}")
        return False


# ── Fetch movie lists ─────────────────────────────────────────────────────────

def fetch_trending(pages: int = MAX_PAGES) -> list:
    movies = []
    for page in range(1, pages + 1):
        try:
            r = requests.get(
                f"{API_BASE}/trending",
                params={"page": page, "per_page": 20},
                timeout=10,
            )
            if r.status_code != 200:
                break
            data = r.json().get("data", {})
            items = data.get("trending") or data.get("items") or []
            if not items:
                break
            movies.extend(items)
        except Exception as e:
            log(f"fetch_trending page={page} error: {e}")
            break
    return movies


def fetch_latest_movies(pages: int = MAX_PAGES) -> list:
    movies = []
    for page in range(1, pages + 1):
        try:
            r = requests.get(
                f"{API_BASE}/movies",
                params={"page": page, "per_page": 20, "type": "movie"},
                timeout=10,
            )
            if r.status_code != 200:
                break
            data = r.json().get("data", {})
            items = data.get("movies") or data.get("items") or []
            if not items:
                break
            movies.extend(items)
        except Exception as e:
            log(f"fetch_latest page={page} error: {e}")
            break
    return movies


def fetch_latest_series(pages: int = MAX_PAGES) -> list:
    series = []
    for page in range(1, pages + 1):
        try:
            r = requests.get(
                f"{API_BASE}/movies",
                params={"page": page, "per_page": 20, "type": "series"},
                timeout=10,
            )
            if r.status_code != 200:
                break
            data = r.json().get("data", {})
            items = data.get("movies") or data.get("items") or []
            if not items:
                break
            series.extend(items)
        except Exception as e:
            log(f"fetch_latest_series page={page} error: {e}")
            break
    return series


# ── Download loop ─────────────────────────────────────────────────────────────

def download_new_movies() -> None:
    log("Starting new-movie fetch run...")
    free = free_disk_gb()
    log(f"Free disk: {free:.1f} GB")

    if free < DISK_LOW_GB:
        log(f"Disk too low ({free:.1f} GB) — skipping download run, running cleanup instead")
        cleanup_old_files()
        return

    all_movies = []
    all_movies.extend(fetch_trending())
    all_movies.extend(fetch_latest_movies())

    # Deduplicate by detail_path
    seen = set()
    unique = []
    for m in all_movies:
        dp = m.get("detail_path") or m.get("detail_url") or ""
        if dp and dp not in seen:
            seen.add(dp)
            unique.append(m)

    log(f"Found {len(unique)} unique titles to check")

    queued = 0
    skipped = 0
    for movie in unique:
        title = movie.get("title", "")
        detail_path = movie.get("detail_path") or movie.get("detail_url") or ""
        is_series = movie.get("type") == "series"

        if not title or not detail_path:
            continue

        ep = 1
        season = 1 if is_series else 0

        if is_on_disk(title, ep, season):
            skipped += 1
            continue

        free = free_disk_gb()
        if free < DISK_LOW_GB:
            log(f"Disk low ({free:.1f} GB) during run — stopping early")
            break

        if trigger_download(detail_path, ep=ep, season=season):
            queued += 1
            log(f"Queued: {title}")
            time.sleep(2)  # gentle throttle between triggers
        else:
            log(f"Failed to queue: {title}")

    log(f"Download run done — queued={queued} already_on_disk={skipped}")


# ── Cleanup loop ──────────────────────────────────────────────────────────────

def cleanup_old_files() -> None:
    free = free_disk_gb()
    if free >= DISK_LOW_GB:
        return  # plenty of space, nothing to do

    log(f"Cleanup triggered — free disk: {free:.1f} GB (target: {DISK_TARGET_GB} GB)")

    if not os.path.isdir(DOWNLOAD_DIR):
        return

    # Collect all video files with their last-access time and size
    files = []
    for fname in os.listdir(DOWNLOAD_DIR):
        if not fname.endswith((".mp4", ".mkv")):
            continue
        fpath = os.path.join(DOWNLOAD_DIR, fname)
        try:
            st = os.stat(fpath)
            size_mb = st.st_size / 1_048_576
            if size_mb < MIN_FILE_MB:
                continue  # skip tiny files (incomplete downloads etc)
            files.append({
                "path": fpath,
                "name": fname,
                "atime": st.st_atime,   # last access time
                "size_mb": size_mb,
            })
        except OSError:
            pass

    # Sort: least recently accessed first (oldest = delete first)
    files.sort(key=lambda f: f["atime"])

    deleted = 0
    freed_mb = 0.0
    for f in files:
        free = free_disk_gb()
        if free >= DISK_TARGET_GB:
            break
        try:
            os.remove(f["path"])
            freed_mb += f["size_mb"]
            deleted += 1
            log(f"Deleted (old): {f['name']} ({f['size_mb']:.0f} MB, "
                f"last accessed {(time.time() - f['atime']) / 86400:.1f} days ago)")
        except OSError as e:
            log(f"Could not delete {f['name']}: {e}")

    log(f"Cleanup done — deleted {deleted} files, freed {freed_mb / 1024:.2f} GB, "
        f"free disk now: {free_disk_gb():.1f} GB")


# ── Scheduler threads ─────────────────────────────────────────────────────────

def download_loop() -> None:
    # Wait 60 seconds on startup to let the API fully boot
    time.sleep(60)
    while True:
        try:
            download_new_movies()
        except Exception as e:
            log(f"download_loop error: {e}")
        log(f"Next download run in {FETCH_INTERVAL // 3600} hours")
        time.sleep(FETCH_INTERVAL)


def cleanup_loop() -> None:
    time.sleep(30)
    while True:
        try:
            cleanup_old_files()
        except Exception as e:
            log(f"cleanup_loop error: {e}")
        time.sleep(CLEAN_INTERVAL)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log(f"Chege Movie Scheduler starting — download_dir={DOWNLOAD_DIR}")
    log(f"Fetch interval: every {FETCH_INTERVAL // 3600}h | "
        f"Cleanup threshold: {DISK_LOW_GB} GB free | "
        f"Cleanup target: {DISK_TARGET_GB} GB free")

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    t1 = threading.Thread(target=download_loop, daemon=True, name="download-loop")
    t2 = threading.Thread(target=cleanup_loop, daemon=True, name="cleanup-loop")
    t1.start()
    t2.start()

    log("Scheduler running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Scheduler stopped.")
