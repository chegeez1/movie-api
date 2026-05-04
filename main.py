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
import threading
import time
import re
import json
import urllib.parse
import httpx

from chege_scraper import ChegeScraper

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


@app.on_event("startup")
async def startup_event():
    threading.Thread(target=_warm_cache, daemon=True).start()


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


@app.get("/stream/{detail_path:path}")
async def get_stream(detail_path: str):
    cache_key = f"stream:{detail_path}"
    result = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not result:
        raise HTTPException(status_code=404, detail="Stream info not found")
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

    safe_title = re.sub(r"[^a-zA-Z0-9_\-]", "_", stream.get("title", detail_path))
    subject_id = stream.get("id", "")

    # 1. Passive-capture cache (populated when anyone watches via the proxy player)
    if subject_id:
        vkey = f"{subject_id}:{ep}:{season}:{resolution}"
        cached = _video_url_cache.get(vkey)
        if cached and time.time() - cached.get("ts", 0) < VIDEO_URL_TTL:
            src_type = cached["type"]
            return {
                "available": True,
                "url": cached["url"],
                "type": src_type,
                "filename": f"{safe_title}_{resolution}p.mp4",
                "needs_conversion": src_type == "m3u8",
                "source": "cache",
            }

    # 2. Video URLs already extracted from the resource data (zero extra requests)
    for u in stream.get("_found_video_urls") or []:
        if u.get("ep") == ep and (not season or u.get("season") == season):
            if abs(u.get("resolution", 0) - resolution) <= 360:
                src_type = "m3u8" if ".m3u8" in u["url"] else "mp4"
                return {
                    "available": True,
                    "url": u["url"],
                    "type": src_type,
                    "filename": f"{safe_title}_{u.get('resolution', resolution)}p.mp4",
                    "needs_conversion": src_type == "m3u8",
                    "source": "resource",
                }

    # 3. Probe netfilm.world (player API + HTML parse)
    if subject_id:
        nf_info = scraper.get_video_url_from_netfilm(
            subject_id, detail_path=detail_path, ep=ep, season=season, resolution=resolution
        )
        if nf_info:
            src_type = nf_info["type"]
            return {
                "available": True,
                "url": nf_info["url"],
                "type": src_type,
                "filename": f"{safe_title}_{resolution}p.mp4",
                "needs_conversion": src_type == "m3u8",
                "source": "netfilm",
            }

    # 4. Trailer fallback
    trailer = stream.get("trailer") or {}
    turl = trailer.get("url")
    if turl:
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

    safe_title = re.sub(r"[^a-zA-Z0-9_\-]", "_", stream.get("title", detail_path))
    subject_id  = stream.get("id", "")

    video_info = None

    # 1. Check the passive-capture cache (populated when anyone watches via the proxy player)
    if subject_id:
        vkey = f"{subject_id}:{ep}:{season}:{resolution}"
        cached = _video_url_cache.get(vkey)
        if cached and time.time() - cached.get("ts", 0) < VIDEO_URL_TTL:
            video_info = {"url": cached["url"], "type": cached["type"]}

    # 2. Probe netfilm.world directly (player API + HTML parsing)
    if not video_info and subject_id:
        video_info = scraper.get_video_url_from_netfilm(
            subject_id, detail_path=detail_path, ep=ep, season=season, resolution=resolution
        )

    # 3. Try aoneroom API endpoints
    if not video_info and subject_id:
        video_info = scraper.get_video_url(subject_id, ep=ep, season=season, resolution=resolution)

    # 4. Fallback: trailer
    if not video_info:
        trailer = stream.get("trailer") or {}
        turl = trailer.get("url")
        if turl:
            video_info  = {"url": turl, "type": "mp4"}
            safe_title  = f"{safe_title}_trailer"

    if not video_info:
        raise HTTPException(
            status_code=404,
            detail="No downloadable file found. Open the watch page and use the player's download option.",
        )

    video_url = video_info["url"]
    is_hls    = video_info.get("type") == "m3u8" or ".m3u8" in video_url
    filename  = f"{safe_title}_{resolution}p.mp4"

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
            media_type="video/mp4",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Access-Control-Allow-Origin": "*",
                "X-Content-Type-Options": "nosniff",
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
        media_type="video/mp4",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Access-Control-Allow-Origin": "*",
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
            # Inject ad-block script at the VERY TOP of <head> so it runs before any page script
            text = text.replace("<head>", "<head>" + ad_block_script, 1)
            if "<head>" not in text:
                text = text.replace("<html", "<head>" + ad_block_script + "</head><html", 1)
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
