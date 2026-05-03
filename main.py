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
            {"label": "Server 3", "url": f"https://www.2embed.cc/embedtv/{imdb_id}&s={season}&e={episode}"},
            {"label": "Server 4", "url": f"https://multiembed.mov/?video_id={imdb_id}&tmdb=0&s={season}&e={episode}"},
        ]
    return [
        {"label": "Server 1", "url": f"https://vidsrc.to/embed/movie/{imdb_id}"},
        {"label": "Server 2", "url": f"https://vidsrc.me/embed/movie?imdb={imdb_id}"},
        {"label": "Server 3", "url": f"https://www.2embed.cc/embed/{imdb_id}"},
        {"label": "Server 4", "url": f"https://multiembed.mov/?video_id={imdb_id}&tmdb=0"},
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


@app.get("/download/{detail_path:path}")
async def download_trailer(detail_path: str):
    cache_key = f"stream:{detail_path}"
    result = _cached(cache_key, scraper.get_stream_info, detail_path=detail_path)
    if not result:
        raise HTTPException(status_code=404, detail="Title not found")
    trailer = result.get("trailer")
    if not trailer or not trailer.get("url"):
        raise HTTPException(status_code=404, detail="No downloadable trailer available")
    trailer_url = trailer["url"]
    safe_title = re.sub(r"[^a-zA-Z0-9_\-]", "_", result.get("title", detail_path))
    filename = f"{safe_title}_trailer.mp4"

    async def stream_file():
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            async with client.stream("GET", trailer_url, headers={"Referer": "https://moviebox.ac"}) as r:
                async for chunk in r.aiter_bytes(chunk_size=65536):
                    yield chunk

    return StreamingResponse(
        stream_file(), media_type="video/mp4",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


_PLAYER_UPSTREAM   = "https://netfilm.world"
_PLAYER_PROXY_PATH = f"{_PREFIX}/proxy/player"

_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    "content-encoding", "content-length",
    "x-frame-options", "content-security-policy", "content-security-policy-report-only",
}


@app.api_route("/proxy/player/{path:path}", methods=["GET", "HEAD", "OPTIONS"])
async def proxy_player(request: Request, path: str = ""):
    qs = str(request.url.query)
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

    if "text/html" in ct or "javascript" in ct or "text/css" in ct:
        text = r.text
        text = text.replace("https://netfilm.world", proxy_base)
        text = text.replace("http://netfilm.world",  proxy_base)

        if "text/html" in ct:
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
            text = text.replace("</head>", fix_script + "</head>", 1)

        body = text.encode("utf-8")

    resp_headers = {k: v for k, v in r.headers.items() if k.lower() not in _HOP_BY_HOP}
    resp_headers["content-length"]          = str(len(body))
    resp_headers["access-control-allow-origin"] = "*"

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
