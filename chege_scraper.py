import re
import requests
import time
import urllib.parse
from typing import List, Dict, Optional, Any

UPSTREAM_API = "https://h5-api.aoneroom.com"
SITE_URL = "https://moviebox.ac"
PLAYER_URL = "https://netfilm.world"

# Known video CDN domains — URLs from these are always valid video sources
_VIDEO_CDN_DOMAINS = (
    "pbcdnw.aoneroom.com",
    "pbcdn.aoneroom.com",
    "macdn.aoneroom.com",
    "cdn.aoneroom.com",
)

# File extensions that are definitely NOT video files
_NON_VIDEO_EXTS = (
    ".js", ".css", ".html", ".json", ".png", ".jpg", ".jpeg",
    ".gif", ".svg", ".ico", ".woff", ".woff2", ".ttf", ".map",
    ".xml", ".txt", ".pdf", ".zip",
)


def _is_video_url(url: str) -> bool:
    """Return True only if the URL looks like an actual video stream or file."""
    if not isinstance(url, str) or not url.startswith("http"):
        return False
    low = url.lower().split("?")[0]  # strip query string for ext check
    # Reject known non-video extensions
    if any(low.endswith(ext) for ext in _NON_VIDEO_EXTS):
        return False
    # Accept if it has a clear video signal
    if ".m3u8" in low or ".mp4" in low or ".ts" in low or ".webm" in low:
        return True
    # Accept if it comes from a known video CDN
    if any(cdn in url for cdn in _VIDEO_CDN_DOMAINS):
        return True
    # Accept if path contains video-related keywords (but not JS/CSS paths)
    if any(kw in low for kw in ["video", "stream", "hls", "play", "media"]):
        return True
    return False

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": SITE_URL,
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def _map_subject(item: Dict) -> Dict:
    cover = item.get("cover") or {}
    stills = item.get("stills") or {}
    return {
        "id": item.get("subjectId", ""),
        "detail_path": item.get("detailPath", ""),
        "title": item.get("title", ""),
        "type": "movie" if item.get("subjectType") == 1 else "series",
        "year": (item.get("releaseDate") or "")[:4] or None,
        "release_date": item.get("releaseDate"),
        "duration": item.get("duration"),
        "genres": [g.strip() for g in item.get("genre", "").split(",") if g.strip()],
        "country": item.get("countryName"),
        "imdb_rating": item.get("imdbRatingValue"),
        "imdb_rating_count": item.get("imdbRatingCount"),
        "poster_url": cover.get("url"),
        "stills_url": stills.get("url"),
        "description": item.get("description") or None,
        "season": item.get("season"),
    }


class ChegeScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, path: str, params: Dict = None, timeout: int = 15) -> Optional[Dict]:
        try:
            r = self.session.get(f"{UPSTREAM_API}{path}", params=params, timeout=timeout)
            data = r.json()
            return data if data.get("code") == 0 else None
        except Exception:
            return None

    def _post(self, path: str, body: Dict = None) -> Optional[Dict]:
        try:
            r = self.session.post(f"{UPSTREAM_API}{path}", json=body or {}, timeout=15)
            data = r.json()
            return data if data.get("code") == 0 else None
        except Exception:
            return None

    def get_home(self) -> Dict:
        data = self._get("/wefeed-h5api-bff/home")
        if not data:
            return {"platformList": [], "operatingList": [], "banner": None}
        inner = data.get("data", {})

        platform_list = inner.get("platformList", [])

        banner = None
        operating_sections = []

        for op in inner.get("operatingList", []):
            op_type = op.get("type", "")

            if op_type == "BANNER" and op.get("banner"):
                raw_items = op["banner"].get("items", [])
                banner_items = []
                for item in raw_items:
                    sub = item.get("subject") or {}
                    cover = sub.get("cover") or {}
                    stills = sub.get("stills") or {}
                    banner_items.append({
                        "title": item.get("title") or sub.get("title"),
                        "subjectId": item.get("subjectId"),
                        "subjectType": item.get("subjectType"),
                        "detailPath": item.get("detailPath") or sub.get("detailPath"),
                        "bannerImage": (item.get("image") or {}).get("url"),
                        "posterUrl": cover.get("url"),
                        "stillsUrl": stills.get("url"),
                        "description": sub.get("description") or "",
                        "genre": sub.get("genre") or "",
                        "imdbRating": sub.get("imdbRatingValue"),
                        "releaseDate": sub.get("releaseDate"),
                        "type": "movie" if sub.get("subjectType") == 1 else "series",
                    })
                banner = {"items": banner_items}

            elif op_type == "SUBJECTS_MOVIE" and op.get("subjects"):
                operating_sections.append({
                    "title": op.get("title", ""),
                    "position": op.get("position", 0),
                    "subjects": [_map_subject(s) for s in op["subjects"]],
                })

        return {
            "platformList": platform_list,
            "banner": banner,
            "operatingList": operating_sections,
        }

    def get_trending(self, page: int = 1, per_page: int = 20) -> Dict:
        data = self._get(
            "/wefeed-h5api-bff/subject/trending",
            params={"page": page, "perPage": per_page},
        )
        if not data:
            return {"items": [], "pager": {}}
        inner = data.get("data", {})
        return {
            "items": [_map_subject(s) for s in inner.get("subjectList", [])],
            "pager": inner.get("pager", {}),
        }

    # Genres that indicate music/audio-only content — excluded when no genre filter applied
    _MUSIC_ONLY_GENRES = {
        "gospel", "afropop", "afrobeats", "afrobeat", "reggae", "r&b",
        "country music", "hip hop", "hip-hop", "pop music", "indie music",
        "folk music", "jazz", "blues", "classical music", "electronic music",
        "worship", "devotional", "nursery rhymes", "nursery", "lullaby",
        "nasheed", "islamic music", "christian music", "praise",
    }

    def _is_music_junk(self, item: Dict) -> bool:
        """Return True if the item is clearly a music playlist / audio content."""
        genre_str = (item.get("genre") or "").lower()
        if not genre_str:
            return False
        genres = {g.strip() for g in genre_str.split(",")}
        return bool(genres) and genres.issubset(self._MUSIC_ONLY_GENRES)

    def get_movies(
        self,
        page: int = 1,
        per_page: int = 20,
        subject_type: Optional[int] = None,
        genre: Optional[str] = None,
        country: Optional[str] = None,
        year: Optional[int] = None,
    ) -> Dict:
        body: Dict[str, Any] = {"page": page, "perPage": per_page}
        if subject_type is not None:
            body["subjectType"] = subject_type
        if genre:
            body["genre"] = genre
        if country:
            body["countryName"] = country
        if year:
            body["year"] = str(year)

        data = self._post("/wefeed-h5api-bff/subject/filter", body)
        if not data:
            return {"items": [], "pager": {}}
        inner = data.get("data", {})
        raw = inner.get("items", [])
        # When no genre filter is applied, strip out music-only content
        if not genre:
            raw = [s for s in raw if not self._is_music_junk(s)]
        return {
            "items": [_map_subject(s) for s in raw],
            "pager": inner.get("pager", {}),
        }

    def search(self, keyword: str, page: int = 1, per_page: int = 20) -> Dict:
        data = self._post(
            "/wefeed-h5api-bff/subject/search",
            {"keyword": keyword, "page": page, "perPage": per_page},
        )
        if not data:
            return {"items": [], "pager": {}, "counts": []}
        inner = data.get("data", {})
        return {
            "items": [_map_subject(s) for s in inner.get("items", [])],
            "pager": inner.get("pager", {}),
            "counts": inner.get("counts", []),
        }

    def get_by_id(self, subject_id: str) -> Optional[Dict]:
        data = self._post(
            "/wefeed-h5api-bff/subject/filter",
            {"page": 1, "perPage": 1, "subjectId": subject_id},
        )
        if data and data.get("data", {}).get("items"):
            return _map_subject(data["data"]["items"][0])
        return None

    def get_popular_searches(self) -> List[str]:
        data = self._get("/wefeed-h5api-bff/subject/everyone-search")
        if not data:
            return []
        return [s.get("title", "") for s in data.get("data", {}).get("everyoneSearch", [])]

    def search_suggest(self, keyword: str) -> List[str]:
        data = self._get(
            "/wefeed-h5api-bff/subject/search-suggest",
            params={"keyword": keyword},
        )
        if not data:
            return []
        return [s.get("title", "") for s in data.get("data", {}).get("suggests", [])]

    def get_ranking(self, ranking_id: str = "", page: int = 1, per_page: int = 20) -> Dict:
        data = self._get(
            "/wefeed-h5api-bff/ranking-list/content",
            params={"id": ranking_id, "page": page, "perPage": per_page},
        )
        if not data:
            return {"items": [], "pager": {}}
        inner = data.get("data", {})
        subjects = inner.get("subjectList") or inner.get("items") or []
        return {
            "items": [_map_subject(s) for s in subjects],
            "pager": inner.get("pager", {}),
        }

    def lookup_imdb_id(self, title: str, year: Optional[str], is_series: bool) -> Optional[str]:
        """Look up IMDB ID using IMDB's free autocomplete API."""
        import re as _re
        try:
            # Strip common suffixes that aoneroom adds but IMDB doesn't use
            clean = title.strip()
            clean = _re.sub(r'\s+S\d+(-S\d+)+$', '', clean, flags=_re.I)   # "S1-S5"
            clean = _re.sub(r'\s+S\d+\s*E\d+$', '', clean, flags=_re.I)    # "S2E3"
            clean = _re.sub(r'\s+Season\s*\d+$', '', clean, flags=_re.I)   # "Season 1"
            clean = _re.sub(r'\s*\(\d{4}\)$', '', clean)                    # "(2024)"
            clean = clean.strip()
            query = urllib.parse.quote(clean.lower())
            r = requests.get(
                f"https://v3.sg.media-imdb.com/suggestion/t/{query}.json",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=6,
            )
            if r.status_code != 200:
                return None
            results = r.json().get("d", [])
            title_lower = title.lower().strip()
            year_int = int(year) if year and year.isdigit() else None

            best_id: Optional[str] = None
            best_score = -1
            for item in results:
                imdb_id = item.get("id", "")
                if not imdb_id.startswith("tt"):
                    continue
                item_title = item.get("l", "").lower().strip()
                item_year = item.get("y")
                item_type = item.get("q", "").lower()

                if item_title != title_lower:
                    continue

                score = 0
                if year_int and item_year:
                    diff = abs(int(item_year) - year_int)
                    if diff > 2:
                        continue
                    score += max(0, 10 - diff * 5)
                if is_series and ("series" in item_type or "mini" in item_type):
                    score += 5
                elif not is_series and item_type in ("feature", ""):
                    score += 5

                if score > best_score:
                    best_score = score
                    best_id = imdb_id

            if not best_id:
                for item in results:
                    if item.get("id", "").startswith("tt"):
                        best_id = item["id"]
                        break
            return best_id
        except Exception:
            return None

    def get_stream_info(self, detail_path: str) -> Optional[Dict]:
        data = self._get(
            "/wefeed-h5api-bff/detail",
            params={"detailPath": detail_path},
        )
        if not data:
            return None

        inner = data.get("data", {})
        subject = inner.get("subject", {})
        resource = inner.get("resource", {})
        trailer = subject.get("trailer") or {}
        trailer_address = trailer.get("videoAddress") or {}
        trailer_cover = trailer.get("cover") or {}
        subject_id = subject.get("subjectId", "")
        subject_type = subject.get("subjectType", 1)
        is_series = subject_type == 2

        seasons_raw = resource.get("seasons") or []
        seasons = []
        # Also collect all video URLs we find in the resource data
        _found_video_urls: List[Dict] = []
        for s in seasons_raw:
            season_num = s.get("se", 0)
            resolutions = s.get("resolutions") or []
            max_ep = max((r.get("epNum", 1) for r in resolutions), default=1)
            available_res = sorted(set(r.get("resolution", 0) for r in resolutions if r.get("resolution")))
            seasons.append({
                "season": season_num,
                "episode_count": max_ep,
                "resolutions": available_res,
            })
            # Try to extract video URLs from the raw resolution objects
            for r in resolutions:
                for key in ("url", "videoUrl", "playUrl", "hlsUrl", "m3u8Url", "address"):
                    val = r.get(key)
                    if _is_video_url(val):
                        _found_video_urls.append({
                            "url": val,
                            "resolution": r.get("resolution", 0),
                            "ep": r.get("epNum", 1),
                            "season": season_num,
                        })
                addr = r.get("videoAddress") or r.get("address") or {}
                if isinstance(addr, dict):
                    val = addr.get("url") or addr.get("address")
                    if _is_video_url(val):
                        _found_video_urls.append({
                            "url": val,
                            "resolution": r.get("resolution", 0),
                            "ep": r.get("epNum", 1),
                            "season": season_num,
                        })

        trailer_url = trailer_address.get("url")
        trailer_duration = trailer_address.get("duration")

        def build_embed(ep: int = 1, resolution: int = 1080, season: int = 0) -> str:
            # Return path through our own proxy — never expose the upstream player domain
            params = f"id={subject_id}&ep={ep}&resolution={resolution}"
            if is_series and season:
                params += f"&se={season}"
            return f"/movies-api/proxy/player/movies/{detail_path}?{params}"

        default_resolution = 1080
        if seasons and seasons[0].get("resolutions"):
            default_resolution = max(seasons[0]["resolutions"])

        default_season = seasons[0]["season"] if seasons else 0

        cover = subject.get("cover") or {}
        stills = subject.get("stills") or {}
        title = subject.get("title") or ""
        release_date = subject.get("releaseDate") or ""
        year = release_date[:4] if release_date else None

        # Look up IMDB ID for geo-unrestricted embed sources (vidsrc.to)
        imdb_id = self.lookup_imdb_id(title, year, is_series)

        return {
            "id": subject_id,
            "detail_path": detail_path,
            "title": title,
            "type": "series" if is_series else "movie",
            "is_series": is_series,
            "imdb_id": imdb_id,
            "cover_url": cover.get("url"),
            "stills_url": stills.get("url"),
            "description": subject.get("description") or None,
            "genre": subject.get("genre") or None,
            "imdb_rating": subject.get("imdbRatingValue"),
            "country": subject.get("countryName"),
            "release_date": release_date or None,
            "trailer": {
                "url": trailer_url,
                "duration_seconds": trailer_duration,
                "thumbnail": trailer_cover.get("url"),
            } if trailer_url else None,
            "seasons": seasons,
            "player": {
                "embed_url": build_embed(ep=1, resolution=default_resolution, season=default_season),
            },
            "_found_video_urls": _found_video_urls,
            "_raw_resource_keys": list(resource.keys()),
        }

    def get_video_url_from_netfilm(self, subject_id: str, detail_path: str = "", ep: int = 1, season: int = 0, resolution: int = 1080) -> Optional[Dict]:
        """Fetch the netfilm.world player page and extract the video URL from HTML/JS or its API."""
        import sys

        player_headers = {
            "User-Agent": HEADERS["User-Agent"],
            "Referer": PLAYER_URL + "/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        # 1. Try netfilm.world internal API endpoints (the player's JS calls these)
        netfilm_api_candidates = [
            f"{PLAYER_URL}/api/video",
            f"{PLAYER_URL}/api/source",
            f"{PLAYER_URL}/api/stream",
            f"{PLAYER_URL}/api/play",
            f"{PLAYER_URL}/api/player",
        ]
        params: Dict = {"id": subject_id, "ep": ep, "resolution": resolution}
        if season:
            params["se"] = season

        api_headers = {
            "User-Agent": HEADERS["User-Agent"],
            "Referer": PLAYER_URL + "/",
            "Accept": "application/json",
        }

        for api_url in netfilm_api_candidates:
            try:
                r = self.session.get(api_url, params=params, headers=api_headers, timeout=5)
                if r.status_code == 200 and "application/json" in r.headers.get("content-type", ""):
                    data = r.json()
                    result = self._extract_video_url_from_json(data)
                    if result:
                        print(f"[netfilm-api] Found video URL via {api_url}", file=sys.stderr)
                        return result
            except Exception:
                pass

        # 2. Fetch the player page HTML and parse for video URLs
        if not detail_path:
            return None
        try:
            page_url = f"{PLAYER_URL}/movies/{detail_path}?id={subject_id}&ep={ep}&resolution={resolution}"
            if season:
                page_url += f"&se={season}"
            r = self.session.get(page_url, headers=player_headers, timeout=10)
            text = r.text

            # Look for HLS / MP4 URLs in the HTML/inline JS
            url_patterns = [
                r'["\']?(https?://[^\s\'"<>]+\.m3u8[^\s\'"<>]*)["\']?',
                r'["\']?(https?://[^\s\'"<>]+\.mp4[^\s\'"<>]*)["\']?',
                r'(?:src|url|source|videoUrl|playUrl)\s*[:=]\s*["\']?(https?://[^\s\'"<>]+)["\']?',
            ]
            for pat in url_patterns:
                matches = re.findall(pat, text, re.IGNORECASE)
                for m in matches:
                    m = m.strip('"\'')
                    if _is_video_url(m):
                        vtype = "m3u8" if ".m3u8" in m else "mp4"
                        print(f"[netfilm-html] Found {vtype} URL: {m[:80]}", file=sys.stderr)
                        return {"url": m, "type": vtype}

            # Look for embedded JSON blobs that might contain the video URL
            json_blobs = re.findall(r'\{[^{}]{20,}\}', text)
            for blob in json_blobs[:30]:
                try:
                    import json as _json
                    d = _json.loads(blob)
                    result = self._extract_video_url_from_json(d)
                    if result:
                        return result
                except Exception:
                    pass

        except Exception as e:
            print(f"[netfilm-html] Error: {e}", file=sys.stderr)

        return None

    def _extract_video_url_from_json(self, data: Any) -> Optional[Dict]:
        """Recursively search a JSON structure for a video URL."""
        if isinstance(data, str):
            if _is_video_url(data):
                return {"url": data, "type": "m3u8" if ".m3u8" in data else "mp4"}
            return None
        if isinstance(data, dict):
            # Direct URL fields — validated through _is_video_url
            for key in ("url", "videoUrl", "playUrl", "hlsUrl", "m3u8Url", "source",
                        "address", "videoAddress", "streamUrl", "mediaUrl", "fileUrl"):
                val = data.get(key)
                if _is_video_url(val):
                    return {"url": val, "type": "m3u8" if ".m3u8" in val else "mp4"}
            # Recurse into nested dicts/lists
            for v in data.values():
                result = self._extract_video_url_from_json(v)
                if result:
                    return result
        if isinstance(data, list):
            for item in data[:10]:
                result = self._extract_video_url_from_json(item)
                if result:
                    return result
        return None

    def get_video_sources_bwm(self, subject_id: str, ep: int = 1, season: int = 0, title: str = "") -> list:
        """
        Query zone.bwmxmd.co.ke's public Supabase function (GiftedTech backend) for
        direct MP4 download URLs.  Returns a list of {quality, url, filename} dicts
        sorted best-quality-first.  Returns [] if the upstream is unreachable or
        returns no results.
        """
        import urllib.request as _req
        BWM = "https://aubiomhswbxrxgfnoles.supabase.co/functions/v1/bwm-xmd"
        endpoint = f"/sources/{subject_id}"
        if season:
            endpoint += f"?season={season}&episode={ep}"
        else:
            endpoint += f""  # movie — no season param
        url = f"{BWM}?action=movie&endpoint={urllib.parse.quote(endpoint)}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 11; M2007J3SC) AppleWebKit/537.36",
            "Origin": "https://zone.bwmxmd.co.ke",
            "Referer": "https://zone.bwmxmd.co.ke/",
            "Accept": "application/json",
        }
        try:
            req = _req.Request(url, headers=headers)
            with _req.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read())
        except Exception as e:
            import sys
            print(f"[bwm-sources] fetch error: {e}", file=sys.stderr)
            return []

        results = data.get("results") or []
        if not results:
            return []

        safe = re.sub(r"[^a-zA-Z0-9_\-]", "_", title) if title else subject_id
        quality_order = {"1080p": 0, "720p": 1, "480p": 2, "360p": 3}

        # Reject trailer/teaser/sample URLs — these are NOT the real movie
        _TRAILER_SIGNALS = ("trailer", "teaser", "preview", "sample", "promo", "clip", "featurette")

        # Minimum file size for a real movie/episode (20 MB).
        # A 4 MB "movie" is a trailer/promo, even if the URL looks fine.
        _MIN_BYTES = 20 * 1024 * 1024  # 20 MB

        import sys

        def _check_size(check_url: str) -> bool:
            """HEAD request to get Content-Length. Reject if known and < _MIN_BYTES."""
            try:
                head_req = _req.Request(check_url, method="HEAD", headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://zone.bwmxmd.co.ke/",
                })
                with _req.urlopen(head_req, timeout=5) as r:
                    cl = r.headers.get("Content-Length")
                    if cl:
                        size = int(cl)
                        if size < _MIN_BYTES:
                            print(f"[bwm-sources] size too small ({size//1024} KB) — skipping: {check_url[:80]}", file=sys.stderr)
                            return False
            except Exception as e:
                # Can't determine size — accept it and let the browser deal with it
                print(f"[bwm-sources] HEAD failed ({e}), accepting: {check_url[:80]}", file=sys.stderr)
            return True

        out = []
        for item in results:
            q = item.get("quality", "")
            dl_url = item.get("download_url", "") or item.get("stream_url", "")
            if not dl_url:
                continue
            url_lower = dl_url.lower()
            label_lower = (item.get("label", "") or q).lower()
            if any(sig in url_lower or sig in label_lower for sig in _TRAILER_SIGNALS):
                print(f"[bwm-sources] skipping trailer URL: {dl_url[:80]}", file=sys.stderr)
                continue
            if not _check_size(dl_url):
                continue
            out.append({
                "quality": q,
                "url": dl_url,
                "filename": f"{safe}_{q}.mp4",
                "_order": quality_order.get(q, 99),
            })
        out.sort(key=lambda x: x["_order"])
        for item in out:
            del item["_order"]
        return out

    def get_video_url(self, subject_id: str, ep: int = 1, season: int = 0, resolution: int = 1080) -> Optional[Dict]:
        """Try to get the direct video/HLS URL from the aoneroom API (3s per attempt, fail fast)."""
        candidates = [
            "/wefeed-h5api-bff/resource/video",
            "/wefeed-h5api-bff/resource/episode-video",
            "/wefeed-h5api-bff/resource/playback",
            "/wefeed-h5api-bff/resource/video-source",
            "/wefeed-h5api-bff/resource/video-address",
        ]
        for path in candidates:
            params: Dict = {"subjectId": subject_id, "ep": ep, "resolution": resolution}
            if season:
                params["se"] = season
            data = self._get(path, params=params, timeout=3)
            if not data:
                continue
            inner = data.get("data", {})
            # Flat URL fields — validated through _is_video_url
            for key in ("url", "videoUrl", "playUrl", "hlsUrl", "m3u8Url", "source", "address", "videoAddress"):
                val = inner.get(key)
                if _is_video_url(val):
                    return {"url": val, "type": "m3u8" if ".m3u8" in val else "mp4", "endpoint": path}
            # Nested address objects
            for key in ("videoAddress", "address", "playInfo"):
                obj = inner.get(key)
                if isinstance(obj, dict):
                    val = obj.get("url") or obj.get("address")
                    if _is_video_url(val):
                        return {"url": val, "type": "m3u8" if ".m3u8" in val else "mp4", "endpoint": path}
            # Log what we got back so we can tune the endpoint discovery
            import sys
            print(f"[download-probe] {path} -> keys={list(inner.keys())}", file=sys.stderr)
        return None

    def get_related(self, subject_id: str, page: int = 1, per_page: int = 12) -> Dict:
        data = self._get(
            "/wefeed-h5api-bff/subject/detail-rec",
            params={"subjectId": subject_id, "page": page, "perPage": per_page},
        )
        if not data:
            return {"items": [], "pager": {}}
        inner = data.get("data", {})
        subjects = inner.get("subjectList") or inner.get("items") or []
        return {
            "items": [_map_subject(s) for s in subjects],
            "pager": inner.get("pager", {}),
        }
