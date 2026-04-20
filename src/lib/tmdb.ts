const TMDB_BASE = "https://api.themoviedb.org/3";
const TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p";
const API_KEY = process.env["TMDB_API_KEY"];

if (!API_KEY) {
  throw new Error("TMDB_API_KEY environment variable is required");
}

function imageUrl(path: string | null, size = "w500"): string | null {
  if (!path) return null;
  return `${TMDB_IMAGE_BASE}/${size}${path}`;
}

async function tmdbFetch(path: string, params: Record<string, string> = {}): Promise<unknown> {
  const url = new URL(`${TMDB_BASE}${path}`);
  url.searchParams.set("api_key", API_KEY!);
  for (const [k, v] of Object.entries(params)) {
    url.searchParams.set(k, v);
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(`TMDb request failed: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export interface MovieResult {
  id: number;
  title: string;
  type: "movie" | "tv";
  poster: string | null;
  backdrop: string | null;
  overview: string;
  rating: number;
  releaseDate: string | null;
  genres: string[];
  language: string;
}

export interface MovieDetail extends MovieResult {
  runtime: number | null;
  status: string;
  tagline: string | null;
  cast: { name: string; character: string; photo: string | null }[];
  trailer: string | null;
  seasons?: number | null;
  episodes?: number | null;
}

const GENRE_MAP: Record<number, string> = {
  28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
  99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
  27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance",
  878: "Science Fiction", 10770: "TV Movie", 53: "Thriller", 10752: "War",
  37: "Western", 10759: "Action & Adventure", 10762: "Kids", 10763: "News",
  10764: "Reality", 10765: "Sci-Fi & Fantasy", 10766: "Soap", 10767: "Talk",
  10768: "War & Politics",
};

function formatMovie(item: Record<string, unknown>): MovieResult {
  const isTV = item["media_type"] === "tv" || item["name"] !== undefined;
  const title = (isTV ? item["name"] : item["title"]) as string ?? "Untitled";
  const releaseDate = (isTV ? item["first_air_date"] : item["release_date"]) as string | null ?? null;
  const genreIds = (item["genre_ids"] as number[] | undefined) ?? [];
  const genres = genreIds.map((id) => GENRE_MAP[id]).filter(Boolean);

  return {
    id: item["id"] as number,
    title,
    type: isTV ? "tv" : "movie",
    poster: imageUrl(item["poster_path"] as string | null),
    backdrop: imageUrl(item["backdrop_path"] as string | null, "w1280"),
    overview: (item["overview"] as string) ?? "",
    rating: parseFloat(((item["vote_average"] as number) ?? 0).toFixed(1)),
    releaseDate,
    genres,
    language: (item["original_language"] as string) ?? "en",
  };
}

export async function getTrending(page = "1"): Promise<{ results: MovieResult[]; totalPages: number; page: number }> {
  const data = await tmdbFetch("/trending/all/week", { page }) as Record<string, unknown>;
  const results = (data["results"] as Record<string, unknown>[]).map(formatMovie);
  return { results, totalPages: data["total_pages"] as number, page: data["page"] as number };
}

export async function searchMovies(query: string, page = "1"): Promise<{ results: MovieResult[]; totalPages: number; page: number; totalResults: number }> {
  const data = await tmdbFetch("/search/multi", { query, page, include_adult: "false" }) as Record<string, unknown>;
  const results = (data["results"] as Record<string, unknown>[])
    .filter((item) => item["media_type"] === "movie" || item["media_type"] === "tv")
    .map(formatMovie);
  return {
    results,
    totalPages: data["total_pages"] as number,
    page: data["page"] as number,
    totalResults: data["total_results"] as number,
  };
}

export async function getMovieInfo(id: string): Promise<MovieDetail> {
  let data: Record<string, unknown>;
  let isTV = false;

  try {
    data = await tmdbFetch(`/movie/${id}`, { append_to_response: "credits,videos" }) as Record<string, unknown>;
  } catch {
    data = await tmdbFetch(`/tv/${id}`, { append_to_response: "credits,videos" }) as Record<string, unknown>;
    isTV = true;
  }

  const title = (isTV ? data["name"] : data["title"]) as string ?? "Untitled";
  const releaseDate = (isTV ? data["first_air_date"] : data["release_date"]) as string | null ?? null;
  const genreList = (data["genres"] as { id: number; name: string }[] | undefined) ?? [];
  const genres = genreList.map((g) => g.name);

  const credits = (data["credits"] as Record<string, unknown> | undefined) ?? {};
  const cast = ((credits["cast"] as Record<string, unknown>[] | undefined) ?? [])
    .slice(0, 15)
    .map((c) => ({
      name: c["name"] as string,
      character: c["character"] as string,
      photo: imageUrl(c["profile_path"] as string | null),
    }));

  const videos = (data["videos"] as Record<string, unknown> | undefined) ?? {};
  const videoResults = (videos["results"] as Record<string, unknown>[] | undefined) ?? [];
  const trailer = videoResults.find(
    (v) => v["type"] === "Trailer" && v["site"] === "YouTube"
  );
  const trailerUrl = trailer ? `https://www.youtube.com/watch?v=${trailer["key"]}` : null;

  return {
    id: data["id"] as number,
    title,
    type: isTV ? "tv" : "movie",
    poster: imageUrl(data["poster_path"] as string | null),
    backdrop: imageUrl(data["backdrop_path"] as string | null, "w1280"),
    overview: (data["overview"] as string) ?? "",
    rating: parseFloat(((data["vote_average"] as number) ?? 0).toFixed(1)),
    releaseDate,
    genres,
    language: (data["original_language"] as string) ?? "en",
    runtime: isTV ? null : (data["runtime"] as number | null ?? null),
    status: (data["status"] as string) ?? "Unknown",
    tagline: (data["tagline"] as string | null) ?? null,
    cast,
    trailer: trailerUrl,
    seasons: isTV ? (data["number_of_seasons"] as number | null ?? null) : undefined,
    episodes: isTV ? (data["number_of_episodes"] as number | null ?? null) : undefined,
  };
}

export async function getHomepage(): Promise<{
  trending: MovieResult[];
  popularMovies: MovieResult[];
  popularTV: MovieResult[];
  topRated: MovieResult[];
}> {
  const [trendingData, moviesData, tvData, topRatedData] = await Promise.all([
    tmdbFetch("/trending/all/week", { page: "1" }) as Promise<Record<string, unknown>>,
    tmdbFetch("/movie/popular", { page: "1" }) as Promise<Record<string, unknown>>,
    tmdbFetch("/tv/popular", { page: "1" }) as Promise<Record<string, unknown>>,
    tmdbFetch("/movie/top_rated", { page: "1" }) as Promise<Record<string, unknown>>,
  ]);

  const withType = (items: Record<string, unknown>[], type: "movie" | "tv") =>
    items.map((item) => formatMovie({ ...item, media_type: type }));

  return {
    trending: ((trendingData["results"] as Record<string, unknown>[]) ?? []).slice(0, 20).map(formatMovie),
    popularMovies: withType((moviesData["results"] as Record<string, unknown>[]) ?? [], "movie").slice(0, 20),
    popularTV: withType((tvData["results"] as Record<string, unknown>[]) ?? [], "tv").slice(0, 20),
    topRated: withType((topRatedData["results"] as Record<string, unknown>[]) ?? [], "movie").slice(0, 20),
  };
}
