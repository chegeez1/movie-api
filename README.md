# Movie API

A blazing-fast REST API for movies and TV shows powered by TMDb.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v2/homepage` | Trending + popular content |
| GET | `/api/v2/trending` | Trending this week |
| GET | `/api/v2/search/:query` | Search movies & TV |
| GET | `/api/v2/info/:id` | Full movie/show details |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TMDB_API_KEY` | Yes | Your TMDb API key |
| `MOVIE_API_KEY` | No | Optional Bearer token auth |
| `PORT` | No | Port (default: 3000) |

## Deploy to Render

1. Push this repo to GitHub
2. Create a new Web Service on [Render](https://render.com)
3. Connect your GitHub repo
4. Set `TMDB_API_KEY` environment variable
5. Build command: `npm install && npm run build`
6. Start command: `npm start`
