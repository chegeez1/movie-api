import { Router, type IRouter } from "express";

const router: IRouter = Router();

const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Movie API — Documentation</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --bg: #0a0a0f;
      --surface: #111118;
      --surface2: #1a1a24;
      --border: #2a2a3a;
      --accent: #6c63ff;
      --accent2: #a78bfa;
      --green: #22c55e;
      --text: #e2e8f0;
      --muted: #94a3b8;
      --code-bg: #0d0d14;
    }
    body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); line-height: 1.6; }

    /* NAV */
    nav { background: rgba(10,10,15,0.95); backdrop-filter: blur(12px); border-bottom: 1px solid var(--border); position: sticky; top: 0; z-index: 100; padding: 0 2rem; display: flex; align-items: center; justify-content: space-between; height: 64px; }
    .nav-brand { display: flex; align-items: center; gap: 10px; font-weight: 700; font-size: 1.1rem; }
    .nav-brand .logo { width: 32px; height: 32px; background: linear-gradient(135deg, var(--accent), var(--accent2)); border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 16px; }
    .status-badge { background: rgba(34,197,94,0.15); color: var(--green); border: 1px solid rgba(34,197,94,0.3); padding: 4px 12px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; display: flex; align-items: center; gap: 6px; }
    .status-dot { width: 6px; height: 6px; background: var(--green); border-radius: 50%; animation: pulse 2s infinite; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

    /* HERO */
    .hero { text-align: center; padding: 80px 2rem 60px; background: radial-gradient(ellipse at 50% 0%, rgba(108,99,255,0.12) 0%, transparent 60%); }
    .hero-badge { display: inline-flex; align-items: center; gap: 8px; background: rgba(108,99,255,0.1); border: 1px solid rgba(108,99,255,0.3); color: var(--accent2); padding: 6px 16px; border-radius: 20px; font-size: 0.8rem; font-weight: 600; margin-bottom: 24px; }
    h1 { font-size: clamp(2rem, 5vw, 3.5rem); font-weight: 800; background: linear-gradient(135deg, #fff 30%, var(--accent2)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; margin-bottom: 16px; }
    .hero-sub { color: var(--muted); font-size: 1.1rem; max-width: 560px; margin: 0 auto 36px; }
    .base-url-box { display: inline-flex; align-items: center; gap: 12px; background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 12px 20px; font-family: monospace; font-size: 0.95rem; color: var(--accent2); }
    .base-label { color: var(--muted); font-family: sans-serif; font-size: 0.8rem; font-weight: 600; }

    /* STATS */
    .stats { display: flex; justify-content: center; gap: 2rem; padding: 40px 2rem; border-bottom: 1px solid var(--border); flex-wrap: wrap; }
    .stat { text-align: center; }
    .stat-val { font-size: 1.8rem; font-weight: 800; background: linear-gradient(135deg, var(--accent), var(--accent2)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }
    .stat-label { color: var(--muted); font-size: 0.8rem; margin-top: 4px; }

    /* MAIN */
    .container { max-width: 900px; margin: 0 auto; padding: 60px 2rem; }
    h2 { font-size: 1.6rem; font-weight: 700; margin-bottom: 28px; color: #fff; }
    h3 { font-size: 1rem; font-weight: 700; margin-bottom: 12px; color: var(--text); }

    /* ENDPOINT CARDS */
    .endpoint { background: var(--surface); border: 1px solid var(--border); border-radius: 14px; margin-bottom: 24px; overflow: hidden; transition: border-color 0.2s; }
    .endpoint:hover { border-color: var(--accent); }
    .endpoint-header { padding: 20px 24px; display: flex; align-items: center; gap: 14px; }
    .method { background: rgba(108,99,255,0.15); color: var(--accent2); border: 1px solid rgba(108,99,255,0.3); padding: 4px 12px; border-radius: 6px; font-size: 0.75rem; font-weight: 700; font-family: monospace; letter-spacing: 0.05em; }
    .path { font-family: monospace; font-size: 1rem; color: var(--text); font-weight: 600; }
    .endpoint-desc { color: var(--muted); font-size: 0.9rem; margin-left: auto; }
    .endpoint-body { border-top: 1px solid var(--border); padding: 20px 24px; background: var(--surface2); }

    /* CODE BLOCKS */
    .code-block { background: var(--code-bg); border: 1px solid var(--border); border-radius: 8px; padding: 16px; font-family: 'Courier New', monospace; font-size: 0.85rem; color: #a78bfa; overflow-x: auto; margin-top: 10px; }
    .code-block .key { color: #7dd3fc; }
    .code-block .str { color: #86efac; }
    .code-block .num { color: #fbbf24; }
    .code-block .bool { color: #f472b6; }

    /* PARAMS TABLE */
    .params-table { width: 100%; border-collapse: collapse; font-size: 0.875rem; margin-top: 10px; }
    .params-table th { text-align: left; padding: 8px 12px; color: var(--muted); font-weight: 600; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid var(--border); }
    .params-table td { padding: 10px 12px; border-bottom: 1px solid rgba(42,42,58,0.5); }
    .param-name { font-family: monospace; color: var(--accent2); }
    .param-type { color: var(--muted); font-size: 0.8rem; }
    .optional-badge { background: rgba(148,163,184,0.1); color: var(--muted); border-radius: 4px; padding: 1px 6px; font-size: 0.7rem; }

    /* AUTH SECTION */
    .auth-box { background: rgba(108,99,255,0.05); border: 1px solid rgba(108,99,255,0.2); border-radius: 12px; padding: 24px; margin-bottom: 40px; }
    .auth-box code { background: var(--code-bg); border: 1px solid var(--border); padding: 12px 16px; border-radius: 8px; font-family: monospace; font-size: 0.875rem; color: var(--accent2); display: block; margin-top: 12px; }

    /* FEATURES GRID */
    .features { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 60px; }
    .feature { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }
    .feature-icon { font-size: 1.5rem; margin-bottom: 10px; }
    .feature h4 { font-size: 0.9rem; font-weight: 700; margin-bottom: 6px; }
    .feature p { color: var(--muted); font-size: 0.8rem; }

    footer { text-align: center; padding: 40px 2rem; border-top: 1px solid var(--border); color: var(--muted); font-size: 0.85rem; }
  </style>
</head>
<body>
  <nav>
    <div class="nav-brand">
      <div class="logo">🎬</div>
      <span>Movie API</span>
    </div>
    <div class="status-badge">
      <span class="status-dot"></span>
      All Systems Operational
    </div>
  </nav>

  <div class="hero">
    <div class="hero-badge">⚡ v2.0.0 · REST API · JSON Responses</div>
    <h1>Movie API</h1>
    <p class="hero-sub">A blazing fast REST API for movies & TV shows. Search, browse trending content, and get detailed metadata — all in JSON.</p>
    <div class="base-url-box">
      <span class="base-label">BASE URL</span>
      <span id="base-url">/api/v2</span>
    </div>
  </div>

  <div class="stats">
    <div class="stat"><div class="stat-val">4</div><div class="stat-label">Endpoints</div></div>
    <div class="stat"><div class="stat-val">GET</div><div class="stat-label">Method</div></div>
    <div class="stat"><div class="stat-val">JSON</div><div class="stat-label">Response Format</div></div>
    <div class="stat"><div class="stat-val">500K+</div><div class="stat-label">Titles Available</div></div>
  </div>

  <div class="container">

    <div class="features">
      <div class="feature"><div class="feature-icon">🔍</div><h4>Advanced Search</h4><p>Search across thousands of movies and TV series with real-time results.</p></div>
      <div class="feature"><div class="feature-icon">📊</div><h4>Trending Content</h4><p>Get what's trending this week across movies and TV shows.</p></div>
      <div class="feature"><div class="feature-icon">🎭</div><h4>Rich Metadata</h4><p>Full cast, trailers, ratings, genres, runtime, and more.</p></div>
      <div class="feature"><div class="feature-icon">⚡</div><h4>Fast Responses</h4><p>Optimized API with response times under 300ms.</p></div>
    </div>

    <div class="auth-box">
      <h3>🔐 Authentication</h3>
      <p style="color:var(--muted);font-size:0.9rem;">If an API key is configured, include it as a Bearer token in every request header:</p>
      <code>Authorization: Bearer YOUR_API_KEY</code>
    </div>

    <h2>API Endpoints</h2>

    <!-- HOMEPAGE -->
    <div class="endpoint">
      <div class="endpoint-header">
        <span class="method">GET</span>
        <span class="path">/api/v2/homepage</span>
        <span class="endpoint-desc">Homepage content</span>
      </div>
      <div class="endpoint-body">
        <p style="color:var(--muted);font-size:0.875rem;margin-bottom:14px;">Returns trending, popular movies, popular TV shows, and top rated movies all in one call.</p>
        <h3>Example Request</h3>
        <div class="code-block">GET /api/v2/homepage</div>
        <h3 style="margin-top:16px;">Example Response</h3>
        <div class="code-block">
{<br>
&nbsp;&nbsp;<span class="key">"status"</span>: <span class="num">200</span>,<br>
&nbsp;&nbsp;<span class="key">"success"</span>: <span class="bool">true</span>,<br>
&nbsp;&nbsp;<span class="key">"results"</span>: {<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"trending"</span>: [...],<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"popularMovies"</span>: [...],<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"popularTV"</span>: [...],<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"topRated"</span>: [...]<br>
&nbsp;&nbsp;}<br>
}
        </div>
      </div>
    </div>

    <!-- TRENDING -->
    <div class="endpoint">
      <div class="endpoint-header">
        <span class="method">GET</span>
        <span class="path">/api/v2/trending</span>
        <span class="endpoint-desc">Trending this week</span>
      </div>
      <div class="endpoint-body">
        <p style="color:var(--muted);font-size:0.875rem;margin-bottom:14px;">Get trending movies and TV series based on current popularity.</p>
        <h3>Parameters</h3>
        <table class="params-table">
          <tr><th>Name</th><th>Type</th><th>Description</th></tr>
          <tr><td class="param-name">page</td><td class="param-type">integer <span class="optional-badge">optional</span></td><td style="color:var(--muted)">Page number (default: 1)</td></tr>
        </table>
        <h3 style="margin-top:16px;">Example Request</h3>
        <div class="code-block">GET /api/v2/trending?page=1</div>
        <h3 style="margin-top:16px;">Example Response</h3>
        <div class="code-block">
{<br>
&nbsp;&nbsp;<span class="key">"status"</span>: <span class="num">200</span>,<br>
&nbsp;&nbsp;<span class="key">"success"</span>: <span class="bool">true</span>,<br>
&nbsp;&nbsp;<span class="key">"page"</span>: <span class="num">1</span>,<br>
&nbsp;&nbsp;<span class="key">"totalPages"</span>: <span class="num">500</span>,<br>
&nbsp;&nbsp;<span class="key">"results"</span>: [<br>
&nbsp;&nbsp;&nbsp;&nbsp;{<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"id"</span>: <span class="num">85552</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"title"</span>: <span class="str">"Euphoria"</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"type"</span>: <span class="str">"tv"</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"poster"</span>: <span class="str">"https://image.tmdb.org/t/p/w500/..."</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"rating"</span>: <span class="num">8.3</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"genres"</span>: [<span class="str">"Drama"</span>]<br>
&nbsp;&nbsp;&nbsp;&nbsp;}<br>
&nbsp;&nbsp;]<br>
}
        </div>
      </div>
    </div>

    <!-- SEARCH -->
    <div class="endpoint">
      <div class="endpoint-header">
        <span class="method">GET</span>
        <span class="path">/api/v2/search/{query}</span>
        <span class="endpoint-desc">Search movies & TV</span>
      </div>
      <div class="endpoint-body">
        <p style="color:var(--muted);font-size:0.875rem;margin-bottom:14px;">Search for movies and TV series by title. Returns paginated results with detailed information.</p>
        <h3>Parameters</h3>
        <table class="params-table">
          <tr><th>Name</th><th>Type</th><th>Description</th></tr>
          <tr><td class="param-name">query</td><td class="param-type">string</td><td style="color:var(--muted)">Search query (movie or TV series title)</td></tr>
          <tr><td class="param-name">page</td><td class="param-type">integer <span class="optional-badge">optional</span></td><td style="color:var(--muted)">Page number (default: 1)</td></tr>
        </table>
        <h3 style="margin-top:16px;">Example Request</h3>
        <div class="code-block">GET /api/v2/search/Black%20Panther?page=1</div>
      </div>
    </div>

    <!-- INFO -->
    <div class="endpoint">
      <div class="endpoint-header">
        <span class="method">GET</span>
        <span class="path">/api/v2/info/{id}</span>
        <span class="endpoint-desc">Full movie/show details</span>
      </div>
      <div class="endpoint-body">
        <p style="color:var(--muted);font-size:0.875rem;margin-bottom:14px;">Get detailed information about a specific movie or TV series including cast, trailer, and metadata.</p>
        <h3>Parameters</h3>
        <table class="params-table">
          <tr><th>Name</th><th>Type</th><th>Description</th></tr>
          <tr><td class="param-name">id</td><td class="param-type">integer</td><td style="color:var(--muted)">Movie or TV series ID (from search results)</td></tr>
        </table>
        <h3 style="margin-top:16px;">Example Request</h3>
        <div class="code-block">GET /api/v2/info/299536</div>
        <h3 style="margin-top:16px;">Example Response</h3>
        <div class="code-block">
{<br>
&nbsp;&nbsp;<span class="key">"status"</span>: <span class="num">200</span>,<br>
&nbsp;&nbsp;<span class="key">"success"</span>: <span class="bool">true</span>,<br>
&nbsp;&nbsp;<span class="key">"result"</span>: {<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"id"</span>: <span class="num">299536</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"title"</span>: <span class="str">"Avengers: Infinity War"</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"type"</span>: <span class="str">"movie"</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"poster"</span>: <span class="str">"https://image.tmdb.org/t/p/w500/..."</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"backdrop"</span>: <span class="str">"https://image.tmdb.org/t/p/w1280/..."</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"rating"</span>: <span class="num">8.2</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"runtime"</span>: <span class="num">149</span>,<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"genres"</span>: [<span class="str">"Action"</span>, <span class="str">"Adventure"</span>],<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"cast"</span>: [...],<br>
&nbsp;&nbsp;&nbsp;&nbsp;<span class="key">"trailer"</span>: <span class="str">"https://www.youtube.com/watch?v=..."</span><br>
&nbsp;&nbsp;}<br>
}
        </div>
      </div>
    </div>

    <!-- CODE EXAMPLES -->
    <h2 style="margin-top:60px;">Code Examples</h2>
    <div class="endpoint">
      <div class="endpoint-header" style="padding-bottom:12px;">
        <span style="font-weight:700;color:#fff;">JavaScript</span>
      </div>
      <div class="endpoint-body">
        <div class="code-block" style="color:#e2e8f0;white-space:pre;">
<span style="color:#7dd3fc;">const</span> BASE = <span style="color:#86efac;">'/api/v2'</span>;

<span style="color:#7dd3fc;">async function</span> <span style="color:#fbbf24;">search</span>(query) {
  <span style="color:#7dd3fc;">const</span> res = <span style="color:#7dd3fc;">await</span> fetch(<span style="color:#86efac;">\`\${BASE}/search/\${query}?page=1\`</span>);
  <span style="color:#7dd3fc;">return</span> res.json();
}

<span style="color:#7dd3fc;">async function</span> <span style="color:#fbbf24;">getInfo</span>(id) {
  <span style="color:#7dd3fc;">const</span> res = <span style="color:#7dd3fc;">await</span> fetch(<span style="color:#86efac;">\`\${BASE}/info/\${id}\`</span>);
  <span style="color:#7dd3fc;">return</span> res.json();
}
        </div>
      </div>
    </div>

  </div>

  <footer>
    <p>Movie API v2.0.0 — Powered by TMDb data</p>
  </footer>

  <script>
    const host = window.location.origin;
    document.getElementById('base-url').textContent = host + '/api/v2';
  </script>
</body>
</html>`;

router.get("/", (_req, res): void => {
  res.setHeader("Content-Type", "text/html");
  res.send(html);
});

router.get("/api", (_req, res): void => {
  res.setHeader("Content-Type", "text/html");
  res.send(html);
});

router.get("/api/docs", (_req, res): void => {
  res.setHeader("Content-Type", "text/html");
  res.send(html);
});

export default router;
