import { Router, type IRouter } from "express";
import { apiKeyAuth } from "../../middlewares/apiKeyAuth.js";
import { getTrending, searchMovies, getMovieInfo, getHomepage } from "../../lib/tmdb.js";

const router: IRouter = Router();

router.use(apiKeyAuth);

router.get("/v2/homepage", async (_req, res): Promise<void> => {
  const data = await getHomepage();
  res.json({ status: 200, success: true, results: data });
});

router.get("/v2/trending", async (req, res): Promise<void> => {
  const page = typeof req.query["page"] === "string" ? req.query["page"] : "1";
  const data = await getTrending(page);
  res.json({ status: 200, success: true, page: data.page, totalPages: data.totalPages, results: data.results });
});

router.get("/v2/search/:query", async (req, res): Promise<void> => {
  const raw = Array.isArray(req.params["query"]) ? req.params["query"][0] : req.params["query"];
  const query = decodeURIComponent(raw ?? "");
  const page = typeof req.query["page"] === "string" ? req.query["page"] : "1";

  if (!query) {
    res.status(400).json({ status: 400, success: false, message: "Query is required" });
    return;
  }

  const data = await searchMovies(query, page);
  res.json({ status: 200, success: true, page: data.page, totalPages: data.totalPages, totalResults: data.totalResults, results: data.results });
});

router.get("/v2/info/:id", async (req, res): Promise<void> => {
  const raw = Array.isArray(req.params["id"]) ? req.params["id"][0] : req.params["id"];
  const id = raw ?? "";

  if (!id) {
    res.status(400).json({ status: 400, success: false, message: "ID is required" });
    return;
  }

  const data = await getMovieInfo(id);
  res.json({ status: 200, success: true, result: data });
});

export default router;
