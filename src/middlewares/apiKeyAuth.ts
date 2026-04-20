import { type Request, type Response, type NextFunction } from "express";

const MOVIE_API_KEY = process.env["MOVIE_API_KEY"];

export function apiKeyAuth(req: Request, res: Response, next: NextFunction): void {
  if (!MOVIE_API_KEY) {
    next();
    return;
  }

  const authHeader = req.headers["authorization"];
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    res.status(403).json({
      status: 403,
      success: false,
      message: "API Key Required. Please provide a valid Bearer token in the Authorization header.",
    });
    return;
  }

  const token = authHeader.slice(7);
  if (token !== MOVIE_API_KEY) {
    res.status(403).json({ status: 403, success: false, message: "Invalid API key." });
    return;
  }

  next();
}
