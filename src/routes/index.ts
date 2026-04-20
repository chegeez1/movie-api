import { Router, type IRouter } from "express";
import healthRouter from "./health.js";
import v2Router from "./v2/index.js";

const router: IRouter = Router();

router.use(healthRouter);
router.use(v2Router);

export default router;
