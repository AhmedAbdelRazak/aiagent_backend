// routes/shortsFromLongs.js
const express = require("express");
const router = express.Router();
const { protect } = require("../middlewares/authMiddleware");
const {
	createShortsFromLong,
	getShortsFromLong,
	listShortsEligibleLongVideos,
} = require("../controllers/shortsGeneratorFromLongs");

// @route   POST /api/long-video/:videoId/shorts
router.post("/long-video/:videoId/shorts", protect, createShortsFromLong);

// @route   GET /api/long-video/:videoId/shorts
router.get("/long-video/:videoId/shorts", protect, getShortsFromLong);

// @route   GET /api/long-video/shorts-eligible
router.get(
	"/long-video/shorts-eligible",
	protect,
	listShortsEligibleLongVideos
);

module.exports = router;
