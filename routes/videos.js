// routes/videos.js
const express = require("express");
const router = express.Router();
const {
	createVideo,
	listVideos,
	getVideoById,
	updateVideo,
	deleteVideo,
} = require("../controllers/videoController");
const {
	createLongVideo,
	getLongVideoStatus,
} = require("../controllers/videoControllerLonger");
const { protect } = require("../middlewares/authMiddleware");
const { authorize } = require("../middlewares/roleMiddleware");
// const { createVideoSoraPro } = require("../controllers/videoControllerSora");

// @route   POST /api/videos
router.post("/videos", protect, createVideo);

// @route   GET /api/videos
router.get("/videos", protect, listVideos);

// @route   GET /api/videos/:videoId
router.get("/videos/:videoId", protect, getVideoById);

// @route   PUT /api/videos/:videoId
router.put("/videos/:videoId", protect, updateVideo);

// @route   DELETE /api/videos/:videoId
router.delete("/videos/:videoId", protect, deleteVideo);

// @route   POST /api/long-video
router.post("/long-video", protect, createLongVideo);

// @route   GET /api/long-video/:jobId
router.get("/long-video/:jobId", protect, getLongVideoStatus);

module.exports = router;
