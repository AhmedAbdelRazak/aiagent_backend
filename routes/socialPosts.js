// routes/socialPosts.js
const express = require("express");
const router = express.Router();
const {
	createPost,
	getUserPosts,
	getPostById,
	updatePost,
	deletePost,
} = require("../controllers/socialPostController");
const { protect } = require("../middlewares/authMiddleware");

// @route   POST /api/social-posts
router.post("/social-posts", protect, createPost);

// @route   GET /api/social-posts
router.get("/social-posts", protect, getUserPosts);

// @route   GET /api/social-posts/:postId
router.get("/social-posts/:postId", protect, getPostById);

// @route   PUT /api/social-posts/:postId
router.put("/social-posts/:postId", protect, updatePost);

// @route   DELETE /api/social-posts/:postId
router.delete("/social-posts/:postId", protect, deletePost);

module.exports = router;
