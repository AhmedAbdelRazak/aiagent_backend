// controllers/socialPostController.js
const SocialPost = require("../models/SocialPost");

// Create a new social post (metadata only; actual file upload handled by front end)
exports.createPost = async (req, res, next) => {
	try {
		const {
			platform,
			title,
			description,
			tags = [],
			script,
			localFileUrl,
			privacy,
		} = req.body;

		if (!platform || !title) {
			return res.status(400).json({ error: "Platform and title are required" });
		}
		// Verify user intended platforms
		if (!req.user.platforms.includes(platform)) {
			return res
				.status(400)
				.json({ error: `You are not authorized to post on ${platform}` });
		}

		// Create the SocialPost document
		const post = new SocialPost({
			user: req.user._id,
			platform,
			title,
			description,
			tags,
			script,
			localFileUrl,
			privacy,
		});
		await post.save();
		res.status(201).json({ success: true, data: post });
	} catch (err) {
		next(err);
	}
};

// Get all posts belonging to the logged-in user
exports.getUserPosts = async (req, res, next) => {
	try {
		const posts = await SocialPost.find({ user: req.user._id });
		res.json({ success: true, data: posts });
	} catch (err) {
		next(err);
	}
};

// Get a single post by ID (only owner or admin)
exports.getPostById = async (req, res, next) => {
	try {
		const post = await SocialPost.findById(req.params.postId);
		if (!post) {
			return res.status(404).json({ error: "Post not found" });
		}
		if (
			post.user.toString() !== req.user._id.toString() &&
			req.user.role !== "admin"
		) {
			return res.status(403).json({ error: "Not authorized" });
		}
		res.json({ success: true, data: post });
	} catch (err) {
		next(err);
	}
};

// Update a post (only owner or admin)
exports.updatePost = async (req, res, next) => {
	try {
		const post = await SocialPost.findById(req.params.postId);
		if (!post) {
			return res.status(404).json({ error: "Post not found" });
		}
		if (
			post.user.toString() !== req.user._id.toString() &&
			req.user.role !== "admin"
		) {
			return res.status(403).json({ error: "Not authorized" });
		}

		const updates = { ...req.body };
		// Only certain fields can be updated; e.g. title, description, tags, script, privacy, localFileUrl
		const allowed = [
			"title",
			"description",
			"tags",
			"script",
			"privacy",
			"localFileUrl",
		];
		allowed.forEach((field) => {
			if (updates[field] !== undefined) post[field] = updates[field];
		});
		await post.save();
		res.json({ success: true, data: post });
	} catch (err) {
		next(err);
	}
};

// Delete a post (only owner or admin)
exports.deletePost = async (req, res, next) => {
	try {
		const post = await SocialPost.findById(req.params.postId);
		if (!post) {
			return res.status(404).json({ error: "Post not found" });
		}
		if (
			post.user.toString() !== req.user._id.toString() &&
			req.user.role !== "admin"
		) {
			return res.status(403).json({ error: "Not authorized" });
		}
		await post.remove();
		res.json({ success: true, message: "Post deleted" });
	} catch (err) {
		next(err);
	}
};
