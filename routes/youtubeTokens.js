// routes/youtubeTokens.js
const express = require("express");
const router = express.Router();
const User = require("../models/User");
const { protect } = require("../middlewares/authMiddleware");

/**
 * POST /api/youtube/save-tokens
 * Body: { accessToken, refreshToken, expiresIn, youtubeEmail }
 * Saves the user's YouTube OAuth tokens and email into their User document.
 */
router.post("/save-tokens", protect, async (req, res, next) => {
	try {
		const { accessToken, refreshToken, expiresIn, youtubeEmail } = req.body;

		if (!accessToken) {
			return res.status(400).json({ error: "accessToken is required" });
		}
		if (!youtubeEmail) {
			return res.status(400).json({ error: "youtubeEmail is required" });
		}

		const user = await User.findById(req.user._id);
		user.youtubeAccessToken = accessToken;
		if (refreshToken) {
			user.youtubeRefreshToken = refreshToken;
		}
		if (expiresIn) {
			user.youtubeTokenExpiresAt = new Date(Date.now() + expiresIn * 1000);
		}
		user.youtubeEmail = String(youtubeEmail || "").trim().toLowerCase();

		await user.save();
		console.log("[youtube/save-tokens] tokens saved", {
			userId: String(user._id),
			hasRefreshToken: Boolean(user.youtubeRefreshToken),
		});

		return res.json({ success: true, message: "YouTube tokens saved" });
	} catch (err) {
		next(err);
	}
});

module.exports = router;
