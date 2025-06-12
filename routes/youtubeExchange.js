// routes/youtubeExchange.js
const express = require("express");
const router = express.Router();
const { google } = require("googleapis");
const User = require("../models/User");
const { protect } = require("../middlewares/authMiddleware");

// Helper: build the same OAuth2 client you used in youtubeAuth.js
function createOAuthClient() {
	return new google.auth.OAuth2(
		process.env.YOUTUBE_CLIENT_ID,
		process.env.YOUTUBE_CLIENT_SECRET,
		process.env.YOUTUBE_REDIRECT_URI // must be "http://localhost:8102/api/youtube/callback"
	);
}

/**
 * POST /api/youtube/exchange-code
 * Body: { code }
 * Exchanges the authorization code for access_token + refresh_token, saves them on the User.
 */
router.post("/exchange-code", protect, async (req, res, next) => {
	try {
		const { code } = req.body;
		if (!code) return res.status(400).json({ error: "code is required" });

		const oauth2Client = createOAuthClient();
		// Exchange code for tokens (this will return both access_token & refresh_token)
		const { tokens } = await oauth2Client.getToken(code);

		// tokens should contain { access_token, refresh_token, expiry_date, ... }
		// If tokens.refresh_token is null, check Google’s "prompt: 'consent'" + "access_type: 'offline'" usage.

		// Fetch userinfo (so we can get the user’s Google email, if you want):
		oauth2Client.setCredentials(tokens);
		const oauth2 = google.oauth2({ auth: oauth2Client, version: "v2" });
		const userInfoRes = await oauth2.userinfo.get();
		const youtubeEmail = userInfoRes.data.email;

		// Save to your User document:
		const user = await User.findById(req.user._id);
		user.youtubeAccessToken = tokens.access_token;
		user.youtubeRefreshToken = tokens.refresh_token; // <— should now be populated
		user.youtubeTokenExpiresAt = tokens.expiry_date;
		user.youtubeEmail = youtubeEmail;
		await user.save();

		return res.json({ success: true, message: "YouTube tokens saved" });
	} catch (err) {
		console.error("Error in /youtube/exchange-code:", err);
		next(err);
	}
});

module.exports = router;
