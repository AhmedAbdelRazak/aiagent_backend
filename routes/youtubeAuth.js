// routes/youtubeAuth.js
const express = require("express");
const router = express.Router();
const { google } = require("googleapis");
const User = require("../models/User");
const { protect } = require("../middlewares/authMiddleware");

function createOAuthClient() {
	return new google.auth.OAuth2(
		process.env.YOUTUBE_CLIENT_ID,
		process.env.YOUTUBE_CLIENT_SECRET,
		process.env.YOUTUBE_REDIRECT_URI,
	);
}

router.get("/auth", protect, (req, res) => {
	const oauth2Client = createOAuthClient();
	const scopes = [
		"https://www.googleapis.com/auth/youtube.upload",
		"https://www.googleapis.com/auth/userinfo.email",
	];

	const url = oauth2Client.generateAuthUrl({
		access_type: "offline",
		prompt: "consent",
		include_granted_scopes: false,
		scope: scopes,
	});

	res.redirect(url);
});

router.get("/callback", protect, async (req, res, next) => {
	try {
		const oauth2Client = createOAuthClient();
		const { code } = req.query;

		if (!code) {
			return res.status(400).send("Missing authorization code");
		}

		const { tokens } = await oauth2Client.getToken(code);
		oauth2Client.setCredentials(tokens);
		const oauth2 = google.oauth2({ auth: oauth2Client, version: "v2" });
		const userinfoRes = await oauth2.userinfo.get();
		const youtubeEmail = String(userinfoRes.data.email || "")
			.trim()
			.toLowerCase();

		const user = await User.findById(req.user._id);
		user.youtubeAccessToken = tokens.access_token;
		user.youtubeRefreshToken = tokens.refresh_token;
		user.youtubeTokenExpiresAt = tokens.expiry_date;
		user.youtubeEmail = youtubeEmail;
		await user.save();

		console.log("[youtube/callback] tokens saved", {
			userId: String(user._id),
			youtubeEmail,
			hasRefreshToken: Boolean(user.youtubeRefreshToken),
		});

		res.redirect(`${process.env.CLIENT_URL}/admin/new-video?youtubeConnected=1`);
	} catch (err) {
		next(err);
	}
});

module.exports = router;
