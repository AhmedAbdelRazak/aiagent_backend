// routes/youtubeAuth.js
const express = require("express");
const router = express.Router();
const { google } = require("googleapis");
const User = require("../models/User");
const { protect } = require("../middlewares/authMiddleware");

// 1) Build an OAuth2 client using our credentials
function createOAuthClient() {
	return new google.auth.OAuth2(
		process.env.YOUTUBE_CLIENT_ID,
		process.env.YOUTUBE_CLIENT_SECRET,
		process.env.YOUTUBE_REDIRECT_URI // e.g. "http://localhost:8102/api/youtube/callback"
	);
}

// 2) GET /api/youtube/auth
//    → Redirect the user into Google’s consent page
router.get("/auth", protect, (req, res) => {
	const oauth2Client = createOAuthClient();

	const scopes = [
		"https://www.googleapis.com/auth/youtube.upload",
		"https://www.googleapis.com/auth/userinfo.email",
	];

	const url = oauth2Client.generateAuthUrl({
		access_type: "offline", // forces Google to return a refresh_token
		prompt: "consent", // forces the account chooser + refresh_token on every consent
		scope: scopes,
	});

	// Redirect the browser to Google’s consent page:
	res.redirect(url);
});

// 3) GET /api/youtube/callback
//    → Google will redirect here with "?code=..."
//    → We exchange the code for tokens, fetch userinfo, store everything in Mongo, then redirect back
router.get("/callback", protect, async (req, res, next) => {
	try {
		const oauth2Client = createOAuthClient();
		const { code } = req.query;

		if (!code) {
			return res.status(400).send("Missing authorization code");
		}

		// 3a) Exchange `code` for `tokens` (includes access_token + refresh_token):
		const { tokens } = await oauth2Client.getToken(code);
		console.log("👉 [youtube/callback] raw tokens from Google:", tokens);
		// tokens might be { access_token, refresh_token, expiry_date, token_type, scope }

		// 3b) Fetch the user’s “email” from Google’s userinfo endpoint:
		oauth2Client.setCredentials(tokens);
		const oauth2 = google.oauth2({ auth: oauth2Client, version: "v2" });
		const userinfoRes = await oauth2.userinfo.get();
		const youtubeEmail = userinfoRes.data.email;
		console.log("👉 [youtube/callback] fetched youtubeEmail:", youtubeEmail);

		// 3c) Save tokens and email on the currently signed-in User document:
		const user = await User.findById(req.user._id);
		console.log(
			"👉 [youtube/callback] before save, user.youtubeRefreshToken:",
			user.youtubeRefreshToken
		);
		user.youtubeAccessToken = tokens.access_token;
		user.youtubeRefreshToken = tokens.refresh_token; // should now be non-null
		user.youtubeTokenExpiresAt = tokens.expiry_date;
		user.youtubeEmail = youtubeEmail;
		await user.save();
		console.log(
			"👉 [youtube/callback] after save, user.youtubeRefreshToken:",
			user.youtubeRefreshToken
		);

		// 3d) Redirect back to front end:
		res.redirect(
			`${process.env.CLIENT_URL}/admin/new-video?youtubeConnected=1`
		);
	} catch (err) {
		next(err);
	}
});

module.exports = router;
