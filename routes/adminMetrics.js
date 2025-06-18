// routes/adminMetrics.js
const express = require("express");
const router = express.Router();
const { protect } = require("../middlewares/authMiddleware");
const { authorize } = require("../middlewares/roleMiddleware");

const User = require("../models/User");
const Video = require("../models/Video");

// ──────────────────────────────────────────────────────────────
// 1.  Admin‑only overview  →  global numbers
// ──────────────────────────────────────────────────────────────
router.get(
	"/admin/metrics/overview",
	protect,
	authorize("admin"),
	async (req, res, next) => {
		try {
			const [users, subs, videosToday] = await Promise.all([
				User.countDocuments(),
				User.countDocuments({ subscription: { $ne: null } }),
				Video.countDocuments({
					createdAt: { $gte: new Date(Date.now() - 86_400_000) },
				}),
			]);
			res.json({ users, subscriptions: subs, videosToday });
		} catch (err) {
			next(err);
		}
	}
);

// ──────────────────────────────────────────────────────────────
// 2.  User overview  →  metrics for *that* user only
// ──────────────────────────────────────────────────────────────
router.get("/user/metrics/overview", protect, async (req, res, next) => {
	try {
		const userId = req.user._id;

		const [totalVideos, videosToday] = await Promise.all([
			Video.countDocuments({ user: userId }),
			Video.countDocuments({
				user: userId,
				createdAt: { $gte: new Date(Date.now() - 86_400_000) },
			}),
		]);

		const hasSubscription = Boolean(req.user.subscription);

		/* The response mirrors the admin keys but is user‑scoped */
		res.json({
			users: 1, // the signed‑in user himself
			subscriptions: hasSubscription ? 1 : 0,
			videosToday,
			totalVideos, // extra helpful metric
		});
	} catch (err) {
		next(err);
	}
});

module.exports = router;
