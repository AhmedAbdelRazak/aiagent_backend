// routes/adminMetrics.js
const express = require("express");
const router = express.Router();
const { protect } = require("../middlewares/authMiddleware");
const { authorize } = require("../middlewares/roleMiddleware");

const User = require("../models/User");
const Video = require("../models/Video");
const Subscription = require("../models/Subscription");

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
					createdAt: { $gte: new Date(Date.now() - 864e5) },
				}),
			]);
			res.json({ users, subscriptions: subs, videosToday });
		} catch (err) {
			next(err);
		}
	}
);

module.exports = router;
