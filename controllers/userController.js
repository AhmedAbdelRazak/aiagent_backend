// controllers/userController.js
const User = require("../models/User");
const Subscription = require("../models/Subscription");
const { sendEmail } = require("../utils/sendEmail");
const { subscriptionConfirmationTemplate } = require("../utils/emailTemplates");
const {
	USER_SAFE_SELECT,
	sanitizeUserForClient,
	pickAllowedFields,
} = require("../utils/security");

function normalizeEmail(email = "") {
	return String(email || "").trim().toLowerCase();
}

function normalizeProfileImage(image = {}) {
	if (!image || typeof image !== "object") return {};
	return {
		public_id: String(image.public_id || "").slice(0, 200),
		url: String(image.url || "").slice(0, 1000),
	};
}

// Get own profile
exports.getProfile = async (req, res, next) => {
	try {
		const user = await User.findById(req.user._id).select(USER_SAFE_SELECT);
		res.json({
			success: true,
			data: sanitizeUserForClient(user, { includeYouTubeStatus: true }),
		});
	} catch (err) {
		next(err);
	}
};

// Update own profile (including adding/updating platform tokens)
exports.updateProfile = async (req, res, next) => {
	try {
		const updates = pickAllowedFields(req.body, [
			"name",
			"email",
			"platforms",
			"profileImage",
			"acceptedTermsAndConditions",
		]);
		if (updates.email) {
			updates.email = normalizeEmail(updates.email);
			const existing = await User.findOne({
				email: updates.email,
				_id: { $ne: req.user._id },
			});
			if (existing) {
				return res.status(400).json({ error: "Email is already registered." });
			}
		}
		if (updates.profileImage) {
			updates.profileImage = normalizeProfileImage(updates.profileImage);
		}
		const user = await User.findByIdAndUpdate(req.user._id, updates, {
			new: true,
			runValidators: true,
		}).select(USER_SAFE_SELECT);
		res.json({
			success: true,
			data: sanitizeUserForClient(user, { includeYouTubeStatus: true }),
		});
	} catch (err) {
		next(err);
	}
};

// Admin-only: get all users
exports.getAllUsers = async (req, res, next) => {
	try {
		const users = await User.find().select(USER_SAFE_SELECT);
		res.json({ success: true, data: users.map((u) => sanitizeUserForClient(u)) });
	} catch (err) {
		next(err);
	}
};

// Admin-only: change a user’s role
exports.changeUserRole = async (req, res, next) => {
	try {
		const { userId, role } = req.body;
		if (!["user", "admin"].includes(role)) {
			return res.status(400).json({ error: "Invalid role" });
		}
		const user = await User.findByIdAndUpdate(
			userId,
			{ role },
			{ new: true }
		).select(USER_SAFE_SELECT);
		res.json({ success: true, data: sanitizeUserForClient(user) });
	} catch (err) {
		next(err);
	}
};

// Subscribe to a plan
exports.subscribePlan = async (req, res, next) => {
	try {
		const { planId } = req.body;
		const plan = await Subscription.findById(planId);
		if (!plan) {
			return res.status(404).json({ error: "Plan not found" });
		}
		// Compute expiry (now + durationDays)
		const expiryDate = new Date();
		expiryDate.setDate(expiryDate.getDate() + plan.durationDays);

		req.user.subscription = plan._id;
		await req.user.save();

		// Send confirmation email
		const html = subscriptionConfirmationTemplate(
			plan.name,
			expiryDate.toDateString()
		);
		sendEmail({
			to: req.user.email,
			subject: "Subscription Activated",
			html,
		});

		res.json({
			success: true,
			data: {
				plan: plan.name,
				expiresAt: expiryDate,
			},
		});
	} catch (err) {
		next(err);
	}
};
