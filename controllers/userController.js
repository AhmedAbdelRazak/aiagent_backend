// controllers/userController.js
const User = require("../models/User");
const Subscription = require("../models/Subscription");
const { sendEmail } = require("../utils/sendEmail");
const { subscriptionConfirmationTemplate } = require("../utils/emailTemplates");

// Get own profile
exports.getProfile = async (req, res, next) => {
	try {
		const user = await User.findById(req.user._id).select("-password");
		res.json({ success: true, data: user });
	} catch (err) {
		next(err);
	}
};

// Update own profile (including adding/updating platform tokens)
exports.updateProfile = async (req, res, next) => {
	try {
		const updates = { ...req.body };
		if (updates.password) delete updates.password; // not handled here
		const user = await User.findByIdAndUpdate(req.user._id, updates, {
			new: true,
			runValidators: true,
		}).select("-password");
		res.json({ success: true, data: user });
	} catch (err) {
		next(err);
	}
};

// Admin-only: get all users
exports.getAllUsers = async (req, res, next) => {
	try {
		const users = await User.find().select("-password");
		res.json({ success: true, data: users });
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
		).select("-password");
		res.json({ success: true, data: user });
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
