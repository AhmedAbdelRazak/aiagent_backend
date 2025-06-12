// controllers/subscriptionController.js
const Subscription = require("../models/Subscription");

// Admin-only: create a new plan
exports.createPlan = async (req, res, next) => {
	try {
		const { name, price, features = [], durationDays } = req.body;
		const plan = new Subscription({ name, price, features, durationDays });
		await plan.save();
		res.status(201).json({ success: true, data: plan });
	} catch (err) {
		next(err);
	}
};

// Get all plans (public endpoint—any logged-in user can view)
exports.getPlans = async (req, res, next) => {
	try {
		const plans = await Subscription.find();
		res.json({ success: true, data: plans });
	} catch (err) {
		next(err);
	}
};

// Get a single plan by ID
exports.getPlanById = async (req, res, next) => {
	try {
		const plan = await Subscription.findById(req.params.planId);
		if (!plan) {
			return res.status(404).json({ error: "Plan not found" });
		}
		res.json({ success: true, data: plan });
	} catch (err) {
		next(err);
	}
};

// Admin-only: update a plan
exports.updatePlan = async (req, res, next) => {
	try {
		const updates = req.body;
		const plan = await Subscription.findByIdAndUpdate(
			req.params.planId,
			updates,
			{ new: true }
		);
		if (!plan) {
			return res.status(404).json({ error: "Plan not found" });
		}
		res.json({ success: true, data: plan });
	} catch (err) {
		next(err);
	}
};

// Admin-only: delete a plan
exports.deletePlan = async (req, res, next) => {
	try {
		const plan = await Subscription.findById(req.params.planId);
		if (!plan) {
			return res.status(404).json({ error: "Plan not found" });
		}
		await plan.remove();
		res.json({ success: true, message: "Plan deleted" });
	} catch (err) {
		next(err);
	}
};
