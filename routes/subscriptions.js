// routes/subscriptions.js
const express = require("express");
const router = express.Router();
const {
	createPlan,
	getPlans,
	getPlanById,
	updatePlan,
	deletePlan,
} = require("../controllers/subscriptionController");
const { protect } = require("../middlewares/authMiddleware");
const { authorize } = require("../middlewares/roleMiddleware");

// @route   GET /api/subscriptions            (any authenticated user)
router.get("/subscriptions", protect, getPlans);

// @route   GET /api/subscriptions/:planId
router.get("/subscriptions/:planId", protect, getPlanById);

// @route   POST /api/subscriptions          (admin only)
router.post("/subscriptions", protect, authorize("admin"), createPlan);

// @route   PUT /api/subscriptions/:planId   (admin only)
router.put("/subscriptions/:planId", protect, authorize("admin"), updatePlan);

// @route   DELETE /api/subscriptions/:planId (admin only)
router.delete(
	"/subscriptions/:planId",
	protect,
	authorize("admin"),
	deletePlan
);

module.exports = router;
