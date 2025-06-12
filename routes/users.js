// routes/users.js
const express = require("express");
const router = express.Router();
const {
	getProfile,
	updateProfile,
	getAllUsers,
	changeUserRole,
	subscribePlan,
} = require("../controllers/userController");
const { protect } = require("../middlewares/authMiddleware");
const { authorize } = require("../middlewares/roleMiddleware");

// @route   GET /api/users/me
router.get("/users/me", protect, getProfile);

// @route   PUT /api/users/me
router.put("/users/me", protect, updateProfile);

// @route   GET /api/users        (Admin only)
router.get("/users", protect, authorize("admin"), getAllUsers);

// @route   PUT /api/users/role   (Admin only)
router.put("/users/role", protect, authorize("admin"), changeUserRole);

// @route   POST /api/users/subscribe
router.post("/users/subscribe", protect, subscribePlan);

module.exports = router;
