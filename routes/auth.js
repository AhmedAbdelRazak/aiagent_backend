// routes/auth.js
const express = require("express");
const router = express.Router();
const {
	register,
	login,
	forgotPassword,
	resetPassword,
	profile, // <— import the new controller
} = require("../controllers/authController");
const { requireSignin } = require("../middlewares/authMiddleware");

// @route   POST /api/auth/register
router.post("/auth/register", register);

// @route   POST /api/auth/login
router.post("/auth/login", login);

// @route   POST /api/auth/forgot-password
router.post("/auth/forgot-password", forgotPassword);

// @route   PUT /api/auth/reset-password/:token
router.put("/auth/reset-password/:token", resetPassword);

// ─── Add this line to expose “who‐am‐I” ───
router.get("/auth/profile", requireSignin, profile);

module.exports = router;
