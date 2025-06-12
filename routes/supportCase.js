// routes/supportCase.js
const express = require("express");
const router = express.Router();
const {
	createCase,
	getOpenCases,
	assignCase,
	getCaseById,
	closeCase,
} = require("../controllers/supportCaseController");
const { protect } = require("../middlewares/authMiddleware");
const { authorize } = require("../middlewares/roleMiddleware");

// @route   POST /api/support/cases
// Client opens a new case
router.post("/support/cases", protect, createCase);

// @route   GET /api/support/cases/open
// Admin only: get all open cases
router.get("/support/cases/open", protect, authorize("admin"), getOpenCases);

// @route   PUT /api/support/cases/:caseId/assign
// Admin only: assign themselves to a case
router.put(
	"/support/cases/:caseId/assign",
	protect,
	authorize("admin"),
	assignCase
);

// @route   GET /api/support/cases/:caseId
// Client or assignedAdmin or any admin can view
router.get("/support/cases/:caseId", protect, getCaseById);

// @route   PUT /api/support/cases/:caseId/close
// Admin marks “resolved” (no body), or client supplies { rating, feedback } to mark “closed”
router.put("/support/cases/:caseId/close", protect, closeCase);

module.exports = router;
