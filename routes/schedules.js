const express = require("express");
const router = express.Router();
const {
	createSchedule,
	listSchedules, // ⬅️ new
	getScheduleById,
	updateSchedule,
	deleteSchedule,
} = require("../controllers/scheduleController");
const { protect } = require("../middlewares/authMiddleware");

/* create */
router.post("/schedules", protect, createSchedule);

/* paginated list */
router.get("/schedules", protect, listSchedules); // ⬅️ replaces getUserSchedules

/* single‑item actions */
router.get("/schedules/:scheduleId", protect, getScheduleById);
router.put("/schedules/:scheduleId", protect, updateSchedule);
router.delete("/schedules/:scheduleId", protect, deleteSchedule);

module.exports = router;
