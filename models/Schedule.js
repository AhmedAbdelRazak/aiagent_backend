// models/Schedule.js
const mongoose = require("mongoose");

const scheduleSchema = new mongoose.Schema(
	{
		user: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			required: true,
		},
		// Optional: direct category for this schedule (used when generating new videos per run)
		category: {
			type: String,
		},
		video: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "Video",
		},
		// Optional: array of seed videos to rotate through (new field; keeps backward compatibility)
		videos: [
			{
				type: mongoose.Schema.Types.ObjectId,
				ref: "Video",
			},
		],
		// Deprecated: use category + videos; kept for compatibility
		// required: true removed to allow category-only schedules
		// required: true,
		scheduleType: {
			type: String,
			enum: ["daily", "weekly", "monthly"],
			required: true,
		},
		timeOfDay: {
			type: String,
			required: true, // "HH:mm"
		},
		startDate: {
			type: Date,
			required: true,
		},
		endDate: {
			type: Date, // optional
		},
		nextRun: {
			type: Date,
			required: true,
		},
		active: {
			type: Boolean,
			default: true,
		},
	},
	{ timestamps: true }
);

module.exports = mongoose.model("Schedule", scheduleSchema);
