// models/Schedule.js
const mongoose = require("mongoose");

const scheduleSchema = new mongoose.Schema(
	{
		user: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			required: true,
		},
		video: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "Video",
			required: true,
		},
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
