// models/PostSchedule.js
const mongoose = require("mongoose");

const postScheduleSchema = new mongoose.Schema(
	{
		user: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			required: true,
		},
		post: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "SocialPost",
			required: true,
		},
		scheduleType: {
			type: String,
			enum: ["daily", "weekly", "monthly"],
			required: true,
		},
		timeOfDay: {
			type: String, // format "HH:mm" (24-hour). e.g. "14:30"
			required: true,
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

module.exports = mongoose.model("PostSchedule", postScheduleSchema);
