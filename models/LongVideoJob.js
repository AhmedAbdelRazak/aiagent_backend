const mongoose = require("mongoose");

const longVideoJobSchema = new mongoose.Schema(
	{
		jobId: {
			type: String,
			required: true,
			unique: true,
			index: true,
			trim: true,
		},
		user: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			default: null,
		},
		status: {
			type: String,
			enum: ["queued", "running", "completed", "failed"],
			default: "queued",
			index: true,
		},
		progressPct: {
			type: Number,
			default: 0,
			min: 0,
			max: 100,
		},
		topic: {
			type: String,
			trim: true,
			default: "",
		},
		finalVideoUrl: {
			type: String,
			trim: true,
			default: "",
		},
		error: {
			type: String,
			trim: true,
			default: "",
		},
		controllerLabel: {
			type: String,
			trim: true,
			default: "",
		},
		statusUrl: {
			type: String,
			trim: true,
			default: "",
		},
		requestSummary: {
			type: mongoose.Schema.Types.Mixed,
			default: null,
		},
		meta: {
			type: mongoose.Schema.Types.Mixed,
			default: {},
		},
		youtubeLink: {
			type: String,
			trim: true,
			default: "",
		},
		videoId: {
			type: String,
			trim: true,
			default: "",
		},
		startedAt: {
			type: Date,
			default: null,
		},
		completedAt: {
			type: Date,
			default: null,
		},
		failedAt: {
			type: Date,
			default: null,
		},
	},
	{
		timestamps: true,
		minimize: false,
	}
);

longVideoJobSchema.index({ status: 1, updatedAt: -1 });

module.exports = mongoose.model("LongVideoJob", longVideoJobSchema);
