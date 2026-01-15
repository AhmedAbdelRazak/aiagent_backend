// models/ShortVideo.js

const mongoose = require("mongoose");

const shortVideoSchema = new mongoose.Schema(
	{
		user: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			required: true,
		},
		longVideo: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "Video",
			required: true,
		},
		clipId: {
			type: String,
			required: true,
			trim: true,
		},
		orderIndex: {
			type: Number,
			default: 0,
		},
		segmentIndex: {
			type: Number,
			default: 0,
		},
		type: {
			type: String,
			trim: true,
			default: "context_needed",
		},
		line: {
			type: String,
			trim: true,
		},
		openLoop: {
			type: Boolean,
			default: false,
		},
		ctaLine: {
			type: String,
			trim: true,
		},
		targetSeconds: {
			type: Number,
			default: 25,
		},
		title: {
			type: String,
			trim: true,
		},
		titleCandidates: [
			{
				type: String,
				trim: true,
			},
		],
		thumbnailTextCandidates: [
			{
				type: String,
				trim: true,
			},
		],
		description: {
			type: String,
			trim: true,
		},
		localPath: {
			type: String,
			trim: true,
		},
		publicUrl: {
			type: String,
			trim: true,
		},
		youtubeLink: {
			type: String,
			trim: true,
		},
		status: {
			type: String,
			enum: ["pending", "ready", "uploaded", "failed"],
			default: "pending",
		},
		lastError: {
			type: String,
			trim: true,
		},
		generatedAt: {
			type: Date,
		},
		uploadedAt: {
			type: Date,
		},
		startSec: {
			type: Number,
			default: 0,
		},
		durationSec: {
			type: Number,
			default: 0,
		},
		fullVideoUrl: {
			type: String,
			trim: true,
		},
	},
	{ timestamps: true }
);

shortVideoSchema.index({ longVideo: 1, clipId: 1 }, { unique: true });

module.exports = mongoose.model("ShortVideo", shortVideoSchema);
