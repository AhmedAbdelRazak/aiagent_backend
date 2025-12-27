// models/Video.js

const mongoose = require("mongoose");

const videoSchema = new mongoose.Schema(
	{
		user: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			required: true,
		},

		/** ── SEO / Metadata fields ── **/
		category: {
			type: String,
			required: true,
		},
		topic: {
			type: String,
			trim: true,
			// no longer required because we’ll autofill if missing
		},
		topics: [
			{
				type: String,
				trim: true,
			},
		],
		isLongVideo: {
			type: Boolean,
			default: false,
		},

		seoTitle: {
			type: String,
			trim: true,
			required: true,
		},
		seoDescription: {
			type: String,
			trim: true,
			required: true,
		},
		tags: [
			{
				type: String,
				trim: true,
			},
		],

		/** ── User‐visible/UI fields ── **/
		localFilePath: {
			type: String,
			trim: true,
		},
		youtubeLink: {
			type: String,
			trim: true,
		}, // final YouTube watch URL

		youtubeAccessToken: {
			type: String,
		},
		youtubeRefreshToken: {
			type: String,
		},
		youtubeTokenExpiresAt: {
			type: Date,
		},
		youtubeEmail: {
			type: String,
			lowercase: true,
			trim: true,
		},

		// Facebook & Instagram tokens
		facebookToken: {
			type: String,
		},
		instagramToken: {
			type: String,
		},

		// RunwayML token
		runwaymlToken: {
			type: String,
		},

		privacy: {
			type: String,
			enum: ["public", "private", "unlisted"],
			default: "private",
		},
		script: {
			type: String,
			trim: true,
			required: true, // full on‐screen script / narration
		},
		scheduled: {
			type: Boolean,
			default: false,
		},

		/** ── Runway‐specific fields ── **/
		runwayTaskId: {
			type: String,
			trim: true,
		}, // the Runway task UUID
		promptImage: {
			type: String,
			trim: true,
		}, // URL of generated intermediate image
		promptText: {
			type: String,
			trim: true,
		}, // the prompt for image_to_video (≤160 chars)
		ratio: {
			type: String,
			enum: [
				"1280:720",
				"720:1280",
				"1104:832",
				"832:1104",
				"960:960",
				"1584:672",
			],
			default: "720:1280",
		},
		duration: {
			type: Number,
			enum: [
				5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90,
				120, 180, 240, 300,
			],
			default: 10,
		},
		model: {
			type: String,
			default: "gen4_turbo",
		},
		useSora: {
			type: Boolean,
			default: false,
		},
		seed: {
			type: Number,
		},
		status: {
			type: String,
			enum: ["PENDING", "SUCCEEDED", "FAILED"],
			default: "PENDING",
		},
		outputUrl: {
			type: String,
			trim: true,
		}, // same as promptVideo if you choose to store it

		/** ── (Optional analytics fields) ── **/
		views: {
			type: Number,
			default: 0,
		},
		likes: {
			type: Number,
			default: 0,
		},
		commentsCount: {
			type: Number,
			default: 0,
		},

		language: {
			type: String,
			trim: true,
			default: "English",
		},
		country: {
			type: String,
			trim: true,
			default: "all countries",
		},

		videoRatedByUser: {
			type: Number,
		},
		videoImage: {
			type: Object,
			default: {
				public_id: "",
				url: "",
			},
		},
		backgroundMusic: {
			type: Object,
		},
		elevenLabsVoice: {
			type: Object,
		},
	},
	{ timestamps: true }
);

module.exports = mongoose.model("Video", videoSchema);
