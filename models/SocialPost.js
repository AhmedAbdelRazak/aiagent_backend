// models/SocialPost.js
const mongoose = require("mongoose");

const socialPostSchema = new mongoose.Schema(
	{
		user: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			required: true,
		},
		platform: {
			type: String,
			enum: ["youtube", "facebook", "instagram"],
			required: true,
		},
		title: {
			type: String,
			required: [true, "Title is required"],
			trim: true,
		},
		description: {
			type: String,
			trim: true,
		},
		tags: [
			{
				type: String,
			},
		],
		script: {
			type: String,
		},
		// If you host the video on S3, Cloudinary, or the client uploads it somewhere,
		// store that URL here. Otherwise, you can implement file uploads later.
		localFileUrl: {
			type: String,
		},
		// After posting, we’ll store the returned link (YouTube URL, Facebook URL, etc.)
		platformLink: {
			type: String,
		},
		scheduled: {
			type: Boolean,
			default: false,
		},
		privacy: {
			type: String,
			enum: ["public", "private", "friends_only", "unlisted"],
			default: "private",
		},
	},
	{ timestamps: true }
);

module.exports = mongoose.model("SocialPost", socialPostSchema);
