// models/SupportCase.js
const mongoose = require("mongoose");

const supportCaseSchema = new mongoose.Schema(
	{
		client: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			required: true,
		},
		assignedAdmin: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
		},
		subject: {
			type: String,
			required: [true, "Subject is required"],
			trim: true,
		},
		description: {
			type: String,
			trim: true,
			required: [true, "Description (initial message) is required"],
		},
		status: {
			type: String,
			enum: ["open", "in_progress", "resolved", "closed"],
			default: "open",
		},
		rating: {
			type: Number,
			min: 1,
			max: 5,
		},
		feedback: {
			type: String,
			trim: true,
		},
	},
	{ timestamps: true }
);

module.exports = mongoose.model("SupportCase", supportCaseSchema);
