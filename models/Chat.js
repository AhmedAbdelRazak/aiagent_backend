// models/Chat.js
const mongoose = require("mongoose");

const chatSchema = new mongoose.Schema(
	{
		supportCase: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "SupportCase",
			required: true,
		},
		participants: [
			{
				type: mongoose.Schema.Types.ObjectId,
				ref: "User",
				required: true,
			},
		],
		isGroup: {
			type: Boolean,
			default: false,
		},
		groupName: {
			type: String,
			trim: true,
		},
	},
	{ timestamps: true }
);

module.exports = mongoose.model("Chat", chatSchema);
