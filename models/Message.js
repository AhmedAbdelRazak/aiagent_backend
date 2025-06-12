// models/Message.js
const mongoose = require("mongoose");

const messageSchema = new mongoose.Schema(
	{
		chat: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "Chat",
			required: true,
		},
		sender: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "User",
			required: true,
		},
		content: {
			type: String,
			trim: true,
			required: [true, "Message content is required"],
		},
		viewedBy: [
			{
				type: mongoose.Schema.Types.ObjectId,
				ref: "User",
			},
		],
		attachments: [
			{
				url: String,
				filename: String,
				mimetype: String,
			},
		],
	},
	{ timestamps: true }
);

// By default, mark the sender as having viewed their own message
messageSchema.pre("save", function (next) {
	if (!this.viewedBy || this.viewedBy.length === 0) {
		this.viewedBy = [this.sender];
	}
	next();
});

module.exports = mongoose.model("Message", messageSchema);
