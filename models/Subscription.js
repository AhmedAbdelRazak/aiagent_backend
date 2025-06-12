// models/Subscription.js
const mongoose = require("mongoose");

const subscriptionSchema = new mongoose.Schema(
	{
		name: {
			type: String,
			required: [true, "Plan name is required"],
			trim: true,
		},
		price: {
			type: Number,
			required: [true, "Price is required"],
		},
		features: [
			{
				type: String,
			},
		],
		durationDays: {
			type: Number,
			required: [true, "Duration (in days) is required"],
		},
	},
	{ timestamps: true }
);

module.exports = mongoose.model("Subscription", subscriptionSchema);
