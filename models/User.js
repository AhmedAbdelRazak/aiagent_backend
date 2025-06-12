// models/User.js
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");

const userSchema = new mongoose.Schema(
	{
		name: {
			type: String,
			trim: true,
			required: [true, "Name is required"],
		},
		email: {
			type: String,
			trim: true,
			unique: true,
			lowercase: true,
			required: [true, "Email is required"],
		},
		profileImage: {
			type: Object,
			default: {
				public_id: "",
				url: "",
			},
		},
		password: {
			type: String,
			required: [true, "Password is required"],
		},
		role: {
			type: String,
			enum: ["user", "admin"],
			default: "user",
		},
		platforms: [
			{
				type: String,
				enum: ["youtube", "facebook", "instagram"],
			},
		],

		// YouTube OAuth fields:
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
		resetPasswordToken: String,
		resetPasswordExpires: Date,
		subscription: {
			type: mongoose.Schema.Types.ObjectId,
			ref: "Subscription",
		},

		activeUser: {
			type: Boolean,
			default: true,
		},
	},
	{ timestamps: true }
);

userSchema.pre("save", async function (next) {
	if (!this.isModified("password")) return next();
	const salt = await bcrypt.genSalt(10);
	this.password = await bcrypt.hash(this.password, salt);
	next();
});

userSchema.methods.comparePassword = async function (candidate) {
	return bcrypt.compare(candidate, this.password);
};

module.exports = mongoose.model("User", userSchema);
