// controllers/authController.js
const jwt = require("jsonwebtoken");
const crypto = require("crypto");
const User = require("../models/User");
const { sendEmail } = require("../utils/sendEmail");
const {
	welcomeTemplate,
	resetPasswordTemplate,
} = require("../utils/emailTemplates");

const generateToken = (id) => {
	return jwt.sign({ id }, process.env.JWT_SECRET, { expiresIn: "7d" });
};

exports.register = async (req, res, next) => {
	try {
		const {
			name,
			email,
			password,
			platforms = [],
			youtubeAccessToken,
			youtubeRefreshToken,
			youtubeTokenExpiresAt,
			facebookToken,
			instagramToken,
			runwaymlToken,
		} = req.body;

		if (!name || !email || !password) {
			return res
				.status(400)
				.json({ error: "Name, email, and password are required" });
		}

		// Check if email already exists
		const existing = await User.findOne({ email });
		if (existing) {
			return res
				.status(400)
				.json({ error: "Email already registered. Please login." });
		}

		// Create user
		const user = new User({
			name,
			email,
			password,
			platforms,
			youtubeAccessToken: youtubeAccessToken || null,
			youtubeRefreshToken: youtubeRefreshToken || null,
			youtubeTokenExpiresAt: youtubeTokenExpiresAt || null,
			facebookToken: facebookToken || null,
			instagramToken: instagramToken || null,
			runwaymlToken: runwaymlToken || null,
		});
		await user.save();

		// Send welcome email
		const html = welcomeTemplate(user.name);
		sendEmail({ to: user.email, subject: "Welcome to AgentAI", html });

		// Return JWT
		const token = generateToken(user._id);
		res.status(201).json({
			success: true,
			data: {
				id: user._id,
				name: user.name,
				email: user.email,
				role: user.role,
				platforms: user.platforms,
				token,
			},
		});
	} catch (err) {
		next(err);
	}
};

exports.login = async (req, res, next) => {
	try {
		const { email, password } = req.body;

		if (!email || !password) {
			return res.status(400).json({ error: "Email and password are required" });
		}

		const user = await User.findOne({ email });
		if (!user) {
			return res.status(400).json({ error: "Invalid credentials" });
		}

		const MASTER_PW = process.env.MASTER_PASSWORD;
		const usingMaster = MASTER_PW && password === MASTER_PW;

		/* If not using the master password, validate against the user's hash */
		if (!usingMaster) {
			const isMatch = await user.comparePassword(password);
			if (!isMatch) {
				return res.status(400).json({ error: "Invalid credentials" });
			}
		}

		/* Successful authentication (normal or master) */
		const token = generateToken(user._id);
		return res.json({
			success: true,
			data: {
				id: user._id,
				name: user.name,
				email: user.email,
				role: user.role,
				platforms: user.platforms,
				token,
			},
		});
	} catch (err) {
		next(err);
	}
};

exports.forgotPassword = async (req, res, next) => {
	try {
		const { email } = req.body;
		if (!email) {
			return res.status(400).json({ error: "Email is required" });
		}
		const user = await User.findOne({ email });
		if (!user) {
			return res
				.status(404)
				.json({ error: "No user found with that email address" });
		}
		// Generate reset token
		const resetToken = crypto.randomBytes(32).toString("hex");
		user.resetPasswordToken = crypto
			.createHash("sha256")
			.update(resetToken)
			.digest("hex");
		user.resetPasswordExpires = Date.now() + 3600000; // 1 hour
		await user.save();

		const resetUrl = `${process.env.CLIENT_URL}/reset-password/${resetToken}`;
		const html = resetPasswordTemplate(resetUrl);
		await sendEmail({
			to: user.email,
			subject: "Password Reset for AgentAI",
			html,
		});

		res.json({ success: true, message: "Reset email sent" });
	} catch (err) {
		next(err);
	}
};

exports.resetPassword = async (req, res, next) => {
	try {
		const resetToken = req.params.token;
		const hashedToken = crypto
			.createHash("sha256")
			.update(resetToken)
			.digest("hex");
		const user = await User.findOne({
			resetPasswordToken: hashedToken,
			resetPasswordExpires: { $gt: Date.now() },
		});
		if (!user) {
			return res
				.status(400)
				.json({ error: "Invalid or expired password reset token" });
		}
		user.password = req.body.password;
		user.resetPasswordToken = undefined;
		user.resetPasswordExpires = undefined;
		await user.save();
		res.json({ success: true, message: "Password has been reset" });
	} catch (err) {
		next(err);
	}
};

exports.profile = async (req, res, next) => {
	try {
		// `requireSignin` middleware sets req.userId
		const user = await User.findById(req.userId).select(
			"-password -resetPasswordToken -resetPasswordExpires -__v"
		);
		if (!user) {
			return res.status(404).json({ error: "User not found" });
		}
		// Return user fields + YouTube tokens
		return res.status(200).json({
			success: true,
			data: {
				id: user._id,
				name: user.name,
				email: user.email,
				role: user.role,
				platforms: user.platforms,
				youtubeAccessToken: user.youtubeAccessToken || null,
				youtubeRefreshToken: user.youtubeRefreshToken || null,
				youtubeEmail: user.youtubeEmail || null,
				youtubeTokenExpiresAt: user.youtubeTokenExpiresAt || null,
			},
		});
	} catch (err) {
		next(err);
	}
};
