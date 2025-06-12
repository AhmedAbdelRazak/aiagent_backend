// middlewares/authMiddleware.js

const jwt = require("jsonwebtoken");
const User = require("../models/User");

/**
 * Protect routes by verifying JWT.
 * Adds `req.user = <userDocument>` if valid.
 */
exports.protect = async (req, res, next) => {
	let token = null;
	if (
		req.headers.authorization &&
		req.headers.authorization.startsWith("Bearer")
	) {
		token = req.headers.authorization.split(" ")[1];
	}

	if (!token) {
		return res.status(401).json({ error: "Not authorized, token missing" });
	}

	try {
		const decoded = jwt.verify(token, process.env.JWT_SECRET);
		req.user = await User.findById(decoded.id).select("-password");
		if (!req.user) {
			return res.status(401).json({ error: "User not found" });
		}
		next();
	} catch (err) {
		console.error("authMiddleware error:", err);
		return res
			.status(401)
			.json({ error: "Not authorized, token invalid or expired" });
	}
};

/**
 * (Optional) if you want a simpler middleware just to extract `req.userId`.
 * Not used by video routes above.
 */
exports.requireSignin = (req, res, next) => {
	try {
		const authHeader = req.headers.authorization;
		if (!authHeader) {
			return res.status(401).json({ error: "No token provided" });
		}
		const token = authHeader.split(" ")[1];
		if (!token) {
			return res.status(401).json({ error: "Token missing" });
		}
		const decoded = jwt.verify(token, process.env.JWT_SECRET);
		req.userId = decoded.id;
		next();
	} catch (err) {
		return res.status(401).json({ error: "Invalid or expired token" });
	}
};
