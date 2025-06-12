// middlewares/roleMiddleware.js

/**
 * Usage:
 *   authorize("admin")             → only admins can proceed
 *   authorize("user", "admin")     → either users or admins
 */
exports.authorize = (...roles) => {
	return (req, res, next) => {
		if (!req.user) {
			return res.status(401).json({ error: "Not authenticated" });
		}
		if (!roles.includes(req.user.role)) {
			return res
				.status(403)
				.json({ error: `Role (${req.user.role}) not allowed` });
		}
		next();
	};
};
