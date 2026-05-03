// middlewares/errorHandler.js

exports.errorHandler = (err, req, res, next) => {
	const statusCode =
		res.statusCode && res.statusCode !== 200 ? res.statusCode : 500;
	const isProduction = process.env.NODE_ENV === "production";
	const requestId = req.get("x-request-id") || "";

	console.error("[Error]", {
		statusCode,
		method: req.method,
		path: req.originalUrl,
		requestId,
		message: err?.message || "Server Error",
		stack: isProduction ? undefined : err?.stack,
	});

	res.status(statusCode).json({
		message: isProduction
			? statusCode >= 500
				? "Server Error"
				: err.message || "Request failed"
			: err.message || "Server Error",
		...(isProduction ? {} : { stack: err.stack }),
	});
};
