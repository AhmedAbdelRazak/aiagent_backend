const LOOPBACKS = new Set(["127.0.0.1", "::1", "localhost"]);

function isPlainObject(value) {
	return (
		value !== null &&
		typeof value === "object" &&
		(Object.getPrototypeOf(value) === Object.prototype ||
			Object.getPrototypeOf(value) === null)
	);
}

function hasUnsafeMongoKey(value, seen = new Set()) {
	if (!value || typeof value !== "object") return false;
	if (seen.has(value)) return false;
	seen.add(value);

	if (Array.isArray(value)) {
		return value.some((item) => hasUnsafeMongoKey(item, seen));
	}

	if (!isPlainObject(value)) return false;

	for (const key of Object.keys(value)) {
		if (key.startsWith("$") || key.includes(".")) return true;
		if (hasUnsafeMongoKey(value[key], seen)) return true;
	}
	return false;
}

function rejectUnsafeMongoKeys(req, res, next) {
	if (
		hasUnsafeMongoKey(req.body) ||
		hasUnsafeMongoKey(req.query) ||
		hasUnsafeMongoKey(req.params)
	) {
		return res.status(400).json({ error: "Invalid request payload." });
	}
	return next();
}

function normalizeRemoteAddress(value = "") {
	return String(value || "")
		.trim()
		.replace(/^::ffff:/, "")
		.replace(/^\[|\]$/g, "")
		.toLowerCase();
}

function isLocalRequest(req) {
	const hasForwardedFor = Boolean(String(req.get?.("x-forwarded-for") || "").trim());
	const candidates = (hasForwardedFor
		? [req.ip]
		: [req.ip, req.socket?.remoteAddress, req.connection?.remoteAddress]
	).map(normalizeRemoteAddress);
	return candidates.some((addr) => LOOPBACKS.has(addr));
}

function timingSafeEqualString(a = "", b = "") {
	const left = Buffer.from(String(a));
	const right = Buffer.from(String(b));
	if (left.length !== right.length) return false;
	return require("crypto").timingSafeEqual(left, right);
}

function requireLocalOrInternalKey(req, res, next) {
	if (isLocalRequest(req)) return next();

	const configuredKey = String(process.env.TRENDS_INTERNAL_API_KEY || "").trim();
	const providedKey = String(req.get("x-agentai-internal-key") || "").trim();
	if (configuredKey && timingSafeEqualString(providedKey, configuredKey)) {
		return next();
	}

	const allowPublic =
		process.env.NODE_ENV !== "production" &&
		["1", "true", "yes", "on"].includes(
			String(process.env.TRENDS_PUBLIC_ACCESS || "true").toLowerCase(),
		);
	if (allowPublic) return next();

	return res.status(403).json({ error: "Internal endpoint only." });
}

module.exports = {
	rejectUnsafeMongoKeys,
	requireLocalOrInternalKey,
};
