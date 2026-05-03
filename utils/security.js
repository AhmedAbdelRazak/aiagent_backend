const SENSITIVE_USER_FIELDS = [
	"password",
	"resetPasswordToken",
	"resetPasswordExpires",
	"youtubeAccessToken",
	"youtubeRefreshToken",
	"facebookToken",
	"instagramToken",
	"runwaymlToken",
	"__v",
];

const SENSITIVE_VIDEO_FIELDS = [
	"youtubeAccessToken",
	"youtubeRefreshToken",
	"facebookToken",
	"instagramToken",
	"runwaymlToken",
	"__v",
];

const USER_SAFE_SELECT = SENSITIVE_USER_FIELDS.map((field) => `-${field}`).join(
	" ",
);
const VIDEO_SAFE_SELECT = SENSITIVE_VIDEO_FIELDS.map((field) => `-${field}`).join(
	" ",
);

function toPlain(value) {
	if (!value) return value;
	if (typeof value.toObject === "function") {
		return value.toObject({ getters: false, virtuals: false });
	}
	return { ...value };
}

function stripFields(target, fields) {
	if (!target || typeof target !== "object") return target;
	for (const field of fields) {
		delete target[field];
	}
	return target;
}

function sanitizeUserForClient(user, options = {}) {
	const data = toPlain(user);
	if (!data || typeof data !== "object") return data;
	const youtubeConnected = Boolean(data.youtubeRefreshToken);
	stripFields(data, SENSITIVE_USER_FIELDS);
	if (options.includeYouTubeStatus) {
		data.youtubeConnected = youtubeConnected;
		data.youtubeEmail = data.youtubeEmail || null;
		data.youtubeTokenExpiresAt = data.youtubeTokenExpiresAt || null;
	}
	return data;
}

function sanitizeVideoForClient(video) {
	const data = toPlain(video);
	if (!data || typeof data !== "object") return data;
	stripFields(data, SENSITIVE_VIDEO_FIELDS);
	if (data.user && typeof data.user === "object") {
		data.user = sanitizeUserForClient(data.user);
	}
	return data;
}

function sanitizeVideosForClient(videos = []) {
	return (Array.isArray(videos) ? videos : []).map(sanitizeVideoForClient);
}

function pickAllowedFields(source = {}, allowed = []) {
	const out = {};
	for (const key of allowed) {
		if (Object.prototype.hasOwnProperty.call(source, key)) {
			out[key] = source[key];
		}
	}
	return out;
}

module.exports = {
	SENSITIVE_USER_FIELDS,
	SENSITIVE_VIDEO_FIELDS,
	USER_SAFE_SELECT,
	VIDEO_SAFE_SELECT,
	sanitizeUserForClient,
	sanitizeVideoForClient,
	sanitizeVideosForClient,
	pickAllowedFields,
};
