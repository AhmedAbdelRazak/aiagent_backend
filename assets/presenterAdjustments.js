/** @format */

const fs = require("fs");
const path = require("path");
const child_process = require("child_process");
const crypto = require("crypto");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;

let ffmpegPath = "";
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
}

let FormDataNode = null;
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	FormDataNode = require("form-data");
} catch {
	FormDataNode = null;
}

const RUNWAY_API_KEY = process.env.RUNWAYML_API_SECRET || "";
const RUNWAY_VERSION = "2024-11-06";
const RUNWAY_IMAGE_MODEL = "gen4_image";
const RUNWAY_IMAGE_POLL_INTERVAL_MS = 2000;
const RUNWAY_IMAGE_MAX_POLL_ATTEMPTS = 120;
const PRESENTER_LIKENESS_MIN = 0.82;
const PRESENTER_FACE_REGION = { x: 0.3, y: 0.05, w: 0.4, h: 0.55 };
const PRESENTER_EYES_REGION = { x: 0.32, y: 0.08, w: 0.36, h: 0.22 };
const PRESENTER_WARDROBE_REGION = { x: 0.22, y: 0.36, w: 0.56, h: 0.58 };
const CANDLE_WIDTH_PCT = parseNumber(process.env.CANDLE_WIDTH_PCT, 0.14);
const CANDLE_X_PCT = parseNumber(process.env.CANDLE_X_PCT, 0.835);
const CANDLE_Y_PCT = parseNumber(process.env.CANDLE_Y_PCT, 0.724);
const CANDLE_BOTTOM_MARGIN_PX = parseNumber(
	process.env.CANDLE_BOTTOM_MARGIN_PX,
	6
);
const CANDLE_BOTTOM_MARGIN_PCT = parseNumber(
	process.env.CANDLE_BOTTOM_MARGIN_PCT,
	0.08
);
const CANDLE_CLEARANCE_PCT = parseNumber(
	process.env.CANDLE_CLEARANCE_PCT,
	0.06
);
const CANDLE_PREP_ENABLED =
	String(process.env.CANDLE_PREP_ENABLED || "1") !== "0";
const CANDLE_KEY_SIMILARITY = parseNumber(
	process.env.CANDLE_KEY_SIMILARITY,
	0.32
);
const CANDLE_KEY_BLEND = parseNumber(process.env.CANDLE_KEY_BLEND, 0.08);
const CANDLE_KEY_MAX_CORNER_DIFF = parseNumber(
	process.env.CANDLE_KEY_MAX_CORNER_DIFF,
	40
);
const CANDLE_TRIM_TOP_PCT = parseNumber(process.env.CANDLE_TRIM_TOP_PCT, 0.12);
const ENABLE_CANDLE_RESTYLE = 1;
const CANDLE_SHADOW_ENABLED =
	String(process.env.CANDLE_SHADOW_ENABLED || "1") !== "0";
const CANDLE_SHADOW_OPACITY = parseNumber(
	process.env.CANDLE_SHADOW_OPACITY,
	0.28
);
const CANDLE_SHADOW_BLUR = parseNumber(process.env.CANDLE_SHADOW_BLUR, 4);
const CANDLE_SHADOW_X_PX = parseNumber(process.env.CANDLE_SHADOW_X_PX, 4);
const CANDLE_SHADOW_Y_PX = parseNumber(process.env.CANDLE_SHADOW_Y_PX, 5);
const CANDLE_CROP_ALPHA_ENABLED =
	String(process.env.CANDLE_CROP_ALPHA_ENABLED || "1") !== "0";
const CANDLE_CROP_ALPHA_THRESHOLD = parseNumber(
	process.env.CANDLE_CROP_ALPHA_THRESHOLD,
	10
);
const CANDLE_CROP_PAD_PCT = parseNumber(process.env.CANDLE_CROP_PAD_PCT, 0.02);
const DESK_EDGE_SEARCH_X_PCT = parseNumber(
	process.env.DESK_EDGE_SEARCH_X_PCT,
	0.62
);
const DESK_EDGE_SEARCH_Y_START_PCT = parseNumber(
	process.env.DESK_EDGE_SEARCH_Y_START_PCT,
	0.6
);
const DESK_EDGE_SEARCH_Y_END_PCT = parseNumber(
	process.env.DESK_EDGE_SEARCH_Y_END_PCT,
	0.96
);
const DESK_EDGE_EXPECTED_WIDE_PCT = parseNumber(
	process.env.DESK_EDGE_EXPECTED_WIDE_PCT,
	0.78
);
const DESK_EDGE_EXPECTED_TALL_PCT = parseNumber(
	process.env.DESK_EDGE_EXPECTED_TALL_PCT,
	0.89
);
const DESK_EDGE_BAND_PCT = parseNumber(process.env.DESK_EDGE_BAND_PCT, 0.16);
const DESK_EDGE_MIN_PCT = parseNumber(process.env.DESK_EDGE_MIN_PCT, 0.68);
const CANDLE_MIN_PX = parseNumber(process.env.CANDLE_MIN_PX, 90);
const CANDLE_MAX_PX = parseNumber(process.env.CANDLE_MAX_PX, 280);
const PRESENTER_MIN_BYTES = 12000;
const PRESENTER_CLOUDINARY_FOLDER = "aivideomatic/long_presenters";
const PRESENTER_CLOUDINARY_PUBLIC_PREFIX = "presenter_master";

function ensureDir(dirPath) {
	if (!dirPath) return;
	if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
}

function safeUnlink(p) {
	try {
		if (p && fs.existsSync(p)) fs.unlinkSync(p);
	} catch {}
}

function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function parseNumber(value, fallback) {
	const n = Number(value);
	return Number.isFinite(n) ? n : fallback;
}

function clamp(n, lo, hi) {
	const x = Number(n);
	if (!Number.isFinite(x)) return lo;
	return Math.min(hi, Math.max(lo, x));
}

function readFileHeader(filePath, bytes = 12) {
	try {
		const fd = fs.openSync(filePath, "r");
		const buf = Buffer.alloc(bytes);
		const read = fs.readSync(fd, buf, 0, bytes, 0);
		fs.closeSync(fd);
		return buf.slice(0, read);
	} catch {
		return null;
	}
}

function detectFileType(filePath) {
	const head = readFileHeader(filePath, 64);
	if (!head || head.length < 4) return null;

	const ascii4 = head.slice(0, 4).toString("ascii");
	const ascii12 = head.slice(0, 12).toString("ascii");
	const lowerText = head.toString("utf8", 0, 32).trim().toLowerCase();

	if (
		lowerText.startsWith("<!doctype") ||
		lowerText.startsWith("<html") ||
		lowerText.startsWith("<?xml") ||
		lowerText.startsWith("<svg")
	) {
		return { kind: "text", ext: "html" };
	}

	if (
		head[0] === 0x89 &&
		head[1] === 0x50 &&
		head[2] === 0x4e &&
		head[3] === 0x47
	)
		return { kind: "image", ext: "png" };

	if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff)
		return { kind: "image", ext: "jpg" };

	if (ascii4 === "GIF8") return { kind: "image", ext: "gif" };

	if (ascii4 === "RIFF" && ascii12.slice(8, 12) === "WEBP")
		return { kind: "image", ext: "webp" };

	if (ascii12.slice(4, 8) === "ftyp") return { kind: "video", ext: "mp4" };

	return null;
}

function inferImageMime(filePath) {
	const head = readFileHeader(filePath, 12);
	if (head && head.length >= 4) {
		if (
			head[0] === 0x89 &&
			head[1] === 0x50 &&
			head[2] === 0x4e &&
			head[3] === 0x47
		)
			return "image/png";
		if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff)
			return "image/jpeg";
		if (
			head.toString("ascii", 0, 4) === "RIFF" &&
			head.toString("ascii", 8, 12) === "WEBP"
		)
			return "image/webp";
	}
	const ext = path.extname(filePath).toLowerCase();
	if (ext === ".png") return "image/png";
	if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
	if (ext === ".webp") return "image/webp";
	return null;
}

function parsePngSize(buf) {
	if (!buf || buf.length < 24) return null;
	return {
		width: buf.readUInt32BE(16),
		height: buf.readUInt32BE(20),
	};
}

function parseJpegSize(buf) {
	if (!buf || buf.length < 4) return null;
	let offset = 2;
	while (offset < buf.length) {
		if (buf[offset] !== 0xff) {
			offset += 1;
			continue;
		}
		const marker = buf[offset + 1];
		if (
			(marker >= 0xc0 && marker <= 0xc3) ||
			(marker >= 0xc5 && marker <= 0xc7) ||
			(marker >= 0xc9 && marker <= 0xcb) ||
			(marker >= 0xcd && marker <= 0xcf)
		) {
			if (offset + 8 >= buf.length) return null;
			const height = buf.readUInt16BE(offset + 5);
			const width = buf.readUInt16BE(offset + 7);
			return { width, height };
		}
		if (offset + 4 >= buf.length) break;
		const length = buf.readUInt16BE(offset + 2);
		if (!length) break;
		offset += 2 + length;
	}
	return null;
}

function resolveFfprobePath() {
	let ffprobePath = "ffprobe";
	if (ffmpegPath) {
		const candidate = ffmpegPath.replace(/ffmpeg(\.exe)?$/i, "ffprobe$1");
		if (candidate && candidate !== ffmpegPath) ffprobePath = candidate;
	}
	return ffprobePath;
}

function ffprobePixelFormat(filePath) {
	try {
		const ffprobePath = resolveFfprobePath();
		const out = child_process
			.execSync(
				`"${ffprobePath}" -v error -select_streams v:0 -show_entries stream=pix_fmt -of default=nw=1:nk=1 "${filePath}"`,
				{ stdio: ["ignore", "pipe", "ignore"] }
			)
			.toString()
			.trim();
		return out || "";
	} catch {
		return "";
	}
}

function ffprobeDimensions(filePath) {
	try {
		const ffprobePath = resolveFfprobePath();
		const out = child_process
			.execSync(
				`"${ffprobePath}" -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x "${filePath}"`,
				{ stdio: ["ignore", "pipe", "ignore"] }
			)
			.toString()
			.trim();
		const [w, h] = out.split("x").map((n) => Number(n) || 0);
		return { width: w || 0, height: h || 0 };
	} catch {
		return { width: 0, height: 0 };
	}
}

function imageHasAlpha(filePath) {
	const fmt = ffprobePixelFormat(filePath);
	return Boolean(fmt && fmt.includes("a"));
}

function sampleImageKeyColor(filePath) {
	try {
		const buf = runFfmpegBuffer(
			[
				"-hide_banner",
				"-loglevel",
				"error",
				"-i",
				filePath,
				"-vf",
				"scale=2:2:flags=area,format=rgb24",
				"-frames:v",
				"1",
				"-f",
				"rawvideo",
				"pipe:1",
			],
			"candle_key_sample"
		);
		if (!buf || buf.length < 12) return null;
		const corners = [
			{ r: buf[0], g: buf[1], b: buf[2] },
			{ r: buf[3], g: buf[4], b: buf[5] },
			{ r: buf[6], g: buf[7], b: buf[8] },
			{ r: buf[9], g: buf[10], b: buf[11] },
		];

		let best = null;
		let bestDist = Infinity;
		for (let i = 0; i < corners.length; i++) {
			for (let j = i + 1; j < corners.length; j++) {
				const dist = colorDistance(corners[i], corners[j]);
				if (dist < bestDist) {
					bestDist = dist;
					best = {
						r: Math.round((corners[i].r + corners[j].r) / 2),
						g: Math.round((corners[i].g + corners[j].g) / 2),
						b: Math.round((corners[i].b + corners[j].b) / 2),
					};
				}
			}
		}

		if (!best || bestDist > CANDLE_KEY_MAX_CORNER_DIFF) return null;
		return best;
	} catch {
		return null;
	}
}

function rgbToHex({ r, g, b }) {
	const toHex = (v) =>
		clamp(Math.round(v || 0), 0, 255)
			.toString(16)
			.padStart(2, "0");
	return `${toHex(r)}${toHex(g)}${toHex(b)}`;
}

function colorDistance(a, b) {
	return (
		Math.abs((a?.r || 0) - (b?.r || 0)) +
		Math.abs((a?.g || 0) - (b?.g || 0)) +
		Math.abs((a?.b || 0) - (b?.b || 0))
	);
}

function computeAlphaBounds(filePath, { alphaThreshold = 10 } = {}) {
	const dims = probeImageDimensions(filePath);
	if (!dims.width || !dims.height) return null;

	const maxW = 256;
	const outW = Math.min(dims.width, maxW);
	const buf = runFfmpegBuffer(
		[
			"-hide_banner",
			"-loglevel",
			"error",
			"-i",
			filePath,
			"-vf",
			`scale=${outW}:-1:flags=area,format=rgba`,
			"-frames:v",
			"1",
			"-f",
			"rawvideo",
			"pipe:1",
		],
		"alpha_bounds"
	);
	if (!buf || buf.length < outW * 4) return null;

	const outH = Math.floor(buf.length / (outW * 4));
	if (!outH || outH < 2) return null;

	let minX = outW;
	let minY = outH;
	let maxX = -1;
	let maxY = -1;
	const threshold = clamp(alphaThreshold, 1, 250);

	for (let y = 0; y < outH; y++) {
		const row = y * outW * 4;
		for (let x = 0; x < outW; x++) {
			const a = buf[row + x * 4 + 3];
			if (a > threshold) {
				if (x < minX) minX = x;
				if (x > maxX) maxX = x;
				if (y < minY) minY = y;
				if (y > maxY) maxY = y;
			}
		}
	}

	if (maxX < minX || maxY < minY) return null;

	const scaleX = dims.width / outW;
	const scaleY = dims.height / outH;
	const pad = Math.max(
		1,
		Math.round(
			Math.min(dims.width, dims.height) * clamp(CANDLE_CROP_PAD_PCT, 0, 0.08)
		)
	);

	let x = Math.max(0, Math.floor(minX * scaleX) - pad);
	let y = Math.max(0, Math.floor(minY * scaleY) - pad);
	let w = Math.min(dims.width - x, Math.ceil((maxX + 1) * scaleX) - x + pad);
	let h = Math.min(dims.height - y, Math.ceil((maxY + 1) * scaleY) - y + pad);

	w = Math.max(2, Math.min(w, dims.width - x));
	h = Math.max(2, Math.min(h, dims.height - y));

	return { x, y, w, h, outW, outH };
}

function probeImageDimensions(filePath) {
	try {
		const dt = detectFileType(filePath);
		if (!dt || dt.kind !== "image") return { width: 0, height: 0 };
		const head = readFileHeader(filePath, 256 * 1024);
		if (!head) return { width: 0, height: 0 };
		if (dt.ext === "png") {
			const size = parsePngSize(head);
			return size || { width: 0, height: 0 };
		}
		if (dt.ext === "jpg") {
			const size = parseJpegSize(head);
			return size || { width: 0, height: 0 };
		}
		return ffprobeDimensions(filePath);
	} catch {
		return { width: 0, height: 0 };
	}
}

const RUNWAY_IMAGE_RATIOS = [
	"1920:1080",
	"2112:912",
	"1808:768",
	"1680:720",
	"1360:768",
	"1280:720",
	"960:720",
	"720:960",
	"720:720",
	"1080:1080",
	"1024:1024",
	"1080:1920",
	"720:1280",
	"1440:1080",
	"1080:1440",
	"1168:880",
];

function runwayImageRatioForDims(dims) {
	const base =
		dims && dims.width && dims.height ? dims.width / dims.height : 16 / 9;
	let best = RUNWAY_IMAGE_RATIOS[0];
	let bestDiff = Infinity;
	let bestArea = 0;
	for (const candidate of RUNWAY_IMAGE_RATIOS) {
		const [w, h] = String(candidate)
			.split(":")
			.map((v) => Number(v) || 0);
		if (!w || !h) continue;
		const ratio = w / h;
		const diff = Math.abs(ratio - base);
		const area = w * h;
		if (
			diff < bestDiff - 1e-6 ||
			(diff <= bestDiff + 1e-6 && area > bestArea)
		) {
			best = candidate;
			bestDiff = diff;
			bestArea = area;
		}
	}
	return best || "1920:1080";
}

function seedFromText(value) {
	const h = crypto
		.createHash("sha256")
		.update(String(value || ""))
		.digest();
	return h.readUInt32BE(0);
}

function runwayHeadersJson() {
	return {
		Authorization: `Bearer ${RUNWAY_API_KEY}`,
		"X-Runway-Version": RUNWAY_VERSION,
		"Content-Type": "application/json",
	};
}

async function runwayCreateEphemeralUpload({ filePath, filename }) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	if (!fs.existsSync(filePath))
		throw new Error("file missing for runway upload");

	const baseName = filename || path.basename(filePath || "asset.bin");

	const init = await axios.post(
		"https://api.dev.runwayml.com/v1/uploads",
		{ filename: baseName, type: "ephemeral" },
		{
			headers: runwayHeadersJson(),
			timeout: 20000,
			validateStatus: (s) => s < 500,
		}
	);

	if (init.status >= 300) {
		const msg =
			typeof init.data === "string"
				? init.data
				: JSON.stringify(init.data || {});
		throw new Error(
			`Runway upload init failed (${init.status}): ${msg.slice(0, 500)}`
		);
	}

	const { uploadUrl, fields, runwayUri } = init.data || {};
	if (!uploadUrl || !fields || !runwayUri)
		throw new Error("Runway upload init returned incomplete response");

	if (FormDataNode) {
		const form = new FormDataNode();
		Object.entries(fields || {}).forEach(([k, v]) => form.append(k, v));
		form.append("file", fs.createReadStream(filePath));
		const r = await axios.post(uploadUrl, form, {
			headers: form.getHeaders(),
			maxBodyLength: Infinity,
			maxContentLength: Infinity,
			timeout: 30000,
			validateStatus: (s) => s < 500,
		});
		if (r.status >= 300) throw new Error(`Runway upload failed (${r.status})`);
		return runwayUri;
	}

	if (typeof fetch === "function" && typeof FormData !== "undefined") {
		const form = new FormData();
		Object.entries(fields || {}).forEach(([k, v]) => form.append(k, v));
		const buf = fs.readFileSync(filePath);
		const blob = new Blob([buf]);
		form.append("file", blob, baseName);
		const resp = await fetch(uploadUrl, { method: "POST", body: form });
		if (!resp.ok) throw new Error(`Runway upload failed (${resp.status})`);
		return runwayUri;
	}

	throw new Error(
		"Runway upload requires Node 18+ (fetch/FormData) or install 'form-data'"
	);
}

async function pollRunwayTask(taskId, label) {
	const url = `https://api.dev.runwayml.com/v1/tasks/${taskId}`;
	for (let i = 0; i < RUNWAY_IMAGE_MAX_POLL_ATTEMPTS; i++) {
		await sleep(RUNWAY_IMAGE_POLL_INTERVAL_MS);
		const res = await axios.get(url, {
			headers: {
				Authorization: `Bearer ${RUNWAY_API_KEY}`,
				"X-Runway-Version": RUNWAY_VERSION,
			},
			timeout: 20000,
			validateStatus: (s) => s < 500,
		});
		if (res.status >= 300) {
			const msg =
				typeof res.data === "string"
					? res.data
					: JSON.stringify(res.data || {});
			throw new Error(
				`${label} polling failed (${res.status}): ${msg.slice(0, 500)}`
			);
		}
		const data = res.data || {};
		const status = String(data.status || "").toUpperCase();
		if (status === "SUCCEEDED") {
			if (Array.isArray(data.output) && data.output[0]) return data.output[0];
			if (typeof data.output === "string") return data.output;
			throw new Error(`${label} succeeded but returned no output`);
		}
		if (status === "FAILED") {
			throw new Error(
				`${label} failed: ${data.failureCode || data.error || "FAILED"}`
			);
		}
	}
	throw new Error(`${label} timed out`);
}

async function runwayTextToImage({ promptText, ratio, referenceImages, seed }) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	const payload = {
		model: RUNWAY_IMAGE_MODEL,
		promptText: String(promptText || "").slice(0, 1000),
		ratio: String(ratio || "1920:1080"),
		...(Array.isArray(referenceImages) && referenceImages.length
			? { referenceImages }
			: {}),
		...(Number.isFinite(seed) ? { seed } : {}),
	};

	const res = await axios.post(
		"https://api.dev.runwayml.com/v1/text_to_image",
		payload,
		{
			headers: runwayHeadersJson(),
			timeout: 30000,
			validateStatus: (s) => s < 500,
		}
	);

	if (res.status >= 300 || !res.data?.id) {
		const msg =
			typeof res.data === "string" ? res.data : JSON.stringify(res.data || {});
		throw new Error(
			`Runway text_to_image failed (${res.status}): ${msg.slice(0, 700)}`
		);
	}
	return await pollRunwayTask(res.data.id, "runway_text_to_image");
}

async function downloadRunwayImageToPath({ uri, outPath }) {
	if (!uri) throw new Error("runway output missing");
	const target = String(uri);
	if (target.startsWith("data:image/")) {
		const base64 = target.split(",")[1] || "";
		const buf = Buffer.from(base64, "base64");
		fs.writeFileSync(outPath, buf);
		return outPath;
	}
	if (!/^https?:\/\//i.test(target))
		throw new Error(`unsupported runway output uri: ${target.slice(0, 50)}`);
	const res = await axios.get(target, {
		responseType: "arraybuffer",
		timeout: 30000,
		validateStatus: (s) => s < 500,
	});
	if (res.status >= 300)
		throw new Error(`runway output download failed (${res.status})`);
	fs.writeFileSync(outPath, Buffer.from(res.data));
	return outPath;
}

async function createCropFromRegion({ inputPath, regionPct, outPath, label }) {
	const dims = probeImageDimensions(inputPath);
	if (!dims.width || !dims.height) throw new Error("crop_dims_missing");
	const rx = Math.max(0, Math.round(dims.width * (regionPct.x || 0)));
	const ry = Math.max(0, Math.round(dims.height * (regionPct.y || 0)));
	const rw = Math.max(1, Math.round(dims.width * (regionPct.w || 1)));
	const rh = Math.max(1, Math.round(dims.height * (regionPct.h || 1)));
	await runFfmpeg(
		[
			"-i",
			inputPath,
			"-vf",
			`crop=${rw}:${rh}:${rx}:${ry}`,
			"-frames:v",
			"1",
			"-y",
			outPath,
		],
		label || "crop_ref"
	);
	return outPath;
}

function assertCloudinaryReady() {
	if (
		!process.env.CLOUDINARY_CLOUD_NAME ||
		!process.env.CLOUDINARY_API_KEY ||
		!process.env.CLOUDINARY_API_SECRET
	) {
		throw new Error(
			"Cloudinary credentials missing (CLOUDINARY_CLOUD_NAME/API_KEY/API_SECRET)."
		);
	}
	cloudinary.config({
		cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
		api_key: process.env.CLOUDINARY_API_KEY,
		api_secret: process.env.CLOUDINARY_API_SECRET,
	});
}

async function uploadPresenterToCloudinary(filePath, jobId) {
	assertCloudinaryReady();
	const publicId = `${PRESENTER_CLOUDINARY_PUBLIC_PREFIX}_${jobId}_${Date.now()}`;
	const result = await cloudinary.uploader.upload(filePath, {
		public_id: publicId,
		folder: PRESENTER_CLOUDINARY_FOLDER,
		resource_type: "image",
		overwrite: true,
	});
	return {
		public_id: result.public_id,
		url: result.secure_url,
		width: result.width,
		height: result.height,
	};
}

function cleanTopicText(text = "") {
	return String(text || "")
		.replace(/["'`]/g, "")
		.replace(/[^a-z0-9\s]/gi, " ")
		.replace(/\s+/g, " ")
		.trim();
}

function buildTopicFocus({ title, topics = [] }) {
	const topicLine = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join(" / ")
		: "";
	const raw = [title, topicLine].filter(Boolean).join(" | ");
	return cleanTopicText(raw || topicLine || title || "the topic");
}

function inferWardrobeStyle({ categoryLabel = "", text = "" }) {
	const lower = String(text || "").toLowerCase();
	const cat = String(categoryLabel || "").toLowerCase();
	if (
		/(finance|business|money|stock|market|economy)/.test(lower) ||
		/(finance|business)/.test(cat)
	) {
		return "premium button-up shirt (open collar), smart-casual, youthful and lively";
	}
	if (
		/(politic|election|policy|government)/.test(lower) ||
		cat === "politics"
	) {
		return "clean button-up shirt with a modern, smart-casual look (open collar)";
	}
	if (
		/(tech|ai|software|developer|programming|startup)/.test(lower) ||
		cat === "technology"
	) {
		return "stylish patterned or textured button-up shirt, open collar, smart-casual";
	}
	if (
		/(movie|tv|series|show|celebrity|music|album|tour|concert)/.test(lower) ||
		cat === "entertainment"
	) {
		return "fashion-forward button-up shirt, modern smart-casual, open collar";
	}
	return "fancy button-up shirt, youthful smart-casual, open collar";
}

function inferExpressionLine(text = "") {
	const lower = String(text || "").toLowerCase();
	const negative =
		/(death|died|dead|killed|suicide|murder|arrest|charged|trial|lawsuit|injury|accident|crash|sad|tragic|funeral|hospital|scandal)/.test(
			lower
		);
	const positive =
		/(returns|revival|win|wins|won|success|smile|happy|laugh|funny|hilarious|amazing|great|best|top|comeback|surprise)/.test(
			lower
		);
	if (negative) return "Expression: calm, neutral, professional (no smile).";
	if (positive)
		return "Expression: warm, friendly, subtle smile (not exaggerated).";
	return "Expression: calm, professional, subtle friendly smile (not exaggerated).";
}

function buildWardrobeEditPrompt({ title, topics, categoryLabel }) {
	const topicFocus = buildTopicFocus({ title, topics });
	const wardrobe = inferWardrobeStyle({
		categoryLabel,
		text: `${title || ""} ${topicFocus || ""}`,
	});
	const expressionLine = inferExpressionLine(
		`${title || ""} ${topicFocus || ""}`
	);
	return `
Use @presenter_ref for exact framing, lighting, pose, desk, and studio environment.
Use @face_ref for the exact facial identity and features.
Change ONLY the outfit on the torso/upper body area to: ${wardrobe}.
Keep skin tone, arms, hands, body proportions, and posture identical to @presenter_ref.
Face, glasses, beard, hairline, skin texture, eye shape, eye color, eyebrows, ears, and head shape must remain EXACTLY the same.
Studio background, desk, lighting, color grading, camera angle, framing, and all background objects must remain EXACTLY the same.
No tie. No formal suit jacket. Keep it youthful, lively, and fancy-casual.
Do NOT alter facial features, hair, or body proportions. Do NOT alter the studio, desk, or props.
If any non-wardrobe area would change, leave it unchanged.
${expressionLine}
Eyes: natural, forward-looking, aligned; no crossed eyes or odd gaze.
No extra people, no text, no logos, no new props.
`.trim();
}

function runFfmpeg(args, label = "ffmpeg") {
	return new Promise((resolve, reject) => {
		if (!ffmpegPath) return reject(new Error("ffmpeg not available"));
		const proc = child_process.spawn(ffmpegPath, args, {
			stdio: ["ignore", "pipe", "pipe"],
			windowsHide: true,
		});
		let stderr = "";
		proc.stderr.on("data", (d) => (stderr += d.toString()));
		proc.on("error", reject);
		proc.on("close", (code) => {
			if (code === 0) return resolve();
			const head = stderr.slice(0, 4000);
			reject(new Error(`${label} failed (code ${code}): ${head}`));
		});
	});
}

function runFfmpegBuffer(args, label = "ffmpeg_buffer") {
	if (!ffmpegPath) throw new Error("ffmpeg not available");
	const res = child_process.spawnSync(ffmpegPath, args, {
		encoding: null,
		windowsHide: true,
	});
	if (res.status === 0) return res.stdout || Buffer.alloc(0);
	const err = (res.stderr || Buffer.alloc(0)).toString().slice(0, 4000);
	throw new Error(`${label} failed (code ${res.status}): ${err}`);
}

function estimateDeskEdgeY(basePath, baseDims) {
	try {
		if (!ffmpegPath) return null;
		if (!baseDims?.width || !baseDims?.height) return null;

		const outW = 256;
		const buf = runFfmpegBuffer(
			[
				"-hide_banner",
				"-loglevel",
				"error",
				"-i",
				basePath,
				"-vf",
				`scale=${outW}:-1:flags=area,format=gray`,
				"-frames:v",
				"1",
				"-f",
				"rawvideo",
				"pipe:1",
			],
			"desk_edge_gray"
		);

		if (!buf || buf.length < outW * 32) return null;
		const outH = Math.floor(buf.length / outW);
		if (!outH || outH < 32) return null;

		const aspect = baseDims.height / baseDims.width;
		const expectedPct =
			aspect > 0.62 ? DESK_EDGE_EXPECTED_WIDE_PCT : DESK_EDGE_EXPECTED_TALL_PCT;
		const expectedY = Math.floor(outH * expectedPct);
		const band = Math.max(
			2,
			Math.floor(outH * clamp(DESK_EDGE_BAND_PCT, 0.08, 0.22))
		);

		const xStart = Math.floor(outW * clamp(DESK_EDGE_SEARCH_X_PCT, 0.4, 0.85));
		const yStartRaw = Math.floor(
			outH * clamp(DESK_EDGE_SEARCH_Y_START_PCT, 0.45, 0.85)
		);
		const yEndRaw = Math.floor(
			outH * clamp(DESK_EDGE_SEARCH_Y_END_PCT, 0.75, 0.98)
		);

		let yStart = Math.max(yStartRaw, expectedY - band);
		let yEnd = Math.min(yEndRaw, expectedY + band);
		if (yEnd <= yStart + 2) {
			yStart = yStartRaw;
			yEnd = yEndRaw;
		}

		let bestY = Math.floor(outH * 0.82);
		let bestScore = -1;

		for (let y = yStart; y < yEnd - 1; y++) {
			let sum = 0;
			const row = y * outW;
			const row2 = (y + 1) * outW;
			for (let x = xStart; x < outW; x++) {
				sum += Math.abs(buf[row + x] - buf[row2 + x]);
			}
			const score = sum / Math.max(1, outW - xStart);
			if (score > bestScore) {
				bestScore = score;
				bestY = y;
			}
		}

		return Math.round((bestY / outH) * baseDims.height);
	} catch {
		return null;
	}
}

async function generateRunwayCandleImage({ candlePath, tmpDir, jobId, log }) {
	if (!ENABLE_CANDLE_RESTYLE || !RUNWAY_API_KEY) return null;
	if (!candlePath || !fs.existsSync(candlePath)) return null;
	ensureDir(tmpDir);

	const dims = probeImageDimensions(candlePath);
	const ratio = runwayImageRatioForDims(dims);
	const seed = seedFromText(`${jobId || "candle"}_candle`);

	const prompt = `
Use @candle_ref as the exact reference for the brand label, logo, glass, and proportions.
Create a photorealistic product cutout of the same candle.
The jar is OPEN with NO lid or cap visible anywhere in frame.
The candle is LIT with a small clean flame and glowing wick; wax surface visible.
Keep label text/logo EXACTLY the same, sharp, readable, and undistorted.
Single candle only, upright, centered, fully visible (no cropping).
Isolate on a flat pure green (#00FF00) background; no shadows, reflections, or extra objects.
`.trim();

	let refUri = null;
	try {
		refUri = await runwayCreateEphemeralUpload({
			filePath: candlePath,
			filename: path.basename(candlePath),
		});
	} catch (e) {
		if (log)
			log("candle restyle upload failed", { error: e?.message || String(e) });
		return null;
	}

	try {
		const outputUri = await runwayTextToImage({
			promptText: prompt,
			ratio,
			referenceImages: [{ uri: refUri, tag: "candle_ref" }],
			seed,
		});
		const outPath = path.join(
			tmpDir,
			`candle_open_lit_${jobId || crypto.randomUUID()}.png`
		);
		await downloadRunwayImageToPath({ uri: outputUri, outPath });
		const dt = detectFileType(outPath);
		if (dt?.kind === "image") {
			if (log) log("candle restyle ready", { path: path.basename(outPath) });
			return outPath;
		}
		safeUnlink(outPath);
	} catch (e) {
		if (log)
			log("candle restyle failed (using original)", {
				error: e?.message || String(e),
			});
	}

	return null;
}

async function prepareCandleOverlayAsset({ candlePath, tmpDir, jobId, log }) {
	if (!CANDLE_PREP_ENABLED) return candlePath;
	if (!candlePath || !fs.existsSync(candlePath)) return candlePath;

	let workingPath = candlePath;
	const restyledPath = await generateRunwayCandleImage({
		candlePath,
		tmpDir,
		jobId,
		log,
	});
	if (restyledPath) workingPath = restyledPath;

	const detected = detectFileType(workingPath);
	if (!detected || detected.kind !== "image") return candlePath;

	const hasAlpha = imageHasAlpha(workingPath);
	const keyColor = hasAlpha ? null : sampleImageKeyColor(workingPath);
	const trimTop = clamp(
		restyledPath ? Math.min(CANDLE_TRIM_TOP_PCT, 0.02) : CANDLE_TRIM_TOP_PCT,
		0,
		0.3
	);
	const useKey =
		!hasAlpha &&
		keyColor &&
		Number.isFinite(CANDLE_KEY_SIMILARITY) &&
		Number.isFinite(CANDLE_KEY_BLEND);

	if (!useKey && trimTop <= 0.001) return workingPath;

	const suffix = jobId || crypto.randomUUID();
	const outPath = path.join(tmpDir, `candle_prepped_${suffix}.png`);
	const filters = ["format=rgba"];
	if (useKey) {
		const hex = rgbToHex(keyColor);
		filters.push(
			`colorkey=0x${hex}:${CANDLE_KEY_SIMILARITY.toFixed(
				3
			)}:${CANDLE_KEY_BLEND.toFixed(3)}`
		);
	}
	if (trimTop > 0.001) {
		const keepPct = (1 - trimTop).toFixed(4);
		filters.push(`crop=iw:ih*${keepPct}:0:ih*${trimTop.toFixed(4)}`);
	}

	try {
		await runFfmpeg(
			[
				"-i",
				workingPath,
				"-vf",
				filters.join(","),
				"-frames:v",
				"1",
				"-y",
				outPath,
			],
			"candle_prep"
		);
		const dt = detectFileType(outPath);
		if (dt?.kind === "image") {
			if (CANDLE_CROP_ALPHA_ENABLED) {
				try {
					const bounds = computeAlphaBounds(outPath, {
						alphaThreshold: CANDLE_CROP_ALPHA_THRESHOLD,
					});
					if (bounds && bounds.w && bounds.h) {
						const croppedPath = path.join(
							tmpDir,
							`candle_cropped_${suffix}.png`
						);
						await runFfmpeg(
							[
								"-i",
								outPath,
								"-vf",
								`crop=${bounds.w}:${bounds.h}:${bounds.x}:${bounds.y}`,
								"-frames:v",
								"1",
								"-y",
								croppedPath,
							],
							"candle_crop_alpha"
						);
						const dt2 = detectFileType(croppedPath);
						if (dt2?.kind === "image") {
							safeUnlink(outPath);
							if (log)
								log("candle asset cropped", {
									path: path.basename(croppedPath),
								});
							return croppedPath;
						}
						safeUnlink(croppedPath);
					}
				} catch (e) {
					if (log)
						log("candle alpha crop failed (using prepped)", {
							error: e?.message || String(e),
						});
				}
			}
			if (log)
				log("candle asset prepped", {
					path: path.basename(outPath),
					hasAlpha,
					trimTopPct: Number(trimTop.toFixed(3)),
				});
			return outPath;
		}
	} catch (e) {
		if (log)
			log("candle prep failed (using original)", {
				error: e?.message || String(e),
			});
	}

	safeUnlink(outPath);
	return workingPath;
}

function computeImageHash(filePath, regionPct = null) {
	if (!ffmpegPath) return null;
	const dims = ffprobeDimensions(filePath);
	if (!dims.width || !dims.height) return null;
	let crop = "";
	if (regionPct && dims.width && dims.height) {
		const rx = Math.max(0, Math.round(dims.width * (regionPct.x || 0)));
		const ry = Math.max(0, Math.round(dims.height * (regionPct.y || 0)));
		const rw = Math.max(1, Math.round(dims.width * (regionPct.w || 1)));
		const rh = Math.max(1, Math.round(dims.height * (regionPct.h || 1)));
		crop = `crop=${rw}:${rh}:${rx}:${ry},`;
	}
	const filter = `${crop}scale=9:8:flags=area,format=gray`;
	const args = [
		"-hide_banner",
		"-loglevel",
		"error",
		"-i",
		filePath,
		"-vf",
		filter,
		"-frames:v",
		"1",
		"-f",
		"rawvideo",
		"pipe:1",
	];
	try {
		const buf = runFfmpegBuffer(args, "presenter_hash");
		if (!buf || buf.length < 72) return null;
		const bits = new Array(64);
		let idx = 0;
		for (let y = 0; y < 8; y++) {
			for (let x = 0; x < 8; x++) {
				const left = buf[y * 9 + x];
				const right = buf[y * 9 + x + 1];
				bits[idx] = left > right ? 1 : 0;
				idx += 1;
			}
		}
		return bits;
	} catch {
		return null;
	}
}

function hashSimilarity(a, b) {
	if (!a || !b || a.length !== b.length) return null;
	let diff = 0;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) diff += 1;
	}
	return 1 - diff / a.length;
}

function comparePresenterSimilarity(originalPath, candidatePath) {
	const regions = [PRESENTER_EYES_REGION, PRESENTER_FACE_REGION];
	const scores = [];
	for (const region of regions) {
		const a = computeImageHash(originalPath, region);
		const b = computeImageHash(candidatePath, region);
		const score = hashSimilarity(a, b);
		if (Number.isFinite(score)) scores.push(score);
	}
	if (!scores.length) return null;
	const sum = scores.reduce((acc, v) => acc + v, 0);
	return sum / scores.length;
}

async function overlayWardrobeFromGenerated({
	basePath,
	generatedPath,
	tmpDir,
	jobId,
	log,
}) {
	if (!basePath || !fs.existsSync(basePath)) return basePath;
	if (!generatedPath || !fs.existsSync(generatedPath)) return basePath;

	const baseDims = probeImageDimensions(basePath);
	const genDims = probeImageDimensions(generatedPath);
	if (!baseDims.width || !baseDims.height || !genDims.width || !genDims.height)
		return basePath;

	const rx = Math.max(
		0,
		Math.round(baseDims.width * PRESENTER_WARDROBE_REGION.x)
	);
	const ry = Math.max(
		0,
		Math.round(baseDims.height * PRESENTER_WARDROBE_REGION.y)
	);
	const rw = Math.max(
		1,
		Math.round(baseDims.width * PRESENTER_WARDROBE_REGION.w)
	);
	const rh = Math.max(
		1,
		Math.round(baseDims.height * PRESENTER_WARDROBE_REGION.h)
	);

	const suffix = jobId || crypto.randomUUID();
	const outPath = path.join(tmpDir, `presenter_wardrobe_${suffix}.png`);
	const scaleFilter =
		genDims.width !== baseDims.width || genDims.height !== baseDims.height
			? `[1:v]scale=${baseDims.width}:${baseDims.height}:flags=lanczos,format=rgba[gen];`
			: `[1:v]format=rgba[gen];`;
	const filter =
		`${scaleFilter}` +
		`[gen]crop=${rw}:${rh}:${rx}:${ry}[wardrobe];` +
		`[0:v][wardrobe]overlay=x=${rx}:y=${ry}:format=auto[outv]`;

	await runFfmpeg(
		[
			"-i",
			basePath,
			"-i",
			generatedPath,
			"-filter_complex",
			filter,
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-y",
			outPath,
		],
		"presenter_wardrobe_overlay"
	);

	if (log)
		log("presenter wardrobe overlay ready", {
			path: path.basename(outPath),
		});

	return outPath;
}

async function generateRunwayPresenterImage({
	jobId,
	tmpDir,
	presenterLocalPath,
	prompt,
	log,
}) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	if (!presenterLocalPath || !fs.existsSync(presenterLocalPath))
		throw new Error("presenter image inputs missing");
	ensureDir(tmpDir);

	const dims = probeImageDimensions(presenterLocalPath);
	const ratio = runwayImageRatioForDims(dims);
	const seed = seedFromText(`${jobId || "presenter"}_presenter`);

	const facePath = path.join(
		tmpDir,
		`presenter_face_${jobId || crypto.randomUUID()}.png`
	);
	await createCropFromRegion({
		inputPath: presenterLocalPath,
		regionPct: PRESENTER_FACE_REGION,
		outPath: facePath,
		label: "presenter_face_crop",
	});

	let presenterUri = null;
	let faceUri = null;
	try {
		presenterUri = await runwayCreateEphemeralUpload({
			filePath: presenterLocalPath,
			filename: path.basename(presenterLocalPath),
		});
		faceUri = await runwayCreateEphemeralUpload({
			filePath: facePath,
			filename: path.basename(facePath),
		});
	} catch (e) {
		safeUnlink(facePath);
		throw e;
	}

	const referenceImages = [
		{ uri: presenterUri, tag: "presenter_ref" },
		{ uri: faceUri, tag: "face_ref" },
	];

	if (log)
		log("presenter runway prompt", {
			prompt: String(prompt || "").slice(0, 200),
			ratio,
		});

	let runwayPath = null;
	try {
		const outputUri = await runwayTextToImage({
			promptText: prompt,
			ratio,
			referenceImages,
			seed,
		});

		runwayPath = path.join(
			tmpDir,
			`presenter_runway_${jobId || crypto.randomUUID()}.png`
		);
		await downloadRunwayImageToPath({ uri: outputUri, outPath: runwayPath });
	} finally {
		safeUnlink(facePath);
	}
	if (!runwayPath) throw new Error("runway presenter image missing");

	const compositePath = await overlayWardrobeFromGenerated({
		basePath: presenterLocalPath,
		generatedPath: runwayPath,
		tmpDir,
		jobId,
		log,
	});
	if (compositePath && compositePath !== runwayPath) safeUnlink(runwayPath);
	return compositePath || runwayPath;
}

async function overlayCandleOnPresenterSmart({
	basePath,
	candlePath,
	tmpDir,
	jobId,
	log,
}) {
	if (!basePath || !fs.existsSync(basePath)) return basePath;
	if (!candlePath || !fs.existsSync(candlePath)) return basePath;

	const baseDims = probeImageDimensions(basePath);
	const candleDims = probeImageDimensions(candlePath);
	if (
		!baseDims.width ||
		!baseDims.height ||
		!candleDims.width ||
		!candleDims.height
	)
		return basePath;

	let targetW = Math.round(baseDims.width * CANDLE_WIDTH_PCT);
	const maxW = Math.min(
		Math.max(CANDLE_MAX_PX, 1),
		Math.max(1, baseDims.width - 20)
	);
	const minW = Math.min(Math.max(CANDLE_MIN_PX, 1), maxW);
	targetW = clamp(targetW, minW, maxW);

	const scaleFactor = targetW / candleDims.width;

	const aspect = baseDims.height / baseDims.width;
	const fallbackDeskPct =
		aspect > 0.62 ? DESK_EDGE_EXPECTED_WIDE_PCT : DESK_EDGE_EXPECTED_TALL_PCT;
	const minDeskY = Math.round(baseDims.height * DESK_EDGE_MIN_PCT);
	let deskEdgeY = estimateDeskEdgeY(basePath, baseDims);
	if (Number.isFinite(deskEdgeY) && deskEdgeY < minDeskY) deskEdgeY = null;
	deskEdgeY = deskEdgeY || Math.round(baseDims.height * fallbackDeskPct);
	deskEdgeY = clamp(deskEdgeY, 0, baseDims.height - 1);

	const bottomMarginPx = Math.max(
		CANDLE_BOTTOM_MARGIN_PX,
		Math.round(baseDims.height * clamp(CANDLE_BOTTOM_MARGIN_PCT, 0, 0.2))
	);
	const bottomY = clamp(deskEdgeY - bottomMarginPx, 1, baseDims.height - 1);

	const clearancePct = clamp(CANDLE_CLEARANCE_PCT, 0, 0.2);
	const wardrobeRight = Math.round(
		baseDims.width *
			Math.max(
				0,
				PRESENTER_WARDROBE_REGION.x + PRESENTER_WARDROBE_REGION.w - clearancePct
			)
	);
	const margin = 12;
	const desiredXCenter = Math.round(baseDims.width * CANDLE_X_PCT);
	let minXCenter = wardrobeRight + Math.round(targetW / 2) + margin;
	let maxXCenter = baseDims.width - Math.round(targetW / 2) - margin;
	if (minXCenter > maxXCenter) {
		minXCenter = Math.round(targetW / 2) + margin;
		maxXCenter = baseDims.width - Math.round(targetW / 2) - margin;
	}
	const xCenter = clamp(desiredXCenter, minXCenter, maxXCenter);

	const outPath = path.join(tmpDir, `presenter_candle_${jobId}.png`);

	const shadowOpacity = clamp(CANDLE_SHADOW_OPACITY, 0, 0.6);
	const shadowBlur = Math.max(1, Math.round(CANDLE_SHADOW_BLUR));
	const shadowX = Math.round(CANDLE_SHADOW_X_PX);
	const shadowY = Math.round(CANDLE_SHADOW_Y_PX);
	const addShadow = CANDLE_SHADOW_ENABLED && shadowOpacity > 0;
	const filter = addShadow
		? `[1:v]scale=iw*${scaleFactor}:ih*${scaleFactor}:flags=lanczos,format=rgba[candle];` +
		  `[candle]split=2[candle_main][candle_shadow];` +
		  `[candle_shadow]hue=s=0,eq=brightness=-0.4:contrast=1.0,boxblur=${shadowBlur}:${shadowBlur},format=rgba,colorchannelmixer=aa=${shadowOpacity.toFixed(
				3
		  )}[shadow];` +
		  `[0:v][shadow]overlay=x=${xCenter}-w/2+${shadowX}:y=${bottomY}-h+${shadowY}:format=auto[bg];` +
		  `[bg][candle_main]overlay=x=${xCenter}-w/2:y=${bottomY}-h:format=auto[outv]`
		: `[1:v]scale=iw*${scaleFactor}:ih*${scaleFactor}:flags=lanczos,format=rgba[candle];` +
		  `[0:v][candle]overlay=x=${xCenter}-w/2:y=${bottomY}-h:format=auto[outv]`;

	await runFfmpeg(
		[
			"-i",
			basePath,
			"-i",
			candlePath,
			"-filter_complex",
			filter,
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-y",
			outPath,
		],
		"presenter_candle_overlay_smart"
	);

	if (log)
		log("presenter candle overlay (smart) ready", {
			path: path.basename(outPath),
			targetW,
			xCenter,
			deskEdgeY,
		});

	return outPath;
}

async function overlayCandleOnPresenter({
	basePath,
	candlePath,
	tmpDir,
	jobId,
	log,
}) {
	if (!basePath || !fs.existsSync(basePath)) return basePath;
	if (!candlePath || !fs.existsSync(candlePath)) return basePath;
	const baseDims = probeImageDimensions(basePath);
	const candleDims = probeImageDimensions(candlePath);
	if (
		!baseDims.width ||
		!baseDims.height ||
		!candleDims.width ||
		!candleDims.height
	)
		return basePath;

	const targetW = Math.max(12, Math.round(baseDims.width * CANDLE_WIDTH_PCT));
	const scaleFactor = targetW / candleDims.width;
	const outPath = path.join(tmpDir, `presenter_candle_${jobId}.png`);
	const overlayX = Math.round(baseDims.width * CANDLE_X_PCT);
	const overlayY = Math.round(baseDims.height * CANDLE_Y_PCT);

	await runFfmpeg(
		[
			"-i",
			basePath,
			"-i",
			candlePath,
			"-filter_complex",
			`[1:v]scale=iw*${scaleFactor}:ih*${scaleFactor}:flags=lanczos,format=rgba[candle];` +
				`[0:v][candle]overlay=x=${overlayX}-w/2:y=${overlayY}-h/2:format=auto[outv]`,
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-y",
			outPath,
		],
		"presenter_candle_overlay"
	);
	if (log)
		log("presenter candle overlay ready", {
			path: path.basename(outPath),
		});
	return outPath;
}

function ensurePresenterFile(filePath) {
	if (!filePath || !fs.existsSync(filePath))
		throw new Error("presenter_image_missing");
	const st = fs.statSync(filePath);
	if (!st || st.size < PRESENTER_MIN_BYTES)
		throw new Error("presenter_image_too_small");
	const dt = detectFileType(filePath);
	if (!dt || dt.kind !== "image") throw new Error("presenter_image_invalid");
	return filePath;
}

async function generatePresenterAdjustedImage({
	jobId,
	tmpDir,
	presenterLocalPath,
	candleLocalPath,
	title,
	topics = [],
	categoryLabel,
	log,
}) {
	if (!presenterLocalPath || !fs.existsSync(presenterLocalPath))
		throw new Error("presenter_base_missing");
	ensureDir(tmpDir);

	let outPath = null;
	let method = "original";

	const wardrobePrompt = buildWardrobeEditPrompt({
		title,
		topics,
		categoryLabel,
	});
	try {
		outPath = await generateRunwayPresenterImage({
			jobId,
			tmpDir,
			presenterLocalPath,
			prompt: wardrobePrompt,
			log,
		});
		method = "runway_wardrobe";
		const similarity = comparePresenterSimilarity(presenterLocalPath, outPath);
		if (Number.isFinite(similarity) && similarity < PRESENTER_LIKENESS_MIN) {
			if (log)
				log("presenter edit rejected (low similarity)", {
					similarity: Number(similarity.toFixed(3)),
				});
			safeUnlink(outPath);
			outPath = presenterLocalPath;
			method = "original";
		}
	} catch (e) {
		if (log)
			log("presenter wardrobe edit failed; using original", {
				error: e?.message || String(e),
			});
		outPath = presenterLocalPath;
	}

	if (outPath !== presenterLocalPath) {
		ensurePresenterFile(outPath);
	}

	let withCandle = null;
	let candleOverlayPath = candleLocalPath;
	try {
		candleOverlayPath = await prepareCandleOverlayAsset({
			candlePath: candleLocalPath,
			tmpDir,
			jobId,
			log,
		});
	} catch (e) {
		if (log)
			log("candle prep failed (using original)", {
				error: e?.message || String(e),
			});
		candleOverlayPath = candleLocalPath;
	}
	try {
		withCandle = await overlayCandleOnPresenterSmart({
			basePath: outPath,
			candlePath: candleOverlayPath,
			tmpDir,
			jobId,
			log,
		});
	} catch (e) {
		if (log)
			log("smart candle overlay failed; falling back", {
				error: e?.message || String(e),
			});
		withCandle = await overlayCandleOnPresenter({
			basePath: outPath,
			candlePath: candleOverlayPath,
			tmpDir,
			jobId,
			log,
		});
	}
	if (withCandle && withCandle !== outPath && outPath !== presenterLocalPath) {
		safeUnlink(outPath);
	}
	outPath = withCandle || outPath;
	ensurePresenterFile(outPath);

	let uploadResult = null;
	try {
		uploadResult = await uploadPresenterToCloudinary(outPath, jobId);
	} catch (e) {
		if (log)
			log("presenter cloudinary upload failed", {
				error: e?.message || String(e),
			});
	}

	return {
		localPath: outPath,
		url: uploadResult?.url || "",
		publicId: uploadResult?.public_id || "",
		width: uploadResult?.width || 0,
		height: uploadResult?.height || 0,
		method,
	};
}

module.exports = {
	generatePresenterAdjustedImage,
};
