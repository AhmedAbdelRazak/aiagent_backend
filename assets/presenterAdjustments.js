/** @format */

const fs = require("fs");
const path = require("path");
const child_process = require("child_process");
const crypto = require("crypto");
const { OpenAI, toFile } = require("openai");
const cloudinary = require("cloudinary").v2;

let ffmpegPath = "";
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
}

const SORA_MODEL = process.env.SORA_MODEL || "sora-2-pro";
const SORA_SECONDS = "4";
const SORA_POLL_INTERVAL_MS = 2000;
const SORA_MAX_POLL_ATTEMPTS = 120;
const DEFAULT_RATIO = "1280:720";
const DEFAULT_IMAGE_MODEL = "gpt-image-1";
const DEFAULT_IMAGE_SIZE = "1536x1024";
const DEFAULT_IMAGE_QUALITY = "high";
const DEFAULT_IMAGE_INPUT_FIDELITY = "high";
const PRESENTER_LOCK_IDENTITY = true;
const PRESENTER_LIKENESS_MIN = 0.82;
const PRESENTER_FACE_REGION = { x: 0.3, y: 0.05, w: 0.4, h: 0.55 };
const PRESENTER_EYES_REGION = { x: 0.32, y: 0.08, w: 0.36, h: 0.22 };
const PRESENTER_WARDROBE_REGION = { x: 0.22, y: 0.36, w: 0.56, h: 0.58 };
const CANDLE_WIDTH_PCT = parseNumber(process.env.CANDLE_WIDTH_PCT, 0.12);
const CANDLE_X_PCT = parseNumber(process.env.CANDLE_X_PCT, 0.86);
const CANDLE_Y_PCT = parseNumber(process.env.CANDLE_Y_PCT, 0.8);
const CANDLE_BOTTOM_MARGIN_PX = parseNumber(
	process.env.CANDLE_BOTTOM_MARGIN_PX,
	6
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
const ENABLE_CANDLE_RESTYLE =
	String(process.env.ENABLE_CANDLE_RESTYLE || "1") !== "0";
const CANDLE_RESTYLE_MODEL =
	process.env.CANDLE_RESTYLE_MODEL || DEFAULT_IMAGE_MODEL;
const CANDLE_RESTYLE_SIZE = process.env.CANDLE_RESTYLE_SIZE || "1024x1024";
const CANDLE_RESTYLE_QUALITY =
	process.env.CANDLE_RESTYLE_QUALITY || DEFAULT_IMAGE_QUALITY;
const CANDLE_RESTYLE_INPUT_FIDELITY =
	process.env.CANDLE_RESTYLE_INPUT_FIDELITY || DEFAULT_IMAGE_INPUT_FIDELITY;
const CANDLE_SHADOW_ENABLED =
	String(process.env.CANDLE_SHADOW_ENABLED || "1") !== "0";
const CANDLE_SHADOW_OPACITY = parseNumber(
	process.env.CANDLE_SHADOW_OPACITY,
	0.28
);
const CANDLE_SHADOW_BLUR = parseNumber(process.env.CANDLE_SHADOW_BLUR, 4);
const CANDLE_SHADOW_X_PX = parseNumber(process.env.CANDLE_SHADOW_X_PX, 4);
const CANDLE_SHADOW_Y_PX = parseNumber(process.env.CANDLE_SHADOW_Y_PX, 5);
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

function getOpenAiKey() {
	return process.env.OPENAI_API_KEY || process.env.CHATGPT_API_TOKEN || "";
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
Keep everything identical to the reference image, including face, glasses, beard, hairline, skin texture, studio background, desk, and lighting.
Only change the clothing on the torso area to: ${wardrobe}.
No tie. No formal suit jacket. Keep it youthful, lively, and fancy-casual.
Do NOT change facial features or body proportions. Do NOT change the studio or desk.
${expressionLine}
Eyes: natural, forward-looking, aligned; no crossed eyes or odd gaze.
No extra people, no text, no logos.
`.trim();
}

function buildPresenterPrompt({ title, topics, categoryLabel }) {
	const topicFocus = buildTopicFocus({ title, topics });
	const wardrobe = inferWardrobeStyle({
		categoryLabel,
		text: `${title || ""} ${topicFocus || ""}`,
	});
	const expressionLine = inferExpressionLine(
		`${title || ""} ${topicFocus || ""}`
	);
	const candleLine =
		"Add one small branded candle on the desk to the presenter's left (viewer-right), toward the back-right corner. Keep it a realistic small tabletop size, fully on the desk with a safe margin from the edge, lit, open jar with no lid, label readable, not centered and not in the foreground. No square background, no sticker edges, no pedestal.";
	const candleRefLine =
		"If a candle reference is provided, match its label, glass, and color exactly (no redesign).";

	return `
Photorealistic studio portrait of the SAME person as the reference image.
Keep identity (face, beard, glasses), age, skin tone, and hairline the same.
Keep the SAME studio background, desk, lighting, and camera angle. Do not change the room.
Wardrobe: ${wardrobe}. Make it different from the reference while staying classy and topic-appropriate: ${topicFocus}.
No tie. No formal suit jacket. Keep it youthful, lively, and fancy-casual.
${candleLine}
${candleRefLine}
${expressionLine}
Eyes: natural, forward-looking, aligned; no crossed eyes or odd gaze.
No extra people, no text, no logos (except the candle brand), no distortions or warped face.
`.trim();
}

async function createWardrobeMask(inputPath, tmpDir, jobId) {
	const dims = probeImageDimensions(inputPath);
	if (!dims.width || !dims.height)
		throw new Error("wardrobe_mask_dims_missing");
	const rx = Math.max(0, Math.round(dims.width * PRESENTER_WARDROBE_REGION.x));
	const ry = Math.max(0, Math.round(dims.height * PRESENTER_WARDROBE_REGION.y));
	const rw = Math.max(1, Math.round(dims.width * PRESENTER_WARDROBE_REGION.w));
	const rh = Math.max(1, Math.round(dims.height * PRESENTER_WARDROBE_REGION.h));
	const outPath = path.join(tmpDir, `presenter_mask_${jobId}.png`);
	await runFfmpeg(
		[
			"-f",
			"lavfi",
			"-i",
			`color=c=white@1.0:s=${dims.width}x${dims.height}`,
			"-vf",
			`format=rgba,drawbox=x=${rx}:y=${ry}:w=${rw}:h=${rh}:color=black@0.0:t=fill`,
			"-frames:v",
			"1",
			"-y",
			outPath,
		],
		"presenter_mask"
	);
	return outPath;
}

function soraSizeForRatio(ratio) {
	const raw = String(ratio || "").trim() || DEFAULT_RATIO;
	if (raw === "720:1280") return "720x1280";
	if (raw === "832:1104") return "832x1104";
	if (raw === "1104:832") return "1104x832";
	return "1280x720";
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

async function generateOpenAiCandleImage({
	candlePath,
	tmpDir,
	jobId,
	openai,
	log,
}) {
	if (!ENABLE_CANDLE_RESTYLE || !openai) return null;
	if (!candlePath || !fs.existsSync(candlePath)) return null;

	let imageInput = null;
	try {
		const buf = fs.readFileSync(candlePath);
		const mime = inferImageMime(candlePath) || "image/png";
		imageInput = await toFile(buf, path.basename(candlePath), { type: mime });
	} catch (e) {
		if (log)
			log("candle restyle read failed", { error: e?.message || String(e) });
		return null;
	}

	const prompt = `
Keep the exact same candle label, glass, colors, proportions, and perspective.
Remove the lid completely (no lid visible anywhere). Make the candle OPEN.
Add a small, realistic lit flame and glowing wick.
Transparent background only; no shadow, no pedestal, no extra objects.
Do not change the brand text or logo.
`.trim();

	try {
		const resp = await openai.images.edit({
			model: CANDLE_RESTYLE_MODEL,
			image: imageInput,
			prompt,
			quality: CANDLE_RESTYLE_QUALITY,
			output_format: "png",
			background: "transparent",
			size: CANDLE_RESTYLE_SIZE,
			input_fidelity: CANDLE_RESTYLE_INPUT_FIDELITY || undefined,
		});
		const image = resp?.data?.[0];
		if (!image?.b64_json) throw new Error("candle_restyle_empty");
		const buf = Buffer.from(String(image.b64_json), "base64");
		const outPath = path.join(
			tmpDir,
			`candle_open_lit_${jobId || crypto.randomUUID()}.png`
		);
		fs.writeFileSync(outPath, buf);
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

async function prepareCandleOverlayAsset({
	candlePath,
	tmpDir,
	jobId,
	log,
	openai,
}) {
	if (!CANDLE_PREP_ENABLED) return candlePath;
	if (!candlePath || !fs.existsSync(candlePath)) return candlePath;

	let workingPath = candlePath;
	const restyledPath = await generateOpenAiCandleImage({
		candlePath,
		tmpDir,
		jobId,
		openai,
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

async function extractFrameFromVideo({ videoPath, outPath }) {
	const args = ["-ss", "0.4", "-i", videoPath, "-frames:v", "1", "-y", outPath];
	await runFfmpeg(args, "presenter_sora_frame");
	return outPath;
}

async function generateSoraPresenterFrame({
	jobId,
	tmpDir,
	presenterLocalPath,
	prompt,
	ratio,
	openai,
	log,
}) {
	const apiKey = getOpenAiKey();
	if (!apiKey && !openai)
		throw new Error(
			"OpenAI API key missing (OPENAI_API_KEY or CHATGPT_API_TOKEN)."
		);
	const client = openai || new OpenAI({ apiKey });
	const size = soraSizeForRatio(ratio);
	const seconds = String(SORA_SECONDS || "4");

	if (log)
		log("presenter sora prompt", {
			prompt: prompt.slice(0, 200),
			size,
			seconds,
		});

	let imageInput = null;
	if (presenterLocalPath && fs.existsSync(presenterLocalPath)) {
		try {
			const buf = fs.readFileSync(presenterLocalPath);
			const mime = inferImageMime(presenterLocalPath) || "image/png";
			imageInput = await toFile(buf, path.basename(presenterLocalPath), {
				type: mime,
			});
		} catch {}
	}

	const createJob = async (includeImage) => {
		const payload = {
			model: SORA_MODEL,
			prompt,
			seconds,
			size,
		};
		if (includeImage && imageInput) payload.image = imageInput;
		return await client.videos.create(payload);
	};

	let job = null;
	try {
		job = await createJob(Boolean(imageInput));
	} catch (e) {
		if (imageInput && log)
			log("presenter sora create retry without image", {
				error: e?.message || String(e),
			});
		job = await createJob(false);
	}

	const running = new Set(["queued", "in_progress", "processing"]);
	let attempts = 0;
	while (
		running.has(String(job?.status || "")) &&
		attempts < SORA_MAX_POLL_ATTEMPTS
	) {
		await sleep(SORA_POLL_INTERVAL_MS);
		try {
			const updated = await client.videos.retrieve(job.id);
			if (updated) job = updated;
		} catch (pollErr) {
			if (log)
				log("presenter sora poll failed", {
					attempt: attempts + 1,
					message: pollErr?.message,
					code: pollErr?.code || pollErr?.response?.data?.error?.code || null,
					status: pollErr?.response?.status || null,
				});
		}
		attempts++;
	}

	if (String(job?.status) !== "completed") {
		const jobErr = job?.error || job?.last_error || job?.failure || null;
		const code = jobErr?.code || jobErr?.type || null;
		const message =
			jobErr?.message || jobErr?.error?.message || job?.failure_reason || null;
		const err = new Error(
			message ||
				`Sora job ${job?.id} failed (status=${job?.status || "unknown"})`
		);
		err.code = code;
		err.jobId = job?.id;
		err.status = job?.status;
		throw err;
	}

	let response = null;
	try {
		response = await client.videos.downloadContent(job.id, {
			variant: "video",
		});
	} catch {
		response = await client.videos.downloadContent(job.id, {
			variant: "mp4",
		});
	}

	const buf = Buffer.from(await response.arrayBuffer());
	const videoPath = path.join(tmpDir, `presenter_sora_${jobId}.mp4`);
	fs.writeFileSync(videoPath, buf);
	const framePath = path.join(tmpDir, `presenter_sora_${jobId}.png`);
	await extractFrameFromVideo({ videoPath, outPath: framePath });
	return framePath;
}

async function generateOpenAiPresenterImage({
	jobId,
	tmpDir,
	presenterLocalPath,
	prompt,
	maskPath,
	openai,
	log,
}) {
	const apiKey = getOpenAiKey();
	if (!apiKey && !openai)
		throw new Error(
			"OpenAI API key missing (OPENAI_API_KEY or CHATGPT_API_TOKEN)."
		);
	const client = openai || new OpenAI({ apiKey });
	const inputs = [];
	if (presenterLocalPath && fs.existsSync(presenterLocalPath)) {
		try {
			const buf = fs.readFileSync(presenterLocalPath);
			const mime = inferImageMime(presenterLocalPath);
			if (mime)
				inputs.push(
					await toFile(buf, path.basename(presenterLocalPath), { type: mime })
				);
		} catch {}
	}
	if (!inputs.length) throw new Error("presenter image inputs missing");
	let maskInput = null;
	if (maskPath && fs.existsSync(maskPath)) {
		try {
			const buf = fs.readFileSync(maskPath);
			maskInput = await toFile(buf, path.basename(maskPath), {
				type: "image/png",
			});
		} catch {}
	}

	const editOptions = {
		model: DEFAULT_IMAGE_MODEL,
		image: inputs.length === 1 ? inputs[0] : inputs,
		prompt,
		quality: DEFAULT_IMAGE_QUALITY,
		output_format: "png",
		background: "auto",
	};
	if (maskInput) editOptions.mask = maskInput;
	if (DEFAULT_IMAGE_INPUT_FIDELITY)
		editOptions.input_fidelity = DEFAULT_IMAGE_INPUT_FIDELITY;

	let resp = null;
	try {
		resp = await client.images.edit(editOptions);
	} catch {
		editOptions.size = DEFAULT_IMAGE_SIZE;
		resp = await client.images.edit(editOptions);
	}

	const image = resp?.data?.[0];
	if (!image?.b64_json) throw new Error("presenter_openai_empty");
	const buf = Buffer.from(String(image.b64_json), "base64");
	const outPath = path.join(tmpDir, `presenter_openai_${jobId}.png`);
	fs.writeFileSync(outPath, buf);
	if (log)
		log("presenter openai fallback ready", {
			path: path.basename(outPath),
		});
	return outPath;
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

	const bottomY = clamp(
		deskEdgeY - CANDLE_BOTTOM_MARGIN_PX,
		1,
		baseDims.height - 1
	);

	const wardrobeRight = Math.round(
		baseDims.width * (PRESENTER_WARDROBE_REGION.x + PRESENTER_WARDROBE_REGION.w)
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
	ratio = DEFAULT_RATIO,
	title,
	topics = [],
	categoryLabel,
	openai,
	log,
}) {
	if (!presenterLocalPath || !fs.existsSync(presenterLocalPath))
		throw new Error("presenter_base_missing");
	ensureDir(tmpDir);

	let outPath = null;
	let method = "original";

	if (PRESENTER_LOCK_IDENTITY) {
		const wardrobePrompt = buildWardrobeEditPrompt({
			title,
			topics,
			categoryLabel,
		});
		try {
			const maskPath = await createWardrobeMask(
				presenterLocalPath,
				tmpDir,
				jobId
			);
			outPath = await generateOpenAiPresenterImage({
				jobId,
				tmpDir,
				presenterLocalPath,
				prompt: wardrobePrompt,
				maskPath,
				openai,
				log,
			});
			safeUnlink(maskPath);
			method = "openai_mask";
			const similarity = comparePresenterSimilarity(
				presenterLocalPath,
				outPath
			);
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
	} else {
		const prompt = buildPresenterPrompt({ title, topics, categoryLabel });
		method = "sora";
		try {
			outPath = await generateSoraPresenterFrame({
				jobId,
				tmpDir,
				presenterLocalPath,
				prompt,
				ratio,
				openai,
				log,
			});
			const similarity = comparePresenterSimilarity(
				presenterLocalPath,
				outPath
			);
			if (Number.isFinite(similarity) && similarity < PRESENTER_LIKENESS_MIN) {
				if (log)
					log("presenter sora rejected (low similarity)", {
						similarity: Number(similarity.toFixed(3)),
					});
				safeUnlink(outPath);
				outPath = presenterLocalPath;
				method = "original";
			}
		} catch (e) {
			method = "openai_image";
			if (log)
				log("presenter sora failed; trying image edit", {
					error: e?.message || String(e),
				});
			try {
				outPath = await generateOpenAiPresenterImage({
					jobId,
					tmpDir,
					presenterLocalPath,
					prompt,
					openai,
					log,
				});
				const similarity = comparePresenterSimilarity(
					presenterLocalPath,
					outPath
				);
				if (
					Number.isFinite(similarity) &&
					similarity < PRESENTER_LIKENESS_MIN
				) {
					if (log)
						log("presenter image edit rejected (low similarity)", {
							similarity: Number(similarity.toFixed(3)),
						});
					safeUnlink(outPath);
					outPath = presenterLocalPath;
					method = "original";
				}
			} catch (e2) {
				if (log)
					log("presenter image edit failed; using original", {
						error: e2?.message || String(e2),
					});
				outPath = presenterLocalPath;
				method = "original";
			}
		}
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
			openai,
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
