/** @format */

const fs = require("fs");
const os = require("os");
const path = require("path");
const child_process = require("child_process");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;
const { OpenAI } = require("openai");
const { EXPLICIT_SERIOUS_CUES } = require("./utils");

let FormDataNode = null;
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	FormDataNode = require("form-data");
} catch {
	FormDataNode = null;
}

const RUNWAY_API_KEY = process.env.RUNWAYML_API_SECRET || "";
const RUNWAY_VERSION = "2024-11-06";
const RUNWAY_IMAGE_MODEL = process.env.RUNWAY_IMAGE_MODEL || "gen4_image";
const RUNWAY_IMAGE_POLL_INTERVAL_MS = 2000;
const RUNWAY_IMAGE_MAX_POLL_ATTEMPTS = 120;
const PRESENTER_MIN_BYTES = 12000;
const PRESENTER_CLOUDINARY_FOLDER = "aivideomatic/long_presenters";
const PRESENTER_CLOUDINARY_PUBLIC_PREFIX = "presenter_master";
const PRESENTER_STRICT_PROMPT_ONLY = true;
const PRESENTER_STRICT_FALLBACK_TO_ORIGINAL = true;
const PRESENTER_DIMENSIONS_MUST_MATCH = true;
const PRESENTER_FACE_SIMILARITY_MIN = 0.98;
const PRESENTER_UPPER_SIMILARITY_MIN = 0.965;
const PRESENTER_OVERALL_SIMILARITY_MIN = 0.98;
const PRESENTER_SSIM_ENABLED = true;
const PRESENTER_SSIM_FACE_MIN = 0.99;
const PRESENTER_SSIM_EYES_MIN = 0.99;
const PRESENTER_SSIM_MOUTH_MIN = 0.985;
const PRESENTER_SSIM_GEOMETRY_MIN = 0.93;
const PRESENTER_FACE_REGION = { x: 0.24, y: 0.02, w: 0.52, h: 0.44 };
const PRESENTER_EYES_REGION = { x: 0.3, y: 0.08, w: 0.4, h: 0.18 };
const PRESENTER_CHIN_REGION = { x: 0.3, y: 0.3, w: 0.4, h: 0.16 };
const PRESENTER_MOUTH_REGION = { x: 0.36, y: 0.26, w: 0.28, h: 0.18 };
const PRESENTER_UPPER_REGION = { x: 0, y: 0, w: 1, h: 0.62 };
const PRESENTER_TORSO_GEOMETRY_REGION = { x: 0.14, y: 0.42, w: 0.72, h: 0.52 };
const CHAT_MODEL = "gpt-5.2";
const ORCHESTRATOR_PRESENTER_REF_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767066355/aivideomatic/long_presenters/presenter_master_4b76c718-6a2a-4749-895e-e05bd2b2ecfc_1767066355424.png";

let ffmpegPath = "";
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
}

const PRESENTER_WARDROBE_ATTEMPTS = 2;
const WARDROBE_VARIANTS = [
	"dark charcoal matte button-up, open collar, no blazer",
	"black band-collar button-up, no blazer",
	"deep navy oxford button-up, open collar, no blazer",
	"black button-up with standard placket, open collar, no blazer",
	"black button-up with hidden placket, open collar, no blazer",
	"deep forest green button-up, open collar, no blazer",
	"dark burgundy button-up, open collar, no blazer",
	"midnight teal button-up, open collar, no blazer",
	"dark aubergine button-up, open collar, no blazer",
	"graphite twill button-up, open collar, no blazer",
	"charcoal herringbone button-up, open collar, no blazer",
	"ink navy poplin button-up, open collar, no blazer",
	"deep espresso button-up, open collar, no blazer",
	"near-black button-up, open collar, no blazer",
	"black micro-texture button-up, open collar, no blazer",
	"deep slate button-up, open collar, no blazer",
	"deep navy textured button-up, open collar, unstructured dark blazer",
	"dark graphite micro-pattern button-up, open collar, soft knit blazer",
	"dark slate button-up, open collar, open blazer with subtle texture",
	"black button-up with subtle sheen, open collar, slim dark blazer",
	"charcoal button-up with thin pinstripe, open collar, open blazer",
	"midnight-blue button-up, open collar, relaxed dark blazer",
	"dark espresso button-up, open collar, tailored black blazer",
	"deep charcoal twill button-up, open collar, structured dark blazer",
	"black micro-texture button-up, open collar, clean dark blazer",
	"midnight navy button-up, open collar, matte charcoal blazer",
	"graphite button-up, open collar, dark windowpane blazer",
	"deep slate button-up, open collar, minimalist black blazer",
	"black poplin button-up, open collar, dark subtle-check blazer",
	"charcoal oxford button-up, open collar, slim dark blazer",
	"dark navy button-up, open collar, soft-structured black blazer",
	"near-black button-up, open collar, clean dark blazer",
];
const NO_BLAZER_PATTERN = /\bno blazer\b/i;
const FORMAL_CATEGORY_KEYWORDS = new Set([
	"politics",
	"world",
	"health",
	"social",
	"socialissues",
	"crime",
	"law",
	"justice",
	"government",
	"public safety",
]);
const FORMAL_CONTEXT_PHRASES = [
	"official statement",
	"official report",
	"official announcement",
	"press conference",
	"court",
	"trial",
	"verdict",
	"sentenced",
	"indictment",
	"charged",
	"arrest",
	"lawsuit",
	"sued",
	"investigation",
	"police",
	"government",
	"parliament",
	"congress",
	"senate",
	"white house",
	"policy",
	"regulation",
	"minister",
	"president",
	"prime minister",
	"military",
	"war",
	"conflict",
];
const SERIOUS_CONTEXT_TOKENS = Array.from(
	new Set([
		...(EXPLICIT_SERIOUS_CUES || []),
		"funeral",
		"shooting",
		"murder",
		"assault",
		"violence",
		"cancer",
		"illness",
		"crash",
		"fatal",
		"fatalities",
		"victim",
		"victims",
		"hospitalized",
		"critical",
		"emergency",
	]),
).map((token) => String(token || "").toLowerCase());

const openai = process.env.CHATGPT_API_TOKEN
	? new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN })
	: null;

function createLogger(log, jobId) {
	const prefix = `[presenter_adjustments${jobId ? `:${jobId}` : ""}]`;
	return (message, data = null) => {
		try {
			if (typeof log === "function") log(message, data || {});
		} catch {}
		try {
			if (data && Object.keys(data).length) {
				console.log(prefix, message, data);
			} else {
				console.log(prefix, message);
			}
		} catch {}
	};
}

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

function readFileHeader(filePath, bytes = 16) {
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

function detectImageType(filePath) {
	const head = readFileHeader(filePath, 12);
	if (!head || head.length < 4) return null;
	if (
		head[0] === 0x89 &&
		head[1] === 0x50 &&
		head[2] === 0x4e &&
		head[3] === 0x47
	)
		return "png";
	if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff) return "jpg";
	if (
		head.toString("ascii", 0, 4) === "RIFF" &&
		head.toString("ascii", 8, 12) === "WEBP"
	)
		return "webp";
	return null;
}

function parsePngSize(buf) {
	if (!buf || buf.length < 24) return null;
	if (buf[0] !== 0x89 || buf[1] !== 0x50 || buf[2] !== 0x4e || buf[3] !== 0x47)
		return null;
	const width = buf.readUInt32BE(16);
	const height = buf.readUInt32BE(20);
	return width && height ? { width, height } : null;
}

function parseJpegSize(buf) {
	if (!buf || buf.length < 4) return null;
	if (buf[0] !== 0xff || buf[1] !== 0xd8) return null;
	let i = 2;
	while (i + 1 < buf.length) {
		if (buf[i] !== 0xff) {
			i += 1;
			continue;
		}
		while (buf[i] === 0xff) i += 1;
		const marker = buf[i];
		i += 1;
		if (marker === 0xd9 || marker === 0xda) break;
		if (i + 1 >= buf.length) break;
		const len = buf.readUInt16BE(i);
		if (len < 2) break;
		if (
			(marker >= 0xc0 && marker <= 0xc3) ||
			(marker >= 0xc5 && marker <= 0xc7) ||
			(marker >= 0xc9 && marker <= 0xcb) ||
			(marker >= 0xcd && marker <= 0xcf)
		) {
			if (i + 7 >= buf.length) break;
			const height = buf.readUInt16BE(i + 3);
			const width = buf.readUInt16BE(i + 5);
			return width && height ? { width, height } : null;
		}
		i += len;
	}
	return null;
}

function parseWebpSize(buf) {
	if (!buf || buf.length < 30) return null;
	if (
		buf.toString("ascii", 0, 4) !== "RIFF" ||
		buf.toString("ascii", 8, 12) !== "WEBP"
	)
		return null;
	const chunk = buf.toString("ascii", 12, 16);
	if (chunk === "VP8X" && buf.length >= 30) {
		const width = 1 + buf.readUIntLE(24, 3);
		const height = 1 + buf.readUIntLE(27, 3);
		return width && height ? { width, height } : null;
	}
	if (chunk === "VP8 " && buf.length >= 30) {
		const width = buf.readUInt16LE(26) & 0x3fff;
		const height = buf.readUInt16LE(28) & 0x3fff;
		return width && height ? { width, height } : null;
	}
	if (chunk === "VP8L" && buf.length >= 25) {
		const b0 = buf[21];
		const b1 = buf[22];
		const b2 = buf[23];
		const b3 = buf[24];
		const width = 1 + ((b0 | ((b1 & 0x3f) << 8)) >>> 0);
		const height = 1 + (((b1 & 0xc0) >> 6) | (b2 << 2) | ((b3 & 0x0f) << 10));
		return width && height ? { width, height } : null;
	}
	return null;
}

function fallbackImageDimensions(filePath) {
	const kind = detectImageType(filePath);
	const head = readFileHeader(filePath, 65536);
	if (!head) return { width: 0, height: 0 };
	if (kind === "png") return parsePngSize(head) || { width: 0, height: 0 };
	if (kind === "jpg") return parseJpegSize(head) || { width: 0, height: 0 };
	if (kind === "webp") return parseWebpSize(head) || { width: 0, height: 0 };
	const png = parsePngSize(head);
	if (png) return png;
	const jpg = parseJpegSize(head);
	if (jpg) return jpg;
	const webp = parseWebpSize(head);
	if (webp) return webp;
	return { width: 0, height: 0 };
}

function resolveFfprobePath() {
	let ffprobePath = "ffprobe";
	if (ffmpegPath) {
		const candidate = ffmpegPath.replace(/ffmpeg(\.exe)?$/i, "ffprobe$1");
		if (candidate && candidate !== ffmpegPath) ffprobePath = candidate;
	}
	return ffprobePath;
}

function ffprobeDimensions(filePath) {
	try {
		const ffprobePath = resolveFfprobePath();
		const out = child_process
			.execSync(
				`"${ffprobePath}" -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x "${filePath}"`,
				{ stdio: ["ignore", "pipe", "ignore"] },
			)
			.toString()
			.trim();
		const [w, h] = out.split("x").map((n) => Number(n) || 0);
		return { width: w || 0, height: h || 0 };
	} catch {
		return { width: 0, height: 0 };
	}
}

function getImageDimensions(filePath) {
	const dims = ffprobeDimensions(filePath);
	if (dims.width && dims.height) return dims;
	const fallback = fallbackImageDimensions(filePath);
	if (fallback.width && fallback.height) return fallback;
	return dims;
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

function runFfmpegText(args, label = "ffmpeg_text") {
	if (!ffmpegPath) throw new Error("ffmpeg not available");
	const res = child_process.spawnSync(ffmpegPath, args, {
		encoding: "utf8",
		windowsHide: true,
	});
	if (res.status === 0) {
		return {
			stdout: String(res.stdout || ""),
			stderr: String(res.stderr || ""),
		};
	}
	const err = String(res.stderr || "").slice(0, 4000);
	throw new Error(`${label} failed (code ${res.status}): ${err}`);
}

function computeImageHash(filePath, regionPct = null) {
	if (!ffmpegPath) return null;
	const dims = getImageDimensions(filePath);
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

function buildCropFilter(regionPct, dims) {
	if (!dims || !dims.width || !dims.height) return null;
	const rx = Math.max(0, Math.round(dims.width * (regionPct.x || 0)));
	const ry = Math.max(0, Math.round(dims.height * (regionPct.y || 0)));
	const rw = Math.max(2, Math.round(dims.width * (regionPct.w || 1)));
	const rh = Math.max(2, Math.round(dims.height * (regionPct.h || 1)));
	return `crop=${rw}:${rh}:${rx}:${ry}`;
}

function parseSsimOutput(text) {
	const match = String(text || "").match(/All:([0-9.]+)/i);
	if (!match) return null;
	const value = Number(match[1]);
	return Number.isFinite(value) ? value : null;
}

function computeRegionSsim(
	fileA,
	fileB,
	regionPct,
	{ edgeDetect = false } = {},
) {
	if (!ffmpegPath) return null;
	const dims = getImageDimensions(fileA);
	if (!dims.width || !dims.height) return null;
	const crop = buildCropFilter(regionPct, dims);
	if (!crop) return null;
	const edge = edgeDetect ? ",edgedetect=low=0.1:high=0.4" : "";
	const filter = [
		`[0:v]${crop},format=gray${edge}[a]`,
		`[1:v]${crop},format=gray${edge}[b]`,
		`[a][b]ssim`,
	].join(";");
	const args = [
		"-hide_banner",
		"-loglevel",
		"info",
		"-i",
		fileA,
		"-i",
		fileB,
		"-filter_complex",
		filter,
		"-f",
		"null",
		"-",
	];
	try {
		const res = runFfmpegText(args, "presenter_ssim");
		const ssim = parseSsimOutput(res.stderr || res.stdout || "");
		return ssim;
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

function comparePresenterSimilarityDetailed(originalPath, candidatePath) {
	const regions = {
		eyes: PRESENTER_EYES_REGION,
		face: PRESENTER_FACE_REGION,
		chin: PRESENTER_CHIN_REGION,
		upper: PRESENTER_UPPER_REGION,
	};
	const scores = {};
	const values = [];
	for (const [name, region] of Object.entries(regions)) {
		const a = computeImageHash(originalPath, region);
		const b = computeImageHash(candidatePath, region);
		const score = hashSimilarity(a, b);
		if (Number.isFinite(score)) {
			scores[name] = score;
			values.push(score);
		}
	}
	const average = values.length
		? values.reduce((acc, v) => acc + v, 0) / values.length
		: null;
	return { scores, average };
}

function comparePresenterSimilarity(originalPath, candidatePath) {
	const result = comparePresenterSimilarityDetailed(
		originalPath,
		candidatePath,
	);
	return result.average;
}

function ensurePresenterFile(filePath) {
	if (!filePath || !fs.existsSync(filePath))
		throw new Error("presenter_image_missing");
	const st = fs.statSync(filePath);
	if (!st || st.size < PRESENTER_MIN_BYTES)
		throw new Error("presenter_image_too_small");
	const kind = detectImageType(filePath);
	if (!kind) throw new Error("presenter_image_invalid");
	return filePath;
}

function buildTopicLine({ title, topics = [] }) {
	const topicLine = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join(" / ")
		: "";
	const raw = [title, topicLine].filter(Boolean).join(" | ");
	return String(raw || topicLine || title || "the topic").slice(0, 220);
}

function hashStringToInt(value = "") {
	let hash = 0;
	const str = String(value || "");
	for (let i = 0; i < str.length; i++) {
		hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
	}
	return hash;
}

function buildWardrobeContextText({ title, topics, categoryLabel }) {
	const topicText = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join(" ")
		: "";
	return `${title || ""} ${topicText} ${categoryLabel || ""}`
		.trim()
		.toLowerCase();
}

function isFormalCategoryLabel(label = "") {
	const normalized = String(label || "")
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, " ")
		.trim();
	if (!normalized) return false;
	for (const keyword of FORMAL_CATEGORY_KEYWORDS) {
		if (normalized.includes(keyword)) return true;
	}
	return false;
}

function isFormalContext({ title, topics, categoryLabel }) {
	const context = buildWardrobeContextText({ title, topics, categoryLabel });
	if (!context) return false;
	if (isFormalCategoryLabel(categoryLabel)) return true;
	if (SERIOUS_CONTEXT_TOKENS.some((token) => token && context.includes(token)))
		return true;
	if (FORMAL_CONTEXT_PHRASES.some((phrase) => context.includes(phrase)))
		return true;
	return false;
}

function pickWardrobeVariant({
	jobId,
	title,
	topics,
	categoryLabel,
	avoidOutfits = [],
}) {
	const topicText = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join("|")
		: "";
	const normalizedAvoid = new Set(
		(avoidOutfits || [])
			.map((v) =>
				String(v || "")
					.trim()
					.toLowerCase(),
			)
			.filter(Boolean),
	);
	const prefersFormal = isFormalContext({ title, topics, categoryLabel });
	const noBlazerVariants = WARDROBE_VARIANTS.filter((variant) =>
		NO_BLAZER_PATTERN.test(variant),
	);
	const blazerVariants = WARDROBE_VARIANTS.filter(
		(variant) => !NO_BLAZER_PATTERN.test(variant),
	);
	const primaryPool = prefersFormal ? blazerVariants : noBlazerVariants;
	const candidates = primaryPool.filter(
		(v) => !normalizedAvoid.has(String(v).toLowerCase()),
	);
	const fallbackPool = WARDROBE_VARIANTS.filter(
		(v) => !normalizedAvoid.has(String(v).toLowerCase()),
	);
	const pool = candidates.length
		? candidates
		: fallbackPool.length
			? fallbackPool
			: WARDROBE_VARIANTS;
	const jitter = `${Date.now()}-${Math.random()}`;
	const seed = `${jobId || ""}|${title || ""}|${topicText}|${jitter}`;
	const idx = hashStringToInt(seed) % pool.length;
	return pool[idx] || WARDROBE_VARIANTS[0];
}

function buildStrictWardrobePrompt({ wardrobeVariant }) {
	return `
Use @presenter_ref as the ONLY source.

LOCK IDENTITY + GEOMETRY:
- Face/head/neck/hair/skin/glasses/expression: unchanged.
- Head size/position and head-to-shoulder ratio: unchanged.
- Torso anatomy/silhouette (shoulders/chest/waist/posture): unchanged.
- No pose change. No move/warp/stretch/slim/widen.
- Scene unchanged: framing/crop/camera/lighting/background/desk/props.
- No retouching/stylization/text/logos/watermarks/new objects.

ONLY EDIT:
- Change ONLY upper-body clothing to: ${wardrobeVariant}.
- Clothing must be dark, elegant, open-collar, intact (no rips/tears/holes), straight placket, aligned buttons.
- No accessories.

QUALITY:
- Single natural face; no double-face, ghosting, blur, or artifacts.
- If any non-clothing pixel would change, output original unchanged presenter.
`.trim();
}

function fallbackWardrobePrompt({ wardrobeVariant }) {
	return buildStrictWardrobePrompt({ wardrobeVariant });
}

function parseJsonObject(text = "") {
	const raw = String(text || "").trim();
	if (!raw) return null;
	try {
		return JSON.parse(raw);
	} catch {}
	const start = raw.indexOf("{");
	const end = raw.lastIndexOf("}");
	if (start >= 0 && end > start) {
		const slice = raw.slice(start, end + 1);
		try {
			return JSON.parse(slice);
		} catch {}
	}
	return null;
}

async function buildOrchestratedPrompts({
	jobId,
	title,
	topics,
	categoryLabel,
	avoidOutfits = [],
	log,
}) {
	const topicLine = buildTopicLine({ title, topics });
	const wardrobeVariant = pickWardrobeVariant({
		jobId,
		title,
		topics,
		categoryLabel,
		avoidOutfits,
	});
	const strictPrompt = buildStrictWardrobePrompt({ wardrobeVariant });
	if (log)
		log("wardrobe variation selected", {
			variant: wardrobeVariant,
			avoided: (avoidOutfits || []).length,
			formalContext: isFormalContext({ title, topics, categoryLabel }),
		});
	if (log)
		log("wardrobe strict prompt", {
			wardrobe: strictPrompt.slice(0, 300),
		});
	if (PRESENTER_STRICT_PROMPT_ONLY || !openai) {
		return {
			wardrobePrompt: strictPrompt,
			wardrobeVariant,
		};
	}

	const system = `
You write precise prompts for Runway text_to_image with model gen4_image.
Return JSON only with key: wardrobePrompt.
	Rules:
	- Use @presenter_ref as the only source.
	- Lock face/head/neck/hair/skin/glasses/expression exactly.
	- Lock torso anatomy and silhouette exactly (shoulders/chest/waist/posture).
	- Do not change pose, geometry, crop, camera, lighting, background, desk, or props.
	- Wardrobe: apply the provided wardrobe variation cue exactly. If the cue says "no blazer", do not add a blazer.
	- Outfit must be intact: no rips/tears/holes; straight placket; aligned buttons; dark elegant tones; open collar.
	- The ONLY allowed edit is upper-body clothing. No accessories, no text/logos/watermarks, no extra objects.
	- Keep it concise and direct.
`.trim();

	const userText = `
Title: ${String(title || "").trim()}
Topics: ${topicLine}
	Category: ${String(categoryLabel || "").trim()}
	Wardrobe variation cue (use exactly): ${wardrobeVariant}
	Output JSON only.
`.trim();

	try {
		const resp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [
				{ role: "system", content: system },
				{
					role: "user",
					content: [
						{ type: "text", text: userText },
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_PRESENTER_REF_URL },
						},
					],
				},
			],
			temperature: 0.4,
			max_completion_tokens: 500,
		});
		const content = String(resp?.choices?.[0]?.message?.content || "").trim();
		const parsed = parseJsonObject(content);
		if (parsed && parsed.wardrobePrompt) {
			const integrityLine =
				"Outfit must be intact: no rips, tears, holes, or missing fabric; shirt placket straight and buttons aligned.";
			const promptBase = String(parsed.wardrobePrompt).trim();
			const promptWithIntegrity = promptBase.toLowerCase().includes("rips")
				? promptBase
				: `${promptBase}\n${integrityLine}`;
			const result = {
				wardrobePrompt: `${strictPrompt}\n${promptWithIntegrity}`,
				wardrobeVariant,
			};
			if (log)
				log("orchestrator prompts", {
					wardrobe: result.wardrobePrompt.slice(0, 300),
				});
			return result;
		}
	} catch (e) {
		if (log)
			log("prompt orchestrator failed; using fallback", {
				error: e?.message || String(e),
			});
	}

	const fallback = {
		wardrobePrompt: fallbackWardrobePrompt({
			wardrobeVariant,
		}),
		wardrobeVariant,
	};
	if (log)
		log("orchestrator prompts (fallback)", {
			wardrobe: fallback.wardrobePrompt.slice(0, 300),
		});
	return fallback;
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
		},
	);
	if (init.status >= 300) {
		const msg =
			typeof init.data === "string"
				? init.data
				: JSON.stringify(init.data || {});
		throw new Error(
			`Runway upload init failed (${init.status}): ${msg.slice(0, 500)}`,
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
		"Runway upload requires Node 18+ (fetch/FormData) or install 'form-data'",
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
				`${label} polling failed (${res.status}): ${msg.slice(0, 500)}`,
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
				`${label} failed: ${data.failureCode || data.error || "FAILED"}`,
			);
		}
	}
	throw new Error(`${label} timed out`);
}

async function runwayTextToImage({ promptText, referenceImages, ratio }) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	const payload = {
		model: RUNWAY_IMAGE_MODEL,
		promptText: String(promptText || "").slice(0, 1000),
		ratio: String(ratio || "1920:1080"),
		...(Array.isArray(referenceImages) && referenceImages.length
			? { referenceImages }
			: {}),
	};

	const res = await axios.post(
		"https://api.dev.runwayml.com/v1/text_to_image",
		payload,
		{
			headers: runwayHeadersJson(),
			timeout: 30000,
			validateStatus: (s) => s < 500,
		},
	);

	if (res.status >= 300 || !res.data?.id) {
		const msg =
			typeof res.data === "string" ? res.data : JSON.stringify(res.data || {});
		throw new Error(
			`Runway text_to_image failed (${res.status}): ${msg.slice(0, 700)}`,
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

function assertCloudinaryReady() {
	if (
		!process.env.CLOUDINARY_CLOUD_NAME ||
		!process.env.CLOUDINARY_API_KEY ||
		!process.env.CLOUDINARY_API_SECRET
	) {
		throw new Error(
			"Cloudinary credentials missing (CLOUDINARY_CLOUD_NAME/API_KEY/API_SECRET).",
		);
	}
	cloudinary.config({
		cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
		api_key: process.env.CLOUDINARY_API_KEY,
		api_secret: process.env.CLOUDINARY_API_SECRET,
	});
}

async function uploadPresenterToCloudinary(filePath, jobId, prefix) {
	assertCloudinaryReady();
	const publicId = `${prefix}_${jobId}_${Date.now()}`;
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

async function deleteCloudinaryAsset(publicId, log) {
	if (!publicId) return;
	assertCloudinaryReady();
	try {
		await cloudinary.uploader.destroy(publicId, { resource_type: "image" });
		if (log)
			log("cloudinary asset removed", {
				publicId,
			});
	} catch (e) {
		if (log)
			log("cloudinary asset delete failed", {
				publicId,
				error: e?.message || String(e),
			});
	}
}

async function generateRunwayOutfitStage({
	jobId,
	tmpDir,
	presenterLocalPath,
	wardrobePrompt,
	log,
}) {
	if (log)
		log("runway upload start", {
			file: path.basename(presenterLocalPath || ""),
		});
	const presenterUri = await runwayCreateEphemeralUpload({
		filePath: presenterLocalPath,
		filename: path.basename(presenterLocalPath),
	});
	if (log)
		log("runway wardrobe prompt", {
			prompt: String(wardrobePrompt || "").slice(0, 200),
		});
	const baseDims = getImageDimensions(presenterLocalPath);
	const ratio =
		baseDims.width && baseDims.height
			? `${baseDims.width}:${baseDims.height}`
			: "1920:1080";
	if (log)
		log("runway ratio", {
			ratio,
		});
	const outputUri = await runwayTextToImage({
		promptText: wardrobePrompt,
		referenceImages: [{ uri: presenterUri, tag: "presenter_ref" }],
		ratio,
	});
	const outPath = path.join(tmpDir, `presenter_outfit_${jobId}.png`);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	if (log)
		log("runway output downloaded", {
			outPath,
		});
	return outPath;
}

async function generatePresenterAdjustedImage({
	jobId,
	tmpDir,
	presenterLocalPath,
	title,
	topics = [],
	categoryLabel,
	recentOutfits = [],
	log,
}) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	if (!presenterLocalPath || !fs.existsSync(presenterLocalPath))
		throw new Error("presenter_base_missing");

	const workingDir = tmpDir || path.join(os.tmpdir(), "presenter_adjustments");
	const logger = createLogger(log, jobId);
	ensureDir(workingDir);
	ensurePresenterFile(presenterLocalPath);
	logger("presenter adjust start", {
		workingDir,
	});

	const attemptedOutfits = [];
	let lastError = null;

	for (let attempt = 0; attempt < PRESENTER_WARDROBE_ATTEMPTS; attempt++) {
		const prompts = await buildOrchestratedPrompts({
			jobId,
			title,
			topics,
			categoryLabel,
			avoidOutfits: [...(recentOutfits || []), ...attemptedOutfits],
			log: logger,
		});
		const presenterOutfit = String(prompts.wardrobeVariant || "").trim();
		if (presenterOutfit) attemptedOutfits.push(presenterOutfit);

		logger("presenter attempt", {
			attempt: attempt + 1,
			outfit: presenterOutfit || "unknown",
		});

		let outfitPath = null;
		try {
			outfitPath = await generateRunwayOutfitStage({
				jobId,
				tmpDir: workingDir,
				presenterLocalPath,
				wardrobePrompt: prompts.wardrobePrompt,
				log: logger,
			});
			ensurePresenterFile(outfitPath);

			const baseDims = getImageDimensions(presenterLocalPath);
			const outDims = getImageDimensions(outfitPath);
			logger("presenter dimensions", {
				base: baseDims,
				candidate: outDims,
			});
			if (
				!baseDims.width ||
				!baseDims.height ||
				!outDims.width ||
				!outDims.height
			) {
				throw new Error("presenter_dimensions_unavailable");
			}
			if (
				PRESENTER_DIMENSIONS_MUST_MATCH &&
				baseDims.width &&
				baseDims.height &&
				outDims.width &&
				outDims.height &&
				(baseDims.width !== outDims.width || baseDims.height !== outDims.height)
			) {
				throw new Error("presenter_dimensions_mismatch");
			}

			const similarity = comparePresenterSimilarityDetailed(
				presenterLocalPath,
				outfitPath,
			);
			const phashPass =
				Number.isFinite(similarity.average) &&
				(!Number.isFinite(similarity.scores.eyes) ||
					similarity.scores.eyes >= PRESENTER_FACE_SIMILARITY_MIN) &&
				(!Number.isFinite(similarity.scores.face) ||
					similarity.scores.face >= PRESENTER_FACE_SIMILARITY_MIN) &&
				(!Number.isFinite(similarity.scores.chin) ||
					similarity.scores.chin >= PRESENTER_FACE_SIMILARITY_MIN) &&
				(!Number.isFinite(similarity.scores.upper) ||
					similarity.scores.upper >= PRESENTER_UPPER_SIMILARITY_MIN) &&
				similarity.average >= PRESENTER_OVERALL_SIMILARITY_MIN;
			logger("presenter phash", {
				average: similarity.average,
				scores: similarity.scores,
				thresholds: {
					face: PRESENTER_FACE_SIMILARITY_MIN,
					upper: PRESENTER_UPPER_SIMILARITY_MIN,
					overall: PRESENTER_OVERALL_SIMILARITY_MIN,
				},
				pass: phashPass,
			});
			if (!Number.isFinite(similarity.average)) {
				throw new Error("presenter_similarity_unavailable");
			}

			let ssimChecks = null;
			if (PRESENTER_SSIM_ENABLED) {
				ssimChecks = {
					eyes: computeRegionSsim(
						presenterLocalPath,
						outfitPath,
						PRESENTER_EYES_REGION,
					),
					face: computeRegionSsim(
						presenterLocalPath,
						outfitPath,
						PRESENTER_FACE_REGION,
					),
					mouth: computeRegionSsim(
						presenterLocalPath,
						outfitPath,
						PRESENTER_MOUTH_REGION,
					),
					geometry: computeRegionSsim(
						presenterLocalPath,
						outfitPath,
						PRESENTER_TORSO_GEOMETRY_REGION,
						{ edgeDetect: true },
					),
				};
				logger("presenter ssim", {
					scores: ssimChecks,
					thresholds: {
						eyes: PRESENTER_SSIM_EYES_MIN,
						face: PRESENTER_SSIM_FACE_MIN,
						mouth: PRESENTER_SSIM_MOUTH_MIN,
						geometry: PRESENTER_SSIM_GEOMETRY_MIN,
					},
				});

				if (
					!Number.isFinite(ssimChecks.eyes) ||
					!Number.isFinite(ssimChecks.face) ||
					!Number.isFinite(ssimChecks.mouth) ||
					!Number.isFinite(ssimChecks.geometry)
				) {
					throw new Error("presenter_ssim_unavailable");
				}
				if (ssimChecks.eyes < PRESENTER_SSIM_EYES_MIN) {
					throw new Error("presenter_ssim_eyes_too_low");
				}
				if (ssimChecks.face < PRESENTER_SSIM_FACE_MIN) {
					throw new Error("presenter_ssim_face_too_low");
				}
				if (ssimChecks.mouth < PRESENTER_SSIM_MOUTH_MIN) {
					throw new Error("presenter_ssim_mouth_too_low");
				}
				if (ssimChecks.geometry < PRESENTER_SSIM_GEOMETRY_MIN) {
					throw new Error("presenter_ssim_geometry_too_low");
				}
			}
			if (!phashPass) {
				logger("presenter phash warning (non-blocking)", {
					reason: "phash_below_threshold",
				});
			}

			const finalUpload = await uploadPresenterToCloudinary(
				outfitPath,
				jobId,
				PRESENTER_CLOUDINARY_PUBLIC_PREFIX,
			);

			logger("presenter upload complete", {
				url: finalUpload?.url || "",
				publicId: finalUpload?.public_id || "",
			});

			return {
				localPath: outfitPath,
				url: finalUpload?.url || "",
				publicId: finalUpload?.public_id || "",
				width: finalUpload?.width || 0,
				height: finalUpload?.height || 0,
				method: "runway_outfit_strict",
				presenterOutfit,
			};
		} catch (e) {
			lastError = e;
			logger("presenter attempt failed", {
				attempt: attempt + 1,
				error: e?.message || String(e),
			});
			if (outfitPath && outfitPath !== presenterLocalPath)
				safeUnlink(outfitPath);
		}
	}

	logger("presenter strict fallback to original", {
		error: lastError?.message || String(lastError || "unknown"),
	});
	if (PRESENTER_STRICT_FALLBACK_TO_ORIGINAL) {
		return {
			localPath: presenterLocalPath,
			url: "",
			publicId: "",
			width: 0,
			height: 0,
			method: "strict_fallback_original",
			presenterOutfit: "",
		};
	}
	throw lastError || new Error("presenter_adjustment_failed");
}

module.exports = {
	generatePresenterAdjustedImage,
};
