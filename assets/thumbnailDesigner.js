/** @format */

const fs = require("fs");
const path = require("path");
const child_process = require("child_process");
const axios = require("axios");
const { OpenAI, toFile } = require("openai");
const cloudinary = require("cloudinary").v2;
const { TOPIC_STOP_WORDS, GENERIC_TOPIC_TOKENS } = require("./utils");

let ffmpegPath = process.env.FFMPEG_PATH || "";
if (!ffmpegPath) {
	try {
		// eslint-disable-next-line import/no-extraneous-dependencies
		ffmpegPath = require("ffmpeg-static");
	} catch {
		ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
	}
}

const DEFAULT_IMAGE_MODEL = "gpt-image-1";
const DEFAULT_IMAGE_SIZE = "1536x1024";
const DEFAULT_IMAGE_QUALITY = "high";
const DEFAULT_IMAGE_INPUT_FIDELITY = "high";
const MAX_INPUT_IMAGES = 4;
const DEFAULT_CANVAS_WIDTH = 1280;
const DEFAULT_CANVAS_HEIGHT = 720;
const LEFT_PANEL_PCT = 0.48;
const PANEL_MARGIN_PCT = 0.035;
const PRESENTER_OVERLAP_PCT = 0.06;

const THUMBNAIL_RATIO = "1280:720";
const THUMBNAIL_WIDTH = 1280;
const THUMBNAIL_HEIGHT = 720;
const THUMBNAIL_TEXT_MAX_WORDS = 3;
const THUMBNAIL_TEXT_BASE_MAX_CHARS = 12;
const THUMBNAIL_TEXT_BOX_WIDTH_PCT = 0.38;
const THUMBNAIL_TEXT_BOX_OPACITY = 0.28;
const THUMBNAIL_TEXT_MARGIN_PCT = 0.05;
const THUMBNAIL_TEXT_SIZE_PCT = 0.12;
const THUMBNAIL_TEXT_LINE_SPACING_PCT = 0.2;
const THUMBNAIL_TEXT_Y_OFFSET_PCT = 0.22;
const THUMBNAIL_TOPIC_MAX_IMAGES = 1;
const THUMBNAIL_TOPIC_MIN_EDGE = 900;
const THUMBNAIL_TOPIC_MIN_BYTES = 60000;
const THUMBNAIL_TOPIC_MAX_DOWNLOADS = 6;
const THUMBNAIL_MIN_BYTES = 12000;
const THUMBNAIL_CLOUDINARY_FOLDER = "aivideomatic/long_thumbnails";
const THUMBNAIL_CLOUDINARY_PUBLIC_PREFIX = "long_thumb";

const GOOGLE_CSE_ID = process.env.GOOGLE_CSE_ID || null;
const GOOGLE_CSE_KEY = process.env.GOOGLE_CSE_KEY || null;
const GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1";
const CSE_PREFERRED_IMG_SIZE = "xlarge";
const CSE_FALLBACK_IMG_SIZE = "large";
const CSE_PREFERRED_IMG_COLOR = "color";
const CSE_MIN_IMAGE_SHORT_EDGE = 720;
const REQUIRE_THUMBNAIL_TOPIC_IMAGES =
	String(process.env.REQUIRE_THUMBNAIL_TOPIC_IMAGES ?? "true").toLowerCase() !==
	"false";

const WATERMARK_URL_TOKENS = [
	"gettyimages",
	"getty",
	"alamy",
	"shutterstock",
	"istock",
	"istockphoto",
	"adobestock",
	"depositphotos",
	"dreamstime",
	"123rf",
	"bigstock",
	"bigstockphoto",
	"fotolia",
	"pond5",
	"envato",
	"stockphoto",
	"stockphotography",
	"imagebroker",
	"imago-images",
	"historicimages",
	"historic-images",
	"wireimage",
	"pressphoto",
	"newscom",
	"pixelsquid",
	"watermark",
];

const SORA_MODEL = process.env.SORA_MODEL || "sora-2";
const SORA_THUMBNAIL_ENABLED =
	String(process.env.SORA_THUMBNAIL_ENABLED ?? "true").toLowerCase() !==
	"false";
const SORA_THUMBNAIL_SECONDS = String(
	process.env.SORA_THUMBNAIL_SECONDS || "4"
);
const SORA_POLL_INTERVAL_MS = 2000;
const SORA_MAX_POLL_ATTEMPTS = 120;
const SORA_PROMPT_CHAR_LIMIT = 320;

function cleanThumbnailText(text = "") {
	return String(text || "")
		.replace(/([a-z])([A-Z])/g, "$1 $2")
		.replace(/["'`]/g, "")
		.replace(/[^a-z0-9\s]/gi, " ")
		.replace(/\s+/g, " ")
		.trim();
}

function sanitizeThumbnailContext(text = "") {
	const banned = [
		"death",
		"dead",
		"died",
		"dies",
		"killed",
		"shooting",
		"murder",
		"suicide",
		"funeral",
		"tragedy",
		"tragic",
		"memorial",
		"accident",
		"crash",
		"assault",
		"abuse",
		"lawsuit",
		"arrest",
		"charged",
		"trial",
		"court",
		"injury",
		"hospital",
	];
	let cleaned = String(text || "");
	for (const word of banned) {
		cleaned = cleaned.replace(new RegExp(`\\b${word}\\b`, "gi"), "");
	}
	return cleaned.replace(/\s+/g, " ").trim();
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

function clampNumber(val, min, max) {
	const n = Number(val);
	if (!Number.isFinite(n)) return min;
	if (n < min) return min;
	if (n > max) return max;
	return n;
}

function tokenizeLabel(text = "") {
	return String(text || "")
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, " ")
		.split(/\s+/)
		.filter(Boolean)
		.filter((t) => t.length >= 2 && !/^\d+$/.test(t));
}

function topicTokensFromTitle(title = "") {
	return tokenizeLabel(title || "").filter((t) => !TOPIC_STOP_WORDS.has(t));
}

function normalizeTopicTokens(tokens = []) {
	return Array.from(
		new Set(
			(tokens || [])
				.map((t) =>
					String(t || "")
						.toLowerCase()
						.trim()
				)
				.filter(Boolean)
		)
	);
}

function filterSpecificTopicTokens(tokens = []) {
	const norm = normalizeTopicTokens(tokens);
	const filtered = norm.filter(
		(t) => t.length >= 3 && !GENERIC_TOPIC_TOKENS.has(t)
	);
	return filtered.length ? filtered : norm;
}

const CONTEXT_STOP_TOKENS = new Set([
	...TOPIC_STOP_WORDS,
	"what",
	"know",
	"known",
	"why",
	"how",
	"reports",
	"report",
	"reported",
	"dies",
	"died",
	"death",
	"dead",
	"age",
	"aged",
	"year",
	"years",
	"says",
	"said",
	"say",
	"details",
	"return",
	"returns",
	"returning",
	"back",
	"revival",
	"reboot",
	"comeback",
	"cast",
	"watch",
	"watching",
	"hulu",
	"netflix",
	"prime",
	"amazon",
	"disney",
	"disneyplus",
	"hbo",
	"hbomax",
	"peacock",
	"paramount",
	"paramountplus",
	"apple",
]);

function filterContextTokens(tokens = []) {
	const norm = normalizeTopicTokens(tokens);
	const filtered = norm.filter(
		(t) =>
			t.length >= 3 &&
			!GENERIC_TOPIC_TOKENS.has(t) &&
			!CONTEXT_STOP_TOKENS.has(t)
	);
	return filtered.length ? filtered : [];
}

function minImageTokenMatches(tokens = []) {
	const norm = normalizeTopicTokens(tokens);
	if (!norm.length) return 0;
	if (norm.length >= 4) return 3;
	if (norm.length >= 2) return 2;
	return 1;
}

function topicMatchInfo(tokens = [], fields = []) {
	const norm = normalizeTopicTokens(tokens);
	if (!norm.length) return { count: 0, matchedTokens: [] };
	const hay = (fields || [])
		.map((f) => String(f || "").toLowerCase())
		.join(" ");
	const matchedTokens = norm.filter((tok) => hay.includes(tok));
	return { count: matchedTokens.length, matchedTokens };
}

function buildImageMatchCriteria(topic = "", extraTokens = []) {
	const rawTokens = tokenizeLabel(topic);
	const baseTokens = topicTokensFromTitle(topic);
	const wordSource = baseTokens.length >= 2 ? baseTokens : rawTokens;
	const specificWords = filterSpecificTopicTokens(wordSource);
	const wordTokens = specificWords.length ? specificWords : wordSource;
	const phraseToken = rawTokens.length >= 2 ? rawTokens.join(" ") : "";
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const wordSet = new Set(normalizeTopicTokens(wordTokens));
	const contextTokens = filterContextTokens(
		extra.filter((tok) => !wordSet.has(String(tok).toLowerCase()))
	);
	return {
		wordTokens,
		phraseToken,
		contextTokens: contextTokens.slice(0, 6),
		minWordMatches: minImageTokenMatches(wordTokens),
		rawTokenCount: rawTokens.length,
	};
}

function sanitizeOverlayQuery(query = "") {
	return String(query || "")
		.replace(/[^a-z0-9\s]/gi, " ")
		.replace(/\s+/g, " ")
		.trim()
		.slice(0, 80);
}

function uniqueStrings(list = [], { limit = 0 } = {}) {
	const seen = new Set();
	const out = [];
	for (const raw of Array.isArray(list) ? list : []) {
		const val = String(raw || "").trim();
		if (!val) continue;
		const key = val.toLowerCase();
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(val);
		if (limit && out.length >= limit) break;
	}
	return out;
}

async function fetchCseItems(
	queries,
	{ num = 4, searchType = null, imgSize = null, imgColorType = null } = {}
) {
	if (!GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) return [];
	const list = Array.isArray(queries) ? queries.filter(Boolean) : [];
	if (!list.length) return [];

	const results = [];
	const seen = new Set();

	for (const q of list) {
		try {
			const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
				params: {
					key: GOOGLE_CSE_KEY,
					cx: GOOGLE_CSE_ID,
					q,
					num,
					safe: "active",
					gl: "us",
					hl: "en",
					...(searchType ? { searchType } : {}),
					...(searchType === "image"
						? {
								imgType: "photo",
								imgSize: imgSize || CSE_PREFERRED_IMG_SIZE,
								...(imgColorType ? { imgColorType } : {}),
						  }
						: {}),
				},
				timeout: 12000,
				validateStatus: (s) => s < 500,
			});

			if (!data || data.error) continue;

			const items = Array.isArray(data?.items) ? data.items : [];
			for (const it of items) {
				const title = String(it.title || "").trim();
				const link = it.link || it.formattedUrl || "";
				if (!title || !link) continue;
				const key = `${title}|${link}`.toLowerCase();
				if (seen.has(key)) continue;
				seen.add(key);
				results.push({
					title: title.slice(0, 180),
					snippet: String(it.snippet || "")
						.trim()
						.slice(0, 260),
					link,
					image: it.image || null,
				});
			}
		} catch {
			// ignore
		}
	}
	return results;
}

function isLikelyWatermarkedSource(url = "", contextLink = "") {
	const hay = `${url} ${contextLink}`.toLowerCase();
	return WATERMARK_URL_TOKENS.some((token) => hay.includes(token));
}

function isProbablyDirectImageUrl(u) {
	const url = String(u || "").trim();
	if (!/^https?:\/\//i.test(url)) return false;
	return /\.(png|jpe?g|webp)(\?|#|$)/i.test(url);
}

async function headContentType(url, timeoutMs = 8000) {
	try {
		const res = await axios.head(url, {
			timeout: timeoutMs,
			validateStatus: (s) => s >= 200 && s < 400,
			headers: { "User-Agent": "agentai-thumbnail/1.0" },
		});
		const ct = String(res.headers?.["content-type"] || "").toLowerCase();
		return ct || null;
	} catch {
		return null;
	}
}

function parseMetaAttributes(tag = "") {
	const attrs = {};
	const re = /([a-zA-Z0-9:_-]+)\s*=\s*["']([^"']+)["']/g;
	let match = null;
	while ((match = re.exec(tag))) {
		const key = String(match[1] || "").toLowerCase();
		const val = String(match[2] || "").trim();
		if (key && val) attrs[key] = val;
	}
	return attrs;
}

function extractOpenGraphImage(html = "", baseUrl = "") {
	const metaTags = String(html || "").match(/<meta[^>]+>/gi) || [];
	const priority = [
		"og:image:secure_url",
		"og:image",
		"twitter:image:src",
		"twitter:image",
	];
	for (const key of priority) {
		for (const tag of metaTags) {
			const attrs = parseMetaAttributes(tag);
			const prop = attrs.property || attrs.name || "";
			if (!prop || prop.toLowerCase() !== key) continue;
			const content = attrs.content || "";
			if (!content) continue;
			try {
				const resolved = new URL(content, baseUrl);
				if (!/^https?:$/i.test(resolved.protocol)) continue;
				return resolved.toString();
			} catch {
				continue;
			}
		}
	}
	return "";
}

async function fetchOpenGraphImageUrl(pageUrl, timeoutMs = 9000) {
	try {
		if (!/^https?:\/\//i.test(pageUrl || "")) return null;
		const res = await axios.get(pageUrl, {
			timeout: timeoutMs,
			maxContentLength: 1024 * 1024,
			maxBodyLength: 1024 * 1024,
			headers: { "User-Agent": "agentai-thumbnail/1.0" },
			validateStatus: (s) => s >= 200 && s < 400,
		});
		const html = String(res.data || "");
		if (!html) return null;
		const og = extractOpenGraphImage(html, pageUrl);
		return og || null;
	} catch {
		return null;
	}
}

async function downloadToFile(url, outPath, timeoutMs = 20000, retries = 1) {
	let lastErr = null;
	for (let attempt = 0; attempt <= retries; attempt++) {
		try {
			const res = await axios.get(url, {
				responseType: "stream",
				timeout: timeoutMs,
				headers: { "User-Agent": "agentai-thumbnail/1.0" },
				validateStatus: (s) => s >= 200 && s < 400,
			});
			await new Promise((resolve, reject) => {
				const ws = fs.createWriteStream(outPath);
				res.data.pipe(ws);
				ws.on("finish", resolve);
				ws.on("error", reject);
			});
			const st = fs.statSync(outPath);
			if (!st || st.size < 256) throw new Error("downloaded file too small");
			return outPath;
		} catch (e) {
			lastErr = e;
			safeUnlink(outPath);
			if (attempt < retries) {
				await sleep(250 * Math.pow(2, attempt));
				continue;
			}
		}
	}
	throw lastErr || new Error("download failed");
}

function buildThumbnailPrompt({ title, topics, topicImageCount = 0 }) {
	const topicLine = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic)
				.filter(Boolean)
				.join(" / ")
		: "";
	const keywordLine = Array.isArray(topics)
		? topics
				.flatMap((t) => (Array.isArray(t.keywords) ? t.keywords : []))
				.filter(Boolean)
				.slice(0, 10)
				.join(", ")
		: "";
	const contextRaw = [title, topicLine]
		.filter(Boolean)
		.join(" | ")
		.slice(0, 240);
	const safeContext =
		sanitizeThumbnailContext(contextRaw) ||
		cleanThumbnailText(topicLine || title || "");
	const safeKeywords =
		sanitizeThumbnailContext(keywordLine) || cleanThumbnailText(keywordLine);
	const topicFocusRaw = [safeContext, safeKeywords].filter(Boolean).join(" | ");
	const topicFocus =
		topicFocusRaw || cleanThumbnailText(title || "") || "the topic";
	const topicImageLine =
		topicImageCount > 0
			? `Incorporate ${Math.min(
					2,
					Math.max(1, topicImageCount)
			  )} provided topic reference images as clean panels on the left side (soft shadow, slight depth, no clutter). Keep them clearly about ${topicFocus}.`
			: `Add subtle, tasteful visual cues related to: ${topicFocus}.`;

	return `
Create a YouTube thumbnail image (no text in the image).
Use the provided person reference; keep identity, face shape, and wardrobe consistent with the studio desk setup and lighting.
Composition: presenter on the right third (face and shoulders fully inside the right third), leave the left ~40% clean for headline text.
${topicImageLine}
Style: ultra sharp, clean, premium, high contrast, cinematic studio lighting, shallow depth of field, crisp subject separation.
Expression: confident, intrigued, camera-ready.
No candles, no logos, no watermarks, no extra people, no extra hands, no distortion, no text.
`.trim();
}

function buildSoraThumbnailPrompt({ title, topics }) {
	const topicLine = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic)
				.filter(Boolean)
				.join(" / ")
		: "";
	const keywordLine = Array.isArray(topics)
		? topics
				.flatMap((t) => (Array.isArray(t.keywords) ? t.keywords : []))
				.filter(Boolean)
				.slice(0, 10)
				.join(", ")
		: "";
	const contextRaw = [title, topicLine]
		.filter(Boolean)
		.join(" | ")
		.slice(0, 240);
	const safeContext =
		sanitizeThumbnailContext(contextRaw) ||
		cleanThumbnailText(topicLine || title || "");
	const safeKeywords =
		sanitizeThumbnailContext(keywordLine) || cleanThumbnailText(keywordLine);
	const topicFocusRaw = [safeContext, safeKeywords].filter(Boolean).join(" | ");
	const topicFocus =
		topicFocusRaw || cleanThumbnailText(title || "") || "the topic";

	const prompt = `
Cinematic studio background plate for a YouTube thumbnail.
No people, no faces, no text, no logos, no watermarks.
Left ~40% is clean, brighter, and ready for headline text + one hero image; no furniture or props on the left; smooth gradient backdrop.
Subtle topic-related props or atmosphere inspired by: ${topicFocus}.
High contrast, crisp detail, premium lighting, shallow depth of field, soft vignette, rich but tasteful color.
`.trim();

	return prompt.length > SORA_PROMPT_CHAR_LIMIT
		? prompt.slice(0, SORA_PROMPT_CHAR_LIMIT)
		: prompt;
}

function collectThumbnailInputImages({
	presenterLocalPath,
	candleLocalPath,
	topicImagePaths = [],
}) {
	const list = [];
	const add = (p) => {
		if (!p) return;
		if (!fs.existsSync(p)) return;
		if (!list.includes(p)) list.push(p);
	};
	add(presenterLocalPath);
	add(candleLocalPath);
	for (const p of topicImagePaths) add(p);
	return list.slice(0, MAX_INPUT_IMAGES);
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

	// PNG
	if (
		head[0] === 0x89 &&
		head[1] === 0x50 &&
		head[2] === 0x4e &&
		head[3] === 0x47
	)
		return { kind: "image", ext: "png" };

	// JPEG
	if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff)
		return { kind: "image", ext: "jpg" };

	// GIF
	if (ascii4 === "GIF8") return { kind: "image", ext: "gif" };

	// WEBP
	if (ascii4 === "RIFF" && ascii12.slice(8, 12) === "WEBP")
		return { kind: "image", ext: "webp" };

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
		// SOF markers
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
		return { width: 0, height: 0 };
	} catch {
		return { width: 0, height: 0 };
	}
}

function inferImageMime(filePath) {
	const head = readFileHeader(filePath, 12);
	if (head && head.length >= 4) {
		// PNG
		if (
			head[0] === 0x89 &&
			head[1] === 0x50 &&
			head[2] === 0x4e &&
			head[3] === 0x47
		)
			return "image/png";
		// JPEG
		if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff)
			return "image/jpeg";
		// WEBP
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

function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function makeEven(n) {
	const x = Math.round(Number(n) || 0);
	if (!Number.isFinite(x) || x <= 0) return 2;
	return x % 2 === 0 ? x : x + 1;
}

function soraSizeForRatio(ratio) {
	switch (ratio) {
		case "720:1280":
		case "832:1104":
			return "720x1280";
		default:
			return "1280x720";
	}
}

async function withRetries(fn, { retries = 1, baseDelayMs = 900, log } = {}) {
	let lastErr = null;
	for (let attempt = 0; attempt <= retries; attempt++) {
		try {
			return await fn(attempt);
		} catch (e) {
			lastErr = e;
			if (attempt >= retries) throw e;
			const delay = Math.round(
				baseDelayMs * Math.pow(2, attempt) + Math.random() * 150
			);
			if (log) log("thumbnail openai retry", { attempt: attempt + 1, delay });
			await sleep(delay);
		}
	}
	throw lastErr || new Error("thumbnail openai retry failed");
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

async function downloadUrlToFile(url, outPath) {
	const res = await axios.get(url, {
		responseType: "arraybuffer",
		timeout: 60000,
	});
	if (!res?.data) throw new Error("thumbnail download empty");
	fs.writeFileSync(outPath, res.data);
	return outPath;
}

async function extractFrameFromVideo({ videoPath, outPath }) {
	const ext = path.extname(outPath).toLowerCase();
	const useJpegQ = ext === ".jpg" || ext === ".jpeg";
	const args = ["-ss", "0.4", "-i", videoPath, "-frames:v", "1"];
	if (useJpegQ) {
		args.push("-q:v", "2");
	}
	args.push("-y", outPath);
	await runFfmpeg(args, "sora_thumbnail_frame");
	return outPath;
}

async function generateSoraThumbnailFrame({
	jobId,
	tmpDir,
	prompt,
	ratio = "1280:720",
	openai,
	model = SORA_MODEL,
	log,
}) {
	if (!SORA_THUMBNAIL_ENABLED) return null;
	const apiKey = process.env.CHATGPT_API_TOKEN;
	if (!apiKey) throw new Error("CHATGPT_API_TOKEN missing");
	const client = openai || new OpenAI({ apiKey });
	const size = soraSizeForRatio(ratio);

	if (log) log("thumbnail sora prompt", { prompt: prompt.slice(0, 200), size });

	let job = null;
	try {
		job = await client.videos.create({
			model,
			prompt,
			seconds: SORA_THUMBNAIL_SECONDS,
			size,
		});
	} catch (err) {
		if (log)
			log("thumbnail sora create failed", {
				message: err?.message,
				code: err?.code || err?.response?.data?.error?.code || null,
				status: err?.response?.status || null,
			});
		throw err;
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
				log("thumbnail sora poll failed", {
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
	const videoPath = path.join(tmpDir, `thumb_sora_${jobId}.mp4`);
	fs.writeFileSync(videoPath, buf);
	const framePath = path.join(tmpDir, `thumb_sora_${jobId}.png`);
	await extractFrameFromVideo({ videoPath, outPath: framePath });
	return framePath;
}

async function generateOpenAiThumbnailBase({
	jobId,
	tmpDir,
	prompt,
	imagePaths = [],
	openai,
	model = DEFAULT_IMAGE_MODEL,
	size = DEFAULT_IMAGE_SIZE,
	quality = DEFAULT_IMAGE_QUALITY,
	inputFidelity = DEFAULT_IMAGE_INPUT_FIDELITY,
	log,
}) {
	const cleanPrompt = String(prompt || "").trim();
	if (!cleanPrompt) throw new Error("thumbnail_prompt_missing");
	const apiKey = process.env.CHATGPT_API_TOKEN;
	if (!apiKey) throw new Error("CHATGPT_API_TOKEN missing");
	const client = openai || new OpenAI({ apiKey });

	const outPath = path.join(tmpDir, `thumb_openai_${jobId}.png`);

	const doRequest = async () => {
		if (imagePaths.length) {
			const inputs = [];
			for (const p of imagePaths) {
				try {
					const buf = fs.readFileSync(p);
					const mime = inferImageMime(p);
					if (!mime) continue;
					inputs.push(await toFile(buf, path.basename(p), { type: mime }));
				} catch {}
			}
			if (inputs.length) {
				return await client.images.edit({
					model,
					image: inputs.length === 1 ? inputs[0] : inputs,
					prompt: cleanPrompt,
					size,
					quality,
					output_format: "png",
					background: "auto",
					input_fidelity: inputFidelity,
				});
			}
		}
		return await client.images.generate({
			model,
			prompt: cleanPrompt,
			size,
			quality,
			output_format: "png",
			background: "auto",
		});
	};

	const resp = await withRetries(doRequest, {
		retries: 1,
		baseDelayMs: 900,
		log,
	});

	const image = resp?.data?.[0];
	if (image?.b64_json) {
		const buf = Buffer.from(String(image.b64_json), "base64");
		fs.writeFileSync(outPath, buf);
		return outPath;
	}
	if (image?.url) {
		await downloadUrlToFile(image.url, outPath);
		return outPath;
	}

	throw new Error("thumbnail_openai_empty");
}

async function composeThumbnailBase({
	baseImagePath,
	presenterImagePath,
	topicImagePaths = [],
	outPath,
	width = DEFAULT_CANVAS_WIDTH,
	height = DEFAULT_CANVAS_HEIGHT,
}) {
	const W = makeEven(Math.max(2, Math.round(width)));
	const H = makeEven(Math.max(2, Math.round(height)));
	const leftW = Math.max(1, Math.round(W * LEFT_PANEL_PCT));
	const margin = Math.max(4, Math.round(W * PANEL_MARGIN_PCT));
	const overlap = Math.max(0, Math.round(W * PRESENTER_OVERLAP_PCT));
	let presenterW = Math.max(2, W - leftW + overlap);
	presenterW = makeEven(Math.min(W, presenterW));
	const presenterX = Math.max(0, Math.min(W - presenterW, leftW - overlap));

	const topics = Array.isArray(topicImagePaths)
		? topicImagePaths.filter(Boolean).slice(0, 2)
		: [];
	const panelCount = topics.length;
	const panelW = makeEven(Math.max(2, leftW - margin * 2));
	const panelH =
		panelCount > 1
			? makeEven(Math.max(2, Math.round((H - margin * 3) / 2)))
			: makeEven(Math.max(2, H - margin * 2));
	const panelBorder = Math.max(4, Math.round(W * 0.004));
	const panelInnerW = makeEven(Math.max(2, panelW - panelBorder * 2));
	const panelInnerH = makeEven(Math.max(2, panelH - panelBorder * 2));

	const inputs = [baseImagePath, presenterImagePath, ...topics];
	const filters = [];
	filters.push(
		`[0:v]scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H}[base]`
	);
	filters.push(
		`[1:v]scale=${presenterW}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${presenterW}:${H}:(iw-ow)/2:(ih-oh)/2[presenter]`
	);

	let current = "[base]";

	if (panelCount >= 1) {
		const panel1Idx = 2;
		const panelY =
			panelCount > 1 ? margin : Math.max(0, Math.round((H - panelH) / 2));
		const panelCropX = "(iw-ow)/2";
		const panelCropY = "(ih-oh)/2";
		filters.push(
			`[${panel1Idx}:v]scale=${panelInnerW}:${panelInnerH}:force_original_aspect_ratio=increase:flags=lanczos,crop=${panelInnerW}:${panelInnerH}:${panelCropX}:${panelCropY}[panel1i]`
		);
		filters.push(
			`[panel1i]pad=${panelW}:${panelH}:${panelBorder}:${panelBorder}:color=black@0.35[panel1]`
		);
		filters.push(`${current}[panel1]overlay=${margin}:${panelY}[tmp1]`);
		current = "[tmp1]";
	}

	if (panelCount >= 2) {
		const panel2Idx = 3;
		const panel2Y = Math.max(0, margin * 2 + panelH);
		const panelCropX = "(iw-ow)/2";
		const panelCropY = "(ih-oh)/2";
		filters.push(
			`[${panel2Idx}:v]scale=${panelInnerW}:${panelInnerH}:force_original_aspect_ratio=increase:flags=lanczos,crop=${panelInnerW}:${panelInnerH}:${panelCropX}:${panelCropY}[panel2i]`
		);
		filters.push(
			`[panel2i]pad=${panelW}:${panelH}:${panelBorder}:${panelBorder}:color=black@0.35[panel2]`
		);
		filters.push(`${current}[panel2]overlay=${margin}:${panel2Y}[tmp2]`);
		current = "[tmp2]";
	}

	filters.push(`${current}[presenter]overlay=${presenterX}:0[outv]`);

	const outExt = path.extname(outPath).toLowerCase();
	const useJpegQ = outExt === ".jpg" || outExt === ".jpeg";
	const args = [];
	for (const input of inputs) {
		args.push("-i", input);
	}
	args.push(
		"-filter_complex",
		filters.join(";"),
		"-map",
		"[outv]",
		"-frames:v",
		"1"
	);
	if (useJpegQ) args.push("-q:v", "2");
	args.push("-y", outPath);

	await runFfmpeg(args, "thumbnail_compose");
	return outPath;
}

async function generateThumbnailCompositeBase({
	jobId,
	tmpDir,
	presenterImagePath,
	topicImagePaths = [],
	title,
	topics,
	ratio = "1280:720",
	width = DEFAULT_CANVAS_WIDTH,
	height = DEFAULT_CANVAS_HEIGHT,
	openai,
	log,
	useSora = true,
}) {
	if (!presenterImagePath)
		throw new Error("thumbnail_presenter_missing_or_invalid");

	let baseImagePath = presenterImagePath;
	if (useSora && SORA_THUMBNAIL_ENABLED) {
		const prompt = buildSoraThumbnailPrompt({ title, topics });
		try {
			const soraFrame = await generateSoraThumbnailFrame({
				jobId,
				tmpDir,
				prompt,
				ratio,
				openai,
				log,
			});
			if (soraFrame) baseImagePath = soraFrame;
		} catch (e) {
			if (log)
				log("thumbnail sora failed; using presenter base", {
					error: e.message,
				});
		}
	}

	const outPath = path.join(tmpDir, `thumb_composite_${jobId}.png`);
	return await composeThumbnailBase({
		baseImagePath,
		presenterImagePath,
		topicImagePaths,
		outPath,
		width,
		height,
	});
}

function escapeDrawtext(s = "") {
	const placeholder = "__NL__";
	return String(s || "")
		.replace(/\r\n|\r|\n/g, placeholder)
		.replace(/\\/g, "\\\\")
		.replace(/:/g, "\\:")
		.replace(/'/g, "\\'")
		.replace(/%/g, "\\%")
		.replace(/\[/g, "\\[")
		.replace(/\]/g, "\\]")
		.replace(new RegExp(placeholder, "g"), "\\\\n")
		.trim();
}

function resolveThumbnailFontFile() {
	const candidates = [
		"C:/Windows/Fonts/impact.ttf",
		"C:/Windows/Fonts/arialbd.ttf",
		"C:/Windows/Fonts/arialblack.ttf",
		"C:/Windows/Fonts/arial.ttf",
		"/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
		"/Library/Fonts/Arial Bold.ttf",
	];
	for (const p of candidates) {
		try {
			if (p && fs.existsSync(p)) return p;
		} catch {}
	}
	return "";
}

const THUMBNAIL_FONT_FILE = resolveThumbnailFontFile();

function hardTruncateText(text = "", maxChars = 40) {
	const t = String(text || "").trim();
	if (t.length <= maxChars) return t;
	return t.slice(0, Math.max(0, maxChars)).trimEnd();
}

function wrapText(text = "", maxCharsPerLine = 36, maxLines = 2) {
	const words = String(text || "")
		.trim()
		.split(/\s+/)
		.filter(Boolean);
	if (!words.length)
		return { text: "", lines: 0, maxLineLen: 0, overflow: false };

	const lines = [];
	let line = "";

	for (let i = 0; i < words.length; i++) {
		const word = words[i];
		const next = line ? `${line} ${word}` : word;
		if (next.length <= maxCharsPerLine) {
			line = next;
			continue;
		}
		if (lines.length < maxLines - 1) {
			if (line) lines.push(line);
			line = word;
			continue;
		}
		line = line ? `${line} ${word}` : word;
	}

	if (line) lines.push(line);
	const maxLineLen = lines.reduce((m, l) => Math.max(m, l.length), 0);
	return {
		text: lines.join("\n").trim(),
		lines: lines.length,
		maxLineLen,
		overflow: maxLineLen > maxCharsPerLine,
	};
}

function fitHeadlineText(
	text = "",
	{ baseMaxChars = 36, preferLines = 1, maxLines = 1 } = {}
) {
	const clean = String(text || "").trim();
	if (!clean) return { text: "", fontScale: 1, lines: 0, truncated: false };

	let fontScale = 1.0;
	let maxChars = baseMaxChars;
	let wrap = wrapText(clean, maxChars, preferLines);

	if (wrap.overflow || wrap.lines > preferLines) {
		wrap = wrapText(clean, maxChars, maxLines);
	}

	if (wrap.overflow) {
		const scales = [0.94, 0.9, 0.86];
		for (const scale of scales) {
			fontScale = scale;
			maxChars = Math.round(baseMaxChars / scale);
			wrap = wrapText(clean, maxChars, maxLines);
			if (!wrap.overflow) break;
		}
	}

	let truncated = false;
	if (wrap.overflow) {
		const maxTotal = maxChars * maxLines;
		const cut = hardTruncateText(clean, maxTotal);
		wrap = wrapText(cut, maxChars, maxLines);
		truncated = cut.length < clean.length;
	}

	return { text: wrap.text, fontScale, lines: wrap.lines, truncated };
}

function titleCaseIfLower(text = "") {
	const cleaned = String(text || "").trim();
	if (!cleaned) return "";
	if (/[A-Z]/.test(cleaned)) return cleaned;
	return cleaned.replace(/\b[a-z]/g, (m) => m.toUpperCase());
}

function buildThumbnailText(title = "") {
	const cleaned = cleanThumbnailText(title);
	if (!cleaned) return { text: "", fontScale: 1 };
	const pretty = titleCaseIfLower(cleaned);
	const words = pretty.split(" ").filter(Boolean);
	const trimmedWords = words.slice(0, THUMBNAIL_TEXT_MAX_WORDS);
	const trimmed = trimmedWords.join(" ");
	const baseChars = Math.max(THUMBNAIL_TEXT_BASE_MAX_CHARS, 14);
	const fit = fitHeadlineText(trimmed, {
		baseMaxChars: baseChars,
		preferLines: 1,
		maxLines: 1,
	});
	return {
		text: fit.text || trimmed,
		fontScale: fit.fontScale || 1,
	};
}

async function renderThumbnailOverlay({ inputPath, outputPath, title }) {
	const { text, fontScale } = buildThumbnailText(title);
	const safeText = escapeDrawtext(text);
	const fontFile = THUMBNAIL_FONT_FILE
		? `:fontfile='${escapeDrawtext(THUMBNAIL_FONT_FILE)}'`
		: "";
	const fontSize = Math.max(
		42,
		Math.round(THUMBNAIL_HEIGHT * THUMBNAIL_TEXT_SIZE_PCT * fontScale)
	);
	const lineSpacing = Math.round(fontSize * THUMBNAIL_TEXT_LINE_SPACING_PCT);
	const textYOffset = Math.round(
		THUMBNAIL_HEIGHT * THUMBNAIL_TEXT_Y_OFFSET_PCT
	);

	const filters = [
		`scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}`,
		"eq=contrast=1.08:saturation=1.12:brightness=0.02",
		"unsharp=5:5:0.8",
		`drawbox=x=0:y=0:w=iw*${THUMBNAIL_TEXT_BOX_WIDTH_PCT}:h=ih:color=black@${THUMBNAIL_TEXT_BOX_OPACITY}:t=fill`,
	];
	if (safeText) {
		filters.push(
			`drawtext=text='${safeText}'${fontFile}:fontsize=${fontSize}:fontcolor=white:borderw=3:bordercolor=black@0.6:shadowcolor=black@0.5:shadowx=2:shadowy=2:line_spacing=${lineSpacing}:x=w*${THUMBNAIL_TEXT_MARGIN_PCT}:y=(h-text_h)/2+${textYOffset}`
		);
	}

	await runFfmpeg(
		[
			"-i",
			inputPath,
			"-vf",
			filters.join(","),
			"-frames:v",
			"1",
			"-q:v",
			"2",
			"-y",
			outputPath,
		],
		"thumbnail_render"
	);

	return outputPath;
}

function ensureThumbnailFile(filePath, minBytes = THUMBNAIL_MIN_BYTES) {
	if (!filePath || !fs.existsSync(filePath))
		throw new Error("thumbnail_missing");
	const st = fs.statSync(filePath);
	if (!st || st.size < minBytes) throw new Error("thumbnail_too_small");
	const dt = detectFileType(filePath);
	if (!dt || dt.kind !== "image") throw new Error("thumbnail_invalid");
	return filePath;
}

async function fetchCseContext(topic, extraTokens = []) {
	if (!topic || !GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) return [];
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const baseTokens = [...topicTokensFromTitle(topic), ...extra];
	const queries = [
		`${topic} news`,
		`${topic} interview`,
		`${topic} official`,
		`${topic} cast`,
		`${topic} trailer`,
	];

	const items = await fetchCseItems(queries, { num: 3 });
	const matchTokens = filterSpecificTopicTokens(baseTokens);
	const minMatches = Math.max(1, Math.min(2, matchTokens.length));
	return items
		.filter(
			(it) =>
				topicMatchInfo(matchTokens, [it.title, it.snippet, it.link]).count >=
				minMatches
		)
		.slice(0, 6);
}

async function fetchCseImages(topic, extraTokens = []) {
	if (!topic || !GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) return [];
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const criteria = buildImageMatchCriteria(topic, extraTokens);
	const requireContext =
		criteria.contextTokens.length > 0 && criteria.rawTokenCount <= 3;

	const queries = [
		`${topic} press photo`,
		`${topic} news photo`,
		`${topic} photo`,
		`${topic} cast photo`,
		`${topic} still`,
	];
	const keyPhrase = filterSpecificTopicTokens(
		topicTokensFromTitle(topic)
	).slice(0, 2);
	if (keyPhrase.length) {
		queries.unshift(`${keyPhrase.join(" ")} photo`);
	}
	const contextHint = criteria.contextTokens.slice(0, 2).join(" ");
	const contextQueries = contextHint
		? [`${topic} ${contextHint} photo`, `${topic} ${contextHint} press photo`]
		: [];
	const searchQueries = contextQueries.length
		? [...contextQueries, ...queries]
		: queries;

	let items = await fetchCseItems(searchQueries, {
		num: 8,
		searchType: "image",
		imgSize: CSE_PREFERRED_IMG_SIZE,
		imgColorType: CSE_PREFERRED_IMG_COLOR,
	});
	if (!items.length) {
		items = await fetchCseItems(searchQueries, {
			num: 8,
			searchType: "image",
			imgSize: CSE_FALLBACK_IMG_SIZE,
			imgColorType: CSE_PREFERRED_IMG_COLOR,
		});
	}
	if (!items.length) {
		items = await fetchCseItems(queries, {
			num: 8,
			searchType: "image",
			imgSize: CSE_PREFERRED_IMG_SIZE,
		});
	}
	if (!items.length) {
		items = await fetchCseItems(queries, {
			num: 8,
			searchType: "image",
			imgSize: CSE_FALLBACK_IMG_SIZE,
		});
	}

	const candidates = [];
	for (const it of items) {
		const url = it.link || "";
		if (!url || !/^https:\/\//i.test(url)) continue;
		const contextLink = it.image?.contextLink || "";
		if (isLikelyWatermarkedSource(url, contextLink)) continue;
		const fields = [it.title, it.snippet, it.link, contextLink];
		const info = topicMatchInfo(criteria.wordTokens, fields);
		const phraseTokens = [];
		if (criteria.phraseToken) {
			phraseTokens.push(criteria.phraseToken);
			const compact = criteria.phraseToken.replace(/\s+/g, "");
			if (compact && compact !== criteria.phraseToken)
				phraseTokens.push(compact);
		}
		const phraseInfo = phraseTokens.length
			? topicMatchInfo(phraseTokens, fields)
			: { count: 0 };
		const hasPhrase = phraseInfo.count >= 1;
		const hasWordMatch = info.count >= criteria.minWordMatches;
		const requirePhrase =
			criteria.rawTokenCount <= 2 && Boolean(criteria.phraseToken);
		if (requirePhrase) {
			if (!hasPhrase) continue;
		} else if (!hasPhrase && !hasWordMatch) {
			continue;
		}
		if (requireContext) {
			const ctx = topicMatchInfo(criteria.contextTokens, fields);
			if (ctx.count < 1) continue;
		}
		const urlText = `${it.link || ""} ${
			it.image?.contextLink || ""
		}`.toLowerCase();
		const urlMatches = criteria.wordTokens.filter((tok) =>
			urlText.includes(tok)
		).length;
		const phraseBoost = phraseInfo.count ? 1.5 : 0;
		const score = info.count + urlMatches * 0.75 + phraseBoost;
		candidates.push({ url, score });
		if (candidates.length >= 14) break;
	}

	candidates.sort((a, b) => b.score - a.score);

	const filtered = [];
	const seen = new Set();
	for (const c of candidates) {
		if (!c?.url || seen.has(c.url)) continue;
		seen.add(c.url);
		const ct = await headContentType(c.url, 7000);
		if (ct) {
			if (!ct.startsWith("image/")) continue;
		} else if (!isProbablyDirectImageUrl(c.url)) {
			continue;
		}
		filtered.push(c.url);
		if (filtered.length >= 6) break;
	}
	return filtered;
}

async function collectThumbnailTopicImages({
	topics = [],
	tmpDir,
	jobId,
	title = "",
	maxImages = THUMBNAIL_TOPIC_MAX_IMAGES,
	requireTopicImages = REQUIRE_THUMBNAIL_TOPIC_IMAGES,
	log,
}) {
	const target = Math.max(0, Math.floor(maxImages));
	if (!target) return [];
	if (!GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) {
		if (requireTopicImages) throw new Error("thumbnail_cse_missing");
		if (log) log("thumbnail topic images skipped (CSE missing)");
		return [];
	}
	const topicList = Array.isArray(topics) ? topics : [];
	if (!topicList.length) return [];

	const contextQuery = sanitizeOverlayQuery(
		sanitizeThumbnailContext(title || "")
	);
	const contextTokens =
		contextQuery && contextQuery.length >= 4 ? [contextQuery] : [];

	const urls = [];
	const seen = new Set();
	const maxUrls = Math.max(target * 4, THUMBNAIL_TOPIC_MAX_DOWNLOADS);
	for (const t of topicList) {
		if (urls.length >= maxUrls) break;
		const label = t?.displayTopic || t?.topic || "";
		if (!label) continue;
		const extraTokens = Array.isArray(t?.keywords) ? t.keywords : [];
		const mergedTokens = contextTokens.length
			? [...contextTokens, ...extraTokens]
			: extraTokens;
		let hits = await fetchCseImages(label, mergedTokens);
		if (!hits.length) {
			const ctxItems = await fetchCseContext(label, mergedTokens);
			const articleUrls = uniqueStrings(
				[
					...(Array.isArray(t?.trendStory?.articles)
						? t.trendStory.articles.map((a) => a?.url)
						: []),
					...ctxItems.map((c) => c?.link),
				],
				{ limit: 6 }
			);
			const ogHits = [];
			for (const pageUrl of articleUrls) {
				if (ogHits.length >= 3) break;
				if (!pageUrl || ogHits.includes(pageUrl)) continue;
				const og = await fetchOpenGraphImageUrl(pageUrl);
				if (!og) continue;
				if (isLikelyWatermarkedSource(og, pageUrl)) continue;
				const ct = await headContentType(og, 7000);
				if (ct && !ct.startsWith("image/")) continue;
				ogHits.push(og);
			}
			if (ogHits.length) {
				if (log)
					log("thumbnail topic images fallback og", {
						topic: label,
						count: ogHits.length,
					});
				hits = ogHits;
			}
		}
		for (const url of hits) {
			if (urls.length >= maxUrls) break;
			if (!url || seen.has(url)) continue;
			seen.add(url);
			urls.push(url);
		}
	}

	if (!urls.length) {
		if (requireTopicImages) throw new Error("thumbnail_topic_images_missing");
		if (log)
			log("thumbnail topic images none", {
				reason: "no_urls",
				target,
			});
		return [];
	}

	const candidates = [];
	const downloadCount = Math.min(urls.length, THUMBNAIL_TOPIC_MAX_DOWNLOADS);
	for (let i = 0; i < downloadCount; i++) {
		const url = urls[i];
		const extGuess = path
			.extname(String(url).split("?")[0] || "")
			.toLowerCase();
		const ext = extGuess && extGuess.length <= 5 ? extGuess : ".jpg";
		const out = path.join(tmpDir, `thumb_topic_${jobId}_${i}${ext}`);
		try {
			await downloadToFile(url, out, 25000, 1);
			const detected = detectFileType(out);
			if (!detected || detected.kind !== "image") {
				safeUnlink(out);
				continue;
			}
			const st = fs.statSync(out);
			if (!st?.size || st.size < 4096) {
				safeUnlink(out);
				continue;
			}
			const dims = probeImageDimensions(out);
			const minEdge = Math.min(dims.width || 0, dims.height || 0);
			if (minEdge && minEdge < CSE_MIN_IMAGE_SHORT_EDGE) {
				safeUnlink(out);
				continue;
			}
			candidates.push({
				path: out,
				size: st.size,
				width: dims.width || 0,
				height: dims.height || 0,
			});
		} catch {
			safeUnlink(out);
		}
	}

	if (!candidates.length) {
		if (requireTopicImages) throw new Error("thumbnail_topic_images_missing");
		if (log)
			log("thumbnail topic images none", {
				reason: "no_candidates",
				target,
			});
		return [];
	}

	const preferred = candidates.filter((c) => {
		const minEdge = Math.min(c.width || 0, c.height || 0);
		if (minEdge && minEdge < THUMBNAIL_TOPIC_MIN_EDGE) return false;
		return c.size >= THUMBNAIL_TOPIC_MIN_BYTES;
	});

	const pickPool = preferred.length ? preferred : candidates;
	pickPool.sort((a, b) => b.size - a.size);
	const selected = pickPool.slice(0, target).map((c) => c.path);

	for (const c of candidates) {
		if (!selected.includes(c.path)) safeUnlink(c.path);
	}

	if (log)
		log("thumbnail topic images selected", {
			count: selected.length,
			target,
		});

	return selected;
}

function selectThumbnailTitle({ title, shortTitle, seoTitle, topics }) {
	const topicLabel =
		Array.isArray(topics) && topics.length
			? topics[0]?.displayTopic || topics[0]?.topic || ""
			: "";
	const topicWords = cleanThumbnailText(topicLabel).split(" ").filter(Boolean);
	const shortWords = cleanThumbnailText(shortTitle || "")
		.split(" ")
		.filter(Boolean);
	if (topicWords.length && topicWords.length <= 3) return topicLabel;
	if (shortWords.length && shortWords.length <= 3) return shortTitle || "";
	return shortTitle || seoTitle || title || topicLabel;
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

async function uploadThumbnailToCloudinary(filePath, jobId) {
	assertCloudinaryReady();
	ensureThumbnailFile(filePath);
	const publicId = `${THUMBNAIL_CLOUDINARY_PUBLIC_PREFIX}_${jobId}_${Date.now()}`;
	const result = await cloudinary.uploader.upload(filePath, {
		public_id: publicId,
		folder: THUMBNAIL_CLOUDINARY_FOLDER,
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

async function generateThumbnailPackage({
	jobId,
	tmpDir,
	presenterLocalPath,
	title,
	shortTitle,
	seoTitle,
	topics = [],
	openai,
	log,
	requireTopicImages = REQUIRE_THUMBNAIL_TOPIC_IMAGES,
	useSora = true,
}) {
	if (!presenterLocalPath)
		throw new Error("thumbnail_presenter_missing_or_invalid");
	ensureDir(tmpDir);
	const presenterDetected = detectFileType(presenterLocalPath);
	if (presenterDetected?.kind !== "image")
		throw new Error("thumbnail_presenter_missing_or_invalid");

	const thumbTitle = selectThumbnailTitle({
		title,
		shortTitle,
		seoTitle,
		topics,
	});

	const topicImagePaths = await collectThumbnailTopicImages({
		topics,
		tmpDir,
		jobId,
		title: title || seoTitle || "",
		maxImages: THUMBNAIL_TOPIC_MAX_IMAGES,
		requireTopicImages,
		log,
	});

	const baseImage = await generateThumbnailCompositeBase({
		jobId,
		tmpDir,
		presenterImagePath: presenterLocalPath,
		topicImagePaths,
		title: thumbTitle,
		topics,
		ratio: THUMBNAIL_RATIO,
		width: THUMBNAIL_WIDTH,
		height: THUMBNAIL_HEIGHT,
		openai,
		log,
		useSora,
	});

	const finalPath = path.join(tmpDir, `thumb_${jobId}.jpg`);
	await renderThumbnailOverlay({
		inputPath: baseImage,
		outputPath: finalPath,
		title: thumbTitle,
	});
	ensureThumbnailFile(finalPath);
	const uploaded = await uploadThumbnailToCloudinary(finalPath, jobId);
	return {
		localPath: finalPath,
		url: uploaded.url,
		publicId: uploaded.public_id,
		width: uploaded.width,
		height: uploaded.height,
		title: thumbTitle,
	};
}

module.exports = {
	buildThumbnailPrompt,
	buildSoraThumbnailPrompt,
	generateThumbnailCompositeBase,
	generateThumbnailPackage,
};
