/** @format */
"use strict";

/**
 * videoControllerSora.js — DROP-IN replacement (fixes 400 image URLs + index mismatch + placeholder issue)
 *
 * Key fixes:
 * 1) Normalize/decode HTML-escaped image URLs (e.g. &amp; -> &).
 * 2) Build ONE authoritative list of usable image pairs (reachability checked) and use it
 *    for BOTH planning and rendering to keep imageIndex aligned.
 * 3) Cloudinary upload fallback: if remote fetch fails, download locally with browser headers and upload file.
 * 4) Reachability check uses HEAD/Range (no false negatives from maxContentLength).
 * 5) If trends images are unavailable, auto-generate fallback images (no gray screens).
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawn, execSync } = require("child_process");

const axios = require("axios");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");
const cheerio = require("cheerio");

const { google } = require("googleapis");
const OpenAI = require("openai");
const ffmpeg = require("fluent-ffmpeg");
const cloudinary = require("cloudinary").v2;

const Video = require("../models/Video");
const Schedule = require("../models/Schedule");
const {
	ALL_TOP5_TOPICS,
	googleTrendingCategoriesId,
} = require("../assets/utils");

// Polyfill File for older Node runtimes
if (typeof globalThis.File === "undefined") {
	globalThis.File = require("node:buffer").File;
}

dayjs.extend(utc);
dayjs.extend(timezone);

const ENV = process.env;
const PST_TZ = "America/Los_Angeles";

/* -------------------------------------------------------------------------- */
/*  Cloudinary                                                                 */
/* -------------------------------------------------------------------------- */
cloudinary.config({
	cloud_name: ENV.CLOUDINARY_CLOUD_NAME,
	api_key: ENV.CLOUDINARY_API_KEY,
	api_secret: ENV.CLOUDINARY_API_SECRET,
});

/* -------------------------------------------------------------------------- */
/*  FFmpeg bootstrap                                                           */
/* -------------------------------------------------------------------------- */
function resolveFfmpegPath() {
	if (ENV.FFMPEG_PATH) return ENV.FFMPEG_PATH;
	try {
		return require("ffmpeg-static");
	} catch {
		return process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
	}
}
const FFMPEG_BIN = resolveFfmpegPath();
try {
	execSync(`"${FFMPEG_BIN}" -version`, { stdio: "ignore" });
} catch {
	console.error(
		"[Startup] FATAL – FFmpeg binary not found. Install ffmpeg or set FFMPEG_PATH."
	);
	process.exit(1);
}
ffmpeg.setFfmpegPath(FFMPEG_BIN);
ffmpeg.setFfprobePath(ENV.FFPROBE_PATH || "ffprobe");

/* -------------------------------------------------------------------------- */
/*  Global config                                                              */
/* -------------------------------------------------------------------------- */
const POLL_INTERVAL_MS = 2000;
const MAX_POLL_ATTEMPTS = 180;

const openai = new OpenAI({ apiKey: ENV.CHATGPT_API_TOKEN });

const CHAT_MODEL = "gpt-5.1";
const DEFAULT_LANGUAGE = "English";

const JAMENDO_ID = ENV.JAMENDO_CLIENT_ID;
const ELEVEN_API_KEY = ENV.ELEVENLABS_API_KEY;

const SORA_MODEL = ENV.SORA_MODEL || "sora-2-pro";
const SORA_USAGE_MODE = String(ENV.SORA_USAGE_MODE || "economy").toLowerCase();
const SORA_MAX_SECONDS_PER_VIDEO = Number(ENV.SORA_MAX_SECONDS_PER_VIDEO || 0);
const SORA_PRICE_PER_SECOND =
	ENV.SORA_PRICE_PER_SECOND !== undefined
		? Number(ENV.SORA_PRICE_PER_SECOND)
		: SORA_MODEL === "sora-2-pro"
		? 0.3
		: 0.1;

// If true, will generate fallback images when trending images are unusable to avoid gray screens
const AUTO_GENERATE_FALLBACK_IMAGES =
	String(ENV.AUTO_GENERATE_FALLBACK_IMAGES ?? "true").toLowerCase() !== "false";

const VALID_RATIOS = [
	"1280:720",
	"720:1280",
	"1104:832",
	"832:1104",
	"960:960",
	"1584:672",
];

const WORDS_PER_SEC = 2.2;
const NATURAL_WPS = 2.25;
const ENGAGEMENT_TAIL_MIN = 5;
const ENGAGEMENT_TAIL_MAX = 6;

const MAX_SILENCE_PAD = 0.35;
const MAX_ATEMPO = 1.08;

const PROMPT_CHAR_LIMIT = 220;
const AI_TOPIC_RE =
	/\b(ai|artificial intelligence|machine learning|genai|chatgpt|gpt-?\d*(?:\.\d+)?|openai|sora)\b/i;

const PROMPT_BITS = {
	quality:
		"photorealistic, ultra-detailed, HDR, cinematic lighting, smooth camera motion, subtle subject motion, emotional body language",
	physics:
		"realistic physics, natural hand-object contact, consistent lighting and shadows, no collage artifacts, no floating props",
	eyes: "natural eye focus and blinking, subtle micro-expressions, no jittering pupils, no crossed or wall-eyed look",
	softSafety:
		"fully clothed, respectful framing, wholesome, safe for work, no sexualised framing, no injuries",
	defects:
		"extra limbs, extra heads, mutated hands, fused fingers, missing limbs, contorted, twisted neck, bad anatomy, lowres, pixelated, blurry, heavy motion blur, overexposed, underexposed, watermark, logo, text overlay, nsfw, gore, floating props, collage look, weird physics, mismatched lighting, unnatural eye movement, jittering pupils, dead eyes, awkward pose, crossed eyes, wall-eyed, sliding feet, static frame, frozen frame, deformed face, melted face, distorted face, warped face, plastic skin, doll-like face, oversharpened, grainy, compression artifacts, glitch, ghosting",
	humanSafety:
		"anatomically correct, natural human faces, one natural-looking head, two eyes, normal limbs, realistic body proportions, natural head position, natural skin texture, sharp and in-focus facial features, no distortion, no warping, no blurring",
	brand:
		"subtle global brightness and contrast boost, slightly brighter and clearer faces while preserving natural skin tones, consistent AiVideomatic brand color grading",
};

const YT_CATEGORY_MAP = {
	Sports: "17",
	Politics: "25",
	Finance: "25",
	Entertainment: "24",
	Technology: "28",
	Health: "0",
	World: "0",
	Lifestyle: "0",
	Science: "0",
	Other: "0",
	Top5: "0",
	Gaming: "20",
	PetsAndAnimals: "15",
	Business: "21",
	Travel: "19",
	FoodDrink: "0",
	CelebrityNews: "25",
	Climate: "0",
	SocialIssues: "22",
	Education: "27",
	Fashion: "22",
};

const ELEVEN_VOICES = {
	English: "21m00Tcm4TlvDq8ikWAM",
	Spanish: "CYw3kZ02Hs0563khs1Fj",
	Francais: "gqjD3Awy6ZnJf2el9DnG",
	Deutsch: "IFHEeWG1IGkfXpxmB1vN",
	Hindi: "ykoxtvL6VZTyas23mE9F",
	Arabic: "", // blank => dynamic pick
};

const ELEVEN_STYLE_BY_CATEGORY = {
	Sports: 1.0,
	Politics: 0.7,
	Finance: 0.7,
	Entertainment: 0.9,
	Technology: 0.8,
	Health: 0.7,
	World: 0.7,
	Lifestyle: 0.9,
	Science: 0.8,
	Other: 0.7,
	Top5: 1.0,
};

const TONE_HINTS = {
	Sports: "Use an energetic, but professional broadcast tone.",
	Politics: "Maintain an authoritative yet neutral tone, like a documentary.",
	Finance: "Speak in a confident, analytical tone.",
	Entertainment: "Keep it upbeat and engaging.",
	Technology: "Adopt a forward-looking, curious tone.",
	Health: "Stay reassuring and informative.",
	Lifestyle: "Be friendly and encouraging.",
	Science: "Convey wonder and clarity.",
	World: "Maintain an objective, international outlook.",
	Top5: "Keep each item snappy and clearly ranked.",
};

const SENSITIVE_TONE_RE =
	/\b(died|dead|death|killed|slain|shot dead|massacre|tragedy|tragic|funeral|mourning|passed away|succumbed|fatal|fatalities|casualty|casualties|victim|victims|hospitalized|critically ill|coma|cancer|tumor|tumour|leukemia|stroke|heart attack|illness|terminal|pandemic|epidemic|outbreak|bombing|explosion|airstrike|genocide)\b/i;

const HYPE_TONE_RE =
	/\b(breaking|incredible|amazing|unbelievable|huge|massive|record|historic|epic|insane|wild|stunning|shocking|explodes|erupt(s|ed)?|surge(s|d)?|soar(s|ed)?|smashes|crushes|upset|thriller|overtime|buzzer-beater|comeback)\b/i;

const BRAND_TAG = "AiVideomatic";
const BRAND_CREDIT = "Powered by Serene Jannat";
const MERCH_INTRO =
	"Support the channel & customize your own merch:\nhttps://www.serenejannat.com/custom-gifts\nhttps://www.serenejannat.com/custom-gifts/6815366fd8583c434ec42fec\nhttps://www.serenejannat.com/custom-gifts/67b7fb9c3d0cd90c4fc410e3\n\n";

/* -------------------------------------------------------------------------- */
/*  HTTP headers to avoid image hotlink blocks                                 */
/* -------------------------------------------------------------------------- */
const BROWSER_HEADERS = Object.freeze({
	"User-Agent":
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
	Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
	"Accept-Language": "en-US,en;q=0.9",
	Referer: "https://trends.google.com/",
});

/* -------------------------------------------------------------------------- */
/*  Small utils                                                                */
/* -------------------------------------------------------------------------- */
const norm = (p) => (p ? p.replace(/\\/g, "/") : p);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const choose = (arr) => arr[Math.floor(Math.random() * arr.length)];
const unlinkSafe = (p) => {
	try {
		if (p) fs.unlinkSync(p);
	} catch {}
};

function tmpFile(prefix, ext = "") {
	return path.join(os.tmpdir(), `${prefix}_${crypto.randomUUID()}${ext}`);
}

function stripCodeFence(s) {
	const txt = String(s || "").trim();
	const m = txt.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
	return (m ? m[1] : txt).trim();
}

function strip(s) {
	return stripCodeFence(String(s || "").trim());
}

function decodeHtmlEntities(s) {
	if (!s) return "";
	return String(s)
		.replace(/&amp;/gi, "&")
		.replace(/&#38;|&#038;/g, "&")
		.replace(/&quot;/gi, '"')
		.replace(/&#39;|&apos;/gi, "'")
		.replace(/&lt;/gi, "<")
		.replace(/&gt;/gi, ">")
		.replace(/\u0026amp;/gi, "&"); // sometimes appears as literal \u0026amp;
}

function normalizeRemoteUrl(u) {
	if (!u) return null;
	let s = decodeHtmlEntities(String(u).trim());

	// Remove wrapping quotes, trailing punctuation
	s = s.replace(/^['"]+|['"]+$/g, "").replace(/[)\].,;]+$/g, "");

	// Handle protocol-relative URLs
	if (s.startsWith("//")) s = `https:${s}`;

	// Reject data/blob
	if (/^(data:|blob:)/i.test(s)) return null;

	// Some sources include whitespace
	s = s.replace(/\s/g, "%20");

	try {
		const parsed = new URL(s);
		if (!/^https?:$/i.test(parsed.protocol)) return null;
		return parsed.toString();
	} catch {
		return null;
	}
}

async function isLikelyImageUrlReachable(url) {
	const u = normalizeRemoteUrl(url);
	if (!u) return false;

	const validate = (s) => s >= 200 && s < 400;

	// Try HEAD first
	try {
		const res = await axios.head(u, {
			timeout: 8000,
			maxRedirects: 5,
			validateStatus: validate,
			headers: BROWSER_HEADERS,
		});
		const ct = String(res.headers?.["content-type"] || "").toLowerCase();
		// Some CDNs omit/lie on HEAD; allow empty.
		if (ct && ct.includes("text/html")) return false;
		return true;
	} catch (e) {
		// HEAD can be blocked; fallback to Range GET
	}

	try {
		const res = await axios.get(u, {
			timeout: 8000,
			maxRedirects: 5,
			responseType: "arraybuffer",
			validateStatus: validate,
			headers: { ...BROWSER_HEADERS, Range: "bytes=0-8191" },
		});
		const ct = String(res.headers?.["content-type"] || "").toLowerCase();
		if (ct && ct.includes("text/html")) return false;
		return true;
	} catch {
		return false;
	}
}

async function downloadImageToTemp(url, ext = ".jpg") {
	const u = normalizeRemoteUrl(url);
	if (!u) throw new Error("Invalid image URL");

	const tmp = tmpFile("img", ext);
	const writer = fs.createWriteStream(tmp);

	const resp = await axios.get(u, {
		responseType: "stream",
		timeout: 20000,
		maxRedirects: 5,
		validateStatus: (s) => s >= 200 && s < 400,
		headers: BROWSER_HEADERS,
	});

	await new Promise((resolve, reject) => {
		resp.data.pipe(writer).on("finish", resolve).on("error", reject);
	});

	return tmp;
}

function ensureClickableLinks(text) {
	if (!text || typeof text !== "string") return "";
	const fixed = text
		.split(/\r?\n/)
		.map((line) => {
			let s = line.trim();
			s = s.replace(/\s*\([^)]*\)\s*$/, "");
			s = s.replace(/(^|\s)(www\.[^\s)]+)/gi, "$1https://$2");
			s = s.replace(
				/(https?:\/\/)?(www\.)?(serenejannat\.com[^\s)]*)/gi,
				(_m, _scheme, _www, domain) => `https://${domain}`
			);
			s = s.replace(/(https?:\/\/[^\s)]+)[).,;:]+$/g, "$1");
			s = s.replace(/([^ \t\r\n])(https?:\/\/[^\s)]+)/g, "$1 $2");
			return s;
		})
		.join("\n")
		.replace(/\n{3,}/g, "\n\n");
	return fixed;
}

const looksLikeAITopic = (t) => AI_TOPIC_RE.test(String(t || ""));

function sanitizeAudienceFacingText(text, { allowAITopic = false } = {}) {
	if (!text || typeof text !== "string") return "";
	let cleaned = text;

	cleaned = cleaned.replace(
		/\bAI\s+(voiceover|voice over|narration|script)\b/gi,
		"narration"
	);

	if (!allowAITopic) {
		const swaps = [
			[/\bAI[-\s]?generated\b/gi, "hand-crafted"],
			[/\bAI[-\s]?powered\b/gi, "expert-led"],
			[/\bAI[-\s]?based\b/gi, "data-led"],
			[/\bAI\s+(model|system|engine)\b/gi, "our analysis"],
			[
				/\bAI['’]?\s*(prediction|predictions|forecast|pick|call|take|preview)\b/gi,
				"our $1",
			],
			[/\bgenerative AI\b/gi, "modern tools"],
			[/\bchatgpt\b/gi, "our newsroom"],
			[/\bgpt[-\s]?\d+(?:\.\d+)?\b/gi, "our newsroom"],
			[/\bopenai\b/gi, "the newsroom"],
			[/\bsora\b/gi, "the crew"],
			[/\bartificial intelligence\b/gi, "smart insight"],
			[/\bAI['’]?\b/gi, "our"],
		];
		for (const [re, rep] of swaps) cleaned = cleaned.replace(re, rep);
	}

	return cleaned.trim();
}

function enforceEngagementOutroText(text, { topic, wordCap }) {
	const existing = String(text || "").trim();
	const hasQuestion = /\?/.test(existing);
	const hasCTA = /(comment|subscribe|follow|like)/i.test(existing);
	const hasSignOff =
		/(see you|thanks for|catch you|stay curious|next time)/i.test(existing);

	const safeTopic =
		sanitizeAudienceFacingText(topic, { allowAITopic: true }) || topic || "";
	const question = safeTopic
		? `What do you think about ${safeTopic}?`
		: "What do you think?";
	const cta = "Comment below, tap like, and subscribe for more quick updates!";
	const signOffs = [
		"See you tomorrow!",
		"Catch you next time!",
		"Thanks for watching!",
		"Stay curious!",
	];
	const signOff = choose(signOffs);

	let combined = existing;
	if (!hasQuestion && !hasCTA) combined = `${question} ${cta}`;
	else if (!hasQuestion) combined = `${existing} ${question}`;
	else if (!hasCTA) combined = `${existing} ${cta}`;
	if (!hasSignOff) combined = `${combined} ${signOff}`;

	combined = combined.replace(/\s+/g, " ").trim();

	if (wordCap && Number.isFinite(wordCap) && wordCap > 3) {
		const words = combined.split(/\s+/);
		if (words.length > wordCap) {
			combined = words
				.slice(0, wordCap)
				.join(" ")
				.replace(/[.,;:]?$/, ".");
		}
	}
	return combined;
}

function scrubPromptForSafety(text) {
	if (!text || typeof text !== "string") return "";
	let t = text;
	const replacements = [
		[/pregnan(t|cy|cies)/gi, "expectant fashion moment"],
		[/baby\s*bump/gi, "fashion silhouette"],
		[/\bbelly\b/gi, "silhouette"],
		[/\bnude\b/gi, "fully clothed"],
		[/\bskin\b/gi, "outfit"],
		[/\bsheer\b/gi, "tasteful fabric"],
	];
	for (const [re, rep] of replacements) t = t.replace(re, rep);
	return `${t}. ${PROMPT_BITS.softSafety}`.trim();
}

const chooseTitleCase = (str = "") =>
	String(str || "")
		.toLowerCase()
		.replace(/(^\w|\s\w)/g, (m) => m.toUpperCase())
		.trim();

const goodDur = (n) =>
	Number.isInteger(+n) && +n >= 5 && +n <= 90 && +n % 5 === 0;

/* -------------------------------------------------------------------------- */
/*  GPT helpers                                                                */
/* -------------------------------------------------------------------------- */
async function gptText(content, { model = CHAT_MODEL } = {}) {
	const { choices } = await openai.chat.completions.create({
		model,
		messages: [{ role: "user", content }],
	});
	return String(choices?.[0]?.message?.content || "").trim();
}

async function gptJSON(content, { model = CHAT_MODEL, retries = 2 } = {}) {
	let lastErr = null;
	for (let i = 0; i < retries; i++) {
		try {
			const raw = strip(await gptText(content, { model }));
			return JSON.parse(raw);
		} catch (e) {
			lastErr = e;
		}
	}
	throw lastErr || new Error("GPT JSON parse failed");
}

/* -------------------------------------------------------------------------- */
/*  Ratio/resolution helpers                                                   */
/* -------------------------------------------------------------------------- */
const VALID_RATIOS_TO_ASPECT = {
	"1280:720": "16:9",
	"1584:672": "16:9",
	"720:1280": "9:16",
	"832:1104": "9:16",
	"960:960": "1:1",
	"1104:832": "4:3",
};

function targetResolutionForRatio(ratio) {
	switch (ratio) {
		case "720:1280":
		case "832:1104":
			return { width: 1080, height: 1920 };
		case "960:960":
			return { width: 1080, height: 1080 };
		case "1104:832":
			return { width: 1440, height: 1080 };
		case "1584:672":
		case "1280:720":
		default:
			return { width: 1920, height: 1080 };
	}
}

function buildCloudinaryTransformForRatio(ratio) {
	const aspect = VALID_RATIOS_TO_ASPECT[ratio] || "16:9";
	const { width, height } = targetResolutionForRatio(ratio);

	const base = {
		crop: "fill",
		gravity: "auto",
		quality: "auto:good",
		fetch_format: "auto",
	};

	if (width && height) return [{ ...base, width, height }];
	return [{ ...base, aspect_ratio: aspect }];
}

function openAIImageSizeForRatio(ratio) {
	switch (ratio) {
		case "720:1280":
		case "832:1104":
			return "1024x1792";
		case "960:960":
			return "1024x1024";
		default:
			return "1792x1024";
	}
}

/* -------------------------------------------------------------------------- */
/*  Voice tone classification                                                   */
/* -------------------------------------------------------------------------- */
function deriveVoiceSettings(text, category = "Other") {
	const baseStyle = ELEVEN_STYLE_BY_CATEGORY[category] ?? 0.7;
	const lower = String(text || "").toLowerCase();
	const isSensitive = SENSITIVE_TONE_RE.test(lower);

	let style = baseStyle;
	let stability = 0.15;
	let similarityBoost = 0.92;
	let openaiSpeed = 1.0;

	if (isSensitive) {
		style = 0.25;
		stability = 0.55;
		similarityBoost = 0.9;
		openaiSpeed = 0.94;
	} else {
		const isHype =
			HYPE_TONE_RE.test(lower) ||
			/[!?]/.test(text) ||
			category === "Sports" ||
			category === "Top5" ||
			category === "Entertainment";

		if (isHype) {
			style = Math.min(1, baseStyle + 0.3);
			stability = 0.13;
			openaiSpeed = 1.06;
		} else {
			style = Math.min(1, baseStyle + 0.15);
			stability = 0.17;
			openaiSpeed = 1.02;
		}
	}

	return { style, stability, similarityBoost, openaiSpeed, isSensitive };
}

/* -------------------------------------------------------------------------- */
/*  Segment timing                                                             */
/* -------------------------------------------------------------------------- */
function recomputeSegmentDurationsFromScript(segments, targetTotalSeconds) {
	if (
		!Array.isArray(segments) ||
		!segments.length ||
		!Number.isFinite(targetTotalSeconds)
	)
		return null;

	const MIN_SEGMENT_SECONDS = 2.5;

	const est = segments.map((s, idx) => {
		const words = String(s.scriptText || "")
			.trim()
			.split(/\s+/)
			.filter(Boolean).length;
		const basePause = idx === segments.length - 1 ? 0.35 : 0.25;
		return Math.max(
			MIN_SEGMENT_SECONDS,
			(words || 1) / NATURAL_WPS + basePause
		);
	});

	const estTotal = est.reduce((a, b) => a + b, 0) || targetTotalSeconds;
	let scale = targetTotalSeconds / estTotal;
	scale = Math.max(0.8, Math.min(1.25, scale));

	let scaled = est.map((v) => v * scale);
	let total = scaled.reduce((a, b) => a + b, 0);
	let diff = +(targetTotalSeconds - total).toFixed(2);

	let idx = scaled.length - 1;
	const step = diff > 0 ? 0.1 : -0.1;
	while (Math.abs(diff) > 0.05 && scaled.length) {
		const cand = scaled[idx] + step;
		if (cand >= MIN_SEGMENT_SECONDS) {
			scaled[idx] = cand;
			diff -= step;
		}
		idx = idx - 1;
		if (idx < 0) idx = scaled.length - 1;
	}

	return scaled.map((v) => +v.toFixed(2));
}

function computeEngagementTail(duration) {
	let tail = Math.round(
		Math.max(
			ENGAGEMENT_TAIL_MIN,
			Math.min(ENGAGEMENT_TAIL_MAX, duration * 0.12 || ENGAGEMENT_TAIL_MIN)
		)
	);
	if (duration < 12) tail = ENGAGEMENT_TAIL_MIN;
	return tail;
}

function computeInitialSegLens(category, duration, tailSeconds) {
	const INTRO = 3;
	const segLens = [];

	if (category === "Top5") {
		const r = duration - INTRO;
		const base = Math.floor(r / 5);
		const extra = r % 5;
		segLens.push(
			INTRO,
			...Array.from({ length: 5 }, (_, i) => base + (i < extra ? 1 : 0))
		);
	} else {
		const r = duration - INTRO;
		const n = Math.ceil(r / 10);
		segLens.push(
			INTRO,
			...Array.from({ length: n }, (_, i) =>
				i === n - 1 ? r - 10 * (n - 1) : 10
			)
		);
	}

	segLens.push(tailSeconds);
	const targetTotal = duration + tailSeconds;
	const delta = targetTotal - segLens.reduce((a, b) => a + b, 0);
	if (Math.abs(delta) >= 1) segLens[segLens.length - 1] += delta;

	return segLens;
}

function planImageIndexes(segments, imgCount) {
	if (
		!imgCount ||
		imgCount <= 0 ||
		!Array.isArray(segments) ||
		!segments.length
	)
		return segments;

	// Prefer unique usage if possible
	if (imgCount >= segments.length) {
		const used = new Set();
		const planned = new Array(segments.length).fill(null);

		segments.forEach((s, i) => {
			if (
				Number.isInteger(s.imageIndex) &&
				s.imageIndex >= 0 &&
				s.imageIndex < imgCount &&
				!used.has(s.imageIndex)
			) {
				planned[i] = s.imageIndex;
				used.add(s.imageIndex);
			}
		});

		let next = 0;
		for (let i = 0; i < segments.length; i++) {
			if (planned[i] !== null) continue;
			while (next < imgCount && used.has(next)) next++;
			planned[i] = next < imgCount ? next : i % imgCount;
			used.add(planned[i]);
		}

		return segments.map((s, i) => ({ ...s, imageIndex: planned[i] }));
	}

	// Otherwise cycle, avoid immediate repeats
	const planned = [];
	let last = null;
	for (let i = 0; i < segments.length; i++) {
		let idx =
			Number.isInteger(segments[i].imageIndex) &&
			segments[i].imageIndex >= 0 &&
			segments[i].imageIndex < imgCount
				? segments[i].imageIndex
				: null;
		if (idx === null || idx === last)
			idx = last === null ? 0 : (last + 1) % imgCount;
		planned.push(idx);
		last = idx;
	}

	return segments.map((s, i) => ({ ...s, imageIndex: planned[i] }));
}

/* -------------------------------------------------------------------------- */
/*  Sora planning + prompt sanitation                                           */
/* -------------------------------------------------------------------------- */
function soraSizeForRatio(ratio) {
	switch (ratio) {
		case "720:1280":
		case "832:1104":
			return "720x1280";
		default:
			return "1280x720";
	}
}

function soraSecondsForSegment(targetSeconds) {
	const t = Number(targetSeconds) || 4;
	if (t <= 4.5) return "4";
	if (t <= 8.5) return "8";
	return "12";
}

function computeSoraBudgetSeconds(videoDurationSeconds) {
	const dur = Math.max(4, Math.min(60, Number(videoDurationSeconds) || 0));
	const factor =
		SORA_USAGE_MODE === "premium"
			? 1.2
			: SORA_USAGE_MODE === "balanced"
			? 0.8
			: 0.6;

	let budget = Math.round(dur * factor);
	if (SORA_MAX_SECONDS_PER_VIDEO > 0)
		budget = Math.min(budget, SORA_MAX_SECONDS_PER_VIDEO);
	return Math.max(4, Math.min(60, budget));
}

function planSoraAllocation(segLens, soraBudgetSeconds) {
	const n = Array.isArray(segLens) ? segLens.length : 0;
	if (!n || !Number.isFinite(soraBudgetSeconds) || soraBudgetSeconds <= 0) {
		return {
			useSora: new Array(n).fill(false),
			perSegSeconds: new Array(n).fill(0),
			totalSeconds: 0,
		};
	}

	const metas = segLens.map((len, idx) => {
		const soraSec = Number(soraSecondsForSegment(len));
		let priority = 50;
		if (idx === 0) priority = 100;
		else if (idx === n - 1) priority = 40;
		else {
			const center = (n - 1) / 2;
			priority = 80 - Math.abs(idx - center) * 3;
		}
		return { index: idx, soraSec, priority };
	});

	metas.sort((a, b) => b.priority - a.priority);

	const use = new Array(n).fill(false);
	const perSegSeconds = new Array(n).fill(0);
	let used = 0;

	for (const m of metas) {
		if (!m.soraSec || used + m.soraSec > soraBudgetSeconds) continue;
		use[m.index] = true;
		perSegSeconds[m.index] = m.soraSec;
		used += m.soraSec;
	}

	// Ensure >= 2 clips if budget allows >= 8 seconds
	const MIN_CLIPS = 2;
	const MIN_SECONDS_PER_CLIP = 4;
	const soraCount = use.filter(Boolean).length;

	if (
		n >= MIN_CLIPS &&
		soraCount < MIN_CLIPS &&
		soraBudgetSeconds >= MIN_CLIPS * MIN_SECONDS_PER_CLIP
	) {
		use.fill(false);
		perSegSeconds.fill(0);
		for (const m of metas.slice(0, MIN_CLIPS)) {
			use[m.index] = true;
			perSegSeconds[m.index] = MIN_SECONDS_PER_CLIP;
		}
		used = MIN_CLIPS * MIN_SECONDS_PER_CLIP;
	} else if (soraCount === 0 && soraBudgetSeconds >= MIN_SECONDS_PER_CLIP) {
		use[metas[0].index] = true;
		perSegSeconds[metas[0].index] = MIN_SECONDS_PER_CLIP;
		used = MIN_SECONDS_PER_CLIP;
	}

	return { useSora: use, perSegSeconds, totalSeconds: used };
}

function sanitizeSoraPrompt(promptText, topicHint = "", ratioHint = "") {
	const safeTopic = String(topicHint || "")
		.replace(/[^\w\s]/g, " ")
		.trim();
	let p = String(promptText || "")
		.replace(/[^\w\s.,-]/g, " ")
		.replace(/\b[A-Z][a-z]{2,}\b/g, "") // reduce proper nouns
		.replace(/\s{2,}/g, " ")
		.trim();

	const guard =
		"single cinematic shot, realistic lighting and physics, smooth camera move, natural faces, no logos, no brand names, no trademarks, no on-screen text, no watermarks, no slideshow frames";
	const topic = safeTopic ? ` Topic focus: ${safeTopic}.` : "";
	const aspect = ratioHint ? ` Frame for ${ratioHint} aspect.` : "";

	const hasMotion =
		/\b(camera|pan|tilt|tracking|dolly|move|motion|gimbal|zoom)\b/i.test(p);
	if (!p || p.length < 20)
		p = "Cinematic editorial news moment with natural movement";
	else if (!hasMotion)
		p = `${p}. gentle gimbal push with subtle subject motion`;

	return `${p}. ${guard}${aspect}${topic}`.replace(/\s{2,}/g, " ").trim();
}

function isModerationBlock(err) {
	const code =
		err?.code ||
		err?.response?.data?.error?.code ||
		err?.response?.data?.error?.type;
	if (code && String(code).toLowerCase().includes("moderation")) return true;
	return String(err?.message || "")
		.toLowerCase()
		.includes("moderation");
}

/* -------------------------------------------------------------------------- */
/*  FFmpeg helpers                                                             */
/* -------------------------------------------------------------------------- */
function ffprobe(file) {
	return new Promise((resolve, reject) => {
		ffmpeg.ffprobe(file, (err, data) => (err ? reject(err) : resolve(data)));
	});
}

function ffmpegPromise(builder) {
	return new Promise((resolve, reject) => {
		const cmd = builder(ffmpeg());
		cmd.on("start", (c) => console.log(`[FFmpeg] ${c}`));
		cmd.on("end", resolve);
		cmd.on("error", reject);
	});
}

async function probeVideoDuration(file) {
	const meta = await ffprobe(file);
	return meta.format?.duration || 0;
}

async function exactLen(
	src,
	targetSeconds,
	out,
	{ ratio = null, enhance = false } = {}
) {
	const meta = await ffprobe(src);
	const inDur = meta.format?.duration || targetSeconds;
	const diff = +(targetSeconds - inDur).toFixed(3);

	const targetRes = ratio ? targetResolutionForRatio(ratio) : null;
	const vStream = Array.isArray(meta.streams)
		? meta.streams.find((s) => s.codec_type === "video")
		: null;
	const inW = vStream?.width;
	const inH = vStream?.height;

	await ffmpegPromise((cmd) => {
		cmd.input(norm(src));

		const vf = [];
		if (enhance && targetRes?.width && targetRes?.height) {
			const { width, height } = targetRes;
			const needsResize =
				!inW || !inH || Math.abs(inW - width) > 8 || Math.abs(inH - height) > 8;
			if (needsResize) {
				vf.push(
					`scale=${width}:${height}:force_original_aspect_ratio=increase:flags=lanczos+accurate_rnd+full_chroma_int`,
					`crop=${width}:${height}`
				);
			}
		}

		if (Math.abs(diff) >= 0.08) {
			if (diff < 0) cmd.outputOptions("-t", String(targetSeconds));
			else vf.push(`tpad=stop_duration=${diff.toFixed(3)}`);
		}

		if (vf.length) cmd.videoFilters(vf.join(","));

		return cmd
			.outputOptions(
				"-c:v",
				"libx264",
				"-preset",
				enhance ? "slow" : "fast",
				"-crf",
				String(enhance ? 16 : 17),
				"-profile:v",
				"high",
				"-pix_fmt",
				"yuv420p",
				"-movflags",
				"+faststart",
				"-y"
			)
			.save(norm(out));
	});
}

async function exactLenAudio(src, targetSeconds, out, opts = {}) {
	const { allowTempo = true, allowPad = true } = opts;
	const meta = await ffprobe(src);
	const inDur = meta.format?.duration || targetSeconds;
	const diff = +(targetSeconds - inDur).toFixed(3);

	await ffmpegPromise((cmd) => {
		cmd.input(norm(src));

		const filters = [];
		if (Math.abs(diff) <= 0.08) {
			// close enough
		} else if (diff < -0.08) {
			const ratio = inDur / targetSeconds;
			if (allowTempo && ratio <= 1.3) {
				filters.push(`atempo=${Math.min(MAX_ATEMPO, ratio).toFixed(3)}`);
			} else {
				cmd.outputOptions("-t", String(targetSeconds));
			}
		} else if (allowPad) {
			const padDur = Math.min(MAX_SILENCE_PAD, diff);
			if (padDur > 0.05) filters.push(`apad=pad_dur=${padDur.toFixed(3)}`);
		}

		if (filters.length) cmd.audioFilters(filters.join(","));
		return cmd.outputOptions("-y").save(norm(out));
	});
}

async function concatWithTransitions(
	clips,
	durationsHint = [],
	ratio = null,
	maxFade = 0.35
) {
	if (!Array.isArray(clips) || !clips.length)
		throw new Error("No clips to stitch");

	const targetRes = ratio ? targetResolutionForRatio(ratio) : null;

	const normalized = [];
	for (let i = 0; i < clips.length; i++) {
		const out = tmpFile(`norm_${i + 1}`, ".mp4");
		await ffmpegPromise((cmd) => {
			cmd.input(norm(clips[i]));
			const vf = [];
			if (targetRes?.width && targetRes?.height) {
				vf.push(
					`scale=${targetRes.width}:${targetRes.height}:force_original_aspect_ratio=increase:flags=lanczos+accurate_rnd+full_chroma_int`,
					`crop=${targetRes.width}:${targetRes.height}`
				);
			}
			vf.push("format=yuv420p", "setsar=1", "fps=30", "setpts=PTS-STARTPTS");
			cmd.videoFilters(vf.join(","));
			return cmd
				.outputOptions(
					"-c:v",
					"libx264",
					"-preset",
					"fast",
					"-crf",
					"18",
					"-movflags",
					"+faststart",
					"-y"
				)
				.save(norm(out));
		});
		normalized.push(out);
	}

	const faded = [];
	for (let i = 0; i < normalized.length; i++) {
		const src = normalized[i];
		const out = tmpFile(`fade_${i + 1}`, ".mp4");

		const hint = Number(durationsHint[i]);
		const dur =
			Number.isFinite(hint) && hint > 0
				? hint
				: (await probeVideoDuration(src)) || 1;
		const fadeDur = Math.max(0.12, Math.min(maxFade, dur * 0.18, dur / 2));

		const isFirst = i === 0;
		const isLast = i === normalized.length - 1;

		const filters = [];
		if (isFirst) filters.push(`fade=t=in:st=0:d=${fadeDur.toFixed(3)}`);
		if (isLast)
			filters.push(
				`fade=t=out:st=${Math.max(0, dur - fadeDur).toFixed(
					3
				)}:d=${fadeDur.toFixed(3)}`
			);
		if (!isFirst && !isLast) {
			filters.push(`fade=t=in:st=0:d=${fadeDur.toFixed(3)}`);
			filters.push(
				`fade=t=out:st=${Math.max(0, dur - fadeDur).toFixed(
					3
				)}:d=${fadeDur.toFixed(3)}`
			);
		}

		await ffmpegPromise((cmd) => {
			cmd.input(norm(src));
			cmd.videoFilters(filters.join(","));
			return cmd
				.outputOptions(
					"-c:v",
					"libx264",
					"-preset",
					"slow",
					"-crf",
					"16",
					"-pix_fmt",
					"yuv420p",
					"-movflags",
					"+faststart",
					"-y"
				)
				.save(norm(out));
		});

		faded.push(out);
	}

	const listFile = tmpFile("concat_list", ".txt");
	fs.writeFileSync(listFile, faded.map((p) => `file '${norm(p)}'`).join("\n"));

	const out = tmpFile("transitioned", ".mp4");
	await ffmpegPromise((cmd) =>
		cmd
			.input(norm(listFile))
			.inputOptions("-f", "concat", "-safe", "0")
			.outputOptions("-c:v", "copy", "-y")
			.save(norm(out))
	);

	for (const p of [...normalized, ...faded]) unlinkSafe(p);
	unlinkSafe(listFile);

	return out;
}

/* -------------------------------------------------------------------------- */
/*  Static clips + placeholder                                                  */
/* -------------------------------------------------------------------------- */
async function generatePlaceholderClip({
	segmentIndex,
	ratio,
	targetDuration,
	color = "gray",
}) {
	const { width, height } = targetResolutionForRatio(ratio);
	const size = `${width}x${height}`;
	const out = tmpFile(`placeholder_${segmentIndex}`, ".mp4");

	// 2 variants: with zoompan, then simple
	const variants = [
		[
			"format=yuv420p",
			"setsar=1",
			`zoompan=z='min(1.0+0.001*n,1.04)':d=1:x='iw/2-(iw/2)/zoom':y='ih/2-(ih/2)/zoom':s=${size}:fps=30`,
		].join(","),
		["format=yuv420p", "setsar=1", "fps=30"].join(","),
	];

	let lastErr = null;
	for (const vf of variants) {
		try {
			await ffmpegPromise((c) =>
				c
					.input(`color=${color}:s=${size}:r=30:d=${targetDuration}`)
					.inputOptions("-f", "lavfi")
					.videoFilters(vf)
					.outputOptions(
						"-t",
						String(targetDuration),
						"-c:v",
						"libx264",
						"-preset",
						"slow",
						"-crf",
						"17",
						"-pix_fmt",
						"yuv420p",
						"-r",
						"30",
						"-y"
					)
					.save(norm(out))
			);
			return out;
		} catch (e) {
			lastErr = e;
		}
	}
	throw lastErr || new Error("[Placeholder] failed to render");
}

async function generateStaticClipFromImage({
	segmentIndex,
	imgUrlOriginal,
	imgUrlCloudinary,
	ratio,
	targetDuration,
	zoomPan = true,
}) {
	const candidates = [imgUrlCloudinary, imgUrlOriginal]
		.map(normalizeRemoteUrl)
		.filter(Boolean);

	if (!candidates.length) throw new Error("Missing image URL for static clip");

	const { width, height } = targetResolutionForRatio(ratio);
	const out = tmpFile(`static_${segmentIndex}`, ".mp4");

	let lastErr = null;
	for (const url of candidates) {
		let localPath = null;
		try {
			localPath = await downloadImageToTemp(url, ".jpg");

			const vfZoom = [
				`scale=${width}:${height}:force_original_aspect_ratio=increase:flags=lanczos`,
				`crop=${width}:${height}`,
				"setsar=1",
				zoomPan
					? `zoompan=z='min(1.0+0.0015*n,1.06)':d=1:x='iw/2-(iw/2)/zoom':y='ih/2-(ih/2)/zoom':s=${width}x${height}:fps=30`
					: "fps=30",
				"format=yuv420p",
			].join(",");

			const vfSimple = [
				`scale=${width}:${height}:force_original_aspect_ratio=increase:flags=lanczos`,
				`crop=${width}:${height}`,
				"setsar=1",
				"fps=30",
				"format=yuv420p",
			].join(",");

			const attempt = async (vf) =>
				ffmpegPromise((c) =>
					c
						.input(norm(localPath))
						.inputOptions("-loop", "1")
						.videoFilters(vf)
						.outputOptions(
							"-t",
							String(targetDuration),
							"-c:v",
							"libx264",
							"-preset",
							"slow",
							"-crf",
							"17",
							"-pix_fmt",
							"yuv420p",
							"-r",
							"30",
							"-y"
						)
						.save(norm(out))
				);

			try {
				await attempt(vfZoom);
			} catch {
				await attempt(vfSimple);
			}

			unlinkSafe(localPath);
			return out;
		} catch (e) {
			lastErr = e;
			unlinkSafe(localPath);
			console.warn(
				`[Seg ${segmentIndex}] Static fallback failed for ${url}`,
				e.message
			);
		}
	}

	console.warn(
		`[Seg ${segmentIndex}] Static fallback failed for all candidates; using placeholder`
	);
	return generatePlaceholderClip({ segmentIndex, ratio, targetDuration });
}

/* -------------------------------------------------------------------------- */
/*  Trends + SEO                                                               */
/* -------------------------------------------------------------------------- */
function resolveTrendsCategoryId(label) {
	const e = googleTrendingCategoriesId.find((c) => c.category === label);
	return e ? e.ids[0] : 0;
}
const TRENDS_API_URL =
	ENV.TRENDS_API_URL || "http://localhost:8102/api/google-trends";
const TRENDS_HTTP_TIMEOUT_MS = 60000;

function inferAspectFromUrl(url) {
	try {
		const u = new URL(url);
		const qW = u.search.match(/[?&]w=(\d{2,4})/i);
		const qH = u.search.match(/[?&]h=(\d{2,4})/i);
		let w = qW ? parseInt(qW[1], 10) : null;
		let h = qH ? parseInt(qH[1], 10) : null;
		if (!w || !h) {
			const m = u.pathname.match(/(\d{3,4})x(\d{3,4})/);
			if (m) {
				w = parseInt(m[1], 10);
				h = parseInt(m[2], 10);
			}
		}
		if (!w || !h) return "unknown";
		const r = w / h;
		if (r >= 1.1) return "landscape";
		if (r <= 0.9) return "portrait";
		return "square";
	} catch {
		return "unknown";
	}
}

function scoreImageUrl(url, { isStoryImage = false, targetRatio = null } = {}) {
	let score = isStoryImage ? 2 : 0;
	try {
		const u = new URL(url);
		const host = u.hostname.toLowerCase();
		const isThumb =
			host.endsWith("gstatic.com") ||
			host.includes("googleusercontent.com") ||
			host.includes("ggpht.com");
		if (!isThumb) score += 4;
		if (/\.(jpe?g|png|webp|avif)$/i.test(u.pathname)) score += 2;

		const wMatch = u.search.match(/[?&]w=(\d{2,4})/i);
		if (wMatch) {
			const w = parseInt(wMatch[1], 10);
			if (w >= 1400) score += 3;
			else if (w >= 1000) score += 2;
			else if (w >= 600) score += 1;
		}

		const aspect = inferAspectFromUrl(url);
		const ratioOrientation = (() => {
			if (!targetRatio) return null;
			const parts = String(targetRatio).split(":").map(Number);
			if (parts.length !== 2 || !parts[0] || !parts[1]) return null;
			return parts[0] > parts[1]
				? "landscape"
				: parts[0] < parts[1]
				? "portrait"
				: "square";
		})();
		if (ratioOrientation && aspect === ratioOrientation) score += 3;
		else if (ratioOrientation && aspect === "square") score += 1;

		return { score, aspect };
	} catch {
		return { score: score + 1, aspect: "unknown" };
	}
}

function normaliseTrendImageBriefs(briefs = [], topic = "") {
	const targets = ["1280:720", "720:1280"];
	const byAspect = new Map(targets.map((t) => [t, null]));

	if (Array.isArray(briefs)) {
		for (const raw of briefs) {
			if (!raw?.aspectRatio) continue;
			const ar = String(raw.aspectRatio).trim();
			if (!byAspect.has(ar) || byAspect.get(ar)) continue;
			byAspect.set(ar, {
				aspectRatio: ar,
				visualHook: String(
					raw.visualHook || raw.idea || raw.hook || raw.description || ""
				).trim(),
				emotion: String(raw.emotion || "").trim(),
				rationale: String(raw.rationale || raw.note || "").trim(),
			});
		}
	}

	for (const [ar, val] of byAspect.entries()) {
		if (val) continue;
		byAspect.set(ar, {
			aspectRatio: ar,
			visualHook:
				ar === "1280:720"
					? `Landscape viral frame about ${topic}`
					: `Vertical viral frame about ${topic}`,
			emotion: "High energy",
			rationale: "Auto-filled to cover both aspect ratios.",
		});
	}

	return Array.from(byAspect.values());
}

async function fetchTrendingStory(
	category,
	geo = "US",
	usedTopics = new Set(),
	language = DEFAULT_LANGUAGE,
	targetRatio = null
) {
	const id = resolveTrendsCategoryId(category);
	const p = new URLSearchParams({
		geo,
		category: String(id),
		hours: "168",
		language,
	});
	const url = `${TRENDS_API_URL}?${p}`;

	const normTitle = (t) =>
		String(t || "")
			.toLowerCase()
			.replace(/\s+/g, " ")
			.trim();
	const usedSet =
		usedTopics instanceof Set
			? new Set(Array.from(usedTopics).map(normTitle).filter(Boolean))
			: new Set();
	const usedList = Array.from(usedSet);
	const isUsed = (term) => {
		const n = normTitle(term);
		if (!n) return false;
		if (usedSet.has(n)) return true;
		return usedList.some((u) => u.includes(n) || n.includes(u));
	};

	try {
		console.log("[Trending] fetch:", url);

		const fetchOnce = (timeoutMs) => axios.get(url, { timeout: timeoutMs });
		let data;
		try {
			({ data } = await fetchOnce(TRENDS_HTTP_TIMEOUT_MS));
		} catch (e) {
			if (!/timeout/i.test(e.message || "")) throw e;
			console.warn("[Trending] timeout, retrying once (extended)");
			({ data } = await fetchOnce(TRENDS_HTTP_TIMEOUT_MS * 1.5));
		}

		const stories = Array.isArray(data?.stories) ? data.stories : [];
		if (!stories.length) return null;

		let picked = null;
		for (const s of stories) {
			const eff = String(
				s.youtubeShortTitle || s.seoTitle || s.title || ""
			).trim();
			const raw = String(s.title || "").trim();
			const entityUsed =
				Array.isArray(s.entityNames) &&
				s.entityNames.some((e) => isUsed(String(e || "")));
			if (eff && !isUsed(eff) && !isUsed(raw) && !entityUsed) {
				picked = { story: s, effectiveTitle: eff };
				break;
			}
		}
		if (!picked) {
			const s = stories[0];
			picked = {
				story: s,
				effectiveTitle:
					String(s.youtubeShortTitle || s.seoTitle || s.title || "").trim() ||
					String(s.title || "").trim(),
			};
		}

		const s = picked.story;
		const effectiveTitle = picked.effectiveTitle;

		const articles = Array.isArray(s.articles) ? s.articles : [];
		const viralBriefs = normaliseTrendImageBriefs(
			s.viralImageBriefs || s.imageDirectives || [],
			effectiveTitle
		);
		const imageComment = String(s.imageComment || s.imageHook || "").trim();

		// Collect candidates, normalize & de-dupe by URL string only (fast + reliable)
		const candidates = [];
		if (s.image) candidates.push({ url: s.image, isStoryImage: true });
		if (Array.isArray(s.images))
			s.images.forEach((u) => candidates.push({ url: u, isStoryImage: true }));
		for (const a of articles)
			if (a?.image) candidates.push({ url: a.image, isStoryImage: false });

		const seen = new Set();
		const deduped = [];
		for (const c of candidates) {
			const nu = normalizeRemoteUrl(c.url);
			if (!nu || seen.has(nu)) continue;
			seen.add(nu);
			deduped.push({ url: nu, isStoryImage: Boolean(c.isStoryImage) });
			if (deduped.length >= 24) break; // allow more; we'll filter later
		}

		const scored = deduped.map((c, idx) => {
			const info = scoreImageUrl(c.url, {
				isStoryImage: c.isStoryImage,
				targetRatio,
			});
			return {
				...c,
				idx,
				score: info.score + (c.isStoryImage ? 1 : 0),
				aspect: info.aspect,
			};
		});
		scored.sort((a, b) =>
			b.score !== a.score ? b.score - a.score : a.idx - b.idx
		);

		const images = scored.map((c) => c.url);

		return {
			title: String(effectiveTitle || s.title || "").trim(),
			rawTitle: String(s.title || "").trim(),
			seoTitle: s.seoTitle ? String(s.seoTitle).trim() : null,
			youtubeShortTitle: s.youtubeShortTitle
				? String(s.youtubeShortTitle).trim()
				: null,
			entityNames: Array.isArray(s.entityNames)
				? s.entityNames.map((e) => String(e || "").trim()).filter(Boolean)
				: [],
			imageComment,
			viralImageBriefs: viralBriefs,
			images,
			articles: articles.map((a) => ({
				title: String(a.title || "").trim(),
				url: a.url || null,
				image: a.image ? normalizeRemoteUrl(a.image) : null,
			})),
		};
	} catch (e) {
		console.warn("[Trending] fetch failed", e.message);
		return null;
	}
}

async function scrapeArticleText(url) {
	if (!url) return null;
	try {
		const { data: html } = await axios.get(url, { timeout: 10000 });
		const $ = cheerio.load(html);
		const body = $("article").text() || $("body").text();
		const cleaned = String(body || "")
			.replace(/\s+/g, " ")
			.replace(/(Advertisement|Subscribe now|Sign up for.*?newsletter).*/gi, "")
			.trim();
		return cleaned.slice(0, 12000) || null;
	} catch (e) {
		console.warn("[Scrape] failed", e.message);
		return null;
	}
}

async function generateSeoTitle(
	headlinesOrTopic,
	category,
	language = DEFAULT_LANGUAGE,
	articleTextSnippet = ""
) {
	const items = Array.isArray(headlinesOrTopic)
		? headlinesOrTopic
		: [headlinesOrTopic];
	const joinedHeadlines = items.filter(Boolean).join(" | ");
	const context = articleTextSnippet
		? `${joinedHeadlines} | ${articleTextSnippet.slice(0, 600)}`
		: joinedHeadlines;

	const ask = `
You are an experienced YouTube editor writing titles for ${
		category === "Sports"
			? "an official sports channel"
			: "a serious news channel"
	}.
Write ONE searchable, professional YouTube Shorts title.

Rules:
- Max 65 characters, Title Case, no emojis, no hashtags, no quotes.
- No tabloid hype ("Insane", "Crazy", "Wild").
- Never mention AI/tools unless the subject itself is AI tech (and no automation disclaimers).
- Include the core subject once, early.
- Use real search phrases ("how to watch", "start time", "highlights", "update", "analysis", etc.).
- Reply only with the title.

Context:
${context || "(none)"}
${language !== DEFAULT_LANGUAGE ? `Respond in ${language}.` : ""}
`.trim();

	try {
		const raw = (await gptText(ask)).replace(/["“”]/g, "").trim();
		return chooseTitleCase(raw);
	} catch (e) {
		console.warn("[SEO title] generation failed", e.message);
		return "";
	}
}

/* -------------------------------------------------------------------------- */
/*  Topic helpers                                                              */
/* -------------------------------------------------------------------------- */
const CURRENT_MONTH_YEAR = dayjs().format("MMMM YYYY");
const CURRENT_YEAR = dayjs().year();

async function topicFromCustomPrompt(text) {
	const make = (attempt) =>
		`
Attempt ${attempt}:
Give ONE click-worthy title (max 70 chars, no hashtags, no quotes) set in ${CURRENT_MONTH_YEAR}.
Do not mention years before ${CURRENT_YEAR}.
<<<${text}>>>
`.trim();

	for (let a = 1; a <= 2; a++) {
		const t = (await gptText(make(a))).replace(/["“”]/g, "").trim();
		if (!/20\d{2}/.test(t) || new RegExp(`\\b${CURRENT_YEAR}\\b`).test(t))
			return t;
	}
	throw new Error("Cannot distil topic");
}

async function pickTrendingTopicFresh(category, language, country) {
	const loc =
		country && String(country).toLowerCase() !== "all countries"
			? ` in ${country}`
			: "US";
	const langLn =
		language !== DEFAULT_LANGUAGE ? ` Respond in ${language}.` : "";
	const ask = (a) =>
		`
Attempt ${a}:
Return a JSON array of 10 trending ${category} titles (${CURRENT_MONTH_YEAR}${loc}), no hashtags, max 70 chars each.${langLn}
`.trim();

	for (let a = 1; a <= 2; a++) {
		try {
			const raw = strip(await gptText(ask(a)));
			const list = JSON.parse(raw || "[]");
			if (Array.isArray(list) && list.length) return list;
		} catch {}
	}
	return [`Breaking ${category} Story – ${CURRENT_MONTH_YEAR}`];
}

async function generateTop5Outline(topic, language = DEFAULT_LANGUAGE) {
	const ask = `
Current date: ${dayjs().format("YYYY-MM-DD")}
You are planning a Top 5 countdown video.

Title: ${topic}

Return strict JSON array of exactly 5 objects, ranks 5 down to 1:
- rank: 5..1
- label: <= 8 words
- oneLine: <= 18 words, why it deserves this rank

Use real-world facts; avoid speculation. Keep everything in ${language}. No extra keys.
`.trim();

	for (let attempt = 1; attempt <= 2; attempt++) {
		try {
			const parsed = await gptJSON(ask, { retries: 1 });
			if (Array.isArray(parsed) && parsed.length === 5) {
				return parsed.sort((a, b) => (b.rank || 0) - (a.rank || 0));
			}
		} catch (e) {
			console.warn(`[GPT] Top-5 outline attempt ${attempt} failed`, e.message);
		}
	}
	return null;
}

/* -------------------------------------------------------------------------- */
/*  Cloudinary image normalization/cache (robust: remote fetch + local fallback) */
/* -------------------------------------------------------------------------- */
async function uploadTrendImageToCloudinarySmart(url, ratio, publicId) {
	const normalized = normalizeRemoteUrl(url);
	if (!normalized) throw new Error("Missing/invalid image URL");

	const baseOpts = {
		public_id:
			publicId ||
			`aivideomatic/trend_seeds/${Date.now()}_${crypto.randomUUID()}`,
		resource_type: "image",
		overwrite: false,
		folder: "aivideomatic/trend_seeds",
		transformation: buildCloudinaryTransformForRatio(ratio),
	};

	// Try Cloudinary remote fetch first
	try {
		const r = await cloudinary.uploader.upload(normalized, baseOpts);
		return {
			originalUrl: normalized,
			public_id: r.public_id,
			url: r.secure_url,
		};
	} catch (e) {
		const msg = String(e?.message || "");
		console.warn("[Cloudinary] remote fetch failed; trying local upload:", msg);
	}

	// Local fallback (handles hotlink protections and fixes &amp; problems too)
	let local = null;
	try {
		local = await downloadImageToTemp(normalized, ".jpg");
		const r = await cloudinary.uploader.upload(local, baseOpts);
		return {
			originalUrl: normalized,
			public_id: r.public_id,
			url: r.secure_url,
		};
	} finally {
		unlinkSafe(local);
	}
}

async function buildUsableImagePairs({
	urls,
	ratio,
	publicIdPrefix,
	maxImages = 8,
}) {
	const input = (Array.isArray(urls) ? urls : [])
		.map(normalizeRemoteUrl)
		.filter(Boolean);

	const seen = new Set();
	const deduped = [];
	for (const u of input) {
		if (seen.has(u)) continue;
		seen.add(u);
		deduped.push(u);
	}

	const usablePairs = [];
	for (let i = 0; i < deduped.length && usablePairs.length < maxImages; i++) {
		const u = deduped[i];

		// Reachability check BEFORE Cloudinary work
		const ok = await isLikelyImageUrlReachable(u);
		if (!ok) {
			console.warn("[Images] unreachable, skipping", u);
			continue;
		}

		try {
			const up = await uploadTrendImageToCloudinarySmart(
				u,
				ratio,
				publicIdPrefix ? `${publicIdPrefix}_${i}` : undefined
			);

			// Verify final URL too (if Cloudinary returns something unexpected, rare)
			const finalUrl = up.url || u;
			const ok2 = await isLikelyImageUrlReachable(finalUrl);
			if (!ok2) {
				console.warn(
					"[Images] uploaded but final unreachable, skipping",
					finalUrl
				);
				continue;
			}

			usablePairs.push({ originalUrl: u, cloudinaryUrl: up.url });
		} catch (e) {
			console.warn("[Images] failed to upload usable image", u, e.message);
		}
	}

	return usablePairs;
}

async function generateOpenAIEditorialFallbackImages({
	segments,
	ratio,
	topic,
	category,
	maxCount = null,
	startIndex = 0,
}) {
	const size = openAIImageSizeForRatio(ratio);
	const outputs = [];

	const safeSegments = Array.isArray(segments) ? segments : [];
	const start = Math.max(0, Number(startIndex) || 0);
	const limitBase = Number.isFinite(maxCount)
		? Math.max(0, Math.min(maxCount, 10))
		: Math.min(safeSegments.length - start, 10);
	const limit = Math.max(0, Math.min(limitBase, safeSegments.length - start));

	for (
		let idx = start;
		idx < safeSegments.length && outputs.length < limit;
		idx++
	) {
		const seg = safeSegments[idx];
		const excerpt = String(seg?.scriptText || "")
			.replace(/\s+/g, " ")
			.trim()
			.slice(0, 160);

		// Keep it generic to avoid logos/trademarks while still relevant.
		const prompt = [
			"Vertical editorial sports-news photograph, cinematic lighting, sharp focus",
			`Topic: ${topic}`,
			`Scene idea: ${excerpt}`,
			"Professional athletes, generic uniforms, no logos, no readable text, no brand names, no watermarks",
			"Clean composition, realistic faces, natural motion blur only, shallow depth of field",
		].join(". ");

		try {
			const resp = await openai.images.generate({
				model: "gpt-image-1",
				prompt,
				size,
				quality: "hd",
				response_format: "url",
			});
			const imgUrl = resp?.data?.[0]?.url;
			if (!imgUrl) continue;

			const up = await uploadTrendImageToCloudinarySmart(
				imgUrl,
				ratio,
				`aivideomatic/fallback_${category}_${Date.now()}_${idx}`
			);

			outputs.push({ originalUrl: imgUrl, cloudinaryUrl: up.url });
		} catch (e) {
			console.warn(`[OpenAI Fallback Image] seg ${idx + 1} failed`, e.message);
		}
	}

	return outputs;
}

async function ensureUniqueImagesPerSegment({
	segments,
	imagePairs,
	ratio,
	topic,
	category,
	allowGeneration = AUTO_GENERATE_FALLBACK_IMAGES,
}) {
	if (!Array.isArray(segments) || !segments.length)
		return { segments, imagePairs };

	const pairs = Array.isArray(imagePairs) ? [...imagePairs] : [];
	const plannedIndexes = new Array(segments.length).fill(null);
	const needs = [];
	const used = new Set();

	segments.forEach((seg, idx) => {
		const candidate =
			Number.isInteger(seg.imageIndex) && seg.imageIndex >= 0
				? seg.imageIndex
				: null;
		const valid =
			candidate !== null &&
			candidate < pairs.length &&
			candidate >= 0 &&
			!used.has(candidate);
		if (valid) {
			plannedIndexes[idx] = candidate;
			used.add(candidate);
		} else {
			needs.push(idx);
		}
	});

	if (needs.length && allowGeneration) {
		const extraImages = await generateOpenAIEditorialFallbackImages({
			segments: needs.map((i) => segments[i]),
			ratio,
			topic,
			category,
			maxCount: needs.length,
		});

		for (let i = 0; i < needs.length; i++) {
			const segIdx = needs[i];
			const extra = extraImages[i];
			if (extra && extra.cloudinaryUrl) {
				pairs.push(extra);
				const newIdx = pairs.length - 1;
				plannedIndexes[segIdx] = newIdx;
				used.add(newIdx);
			}
		}
	}

	for (let i = 0; i < plannedIndexes.length; i++) {
		if (plannedIndexes[i] !== null) continue;
		let candidate = 0;
		while (candidate < pairs.length && used.has(candidate)) candidate++;
		if (candidate < pairs.length) {
			plannedIndexes[i] = candidate;
			used.add(candidate);
		} else if (pairs.length > 0) {
			plannedIndexes[i] = i % pairs.length;
		} else {
			plannedIndexes[i] = null;
		}
	}

	const updatedSegments = segments.map((seg, idx) => ({
		...seg,
		imageIndex: plannedIndexes[idx],
	}));

	return { segments: updatedSegments, imagePairs: pairs };
}

/* -------------------------------------------------------------------------- */
/*  OpenAI director — build full video plan                                    */
/* -------------------------------------------------------------------------- */
async function buildVideoPlanWithGPT({
	topic,
	category,
	language,
	duration,
	segLens,
	trendStory,
	trendImagesForPlanning,
	articleText,
	top5Outline,
	ratio,
	trendImageBriefs,
	engagementTailSeconds,
	country,
}) {
	const segCnt = segLens.length;
	const segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));
	const hasImages =
		Array.isArray(trendImagesForPlanning) && trendImagesForPlanning.length > 0;
	const images = hasImages ? trendImagesForPlanning.slice(0, 10) : [];
	const articleTitles = (trendStory?.articles || [])
		.map((a) => a.title)
		.filter(Boolean);
	const snippet = articleText ? articleText.slice(0, 1800) : "";
	const imageBriefs = Array.isArray(trendImageBriefs) ? trendImageBriefs : [];
	const imageComment = String(trendStory?.imageComment || "").trim();

	const segDescLines = segLens
		.map(
			(sec, i) =>
				`Segment ${i + 1}: ~${sec.toFixed(1)}s, max ${segWordCaps[i]} words.`
		)
		.join("\n");

	const categoryTone = TONE_HINTS[category] || "";
	const outroDirective = `
Segment ${segCnt} is the engagement outro (about ${
		engagementTailSeconds || "5-6"
	} seconds):
- Ask ONE crisp, on-topic question to spark comments.
- Immediately follow with a warm, playful like/subscribe/comment nudge that sounds like a friendly host (American audience).
- End with a 3-5 word friendly sign-off (e.g., "See you tomorrow!" or "Stay curious!").
- Entirely in ${language}.
`.trim();

	const baseIntro = `
Current date: ${dayjs().format("YYYY-MM-DD")}
You are an expert short-form video editor and producer.

We need a ${duration}s ${category} YouTube Shorts video titled "${topic}", split into ${segCnt} segments.

Segment timing:
${segDescLines}

Visual engine: OpenAI Sora text-to-video. Each "soraPrompt" must be one cinematic shot with obvious motion; avoid Runway-specific jargon, avoid still frames, and never ask for text overlays or logos.

Narration rules:
- Natural spoken language, human host.
- Do NOT mention AI, bots, or automation (unless the topic is AI tech; no meta disclaimers).
- Accurate; don't invent quotes/scores/injuries.
- Segment 1 hooks immediately.
- Only the final segment contains the CTA.
- All narration must be in ${language} (even if country is ${country}).
${categoryTone ? `- Tone: ${categoryTone}` : ""}
${outroDirective}
`.trim();

	let promptText = "";
	if (hasImages) {
		promptText = `
${baseIntro}

You have ${
			images.length
		} REAL photos from Google Trends. They are attached below.
The first attached image is imageIndex 0, second is 1, etc.

Trends context:
- Story title: ${trendStory?.title || topic}
- Article headlines:
${
	articleTitles.length
		? articleTitles.map((t) => `  - ${t}`).join("\n")
		: "  - (none)"
}

Article snippet:
${snippet || "(none)"}

Image notes:
- Lead image comment: ${imageComment || "(none)"}
- Viral hooks by aspect ratio (UI requested ratio ${ratio}):
${
	imageBriefs.length
		? imageBriefs
				.map(
					(b) =>
						`  - ${b.aspectRatio}: ${b.visualHook}${
							b.emotion ? ` | emotion: ${b.emotion}` : ""
						}`
				)
				.join("\n")
		: "  - (none)"
}

Your output for each segment:
- scriptText (spoken)
- imageIndex (pick best matching attached photo)
- soraPrompt (Sora text-to-video prompt for that exact attached photo; cinematic, realistic motion; text-only; no real names/brands/logos)
- negativePrompt (comma-separated defects Sora must avoid)

Rules for soraPrompt:
- Match the photo’s setting, clothing, lighting, mood.
- Keep clear motion: camera move and/or subject motion.
- Physical realism, natural faces/eyes.
- Do NOT mention Runway or ask for text overlays; this feeds Sora directly.

Return JSON:
{ "segments": [ { "index": 1, "scriptText": "...", "imageIndex": 0, "soraPrompt": "...", "negativePrompt": "..." }, ... ] }
`.trim();
	} else if (
		category === "Top5" &&
		Array.isArray(top5Outline) &&
		top5Outline.length
	) {
		const outlineText = top5Outline
			.map((it) => `#${it.rank}: ${it.label || ""} — ${it.oneLine || ""}`)
			.join("\n");

		promptText = `
${baseIntro}

This is a Top 5 countdown. Outline:
${outlineText}

Rules:
- Segment 1 teases the countdown and hooks viewers.
- Segments 2-6 correspond to ranks #5..#1, and each MUST start with "#5:", "#4:", "#3:", "#2:", "#1:".
- Segment ${segCnt} is the engagement outro.

No images are provided; imagine visuals from scratch.

For each segment output:
- index
- scriptText
- soraPrompt (Sora text-to-video prompt with explicit motion; no real names/brands/logos; single cinematic shot)
- negativePrompt
- overlayText (4-7 words, matches voice beat)
- referenceImageUrl (direct link to high-quality editorial-style photo)
- Keep it Sora-friendly: no Runway wording, no text overlays, clear motion.

Return JSON:
{ "segments": [ { "index": 1, "scriptText": "...", "soraPrompt": "...", "negativePrompt": "...", "overlayText": "...", "referenceImageUrl": "https://..." }, ... ] }
`.trim();
	} else {
		promptText = `
${baseIntro}

No reliable images are available. Imagine visuals from scratch.

Return JSON:
{ "segments": [ { "index": 1, "scriptText": "...", "soraPrompt": "...", "negativePrompt": "..." }, ... ] }

Visual rules:
- Realistic scenes, good lighting, natural faces.
- EVERY soraPrompt must include explicit motion and read like a single cinematic shot for Sora.
- No real names/brands/logos.
`.trim();
	}

	const contentParts = [{ type: "text", text: promptText }];
	if (hasImages) {
		for (const url of images)
			contentParts.push({ type: "image_url", image_url: { url } });
	}

	const raw = strip(await gptText(contentParts));
	const plan = JSON.parse(raw);

	if (!Array.isArray(plan?.segments) || plan.segments.length !== segCnt) {
		throw new Error(
			`GPT plan returned ${
				plan?.segments?.length || 0
			} segments, expected ${segCnt}`
		);
	}

	let segments = plan.segments.map((s, idx) => ({
		index: typeof s.index === "number" ? s.index : idx + 1,
		scriptText: String(s.scriptText || "").trim(),
		soraPrompt: String(s.soraPrompt || s.visualPrompt || "").trim(),
		soraNegativePrompt: String(
			s.negativePrompt || s.soraNegativePrompt || s.runwayNegativePrompt || ""
		).trim(),
		overlayText: String(s.overlayText || s.overlay || "").trim(),
		referenceImageUrl: normalizeRemoteUrl(s.referenceImageUrl) || "",
		imageIndex: Number.isInteger(s.imageIndex) ? s.imageIndex : null,
	}));

	if (hasImages) {
		const imgCount = images.length;
		segments = segments.map((seg) => {
			const idx = Number.isInteger(seg.imageIndex) ? seg.imageIndex : null;
			return {
				...seg,
				imageIndex: idx !== null && idx >= 0 && idx < imgCount ? idx : null,
			};
		});
		segments = planImageIndexes(segments, imgCount);
	} else {
		segments = segments.map((seg) => ({ ...seg, imageIndex: null }));
	}

	return { segments };
}

/* -------------------------------------------------------------------------- */
/*  YouTube helpers                                                            */
/* -------------------------------------------------------------------------- */
function resolveYouTubeTokens(req, user) {
	const bodyTok = {
		access_token: req.body.youtubeAccessToken,
		refresh_token: req.body.youtubeRefreshToken,
		expiry_date: req.body.youtubeTokenExpiresAt
			? new Date(req.body.youtubeTokenExpiresAt).getTime()
			: undefined,
	};
	const userTok = {
		access_token: user.youtubeAccessToken,
		refresh_token: user.youtubeRefreshToken,
		expiry_date: user.youtubeTokenExpiresAt
			? new Date(user.youtubeTokenExpiresAt).getTime()
			: undefined,
	};

	return bodyTok.refresh_token &&
		(!userTok.refresh_token ||
			(userTok.expiry_date || 0) < (bodyTok.expiry_date || 0))
		? bodyTok
		: userTok;
}

function buildYouTubeOAuth2Client(source) {
	let creds = source;
	if (source && source.youtubeRefreshToken && !source.refresh_token) {
		creds = {
			access_token: source.youtubeAccessToken,
			refresh_token: source.youtubeRefreshToken,
			expiry_date: source.youtubeTokenExpiresAt
				? new Date(source.youtubeTokenExpiresAt).getTime()
				: undefined,
		};
	}

	if (!creds?.refresh_token) return null;

	const o = new google.auth.OAuth2(
		ENV.YOUTUBE_CLIENT_ID,
		ENV.YOUTUBE_CLIENT_SECRET,
		ENV.YOUTUBE_REDIRECT_URI
	);

	o.setCredentials({
		access_token: creds.access_token,
		refresh_token: creds.refresh_token,
		expiry_date: creds.expiry_date,
	});

	return o;
}

async function refreshYouTubeTokensIfNeeded(user, req) {
	const tokens = resolveYouTubeTokens(req, user);
	const o = buildYouTubeOAuth2Client(tokens);
	if (!o) return tokens;

	try {
		const resp = await o.getAccessToken();
		const token = typeof resp === "string" ? resp : resp?.token;
		if (token) {
			const fresh = {
				access_token: o.credentials.access_token,
				refresh_token: o.credentials.refresh_token || tokens.refresh_token,
				expiry_date: o.credentials.expiry_date,
			};
			user.youtubeAccessToken = fresh.access_token;
			user.youtubeRefreshToken = fresh.refresh_token;
			user.youtubeTokenExpiresAt = fresh.expiry_date;
			if (user.isModified?.() && user.role !== "admin") await user.save();
			return fresh;
		}
	} catch {}
	return tokens;
}

async function uploadToYouTube(
	tokens,
	filePath,
	{ title, description, tags, category }
) {
	const o = buildYouTubeOAuth2Client(tokens);
	if (!o) throw new Error("YouTube OAuth missing");

	const yt = google.youtube({ version: "v3", auth: o });
	const categoryId =
		YT_CATEGORY_MAP[category] === "0" ? "22" : YT_CATEGORY_MAP[category];

	const { data } = await yt.videos.insert(
		{
			part: ["snippet", "status"],
			requestBody: {
				snippet: { title, description, tags, categoryId },
				status: { privacyStatus: "public", selfDeclaredMadeForKids: false },
			},
			media: { body: fs.createReadStream(filePath) },
		},
		{ maxContentLength: Infinity, maxBodyLength: Infinity }
	);

	return `https://www.youtube.com/watch?v=${data.id}`;
}

/* -------------------------------------------------------------------------- */
/*  ElevenLabs + Music (unchanged core logic)                                  */
/* -------------------------------------------------------------------------- */
const NUM_WORD = Object.freeze(
	Object.fromEntries(
		[
			"one",
			"two",
			"three",
			"four",
			"five",
			"six",
			"seven",
			"eight",
			"nine",
			"ten",
			"eleven",
			"twelve",
			"thirteen",
			"fourteen",
			"fifteen",
			"sixteen",
			"seventeen",
			"eighteen",
			"nineteen",
			"twenty",
		].map((w, i) => [i + 1, w])
	)
);

function improveTTSPronunciation(text) {
	let t = String(text || "");
	t = t.replace(/#\s*([1-5])\s*:/g, (_, n) => `Number ${NUM_WORD[n]}:`);
	return t.replace(/\b([1-9]|1[0-9]|20)\b/g, (_, n) => NUM_WORD[n] || n);
}

async function fetchElevenVoices() {
	if (!ELEVEN_API_KEY) return null;
	try {
		const { data } = await axios.get("https://api.elevenlabs.io/v1/voices", {
			headers: { "xi-api-key": ELEVEN_API_KEY },
			timeout: 8000,
		});
		const voices = Array.isArray(data?.voices) ? data.voices : [];
		if (!voices.length) return null;
		return voices;
	} catch (e) {
		console.warn("[Eleven] fetch voices failed", e.message);
		return null;
	}
}

function slimVoices(voices, limit = 30) {
	return (voices || [])
		.filter((v) => v && (v.voice_id || v.voiceId))
		.slice(0, limit)
		.map((v) => ({
			id: v.voice_id || v.voiceId,
			name: v.name || "",
			category: v.category || "",
			labels: v.labels || {},
			description: v.description || "",
		}));
}

async function selectBestElevenVoice(
	language,
	category,
	sampleText,
	avoidVoiceIds = []
) {
	const avoidSet = new Set((avoidVoiceIds || []).filter(Boolean));

	const staticLanguageVoice = ELEVEN_VOICES[language];
	const staticDefaultVoice = ELEVEN_VOICES[DEFAULT_LANGUAGE];

	const fallbackCandidates = [staticLanguageVoice, staticDefaultVoice].filter(
		(id, idx, arr) => id && arr.indexOf(id) === idx && !avoidSet.has(id)
	);
	const fallbackId =
		fallbackCandidates[0] || staticLanguageVoice || staticDefaultVoice || null;

	const voices = await fetchElevenVoices();
	if (!voices || !voices.length) {
		if (fallbackId) {
			return {
				voiceId: fallbackId,
				name: "default",
				source: "static",
				reason:
					"ElevenLabs /voices unavailable, using static fallback map (avoiding previous voice where possible).",
			};
		}
		return null;
	}

	const slim = slimVoices(voices, 40);
	let candidates = slim;

	if (language === "Arabic") {
		const isEgyptian = (v) => {
			const labels = v.labels || {};
			const labelStr = JSON.stringify(labels).toLowerCase();
			const desc = String(v.description || "").toLowerCase();
			const name = String(v.name || "").toLowerCase();
			return (
				labelStr.includes("egypt") ||
				desc.includes("egypt") ||
				name.includes("egypt") ||
				name.includes("masri") ||
				desc.includes("masri")
			);
		};

		const isArabic = (v) => {
			const labels = v.labels || {};
			const labelStr = JSON.stringify(labels).toLowerCase();
			const desc = String(v.description || "").toLowerCase();
			const name = String(v.name || "").toLowerCase();
			return (
				labelStr.includes("arabic") ||
				desc.includes("arabic") ||
				name.includes("arabic")
			);
		};

		const egyptian = slim.filter(isEgyptian);
		const arabic = slim.filter(isArabic);
		if (egyptian.length) {
			candidates = egyptian;
			console.log(
				`[Eleven] Prioritising Egyptian Arabic voices (${egyptian.length})`
			);
		} else if (arabic.length) {
			candidates = arabic;
			console.log(`[Eleven] Prioritising Arabic voices (${arabic.length})`);
		}
	}

	if (language === "English") {
		const americanCandidates = slim.filter((v) => {
			const labels = v.labels || {};
			const labelStr = JSON.stringify(labels).toLowerCase();
			const accent = String(labels.accent || labels.Accent || "").toLowerCase();
			const desc = String(v.description || "").toLowerCase();
			const name = String(v.name || "").toLowerCase();

			if (accent.includes("american") || accent.includes("us")) return true;
			if (labelStr.includes("american") || labelStr.includes("us english"))
				return true;
			if (desc.includes("american accent") || desc.includes("us accent"))
				return true;
			if (name.includes("us ") || name.includes("usa")) return true;
			return false;
		});

		if (americanCandidates.length) {
			candidates = americanCandidates;
			console.log(
				`[Eleven] Restricted to ${americanCandidates.length} American English voices`
			);
		} else if (fallbackId) {
			console.warn(
				"[Eleven] No explicit American English voices detected in /voices - using static fallback voice."
			);
			return {
				voiceId: fallbackId,
				name: "default",
				source: "static-fallback-no-american-tag",
				reason:
					"Could not confidently find an American-accent English voice in /voices; using predefined US voice.",
			};
		}
	}

	if (avoidSet.size) {
		const filtered = candidates.filter((v) => !avoidSet.has(v.id));
		if (filtered.length) {
			console.log(
				`[Eleven] Avoiding previously used voice(s) ${[...avoidSet].join(", ")}`
			);
			candidates = filtered;
		} else {
			console.warn(
				"[Eleven] All candidate voices are in the avoid list - keeping full candidate set."
			);
		}
	}

	const tone = deriveVoiceSettings(sampleText || "", category);
	const avoidText = avoidSet.size
		? `
The last video used these voice IDs: ${Array.from(avoidSet).join(", ")}.
You MUST choose a different "id" than any of these from the voices array below.
`
		: "";

	const ask = `
You are selecting the MOST natural-sounding ElevenLabs voice for a YouTube Shorts narration.

Goal:
- Category: ${category}
- Language preference label: ${language}
- Script tone: ${
		tone.isSensitive ? "sensitive / serious" : "neutral to energetic"
	}
- It should sound like a real human news or sports broadcaster, never robotic.

${
	language === "English"
		? "- IMPORTANT: Only select a voice with a clearly American / US English accent.\n- Do NOT pick British, Australian or other non-US accents."
		: ""
}

${avoidText}

You are given a JSON array called "voices" with candidate voices from the ElevenLabs /voices API.
Pick ONE best "id" to use.

voices:
${JSON.stringify(candidates).slice(0, 11000)}

Return ONLY JSON:
{ "voiceId": "<id>", "name": "<readable name>", "reason": "short explanation" }
`.trim();

	try {
		const parsed = await gptJSON(ask, { retries: 1 });
		if (parsed?.voiceId) {
			return {
				voiceId: parsed.voiceId,
				name: parsed.name || "",
				source: "dynamic-gpt",
				reason: parsed.reason || "",
			};
		}
	} catch (e) {
		console.warn("[Eleven] GPT voice selection failed", e.message);
	}

	if (fallbackId) {
		const reused = avoidSet.has(fallbackId);
		if (reused) {
			console.warn(
				`[Eleven] Fallback voice ${fallbackId} is the same as last used; no alternative available.`
			);
		}
		return {
			voiceId: fallbackId,
			name: "default",
			source: reused ? "static-fallback-reused" : "static-fallback",
			reason: reused
				? "Only one suitable voice available; reusing previous voice."
				: "Falling back to predefined voice mapping.",
		};
	}

	return null;
}

async function elevenLabsTTS(
	text,
	language,
	outPath,
	category = "Other",
	voiceIdOverride = null
) {
	if (!ELEVEN_API_KEY) throw new Error("ELEVENLABS_API_KEY missing");

	const voiceId =
		voiceIdOverride ||
		ELEVEN_VOICES[language] ||
		ELEVEN_VOICES[DEFAULT_LANGUAGE];
	const tone = deriveVoiceSettings(text, category);

	const payload = {
		text,
		model_id: "eleven_multilingual_v2",
		voice_settings: {
			stability: tone.stability,
			similarity_boost: tone.similarityBoost,
			style: tone.style,
			use_speaker_boost: true,
		},
	};

	const opts = {
		headers: {
			"xi-api-key": ELEVEN_API_KEY,
			"Content-Type": "application/json",
			accept: "audio/mpeg",
		},
		responseType: "stream",
		validateStatus: (s) => s < 500,
	};

	const url = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream?output_format=mp3_44100_128`;
	let res = await axios.post(url, payload, opts);
	if (res.status === 422) {
		delete payload.voice_settings.style;
		res = await axios.post(url, payload, opts);
	}
	if (res.status >= 300)
		throw new Error(`ElevenLabs TTS failed (${res.status})`);

	await new Promise((r, j) =>
		res.data.pipe(fs.createWriteStream(outPath)).on("finish", r).on("error", j)
	);

	return tone;
}

/* -------------------------------------------------------------------------- */
/*  Background music (Jamendo)                                                 */
/* -------------------------------------------------------------------------- */
async function jamendo(term) {
	try {
		const { data } = await axios.get("https://api.jamendo.com/v3.0/tracks", {
			params: { client_id: JAMENDO_ID, format: "json", limit: 1, search: term },
			timeout: 12000,
		});
		return data.results?.length ? data.results[0].audio : null;
	} catch {
		return null;
	}
}

async function planBackgroundMusic(category, language, script) {
	const defaultVoiceGain = category === "Top5" ? 1.5 : 1.4;
	const defaultMusicGain = category === "Top5" ? 0.18 : 0.14;

	const ask = `
You are a sound designer for short-form YouTube videos.

Goal:
- Category: ${category}
- Language: ${language}
- Script (excerpt): ${String(script || "").slice(0, 600)}

Pick background music that:
- Has NO vocals (instrumental only).
- Fits pacing + emotion.
- Never overpowers narration.

Return JSON:
{
  "jamendoSearch": "one concise English search term implying no vocals",
  "fallbackSearchTerms": ["term1", "term2"],
  "voiceGain": ${defaultVoiceGain},
  "musicGain": ${defaultMusicGain}
}

Constraints:
- fallbackSearchTerms: exactly 2
- voiceGain: 1.2..1.7
- musicGain: 0.08..0.22
`.trim();

	try {
		const parsed = await gptJSON(ask, { retries: 1 });
		let voiceGain = Math.max(
			1.2,
			Math.min(1.7, Number(parsed.voiceGain) || defaultVoiceGain)
		);
		let musicGain = Math.max(
			0.08,
			Math.min(0.22, Number(parsed.musicGain) || defaultMusicGain)
		);
		const fallbackSearchTerms = Array.isArray(parsed.fallbackSearchTerms)
			? parsed.fallbackSearchTerms
					.map((t) => String(t || "").trim())
					.filter(Boolean)
					.slice(0, 2)
			: [];
		return {
			jamendoSearch: String(parsed.jamendoSearch || "").slice(0, 120),
			fallbackSearchTerms,
			voiceGain,
			musicGain,
		};
	} catch (e) {
		console.warn("[MusicPlan] planning failed", e.message);
		return null;
	}
}

/* -------------------------------------------------------------------------- */
/*  Sora (TEXT→VIDEO ONLY)                                                     */
/* -------------------------------------------------------------------------- */
async function generateSoraClip({
	segmentIndex,
	promptText,
	ratio,
	targetDuration,
	soraSecondsOverride,
	topic = "",
}) {
	const seconds =
		soraSecondsOverride && Number(soraSecondsOverride)
			? String(Number(soraSecondsOverride))
			: soraSecondsForSegment(targetDuration);
	const size = soraSizeForRatio(ratio);
	const prompt = sanitizeSoraPrompt(promptText, topic, ratio);

	console.log(
		`[Sora] seg ${segmentIndex}: TEXT-ONLY seconds=${seconds} size=${size}`
	);

	const job = await openai.videos.create({
		model: SORA_MODEL,
		prompt,
		seconds,
		size,
	});

	let status = job.status;
	let attempts = 0;

	while (
		(status === "queued" || status === "in_progress") &&
		attempts < MAX_POLL_ATTEMPTS
	) {
		await sleep(POLL_INTERVAL_MS);
		const updated = await openai.videos.retrieve(job.id);
		status = updated.status;
		attempts++;
	}

	if (status !== "completed") {
		const err = new Error(
			job.error?.message || `Sora job ${job.id} failed (status=${status})`
		);
		err.code = job.error?.code;
		throw err;
	}

	const response = await openai.videos.downloadContent(job.id, {
		variant: "video",
	});
	const buf = Buffer.from(await response.arrayBuffer());
	const outPath = tmpFile(`sora_${segmentIndex}`, ".mp4");
	fs.writeFileSync(outPath, buf);
	return outPath;
}

/* -------------------------------------------------------------------------- */
/*  Scheduler helpers                                                          */
/* -------------------------------------------------------------------------- */
async function computeNextRunPST({ type, timeOfDay, startDate, endDate }) {
	const startDateStr = dayjs(startDate).format("YYYY-MM-DD");
	let next = dayjs.tz(
		`${startDateStr} ${timeOfDay}`,
		"YYYY-MM-DD HH:mm",
		PST_TZ
	);

	const nowPST = dayjs().tz(PST_TZ);
	while (next.isBefore(nowPST)) {
		if (type === "daily") next = next.add(1, "day");
		else if (type === "weekly") next = next.add(1, "week");
		else if (type === "monthly") next = next.add(1, "month");
		else break;
	}

	const startPST = dayjs.tz(startDateStr, "YYYY-MM-DD", PST_TZ).startOf("day");
	const endPST =
		endDate && dayjs(endDate).isValid()
			? dayjs
					.tz(dayjs(endDate).format("YYYY-MM-DD"), "YYYY-MM-DD", PST_TZ)
					.startOf("day")
			: null;

	return {
		next: next.toDate(),
		start: startPST.toDate(),
		end: endPST ? endPST.toDate() : undefined,
	};
}

/* -------------------------------------------------------------------------- */
/*  SSE helpers                                                                */
/* -------------------------------------------------------------------------- */
function sseBoot(res) {
	res.setHeader("Content-Type", "text/event-stream");
	res.setHeader("Cache-Control", "no-cache");
	res.setHeader("Connection", "keep-alive");
	res.setHeader("X-Accel-Buffering", "no");
	if (typeof res.flushHeaders === "function") res.flushHeaders();

	const history = [];
	const send = (phase, extra = {}) => {
		const safe =
			phase === "COMPLETED" && extra.phases
				? { ...extra, phases: JSON.parse(JSON.stringify(extra.phases)) }
				: extra;
		res.write(`data:${JSON.stringify({ phase, extra: safe })}\n\n`);
		if (typeof res.flush === "function") res.flush();
		history.push({ phase, ts: Date.now(), extra: safe });
	};
	const fail = (msg) => {
		send("ERROR", { msg });
		try {
			res.end();
		} catch {}
	};
	return { send, fail, history };
}

function toBool(v) {
	return v === true || v === "true" || v === 1 || v === "1";
}

/* -------------------------------------------------------------------------- */
/*  Controller — createVideoSoraPro                                            */
/* -------------------------------------------------------------------------- */
exports.createVideoSoraPro = async (req, res) => {
	const { category, ratio: ratioIn, duration: durIn } = req.body;

	if (!category || !YT_CATEGORY_MAP[category])
		return res.status(400).json({ error: "Bad category" });
	if (!VALID_RATIOS.includes(ratioIn))
		return res.status(400).json({ error: "Bad ratio" });
	if (!goodDur(durIn)) return res.status(400).json({ error: "Bad duration" });

	const ratio = ratioIn;
	const duration = +durIn;

	const { send, fail, history } = sseBoot(res);
	send("INIT");
	res.setTimeout(0);

	try {
		const {
			language: langIn,
			country: countryIn,
			customPrompt: customPromptRaw = "",
			videoImage,
			schedule,
			youtubeEmail,
			useSora: useSoraIn,
		} = req.body;

		const user = req.user;
		const language = String(langIn || DEFAULT_LANGUAGE).trim();
		const country =
			countryIn && String(countryIn).toLowerCase() !== "all countries"
				? String(countryIn).trim()
				: "US";
		const customPrompt = String(customPromptRaw || "").trim();
		const useSora = toBool(useSoraIn);

		console.log(
			`[Job] user=${user.email} cat=${category} dur=${duration}s geo=${country} model=${SORA_MODEL} mode=${SORA_USAGE_MODE} useSora=${useSora}`
		);

		/* 1) Avoid duplicates: last 3 days topics */
		const threeDaysAgo = dayjs().subtract(3, "day").toDate();
		const recentVideos = await Video.find({
			user: user._id,
			category,
			createdAt: { $gte: threeDaysAgo },
		}).select("topic seoTitle");

		const usedTopics = new Set();
		for (const v of recentVideos) {
			const base = String(v.topic || v.seoTitle || "").trim();
			if (!base) continue;
			const n = base.toLowerCase().replace(/\s+/g, " ").trim();
			if (n) usedTopics.add(n);
			const two = n.split(" ").slice(0, 2).join(" ");
			const three = n.split(" ").slice(0, 3).join(" ");
			if (two.length >= 4) usedTopics.add(two);
			if (three.length >= 6) usedTopics.add(three);
		}

		/* 2) Topic selection */
		let topic = "";
		let trendStory = null;
		let trendArticleText = null;

		const userOverrides = Boolean(videoImage) || customPrompt.length > 0;

		if (!userOverrides && category !== "Top5") {
			trendStory = await fetchTrendingStory(
				category,
				country,
				usedTopics,
				language,
				ratio
			);
			if (trendStory?.title) {
				topic = trendStory.title;
				usedTopics.add(topic.toLowerCase().replace(/\s+/g, " ").trim());
			}
		}

		if (!topic && customPrompt) {
			try {
				topic = await topicFromCustomPrompt(customPrompt);
			} catch {}
		}

		if (!topic) {
			if (category === "Top5") {
				const remaining = ALL_TOP5_TOPICS.filter(
					(t) => !usedTopics.has(String(t).toLowerCase())
				);
				topic = remaining.length ? remaining[0] : choose(ALL_TOP5_TOPICS);
			} else {
				const list = await pickTrendingTopicFresh(category, language, country);
				const normT = (t) =>
					String(t || "")
						.toLowerCase()
						.replace(/\s+/g, " ")
						.trim();
				topic = list.find((t) => !usedTopics.has(normT(t))) || list[0];
			}
		}

		console.log(`[Job] final topic="${topic}"`);
		const topicIsAITopic = looksLikeAITopic(topic);

		/* 3) Richer context */
		if (trendStory?.articles?.length) {
			trendArticleText = await scrapeArticleText(
				trendStory.articles[0]?.url || null
			);
		}

		/* 4) Segment timing */
		const engagementTailSeconds = computeEngagementTail(duration);
		const totalDurationTarget = duration + engagementTailSeconds;

		let segLens = computeInitialSegLens(
			category,
			duration,
			engagementTailSeconds
		);
		console.log("[Timing] initial segment lengths", segLens);

		// Aim to upload up to 5 high-quality images from Trends (matches puppeteer output)
		const maxTrendUploads = Math.min(5, trendStory?.images?.length || 5);

		/* 5) Top5 outline */
		const top5Outline =
			category === "Top5" ? await generateTop5Outline(topic, language) : null;

		/* 6) Build ONE authoritative usable image-pairs list (FIXES YOUR PLACEHOLDER ISSUE) */
		let trendImagePairs = [];
		let hasTrendImages = false;

		const canUseTrendsImages =
			category !== "Top5" &&
			!userOverrides &&
			trendStory &&
			Array.isArray(trendStory.images) &&
			trendStory.images.length > 0;

		if (canUseTrendsImages) {
			send("FETCHING_IMAGES", {
				msg: "Validating + uploading trend images (up to 5, robust)...",
			});

			const slugBase = topic
				.toLowerCase()
				.replace(/[^\w]+/g, "_")
				.replace(/^_+|_+$/g, "")
				.slice(0, 40);

			// Upload a concise, high-quality set (up to 5) from the Google Trends bundle
			trendImagePairs = await buildUsableImagePairs({
				urls: trendStory.images.slice(0, 16),
				ratio,
				publicIdPrefix: `aivideomatic/trend_seeds/${slugBase}`,
				maxImages: maxTrendUploads,
			});
		}

		hasTrendImages = trendImagePairs.length > 0;

		// Planning URLs must come from the SAME pairs list (index alignment!)
		let trendImagesForPlanning = hasTrendImages
			? trendImagePairs
					.map((p) => p.cloudinaryUrl || p.originalUrl)
					.filter(Boolean)
			: null;

		/* 7) GPT builds segments + visuals */
		const plan = await buildVideoPlanWithGPT({
			topic,
			category,
			language,
			duration,
			segLens,
			trendStory: hasTrendImages ? trendStory : null,
			trendImagesForPlanning,
			articleText: trendArticleText,
			top5Outline,
			ratio,
			trendImageBriefs: trendStory?.viralImageBriefs || [],
			engagementTailSeconds,
			country,
		});

		let segments = plan.segments;

		/* 8) If we still have no images, auto-generate fallbacks to prevent gray segments */
		if (!hasTrendImages && AUTO_GENERATE_FALLBACK_IMAGES) {
			send("FALLBACK_IMAGES", {
				msg: "Generating fallback images to avoid placeholders...",
			});
			const generated = await generateOpenAIEditorialFallbackImages({
				segments,
				ratio,
				topic,
				category,
			});
			if (generated.length) {
				trendImagePairs = generated;
				hasTrendImages = true;
				trendImagesForPlanning = trendImagePairs.map(
					(p) => p.cloudinaryUrl || p.originalUrl
				);
			}
		}

		/* 9) Ensure every segment has a valid imageIndex if images exist */
		if (hasTrendImages) {
			segments = planImageIndexes(segments, trendImagePairs.length);
		}

		/* 10) Guarantee one image per segment (no repeats), topping up with AI fallbacks if needed */
		const uniqueness = await ensureUniqueImagesPerSegment({
			segments,
			imagePairs: trendImagePairs,
			ratio,
			topic,
			category,
			allowGeneration: AUTO_GENERATE_FALLBACK_IMAGES,
		});
		segments = uniqueness.segments;
		trendImagePairs = uniqueness.imagePairs;
		hasTrendImages = trendImagePairs.length > 0;

		/* 11) Tighten narration to word caps */
		const segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));
		for (let i = 0; i < segments.length; i++) {
			const words = segments[i].scriptText
				.trim()
				.split(/\s+/)
				.filter(Boolean).length;
			if (words <= segWordCaps[i]) continue;

			const ask = `
Rewrite the narration in active voice.
Keep key facts, remove filler.
Max ${segWordCaps[i]} words.
One or two sentences.

"${segments[i].scriptText}"
`.trim();

			segments[i].scriptText = (await gptText(ask)).trim();
		}

		segments = segments.map((seg) => ({
			...seg,
			scriptText: sanitizeAudienceFacingText(seg.scriptText, {
				allowAITopic: topicIsAITopic,
			}),
			overlayText: sanitizeAudienceFacingText(seg.overlayText, {
				allowAITopic: topicIsAITopic,
			}),
		}));

		/* 12) Ensure engagement outro has question + CTA */
		if (segments.length) {
			const tailCap = Math.floor(segLens[segLens.length - 1] * WORDS_PER_SEC);
			const lastIdx = segments.length - 1;
			segments[lastIdx] = {
				...segments[lastIdx],
				scriptText: enforceEngagementOutroText(segments[lastIdx].scriptText, {
					topic,
					wordCap: tailCap,
				}),
			};
		}

		const fullScript = segments.map((s) => s.scriptText.trim()).join(" ");

		/* 13) Recompute segment durations from script */
		const recomputed = recomputeSegmentDurationsFromScript(
			segments,
			totalDurationTarget
		);
		if (recomputed && recomputed.length === segLens.length) {
			console.log("[Timing] recomputed seg lens", {
				before: segLens,
				after: recomputed,
			});
			segLens = recomputed;
		}

		/* 14) Sora allocation (cost-aware) */
		const soraBudgetSeconds = useSora ? computeSoraBudgetSeconds(duration) : 0;
		const soraPlan = useSora
			? planSoraAllocation(segLens, soraBudgetSeconds)
			: null;

		segments = segments.map((s, idx) => ({
			...s,
			useSora: Boolean(soraPlan?.useSora?.[idx]),
		}));

		const soraSecondsPlanned = soraPlan?.totalSeconds || 0;
		const soraCostEstimateUSD =
			Number.isFinite(soraSecondsPlanned) &&
			Number.isFinite(SORA_PRICE_PER_SECOND)
				? +(soraSecondsPlanned * SORA_PRICE_PER_SECOND).toFixed(2)
				: null;

		send("SORA_BUDGET", {
			usageMode: SORA_USAGE_MODE,
			useSora,
			budgetSeconds: soraBudgetSeconds,
			soraSecondsPlanned,
			soraCostEstimateUSD,
		});

		/* 15) Global style phrase */
		let globalStyle = "";
		try {
			globalStyle = (
				await gptText(
					`Give one short cinematic style phrase describing visual mood, camera movement, and pacing for topic "${topic}". No real names/brands.`
				)
			)
				.replace(/^[-–•\s]+/, "")
				.trim();
		} catch {}

		/* 16) SEO title, description, tags */
		let seoTitle = "";
		try {
			const seedHeadlines = trendStory
				? [
						trendStory.youtubeShortTitle,
						trendStory.seoTitle,
						trendStory.rawTitle,
						...(trendStory.articles || []).map((a) => a.title),
				  ].filter(Boolean)
				: [topic];
			seoTitle = await generateSeoTitle(
				seedHeadlines,
				category,
				language,
				(trendArticleText || "").slice(0, 800)
			);
		} catch {}

		const fallbackTitle =
			category === "Top5"
				? `${chooseTitleCase(topic)} | Top 5`
				: `${chooseTitleCase(topic)} | Update`;
		if (!seoTitle) seoTitle = fallbackTitle;
		seoTitle =
			sanitizeAudienceFacingText(seoTitle, { allowAITopic: topicIsAITopic }) ||
			sanitizeAudienceFacingText(fallbackTitle, {
				allowAITopic: topicIsAITopic,
			}) ||
			fallbackTitle;

		const descRaw = await gptText(
			`Write a YouTube description (<=150 words) for "${seoTitle}". First 2 lines keyword-rich; add 1 quick CTA; end with 5-7 relevant hashtags. Never mention AI/bots/automation.`
		);
		const seoDescription = ensureClickableLinks(
			sanitizeAudienceFacingText(
				`${MERCH_INTRO}${descRaw.trim()}\n\n${BRAND_CREDIT}`,
				{
					allowAITopic: topicIsAITopic,
				}
			)
		);

		let tags = ["shorts", BRAND_TAG];
		try {
			const parsed = await gptJSON(
				`Return a JSON array of 5-8 SHORT tags (1-3 words) for "${seoTitle}". No hashtags. Avoid AI tags unless topic is AI tech.`
			);
			if (Array.isArray(parsed)) tags.push(...parsed);
		} catch {}
		if (category === "Top5") tags.unshift("Top5");
		tags = [...new Set(tags)]
			.map(
				(t) =>
					sanitizeAudienceFacingText(String(t), {
						allowAITopic: topicIsAITopic,
					}) || String(t)
			)
			.filter(Boolean);
		tags = [...new Set(tags)];

		/* 17) Load last voice + pick new ElevenLabs voice (avoid repetition) */
		let avoidVoiceIds = [];
		try {
			const last = await Video.findOne({
				user: user._id,
				"elevenLabsVoice.voiceId": { $exists: true },
			})
				.sort({ createdAt: -1 })
				.select("elevenLabsVoice");
			if (last?.elevenLabsVoice?.voiceId) {
				avoidVoiceIds.push(last.elevenLabsVoice.voiceId);
				console.log("[TTS] Last used ElevenLabs voice", {
					voiceId: last.elevenLabsVoice.voiceId,
				});
			}
		} catch (e) {
			console.warn(
				"[TTS] Unable to load last ElevenLabs voice metadata",
				e.message
			);
		}

		let chosenVoice = null;
		try {
			chosenVoice = await selectBestElevenVoice(
				language,
				category,
				fullScript,
				avoidVoiceIds
			);
			if (chosenVoice) {
				console.log("[TTS] Using ElevenLabs voice", {
					id: chosenVoice.voiceId,
					name: chosenVoice.name,
					source: chosenVoice.source,
					reason: chosenVoice.reason,
				});
			}
		} catch (e) {
			console.warn("[TTS] Voice selection failed", e.message);
		}

		/* 18) Background music */
		let music = null;
		let voiceGain = 1.4;
		let musicGain = 0.12;
		let backgroundMusicMeta = null;

		let musicPlan = null;
		let jamendoUrl = null;
		let jamendoSearchUsed = null;
		let jamendoSearchTermsTried = [];

		try {
			musicPlan = await planBackgroundMusic(category, language, fullScript);
			if (musicPlan) {
				voiceGain = Math.min(1.8, Math.max(1.1, musicPlan.voiceGain));
				musicGain = Math.min(0.25, Math.max(0.06, musicPlan.musicGain));
			}
		} catch {}

		try {
			const searchTerms = [];
			if (musicPlan?.jamendoSearch) searchTerms.push(musicPlan.jamendoSearch);
			if (Array.isArray(musicPlan?.fallbackSearchTerms))
				searchTerms.push(...musicPlan.fallbackSearchTerms);
			if (!searchTerms.length)
				searchTerms.push(
					topic.split(" ")[0],
					`${category.toLowerCase()} instrumental`,
					"ambient instrumental no vocals"
				);

			jamendoSearchTermsTried = searchTerms.slice();
			for (const term of searchTerms) {
				const u = await jamendo(term);
				if (u) {
					jamendoUrl = u;
					jamendoSearchUsed = term;
					break;
				}
			}

			if (jamendoUrl) {
				music = tmpFile("bg", ".mp3");
				const ws = fs.createWriteStream(music);
				const { data } = await axios.get(jamendoUrl, {
					responseType: "stream",
					timeout: 20000,
				});
				await new Promise((r, j) =>
					data.pipe(ws).on("finish", r).on("error", j)
				);
			}
		} catch (e) {
			console.warn("[Music] Jamendo failed", e.message);
		}

		if (musicPlan || jamendoUrl || jamendoSearchTermsTried.length) {
			backgroundMusicMeta = {
				plan: musicPlan || null,
				jamendoUrl: jamendoUrl || null,
				searchTerm: jamendoSearchUsed || musicPlan?.jamendoSearch || null,
				searchTermsTried: jamendoSearchTermsTried,
				voiceGain,
				musicGain,
			};
		}

		/* 19) Generate per-segment video clips (Sora + static fallback) */
		const segCnt = segLens.length;
		const soraPerSegSeconds = soraPlan?.perSegSeconds || [];
		const clips = [];

		send("GENERATING_CLIPS", {
			msg: "Generating Sora + static clips",
			total: segCnt,
			done: 0,
		});

		for (let i = 0; i < segCnt; i++) {
			const segIndex = i + 1;
			const d = segLens[i];
			const seg = segments[i];

			console.log(
				`[Seg ${segIndex}/${segCnt}] target=${d.toFixed(2)}s useSora=${
					seg.useSora ? "yes" : "no"
				}`
			);

			const basePrompt = [
				seg.soraPrompt,
				globalStyle,
				PROMPT_BITS.quality,
				PROMPT_BITS.physics,
				PROMPT_BITS.eyes,
				PROMPT_BITS.humanSafety,
				PROMPT_BITS.brand,
				`Avoid: ${PROMPT_BITS.defects}`,
			]
				.filter(Boolean)
				.join(". ");

			let promptText = scrubPromptForSafety(basePrompt);
			if (promptText.length > PROMPT_CHAR_LIMIT)
				promptText = promptText.slice(0, PROMPT_CHAR_LIMIT);

			const negative = (
				seg.soraNegativePrompt ||
				seg.runwayNegativePrompt ||
				PROMPT_BITS.defects
			).slice(0, PROMPT_CHAR_LIMIT);

			let clipPath = null;

			if (seg.useSora) {
				try {
					const merged = `${promptText}${
						negative ? `. Strictly avoid: ${negative}.` : ""
					}`;
					const override =
						Number(soraPerSegSeconds[i]) > 0
							? Number(soraPerSegSeconds[i])
							: null;
					clipPath = await generateSoraClip({
						segmentIndex: segIndex,
						promptText: merged,
						ratio,
						targetDuration: d,
						soraSecondsOverride: override,
						topic,
					});
				} catch (e) {
					if (isModerationBlock(e)) {
						console.warn(
							`[Sora] seg ${segIndex}: moderation_blocked -> static fallback`
						);
					} else {
						console.warn(
							`[Sora] seg ${segIndex}: failed -> static fallback`,
							e.message
						);
					}
				}
			}

			if (!clipPath) {
				// Use our authoritative pairs list (aligned indices)
				if (hasTrendImages && trendImagePairs.length) {
					const idx = Number.isInteger(seg.imageIndex) ? seg.imageIndex : 0;
					const pair = trendImagePairs[idx] || trendImagePairs[0] || null;

					const imgUrlCloudinary = pair?.cloudinaryUrl || null;
					const imgUrlOriginal = pair?.originalUrl || null;

					if (imgUrlOriginal || imgUrlCloudinary) {
						clipPath = await generateStaticClipFromImage({
							segmentIndex: segIndex,
							imgUrlOriginal,
							imgUrlCloudinary,
							ratio,
							targetDuration: d,
							zoomPan: true,
						});
					}
				} else if (seg.referenceImageUrl) {
					clipPath = await generateStaticClipFromImage({
						segmentIndex: segIndex,
						imgUrlOriginal: seg.referenceImageUrl,
						imgUrlCloudinary: null,
						ratio,
						targetDuration: d,
						zoomPan: true,
					});
				}
			}

			if (!clipPath) {
				clipPath = await generatePlaceholderClip({
					segmentIndex: segIndex,
					ratio,
					targetDuration: d,
				});
			}

			const fixed = tmpFile(`seg_${segIndex}`, ".mp4");
			await exactLen(clipPath, d, fixed, { ratio, enhance: true });
			unlinkSafe(clipPath);

			clips.push(fixed);
			send("GENERATING_CLIPS", {
				msg: `Rendering segment ${segIndex}/${segCnt}`,
				total: segCnt,
				done: segIndex,
			});
		}

		/* 20) Assemble silent video */
		send("ASSEMBLING_VIDEO", {
			msg: "Blending clips with cinematic transitions...",
		});

		let silent = null;
		try {
			silent = await concatWithTransitions(clips, segLens, ratio, 0.35);
		} catch (err) {
			console.warn(
				"[Transitions] Failed, falling back to direct concat",
				err.message
			);
			const listFile = tmpFile("concat", ".txt");
			fs.writeFileSync(
				listFile,
				clips.map((p) => `file '${norm(p)}'`).join("\n")
			);
			silent = tmpFile("silent", ".mp4");
			await ffmpegPromise((c) =>
				c
					.input(norm(listFile))
					.inputOptions("-f", "concat", "-safe", "0")
					.outputOptions("-c", "copy", "-y")
					.save(norm(silent))
			);
			unlinkSafe(listFile);
		} finally {
			for (const p of clips) unlinkSafe(p);
		}

		const silentFixed = tmpFile("silent_fix", ".mp4");
		await exactLen(silent, totalDurationTarget, silentFixed, {
			ratio,
			enhance: false,
		});
		unlinkSafe(silent);

		/* 21) Voice-over & music */
		send("ADDING_VOICE_MUSIC", { msg: "Creating audio layer" });

		const fixedPieces = [];
		let voiceToneSample = null;

		for (let i = 0; i < segCnt; i++) {
			const raw = tmpFile(`tts_raw_${i + 1}`, ".mp3");
			const fixed = tmpFile(`tts_fix_${i + 1}`, ".wav");
			const txt = improveTTSPronunciation(segments[i].scriptText);
			const localTone = deriveVoiceSettings(txt, category);
			if (!voiceToneSample) voiceToneSample = localTone;

			try {
				await elevenLabsTTS(
					txt,
					language,
					raw,
					category,
					chosenVoice?.voiceId || null
				);
			} catch (e) {
				console.warn(
					`[TTS] ElevenLabs failed seg ${i + 1}; fallback to OpenAI`,
					e.message
				);
				const tts = await openai.audio.speech.create({
					model: "tts-1-hd",
					voice: "shimmer",
					speed: localTone.openaiSpeed,
					input: txt,
					format: "mp3",
				});
				fs.writeFileSync(raw, Buffer.from(await tts.arrayBuffer()));
			}

			await exactLenAudio(raw, segLens[i], fixed, {
				allowTempo: false,
				allowPad: true,
			});
			unlinkSafe(raw);
			fixedPieces.push(fixed);
		}

		const audioList = tmpFile("audio_list", ".txt");
		fs.writeFileSync(
			audioList,
			fixedPieces.map((p) => `file '${norm(p)}'`).join("\n")
		);

		const ttsJoin = tmpFile("tts_join", ".wav");
		await ffmpegPromise((c) =>
			c
				.input(norm(audioList))
				.inputOptions("-f", "concat", "-safe", "0")
				.outputOptions("-c", "copy", "-y")
				.save(norm(ttsJoin))
		);
		unlinkSafe(audioList);
		for (const p of fixedPieces) unlinkSafe(p);

		const mixedRaw = tmpFile("mix_raw", ".wav");
		const mixed = tmpFile("mix_fix", ".wav");

		if (music) {
			const trim = tmpFile("music_trim", ".mp3");
			await ffmpegPromise((c) =>
				c
					.input(norm(music))
					.outputOptions("-t", String(totalDurationTarget), "-y")
					.save(norm(trim))
			);
			unlinkSafe(music);

			await ffmpegPromise((c) =>
				c
					.input(norm(ttsJoin))
					.input(norm(trim))
					.complexFilter([
						`[0:a]volume=${voiceGain.toFixed(3)}[a0]`,
						`[1:a]volume=${musicGain.toFixed(3)}[a1]`,
						"[a0][a1]amix=inputs=2:duration=first[aout]",
					])
					.outputOptions("-map", "[aout]", "-c:a", "pcm_s16le", "-y")
					.save(norm(mixedRaw))
			);
			unlinkSafe(trim);
		} else {
			await ffmpegPromise((c) =>
				c
					.input(norm(ttsJoin))
					.audioFilters("volume=1.4")
					.outputOptions("-c:a", "pcm_s16le", "-y")
					.save(norm(mixedRaw))
			);
		}
		unlinkSafe(ttsJoin);

		await exactLenAudio(mixedRaw, totalDurationTarget, mixed, {
			allowTempo: false,
			allowPad: true,
		});
		unlinkSafe(mixedRaw);

		/* 22) Mux audio + video */
		send("SYNCING_VOICE_MUSIC", { msg: "Muxing final video" });

		const safeTitle = seoTitle
			.toLowerCase()
			.replace(/[^\w\d]+/g, "_")
			.replace(/^_+|_+$/g, "");
		const finalPath = tmpFile(safeTitle || "video", ".mp4");

		await ffmpegPromise((c) =>
			c
				.input(norm(silentFixed))
				.input(norm(mixed))
				.outputOptions(
					"-map",
					"0:v",
					"-map",
					"1:a",
					"-c:v",
					"libx264",
					"-preset",
					"slow",
					"-crf",
					"17",
					"-c:a",
					"aac",
					"-t",
					String(totalDurationTarget),
					"-y"
				)
				.save(norm(finalPath))
		);
		unlinkSafe(silentFixed);
		unlinkSafe(mixed);

		/* 23) YouTube upload */
		let youtubeLink = "";
		let youtubeTokens = null;
		try {
			youtubeTokens = await refreshYouTubeTokensIfNeeded(user, req);
			if (buildYouTubeOAuth2Client(youtubeTokens)) {
				youtubeLink = await uploadToYouTube(youtubeTokens, finalPath, {
					title: seoTitle,
					description: seoDescription,
					tags,
					category,
				});
				send("VIDEO_UPLOADED", { youtubeLink });
			}
		} catch (e) {
			console.warn("[YouTube] upload skipped", e.message);
		}

		/* 24) Persist to Mongo */
		const elevenLabsVoice =
			chosenVoice || voiceToneSample
				? {
						voiceId: chosenVoice?.voiceId || null,
						name: chosenVoice?.name || null,
						source: chosenVoice?.source || null,
						reason: chosenVoice?.reason || null,
						category,
						language,
						tone: voiceToneSample || null,
				  }
				: null;

		const doc = await Video.create({
			user: user._id,
			category,
			topic,
			seoTitle,
			seoDescription,
			tags,
			script: fullScript,
			ratio,
			duration,
			model: SORA_MODEL,
			useSora,
			status: "SUCCEEDED",
			youtubeLink,
			language,
			country,
			customPrompt,
			refinedRunwayStub: customPrompt,
			videoImage,
			youtubeEmail,
			youtubeAccessToken:
				youtubeTokens?.access_token || req.body.youtubeAccessToken || "",
			youtubeRefreshToken:
				youtubeTokens?.refresh_token || req.body.youtubeRefreshToken || "",
			youtubeTokenExpiresAt: youtubeTokens?.expiry_date
				? new Date(youtubeTokens.expiry_date)
				: req.body.youtubeTokenExpiresAt
				? new Date(req.body.youtubeTokenExpiresAt)
				: undefined,
			elevenLabsVoice,
			backgroundMusic: backgroundMusicMeta,
		});

		/* 25) Optional scheduling */
		if (schedule) {
			const { type, timeOfDay, startDate, endDate } = schedule;
			const { next, start, end } = await computeNextRunPST({
				type,
				timeOfDay,
				startDate,
				endDate,
			});

			await new Schedule({
				user: user._id,
				category,
				video: doc._id,
				scheduleType: type,
				timeOfDay,
				startDate: start,
				endDate: end,
				nextRun: next,
				active: true,
			}).save();

			doc.scheduled = true;
			await doc.save();
			send("VIDEO_SCHEDULED", { msg: "Scheduled" });
		}

		/* 26) Done */
		send("COMPLETED", {
			id: doc._id,
			youtubeLink,
			phases: JSON.parse(JSON.stringify(history)),
		});
		try {
			res.end();
		} catch {}

		// Keep finalPath; delete if you prefer:
		// unlinkSafe(finalPath);
	} catch (err) {
		console.error("[createVideoSoraPro] ERROR", {
			message: err?.message,
			stack: err?.stack,
		});
		fail(err?.message || "Internal error");
	}
};

/* -------------------------------------------------------------------------- */
/*  Exports (required drop-in surface)                                         */
/* -------------------------------------------------------------------------- */
exports.buildYouTubeOAuth2Client = buildYouTubeOAuth2Client;
exports.refreshYouTubeTokensIfNeeded = refreshYouTubeTokensIfNeeded;
exports.uploadToYouTube = uploadToYouTube;
