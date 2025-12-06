/** @format */
/* videoController.js — high-motion, trends-driven edition (enhanced, multi-image) *
? Uses multiple Google Trends images per video for visual variety (hero + article images) *
? GPT is encouraged to rotate images; hard fallback enforces round-robin variety if GPT doesn't *
? Cloudinary normalises aspect ratio & cleanly crops images before Runway (no extra AI upscaling) *
? 25MP Cloudinary limit handled via local downscale ? always keep Trends photos *
? Runway image-to-video as the primary path; safety-only fallback to original static image *
? Static fallback now uses best-quality source (original URL first) while respecting aspect ratio with gentle scaling *
? Runway clips always = segment duration (no big freeze-frame padding) *
? OpenAI plans narration + visuals dynamically from Trends + article links *
? Prompts emphasise clear, human-like motion in every segment *
? ElevenLabs voice picked dynamically via /voices + GPT, with American accent for English *
? Orchestrator avoids reusing the last ElevenLabs voice for the same user when possible *
? Voice planning nudged towards clear, motivated, brisk American-style delivery (non-sensitive topics) *
? Background music planned via GPT (search term + voice/music gains) & metadata saved on Video *
? Script timing recomputed from words ? far fewer long pauses *
? Phases kept in sync with GenerationModal (INIT ? … ? COMPLETED / ERROR) */

const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const child_process = require("child_process");
const mongoose = require("mongoose");
const axios = require("axios");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");
const cheerio = require("cheerio");
const qs = require("querystring");

dayjs.extend(utc);
dayjs.extend(timezone);

const { google } = require("googleapis");
const { OpenAI } = require("openai");
const ffmpeg = require("fluent-ffmpeg");
const cloudinary = require("cloudinary").v2;

const Video = require("../models/Video");
const Schedule = require("../models/Schedule");
const {
	ALL_TOP5_TOPICS,
	googleTrendingCategoriesId,
} = require("../assets/utils");

cloudinary.config({
	cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
	api_key: process.env.CLOUDINARY_API_KEY,
	api_secret: process.env.CLOUDINARY_API_SECRET,
});

const PST_TZ = "America/Los_Angeles";

/* ---------------------------------------------------------------
 *  Runtime guards + ffmpeg bootstrap
 * ------------------------------------------------------------- */
function assertExists(cond, msg) {
	if (!cond) {
		console.error(`[Startup] FATAL – ${msg}`);
		process.exit(1);
	}
}

function resolveFfmpegPath() {
	if (process.env.FFMPEG_PATH) return process.env.FFMPEG_PATH;
	try {
		return require("ffmpeg-static");
	} catch {
		/* ignore */
	}
	return process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
}
const ffmpegPath = resolveFfmpegPath();
assertExists(
	(() => {
		try {
			child_process.execSync(`"${ffmpegPath}" -version`, { stdio: "ignore" });
			return true;
		} catch {
			return false;
		}
	})(),
	"FFmpeg binary not found – install ffmpeg or set FFMPEG_PATH."
);
ffmpeg.setFfmpegPath(ffmpegPath);

const ffprobePath = process.env.FFPROBE_PATH || "ffprobe";
ffmpeg.setFfprobePath(ffprobePath);
console.log(`[FFprobe]  binary : ${ffprobePath}`);

function ffmpegSupportsLavfi() {
	try {
		child_process.execSync(
			`"${ffmpegPath}" -hide_banner -loglevel error -f lavfi -i color=c=black:s=16x16:d=0.1 -frames:v 1 -f null -`,
			{ stdio: "ignore" }
		);
		return true;
	} catch {
		return false;
	}
}
const hasLavfi = ffmpegSupportsLavfi();
console.log(`[FFmpeg]   binary : ${ffmpegPath}`);
console.log(`[FFmpeg]   lavfi  ? ${hasLavfi}`);

/* font discovery (for any future overlays) */
function resolveFontPath() {
	const env = process.env.FFMPEG_FONT_PATH;
	if (env && fs.existsSync(env)) return env;
	const candidates = [
		path.join(__dirname, "../assets/fonts/DejaVuSans.ttf"),
		"/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
		"/usr/share/fonts/dejavu/DejaVuSans.ttf",
		"C:\\Windows\\Fonts\\arial.ttf",
	];
	for (const c of candidates) if (fs.existsSync(c)) return c;
	return null;
}
const FONT_PATH = resolveFontPath();
assertExists(
	FONT_PATH,
	"No valid TTF font found – set FFMPEG_FONT_PATH or install DejaVu/Arial."
);
const FONT_PATH_FFMPEG = FONT_PATH.replace(/\\/g, "/").replace(/:/g, "\\:");

/* ---------------------------------------------------------------
 *  Global constants
 * ------------------------------------------------------------- */
const RUNWAY_VERSION = "2024-11-06";
const POLL_INTERVAL_MS = 2000;
const MAX_POLL_ATTEMPTS = 90;
const RUNWAY_MODEL_PRIORITY = [
	"gen4.5",
	"gen4",
	"gen4_turbo",
	"gen3a_turbo",
	"veo3.1",
	"veo3.1_fast",
	"veo3",
];

const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });
const JAMENDO_ID = process.env.JAMENDO_CLIENT_ID;
const RUNWAY_ADMIN_KEY = process.env.RUNWAYML_API_SECRET;
const ELEVEN_API_KEY = process.env.ELEVENLABS_API_KEY;
const GOOGLE_CSE_ID =
	process.env.GOOGLE_CSE_ID ||
	process.env.GOOGLE_CUSTOM_SEARCH_CX ||
	process.env.GOOGLE_CUSTOM_SEARCH_ID ||
	null;
const GOOGLE_CSE_KEY =
	process.env.GOOGLE_CSE_KEY ||
	process.env.GOOGLE_CUSTOM_SEARCH_KEY ||
	process.env.GOOGLE_API_KEY ||
	null;
const GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1";
const GOOGLE_CSE_TIMEOUT_MS = 12000;

const VALID_RATIOS = [
	"1280:720",
	"720:1280",
	"1104:832",
	"832:1104",
	"960:960",
	"1584:672",
];

const AI_TOPIC_RE =
	/\b(ai|artificial intelligence|machine learning|genai|chatgpt|gpt-?\d*(?:\.\d+)?|openai|sora)\b/i;

const ARTICLE_FETCH_HEADERS = Object.freeze({
	"User-Agent":
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
	Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	"Accept-Language": "en-US,en;q=0.9",
	Referer: "https://www.google.com/",
});

/**
 * WORDS_PER_SEC: cap used when asking GPT for max words.
 * NATURAL_WPS: realistic speed used when recomputing durations from script.
 */
const WORDS_PER_SEC = 2.2;
const NATURAL_WPS = 2.25;
const ENGAGEMENT_TAIL_MIN = 5;
const ENGAGEMENT_TAIL_MAX = 6;
const MIN_OUTRO_WORDS = 16;
const OUTRO_TOLERANCE_MAX = 5;
const OUTRO_TOLERANCE_DEFAULT = 4;

const MAX_SILENCE_PAD = 0.35;
const MIN_ATEMPO = 0.9;
const MAX_ATEMPO = 1.08;

const T2V_MODEL = "gen4_turbo"; // prefer high quality but avoid unavailable variants by default
const ITV_MODEL = "gen4_turbo";
const TTI_MODEL = "gen4_image";

const RUNWAY_IMAGE_TO_VIDEO_MODELS = new Set([
	"gen4_turbo",
	"veo3.1_fast",
	"veo3.1",
	"veo3",
	"gen3a_turbo",
]);

const QUALITY_BONUS =
	"photorealistic, ultra-detailed, HDR, 8K, cinema lighting, cinematic camera movement, smooth parallax, subtle subject motion, emotional body language";
const PHYSICAL_REALISM_HINT =
	"single cohesive shot, realistic physics, natural hand-object contact, consistent lighting and shadows, no collage artifacts, no floating props";
const EYE_REALISM_HINT =
	"natural eye focus and blinking, subtle micro-expressions, no jittering pupils, no crossed or wall-eyed look";
const SOFT_SAFETY_PAD =
	"fully clothed, respectful framing, wholesome, safe for work, no sexualised framing, no injuries";

const RUNWAY_NEGATIVE_PROMPT = [
	"duplicate",
	"mirror",
	"reverse",
	"backwards walk",
	"extra limbs",
	"extra heads",
	"mutated hands",
	"fused fingers",
	"missing limbs",
	"contorted",
	"twisted neck",
	"broken fingers",
	"bad anatomy",
	"dislocated joints",
	"lowres",
	"pixelated",
	"blur",
	"blurry",
	"soft focus",
	"out of focus",
	"motion blur",
	"overexposed",
	"underexposed",
	"watermark",
	"logo",
	"text overlay",
	"nsfw",
	"gore",
	"floating props",
	"sticker edges",
	"collage look",
	"weird physics",
	"mismatched lighting",
	"unnatural eye movement",
	"jittering pupils",
	"dead eyes",
	"lazy eye",
	"unsafe text prompt",
	"awkward pose",
	"mismatched gaze",
	"crossed eyes",
	"wall-eyed",
	"sliding feet",
	"static frame",
	"frozen frame",
	"no motion",
	"still image",
	"deformed face",
	"melted face",
	"distorted face",
	"warped face",
	"plastic skin",
	"doll-like face",
	"over-sharpened",
	"oversharpened",
	"grainy",
	"artifact",
	"compression artifacts",
	"glitch",
	"streaks",
	"ghosting",
].join(", ");

const HUMAN_SAFETY =
	"anatomically correct, natural human faces, one natural-looking head, two eyes, normal limbs, realistic body proportions, natural head position, natural skin texture, sharp and in-focus facial features, no distortion, no warping, no blurring";

const BRAND_ENHANCEMENT_HINT =
	"subtle global brightness and contrast boost, slightly brighter and clearer faces while preserving natural skin tones, consistent AiVideomatic brand color grading";

const CHAT_MODEL = "gpt-5.1";

const ELEVEN_VOICES = {
	English: "21m00Tcm4TlvDq8ikWAM",
	Spanish: "CYw3kZ02Hs0563khs1Fj",
	Francais: "gqjD3Awy6ZnJf2el9DnG",
	Deutsch: "IFHEeWG1IGkfXpxmB1vN",
	Hindi: "ykoxtvL6VZTyas23mE9F",
	Arabic: "", // leave blank to force dynamic Egyptian/Arabic pick
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
	Top5: 1.0,
	Other: 0.7,
};

const SENSITIVE_TONE_RE =
	/\b(died|dead|death|killed|slain|shot dead|massacre|tragedy|tragic|funeral|mourning|mourner|passed away|succumbed|fatal|fatalities|casualty|casualties|victim|victims|hospitalized|in intensive care|on life support|critically ill|coma|cancer|tumor|tumour|leukemia|stroke|heart attack|illness|terminal|pandemic|epidemic|outbreak|bombing|explosion|airstrike|air strike|genocide)\b/i;

const HYPE_TONE_RE =
	/\b(breaking|incredible|amazing|unbelievable|huge|massive|record|historic|epic|insane|wild|stunning|shocking|explodes|erupt(s|ed)?|surge(s|d)?|soar(s|ed)?|smashes|crushes|upset|thriller|last-second|overtime|buzzer-beater|comeback)\b/i;

const DEFAULT_LANGUAGE = "English";
const TONE_HINTS = {
	Sports:
		"High-energy, play-by-play excitement; sound like a top commentator calling a pivotal moment without losing accuracy.",
	Politics:
		"Maintain an authoritative yet neutral tone, like a high-end documentary voiceover.",
	Finance: "Speak in a confident, analytical tone.",
	Entertainment: "Keep it upbeat and engaging.",
	Technology: "Adopt a forward-looking, curious tone.",
	Health: "Stay reassuring and informative.",
	Lifestyle: "Be friendly and encouraging.",
	Science: "Convey wonder and clarity.",
	World: "Maintain an objective, international outlook.",
	Top5: "Countdown voice: snappy, motivational, and appealing; clearly announce each rank.",
	Other: "Keep it concise and confident.",
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

const BRAND_TAG = "SereneJannat";
const BRAND_CREDIT = "Powered by Serene Jannat";
const MERCH_INTRO =
	"Support the channel & customize your own merch:\nhttps://www.serenejannat.com/custom-gifts\nhttps://www.serenejannat.com/custom-gifts/6815366fd8583c434ec42fec\nhttps://www.serenejannat.com/custom-gifts/67b7fb9c3d0cd90c4fc410e3\n\n";
const PROMPT_CHAR_LIMIT = 220;

/* ---------------------------------------------------------------
 *  Small helpers
 * ------------------------------------------------------------- */
const norm = (p) => (p ? p.replace(/\\/g, "/") : p);
const choose = (a) => a[Math.floor(Math.random() * a.length)];
const toBool = (v) => v === true || v === "true" || v === 1 || v === "1";
const looksLikeAITopic = (t) => AI_TOPIC_RE.test(String(t || ""));

const IMAGE_BLOCKLIST_HOSTS = [
	"pinimg.com",
	"pinterest.com",
	"blogspot.com",
	"bp.blogspot.com",
	"fbcdn.net",
	"lookaside.fbsbx.com",
	"gstatic.com",
	"ytimg.com",
	"wikimedia.org",
	"wikipedia.org",
	"tiktok.com",
	"twimg.com",
];

const IMAGE_UPLOAD_SOFT_BLOCK = [
	"optimole.com",
	"yourbasin.com",
	"amny.com",
	"nwahomepage.com",
	"arcpublishing.com",
	"conifa.org",
	"statcdn.com",
	"lookaside.instagram.com",
];

function normalizeImageKey(url) {
	try {
		const u = new URL(url);
		return `${u.hostname}${u.pathname}`.toLowerCase();
	} catch {
		return url;
	}
}

function tokenizeLabel(label = "") {
	return String(label || "")
		.toLowerCase()
		.replace(/[^a-z0-9\s]/gi, " ")
		.split(/\s+/)
		.filter((w) => w.length >= 3);
}

const TOPIC_STOP_WORDS = new Set([
	"top",
	"most",
	"best",
	"now",
	"today",
	"this",
	"year",
	"years",
	"season",
	"around",
	"world",
	"globally",
	"right",
	"popular",
	"latest",
	"to",
	"of",
	"in",
	"the",
	"for",
	"and",
	"with",
	"list",
	"five",
	"5",
]);

function topicTokensFromTitle(title = "") {
	return tokenizeLabel(title || "").filter((t) => !TOPIC_STOP_WORDS.has(t));
}

function matchesAnyToken(str = "", tokens = []) {
	if (!str || !tokens || !tokens.length) return false;
	const hay = str.toLowerCase();
	return tokens.some((t) => hay.includes(t.toLowerCase()));
}

function safeSlug(text = "", max = 60) {
	return String(text || "")
		.toLowerCase()
		.replace(/[^\w]+/g, "_")
		.replace(/^_+|_+$/g, "")
		.slice(0, max);
}

function rankSlug(rank) {
	if (rank === "intro") return "intro";
	if (rank === "outro") return "outro";
	const num = Number(rank);
	if (!num || num < 1) return "segment";
	return `number_${num}`;
}

function enforceTop5Order(planSegments = []) {
	const expected = [
		{ type: "intro", rank: null },
		{ type: "rank", rank: 5 },
		{ type: "rank", rank: 4 },
		{ type: "rank", rank: 3 },
		{ type: "rank", rank: 2 },
		{ type: "rank", rank: 1 },
		{ type: "outro", rank: null },
	];
	const take = [];
	const remaining = Array.isArray(planSegments) ? planSegments.slice() : [];
	for (const exp of expected) {
		const idx = remaining.findIndex(
			(s) =>
				(s?.type || (s?.rank ? "rank" : null)) === exp.type &&
				(exp.rank === null ? true : Number(s.rank) === exp.rank)
		);
		if (idx === -1) return null;
		take.push(remaining[idx]);
		remaining.splice(idx, 1);
	}
	return take;
}

function normalizeLabelForTopic(label = "", topic = "") {
	let normalized = String(label || "").trim();
	const topicLower = String(topic || "").toLowerCase();
	const labelLower = normalized.toLowerCase();
	const topicHasSoccer =
		topicLower.includes("soccer") ||
		topicLower.includes("football (soccer)") ||
		(topicLower.includes("popular sports") && labelLower.includes("football"));
	const labelLooksAmerican = /american\s+football/i.test(normalized);
	if (!labelLooksAmerican && topicHasSoccer && /football/i.test(normalized)) {
		normalized = normalized.replace(/football(\s*\(soccer\))?/gi, "Soccer");
	}
	return normalized.trim();
}

function isSoftBlockedHost(host = "") {
	const h = String(host || "").toLowerCase();
	return IMAGE_UPLOAD_SOFT_BLOCK.some((b) => h === b || h.endsWith(`.${b}`));
}

function isBlockedHost(host = "") {
	const h = String(host || "").toLowerCase();
	return IMAGE_BLOCKLIST_HOSTS.some((b) => h === b || h.endsWith(`.${b}`));
}

const SPORTS_TOKEN_MAP = {
	volleyball: ["volleyball", "spike", "serve", "net"],
	tennis: ["tennis", "racket", "serve", "court"],
	basketball: ["basketball", "hoop", "nba", "dunk", "layup"],
	cricket: ["cricket", "bat", "bowler", "wicket", "batter"],
	football: ["soccer", "football", "goalkeeper", "goal", "pitch", "fifa"],
	soccer: ["soccer", "football", "goalkeeper", "goal", "pitch", "fifa"],
	athletics: ["athletics", "track", "sprinter", "relay", "hurdle", "stadium"],
};

function requiredTokensForLabel(label = "") {
	const lower = String(label || "").toLowerCase();
	for (const [k, v] of Object.entries(SPORTS_TOKEN_MAP)) {
		if (lower.includes(k)) return v;
	}
	return [];
}

function classifyAspectFromDims(w, h) {
	if (!w || !h) return "unknown";
	const ar = w / h;
	if (ar > 1.2) return "landscape";
	if (ar < 0.8) return "portrait";
	return "square";
}

function targetAspectValue(ratio) {
	if (ratio === "720:1280" || ratio === "832:1104") return 9 / 16;
	if (ratio === "1280:720" || ratio === "1584:672" || ratio === "1104:832")
		return 16 / 9;
	return null;
}

function aspectMatchesRatio(candidateRatio, width, height) {
	const target = targetAspectValue(candidateRatio);
	if (!target || !width || !height) return false;
	const ar = width / height;
	return Math.abs(ar - target) <= 0.32;
}

function minEdgeForRatio(ratio) {
	if (ratio === "720:1280" || ratio === "832:1104") return 900;
	if (ratio === "960:960") return 900;
	return 1400;
}

function isPortraitRatio(ratio) {
	const target = targetAspectValue(ratio);
	return target && target < 1;
}

function dedupeImageUrls(urls, limit = 8) {
	const uniq = [];
	const seen = new Set();
	const seenKeys = new Set();
	const hostCount = new Map();
	for (const url of urls) {
		if (!url || typeof url !== "string") continue;
		if (!/^https?:\/\//i.test(url)) continue;
		const trimmed = url.trim();
		if (seen.has(trimmed)) continue;
		let host = "";
		try {
			host = new URL(trimmed).hostname.toLowerCase();
		} catch {
			continue;
		}
		if (IMAGE_BLOCKLIST_HOSTS.some((b) => host === b || host.endsWith(`.${b}`)))
			continue;
		const key = normalizeImageKey(trimmed);
		if (seenKeys.has(key)) continue;
		const c = hostCount.get(host) || 0;
		if (c >= 2) continue;

		seen.add(trimmed);
		seenKeys.add(key);
		hostCount.set(host, c + 1);
		uniq.push(trimmed);
		if (uniq.length >= limit) break;
	}
	return uniq;
}

function filterUploadCandidates(urls, limit = 7) {
	const out = [];
	for (const u of urls) {
		if (!u || typeof u !== "string") continue;
		let host = "";
		try {
			host = new URL(u).hostname.toLowerCase();
		} catch {
			continue;
		}
		if (
			IMAGE_UPLOAD_SOFT_BLOCK.some((b) => host === b || host.endsWith(`.${b}`))
		)
			continue;
		out.push(u);
		if (out.length >= limit) break;
	}
	return out;
}

function pickIntroOutroUrls(urls = [], topicTokens = []) {
	const uniq = [];
	const seen = new Set();
	const hasTokens = Array.isArray(topicTokens) && topicTokens.length > 0;
	const normalizedTokens = hasTokens
		? topicTokens.map((t) => t.toLowerCase())
		: [];

	const hayHasToken = (str = "") =>
		normalizedTokens.some((t) => str.toLowerCase().includes(t));

	for (const u of urls) {
		if (!u || typeof u !== "string") continue;
		if (seen.has(u)) continue;
		const keep =
			!hasTokens ||
			hayHasToken(u) ||
			hayHasToken(decodeURIComponent(u)) ||
			hayHasToken(u.split("/").slice(-1)[0] || "");
		if (!keep) continue;
		seen.add(u);
		uniq.push(u);
		if (uniq.length >= 3) break;
	}
	// fallback: if filtering removed too many, allow unfiltered to fill slots
	if (uniq.length < 2) {
		for (const u of urls) {
			if (!u || typeof u !== "string") continue;
			if (seen.has(u)) continue;
			seen.add(u);
			uniq.push(u);
			if (uniq.length >= 3) break;
		}
	}

	return {
		intro: uniq[0] || null,
		outro: uniq[1] || null,
		remaining: uniq.slice(2),
	};
}

function aspectForRatio(ratio) {
	if (!ratio) return null;
	const parts = String(ratio)
		.split(":")
		.map((p) => parseFloat(p));
	if (parts.length !== 2 || !parts[0] || !parts[1]) return null;
	return parts[0] >= parts[1] ? "landscape" : "portrait";
}

function pickTrendImagesForRatio(trendStory, ratio, desiredCount = 5) {
	if (!trendStory) return [];
	const aspect = aspectForRatio(ratio) || "landscape";
	const exact =
		trendStory.imagesByAspect &&
		Array.isArray(trendStory.imagesByAspect[ratio]) &&
		trendStory.imagesByAspect[ratio].length
			? trendStory.imagesByAspect[ratio]
			: null;
	const fallbackKey = aspect === "portrait" ? "720:1280" : "1280:720";
	const fallback =
		trendStory.imagesByAspect && trendStory.imagesByAspect[fallbackKey]
			? trendStory.imagesByAspect[fallbackKey]
			: [];

	const pools = [
		...(exact || []),
		...fallback,
		...(Array.isArray(trendStory.images) ? trendStory.images : []),
		...(trendStory.imagesByAspect?.square || []),
		...(trendStory.imagesByAspect?.unknown || []),
	];

	const seen = new Set();
	const picked = [];
	for (const url of pools) {
		if (!url || typeof url !== "string") continue;
		if (!/^https?:\/\//i.test(url)) continue;
		if (seen.has(url)) continue;
		seen.add(url);
		picked.push(url);
		if (picked.length >= desiredCount) break;
	}
	return picked.slice(0, desiredCount);
}

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
				/\bAI['"]?\s*(prediction|predictions|forecast|pick|call|take|preview)\b/gi,
				"our $1",
			],
			[/\bgenerative AI\b/gi, "modern tools"],
			[/\bchatgpt\b/gi, "our newsroom"],
			[/\bgpt[-\s]?\d+(?:\.\d+)?\b/gi, "our newsroom"],
			[/\bopenai\b/gi, "the newsroom"],
			[/\bsora\b/gi, "the crew"],
			[/\bartificial intelligence\b/gi, "smart insight"],
			[/\bAI['"]?\b/gi, "our"],
		];
		for (const [re, rep] of swaps) cleaned = cleaned.replace(re, rep);
	}

	return cleaned.trim();
}

function enforceEngagementOutroText(text, { topic, wordCap, category }) {
	const existing = String(text || "").trim();
	const hasQuestion = /\?/.test(existing);
	const hasCTA = /(comment|subscribe|follow|like)/i.test(existing);
	const hasSignOff =
		/(see you|thanks for|catch you|stay curious|next time)/i.test(existing);

	const safeTopic =
		sanitizeAudienceFacingText(topic, { allowAITopic: true }) || topic || "";
	const question =
		category === "Top5"
			? choose([
					"Do you agree with this ranking?",
					"What would you swap on this list?",
					"Which pick shocked you most?",
			  ])
			: choose(
					safeTopic
						? [
								`What do you make of ${safeTopic}?`,
								`Does ${safeTopic} surprise you?`,
								`Where do you stand on ${safeTopic}?`,
						  ]
						: [
								"What do you think?",
								"Did this surprise you?",
								"What stood out to you?",
						  ]
			  );
	const cta = choose([
		"Drop your take below, tap like, and subscribe for more quick hits.",
		"Tell me your angle in the comments, hit like, and subscribe for the next drop.",
		"Share your thoughts, smash like, and follow for tomorrow's update.",
	]);
	const signOff = choose([
		"See you tomorrow!",
		"Catch you next time!",
		"Thanks for watching!",
		"Stay curious!",
		"Stay sharp out there!",
	]);

	let combined = existing;
	if (!hasQuestion && !hasCTA) combined = `${question} ${cta}`;
	else if (!hasQuestion) combined = `${existing} ${question}`;
	else if (!hasCTA) combined = `${existing} ${cta}`;
	if (!hasSignOff) combined = `${combined} ${signOff}`;

	// Ensure the CTA is complete and not dangling.
	if (/[&]\s*$/.test(combined) || /\band\s*$/i.test(combined)) {
		combined = combined.replace(/[&]\s*$/g, "and").replace(/\band\s*$/i, "and");
		combined = `${combined} subscribe!`;
	}
	if (!/subscribe/i.test(combined) && /like/i.test(combined)) {
		combined = `${combined} Subscribe for more!`;
	}
	if (!/[.!?]$/.test(combined)) combined = `${combined}!`;

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

const TICK_CHAR = String.fromCharCode(96);
function stripCodeFence(s) {
	const marker = TICK_CHAR + TICK_CHAR + TICK_CHAR;
	const first = s.indexOf(marker);
	if (first === -1) return s;
	const after = s.slice(first + marker.length);
	const second = after.lastIndexOf(marker);
	if (second === -1) return s;
	let inner = after.slice(0, second);
	inner = inner.replace(/^\s*json/i, "").trim();
	return inner || s;
}

function ensureClickableLinks(text) {
	if (!text || typeof text !== "string") return "";
	const lines = text.split(/\r?\n/);
	const fixed = lines.map((line) => {
		let s = line.trim();
		// remove trailing parenthetical labels
		s = s.replace(/\s*\([^)]*\)\s*$/, "");
		// fix bare domains
		s = s.replace(/(^|\s)(www\.[^\s)]+)/gi, "$1https://$2");
		s = s.replace(
			/(^|\s)(serenejannat\.com[^\s)]*)/gi,
			(_m, prefix, url) =>
				`${prefix}https://${url.replace(/^https?:\/\//i, "")}`
		);
		// general bare domain -> clickable
		s = s.replace(
			/(^|\s)([a-z0-9.-]+\.[a-z]{2,}[^\s)]*)/gi,
			(_m, prefix, url) =>
				`${prefix}https://${url.replace(/^https?:\/\//i, "")}`
		);
		// strip trailing punctuation that breaks linkification
		s = s.replace(/(https?:\/\/[^\s)]+)[).,;:]+$/g, "$1");
		// enforce a newline separation after any URL chain to avoid trailing text sticking
		s = s.replace(/(https?:\/\/[^\s)]+)/g, "$1");
		return s;
	});
	return fixed.join("\n");
}

function scrubPromptForSafety(text) {
	if (!text || typeof text !== "string") return "";
	let t = text;
	const replacements = [
		{ find: /pregnan(t|cy|cies)/gi, replace: "expectant fashion moment" },
		{ find: /baby\s*bump/gi, replace: "fashion silhouette" },
		{ find: /\bbelly\b/gi, replace: "silhouette" },
		{ find: /\bnude\b/gi, replace: "fully clothed" },
		{ find: /\bskin\b/gi, replace: "outfit" },
		{ find: /\bsheer\b/gi, replace: "tasteful fabric" },
	];
	replacements.forEach(({ find, replace }) => {
		t = t.replace(find, replace);
	});
	return `${t}. ${SOFT_SAFETY_PAD}`.trim();
}
const strip = (s) => stripCodeFence(String(s || "").trim());

const goodDur = (n) =>
	Number.isInteger(+n) && +n >= 5 && +n <= 90 && +n % 5 === 0;

const escTxt = (t) =>
	String(t || "")
		.replace(/\\/g, "\\\\")
		.replace(/[’']/g, "\\'")
		.replace(/:/g, "\\:")
		.replace(/,/g, "\\,");

function tmpFile(tag, ext = "") {
	return path.join(os.tmpdir(), `${tag}_${crypto.randomUUID()}${ext}`);
}

async function downloadImageToTemp(url, ext = ".jpg") {
	const tmp = tmpFile("trend_raw", ext);
	const writer = fs.createWriteStream(tmp);
	const resp = await axios.get(url, { responseType: "stream" });
	await new Promise((resolve, reject) => {
		resp.data.pipe(writer).on("finish", resolve).on("error", reject);
	});
	return tmp;
}

function toTitleCase(str = "") {
	return str
		.toLowerCase()
		.replace(/(^\w|\s\w)/g, (m) => m.toUpperCase())
		.trim();
}

function fallbackSeoTitle(topic, category) {
	const base = toTitleCase(topic || "Breaking Update");
	if (category === "Top5") return `Top 5 ${base}`;
	if (category === "Sports") return `${base} | Highlights & Preview`;
	return `${base} | Update`;
}

const NUM_WORD = Object.freeze({
	1: "one",
	2: "two",
	3: "three",
	4: "four",
	5: "five",
	6: "six",
	7: "seven",
	8: "eight",
	9: "nine",
	10: "ten",
	11: "eleven",
	12: "twelve",
	13: "thirteen",
	14: "fourteen",
	15: "fifteen",
	16: "sixteen",
	17: "seventeen",
	18: "eighteen",
	19: "nineteen",
	20: "twenty",
});
function improveTTSPronunciation(text) {
	text = text.replace(/#\s*([1-5])\s*:/g, (_, n) => `Number ${NUM_WORD[n]}:`);
	return text.replace(/\b([1-9]|1[0-9]|20)\b/g, (_, n) => NUM_WORD[n] || n);
}

/* Voice tone classification */
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

	return {
		style,
		stability,
		similarityBoost,
		openaiSpeed,
		isSensitive,
	};
}

/* Recompute segment durations from script words */
function recomputeSegmentDurationsFromScript(segments, targetTotalSeconds) {
	if (
		!Array.isArray(segments) ||
		!segments.length ||
		!targetTotalSeconds ||
		!Number.isFinite(targetTotalSeconds)
	)
		return null;

	const MIN_SEGMENT_SECONDS = 4;

	const est = segments.map((s, idx) => {
		const words = String(s.scriptText || "")
			.trim()
			.split(/\s+/)
			.filter(Boolean).length;
		const basePause = idx === segments.length - 1 ? 0.35 : 0.25;
		const countdownPause = s?.countdownRank ? 0.18 : 0;
		const raw = (words || 1) / NATURAL_WPS + basePause + countdownPause;
		return Math.max(MIN_SEGMENT_SECONDS, raw);
	});

	const estTotal = est.reduce((a, b) => a + b, 0) || targetTotalSeconds;
	let scale = targetTotalSeconds / estTotal;
	if (scale < 0.8) scale = 0.8;
	if (scale > 1.25) scale = 1.25;

	let scaled = est.map((v) => v * scale);
	let total = scaled.reduce((a, b) => a + b, 0);
	let diff = +(targetTotalSeconds - total).toFixed(2);

	let idx = scaled.length - 1;
	const step = diff > 0 ? 0.1 : -0.1;
	while (Math.abs(diff) > 0.05 && scaled.length && idx >= 0) {
		const candidate = scaled[idx] + step;
		if (candidate >= MIN_SEGMENT_SECONDS) {
			scaled[idx] = candidate;
			diff -= step;
		}
		idx--;
		if (idx < 0 && Math.abs(diff) > 0.05) idx = scaled.length - 1;
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

function computeOptionalOutroTolerance(tailSeconds) {
	const needed = MIN_OUTRO_WORDS / WORDS_PER_SEC; // seconds needed to comfortably fit CTA
	const deficit = +(needed - tailSeconds).toFixed(2);
	if (deficit <= 0) return 0;
	return +Math.min(Math.max(deficit, 0), OUTRO_TOLERANCE_MAX).toFixed(2);
}

function computeInitialSegLens(
	category,
	duration,
	tailSeconds,
	toleranceSeconds
) {
	if (category === "Top5") {
		const INTRO = 3;
		const outro = +(tailSeconds + (toleranceSeconds || 0)).toFixed(2);
		const contentTarget = Math.max(duration - INTRO, 12);
		const base = Math.max(4, Math.floor(contentTarget / 5));
		let remainder = contentTarget - base * 5;
		const segLens = [INTRO];
		for (let i = 0; i < 5; i++) {
			const add = remainder > 0 ? 1 : 0;
			segLens.push(base + add);
			remainder -= add;
		}
		segLens.push(outro);
		return segLens.map((n) => +n.toFixed(2));
	}

	const INTRO = 3;
	const MIN_SEG_SECONDS = 4;
	const outro = +(tailSeconds + (toleranceSeconds || 0)).toFixed(2);
	const contentTarget = Math.max(duration - INTRO, MIN_SEG_SECONDS * 2);
	const contentSegCount = Math.max(
		2,
		Math.min(4, Math.round(contentTarget / 8))
	);
	const baseContent = Math.max(
		MIN_SEG_SECONDS + 1,
		Math.floor(contentTarget / contentSegCount)
	);
	let remainder = contentTarget - baseContent * contentSegCount;

	const segLens = [INTRO];
	for (let i = 0; i < contentSegCount; i++) {
		const add = remainder > 0 ? 1 : 0;
		segLens.push(baseContent + add);
		remainder -= add;
	}

	segLens.push(outro);
	return segLens.map((n) => +n.toFixed(2));
}

/* ---------------------------------------------------------------
 *  Cloudinary + resolution helpers
 * ------------------------------------------------------------- */
const VALID_RATIOS_TO_ASPECT = {
	"1280:720": "16:9",
	"1584:672": "16:9",
	"720:1280": "9:16",
	"832:1104": "9:16",
	"960:960": "1:1",
	"1104:832": "4:3",
};
function ratioToCloudinaryAspect(ratio) {
	return VALID_RATIOS_TO_ASPECT[ratio] || "16:9";
}

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
	const aspect = ratioToCloudinaryAspect(ratio);
	const { width, height } = targetResolutionForRatio(ratio);

	const base = {
		crop: "fill",
		gravity: "auto",
		quality: "auto:best",
		fetch_format: "auto",
	};

	if (width && height) {
		base.width = width;
		base.height = height;
	} else {
		base.aspect_ratio = aspect;
	}

	return [base];
}

function openAIImageSizeForRatio(ratio) {
	switch (ratio) {
		case "720:1280":
		case "832:1104":
			return "1024x1536";
		case "960:960":
			return "1024x1024";
		default:
			return "1536x1024";
	}
}

/* ---------------------------------------------------------------
 *  ffmpeg helpers
 * ------------------------------------------------------------- */
function ffmpegPromise(cfg) {
	return new Promise((res, rej) => {
		const p = cfg(ffmpeg()) || ffmpeg();
		p.on("start", (cmd) => console.log(`[FFmpeg] ${cmd}`))
			.on("end", () => res())
			.on("error", (e) => rej(e));
	});
}

async function exactLen(src, target, out, opts = {}) {
	const { ratio = null, enhance = false } = opts;

	const meta = await new Promise((resolve, reject) =>
		ffmpeg.ffprobe(src, (err, data) => (err ? reject(err) : resolve(data)))
	);

	const inDur = meta.format?.duration || target;
	const diff = +(target - inDur).toFixed(3);

	const targetRes = ratio ? targetResolutionForRatio(ratio) : null;
	const videoStream = Array.isArray(meta.streams)
		? meta.streams.find((s) => s.codec_type === "video")
		: null;
	const inW = videoStream?.width;
	const inH = videoStream?.height;

	await ffmpegPromise((cmd) => {
		cmd.input(norm(src));

		const vf = [];

		if (enhance && targetRes && targetRes.width && targetRes.height) {
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
			if (diff < 0) {
				cmd.outputOptions("-t", String(target));
			} else {
				// Cap extra silence to avoid long dead-air tails.
				const capped = Math.min(diff, 1.0);
				vf.push(`tpad=stop_duration=${capped.toFixed(3)}`);
			}
		}

		if (vf.length) {
			cmd.videoFilters(vf.join(","));
		}

		const preset = enhance ? "slow" : "fast";
		const crf = enhance ? 16 : 17;

		return cmd
			.outputOptions(
				"-c:v",
				"libx264",
				"-preset",
				preset,
				"-crf",
				String(crf),
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

async function exactLenAudio(src, target, out, opts = {}) {
	const { maxAtempo = MAX_ATEMPO, forceTrim = false } = opts;
	const meta = await new Promise((resolve, reject) =>
		ffmpeg.ffprobe(src, (err, data) => (err ? reject(err) : resolve(data)))
	);

	const inDur = meta.format?.duration || target;
	const diff = +(target - inDur).toFixed(3);

	await ffmpegPromise((cmd) => {
		cmd.input(norm(src));

		const filters = [];
		let padAfterTempo = diff > 0 ? diff : 0;

		if (Math.abs(diff) <= 0.08) {
			// match
		} else if (diff < -0.08) {
			const ratio = inDur / target;

			if (ratio <= 2.0) {
				let tempo = ratio;
				if (tempo > maxAtempo) tempo = maxAtempo;
				if (tempo < MIN_ATEMPO) tempo = MIN_ATEMPO;
				filters.push(`atempo=${tempo.toFixed(3)}`);
				padAfterTempo = target - inDur / tempo;
			} else if (ratio <= 4.0) {
				const r = Math.min(maxAtempo, Math.sqrt(ratio));
				filters.push(`atempo=${r.toFixed(3)},atempo=${r.toFixed(3)}`);
				padAfterTempo = target - inDur / (r * r);
			} else {
				cmd.outputOptions("-t", String(target));
				padAfterTempo = 0;
			}
		}

		let padDur = forceTrim
			? Math.max(0, padAfterTempo)
			: Math.min(MAX_SILENCE_PAD, Math.max(0, padAfterTempo));
		const shouldPad = forceTrim ? padDur > 0 : padDur > 0.05;
		if (shouldPad) {
			if (padDur < 0.05) padDur = 0.05;
			filters.push(`apad=pad_dur=${padDur.toFixed(3)}`);
		}
		if (forceTrim) {
			filters.push(`atrim=0:${target.toFixed(3)}`);
		}

		if (filters.length) cmd.audioFilters(filters.join(","));

		return cmd.outputOptions("-y").save(norm(out));
	});
}

function escapeDrawtext(text = "") {
	return String(text || "")
		.replace(/\\/g, "\\\\")
		.replace(/:/g, "\\:")
		.replace(/'/g, "\\'")
		.trim();
}

async function overlayCountdownSlate({
	src,
	out,
	ratio,
	rank,
	label,
	displaySeconds = 2,
}) {
	if (!src || !out || !rank) return src;
	const res = targetResolutionForRatio(ratio);
	const fontSize = res?.height ? Math.round(res.height * 0.055) : 64;
	const yPos = res?.height ? Math.round(res.height * 0.08) : 80;
	const fadeIn = 0.2;
	const fadeOut = 0.3;
	const hold = Math.max(0.6, displaySeconds - (fadeIn + fadeOut));
	const fadeOutStart = +(fadeIn + hold).toFixed(3);
	const visibleUntil = +(fadeIn + hold + fadeOut).toFixed(3);

	const text = escapeDrawtext(`#${rank}- ${label || ""}`);
	const alphaExpr = `if(lt(t\\,${fadeIn})\\, t/${fadeIn}\\, if(lt(t\\,${fadeOutStart})\\, 1\\, if(lt(t\\,${visibleUntil})\\, (${visibleUntil}-t)/${fadeOut}\\, 0)))`;

	const vf = [
		`drawtext=fontfile=${FONT_PATH_FFMPEG}:text='${text}':fontsize=${fontSize}:fontcolor=white:alpha=${alphaExpr}:x=(w-text_w)/2:y=${yPos}:borderw=6:bordercolor=black@0.6:shadowcolor=black@0.55:shadowx=3:shadowy=3:box=1:boxcolor=black@0.35:boxborderw=18:enable='lt(t\\,${visibleUntil})'`,
	];

	await ffmpegPromise((cmd) =>
		cmd
			.input(norm(src))
			.videoFilters(vf.join(","))
			.outputOptions(
				"-c:v",
				"libx264",
				"-preset",
				"fast",
				"-crf",
				"18",
				"-pix_fmt",
				"yuv420p",
				"-movflags",
				"+faststart",
				"-y"
			)
			.save(norm(out))
	);
	return out;
}

async function probeVideoDuration(file) {
	const meta = await new Promise((resolve, reject) =>
		ffmpeg.ffprobe(file, (err, data) => (err ? reject(err) : resolve(data)))
	);
	return meta.format?.duration || 0;
}

async function concatWithTransitions(
	clips,
	durationsHint = [],
	ratio = null,
	transitionDuration = 0.85,
	options = {}
) {
	/* KNOBS YOU CARE ABOUT:
	 * 1) transitionDuration (4th argument at call-site)
	 *    - LOWER (e.g. 0.3) -> faster fades. HIGHER (e.g. 1.2) -> slower fades.
	 * 2) options.maxFadeFraction / minFadeSeconds: how much of each clip we spend fading.
	 * 3) options.fadeIn flags control fades at start/middle; options.fadeOut flags control fades at end/middle.
	 */

	if (!clips || !clips.length) throw new Error("No clips to stitch");

	const {
		// Global “where to fade” behaviour
		fadeInFirst = true,
		fadeOutLast = true,
		fadeInMiddle = true,
		fadeOutMiddle = true,

		// How aggressive fades can be relative to each clip duration.
		// Smaller maxFadeFraction + smaller transitionDuration => snappier fades.
		maxFadeFraction = 0.14, // <= 14% of each clip duration
		minFadeSeconds = 0.15, // minimum fade length so it's not a 1-frame flash
	} = options;

	const maxFadeSeconds = transitionDuration || 0.85;

	// 1) Normalize all clips first to identical geometry/timestamps
	const normalized = [];
	const targetRes = ratio ? targetResolutionForRatio(ratio) : null;

	for (let i = 0; i < clips.length; i++) {
		const src = clips[i];
		const normOut = tmpFile(`norm_${i + 1}`, ".mp4");

		await ffmpegPromise((cmd) => {
			cmd.input(norm(src));

			const vf = [];
			if (targetRes?.width && targetRes?.height) {
				vf.push(
					`scale=${targetRes.width}:${targetRes.height}:force_original_aspect_ratio=increase:flags=lanczos+accurate_rnd+full_chroma_int`,
					`crop=${targetRes.width}:${targetRes.height}`
				);
			}
			vf.push("format=yuv420p", "setsar=1", "fps=30", "setpts=PTS-STARTPTS");

			cmd.videoFilters(vf.join(","));
			cmd.outputOptions(
				"-c:v",
				"libx264",
				"-preset",
				"fast",
				"-crf",
				"18",
				"-movflags",
				"+faststart",
				"-y"
			);

			return cmd.save(norm(normOut));
		});

		normalized.push(normOut);
	}

	// Helper: apply fade-in/out to a single normalized clip
	async function fadeOne(srcPath, idx, totalClips) {
		const outFade = tmpFile(`fade_${idx + 1}`, ".mp4");

		const durHint =
			typeof durationsHint[idx] === "number"
				? Number(durationsHint[idx])
				: null;

		const realDur =
			durHint && Number.isFinite(durHint)
				? durHint
				: (await probeVideoDuration(srcPath)) || 1;

		// Compute base fade duration:
		// - Cannot exceed maxFadeSeconds (from transitionDuration arg)
		// - Cannot exceed maxFadeFraction * clip length
		// - Cannot exceed half the clip
		let fadeDur = Math.min(
			maxFadeSeconds,
			realDur * maxFadeFraction,
			realDur / 2
		);

		// Enforce a sensible floor
		if (!Number.isFinite(fadeDur) || fadeDur <= 0) {
			fadeDur = minFadeSeconds;
		} else if (fadeDur < minFadeSeconds) {
			fadeDur = minFadeSeconds;
		}

		const isFirst = idx === 0;
		const isLast = idx === totalClips - 1;

		const doFadeIn = isFirst ? fadeInFirst : fadeInMiddle;
		const doFadeOut = isLast ? fadeOutLast : fadeOutMiddle;

		const filters = [];

		if (doFadeIn) {
			filters.push(`fade=t=in:st=0:d=${fadeDur.toFixed(3)}`);
		}
		if (doFadeOut) {
			const fadeStartOut = Math.max(0, realDur - fadeDur);
			filters.push(
				`fade=t=out:st=${fadeStartOut.toFixed(3)}:d=${fadeDur.toFixed(3)}`
			);
		}

		await ffmpegPromise((cmd) => {
			cmd.input(norm(srcPath));

			if (filters.length) {
				cmd.videoFilters(filters.join(","));
			}

			cmd.outputOptions(
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
			);

			return cmd.save(norm(outFade));
		});

		return outFade;
	}

	// 2) Apply fade logic to every normalized clip
	const fadedClips = [];
	for (let i = 0; i < normalized.length; i++) {
		const src = normalized[i];
		const faded = await fadeOne(src, i, normalized.length);
		fadedClips.push(faded);
	}

	// 3) Concat all faded clips using the concat demuxer (no xfade, no complex graph)
	const listFile = tmpFile("list_concat", ".txt");
	fs.writeFileSync(
		listFile,
		fadedClips.map((p) => `file '${norm(p)}'`).join("\n")
	);

	const out = tmpFile("transitioned", ".mp4");

	await ffmpegPromise((cmd) =>
		cmd
			.input(norm(listFile))
			.inputOptions("-f", "concat", "-safe", "0")
			// All clips are re-encoded the same way above,
			// so we can safely stream-copy video here.
			.outputOptions("-c:v", "copy", "-y")
			.save(norm(out))
	);

	// 4) Cleanup temp files
	fadedClips.forEach((p) => {
		try {
			fs.unlinkSync(p);
		} catch (_) {}
	});
	normalized.forEach((p) => {
		try {
			fs.unlinkSync(p);
		} catch (_) {}
	});
	try {
		fs.unlinkSync(listFile);
	} catch (_) {}

	return out;
}

/* ---------------------------------------------------------------
 *  Google Trends helpers & SEO title
 * ------------------------------------------------------------- */
function resolveTrendsCategoryId(label) {
	const e = googleTrendingCategoriesId.find((c) => c.category === label);
	return e ? e.ids[0] : 0;
}

const TRENDS_API_URL =
	process.env.TRENDS_API_URL || "http://localhost:8102/api/google-trends";
const TRENDS_HTTP_TIMEOUT_MS = 60000;

function isSportsTopic(topic = "", category = "", trendStory = null) {
	if (category && category.toLowerCase().includes("sport")) return true;
	const haystack = [topic]
		.concat(trendStory?.title || [], trendStory?.seoTitle || [])
		.concat(trendStory?.entityNames || [])
		.map((t) => String(t || "").toLowerCase());
	return haystack.some((t) =>
		Array.from(SPORTS_KEYWORDS_SET).some((k) => t.includes(k))
	);
}
const SPORTS_KEYWORDS = [
	"soccer",
	"football",
	"nfl",
	"nba",
	"mlb",
	"mls",
	"nhl",
	"ufc",
	"mma",
	"f1",
	"formula 1",
	"motogp",
	"rugby",
	"cricket",
	"golf",
	"tennis",
	"baseball",
	"basketball",
	"hockey",
	"volleyball",
	"champions league",
	"world cup",
	"euros",
	"super bowl",
	"playoffs",
	"finals",
];
const SPORTS_KEYWORDS_SET = new Set(SPORTS_KEYWORDS);

function isThumbnailHost(hostname) {
	const h = String(hostname || "").toLowerCase();
	return (
		h.endsWith("gstatic.com") ||
		h.includes("googleusercontent.com") ||
		h.includes("ggpht.com")
	);
}

function analyseImageUrl(url, isStoryImage = false) {
	let score = 0;
	let isThumbnail = false;

	try {
		const u = new URL(url);
		const host = u.hostname.toLowerCase();
		const search = u.search || "";
		const path = u.pathname || "";

		isThumbnail = isThumbnailHost(host);

		if (!isThumbnail) score += 5;
		else score -= 5;

		if (isStoryImage) score += 3;

		if (/\.(jpe?g|png|webp|avif)$/i.test(u.pathname)) score += 2;

		const wMatch = search.match(/[?&]w=(\d{2,4})/i);
		if (wMatch) {
			const w = parseInt(wMatch[1], 10);
			if (w >= 1400) score += 3;
			else if (w >= 1000) score += 2;
			else if (w >= 600) score += 1;
		}

		const hMatch = search.match(/[?&]h=(\d{2,4})/i);
		if (hMatch) {
			const h = parseInt(hMatch[1], 10);
			if (h < 200) score -= 1;
		}

		// bonus for high-res hints in path
		const resMatch = path.match(/(\d{3,4})x(\d{3,4})/);
		if (resMatch) {
			const w = parseInt(resMatch[1], 10);
			const h = parseInt(resMatch[2], 10);
			if (w >= 1400 || h >= 1400) score += 2;
		}
	} catch {
		if (isStoryImage) score += 1;
	}

	return { score, isThumbnail };
}

function normaliseTrendImageBriefs(briefs = [], topic = "") {
	const targets = ["1280:720", "720:1280"];
	const byAspect = new Map(targets.map((t) => [t, null]));

	if (Array.isArray(briefs)) {
		for (const raw of briefs) {
			if (!raw || !raw.aspectRatio) continue;
			const ar = String(raw.aspectRatio).trim();
			if (!byAspect.has(ar)) continue;
			if (byAspect.get(ar)) continue;
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
			rationale:
				"Auto-filled to keep both aspect ratios covered for the video orchestrator.",
		});
	}

	return Array.from(byAspect.values());
}

/**
 * Fetch a Trends story for this category / geo, preferring:
 * - A story the user hasn't used yet (avoid duplicates)
 * - High-quality hero images from real publishers (CNN, Deadline, etc.)
 * Returns:
 * {
 *   title, rawTitle, seoTitle, youtubeShortTitle, entityNames[],
 *   images: [bestFirst...],
 *   articles: [{ title, url, image }]
 * }
 */
async function fetchTrendingStory(
	category,
	geo = "US",
	usedTopics = new Set(),
	language = DEFAULT_LANGUAGE
) {
	const id = resolveTrendsCategoryId(category);
	const baseUrl =
		`${TRENDS_API_URL}?` +
		qs.stringify({ geo, category: id, hours: 168, language });

	const normTitle = (t) =>
		String(t || "")
			.toLowerCase()
			.replace(/\s+/g, " ")
			.trim();
	const usedSet =
		usedTopics instanceof Set
			? new Set(Array.from(usedTopics).map(normTitle))
			: new Set(
					(Array.isArray(usedTopics)
						? usedTopics
						: usedTopics
						? [usedTopics]
						: []
					)
						.filter(Boolean)
						.map(normTitle)
			  );
	const usedList = Array.from(usedSet);
	const isTermUsed = (term) => {
		if (!term) return false;
		const n = normTitle(term);
		if (!n) return false;
		if (usedSet.has(n)) return true;
		// catch near-duplicates where the base term is contained in prior titles
		return usedList.some((u) => u.includes(n) || n.includes(u));
	};

	try {
		console.log("[Trending] fetch:", baseUrl);

		const fetchOnce = async (timeoutMs) =>
			axios.get(baseUrl, {
				timeout: timeoutMs,
			});

		let data;
		try {
			({ data } = await fetchOnce(TRENDS_HTTP_TIMEOUT_MS));
		} catch (e) {
			const isTimeout = /timeout/i.test(e.message || "");
			if (!isTimeout) throw e;
			console.warn("[Trending] timeout, retrying once with extended window");
			({ data } = await fetchOnce(TRENDS_HTTP_TIMEOUT_MS * 1.5));
		}

		const stories = Array.isArray(data?.stories) ? data.stories : [];
		if (!stories.length) throw new Error("empty trends payload");

		// Pick the first story whose title / entity we haven't used yet (keep Trends order)
		let picked = null;
		for (const s of stories) {
			const t = String(
				s.youtubeShortTitle || s.seoTitle || s.title || ""
			).trim();
			if (!t) continue;
			const raw = String(s.title || "").trim();
			const normT = normTitle(t);
			const normRaw = normTitle(raw);
			const usedByEntity =
				Array.isArray(s.entityNames) &&
				s.entityNames.some((e) => isTermUsed(String(e || "")));
			const alreadyUsed =
				isTermUsed(normT) || (normRaw && isTermUsed(normRaw)) || usedByEntity;
			if (!alreadyUsed) {
				picked = { story: s, effectiveTitle: t };
				break;
			}
		}

		if (!picked) {
			const s = stories[0];
			const effectiveTitle =
				String(s.youtubeShortTitle || s.seoTitle || s.title || "").trim() ||
				String(s.title || "").trim();
			picked = { story: s, effectiveTitle };
		}

		const s = picked.story;
		const effectiveTitle = picked.effectiveTitle;

		const articles = Array.isArray(s.articles) ? s.articles : [];
		const viralBriefs = normaliseTrendImageBriefs(
			s.viralImageBriefs || s.imageDirectives || [],
			effectiveTitle
		);
		const imageComment = String(s.imageComment || s.imageHook || "").trim();
		const providedImages = Array.isArray(s.images)
			? s.images.filter((u) => typeof u === "string" && /^https?:\/\//i.test(u))
			: [];
		const imagesByAspect =
			s.imagesByAspect && typeof s.imagesByAspect === "object"
				? {
						"1280:720": Array.isArray(s.imagesByAspect["1280:720"])
							? s.imagesByAspect["1280:720"].filter(
									(u) => typeof u === "string" && /^https?:\/\//i.test(u)
							  )
							: [],
						"720:1280": Array.isArray(s.imagesByAspect["720:1280"])
							? s.imagesByAspect["720:1280"].filter(
									(u) => typeof u === "string" && /^https?:\/\//i.test(u)
							  )
							: [],
						square: Array.isArray(s.imagesByAspect.square)
							? s.imagesByAspect.square.filter(
									(u) => typeof u === "string" && /^https?:\/\//i.test(u)
							  )
							: [],
						unknown: Array.isArray(s.imagesByAspect.unknown)
							? s.imagesByAspect.unknown.filter(
									(u) => typeof u === "string" && /^https?:\/\//i.test(u)
							  )
							: [],
				  }
				: {
						"1280:720": [],
						"720:1280": [],
						square: [],
						unknown: [],
				  };

		if (providedImages.length) {
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
				images: providedImages,
				imagesByAspect,
				imageSummary: s.imageSummary || null,
				articles: articles.map((a) => ({
					title: String(a.title || "").trim(),
					url: a.url || null,
					image: a.image || null,
				})),
			};
		}

		const candidates = [];
		if (s.image) {
			candidates.push({
				url: s.image,
				isStoryImage: true,
			});
		}
		for (const a of articles) {
			if (a.image) {
				candidates.push({
					url: a.image,
					isStoryImage: false,
				});
			}
		}

		if (!candidates.length) {
			console.warn("[Trending] story has no images at all");
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
				images: [],
				imagesByAspect,
				imageSummary: s.imageSummary || null,
				articles: articles.map((a) => ({
					title: String(a.title || "").trim(),
					url: a.url || null,
					image: a.image || null,
				})),
			};
		}

		const scored = candidates.map((c, idx) => {
			const info = analyseImageUrl(c.url, c.isStoryImage);
			return {
				...c,
				...info,
				idx,
			};
		});

		const nonThumb = scored.filter((c) => !c.isThumbnail);
		const activeList = nonThumb.length ? nonThumb : scored;

		activeList.sort((a, b) => {
			if (b.score !== a.score) return b.score - a.score;
			return a.idx - b.idx;
		});

		const seen = new Set();
		const images = [];
		for (const c of activeList) {
			if (!seen.has(c.url)) {
				seen.add(c.url);
				images.push(c.url);
			}
		}

		console.log("[Trending] chosen hero image:", images[0]);
		if (images.length > 1) {
			console.log(
				"[Trending] additional images:",
				images.slice(1, 5).join(" | ")
			);
		}

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
			imagesByAspect,
			imageSummary: s.imageSummary || null,
			articles: articles.map((a) => ({
				title: String(a.title || "").trim(),
				url: a.url || null,
				image: a.image || null,
			})),
		};
	} catch (e) {
		console.warn("[Trending] fetch failed ?", e.message);
		if (e.response) {
			console.warn("[Trending] HTTP status:", e.response.status);
			if (e.response.data) {
				try {
					console.warn(
						"[Trending] response data snippet:",
						typeof e.response.data === "string"
							? e.response.data.slice(0, 300)
							: JSON.stringify(e.response.data).slice(0, 300)
					);
				} catch (_) {
					console.warn("[Trending] response data snippet: [unserializable]");
				}
			}
		}
		return null;
	}
}

async function scrapeArticleText(url) {
	if (!url) return null;
	try {
		const { data: html } = await axios.get(url, {
			timeout: 10000,
			headers: ARTICLE_FETCH_HEADERS,
		});
		const $ = cheerio.load(html);
		const body = $("article").text() || $("body").text();
		const cleaned = body
			.replace(/\s+/g, " ")
			.replace(/(Advertisement|Subscribe now|Sign up for.*?newsletter).*/gi, "")
			.trim();
		return cleaned.slice(0, 12000) || null;
	} catch (e) {
		console.warn("[Scrape] article failed ?", e.message);
		if (e.response) {
			console.warn("[Scrape] HTTP status:", e.response.status);
			if (e.response.data) {
				try {
					console.warn(
						"[Scrape] response data snippet: ",
						typeof e.response.data === "string"
							? e.response.data.slice(0, 300)
							: JSON.stringify(e.response.data).slice(0, 300)
					);
				} catch (_) {}
			}
		}
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

	const isSports = category === "Sports";

	const ask = `
You are an experienced YouTube editor writing titles for ${
		isSports ? "an official sports league channel" : "a serious news channel"
	}.

Write ONE highly searchable, professional YouTube Shorts title that mirrors how people actually search.

Hard constraints:
- Maximum 65 characters.
- Title Case.
- No emojis.
- No hashtags.
- No quotation marks.
- No over-hyped or tabloid adjectives like "Insane", "Crazy", "Wild".
- The style must feel ${
		isSports
			? "like ESPN or an official league/NFL/NBA channel, not a meme or fan channel."
			: "like a major newspaper or broadcaster, not a clickbait channel."
	}
${
	category === "Top5"
		? '- The title MUST start with "Top 5" followed by the topic. Example: "Top 5 Most Popular Sports Globally".'
		: ""
}

SEO behavior:
- Include the core subject once, close to the start.
- Prefer exact search phrases users type, like ${
		isSports
			? '"start time", "how to watch", "full card", "highlights", "preview", "results".'
			: '"explained", "update", "analysis", "what to know", "timeline", "breaking news".'
	}
- Avoid filler words; every word should boost search intent or clarity.

Context from Google Trends and linked articles:
${context || "(no extra context)"}

${
	language !== DEFAULT_LANGUAGE
		? `Respond in ${language}, keeping any names in their original spelling.`
		: ""
}
If category is Top5, always begin with "Top 5" + the topic.

Return only the final title, nothing else.
`.trim();

	try {
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: ask }],
		});

		const raw = choices[0].message.content.replace(/["“”]/g, "").trim();
		return toTitleCase(raw);
	} catch (e) {
		console.warn("[SEO title] generation failed ?", e.message);
		return "";
	}
}

/* ---------------------------------------------------------------
 *  Topic helpers
 * ------------------------------------------------------------- */
const CURRENT_MONTH_YEAR = dayjs().format("MMMM YYYY");
const CURRENT_YEAR = dayjs().year();

async function topicFromCustomPrompt(text) {
	const make = (a) =>
		`
Attempt ${a}:
Give one click-worthy title (at most 70 characters, no hashtags, no quotes) set in ${CURRENT_MONTH_YEAR}.
Do not mention years before ${CURRENT_YEAR}.
<<<${text}>>>
`.trim();

	for (let a = 1; a <= 2; a++) {
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: make(a) }],
		});
		const t = choices[0].message.content.replace(/["“”]/g, "").trim();
		if (!/20\d{2}/.test(t) || new RegExp(`\\b${CURRENT_YEAR}\\b`).test(t))
			return t;
	}
	throw new Error("Cannot distil topic");
}

async function pickTrendingTopicFresh(category, language, country) {
	const loc =
		country && country.toLowerCase() !== "all countries"
			? ` in ${country}`
			: "US";
	const langLn =
		language !== DEFAULT_LANGUAGE ? ` Respond in ${language}.` : "";
	const base = (a) =>
		`
Attempt ${a}:
Return a JSON array of 10 trending ${category} titles (${CURRENT_MONTH_YEAR}${loc}), no hashtags, at most 70 characters per title.${langLn}
`.trim();

	for (let a = 1; a <= 2; a++) {
		try {
			const g = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: base(a) }],
			});
			const raw = strip(g.choices[0].message.content);
			const list = JSON.parse(raw || "[]");
			if (Array.isArray(list) && list.length) return list;
		} catch {
			/* ignore */
		}
	}
	return [`Breaking ${category} Story – ${CURRENT_MONTH_YEAR}`];
}

async function generateTop5Outline(topic, language = DEFAULT_LANGUAGE) {
	const ask = `
Current date: ${dayjs().format("YYYY-MM-DD")}

You are planning a Top 5 countdown video.

Title: ${topic}

Return a strict JSON array of exactly 5 objects, one per rank from 5 down to 1.
Each object must have:
- "rank": 5, 4, 3, 2 or 1
- "label": a short name for the item (maximum 8 words)
- "oneLine": one punchy sentence (maximum 18 words) explaining why it deserves this rank.

Use real-world facts and widely known names when appropriate; avoid speculation.
Keep everything in ${language}. Do not include any other keys or free-text.
`.trim();

	for (let attempt = 1; attempt <= 2; attempt++) {
		try {
			const { choices } = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: ask }],
			});
			const raw = strip(choices[0].message.content);
			const parsed = JSON.parse(raw);
			if (Array.isArray(parsed) && parsed.length === 5) {
				return parsed.sort((a, b) => (b.rank || 0) - (a.rank || 0));
			}
		} catch (err) {
			console.warn(
				`[GPT] Top-5 outline attempt ${attempt} failed ? ${err.message}`
			);
		}
	}
	return null;
}

function stripCountdownPrefix(text = "") {
	return String(text || "")
		.replace(/^\s*#\s*[1-5]\s*[\u2013\u2014-]?\s*/i, "")
		.replace(
			/^\s*Number\s+(one|two|three|four|five)\s*[:\u2013\u2014-]?\s*/i,
			""
		)
		.trim();
}

function removeLeadingLabel(text = "", label = "") {
	const base = String(text || "").trim();
	const lbl = String(label || "").trim();
	if (!base || !lbl) return base;
	const safe = lbl.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
	return base
		.replace(new RegExp(`^${safe}\\s*[:\\u2013\\u2014-]?\\s*`, "i"), "")
		.trim();
}

function buildCountdownLine(rank, label, bodyText = "") {
	const cleanLabel = String(label || "").trim();
	const prefix = `#${rank}- ${cleanLabel}`.trim();
	const body = removeLeadingLabel(stripCountdownPrefix(bodyText), cleanLabel);
	return body ? `${prefix}: ${body}` : prefix;
}

function applyTop5CountdownStructure(segments, outline) {
	if (!Array.isArray(segments) || !Array.isArray(outline) || !outline.length)
		return segments;

	const sorted = outline
		.slice()
		.filter((o) => o && o.rank)
		.sort((a, b) => (b.rank || 0) - (a.rank || 0));
	const updated = segments.map((seg) => ({ ...seg }));
	const max = Math.min(sorted.length, Math.max(0, updated.length - 2));

	for (let i = 0; i < max; i++) {
		const segIdx = i + 1; // segment 1 = intro
		const seg = updated[segIdx];
		if (!seg) break;

		const rank = sorted[i].rank || sorted.length - i;
		const label = String(sorted[i].label || "").trim();

		const scriptText = buildCountdownLine(rank, label, seg.scriptText || "");
		const overlayBase = `#${rank}: ${label}`.trim();
		const overlayBody = removeLeadingLabel(
			stripCountdownPrefix(seg.overlayText || ""),
			label
		);
		const overlayText = overlayBody
			? `${overlayBase} - ${overlayBody}`
			: overlayBase;

		updated[segIdx] = {
			...seg,
			scriptText,
			overlayText,
			countdownRank: rank,
			countdownLabel: label,
		};
	}

	return updated;
}

async function punchUpTop5Scripts(segments = [], segWordCaps = []) {
	if (!Array.isArray(segments) || !segments.length) return segments;

	const tightened = [];
	for (let i = 0; i < segments.length; i++) {
		const seg = segments[i];
		if (!seg) {
			tightened.push(seg);
			continue;
		}

		const cap = Math.max(5, Math.min(segWordCaps?.[i] || 32, 34));
		const rank = seg.countdownRank;
		const label = String(seg.countdownLabel || seg.overlayText || "").trim();
		const prefix = rank ? `#${rank}- ${label}`.trim() : "";

		let prompt;
		if (rank) {
			prompt = `
You are rewriting a Top 5 countdown voiceover line.
Keep the exact prefix "${prefix}" and then deliver one high-energy, non-redundant reason to care that includes a vivid detail (scene, stat, or feeling). Avoid filler like "coming in at number" or "next up". Maximum ${cap} words total.
Original: "${seg.scriptText}"
`.trim();
		} else if (i === 0) {
			prompt = `
Punch up this countdown intro so it hooks viewers to stay for #1. One sentence, max ${cap} words, energetic, specific to the topic, and clearly promises a surprising #1 pick.
Original: "${seg.scriptText}"
`.trim();
		} else if (i === segments.length - 1) {
			prompt = `
Rewrite this outro with a quick CTA to like/subscribe and explicitly invite viewers to post their own #1 in the comments. One sentence, max ${cap} words.
Original: "${seg.scriptText}"
`.trim();
		} else {
			prompt = `
Rewrite this transition so it keeps the countdown momentum. Max ${cap} words, energetic, and concrete.
Original: "${seg.scriptText}"
`.trim();
		}

		try {
			const { choices } = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: prompt }],
			});
			let rewritten = choices[0].message.content
				.trim()
				.replace(/^["'\s]+|["'\s]+$/g, "");

			if (rank) {
				const remainder = removeLeadingLabel(
					stripCountdownPrefix(rewritten),
					label
				);
				const body = remainder || stripCountdownPrefix(rewritten) || "";
				rewritten = body
					? `${prefix}: ${body}`
					: `${prefix}: ${stripCountdownPrefix(seg.scriptText)}`;
			}
			if (i === segments.length - 1 && !/comment/i.test(rewritten)) {
				rewritten = `${rewritten} Drop your #1 in the comments!`;
			}

			tightened.push({ ...seg, scriptText: rewritten });
		} catch (e) {
			console.warn("[Top5] punch-up failed ?", e.message);
			tightened.push(seg);
		}
	}

	return tightened;
}

function buildTtsPartsForSegment(seg, category) {
	const text = String(seg?.scriptText || "").trim();
	if (category !== "Top5" || !seg?.countdownRank) {
		return { parts: [text], pauseSeconds: 0 };
	}

	const rankWord = NUM_WORD[seg.countdownRank] || String(seg.countdownRank);
	const label = String(seg.countdownLabel || "").trim();
	const remainder = removeLeadingLabel(stripCountdownPrefix(text), label);
	const labelLine = label
		? `${label}${remainder ? `: ${remainder}` : ""}`
		: text;

	return {
		parts: [`Number ${rankWord}`, labelLine],
		pauseSeconds: 0.18,
	};
}

function shortImageDesc(title = "", label = "") {
	const text = String(title || label || "").trim();
	if (!text) return "";
	return text.split(/\s+/).slice(0, 5).join(" ");
}

function canUseGoogleSearch() {
	return Boolean(GOOGLE_CSE_ID && GOOGLE_CSE_KEY);
}

async function isUrlReachable(url) {
	if (!url) return false;
	const tryHead = async () =>
		axios({
			url,
			method: "head",
			timeout: 5000,
			maxRedirects: 2,
			validateStatus: (s) => s < 500,
		});
	const tryTinyGet = async () =>
		axios({
			url,
			method: "get",
			responseType: "stream",
			timeout: 6000,
			maxRedirects: 2,
			validateStatus: (s) => s < 500,
		});

	try {
		const res = await tryHead();
		if (res.status && res.status < 400) return true;
		if (res.status === 405 || res.status === 403 || res.status === 400) {
			const res2 = await tryTinyGet();
			if (res2.status && res2.status < 400) {
				if (res2.data?.destroy) res2.data.destroy();
				return true;
			}
			if (res2.data?.destroy) res2.data.destroy();
			return false;
		}
		return false;
	} catch (e) {
		return false;
	}
}

function scoreCseImageCandidate(it, target) {
	const link = it.link || it.url;
	const { score, isThumbnail } = analyseImageUrl(link, true);
	if (isThumbnail) return null;

	let total = score;
	const w = Number(it.image?.width || 0);
	const h = Number(it.image?.height || 0);
	if (w && h) {
		if (w >= 1400 || h >= 1400) total += 3;
		else if (w >= 1000 || h >= 1000) total += 2;

		if (target?.width && target?.height) {
			const ar = h ? w / h : 0;
			const targetAr = target.height ? target.width / target.height : 0;
			if (ar && targetAr) {
				const diff = Math.abs(ar - targetAr);
				if (diff < 0.08) total += 2;
				else if (diff < 0.15) total += 1;
			}
		}
	}

	if (it.displayLink && /news|cdn|images|static/i.test(it.displayLink)) {
		total += 0.5;
	}

	return {
		link,
		url: link,
		score: total,
		source: it.image?.contextLink || it.displayLink || "",
		title: it.title || "",
	};
}

async function pickBestImageFromSearch(
	items = [],
	ratio = null,
	label = "",
	avoidSet = new Set(),
	options = {}
) {
	if (!Array.isArray(items) || !items.length) return null;
	const target = targetResolutionForRatio(ratio);
	const labelTokens = tokenizeLabel(label);
	const requirePortraitForRatio =
		options.requirePortraitForRatio && isPortraitRatio(ratio);
	const minEdgeOverride = Number(options.minEdge) || 0;
	const negativeTitleRe = options.negativeTitleRe || null;
	const requireTokens = Array.isArray(options.requireTokens)
		? options.requireTokens
				.map((t) => String(t || "").toLowerCase())
				.filter(Boolean)
		: [];
	const topicTokens = Array.isArray(options.topicTokens)
		? options.topicTokens
				.map((t) => String(t || "").toLowerCase())
				.filter(Boolean)
		: [];
	const combinedTokens = [...new Set([...requireTokens, ...topicTokens])];
	const requireAnyToken = options.requireAnyToken || false;

	const candidates = [];
	for (const it of items) {
		if (!it) continue;
		const link =
			it.link ||
			it.image?.originalImageUrl ||
			it.image?.thumbnailLink ||
			it.image?.contextLink ||
			null;
		if (!link) continue;
		if (avoidSet.has(link)) continue;
		let host = "";
		try {
			host = new URL(link).hostname.toLowerCase();
			if (isBlockedHost(host) || isSoftBlockedHost(host)) continue;
		} catch {
			/* ignore malformed URL */
		}
		const title = (it.title || "").toLowerCase();
		if (negativeTitleRe && negativeTitleRe.test(title)) continue;
		const hay = `${title} ${link}`.toLowerCase();
		if (labelTokens.length && !labelTokens.some((w) => hay.includes(w)))
			continue;
		if (
			requireAnyToken &&
			combinedTokens.length &&
			!combinedTokens.some((w) => hay.includes(w))
		)
			continue;
		if (requireTokens.length && !requireTokens.some((w) => hay.includes(w)))
			continue;
		const w = Number(it.image?.width || 0);
		const h = Number(it.image?.height || 0);
		if (requirePortraitForRatio && w && h && w > h * 1.05) continue;
		if (minEdgeOverride && w && h && Math.min(w, h) < minEdgeOverride) continue;
		const scored = scoreCseImageCandidate({ ...it, link, url: link }, target);
		if (scored) candidates.push(scored);
	}

	candidates.sort((a, b) => (b.score || 0) - (a.score || 0));

	for (const cand of candidates) {
		const url = cand.url || cand.link;
		if (!url) continue;
		const ok = await isUrlReachable(url);
		if (ok) {
			console.log("[Top5] Image chosen", {
				label,
				url,
				source: cand.source,
				desc: shortImageDesc(cand.title, label),
			});
			return { ...cand, link: url, url };
		}
		console.warn("[Top5] Image unreachable, skipping", {
			label,
			url,
			source: cand.source,
		});
	}

	// Fallback: try first reachable raw item even if score failed
	for (const it of items) {
		if (!it) continue;
		const link =
			it.link ||
			it.image?.originalImageUrl ||
			it.image?.thumbnailLink ||
			it.image?.contextLink ||
			null;
		if (!link || avoidSet.has(link)) continue;
		let host = "";
		try {
			host = new URL(link).hostname.toLowerCase();
			if (isBlockedHost(host) || isSoftBlockedHost(host)) continue;
		} catch {
			/* ignore malformed URL */
		}
		const title = (it.title || "").toLowerCase();
		if (negativeTitleRe && negativeTitleRe.test(title)) continue;
		const hay = `${title} ${link}`.toLowerCase();
		if (labelTokens.length && !labelTokens.some((w) => hay.includes(w)))
			continue;
		if (requireTokens.length && !requireTokens.some((w) => hay.includes(w)))
			continue;
		const w = Number(it.image?.width || 0);
		const h = Number(it.image?.height || 0);
		if (requirePortraitForRatio && w && h && w > h * 1.05) continue;
		if (minEdgeOverride && w && h && Math.min(w, h) < minEdgeOverride) continue;
		const ok = await isUrlReachable(link);
		if (ok) {
			console.log("[Top5] Fallback image chosen", { label, url: link });
			return {
				...it,
				link,
				url: link,
				source: it.source || it.image?.contextLink || "",
			};
		}
	}

	return null;
}

async function fetchTop5LiveContext(outline = [], topic = "") {
	if (!canUseGoogleSearch()) {
		console.warn(
			"[Top5] Google Custom Search credentials missing - skipping live context"
		);
		return [];
	}
	const ctx = [];

	for (const item of outline) {
		if (!item || !item.label) continue;
		const q = [topic, item.label].filter(Boolean).join(" ");
		try {
			const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
				params: {
					key: GOOGLE_CSE_KEY,
					cx: GOOGLE_CSE_ID,
					q,
					num: 1,
					safe: "active",
				},
				timeout: GOOGLE_CSE_TIMEOUT_MS,
			});
			const hit = Array.isArray(data?.items) ? data.items[0] : null;
			if (hit) {
				ctx.push({
					rank: item.rank,
					label: item.label,
					title: String(hit.title || "").slice(0, 180),
					snippet: String(hit.snippet || "").slice(0, 260),
					link: hit.link || hit.formattedUrl || "",
				});
			}
		} catch (e) {
			console.warn("[Top5] Live search failed", {
				label: item.label,
				message: e.message,
				status: e.response?.status,
				data: e.response?.data,
			});
		}
	}

	return ctx;
}

async function fetchOgImage(url) {
	if (!url) return null;
	try {
		const { data: html } = await axios.get(url, {
			timeout: 8000,
			headers: ARTICLE_FETCH_HEADERS,
		});
		const matches = [
			/html\s*property=["']og:image["'][^>]+content=["']([^"'>]+)["']/i,
			/property=["']og:image["'][^>]+content=["']([^"'>]+)["']/i,
			/name=["']og:image["'][^>]+content=["']([^"'>]+)["']/i,
			/name=["']twitter:image["'][^>]+content=["']([^"'>]+)["']/i,
			/name=["']twitter:image:src["'][^>]+content=["']([^"'>]+)["']/i,
		];
		for (const re of matches) {
			const m = html.match(re);
			if (m && m[1]) return m[1];
		}
		return null;
	} catch (e) {
		const status = e?.response?.status;
		if (status && status !== 404) {
			console.warn("[OG] fetch failed", { url, status, msg: e.message });
		}
		return null;
	}
}

function scoreImageCandidateByRatio({ url, width, height, ratio, source }) {
	if (!url) return -1;
	const minEdge = minEdgeForRatio(ratio);
	const w = Number(width) || 0;
	const h = Number(height) || 0;
	const minOk = w && h ? Math.min(w, h) >= minEdge : true;
	if (!minOk) return -1;
	const aspectOk = w && h ? aspectMatchesRatio(ratio, w, h) : false;
	const ar = w && h ? w / h : null;
	const mp = w && h ? (w * h) / 1_000_000 : 0;
	const sourceBonus =
		/nytimes|espn|reuters|apnews|bbc|cnn|cnbc|bloomberg|guardian|washingtonpost/i.test(
			source || ""
		)
			? 1.5
			: 0.3;
	return (
		(mp ? mp * 1.2 : 0.5) +
		(aspectOk ? 1.5 : -0.8) +
		(ar
			? Math.max(0, 1 - Math.abs((targetAspectValue(ratio) || ar) - ar))
			: 0) +
		sourceBonus
	);
}

async function fetchHighQualityImagesForTopic({
	topic,
	ratio,
	articleLinks = [],
	desiredCount = 7,
	limit = 16,
	topicTokens = [],
	requireAnyToken = false,
	negativeTitleRe = null,
}) {
	const candidates = [];
	const dedupeSet = new Set();

	// 1) OG images from article links
	for (const link of articleLinks.slice(0, 8)) {
		const og = await fetchOgImage(link);
		if (og) {
			candidates.push({ url: og, source: link });
			dedupeSet.add(og);
		}
	}

	// 2) Google CSE search
	if (canUseGoogleSearch()) {
		const year = dayjs().format("YYYY");
		const queries = [
			`${topic} latest news photo ${year}`,
			`${topic} highlight photo ${year}`,
			`${topic} editorial photo ${year}`,
			`${topic} vertical phone photo ${year}`,
		];
		const pages = [1, 11, 21];

		for (const q of queries) {
			for (const start of pages) {
				try {
					const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
						params: {
							key: GOOGLE_CSE_KEY,
							cx: GOOGLE_CSE_ID,
							q,
							searchType: "image",
							imgType: "photo",
							imgSize: "huge",
							num: 10,
							start,
							safe: "high",
						},
						timeout: GOOGLE_CSE_TIMEOUT_MS,
					});
					const items = Array.isArray(data?.items) ? data.items : [];
					for (const it of items) {
						const url = it.link;
						if (!url || dedupeSet.has(url)) continue;
						const w = Number(it.image?.width || 0);
						const h = Number(it.image?.height || 0);
						const host = (() => {
							try {
								return new URL(url).hostname.toLowerCase();
							} catch {
								return "";
							}
						})();
						if (
							IMAGE_BLOCKLIST_HOSTS.some(
								(b) => host === b || host.endsWith(`.${b}`)
							)
						)
							continue;
						const title = (it.title || "").toLowerCase();
						if (negativeTitleRe && negativeTitleRe.test(title)) continue;
						if (
							requireAnyToken &&
							Array.isArray(topicTokens) &&
							topicTokens.length &&
							!matchesAnyToken(`${title} ${url}`, topicTokens)
						)
							continue;
						candidates.push({
							url,
							width: w,
							height: h,
							source: it.image?.contextLink || it.displayLink || "",
						});
						dedupeSet.add(url);
					}
				} catch (e) {
					console.warn("[ImageSearch] CSE failed", {
						query: q,
						start,
						msg: e.message,
						status: e.response?.status,
					});
				}
			}
		}
	}

	const scored = candidates
		.map((c) => ({
			...c,
			score: scoreImageCandidateByRatio({
				url: c.url,
				width: c.width,
				height: c.height,
				ratio,
				source: c.source,
			}),
		}))
		.filter((c) => c.score > 0)
		.sort((a, b) => (b.score || 0) - (a.score || 0));

	const urls = dedupeImageUrls(
		scored.map((c) => c.url),
		limit
	);

	const sliced = urls.slice(
		0,
		Math.max(desiredCount, Math.min(limit, urls.length))
	);

	console.log("[ImageSearch] candidates", {
		topic,
		ratio,
		candidates: candidates.length,
		scored: scored.length,
		returning: sliced.length,
	});

	return sliced;
}

async function fetchTop5ImagePool(outline = [], topic = "", ratio = null) {
	if (!canUseGoogleSearch()) {
		console.warn(
			"[Top5] Google Custom Search credentials missing - skipping live image pool"
		);
		return [];
	}
	const results = [];
	const yearHint = dayjs().format("YYYY");
	const negativeTitleRe = /(stock|wallpaper|logo|cartoon|illustration)/i;
	const requireTokensByLabel = new Map(
		(outline || []).map((o) => [o.label, requiredTokensForLabel(o.label)])
	);
	const topicTokens = topicTokensFromTitle(topic);

	for (const item of outline) {
		if (!item || !item.label) continue;
		const q = [
			item.label,
			topic,
			"action photo",
			"vertical 9:16",
			"editorial",
			yearHint,
			"-logo",
			"-infographic",
			"-cartoon",
			"-illustration",
		]
			.filter(Boolean)
			.join(" ");
		try {
			const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
				params: {
					key: GOOGLE_CSE_KEY,
					cx: GOOGLE_CSE_ID,
					q,
					searchType: "image",
					imgType: "photo",
					imgSize: "large",
					num: 10,
					safe: "active",
				},
				timeout: GOOGLE_CSE_TIMEOUT_MS,
			});

			let best = await pickBestImageFromSearch(
				data?.items || [],
				ratio,
				item.label,
				new Set(),
				{
					requirePortraitForRatio: true,
					minEdge: Math.max(1100, minEdgeForRatio(ratio) || 0),
					negativeTitleRe,
					requireTokens: requireTokensByLabel.get(item.label) || [],
					topicTokens,
					requireAnyToken: true,
				}
			);
			if (best?.link) {
				try {
					const host = new URL(best.link).hostname.toLowerCase();
					if (isSoftBlockedHost(host)) continue;
				} catch {
					/* ignore */
				}
				results.push({
					rank: item.rank,
					label: item.label,
					url: best.link,
					source: best.source || "",
					title: best.title || "",
				});
				continue;
			}

			// Fallback: use broader search per label + topic
			const fallbackUrls = await fetchHighQualityImagesForTopic({
				topic: `${item.label} ${topic}`,
				ratio,
				articleLinks: [],
				desiredCount: 3,
				limit: 8,
				topicTokens: topicTokensFromTitle(`${item.label} ${topic}`),
				requireAnyToken: true,
				negativeTitleRe,
			});
			const chosen = fallbackUrls.find((u) => !!u);
			if (chosen) {
				try {
					const host = new URL(chosen).hostname.toLowerCase();
					if (isSoftBlockedHost(host)) continue;
				} catch {
					/* ignore */
				}
				results.push({
					rank: item.rank,
					label: item.label,
					url: chosen,
					source: "",
					title: item.label,
				});
			}
		} catch (e) {
			console.warn("[Top5] Image search failed", {
				label: item.label,
				message: e.message,
				status: e.response?.status,
				data: e.response?.data,
			});
		}
	}

	return results;
}

async function fetchTop5ReplacementImage(
	label,
	topic,
	ratio,
	avoidSet = new Set()
) {
	if (!canUseGoogleSearch()) return null;
	const yearHint = dayjs().format("YYYY");
	const q = [label, topic, "editorial photo", yearHint]
		.filter(Boolean)
		.join(" ");
	const topicTokens = topicTokensFromTitle(topic);

	try {
		const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
			params: {
				key: GOOGLE_CSE_KEY,
				cx: GOOGLE_CSE_ID,
				q,
				searchType: "image",
				imgType: "photo",
				imgSize: "large",
				num: 10,
				safe: "active",
			},
			timeout: GOOGLE_CSE_TIMEOUT_MS,
		});

		return await pickBestImageFromSearch(
			data?.items || [],
			ratio,
			label,
			avoidSet,
			{
				requirePortraitForRatio: true,
				minEdge: Math.max(1100, minEdgeForRatio(ratio) || 0),
				negativeTitleRe: /(stock|wallpaper|logo|cartoon|illustration)/i,
				requireTokens: requiredTokensForLabel(label),
				topicTokens,
				requireAnyToken: true,
			}
		);
	} catch (e) {
		console.warn("[Top5] Replacement image search failed", {
			label,
			message: e.message,
			status: e.response?.status,
			data: e.response?.data,
		});
		return null;
	}
}

/* ---------------------------------------------------------------
 *  Cloudinary helpers for Trends images
 * ------------------------------------------------------------- */
async function uploadTrendImageToCloudinary(url, ratio, slugBase) {
	if (!url) throw new Error("Missing Trends image URL");

	const publicIdBase =
		slugBase || `aivideomatic/trend_seeds/${Date.now()}_${crypto.randomUUID()}`;

	const baseOpts = {
		public_id: publicIdBase,
		resource_type: "image",
		overwrite: false,
		folder: "aivideomatic/trend_seeds",
	};

	const transform = buildCloudinaryTransformForRatio(ratio);

	try {
		const result = await cloudinary.uploader.upload(url, {
			...baseOpts,
			transformation: transform,
		});
		console.log("[Cloudinary] Seed image uploaded ?", {
			public_id: result.public_id,
			width: result.width,
			height: result.height,
			format: result.format,
		});
		return {
			public_id: result.public_id,
			url: result.secure_url,
		};
	} catch (e) {
		const msg = String(e?.message || "");
		const sizeIssue =
			msg.includes("Maximum image size is 25 Megapixels") ||
			msg.includes("File size too large");
		if (!sizeIssue) {
			throw e;
		}

		console.warn(
			"[Cloudinary] Size limit hit, pre-downscaling locally and retrying …"
		);

		const { width, height } = targetResolutionForRatio(ratio);
		const rawPath = await downloadImageToTemp(url, ".jpg");
		const scaledPath = tmpFile("trend_scaled", ".jpg");

		try {
			const vf = [];
			if (width && height) {
				vf.push(
					`scale=${width}:${height}:force_original_aspect_ratio=decrease:flags=lanczos`,
					`crop=${width}:${height}`
				);
			}

			try {
				await ffmpegPromise((c) =>
					c
						.input(norm(rawPath))
						.videoFilters(vf.join(","))
						// Lower quality to keep under Cloudinary's 10MB hard cap
						.outputOptions("-frames:v", "1", "-q:v", "4", "-y")
						.save(norm(scaledPath))
				);
			} catch (err) {
				console.warn(
					"[Cloudinary] Primary downscale failed, retrying safe scale",
					err.message
				);
				await ffmpegPromise((c) =>
					c
						.input(norm(rawPath))
						.videoFilters(
							`scale=${
								width || 1080
							}:-2:force_original_aspect_ratio=decrease:flags=lanczos`
						)
						.outputOptions("-frames:v", "1", "-q:v", "5", "-y")
						.save(norm(scaledPath))
				);
			}
		} finally {
			try {
				fs.unlinkSync(rawPath);
			} catch (_) {}
		}

		const result = await cloudinary.uploader.upload(scaledPath, {
			...baseOpts,
			quality: "auto:good",
			fetch_format: "auto",
		});

		try {
			fs.unlinkSync(scaledPath);
		} catch (_) {}

		console.log("[Cloudinary] Seed image uploaded (fallback path) ?", {
			public_id: result.public_id,
			width: result.width,
			height: result.height,
			format: result.format,
		});

		return {
			public_id: result.public_id,
			url: result.secure_url,
		};
	}
}

async function generateOpenAIImagesForTop5(segments, ratio, topic) {
	if (!openai || !segments || !segments.length) return [];

	const size = openAIImageSizeForRatio(ratio);
	const maxSegs = Math.min(segments.length, 8);
	const outputs = [];

	for (let i = 0; i < maxSegs; i++) {
		const seg = segments[i];
		const segLabel = seg?.scriptText
			? seg.scriptText.slice(0, 120)
			: `Segment ${i + 1}`;

		const prompt = [
			`Cinematic, photorealistic keyframe for Top 5 segment #${i + 1}`,
			`Topic: ${topic}`,
			`Narration: ${segLabel}`,
			`Style: ${QUALITY_BONUS}`,
			"Sharp focus, realistic faces, clean lighting, bold composition, zero text, no logos.",
		]
			.filter(Boolean)
			.join(". ");

		try {
			const resp = await openai.images.generate({
				model: "gpt-image-1",
				prompt,
				size,
				quality: "high",
			});
			const imgUrl = resp?.data?.[0]?.url || null;
			if (!imgUrl) continue;

			const uploaded = await uploadTrendImageToCloudinary(
				imgUrl,
				ratio,
				`aivideomatic/top5_${i}_${Date.now()}`
			);
			outputs.push({
				originalUrl: imgUrl,
				cloudinaryUrl: uploaded.url,
			});
		} catch (e) {
			console.warn(
				`[OpenAI Image] Top5 segment ${i + 1} image generation failed:`,
				e.message || e
			);
		}
	}

	return outputs;
}

async function generateOpenAIImageSingle(prompt, ratio, publicIdBase) {
	const size = openAIImageSizeForRatio(ratio);
	const resp = await openai.images.generate({
		model: "gpt-image-1",
		prompt,
		size,
		quality: "high",
	});
	const imgUrl = resp?.data?.[0]?.url || null;
	if (!imgUrl) return null;
	const uploaded = await uploadTrendImageToCloudinary(
		imgUrl,
		ratio,
		publicIdBase
	);
	return {
		originalUrl: imgUrl,
		cloudinaryUrl: uploaded.url,
	};
}

async function gptTop5Plan(topic, language = DEFAULT_LANGUAGE) {
	const ask = `
 You are building a 7-part Top 5 countdown video. Make it current and exciting.
 Topic: ${topic}
 Language: ${language}
 
 Return JSON with exactly 7 objects in order: intro, rank5, rank4, rank3, rank2, rank1, outro.
 Each object must have:
  - "type": "intro" | "rank" | "outro"
  - "rank": 5/4/3/2/1 for rank items (null for intro/outro)
  - "label": short name for the item (intro/outro can reuse the topic)
  - "script": countdown pacing; 1-2 high-energy sentences (<= 32 words) with a vivid reason, scene, or stat; avoid filler like "coming in at number"
  - "overlay": action-forward 3-8 word overlay text (no punctuation/hashtags)
  - "imageQuery": concise search query for a strong photo, include action words and the item name; avoid generic wallpapers.
  - "runwayPrompt": vivid cinematic prompt describing motion for image_to_video; include setting, action, camera, lighting; avoid text/logos.
 
 Accuracy rules:
  - Use up-to-date, widely accepted rankings as of TODAY. Do not invent teams/players/items that are not truly in the current Top 5 for this topic.
  - Ranks must be in true 5 -> 1 order, no duplicates, no skipped ranks.
  - If unsure, pick the most globally recognized consensus list rather than speculation.
  - Avoid player or team names unless the topic explicitly requires individuals (e.g., "richest men", "top players"); otherwise focus on the item/sport itself.
  - Use "Soccer" explicitly for the global sport (never "football" unless "American football" is truly intended).
  Style rules:
  - Vary verbs and sentence openings across ranks; no repeated phrasing.
  - Intro should hook viewers to stay for #1; outro should invite them to share their own #1 plus a quick CTA.
 `.trim();

	for (let attempt = 1; attempt <= 2; attempt++) {
		try {
			const { choices } = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: ask }],
			});
			const raw = strip(choices[0].message.content);
			const parsed = JSON.parse(raw);
			if (Array.isArray(parsed) && parsed.length === 7) return parsed;
		} catch (e) {
			console.warn(`[GPT] Top5 plan attempt ${attempt} failed ? ${e.message}`);
		}
	}

	console.warn("[Top5] GPT plan failed, falling back to outline-based plan");
	const fallback = await buildTop5FallbackPlan(topic, language);
	if (fallback && Array.isArray(fallback) && fallback.length === 7)
		return fallback;
	throw new Error("Top5 GPT plan missing 7 segments");
}

async function buildTop5FallbackPlan(topic, language = DEFAULT_LANGUAGE) {
	const outline = await generateTop5Outline(topic, language);
	if (!Array.isArray(outline) || outline.length < 5) return null;

	const sorted = outline
		.filter((o) => o && o.rank)
		.sort((a, b) => (b.rank || 0) - (a.rank || 0))
		.slice(0, 5);
	const year = dayjs().format("YYYY");

	const intro = {
		type: "intro",
		rank: null,
		label: topic,
		script:
			"Counting down the top cities to visit right now—stick around for #1.",
		overlay: "Top 5 Cities Now",
		imageQuery: `${topic} skyline golden hour ${year}`,
		runwayPrompt:
			"Cinematic aerial over a vibrant city skyline at sunset, people and traffic in motion, smooth drone move, warm light",
	};

	const ranks = sorted.map((o, idx) => {
		const rank = Number(o.rank) || 5 - idx;
		const label = normalizeLabelForTopic(o.label || `Pick ${rank}`, topic);
		const body =
			String(o.oneLine || "").trim() || "Buzzing right now for travelers.";
		return {
			type: "rank",
			rank,
			label,
			script: `#${rank}- ${label}: ${body}`,
			overlay: `${label}`.slice(0, 32),
			imageQuery: `${label} travel photo ${year} action`,
			runwayPrompt: `Cinematic travel shot in ${label}, locals and visitors moving through iconic streets, golden hour light, smooth camera glide`,
		};
	});

	const outro = {
		type: "outro",
		rank: null,
		label: topic,
		script:
			"Which city is your #1? Drop it below and save this list for your next trip.",
		overlay: "Your #1 city?",
		imageQuery: `${topic} traveler airport window ${year}`,
		runwayPrompt:
			"Traveler at an airport window watching planes take off, soft morning light, gentle dolly move",
	};

	return [intro, ...ranks, outro];
}

async function searchAndUploadTop5Image({
	query,
	label,
	rank,
	ratio,
	topic,
	slug,
}) {
	const topicTokens = topicTokensFromTitle(`${topic} ${label || ""}`);
	const urlCandidates = await fetchHighQualityImagesForTopic({
		topic: query,
		ratio,
		articleLinks: [],
		desiredCount: 4,
		limit: 10,
		topicTokens,
		requireAnyToken: true,
		negativeTitleRe: /(stock|wallpaper|logo|cartoon|illustration|wallpaper)/i,
	});
	const publicIdBase = `aivideomatic/top5_refs/${slug}/${rankSlug(
		rank
	)}_${safeSlug(label || topic, 32)}`;

	for (const url of urlCandidates) {
		try {
			const up = await uploadTrendImageToCloudinary(url, ratio, publicIdBase);
			return { originalUrl: url, cloudinaryUrl: up.url };
		} catch (e) {
			console.warn(
				"[Top5] Upload failed for candidate, trying next",
				e.message
			);
		}
	}

	// Fallback to AI image
	const aiPrompt = `Cinematic, photorealistic vertical photo for ${
		label || topic
	}. ${QUALITY_BONUS}. No text, no logos.`;
	const aiImage = await generateOpenAIImageSingle(
		aiPrompt,
		ratio,
		publicIdBase
	);
	return aiImage;
}

async function buildTop5SegmentsAndImages({ topic, ratio, language, segLens }) {
	const plan = await gptTop5Plan(topic, language);
	const orderedPlan = enforceTop5Order(plan);
	if (!orderedPlan) {
		throw new Error("Top5 GPT plan missing intro/#5/#4/#3/#2/#1/outro");
	}
	const slug = safeSlug(topic || "top5");
	const year = dayjs().format("YYYY");

	const segments = [];
	const trendImagePairs = [];

	for (let idx = 0; idx < orderedPlan.length; idx++) {
		const p = orderedPlan[idx] || {};
		const segType = p.type || (p.rank ? "rank" : idx === 0 ? "intro" : "outro");
		const rank = segType === "rank" ? p.rank || 6 - idx : null;
		const labelRaw = String(
			p.label || (segType === "intro" ? topic : "Outro")
		).trim();
		const label = normalizeLabelForTopic(labelRaw, topic);
		const baseQuery =
			p.imageQuery ||
			`${label} ${topic} action photo ${
				segType === "intro" ? "opening" : ""
			} ${year}`;

		const rawScript = String(p.script || "").trim();
		const cleanBody = removeLeadingLabel(
			stripCountdownPrefix(rawScript),
			label
		);
		const scriptText = buildCountdownLine(
			rank || "",
			label,
			cleanBody || rawScript
		);
		const overlayBody = removeLeadingLabel(
			stripCountdownPrefix(p.overlay || label),
			label
		);
		const overlayText =
			segType === "rank"
				? `#${rank}: ${overlayBody || label}`.trim()
				: String(p.overlay || label).trim();
		const runwayPrompt = String(
			p.runwayPrompt || `${label} cinematic action shot`
		).trim();

		const img = await searchAndUploadTop5Image({
			query: baseQuery,
			label,
			rank:
				segType === "intro" ? "intro" : segType === "outro" ? "outro" : rank,
			ratio,
			topic,
			slug,
		});
		if (!img || !img.cloudinaryUrl) {
			throw new Error(`Top5 missing image for segment ${idx + 1}`);
		}

		trendImagePairs.push(img);

		segments.push({
			index: idx + 1,
			scriptText,
			overlayText,
			runwayPrompt,
			runwayNegativePrompt: RUNWAY_NEGATIVE_PROMPT,
			countdownRank: rank,
			countdownLabel: rank ? label : undefined,
			imageIndex: idx,
			referenceImageUrl: img.originalUrl,
		});
	}

	if (trendImagePairs.length !== 7) {
		throw new Error(
			"Top5 requires exactly 7 images; planning failed to secure all slots."
		);
	}

	// Align segment lengths if GPT returned anything different
	if (Array.isArray(segLens) && segLens.length === segments.length) {
		segments.forEach((s, i) => (s.targetDuration = segLens[i]));
	}

	return { segments, trendImagePairs };
}

async function uploadReferenceImagesForTop5(
	segments,
	ratio,
	topic,
	top5Outline
) {
	const pairs = [];
	const urlToIdx = new Map();
	const safeSlug = String(topic || "top5")
		.toLowerCase()
		.replace(/[^\w]+/g, "_")
		.replace(/^_+|_+$/g, "")
		.slice(0, 40);
	const attempted = new Set();
	const desiredRanks =
		Array.isArray(top5Outline) && top5Outline.length
			? Array.from(new Set(top5Outline.map((o) => o.rank).filter(Boolean)))
			: null;
	const topicTokens = topicTokensFromTitle(topic);

	for (let i = 0; i < segments.length; i++) {
		// only upload ranked content segments; skip intro/outro or non-countdown parts
		if (!segments[i].countdownRank && !segments[i].rank) continue;

		let url = String(segments[i].referenceImageUrl || "").trim();
		const segLabel =
			segments[i].countdownLabel ||
			segments[i].label ||
			segments[i].overlayText ||
			`Segment ${i + 1}`;
		const labelTokens = tokenizeLabel(segLabel);

		// Hard cap uploads to avoid excess cost; keep up to 5 ranked images
		if (pairs.length >= 5) break;

		let uploaded = false;
		let attempts = 0;
		while (!uploaded && attempts < 3) {
			attempts += 1;
			if (
				!url ||
				urlToIdx.has(url) ||
				attempted.has(url) ||
				(labelTokens.length || topicTokens.length
					? !matchesAnyToken(url, [...labelTokens, ...topicTokens]) &&
					  !matchesAnyToken(decodeURIComponent(url), [
							...labelTokens,
							...topicTokens,
					  ])
					: false)
			) {
				const replacement = await fetchTop5ReplacementImage(
					segLabel,
					topic,
					ratio,
					attempted
				);
				url = replacement?.url || replacement?.link || "";
			}
			if (!url) break;

			const reachable = await isUrlReachable(url);
			if (!reachable) {
				attempted.add(url);
				url = "";
				continue;
			}

			const host = (() => {
				try {
					return new URL(url).hostname.toLowerCase();
				} catch {
					return "";
				}
			})();
			if (
				IMAGE_UPLOAD_SOFT_BLOCK.some(
					(b) => host === b || host.endsWith(`.${b}`)
				)
			) {
				url = "";
				continue;
			}

			attempted.add(url);
			console.log("[Top5] Uploading reference image", {
				segment: i + 1,
				label: segLabel,
				url,
				desc: shortImageDesc("", segLabel),
			});

			try {
				const up = await uploadTrendImageToCloudinary(
					url,
					ratio,
					`aivideomatic/top5_refs/${safeSlug}_${i}`
				);
				const idx = pairs.length;
				const rankVal =
					segments[i].countdownRank ||
					segments[i].rank ||
					(() => {
						const m = String(segLabel || "").match(/#(\d+)/);
						return m ? Number(m[1]) : null;
					})();
				pairs.push({ originalUrl: url, cloudinaryUrl: up.url, rank: rankVal });
				urlToIdx.set(url, idx);
				segments[i].referenceImageUrl = url;
				uploaded = true;
			} catch (e) {
				console.warn("[Top5] Upload reference image failed ?", e.message);
				attempted.add(url);
				url = "";
			}
		}
	}

	const segImageIndex = segments.map((s) => {
		const url = String(s.referenceImageUrl || "").trim();
		return urlToIdx.has(url) ? urlToIdx.get(url) : null;
	});

	// If we are still short of 5 unique ranked images, try to fill missing ranks
	if (pairs.length < 5 && desiredRanks && desiredRanks.length) {
		for (const rank of desiredRanks) {
			if (pairs.find((p) => p.rank === rank)) continue;
			const label =
				(top5Outline || []).find((o) => o.rank === rank)?.label ||
				`Rank ${rank}`;
			let nextUrl = "";

			const replacement = await fetchTop5ReplacementImage(
				label,
				topic,
				ratio,
				attempted
			);
			nextUrl = replacement?.url || replacement?.link || "";

			if (!nextUrl) {
				const extra = await fetchHighQualityImagesForTopic({
					topic: `${label} sport action photo`,
					ratio,
					articleLinks: [],
					desiredCount: 2,
					limit: 5,
					topicTokens: topicTokensFromTitle(`${label} ${topic}`),
					requireAnyToken: true,
					negativeTitleRe: /(stock|wallpaper|logo|cartoon|illustration)/i,
				});
				nextUrl = (extra && extra[0]) || "";
			}

			if (!nextUrl || attempted.has(nextUrl)) continue;
			const reachable = await isUrlReachable(nextUrl);
			if (!reachable) {
				attempted.add(nextUrl);
				continue;
			}

			attempted.add(nextUrl);
			try {
				const up = await uploadTrendImageToCloudinary(
					nextUrl,
					ratio,
					`aivideomatic/top5_refs/${safeSlug}_fill_${rank}`
				);
				pairs.push({ originalUrl: nextUrl, cloudinaryUrl: up.url, rank });
			} catch (e) {
				console.warn("[Top5] Fallback fill upload failed", e.message);
			}
			if (pairs.length >= 5) break;
		}
	}

	return { pairs, segImageIndex };
}

function alignTop5ImageIndexes(segments, top5Outline, pairs) {
	if (!Array.isArray(segments) || !Array.isArray(pairs) || !pairs.length)
		return segments;
	const byRank = new Map();
	(pairs || []).forEach((p, idx) => byRank.set(p.rank || idx + 1, idx));
	return segments.map((seg, idx) => {
		const rank =
			seg?.countdownRank ||
			(top5Outline && idx - 1 >= 0 && idx - 1 < top5Outline.length
				? top5Outline[idx - 1].rank
				: null);
		if (!rank) return seg;
		if (seg.imageIndex !== null && seg.imageIndex !== undefined) return seg;
		const mapped = byRank.get(rank);
		return mapped !== undefined ? { ...seg, imageIndex: mapped } : seg;
	});
}

/* ---------------------------------------------------------------
 *  Runway poll + retry
 * ------------------------------------------------------------- */
async function pollRunway(id, tk, lbl) {
	const url = `https://api.dev.runwayml.com/v1/tasks/${id}`;
	for (let i = 0; i < MAX_POLL_ATTEMPTS; i++) {
		await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
		let resp;
		try {
			resp = await axios.get(url, {
				headers: {
					Authorization: `Bearer ${tk}`,
					"X-Runway-Version": RUNWAY_VERSION,
				},
			});
		} catch (e) {
			console.error("[Runway] poll error", {
				label: lbl,
				taskId: id,
				message: e.message,
				status: e.response?.status,
				data: e.response?.data
					? (() => {
							try {
								return typeof e.response.data === "string"
									? e.response.data.slice(0, 400)
									: JSON.stringify(e.response.data).slice(0, 400);
							} catch {
								return "[unserializable response data]";
							}
					  })()
					: undefined,
			});
			throw e;
		}

		const { data } = resp;
		if (data.status === "SUCCEEDED") {
			console.log("[Runway] task SUCCEEDED", { label: lbl, taskId: id });
			return data.output[0];
		}
		if (data.status === "FAILED") {
			console.error("[Runway] task FAILED", {
				label: lbl,
				taskId: id,
				status: data.status,
				failureCode: data.failureCode,
				failure: data.failure,
			});
			const err = new Error(
				`${lbl} failed (Runway: ${data.failureCode || "FAILED"})`
			);
			if (/SAFETY/i.test(String(data.failureCode || ""))) err.isSafety = true;
			err.failureCode = data.failureCode || null;
			throw err;
		}
	}
	console.error("[Runway] task TIMED OUT", { label: lbl, taskId: id });
	throw new Error(`${lbl} timed out`);
}

async function retry(fn, max, lbl) {
	let last;
	for (let a = 1; a <= max; a++) {
		try {
			console.log(`[Retry] ${lbl} attempt ${a}/${max}`);
			return await fn();
		} catch (e) {
			const status = e?.response?.status;
			console.warn(
				`[Retry] ${lbl} attempt ${a} failed${
					status ? ` (HTTP ${status})` : ""
				} ? ${e.message}`
			);
			if (e.response?.data) {
				try {
					const snippet =
						typeof e.response.data === "string"
							? e.response.data.slice(0, 300)
							: JSON.stringify(e.response.data).slice(0, 300);
					console.warn(`[Retry] ${lbl} response data snippet:`, snippet);
				} catch (_) {
					console.warn(
						`[Retry] ${lbl} response data snippet: [unserializable]`
					);
				}
			}
			last = e;
			const isSafety = Boolean(e?.isSafety) || /SAFETY/i.test(e?.message || "");
			if (isSafety) break;
			if (status && status >= 400 && status < 500 && status !== 429) break;
		}
	}
	throw last;
}

/* ---------------------------------------------------------------
 *  Runway helpers for clips
 * ------------------------------------------------------------- */
async function generateItvClipFromImage({
	segmentIndex,
	imgUrl,
	promptText,
	negativePrompt,
	ratio,
	runwayDuration,
}) {
	const itvLabel = `itv_seg${segmentIndex}`;
	const pollLabel = `poll_itv_seg${segmentIndex}`;

	const runwayModel =
		process.env.RUNWAY_ITV_MODEL && process.env.RUNWAY_ITV_MODEL.trim()
			? process.env.RUNWAY_ITV_MODEL.trim()
			: ITV_MODEL;

	const idVid = await retry(
		async () => {
			const { data } = await axios.post(
				"https://api.dev.runwayml.com/v1/image_to_video",
				{
					model: runwayModel,
					promptImage: imgUrl,
					promptText,
					ratio,
					duration: runwayDuration,
					promptStrength: 0.55,
					negativePrompt: negativePrompt || RUNWAY_NEGATIVE_PROMPT,
				},
				{
					headers: {
						Authorization: `Bearer ${RUNWAY_ADMIN_KEY}`,
						"X-Runway-Version": RUNWAY_VERSION,
					},
				}
			);
			return data.id;
		},
		2,
		itvLabel
	);

	const vidUrl = await retry(
		() => pollRunway(idVid, RUNWAY_ADMIN_KEY, pollLabel),
		3,
		pollLabel
	);

	const p = tmpFile(`seg_itv_${segmentIndex}`, ".mp4");
	await new Promise((r, j) =>
		axios
			.get(vidUrl, { responseType: "stream" })
			.then(({ data }) =>
				data.pipe(fs.createWriteStream(p)).on("finish", r).on("error", j)
			)
	);
	return p;
}

async function generateTtiItvClip({
	segmentIndex,
	promptText,
	negativePrompt,
	ratio,
	runwayDuration,
}) {
	throw new Error("Text-to-image disabled for this pipeline");
}

/**
 * Static fallback when Runway refuses / errors on a Trends image.
 */
async function generateStaticClipFromImage({
	segmentIndex,
	imgUrlOriginal,
	imgUrlCloudinary,
	ratio,
	targetDuration,
	zoomPan = false,
}) {
	const candidates = [imgUrlOriginal, imgUrlCloudinary].filter(Boolean);
	if (!candidates.length) throw new Error("Missing image URL for static clip");

	console.log(
		`[Seg ${segmentIndex}] Using static image fallback. Candidates:`,
		candidates
	);

	let lastErr;

	for (const url of candidates) {
		try {
			const localPath = await downloadImageToTemp(url, ".jpg");
			const out = tmpFile(`seg_static_${segmentIndex}`, ".mp4");
			const { width, height } = targetResolutionForRatio(ratio);
			const filterPresets = [
				() => {
					const vf = ["format=yuv420p", "setsar=1"];
					if (width && height) {
						vf.push(
							`scale=${width}:${height}:force_original_aspect_ratio=increase:flags=lanczos`,
							`crop=${width}:${height}`
						);
					}
					const minEdge = Math.min(width || 0, height || 0);
					if (zoomPan && width && height && minEdge >= 960) {
						vf.push(
							`zoompan=z='min(1.0+0.0015*n,1.06)':d=1:x='iw/2-(iw/2)/zoom':y='ih/2-(ih/2)/zoom':s=${width}x${height}:fps=30`
						);
					}
					return vf;
				},
				() => {
					const vf = ["format=yuv420p", "setsar=1"];
					if (width && height) {
						vf.push(
							`scale=${width}:${height}:force_original_aspect_ratio=increase:flags=lanczos`,
							`crop=${width}:${height}`
						);
					}
					// no zoompan
					return vf;
				},
			];

			let success = false;
			for (
				let attempt = 0;
				attempt < filterPresets.length && !success;
				attempt++
			) {
				const vf = filterPresets[attempt]();
				try {
					await ffmpegPromise((c) => {
						c.input(norm(localPath)).inputOptions("-loop", "1");

						if (vf.length) {
							c.videoFilters(vf.join(","));
						}

						return c
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
							.save(norm(out));
					});
					success = true;
				} catch (inner) {
					lastErr = inner;
					console.warn(
						`[Seg ${segmentIndex}] Static fallback filter attempt ${
							attempt + 1
						} failed for ${url}`,
						inner.message
					);
				}
			}

			try {
				fs.unlinkSync(localPath);
			} catch (_) {}

			if (!success) throw lastErr || new Error("Static filter failed");

			console.log(
				`[Seg ${segmentIndex}] Static fallback clip created from ${url}`
			);

			return out;
		} catch (e) {
			lastErr = e;
			console.warn(
				`[Seg ${segmentIndex}] Static fallback failed for ${url} ?`,
				e.message
			);
		}
	}

	console.warn(
		`[Seg ${segmentIndex}] All static fallbacks failed, using placeholder clip`
	);
	return await generatePlaceholderClip({
		segmentIndex,
		ratio,
		targetDuration,
		color: "gray",
	});
}

async function generatePlaceholderClip({
	segmentIndex,
	ratio,
	targetDuration,
	color = "gray",
}) {
	const { width, height } = targetResolutionForRatio(ratio);
	const size = width && height ? `${width}x${height}` : "1080x1920";
	const out = tmpFile(`seg_placeholder_${segmentIndex}`, ".mp4");
	try {
		await ffmpegPromise((c) =>
			c
				.input(`color=${color}:s=${size}:r=30:d=${targetDuration}`)
				.inputOptions("-f", "lavfi")
				.videoFilters(
					[
						"format=yuv420p",
						"setsar=1",
						`zoompan=z='min(1.0+0.001*n,1.04)':d=1:x='iw/2-(iw/2)/zoom':y='ih/2-(ih/2)/zoom':s=${size}:fps=30`,
					].join(",")
				)
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
		const simple = tmpFile(`seg_placeholder_simple_${segmentIndex}`, ".mp4");
		console.warn(
			`[Seg ${segmentIndex}] Placeholder zoompan failed, falling back to solid frame`,
			e.message
		);
		await ffmpegPromise((c) =>
			c
				.input(`color=${color}:s=${size}:r=30:d=${targetDuration}`)
				.inputOptions("-f", "lavfi")
				.videoFilters(["format=yuv420p", "setsar=1"].join(","))
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
				.save(norm(simple))
		);
		return simple;
	}
}

function buildRunwayPrompt(seg, globalStyle) {
	const parts = [
		seg.runwayPrompt || "",
		globalStyle,
		"cinematic, cohesive single shot with smooth camera motion",
		QUALITY_BONUS,
		PHYSICAL_REALISM_HINT,
		EYE_REALISM_HINT,
		SOFT_SAFETY_PAD,
		HUMAN_SAFETY,
		BRAND_ENHANCEMENT_HINT,
	]
		.filter(Boolean)
		.join(". ");

	let promptText = scrubPromptForSafety(parts);
	if (promptText.length > PROMPT_CHAR_LIMIT)
		promptText = promptText.slice(0, PROMPT_CHAR_LIMIT);

	const negativeBase =
		seg.runwayNegativePrompt && seg.runwayNegativePrompt.trim().length
			? seg.runwayNegativePrompt.trim()
			: RUNWAY_NEGATIVE_PROMPT;
	const negativePrompt =
		negativeBase.length > PROMPT_CHAR_LIMIT
			? negativeBase.slice(0, PROMPT_CHAR_LIMIT)
			: negativeBase;

	return { promptText, negativePrompt };
}

/* ---------------------------------------------------------------
 *  YouTube & Jamendo helpers
 * ------------------------------------------------------------- */
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
	const creds =
		source && source.access_token !== undefined
			? source
			: resolveYouTubeTokens({ body: {} }, source);
	if (!creds.refresh_token) return null;
	const o = new google.auth.OAuth2(
		process.env.YOUTUBE_CLIENT_ID,
		process.env.YOUTUBE_CLIENT_SECRET,
		process.env.YOUTUBE_REDIRECT_URI
	);
	o.setCredentials(creds);
	return o;
}
async function refreshYouTubeTokensIfNeeded(user, req) {
	const tokens = resolveYouTubeTokens(req, user);
	const o = buildYouTubeOAuth2Client(tokens);
	if (!o) return tokens;
	try {
		const { token } = await o.getAccessToken();
		if (token) {
			const fresh = {
				access_token: o.credentials.access_token,
				refresh_token: o.credentials.refresh_token || tokens.refresh_token,
				expiry_date: o.credentials.expiry_date,
			};
			user.youtubeAccessToken = fresh.access_token;
			user.youtubeRefreshToken = fresh.refresh_token;
			user.youtubeTokenExpiresAt = fresh.expiry_date;
			if (user.isModified && user.isModified() && user.role !== "admin")
				await user.save();
			return fresh;
		}
	} catch {}
	return tokens;
}
async function uploadToYouTube(u, fp, { title, description, tags, category }) {
	const o = buildYouTubeOAuth2Client(u);
	if (!o) throw new Error("YouTube OAuth missing");
	const yt = google.youtube({ version: "v3", auth: o });
	const { data } = await yt.videos.insert(
		{
			part: ["snippet", "status"],
			requestBody: {
				snippet: {
					title,
					description,
					tags,
					categoryId:
						YT_CATEGORY_MAP[category] === "0"
							? "22"
							: YT_CATEGORY_MAP[category],
				},
				status: { privacyStatus: "public", selfDeclaredMadeForKids: false },
			},
			media: { body: fs.createReadStream(fp) },
		},
		{ maxContentLength: Infinity, maxBodyLength: Infinity }
	);
	return `https://www.youtube.com/watch?v=${data.id}`;
}
async function jamendo(term) {
	try {
		const { data } = await axios.get("https://api.jamendo.com/v3.0/tracks", {
			params: { client_id: JAMENDO_ID, format: "json", limit: 1, search: term },
		});
		return data.results?.length ? data.results[0].audio : null;
	} catch {
		return null;
	}
}

/* Background music planning */
async function planBackgroundMusic(category, language, script) {
	const defaultVoiceGain = category === "Top5" ? 1.5 : 1.4;
	const defaultMusicGain = category === "Top5" ? 0.18 : 0.14;
	const upbeatHint =
		category === "Top5"
			? "Fun, upbeat, exciting countdown energy with a motivational feel, percussive, no vocals."
			: "";

	const ask = `
You are a sound designer for short-form YouTube videos.

Goal:
- Category: ${category}
- Language: ${language}
- Script (excerpt): ${String(script || "").slice(0, 600)}

Pick background music that:
- Has NO vocals (instrumental only).
- Fits the pacing and emotion of the script.
- Never overpowers the narration.
${upbeatHint ? "- " + upbeatHint : ""}

Return JSON:
{
  "jamendoSearch": "one concise search term for Jamendo, including genre and mood, must imply no vocals",
  "fallbackSearchTerms": ["term1", "term2"],
  "voiceGain": ${defaultVoiceGain},
	"musicGain": ${defaultMusicGain}
}

Constraints:
- "fallbackSearchTerms" must be an array of exactly 2 short strings.
- "voiceGain" between 1.2 and 1.7.
- "musicGain" between 0.08 and 0.22.
- Use English for search terms even if narration language is different.
`.trim();

	try {
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: ask }],
		});
		const raw = strip(choices[0].message.content);
		const parsed = JSON.parse(raw);
		if (!parsed || typeof parsed !== "object") return null;

		let voiceGain = Number(parsed.voiceGain) || 1.4;
		let musicGain = Number(parsed.musicGain) || 0.14;

		voiceGain = Math.max(1.2, Math.min(1.7, voiceGain));
		musicGain = Math.max(0.08, Math.min(0.22, musicGain));

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
		console.warn("[MusicPlan] planning failed ?", e.message);
		return null;
	}
}

/* ---------------------------------------------------------------
 *  ElevenLabs helpers – dynamic voice selection + TTS
 * ------------------------------------------------------------- */
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
		console.warn("[Eleven] fetch voices failed ?", e.message);
		return null;
	}
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

	const slimVoices = voices
		.filter((v) => v && (v.voice_id || v.voiceId))
		.slice(0, 30)
		.map((v) => ({
			id: v.voice_id || v.voiceId,
			name: v.name || "",
			category: v.category || "",
			labels: v.labels || {},
			description: v.description || "",
		}));

	let candidates = slimVoices;

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

		const egyptian = slimVoices.filter(isEgyptian);
		const arabic = slimVoices.filter(isArabic);
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
		const americanCandidates = slimVoices.filter((v) => {
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
				"[Eleven] No explicit American English voices detected in /voices – using static fallback voice."
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
				"[Eleven] All candidate voices are in the avoid list – keeping full candidate set."
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
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: ask }],
		});
		const parsed = JSON.parse(strip(choices[0].message.content));
		if (parsed && parsed.voiceId) {
			return {
				voiceId: parsed.voiceId,
				name: parsed.name || "",
				source: "dynamic-gpt",
				reason: parsed.reason || "",
			};
		}
	} catch (e) {
		console.warn("[Eleven] GPT voice selection failed ?", e.message);
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

/* ---------------------------------------------------------------
 *  OpenAI director – build full video plan (multi-image aware)
 * ------------------------------------------------------------- */
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
	top5LiveContext,
	top5ImagePool,
	ratio,
	trendImageBriefs,
	engagementTailSeconds,
	country,
	forceStaticVisuals = false,
}) {
	const segCnt = segLens.length;
	const segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));
	const hasImages =
		trendImagesForPlanning &&
		Array.isArray(trendImagesForPlanning) &&
		trendImagesForPlanning.length > 0;
	const images = hasImages ? trendImagesForPlanning.slice(0, 8) : [];
	const articleTitles = (trendStory?.articles || [])
		.map((a) => a.title)
		.filter(Boolean);
	const snippet = articleText ? articleText.slice(0, 1800) : "";
	const imageBriefs = Array.isArray(trendImageBriefs) ? trendImageBriefs : [];
	const ratioBrief =
		ratio && imageBriefs.length
			? imageBriefs.find((b) => b.aspectRatio === ratio) ||
			  imageBriefs[0] ||
			  null
			: imageBriefs[0] || null;
	const imageComment = String(trendStory?.imageComment || "").trim();
	const liveContext = Array.isArray(top5LiveContext) ? top5LiveContext : [];
	const liveImages = Array.isArray(top5ImagePool) ? top5ImagePool : [];
	const top5NeedsExtraDetail =
		category === "Top5" && Number.isFinite(duration) && duration >= 45;
	const runwayAnimationNote =
		category === "Top5"
			? "All five ranked segments will be animated with Runway (less strict than Sora) using unique real photos that clearly show each ranked item; if animation fails on a photo, we will show a static clip instead."
			: "Only segments 1 and 2 will be animated with Runway (less strict than Sora); keep the core story beats there so later segments can stay simple if needed.";
	const segDescLines = segLens
		.map(
			(sec, i) =>
				`Segment ${i + 1}: ~${sec.toFixed(1)}s, = ${
					segWordCaps[i]
				} spoken words.`
		)
		.join("\n");
	const totalSegSeconds = segLens.reduce((a, b) => a + b, 0) || 1;
	const runwayStoryCount = category === "Top5" ? 0 : Math.min(2, segCnt - 1);
	const runwaySharePct =
		runwayStoryCount > 0
			? Math.round(
					(segLens.slice(0, runwayStoryCount).reduce((a, b) => a + b, 0) /
						totalSegSeconds) *
						100
			  )
			: 0;

	const categoryTone = TONE_HINTS[category] || "";
	const outroDirective = `
Segment ${segCnt} is the engagement outro (about ${
		engagementTailSeconds || "5-6"
	} seconds):
- Ask one crisp, on-topic question to spark comments.
- Immediately follow with a warm, slightly funny like/subscribe/comment nudge for an American audience that feels tailored to this topic.
- Vary the phrasing so outros never feel templated; keep it playful and topic-aware.
- Keep it concise and entirely in ${language}.
- Make it sound human and upbeat, not robotic; a friendly host riffing on the story.
- This extra outro is appended on top of the requested duration, with a 3-5s tolerance buffer baked in, so you can finish the thought without cutting yourself off.
`.trim();

	const baseIntro = `
Current date: ${dayjs().format("YYYY-MM-DD")}

You are an expert short-form video editor and producer.

We need a ${duration}s ${category} YouTube Shorts video titled "${topic}",
split into ${segCnt} sequential segments.

Segment timing:
${segDescLines}

Narration rules:
- Natural spoken language, like a professional commentator.
- Vary sentence lengths and verbs so it feels human and lively, not robotic.
- Sprinkle quick, honest reactions that match the facts (amazed, relieved, concerned) without overhyping.
- Even for somber news, stay compassionate but keep momentum with clear, visual language - no flat recaps.
- All core narration (intro + content) must fit inside the requested ${duration}s; outro sits on top with a tiny buffer so it never truncates mid-sentence.
- Use the provided article headlines/snippets as your source of truth; if something is unconfirmed, state that instead of inventing details. Stay timely to the trend.
- Give each segment one concrete, visual detail or comparison that makes the scene easy to picture.
- Stay accurate; do NOT invent fake scores, injuries, or quotes.
- No "In this video" filler; keep like/subscribe wording ONLY in the final engagement segment.
- Segment 1 must hook immediately.
- Later segments deepen context: stakes, key players, what to watch, etc.
- Stay within word caps so narration fits timing.
- All narration MUST be in ${language}.
- Ignore the country's native language; keep EVERY word in ${language} even if geo/country differs.
- For nontragic topics, pacing should feel clear and slightly brisk.
- For clearly tragic or sensitive stories, slow pacing slightly but keep it clear and respectful.
- Avoid speculation or hallucinations; if a detail is unconfirmed, state that it's unconfirmed rather than inventing facts.
- Keep pacing human and coherent; do not cram unnatural speed-reading into segments.
- Keep every segment directly on-topic for "${topic}"; no unrelated tangents.
${categoryTone ? `- Tone: ${categoryTone}` : ""}
${
	category === "Sports"
		? "- Sports: call it like a clutch highlight; use the freshest headline details, be precise with scores, and keep the energy up without inventing stats."
		: ""
}
${runwayAnimationNote ? `- ${runwayAnimationNote}` : ""}
${
	runwayStoryCount
		? `- Segments 1${
				runwayStoryCount === 1 ? "" : `-${runwayStoryCount}`
		  } carry the core visuals and together use about ${runwaySharePct}% of the runtime; pack the key beats there so later segments can stay simple.`
		: ""
}
${outroDirective}
`.trim();

	let promptText;
	if (hasImages) {
		const imgCount = images.length;
		promptText = `
${baseIntro}

You also have ${imgCount} REAL photos from Google Trends for this story.

Google Trends context:
- Story title: ${trendStory?.title || topic}
- Article headlines:
  ${articleTitles.map((t) => `- ${t}`).join("\n  ") || "- (none)"}

Article text snippet (may be truncated):
${snippet || "(no article text available)"}

Image notes for the orchestrator:
- General comment about what the lead image depicts: ${
			imageComment || "(none provided)"
		}.
- Viral hooks by aspect ratio (use the one matching the requested ratio ${
			ratio || "unspecified"
		}):
  ${
		imageBriefs.length
			? imageBriefs
					.map(
						(b) =>
							`- ${b.aspectRatio}: ${b.visualHook}${
								b.emotion ? " | emotion: " + b.emotion : ""
							}`
					)
					.join("\n  ")
			: "- (no hooks provided)"
	}

Images:
The FIRST attached image is imageIndex 0, the second is 1, etc.
The video engine will receive an upscaled, cropped version of these photos (via Cloudinary),
but it is still the same real shot and real people.
- There are ${imgCount} curated photos already cropped for ${ratio}; rotate through them so each one gets used before any repeat (unless there are fewer segments than photos).

Your job:
1) Write the voice-over script for each segment.
2) Decide which imageIndex to animate for each segment.
3) ${
			forceStaticVisuals
				? "We will only use static photos with gentle camera movement (no AI video generation)."
				: 'For each segment, write one concise "runwayPrompt" telling a video model how to animate THAT exact real photo.'
		}
4) For each segment, also write a "negativePrompt" listing visual problems the video model must avoid.
5) ${runwayAnimationNote}

Critical visual rules:
- The model ALREADY SEES the real Google Trends photo. "runwayPrompt" describes motion and subtle, realistic changes.
- Do NOT change who the people are. Do not add new people that are not implied.
- Do NOT change the basic setting in a drastic way.
- NEVER mention any real person names, team names, jersey numbers, or brand names in "runwayPrompt".
- Use generic roles like "a young woman on the street", "fans in the crowd".
- EVERY segment must have clear motion: no still-photo look.
- Use camera movement (slow zoom, dolly, pan, tilt) and/or subject motion (breathing, hair moving, lights flickering).
- Physical realism: body poses must be possible; props (mics, belts, ropes) are held or touched naturally; no floating or stitched-together objects; lighting and shadows must match a single scene.
- Eyes and faces must feel alive: natural blinks, gentle gaze shifts, no jittering pupils or crossed eyes.
- Choose the imageIndex that visually matches the script beat (setting, action, subject); avoid lazy repeats.
- Use each imageIndex at most once before reusing any image.
- Keep faces human and natural, no distortion.

For each "runwayPrompt":
- Describe motion consistent with what you actually see in that attached photo.
- Explicitly include at least ONE motion verb.
${
	forceStaticVisuals
		? "- Keep motions to gentle camera moves (subtle zoom/pan) since we will not synthesize new video frames for sports topics."
		: ""
}

For each "negativePrompt":
- List defects to avoid (extra limbs, extra heads, distorted faces, lowres, pixelated, blur, heavy motion blur, watermark, logo, text overlay, static frame, no motion, gore, nsfw).

Return JSON:
{
  "segments": [
    {
      "index": 1,
      "scriptText": "spoken narration",
      "imageIndex": 0,
      "runwayPrompt": "how to animate that attached photo",
      "negativePrompt": "comma-separated list of defects to avoid"
    }
    // exactly ${segCnt} segments, index 1..${segCnt}
  ]
}
`.trim();
	} else if (
		category === "Top5" &&
		Array.isArray(top5Outline) &&
		top5Outline.length
	) {
		const outlineText = top5Outline
			.map((it) => `#${it.rank}: ${it.label || ""} - ${it.oneLine || ""}`)
			.join("\n");
		promptText = `
${baseIntro}

This is a Top 5 countdown. Outline:

${outlineText}

Latest live web context (use this to stay current and factual):
${
	liveContext.length
		? liveContext
				.slice()
				.sort((a, b) => (b.rank || 0) - (a.rank || 0))
				.map(
					(ctx) =>
						`#${ctx.rank}: ${ctx.label || ""} | ${ctx.title || ""}${
							ctx.snippet ? " - " + ctx.snippet : ""
						}`
				)
				.join("\n")
		: "- No live snippets found; avoid dated claims or speculation."
}

Fresh reference images already pulled from the web (prefer these when they fit the beat):
${
	liveImages.length
		? liveImages
				.slice()
				.sort((a, b) => (b.rank || 0) - (a.rank || 0))
				.map(
					(img) =>
						`#${img.rank}: ${img.label || ""} -> ${img.url}${
							img.source ? ` (source: ${img.source})` : ""
						}`
				)
				.join("\n")
		: "- None provided; pick new, descriptive editorial photos from current search results, not stock icons or thumbnails."
}

Rules:
- Segment 1 teases the countdown and hooks the viewer.
- Segments 2-6 correspond to ranks #5, #4, #3, #2, and #1.
- Segment ${segCnt} is reserved for the engagement outro; keep it short, question-driven, and include a friendly like/subscribe nudge.
- Each of those segments MUST start with "#5-", "#4-", "#3-", "#2-" or "#1-" before the label; keep it sounding natural.
- ALL countdown segments (#5 through #1) will be animated with Runway; each rank must have its own unique reference image.
${
	top5NeedsExtraDetail
		? "- Because the duration is 45-60 seconds, include one extra vivid descriptive clause about what makes each city unique (landmark, vibe, food, or culture) so the narration feels richer."
		: ""
}
${
	top5NeedsExtraDetail
		? "- Make the narration feel motivational and appealing, inviting the viewer to imagine visiting each place."
		: ""
}
- Visual integrity is critical: humans must look human (no warped faces or hands), food must be appetizing and clearly that dish, and every chosen photo must obviously match the ranked item.
- Every referenceImageUrl must directly show the ranked item (for food: a close-up of the dish/plating; for travel/cities: a recognisable landmark or skyline). Avoid portraits unless someone is eating that exact food. No unrelated objects or locations.
- If a fresh image URL is provided above for that rank, use it. Otherwise, search current results and pick a sharp, descriptive editorial-style photo that matches the aspect ratio ${ratio}; avoid gstatic/thumbnail URLs.
- Write the runwayPrompt to match the exact chosen photo and make the motion feel dynamic (camera move + subtle subject motion).

You must select real reference photos via search before planning the visuals; do not leave any referenceImageUrl blank.

For each segment, output:
- "index"
- "scriptText"
- "runwayPrompt": a vivid scene description to generate, explicitly tailored to the requested aspect ratio ${ratio}. Include camera motion and subject motion and keep it photorealistic.
- "negativePrompt": comma-separated defects to avoid.
- "overlayText": 4-7 word on-screen text that fits the aspect ratio ${ratio} and stays perfectly in sync with the voiceover line for that segment.
- "referenceImageUrl": a DIRECT URL to a high-quality photo online that matches the script beat and aspect ratio ${ratio}. You MUST pick one unique, real, editorial-style photo per rank via search (no AI, no blanks, no repeats, no logos).

Visual rules:
- Realistic scenes; no logos or trademarks.
- Clear focal subject, good lighting.
- Physical realism: body poses must be possible; props (mics, gear, objects) are held or touched naturally; no floating or stitched-together objects; lighting and shadows must match a single scene.
- Eyes and faces must feel alive: natural blinks, gentle gaze shifts, no jittering pupils or crossed eyes.
- Avoid trademarks and logos; use generic jerseys and arenas.
- If people are visible, faces must be natural, no distortion.
- EVERY runwayPrompt must include explicit motion.
- Do NOT mention real names or brands in "runwayPrompt"; use roles.
- Keep overlays concise so they fit safely within the frame for ${ratio}.
- Make the vibe fun and energetic; imagine upbeat Top 5 YouTube Shorts.

"negativePrompt":
- Include extra limbs, extra heads, mutated/fused fingers, broken joints, twisted necks, distorted faces, lowres, pixelated, blur, out of focus, heavy motion blur, overexposed, underexposed, watermark, logo, text overlay, static frame, no motion, gore, nsfw.

Return JSON:
{
  "segments": [
    { "index": 1, "scriptText": "...", "runwayPrompt": "...", "negativePrompt": "...", "overlayText": "...", "referenceImageUrl": "https://..." },
    ...
  ]
}
`.trim();
	} else {
		promptText = `
${baseIntro}

No reliable Google Trends images are available.

You must imagine the visuals from scratch. For each segment output:
- "index"
- "scriptText"
- "runwayPrompt"
- "negativePrompt"
- ${runwayAnimationNote}

Visual rules:
- Realistic, grounded scenes.
- Clear focal subject, good lighting.
- Physical realism: body poses must be possible; props (mics, tools, objects) are held or touched naturally; no floating or stitched-together objects; lighting and shadows must match a single scene.
- Eyes and faces must feel alive: natural blinks, gentle gaze shifts, no jittering pupils or crossed eyes.
- Avoid trademarks and logos; use generic jerseys and arenas.
- If people are visible, faces must be natural, no distortion.
- EVERY runwayPrompt must include explicit motion.
- Do NOT mention real names or brands in "runwayPrompt"; use roles.

"negativePrompt":
- Include extra limbs, extra heads, mutated/fused fingers, broken joints, twisted necks, distorted faces, lowres, pixelated, blur, out of focus, heavy motion blur, overexposed, underexposed, watermark, logo, text overlay, static frame, no motion, gore, nsfw.

Return JSON:
{
  "segments": [
    { "index": 1, "scriptText": "...", "runwayPrompt": "...", "negativePrompt": "..." },
    ...
  ]
}
`.trim();
	}

	const contentParts = [{ type: "text", text: promptText }];
	if (hasImages) {
		images.forEach((url) => {
			contentParts.push({
				type: "image_url",
				image_url: { url },
			});
		});
	}

	const { choices } = await openai.chat.completions.create({
		model: CHAT_MODEL,
		messages: [{ role: "user", content: contentParts }],
	});

	const raw = strip(choices[0].message.content);
	let plan;
	try {
		plan = JSON.parse(raw);
	} catch (e) {
		console.error("[GPT] plan JSON parse failed:", raw);
		throw new Error("GPT video plan JSON malformed");
	}

	if (!Array.isArray(plan.segments) || plan.segments.length !== segCnt) {
		throw new Error(
			`GPT plan returned ${
				plan.segments?.length || 0
			} segments, expected ${segCnt}`
		);
	}

	// First pass: normalize text fields and carry raw imageIndex
	let segments = plan.segments.map((s, idx) => {
		const runwayPrompt = String(s.runwayPrompt || "").trim();
		const negativePromptRaw = String(s.negativePrompt || "").trim();

		return {
			index: typeof s.index === "number" ? s.index : idx + 1,
			scriptText: String(s.scriptText || "").trim(),
			runwayPrompt,
			runwayNegativePrompt: negativePromptRaw,
			overlayText: String(s.overlayText || s.overlay || "").trim(),
			referenceImageUrl: String(s.referenceImageUrl || "").trim(),
			imageIndex:
				typeof s.imageIndex === "number" && Number.isInteger(s.imageIndex)
					? s.imageIndex
					: null,
		};
	});

	// Second pass: enforce sane, non-redundant image usage
	if (hasImages) {
		const imgCount = images.length;

		// Normalize invalid indices to null
		segments = segments.map((seg) => {
			let imgIdx =
				typeof seg.imageIndex === "number" && Number.isInteger(seg.imageIndex)
					? seg.imageIndex
					: null;
			if (imgIdx === null || imgIdx < 0 || imgIdx >= imgCount) {
				imgIdx = null;
			}
			return { ...seg, imageIndex: imgIdx };
		});

		const validIndexes = segments
			.map((s) => s.imageIndex)
			.filter((v) => v !== null);
		const distinctCount = new Set(validIndexes).size;
		const desiredDistinct = Math.min(imgCount, segments.length, 5);

		// If GPT barely used images, force round-robin to cover the curated pool
		if (!validIndexes.length || distinctCount < desiredDistinct) {
			segments = segments.map((seg, idx) => ({
				...seg,
				imageIndex: idx % imgCount,
			}));
		} else {
			// Keep GPT's valid choices, fill nulls with round-robin
			let rr = 0;
			segments = segments.map((seg) => {
				if (seg.imageIndex !== null) return seg;
				const imgIdx = rr % imgCount;
				rr += 1;
				return { ...seg, imageIndex: imgIdx };
			});
		}
	} else {
		segments = segments.map((seg) => ({ ...seg, imageIndex: null }));
	}

	return { segments };
}

/* ---------------------------------------------------------------
 *  Main controller – createVideo
 * ------------------------------------------------------------- */
exports.createVideo = async (req, res) => {
	const { category, ratio: ratioIn, duration: durIn } = req.body;

	if (!category || !YT_CATEGORY_MAP[category])
		return res.status(400).json({ error: "Bad category" });
	if (!VALID_RATIOS.includes(ratioIn))
		return res.status(400).json({ error: "Bad ratio" });
	if (!goodDur(durIn)) return res.status(400).json({ error: "Bad duration" });

	const ratio = ratioIn;
	const duration = +durIn;

	/* SSE bootstrap */
	res.setHeader("Content-Type", "text/event-stream");
	res.setHeader("Cache-Control", "no-cache");
	res.setHeader("Connection", "keep-alive");
	res.setHeader("X-Accel-Buffering", "no");
	if (typeof res.flushHeaders === "function") res.flushHeaders();

	const history = [];
	const sendPhase = (phase, extra = {}) => {
		const safe =
			phase === "COMPLETED" && extra.phases
				? { ...extra, phases: JSON.parse(JSON.stringify(extra.phases)) }
				: extra;
		res.write(`data:${JSON.stringify({ phase, extra: safe })}\n\n`);
		if (typeof res.flush === "function") res.flush();
		history.push({ phase, ts: Date.now(), extra: safe });
	};
	const sendErr = (m) => {
		sendPhase("ERROR", { msg: m });
		try {
			if (!res.headersSent) {
				res.status(500).json({ error: m });
			}
		} catch {}
		try {
			res.end();
		} catch {}
	};

	sendPhase("INIT");
	console.log("[Phase] INIT ? Starting pipeline");
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
		const language = (langIn || DEFAULT_LANGUAGE).trim();
		const country =
			countryIn && countryIn.toLowerCase() !== "all countries"
				? countryIn.trim()
				: "US";
		const customPrompt = customPromptRaw.trim();
		const useSora = toBool(useSoraIn); // reuse frontend flag: true => allow Runway clips

		console.log(
			`[Job] user=${user.email}  cat=${category}  dur=${duration}s  geo=${country}  useRunway=${useSora}`
		);

		// Preload recent topics for this user/category (last 3 days) to avoid duplicates
		const threeDaysAgo = dayjs().subtract(3, "day").toDate();
		const recentVideos = await Video.find({
			user: user._id,
			category,
			createdAt: { $gte: threeDaysAgo },
		}).select("topic seoTitle");
		const normRecent = [];
		for (const v of recentVideos) {
			const base = String(v.topic || v.seoTitle || "").trim();
			if (!base) continue;
			const normFull = base.toLowerCase().replace(/\s+/g, " ").trim();
			if (normFull) normRecent.push(normFull);
			const firstTwo = normFull.split(" ").slice(0, 2).join(" ");
			if (firstTwo && firstTwo.length >= 4) normRecent.push(firstTwo);
			const firstThree = normFull.split(" ").slice(0, 3).join(" ");
			if (firstThree && firstThree.length >= 6) normRecent.push(firstThree);
		}
		const usedTopics = new Set(normRecent);

		let topic = "";
		let trendStory = null;
		let trendArticleText = null;

		const userOverrides = Boolean(videoImage) || customPrompt.length > 0;

		// 1) Try Trends story first (no Top5, no custom overrides)
		if (!userOverrides && category !== "Top5") {
			trendStory = await fetchTrendingStory(
				category,
				country,
				usedTopics,
				language
			);
			if (trendStory && trendStory.title) {
				topic = trendStory.title;
				console.log(`[Trending] candidate topic="${topic}"`);
				// mark this as used so later fallbacks don't duplicate
				usedTopics.add(topic);
			}
		}

		// 2) Custom prompt fallback
		if (customPrompt && !topic) {
			try {
				topic = await topicFromCustomPrompt(customPrompt);
			} catch {
				/* fallback below */
			}
		}

		// 3) Generic GPT trending fallback
		if (!topic) {
			if (category === "Top5") {
				const remaining = ALL_TOP5_TOPICS.filter((t) => !usedTopics.has(t));
				topic = remaining.length ? remaining[0] : choose(ALL_TOP5_TOPICS);
			} else {
				const list = await pickTrendingTopicFresh(category, language, country);
				topic = list.find((t) => !usedTopics.has(t)) || list[0];
			}
		}

		console.log(`[Job] final topic="${topic}"`);
		const topicIsAITopic = looksLikeAITopic(topic);

		// Scrape a bit of article text for richer context, if we have a Trends story
		if (trendStory && trendStory.articles && trendStory.articles.length) {
			trendArticleText = await scrapeArticleText(
				trendStory.articles[0].url || null
			);
		}

		/* 2. Segment timing */
		const requestedTailSeconds = computeEngagementTail(duration);
		const tolerancePadSeconds =
			computeOptionalOutroTolerance(requestedTailSeconds);
		console.log(
			"[Job] ratio + target duration",
			JSON.stringify({
				ratio,
				durationCore: duration,
				tailSeconds: requestedTailSeconds,
				tolerancePadSeconds,
			})
		);
		let segLens = computeInitialSegLens(
			category,
			duration,
			requestedTailSeconds,
			tolerancePadSeconds
		);
		let segCnt = segLens.length;
		const engagementTailSeconds = segLens[segCnt - 1];
		const totalDurationTarget = segLens.reduce((a, b) => a + b, 0);
		const segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));
		if (segWordCaps.length) {
			const lastIdx = segWordCaps.length - 1;
			segWordCaps[lastIdx] = Math.max(segWordCaps[lastIdx], MIN_OUTRO_WORDS);
		}
		console.log("[Timing] initial segment lengths", {
			segLens,
			segWordCaps,
			totalDurationTarget,
		});

		/* 3. Top-5 outline */
		let top5Outline = null;
		let top5LiveContext = [];
		let top5ImagePool = [];
		let top5Slug = safeSlug(topic || "top5");

		/* 4. Search & upload Trends images to Cloudinary (single target ratio) */
		let trendImagePairs = []; // [{ originalUrl, cloudinaryUrl }]
		let trendImagesForRatio = [];
		if (!userOverrides && category !== "Top5") {
			const articleLinks =
				trendStory && Array.isArray(trendStory.articles)
					? trendStory.articles.map((a) => a.url).filter(Boolean)
					: [];
			trendImagesForRatio = await fetchHighQualityImagesForTopic({
				topic,
				ratio,
				articleLinks,
				desiredCount: 7,
				limit: 16,
			});
			trendImagesForRatio = filterUploadCandidates(trendImagesForRatio, 7);
		}
		const canUseTrendsImages =
			category !== "Top5" &&
			!userOverrides &&
			trendStory &&
			trendImagesForRatio.length > 0;

		if (canUseTrendsImages) {
			const slugBase = topic
				.toLowerCase()
				.replace(/[^\w]+/g, "_")
				.replace(/^_+|_+$/g, "")
				.slice(0, 40);
			const uploadLimit = Math.min(6, trendImagesForRatio.length); // 5 needed + 1 backup
			for (let i = 0; i < uploadLimit; i++) {
				const url = trendImagesForRatio[i];
				try {
					const up = await uploadTrendImageToCloudinary(
						url,
						ratio,
						`aivideomatic/trend_seeds/${slugBase}_${i}`
					);
					trendImagePairs.push({
						originalUrl: url,
						cloudinaryUrl: up.url,
					});
				} catch (e) {
					console.warn("[Cloudinary] upload failed ?", e.message);
				}
			}
			if (!trendImagePairs.length) {
				console.warn(
					"[Cloudinary] All Trends uploads failed - falling back to prompt-only mode"
				);
			} else {
				console.log("[Cloudinary] Trends images uploaded", {
					count: trendImagePairs.length,
					requested: Math.min(trendImagesForRatio.length, 6),
					ratio,
				});
				if (trendImagePairs.length < 5) {
					console.warn(
						"[Cloudinary] Fewer than 5 ratio-matched images uploaded; will reuse where needed."
					);
				}
			}
		}

		let hasTrendImages = trendImagePairs.length > 0;
		const forceStaticVisuals =
			!useSora || isSportsTopic(topic, category, trendStory);

		let segments;
		if (category === "Top5") {
			const built = await buildTop5SegmentsAndImages({
				topic,
				ratio,
				language,
				segLens,
			});
			segments = built.segments;
			trendImagePairs = built.trendImagePairs;
			hasTrendImages = true;
			console.log("[Top5] New flow built segments & images", {
				segments: segments.length,
				images: trendImagePairs.length,
				ratio,
			});
			segments = await punchUpTop5Scripts(segments, segWordCaps);
		} else {
			/* 5. Let OpenAI orchestrate segments + visuals */
			console.log("[GPT] building full video plan …");

			const plan = await buildVideoPlanWithGPT({
				topic,
				category,
				language,
				duration,
				segLens,
				trendStory: hasTrendImages ? trendStory : null,
				trendImagesForPlanning: hasTrendImages
					? trendImagePairs.map((p) => p.cloudinaryUrl)
					: null,
				articleText: trendArticleText,
				top5Outline,
				top5LiveContext,
				top5ImagePool,
				ratio,
				trendImageBriefs: trendStory?.viralImageBriefs || [],
				engagementTailSeconds,
				country,
				forceStaticVisuals,
			});

			segments = plan.segments;

			console.log("[GPT] buildVideoPlanWithGPT ? plan ready", {
				segments: segments.length,
				hasImages: hasTrendImages,
			});

			if (Array.isArray(segments)) {
				segments = segments
					.slice()
					.sort(
						(a, b) =>
							(typeof a.index === "number" ? a.index : 0) -
							(typeof b.index === "number" ? b.index : 0)
					);
			}
		}

		// Tighten narration to fit word caps
		await Promise.all(
			segments.map((s, i) =>
				s.scriptText.trim().split(/\s+/).length <= segWordCaps[i]
					? s
					: (async () => {
							const ask = `
Rewrite the following narration in active voice.
Keep all important facts, remove filler.
Maximum ${segWordCaps[i]} words.
One or two sentences only.

"${s.scriptText}"
`.trim();
							const { choices } = await openai.chat.completions.create({
								model: CHAT_MODEL,
								messages: [{ role: "user", content: ask }],
							});
							s.scriptText = choices[0].message.content.trim();
					  })()
			)
		);

		segments = segments.map((seg) => ({
			...seg,
			scriptText: sanitizeAudienceFacingText(seg.scriptText, {
				allowAITopic: topicIsAITopic,
			}),
			overlayText: sanitizeAudienceFacingText(seg.overlayText, {
				allowAITopic: topicIsAITopic,
			}),
		}));

		if (segments.length) {
			const tailCap = Math.floor(segLens[segLens.length - 1] * WORDS_PER_SEC);
			const lastIdx = segments.length - 1;
			segments[lastIdx] = {
				...segments[lastIdx],
				scriptText: enforceEngagementOutroText(segments[lastIdx].scriptText, {
					topic,
					wordCap: tailCap,
					category,
				}),
			};
		}

		const fullScript = segments.map((s) => s.scriptText.trim()).join(" ");
		const recomputed = recomputeSegmentDurationsFromScript(
			segments,
			totalDurationTarget
		);
		if (recomputed && recomputed.length === segLens.length) {
			console.log("[Timing] Recomputed segment durations from script:", {
				before: segLens,
				after: recomputed,
			});
			segLens = recomputed;
		} else {
			const sum = segLens.reduce((a, b) => a + b, 0);
			const delta = +(totalDurationTarget - sum).toFixed(2);
			console.log("[Timing] Using planned segment durations", {
				segLens,
				totalDurationTarget,
				sum,
				delta,
			});
		}
		segCnt = segLens.length;

		// Final pass: enforce exact total duration target by nudging last segment if needed
		{
			const sum = segLens.reduce((a, b) => a + b, 0);
			const delta = +(totalDurationTarget - sum).toFixed(2);
			if (Math.abs(delta) >= 0.05 && segLens.length) {
				segLens[segLens.length - 1] = +(
					segLens[segLens.length - 1] + delta
				).toFixed(2);
				console.log("[Timing] Adjusted last segment to hit target total", {
					segLens,
					totalDurationTarget,
				});
			}
		}

		/* 6. Global style, SEO title, tags */
		let globalStyle = "";
		try {
			const g = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [
					{
						role: "user",
						content: `Give one short cinematic style phrase describing the visual mood, camera movement, and pacing for the video topic "${topic}". Do NOT include any real person names, team names, or brand names in this phrase.`,
					},
				],
			});
			globalStyle = g.choices[0].message.content
				.replace(/^[-–•\s]+/, "")
				.trim();
		} catch (e) {
			console.warn("[GPT] global style generation failed ?", e.message);
		}

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
			const snippet = trendArticleText ? trendArticleText.slice(0, 800) : "";
			seoTitle = await generateSeoTitle(
				seedHeadlines,
				category,
				language,
				snippet
			);
		} catch (e) {
			console.warn("[SEO title] generation outer failed ?", e.message);
		}
		if (!seoTitle) seoTitle = fallbackSeoTitle(topic, category);

		const descResp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [
				{
					role: "user",
					content: `Write a YouTube description (at most 150 words) for the video titled "${seoTitle}". Make the first 2 lines keyword-rich so they rank in search; include the core query (time/date/how to watch/card/lineup/etc. as appropriate). Use short sentences, no fluff. Add 1 quick CTA. End with 5-7 relevant, high-volume hashtags.`,
				},
			],
		});
		const seoDescriptionRaw = `${MERCH_INTRO}${descResp.choices[0].message.content.trim()}\n\n${BRAND_CREDIT}`;
		const seoDescription = ensureClickableLinks(seoDescriptionRaw);

		let tags = ["shorts"];
		try {
			const tagResp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [
					{
						role: "user",
						content: `Return a JSON array of 5-8 SHORT tags for the YouTube video "${seoTitle}". Use high-volume search terms viewers actually type (1-3 words each). No hashtags, no duplicates.`,
					},
				],
			});
			const parsed = JSON.parse(strip(tagResp.choices[0].message.content));
			if (Array.isArray(parsed)) tags.push(...parsed);
		} catch (e) {
			console.warn("[Tags] generation failed ?", e.message);
		}
		if (category === "Top5") tags.unshift("Top5");
		if (!tags.includes(BRAND_TAG)) tags.unshift(BRAND_TAG);
		tags = [...new Set(tags)];

		/* 7. Load last ElevenLabs voice (avoid repetition) */
		let lastVoiceMeta = null;
		let avoidVoiceIds = [];
		try {
			lastVoiceMeta = await Video.findOne({
				user: user._id,
				"elevenLabsVoice.voiceId": { $exists: true },
			})
				.sort({ createdAt: -1 })
				.select("elevenLabsVoice category language");
			if (lastVoiceMeta?.elevenLabsVoice?.voiceId) {
				avoidVoiceIds.push(lastVoiceMeta.elevenLabsVoice.voiceId);
				console.log("[TTS] Last used ElevenLabs voice", {
					voiceId: lastVoiceMeta.elevenLabsVoice.voiceId,
					language: lastVoiceMeta.language,
					category: lastVoiceMeta.category,
				});
			}
		} catch (e) {
			console.warn(
				"[TTS] Unable to load last ElevenLabs voice metadata ?",
				e.message
			);
		}

		/* 8. Dynamic ElevenLabs voice selection */
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
			console.warn("[TTS] Voice selection failed ?", e.message);
		}

		/* 9. Background music */
		let music = null;
		let voiceGain = 1.4;
		let musicGain = 0.12;
		let musicPlan = null;
		let backgroundMusicMeta = null;
		let jamendoUrl = null;
		let jamendoSearchUsed = null;
		let jamendoSearchTermsTried = [];

		try {
			musicPlan = await planBackgroundMusic(category, language, fullScript);
			if (musicPlan) {
				console.log("[MusicPlan] planned", musicPlan);
				if (typeof musicPlan.voiceGain === "number") {
					voiceGain = Math.min(1.8, Math.max(1.1, musicPlan.voiceGain));
				}
				if (typeof musicPlan.musicGain === "number") {
					musicGain = Math.min(0.25, Math.max(0.06, musicPlan.musicGain));
				}
			}
		} catch (e) {
			console.warn("[MusicPlan] planning failed ?", e.message);
		}

		try {
			const searchTerms = [];
			if (musicPlan?.jamendoSearch) {
				searchTerms.push(musicPlan.jamendoSearch);
			}
			if (Array.isArray(musicPlan?.fallbackSearchTerms)) {
				musicPlan.fallbackSearchTerms.forEach((t) => {
					if (t && typeof t === "string") searchTerms.push(t);
				});
			}
			if (!searchTerms.length) {
				searchTerms.push(
					topic.split(" ")[0],
					`${category.toLowerCase()} instrumental`,
					"ambient instrumental no vocals"
				);
			}

			jamendoSearchTermsTried = searchTerms.slice();
			let jamUrl = null;
			let usedSearch = null;
			for (const term of searchTerms) {
				if (!term) continue;
				const u = await jamendo(term);
				if (u) {
					jamUrl = u;
					usedSearch = term;
					break;
				}
			}

			if (jamUrl) {
				console.log("[Music] Jamendo match", usedSearch);
				jamendoUrl = jamUrl;
				jamendoSearchUsed = usedSearch;
				music = tmpFile("bg", ".mp3");
				const ws = fs.createWriteStream(music);
				const { data } = await axios.get(jamUrl, { responseType: "stream" });
				await new Promise((r, j) =>
					data.pipe(ws).on("finish", r).on("error", j)
				);
			}
		} catch (e) {
			console.warn("[Music] Jamendo failed ?", e.message);
		}

		if (musicPlan || jamendoUrl || jamendoSearchTermsTried.length) {
			backgroundMusicMeta = {
				plan: musicPlan || null,
				jamendoUrl: jamendoUrl || null,
				searchTerm:
					jamendoSearchUsed ||
					(musicPlan ? musicPlan.jamendoSearch : null) ||
					null,
				searchTermsTried: jamendoSearchTermsTried,
				voiceGain,
				musicGain,
			};
		}

		/* 10. Per-segment video generation */
		const clips = [];
		sendPhase("GENERATING_CLIPS", {
			msg: "Generating clips",
			total: segCnt,
			done: 0,
		});
		console.log("[Phase] GENERATING_CLIPS ? Generating clips", {
			segCnt,
			ratio,
			hasTrendImages,
			trendImages: trendImagePairs.length,
		});

		const runwaySafetyBans = new Set(); // image indexes that hit Runway safety and should not be retried
		const staticOnlyImages = new Set(); // images we will use only as static fallback going forward
		const staticFallbackUsed = new Set(); // track which images already used as static fallback to avoid repeats until all tried
		let firstSafetyBan = null;
		const allowRunway = useSora && !forceStaticVisuals;

		for (let i = 0; i < segCnt; i++) {
			const d = segLens[i];
			const seg = segments[i];
			const segIndex = i + 1;

			const rw = Math.max(5, Math.min(10, Math.round(d)));
			const { promptText, negativePrompt } = buildRunwayPrompt(
				seg,
				globalStyle
			);
			const canUseRunway =
				allowRunway &&
				hasTrendImages &&
				seg.imageIndex !== null &&
				(category === "Top5" || i < 2);

			console.log(
				`[Seg ${segIndex}/${segCnt}] targetDuration=${d.toFixed(
					2
				)}s runwayDuration=${rw}s useRunway=${canUseRunway ? "yes" : "no"}`
			);

			let clipPath = null;

			if (forceStaticVisuals && hasTrendImages && seg.imageIndex !== null) {
				const imgCount = trendImagePairs.length;
				const baseIdx =
					seg.imageIndex >= 0 && seg.imageIndex < imgCount ? seg.imageIndex : 0;

				let idx;
				if (category === "Top5") {
					// Keep countdown segments locked to their intended images (no round-robin)
					idx = baseIdx;
				} else {
					const unused = Array.from({ length: imgCount }, (_, k) => k).find(
						(k) => !staticFallbackUsed.has(k)
					);
					idx = unused !== undefined ? unused : baseIdx;
				}

				staticFallbackUsed.add(idx);
				const pair =
					trendImagePairs[idx] ||
					trendImagePairs[baseIdx] ||
					trendImagePairs[0] ||
					null;
				const imgUrlCloudinary = pair?.cloudinaryUrl;
				// Prefer Cloudinary-normalised image; fall back to raw only if missing
				const imgUrlOriginal = imgUrlCloudinary || pair?.originalUrl;
				try {
					clipPath = await generateStaticClipFromImage({
						segmentIndex: segIndex,
						imgUrlOriginal,
						imgUrlCloudinary,
						ratio,
						targetDuration: d,
						zoomPan: true,
					});
				} catch (err) {
					console.warn(
						`[Seg ${segIndex}] Static clip failed, using placeholder`,
						err.message
					);
					clipPath = await generatePlaceholderClip({
						segmentIndex: segIndex,
						ratio,
						targetDuration: d,
					});
				}
			} else if (canUseRunway) {
				const candidatesIdx = [];
				const baseIdx =
					seg.imageIndex >= 0 && seg.imageIndex < trendImagePairs.length
						? seg.imageIndex
						: 0;
				for (let k = 0; k < trendImagePairs.length; k++) {
					const idx = (baseIdx + k) % trendImagePairs.length;
					if (!candidatesIdx.includes(idx)) candidatesIdx.push(idx);
				}
				const runwayCandidates =
					category === "Top5"
						? [baseIdx] // lock intro/outro and ranked segments to their intended images
						: candidatesIdx.filter((idx) => !staticOnlyImages.has(idx));

				console.log("[Runway] prompt preview", {
					segment: segIndex,
					promptPreview: promptText.slice(0, 160),
					hasTrendImage: true,
					candidates: runwayCandidates.length,
					staticOnly: Array.from(staticOnlyImages),
				});

				let safetyTriggered = false;
				for (const idx of runwayCandidates) {
					const pair = trendImagePairs[idx];
					if (!pair || !pair.cloudinaryUrl) continue;
					try {
						clipPath = await generateItvClipFromImage({
							segmentIndex: segIndex,
							imgUrl: pair.cloudinaryUrl,
							promptText,
							negativePrompt,
							ratio,
							runwayDuration: rw,
						});
						break;
					} catch (e) {
						const msg = String(e?.message || "");
						const failureCode =
							e?.response?.data?.failureCode || e?.response?.data?.code || "";
						const isSafety =
							/SAFETY/i.test(msg) ||
							/SAFETY/i.test(String(failureCode || "")) ||
							/HUMAN/i.test(String(failureCode || ""));
						safetyTriggered = safetyTriggered || isSafety;
						if (isSafety) {
							runwaySafetyBans.add(idx);
							staticOnlyImages.add(idx);
							if (firstSafetyBan === null) firstSafetyBan = idx;
						}
						console.warn(
							`[Seg ${segIndex}] Runway image_to_video failed for image #${idx}`,
							msg
						);
						continue;
					}
				}

				if (!clipPath) {
					const unusedStatic = candidatesIdx.filter(
						(idx) => !staticFallbackUsed.has(idx)
					);
					const fallbackIdx =
						category === "Top5"
							? baseIdx // keep intro/outro unique and countdown aligned
							: unusedStatic.find((idx) => staticOnlyImages.has(idx)) ??
							  unusedStatic.find((idx) => runwaySafetyBans.has(idx)) ??
							  unusedStatic[0] ??
							  firstSafetyBan ??
							  candidatesIdx[0] ??
							  baseIdx;
					staticFallbackUsed.add(fallbackIdx);
					const pair =
						trendImagePairs[fallbackIdx] || trendImagePairs[0] || null;
					const imgUrlCloudinary = pair?.cloudinaryUrl;
					const imgUrlOriginal = imgUrlCloudinary || pair?.originalUrl;
					try {
						clipPath = await generateStaticClipFromImage({
							segmentIndex: segIndex,
							imgUrlOriginal,
							imgUrlCloudinary,
							ratio,
							targetDuration: d,
						});
					} catch (err) {
						console.warn(
							`[Seg ${segIndex}] Static fallback failed after Runway, using placeholder`,
							err.message
						);
						clipPath = await generatePlaceholderClip({
							segmentIndex: segIndex,
							ratio,
							targetDuration: d,
						});
					}
				}
			} else if (hasTrendImages) {
				const baseIdx =
					Number.isInteger(seg.imageIndex) &&
					seg.imageIndex >= 0 &&
					seg.imageIndex < trendImagePairs.length
						? seg.imageIndex
						: 0;
				const pair = trendImagePairs[baseIdx] || trendImagePairs[0] || null;
				const imgUrlCloudinary = pair?.cloudinaryUrl;
				const imgUrlOriginal = imgUrlCloudinary || pair?.originalUrl;

				if (imgUrlOriginal || imgUrlCloudinary) {
					try {
						clipPath = await generateStaticClipFromImage({
							segmentIndex: segIndex,
							imgUrlOriginal,
							imgUrlCloudinary,
							ratio,
							targetDuration: d,
							zoomPan: true,
						});
					} catch (err) {
						console.warn(
							`[Seg ${segIndex}] Static clip failed, using placeholder`,
							err.message
						);
						clipPath = await generatePlaceholderClip({
							segmentIndex: segIndex,
							ratio,
							targetDuration: d,
						});
					}
				}
			} else {
				console.log("[Runway] prompt preview", {
					segment: segIndex,
					promptPreview: promptText.slice(0, 160),
					hasTrendImage: false,
				});
				clipPath = await generatePlaceholderClip({
					segmentIndex: segIndex,
					ratio,
					targetDuration: d,
				});
			}

			if (!clipPath) {
				clipPath = await generatePlaceholderClip({
					segmentIndex: segIndex,
					ratio,
					targetDuration: d,
				});
			}

			const fixed = tmpFile(`fx_${segIndex}`, ".mp4");
			await exactLen(clipPath, d, fixed, { ratio, enhance: true });
			if (category === "Top5" && seg.countdownRank) {
				const withSlate = tmpFile(`slate_${segIndex}`, ".mp4");
				try {
					await overlayCountdownSlate({
						src: fixed,
						out: withSlate,
						ratio,
						rank: seg.countdownRank,
						label:
							seg.countdownLabel ||
							stripCountdownPrefix(seg.overlayText || "") ||
							"",
						displaySeconds: 2,
					});
					try {
						fs.unlinkSync(fixed);
					} catch {}
					clips.push(withSlate);
				} catch (e) {
					console.warn(
						`[Top5] Slate overlay failed for segment ${segIndex} ?`,
						e.message
					);
					clips.push(fixed);
				}
			} else {
				clips.push(fixed);
			}
			try {
				fs.unlinkSync(clipPath);
			} catch {}

			sendPhase("GENERATING_CLIPS", {
				msg: `Rendering segment ${segIndex}/${segCnt}`,
				total: segCnt,
				done: segIndex,
			});
			console.log("[Phase] GENERATING_CLIPS ? Rendering segment", segIndex);
		}

		/* 11. Concatenate silent video */
		sendPhase("ASSEMBLING_VIDEO", {
			msg: "Blending clips with cinematic transitions...",
		});
		console.log("[Phase] ASSEMBLING_VIDEO + Blending clips with transitions");

		let silent;
		try {
			silent = await concatWithTransitions(clips, segLens, ratio, 0.5, {
				maxFadeFraction: 0.14,
				minFadeSeconds: 0.15,
			});
		} catch (err) {
			console.warn(
				"[Transitions] Fade pipeline failed, falling back to direct concat:",
				err && err.stack ? err.stack : err?.message || err
			);
			const listFile = tmpFile("list", ".txt");
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
			try {
				fs.unlinkSync(listFile);
			} catch {}
		}
		clips.forEach((p) => {
			try {
				fs.unlinkSync(p);
			} catch {}
		});
		const silentFixed = tmpFile("silent_fix", ".mp4");
		await exactLen(silent, totalDurationTarget, silentFixed, {
			ratio,
			enhance: false,
		});
		try {
			fs.unlinkSync(silent);
		} catch {}

		/* 12. Voice-over & music */
		sendPhase("ADDING_VOICE_MUSIC", { msg: "Creating audio layer" });
		console.log("[Phase] ADDING_VOICE_MUSIC ? Creating audio layer");

		const fixedPieces = [];
		let voiceToneSample = null;

		for (let i = 0; i < segCnt; i++) {
			const fixed = tmpFile(`tts_fix_${i + 1}`, ".wav");
			const ttsPlan = buildTtsPartsForSegment(segments[i], category);
			const requestedParts = Array.isArray(ttsPlan.parts) ? ttsPlan.parts : [];
			const ttsParts = requestedParts
				.map((p) => String(p || "").trim())
				.filter((p) => p.length);
			if (!ttsParts.length)
				ttsParts.push(String(segments[i].scriptText || "").trim() || "Update");

			const toneText = improveTTSPronunciation(ttsParts.join(" ").trim());
			const localTone = deriveVoiceSettings(toneText, category);
			if (!voiceToneSample) voiceToneSample = localTone;

			const piecePaths = [];
			for (let pIdx = 0; pIdx < ttsParts.length; pIdx++) {
				const partText = improveTTSPronunciation(
					ttsParts[pIdx] || "Countdown update"
				);
				const partPath = tmpFile(`tts_part_${i + 1}_${pIdx + 1}`, ".mp3");

				try {
					await elevenLabsTTS(
						partText,
						language,
						partPath,
						category,
						chosenVoice?.voiceId || null
					);
				} catch (e) {
					console.warn(
						`[TTS] ElevenLabs failed for seg ${i + 1} part ${
							pIdx + 1
						}, falling back to OpenAI ?`,
						e.message
					);

					const tts = await openai.audio.speech.create({
						model: "tts-1-hd",
						voice: "shimmer",
						speed: localTone.openaiSpeed,
						input: partText,
						format: "mp3",
					});
					fs.writeFileSync(partPath, Buffer.from(await tts.arrayBuffer()));
				}
				piecePaths.push(partPath);
			}

			let raw = piecePaths[0] || tmpFile(`tts_raw_${i + 1}`, ".mp3");
			let silencePath = null;
			let concatList = null;

			if (piecePaths.length > 1) {
				if (ttsPlan.pauseSeconds > 0) {
					silencePath = tmpFile(`tts_pause_${i + 1}`, ".wav");
					await ffmpegPromise((c) =>
						c
							.input("anullsrc=r=44100:cl=mono")
							.inputOptions("-f", "lavfi")
							.outputOptions(
								"-t",
								ttsPlan.pauseSeconds.toFixed(3),
								"-ac",
								"1",
								"-ar",
								"44100",
								"-y"
							)
							.save(norm(silencePath))
					);
				}

				const concatInputs = [];
				for (let idx = 0; idx < piecePaths.length; idx++) {
					concatInputs.push(piecePaths[idx]);
					if (silencePath && idx < piecePaths.length - 1) {
						concatInputs.push(silencePath);
					}
				}

				concatList = tmpFile(`audio_list_parts_${i + 1}`, ".txt");
				fs.writeFileSync(
					concatList,
					concatInputs.map((p) => `file '${norm(p)}'`).join("\n")
				);

				raw = tmpFile(`tts_join_${i + 1}`, ".wav");
				await ffmpegPromise((c) =>
					c
						.input(norm(concatList))
						.inputOptions("-f", "concat", "-safe", "0")
						.outputOptions("-c", "copy", "-y")
						.save(norm(raw))
				);

				try {
					fs.unlinkSync(concatList);
				} catch {}
				piecePaths.forEach((p) => {
					try {
						fs.unlinkSync(p);
					} catch {}
				});
				if (silencePath) {
					try {
						fs.unlinkSync(silencePath);
					} catch {}
				}
			}

			await exactLenAudio(raw, segLens[i], fixed, {
				maxAtempo: category === "Top5" ? 1.08 : MAX_ATEMPO,
				forceTrim: category === "Top5",
			});
			try {
				if (raw && fs.existsSync(raw)) fs.unlinkSync(raw);
			} catch {}
			fixedPieces.push(fixed);
		}

		const audioConcatList = tmpFile("audio_list", ".txt");
		fs.writeFileSync(
			audioConcatList,
			fixedPieces.map((p) => `file '${norm(p)}'`).join("\n")
		);
		const ttsJoin = tmpFile("tts_join", ".wav");
		await ffmpegPromise((c) =>
			c
				.input(norm(audioConcatList))
				.inputOptions("-f", "concat", "-safe", "0")
				.outputOptions("-c", "copy", "-y")
				.save(norm(ttsJoin))
		);
		try {
			fs.unlinkSync(audioConcatList);
		} catch {}
		fixedPieces.forEach((p) => {
			try {
				fs.unlinkSync(p);
			} catch {}
		});

		const mixedRaw = tmpFile("mix_raw", ".wav");
		const mixed = tmpFile("mix_fix", ".wav");
		if (music) {
			const trim = tmpFile("trim", ".mp3");
			await ffmpegPromise((c) =>
				c
					.input(norm(music))
					.outputOptions("-t", String(totalDurationTarget), "-y")
					.save(norm(trim))
			);
			try {
				fs.unlinkSync(music);
			} catch {}

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
			try {
				fs.unlinkSync(trim);
			} catch {}
		} else {
			await ffmpegPromise((c) =>
				c
					.input(norm(ttsJoin))
					.audioFilters("volume=1.4")
					.outputOptions("-c:a", "pcm_s16le", "-y")
					.save(norm(mixedRaw))
			);
		}
		try {
			fs.unlinkSync(ttsJoin);
		} catch {}

		await exactLenAudio(mixedRaw, totalDurationTarget, mixed, {
			maxAtempo: category === "Top5" ? 1.08 : MAX_ATEMPO,
			forceTrim: category === "Top5",
		});
		try {
			fs.unlinkSync(mixedRaw);
		} catch {}

		/* 13. Mux audio + video */
		sendPhase("SYNCING_VOICE_MUSIC", { msg: "Muxing final video" });
		console.log("[Phase] SYNCING_VOICE_MUSIC ? Muxing final video");

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
					"-shortest",
					"-y"
				)
				.save(norm(finalPath))
		);
		try {
			fs.unlinkSync(silentFixed);
		} catch {}
		try {
			fs.unlinkSync(mixed);
		} catch {}

		/* 14. YouTube upload */
		let youtubeLink = "";
		let youtubeTokens = null;
		try {
			youtubeTokens = await refreshYouTubeTokensIfNeeded(user, req);
			const oauth2 = buildYouTubeOAuth2Client(youtubeTokens);
			if (oauth2) {
				const yt = google.youtube({ version: "v3", auth: oauth2 });
				const { data } = await yt.videos.insert(
					{
						part: ["snippet", "status"],
						requestBody: {
							snippet: {
								title: seoTitle,
								description: seoDescription,
								tags,
								categoryId:
									YT_CATEGORY_MAP[category] === "0"
										? "22"
										: YT_CATEGORY_MAP[category],
							},
							status: {
								privacyStatus: "public",
								selfDeclaredMadeForKids: false,
							},
						},
						media: { body: fs.createReadStream(finalPath) },
					},
					{ maxContentLength: Infinity, maxBodyLength: Infinity }
				);
				youtubeLink = `https://www.youtube.com/watch?v=${data.id}`;
				sendPhase("VIDEO_UPLOADED", { youtubeLink });
				console.log("[Phase] VIDEO_UPLOADED", youtubeLink);
			}
		} catch (e) {
			console.warn("[YouTube] upload skipped ?", e.message);
		}

		/* 15. Voice + music metadata */
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

		/* 16. Persist to Mongo */
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
			model: ITV_MODEL,
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

		/* optional scheduling */
		if (schedule) {
			const { type, timeOfDay, startDate, endDate } = schedule;

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

			const startPST = dayjs
				.tz(startDateStr, "YYYY-MM-DD", PST_TZ)
				.startOf("day");
			const endPST =
				endDate && dayjs(endDate).isValid()
					? dayjs
							.tz(dayjs(endDate).format("YYYY-MM-DD"), "YYYY-MM-DD", PST_TZ)
							.startOf("day")
					: null;

			await new Schedule({
				user: user._id,
				category,
				video: doc._id,
				scheduleType: type,
				timeOfDay,
				startDate: startPST.toDate(),
				endDate: endPST ? endPST.toDate() : undefined,
				nextRun: next.toDate(),
				active: true,
			}).save();

			doc.scheduled = true;
			await doc.save();
			sendPhase("VIDEO_SCHEDULED", { msg: "Scheduled" });
			console.log("[Phase] VIDEO_SCHEDULED");
		}

		/* 17. DONE */
		sendPhase("COMPLETED", {
			id: doc._id,
			youtubeLink,
			phases: JSON.parse(JSON.stringify(history)),
		});
		console.log("[Phase] COMPLETED", doc._id, youtubeLink);
		res.end();
	} catch (err) {
		console.error("[createVideo] ERROR", {
			message: err?.message,
			stack: err?.stack,
		});
		if (err?.response) {
			console.error(
				"[createVideo] ERROR response status:",
				err.response.status
			);
			try {
				console.error(
					"[createVideo] ERROR response data snippet:",
					typeof err.response.data === "string"
						? err.response.data.slice(0, 500)
						: JSON.stringify(err.response.data).slice(0, 500)
				);
			} catch (_) {
				console.error(
					"[createVideo] ERROR response data snippet: [unserializable]"
				);
			}
		}
		sendErr(err.message || "Internal error");
	}
};

/* expose helpers for tests / cli */
exports.buildYouTubeOAuth2Client = buildYouTubeOAuth2Client;
exports.refreshYouTubeTokensIfNeeded = refreshYouTubeTokensIfNeeded;
exports.uploadToYouTube = uploadToYouTube;

/* -------------------------------------------------------------------------- */
/*  Controller: Get All Videos for a User                                      */
/* -------------------------------------------------------------------------- */
exports.getUserVideos = async (req, res, next) => {
	try {
		const user = req.user;
		const videos = await Video.find({ user: user._id }).sort({ createdAt: -1 });
		return res
			.status(200)
			.json({ success: true, count: videos.length, data: videos });
	} catch (err) {
		console.error("[getUserVideos] error:", err);
		next(err);
	}
};

/* -------------------------------------------------------------------------- */
/*  Controller: Get Single Video by ID                                         */
/* -------------------------------------------------------------------------- */
exports.getVideoById = async (req, res, next) => {
	try {
		const { role, _id: userId } = req.user;
		const { videoId } = req.params;

		if (!mongoose.Types.ObjectId.isValid(videoId)) {
			return res.status(400).json({ error: "Invalid video ID." });
		}

		const video = await Video.findById(videoId).populate(
			"user",
			"name email role"
		);

		if (!video) {
			return res.status(404).json({ error: "Video not found." });
		}

		/* Authorisation */
		if (video.user._id.toString() !== userId.toString() && role !== "admin") {
			return res
				.status(403)
				.json({ error: "Not authorised to view this video." });
		}

		return res.status(200).json({ success: true, data: video });
	} catch (err) {
		console.error("[getVideoById] error:", err);
		next(err);
	}
};

/* -------------------------------------------------------------------------- */
/*  Controller: Update Video                                                   */
/* -------------------------------------------------------------------------- */
exports.updateVideo = async (req, res, next) => {
	try {
		const user = req.user;
		const { videoId } = req.params;
		const updates = req.body;

		if (!mongoose.Types.ObjectId.isValid(videoId)) {
			return res.status(400).json({ error: "Invalid video ID." });
		}

		const video = await Video.findById(videoId);
		if (!video) {
			return res.status(404).json({ error: "Video not found." });
		}

		// Only the owner (or admin) can update
		if (
			video.user.toString() !== user._id.toString() &&
			user.role !== "admin"
		) {
			return res
				.status(403)
				.json({ error: "Not authorized to update this video." });
		}

		// Prevent category/topic duplicates if those fields change
		if (
			(updates.category && updates.category !== video.category) ||
			(updates.topic && updates.topic !== video.topic)
		) {
			const existing = await Video.findOne({
				_id: { $ne: videoId },
				category: updates.category || video.category,
				topic: updates.topic || video.topic,
			});
			if (existing) {
				return res.status(400).json({
					error: "Another video with the same category & topic already exists.",
				});
			}
		}

		// Apply only whitelisted updates (prevent overwriting system fields)
		const allowedFields = [
			"category",
			"topic",
			"seoTitle",
			"seoDescription",
			"tags",
			"script",
			"privacy",
		];
		for (const key of Object.keys(updates)) {
			if (allowedFields.includes(key)) {
				video[key] = updates[key];
			}
		}

		await video.save();
		return res.status(200).json({ success: true, data: video });
	} catch (err) {
		console.error("[updateVideo] error:", err);
		next(err);
	}
};

/* -------------------------------------------------------------------------- */
/*  Controller: Delete Video                                                   */
/* -------------------------------------------------------------------------- */
exports.deleteVideo = async (req, res, next) => {
	try {
		const user = req.user;
		const { videoId } = req.params;

		if (!mongoose.Types.ObjectId.isValid(videoId)) {
			return res.status(400).json({ error: "Invalid video ID." });
		}

		const video = await Video.findById(videoId);
		if (!video) {
			return res.status(404).json({ error: "Video not found." });
		}

		// Only owner or admin can delete
		if (
			video.user.toString() !== user._id.toString() &&
			user.role !== "admin"
		) {
			return res
				.status(403)
				.json({ error: "Not authorized to delete this video." });
		}

		await video.remove();
		return res.status(200).json({ success: true, message: "Video deleted." });
	} catch (err) {
		console.error("[deleteVideo] error:", err);
		next(err);
	}
};

exports.listVideos = async (req, res, next) => {
	try {
		const { role, _id: userId } = req.user;

		/* Pagination params */
		let page = parseInt(req.query.page, 10) || 1;
		let limit = parseInt(req.query.limit, 10) || 20;
		if (page < 1) page = 1;
		if (limit < 1) limit = 20;
		if (limit > 100) limit = 100; // hard cap to avoid DoS

		const filter = role === "admin" ? {} : { user: userId };

		const total = await Video.countDocuments(filter);
		const pages = Math.ceil(total / limit);
		const skip = (page - 1) * limit;

		const videos = await Video.find(filter)
			.sort({ createdAt: -1 })
			.skip(skip)
			.limit(limit)
			.populate("user", "name email role"); // choose which fields you expose

		return res.status(200).json({
			success: true,
			page,
			pages,
			limit,
			count: videos.length,
			total,
			data: videos,
		});
	} catch (err) {
		console.error("[listVideos] error:", err);
		next(err);
	}
};
