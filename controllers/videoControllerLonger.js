/** @format */
/**
 * videoControllerLonger.js (DROP-IN REPLACEMENT - QUALITY + STABILITY)
 *
 * Key improvements (mapped to Amad's requirements):
 * 1) No voice stutter / no silent gaps:
 *    - Generate TTS per segment -> convert to WAV -> remove leading/trailing silence
 *    - Compute ONE global atempo factor to perfectly fill narration duration (no padding)
 *    - Avoid aresample async drift correction (removes "stutter" artifacts)
 *
 * 2) Presenter looks natural (less creepy):
 *    - Generate ONE stable Runway baseline talking-head clip (wide, calm)
 *    - Reuse/loop baseline for all segments (no per-segment Runway randomness)
 *    - Cap gestures; avoid hands; stabilize prompt
 *
 * 3) Presenter wardrobe adjustment (classy outfit):
 *    - Optional Runway presenter edit after script + thumbnail
 *    - Keeps identity/studio consistent
 *
 * 4) Camera is slightly farther away:
 *    - After lipsync, apply a subtle zoom-out with blurred background padding
 *
 * 5) Professional intro/outro structure:
 *    - Intro 2-4s with title overlay + voiced greeting (excited when appropriate)
 *    - Outro 3-6s with engagement question + like CTA
 *    - Final fade-out for a clean finish
 *
 * 6-8) Script is "spicy" (American audience) + smooth transitions:
 *    - Strong prompt guidance: conversational, punchy, not formal
 *    - Explicit transition language between segments
 *
 * 9) No empty/silent parts:
 *    - Silence removed; no apad; durations filled via global atempo
 *
 * 10) Code cleaned:
 *    - Removed unused/fragile paths (perf-ref vision, per-segment Runway by default)
 *
 * 11) Background music MUST work:
 *    - Validate chosen track has audio; try multiple Jamendo candidates
 *    - If still none and disableMusic=false => job fails with actionable message
 *
 * ENDPOINTS (unchanged):
 *   POST /api/long-video
 *   GET  /api/long-video/:jobId
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const child_process = require("child_process");
const axios = require("axios");
const dayjs = require("dayjs");
const { google } = require("googleapis");
const { OpenAI } = require("openai");
const cloudinary = require("cloudinary").v2;
const { generateThumbnailPackage } = require("../assets/thumbnailDesigner");
const {
	generatePresenterAdjustedImage,
} = require("../assets/presenterAdjustments");
const Video = require("../models/Video");
const Schedule = require("../models/Schedule");
const {
	googleTrendingCategoriesId,
	EXPLICIT_EXCITED_CUES,
	EXPLICIT_SERIOUS_CUES,
	EXPLICIT_WARM_CUES,
	EXPLICIT_THOUGHTFUL_CUES,
	SERIOUS_TONE_TOKENS,
	EXCITED_TONE_TOKENS,
	ENTERTAINMENT_KEYWORDS,
	TREND_SIGNAL_TOKENS,
	CSE_ENTERTAINMENT_QUERIES,
	TOPIC_STOP_WORDS,
	GENERIC_TOPIC_TOKENS,
	YT_CATEGORY_MAP,
} = require("../assets/utils");

const ffmpegStatic = require("ffmpeg-static");

let FormDataNode = null;
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	FormDataNode = require("form-data");
} catch {
	FormDataNode = null;
}

/* ---------------------------------------------------------------
 * ENV
 * ------------------------------------------------------------- */

const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });

const CHAT_MODEL = "gpt-5.2";
const OWNER_ONLY_USER_ID = "683e3a0329b0515ff5f7a1e1";

function isOwnerOnlyUser(req) {
	const userId = req?.user?._id || req?.user?.id || req?.userId;
	return String(userId || "") === OWNER_ONLY_USER_ID;
}

const ELEVEN_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ELEVEN_FIXED_VOICE_ID = "uKepyVD0sANZxUFnIoI2";
const ELEVEN_TTS_MODEL = "eleven_turbo_v2_5";
const ELEVEN_TTS_MODEL_FALLBACKS = String(
	"eleven_multilingual_v2,eleven_monolingual_v1"
)
	.split(",")
	.map((s) => s.trim())
	.filter(Boolean);
// TTS realism tuning (favor natural cadence, avoid over-stylization)
const ELEVEN_TTS_STABILITY = clampNumber(0.78, 0.1, 1);
const ELEVEN_TTS_SIMILARITY = clampNumber(0.94, 0.1, 1);
const ELEVEN_TTS_STYLE = clampNumber(0.12, 0, 1);
const ELEVEN_TTS_SPEAKER_BOOST = true;
const UNIFORM_TTS_VOICE_SETTINGS = true;

const RUNWAY_API_KEY = process.env.RUNWAYML_API_SECRET || "";

const RUNWAY_VERSION = "2024-11-06";
const RUNWAY_VIDEO_MODEL = "gen4_turbo";
const RUNWAY_VIDEO_MODEL_FALLBACK = "gen4_turbo";

const SYNC_SO_API_KEY = process.env.SYNC_SO_API_KEY || "";
const SYNC_SO_BASE = "https://api.sync.so";
const SYNC_SO_MODEL = "lipsync-2";
// const SYNC_SO_MODEL = "lipsync-2-pro";
const SYNC_SO_GENERATE_PATH = "/v2/generate";

const GOOGLE_CSE_ID = process.env.GOOGLE_CSE_ID || null;

const GOOGLE_CSE_KEY = process.env.GOOGLE_CSE_KEY || null;

const CLOUDINARY_ENABLED = Boolean(
	process.env.CLOUDINARY_CLOUD_NAME &&
		process.env.CLOUDINARY_API_KEY &&
		process.env.CLOUDINARY_API_SECRET
);
if (CLOUDINARY_ENABLED) {
	cloudinary.config({
		cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
		api_key: process.env.CLOUDINARY_API_KEY,
		api_secret: process.env.CLOUDINARY_API_SECRET,
	});
}

const GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1";
const WIKIPEDIA_API_BASE = "https://en.wikipedia.org/w/api.php";
const WIKIMEDIA_API_BASE = "https://commons.wikimedia.org/w/api.php";

const TRENDS_API_URL =
	process.env.TRENDS_API_URL || "http://localhost:8102/api/google-trends";
const TRENDS_HTTP_TIMEOUT_MS = 180000;
const TRENDS_HTTP_MAX_ATTEMPTS = 2;
const TRENDS_HTTP_RETRY_DELAY_MS = 5000;
const LONG_VIDEO_REQUIRE_TRENDS = true;
const LONG_VIDEO_TRENDS_GEO = "US";
const LONG_VIDEO_TRENDS_CATEGORY = "Entertainment";

function normalizeTrendsApiUrl(raw) {
	return String(raw || "")
		.trim()
		.replace(/\/+$/, "");
}

function buildTrendsApiCandidates(baseUrl) {
	const list = [];
	const add = (u) => {
		const cleaned = normalizeTrendsApiUrl(u);
		if (!cleaned) return;
		list.push(cleaned);
		if (/localhost/i.test(cleaned)) {
			list.push(cleaned.replace(/localhost/gi, "127.0.0.1"));
		}
		if (/\[::1\]/.test(cleaned)) {
			list.push(cleaned.replace(/\[::1\]/g, "127.0.0.1"));
		}
	};
	add(TRENDS_API_URL);
	if (baseUrl) add(`${String(baseUrl).replace(/\/+$/, "")}/api/google-trends`);
	return Array.from(new Set(list));
}

function delay(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function deriveTrendsServiceBase(raw) {
	const cleaned = normalizeTrendsApiUrl(raw);
	if (!cleaned) return "";
	return cleaned.replace(/\/api\/google-trends$/i, "");
}

function buildGoogleImagesApiCandidates(baseUrl) {
	const list = [];
	const bases = [
		deriveTrendsServiceBase(TRENDS_API_URL),
		String(baseUrl || "").trim(),
	].filter(Boolean);
	for (const base of bases) {
		const trimmed = base.replace(/\/+$/, "");
		list.push(`${trimmed}/api/google-images`);
		if (/localhost/i.test(trimmed)) {
			list.push(
				`${trimmed.replace(/localhost/gi, "127.0.0.1")}/api/google-images`
			);
		}
		if (/\[::1\]/.test(trimmed)) {
			list.push(
				`${trimmed.replace(/\[::1\]/g, "127.0.0.1")}/api/google-images`
			);
		}
	}
	return Array.from(new Set(list));
}

const LONG_VIDEO_YT_CATEGORY = "Entertainment";

const BRAND_TAG = "SereneJannat";
const BRAND_CREDIT = "Powered by Serene Jannat";
const CHANNEL_NAME = "Prime Time Brief";
const INTRO_OVERLAY_TEXT = "https://serenejannat.com";
const MERCH_INTRO =
	"Support the channel & customize your own merch:\n" +
	"https://serenejannat.com/our-products?category=candles/\n" +
	"https://www.serenejannat.com/custom-gifts\n" +
	"https://www.serenejannat.com/custom-gifts/67b7fb9c3d0cd90c4fc410e3\n\n";

const JAMENDO_CLIENT_ID = process.env.JAMENDO_CLIENT_ID || "";
const JAMENDO_BASE = "https://api.jamendo.com/v3.0";

const TMP_ROOT = path.join(os.tmpdir(), "agentai_long_video");
const OUTPUT_DIR = path.join(__dirname, "../uploads/videos");
const THUMBNAIL_DIR = path.join(__dirname, "../uploads/thumbnails");
const LONG_VIDEO_PERSIST_OUTPUT = false;

// Your classy suit reference (also default presenter)
const DEFAULT_PRESENTER_ASSET_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767062842/aivideomatic/long_thumbnails/MyPhotoWithASuit_s1xay4.png";
const DEFAULT_PRESENTER_MOTION_VIDEO_URL =
	"https://res.cloudinary.com/infiniteapps/video/upload/v1766438047/aivideomatic/trend_seeds/aivideomatic/trend_seeds/MyVideoToReplicate_qlwrmu.mp4";
const STUDIO_EMPTY_PROMPT =
	"Studio is empty and locked; remove any background people from the reference; no people in the background, no passersby, no background figures or silhouettes, no reflections of people, no movement behind the presenter; background must be static with no moving elements, screens, or window activity.";
const PRESENTER_MOTION_STYLE =
	"natural head and neck movement with very occasional micro-nods (not repetitive); head mostly steady; slow and controlled; avoid rhythmic bobbing; no fast turns or jerky motion; human blink rate with slight variation (every few seconds), soft eyelid closures, subtle breathing, soft micro-expressions, natural jaw movement, relaxed eyes, natural forehead movement; mouth neutral or very light smile when appropriate; no exaggerated expressions";

// Output defaults
const DEFAULT_OUTPUT_RATIO = "1280:720";
const DEFAULT_OUTPUT_FPS = 30;
const DEFAULT_SCALE_MODE = "cover";
const DEFAULT_IMAGE_SCALE_MODE = "blur";
const INTERMEDIATE_VIDEO_CRF = clampNumber(16, 12, 24);
const FINAL_VIDEO_CRF = clampNumber(15, 10, 20);
const INTERMEDIATE_PRESET = "fast";
const FINAL_PRESET = "slow";
const AUDIO_BITRATE = "256k";
const FINAL_LOUDNORM_FILTER = "loudnorm=I=-16:TP=-1.0:LRA=11";
const FINAL_MASTER_MAX_HEIGHT = 2160;
const FINAL_MASTER_MIN_HEIGHT = 1080;
const FINAL_GOP_SECONDS = 2;
const FINAL_COLOR_SPACE = "bt709";
const FINAL_COLOR_RANGE = "tv";
const WATERMARK_TEXT = "https://serenejannat.com";
const WATERMARK_FONT_SIZE_PCT = 0.042;
const WATERMARK_MARGIN_PCT = 0.035;
const WATERMARK_OPACITY = 0.55;
const WATERMARK_SHADOW_OPACITY = 0.3;
const WATERMARK_SHADOW_PX = 2;
const CSE_PREFERRED_IMG_SIZE = "xlarge";
const CSE_FALLBACK_IMG_SIZE = "large";
const CSE_ULTRA_IMG_SIZE = "xxlarge";
const CSE_MIN_IMAGE_SHORT_EDGE = 720;
const CSE_MAX_PAGE_SIZE = 10;
const CSE_MAX_PAGES = 5;
const CSE_MAX_IMAGE_RESULTS = 40;
const CSE_RELAXED_MIN_IMAGE_SHORT_EDGE = 480;

// Intro (seconds)
const DEFAULT_INTRO_SEC = 3.2;
const INTRO_MIN_SEC = clampNumber(2, 2, 4);
const INTRO_MAX_SEC = clampNumber(4, 2, 5);
// Outro (seconds)
const OUTRO_MIN_SEC = clampNumber(3, 3, 6);
const OUTRO_MAX_SEC = clampNumber(6, 3, 6);
const DEFAULT_OUTRO_SEC = clampNumber(4.8, OUTRO_MIN_SEC, OUTRO_MAX_SEC);
const OUTRO_SMILE_TAIL_SEC = clampNumber(1.0, 0.6, 2.0);
const INTRO_VIDEO_BLUR_SIGMA = clampNumber(2.6, 0, 8);
const INTRO_TEXT_FADE_IN_START = clampNumber(0.5, 0, 1.5);
const INTRO_TEXT_FADE_IN_DUR = clampNumber(0.55, 0.15, 1.2);
const INTRO_TEXT_X_PCT = clampNumber(0.12, 0.04, 0.2);
const INTRO_TEXT_Y_PCT = clampNumber(0.44, 0.2, 0.6);
const INTRO_SUBTITLE_Y_PCT = clampNumber(0.58, 0.3, 0.7);
const FINAL_FADE_OUT_SEC = clampNumber(0.5, 0, 1.2);

// Script pacing
const SCRIPT_VOICE_WPS = 2.75; // used only for word caps
// Slightly faster default pacing (~5% more words).
const SCRIPT_PACE_BIAS = clampNumber(1.05, 0.85, 1.35);
const SEGMENT_TARGET_SEC = 8;
const MAX_SEGMENTS = 45;
const SCRIPT_TOLERANCE_SEC = clampNumber(4.5, 2, 5);
const MAX_SCRIPT_REWRITES = clampNumber(4, 0, 5);
const MAX_QA_REWRITES = clampNumber(2, 0, 3);
const QA_SIMILARITY_THRESHOLD = clampNumber(0.88, 0.75, 0.96);
const QA_MIN_SEGMENT_WORDS = clampNumber(10, 6, 16);
const REWRITE_RATIO_DAMPING = clampNumber(0.6, 0.4, 0.85);
const REWRITE_CLOSE_RATIO_DELTA = clampNumber(0.05, 0.03, 0.1);
const REWRITE_CLOSE_DRIFT_MULT = clampNumber(1.2, 1.0, 1.6);
const REWRITE_ADJUST_MIN = clampNumber(6, 2, 12);
const REWRITE_ADJUST_MAX = clampNumber(22, 10, 35);
const MAX_FILLER_WORDS_PER_VIDEO = clampNumber(0, 0, 2);
const MAX_FILLER_WORDS_PER_SEGMENT = clampNumber(0, 0, 2);
const MAX_MICRO_EMOTES_PER_VIDEO = clampNumber(0, 0, 1);
const ENABLE_MICRO_EMOTES = true;

// Audio processing
const AUDIO_SR = 48000;
const AUDIO_CHANNELS = 1; // mono voice for stability + smaller sync payload
const GLOBAL_ATEMPO_MIN = 0.95;
const GLOBAL_ATEMPO_MAX = 1.07;
const INTRO_ATEMPO_MIN = clampNumber(0.9, 0.9, 1.05);
const INTRO_ATEMPO_MAX = clampNumber(1.06, 1.0, 1.15);
const OUTRO_ATEMPO_MIN = clampNumber(0.9, 0.9, 1.05);
const OUTRO_ATEMPO_MAX = clampNumber(1.06, 1.0, 1.15);
const SEGMENT_PAD_SEC = clampNumber(0.08, 0, 0.3);
const VOICE_SPEED_BOOST = clampNumber(1.0, 0.98, 1.08);
const FORCE_NEUTRAL_VOICEOVER = true;
const ALIGN_INTRO_OUTRO_ATEMPO = true;
const ALLOW_NARRATION_OVERRUN = true;
const MAX_NARRATION_OVERAGE_RATIO = clampNumber(1.5, 1.0, 1.8);
const MAX_NARRATION_OVERAGE_SEC = clampNumber(30, 5, 60);
const MAX_SUBTLE_VISUAL_EXPRESSIONS = clampNumber(2, 0, 2);
const SUBTLE_VISUAL_EDGE_BUFFER = clampNumber(1, 0, 3);

// Sync input prep
const SYNC_SO_INPUT_FPS = 30;
const SYNC_SO_INPUT_CRF = 22;
const SYNC_SO_MAX_BYTES = 19_900_000;
const SYNC_SO_PRE_MAX_EDGE = clampNumber(960, 640, 1280);
const SYNC_SO_PRESCALE_ALWAYS = false;
const SYNC_SO_PRESCALE_SIZE_PCT = clampNumber(0.85, 0.5, 0.98);
const SYNC_SO_PRESCALE_MIN_SEC = clampNumber(9, 4, 15);
const SYNC_SO_FALLBACK_MAX_EDGE = clampNumber(960, 640, 1280);
const SYNC_SO_SEGMENT_MAX_RETRIES = clampNumber(2, 0, 5);
const SYNC_SO_RETRY_DELAY_MS = clampNumber(1500, 250, 5000);
const SYNC_SO_REQUEST_GAP_MS = clampNumber(350, 0, 2000);
const REQUIRE_LIPSYNC = true;

// Presenter stability
const ENABLE_WARDROBE_EDIT = true;
const ENABLE_RUNWAY_BASELINE = true;
const USE_MOTION_REF_BASELINE = false;
const BASELINE_DUR_SEC = clampNumber(12, 6, 15);
const BASELINE_VARIANTS = clampNumber(1, 1, 3);
const CAMERA_ZOOM_OUT = clampNumber(0.9, 0.84, 1.0);
const ENABLE_SEGMENT_FADES = false;

// Music
const MUSIC_VOLUME = clampNumber(0.18, 0.06, 0.5);
const MUSIC_DUCK_THRESHOLD = clampNumber(0.09, 0.03, 0.3);
const MUSIC_DUCK_RATIO = clampNumber(6, 2, 14);
const MUSIC_DUCK_ATTACK = clampNumber(25, 5, 200);
const MUSIC_DUCK_RELEASE = clampNumber(260, 40, 1200);
const MUSIC_DUCK_MAKEUP = clampNumber(1.6, 1, 4);

const DEFAULT_MUSIC_URL = "";
const DEFAULT_MUSIC_PATH = "";

// Overlays
// Larger overlays by default; cap size relative to frame width.
const OVERLAY_SCALE = clampNumber(0.4, 0.14, 0.55);
const OVERLAY_MAX_WIDTH_PCT = clampNumber(0.45, 0.28, 0.5);
const OVERLAY_BORDER_PX = clampNumber(6, 0, 18);
const OVERLAY_MARGIN_PX = clampNumber(28, 6, 120);
const OVERLAY_DEFAULT_POSITION = "topRight";
const MAX_AUTO_OVERLAYS = clampNumber(10, 3, 16);

// Content visual mix (presenter vs static images)
const CONTENT_PRESENTER_RATIO = clampNumber(0.4, 0.2, 0.8);
const IMAGE_SEGMENT_TARGET_SEC = clampNumber(3.8, 2.5, 8);
const IMAGE_SEGMENT_MIN_IMAGES = clampNumber(2, 1, 6);
const IMAGE_SEGMENT_MAX_IMAGES = clampNumber(6, 2, 10);
const IMAGE_SEGMENT_MULTI_MIN_SEC = clampNumber(4.8, 3, 12);
const IMAGE_SEARCH_MAX_QUERY_VARIANTS = clampNumber(10, 4, 12);
const IMAGE_SEARCH_CANDIDATE_MULTIPLIER = clampNumber(7, 2, 8);
const GOOGLE_IMAGES_SEARCH_ENABLED = true;
const GOOGLE_IMAGES_VARIANT_LIMIT = clampNumber(4, 1, 6);
const GOOGLE_IMAGES_RESULTS_PER_QUERY = clampNumber(28, 8, 40);
const GOOGLE_IMAGES_MIN_POOL_MULTIPLIER = clampNumber(3, 1, 5);

const ENABLE_LONG_VIDEO_OVERLAYS = false;

const LONG_VIDEO_KEEP_TMP = false;

/* ---------------------------------------------------------------
 * In-memory job store
 * ------------------------------------------------------------- */

const JOBS = new Map();
const MAX_JOBS_TO_KEEP = 250;
const JOB_TTL_MS = 1000 * 60 * 60 * 6;

setInterval(() => {
	const now = Date.now();
	for (const [id, job] of JOBS.entries()) {
		const t = new Date(job.updatedAt || job.createdAt || now).getTime();
		if (now - t > JOB_TTL_MS) JOBS.delete(id);
	}
	if (JOBS.size > MAX_JOBS_TO_KEEP) {
		const entries = Array.from(JOBS.entries()).sort(
			(a, b) =>
				new Date(a[1].updatedAt || a[1].createdAt).getTime() -
				new Date(b[1].updatedAt || b[1].createdAt).getTime()
		);
		for (let i = 0; i < entries.length - MAX_JOBS_TO_KEEP; i++) {
			JOBS.delete(entries[i][0]);
		}
	}
}, 60_000).unref?.();

function nowIso() {
	return new Date().toISOString();
}

function logJob(jobId, msg, extra) {
	const prefix = jobId ? `[LongVideo][${jobId}]` : "[LongVideo]";
	if (extra !== undefined) {
		try {
			console.log(prefix, msg, JSON.stringify(extra));
		} catch {
			console.log(prefix, msg, extra);
		}
		return;
	}
	console.log(prefix, msg);
}

function updateJob(jobId, patch = {}) {
	const job = JOBS.get(jobId);
	if (!job) return;
	JOBS.set(jobId, { ...job, ...patch, updatedAt: nowIso() });
}

function ensureDir(dir) {
	if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
ensureDir(TMP_ROOT);
if (LONG_VIDEO_PERSIST_OUTPUT) ensureDir(OUTPUT_DIR);
if (LONG_VIDEO_PERSIST_OUTPUT) ensureDir(THUMBNAIL_DIR);

function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function clampNumber(n, min, max) {
	const x = Number(n);
	if (!Number.isFinite(x)) return min;
	return Math.max(min, Math.min(max, x));
}

function makeEven(n) {
	const x = Math.round(Number(n) || 0);
	return x % 2 === 0 ? x : x + 1;
}

function isHttpUrl(u) {
	return typeof u === "string" && /^https?:\/\//i.test(u);
}

function safeUnlink(file) {
	try {
		if (file && fs.existsSync(file)) fs.unlinkSync(file);
	} catch {}
}

function safeRmRecursive(dir) {
	try {
		if (dir && fs.existsSync(dir))
			fs.rmSync(dir, { recursive: true, force: true });
	} catch {}
}

function stripCodeFence(s = "") {
	const t = String(s || "").trim();
	if (!t.includes("```")) return t;
	const first = t.indexOf("```");
	const last = t.lastIndexOf("```");
	if (first === -1 || last === -1 || last <= first) return t;
	let inner = t.slice(first + 3, last).trim();
	inner = inner.replace(/^json/i, "").trim();
	return inner || t;
}

function parseJsonFlexible(raw = "") {
	const cleaned = stripCodeFence(String(raw || "").trim());
	if (!cleaned) return null;
	try {
		return JSON.parse(cleaned);
	} catch {
		const m = cleaned.match(/\{[\s\S]*\}/);
		if (!m) return null;
		try {
			return JSON.parse(m[0]);
		} catch {
			return null;
		}
	}
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
			s = s.replace(
				/(^|\s)([a-z0-9.-]+\.[a-z]{2,}[^\s)]*)/gi,
				(_m, prefix, url) =>
					`${prefix}https://${url.replace(/^https?:\/\//i, "")}`
			);
			s = s.replace(/(https?:\/\/[^\s)]+)[).,;:]+$/g, "$1");
			s = s.replace(/([^ \t\r\n])(https?:\/\/[^\s)]+)/g, "$1 $2");
			return s;
		})
		.join("\n")
		.replace(/\n{3,}/g, "\n\n");
	return fixed;
}

function countWords(text = "") {
	return String(text || "")
		.trim()
		.split(/\s+/)
		.filter(Boolean).length;
}

function normalizeQaText(text = "") {
	return String(text || "")
		.toLowerCase()
		.replace(/[^a-z0-9\s]/g, " ")
		.replace(/\s+/g, " ")
		.trim();
}

function tokenizeQaText(text = "") {
	const tokens = normalizeQaText(text)
		.split(" ")
		.filter(Boolean)
		.filter((t) => t.length >= 3);
	return tokens.filter(
		(t) => !TOPIC_STOP_WORDS.has(t) && !GENERIC_TOPIC_TOKENS.has(t)
	);
}

function overlapRatio(aTokens = [], bTokens = []) {
	if (!aTokens.length || !bTokens.length) return 0;
	const a = new Set(aTokens);
	const b = new Set(bTokens);
	let hit = 0;
	for (const tok of a) {
		if (b.has(tok)) hit += 1;
	}
	return hit / Math.max(1, Math.min(a.size, b.size));
}

const ATTRIBUTION_CUE_RE =
	/\b(according to|reported by|as reported by|per|via|sources? say|reports?|says|said|told)\b/i;

function extractSourceTokensFromContext(contextItems = []) {
	const tokens = new Set();
	for (const item of Array.isArray(contextItems) ? contextItems : []) {
		const host = getUrlHost(item?.link || "");
		if (!host) continue;
		const lowered = host.toLowerCase();
		const base = lowered.replace(
			/\.(com|net|org|co|us|uk|io|tv|info|biz|gov)$/i,
			""
		);
		const cleaned = base.replace(/[^a-z0-9]+/g, " ").trim();
		if (cleaned) tokens.add(cleaned);
		if (lowered) tokens.add(lowered);
	}
	return Array.from(tokens);
}

function segmentHasAttribution(text = "", sourceTokens = []) {
	const lower = String(text || "").toLowerCase();
	const hasCue = ATTRIBUTION_CUE_RE.test(lower);
	if (!sourceTokens.length) return hasCue;
	const hasSource = sourceTokens.some((tok) => tok && lower.includes(tok));
	if (hasCue && hasSource) return true;
	if (hasSource && /\b(reports?|according|per|via|says|said)\b/i.test(lower))
		return true;
	return false;
}

/* ---------------------------------------------------------------
 * ffmpeg / ffprobe bootstrap
 * ------------------------------------------------------------- */

function canExecBin(bin, args = ["-version"]) {
	try {
		const r = child_process.spawnSync(bin, args, {
			stdio: "ignore",
			windowsHide: true,
		});
		return r && r.status === 0;
	} catch {
		return false;
	}
}

const FFMPEG_CANDIDATES = [
	(typeof ffmpegStatic === "string" && ffmpegStatic.trim()) || null,
	"ffmpeg",
	os.platform() === "win32" ? "ffmpeg.exe" : "/usr/bin/ffmpeg",
].filter(Boolean);

function resolveFfmpegPath() {
	for (const candidate of FFMPEG_CANDIDATES) {
		if (!candidate) continue;
		if (canExecBin(candidate, ["-version"])) return candidate;
	}
	return null;
}

const ffmpegPath = resolveFfmpegPath();
if (ffmpegPath) console.log(`[FFmpeg]  binary : ${ffmpegPath}`);
else
	console.warn(
		"[LongVideo] WARN - No valid FFmpeg binary found. Set FFMPEG_PATH or ensure ffmpeg is on PATH."
	);

function resolveFfprobePath() {
	if (ffmpegPath) {
		const dir = path.dirname(ffmpegPath);
		const probeName = os.platform() === "win32" ? "ffprobe.exe" : "ffprobe";
		const candidate = path.join(dir, probeName);
		if (fs.existsSync(candidate) && canExecBin(candidate, ["-version"]))
			return candidate;
	}

	if (canExecBin("ffprobe", ["-version"])) return "ffprobe";
	if (os.platform() === "win32" && canExecBin("ffprobe.exe", ["-version"]))
		return "ffprobe.exe";

	return os.platform() === "win32" ? "ffprobe.exe" : "ffprobe";
}

const ffprobePath = resolveFfprobePath();
console.log(`[FFprobe] binary : ${ffprobePath}`);

function spawnBin(binPath, args, label, { timeoutMs } = {}) {
	return new Promise((resolve, reject) => {
		if (!binPath) {
			reject(new Error(`${label}: binary not found`));
			return;
		}

		const proc = child_process.spawn(binPath, args, {
			stdio: ["ignore", "pipe", "pipe"],
			windowsHide: true,
		});

		let stderr = "";
		let stdout = "";
		let killedByTimeout = false;

		const killTimer =
			timeoutMs && Number(timeoutMs) > 0
				? setTimeout(() => {
						killedByTimeout = true;
						try {
							proc.kill("SIGKILL");
						} catch {}
				  }, Number(timeoutMs))
				: null;

		proc.stdout.on("data", (d) => (stdout += d.toString()));
		proc.stderr.on("data", (d) => (stderr += d.toString()));

		proc.on("error", (err) => {
			if (killTimer) clearTimeout(killTimer);
			reject(err);
		});

		proc.on("close", (code) => {
			if (killTimer) clearTimeout(killTimer);
			if (code === 0) return resolve({ stdout, stderr });
			const head = (stderr || stdout || "").slice(0, 4000);
			const tailHint = killedByTimeout ? " (killed by timeout)" : "";
			reject(new Error(`${label} failed (code ${code})${tailHint}: ${head}`));
		});
	});
}

async function probeMedia(filePath) {
	if (!filePath || !fs.existsSync(filePath))
		return { duration: 0, hasVideo: false, hasAudio: false, streams: [] };

	return await new Promise((resolve) => {
		const args = [
			"-v",
			"error",
			"-print_format",
			"json",
			"-show_format",
			"-show_streams",
			filePath,
		];
		child_process.execFile(
			ffprobePath,
			args,
			{ timeout: 15000 },
			(err, stdout) => {
				if (err)
					return resolve({
						duration: 0,
						hasVideo: false,
						hasAudio: false,
						streams: [],
					});
				try {
					const data = JSON.parse(stdout || "{}");
					const dur = Number(data?.format?.duration || 0);
					const streams = Array.isArray(data?.streams) ? data.streams : [];
					const hasVideo = streams.some((s) => s.codec_type === "video");
					const hasAudio = streams.some((s) => s.codec_type === "audio");
					return resolve({
						duration: Number.isFinite(dur) ? dur : 0,
						hasVideo,
						hasAudio,
						streams,
					});
				} catch {
					return resolve({
						duration: 0,
						hasVideo: false,
						hasAudio: false,
						streams: [],
					});
				}
			}
		);
	});
}

async function probeDurationSeconds(filePath) {
	const info = await probeMedia(filePath);
	return info.duration || 0;
}

/* ---------------------------------------------------------------
 * Retry + HTTP helpers
 * ------------------------------------------------------------- */

function isRetriableAxiosError(err) {
	const status = err?.response?.status;
	if (!status) return true;
	if (status === 429) return true;
	if (status >= 500 && status <= 599) return true;
	return false;
}

async function withRetries(
	fn,
	{ retries = 2, baseDelayMs = 600, label = "" } = {}
) {
	let lastErr = null;
	for (let attempt = 0; attempt <= retries; attempt++) {
		try {
			return await fn(attempt);
		} catch (e) {
			lastErr = e;
			const retriable =
				isRetriableAxiosError(e) ||
				/ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN/i.test(
					String(e?.message || "")
				);
			if (attempt >= retries || !retriable) throw e;
			const delay = Math.round(
				baseDelayMs * Math.pow(2, attempt) + Math.random() * 150
			);
			if (label)
				console.warn(
					`[Retry] ${label} attempt ${attempt + 1}/${retries + 1} failed: ${
						e.message
					}. waiting ${delay}ms`
				);
			await sleep(delay);
		}
	}
	throw lastErr || new Error("retry failed");
}

async function downloadToFile(url, outPath, timeoutMs = 30000, retries = 2) {
	ensureDir(path.dirname(outPath));
	let lastErr = null;

	for (let attempt = 0; attempt <= retries; attempt++) {
		try {
			const res = await axios.get(url, {
				responseType: "stream",
				timeout: timeoutMs,
				headers: {
					"User-Agent": "agentai-long-video/2.0",
					Accept: "*/*",
				},
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

async function headContentType(url, timeoutMs = 8000) {
	try {
		const res = await axios.head(url, {
			timeout: timeoutMs,
			validateStatus: (s) => s >= 200 && s < 400,
			headers: { "User-Agent": "agentai-long-video/2.0" },
		});
		const ct = String(res.headers?.["content-type"] || "").toLowerCase();
		return ct || null;
	} catch {
		return null;
	}
}

/* ---------------------------------------------------------------
 * File type detection
 * ------------------------------------------------------------- */

function readFileHeader(filePath, bytes = 64) {
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

	// MP4/MOV-ish
	if (ascii12.slice(4, 8) === "ftyp") return { kind: "video", ext: "mp4" };

	return null;
}

/* ---------------------------------------------------------------
 * Output config + validation
 * ------------------------------------------------------------- */

function parseRatio(ratio, fallback = DEFAULT_OUTPUT_RATIO) {
	const raw = String(ratio || "").trim() || fallback;
	const m2 = String(fallback).match(/^(\d{2,5})\s*:\s*(\d{2,5})$/);
	const fallbackW = makeEven(Number(m2?.[1] || 1280));
	const fallbackH = makeEven(Number(m2?.[2] || 720));
	const fallbackLandscape = fallbackW >= fallbackH;
	// Accept common aspect-ratio shorthands
	if (raw === "16:9") {
		const w = fallbackLandscape ? fallbackW : fallbackH;
		const h = fallbackLandscape ? fallbackH : fallbackW;
		return { ratio: `${w}:${h}`, w, h };
	}
	if (raw === "9:16") {
		const w = fallbackLandscape ? fallbackH : fallbackW;
		const h = fallbackLandscape ? fallbackW : fallbackH;
		return { ratio: `${w}:${h}`, w, h };
	}
	const m = raw.match(/^(\d{2,5})\s*:\s*(\d{2,5})$/);
	if (!m) {
		return {
			ratio: fallback,
			w: fallbackW,
			h: fallbackH,
		};
	}
	return {
		ratio: `${Number(m[1])}:${Number(m[2])}`,
		w: makeEven(Number(m[1])),
		h: makeEven(Number(m[2])),
	};
}

function validateCreateBody(body = {}) {
	const errors = [];

	const targetDurationSec = Number(body.targetDurationSec || 60);
	const duration = Number.isFinite(targetDurationSec)
		? clampNumber(targetDurationSec, 20, 420)
		: 60;
	if (!Number.isFinite(duration))
		errors.push("targetDurationSec must be number");

	const outRatio = parseRatio(DEFAULT_OUTPUT_RATIO);
	const fps = clampNumber(Number(DEFAULT_OUTPUT_FPS), 15, 60);
	const scaleMode = DEFAULT_SCALE_MODE;
	const imageScaleMode = DEFAULT_IMAGE_SCALE_MODE;

	const introSec = clampNumber(DEFAULT_INTRO_SEC, INTRO_MIN_SEC, INTRO_MAX_SEC);
	const outroSec = clampNumber(DEFAULT_OUTRO_SEC, OUTRO_MIN_SEC, OUTRO_MAX_SEC);

	const presenterAssetUrl = DEFAULT_PRESENTER_ASSET_URL;
	const voiceId = "";
	const enableRunwayPresenterMotion = true;
	const enableWardrobeEdit = ENABLE_WARDROBE_EDIT;
	const disableMusic = false;

	return {
		errors,
		clean: {
			preferredTopicHint: String(body.preferredTopicHint || "").trim(),
			category: normalizeCategoryLabel(
				String(body.category || LONG_VIDEO_TRENDS_CATEGORY || "Entertainment")
			).trim(),
			language: normalizeLanguageLabel(body.language || "English"),
			targetDurationSec: duration,
			introSec,
			outroSec,
			output: { ...outRatio, fps, scaleMode, imageScaleMode },
			presenterAssetUrl,
			voiceoverUrl: "",
			voiceId,
			musicUrl: "",
			disableMusic,
			dryRun: Boolean(body.dryRun),
			enableRunwayPresenterMotion,
			enableWardrobeEdit,
			youtubeAccessToken: String(body.youtubeAccessToken || "").trim(),
			youtubeRefreshToken: String(body.youtubeRefreshToken || "").trim(),
			youtubeTokenExpiresAt: body.youtubeTokenExpiresAt || "",
			youtubeCategory: String(
				body.youtubeCategory || LONG_VIDEO_YT_CATEGORY || "Entertainment"
			).trim(),
			// overlays are optional; pass through as-is
			overlayAssets: Array.isArray(body.overlayAssets)
				? body.overlayAssets
				: [],
		},
	};
}

function buildBaseUrl(req) {
	return `${req.protocol || "http"}://${req.get("host")}`;
}

/* ---------------------------------------------------------------
 * Topic + CSE
 * ------------------------------------------------------------- */

const TOPIC_TOKEN_ALIASES = Object.freeze({
	oscar: ["oscars", "academy awards", "academy award"],
	oscars: ["oscar", "academy awards", "academy award"],
	grammy: ["grammys", "grammy awards"],
	grammys: ["grammy", "grammy awards"],
	emmy: ["emmys", "emmy awards"],
	emmys: ["emmy", "emmy awards"],
	"golden globe": ["golden globes"],
	"golden globes": ["golden globe"],
});

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

function expandTopicTokens(tokens = []) {
	const base = normalizeTopicTokens(tokens);
	const out = new Set(base);
	for (const tok of base) {
		if (TOPIC_TOKEN_ALIASES[tok]) {
			for (const alias of TOPIC_TOKEN_ALIASES[tok]) out.add(alias);
		}
	}
	return Array.from(out);
}

function minTopicTokenMatches(tokens = []) {
	const norm = normalizeTopicTokens(tokens);
	if (!norm.length) return 0;
	if (norm.length >= 3) return 2;
	return 1;
}

function minImageTopicTokenMatches(tokens = []) {
	const norm = normalizeTopicTokens(tokens);
	if (!norm.length) return 0;
	if (norm.length >= 2) return 2;
	return 1;
}

function hasRequiredTopicMatch(tokens = [], fields = []) {
	const required = minImageTopicTokenMatches(tokens);
	if (!required) return true;
	return topicMatchInfo(tokens, fields).count >= required;
}

function topicMatchInfo(tokens = [], fields = []) {
	const norm = expandTopicTokens(tokens);
	if (!norm.length) return { count: 0, matchedTokens: [], normTokens: [] };
	const hay = (fields || [])
		.flatMap((f) => {
			const str = String(f || "");
			const lowers = [str.toLowerCase()];
			try {
				lowers.push(decodeURIComponent(str).toLowerCase());
			} catch {}
			return lowers;
		})
		.join(" ");
	const matchedTokens = norm.filter((tok) => hay.includes(tok));
	return { count: matchedTokens.length, matchedTokens, normTokens: norm };
}

function cleanTopicCandidate(title = "") {
	let t = String(title || "")
		.replace(/\s*[-|]\s*[^-|]{2,}$/g, "")
		.replace(/^breaking:\s*/i, "")
		.trim();
	t = t.replace(/\s+/g, " ").trim();
	return t.slice(0, 120);
}

function isEntertainmentCandidate(title = "", snippet = "") {
	const hay = `${title} ${snippet}`.toLowerCase();
	return ENTERTAINMENT_KEYWORDS.some((k) => hay.includes(k));
}

function scoreTrendingCandidate(item) {
	const text = `${item.title || ""} ${item.snippet || ""}`.toLowerCase();
	let score = 0;
	for (const tok of TREND_SIGNAL_TOKENS) {
		if (text.includes(tok)) score += 2;
	}
	if (/top\s+\d+|most anticipated|best of/i.test(text)) score -= 2;
	if (isEntertainmentCandidate(item.title, item.snippet)) score += 3;
	return score;
}

function normalizeRelatedQueries(raw = null) {
	const obj = raw && typeof raw === "object" ? raw : {};
	const top = uniqueStrings(Array.isArray(obj.top) ? obj.top : [], {
		limit: 10,
	});
	const rising = uniqueStrings(Array.isArray(obj.rising) ? obj.rising : [], {
		limit: 10,
	});
	return { top, rising };
}

function normalizeInterestOverTime(raw = null) {
	const obj = raw && typeof raw === "object" ? raw : {};
	const points = clampNumber(Number(obj.points) || 0, 0, 500);
	const avg = clampNumber(Number(obj.avg) || 0, 0, 100);
	const latest = clampNumber(Number(obj.latest) || 0, 0, 100);
	const peak = clampNumber(Number(obj.peak) || 0, 0, 100);
	const slope = clampNumber(Number(obj.slope) || 0, -100, 100);
	return { points, avg, latest, peak, slope };
}

function scoreTrendStoryForYouTube(story) {
	if (!story) return 0;
	const title = String(
		story.topic || story.rawTitle || story.title || ""
	).trim();
	const snippet = (story.searchPhrases || []).join(" ");
	let score = scoreTrendingCandidate({ title, snippet });

	const articlesCount = Array.isArray(story.articles)
		? story.articles.length
		: 0;
	score += Math.min(articlesCount, 6) * 1.2;

	const related = normalizeRelatedQueries(story.relatedQueries);
	score += Math.min(related.top.length, 10) * 0.6;
	score += Math.min(related.rising.length, 10) * 1.1;

	const interest = normalizeInterestOverTime(story.interestOverTime);
	score += interest.peak / 25; // 0-4
	score += interest.latest / 33; // 0-3
	if (interest.slope > 0) score += Math.min(interest.slope, 50) / 10;

	if (story.image || (Array.isArray(story.images) && story.images.length))
		score += 1;

	return Number(score.toFixed(2));
}

function rankTrendStoriesForYouTube(stories = []) {
	if (!Array.isArray(stories) || !stories.length) return stories;
	const hasSignals = stories.some(
		(s) =>
			(s.relatedQueries &&
				((Array.isArray(s.relatedQueries.top) &&
					s.relatedQueries.top.length > 0) ||
					(Array.isArray(s.relatedQueries.rising) &&
						s.relatedQueries.rising.length > 0))) ||
			(s.interestOverTime && Number(s.interestOverTime.points) > 0)
	);
	if (!hasSignals) return stories;
	const scored = stories.map((s, idx) => ({
		...s,
		trendScore: scoreTrendStoryForYouTube(s),
		_rankIdx: idx,
	}));
	scored.sort((a, b) => {
		const diff = (b.trendScore || 0) - (a.trendScore || 0);
		return diff !== 0 ? diff : a._rankIdx - b._rankIdx;
	});
	return scored.map(({ _rankIdx, ...rest }) => rest);
}

function inferEntertainmentCategory(tokens = []) {
	const set = new Set(tokens.map((t) => t.toLowerCase()));
	if (
		["movie", "film", "trailer", "cast", "director", "box", "office"].some(
			(t) => set.has(t)
		)
	)
		return "film";
	if (
		["tv", "series", "season", "episode", "streaming"].some((t) => set.has(t))
	)
		return "tv";
	if (
		["song", "album", "music", "tour", "concert", "singer", "rapper"].some(
			(t) => set.has(t)
		)
	)
		return "music";
	if (
		["celebrity", "actor", "actress", "influencer", "tiktok"].some((t) =>
			set.has(t)
		)
	)
		return "celebrity";
	return "general";
}

const CATEGORY_LABEL_ALIASES = {
	petsandanimals: "Pets and Animals",
};

function normalizeCategoryLabel(label) {
	const raw = String(label || "").trim();
	if (!raw) return "";
	const key = raw.toLowerCase().replace(/\s+/g, "");
	return CATEGORY_LABEL_ALIASES[key] || raw;
}

const LANGUAGE_LABEL_MAP = {
	en: "English",
	es: "Spanish",
	fr: "French",
	de: "German",
	ar: "Arabic",
};

function normalizeLanguageLabel(label) {
	const raw = String(label || "").trim();
	if (!raw) return "English";
	const key = raw.toLowerCase();
	return LANGUAGE_LABEL_MAP[key] || raw;
}

function resolveTrendsCategoryId(label) {
	const target = normalizeCategoryLabel(label).toLowerCase();
	const entry = googleTrendingCategoriesId.find(
		(c) =>
			String(c.category || "")
				.trim()
				.toLowerCase() === target
	);
	return entry ? entry.ids[0] : 0;
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

function safeSlug(text = "", max = 60) {
	return String(text || "")
		.toLowerCase()
		.replace(/[^\w]+/g, "_")
		.replace(/^_+|_+$/g, "")
		.slice(0, max);
}

function normalizeTrendStory(raw = {}) {
	const baseTitle = String(
		raw.trendDialogTitle ||
			raw.title ||
			raw.rawTitle ||
			raw.dialogTitle ||
			raw.youtubeShortTitle ||
			raw.seoTitle ||
			""
	).trim();
	const topic = cleanTopicCandidate(baseTitle);
	const rawTitle = String(raw.rawTitle || raw.title || baseTitle || "").trim();
	const relatedQueries = normalizeRelatedQueries(
		raw.relatedQueries || raw.trendSignals?.relatedQueries || null
	);
	const interestOverTime = normalizeInterestOverTime(
		raw.interestOverTime || raw.trendSignals?.interestOverTime || null
	);
	const relatedPhrases = uniqueStrings(
		[...relatedQueries.rising, ...relatedQueries.top],
		{ limit: 8 }
	);
	const searchPhrases = uniqueStrings(
		[
			topic,
			rawTitle,
			...relatedPhrases,
			...(raw.searchPhrases || []),
			...(raw.entityNames || []),
		],
		{ limit: 12 }
	);
	const articles = Array.isArray(raw.articles)
		? raw.articles
				.map((a) => ({
					title: String(a.title || "").trim(),
					url: a.url || null,
					image: isHttpUrl(a.image) ? a.image : null,
				}))
				.filter((a) => a.title)
		: [];
	const image = isHttpUrl(raw.image) ? raw.image : null;
	const images = uniqueStrings(
		[
			image,
			...(Array.isArray(raw.images) ? raw.images : []),
			...articles.map((a) => a.image).filter(Boolean),
		],
		{ limit: 10 }
	).filter((u) => isHttpUrl(u));
	const keywords = uniqueStrings(
		[
			...searchPhrases,
			...relatedPhrases,
			...articles.slice(0, 4).map((a) => a.title),
			topic,
		],
		{ limit: 12 }
	);
	return {
		topic,
		rawTitle,
		fromGoogleTrends: true,
		seoTitle: raw.seoTitle ? String(raw.seoTitle).trim() : null,
		youtubeShortTitle: raw.youtubeShortTitle
			? String(raw.youtubeShortTitle).trim()
			: null,
		searchPhrases,
		entityNames: uniqueStrings(raw.entityNames || [], { limit: 8 }),
		imageComment: String(raw.imageComment || raw.imageHook || "").trim(),
		viralImageBriefs: Array.isArray(raw.viralImageBriefs)
			? raw.viralImageBriefs
			: [],
		relatedQueries,
		interestOverTime,
		trendScore: Number(raw.trendScore) || 0,
		image,
		images,
		articles,
		keywords,
	};
}

async function fetchTrendsStories({
	categoryLabel = LONG_VIDEO_TRENDS_CATEGORY,
	geo = LONG_VIDEO_TRENDS_GEO,
	language = "English",
	baseUrl,
} = {}) {
	const categoryId = resolveTrendsCategoryId(categoryLabel);
	const params = new URLSearchParams({
		geo,
		hours: "48",
		language,
		category: String(categoryId),
		includeImages: "1",
	});
	const candidates = buildTrendsApiCandidates(baseUrl);
	for (let i = 0; i < candidates.length; i++) {
		const url = `${candidates[i]}?${params.toString()}`;
		for (let attempt = 1; attempt <= TRENDS_HTTP_MAX_ATTEMPTS; attempt++) {
			try {
				logJob(null, "trends fetch", { url, attempt });
				const { data } = await axios.get(url, {
					timeout: TRENDS_HTTP_TIMEOUT_MS,
					validateStatus: (s) => s < 500,
				});
				const stories = Array.isArray(data?.stories) ? data.stories : [];
				if (stories.length) {
					return stories
						.map((s) => normalizeTrendStory(s))
						.filter((s) => s.topic);
				}
				logJob(null, "trends fetch empty", { url, attempt });
			} catch (e) {
				logJob(null, "trends fetch failed", { error: e.message, url, attempt });
				if (attempt < TRENDS_HTTP_MAX_ATTEMPTS) {
					await delay(TRENDS_HTTP_RETRY_DELAY_MS);
				}
			}
		}
	}
	return [];
}

async function fetchCseItems(
	queries,
	{ num = 4, searchType = null, imgSize = null, start = 1, maxPages = 1 } = {}
) {
	if (!GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) return [];
	const list = Array.isArray(queries) ? queries.filter(Boolean) : [];
	if (!list.length) return [];

	const results = [];
	const seen = new Set();
	const totalTarget = Math.max(1, Math.floor(Number(num) || 1));
	const pageSize = Math.min(CSE_MAX_PAGE_SIZE, totalTarget);
	const pageCap = clampNumber(Number(maxPages) || 1, 1, 5);
	const baseStart = Math.max(1, Math.floor(Number(start) || 1));

	for (const q of list) {
		let pageStart = baseStart;
		let pagesFetched = 0;
		while (pagesFetched < pageCap) {
			const remaining = totalTarget - pagesFetched * pageSize;
			if (remaining <= 0) break;
			const pageNum = Math.min(pageSize, remaining);
			try {
				const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
					params: {
						key: GOOGLE_CSE_KEY,
						cx: GOOGLE_CSE_ID,
						q,
						num: pageNum,
						start: pageStart,
						safe: "active",
						gl: "us",
						hl: "en",
						...(searchType ? { searchType } : {}),
						...(searchType === "image"
							? {
									imgType: "photo",
									imgSize: imgSize || CSE_PREFERRED_IMG_SIZE,
							  }
							: {}),
					},
					timeout: 12000,
					validateStatus: (s) => s < 500,
				});

				if (!data || data.error) break;

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
				break;
			}

			pageStart += pageNum;
			pagesFetched += 1;
			if (pagesFetched < pageCap) await sleep(150);
		}
	}
	return results;
}

async function pickTrendingTopicFromCse() {
	const items = await fetchCseItems(CSE_ENTERTAINMENT_QUERIES, { num: 5 });
	if (!items.length) return null;

	const filtered = items.filter((it) =>
		isEntertainmentCandidate(it.title, it.snippet)
	);
	const pool = filtered.length ? filtered : items;
	const ranked = pool
		.map((it) => ({ ...it, score: scoreTrendingCandidate(it) }))
		.sort((a, b) => b.score - a.score);
	const shortlist = ranked.slice(0, 12);
	const top = shortlist[0] || pool[0];

	if (!process.env.CHATGPT_API_TOKEN || !top) {
		return {
			topic: cleanTopicCandidate(top?.title || ""),
			angle: "",
			reason: "CSE trending",
		};
	}

	const context = shortlist
		.map(
			(it, idx) =>
				`${idx + 1}) ${it.title}${it.snippet ? " | " + it.snippet : ""}`
		)
		.join("\n");

	const ask = `
Pick ONE specific, high-interest entertainment topic for a US audience.
It must be something people are searching for now (celebrity, movie/TV title, trailer, scandal, tour, or awards).
Avoid broad listicles like "Top 10..." unless nothing else fits.
Keep it tightly searchable and clear.

Options:
${context}

Return JSON ONLY:
{ "topic": "...", "angle": "...", "reason": "...", "keywords": ["..."] }
`.trim();

	try {
		const resp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: ask }],
		});

		const parsed = parseJsonFlexible(
			resp?.choices?.[0]?.message?.content || ""
		);
		if (parsed?.topic) {
			const keywords = Array.isArray(parsed.keywords)
				? parsed.keywords.map((k) => String(k || "").trim()).filter(Boolean)
				: [];
			return {
				topic: String(parsed.topic).slice(0, 120),
				angle: String(parsed.angle || "").slice(0, 180),
				reason: String(parsed.reason || "CSE + OpenAI").slice(0, 220),
				keywords: keywords.slice(0, 8),
			};
		}
	} catch {
		// ignore
	}

	return {
		topic: cleanTopicCandidate(top?.title || ""),
		angle: "",
		reason: "CSE trending",
	};
}

function topicCountForDuration(contentTargetSec) {
	const sec = Number(contentTargetSec || 0);
	if (!Number.isFinite(sec) || sec <= 0) return 1;
	if (sec <= 180) return 1;
	if (sec <= 300) return 2;
	return 3;
}

function computeFlexibleNarrationTargetSec({
	requestedSec,
	topics = [],
	topicContexts = [],
}) {
	const requested = Math.max(18, Number(requestedSec) || 0);
	const topicCount = Math.max(1, topics.length || 1);

	const minSec = Math.max(18, Math.round(requested * 0.5));
	const maxSec = Math.max(minSec, Math.round(requested * 2));

	let totalSignal = 0;
	for (let i = 0; i < topicCount; i++) {
		const t = topics[i] || {};
		const ctx = Array.isArray(topicContexts?.[i]?.context)
			? topicContexts[i].context
			: [];
		const story = t.trendStory || {};
		const articles = Array.isArray(story.articles) ? story.articles : [];
		const phrases = Array.isArray(story.searchPhrases)
			? story.searchPhrases
			: [];
		const entities = Array.isArray(story.entityNames) ? story.entityNames : [];

		const signal =
			ctx.length * 1.0 +
			articles.length * 1.4 +
			phrases.length * 0.4 +
			entities.length * 0.3;
		totalSignal += signal;
	}

	const avgSignal = totalSignal / topicCount;
	const normalized = clampNumber(avgSignal / 10, 0, 1);

	let target = requested;
	if (normalized >= 0.92) {
		target = maxSec;
	} else if (normalized >= 0.75) {
		target = Math.min(maxSec, Math.round(requested * 1.7));
	} else if (normalized >= 0.55) {
		target = Math.min(maxSec, Math.round(requested * 1.35));
	} else if (normalized >= 0.4) {
		target = Math.min(maxSec, Math.round(requested * 1.1));
	} else if (normalized <= 0.15) {
		target = minSec;
	} else if (normalized <= 0.3) {
		target = Math.max(minSec, Math.round(requested * 0.67));
	}

	target = clampNumber(target, minSec, maxSec);

	return {
		targetSec: target,
		minSec,
		maxSec,
		mode: "flex",
		signal: {
			total: Number(totalSignal.toFixed(2)),
			avg: Number(avgSignal.toFixed(2)),
			normalized: Number(normalized.toFixed(3)),
		},
	};
}

function topicSignature(text = "") {
	const cleaned = cleanTopicLabel(text);
	return cleanTopicCandidate(cleaned).toLowerCase();
}

function addUsedTopicVariants(set, text = "") {
	if (!set) return;
	const norm = topicSignature(text);
	if (!norm) return;
	set.add(norm);
	const parts = norm.split(/\s+/).filter(Boolean);
	if (parts.length >= 2) set.add(parts.slice(0, 2).join(" "));
	if (parts.length >= 3) set.add(parts.slice(0, 3).join(" "));
}

async function loadRecentLongVideoTopics({ userId, categoryLabel }) {
	if (!userId) return new Set();
	const threeDaysAgo = dayjs().subtract(3, "day").toDate();
	const query = {
		user: userId,
		isLongVideo: true,
		createdAt: { $gte: threeDaysAgo },
	};
	if (categoryLabel) query.category = categoryLabel;
	try {
		const recentVideos = await Video.find(query).select(
			"topic topics seoTitle"
		);
		const used = new Set();
		for (const v of recentVideos) {
			const list = [];
			if (Array.isArray(v.topics)) list.push(...v.topics);
			if (v.topic) list.push(v.topic);
			if (v.seoTitle) list.push(v.seoTitle);
			for (const txt of list) addUsedTopicVariants(used, txt);
		}
		return used;
	} catch (e) {
		logJob(null, "recent topics lookup failed", { error: e.message });
		return new Set();
	}
}

async function loadRecentPresenterOutfits({ userId, limit = 10 }) {
	if (!userId) return [];
	try {
		const recent = await Video.find({
			user: userId,
			isLongVideo: true,
			presenterOutfit: { $exists: true, $ne: "" },
		})
			.sort({ createdAt: -1 })
			.limit(Math.max(0, Number(limit) || 0))
			.select({ presenterOutfit: 1 })
			.lean();
		return (recent || [])
			.map((v) => String(v.presenterOutfit || "").trim())
			.filter(Boolean);
	} catch (e) {
		logJob(null, "recent outfits lookup failed", { error: e.message });
		return [];
	}
}

function isDuplicateTopic(topic, existing = [], usedTopics = null) {
	const norm = topicSignature(topic);
	if (!norm) return true;
	const tokens = topicTokensFromTitle(norm);

	const matches = (candidate) => {
		const existingTitle = topicSignature(candidate);
		if (!existingTitle) return false;
		if (existingTitle === norm) return true;
		if (existingTitle.includes(norm) || norm.includes(existingTitle))
			return true;
		const existingTokens = topicTokensFromTitle(existingTitle);
		const overlap = tokens.filter((t) => existingTokens.includes(t));
		return overlap.length >= Math.min(2, tokens.length, existingTokens.length);
	};

	if (usedTopics && usedTopics.size) {
		for (const used of usedTopics) {
			if (matches(used)) return true;
		}
	}
	for (const item of existing) {
		if (matches(item.topic || item.title || "")) return true;
	}
	return false;
}

async function selectTopics({
	preferredTopicHint,
	dryRun,
	topicCount,
	language = "English",
	categoryLabel,
	usedTopics,
	baseUrl,
} = {}) {
	const desired = Math.max(1, Number(topicCount) || 1);
	const usedSet = usedTopics instanceof Set ? new Set(usedTopics) : new Set();

	if (dryRun) {
		const hint = String(preferredTopicHint || "").trim();
		const topic = hint || "Dry run topic (provide preferredTopicHint)";
		const displayTopic = cleanTopicLabel(topic) || topic;
		return [
			{
				topic: topic.slice(0, 120),
				displayTopic,
				reason: dryRun ? "Dry run" : "preferredTopicHint",
				angle: "",
				keywords: topicTokensFromTitle(topic).slice(0, 8),
			},
		];
	}

	const topics = [];
	const hint = String(preferredTopicHint || "").trim();
	if (hint) {
		if (LONG_VIDEO_REQUIRE_TRENDS) {
			logJob(null, "preferred topic hint ignored (trends-only)", { hint });
		} else if (isDuplicateTopic(hint, topics, usedSet)) {
			logJob(null, "preferred topic hint skipped (duplicate)", { hint });
		} else {
			const displayTopic = cleanTopicLabel(hint) || hint;
			topics.push({
				topic: hint.slice(0, 120),
				displayTopic,
				reason: "preferredTopicHint",
				angle: "",
				keywords: topicTokensFromTitle(hint).slice(0, 8),
			});
			addUsedTopicVariants(usedSet, hint);
		}
	}

	const trendStories = await fetchTrendsStories({
		categoryLabel: categoryLabel || LONG_VIDEO_TRENDS_CATEGORY,
		geo: LONG_VIDEO_TRENDS_GEO,
		language,
		baseUrl,
	});
	if (!trendStories.length) {
		if (LONG_VIDEO_REQUIRE_TRENDS) {
			throw new Error("trends_unavailable");
		}
	}
	const primaryTrendStory = Array.isArray(trendStories)
		? trendStories[0]
		: null;
	if (
		primaryTrendStory?.topic &&
		!isDuplicateTopic(primaryTrendStory.topic, topics, usedSet)
	) {
		const displayTopic =
			cleanTopicLabel(primaryTrendStory.topic) || primaryTrendStory.topic;
		const relatedQueries = normalizeRelatedQueries(
			primaryTrendStory.relatedQueries
		);
		topics.push({
			topic: primaryTrendStory.topic,
			displayTopic,
			angle: "",
			reason: "Google Trends (first)",
			keywords: topicTokensFromTitle(primaryTrendStory.topic)
				.concat(topicTokensFromTitle(primaryTrendStory.rawTitle || ""))
				.concat(relatedQueries.rising.flatMap((q) => topicTokensFromTitle(q)))
				.concat(relatedQueries.top.flatMap((q) => topicTokensFromTitle(q)))
				.slice(0, 10),
			trendStory: primaryTrendStory,
		});
		addUsedTopicVariants(usedSet, primaryTrendStory.topic);
	}

	const rankedTrendStories = rankTrendStoriesForYouTube(trendStories);

	for (const story of rankedTrendStories) {
		if (topics.length >= desired) break;
		if (!story?.topic) continue;
		if (isDuplicateTopic(story.topic, topics, usedSet)) continue;
		const displayTopic = cleanTopicLabel(story.topic) || story.topic;
		const relatedQueries = normalizeRelatedQueries(story.relatedQueries);
		topics.push({
			topic: story.topic,
			displayTopic,
			angle: "",
			reason: "Google Trends",
			keywords: topicTokensFromTitle(story.topic)
				.concat(topicTokensFromTitle(story.rawTitle || ""))
				.concat(relatedQueries.rising.flatMap((q) => topicTokensFromTitle(q)))
				.concat(relatedQueries.top.flatMap((q) => topicTokensFromTitle(q)))
				.slice(0, 10),
			trendStory: story,
		});
		addUsedTopicVariants(usedSet, story.topic);
	}

	if (LONG_VIDEO_REQUIRE_TRENDS) {
		if (!topics.length) {
			throw new Error("Unable to pick topics from Google Trends.");
		}
		if (topics.length < desired) {
			logJob(null, "trends-only topic count below desired", {
				desired,
				count: topics.length,
			});
		}
		return topics.slice(0, desired);
	}

	let guard = 0;
	while (topics.length < desired && guard < 3) {
		guard += 1;
		const csePick = await pickTrendingTopicFromCse();
		if (csePick?.topic && !isDuplicateTopic(csePick.topic, topics, usedSet)) {
			const displayTopic = cleanTopicLabel(csePick.topic) || csePick.topic;
			topics.push({ ...csePick, displayTopic });
			addUsedTopicVariants(usedSet, csePick.topic);
		} else break;
	}

	if (topics.length < desired && process.env.CHATGPT_API_TOKEN) {
		try {
			const ask = `
Return JSON ONLY: { "topics": ["topic1", "topic2"] }
Provide ${
				desired - topics.length
			} current entertainment topics for a US audience.
Each topic must be specific (celebrity, movie/TV title, trailer, scandal, tour, awards).
Avoid broad listicles. Keep each short and searchable.
`.trim();
			const resp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: ask }],
			});
			const parsed = parseJsonFlexible(
				resp?.choices?.[0]?.message?.content || ""
			);
			const list = Array.isArray(parsed?.topics) ? parsed.topics : null;
			if (Array.isArray(list)) {
				for (const t of list) {
					if (topics.length >= desired) break;
					const topic = String(t || "").trim();
					if (!topic || isDuplicateTopic(topic, topics, usedSet)) continue;
					const displayTopic = cleanTopicLabel(topic) || topic;
					topics.push({
						topic: topic.slice(0, 120),
						displayTopic,
						reason: "OpenAI fallback",
						angle: "",
						keywords: topicTokensFromTitle(topic).slice(0, 8),
					});
					addUsedTopicVariants(usedSet, topic);
				}
			}
		} catch {
			// ignore
		}
	}

	if (!topics.length) {
		throw new Error(
			"Unable to pick topics. Provide preferredTopicHint or ensure Google Trends is available."
		);
	}

	return topics.slice(0, desired);
}

function isProbablyDirectImageUrl(u) {
	const url = String(u || "").trim();
	if (!/^https?:\/\//i.test(url)) return false;
	return /\.(png|jpe?g|webp)(\?|#|$)/i.test(url);
}

function isLikelyThumbnailUrl(u = "") {
	const url = String(u || "").toLowerCase();
	if (!url) return true;
	if (url.startsWith("data:image/")) return true;
	if (url.includes("encrypted-tbn0") || url.includes("tbn:")) return true;
	if (url.includes("gstatic.com/images?q=tbn")) return true;
	return false;
}

async function fetchGoogleImagesFromService(
	query,
	{ limit = GOOGLE_IMAGES_RESULTS_PER_QUERY, baseUrl, jobId } = {}
) {
	const q = sanitizeOverlayQuery(query);
	if (!q) return [];
	const candidates = buildGoogleImagesApiCandidates(baseUrl);
	for (const endpoint of candidates) {
		try {
			const { data } = await axios.get(endpoint, {
				params: { q, limit: Math.max(6, Number(limit) || 12) },
				timeout: 45000,
				validateStatus: (s) => s < 500,
			});
			const raw =
				(Array.isArray(data?.images) && data.images) ||
				(Array.isArray(data?.urls) && data.urls) ||
				(Array.isArray(data?.results) && data.results) ||
				[];
			const urls = uniqueStrings(
				raw.filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u)),
				{ limit: Math.max(12, Number(limit) || 12) }
			);
			if (urls.length) {
				if (jobId)
					logJob(jobId, "google images fallback hit", {
						query: q,
						endpoint,
						count: urls.length,
					});
				return urls;
			}
		} catch (e) {
			if (jobId)
				logJob(jobId, "google images fallback failed", {
					query: q,
					endpoint,
					error: e.message,
				});
		}
	}
	return [];
}

async function fetchCseContext(topic, extraTokens = []) {
	if (!topic) return [];
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const baseTokens = [...topicTokensFromTitle(topic), ...extra];
	const category = inferEntertainmentCategory(baseTokens);
	const queries = [
		`${topic} latest news`,
		`${topic} trending`,
		`${topic} explained`,
		`${topic} timeline`,
		`${topic} history`,
		`${topic} report`,
	];
	if (category === "film") {
		queries.push(`${topic} trailer`, `${topic} cast`, `${topic} box office`);
	} else if (category === "tv") {
		queries.push(
			`${topic} episode`,
			`${topic} season`,
			`${topic} streaming`,
			`${topic} finale`,
			`${topic} ending`
		);
	} else if (category === "music") {
		queries.push(`${topic} chart`, `${topic} music video`, `${topic} tour`);
	} else if (category === "celebrity") {
		queries.push(
			`${topic} interview`,
			`${topic} controversy`,
			`${topic} social media`
		);
	}
	if (category === "film" || category === "tv" || category === "celebrity") {
		queries.push(`${topic} rumor`, `${topic} leak`);
	}

	const items = await fetchCseItems(queries, { num: 5, maxPages: 2 });
	const matchTokens = expandTopicTokens(filterSpecificTopicTokens(baseTokens));
	const minMatches = minTopicTokenMatches(matchTokens);
	return items
		.filter(
			(it) =>
				topicMatchInfo(matchTokens, [it.title, it.snippet, it.link]).count >=
				minMatches
		)
		.slice(0, 6);
}

async function fetchCseImages(
	topic,
	extraTokens = [],
	jobId = null,
	opts = {}
) {
	if (!topic) return [];
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const baseTokens = [...topicTokensFromTitle(topic), ...extra];
	const category = inferEntertainmentCategory(baseTokens);
	const topicTokens = filterSpecificTopicTokens(topicTokensFromTitle(topic));
	const searchLabel = topicTokens.slice(0, 4).join(" ") || topic;
	const requiredTopicMatches = minImageTopicTokenMatches(topicTokens);
	const maxResults = clampNumber(
		Number(opts.maxResults) || 6,
		1,
		CSE_MAX_IMAGE_RESULTS
	);
	const maxPages = clampNumber(Number(opts.maxPages) || CSE_MAX_PAGES, 1, 5);
	const relaxedMinEdge = clampNumber(
		Number(opts.relaxedMinEdge) || CSE_RELAXED_MIN_IMAGE_SHORT_EDGE,
		200,
		CSE_MIN_IMAGE_SHORT_EDGE
	);
	const requestSize = Math.min(
		CSE_MAX_IMAGE_RESULTS,
		Math.max(12, maxResults * IMAGE_SEARCH_CANDIDATE_MULTIPLIER)
	);

	const queries = [
		`${searchLabel} press photo`,
		`${searchLabel} news photo`,
		`${searchLabel} photo`,
	];
	if (category === "film") {
		queries.unshift(
			`${searchLabel} official still`,
			`${searchLabel} movie still`,
			`${searchLabel} premiere`
		);
	} else if (category === "tv") {
		queries.unshift(
			`${searchLabel} episode still`,
			`${searchLabel} cast photo`
		);
	} else if (category === "music") {
		queries.unshift(
			`${searchLabel} live performance`,
			`${searchLabel} stage photo`
		);
	} else if (category === "celebrity") {
		queries.unshift(
			`${searchLabel} red carpet`,
			`${searchLabel} interview photo`
		);
	}

	const fallbackQueries = [
		`${searchLabel} photo`,
		`${searchLabel} press`,
		`${searchLabel} red carpet`,
		`${searchLabel} still`,
		`${searchLabel} interview`,
	];
	const keyPhrase = filterSpecificTopicTokens(baseTokens).slice(0, 2).join(" ");
	if (keyPhrase) {
		fallbackQueries.push(`${keyPhrase} photo`, `${keyPhrase} press`);
	}

	const attemptStats = [];
	let items = await fetchCseItems(queries, {
		num: requestSize,
		maxPages,
		searchType: "image",
		imgSize: CSE_ULTRA_IMG_SIZE,
	});
	attemptStats.push({
		label: "primary_ultra",
		items: items.length,
		imgSize: CSE_ULTRA_IMG_SIZE,
		maxPages,
	});
	if (!items.length) {
		items = await fetchCseItems(queries, {
			num: requestSize,
			maxPages,
			searchType: "image",
			imgSize: CSE_PREFERRED_IMG_SIZE,
		});
		attemptStats.push({
			label: "primary_preferred",
			items: items.length,
			imgSize: CSE_PREFERRED_IMG_SIZE,
			maxPages,
		});
	}
	if (!items.length) {
		items = await fetchCseItems(fallbackQueries, {
			num: requestSize,
			maxPages,
			searchType: "image",
			imgSize: CSE_PREFERRED_IMG_SIZE,
		});
		attemptStats.push({
			label: "fallback_preferred",
			items: items.length,
			imgSize: CSE_PREFERRED_IMG_SIZE,
			maxPages,
		});
	}
	if (!items.length) {
		items = await fetchCseItems(fallbackQueries, {
			num: requestSize,
			maxPages,
			searchType: "image",
			imgSize: CSE_FALLBACK_IMG_SIZE,
		});
		attemptStats.push({
			label: "fallback_large",
			items: items.length,
			imgSize: CSE_FALLBACK_IMG_SIZE,
			maxPages,
		});
	}
	const matchTokens = expandTopicTokens(filterSpecificTopicTokens(baseTokens));
	const minMatches = minTopicTokenMatches(matchTokens);
	const relaxedMinMatches = Math.max(1, minMatches - 1);
	const relaxedRequiredMatches = requiredTopicMatches ? 1 : 0;

	const strictCandidates = [];
	const relaxedCandidates = [];
	const maxCandidates = Math.max(24, maxResults * 4);
	for (const it of items) {
		const url = it.link || "";
		if (!url || !/^https:\/\//i.test(url)) continue;
		const fields = [it.title, it.snippet, it.link, it.image?.contextLink || ""];
		const info = topicMatchInfo(matchTokens, fields);
		const topicInfo = topicMatchInfo(topicTokens, fields);
		const w = Number(it.image?.width || 0);
		const h = Number(it.image?.height || 0);
		const shortEdge = w && h ? Math.min(w, h) : 0;
		const urlText = `${it.link || ""} ${
			it.image?.contextLink || ""
		}`.toLowerCase();
		const urlMatches = matchTokens.filter((tok) =>
			urlText.includes(tok)
		).length;
		const score = info.count + urlMatches * 0.75;
		const entry = { url, score, urlMatches, w, h };
		const strictOk =
			(!requiredTopicMatches || topicInfo.count >= requiredTopicMatches) &&
			info.count >= minMatches &&
			(!shortEdge || shortEdge >= CSE_MIN_IMAGE_SHORT_EDGE);
		if (strictOk) {
			strictCandidates.push(entry);
		} else {
			const relaxedOk =
				(!relaxedRequiredMatches ||
					topicInfo.count >= relaxedRequiredMatches) &&
				info.count >= relaxedMinMatches &&
				(!shortEdge || shortEdge >= relaxedMinEdge);
			if (relaxedOk) relaxedCandidates.push(entry);
		}
		if (strictCandidates.length + relaxedCandidates.length >= maxCandidates)
			break;
	}

	const candidates = strictCandidates.length
		? [...strictCandidates, ...relaxedCandidates]
		: relaxedCandidates;
	candidates.sort((a, b) => {
		if (b.score !== a.score) return b.score - a.score;
		if (b.w !== a.w) return b.w - a.w;
		return b.h - a.h;
	});

	let pool = candidates;
	if (matchTokens.length >= 2) {
		const strict = candidates.filter((c) => c.urlMatches >= 1);
		if (strict.length) {
			const relaxed = candidates.filter((c) => c.urlMatches < 1);
			pool = [...strict, ...relaxed];
		}
	}

	const filtered = [];
	const seen = new Set();
	for (const c of pool) {
		if (!c?.url) continue;
		const key = normalizeImageUrlKey(c.url);
		if (seen.has(key)) continue;
		seen.add(key);
		const looksDirect = isProbablyDirectImageUrl(c.url);
		const ct = looksDirect ? null : await headContentType(c.url, 7000);
		if (ct && !ct.startsWith("image/")) continue;
		filtered.push(c.url);
		if (filtered.length >= maxResults) break;
	}
	if (jobId)
		logJob(jobId, "cse image search summary", {
			topic,
			category,
			attempts: attemptStats,
			candidates: candidates.length,
			filtered: filtered.length,
			maxResults,
		});
	return filtered;
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
			headers: { "User-Agent": "agentai-long-video/2.0" },
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

async function fetchWikipediaPageImageUrl(topic = "") {
	const title = cleanTopicLabel(topic);
	if (!title) return null;
	const topicTokens = filterSpecificTopicTokens(topicTokensFromTitle(topic));
	const requiredTopicMatches = minImageTopicTokenMatches(topicTokens);
	try {
		const { data } = await axios.get(WIKIPEDIA_API_BASE, {
			params: {
				action: "query",
				format: "json",
				prop: "pageimages|info",
				inprop: "url",
				piprop: "original|thumbnail",
				pithumbsize: 1200,
				redirects: 1,
				titles: title,
			},
			timeout: 8000,
			validateStatus: (s) => s < 500,
			headers: { "User-Agent": "agentai-long-video/2.0" },
		});
		const pages = data?.query?.pages || {};
		const page = Object.values(pages)[0];
		if (!page || page.missing) return null;
		if (
			requiredTopicMatches &&
			topicMatchInfo(topicTokens, [page.title]).count < requiredTopicMatches
		)
			return null;
		const imageUrl = page.original?.source || page.thumbnail?.source || "";
		return imageUrl || null;
	} catch {
		return null;
	}
}

async function fetchWikimediaImageUrls(query = "", limit = 3) {
	const q = sanitizeOverlayQuery(query);
	if (!q) return [];
	const target = clampNumber(Number(limit) || 3, 1, 8);
	const matchTokens = filterSpecificTopicTokens(tokenizeLabel(q));
	const requiredTopicMatches = minImageTopicTokenMatches(matchTokens);
	try {
		const { data } = await axios.get(WIKIMEDIA_API_BASE, {
			params: {
				action: "query",
				format: "json",
				generator: "search",
				gsrsearch: q,
				gsrnamespace: 6,
				gsrlimit: Math.max(5, target * 2),
				prop: "imageinfo",
				iiprop: "url|size|mime",
				iiurlwidth: 1600,
			},
			timeout: 8000,
			validateStatus: (s) => s < 500,
			headers: { "User-Agent": "agentai-long-video/2.0" },
		});
		const pages = data?.query?.pages || {};
		const urls = [];
		for (const page of Object.values(pages)) {
			if (
				requiredTopicMatches &&
				topicMatchInfo(matchTokens, [page.title]).count < requiredTopicMatches
			)
				continue;
			const info = Array.isArray(page.imageinfo) ? page.imageinfo[0] : null;
			const url = String(info?.url || info?.thumburl || "").trim();
			const mime = String(info?.mime || "").toLowerCase();
			if (!url || (mime && !mime.startsWith("image/"))) continue;
			urls.push(url);
			if (urls.length >= target) break;
		}
		return uniqueStrings(urls, { limit: target });
	} catch {
		return [];
	}
}

function sanitizeOverlayQuery(query = "") {
	return String(query || "")
		.replace(/[^a-z0-9\s]/gi, " ")
		.replace(/\s+/g, " ")
		.trim()
		.slice(0, 80);
}

function ensureTopicInQuery(query = "", topicLabel = "") {
	const base = sanitizeOverlayQuery(query);
	const topic = sanitizeOverlayQuery(topicLabel);
	if (!topic) return base;
	if (!base) return topic;
	const baseTokens = new Set(tokenizeLabel(base));
	const topicTokens = tokenizeLabel(topic);
	const hasTopicToken = topicTokens.some((t) => baseTokens.has(t));
	if (hasTopicToken) return base;
	return sanitizeOverlayQuery(`${topic} ${base}`) || topic;
}

function isGenericOverlayQuery(query = "", topicLabel = "") {
	const base = sanitizeOverlayQuery(query);
	if (!base) return true;
	const tokens = tokenizeLabel(base);
	if (tokens.length < 2) return true;
	const topicTokens = new Set(tokenizeLabel(topicLabel || ""));
	const nonTopic = tokens.filter((t) => !topicTokens.has(t));
	return nonTopic.length === 0;
}

function buildOverlayQueryFallback(text = "", topic = "") {
	const base = cleanTopicCandidate(topic);
	const tokens = filterSpecificTopicTokens(tokenizeLabel(text)).slice(0, 4);
	const extras = tokens.filter(
		(t) => !base.toLowerCase().includes(String(t || "").toLowerCase())
	);
	const parts = [base, ...extras].filter(Boolean);
	return sanitizeOverlayQuery(parts.join(" "));
}

async function fetchCseImagesForQuery(
	query,
	topicTokens = [],
	maxResults = 4,
	jobId = null,
	opts = {}
) {
	const q = sanitizeOverlayQuery(query);
	if (!q) return [];
	const target = clampNumber(Number(maxResults) || 4, 1, CSE_MAX_IMAGE_RESULTS);
	const maxPages = clampNumber(Number(opts.maxPages) || CSE_MAX_PAGES, 1, 5);
	const relaxedMinEdge = clampNumber(
		Number(opts.relaxedMinEdge) || CSE_RELAXED_MIN_IMAGE_SHORT_EDGE,
		200,
		CSE_MIN_IMAGE_SHORT_EDGE
	);
	const requestSize = Math.min(
		CSE_MAX_IMAGE_RESULTS,
		Math.max(12, target * IMAGE_SEARCH_CANDIDATE_MULTIPLIER)
	);
	const strictTopicTokens = filterSpecificTopicTokens(topicTokens);
	const tokens = expandTopicTokens(
		filterSpecificTopicTokens([...tokenizeLabel(q), ...strictTopicTokens])
	);
	const minMatches = minTopicTokenMatches(tokens);
	const relaxedMinMatches = Math.max(1, minMatches - 1);
	const requiredTopicMatches = minImageTopicTokenMatches(strictTopicTokens);
	const relaxedRequiredMatches = requiredTopicMatches ? 1 : 0;
	const attemptStats = [];
	let items = await fetchCseItems([q], {
		num: requestSize,
		maxPages,
		searchType: "image",
		imgSize: CSE_ULTRA_IMG_SIZE,
	});
	attemptStats.push({
		label: "query_ultra",
		items: items.length,
		imgSize: CSE_ULTRA_IMG_SIZE,
		maxPages,
	});
	if (!items.length) {
		items = await fetchCseItems([q], {
			num: requestSize,
			maxPages,
			searchType: "image",
			imgSize: CSE_PREFERRED_IMG_SIZE,
		});
		attemptStats.push({
			label: "query_preferred",
			items: items.length,
			imgSize: CSE_PREFERRED_IMG_SIZE,
			maxPages,
		});
	}
	const strictCandidates = [];
	const relaxedCandidates = [];
	const maxCandidates = Math.max(12, target * 4);

	for (const it of items) {
		const url = it.link || "";
		if (!url || !/^https:\/\//i.test(url)) continue;
		const fields = [it.title, it.snippet, it.link, it.image?.contextLink || ""];
		const info = topicMatchInfo(tokens, fields);
		const topicInfo = topicMatchInfo(strictTopicTokens, fields);
		const w = Number(it.image?.width || 0);
		const h = Number(it.image?.height || 0);
		const shortEdge = w && h ? Math.min(w, h) : 0;
		const urlText = `${it.link || ""} ${
			it.image?.contextLink || ""
		}`.toLowerCase();
		const urlMatches = tokens.filter((tok) => urlText.includes(tok)).length;
		const score = info.count + urlMatches * 0.75;
		const entry = { url, score, urlMatches, w, h };
		const strictOk =
			(!requiredTopicMatches || topicInfo.count >= requiredTopicMatches) &&
			info.count >= minMatches &&
			(!shortEdge || shortEdge >= CSE_MIN_IMAGE_SHORT_EDGE);
		if (strictOk) {
			strictCandidates.push(entry);
		} else {
			const relaxedOk =
				(!relaxedRequiredMatches ||
					topicInfo.count >= relaxedRequiredMatches) &&
				info.count >= relaxedMinMatches &&
				(!shortEdge || shortEdge >= relaxedMinEdge);
			if (relaxedOk) relaxedCandidates.push(entry);
		}
		if (strictCandidates.length + relaxedCandidates.length >= maxCandidates)
			break;
	}

	const candidates = strictCandidates.length
		? [...strictCandidates, ...relaxedCandidates]
		: relaxedCandidates;
	candidates.sort((a, b) => {
		if (b.score !== a.score) return b.score - a.score;
		if (b.w !== a.w) return b.w - a.w;
		return b.h - a.h;
	});

	let pool = candidates;
	if (tokens.length >= 2) {
		const strict = candidates.filter((c) => c.urlMatches >= 1);
		if (strict.length) {
			const relaxed = candidates.filter((c) => c.urlMatches < 1);
			pool = [...strict, ...relaxed];
		}
	}

	const filtered = [];
	const seen = new Set();
	for (const c of pool) {
		if (!c?.url) continue;
		const key = normalizeImageUrlKey(c.url);
		if (seen.has(key)) continue;
		seen.add(key);
		const looksDirect = isProbablyDirectImageUrl(c.url);
		const ct = looksDirect ? null : await headContentType(c.url, 7000);
		if (ct && !ct.startsWith("image/")) continue;
		filtered.push(c.url);
		if (filtered.length >= target) break;
	}
	if (jobId)
		logJob(jobId, "cse image query summary", {
			query: q,
			attempts: attemptStats,
			candidates: candidates.length,
			filtered: filtered.length,
			target,
		});

	return filtered;
}

async function buildOverlayAssetsFromSegments({
	segments = [],
	timeline = [],
	topics = [],
	maxOverlays = MAX_AUTO_OVERLAYS,
}) {
	if (!segments.length || !GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) return [];

	const byIndex = new Map((timeline || []).map((t) => [Number(t.index), t]));
	const prioritized = [];
	const extras = [];
	const seenTopics = new Set();

	for (const seg of segments) {
		const t = byIndex.get(Number(seg.index));
		if (!t) continue;
		const topicIndex = Number(seg.topicIndex) || 0;
		const topicLabel = String(
			seg.topicLabel ||
				topics[topicIndex]?.displayTopic ||
				topics[topicIndex]?.topic ||
				""
		).trim();
		const cueRaw = Array.isArray(seg.overlayCues) ? seg.overlayCues[0] : null;
		const cueQuery =
			cueRaw?.query || buildOverlayQueryFallback(seg.text, topicLabel);
		const query = ensureTopicInQuery(cueQuery, topicLabel);
		if (!query) continue;
		const rawPos = String(cueRaw?.position || OVERLAY_DEFAULT_POSITION);
		const position = rawPos.startsWith("bottom")
			? rawPos.replace("bottom", "top")
			: rawPos;
		const cue = {
			segmentIndex: seg.index,
			topicIndex,
			topicLabel,
			query,
			position,
			startPct: Number(cueRaw?.startPct),
			endPct: Number(cueRaw?.endPct),
		};
		if (!seenTopics.has(topicIndex)) {
			seenTopics.add(topicIndex);
			prioritized.push(cue);
		} else {
			extras.push(cue);
		}
	}

	const candidates = [...prioritized, ...extras].slice(0, maxOverlays);
	const overlays = [];
	const fallbackByTopic = new Map();

	for (const cue of candidates) {
		const t = byIndex.get(Number(cue.segmentIndex));
		if (!t) continue;
		const segDur = Math.max(0.6, Number(t.endSec) - Number(t.startSec));
		const startPct = clampNumber(
			Number.isFinite(cue.startPct) ? cue.startPct : 0.25,
			0.2,
			0.75
		);
		const endPct = clampNumber(
			Number.isFinite(cue.endPct) ? cue.endPct : 0.75,
			startPct + 0.2,
			0.9
		);
		let startSec = Number(t.startSec) + segDur * startPct;
		let endSec = Number(t.startSec) + segDur * endPct;
		if (endSec - startSec < 1.6) {
			endSec = Math.min(Number(t.endSec) - 0.1, startSec + 2.0);
		}
		if (endSec <= startSec) continue;

		const topicTokens = topicTokensFromTitle(cue.topicLabel || "");
		let images = await fetchCseImagesForQuery(cue.query, topicTokens);
		if (!images.length && cue.topicLabel) {
			images = await fetchCseImages(cue.topicLabel, [cue.query]);
		}
		if (
			!images.length &&
			cue.topicLabel &&
			fallbackByTopic.has(cue.topicLabel)
		) {
			images = [fallbackByTopic.get(cue.topicLabel)];
		}
		const url = images[0];
		if (!url) continue;

		overlays.push({
			type: "image",
			url,
			startSec,
			endSec,
			position: cue.position || OVERLAY_DEFAULT_POSITION,
			scale: OVERLAY_SCALE,
		});
		if (cue.topicLabel && url) fallbackByTopic.set(cue.topicLabel, url);
		if (overlays.length >= maxOverlays) break;
	}

	return overlays;
}

function pickEvenlySpacedIndices(total, target) {
	if (!Number.isFinite(total) || total <= 0) return [];
	const t = Math.max(0, Math.min(Math.floor(target), total));
	if (t <= 0) return [];
	if (t >= total) return Array.from({ length: total }, (_, i) => i);
	const out = [];
	for (let i = 0; i < t; i++) {
		const idx = Math.floor((i * total) / t);
		out.push(Math.min(total - 1, Math.max(0, idx)));
	}
	return Array.from(new Set(out)).sort((a, b) => a - b);
}

function computeSegmentImageCount(segDur) {
	const dur = Math.max(0, Number(segDur) || 0);
	if (dur < IMAGE_SEGMENT_MULTI_MIN_SEC) return 1;
	const ideal = Math.round(dur / IMAGE_SEGMENT_TARGET_SEC);
	return clampNumber(ideal, IMAGE_SEGMENT_MIN_IMAGES, IMAGE_SEGMENT_MAX_IMAGES);
}

function resolveSegmentImageQuery(seg, topics = []) {
	const topicIndex = Number(seg?.topicIndex) || 0;
	const topicLabel = String(
		seg?.topicLabel ||
			topics?.[topicIndex]?.displayTopic ||
			topics?.[topicIndex]?.topic ||
			""
	).trim();
	const cueRaw = Array.isArray(seg?.overlayCues) ? seg.overlayCues[0] : null;
	const fallbackQuery = buildOverlayQueryFallback(seg?.text || "", topicLabel);
	const baseQuery = String(cueRaw?.query || "").trim();
	const preferredQuery = isGenericOverlayQuery(baseQuery, topicLabel)
		? fallbackQuery
		: baseQuery || fallbackQuery;
	const query = ensureTopicInQuery(preferredQuery, topicLabel);
	return { query, topicLabel };
}

function buildSegmentImageQueryVariants({
	baseQuery,
	topicLabel,
	segmentText,
	topicKeywords = [],
	articleTitles = [],
	maxVariants = IMAGE_SEARCH_MAX_QUERY_VARIANTS,
} = {}) {
	const variants = [];
	const push = (raw) => {
		const q = sanitizeOverlayQuery(raw);
		if (!q) return;
		variants.push(q);
	};

	push(baseQuery);
	if (segmentText || topicLabel) {
		const fallback = buildOverlayQueryFallback(
			segmentText || "",
			topicLabel || ""
		);
		push(fallback);
	}
	if (topicLabel) push(topicLabel);

	const topicTokens = new Set(tokenizeLabel(topicLabel || ""));
	const textTokens = filterSpecificTopicTokens(
		tokenizeLabel(segmentText || "")
	).filter((t) => !topicTokens.has(t));
	if (topicLabel && textTokens.length) {
		push(`${topicLabel} ${textTokens.slice(0, 3).join(" ")}`);
	}

	const hintList = uniqueStrings(
		[
			...(Array.isArray(topicKeywords) ? topicKeywords : []),
			...(Array.isArray(articleTitles) ? articleTitles : []),
		],
		{ limit: Math.max(6, Number(maxVariants) || 6) }
	);
	for (const hint of hintList) {
		if (variants.length >= maxVariants) break;
		const withTopic = ensureTopicInQuery(hint, topicLabel);
		push(withTopic);
	}

	const unique = uniqueStrings(variants, { limit: maxVariants });
	const multiWord = unique.filter((v) => tokenizeLabel(v).length >= 2);
	return multiWord.length
		? uniqueStrings(multiWord, { limit: maxVariants })
		: unique;
}

function extractSegmentMatchTokens(
	segmentText = "",
	topicLabel = "",
	maxTokens = 4
) {
	const topicTokens = new Set(tokenizeLabel(topicLabel || ""));
	const tokens = filterSpecificTopicTokens(
		tokenizeLabel(segmentText || "")
	).filter((t) => !topicTokens.has(t));
	return tokens.slice(0, Math.max(1, Number(maxTokens) || 1));
}

function getUrlHost(url = "") {
	try {
		const host = new URL(String(url)).hostname || "";
		return host.replace(/^www\./i, "");
	} catch {
		return "";
	}
}

function normalizeImageUrlKey(url = "") {
	try {
		const parsed = new URL(String(url || ""));
		parsed.hash = "";
		parsed.search = "";
		return parsed.toString().toLowerCase();
	} catch {
		return String(url || "")
			.split("?")[0]
			.split("#")[0]
			.toLowerCase();
	}
}

function scoreUrlTokenMatch(url = "", tokens = []) {
	const normTokens = normalizeTopicTokens(tokens);
	if (!normTokens.length) return 0;
	const base = String(url || "").toLowerCase();
	let hay = base;
	try {
		hay += ` ${decodeURIComponent(base)}`;
	} catch {}
	let count = 0;
	for (const tok of normTokens) {
		if (tok && hay.includes(tok)) count += 1;
	}
	return count;
}

function dedupeUrlsPreserveOrder(urls = []) {
	const out = [];
	const seen = new Set();
	for (const raw of Array.isArray(urls) ? urls : []) {
		const url = String(raw || "").trim();
		if (!url) continue;
		const key = normalizeImageUrlKey(url);
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(url);
	}
	return out;
}

function pickSegmentImageUrls(
	candidates = [],
	desiredCount = 1,
	usedUrls,
	usedHosts,
	opts = {}
) {
	let pool = dedupeUrlsPreserveOrder(candidates);
	if (!pool.length) return [];

	const target = Math.max(1, Math.floor(desiredCount));
	const maxPicks = Math.max(
		target,
		Math.floor(Number(opts.maxPicks) || target)
	);
	const usedUrlsGlobal =
		opts && opts.usedUrlsGlobal instanceof Set ? opts.usedUrlsGlobal : null;
	const requireTokens = Array.isArray(opts.requireTokens)
		? normalizeTopicTokens(opts.requireTokens)
		: [];
	const preferTokens = Array.isArray(opts.preferTokens)
		? normalizeTopicTokens(opts.preferTokens)
		: [];
	if (requireTokens.length) {
		const matched = pool.filter(
			(url) => scoreUrlTokenMatch(url, requireTokens) > 0
		);
		if (matched.length) {
			const matchedKeys = new Set(
				matched.map((url) => normalizeImageUrlKey(url))
			);
			const rest = pool.filter(
				(url) => !matchedKeys.has(normalizeImageUrlKey(url))
			);
			pool = matched.concat(rest);
			if (matched.length >= target) pool = matched;
		}
	}
	if (preferTokens.length && pool.length > 1) {
		const scored = pool.map((url) => ({
			url,
			score: scoreUrlTokenMatch(url, preferTokens),
		}));
		const withScore = scored.filter((c) => c.score > 0);
		const withoutScore = scored.filter((c) => c.score === 0);
		withScore.sort((a, b) => b.score - a.score);
		pool = [...withScore, ...withoutScore].map((c) => c.url);
	}
	const picks = [];
	const picked = new Set();

	for (const url of pool) {
		if (picks.length >= maxPicks) break;
		const key = normalizeImageUrlKey(url);
		if (usedUrls && usedUrls.has(key)) continue;
		if (usedUrlsGlobal && usedUrlsGlobal.has(key)) continue;
		const host = getUrlHost(url);
		if (usedHosts && host && usedHosts.has(host)) continue;
		picks.push(url);
		picked.add(key);
	}

	if (picks.length < maxPicks) {
		for (const url of pool) {
			if (picks.length >= maxPicks) break;
			const key = normalizeImageUrlKey(url);
			if (picked.has(key)) continue;
			if (usedUrls && usedUrls.has(key)) continue;
			if (usedUrlsGlobal && usedUrlsGlobal.has(key)) continue;
			picks.push(url);
			picked.add(key);
		}
	}

	if (picks.length < target && usedHosts) {
		for (const url of pool) {
			if (picks.length >= target) break;
			const key = normalizeImageUrlKey(url);
			if (picked.has(key)) continue;
			if (usedUrls && usedUrls.has(key)) continue;
			if (usedUrlsGlobal && usedUrlsGlobal.has(key)) continue;
			picks.push(url);
			picked.add(key);
		}
	}

	return picks;
}

async function downloadSegmentImages(
	urls,
	tmpDir,
	jobId,
	segIndex,
	targetCount = 0
) {
	const localPaths = [];
	const usedUrls = [];
	const seen = new Set();
	for (let i = 0; i < urls.length; i++) {
		const url = urls[i];
		if (!url) continue;
		const key = normalizeImageUrlKey(url);
		if (seen.has(key)) continue;
		seen.add(key);
		const extGuess = path
			.extname(String(url).split("?")[0] || "")
			.toLowerCase();
		const ext = extGuess && extGuess.length <= 5 ? extGuess : ".jpg";
		const out = path.join(
			tmpDir,
			`seg_${jobId}_${segIndex}_img_${i}_${crypto
				.randomUUID()
				.slice(0, 8)}${ext}`
		);
		try {
			await downloadToFile(url, out, 25000, 2);
			const detected = detectFileType(out);
			if (!detected || detected.kind !== "image") {
				safeUnlink(out);
				continue;
			}
			localPaths.push(out);
			usedUrls.push(url);
			if (targetCount && localPaths.length >= targetCount) break;
		} catch {
			safeUnlink(out);
		}
	}
	return { localPaths, usedUrls };
}

async function uploadLocalImageToCloudinary(
	localPath,
	{ publicIdBase, output, jobId, segIndex } = {}
) {
	if (!CLOUDINARY_ENABLED || !localPath || !fs.existsSync(localPath))
		return null;
	const baseOpts = {
		public_id: publicIdBase,
		resource_type: "image",
		overwrite: false,
		folder: "aivideomatic/long_feed",
	};
	try {
		const result = await cloudinary.uploader.upload(localPath, {
			...baseOpts,
			quality: "auto:good",
			fetch_format: "auto",
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
		if (!sizeIssue) throw e;

		const targetW = Math.max(640, Number(output?.w) || 1280);
		const targetH = Math.max(360, Number(output?.h) || 720);
		const scaledPath = path.join(
			path.dirname(localPath),
			`cloud_scaled_${jobId || "job"}_${segIndex || "seg"}_${crypto
				.randomUUID()
				.slice(0, 8)}.jpg`
		);
		await spawnBin(
			ffmpegPath,
			[
				"-i",
				localPath,
				"-vf",
				`scale=${targetW}:${targetH}:force_original_aspect_ratio=decrease:flags=lanczos`,
				"-frames:v",
				"1",
				"-q:v",
				"4",
				"-y",
				scaledPath,
			],
			"cloudinary_downscale",
			{ timeoutMs: 120000 }
		);
		const result = await cloudinary.uploader.upload(scaledPath, {
			...baseOpts,
			quality: "auto:good",
			fetch_format: "auto",
		});
		safeUnlink(scaledPath);
		return {
			public_id: result.public_id,
			url: result.secure_url,
		};
	}
}

async function uploadSegmentImagesToCloudinary({
	localPaths = [],
	jobId,
	segIndex,
	topicLabel,
	output,
}) {
	if (!CLOUDINARY_ENABLED || !Array.isArray(localPaths) || !localPaths.length)
		return [];
	const slug = safeSlug(topicLabel || `segment_${segIndex}`, 40) || "segment";
	const uploaded = [];
	for (let i = 0; i < localPaths.length; i++) {
		const publicIdBase = `aivideomatic/long_feed/${slug}_${jobId}_${segIndex}_${
			i + 1
		}`;
		try {
			const result = await uploadLocalImageToCloudinary(localPaths[i], {
				publicIdBase,
				output,
				jobId,
				segIndex,
			});
			if (result?.url) uploaded.push(result.url);
		} catch (e) {
			logJob(jobId, "cloudinary upload failed (segment image)", {
				segment: segIndex,
				error: e.message,
			});
		}
	}
	return uploaded;
}

async function fetchFallbackImageUrlsForSegment({
	query,
	topicLabel,
	limit = 4,
	articleUrls = [],
	seedUrls = [],
}) {
	const target = clampNumber(Number(limit) || 4, 1, 8);
	const urls = [];
	const seeded = uniqueStrings(seedUrls, { limit: Math.max(6, target * 2) });
	urls.push(...seeded);

	const topicTokens = filterSpecificTopicTokens(
		topicTokensFromTitle(topicLabel || query || "")
	);
	const requiredTopicMatches = minImageTopicTokenMatches(topicTokens);
	const contextItems = await fetchCseContext(
		query || topicLabel,
		topicLabel ? [topicLabel] : []
	);
	const strictContextItems = requiredTopicMatches
		? contextItems.filter(
				(it) =>
					topicMatchInfo(topicTokens, [it.title, it.snippet, it.link]).count >=
					requiredTopicMatches
		  )
		: contextItems;
	const contextArticleUrls = uniqueStrings(
		[
			...strictContextItems.map((c) => c?.link).filter(Boolean),
			...(Array.isArray(articleUrls) ? articleUrls : []),
		],
		{ limit: 10 }
	);
	for (const pageUrl of contextArticleUrls) {
		if (urls.length >= target) break;
		const og = await fetchOpenGraphImageUrl(pageUrl);
		if (!og) continue;
		if (isProbablyDirectImageUrl(og)) {
			urls.push(og);
			continue;
		}
		const ct = await headContentType(og, 7000);
		if (ct && ct.startsWith("image/")) urls.push(og);
	}

	if (urls.length < target && topicLabel) {
		const wiki = await fetchWikipediaPageImageUrl(topicLabel);
		if (wiki) urls.push(wiki);
	}

	if (urls.length < target && topicLabel) {
		const commons = await fetchWikimediaImageUrls(topicLabel, target);
		urls.push(...commons);
	}

	return uniqueStrings(urls, { limit: target });
}

async function prepareImageSegments({
	timeline = [],
	topics = [],
	tmpDir,
	jobId,
	baseUrl,
	output,
}) {
	if (!timeline.length) {
		return {
			timeline,
			segmentImagePaths: new Map(),
			imagePlanSummary: [],
		};
	}

	if (!GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) {
		logJob(jobId, "image segments skipped (CSE missing)");
		return {
			timeline: timeline.map((seg) =>
				seg.visualType === "image" ? { ...seg, visualType: "presenter" } : seg
			),
			segmentImagePaths: new Map(),
			imagePlanSummary: [],
		};
	}

	const queryCache = new Map();
	const topicCache = new Map();
	const fallbackCache = new Map();
	const googleImageCache = new Map();
	const usedHosts = new Set();
	const usedUrlsGlobal = new Set();
	const usedUrlsByTopic = new Map();
	const topicMetaByIndex = new Map();
	const outputCfg =
		output && typeof output === "object"
			? output
			: parseRatio(output || DEFAULT_OUTPUT_RATIO);

	for (let i = 0; i < (topics || []).length; i++) {
		const t = topics[i] || {};
		const label = String(t.displayTopic || t.topic || "").trim();
		const keywordHints = uniqueStrings(
			[
				...(Array.isArray(t.keywords) ? t.keywords : []),
				...(t.trendStory?.searchPhrases || []),
				...(t.trendStory?.entityNames || []),
			],
			{ limit: 10 }
		);
		const articleTitles = (t.trendStory?.articles || [])
			.map((a) => a.title)
			.filter(Boolean);
		const articleUrls = (t.trendStory?.articles || [])
			.map((a) => a.url)
			.filter((u) => isHttpUrl(u));
		const seedUrls = uniqueStrings(
			[
				t.trendStory?.image,
				...(Array.isArray(t.trendStory?.images) ? t.trendStory.images : []),
				...(t.trendStory?.articles || [])
					.map((a) => a.image)
					.filter((u) => isHttpUrl(u)),
			],
			{ limit: 10 }
		);
		topicMetaByIndex.set(i, {
			label,
			keywordHints,
			articleTitles,
			articleUrls,
			seedUrls,
		});
	}

	const getUsedUrlKeys = (topicIndex) => {
		if (!usedUrlsByTopic.has(topicIndex)) {
			usedUrlsByTopic.set(topicIndex, new Set());
		}
		return usedUrlsByTopic.get(topicIndex);
	};

	const segmentImagePaths = new Map();
	const imagePlanSummary = [];
	const updated = [];

	for (const seg of timeline) {
		if (seg.visualType !== "image") {
			updated.push(seg);
			continue;
		}

		const segDur = Math.max(0.2, Number(seg.endSec) - Number(seg.startSec));
		const desiredCount = computeSegmentImageCount(segDur);
		const topicIndex = Number(seg.topicIndex) || 0;
		const { query, topicLabel } = resolveSegmentImageQuery(seg, topics);
		const meta = topicMetaByIndex.get(topicIndex) || {};
		const effectiveTopicLabel = topicLabel || meta.label || "";
		const topicTokens = topicTokensFromTitle(effectiveTopicLabel || "");
		const segmentTokens = extractSegmentMatchTokens(
			seg.text || "",
			effectiveTopicLabel,
			4
		);
		const queryVariants = buildSegmentImageQueryVariants({
			baseQuery: query,
			topicLabel: effectiveTopicLabel,
			segmentText: seg.text,
			topicKeywords: meta.keywordHints,
			articleTitles: meta.articleTitles,
			maxVariants: IMAGE_SEARCH_MAX_QUERY_VARIANTS,
		});
		if (!queryVariants.length && query) queryVariants.push(query);
		if (!queryVariants.length && effectiveTopicLabel)
			queryVariants.push(effectiveTopicLabel);

		logJob(jobId, "segment image search", {
			segment: seg.index,
			query,
			topicLabel: effectiveTopicLabel,
			desiredCount,
			variantCount: queryVariants.length,
			segmentTokens,
		});

		const fromQueryUrls = [];
		for (const qVariant of queryVariants) {
			const cacheKey = `q::${qVariant}`;
			let urls = queryCache.get(cacheKey);
			if (!urls) {
				urls = await fetchCseImagesForQuery(
					qVariant,
					topicTokens,
					Math.max(12, desiredCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER),
					jobId,
					{ maxPages: CSE_MAX_PAGES }
				);
				queryCache.set(cacheKey, urls);
			}
			fromQueryUrls.push(...(urls || []));
		}

		let fromTopicUrls = [];
		if (effectiveTopicLabel) {
			const topicKey = `topic::${topicIndex}`;
			fromTopicUrls = topicCache.get(topicKey) || [];
			if (!fromTopicUrls.length) {
				const topicExtras = uniqueStrings(
					[...queryVariants, ...(segmentTokens || [])],
					{ limit: 12 }
				);
				fromTopicUrls = await fetchCseImages(
					effectiveTopicLabel,
					topicExtras,
					jobId,
					{
						maxResults: Math.max(
							12,
							desiredCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER
						),
						maxPages: CSE_MAX_PAGES,
					}
				);
				topicCache.set(topicKey, fromTopicUrls);
			}
		}

		let candidates = dedupeUrlsPreserveOrder([
			...fromQueryUrls,
			...fromTopicUrls,
			...(Array.isArray(meta.seedUrls) ? meta.seedUrls : []),
		]);

		logJob(jobId, "segment image candidates", {
			segment: seg.index,
			query,
			topicLabel: effectiveTopicLabel,
			queryVariants: queryVariants.length,
			fromQuery: fromQueryUrls.length,
			fromTopic: fromTopicUrls.length,
			seeded: Array.isArray(meta.seedUrls) ? meta.seedUrls.length : 0,
			total: candidates.length,
		});

		if (
			GOOGLE_IMAGES_SEARCH_ENABLED &&
			candidates.length <
				desiredCount * Math.max(1, GOOGLE_IMAGES_MIN_POOL_MULTIPLIER)
		) {
			const googleVariants = queryVariants.slice(
				0,
				Math.max(1, GOOGLE_IMAGES_VARIANT_LIMIT)
			);
			const googleUrls = [];
			for (const gQuery of googleVariants) {
				const cacheKey = `gimg::${gQuery}`;
				let urls = googleImageCache.get(cacheKey);
				if (!urls) {
					urls = await fetchGoogleImagesFromService(gQuery, {
						limit: Math.max(
							12,
							desiredCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
							GOOGLE_IMAGES_RESULTS_PER_QUERY
						),
						baseUrl,
						jobId,
					});
					googleImageCache.set(cacheKey, urls);
				}
				googleUrls.push(...(urls || []));
			}
			if (googleUrls.length) {
				candidates = dedupeUrlsPreserveOrder([...candidates, ...googleUrls]);
				logJob(jobId, "segment google images added", {
					segment: seg.index,
					query,
					topicLabel: effectiveTopicLabel,
					variants: googleVariants.length,
					added: googleUrls.length,
					total: candidates.length,
				});
			}
		}

		if (candidates.length < desiredCount) {
			const fallbackKey = `${query}||${effectiveTopicLabel}`;
			let fallbackUrls = fallbackCache.get(fallbackKey);
			if (!fallbackUrls) {
				fallbackUrls = await fetchFallbackImageUrlsForSegment({
					query,
					topicLabel: effectiveTopicLabel,
					limit: Math.max(6, desiredCount * 2),
					articleUrls: meta.articleUrls,
					seedUrls: meta.seedUrls,
				});
				fallbackCache.set(fallbackKey, fallbackUrls);
			}
			candidates = dedupeUrlsPreserveOrder([
				...candidates,
				...(fallbackUrls || []),
			]);
			logJob(jobId, "segment image candidates (fallback)", {
				segment: seg.index,
				query,
				topicLabel: effectiveTopicLabel,
				total: candidates.length,
			});
		}

		const usedUrlKeys = getUsedUrlKeys(topicIndex);
		const picks = pickSegmentImageUrls(
			candidates,
			desiredCount,
			usedUrlKeys,
			usedHosts,
			{
				maxPicks: Math.max(
					desiredCount,
					desiredCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER
				),
				requireTokens: segmentTokens,
				preferTokens: topicTokens,
				usedUrlsGlobal,
			}
		);
		let download = await downloadSegmentImages(
			picks,
			tmpDir,
			jobId,
			seg.index,
			desiredCount
		);
		let localPaths = download.localPaths;
		let pickedUrls = download.usedUrls;

		if (pickedUrls.length) {
			for (const url of pickedUrls) {
				usedUrlKeys.add(normalizeImageUrlKey(url));
				usedUrlsGlobal.add(normalizeImageUrlKey(url));
				const host = getUrlHost(url);
				if (host) usedHosts.add(host);
			}
		}

		logJob(jobId, "segment image picks", {
			segment: seg.index,
			desiredCount,
			picked: picks.length,
			downloaded: localPaths.length,
		});

		if (localPaths.length < desiredCount) {
			const missing = Math.max(0, desiredCount - localPaths.length);
			const fallbackKey = `${query}||${effectiveTopicLabel}`;
			const fallbackUrls =
				fallbackCache.get(fallbackKey) ||
				(await fetchFallbackImageUrlsForSegment({
					query,
					topicLabel: effectiveTopicLabel,
					limit: Math.max(6, desiredCount * 2),
					articleUrls: meta.articleUrls,
					seedUrls: meta.seedUrls,
				}));
			fallbackCache.set(fallbackKey, fallbackUrls);
			const fallbackPicks = pickSegmentImageUrls(
				fallbackUrls,
				missing || desiredCount,
				usedUrlKeys,
				usedHosts,
				{
					maxPicks: Math.max(
						missing || desiredCount,
						(missing || desiredCount) * IMAGE_SEARCH_CANDIDATE_MULTIPLIER
					),
					requireTokens: segmentTokens,
					preferTokens: topicTokens,
					usedUrlsGlobal,
				}
			);
			download = await downloadSegmentImages(
				fallbackPicks,
				tmpDir,
				jobId,
				seg.index,
				missing || desiredCount
			);
			localPaths = localPaths.concat(download.localPaths || []);
			pickedUrls = pickedUrls.concat(download.usedUrls || []);
			if (download.usedUrls?.length) {
				for (const url of download.usedUrls) {
					usedUrlKeys.add(normalizeImageUrlKey(url));
					usedUrlsGlobal.add(normalizeImageUrlKey(url));
					const host = getUrlHost(url);
					if (host) usedHosts.add(host);
				}
			}
			logJob(jobId, "segment image picks (fallback)", {
				segment: seg.index,
				desiredCount,
				picked: fallbackPicks.length,
				downloaded: localPaths.length,
			});
		}

		let cloudinaryUrls = [];
		if (localPaths.length) {
			cloudinaryUrls = await uploadSegmentImagesToCloudinary({
				localPaths,
				jobId,
				segIndex: seg.index,
				topicLabel: effectiveTopicLabel,
				output: outputCfg,
			});
		}

		if (!localPaths.length) {
			logJob(jobId, "segment images missing; fallback to presenter", {
				segment: seg.index,
				query,
				topicLabel: effectiveTopicLabel,
			});
			updated.push({ ...seg, visualType: "presenter" });
			continue;
		}

		segmentImagePaths.set(seg.index, localPaths);
		imagePlanSummary.push({
			segment: seg.index,
			imageCount: localPaths.length,
			cloudinaryCount: cloudinaryUrls.length,
			query,
			topicLabel: effectiveTopicLabel,
		});
		updated.push({
			...seg,
			imageUrls: pickedUrls,
			imageCloudinaryUrls: cloudinaryUrls,
		});
	}

	return { timeline: updated, segmentImagePaths, imagePlanSummary };
}

/* ---------------------------------------------------------------
 * Presenter handling
 * ------------------------------------------------------------- */

async function ensureLocalPresenterAsset(assetUrl, tmpDir, jobId) {
	const requested = String(assetUrl || "").trim();
	let url = DEFAULT_PRESENTER_ASSET_URL;
	if (requested && requested !== DEFAULT_PRESENTER_ASSET_URL) {
		logJob(jobId, "presenter asset override ignored (forced default)");
	}

	const downloadAndValidate = async (u) => {
		const extGuess = path.extname(u.split("?")[0] || "").toLowerCase();
		const ext = extGuess && extGuess.length <= 5 ? extGuess : ".png";
		const outPath = path.join(tmpDir, `presenter_${crypto.randomUUID()}${ext}`);
		await downloadToFile(u, outPath, 35000, 2);

		const detected = detectFileType(outPath);
		if (!detected || detected.kind === "text") {
			safeUnlink(outPath);
			return null;
		}
		return outPath;
	};

	if (isHttpUrl(url)) {
		const ct = await headContentType(url, 9000);
		if (ct && ct.startsWith("text/")) {
			logJob(jobId, "presenter url invalid content-type; fallback to default", {
				url,
				ct,
			});
			url = DEFAULT_PRESENTER_ASSET_URL;
		}
		const p = await downloadAndValidate(url);
		if (p) return p;
		if (url !== DEFAULT_PRESENTER_ASSET_URL) {
			const p2 = await downloadAndValidate(DEFAULT_PRESENTER_ASSET_URL);
			if (p2) return p2;
		}
		throw new Error("Presenter asset could not be downloaded/validated");
	}

	if (!fs.existsSync(url)) {
		logJob(jobId, "presenter local path missing; fallback to default", { url });
		const p2 = await downloadAndValidate(DEFAULT_PRESENTER_ASSET_URL);
		if (p2) return p2;
		throw new Error("Presenter asset not found");
	}

	const detected = detectFileType(url);
	if (!detected || detected.kind === "text") {
		logJob(jobId, "presenter local invalid; fallback to default", { url });
		const p2 = await downloadAndValidate(DEFAULT_PRESENTER_ASSET_URL);
		if (p2) return p2;
		throw new Error("Presenter asset invalid");
	}

	return url;
}

async function ensureLocalMotionReferenceVideo(tmpDir, jobId) {
	if (!USE_MOTION_REF_BASELINE) return null;
	const url = DEFAULT_PRESENTER_MOTION_VIDEO_URL;
	if (!url) return null;

	const downloadAndValidate = async (u) => {
		const extGuess = path.extname(u.split("?")[0] || "").toLowerCase();
		const ext = extGuess && extGuess.length <= 5 ? extGuess : ".mp4";
		const outPath = path.join(
			tmpDir,
			`motion_ref_${crypto.randomUUID()}${ext}`
		);
		await downloadToFile(u, outPath, 60000, 2);
		const detected = detectFileType(outPath);
		if (!detected || detected.kind !== "video") {
			safeUnlink(outPath);
			return null;
		}
		return outPath;
	};

	try {
		const p = await downloadAndValidate(url);
		if (p) return p;
	} catch (e) {
		logJob(jobId, "motion reference download failed (ignored)", {
			error: e.message,
		});
	}
	return null;
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

	// Upload to presigned URL
	if (FormDataNode) {
		const form = new FormDataNode();
		Object.entries(fields || {}).forEach(([k, v]) => form.append(k, v));
		form.append("file", fs.createReadStream(filePath));
		const r = await axios.post(uploadUrl, form, {
			headers: { ...form.getHeaders() },
			maxBodyLength: Infinity,
			timeout: 60000,
			validateStatus: () => true,
		});
		if (r.status >= 300) throw new Error(`Runway upload failed (${r.status})`);
		return runwayUri;
	}

	// Node 18+ fallback
	if (typeof fetch !== "function" || typeof FormData !== "function") {
		throw new Error(
			"Runway upload requires Node 18+ (fetch/FormData) or install 'form-data'"
		);
	}
	const form = new FormData();
	Object.entries(fields || {}).forEach(([k, v]) => form.append(k, v));
	form.append("file", new Blob([fs.readFileSync(filePath)]), baseName);
	const resp = await fetch(uploadUrl, { method: "POST", body: form });
	if (!resp.ok) throw new Error(`Runway upload failed (${resp.status})`);
	return runwayUri;
}

async function pollRunwayTask(taskId, label) {
	const url = `https://api.dev.runwayml.com/v1/tasks/${taskId}`;
	for (let i = 0; i < 120; i++) {
		await sleep(2000);
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

function runwayRatio(ratio) {
	// Runway accepts explicit resolutions; keep safe set
	const allowed = new Set([
		"1280:720",
		"720:1280",
		"1104:832",
		"832:1104",
		"960:960",
		"1584:672",
		"1280:768",
		"768:1280",
	]);
	const r = String(ratio || "").trim();
	if (allowed.has(r)) return r;
	if (r === "16:9") return "1280:720";
	if (r === "9:16") return "720:1280";
	return "1280:720";
}

function seedFromJobId(jobId) {
	// Deterministic 32-bit seed from uuid
	const h = crypto.createHash("sha256").update(String(jobId)).digest();
	return h.readUInt32BE(0);
}

function pickIntroExpression(jobId) {
	void jobId;
	return "calm, neutral expression with relaxed eyes";
}

async function runwayImageToVideo({
	runwayImageUri,
	promptText,
	durationSec,
	ratio,
	modelOrder,
}) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	const models = [];
	const requested = Array.isArray(modelOrder)
		? modelOrder.map((m) => String(m || "").trim()).filter(Boolean)
		: [];
	if (requested.length) {
		for (const m of requested) {
			if (!models.includes(m)) models.push(m);
		}
	} else {
		if (RUNWAY_VIDEO_MODEL) models.push(RUNWAY_VIDEO_MODEL);
		if (
			RUNWAY_VIDEO_MODEL_FALLBACK &&
			RUNWAY_VIDEO_MODEL_FALLBACK !== RUNWAY_VIDEO_MODEL
		)
			models.push(RUNWAY_VIDEO_MODEL_FALLBACK);
	}
	if (!models.length) throw new Error("Runway image_to_video model missing");

	let lastErr = null;
	for (const model of models) {
		const payload = {
			model,
			promptImage: [{ uri: runwayImageUri, position: "first" }],
			promptText: String(promptText || "").slice(0, 900),
			ratio: runwayRatio(ratio),
			duration: clampNumber(Math.round(Number(durationSec) || 5), 2, 10),
		};

		const res = await axios.post(
			"https://api.dev.runwayml.com/v1/image_to_video",
			payload,
			{
				headers: runwayHeadersJson(),
				timeout: 30000,
				validateStatus: (s) => s < 500,
			}
		);
		if (res.status < 300 && res.data?.id)
			return await pollRunwayTask(res.data.id, "runway_image_to_video");

		const msg =
			typeof res.data === "string" ? res.data : JSON.stringify(res.data || {});
		lastErr = new Error(
			`Runway image_to_video failed (${res.status}): ${msg.slice(0, 700)}`
		);
		if (model !== models[models.length - 1]) {
			console.warn(
				`[Runway] image_to_video failed for model ${model}; trying fallback ${
					models[models.length - 1]
				}`
			);
		}
	}
	throw lastErr || new Error("Runway image_to_video failed");
}

async function runwayVideoToVideo({
	runwayVideoUri,
	promptText,
	ratio,
	seed,
	references,
}) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");

	// Per Runway docs, video_to_video expects model gen4_aleph and videoUri field.
	const payload = {
		model: "gen4_aleph",
		videoUri: runwayVideoUri,
		promptText: String(promptText || "").slice(0, 900),
		ratio: runwayRatio(ratio),
		seed: Number.isFinite(seed) ? seed : undefined,
		...(Array.isArray(references) && references.length ? { references } : {}),
	};

	const res = await axios.post(
		"https://api.dev.runwayml.com/v1/video_to_video",
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
			`Runway video_to_video failed (${res.status}): ${msg.slice(0, 700)}`
		);
	}

	return await pollRunwayTask(res.data.id, "runway_video_to_video");
}

function buildRestylePrompt({ mood = "neutral" } = {}) {
	const expr = normalizeExpression(mood);
	const smileLine =
		expr === "warm"
			? "Allow a natural friendly smile toward the end of the clip (not constant)."
			: "";

	return `
Restyle the provided performance video into a classy modern studio.
Keep the SAME identity (Ahmed): same face structure, beard, glasses, and age.
Location: clean desk, tasteful background, soft practical lighting, studio quality. ${STUDIO_EMPTY_PROMPT}
Outfit: classy tailored suit or blazer with a neat shirt.
Lighting: slightly darker cinematic look with a warm key light and gentle shadows (not too dark).
Props: keep all existing props exactly as in the reference; do not add or remove objects. If a candle is visible, keep it subtle and unchanged with a calm flame; do not add extra candles.
Preserve the original performance timing and micro-expressions (eyebrows, blinks, subtle reactions).
No text overlays, no extra people, no weird hands, no face warping, no mouth distortion.
${smileLine}
`.trim();
}

function buildBaselinePrompt(
	expression = "neutral",
	motionRefVideo,
	variant = 0
) {
	const expr = normalizeExpression(expression);
	let expressionLine =
		"Expression: calm and professional; neutral mouth, no smile.";
	if (expr === "warm")
		expressionLine =
			"Expression: friendly and approachable with a tiny micro-smile (closed mouth, no teeth).";
	if (expr === "excited")
		expressionLine =
			"Expression: upbeat and engaged with a brief, very slight smile; minimal teeth only for a moment, no wide grin.";
	if (expr === "serious")
		expressionLine =
			"Expression: neutral and steady, no frown, no exaggerated concern, soft eye contact.";
	if (expr === "thoughtful")
		expressionLine =
			"Expression: thoughtful and composed, neutral mouth, gentle eye focus, no smile.";

	const variantHint =
		variant === 1
			? "Add tiny head tilts and micro shifts in posture; keep movement subtle."
			: variant === 2
			? "Use a slightly different blink cadence and a soft head turn or two."
			: "";
	const motionHint = motionRefVideo
		? "Match the natural motion style from the reference performance: gentle head movement with rare micro-nods (no repeated nodding); head mostly steady; slow and controlled; no fast turns or jerky motion; no hand gestures."
		: PRESENTER_MOTION_STYLE;

	return `
Photorealistic talking-head video of the SAME person as the reference image.
Keep identity, studio background, lighting, and wardrobe consistent. ${STUDIO_EMPTY_PROMPT}
Background must remain locked and static; no movement or people behind the presenter.
Props: keep all existing props exactly as in the reference; do not add or remove objects. If a candle is visible, keep it subtle and unchanged with a calm flame; no extra candles.
Framing: medium shot (not too close, not too far), upper torso to mid torso, moderate headroom; desk visible; camera at a comfortable distance.
${expressionLine}
Motion: ${motionHint} ${variantHint}
Mouth and jaw: natural, human movement; avoid robotic or stiff mouth shapes.
Smiles/laughter: tiny, brief smiles only; no laughs or exaggerated emotion.
Forehead: natural skin texture and subtle movement; avoid waxy smoothing.
Eyes: relaxed, comfortable, natural reflections and blink cadence; avoid glassy or robotic eyes.
Avoid exaggerated eye expressions or wide-eyed looks.
Hands: subtle, small gestures near the desk; do NOT cover the face.
No extra people, no text overlays, no screens, no charts, no logos except those already present in the reference, no camera shake, no mouth warping.
Do NOT try to lip-sync.
`.trim();
}

/* ---------------------------------------------------------------
 * Script generation (spicy US tone)
 * ------------------------------------------------------------- */

function computeSegmentCount(narrationTargetSec) {
	const n = Math.ceil((Number(narrationTargetSec) || 1) / SEGMENT_TARGET_SEC);
	const minSegs = narrationTargetSec < 26 ? 2 : 3;
	return clampNumber(n, minSegs, MAX_SEGMENTS);
}

function buildWordCaps(segmentCount, narrationTargetSec) {
	const avg = (Number(narrationTargetSec) || 60) / segmentCount;
	const caps = [];
	for (let i = 0; i < segmentCount; i++) {
		const hookBoost = i === 0 ? 1.05 : 1.0;
		const endCut = i === segmentCount - 1 ? 0.95 : 1.0;
		const cap = Math.max(
			14,
			Math.round(avg * SCRIPT_VOICE_WPS * SCRIPT_PACE_BIAS * hookBoost * endCut)
		);
		caps.push(cap);
	}
	return caps;
}

function allocateTopicSegments(segmentCount, topics = []) {
	const topicCount = Math.max(1, topics.length || 1);
	const base = Math.max(1, Math.floor(segmentCount / topicCount));
	let remainder = Math.max(0, segmentCount - base * topicCount);
	const ranges = [];
	let start = 0;

	for (let i = 0; i < topicCount; i++) {
		const count = base + (remainder > 0 ? 1 : 0);
		remainder = Math.max(0, remainder - 1);
		const end = Math.min(segmentCount - 1, start + count - 1);
		ranges.push({ topicIndex: i, startIndex: start, endIndex: end, count });
		start = end + 1;
	}

	return ranges;
}

const FICTIONAL_CONTEXT_STRONG_TOKENS = [
	"episode",
	"season",
	"series",
	"character",
	"plot",
	"storyline",
	"ending",
	"finale",
	"spoiler",
	"recap",
	"scene",
];

const FICTIONAL_CONTEXT_WEAK_TOKENS = [
	"show",
	"tv",
	"television",
	"movie",
	"film",
	"trailer",
	"cast",
	"premiere",
	"streaming",
	"netflix",
	"hbo",
	"disney",
	"prime",
	"paramount",
	"peacock",
	"apple tv",
];

const REAL_PERSON_CONTEXT_TOKENS = [
	"actor",
	"actress",
	"singer",
	"rapper",
	"musician",
	"comedian",
	"director",
	"producer",
	"influencer",
	"model",
	"celebrity",
	"instagram",
	"tiktok",
	"onlyfans",
	"youtube",
	"twitter",
	"x.com",
	"facebook",
	"snapchat",
	"podcast",
	"interview",
	"net worth",
	"paparazzi",
	"viral",
	"born",
	"birth",
	"age",
	"daughter",
	"son",
	"wife",
	"husband",
	"family",
	"parent",
	"parents",
	"child",
	"children",
	"police",
	"court",
	"trial",
	"arrest",
	"charged",
	"lawsuit",
	"hospital",
	"overdose",
	"coroner",
	"autopsy",
	"obituary",
	"investigation",
];

const REAL_WORLD_OVERRIDE_TOKENS = [
	"daughter",
	"son",
	"wife",
	"husband",
	"family",
	"parents",
	"child",
	"children",
	"police",
	"court",
	"trial",
	"arrest",
	"charged",
	"lawsuit",
	"hospital",
	"overdose",
	"coroner",
	"autopsy",
	"obituary",
	"investigation",
	"found dead",
	"cause of death",
	"instagram",
	"tiktok",
	"onlyfans",
	"net worth",
	"paparazzi",
];

const ANCHOR_NOISE_TOKENS = new Set([
	"latest",
	"trending",
	"trend",
	"news",
	"update",
	"updates",
	"explained",
	"report",
	"reports",
	"reporting",
	"breaking",
	"official",
	"video",
]);

const TOPIC_DOMAIN_TOKENS = [
	{
		domain: "sports",
		tokens: [
			"nba",
			"nfl",
			"mlb",
			"nhl",
			"wnba",
			"fifa",
			"uefa",
			"premier league",
			"champions league",
			"match",
			"game",
			"playoff",
			"finals",
			"tournament",
			"team",
			"coach",
		],
	},
	{
		domain: "music",
		tokens: [
			"album",
			"song",
			"single",
			"tour",
			"concert",
			"festival",
			"track",
			"band",
			"singer",
			"rapper",
			"billboard",
		],
	},
	{
		domain: "politics",
		tokens: [
			"election",
			"senate",
			"congress",
			"house",
			"president",
			"campaign",
			"vote",
			"policy",
			"governor",
			"mayor",
			"parliament",
		],
	},
	{
		domain: "business",
		tokens: [
			"earnings",
			"stock",
			"ipo",
			"merger",
			"acquisition",
			"ceo",
			"company",
			"startup",
			"investor",
			"funding",
		],
	},
	{
		domain: "tech",
		tokens: [
			"ai",
			"app",
			"iphone",
			"android",
			"software",
			"hardware",
			"release",
			"update",
			"startup",
			"platform",
		],
	},
	{
		domain: "gaming",
		tokens: ["game", "gaming", "esports", "console", "steam", "playstation"],
	},
];

function detectFictionalContext(text = "") {
	const raw = String(text || "");
	const hay = raw.toLowerCase();
	if (!hay) return false;
	const hasStrong = FICTIONAL_CONTEXT_STRONG_TOKENS.some((tok) =>
		hay.includes(tok)
	);
	const hasRealWorldOverride = REAL_WORLD_OVERRIDE_TOKENS.some((tok) =>
		hay.includes(tok)
	);
	const hasPersonCue = REAL_PERSON_CONTEXT_TOKENS.some((tok) =>
		hay.includes(tok)
	);
	const hasNamePattern =
		/\b[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?\b/.test(raw);
	if (hasStrong && (hasRealWorldOverride || hasPersonCue)) return false;
	if (hasStrong) return true;
	if (hasPersonCue) return false;
	if (hasNamePattern && hasRealWorldOverride) return false;
	const hasWeak = FICTIONAL_CONTEXT_WEAK_TOKENS.some((tok) =>
		hay.includes(tok)
	);
	if (!hasWeak) return false;
	const hasQuestionCue =
		/\b(did|does|do)\s+\w[\w\s]{0,40}\b(die|dies|died|killed|survive|survives|alive)\b/.test(
			hay
		) ||
		/\bending explained\b/.test(hay) ||
		/\bwho\s+(dies|died|survives|survived)\b/.test(hay);
	return hasQuestionCue;
}

function inferFictionalMedium(text = "") {
	const hay = String(text || "").toLowerCase();
	if (
		/\b(season|episode|series|show|tv|television|streaming|finale)\b/.test(hay)
	)
		return "series";
	if (/\b(movie|film|trailer|premiere)\b/.test(hay)) return "film";
	if (/\b(game|gaming|videogame)\b/.test(hay)) return "game";
	if (/\b(anime|manga|novel|book|comic)\b/.test(hay)) return "story";
	return "story";
}

function inferTopicDomainFromText(text = "") {
	const hay = String(text || "").toLowerCase();
	if (!hay) return { domain: "general" };
	if (detectFictionalContext(hay)) {
		return { domain: "fictional", medium: inferFictionalMedium(hay) };
	}
	let best = { domain: "general", score: 0 };
	for (const group of TOPIC_DOMAIN_TOKENS) {
		const score = group.tokens.reduce(
			(acc, tok) => acc + (hay.includes(tok) ? 1 : 0),
			0
		);
		if (score > best.score) best = { domain: group.domain, score };
	}
	return best.score ? { domain: best.domain } : { domain: "general" };
}

function splitTitleSegments(text = "") {
	const raw = String(text || "").trim();
	if (!raw) return [];
	return raw
		.split(/\s(?:-|\u2013|\u2014|\||:)\s/)
		.map((seg) => seg.trim())
		.filter(Boolean);
}

function buildTopicContextStrings(topicObj, contextItems = []) {
	const list = [];
	const topicLabel = cleanTopicLabel(
		topicObj?.displayTopic || topicObj?.topic || ""
	);
	if (topicLabel) list.push(topicLabel);
	if (topicObj?.rawTitle) list.push(String(topicObj.rawTitle));
	if (topicObj?.seoTitle) list.push(String(topicObj.seoTitle));
	if (topicObj?.youtubeShortTitle)
		list.push(String(topicObj.youtubeShortTitle));

	const story = topicObj?.trendStory || topicObj || {};
	const phrases = Array.isArray(story.searchPhrases) ? story.searchPhrases : [];
	const entities = Array.isArray(story.entityNames) ? story.entityNames : [];
	const articles = Array.isArray(story.articles) ? story.articles : [];
	const articleTitles = articles.map((a) => a?.title).filter(Boolean);
	const imageComment = story.imageComment || "";
	const related = normalizeRelatedQueries(
		story.relatedQueries || topicObj?.relatedQueries || null
	);
	const articleUrls = uniqueStrings(
		[
			...(Array.isArray(story.articleUrls) ? story.articleUrls : []),
			...articles.map((a) => a?.url).filter(Boolean),
		],
		{ limit: 8 }
	);
	const articleHosts = uniqueStrings(
		articleUrls.map((u) => getUrlHost(u)).filter(Boolean),
		{ limit: 8 }
	);

	list.push(
		...phrases,
		...entities,
		...articleTitles,
		...articleHosts,
		...related.rising,
		...related.top
	);
	if (imageComment) list.push(String(imageComment));

	for (const item of Array.isArray(contextItems) ? contextItems : []) {
		if (typeof item === "string") {
			list.push(item);
			continue;
		}
		if (item?.title) list.push(String(item.title));
		if (item?.snippet) list.push(String(item.snippet));
	}

	return uniqueStrings(list.filter(Boolean), { limit: 24 });
}

function scoreAnchorCandidate(candidate = "", baseTokens = []) {
	const cleaned = cleanTopicLabel(candidate);
	if (!cleaned) return -999;
	const lower = cleaned.toLowerCase();
	const tokens = tokenizeLabel(cleaned);
	if (!tokens.length) return -999;
	const matchCount = baseTokens.filter((t) => lower.includes(t)).length;
	const noiseHits = tokens.filter((t) => ANCHOR_NOISE_TOKENS.has(t)).length;
	const capWords = (candidate.match(/\b[A-Z][a-z]+\b/g) || []).length;
	const wordCount = tokens.length;
	let score =
		matchCount * 2 +
		capWords * 0.6 +
		Math.min(wordCount, 6) * 0.25 -
		Math.max(0, wordCount - 8) * 0.4;
	if (noiseHits) score -= Math.min(1.4, noiseHits * 0.7);
	if (
		/^(did|does|do|is|are|was|were|will|can|could|should|would|has|have|had)\b/i.test(
			cleaned
		)
	) {
		score -= 0.7;
	}
	const isGenericOnly = tokens.every(
		(t) => TOPIC_STOP_WORDS.has(t) || GENERIC_TOPIC_TOKENS.has(t)
	);
	if (isGenericOnly) score -= 2;
	return score;
}

function pickTopicAnchorLabel(topicLabel = "", contextStrings = []) {
	const baseLabel = normalizeTopicLabelForQuestion(topicLabel) || topicLabel;
	const baseTokens = filterSpecificTopicTokens(
		topicTokensFromTitle(baseLabel || topicLabel)
	);
	const candidates = new Set();
	const pushCandidate = (value) => {
		const cleaned = cleanTopicLabel(String(value || ""));
		if (!cleaned || cleaned.length < 3) return;
		candidates.add(cleaned);
	};

	pushCandidate(baseLabel);
	pushCandidate(topicLabel);
	for (const raw of Array.isArray(contextStrings) ? contextStrings : []) {
		pushCandidate(raw);
		const segments = splitTitleSegments(raw);
		for (const seg of segments) pushCandidate(seg);
	}

	let best = "";
	let bestScore = -999;
	for (const c of candidates) {
		const score = scoreAnchorCandidate(c, baseTokens);
		if (score > bestScore) {
			bestScore = score;
			best = c;
		}
	}

	if (!best) return shortTopicLabel(baseLabel || topicLabel, 5);
	const anchor = shortTopicLabel(best, 5);
	const cleanedAnchor = stripAnchorNoise(anchor);
	return cleanedAnchor || anchor || shortTopicLabel(baseLabel || topicLabel, 5);
}

function pickIntentEvidenceLine(
	contextStrings = [],
	anchor = "",
	topicLabel = ""
) {
	const lines = Array.isArray(contextStrings) ? contextStrings : [];
	const anchorLower = String(anchor || "").toLowerCase();
	if (anchorLower) {
		const hit = lines.find((l) =>
			String(l || "")
				.toLowerCase()
				.includes(anchorLower)
		);
		if (hit) return String(hit || "").slice(0, 160);
	}
	const baseTokens = topicTokensFromTitle(
		normalizeTopicLabelForQuestion(topicLabel) || topicLabel
	);
	if (baseTokens.length) {
		const hit = lines.find((l) =>
			baseTokens.some((t) =>
				String(l || "")
					.toLowerCase()
					.includes(t)
			)
		);
		if (hit) return String(hit || "").slice(0, 160);
	}
	return lines.length ? String(lines[0] || "").slice(0, 160) : "";
}

function buildTopicIntentSummary(topicObj, contextItems = []) {
	const label = cleanTopicLabel(
		topicObj?.displayTopic || topicObj?.topic || ""
	);
	const contextStrings = buildTopicContextStrings(topicObj, contextItems);
	const contextText = contextStrings.join(" ");
	const domainInfo = inferTopicDomainFromText(contextText);
	const anchor = pickTopicAnchorLabel(
		label || topicObj?.topic || "",
		contextStrings
	);
	const evidence = pickIntentEvidenceLine(contextStrings, anchor, label);
	return {
		label,
		anchor,
		domain: domainInfo.domain,
		medium: domainInfo.medium,
		evidence,
		hasContext: Boolean(contextStrings.length),
	};
}

function inferTonePlan({ topic, topics, angle, liveContext }) {
	const topicLine =
		Array.isArray(topics) && topics.length
			? topics.map((t) => t.topic || "").join(" ")
			: topic || "";
	const contextLines = Array.isArray(liveContext)
		? liveContext.map((c) =>
				typeof c === "string" ? c : `${c.title || ""} ${c.snippet || ""}`
		  )
		: [];
	const hay = [topicLine, angle || "", ...contextLines].join(" ").toLowerCase();

	let seriousScore = 0;
	let excitedScore = 0;

	for (const tok of SERIOUS_TONE_TOKENS) {
		if (hay.includes(tok)) seriousScore += 2;
	}
	for (const tok of EXCITED_TONE_TOKENS) {
		if (hay.includes(tok)) excitedScore += 1;
	}

	const mood =
		seriousScore >= excitedScore + 2
			? "serious"
			: excitedScore > seriousScore
			? "excited"
			: "neutral";
	return { mood };
}

const EXPRESSION_SET = new Set([
	"neutral",
	"warm",
	"serious",
	"excited",
	"thoughtful",
]);

function normalizeExpression(raw, mood = "neutral") {
	const t = String(raw || "")
		.trim()
		.toLowerCase();
	if (EXPRESSION_SET.has(t)) return t;
	if (t.includes("smile") || t.includes("friendly")) return "warm";
	if (t.includes("happy") || t.includes("joy")) return "warm";
	if (t.includes("serious") || t.includes("concern")) return "serious";
	if (t.includes("sad") || t.includes("sorrow") || t.includes("grief"))
		return "serious";
	if (t.includes("excite") || t.includes("hype")) return "excited";
	if (t.includes("think") || t.includes("reflect")) return "thoughtful";
	if (mood === "serious") return "serious";
	if (mood === "excited") return "excited";
	return "neutral";
}

function inferExplicitExpression(text = "") {
	const t = String(text || "").toLowerCase();
	const has = (list) => list.some((tok) => t.includes(tok));
	if (has(EXPLICIT_SERIOUS_CUES)) return "serious";
	if (has(EXPLICIT_EXCITED_CUES)) return "excited";
	if (has(EXPLICIT_WARM_CUES)) return "warm";
	if (has(EXPLICIT_THOUGHTFUL_CUES)) return "thoughtful";
	return null;
}

function coerceExpressionForNaturalness(rawExpression, text, mood = "neutral") {
	const base = normalizeExpression(rawExpression, mood);
	const explicit = inferExplicitExpression(text);
	if (explicit) {
		if (explicit === "serious") return "neutral";
		if (explicit === "excited") return "excited";
		return explicit;
	}
	if (mood === "serious") return "neutral";
	if (base === "excited") return "warm";
	if (base === "warm" || base === "thoughtful") return base;
	return "neutral";
}

function smoothExpressionPlan(expressions = [], mood = "neutral") {
	if (!expressions.length) return expressions;
	const out = [];
	let last = normalizeExpression(expressions[0], mood);
	out.push(last);

	for (let i = 1; i < expressions.length; i++) {
		const next = normalizeExpression(expressions[i], mood);
		const allowed =
			next === last ||
			(last === "neutral" &&
				["warm", "serious", "excited", "thoughtful"].includes(next)) ||
			(next === "neutral" &&
				["warm", "serious", "excited", "thoughtful"].includes(last)) ||
			(last === "warm" && next === "thoughtful") ||
			(last === "thoughtful" && next === "warm");

		if (!allowed) {
			out.push(last);
			continue;
		}
		out.push(next);
		last = next;
	}
	return out;
}

function buildVideoExpressionPlan(expressions = [], mood = "neutral") {
	if (!expressions.length) return expressions;
	const normalized = expressions.map((e) => normalizeExpression(e, mood));
	const out = [];
	let last = normalized[0];
	out.push(last);
	for (let i = 1; i < normalized.length; i++) {
		const next = normalized[i];
		const persists = i + 1 < normalized.length && normalized[i + 1] === next;
		if (next !== last && !persists) {
			out.push(last);
			continue;
		}
		out.push(next);
		last = next;
	}
	return out;
}

function pickSubtleExpressionIndices(
	total,
	seed,
	maxCount = MAX_SUBTLE_VISUAL_EXPRESSIONS,
	edgeBuffer = SUBTLE_VISUAL_EDGE_BUFFER
) {
	const t = Number.isFinite(Number(total)) ? Number(total) : 0;
	const buffer = Math.max(0, Math.floor(Number(edgeBuffer) || 0));
	const max = Math.max(0, Math.floor(Number(maxCount) || 0));
	if (!t || max <= 0) return [];
	if (t <= buffer * 2 + 1) return [];

	const eligible = [];
	for (let i = buffer; i <= t - buffer - 1; i++) eligible.push(i);
	if (!eligible.length) return [];

	const pickCount = Math.min(max, eligible.length);
	const base = pickEvenlySpacedIndices(eligible.length, pickCount);
	const shift = Math.abs(Number(seed) || 0) % eligible.length;
	const shifted = base.map((idx) => eligible[(idx + shift) % eligible.length]);
	return Array.from(new Set(shifted)).sort((a, b) => a - b);
}

function buildSubtleVideoExpressionPlan(
	segments = [],
	mood = "neutral",
	jobId
) {
	if (!segments.length) return [];
	const total = segments.length;
	const plan = Array.from({ length: total }, () => "neutral");
	if (mood === "serious") return plan;

	const seed = jobId ? seedFromJobId(jobId) : 0;
	const indices = pickSubtleExpressionIndices(total, seed);
	if (!indices.length) return plan;

	const normalized = segments.map((s) =>
		normalizeExpression(s.expression, mood)
	);
	for (const idx of indices) {
		const preferred = normalized[idx];
		if (preferred === "excited") plan[idx] = "excited";
		else if (preferred === "thoughtful") plan[idx] = "thoughtful";
		else plan[idx] = "warm";
	}
	return plan;
}

function shortTitleFromText(text = "") {
	const words = String(text || "")
		.replace(/["'(){}\[\]]/g, "")
		.replace(/[.,;:!?]+/g, " ")
		.trim()
		.split(/\s+/)
		.filter(Boolean);
	if (!words.length) return "Quick Update";
	return words.slice(0, 5).join(" ");
}

function normalizeRelatedQueriesAny(rq) {
	if (!rq || typeof rq !== "object") return { top: [], rising: [] };
	const top = Array.isArray(rq.top)
		? rq.top
		: Array.isArray(rq.topSample)
		? rq.topSample
		: [];
	const rising = Array.isArray(rq.rising)
		? rq.rising
		: Array.isArray(rq.risingSample)
		? rq.risingSample
		: [];
	return {
		top: uniqueStrings(top.map((s) => String(s || "").trim()).filter(Boolean), {
			limit: 12,
		}),
		rising: uniqueStrings(
			rising.map((s) => String(s || "").trim()).filter(Boolean),
			{ limit: 12 }
		),
	};
}

function normalizeInterestAny(io) {
	const safe = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
	return {
		points: safe(io?.points),
		avg: safe(io?.avg),
		latest: safe(io?.latest),
		peak: safe(io?.peak),
		slope: safe(io?.slope),
	};
}

function extractArticleFacts(story) {
	const articles = Array.isArray(story?.articles) ? story.articles : [];
	const titles = articles
		.map((a) => String(a?.title || "").trim())
		.filter(Boolean);
	const urls = articles.map((a) => String(a?.url || "").trim()).filter(Boolean);
	return { titles, urls };
}

function buildThumbnailSignalsFromTopicPick(topicPick) {
	const t = topicPick || {};
	const story = t.trendStory || t || {};
	const displayTopic = String(
		t.displayTopic || t.topic || story.title || story.rawTitle || ""
	).trim();
	const keywords = Array.isArray(t.keywords)
		? t.keywords.map((s) => String(s || "").trim()).filter(Boolean)
		: [];
	const relatedQueries = normalizeRelatedQueriesAny(
		story.relatedQueries || t.relatedQueries
	);
	const interestOverTime = normalizeInterestAny(
		story.interestOverTime || t.interestOverTime
	);
	const { titles: articleTitles, urls: articleUrls } =
		extractArticleFacts(story);
	const seedImages = Array.isArray(story.images)
		? story.images
		: Array.isArray(t.images)
		? t.images
		: [];
	const searchPhrases = Array.isArray(story.searchPhrases)
		? story.searchPhrases
		: Array.isArray(t.searchPhrases)
		? t.searchPhrases
		: [];
	const entityNames = Array.isArray(story.entityNames)
		? story.entityNames
		: Array.isArray(t.entityNames)
		? t.entityNames
		: [];

	return {
		displayTopic,
		keywords,
		relatedQueries,
		interestOverTime,
		articleTitles,
		articleUrls,
		seedImages: seedImages.map((u) => String(u || "").trim()).filter(Boolean),
		searchPhrases: searchPhrases
			.map((s) => String(s || "").trim())
			.filter(Boolean),
		entityNames: entityNames.map((s) => String(s || "").trim()).filter(Boolean),
		imageComment: String(story.imageComment || t.imageComment || "").trim(),
		angle: String(t.angle || "").trim(),
		reason: String(t.reason || "").trim(),
	};
}

const THUMBNAIL_INTENT_RULES = [
	{
		intent: "legal",
		re: /\b(lawsuit|court|judge|trial|appeal|charges|indict|arrest|police|investigation|filing|custody|conservatorship|bankruptcy)\b/i,
	},
	{
		intent: "finance",
		re: /\b(stock|shares|ipo|earnings|revenue|sec|market|inflation|interest rate|crypto|bitcoin|ethereum)\b/i,
	},
	{
		intent: "sports",
		re: /\b(nfl|nba|nhl|mlb|ufc|f1|match|goal|playoffs|draft|trade|transfer)\b/i,
	},
	{
		intent: "entertainment",
		re: /\b(trailer|season|episode|premiere|cast|box office|album|tour)\b/i,
	},
	{
		intent: "politics",
		re: /\b(election|vote|president|prime minister|senator|congress|parliament|campaign)\b/i,
	},
	{
		intent: "weather",
		re: /\b(hurricane|storm|tornado|wildfire|flood|heat wave|snow)\b/i,
	},
];

function inferIntentFromSignals({ title, signals }) {
	const rq = signals.relatedQueries || { top: [], rising: [] };
	const io = signals.interestOverTime || {};
	const hay = [
		title || "",
		signals.displayTopic || "",
		signals.angle || "",
		signals.reason || "",
		(rq.top || []).join(" "),
		(rq.rising || []).join(" "),
		(signals.articleTitles || []).join(" "),
		(signals.keywords || []).join(" "),
		signals.imageComment || "",
	]
		.join(" ")
		.toLowerCase();

	for (const rule of THUMBNAIL_INTENT_RULES) {
		if (rule.re.test(hay)) return rule.intent;
	}

	if (Number(io.slope) >= 15) return "general_trending";
	return "general";
}

function clampHeadline(text) {
	const t = String(text || "")
		.trim()
		.toUpperCase();
	if (!t) return "";
	return t.length > 18 ? t.slice(0, 18).trim() : t;
}

function pickHookFromQueries({
	rqTop = [],
	rqRising = [],
	intent = "general",
	slope = 0,
}) {
	const hay = `${rqTop.join(" ")} ${rqRising.join(" ")}`.toLowerCase();

	if (/\bwhat happened\b|\bwhat happened to\b/.test(hay))
		return { headline: "WHAT HAPPENED", badge: "UPDATE" };
	if (/\bwhy\b|\bexplained\b|\bmeaning\b/.test(hay))
		return { headline: "EXPLAINED", badge: "UPDATE" };
	if (/\breaction\b|\bresigns?\b|\bsteps down\b/.test(hay))
		return { headline: "BIG REACTION", badge: "UPDATE" };

	if (intent === "legal") return { headline: "LEGAL MOVE", badge: "REPORTS" };
	if (intent === "finance")
		return {
			headline: "MARKET MOVE",
			badge: slope >= 15 ? "TRENDING" : "UPDATE",
		};
	if (intent === "sports") return { headline: "MAJOR NEWS", badge: "UPDATE" };
	if (intent === "entertainment")
		return {
			headline: "NEW DETAILS",
			badge: slope >= 15 ? "TRENDING" : "UPDATE",
		};
	if (intent === "politics") return { headline: "NEW UPDATE", badge: "UPDATE" };
	if (intent === "weather") return { headline: "STORM UPDATE", badge: "ALERT" };

	return {
		headline: slope >= 15 ? "TRENDING NOW" : "NEW UPDATE",
		badge: "UPDATE",
	};
}

function buildTopicImageQueries({ signals, intent }) {
	const topic = signals.displayTopic || "";
	const entities = (signals.entityNames || []).slice(0, 2);
	const rqRising = (signals.relatedQueries?.rising || []).slice(0, 4);

	const base = [topic, ...entities, ...rqRising]
		.map((s) => String(s || "").trim())
		.filter(Boolean);
	const core = base[0] || topic;

	if (intent === "legal") {
		return uniqueStrings(
			[
				`${core} headshot`,
				`${core} portrait`,
				`${core} press photo`,
				`${core} interview`,
			],
			{ limit: 6 }
		);
	}

	if (intent === "finance") {
		return uniqueStrings(
			[
				`${core} CEO headshot`,
				`${core} logo`,
				`${core} press photo`,
				`${core} conference`,
			],
			{ limit: 6 }
		);
	}

	if (intent === "entertainment") {
		return uniqueStrings(
			[
				`${core} portrait`,
				`${core} close up`,
				`${core} interview`,
				`${core} red carpet`,
			],
			{ limit: 6 }
		);
	}

	return uniqueStrings(
		[`${core} portrait`, `${core} close up`, `${core} press photo`],
		{ limit: 6 }
	);
}

function buildThumbnailHookPlan({ title, topicPicks }) {
	const topics = Array.isArray(topicPicks) ? topicPicks : [];
	const t0 = topics[0] || {};
	const signals = buildThumbnailSignalsFromTopicPick(t0);
	const intent = inferIntentFromSignals({ title, signals });
	const rq = signals.relatedQueries || { top: [], rising: [] };
	const slope = Number(signals.interestOverTime?.slope || 0);
	const { headline, badge } = pickHookFromQueries({
		rqTop: rq.top,
		rqRising: rq.rising,
		intent,
		slope,
	});

	if (topics.length > 1) {
		return {
			intent: "multi",
			headline: "TOP STORIES",
			badgeText: `${Math.min(topics.length, 9)} STORIES`,
			imageQueries: [],
		};
	}

	return {
		intent,
		headline: clampHeadline(headline),
		badgeText: String(badge || "UPDATE")
			.trim()
			.toUpperCase(),
		imageQueries: buildTopicImageQueries({ signals, intent }),
	};
}

function cleanTopicLabel(text = "") {
	return String(text || "")
		.replace(/["'(){}\[\]]/g, "")
		.replace(/\s+/g, " ")
		.replace(/[.!?]+$/g, "")
		.trim();
}

function stripAnchorNoise(label = "") {
	const tokens = cleanTopicLabel(label)
		.split(/\s+/)
		.filter(Boolean)
		.filter((t) => /[a-z0-9]/i.test(t));
	while (tokens.length && ANCHOR_NOISE_TOKENS.has(tokens[0].toLowerCase())) {
		tokens.shift();
	}
	while (
		tokens.length &&
		ANCHOR_NOISE_TOKENS.has(tokens[tokens.length - 1].toLowerCase())
	) {
		tokens.pop();
	}
	return tokens.join(" ");
}

function looksLikeQuestionTopic(text = "") {
	const t = String(text || "")
		.trim()
		.toLowerCase();
	return (
		/\?/.test(t) ||
		/^(what|when|where|why|how|who|did|does|do|is|are|was|were|can|could|will|would|should|has|have|had|may|might)\b/.test(
			t
		) ||
		/\bwhat time\b/.test(t) ||
		/\bcome out\b/.test(t)
	);
}

function normalizeTopicLabelForQuestion(text = "") {
	let t = cleanTopicLabel(text);
	if (!t) return t;
	t = t
		.replace(
			/^(did|does|do|is|are|was|were|can|could|will|would|should|has|have|had|may|might)\s+/i,
			""
		)
		.replace(/^(what time does|what time do|when does|when do)\s+/i, "")
		.replace(
			/^(what is|who is|who are|how does|how do|why does|why is)\s+/i,
			""
		)
		.replace(/\bcome out\b/i, "")
		.replace(/\brelease date\b/i, "release")
		.replace(/\s+/g, " ")
		.trim();
	return t;
}

function stripTrailingPreposition(text = "") {
	return String(text || "")
		.replace(/\b(in|on|at|about|for|to|of|from|with|by|during)\s*$/i, "")
		.trim();
}

function normalizeEngagementLabel(text = "") {
	let t = normalizeTopicLabelForQuestion(text);
	if (!t) return t;
	const deathMatch = t.match(
		/^(.*)\b(die|dies|died|death)\b\s*(?:in|on|at|during)?\s*(.*)$/i
	);
	if (deathMatch) {
		const subject = String(deathMatch[1] || "").trim();
		let tail = String(deathMatch[3] || "").trim();
		tail = tail.replace(/^(in|on|at|during)\s+/i, "").trim();
		if (subject) {
			const possessive = subject.endsWith("s") ? `${subject}'` : `${subject}'s`;
			t = tail ? `${possessive} fate in ${tail}` : `${possessive} fate`;
		}
	}
	t = stripTrailingPreposition(t);
	return t || normalizeTopicLabelForQuestion(text) || cleanTopicLabel(text);
}

function selectEngagementLabel({ topicLabel, shortTitle, maxWords = 4 }) {
	const base = cleanTopicLabel(topicLabel);
	const isQuestion = looksLikeQuestionTopic(base);
	const normalized = isQuestion ? normalizeEngagementLabel(base) : base;
	const safeShortTitle = cleanTopicLabel(shortTitle || "");
	const normalizedShortTitle = isQuestion
		? normalizeEngagementLabel(safeShortTitle)
		: safeShortTitle;
	const preferred =
		isQuestion &&
		normalizedShortTitle &&
		normalizedShortTitle.toLowerCase() !== "quick update"
			? normalizedShortTitle
			: normalized || normalizedShortTitle || base;
	return shortTopicLabel(preferred, maxWords);
}

function shortTopicLabel(text = "", maxWords = 4) {
	const base = cleanTopicLabel(text);
	const words = base.split(/\s+/).filter(Boolean);
	if (!words.length) return "today's topic";
	if (words.length <= maxWords) {
		const full = words.join(" ");
		return stripTrailingPreposition(full) || full;
	}
	const clipped = stripTrailingPreposition(words.slice(0, maxWords).join(" "));
	return clipped || words[0];
}

function formatTopicList(topics = []) {
	const labels = (topics || [])
		.map((t) => shortTopicLabel(t?.displayTopic || t?.topic || t, 3))
		.filter(Boolean);
	if (!labels.length) return "today's topic";
	if (labels.length === 1) return labels[0];
	if (labels.length === 2) return `${labels[0]} and ${labels[1]}`;
	return `${labels[0]}, ${labels[1]}, and ${labels[2]}`;
}

const FILLER_WORD_REGEX = /\b(?:um+|uh+|uhm+|erm+|er|ah+|hmm+)\b/gi;
const LIKE_FILLER_REGEX = /([,.!?]\s+)like\s*,\s*/gi;
const MICRO_EMOTE_REGEX = /\b(?:heh|whew)\b/gi;

function cleanupSpeechText(text = "") {
	let t = String(text || "");
	t = t.replace(/\s+([,.;:!?])/g, "$1");
	t = t.replace(/([,;:!?]){2,}/g, "$1");
	t = t.replace(/,\s*,/g, ", ");
	t = t.replace(/,\s*([.!?])/g, "$1");
	t = t.replace(/\s+/g, " ").trim();
	return t;
}

const META_SENTENCE_PATTERNS = [
	/\b(outro|intro)\b/i,
	/\b(in this video|in this clip|in this segment|next video|next clip)\b/i,
	/\b(next|this|that|first|second|third|final)\s+segment\b/i,
	/\b(move on to the outro|moving to the outro|go to the outro)\b/i,
];

function isMetaSentence(sentence = "") {
	const s = String(sentence || "").toLowerCase();
	return META_SENTENCE_PATTERNS.some((rx) => rx.test(s));
}

function splitSentences(text = "") {
	const raw = String(text || "").trim();
	if (!raw) return [];
	const parts = raw.split(/([.!?])\s+/);
	const sentences = [];
	for (let i = 0; i < parts.length; i += 2) {
		const chunk = String(parts[i] || "").trim();
		const punct = String(parts[i + 1] || "").trim();
		const sentence = `${chunk}${punct}`.trim();
		if (sentence) sentences.push(sentence);
	}
	return sentences.length ? sentences : [raw];
}

function trimToSentenceCap(text = "", cap = 0) {
	const clean = String(text || "").trim();
	if (!clean) return clean;
	const limit = Number(cap) || 0;
	if (!limit) return clean;
	const words = clean.split(/\s+/).filter(Boolean);
	if (words.length <= limit) return clean;

	const sentences = splitSentences(clean);
	if (sentences.length <= 1) return clean;

	let count = 0;
	const kept = [];
	for (const sentence of sentences) {
		const w = countWords(sentence);
		if (!kept.length && w > limit) {
			return clean;
		}
		if (count + w > limit) break;
		kept.push(sentence);
		count += w;
	}
	const trimmed = cleanupSpeechText(kept.join(" "));
	return trimmed || clean;
}

function stripMetaNarration(text = "") {
	const raw = String(text || "").trim();
	if (!raw) return raw;
	const parts = splitSentences(raw);
	const kept = parts.filter((p) => !isMetaSentence(p));
	const cleaned = cleanupSpeechText(kept.join(" "));
	if (cleaned) return cleaned;
	const softened = raw
		.replace(/\b(outro|intro)\b/gi, "")
		.replace(
			/\b(in this video|in this clip|in this segment|next video|next clip)\b/gi,
			""
		)
		.replace(/\b(next|this|that|first|second|third|final)\s+segment\b/gi, "")
		.replace(/\s+/g, " ")
		.trim();
	return cleanupSpeechText(softened);
}

function stripFillerAndEmotes(
	text = "",
	state,
	{ maxFillers = 0, maxEmotes = 0 } = {}
) {
	const counter = state || { fillers: 0, emotes: 0 };
	let t = String(text || "");

	t = t.replace(FILLER_WORD_REGEX, (match) => {
		if (counter.fillers >= maxFillers) return "";
		counter.fillers += 1;
		return match;
	});
	t = t.replace(LIKE_FILLER_REGEX, (match, prefix) => {
		if (counter.fillers >= maxFillers) return prefix;
		counter.fillers += 1;
		return match;
	});
	t = t.replace(MICRO_EMOTE_REGEX, (match) => {
		if (!ENABLE_MICRO_EMOTES || counter.emotes >= maxEmotes) return "";
		counter.emotes += 1;
		return match;
	});

	return { text: cleanupSpeechText(t), state: counter };
}

function limitFillerAndEmotesAcrossSegments(segments = [], opts = {}) {
	const {
		maxFillers = 0,
		maxEmotes = 0,
		maxFillersPerSegment = maxFillers,
		maxEmotesPerSegment = maxEmotes,
		noFillerSegmentIndices = [],
	} = opts;
	const globalState = { fillers: 0, emotes: 0 };

	return (segments || []).map((seg, i) => {
		const segIndex = Number.isFinite(Number(seg.index)) ? Number(seg.index) : i;
		const allowFillers = !noFillerSegmentIndices.includes(segIndex);
		const perSegState = { fillers: 0, emotes: 0 };
		const segmentMaxFillers = allowFillers ? maxFillersPerSegment : 0;
		const segmentMaxEmotes = maxEmotesPerSegment;

		const perSegPass = stripFillerAndEmotes(seg.text, perSegState, {
			maxFillers: segmentMaxFillers,
			maxEmotes: segmentMaxEmotes,
		});

		const remainingFillers = Math.max(0, maxFillers - globalState.fillers);
		const remainingEmotes = Math.max(0, maxEmotes - globalState.emotes);
		const globalPass = stripFillerAndEmotes(perSegPass.text, globalState, {
			maxFillers: remainingFillers,
			maxEmotes: remainingEmotes,
		});

		return { ...seg, text: globalPass.text };
	});
}

function sanitizeIntroOutroLine(text = "") {
	const base = stripMetaNarration(text);
	const { text: cleaned } = stripFillerAndEmotes(
		base,
		{ fillers: 0, emotes: 0 },
		{ maxFillers: 0, maxEmotes: 0 }
	);
	return cleaned;
}

function stripAllFillers(text = "") {
	const base = stripMetaNarration(text);
	const { text: cleaned } = stripFillerAndEmotes(
		base,
		{ fillers: 0, emotes: 0 },
		{ maxFillers: 0, maxEmotes: 0 }
	);
	return cleaned;
}

function sanitizeSegmentText(text = "") {
	const cleaned = stripAllFillers(text);
	return cleaned || "Quick update.";
}

const REAL_WORLD_FICTIONAL_REWRITE_RULES = [
	{ regex: /(^|[.!?]\s+)in[-\s]?universe[:,]?\s+/gi, replace: "$1" },
	{ regex: /\bin[-\s]?universe\b/gi, replace: "" },
	{ regex: /\bfictional\b/gi, replace: "" },
	{ regex: /\bplotline\b/gi, replace: "story" },
	{ regex: /\bstoryline\b/gi, replace: "story" },
	{ regex: /\bcharacter arc\b/gi, replace: "story" },
	{ regex: /\bcanon\b/gi, replace: "record" },
	{ regex: /\blore\b/gi, replace: "background" },
];

function stripFictionalFraming(text = "") {
	let updated = String(text || "");
	for (const rule of REAL_WORLD_FICTIONAL_REWRITE_RULES) {
		updated = updated.replace(rule.regex, rule.replace);
	}
	updated = cleanupSpeechText(updated);
	return updated || String(text || "").trim();
}

function enforceRealWorldFraming(segments = [], topicContextFlags = []) {
	if (!Array.isArray(segments) || !segments.length) return segments;
	if (!Array.isArray(topicContextFlags) || !topicContextFlags.length)
		return segments;
	return segments.map((seg) => {
		const topicIndex =
			Number.isFinite(Number(seg.topicIndex)) && Number(seg.topicIndex) >= 0
				? Number(seg.topicIndex)
				: 0;
		const isFictional = Boolean(topicContextFlags?.[topicIndex]?.isFictional);
		if (isFictional) return seg;
		const cleaned = stripFictionalFraming(seg.text || "");
		if (!cleaned || cleaned === seg.text) return seg;
		return { ...seg, text: cleaned };
	});
}

const INTRO_TEMPLATES = {
	neutral: [
		"Hi, I'm Amad. Today: {topic}.",
		"Hi, I'm Amad. Covering {topic}.",
		"Hi, it's Amad. Here's {topic}.",
	],
	excited: [
		"Hi, I'm Amad. Big update on {topic}.",
		"Hi, I'm Amad. Let's get into {topic}.",
	],
	serious: [
		"Hi, I'm Amad. A quick update on {topic}.",
		"Hi, I'm Amad. The latest on {topic}.",
	],
};

function pickIntroTemplate(mood = "neutral", jobId) {
	const key = INTRO_TEMPLATES[mood] ? mood : "neutral";
	const pool = INTRO_TEMPLATES[key];
	if (!pool.length) return INTRO_TEMPLATES.neutral[0];
	const seed = jobId ? seedFromJobId(jobId) : 0;
	return pool[seed % pool.length];
}

function buildIntroLine({ topics = [], shortTitle, mood = "neutral", jobId }) {
	const normalizedMood = normalizeExpression(mood);
	const subject =
		normalizedMood === "serious"
			? formatTopicList(topics)
			: shortTitle
			? shortTopicLabel(shortTitle, 5)
			: formatTopicList(topics);
	const safeSubject = subject || "today's topic";
	const template = pickIntroTemplate(normalizedMood, jobId);
	const line = template.replace("{topic}", safeSubject);
	return sanitizeIntroOutroLine(line);
}

function buildTopicEngagementQuestionForLabel(
	topicLabel,
	mood = "neutral",
	{ compact = false, shortTitle = "" } = {}
) {
	const label = selectEngagementLabel({
		topicLabel,
		shortTitle,
		maxWords: compact ? 5 : 5,
	});
	if (!label) return "What do you think?";
	if (compact) return `Thoughts on ${label}?`;
	if (mood === "serious") return `What is your take on ${label}?`;
	return `What do you think about ${label}?`;
}

function buildTopicEngagementQuestion({
	topics = [],
	shortTitle,
	mood = "neutral",
	compact = false,
} = {}) {
	const topicLabels = (topics || [])
		.map((t) => shortTopicLabel(t?.displayTopic || t?.topic || t, 3))
		.filter(Boolean);
	const labels =
		topicLabels.length > 0
			? topicLabels
			: shortTitle
			? [shortTopicLabel(shortTitle, 4)]
			: [];
	if (!labels.length) return "What do you think?";
	if (labels.length === 1)
		return buildTopicEngagementQuestionForLabel(labels[0], mood, {
			compact,
			shortTitle,
		});

	if (compact)
		return mood === "serious"
			? "Which topic matters most to you?"
			: "Which topic stood out to you most?";

	const list = formatTopicList(labels);
	return mood === "serious"
		? `Which of these topics matters most to you: ${list}?`
		: `Which of these stood out to you most: ${list}?`;
}

function buildOutroLine({
	topics = [],
	shortTitle,
	mood = "neutral",
	includeQuestion = true,
}) {
	const question = includeQuestion
		? buildTopicEngagementQuestion({
				topics,
				shortTitle,
				mood,
				compact: true,
		  })
		: "";
	let line = includeQuestion
		? `${question} Thank you for watching, and see you next time.`
		: "Thank you for watching, and see you next time.";
	if (includeQuestion) {
		if (countWords(line) > 18) {
			line = `${question} Thank you for watching. See you next time.`;
		}
		if (countWords(line) > 14) {
			line = `${question} Thank you for watching.`;
		}
	} else {
		if (countWords(line) > 12) {
			line = "Thank you for watching. See you next time.";
		}
		if (countWords(line) < 12) {
			line =
				"Thank you for watching. We appreciate you, and we'll see you next time.";
		}
	}
	return sanitizeIntroOutroLine(line);
}

const SEGMENT_ENDING_BLOCKLIST = new Set([
	"and",
	"but",
	"so",
	"because",
	"with",
	"to",
	"for",
	"that",
]);

function endsWithTerminalPunctuation(text = "") {
	const t = String(text || "").trim();
	return /[.!?]["')\]]?$/.test(t);
}

function endsWithBlockedWord(text = "") {
	const t = String(text || "")
		.trim()
		.replace(/["')\]]+$/g, "")
		.replace(/[.!?,;:]+$/g, "");
	const parts = t.split(/\s+/).filter(Boolean);
	if (!parts.length) return false;
	return SEGMENT_ENDING_BLOCKLIST.has(parts[parts.length - 1].toLowerCase());
}

function hasOpenParenthetical(text = "") {
	const t = String(text || "").trim();
	if (/[([{]$/.test(t)) return true;
	const open = (t.match(/\(/g) || []).length;
	const close = (t.match(/\)/g) || []).length;
	return open > close;
}

function appendClosingPhrase(text = "", mood = "neutral") {
	const closer =
		mood === "serious"
			? "That's the key takeaway in this moment."
			: "That's the key takeaway right now.";
	return `${String(text || "").trim()} ${closer}`.trim();
}

function enforceCtaQuestion(text = "", mood = "neutral") {
	let t = String(text || "").trim();
	if (!t) t = "Quick final thought.";

	const hasSubscribe = /subscribe/i.test(t);
	const hasQuestionMark = /\?/.test(t);
	const subscribeStatement =
		mood === "serious" ? "Subscribe for updates." : "Subscribe for more.";
	const commentQuestion = "What do you think?";
	const combinedCta =
		mood === "serious"
			? "What do you think, and will you subscribe for updates?"
			: "What do you think, and will you subscribe for more?";

	if (hasQuestionMark) {
		// De-dup: keep one question and avoid repeating subscribe prompts.
		t = t.replace(/\?(?=[^?]*\?)/g, ".").trim();
		if (hasSubscribe) return t;
		const needsPunct = /[.!?]["')\]]*$/.test(t) ? "" : ".";
		return `${t}${needsPunct} ${subscribeStatement}`;
	}

	t = t.replace(/[.!?]+["')\]]*$/g, "").trim();
	if (!t) t = "Quick final thought.";
	if (hasSubscribe) return `${t}. ${commentQuestion}`;
	return `${t}. ${combinedCta}`;
}

function enforceSegmentCompleteness(
	segments = [],
	mood = "neutral",
	{ includeCta = true } = {}
) {
	return (segments || []).map((s, i, arr) => {
		const isLast = i === arr.length - 1;
		let text = String(s.text || "")
			.replace(/\s+/g, " ")
			.trim();
		const hadTrailingOpen = /[([{]$/.test(text);
		const hadBlockedEnding = endsWithBlockedWord(text);

		if (hadTrailingOpen) text = text.replace(/[([{]\s*$/g, "").trim();
		const openParen = (text.match(/\(/g) || []).length;
		const closeParen = (text.match(/\)/g) || []).length;
		if (openParen > closeParen) {
			if (endsWithTerminalPunctuation(text)) {
				text = text.replace(/([.!?]["')\]]*)$/, ")$1");
			} else {
				text = `${text})`;
			}
		}
		if (hadBlockedEnding) {
			text = text
				.replace(/\b(and|but|so|because|with|to|for|that)\b[.!?,;:]*$/i, "")
				.trim();
		}

		const needsClosure =
			hadTrailingOpen ||
			hadBlockedEnding ||
			hasOpenParenthetical(text) ||
			/[,:;]$/.test(text);
		if (needsClosure) text = appendClosingPhrase(text, mood);
		if (!endsWithTerminalPunctuation(text)) text = `${text}.`;

		if (isLast && includeCta) text = enforceCtaQuestion(text, mood);

		return { ...s, text };
	});
}

function trimSegmentToCap(text = "", cap = 0) {
	return trimToSentenceCap(text, cap);
}

const TOPIC_TRANSITION_TEMPLATES = [
	"Alright, switching gears to {topic}. Here's the quick read.",
	"Next up: {topic}. Here's the key update.",
	"Now pivoting to {topic}. Here's what matters.",
	"Alright, moving on to {topic}. Here's the latest.",
	"Turning to {topic}. Here's the headline.",
];

function dropIntroTransitionSentence(text = "") {
	const trimmed = String(text || "").trim();
	if (!trimmed) return "";
	const transitionRegex =
		/^(and now|now|next up|switching gears|turning to|moving on|pivoting|lets talk about|let\W*s talk about|we\W*re talking about|we are talking about)\b/i;
	if (!transitionRegex.test(trimmed)) return trimmed;
	const boundary = trimmed.search(/[.!?]\s+/);
	if (boundary >= 0) {
		const rest = trimmed.slice(boundary + 1).trim();
		if (rest) return rest;
	}
	return trimmed
		.replace(transitionRegex, "")
		.replace(/^[,:\-\s]+/, "")
		.trim();
}

function ensureTopicTransitions(segments = [], topics = []) {
	const out = [];
	let lastTopicIndex = null;

	for (let i = 0; i < (segments || []).length; i++) {
		const seg = segments[i];
		const topicIndex =
			Number.isFinite(Number(seg.topicIndex)) && Number(seg.topicIndex) >= 0
				? Number(seg.topicIndex)
				: 0;
		const topicLabel =
			String(seg.topicLabel || "").trim() ||
			String(
				topics[topicIndex]?.displayTopic || topics[topicIndex]?.topic || ""
			).trim();
		let text = String(seg.text || "").trim();

		if (i === 0) {
			text = dropIntroTransitionSentence(text);
		} else if (topicIndex !== lastTopicIndex && topicLabel) {
			const lower = text.toLowerCase();
			const topicLower = topicLabel.toLowerCase();
			const hasTransition =
				/^(and now|next up|now|switching gears|turning to|moving on|pivoting)/i.test(
					text
				);
			const mentionsTopic = topicLower && lower.includes(topicLower);
			if (!hasTransition || !mentionsTopic) {
				const template =
					TOPIC_TRANSITION_TEMPLATES[
						Math.abs(topicIndex + i) % TOPIC_TRANSITION_TEMPLATES.length
					];
				const transition = template.replace("{topic}", topicLabel).trim();
				text = `${transition} ${text}`.trim();
			}
		}

		out.push({
			...seg,
			topicIndex,
			topicLabel,
			text: cleanupSpeechText(text),
		});
		lastTopicIndex = topicIndex;
	}

	return out;
}

function ensureTopicAnchors(segments = [], topics = [], topicIntents = []) {
	const firstIndexByTopic = new Map();
	for (let i = 0; i < (segments || []).length; i++) {
		const topicIndex =
			Number.isFinite(Number(segments[i]?.topicIndex)) &&
			Number(segments[i]?.topicIndex) >= 0
				? Number(segments[i]?.topicIndex)
				: 0;
		if (!firstIndexByTopic.has(topicIndex))
			firstIndexByTopic.set(topicIndex, i);
	}

	return (segments || []).map((seg, idx) => {
		const topicIndex =
			Number.isFinite(Number(seg.topicIndex)) && Number(seg.topicIndex) >= 0
				? Number(seg.topicIndex)
				: 0;
		const intent = topicIntents?.[topicIndex] || {};
		const anchor = cleanTopicLabel(intent.anchor || "");
		if (!anchor || anchor.toLowerCase() === "today's topic") return seg;
		const text = String(seg.text || "").trim();
		if (!text) return seg;
		const lower = text.toLowerCase();
		const anchorLower = anchor.toLowerCase();
		if (lower.includes(anchorLower)) return seg;
		if (idx !== firstIndexByTopic.get(topicIndex)) return seg;

		const domain = intent.domain || "general";
		const prefix = domain === "fictional" ? `In ${anchor}, ` : `${anchor}: `;
		const merged = cleanupSpeechText(`${prefix}${text}`);
		return { ...seg, text: merged };
	});
}

function escapeRegExp(value = "") {
	return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function hasInstallmentEvidence(text = "") {
	const raw = String(text || "");
	return (
		/\b(season|episode|part|chapter|volume)\s*\d+\b/i.test(raw) ||
		/\bs\s*\d+\s*e\s*\d+\b/i.test(raw)
	);
}

function stripUnverifiedInstallmentDetails(
	text,
	{ anchor = "", allowInstallmentNumbers = false } = {}
) {
	if (!text || allowInstallmentNumbers) return text;
	let updated = String(text);
	const cleanedAnchor = cleanTopicLabel(anchor || "");
	const hasAnchor = Boolean(cleanedAnchor);
	if (hasAnchor) {
		const escaped = escapeRegExp(cleanedAnchor);
		updated = updated
			.replace(
				new RegExp(`\\b${escaped}\\s+\\d+\\s+episode\\s+\\d+\\b`, "gi"),
				cleanedAnchor
			)
			.replace(
				new RegExp(`\\b${escaped}\\s+episode\\s+\\d+\\b`, "gi"),
				cleanedAnchor
			)
			.replace(
				new RegExp(`\\b${escaped}\\s+season\\s+\\d+\\b`, "gi"),
				cleanedAnchor
			);
		if (!/\d/.test(cleanedAnchor)) {
			updated = updated.replace(
				new RegExp(`\\b${escaped}\\s+\\d+\\b`, "gi"),
				cleanedAnchor
			);
		}
	}
	updated = updated
		.replace(/\bseason\s*\d+\b/gi, "the season")
		.replace(/\bepisode\s*\d+\b/gi, "the episode")
		.replace(/\bpart\s*\d+\b/gi, "the part")
		.replace(/\bchapter\s*\d+\b/gi, "the chapter")
		.replace(/\bvolume\s*\d+\b/gi, "the volume")
		.replace(/\bs\s*\d+\s*e\s*\d+\b/gi, "the episode");
	if (hasAnchor) {
		const escaped = escapeRegExp(cleanedAnchor);
		updated = updated.replace(
			new RegExp(
				`\\b${escaped}\\s+the\\s+(episode|season|part|chapter|volume)\\b`,
				"gi"
			),
			cleanedAnchor
		);
	}
	return updated
		.replace(/\s{2,}/g, " ")
		.replace(/\s+,/g, ",")
		.trim();
}

function enforceTopicSpecificityGuards(
	segments = [],
	topics = [],
	topicContexts = [],
	topicIntents = []
) {
	const topicMeta = new Map();
	for (let i = 0; i < (topics || []).length; i++) {
		const label = String(
			topics[i]?.displayTopic || topics[i]?.topic || ""
		).trim();
		const contextItems = Array.isArray(topicContexts?.[i]?.context)
			? topicContexts[i].context
			: [];
		const contextText = contextItems
			.map((c) =>
				typeof c === "string" ? c : `${c.title || ""} ${c.snippet || ""}`
			)
			.join(" ");
		const combined = `${label} ${contextText}`.trim();
		const anchor = cleanTopicLabel(topicIntents?.[i]?.anchor || label || "");
		topicMeta.set(i, {
			anchor,
			allowInstallmentNumbers: hasInstallmentEvidence(combined),
		});
	}

	return (segments || []).map((seg) => {
		const idx =
			Number.isFinite(Number(seg.topicIndex)) && Number(seg.topicIndex) >= 0
				? Number(seg.topicIndex)
				: 0;
		const meta = topicMeta.get(idx);
		if (!meta) return seg;
		const updated = stripUnverifiedInstallmentDetails(seg.text, meta);
		if (!updated || updated === seg.text) return seg;
		return { ...seg, text: updated };
	});
}

function ensureTopicEngagementQuestions(
	segments = [],
	topics = [],
	mood = "neutral",
	wordCapsByIndex = []
) {
	const lastByTopic = new Map();
	for (let i = 0; i < (segments || []).length; i++) {
		const seg = segments[i];
		const topicIndex =
			Number.isFinite(Number(seg.topicIndex)) && Number(seg.topicIndex) >= 0
				? Number(seg.topicIndex)
				: 0;
		lastByTopic.set(topicIndex, i);
	}

	return (segments || []).map((seg, i) => {
		const topicIndex =
			Number.isFinite(Number(seg.topicIndex)) && Number(seg.topicIndex) >= 0
				? Number(seg.topicIndex)
				: 0;
		if (lastByTopic.get(topicIndex) !== i) return seg;

		const text = String(seg.text || "").trim();
		if (/\?/.test(text)) return seg;

		const topicLabel =
			String(seg.topicLabel || "").trim() ||
			String(
				topics[topicIndex]?.displayTopic || topics[topicIndex]?.topic || ""
			).trim();
		const question = buildTopicEngagementQuestionForLabel(topicLabel, mood, {
			compact: true,
		});
		const base = text.replace(/[.!?]+["')\]]*$/g, "").trim();
		const segIndex = Number.isFinite(Number(seg.index)) ? Number(seg.index) : i;
		const cap =
			Array.isArray(wordCapsByIndex) &&
			Number.isFinite(Number(wordCapsByIndex[segIndex]))
				? Number(wordCapsByIndex[segIndex])
				: null;

		let baseText = base;
		if (cap) {
			const questionWords = question.split(/\s+/).filter(Boolean);
			const allowedBaseWords = Math.max(0, cap - questionWords.length);
			if (allowedBaseWords > 0) {
				baseText = trimToSentenceCap(baseText, allowedBaseWords);
			}
		}

		const combined = cleanupSpeechText(
			`${baseText ? `${baseText}. ` : ""}${question}`.trim()
		);
		return { ...seg, text: combined };
	});
}

async function generateScript({
	jobId,
	topics = [],
	languageLabel,
	narrationTargetSec,
	segmentCount,
	wordCaps,
	topicContexts,
	tonePlan,
	topicContextFlags = [],
	includeOutro = false,
}) {
	if (!process.env.CHATGPT_API_TOKEN)
		throw new Error("CHATGPT_API_TOKEN missing");

	const safeTopics =
		Array.isArray(topics) && topics.length
			? topics.filter((t) => t && t.topic)
			: [{ topic: "today's topic" }];
	const topicCount = safeTopics.length;
	const topicLabelFor = (t) => String(t?.displayTopic || t?.topic || "").trim();
	const topicRanges = allocateTopicSegments(segmentCount, safeTopics);
	const capsLine = wordCaps.map((c, i) => `#${i}: <= ${c} words`).join(", ");
	const mood = tonePlan?.mood || "neutral";
	const deepDiveGuide =
		topicCount === 1
			? "Single-topic deep dive: spend more time on background, timeline, key evidence, and implications while staying concise and non-repetitive."
			: "";
	const outroGuide = includeOutro
		? "Last segment: clean wrap that naturally closes the story and leaves space for the closing line (no like/subscribe CTA)."
		: "Last segment: wrap + CTA question.";
	const toneGuide =
		mood === "serious"
			? `Segment 0: measured, serious tone, slower pacing. ${outroGuide}`
			: mood === "excited"
			? `Segment 0: high-energy, upbeat hook. ${outroGuide}`
			: `Segment 0: confident, neutral hook. ${outroGuide}`;
	const ctaLine = includeOutro
		? "End the LAST segment of EACH topic with one short, topic-specific engagement question for comments. Do NOT add like/subscribe in content; the closing line only says thank you and see you next time."
		: "Last segment ends with ONE short CTA question (comment + subscribe).";

	const topicPlanLines = topicRanges
		.map((r) => {
			const label =
				topicLabelFor(safeTopics[r.topicIndex]) || `Topic ${r.topicIndex + 1}`;
			return `- Topic ${r.topicIndex + 1} (${label}): segments ${
				r.startIndex
			}-${r.endIndex}`;
		})
		.join("\n");

	const topicHintLines = safeTopics
		.map((t, i) => {
			const hints = uniqueStrings(
				[
					...(Array.isArray(t.keywords) ? t.keywords : []),
					...(t.trendStory?.searchPhrases || []),
					...(t.trendStory?.entityNames || []),
					...(Array.isArray(t.trendStory?.relatedQueries?.rising)
						? t.trendStory.relatedQueries.rising
						: []),
					...(Array.isArray(t.trendStory?.relatedQueries?.top)
						? t.trendStory.relatedQueries.top
						: []),
				],
				{ limit: 8 }
			);
			const articles = (t.trendStory?.articles || [])
				.map((a) => a.title)
				.filter(Boolean)
				.slice(0, 3);
			return `Topic ${i + 1}: ${topicLabelFor(t) || t.topic}\n- Hints: ${
				hints.length ? hints.join(", ") : "(none)"
			}\n- Articles: ${articles.length ? articles.join(" | ") : "(none)"}`;
		})
		.join("\n\n");
	const trendSignalLines = buildTrendSignalLines(safeTopics);

	const topicIntents = safeTopics.map((t, idx) => {
		const contextItems = Array.isArray(topicContexts)
			? topicContexts[idx]?.context
			: [];
		return buildTopicIntentSummary(t, contextItems);
	});
	const topicIntentLines = topicIntents
		.map((intent, idx) => {
			const label =
				topicLabelFor(safeTopics[idx]) || intent?.label || `Topic ${idx + 1}`;
			const anchor = String(intent?.anchor || "(unknown)")
				.replace(/"/g, "")
				.trim();
			const domain = intent?.domain || "general";
			const medium = intent?.medium ? ` | medium=${intent.medium}` : "";
			const evidence = String(intent?.evidence || "(none)")
				.replace(/\s+/g, " ")
				.trim();
			return `- Topic ${
				idx + 1
			} (${label}): anchor="${anchor}" | domain=${domain}${medium}\n  Evidence: ${evidence}`;
		})
		.join("\n");

	const topicContextGuide =
		Array.isArray(topicContextFlags) && topicContextFlags.length
			? topicContextFlags
					.map((flag, idx) => {
						const label =
							topicLabelFor(safeTopics[idx]) ||
							flag?.topic ||
							`Topic ${idx + 1}`;
						if (flag?.isFictional) {
							return `- Topic ${
								idx + 1
							} (${label}): Fictional or in-universe discussion. Frame as plot/character analysis. Do NOT imply a real person died or use condolence language.`;
						}
						return `- Topic ${
							idx + 1
						} (${label}): Real-world coverage. Keep it factual and grounded. Avoid any in-universe or fictional framing.`;
					})
					.join("\n")
			: "- (none)";

	const contextLines =
		Array.isArray(topicContexts) && topicContexts.length
			? topicContexts
					.map((tc, idx) => {
						const items = Array.isArray(tc.context) ? tc.context : [];
						const lineItems = items
							.map((c) => {
								if (typeof c === "string") return c;
								const title = String(c?.title || "").trim();
								const snippet = String(c?.snippet || "").trim();
								const sourceHost = getUrlHost(c?.link || "");
								const sourceTag = sourceHost ? ` (source: ${sourceHost})` : "";
								if (!title && !snippet) return "";
								return `${title}${snippet ? " | " + snippet : ""}${sourceTag}`;
							})
							.filter(Boolean)
							.slice(0, 5);
						return `Topic ${idx + 1} (${tc.topic}):\n${
							lineItems.length
								? lineItems.map((l) => `- ${l}`).join("\n")
								: "- (no context)"
						}`;
					})
					.join("\n\n")
			: "- (no live context)";
	const sourceLines =
		Array.isArray(topicContexts) && topicContexts.length
			? topicContexts
					.map((tc, idx) => {
						const items = Array.isArray(tc.context) ? tc.context : [];
						const sources = uniqueStrings(
							items
								.map((c) =>
									typeof c === "string" ? "" : getUrlHost(c?.link || "")
								)
								.filter(Boolean),
							{ limit: 6 }
						);
						return `Topic ${idx + 1} (${tc.topic}): ${
							sources.length ? sources.join(", ") : "(none)"
						}`;
					})
					.join("\n")
			: "- (none)";

	const prompt = `
Current date: ${dayjs().format("YYYY-MM-DD")}

Write a YouTube talking-head script for a US audience.
This is a multi-topic news brief.
Language: ${languageLabel}
Tone plan: ${mood} (${toneGuide})

Topics in order (do NOT change order):
${safeTopics
	.map((t, i) => `${i + 1}) ${topicLabelFor(t) || t.topic}`)
	.join("\n")}

Segment allocation (follow exactly):
${topicPlanLines}

${deepDiveGuide}

Target narration duration (NOT counting intro/outro): ~${narrationTargetSec.toFixed(
		1
	)}s
Segments: EXACTLY ${segmentCount}
Per-segment word caps: ${capsLine}

Use this background context as hints; do NOT pretend its real-time verified:
${contextLines}

Sources for attribution (use when referencing facts):
${sourceLines}

Topic context guidance:
${topicContextGuide}

Topic notes:
${topicHintLines}

Trending signals (address the #1 rising reason early if present):
${trendSignalLines}

Topic intent resolution (MUST follow; do NOT invent beyond this):
${topicIntentLines}

Style rules (IMPORTANT):
- Keep pacing steady and conversational; no sudden speed-ups.
- Slightly brisk, natural American delivery; avoid drawn-out phrasing.
- Sound like a real creator, not a press release. No "Ladies and gentlemen", no "In conclusion", no corporate tone.
- Keep it lightly casual: a few friendly, natural phrases like "real quick" or "here's the thing" (max 1 per topic), but stay professional.
- Use contractions. Punchy sentences. A little playful, but not cringe.
- Avoid staccato punctuation. Do NOT put commas between single words.
- Keep punctuation light and flowing; prefer smooth, natural sentences.
- Lead with the answer, then add context (what happened, why it matters, what to watch for).
- Prioritize genuinely interesting facts (history, timeline, behind-the-scenes, credible rumors, estimates) without overstating.
- If you mention a rumor or estimate, label it clearly as unconfirmed and attribute it (\"reports suggest\", \"according to [source]\").
- Include at least one brief source attribution per topic using the provided context (e.g., \"According to Variety...\").
- Target duration is a guideline; if clarity needs more time, it's OK to run longer, but still try to stay close to the target.
- Avoid repeating the topic question or using vague filler phrasing; be specific and helpful.
- Avoid repeating the headline or the same fact across segments; each segment must add a new detail or angle.
- No redundancy: do not restate the same fact or idea in different words.
- Avoid exclamation points unless the script explicitly calls for excitement.
- Each segment should be 1-2 sentences. Do NOT switch topics mid-sentence.
- Stay close to the per-segment word caps (aim ~90-100% of each cap); do not be significantly shorter.
- Avoid specific dates, rankings, or stats unless they appear in the provided context above.
- Do NOT invent season/episode/part/chapter numbers. Only mention numbered installments if they appear in the topic label or provided context; otherwise say "the episode" or "the season" without numbers.
- Avoid filler words ("um", "uh", "umm", "uhm", "ah", "like"). Use zero filler words in the entire script, especially in segments 0-2.
- Do NOT add micro vocalizations ("heh", "whew", "hmm").
- Do NOT mention "intro", "outro", "segment", "next segment", or say "in this video/clip".
- Segment 0 must be a strong hook that makes people stay.
- Segment 0 should open with a tension/contrast line (what people assume vs what the evidence actually shows) in the first sentence.
- Do NOT start segment 0 with "Quick update on..." or restate the intro line; the intro handles that.
- Segment 0 should read like the very next sentence after the intro, continuing the same thought without reintroducing the topic.
- Do NOT start segment 0 with transition phrases like "And now", "Now", "Next up", or "Let's talk about".
- Segment 0 follows the tone plan; middle segments stay conversational/neutral; last segment wraps with the tone plan.
- Each topic is its own mini story with clear transitions.
- Make topic handoffs feel smooth and coherent; use a brief bridge phrase to set up the next topic.
- For Topic 2+ only, the FIRST segment must START with an explicit transition line that names the topic. Do NOT use that transition for Topic 1.
- The FIRST segment for every topic must mention the topic name in the first sentence.
- Each segment should naturally flow into the next with a quick transition phrase.
- Each segment ends with a complete sentence and strong terminal punctuation. Do NOT end with "and", "but", "so", "because", "with", "to", "for", "that", or an open parenthetical.
- No long lists. If you must list, cap at 3 items.
- If you are unsure about a detail, say "reports suggest" or "early signs".
- ${ctaLine}
- Topic questions must be short and end with a single question mark.
- Provide "shortTitle": 2-5 words, punchy and easy to read.
- For each segment, include "expression" from: neutral, warm, serious, excited, thoughtful.
- Default to neutral for most segments. Use warm/thoughtful sparingly (1-2 middle segments max) and keep it subtle.
- If the topic is sad or serious, use neutral (no exaggerated sadness).
- ONLY if a topic is about a TV show, film, or fictional character, frame it as plot/character discussion, not real-life tragedy.
- ONLY if a topic is marked as Fictional/Story, keep it in-universe and avoid real-world mourning language.
- If a topic is real-world, do NOT use in-universe/fictional framing or words like "in-universe", "fictional", "plotline", "storyline", "canon", "lore".
- Avoid phrasing like "sad news" unless it is a real-world tragedy.
- Use the topic anchor phrase in the FIRST segment of each topic.
- If a topic's evidence is "(none)", keep statements high-level and avoid specific claims; say it's trending and frame it as an open question.
- If the line is happy, use a light smile; if very happy, a brief small smile with slight teeth (never a wide grin).
- Keep expressions coherent across segments; avoid abrupt mood flips and avoid exaggerated expressions.
- Each segment must include EXACTLY one overlayCues entry with a search query that matches that segment.
- overlayCues.query must be 2-6 words, describe a real photo to search for, include the topic name or a key subject from that segment, no punctuation or hashtags.
- overlayCues.query must name a concrete visual detail from the segment (person, work, location, event). Avoid generic words like "news", "update", "story".
- overlayCues.startPct and endPct must be between 0.2 and 0.85, with endPct at least 0.2 greater than startPct.
- overlayCues.position must be "topRight" only.

Return JSON ONLY:
{
  "title": "...",
  "shortTitle": "...",
  "segments": [
	{
	  "index": 0,
	  "topicIndex": 0,
	  "topicLabel": "...",
	  "text": "...",
	  "expression": "neutral|warm|serious|excited|thoughtful",
	  "overlayCues": [ { "query":"...", "startPct":0.25, "endPct":0.75, "position":"topRight" } ]
	}
  ]
}
`.trim();

	const resp = await openai.chat.completions.create({
		model: CHAT_MODEL,
		messages: [{ role: "user", content: prompt }],
	});

	const parsed = parseJsonFlexible(resp?.choices?.[0]?.message?.content || "");
	if (!parsed || !Array.isArray(parsed.segments))
		throw new Error("OpenAI script JSON parse failed");

	const topicIndexForSegment = (idx) => {
		const match = topicRanges.find(
			(r) => Number(idx) >= r.startIndex && Number(idx) <= r.endIndex
		);
		return match ? match.topicIndex : 0;
	};

	let segments = parsed.segments
		.map((s, idx) => {
			const index = Number.isFinite(Number(s.index)) ? Number(s.index) : idx;
			const rawTopicIndex = Number(s.topicIndex);
			const topicIndex =
				Number.isFinite(rawTopicIndex) &&
				rawTopicIndex >= 0 &&
				rawTopicIndex < safeTopics.length
					? rawTopicIndex
					: topicIndexForSegment(index);
			const topicLabel =
				String(s.topicLabel || "").trim() ||
				topicLabelFor(safeTopics[topicIndex]) ||
				String(safeTopics[topicIndex]?.topic || "").trim();
			return {
				index,
				topicIndex,
				topicLabel,
				text: String(s.text || "").trim(),
				expression: normalizeExpression(s.expression, mood),
				overlayCues: Array.isArray(s.overlayCues) ? s.overlayCues : [],
			};
		})
		.filter((s) => s.text);

	segments = segments.map((s) => ({
		...s,
		expression: coerceExpressionForNaturalness(s.expression, s.text, mood),
	}));

	// Force exact segment count
	if (segments.length !== segmentCount) {
		segments = segments.slice(0, segmentCount);
		while (segments.length < segmentCount) {
			const idx = segments.length;
			const topicIndex = topicIndexForSegment(idx);
			segments.push({
				index: idx,
				topicIndex,
				topicLabel:
					topicLabelFor(safeTopics[topicIndex]) ||
					String(safeTopics[topicIndex]?.topic || "").trim(),
				text: "Quick transition and here is the key detail you should watch.",
				overlayCues: [],
			});
		}
	}

	segments = ensureTopicTransitions(segments, safeTopics);
	segments = ensureTopicAnchors(segments, safeTopics, topicIntents);
	segments = enforceTopicSpecificityGuards(
		segments,
		safeTopics,
		topicContexts,
		topicIntents
	);
	segments = enforceRealWorldFraming(segments, topicContextFlags);

	// Enforce caps softly (avoid mid-sentence cutoffs; allow longer if needed).
	segments = segments.map((s, i) => {
		const cap = wordCaps[i] || 22;
		const trimmed = trimSegmentToCap(s.text, cap);
		if (trimmed === s.text) return s;
		return { ...s, text: trimmed };
	});

	segments = ensureTopicEngagementQuestions(
		segments,
		safeTopics,
		mood,
		wordCaps
	);

	// Ensure clean segment endings and CTA consistency.
	segments = enforceSegmentCompleteness(segments, mood, {
		includeCta: !includeOutro,
	});
	segments = limitFillerAndEmotesAcrossSegments(segments, {
		maxFillers: MAX_FILLER_WORDS_PER_VIDEO,
		maxFillersPerSegment: MAX_FILLER_WORDS_PER_SEGMENT,
		maxEmotes: MAX_MICRO_EMOTES_PER_VIDEO,
		maxEmotesPerSegment: MAX_MICRO_EMOTES_PER_VIDEO,
		noFillerSegmentIndices: [0, 1, 2],
	});
	segments = segments.map((s) => ({
		...s,
		text: sanitizeSegmentText(s.text),
	}));
	// Smooth expressions so adjacent segments stay coherent.
	const smoothed = smoothExpressionPlan(
		segments.map((s) => s.expression),
		mood
	);
	segments = segments.map((s, i) => ({ ...s, expression: smoothed[i] }));

	const fallbackTitle = safeTopics.map((t) => t.topic).join(" | ");
	const finalTitle = String(parsed.title || fallbackTitle)
		.trim()
		.slice(0, 120);
	const finalShortTitle = shortTitleFromText(
		String(parsed.shortTitle || "").trim() || finalTitle
	).slice(0, 60);

	logJob(jobId, "script ready", {
		title: finalTitle,
		shortTitle: finalShortTitle,
		segments: segments.length,
		words: segments.reduce((a, s) => a + countWords(s.text), 0),
	});

	return {
		title: finalTitle,
		shortTitle: finalShortTitle,
		segments,
	};
}

function buildTrendSignalLines(topics = []) {
	const list = Array.isArray(topics) ? topics : [];
	if (!list.length) return "- (none)";
	return list
		.map((t, idx) => {
			const label =
				String(t?.displayTopic || t?.topic || "").trim() || `Topic ${idx + 1}`;
			const related = normalizeRelatedQueries(t?.trendStory?.relatedQueries);
			const interest = normalizeInterestOverTime(
				t?.trendStory?.interestOverTime
			);
			const rising = related.rising.slice(0, 4);
			const top = related.top.slice(0, 4);
			const interestLine =
				interest.points > 0
					? `interest(avg=${interest.avg}, latest=${interest.latest}, peak=${interest.peak})`
					: "";
			return `- Topic ${idx + 1} (${label}): rising=${
				rising.length ? rising.join(", ") : "(none)"
			}; top=${top.length ? top.join(", ") : "(none)"}${
				interestLine ? ` | ${interestLine}` : ""
			}`;
		})
		.join("\n");
}

function extractTrendSignalTokens(relatedQueries = null) {
	const related = normalizeRelatedQueries(relatedQueries);
	const list = uniqueStrings(
		[...related.rising, ...related.top].filter(Boolean),
		{ limit: 12 }
	);
	if (!list.length) return [];
	const tokens = list.flatMap((q) => tokenizeQaText(q));
	return uniqueStrings(tokens, { limit: 12 });
}

function assessTrendSignalCoverage(script = {}, topics = []) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length || !Array.isArray(topics) || !topics.length) {
		return { missingTopics: [], coverage: [] };
	}
	const byTopic = new Map();
	for (const seg of segments) {
		const topicIndex =
			Number.isFinite(Number(seg.topicIndex)) && Number(seg.topicIndex) >= 0
				? Number(seg.topicIndex)
				: 0;
		const prev = byTopic.get(topicIndex) || "";
		byTopic.set(topicIndex, `${prev} ${seg.text || ""}`.trim());
	}
	const coverage = [];
	const missingTopics = [];
	for (let i = 0; i < topics.length; i++) {
		const topic = topics[i] || {};
		const tokens = extractTrendSignalTokens(topic?.trendStory?.relatedQueries);
		if (!tokens.length) continue;
		const text = String(byTopic.get(i) || "").toLowerCase();
		const hits = tokens.filter((tok) => text.includes(tok));
		const ok = hits.length > 0;
		coverage.push({
			topicIndex: i,
			tokens: tokens.slice(0, 6),
			hits: hits.slice(0, 6),
		});
		if (!ok) missingTopics.push(i);
	}
	return { missingTopics, coverage };
}

function analyzeScriptQuality({
	script,
	topics = [],
	topicContexts = [],
	wordCaps = [],
}) {
	const issues = [];
	const warnings = [];
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const title = String(script?.title || "").trim();
	const shortTitle = String(script?.shortTitle || "").trim();

	if (!title || countWords(title) < 2) {
		issues.push("title_missing_or_too_short");
	}
	if (!shortTitle || countWords(shortTitle) < 2) {
		warnings.push("short_title_missing_or_too_short");
	}
	if (!segments.length) {
		issues.push("segments_missing");
	}

	const tokenSets = segments.map((s) => tokenizeQaText(s.text || ""));
	const duplicatePairs = [];
	for (let i = 0; i < segments.length; i++) {
		const a = normalizeQaText(segments[i]?.text || "");
		const aCount = countWords(a);
		for (let j = i + 1; j < segments.length; j++) {
			const b = normalizeQaText(segments[j]?.text || "");
			if (!a || !b) continue;
			if (a === b) {
				duplicatePairs.push([i, j]);
				continue;
			}
			const bCount = countWords(b);
			if (aCount < QA_MIN_SEGMENT_WORDS || bCount < QA_MIN_SEGMENT_WORDS)
				continue;
			const ratio = overlapRatio(tokenSets[i], tokenSets[j]);
			if (ratio >= QA_SIMILARITY_THRESHOLD) duplicatePairs.push([i, j]);
		}
	}
	if (duplicatePairs.length) {
		warnings.push("segment_redundancy_detected");
	}

	const shortSegments = segments.filter(
		(s) => countWords(s.text) < QA_MIN_SEGMENT_WORDS
	);
	if (shortSegments.length) warnings.push("short_segments_detected");

	const sourceTokensByTopic = new Map();
	for (let i = 0; i < (topics || []).length; i++) {
		const ctx = Array.isArray(topicContexts?.[i]?.context)
			? topicContexts[i].context
			: [];
		sourceTokensByTopic.set(i, extractSourceTokensFromContext(ctx));
	}

	const missingAttributionTopics = [];
	for (let i = 0; i < (topics || []).length; i++) {
		const tokens = sourceTokensByTopic.get(i) || [];
		if (!tokens.length) continue;
		const topicSegments = segments.filter((s) => Number(s.topicIndex) === i);
		const hasAttribution = topicSegments.some((s) =>
			segmentHasAttribution(s.text || "", tokens)
		);
		if (!hasAttribution) missingAttributionTopics.push(i);
	}
	if (missingAttributionTopics.length)
		warnings.push("missing_attribution_by_topic");

	const trendCoverage = assessTrendSignalCoverage(script, topics);
	if (trendCoverage.missingTopics.length)
		warnings.push("missing_trend_signal_coverage");

	const needsRewrite =
		duplicatePairs.length > 0 ||
		missingAttributionTopics.length > 0 ||
		trendCoverage.missingTopics.length > 0;
	const hasCritical = issues.length > 0;

	return {
		pass: !hasCritical,
		needsRewrite,
		hasCritical,
		issues,
		warnings,
		stats: {
			segmentCount: segments.length,
			duplicatePairs,
			shortSegments: shortSegments.map((s) => s.index),
			missingAttributionTopics,
			missingTrendSignalTopics: trendCoverage.missingTopics,
		},
	};
}

function buildScriptLogText(script = {}) {
	const title = String(script?.title || "").trim();
	const shortTitle = String(script?.shortTitle || "").trim();
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const lines = [];
	if (title) lines.push(`TITLE: ${title}`);
	if (shortTitle) lines.push(`SHORT: ${shortTitle}`);
	for (let i = 0; i < segments.length; i++) {
		const seg = segments[i] || {};
		const idx = Number.isFinite(Number(seg.index)) ? Number(seg.index) : i;
		const topicLabel = String(seg.topicLabel || "").trim();
		const expr = String(seg.expression || "").trim();
		const headerParts = [`#${idx}`];
		if (topicLabel) headerParts.push(`topic=${topicLabel}`);
		if (expr) headerParts.push(`expr=${expr}`);
		const text = String(seg.text || "").trim();
		lines.push(`${headerParts.join(" | ")}: ${text}`);
	}
	return lines.join("\n");
}

function summarizeScriptEngagement(script = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const totalWords = segments.reduce((sum, s) => sum + countWords(s.text), 0);
	const avgWords =
		segments.length > 0 ? totalWords / segments.length : totalWords;
	const questionSegments = segments.filter((s) =>
		/\?/.test(String(s.text || ""))
	).length;
	const fullText = segments
		.map((s) => String(s.text || "").toLowerCase())
		.join(" ");
	const countTokenHits = (tokens = []) =>
		(tokens || []).reduce(
			(count, tok) =>
				fullText.includes(String(tok || "").toLowerCase()) ? count + 1 : count,
			0
		);
	return {
		segmentCount: segments.length,
		totalWords,
		avgWords: Number(avgWords.toFixed(1)),
		questionSegments,
		questionRatio: Number(
			(segments.length ? questionSegments / segments.length : 0).toFixed(2)
		),
		trendTokenHits: countTokenHits(TREND_SIGNAL_TOKENS),
		excitedTokenHits: countTokenHits(EXCITED_TONE_TOKENS),
		entertainmentTokenHits: countTokenHits(ENTERTAINMENT_KEYWORDS),
	};
}

function formatSourceLabel(host = "") {
	const cleaned = String(host || "")
		.replace(/^www\./i, "")
		.trim();
	if (!cleaned) return "";
	const base = cleaned.replace(
		/\.(com|net|org|co|us|uk|io|tv|info|biz|gov)$/i,
		""
	);
	const words = base
		.replace(/[^a-z0-9]+/gi, " ")
		.split(/\s+/)
		.filter(Boolean);
	if (!words.length) return cleaned;
	return words
		.map((w) =>
			w.length <= 3 ? w.toUpperCase() : w[0].toUpperCase() + w.slice(1)
		)
		.join(" ");
}

const SOURCE_HOST_DEPRIORITY = new Set([
	"wikipedia.org",
	"imdb.com",
	"fandom.com",
	"wikia.com",
	"twitter.com",
	"x.com",
	"facebook.com",
	"instagram.com",
	"tiktok.com",
	"youtube.com",
]);

function normalizeSourceHost(host = "") {
	return String(host || "")
		.toLowerCase()
		.replace(/^www\./i, "")
		.trim();
}

function isDeprioritizedSourceHost(host = "") {
	const normalized = normalizeSourceHost(host);
	if (!normalized) return false;
	for (const entry of SOURCE_HOST_DEPRIORITY) {
		if (normalized === entry || normalized.endsWith(`.${entry}`)) return true;
	}
	return false;
}

function buildSourceTokensFromHosts(hosts = []) {
	const tokens = new Set();
	for (const host of Array.isArray(hosts) ? hosts : []) {
		const lowered = String(host || "")
			.toLowerCase()
			.trim();
		if (!lowered) continue;
		tokens.add(lowered);
		const base = lowered.replace(
			/\.(com|net|org|co|us|uk|io|tv|info|biz|gov)$/i,
			""
		);
		const cleaned = base.replace(/[^a-z0-9]+/g, " ").trim();
		if (cleaned) tokens.add(cleaned);
	}
	return Array.from(tokens);
}

function pickTopicSourceHosts(
	topic,
	topicContext = [],
	{ preferArticles = false } = {}
) {
	const ctx = Array.isArray(topicContext) ? topicContext : [];
	const ctxHosts = ctx.map((c) => getUrlHost(c?.link || "")).filter(Boolean);
	const articleHosts = Array.isArray(topic?.trendStory?.articles)
		? topic.trendStory.articles
				.map((a) => getUrlHost(a?.url || ""))
				.filter(Boolean)
		: [];
	if (!preferArticles) {
		return uniqueStrings([...ctxHosts, ...articleHosts], { limit: 6 });
	}
	const prioritizedCtxHosts = articleHosts.length
		? [
				...ctxHosts.filter((host) => !isDeprioritizedSourceHost(host)),
				...ctxHosts.filter((host) => isDeprioritizedSourceHost(host)),
		  ]
		: ctxHosts;
	return uniqueStrings([...articleHosts, ...prioritizedCtxHosts], { limit: 6 });
}

function ensureTopicAttributions({
	script,
	topics = [],
	topicContexts = [],
	topicContextFlags,
	wordCaps = [],
	log,
} = {}) {
	const segments = Array.isArray(script?.segments)
		? script.segments.map((s) => ({ ...s }))
		: [];
	if (!segments.length) return { segments, didInsert: false, inserted: [] };

	const inserted = [];
	for (let i = 0; i < (topics || []).length; i++) {
		const ctx = Array.isArray(topicContexts?.[i]?.context)
			? topicContexts[i].context
			: [];
		const contentType = String(
			topicContextFlags?.contentType || ""
		).toLowerCase();
		const topicFlag = Array.isArray(topicContextFlags?.topics)
			? topicContextFlags.topics[i]
			: null;
		const preferArticles =
			contentType === "real" || topicFlag?.isFictional === false;
		const sourceHosts = pickTopicSourceHosts(topics[i], ctx, {
			preferArticles,
		});
		if (!sourceHosts.length) continue;
		let sourceTokens = extractSourceTokensFromContext(ctx);
		if (!sourceTokens.length)
			sourceTokens = buildSourceTokensFromHosts(sourceHosts);
		const topicSegments = segments.filter((s) => Number(s.topicIndex) === i);
		const hasAttribution = topicSegments.some((s) =>
			segmentHasAttribution(s.text || "", sourceTokens)
		);
		if (hasAttribution) continue;

		const targetIndex = segments.findIndex((s) => Number(s.topicIndex) === i);
		if (targetIndex < 0) continue;
		const sourceLabel = formatSourceLabel(sourceHosts[0]);
		if (!sourceLabel) continue;
		const prefix = `According to ${sourceLabel}, `;
		const baseText = String(segments[targetIndex].text || "").trim();
		let updated = baseText.startsWith(prefix)
			? baseText
			: `${prefix}${baseText}`;
		const cap =
			Array.isArray(wordCaps) && Number.isFinite(Number(wordCaps[targetIndex]))
				? Number(wordCaps[targetIndex])
				: null;
		if (cap) updated = trimSegmentToCap(updated, cap);
		updated = sanitizeSegmentText(updated);
		segments[targetIndex] = { ...segments[targetIndex], text: updated };
		inserted.push({ topicIndex: i, segmentIndex: targetIndex, sourceLabel });
	}

	if (log && inserted.length) log("script attribution inserted", { inserted });
	if (inserted.length && script && Array.isArray(script.segments)) {
		script.segments = segments;
	}
	return { segments, didInsert: inserted.length > 0, inserted };
}

async function rewriteSegmentsForQuality({
	jobId,
	script,
	topics = [],
	topicContexts = [],
	topicContextFlags = [],
	wordCaps = [],
	tonePlan,
	narrationTargetSec,
	includeOutro = true,
}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return script;
	const mood = tonePlan?.mood || "neutral";

	const topicSummaries = (topics || []).map((t, idx) => {
		const ctx = Array.isArray(topicContexts?.[idx]?.context)
			? topicContexts[idx].context
			: [];
		const intent = buildTopicIntentSummary(t, ctx);
		const isFictional = Boolean(topicContextFlags?.[idx]?.isFictional);
		const contextLabel = isFictional ? "fictional" : "real-world";
		const sources = uniqueStrings(
			ctx.map((c) => getUrlHost(c?.link || "")).filter(Boolean),
			{ limit: 6 }
		);
		return `Topic ${idx + 1} (${
			intent.label || t?.topic || "topic"
		}): context=${contextLabel} | anchor="${intent.anchor || ""}" | evidence="${
			intent.evidence || ""
		}" | sources=${sources.length ? sources.join(", ") : "(none)"}`;
	});

	const capsLine = wordCaps.map((c, i) => `#${i}: <= ${c} words`).join(", ");
	const topicLine = segments
		.map(
			(s) =>
				`#${s.index}: topic ${s.topicIndex} (${
					s.topicLabel || topics?.[s.topicIndex]?.topic || ""
				})`
		)
		.join(", ");
	const trendSignalLines = buildTrendSignalLines(topics);

	const rewritePrompt = `
Improve this script for clarity, interesting facts, and attribution.
Target narration duration: ~${Number(narrationTargetSec || 0).toFixed(1)}s
Mood: ${mood}
Topic summaries:
${topicSummaries.join("\n")}

Trending signals (address the #1 rising reason early if present):
${trendSignalLines}

Topic assignment by segment (do NOT change):
${topicLine}

Per-segment word caps: ${capsLine}

Rules:
- Keep EXACTLY ${segments.length} segments with the same indexes.
- Keep the same topic order and assignments.
- No redundancy: each segment adds a new detail or angle with concrete, interesting facts.
- Add at least one short attribution per topic when sources are available (e.g., "According to Variety...").
- If you mention rumors or estimates, label them clearly as unconfirmed.
- If a topic is real-world, do NOT use in-universe/fictional framing or words like "in-universe", "fictional", "plotline", "storyline", "canon", "lore".
- Keep it conversational and clear; no filler words.
- End the last segment of each topic with a short engagement question.
- Do NOT add like/subscribe CTAs.

Return JSON ONLY:
{ "segments":[{"index":0,"text":"..."}] }

Script:
${segments.map((s) => `#${s.index}: ${s.text}`).join("\n")}
`.trim();

	const resp = await openai.chat.completions.create({
		model: CHAT_MODEL,
		messages: [{ role: "user", content: rewritePrompt }],
	});
	const parsed = parseJsonFlexible(resp?.choices?.[0]?.message?.content || "");
	if (!parsed || !Array.isArray(parsed.segments)) return script;

	const textByIndex = new Map();
	for (const seg of parsed.segments) {
		const idx = Number(seg?.index);
		if (!Number.isFinite(idx)) continue;
		const text = String(seg?.text || "").trim();
		if (text) textByIndex.set(idx, text);
	}

	let updated = segments.map((s, i) => {
		const nextText = textByIndex.get(i) || s.text;
		const cap = wordCaps[i] || 22;
		const trimmed = trimSegmentToCap(String(nextText || ""), cap);
		return { ...s, text: sanitizeSegmentText(trimmed) };
	});

	const topicIntents = (topics || []).map((t, idx) => {
		const ctx = Array.isArray(topicContexts?.[idx]?.context)
			? topicContexts[idx].context
			: [];
		return buildTopicIntentSummary(t, ctx);
	});

	updated = ensureTopicTransitions(updated, topics);
	updated = ensureTopicAnchors(updated, topics, topicIntents);
	updated = enforceTopicSpecificityGuards(
		updated,
		topics,
		topicContexts,
		topicIntents
	);
	updated = enforceRealWorldFraming(updated, topicContextFlags);
	updated = ensureTopicEngagementQuestions(updated, topics, mood, wordCaps);
	updated = enforceSegmentCompleteness(updated, mood, {
		includeCta: !includeOutro,
	});
	updated = limitFillerAndEmotesAcrossSegments(updated, {
		maxFillers: MAX_FILLER_WORDS_PER_VIDEO,
		maxFillersPerSegment: MAX_FILLER_WORDS_PER_SEGMENT,
		maxEmotes: MAX_MICRO_EMOTES_PER_VIDEO,
		maxEmotesPerSegment: MAX_MICRO_EMOTES_PER_VIDEO,
		noFillerSegmentIndices: [0, 1, 2],
	});
	updated = updated.map((s) => ({
		...s,
		text: sanitizeSegmentText(s.text),
	}));

	return { ...script, segments: updated };
}

/* ---------------------------------------------------------------
 * ElevenLabs TTS -> WAV (silence removed) -> global atempo
 * ------------------------------------------------------------- */

function buildVoiceSettingsForExpression(
	expression = "neutral",
	mood = "neutral",
	text = "",
	opts = {}
) {
	const uniform = Boolean(opts?.uniform);
	const forceNeutral = Boolean(opts?.forceNeutral);
	const expr = forceNeutral
		? "neutral"
		: coerceExpressionForNaturalness(expression, text, mood);
	let stability = Math.max(ELEVEN_TTS_STABILITY, uniform ? 0.72 : 0.6);
	let style = Math.min(ELEVEN_TTS_STYLE, uniform ? 0.16 : 0.22);

	if (uniform || forceNeutral) {
		// Lock a neutral, natural voice regardless of mood/expression.
		stability = ELEVEN_TTS_STABILITY;
		style = ELEVEN_TTS_STYLE;
	} else {
		switch (expr) {
			case "warm":
				stability += 0.03;
				style += 0.06;
				break;
			case "excited":
				stability -= 0.08;
				style += 0.12;
				break;
			case "serious":
				stability += 0.1;
				style -= 0.08;
				break;
			case "thoughtful":
				stability += 0.05;
				style -= 0.03;
				break;
			default:
				stability += 0.02;
				style -= 0.02;
				break;
		}
	}

	return {
		stability: clampNumber(stability, 0.1, 1),
		similarity_boost: clampNumber(ELEVEN_TTS_SIMILARITY, 0.1, 1),
		style: clampNumber(style, 0, 0.35),
		use_speaker_boost: ELEVEN_TTS_SPEAKER_BOOST,
	};
}

async function synthesizeTtsWav({
	text,
	tmpDir,
	jobId,
	label,
	voiceId,
	voiceSettings,
	modelId,
	modelOrder,
}) {
	const safeLabel = String(label || "tts").replace(/[^a-z0-9_-]/gi, "");
	const mp3 = path.join(tmpDir, `${safeLabel}_${jobId}.mp3`);
	const wav = path.join(tmpDir, `${safeLabel}_${jobId}.wav`);
	const cleanText = stripAllFillers(text);
	const usedModelId = await elevenLabsTTS({
		text: cleanText,
		outMp3Path: mp3,
		voiceId,
		voiceSettings,
		modelId,
		modelOrder,
	});
	await mp3ToCleanWav(mp3, wav);
	safeUnlink(mp3);
	const durationSec = await probeDurationSeconds(wav);
	return { wavPath: wav, durationSec, modelId: usedModelId, text: cleanText };
}

async function fitWavToTargetDuration({
	wavPath,
	targetSec,
	minAtempo,
	maxAtempo,
	tmpDir,
	jobId,
	label,
}) {
	const cleanDur = await probeDurationSeconds(wavPath);
	if (!cleanDur || !Number.isFinite(cleanDur)) {
		return { wavPath, durationSec: 0, atempo: 1, rawAtempo: 1 };
	}
	const rawAtempo = Number(targetSec) > 0 ? cleanDur / Number(targetSec) : 1;
	const atempo = clampNumber(rawAtempo, minAtempo, maxAtempo);
	if (Math.abs(atempo - 1) < 0.01) {
		return { wavPath, durationSec: cleanDur, atempo, rawAtempo };
	}
	const safeLabel = String(label || "tts").replace(/[^a-z0-9_-]/gi, "");
	const out = path.join(tmpDir, `${safeLabel}_fit_${jobId}.wav`);
	await applyGlobalAtempoToWav(wavPath, out, atempo);
	safeUnlink(wavPath);
	const durationSec = await probeDurationSeconds(out);
	return { wavPath: out, durationSec, atempo, rawAtempo };
}

async function createSilentWav({ durationSec, outPath }) {
	const dur = Math.max(0.1, Number(durationSec) || 0.1);
	await spawnBin(
		ffmpegPath,
		[
			"-f",
			"lavfi",
			"-i",
			`anullsrc=r=${AUDIO_SR}:cl=stereo`,
			"-t",
			dur.toFixed(3),
			"-acodec",
			"pcm_s16le",
			"-ar",
			String(AUDIO_SR),
			"-ac",
			"2",
			"-y",
			outPath,
		],
		"silent_wav",
		{ timeoutMs: 60000 }
	);
	return outPath;
}

/* ---------------------------------------------------------------
 * YouTube helpers
 * ------------------------------------------------------------- */

function resolveYouTubeTokensFromPayload(payload, user) {
	const bodyTok = {
		access_token: payload.youtubeAccessToken,
		refresh_token: payload.youtubeRefreshToken,
		expiry_date: payload.youtubeTokenExpiresAt
			? new Date(payload.youtubeTokenExpiresAt).getTime()
			: undefined,
	};
	const userTok = {
		access_token: user?.youtubeAccessToken,
		refresh_token: user?.youtubeRefreshToken,
		expiry_date: user?.youtubeTokenExpiresAt
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
			: resolveYouTubeTokensFromPayload({}, source);
	if (!creds.refresh_token) return null;
	const o = new google.auth.OAuth2(
		process.env.YOUTUBE_CLIENT_ID,
		process.env.YOUTUBE_CLIENT_SECRET,
		process.env.YOUTUBE_REDIRECT_URI
	);
	o.setCredentials(creds);
	return o;
}

async function refreshYouTubeTokensIfNeeded(user, payload) {
	const tokens = resolveYouTubeTokensFromPayload(payload || {}, user || {});
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
			if (user) {
				user.youtubeAccessToken = fresh.access_token;
				user.youtubeRefreshToken = fresh.refresh_token;
				user.youtubeTokenExpiresAt = fresh.expiry_date;
				if (user.isModified && user.isModified() && user.role !== "admin")
					await user.save();
			}
			return fresh;
		}
	} catch {}
	return tokens;
}

function normalizeYouTubeTags(tags = []) {
	const list = Array.isArray(tags) ? tags : [tags];
	return Array.from(
		new Set(list.map((t) => String(t || "").trim()).filter(Boolean))
	).slice(0, 15);
}

async function uploadToYouTube(
	u,
	fp,
	{ title, description, tags, category, thumbnailPath, jobId }
) {
	const o = buildYouTubeOAuth2Client(u);
	if (!o) throw new Error("YouTube OAuth missing");
	const yt = google.youtube({ version: "v3", auth: o });
	const safeTitle = String(title || "")
		.trim()
		.slice(0, 95);
	const safeDescription = ensureClickableLinks(description);
	const safeTags = normalizeYouTubeTags(tags);
	const { data } = await yt.videos.insert(
		{
			part: ["snippet", "status"],
			requestBody: {
				snippet: {
					title: safeTitle || "Untitled",
					description: safeDescription,
					tags: safeTags,
					categoryId:
						YT_CATEGORY_MAP[category] === "0"
							? "22"
							: YT_CATEGORY_MAP[category] || "22",
				},
				status: { privacyStatus: "public", selfDeclaredMadeForKids: false },
			},
			media: { body: fs.createReadStream(fp) },
		},
		{ maxContentLength: Infinity, maxBodyLength: Infinity }
	);

	const videoId = data?.id;
	if (videoId && thumbnailPath && fs.existsSync(thumbnailPath)) {
		try {
			await yt.thumbnails.set({
				videoId,
				media: { body: fs.createReadStream(thumbnailPath) },
			});
			logJob(jobId, "youtube thumbnail set", {
				path: path.basename(thumbnailPath),
			});
		} catch (e) {
			logJob(jobId, "youtube thumbnail upload failed (ignored)", {
				error: e.message,
			});
		}
	}

	return `https://www.youtube.com/watch?v=${videoId}`;
}

async function buildSeoMetadata({ topics = [], scriptTitle, languageLabel }) {
	let seoTitle = String(scriptTitle || "").trim();
	const topicLine = topics
		.map((t) => t.displayTopic || t.topic)
		.filter(Boolean)
		.join(" | ");

	if (process.env.CHATGPT_API_TOKEN) {
		try {
			const titlePrompt = `Write ONE SEO-friendly YouTube title (max 90 characters) for a long-form news brief covering: ${topicLine}. Use natural search phrasing, no quotes, no hashtags.`;
			const titleResp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: titlePrompt }],
			});
			const t = String(titleResp.choices?.[0]?.message?.content || "")
				.replace(/["']/g, "")
				.trim();
			if (t) seoTitle = t.slice(0, 90);
		} catch {}
	}

	let seoDescription = "";
	if (process.env.CHATGPT_API_TOKEN) {
		try {
			const descPrompt = `Write a YouTube description (max 180 words) for a long-form news brief titled "${seoTitle}". Make the first 2 lines keyword-rich for search. Use short sentences. Add a friendly CTA to comment and like (not pushy). End with 5-7 relevant hashtags.`;
			const descResp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: descPrompt }],
			});
			const descRaw = String(descResp.choices?.[0]?.message?.content || "")
				.trim()
				.replace(/\n{3,}/g, "\n\n");
			seoDescription = ensureClickableLinks(
				`${MERCH_INTRO}${descRaw}\n\n${BRAND_CREDIT}`
			);
		} catch {}
	}
	if (!seoDescription) {
		seoDescription = ensureClickableLinks(
			`${MERCH_INTRO}${seoTitle}\n\nTell me your take and tap like if this helped.\n\n${BRAND_CREDIT}`
		);
	}

	let tags = ["news", "entertainment", "longform"];
	if (process.env.CHATGPT_API_TOKEN) {
		try {
			const tagPrompt = `Return a JSON array of 8-12 SHORT tags for the YouTube video "${seoTitle}". Use high-volume search terms viewers actually type (1-3 words each). No hashtags, no duplicates.`;
			const tagResp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: tagPrompt }],
			});
			const parsed = parseJsonFlexible(
				stripCodeFence(tagResp.choices?.[0]?.message?.content || "")
			);
			if (Array.isArray(parsed)) tags.push(...parsed);
		} catch {}
	}

	if (!tags.includes(BRAND_TAG)) tags.unshift(BRAND_TAG);
	tags = [...new Set(tags.filter(Boolean).map((t) => String(t).trim()))];

	return { seoTitle, seoDescription, tags, languageLabel };
}

function cleanForTTS(text = "") {
	let t = String(text || "");
	// remove URLs/emails
	t = t.replace(/(https?:\/\/\S+|www\.[^\s]+|\S+@\S+\.\S+)/gi, " ");
	// normalize excessive punctuation while preserving natural pauses
	t = t.replace(/\.{4,}/g, "...");
	t = t.replace(/([!?]){2,}/g, "$1");
	t = t.replace(/,{2,}/g, ",");
	t = t.replace(/;{2,}/g, ";");
	t = t.replace(/:{2,}/g, ":");
	// normalize spacing
	t = t.replace(/\s+/g, " ").trim();
	return t;
}

function isElevenModelNotFound(err) {
	const msg = String(err?.message || "").toLowerCase();
	if (msg.includes("model_not_found")) return true;
	if (msg.includes("model id") && msg.includes("does not exist")) return true;
	return false;
}

async function streamToString(readable, limitBytes = 2000) {
	return await new Promise((resolve) => {
		try {
			const chunks = [];
			let total = 0;
			readable.on("data", (d) => {
				if (total >= limitBytes) return;
				const buf = Buffer.isBuffer(d) ? d : Buffer.from(d);
				const take = buf.slice(0, Math.max(0, limitBytes - total));
				chunks.push(take);
				total += take.length;
			});
			readable.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
			readable.on("error", () => resolve(""));
		} catch {
			resolve("");
		}
	});
}

async function elevenLabsTTS({
	text,
	outMp3Path,
	voiceId,
	voiceSettings,
	modelId,
	modelOrder,
}) {
	if (!ELEVEN_API_KEY) throw new Error("ELEVENLABS_API_KEY missing");
	const vId = String(voiceId || ELEVEN_FIXED_VOICE_ID).trim();
	if (!vId) throw new Error("ELEVENLABS voiceId missing");

	// Use a more stable configuration to reduce glitches
	// Higher bitrate MP3 for more natural timbre (avoid low-bitrate artifacts)
	const url = `https://api.elevenlabs.io/v1/text-to-speech/${vId}/stream?output_format=mp3_44100_192`;
	const basePayload = {
		text: cleanForTTS(text),
		voice_settings: voiceSettings || {
			stability: ELEVEN_TTS_STABILITY,
			similarity_boost: ELEVEN_TTS_SIMILARITY,
			style: ELEVEN_TTS_STYLE,
			use_speaker_boost: ELEVEN_TTS_SPEAKER_BOOST,
		},
	};

	let models = [];
	if (modelId) {
		models = [modelId];
	} else if (Array.isArray(modelOrder) && modelOrder.length) {
		models = modelOrder.filter(Boolean);
	} else {
		if (ELEVEN_TTS_MODEL) models.push(ELEVEN_TTS_MODEL);
		for (const m of ELEVEN_TTS_MODEL_FALLBACKS) {
			if (!models.includes(m)) models.push(m);
		}
	}

	let lastErr = null;
	for (const candidateModel of models) {
		const payload = { ...basePayload, model_id: candidateModel };
		const doReq = async () => {
			const res = await axios.post(url, payload, {
				headers: {
					"xi-api-key": ELEVEN_API_KEY,
					"Content-Type": "application/json",
					accept: "audio/mpeg",
				},
				responseType: "stream",
				timeout: 70000,
				validateStatus: (s) => s < 500,
			});

			if (res.status >= 300) {
				const body = await streamToString(res.data, 2500);
				const hint = body ? ` | ${body.slice(0, 600)}` : "";
				const err = new Error(`ElevenLabs TTS failed (${res.status})${hint}`);
				err.response = { status: res.status };
				throw err;
			}

			await new Promise((resolve, reject) => {
				const ws = fs.createWriteStream(outMp3Path);
				res.data.pipe(ws);
				ws.on("finish", resolve);
				ws.on("error", reject);
			});

			return outMp3Path;
		};

		try {
			await withRetries(doReq, {
				retries: 3,
				baseDelayMs: 700,
				label: "elevenlabs_tts",
			});
			return candidateModel;
		} catch (e) {
			lastErr = e;
			if (isElevenModelNotFound(e)) {
				console.warn(`[ElevenLabs] Model not found: ${candidateModel}`);
				continue;
			}
			throw e;
		}
	}

	throw lastErr || new Error("ElevenLabs TTS failed");
}

function buildAtempoFilterChain(factor) {
	// ffmpeg atempo supports 0.5..2.0 per filter; chain if outside
	let f = Number(factor);
	if (!Number.isFinite(f) || f <= 0) f = 1;
	const filters = [];
	while (f < 0.5) {
		filters.push("atempo=0.5");
		f /= 0.5;
	}
	while (f > 2.0) {
		filters.push("atempo=2.0");
		f /= 2.0;
	}
	filters.push(`atempo=${f.toFixed(4)}`);
	return filters.join(",");
}

async function mp3ToCleanWav(mp3Path, wavPath) {
	// IMPORTANT:
	// - We only trim *leading* and *trailing* silence.
	// - We do NOT remove internal pauses between words/sentences (those pauses are part of
	//   natural speech and removing them can make the voice sound rushed / "stuttery").
	//
	// We use the common "reverse trick" to trim the tail using start-only silenceremove.
	const trimLead =
		"silenceremove=start_periods=1:start_duration=0.12:start_threshold=-50dB";
	const trimTail =
		"areverse,silenceremove=start_periods=1:start_duration=0.12:start_threshold=-50dB,areverse";

	const af = [
		`aresample=${AUDIO_SR}`,
		"aformat=channel_layouts=mono",
		trimLead,
		trimTail,
		// Loudness normalize (single-pass). Keeps perceived volume consistent.
		"loudnorm=I=-16:TP=-1.5:LRA=11",
	].join(",");

	await spawnBin(
		ffmpegPath,
		[
			"-y",
			"-i",
			mp3Path,
			"-vn",
			"-af",
			af,
			"-acodec",
			"pcm_s16le",
			"-ar",
			String(AUDIO_SR),
			"-ac",
			String(AUDIO_CHANNELS),
			wavPath,
		],
		"mp3_to_wav",
		{ timeoutMs: 120000 }
	);
}

async function applyGlobalAtempoToWav(inWav, outWav, atempo) {
	const chain = buildAtempoFilterChain(atempo);
	await spawnBin(
		ffmpegPath,
		[
			"-i",
			inWav,
			"-vn",
			"-filter:a",
			`${chain},aresample=${AUDIO_SR},aformat=channel_layouts=mono`,
			"-acodec",
			"pcm_s16le",
			"-ar",
			String(AUDIO_SR),
			"-ac",
			String(AUDIO_CHANNELS),
			"-y",
			outWav,
		],
		"apply_global_atempo",
		{ timeoutMs: 120000 }
	);
	return outWav;
}

/* ---------------------------------------------------------------
 * Sync.so lipsync (multipart) - supports WAV per Sync docs
 * ------------------------------------------------------------- */

function buildSyncSoHeaders() {
	return {
		"x-api-key": SYNC_SO_API_KEY,
		Authorization: `Bearer ${SYNC_SO_API_KEY}`,
	};
}

function extractSyncSoId(data) {
	return (
		data?.id ||
		data?.jobId ||
		data?.data?.id ||
		data?.data?.jobId ||
		data?.data?.job_id ||
		null
	);
}

function extractSyncSoStatus(data) {
	return (
		data?.status || data?.data?.status || data?.state || data?.data?.state || ""
	);
}

function extractSyncSoOutputUrl(data) {
	return (
		data?.outputUrl ||
		data?.output_url ||
		data?.data?.outputUrl ||
		data?.data?.output_url ||
		data?.output?.url ||
		data?.data?.output?.url ||
		(Array.isArray(data?.output)
			? data.output[0]?.url || data.output[0]
			: null) ||
		(Array.isArray(data?.data?.output)
			? data.data.output[0]?.url || data.data.output[0]
			: null) ||
		null
	);
}

function extractSyncSoError(data, text = "") {
	return (
		data?.message ||
		data?.error ||
		data?.data?.message ||
		data?.data?.error ||
		(text ? String(text).trim().slice(0, 220) : null) ||
		null
	);
}

async function fetchJson(url, options = {}, timeoutMs = 25000) {
	if (typeof fetch === "function") {
		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), timeoutMs);
		try {
			const res = await fetch(url, { ...options, signal: controller.signal });
			const txt = await res.text().catch(() => "");
			let data = null;
			try {
				data = txt ? JSON.parse(stripCodeFence(txt)) : null;
			} catch {
				data = null;
			}
			return { status: res.status, ok: res.ok, data, text: txt };
		} finally {
			clearTimeout(timer);
		}
	}

	const res = await axios.request({
		url,
		method: options.method || "GET",
		headers: options.headers || {},
		data: options.body,
		timeout: timeoutMs,
		validateStatus: () => true,
	});

	const txt =
		typeof res.data === "string" ? res.data : JSON.stringify(res.data || {});
	return {
		status: res.status,
		ok: res.status >= 200 && res.status < 300,
		data: typeof res.data === "object" ? res.data : parseJsonFlexible(txt),
		text: txt,
	};
}

async function requestSyncSoJob({ videoPath, audioPath, jobId }) {
	const endpoint = `${SYNC_SO_BASE}${SYNC_SO_GENERATE_PATH}`;
	if (!fs.existsSync(videoPath)) throw new Error("Sync input video missing");
	if (!fs.existsSync(audioPath)) throw new Error("Sync input audio missing");

	if (
		!FormDataNode &&
		(typeof FormData !== "function" || typeof fetch !== "function")
	) {
		throw new Error(
			"Sync multipart requires Node 18+ (fetch/FormData) or install 'form-data'"
		);
	}

	logJob(jobId, "sync multipart request", { endpoint });

	if (FormDataNode) {
		const form = new FormDataNode();
		form.append("model", SYNC_SO_MODEL);
		form.append("video", fs.createReadStream(videoPath), {
			filename: "presenter.mp4",
			contentType: "video/mp4",
		});
		form.append("audio", fs.createReadStream(audioPath), {
			filename: "segment.wav",
			contentType: "audio/wav",
		});

		const doReq = async () => {
			const res = await axios.post(endpoint, form, {
				headers: { ...buildSyncSoHeaders(), ...form.getHeaders() },
				timeout: 90000,
				validateStatus: () => true,
			});
			const data = res.data;
			const id = extractSyncSoId(data);
			if (res.status >= 300 || !id) {
				const err = new Error(
					`Sync.so generate failed (${res.status}): ${
						extractSyncSoError(data, "") || "unknown error"
					}`
				);
				err.response = { status: res.status, data };
				throw err;
			}
			return { id };
		};

		return await withRetries(doReq, {
			retries: 2,
			baseDelayMs: 900,
			label: "sync_generate",
		});
	}

	// Native FormData
	const form = new FormData();
	form.append("model", SYNC_SO_MODEL);
	form.append(
		"video",
		new Blob([fs.readFileSync(videoPath)], { type: "video/mp4" }),
		"presenter.mp4"
	);
	form.append(
		"audio",
		new Blob([fs.readFileSync(audioPath)], { type: "audio/wav" }),
		"segment.wav"
	);

	const { status, ok, data, text } = await fetchJson(
		endpoint,
		{ method: "POST", headers: buildSyncSoHeaders(), body: form },
		90000
	);

	const id = extractSyncSoId(data);
	if (!ok || !id) {
		throw new Error(
			`Sync.so generate failed (${status}): ${
				extractSyncSoError(data, text) || "unknown error"
			}`
		);
	}
	return { id };
}

async function pollSyncSoJob({ id, label, jobId }) {
	const statusUrl = `${SYNC_SO_BASE}${SYNC_SO_GENERATE_PATH}/${id}`;

	for (let i = 0; i < 160; i++) {
		await sleep(2000);

		const { status, ok, data, text } = await fetchJson(
			statusUrl,
			{ headers: buildSyncSoHeaders() },
			25000
		);
		if (!ok) {
			throw new Error(
				`${label} status check failed (${status}): ${
					extractSyncSoError(data, text) || "unknown error"
				}`
			);
		}

		const st = String(extractSyncSoStatus(data) || "").toLowerCase();
		const out = extractSyncSoOutputUrl(data);

		if ((st === "completed" || st === "succeeded") && out) return out;
		if (st === "failed" || st === "error" || st === "rejected") {
			throw new Error(
				`${label} failed: ${extractSyncSoError(data, text) || "unknown error"}`
			);
		}

		if (jobId && i % 10 === 0)
			logJob(jobId, "sync polling", { label, status: st || "pending" });
	}
	throw new Error(`${label} timed out`);
}

/* ---------------------------------------------------------------
 * Video helpers (normalize + zoom-out + merge)
 * ------------------------------------------------------------- */

function buildScaleFilter({ w, h, mode }) {
	const W = makeEven(w);
	const H = makeEven(h);
	const m = String(mode || "cover").toLowerCase();
	if (m === "contain") {
		return `scale=${W}:${H}:force_original_aspect_ratio=decrease:flags=lanczos,pad=${W}:${H}:(ow-iw)/2:(oh-ih)/2:color=black`;
	}
	if (m === "blur") {
		return `split=2[bg][fg];[bg]scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H},gblur=sigma=18[bg2];[fg]scale=${W}:${H}:force_original_aspect_ratio=decrease:flags=lanczos[fg2];[bg2][fg2]overlay=(W-w)/2:(H-h)/2`;
	}
	return `scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H}`;
}

async function normalizeClip(
	inPath,
	outPath,
	outCfg,
	{ zoomOut = 1.0, addFades = false, fadeOutOnly = false } = {}
) {
	const w = makeEven(outCfg.w);
	const h = makeEven(outCfg.h);
	const fps = Number(outCfg.fps || DEFAULT_OUTPUT_FPS);
	const scaleMode = outCfg.scaleMode || "cover";

	let vf =
		scaleMode === "blur"
			? `scale=${w}:${h}:force_original_aspect_ratio=increase,boxblur=15:1,crop=${w}:${h}`
			: `scale=${w}:${h}:force_original_aspect_ratio=${
					scaleMode === "cover" ? "increase" : "decrease"
			  },crop=${w}:${h}`;

	if (scaleMode === "contain") {
		vf = `scale=${w}:${h}:force_original_aspect_ratio=decrease,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:color=black`;
	}

	vf += `,fps=${fps},format=yuv420p`;

	// Subtle motion: slight zoom-out with blurred padding (no invalid crop).
	if (zoomOut && zoomOut !== 1.0) {
		const zw = Math.max(2, makeEven(w * zoomOut));
		const zh = Math.max(2, makeEven(h * zoomOut));
		vf =
			`${vf},split=2[base][z];` +
			`[base]gblur=sigma=18[bg];` +
			`[z]scale=${zw}:${zh}:flags=lanczos[fg];` +
			`[bg][fg]overlay=(W-w)/2:(H-h)/2`;
	}

	// Stable resample without async drift correction (keeps lipsync timing tight)
	let af = `aresample=${AUDIO_SR},aformat=channel_layouts=stereo:sample_fmts=fltp,volume=1.0`;

	let fadeIn = Boolean(addFades);
	let fadeOut = Boolean(addFades);
	if (fadeOutOnly) {
		fadeIn = false;
		fadeOut = true;
	}

	if (fadeIn || fadeOut) {
		const vFadeDur = 0.06;
		const aFadeDur = 0.04;

		let durSec = 0;
		try {
			durSec = await probeDurationSeconds(inPath);
		} catch (_) {
			durSec = 0;
		}

		// IMPORTANT:
		// ffmpeg's afade does NOT accept expressions like (D-0.04) for st.
		// We compute numeric start times instead.
		if (durSec > 0.15) {
			const vOutStart = Math.max(0, durSec - vFadeDur);
			const aOutStart = Math.max(0, durSec - aFadeDur);

			if (fadeIn) {
				vf += `,fade=t=in:st=0:d=${vFadeDur}`;
				af += `,afade=t=in:st=0:d=${aFadeDur}`;
			}
			if (fadeOut) {
				vf += `,fade=t=out:st=${vOutStart.toFixed(3)}:d=${vFadeDur}`;
				af += `,afade=t=out:st=${aOutStart.toFixed(3)}:d=${aFadeDur}`;
			}
		} else if (fadeIn) {
			vf += `,fade=t=in:st=0:d=${vFadeDur}`;
			af += `,afade=t=in:st=0:d=${aFadeDur}`;
		}
	}

	await spawnBin(
		ffmpegPath,
		[
			"-y",
			"-i",
			inPath,
			"-vf",
			vf,
			"-af",
			af,
			"-r",
			String(fps),
			"-c:v",
			"libx264",
			"-preset",
			INTERMEDIATE_PRESET,
			"-crf",
			String(INTERMEDIATE_VIDEO_CRF),
			"-pix_fmt",
			"yuv420p",
			"-c:a",
			"aac",
			"-b:a",
			AUDIO_BITRATE,
			"-movflags",
			"+faststart",
			outPath,
		],
		"normalize_clip",
		{ timeoutMs: 240000 }
	);
}

async function createSyncFallbackInput(
	videoPath,
	tmpDir,
	jobId,
	label,
	variant = 1
) {
	const out = path.join(
		tmpDir,
		`${label}_${crypto.randomUUID()}_sync_fallback_${variant}.mp4`
	);
	let vf = `fps=${SYNC_SO_INPUT_FPS},format=yuv420p,setpts=PTS-STARTPTS`;
	let crf = Math.max(26, SYNC_SO_INPUT_CRF + 4);
	let tag = "sync_fallback_reencode";

	if (variant >= 2) {
		const scaleExpr = `scale='if(gt(iw,ih),${SYNC_SO_FALLBACK_MAX_EDGE},-2)':'if(gt(ih,iw),${SYNC_SO_FALLBACK_MAX_EDGE},-2)'`;
		vf = `${scaleExpr},fps=${SYNC_SO_INPUT_FPS},format=yuv420p,setpts=PTS-STARTPTS`;
		crf = Math.max(28, SYNC_SO_INPUT_CRF + 6);
		tag = "sync_fallback_scale";
	}

	await spawnBin(
		ffmpegPath,
		[
			"-i",
			videoPath,
			"-an",
			"-vf",
			vf,
			"-c:v",
			"libx264",
			"-preset",
			"veryfast",
			"-crf",
			String(crf),
			"-pix_fmt",
			"yuv420p",
			"-movflags",
			"+faststart",
			"-y",
			out,
		],
		tag,
		{ timeoutMs: 180000 }
	);

	return await ensureUnderBytes(out, SYNC_SO_MAX_BYTES, tmpDir, jobId, label);
}

async function renderLipsyncedSegment({
	jobId,
	tmpDir,
	output,
	baselineSource,
	segDur,
	audioPath,
	label,
	offsetSeed = 0,
	addFades = false,
}) {
	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "");
	const dur = Math.max(0.2, Number(segDur) || 0.2);
	const offset =
		(Number(offsetSeed) * 1.37) % Math.max(2, BASELINE_DUR_SEC - 0.5);
	const baseSeg = path.join(tmpDir, `base_${safeLabel}_${jobId}.mp4`);
	await spawnBin(
		ffmpegPath,
		[
			"-stream_loop",
			"-1",
			"-i",
			baselineSource,
			"-ss",
			offset.toFixed(3),
			"-t",
			dur.toFixed(3),
			"-an",
			"-vf",
			`fps=${SYNC_SO_INPUT_FPS},format=yuv420p,setpts=PTS-STARTPTS`,
			"-c:v",
			"libx264",
			"-preset",
			"veryfast",
			"-crf",
			String(SYNC_SO_INPUT_CRF),
			"-pix_fmt",
			"yuv420p",
			"-movflags",
			"+faststart",
			"-y",
			baseSeg,
		],
		"base_segment",
		{ timeoutMs: 240000 }
	);

	let syncBase = baseSeg;
	let prescaled = null;
	let baseSizeBytes = null;
	if (SYNC_SO_PRE_MAX_EDGE && SYNC_SO_PRE_MAX_EDGE > 0) {
		try {
			baseSizeBytes = fs.statSync(baseSeg).size;
		} catch {}

		const sizeThreshold = Math.floor(
			SYNC_SO_MAX_BYTES * SYNC_SO_PRESCALE_SIZE_PCT
		);
		const shouldPrescale =
			SYNC_SO_PRESCALE_ALWAYS ||
			(Number.isFinite(baseSizeBytes) && baseSizeBytes > sizeThreshold) ||
			dur >= SYNC_SO_PRESCALE_MIN_SEC;

		if (shouldPrescale) {
			prescaled = path.join(tmpDir, `base_${safeLabel}_${jobId}_prescale.mp4`);
			const scaleExpr = `scale='if(gt(iw,ih),${SYNC_SO_PRE_MAX_EDGE},-2)':'if(gt(ih,iw),${SYNC_SO_PRE_MAX_EDGE},-2)'`;
			await spawnBin(
				ffmpegPath,
				[
					"-i",
					baseSeg,
					"-an",
					"-vf",
					`${scaleExpr},fps=${SYNC_SO_INPUT_FPS},format=yuv420p,setpts=PTS-STARTPTS`,
					"-c:v",
					"libx264",
					"-preset",
					"veryfast",
					"-crf",
					String(SYNC_SO_INPUT_CRF),
					"-pix_fmt",
					"yuv420p",
					"-movflags",
					"+faststart",
					"-y",
					prescaled,
				],
				"sync_prescale",
				{ timeoutMs: 180000 }
			);
			syncBase = prescaled;
			logJob(jobId, "sync prescale applied", {
				label: safeLabel,
				segDur: Number(dur.toFixed(3)),
				baseBytes: baseSizeBytes || null,
				thresholdBytes: sizeThreshold,
			});
		}
	}

	const baseSized = await ensureUnderBytes(
		syncBase,
		SYNC_SO_MAX_BYTES,
		tmpDir,
		jobId,
		`base_${safeLabel}`
	);
	if (!LONG_VIDEO_KEEP_TMP) {
		if (baseSeg && baseSeg !== baseSized) safeUnlink(baseSeg);
		if (prescaled && prescaled !== baseSized) safeUnlink(prescaled);
	}

	let lipsynced = null;
	let lastErr = null;
	for (let attempt = 0; attempt <= SYNC_SO_SEGMENT_MAX_RETRIES; attempt++) {
		try {
			if (SYNC_SO_REQUEST_GAP_MS) await sleep(SYNC_SO_REQUEST_GAP_MS);
			const syncInput =
				attempt === 0
					? baseSized
					: await createSyncFallbackInput(
							baseSized,
							tmpDir,
							jobId,
							`sync_${safeLabel}`,
							attempt
					  );

			const fit = path.join(
				tmpDir,
				`base_fit_${jobId}_${safeLabel}_${attempt}.mp4`
			);
			await fitVideoToDuration(syncInput, dur, fit);

			const syncJob = await requestSyncSoJob({
				videoPath: fit,
				audioPath,
				jobId,
			});
			const outUrl = await pollSyncSoJob({
				id: syncJob.id,
				label: `lipsync_${safeLabel}`,
				jobId,
			});

			const raw = path.join(tmpDir, `lip_${jobId}_${safeLabel}.mp4`);
			await downloadToFile(outUrl, raw, 120000, 2);

			const fit2 = path.join(tmpDir, `lip_fit_${jobId}_${safeLabel}.mp4`);
			await fitVideoToDuration(raw, dur, fit2, SEGMENT_PAD_SEC);
			safeUnlink(raw);
			lipsynced = fit2;
			lastErr = null;
			break;
		} catch (e) {
			lastErr = e;
			logJob(jobId, "lipsync attempt failed", {
				label: safeLabel,
				attempt,
				error: e.message,
			});
			if (attempt < SYNC_SO_SEGMENT_MAX_RETRIES) {
				const delay = SYNC_SO_RETRY_DELAY_MS * (attempt + 1);
				await sleep(delay);
			}
		}
	}

	if (!lipsynced) {
		if (REQUIRE_LIPSYNC) {
			throw lastErr || new Error("Lipsync failed");
		}
		logJob(jobId, "lipsync failed; using base video", {
			label: safeLabel,
			error: lastErr?.message || "unknown error",
		});
		const fit = path.join(tmpDir, `base_fit_${jobId}_${safeLabel}.mp4`);
		await fitVideoToDuration(baseSized, dur, fit, SEGMENT_PAD_SEC);
		lipsynced = fit;
	}

	const withAudio = path.join(tmpDir, `seg_${jobId}_${safeLabel}_audio.mp4`);
	await mergeVideoWithAudio(lipsynced, audioPath, withAudio);

	const norm = path.join(tmpDir, `seg_${jobId}_${safeLabel}_norm.mp4`);
	await normalizeClip(withAudio, norm, output, {
		zoomOut: CAMERA_ZOOM_OUT,
		addFades,
	});
	safeUnlink(withAudio);

	return norm;
}

async function createImageMontageClip({
	jobId,
	tmpDir,
	output,
	segDur,
	imagePaths = [],
	label,
}) {
	if (!Array.isArray(imagePaths) || !imagePaths.length)
		throw new Error("No images for segment");

	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "");
	const dur = Math.max(0.2, Number(segDur) || 0.2);
	const w = makeEven(output.w);
	const h = makeEven(output.h);
	const fps = Number(output.fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const imageScaleMode = String(
		output.imageScaleMode || DEFAULT_IMAGE_SCALE_MODE
	)
		.trim()
		.toLowerCase();
	const perDur = Math.max(0.2, dur / imagePaths.length);
	const labelNum = Number(label);
	const useCrossfade =
		imagePaths.length > 1 &&
		perDur >= 1.4 &&
		(!Number.isFinite(labelNum) || labelNum % 2 === 0);
	const crossfadeDur = useCrossfade ? clampNumber(perDur * 0.2, 0.25, 0.6) : 0;

	const inputs = [];
	const filterParts = [];
	const vLabels = [];

	imagePaths.forEach((imgPath, idx) => {
		inputs.push("-loop", "1", "-i", imgPath);
		const outLabel = `v${idx}`;
		const trim = `trim=0:${perDur.toFixed(3)},setpts=PTS-STARTPTS`;
		if (imageScaleMode === "blur") {
			const bg = `bg${idx}`;
			const fg = `fg${idx}`;
			const bg2 = `bg2${idx}`;
			const fg2 = `fg2${idx}`;
			filterParts.push(`[${idx}:v]split=2[${bg}][${fg}]`);
			filterParts.push(
				`[${bg}]scale=${w}:${h}:force_original_aspect_ratio=increase:flags=lanczos,crop=${w}:${h},gblur=sigma=18[${bg2}]`
			);
			filterParts.push(
				`[${fg}]scale=${w}:${h}:force_original_aspect_ratio=decrease:flags=lanczos[${fg2}]`
			);
			filterParts.push(
				`[${bg2}][${fg2}]overlay=(W-w)/2:(H-h)/2,fps=${fps},${trim},setsar=1,format=yuv420p[${outLabel}]`
			);
		} else {
			const scale =
				imageScaleMode === "contain"
					? `scale=${w}:${h}:force_original_aspect_ratio=decrease:flags=lanczos,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:color=black`
					: `scale=${w}:${h}:force_original_aspect_ratio=increase:flags=lanczos,crop=${w}:${h}`;
			const panX = idx % 2 === 0 ? "0" : "iw*0.03";
			const panY = idx % 3 === 0 ? "0" : "ih*0.02";
			const motionMode = idx % 3;
			const motion =
				imageScaleMode === "cover"
					? motionMode === 0
						? `zoompan=z='min(1.08,zoom+0.0007)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:fps=${fps}`
						: motionMode === 1
						? `zoompan=z='min(1.06,zoom+0.0006)':x='iw/2-(iw/zoom/2)+${panX}':y='ih/2-(ih/zoom/2)+${panY}':d=1:fps=${fps}`
						: `fps=${fps}`
					: `fps=${fps}`;
			filterParts.push(
				`[${idx}:v]${scale},${motion},${trim},setsar=1,format=yuv420p[${outLabel}]`
			);
		}
		vLabels.push(`[${outLabel}]`);
	});

	if (useCrossfade && imagePaths.length > 1) {
		let last = "v0";
		let acc = perDur;
		for (let i = 1; i < imagePaths.length; i++) {
			const out = `xf${i}`;
			const offset = Math.max(0, acc - crossfadeDur);
			filterParts.push(
				`[${last}][v${i}]xfade=transition=fade:duration=${crossfadeDur.toFixed(
					3
				)}:offset=${offset.toFixed(3)}[${out}]`
			);
			acc += perDur - crossfadeDur;
			last = out;
		}
		filterParts.push(`[${last}]setsar=1,format=yuv420p[v]`);
	} else {
		filterParts.push(
			`${vLabels.join("")}concat=n=${imagePaths.length}:v=1:a=0[v]`
		);
	}

	const raw = path.join(tmpDir, `seg_img_${jobId}_${safeLabel}_raw.mp4`);
	await spawnBin(
		ffmpegPath,
		[
			...inputs,
			"-filter_complex",
			filterParts.join(";"),
			"-map",
			"[v]",
			"-r",
			String(fps),
			"-c:v",
			"libx264",
			"-preset",
			INTERMEDIATE_PRESET,
			"-crf",
			String(INTERMEDIATE_VIDEO_CRF),
			"-pix_fmt",
			"yuv420p",
			"-movflags",
			"+faststart",
			"-y",
			raw,
		],
		"image_montage",
		{ timeoutMs: 240000 }
	);

	const fit = path.join(tmpDir, `seg_img_${jobId}_${safeLabel}_fit.mp4`);
	await fitVideoToDuration(raw, dur, fit);
	safeUnlink(raw);
	return fit;
}

async function renderImageSegment({
	jobId,
	tmpDir,
	output,
	segDur,
	audioPath,
	imagePaths = [],
	label,
	addFades = false,
}) {
	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "");
	const montage = await createImageMontageClip({
		jobId,
		tmpDir,
		output,
		segDur,
		imagePaths,
		label: safeLabel,
	});

	const withAudio = path.join(tmpDir, `img_${jobId}_${safeLabel}_audio.mp4`);
	await mergeVideoWithAudio(montage, audioPath, withAudio);
	safeUnlink(montage);

	const norm = path.join(tmpDir, `img_${jobId}_${safeLabel}_norm.mp4`);
	await normalizeClip(withAudio, norm, output, {
		zoomOut: CAMERA_ZOOM_OUT,
		addFades,
	});
	safeUnlink(withAudio);
	return norm;
}

async function fitVideoToDuration(inVideo, targetSec, outVideo, padSec = 0) {
	const pad = Math.max(0, Number(padSec) || 0);
	const target = Math.max(0.2, Number(targetSec) || 1) + pad;
	const vf = `setpts=PTS-STARTPTS,tpad=stop_mode=clone:stop_duration=${target.toFixed(
		3
	)},trim=0:${target.toFixed(3)},setpts=PTS-STARTPTS`;
	await spawnBin(
		ffmpegPath,
		[
			"-fflags",
			"+genpts",
			"-i",
			inVideo,
			"-an",
			"-vf",
			vf,
			"-c:v",
			"libx264",
			"-preset",
			INTERMEDIATE_PRESET,
			"-crf",
			String(INTERMEDIATE_VIDEO_CRF),
			"-pix_fmt",
			"yuv420p",
			"-movflags",
			"+faststart",
			"-y",
			outVideo,
		],
		"fit_video",
		{ timeoutMs: 180000 }
	);
	return outVideo;
}

async function mergeVideoWithAudio(videoPath, audioPath, outPath) {
	await spawnBin(
		ffmpegPath,
		[
			"-fflags",
			"+genpts",
			"-i",
			videoPath,
			"-i",
			audioPath,
			"-map",
			"0:v:0",
			"-map",
			"1:a:0",
			"-c:v",
			"copy",
			"-c:a",
			"aac",
			"-b:a",
			AUDIO_BITRATE,
			"-ar",
			String(AUDIO_SR),
			"-ac",
			"2",
			"-shortest",
			"-movflags",
			"+faststart",
			"-y",
			outPath,
		],
		"merge_audio",
		{ timeoutMs: 180000 }
	);
	return outPath;
}

async function ensureUnderBytes(
	videoPath,
	maxBytes,
	tmpDir,
	jobId,
	label = "sync_input"
) {
	try {
		const st = fs.statSync(videoPath);
		if (st.size <= maxBytes) return videoPath;

		const out = path.join(tmpDir, `${label}_${crypto.randomUUID()}_small.mp4`);
		await spawnBin(
			ffmpegPath,
			[
				"-i",
				videoPath,
				"-an",
				"-vf",
				`fps=${SYNC_SO_INPUT_FPS},format=yuv420p,setpts=PTS-STARTPTS`,
				"-c:v",
				"libx264",
				"-preset",
				"veryfast",
				"-crf",
				String(Math.max(28, SYNC_SO_INPUT_CRF + 4)),
				"-pix_fmt",
				"yuv420p",
				"-movflags",
				"+faststart",
				"-y",
				out,
			],
			"shrink_sync_input",
			{ timeoutMs: 180000 }
		);

		const st2 = fs.statSync(out);
		logJob(jobId, "sync input shrunk", {
			label,
			beforeBytes: st.size,
			afterBytes: st2.size,
		});
		if (st2.size <= maxBytes) return out;

		const out2 = path.join(tmpDir, `${label}_${crypto.randomUUID()}_down.mp4`);
		const scaleExpr = `scale='if(gt(iw,ih),${SYNC_SO_FALLBACK_MAX_EDGE},-2)':'if(gt(ih,iw),${SYNC_SO_FALLBACK_MAX_EDGE},-2)'`;
		await spawnBin(
			ffmpegPath,
			[
				"-i",
				out,
				"-an",
				"-vf",
				`${scaleExpr},fps=${SYNC_SO_INPUT_FPS},format=yuv420p,setpts=PTS-STARTPTS`,
				"-c:v",
				"libx264",
				"-preset",
				"veryfast",
				"-crf",
				String(Math.max(26, SYNC_SO_INPUT_CRF + 4)),
				"-pix_fmt",
				"yuv420p",
				"-movflags",
				"+faststart",
				"-y",
				out2,
			],
			"shrink_sync_input_scale",
			{ timeoutMs: 180000 }
		);
		const st3 = fs.statSync(out2);
		logJob(jobId, "sync input scaled down", {
			label,
			afterBytes: st3.size,
		});
		return out2;
	} catch (e) {
		logJob(jobId, "sync input size check failed (ignored)", {
			error: e.message,
		});
		return videoPath;
	}
}

async function concatClips(clips, outPath, outCfg) {
	if (!Array.isArray(clips) || !clips.length)
		throw new Error("No clips to concat");
	if (clips.length === 1) {
		fs.copyFileSync(clips[0], outPath);
		return outPath;
	}

	const w = outCfg?.w ? makeEven(outCfg.w) : null;
	const h = outCfg?.h ? makeEven(outCfg.h) : null;
	const scaleFilter =
		w && h
			? `scale=${w}:${h}:force_original_aspect_ratio=increase:flags=lanczos,crop=${w}:${h},`
			: "";

	const args = [];
	clips.forEach((p) => args.push("-i", p));

	// concat filter
	const pre = clips
		.map(
			(_, i) =>
				`[${i}:v:0]${scaleFilter}setpts=PTS-STARTPTS,format=yuv420p,setsar=1[v${i}];` +
				`[${i}:a:0]asetpts=PTS-STARTPTS,aresample=${AUDIO_SR},aformat=channel_layouts=stereo:sample_fmts=fltp[a${i}]`
		)
		.join(";");

	const catInputs = clips.map((_, i) => `[v${i}][a${i}]`).join("");
	const filter = `${pre};${catInputs}concat=n=${clips.length}:v=1:a=1[v][a]`;

	args.push(
		"-filter_complex",
		filter,
		"-map",
		"[v]",
		"-map",
		"[a]",
		"-c:v",
		"libx264",
		"-preset",
		INTERMEDIATE_PRESET,
		"-crf",
		String(INTERMEDIATE_VIDEO_CRF),
		"-pix_fmt",
		"yuv420p",
		"-c:a",
		"aac",
		"-b:a",
		AUDIO_BITRATE,
		"-ar",
		String(AUDIO_SR),
		"-ac",
		"2",
		"-movflags",
		"+faststart",
		"-y",
		outPath
	);

	await spawnBin(ffmpegPath, args, "concat", { timeoutMs: 420000 });
	return outPath;
}

/* ---------------------------------------------------------------
 * Intro motion (optional, 2-4s)
 * ------------------------------------------------------------- */

async function createPresenterIntroMotion({
	jobId,
	presenterImagePath,
	outputRatio,
	durationSec,
	motionRefVideo,
}) {
	if (!RUNWAY_API_KEY)
		throw new Error("RUNWAY_API_KEY missing (required for intro motion)");

	const runwayUri = await runwayCreateEphemeralUpload({
		filePath: presenterImagePath,
		filename: "presenter_intro.png",
	});

	const dur = clampNumber(
		Number(durationSec) || DEFAULT_INTRO_SEC,
		INTRO_MIN_SEC,
		4
	);

	const motionHint = motionRefVideo
		? "Match the natural motion style from the reference performance: gentle head movement with rare micro-nods (no repeated nodding), human blink rate with slight variation, subtle hand gestures."
		: "Natural head and neck movement, human blink rate with slight variation, subtle breathing.";
	const introFace = pickIntroExpression(jobId);
	const titleTarget = `${Math.round(
		INTRO_TEXT_X_PCT * 100
	)}% from the left edge and ${Math.round(INTRO_TEXT_Y_PCT * 100)}% down`;

	const prompt = `
Photorealistic talking-head video of the SAME person as the reference image.
Same studio background and lighting. Keep identity consistent. ${STUDIO_EMPTY_PROMPT}
Framing: medium shot (not too close, not too far), upper torso to mid torso, moderate headroom; desk visible; camera at a comfortable distance.
Action: calm intro delivery with natural, subtle hand movement near the desk. Keep an OPEN, EMPTY area on the viewer-left side for later title text. Do NOT add any screens, cards, posters, charts, or graphic panels.
Props: keep all existing props exactly as in the reference; do not add or remove objects. If a candle is visible, keep it subtle and unchanged with a calm flame; no extra candles.
Expression: ${introFace}. Calm and neutral, composed and professional with a very subtle, light smile (barely noticeable, not constant).
Mouth and jaw: natural, human movement; avoid robotic or stiff mouth shapes.
Eyes: comfortable, natural, relaxed with realistic blink cadence; no glassy or robotic eyes. Briefly glance toward the open title area, then back to the camera.
Forehead: natural skin texture and subtle movement; avoid waxy smoothing.
${motionHint}
Keep movements small and realistic. Natural sleeve and fabric movement. No exaggerated gestures. No extra people. No text overlays. No screens or charts. No logos except those already present in the reference.
`.trim();

	const fallbackPrompt = `
Photorealistic talking-head video of the SAME person as the reference image.
Same studio background and lighting. Keep identity consistent. ${STUDIO_EMPTY_PROMPT}
Framing: medium shot (not too close, not too far), upper torso to mid torso, moderate headroom; desk visible; camera at a comfortable distance.
Action: small, natural intro gesture near the desk. Keep an OPEN, EMPTY area on the viewer-left side for later title text. Do NOT add any screens, cards, posters, charts, or graphic panels.
Props: keep all existing props exactly as in the reference; do not add or remove objects. If a candle is visible, keep it subtle and unchanged with a calm flame; no extra candles.
Expression: ${introFace}. Calm and neutral; very subtle, light smile only (barely noticeable), not constant.
Mouth and jaw: natural, human movement; avoid robotic or stiff mouth shapes.
Eyes: comfortable, natural, relaxed with realistic blink cadence; no glassy or robotic eyes.
Forehead: natural skin texture and subtle movement; avoid waxy smoothing.
${motionHint}
Keep movements small and realistic. Natural sleeve and fabric movement. No exaggerated gestures. No extra people. No text overlays. No screens or charts. No logos except those already present in the reference.
`.trim();

	const introModelOrder = [];
	if (RUNWAY_VIDEO_MODEL_FALLBACK)
		introModelOrder.push(RUNWAY_VIDEO_MODEL_FALLBACK);
	if (RUNWAY_VIDEO_MODEL && !introModelOrder.includes(RUNWAY_VIDEO_MODEL))
		introModelOrder.push(RUNWAY_VIDEO_MODEL);

	const runIntroPrompt = async (promptText, label) => {
		logJob(jobId, "intro motion prompt", {
			label,
			duration: dur,
			ratio: outputRatio,
		});
		return await runwayImageToVideo({
			runwayImageUri: runwayUri,
			promptText,
			durationSec: dur,
			ratio: outputRatio,
			modelOrder: introModelOrder.length ? introModelOrder : undefined,
		});
	};

	let outUrl;
	try {
		outUrl = await runIntroPrompt(prompt, "primary");
	} catch (e) {
		logJob(jobId, "intro motion failed (primary)", { error: e.message });
		try {
			outUrl = await runIntroPrompt(fallbackPrompt, "fallback");
		} catch (e2) {
			logJob(jobId, "intro motion failed (fallback)", { error: e2.message });

			const outCfg = parseRatio(outputRatio);
			const outMp4Fallback = path.join(
				path.dirname(presenterImagePath),
				`intro_motion_fallback_${jobId}.mp4`
			);
			await spawnBin(
				ffmpegPath,
				[
					"-loop",
					"1",
					"-i",
					presenterImagePath,
					"-t",
					dur.toFixed(3),
					"-an",
					"-vf",
					`scale=${makeEven(outCfg.w)}:${makeEven(
						outCfg.h
					)}:force_original_aspect_ratio=increase:flags=lanczos,crop=${makeEven(
						outCfg.w
					)}:${makeEven(outCfg.h)},fps=${DEFAULT_OUTPUT_FPS},format=yuv420p`,
					"-c:v",
					"libx264",
					"-preset",
					INTERMEDIATE_PRESET,
					"-crf",
					String(INTERMEDIATE_VIDEO_CRF),
					"-pix_fmt",
					"yuv420p",
					"-movflags",
					"+faststart",
					"-y",
					outMp4Fallback,
				],
				"intro_fallback",
				{ timeoutMs: 180000 }
			);
			logJob(jobId, "intro motion fallback created", {
				path: path.basename(outMp4Fallback),
			});
			return outMp4Fallback;
		}
	}

	const outMp4 = path.join(
		path.dirname(presenterImagePath),
		`intro_motion_${jobId}.mp4`
	);
	await downloadToFile(outUrl, outMp4, 120000, 2);
	return outMp4;
}

function resolveFontFile() {
	const candidates = [
		"/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
		"/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
		"/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
		"/Library/Fonts/Arial.ttf",
		"C:/Windows/Fonts/arialbd.ttf",
		"C:/Windows/Fonts/arial.ttf",
	];
	for (const p of candidates) {
		try {
			if (p && fs.existsSync(p)) return p;
		} catch {}
	}
	return null;
}

function escapeDrawtext(s = "") {
	// escape characters used by drawtext
	const placeholder = "__NL__";
	return String(s || "")
		.replace(/\r\n|\r|\n/g, placeholder)
		.replace(/\\/g, "\\\\")
		.replace(/:/g, "\\:")
		.replace(/'/g, "\\'")
		.replace(/%/g, "\\%")
		.replace(/\[/g, "\\[")
		.replace(/\]/g, "\\]")
		.replace(new RegExp(placeholder, "g"), "\\n")
		.trim();
}

function resolveWatermarkFontFile() {
	const candidates = [
		"C:/Windows/Fonts/segoesc.ttf",
		"C:/Windows/Fonts/segoepr.ttf",
		"C:/Windows/Fonts/segoeprb.ttf",
		"C:/Windows/Fonts/BRUSHSCI.TTF",
		"C:/Windows/Fonts/ITCEDSCR.TTF",
		"/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
		"/Library/Fonts/Arial.ttf",
	];
	for (const p of candidates) {
		try {
			if (p && fs.existsSync(p)) return p;
		} catch {}
	}
	return resolveFontFile();
}

const WATERMARK_FONT_FILE = resolveWatermarkFontFile();
if (!WATERMARK_FONT_FILE) {
	console.warn(
		"[LongVideo] WARN - Watermark font not found. Falling back to default drawtext font."
	);
}

function buildWatermarkFilter() {
	const text = escapeDrawtext(WATERMARK_TEXT);
	const fontFile = WATERMARK_FONT_FILE
		? `:fontfile='${escapeDrawtext(WATERMARK_FONT_FILE)}'`
		: "";
	return (
		`drawtext=text='${text}'` +
		`${fontFile}` +
		`:fontsize=h*${WATERMARK_FONT_SIZE_PCT}` +
		`:fontcolor=white@${WATERMARK_OPACITY.toFixed(2)}` +
		`:shadowcolor=black@${WATERMARK_SHADOW_OPACITY.toFixed(2)}` +
		`:shadowx=${WATERMARK_SHADOW_PX}` +
		`:shadowy=${WATERMARK_SHADOW_PX}` +
		`:x=w*${WATERMARK_MARGIN_PCT}` +
		`:y=h-th-h*${WATERMARK_MARGIN_PCT}`
	);
}

function computeFinalMasterSize(outCfg = {}) {
	const baseW = Number(outCfg?.w || 0) || 1280;
	const baseH = Number(outCfg?.h || 0) || 720;
	const ratio = baseW > 0 && baseH > 0 ? baseW / baseH : 16 / 9;
	let targetH =
		baseH >= FINAL_MASTER_MAX_HEIGHT ? baseH : FINAL_MASTER_MAX_HEIGHT;
	if (targetH < FINAL_MASTER_MIN_HEIGHT) targetH = FINAL_MASTER_MIN_HEIGHT;
	const targetW = makeEven(targetH * ratio);
	return { w: makeEven(targetW), h: makeEven(targetH) };
}

function hardTruncateText(text = "", maxChars = 40) {
	const t = String(text || "").trim();
	if (t.length <= maxChars) return t;
	return t.slice(0, Math.max(0, maxChars)).trimEnd();
}

function wrapIntroText(text = "", maxCharsPerLine = 36, maxLines = 2) {
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

		// No more lines left: keep full text and let font-size handle overflow.
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

function fitIntroText(
	text = "",
	{ baseMaxChars = 36, preferLines = 2, maxLines = 3 } = {}
) {
	const clean = String(text || "").trim();
	if (!clean) return { text: "", fontScale: 1, lines: 0, truncated: false };

	let fontScale = 1.0;
	let maxChars = baseMaxChars;
	let wrap = wrapIntroText(clean, maxChars, preferLines);

	if (wrap.overflow || wrap.lines > preferLines) {
		wrap = wrapIntroText(clean, maxChars, maxLines);
	}

	if (wrap.overflow) {
		// Reduce font size before truncation to preserve full text.
		const scales = [0.94, 0.9, 0.86];
		for (const scale of scales) {
			fontScale = scale;
			maxChars = Math.round(baseMaxChars / scale);
			wrap = wrapIntroText(clean, maxChars, maxLines);
			if (!wrap.overflow) break;
		}
	}

	let truncated = false;
	if (wrap.overflow) {
		// Last resort: hard truncate without ellipsis.
		const maxTotal = maxChars * maxLines;
		const cut = hardTruncateText(clean, maxTotal);
		wrap = wrapIntroText(cut, maxChars, maxLines);
		truncated = cut.length < clean.length;
	}

	return { text: wrap.text, fontScale, lines: wrap.lines, truncated };
}

async function createIntroClip({
	title,
	subtitle,
	bgImagePath,
	durationSec,
	outCfg,
	outPath,
	disableVideoBlur = false,
}) {
	const W = makeEven(outCfg.w);
	const H = makeEven(outCfg.h);
	const fps = Number(outCfg.fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const minDur = Math.min(INTRO_MIN_SEC, OUTRO_MIN_SEC);
	const maxDur = Math.max(INTRO_MAX_SEC, OUTRO_MAX_SEC);
	const dur = clampNumber(
		Number(durationSec) || DEFAULT_INTRO_SEC,
		minDur,
		maxDur
	);

	const fontFile = resolveFontFile();
	const fontOpt = fontFile ? `:fontfile='${fontFile}'` : "";

	const titleMaxChars = Math.max(18, Math.round(W / 64));
	const subMaxChars = Math.max(22, Math.round(W / 52));
	// Keep intro text brief and easy to scan.
	const titleFit = fitIntroText(title || "", {
		baseMaxChars: titleMaxChars,
		preferLines: 1,
		maxLines: 2,
	});
	const subFit = fitIntroText(subtitle || "", {
		baseMaxChars: subMaxChars,
		preferLines: 1,
		maxLines: 2,
	});
	const safeTitle = escapeDrawtext(titleFit.text);
	const safeSub = escapeDrawtext(subFit.text);
	const titleFontSize = Math.max(
		16,
		Math.round(H * 0.048 * titleFit.fontScale)
	);
	const subFontSize = Math.max(12, Math.round(H * 0.032 * subFit.fontScale));
	const titleX = Math.round(W * INTRO_TEXT_X_PCT);
	const titleY = Math.round(H * INTRO_TEXT_Y_PCT);
	const subY = Math.round(H * INTRO_SUBTITLE_Y_PCT);
	const textInStart = INTRO_TEXT_FADE_IN_START;
	const textInDur = INTRO_TEXT_FADE_IN_DUR;
	const textInEnd = textInStart + textInDur;
	const alphaExpr = `if(lt(t,${textInStart.toFixed(
		2
	)}),0, if(lt(t,${textInEnd.toFixed(2)}),(t-${textInStart.toFixed(
		2
	)})/${textInDur.toFixed(2)}, 1))`;

	const bgKind = detectFileType(bgImagePath)?.kind;
	const isVideoBg = bgKind === "video";
	const bgInfo = isVideoBg ? await probeMedia(bgImagePath) : null;
	const needsSilentAudio = !bgInfo?.hasAudio;
	// A subtle motion background + title fade in
	const blurSigma = disableVideoBlur ? 0 : INTRO_VIDEO_BLUR_SIGMA;
	const videoBlur = blurSigma > 0 ? `,gblur=sigma=${blurSigma.toFixed(2)}` : "";
	const base = isVideoBg
		? `scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H}${videoBlur},fps=${fps},format=yuv420p,`
		: `scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H},gblur=sigma=18,` +
		  `zoompan=z='min(1.12,zoom+0.0025)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:fps=${fps},format=yuv420p,`;
	const vf =
		base +
		`drawtext=text='${safeTitle}'${fontOpt}:fontsize=${titleFontSize}:fontcolor=white:x=${titleX}:y=${titleY}-(text_h/2):shadowcolor=black:shadowx=2:shadowy=2:alpha='${alphaExpr}',` +
		(safeSub
			? `drawtext=text='${safeSub}'${fontOpt}:fontsize=${subFontSize}:fontcolor=white:x=${titleX}:y=${subY}-(text_h/2):shadowcolor=black:shadowx=2:shadowy=2:alpha='${alphaExpr}',`
			: "");

	const inputArgs = isVideoBg
		? needsSilentAudio
			? ["-stream_loop", "-1", "-i", bgImagePath]
			: ["-i", bgImagePath]
		: ["-loop", "1", "-i", bgImagePath];
	const audioArgs = needsSilentAudio
		? ["-f", "lavfi", "-i", `anullsrc=r=${AUDIO_SR}:cl=stereo`]
		: [];
	const audioMap = needsSilentAudio ? "1:a:0" : "0:a:0";

	await spawnBin(
		ffmpegPath,
		[
			...inputArgs,
			...audioArgs,
			"-t",
			dur.toFixed(3),
			"-vf",
			vf,
			"-map",
			"0:v:0",
			"-map",
			audioMap,
			"-r",
			String(fps),
			"-c:v",
			"libx264",
			"-preset",
			INTERMEDIATE_PRESET,
			"-crf",
			String(INTERMEDIATE_VIDEO_CRF),
			"-pix_fmt",
			"yuv420p",
			"-c:a",
			"aac",
			"-b:a",
			AUDIO_BITRATE,
			"-shortest",
			"-movflags",
			"+faststart",
			"-y",
			outPath,
		],
		"intro_clip",
		{ timeoutMs: 180000 }
	);

	return outPath;
}

/* ---------------------------------------------------------------
 * Overlays (simple, safe)
 * ------------------------------------------------------------- */

function positionToExpr(position = "topRight") {
	switch (position) {
		case "topLeft":
			return { x: String(OVERLAY_MARGIN_PX), y: String(OVERLAY_MARGIN_PX) };
		case "bottomLeft":
			return {
				x: String(OVERLAY_MARGIN_PX),
				y: `main_h-overlay_h-${OVERLAY_MARGIN_PX}`,
			};
		case "bottomRight":
			return {
				x: `main_w-overlay_w-${OVERLAY_MARGIN_PX}`,
				y: `main_h-overlay_h-${OVERLAY_MARGIN_PX}`,
			};
		case "center":
			return { x: "(main_w-overlay_w)/2", y: "(main_h-overlay_h)/2" };
		case "topRight":
		default:
			return {
				x: `main_w-overlay_w-${OVERLAY_MARGIN_PX}`,
				y: String(OVERLAY_MARGIN_PX),
			};
	}
}

function normalizeOverlayAssets(list = [], totalDurationSec) {
	if (!Array.isArray(list)) return [];
	const out = [];
	for (const raw of list) {
		if (!raw || typeof raw !== "object") continue;
		const startSec = Number(raw.startSec);
		const endSec = Number(raw.endSec);
		if (!Number.isFinite(startSec) || !Number.isFinite(endSec)) continue;
		if (endSec <= startSec) continue;
		const url = String(raw.url || "").trim();
		if (!url) continue;
		out.push({
			type: raw.type === "video" ? "video" : "image",
			url,
			startSec: clampNumber(startSec, 0, Math.max(1, totalDurationSec)),
			endSec: clampNumber(endSec, 0, Math.max(1, totalDurationSec)),
			position: String(raw.position || OVERLAY_DEFAULT_POSITION),
			scale: clampNumber(Number(raw.scale || OVERLAY_SCALE), 0.14, 0.6),
		});
	}
	return out;
}

function buildAutoOverlaysFromTimeline({
	timeline = [],
	images = [],
	introSec = 0,
	totalDurationSec = 0,
}) {
	const urls = Array.isArray(images) ? images.filter(Boolean) : [];
	if (!urls.length) return [];

	const count = Math.min(Math.max(3, Math.min(urls.length, 5)), urls.length);
	const segments = Array.isArray(timeline)
		? timeline.filter(
				(s) =>
					Number.isFinite(Number(s.startSec)) &&
					Number.isFinite(Number(s.endSec)) &&
					Number(s.endSec) > Number(s.startSec)
		  )
		: [];

	const positions = [OVERLAY_DEFAULT_POSITION];
	const n = Math.min(count, segments.length || count);
	const overlays = [];

	for (let i = 0; i < n; i++) {
		let startSec = 0;
		let endSec = 0;

		if (segments.length) {
			const idx = Math.min(
				segments.length - 1,
				Math.floor(((i + 0.5) * segments.length) / n)
			);
			const seg = segments[idx];
			const segDur = Math.max(0.6, Number(seg.endSec) - Number(seg.startSec));
			const win = clampNumber(segDur * 0.5, 2.2, 4.2);
			startSec = Number(seg.startSec) + Math.max(0.2, segDur * 0.2);
			endSec = Math.min(Number(seg.endSec) - 0.2, startSec + win);
		} else {
			const available = Math.max(
				1,
				Number(totalDurationSec || 0) - Number(introSec || 0)
			);
			const slotCenter =
				Number(introSec || 0) + (available / (n + 1)) * (i + 1);
			startSec = Math.max(Number(introSec || 0) + 0.2, slotCenter - 1.6);
			endSec = Math.min(Number(totalDurationSec || 0) - 0.2, startSec + 3.2);
		}

		if (endSec <= startSec) continue;
		overlays.push({
			type: "image",
			url: urls[i],
			startSec,
			endSec,
			position: positions[i % positions.length],
			scale: OVERLAY_SCALE,
		});
	}

	return overlays;
}

async function applyOverlays(baseVideoPath, overlays, outPath) {
	if (!overlays.length) {
		fs.copyFileSync(baseVideoPath, outPath);
		return outPath;
	}

	const inputs = ["-i", baseVideoPath];
	const filterParts = ["[0:v]format=yuv420p[base]"]; // base video
	let last = "base";

	overlays.forEach((ov, idx) => {
		if (ov.type === "image") inputs.push("-loop", "1");
		inputs.push("-i", ov.localPath);

		const inV = `${idx + 1}:v`;
		const prep = `ovp${idx}`;
		const scaled = `ovs${idx}`;
		const timed = OVERLAY_BORDER_PX > 0 ? `ovb${idx}` : scaled;
		const baseRef = `base${idx}`;
		const out = `v${idx}`;

		const dur = Math.max(0.1, ov.endSec - ov.startSec);
		const pos = positionToExpr(ov.position);

		filterParts.push(
			`[${inV}]format=rgba,trim=0:${dur.toFixed(3)},setpts=PTS-STARTPTS+${
				ov.startSec
			}/TB[${prep}]`
		);

		// Scale with a hard cap to avoid covering the presenter.
		filterParts.push(
			`[${prep}][${last}]scale2ref=w='min(iw*${ov.scale},main_w*${OVERLAY_MAX_WIDTH_PCT})':h='-1'[${scaled}][${baseRef}]`
		);

		if (OVERLAY_BORDER_PX > 0) {
			filterParts.push(
				`[${scaled}]pad=iw+${OVERLAY_BORDER_PX * 2}:ih+${
					OVERLAY_BORDER_PX * 2
				}:${OVERLAY_BORDER_PX}:${OVERLAY_BORDER_PX}:color=black@0.25[${timed}]`
			);
		}

		filterParts.push(
			`[${baseRef}][${timed}]overlay=${pos.x}:${
				pos.y
			}:enable='between(t,${ov.startSec.toFixed(3)},${ov.endSec.toFixed(
				3
			)})'[${out}]`
		);

		last = out;
	});

	filterParts.push(`[${last}]format=yuv420p[vout]`);

	await spawnBin(
		ffmpegPath,
		[
			...inputs,
			"-filter_complex",
			filterParts.join(";"),
			"-map",
			"[vout]",
			"-map",
			"0:a?",
			"-c:v",
			"libx264",
			"-preset",
			INTERMEDIATE_PRESET,
			"-crf",
			String(INTERMEDIATE_VIDEO_CRF),
			"-pix_fmt",
			"yuv420p",
			"-c:a",
			"aac",
			"-b:a",
			AUDIO_BITRATE,
			"-shortest",
			"-movflags",
			"+faststart",
			"-y",
			outPath,
		],
		"overlay",
		{ timeoutMs: 360000 }
	);

	return outPath;
}

/* ---------------------------------------------------------------
 * Music (Jamendo) - must validate audio
 * ------------------------------------------------------------- */

async function jamendoSearchTracks({
	fuzzytags,
	speed,
	instrumentalOnly = true,
}) {
	if (!JAMENDO_CLIENT_ID) return [];

	const params = {
		client_id: JAMENDO_CLIENT_ID,
		format: "json",
		limit: 20,
		fuzzytags: String(fuzzytags || "cinematic, trailer, energetic").replace(
			/\s+/g,
			"+"
		),
		include: "licenses",
		audioformat: "mp32",
		speed: Array.isArray(speed) && speed.length ? speed.join("+") : "medium",
		order: "popularity_total",
		...(instrumentalOnly ? { vocalinstrumental: "instrumental" } : {}),
	};

	const url = `${JAMENDO_BASE}/tracks/`;
	const res = await axios.get(url, {
		params,
		timeout: 15000,
		validateStatus: (s) => s < 500,
	});
	if (res.status >= 300) return [];

	const results = Array.isArray(res.data?.results) ? res.data.results : [];
	return results
		.map((t) => ({
			id: t.id,
			name: t.name,
			artist: t.artist_name,
			audio: t.audio,
			shareurl: t.shareurl,
			duration: Number(t.duration || 0),
		}))
		.filter((t) => t.audio && t.duration >= 30)
		.sort((a, b) => (b.duration || 0) - (a.duration || 0));
}

async function validateMusicFile(filePath) {
	if (!filePath || !fs.existsSync(filePath)) return false;
	const info = await probeMedia(filePath);
	if (!info.hasAudio) return false;
	if (!info.duration || info.duration < 10) return false;
	return true;
}

async function resolveBackgroundMusic({
	jobId,
	topic,
	disableMusic,
	requestedMusicUrl,
}) {
	if (disableMusic) return null;

	// 1) explicit musicUrl
	const musicUrl = String(requestedMusicUrl || "").trim();
	if (musicUrl) {
		const out = path.join(TMP_ROOT, `music_req_${jobId}.mp3`);
		await downloadToFile(musicUrl, out, 35000, 2);
		if (await validateMusicFile(out)) {
			logJob(jobId, "music ready (requested)", { path: path.basename(out) });
			return out;
		}
		safeUnlink(out);
		throw new Error("Requested musicUrl downloaded but is not valid audio");
	}

	// 2) Default env fallback (preferred)
	if (DEFAULT_MUSIC_PATH && fs.existsSync(DEFAULT_MUSIC_PATH)) {
		if (await validateMusicFile(DEFAULT_MUSIC_PATH)) {
			logJob(jobId, "music ready (default path)", { path: DEFAULT_MUSIC_PATH });
			return DEFAULT_MUSIC_PATH;
		}
	}
	if (DEFAULT_MUSIC_URL) {
		const out = path.join(TMP_ROOT, `music_default_${jobId}.mp3`);
		await downloadToFile(DEFAULT_MUSIC_URL, out, 35000, 2);
		if (await validateMusicFile(out)) {
			logJob(jobId, "music ready (default url)", { path: path.basename(out) });
			return out;
		}
		safeUnlink(out);
	}

	// 3) Jamendo based on topic (fallback)
	const tags = `cinematic, upbeat, modern, instrumental, ${String(
		topic || ""
	).slice(0, 40)}`;
	const speeds = ["medium", "high"];
	const candidates = await jamendoSearchTracks({
		fuzzytags: tags,
		speed: speeds,
		instrumentalOnly: true,
	});

	for (let i = 0; i < Math.min(10, candidates.length); i++) {
		const c = candidates[i];
		try {
			const out = path.join(TMP_ROOT, `music_${jobId}_${c.id}.mp3`);
			await downloadToFile(c.audio, out, 35000, 1);
			if (await validateMusicFile(out)) {
				logJob(jobId, "jamendo picked track", {
					id: c.id,
					name: c.name,
					artist: c.artist,
					duration: c.duration,
					shareurl: c.shareurl,
				});
				return out;
			}
			safeUnlink(out);
		} catch {
			// try next
		}
	}

	throw new Error(
		"Background music is required but could not be resolved. Provide JAMENDO_CLIENT_ID or musicUrl or LONG_VIDEO_DEFAULT_MUSIC_URL/PATH."
	);
}

async function mixBackgroundMusic(
	baseVideoPath,
	musicPath,
	outPath,
	{ jobId }
) {
	const dur = await probeDurationSeconds(baseVideoPath);
	const duration = dur && dur > 1 ? dur : null;

	const vol = MUSIC_VOLUME;
	const threshold = MUSIC_DUCK_THRESHOLD;
	const ratio = MUSIC_DUCK_RATIO;
	const attack = MUSIC_DUCK_ATTACK;
	const release = MUSIC_DUCK_RELEASE;
	const makeup = MUSIC_DUCK_MAKEUP;

	const args = ["-i", baseVideoPath, "-stream_loop", "-1", "-i", musicPath];
	const filter =
		`[0:a]aresample=${AUDIO_SR},aformat=channel_layouts=stereo:sample_fmts=fltp,asplit=2[vox][vox_sc];` +
		`[1:a]aresample=${AUDIO_SR},aformat=channel_layouts=stereo:sample_fmts=fltp,volume=${vol.toFixed(
			3
		)},atrim=0:${duration ? duration.toFixed(2) : "9999"}[music];` +
		`[music][vox_sc]sidechaincompress=threshold=${threshold.toFixed(
			3
		)}:ratio=${ratio.toFixed(2)}:attack=${attack.toFixed(
			0
		)}:release=${release.toFixed(0)}:makeup=${makeup.toFixed(2)}[ducked];` +
		`[vox][ducked]amix=inputs=2:duration=first:dropout_transition=0:normalize=0[aout]`;

	logJob(jobId, "music mix params", {
		volume: vol,
		duck: { threshold, ratio, attack, release, makeup },
		duration,
	});

	args.push(
		"-filter_complex",
		filter,
		"-map",
		"0:v",
		"-map",
		"[aout]",
		"-c:v",
		"copy",
		"-c:a",
		"aac",
		"-b:a",
		AUDIO_BITRATE,
		"-shortest",
		"-movflags",
		"+faststart",
		"-y",
		outPath
	);

	await spawnBin(ffmpegPath, args, "music_mix", { timeoutMs: 240000 });
	return outPath;
}

async function finalizeVideoWithFadeOut({
	inputPath,
	outputPath,
	fadeOutSec,
	outCfg = {},
}) {
	const dur = await probeDurationSeconds(inputPath);
	const safeFade =
		Number.isFinite(fadeOutSec) && fadeOutSec > 0 ? fadeOutSec : 0;
	const fadeDur = clampNumber(
		safeFade,
		0,
		dur > 0 ? Math.min(1.2, dur / 2) : 0
	);
	const shouldFade = Boolean(fadeDur && dur && dur >= 0.4);
	const start = shouldFade ? Math.max(0, dur - fadeDur) : 0;

	const { w: outW, h: outH } = computeFinalMasterSize(outCfg);
	const fps = Number(outCfg?.fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const gop = Math.max(12, Math.round(fps * FINAL_GOP_SECONDS));

	const videoFilters = [
		`scale=${outW}:${outH}:force_original_aspect_ratio=increase:flags=lanczos,crop=${outW}:${outH}`,
		`fps=${fps}`,
		buildWatermarkFilter(),
	];
	if (shouldFade) {
		videoFilters.push(
			`fade=t=out:st=${start.toFixed(3)}:d=${fadeDur.toFixed(3)}`
		);
	}
	videoFilters.push("format=yuv420p");

	const audioFilters = [
		`aresample=${AUDIO_SR}`,
		"aformat=channel_layouts=stereo:sample_fmts=fltp",
	];
	if (shouldFade) {
		audioFilters.push(
			`afade=t=out:st=${start.toFixed(3)}:d=${fadeDur.toFixed(3)}`
		);
	}
	audioFilters.push(FINAL_LOUDNORM_FILTER);

	await spawnBin(
		ffmpegPath,
		[
			"-i",
			inputPath,
			"-vf",
			videoFilters.join(","),
			"-af",
			audioFilters.join(","),
			"-c:v",
			"libx264",
			"-preset",
			FINAL_PRESET,
			"-crf",
			String(FINAL_VIDEO_CRF),
			"-pix_fmt",
			"yuv420p",
			"-g",
			String(gop),
			"-keyint_min",
			String(gop),
			"-sc_threshold",
			"0",
			"-c:a",
			"aac",
			"-b:a",
			AUDIO_BITRATE,
			"-ar",
			String(AUDIO_SR),
			"-ac",
			"2",
			"-movflags",
			"+faststart",
			"-colorspace",
			FINAL_COLOR_SPACE,
			"-color_primaries",
			FINAL_COLOR_SPACE,
			"-color_trc",
			FINAL_COLOR_SPACE,
			"-color_range",
			FINAL_COLOR_RANGE,
			"-y",
			outputPath,
		],
		"final_master",
		{ timeoutMs: 480000 }
	);
}

/* ---------------------------------------------------------------
 * Scheduling helpers (kept)
 * ------------------------------------------------------------- */

function parseTimeOfDay(timeOfDay) {
	const raw = String(timeOfDay || "").trim();
	const m = raw.match(/^(\d{1,2}):(\d{2})$/);
	if (!m) return null;
	const hh = Number(m[1]);
	const mm = Number(m[2]);
	if (!Number.isInteger(hh) || !Number.isInteger(mm)) return null;
	if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
	return { hh, mm };
}

function computeNextRun({ scheduleType, timeOfDay, startDate }) {
	const t = parseTimeOfDay(timeOfDay);
	if (!t) return null;
	let next = dayjs(startDate).hour(t.hh).minute(t.mm).second(0);
	if (next.isBefore(dayjs())) {
		if (scheduleType === "daily") next = next.add(1, "day");
		else if (scheduleType === "weekly") next = next.add(1, "week");
		else if (scheduleType === "monthly") next = next.add(1, "month");
	}
	return next.toDate();
}

/* ---------------------------------------------------------------
 * Job runner
 * ------------------------------------------------------------- */

async function runLongVideoJob(jobId, payload, baseUrl, user = null) {
	const tmpDir = path.join(TMP_ROOT, `job_${jobId}`);
	ensureDir(tmpDir);

	try {
		updateJob(jobId, { status: "running", progressPct: 1 });

		const {
			preferredTopicHint,
			category,
			language,
			targetDurationSec,
			output,
			presenterAssetUrl,
			voiceoverUrl,
			voiceId,
			musicUrl,
			disableMusic,
			dryRun,
			overlayAssets,
			youtubeAccessToken,
			youtubeRefreshToken,
			youtubeTokenExpiresAt,
			youtubeCategory,
		} = payload;
		const enableRunwayPresenterMotion = true;
		const enableWardrobeEdit = true;
		const effectiveVoiceId = String(voiceId || ELEVEN_FIXED_VOICE_ID).trim();
		const contentTargetSec = Number(targetDurationSec || 0);
		const categoryLabel =
			normalizeCategoryLabel(category) || LONG_VIDEO_TRENDS_CATEGORY;
		let introDurationSec = clampNumber(
			DEFAULT_INTRO_SEC,
			INTRO_MIN_SEC,
			INTRO_MAX_SEC
		);
		let outroDurationSec = clampNumber(
			DEFAULT_OUTRO_SEC,
			OUTRO_MIN_SEC,
			OUTRO_MAX_SEC
		);
		const totalTargetSec =
			introDurationSec + contentTargetSec + outroDurationSec;
		const hasYouTubeTokens = Boolean(
			youtubeRefreshToken || youtubeAccessToken || user?.youtubeRefreshToken
		);
		let thumbnailPath = "";
		let thumbnailUrl = "";
		let thumbnailPublicId = "";
		let topicSourceSummary = [];

		logJob(jobId, "job started", {
			dryRun,
			requestedTargetSec: Number(targetDurationSec || 0),
			contentTargetSec,
			category: categoryLabel,
			introSec: introDurationSec,
			outroSec: outroDurationSec,
			totalTargetSec,
			output,
			presenterAssetUrl: presenterAssetUrl ? "(provided)" : "(none)",
			hasVoiceoverUrl: Boolean(voiceoverUrl),
			hasMusicUrl: Boolean(musicUrl),
			hasCseKeys: Boolean(GOOGLE_CSE_ID && GOOGLE_CSE_KEY),
			hasRunway: Boolean(RUNWAY_API_KEY),
			enableRunwayPresenterMotion: Boolean(enableRunwayPresenterMotion),
			enableWardrobeEdit: Boolean(enableWardrobeEdit),
			voiceIdLocked: effectiveVoiceId,
			hasYouTubeTokens,
		});

		if (dryRun) {
			const dummyUrl = LONG_VIDEO_PERSIST_OUTPUT
				? `${baseUrl}/uploads/videos/long_${jobId}_dryrun.mp4`
				: "";
			updateJob(jobId, {
				status: "completed",
				progressPct: 100,
				finalVideoUrl: dummyUrl || null,
			});
			logJob(jobId, "dry run completed", {
				finalVideoUrl: dummyUrl || null,
			});
			return;
		}

		if (!ffmpegPath)
			throw new Error("FFmpeg not found. Install ffmpeg or set FFMPEG_PATH.");
		if (!SYNC_SO_API_KEY) throw new Error("SYNC_SO_API_KEY missing.");
		if (!RUNWAY_API_KEY)
			throw new Error(
				"RUNWAY_API_KEY missing (required for presenter pipeline)."
			);
		if (!process.env.CHATGPT_API_TOKEN)
			throw new Error("CHATGPT_API_TOKEN missing.");
		if (!ELEVEN_API_KEY)
			throw new Error(
				"ELEVENLABS_API_KEY missing (required for intro/outro voice)."
			);
		if (!effectiveVoiceId) throw new Error("ELEVENLABS voiceId missing.");

		updateJob(jobId, { progressPct: 4 });

		// 1) Topics (Google Trends driven, count based on duration)
		const languageLabel = normalizeLanguageLabel(language || "English");
		const topicCount = topicCountForDuration(contentTargetSec);
		const usedTopics = await loadRecentLongVideoTopics({
			userId: user?._id,
			categoryLabel,
		});
		const recentOutfits = await loadRecentPresenterOutfits({
			userId: user?._id,
			limit: 10,
		});
		const topicPicks = await selectTopics({
			preferredTopicHint,
			dryRun,
			topicCount,
			language: languageLabel,
			categoryLabel,
			usedTopics,
			baseUrl,
		});
		const topicTitles = topicPicks
			.map((t) => t.displayTopic || t.topic)
			.filter(Boolean);
		const topicSummary = topicTitles.join(" / ");
		logJob(jobId, "topics selected", {
			count: topicPicks.length,
			topicCount,
			topics: topicTitles,
			reasons: topicPicks.map((t) => t.reason || "").filter(Boolean),
			category: categoryLabel,
			usedTopicsCount: usedTopics.size,
		});
		const topicChoiceDetails = topicPicks.map((t, idx) => {
			const story = t.trendStory || {};
			const related = normalizeRelatedQueries(story.relatedQueries);
			const interest = normalizeInterestOverTime(story.interestOverTime);
			const articleUrls = uniqueStrings(
				(Array.isArray(story.articles) ? story.articles : [])
					.map((a) => a?.url)
					.filter((u) => isHttpUrl(u)),
				{ limit: 6 }
			);
			const articleHosts = uniqueStrings(
				articleUrls.map((u) => getUrlHost(u)).filter(Boolean),
				{ limit: 6 }
			);
			return {
				index: idx,
				topic: t.topic,
				displayTopic: t.displayTopic || t.topic,
				reason: t.reason || "",
				angle: t.angle || "",
				trendScore: Number(story.trendScore) || 0,
				interestOverTime: interest,
				relatedQueries: {
					topCount: related.top.length,
					risingCount: related.rising.length,
					topSample: related.top.slice(0, 5),
					risingSample: related.rising.slice(0, 5),
				},
				articleHosts,
				articleUrls,
				keywords: Array.isArray(t.keywords) ? t.keywords.slice(0, 10) : [],
			};
		});
		logJob(jobId, "topics chosen (detail)", { topics: topicChoiceDetails });
		updateJob(jobId, {
			progressPct: 8,
			topic: topicSummary,
			meta: {
				topics: topicPicks.map((t) => ({
					topic: t.topic,
					displayTopic: t.displayTopic || t.topic,
					reason: t.reason || "",
					angle: t.angle || "",
				})),
				category: categoryLabel,
			},
		});

		// 2) Presenter (forced default image + motion reference)
		let presenterLocal = await ensureLocalPresenterAsset(
			presenterAssetUrl,
			tmpDir,
			jobId
		);
		const motionRefVideo = await ensureLocalMotionReferenceVideo(tmpDir, jobId);
		const detected = detectFileType(presenterLocal);
		let presenterIsVideo = detected?.kind === "video";
		let presenterIsImage = detected?.kind === "image";
		let presenterOutfit = "";
		if (!presenterIsImage)
			throw new Error("Presenter asset must be a valid image");

		logJob(jobId, "presenter asset ready", {
			path: path.basename(presenterLocal),
			detected: detected?.kind || "unknown",
			hasMotionRef: Boolean(motionRefVideo),
		});

		updateJob(jobId, { progressPct: 12 });

		// 4) Context + images (optional)
		const topicContexts = [];
		let liveContext = [];
		for (const t of topicPicks) {
			const extraTokens = Array.isArray(t.keywords) ? t.keywords : [];
			const ctx = await fetchCseContext(t.topic, extraTokens);
			const trendContext = uniqueStrings(
				[
					...(Array.isArray(t.trendStory?.searchPhrases)
						? t.trendStory.searchPhrases
						: []),
					...(Array.isArray(t.trendStory?.entityNames)
						? t.trendStory.entityNames
						: []),
					...(Array.isArray(t.trendStory?.relatedQueries?.rising)
						? t.trendStory.relatedQueries.rising
						: []),
					...(Array.isArray(t.trendStory?.relatedQueries?.top)
						? t.trendStory.relatedQueries.top
						: []),
					...(Array.isArray(t.trendStory?.articles)
						? t.trendStory.articles.map((a) => a?.title)
						: []),
					t.trendStory?.imageComment,
				].filter(Boolean),
				{ limit: 8 }
			);
			const mergedContext = Array.isArray(ctx)
				? ctx.concat(trendContext)
				: trendContext;
			topicContexts.push({ topic: t.topic, context: mergedContext });
			liveContext = liveContext.concat(mergedContext || []);
		}
		const cseImages = [];
		logJob(jobId, "cse context", {
			count: liveContext.length,
			byTopic: topicContexts.map((tc) => ({
				topic: tc.topic,
				count: Array.isArray(tc.context) ? tc.context.length : 0,
			})),
		});
		logJob(jobId, "cse images", { count: cseImages.length });
		topicSourceSummary = topicContexts.map((tc, idx) => {
			const contextItems = Array.isArray(tc.context) ? tc.context : [];
			const cseLinks = uniqueStrings(
				contextItems.map((c) => c?.link).filter((u) => isHttpUrl(u)),
				{ limit: 6 }
			);
			const cseHosts = uniqueStrings(
				cseLinks.map((u) => getUrlHost(u)).filter(Boolean),
				{ limit: 6 }
			);
			const story = topicPicks[idx]?.trendStory || {};
			const articleUrls = uniqueStrings(
				(Array.isArray(story.articles) ? story.articles : [])
					.map((a) => a?.url)
					.filter((u) => isHttpUrl(u)),
				{ limit: 6 }
			);
			const articleHosts = uniqueStrings(
				articleUrls.map((u) => getUrlHost(u)).filter(Boolean),
				{ limit: 6 }
			);
			const related = normalizeRelatedQueries(story.relatedQueries);
			const interest = normalizeInterestOverTime(story.interestOverTime);
			return {
				topic: tc.topic,
				contextCount: contextItems.length,
				cseHosts,
				cseLinks,
				articleHosts,
				articleUrls,
				relatedQueries: {
					topSample: related.top.slice(0, 5),
					risingSample: related.rising.slice(0, 5),
				},
				interestOverTime: interest,
			};
		});
		logJob(jobId, "topic sources", { topics: topicSourceSummary });
		const topicContextFlags = topicContexts.map((tc, idx) => {
			const items = Array.isArray(tc.context) ? tc.context : [];
			const topicObj = Array.isArray(topicPicks) ? topicPicks[idx] : null;
			const contextStrings = buildTopicContextStrings(topicObj || tc, items);
			const contextText = contextStrings.join(" ");
			return {
				topic: tc.topic,
				isFictional: detectFictionalContext(contextText),
			};
		});
		const hasFictionalTopic = topicContextFlags.some((t) => t.isFictional);
		const allFictionalTopics =
			topicContextFlags.length && topicContextFlags.every((t) => t.isFictional);
		const contentType = allFictionalTopics
			? "fictional"
			: hasFictionalTopic
			? "mixed"
			: "real";
		const tonePlan = inferTonePlan({
			topics: topicPicks,
			liveContext,
		});
		if (contentType === "fictional" && tonePlan.mood === "serious") {
			tonePlan.mood = "neutral";
		}
		tonePlan.contentType = contentType;
		tonePlan.topicContextFlags = topicContextFlags;
		logJob(jobId, "topic context flags", {
			contentType,
			topics: topicContextFlags.map((t) => ({
				topic: t.topic,
				isFictional: t.isFictional,
			})),
		});

		// 5) Script (content duration excludes intro/outro)
		const lang = languageLabel || String(language || "en");
		const narrationPlan = computeFlexibleNarrationTargetSec({
			requestedSec: contentTargetSec,
			topics: topicPicks,
			topicContexts,
		});
		const narrationTargetSec = Math.max(
			18,
			Number(narrationPlan?.targetSec || contentTargetSec) || 0
		);
		const segmentCount = computeSegmentCount(narrationTargetSec);
		const wordCaps = buildWordCaps(segmentCount, narrationTargetSec);
		logJob(jobId, "narration target planned", {
			requestedSec: Number(contentTargetSec || 0),
			targetSec: Number(narrationTargetSec || 0),
			minSec: narrationPlan?.minSec,
			maxSec: narrationPlan?.maxSec,
			mode: narrationPlan?.mode,
			signal: narrationPlan?.signal,
		});

		let script = await generateScript({
			jobId,
			topics: topicPicks,
			languageLabel: lang,
			narrationTargetSec,
			segmentCount,
			wordCaps,
			topicContexts,
			tonePlan,
			topicContextFlags,
			includeOutro: true,
		});

		let qaResult = analyzeScriptQuality({
			script,
			topics: topicPicks,
			topicContexts,
			wordCaps,
		});
		logJob(jobId, "script qa", {
			pass: qaResult.pass,
			needsRewrite: qaResult.needsRewrite,
			issues: qaResult.issues,
			warnings: qaResult.warnings,
			stats: qaResult.stats,
		});

		for (let qaAttempt = 0; qaAttempt < MAX_QA_REWRITES; qaAttempt++) {
			if (!qaResult.needsRewrite) break;
			logJob(jobId, "script qa rewrite start", { attempt: qaAttempt + 1 });
			try {
				script = await rewriteSegmentsForQuality({
					jobId,
					script,
					topics: topicPicks,
					topicContexts,
					topicContextFlags,
					wordCaps,
					tonePlan,
					narrationTargetSec,
					includeOutro: true,
				});
			} catch (e) {
				logJob(jobId, "script qa rewrite failed", {
					attempt: qaAttempt + 1,
					error: e.message,
				});
				break;
			}
			qaResult = analyzeScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
			});
			logJob(jobId, "script qa rewrite result", {
				attempt: qaAttempt + 1,
				pass: qaResult.pass,
				needsRewrite: qaResult.needsRewrite,
				issues: qaResult.issues,
				warnings: qaResult.warnings,
				stats: qaResult.stats,
			});
		}
		if (!qaResult.pass) {
			throw new Error(
				`script_qa_failed:${qaResult.issues.join("|") || "unknown"}`
			);
		}

		const attributionFix = ensureTopicAttributions({
			script,
			topics: topicPicks,
			topicContexts,
			topicContextFlags,
			wordCaps,
			log: (message, payload) => logJob(jobId, message, payload),
		});
		if (attributionFix.didInsert) {
			qaResult = analyzeScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
			});
			logJob(jobId, "script qa attribution fix", {
				inserted: attributionFix.inserted,
				qa: qaResult,
			});
		}

		const scriptEngagement = summarizeScriptEngagement(script);
		logJob(jobId, "script qa summary", {
			qa: qaResult,
			engagement: scriptEngagement,
			sources: topicSourceSummary,
		});
		logJob(jobId, `script text (post QA)\n${buildScriptLogText(script)}`);

		updateJob(jobId, {
			progressPct: 18,
			meta: {
				...JOBS.get(jobId)?.meta,
				title: script.title,
				shortTitle: script.shortTitle,
				narrationPlan: {
					requestedSec: Number(contentTargetSec || 0),
					targetSec: Number(narrationTargetSec || 0),
					minSec: narrationPlan?.minSec,
					maxSec: narrationPlan?.maxSec,
					mode: narrationPlan?.mode,
					signal: narrationPlan?.signal,
				},
				scriptQa: {
					pass: qaResult.pass,
					issues: qaResult.issues,
					warnings: qaResult.warnings,
					stats: qaResult.stats,
				},
				script: { title: script.title, segments: script.segments },
			},
		});

		// 5.5) Presenter wardrobe adjustment (post-script)
		if (enableWardrobeEdit && presenterIsImage) {
			try {
				const presenterTitle = String(
					script.title || topicSummary || topicTitles[0] || ""
				).trim();
				const presenterResult = await generatePresenterAdjustedImage({
					jobId,
					tmpDir,
					presenterLocalPath: presenterLocal,
					title: presenterTitle,
					topics: topicPicks,
					categoryLabel,
					recentOutfits,
					log: (message, payload) => logJob(jobId, message, payload),
				});
				if (
					presenterResult?.localPath &&
					fs.existsSync(presenterResult.localPath)
				) {
					const adjustedDetected = detectFileType(presenterResult.localPath);
					if (adjustedDetected?.kind === "image") {
						presenterLocal = presenterResult.localPath;
						presenterIsVideo = false;
						presenterIsImage = true;
						presenterOutfit = String(
							presenterResult.presenterOutfit || ""
						).trim();
						logJob(jobId, "presenter adjustments ready", {
							path: path.basename(presenterLocal),
							method: presenterResult.method || "runway",
							cloudinary: Boolean(presenterResult.url),
						});
						updateJob(jobId, {
							meta: {
								...JOBS.get(jobId)?.meta,
								presenterImageUrl: presenterResult.url || "",
								presenterOutfit,
							},
						});
					} else {
						logJob(jobId, "presenter adjustments invalid; using original", {
							detected: adjustedDetected?.kind || "unknown",
						});
					}
				}
			} catch (e) {
				logJob(jobId, "presenter adjustments failed; using original", {
					error: e.message,
				});
			}
		} else if (enableWardrobeEdit && !presenterIsImage) {
			logJob(jobId, "presenter adjustments skipped (non-image presenter)", {
				detected: presenterIsVideo ? "video" : "unknown",
			});
		}

		// 5.6) Thumbnail (script-aligned, uses adjusted presenter when available)
		try {
			const fallbackTitle = topicTitles[0] || topicSummary || "Quick Update";
			const thumbTitle = String(script.title || fallbackTitle).trim();
			const thumbShortTitle = String(
				script.shortTitle || shortTitleFromText(thumbTitle)
			).trim();
			const thumbExpression =
				script?.segments?.[0]?.expression || tonePlan?.mood || "warm";
			const thumbLog = (message, payload) => logJob(jobId, message, payload);
			const hookPlan = buildThumbnailHookPlan({
				title: thumbTitle,
				topicPicks,
			});
			if (hookPlan) thumbLog("thumbnail hook plan (computed)", hookPlan);
			const hookHeadline = String(hookPlan?.headline || "").trim();
			const resolvedShortTitle = hookHeadline || thumbShortTitle;
			const thumbResult = await generateThumbnailPackage({
				jobId,
				tmpDir,
				presenterLocalPath: presenterLocal,
				title: thumbTitle,
				shortTitle: resolvedShortTitle,
				seoTitle: "",
				topics: topicPicks,
				expression: thumbExpression,
				openai,
				log: thumbLog,
				requireTopicImages: true,
				overrideHeadline: hookHeadline,
				overrideBadgeText: hookPlan?.badgeText,
				overrideIntent: hookPlan?.intent,
				overrideTopicImageQueries: hookPlan?.imageQueries,
			});
			const thumbLocalPath = thumbResult?.localPath || "";
			const thumbCloudUrl = thumbResult?.url || "";
			const thumbPublicId = thumbResult?.publicId || "";
			const thumbVariants = Array.isArray(thumbResult?.variants)
				? thumbResult.variants
				: [];
			thumbnailUrl = thumbCloudUrl;
			thumbnailPublicId = thumbPublicId;
			if (thumbLocalPath && fs.existsSync(thumbLocalPath)) {
				thumbnailPath = thumbLocalPath;
				if (LONG_VIDEO_PERSIST_OUTPUT) {
					const finalThumb = path.join(THUMBNAIL_DIR, `thumb_${jobId}.jpg`);
					fs.copyFileSync(thumbLocalPath, finalThumb);
					thumbnailPath = finalThumb;
				}
			}

			updateJob(jobId, {
				meta: {
					...JOBS.get(jobId)?.meta,
					thumbnailPath: LONG_VIDEO_PERSIST_OUTPUT ? thumbnailPath : "",
					thumbnailUrl: thumbnailUrl || "",
					thumbnailPublicId: thumbnailPublicId || "",
					thumbnailVariants: thumbVariants,
				},
			});
			logJob(jobId, "thumbnail ready", {
				path: thumbnailPath ? path.basename(thumbnailPath) : null,
				cloudinary: Boolean(thumbnailUrl),
				pose: thumbResult?.pose || null,
				accent: thumbResult?.accent || null,
				variants: thumbVariants.map((v) => v.variant).filter(Boolean),
			});
		} catch (e) {
			logJob(jobId, "thumbnail generation failed (hard stop)", {
				error: e.message,
			});
			throw e;
		}

		const seoMeta = await buildSeoMetadata({
			topics: topicPicks,
			scriptTitle: script.title,
			languageLabel: lang,
		});
		const youtubeCategoryFinal = YT_CATEGORY_MAP[youtubeCategory]
			? youtubeCategory
			: LONG_VIDEO_YT_CATEGORY;
		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				seoTitle: seoMeta.seoTitle,
				seoDescription: seoMeta.seoDescription,
				tags: seoMeta.tags,
				youtubeCategory: youtubeCategoryFinal,
			},
		});

		// 6) Orchestrator plan (intro/outro) + voice prep
		const introOutroMood = tonePlan?.mood || "neutral";
		const lastSegmentText =
			script?.segments && script.segments.length
				? script.segments[script.segments.length - 1].text || ""
				: "";
		const lastSegmentHasQuestion = /\?/.test(String(lastSegmentText || ""));
		const includeOutroQuestion = !(
			topicPicks.length === 1 && lastSegmentHasQuestion
		);
		const introLine = buildIntroLine({
			topics: topicPicks,
			shortTitle: script.shortTitle || script.title,
			mood: introOutroMood,
			jobId,
		});
		const outroLine = buildOutroLine({
			topics: topicPicks,
			shortTitle: script.shortTitle || script.title,
			mood: introOutroMood,
			includeQuestion: includeOutroQuestion,
		});
		const introText =
			sanitizeIntroOutroLine(introLine) || String(introLine || "").trim();
		const outroText =
			sanitizeIntroOutroLine(outroLine) || String(outroLine || "").trim();
		let introTextFinal = introText;
		let outroTextFinal = outroText;
		const introExpression = "neutral";
		const outroExpression = "neutral";

		logJob(jobId, "orchestrator plan", {
			mood: introOutroMood,
			contentTargetSec,
			intro: {
				text: introText,
				targetSec: introDurationSec,
				expression: introExpression,
			},
			outro: {
				text: outroText,
				targetSec: outroDurationSec,
				expression: outroExpression,
			},
		});

		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				intro: { text: introText, targetSec: introDurationSec },
				outro: { text: outroText, targetSec: outroDurationSec },
			},
		});

		const lockedVoiceSettings =
			UNIFORM_TTS_VOICE_SETTINGS || FORCE_NEUTRAL_VOICEOVER
				? buildVoiceSettingsForExpression("neutral", "neutral", "", {
						uniform: true,
						forceNeutral: FORCE_NEUTRAL_VOICEOVER,
				  })
				: null;
		const resolveVoiceSettings = (expression, text) =>
			lockedVoiceSettings ||
			buildVoiceSettingsForExpression(expression, tonePlan?.mood, text, {
				forceNeutral: FORCE_NEUTRAL_VOICEOVER,
			});
		const ttsModelOrder = [
			ELEVEN_TTS_MODEL,
			...ELEVEN_TTS_MODEL_FALLBACKS,
		].filter(Boolean);
		let ttsModelId = "";

		const introVoiceSettings = resolveVoiceSettings(introExpression, introText);
		logJob(jobId, "intro tts request", {
			text: introText,
			words: countWords(introText),
			expression: introExpression,
			mood: introOutroMood,
			voiceId: effectiveVoiceId,
			voiceSettings: introVoiceSettings,
			modelOrder: ttsModelOrder,
		});
		const introTts = await synthesizeTtsWav({
			text: introText,
			tmpDir,
			jobId,
			label: "intro",
			voiceId: effectiveVoiceId,
			voiceSettings: introVoiceSettings,
			modelId: ttsModelId || undefined,
			modelOrder: ttsModelOrder,
		});
		if (introTts?.modelId) ttsModelId = introTts.modelId;
		introTextFinal = introTts?.text || introText;
		if (!introTts.durationSec)
			throw new Error("Intro voice generation failed (empty duration)");
		let introAudioPath = introTts.wavPath;
		introDurationSec = introTts.durationSec || introDurationSec;
		let introAtempo = 1;
		let introRawAtempo = 1;
		if (introDurationSec < INTRO_MIN_SEC || introDurationSec > INTRO_MAX_SEC) {
			const introTargetSec = clampNumber(
				introDurationSec,
				INTRO_MIN_SEC,
				INTRO_MAX_SEC
			);
			const introFit = await fitWavToTargetDuration({
				wavPath: introAudioPath,
				targetSec: introTargetSec,
				minAtempo: INTRO_ATEMPO_MIN,
				maxAtempo: INTRO_ATEMPO_MAX,
				tmpDir,
				jobId,
				label: "intro",
			});
			if (introFit.durationSec) {
				introAudioPath = introFit.wavPath;
				introDurationSec = introFit.durationSec || introDurationSec;
				introAtempo = introFit.atempo || 1;
				introRawAtempo = introFit.rawAtempo || 1;
			}
		}
		if (introDurationSec < INTRO_MIN_SEC || introDurationSec > INTRO_MAX_SEC) {
			logJob(jobId, "intro duration outside target range", {
				introDurationSec: Number(introDurationSec.toFixed(3)),
				targetMin: INTRO_MIN_SEC,
				targetMax: INTRO_MAX_SEC,
			});
		}
		logJob(jobId, "intro voice ready", {
			durationSec: Number((introDurationSec || 0).toFixed(3)),
			atempo: Number(introAtempo.toFixed(3)),
			rawAtempo: Number(introRawAtempo.toFixed(3)),
			text: introTextFinal,
			words: countWords(introTextFinal),
			voiceSettings: introVoiceSettings,
			modelId: ttsModelId || "auto",
		});

		const outroVoiceSettings = resolveVoiceSettings(outroExpression, outroText);
		logJob(jobId, "outro tts request", {
			text: outroText,
			words: countWords(outroText),
			expression: outroExpression,
			mood: introOutroMood,
			voiceId: effectiveVoiceId,
			voiceSettings: outroVoiceSettings,
			modelOrder: ttsModelOrder,
		});
		const outroTts = await synthesizeTtsWav({
			text: outroText,
			tmpDir,
			jobId,
			label: "outro",
			voiceId: effectiveVoiceId,
			voiceSettings: outroVoiceSettings,
			modelId: ttsModelId || undefined,
			modelOrder: ttsModelOrder,
		});
		if (outroTts?.modelId) ttsModelId = outroTts.modelId;
		outroTextFinal = outroTts?.text || outroText;
		if (!outroTts.durationSec)
			throw new Error("Outro voice generation failed (empty duration)");
		let outroAudioPath = outroTts.wavPath;
		outroDurationSec = outroTts.durationSec || outroDurationSec;
		let outroAtempo = 1;
		let outroRawAtempo = 1;
		if (outroDurationSec < OUTRO_MIN_SEC || outroDurationSec > OUTRO_MAX_SEC) {
			const outroTargetSec = clampNumber(
				outroDurationSec,
				OUTRO_MIN_SEC,
				OUTRO_MAX_SEC
			);
			const outroFit = await fitWavToTargetDuration({
				wavPath: outroAudioPath,
				targetSec: outroTargetSec,
				minAtempo: OUTRO_ATEMPO_MIN,
				maxAtempo: OUTRO_ATEMPO_MAX,
				tmpDir,
				jobId,
				label: "outro",
			});
			if (outroFit.durationSec) {
				outroAudioPath = outroFit.wavPath;
				outroDurationSec = outroFit.durationSec || outroDurationSec;
				outroAtempo = outroFit.atempo || 1;
				outroRawAtempo = outroFit.rawAtempo || 1;
			}
		}
		if (outroDurationSec < OUTRO_MIN_SEC || outroDurationSec > OUTRO_MAX_SEC) {
			logJob(jobId, "outro duration outside target range", {
				outroDurationSec: Number(outroDurationSec.toFixed(3)),
				targetMin: OUTRO_MIN_SEC,
				targetMax: OUTRO_MAX_SEC,
			});
		}
		logJob(jobId, "outro voice ready", {
			durationSec: Number((outroDurationSec || 0).toFixed(3)),
			atempo: Number(outroAtempo.toFixed(3)),
			rawAtempo: Number(outroRawAtempo.toFixed(3)),
			text: outroTextFinal,
			words: countWords(outroTextFinal),
			voiceSettings: outroVoiceSettings,
			modelId: ttsModelId || "auto",
		});

		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				intro: { text: introTextFinal, targetSec: introDurationSec },
				outro: { text: outroTextFinal, targetSec: outroDurationSec },
			},
		});

		updateJob(jobId, { progressPct: 22 });

		// 7) Resolve background music (MUST unless disabled)
		const musicLocalPath = await resolveBackgroundMusic({
			jobId,
			topic: topicTitles[0] || topicSummary,
			disableMusic,
			requestedMusicUrl: musicUrl,
		});

		updateJob(jobId, { progressPct: 26 });

		// 8) Build narration audio segments
		let segments = script.segments.map((s, idx) => ({
			index: idx,
			text: s.text,
			topicIndex: Number.isFinite(Number(s.topicIndex))
				? Number(s.topicIndex)
				: 0,
			topicLabel: String(s.topicLabel || "").trim(),
			expression: coerceExpressionForNaturalness(
				normalizeExpression(s.expression, tonePlan?.mood),
				s.text,
				tonePlan?.mood
			),
			overlayCues: Array.isArray(s.overlayCues) ? s.overlayCues : [],
		}));
		const smoothedExpressions = smoothExpressionPlan(
			segments.map((s) => s.expression),
			tonePlan?.mood
		);
		const segmentsWithExpressions = segments.map((s, i) => ({
			...s,
			expression: smoothedExpressions[i] || s.expression,
		}));
		const videoExpressionPlan = buildSubtleVideoExpressionPlan(
			segmentsWithExpressions,
			tonePlan?.mood,
			jobId
		);
		segments = segmentsWithExpressions.map((s, i) => ({
			...s,
			videoExpression: videoExpressionPlan[i] || "neutral",
			topicIndex:
				Number.isFinite(Number(s.topicIndex)) && Number(s.topicIndex) >= 0
					? Number(s.topicIndex)
					: 0,
			topicLabel:
				String(s.topicLabel || "").trim() ||
				String(
					topicPicks?.[Number(s.topicIndex)]?.displayTopic ||
						topicPicks?.[Number(s.topicIndex)]?.topic ||
						""
				).trim() ||
				String(topicTitles[0] || "").trim(),
		}));

		let cleanedWavs = [];
		let sumCleanDur = 0;
		let globalAtempo = 1;
		let driftSec = 0;
		let autoOverlayAssets = [];
		let segmentImagePaths = new Map();
		const maxRewriteAttempts = voiceoverUrl ? 0 : MAX_SCRIPT_REWRITES;

		for (let attempt = 0; attempt <= maxRewriteAttempts; attempt++) {
			cleanedWavs = [];
			sumCleanDur = 0;

			if (voiceoverUrl) {
				// If you provide a full voiceoverUrl, we will NOT time-stretch; we just slice precisely.
				// (Best quality approach is to provide already-edited VO that matches the content script.)
				const voicePath = path.join(tmpDir, `voice_${jobId}.wav`);
				await downloadToFile(voiceoverUrl, voicePath, 45000, 2);

				// Convert to wav if needed
				const voiceWav = path.join(tmpDir, `voice_${jobId}_pcm.wav`);
				await spawnBin(
					ffmpegPath,
					[
						"-i",
						voicePath,
						"-vn",
						"-acodec",
						"pcm_s16le",
						"-ar",
						String(AUDIO_SR),
						"-ac",
						String(AUDIO_CHANNELS),
						"-y",
						voiceWav,
					],
					"voiceover_to_wav",
					{ timeoutMs: 180000 }
				);
				safeUnlink(voicePath);

				// naive equal split by expected segment durations
				const totalVoiceDur = await probeDurationSeconds(voiceWav);
				const per = totalVoiceDur / segments.length;
				for (let i = 0; i < segments.length; i++) {
					const start = i * per;
					const dur = i === segments.length - 1 ? totalVoiceDur - start : per;
					const out = path.join(tmpDir, `vo_clean_${jobId}_${i}.wav`);
					await spawnBin(
						ffmpegPath,
						[
							"-i",
							voiceWav,
							"-ss",
							start.toFixed(3),
							"-t",
							dur.toFixed(3),
							"-vn",
							"-acodec",
							"pcm_s16le",
							"-ar",
							String(AUDIO_SR),
							"-ac",
							String(AUDIO_CHANNELS),
							"-y",
							out,
						],
						"split_voiceover",
						{ timeoutMs: 120000 }
					);
					const d = await probeDurationSeconds(out);
					cleanedWavs.push({ index: i, wav: out, cleanDur: d });
					sumCleanDur += d;
				}
				safeUnlink(voiceWav);
			} else {
				logJob(jobId, "eleven voice locked", {
					voiceId: effectiveVoiceId,
					attempt,
					modelId: ttsModelId || "auto",
				});

				for (const seg of segments) {
					const mp3 = path.join(tmpDir, `tts_${jobId}_${seg.index}.mp3`);
					const cleanWav = path.join(
						tmpDir,
						`tts_clean_${jobId}_${seg.index}.wav`
					);

					const rawText = seg.text;
					const cleanText = sanitizeSegmentText(rawText);
					const textChanged = String(rawText || "").trim() !== cleanText;
					seg.text = cleanText;
					const voiceSettings = resolveVoiceSettings(seg.expression, cleanText);
					logJob(jobId, "tts segment start", {
						segment: seg.index,
						attempt,
						words: countWords(cleanText),
						text: cleanText,
						expression: seg.expression,
						voiceSettings,
						voiceId: effectiveVoiceId,
						modelId: ttsModelId || "auto",
						textChanged,
					});
					const usedModelId = await elevenLabsTTS({
						text: cleanText,
						outMp3Path: mp3,
						voiceId: effectiveVoiceId,
						voiceSettings,
						modelId: ttsModelId || undefined,
						modelOrder: ttsModelOrder,
					});
					if (!ttsModelId && usedModelId) ttsModelId = usedModelId;
					await mp3ToCleanWav(mp3, cleanWav);
					safeUnlink(mp3);

					const d = await probeDurationSeconds(cleanWav);
					logJob(jobId, "tts segment ready", {
						segment: seg.index,
						attempt,
						cleanDur: Number(d.toFixed(3)),
						modelId: usedModelId || ttsModelId || "auto",
					});
					cleanedWavs.push({ index: seg.index, wav: cleanWav, cleanDur: d });
					sumCleanDur += d;
				}
			}

			if (sumCleanDur < 3)
				throw new Error("Voice audio generation failed (empty duration)");

			if (cleanedWavs.length) {
				const durList = cleanedWavs
					.map((a) => Number(a.cleanDur || 0))
					.filter((d) => Number.isFinite(d) && d > 0);
				const minDur = durList.length ? Math.min(...durList) : 0;
				const maxDur = durList.length ? Math.max(...durList) : 0;
				const avgDur =
					durList.length > 0
						? durList.reduce((a, b) => a + b, 0) / durList.length
						: 0;
				logJob(jobId, "tts qa summary", {
					segments: cleanedWavs.length,
					minDur: Number(minDur.toFixed(3)),
					maxDur: Number(maxDur.toFixed(3)),
					avgDur: Number(avgDur.toFixed(3)),
					sumCleanDur: Number(sumCleanDur.toFixed(3)),
				});
			}

			const rawAtempo = sumCleanDur / narrationTargetSec;
			driftSec = Math.abs(sumCleanDur - narrationTargetSec);
			const toleranceSec = Math.min(
				SCRIPT_TOLERANCE_SEC,
				Math.max(1, narrationTargetSec * 0.07)
			);
			const ratioDelta = Math.abs(1 - rawAtempo);
			const overageSec = sumCleanDur - narrationTargetSec;
			const maxOverageSec = Math.max(
				0,
				Math.min(
					MAX_NARRATION_OVERAGE_SEC,
					narrationTargetSec * (MAX_NARRATION_OVERAGE_RATIO - 1)
				)
			);
			const allowOverage =
				ALLOW_NARRATION_OVERRUN &&
				overageSec > 0 &&
				overageSec <= maxOverageSec;
			const withinTolerance = driftSec <= toleranceSec || allowOverage;
			const closeEnough =
				allowOverage ||
				ratioDelta <= REWRITE_CLOSE_RATIO_DELTA ||
				driftSec <= toleranceSec * REWRITE_CLOSE_DRIFT_MULT;
			const shouldTimeStretch =
				!voiceoverUrl && (ratioDelta >= 0.04 || driftSec > toleranceSec);
			globalAtempo = shouldTimeStretch
				? clampNumber(rawAtempo, GLOBAL_ATEMPO_MIN, GLOBAL_ATEMPO_MAX)
				: 1;
			if (!voiceoverUrl && VOICE_SPEED_BOOST && VOICE_SPEED_BOOST !== 1) {
				globalAtempo = clampNumber(
					globalAtempo * VOICE_SPEED_BOOST,
					GLOBAL_ATEMPO_MIN,
					GLOBAL_ATEMPO_MAX
				);
			}
			logJob(jobId, "global atempo computed", {
				sumCleanDur: Number(sumCleanDur.toFixed(3)),
				narrationTargetSec: Number(narrationTargetSec.toFixed(3)),
				atempo: Number(globalAtempo.toFixed(4)),
				rawAtempo: Number(rawAtempo.toFixed(4)),
				driftSec: Number(driftSec.toFixed(3)),
				withinTolerance,
				toleranceSec: Number(toleranceSec.toFixed(3)),
				ratioDelta: Number(ratioDelta.toFixed(3)),
				closeEnough,
				shouldTimeStretch,
				allowOverage,
				overageSec: Number(overageSec.toFixed(3)),
				maxOverageSec: Number(maxOverageSec.toFixed(3)),
				attempt,
				voiceSpeedBoost: VOICE_SPEED_BOOST,
			});

			const needsRewrite =
				!voiceoverUrl &&
				!allowOverage &&
				!closeEnough &&
				(!withinTolerance ||
					rawAtempo < GLOBAL_ATEMPO_MIN ||
					rawAtempo > GLOBAL_ATEMPO_MAX);
			if (!needsRewrite || attempt >= maxRewriteAttempts) break;

			// cleanup current audio before rewrite
			for (const a of cleanedWavs) safeUnlink(a.wav);

			const ratio = narrationTargetSec / sumCleanDur;
			const dampedRatio = 1 + (ratio - 1) * REWRITE_RATIO_DAMPING;
			const adjustPct = clampNumber(
				Math.round(Math.abs(1 - dampedRatio) * 100 + 3),
				REWRITE_ADJUST_MIN,
				REWRITE_ADJUST_MAX
			);
			const direction = ratio > 1 ? "LONGER" : "SHORTER";
			const adjustedCaps = wordCaps.map((c) =>
				Math.max(12, Math.round(c * dampedRatio))
			);
			const capsLine2 = adjustedCaps
				.map((c, i) => `#${i}: <= ${c} words`)
				.join(", ");
			const expressionsLine = segments
				.map((s) => `#${s.index}: ${s.expression}`)
				.join(", ");
			const topicsLine = segments
				.map(
					(s) =>
						`#${s.index}: topic ${s.topicIndex} (${
							s.topicLabel || topicTitles?.[s.topicIndex] || ""
						})`
				)
				.join(", ");

			const rewritePrompt = `
Rewrite this script to better fit ~${narrationTargetSec.toFixed(
				1
			)}s of spoken narration.
Make the script about ${adjustPct}% ${direction} while keeping the same vibe.
Quality first: do not remove key details or clarity just to hit the target.
Per-segment word caps (updated): ${capsLine2}
Expressions by segment (keep these expressions, only adjust text): ${expressionsLine}
Topic assignment by segment (do NOT change order): ${topicsLine}

Rules:
- Keep the same topic and tone (US audience, fun, not formal).
- Keep it lightly casual: a few friendly, natural phrases like "real quick" or "here's the thing" (max 1 per topic).
- Keep EXACTLY ${segments.length} segments.
- Preserve smooth transitions.
- Make topic handoffs feel smooth and coherent; use a brief bridge phrase to set up the next topic.
- For Topic 2+ only, if a segment is the first for a new topic, start it with an explicit transition line naming the topic. Do NOT use that transition for Topic 1.
- Improve clarity and specificity; avoid vague filler phrasing or repeating the question.
- Avoid repeating the headline or the same fact across segments; each segment must add a new detail or angle.
- No redundancy: do not restate the same fact or idea in different words.
- Stay close to the per-segment word caps (aim ~90-100% of each cap); do not be significantly shorter.
- Preserve source attributions already in the text; keep at least one brief attribution per topic when possible.
- If you mention rumors or estimates, label them clearly as unconfirmed.
- Avoid filler words ("um", "uh", "umm", "uhm", "ah", "like"). Use zero filler words in the entire script, especially in segments 0-2.
- Do NOT add micro vocalizations ("heh", "whew", "hmm").
- Do NOT mention "intro", "outro", "segment", "next segment", or say "in this video/clip".
- Do NOT start segment 0 with transition phrases like "And now", "Now", "Next up", or "Let's talk about".
- End the LAST segment of EACH topic with one short, topic-specific engagement question for comments.
- Topic questions must be short and end with a single question mark.
- Do NOT ask for likes or subscribe in content; the closing line handles thanks and likes.
- Last segment ends with a clean wrap that leads into the closing line; do NOT mention the outro or transitions to it. Do NOT include a like/subscribe CTA.

Return JSON ONLY: { "segments":[{"index":0,"text":"..."}] }

Script:
${segments.map((s) => `#${s.index}: ${s.text}`).join("\n")}
`.trim();

			const resp2 = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: rewritePrompt }],
			});
			const parsed2 = parseJsonFlexible(
				resp2?.choices?.[0]?.message?.content || ""
			);
			if (!parsed2 || !Array.isArray(parsed2.segments))
				throw new Error("Rewrite parse failed");

			// apply rewrite
			const byIdx = new Map();
			for (const s of parsed2.segments) {
				const idx = Number(s.index);
				const txt = String(s.text || "").trim();
				if (Number.isFinite(idx) && txt) byIdx.set(idx, txt);
			}
			for (const seg of segments) {
				if (byIdx.has(seg.index)) seg.text = byIdx.get(seg.index);
			}
			// Re-apply segment completion rules after rewrite.
			const fixedSegments = enforceSegmentCompleteness(
				segments,
				tonePlan?.mood,
				{ includeCta: false }
			);
			const withTransitions = ensureTopicTransitions(fixedSegments, topicPicks);
			const withQuestions = ensureTopicEngagementQuestions(
				withTransitions,
				topicPicks,
				tonePlan?.mood,
				adjustedCaps
			);
			const fillerLimited = limitFillerAndEmotesAcrossSegments(withQuestions, {
				maxFillers: MAX_FILLER_WORDS_PER_VIDEO,
				maxFillersPerSegment: MAX_FILLER_WORDS_PER_SEGMENT,
				maxEmotes: MAX_MICRO_EMOTES_PER_VIDEO,
				maxEmotesPerSegment: MAX_MICRO_EMOTES_PER_VIDEO,
				noFillerSegmentIndices: [0, 1, 2],
			});
			segments.splice(0, segments.length, ...fillerLimited);
			segments = segments.map((s) => ({
				...s,
				text: sanitizeSegmentText(s.text),
			}));
		}

		if (
			ALIGN_INTRO_OUTRO_ATEMPO &&
			Number.isFinite(globalAtempo) &&
			Math.abs(globalAtempo - 1) >= 0.005
		) {
			const introAligned = path.join(tmpDir, `intro_aligned_${jobId}.wav`);
			await applyGlobalAtempoToWav(introAudioPath, introAligned, globalAtempo);
			safeUnlink(introAudioPath);
			introAudioPath = introAligned;
			introDurationSec = await probeDurationSeconds(introAudioPath);

			const outroAligned = path.join(tmpDir, `outro_aligned_${jobId}.wav`);
			await applyGlobalAtempoToWav(outroAudioPath, outroAligned, globalAtempo);
			safeUnlink(outroAudioPath);
			outroAudioPath = outroAligned;
			outroDurationSec = await probeDurationSeconds(outroAudioPath);

			logJob(jobId, "intro/outro atempo aligned", {
				atempo: Number(globalAtempo.toFixed(4)),
				introDurationSec: Number(introDurationSec.toFixed(3)),
				outroDurationSec: Number(outroDurationSec.toFixed(3)),
			});
			updateJob(jobId, {
				meta: {
					...JOBS.get(jobId)?.meta,
					intro: { text: introTextFinal, targetSec: introDurationSec },
					outro: { text: outroTextFinal, targetSec: outroDurationSec },
				},
			});
		}

		const finalScriptSegments = segments.map((s) => ({
			index: s.index,
			topicIndex: s.topicIndex,
			topicLabel: s.topicLabel,
			text: s.text,
			expression: s.expression,
			overlayCues: Array.isArray(s.overlayCues) ? s.overlayCues : [],
		}));
		script.segments = finalScriptSegments;
		const finalQa = analyzeScriptQuality({
			script,
			topics: topicPicks,
			topicContexts,
			wordCaps,
		});
		const finalEngagement = summarizeScriptEngagement(script);
		logJob(jobId, "final script summary", {
			qa: finalQa,
			engagement: finalEngagement,
			narrationTargetSec: Number(narrationTargetSec || 0),
			segmentCount: finalScriptSegments.length,
		});
		logJob(jobId, `final script text\n${buildScriptLogText(script)}`);
		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				script: { title: script.title, segments: finalScriptSegments },
			},
		});

		// Apply global atempo to each segment (may be 1.0 within tolerance)
		const segmentAudio = [];
		for (const a of cleanedWavs.sort((x, y) => x.index - y.index)) {
			const out = path.join(tmpDir, `seg_audio_${jobId}_${a.index}.wav`);
			await applyGlobalAtempoToWav(a.wav, out, globalAtempo);
			const d2 = await probeDurationSeconds(out);
			logJob(jobId, "tts segment atempo applied", {
				segment: a.index,
				atempo: Number(globalAtempo.toFixed(4)),
				cleanDur: Number((a.cleanDur || 0).toFixed(3)),
				finalDur: Number((d2 || 0).toFixed(3)),
			});
			segmentAudio.push({ index: a.index, wav: out, dur: d2 });
			safeUnlink(a.wav);
		}

		// Build timeline from actual audio durations
		let t = 0;
		let timeline = segmentAudio.map((a) => {
			const startSec = Number((introDurationSec + t).toFixed(3));
			t += a.dur;
			const endSec = Number((introDurationSec + t).toFixed(3));
			const seg = segments[a.index];
			return {
				index: a.index,
				text: seg.text,
				overlayCues: seg.overlayCues,
				topicIndex: seg.topicIndex,
				topicLabel: seg.topicLabel,
				startSec,
				endSec,
				audioPath: a.wav,
			};
		});

		// sanity: trim tiny rounding error
		if (timeline.length) {
			const finalEnd = timeline[timeline.length - 1].endSec;
			const desired = Number(
				(introDurationSec + narrationTargetSec).toFixed(3)
			);
			if (Math.abs(finalEnd - desired) > 0.08) {
				logJob(jobId, "timeline end differs from target (small drift)", {
					finalEnd,
					desired,
				});
			}
		}

		const narrationActualSec = segmentAudio.reduce(
			(sum, a) => sum + (Number(a.dur) || 0),
			0
		);
		const totalPlannedSec =
			Number(introDurationSec || 0) +
			Number(narrationTargetSec || 0) +
			Number(outroDurationSec || 0);
		const totalActualSec =
			Number(introDurationSec || 0) +
			Number(narrationActualSec || 0) +
			Number(outroDurationSec || 0);
		logJob(jobId, "final narration timing", {
			requestedTargetSec: Number(contentTargetSec || 0),
			plannedNarrationSec: Number(narrationTargetSec || 0),
			narrationActualSec: Number(narrationActualSec.toFixed(3)),
			introSec: Number((introDurationSec || 0).toFixed(3)),
			outroSec: Number((outroDurationSec || 0).toFixed(3)),
			totalPlannedSec: Number(totalPlannedSec.toFixed(3)),
			totalActualSec: Number(totalActualSec.toFixed(3)),
			outroSmileTailSec: Number(OUTRO_SMILE_TAIL_SEC || 0),
		});
		logJob(jobId, "final segment durations", {
			segments: timeline.map((seg) => ({
				index: seg.index,
				topicLabel: seg.topicLabel,
				startSec: seg.startSec,
				endSec: seg.endSec,
				durationSec: Number(
					Math.max(0, Number(seg.endSec) - Number(seg.startSec)).toFixed(3)
				),
			})),
		});

		updateJob(jobId, {
			progressPct: 40,
			meta: {
				...JOBS.get(jobId)?.meta,
				timeline,
			},
		});

		// 8.5) Visual plan: 50/50 presenter vs static image segments (content only)
		const totalSegments = timeline.length;
		let presenterCount = Math.floor(totalSegments * CONTENT_PRESENTER_RATIO);
		if (totalSegments >= 2) {
			presenterCount = clampNumber(presenterCount, 1, totalSegments - 1);
		} else {
			presenterCount = totalSegments;
		}
		const presenterPositions = pickEvenlySpacedIndices(
			totalSegments,
			presenterCount
		);
		const presenterPosSet = new Set(presenterPositions);
		const presenterSegments = [];
		const imageSegments = [];
		timeline = timeline.map((seg, idx) => {
			const visualType = presenterPosSet.has(idx) ? "presenter" : "image";
			if (visualType === "presenter") presenterSegments.push(seg.index);
			else imageSegments.push(seg.index);
			return { ...seg, visualType };
		});
		logJob(jobId, "segment visual plan", {
			totalSegments,
			presenterSegments,
			imageSegments,
		});

		const imagePrep = await prepareImageSegments({
			timeline,
			topics: topicPicks,
			tmpDir,
			jobId,
			baseUrl,
			output,
		});
		timeline = imagePrep.timeline;
		segmentImagePaths = imagePrep.segmentImagePaths || new Map();
		const imagePlanSummary = imagePrep.imagePlanSummary || [];

		const finalPresenterSegments = [];
		const finalImageSegments = [];
		for (const seg of timeline) {
			if (seg.visualType === "image") finalImageSegments.push(seg.index);
			else finalPresenterSegments.push(seg.index);
		}
		const allImageUrls = [];
		for (const seg of timeline) {
			if (Array.isArray(seg.imageUrls)) allImageUrls.push(...seg.imageUrls);
		}
		const uniqueImageUrls = new Set(
			allImageUrls.map((u) => normalizeImageUrlKey(u))
		);
		const duplicateImageCount = Math.max(
			0,
			allImageUrls.length - uniqueImageUrls.size
		);
		logJob(jobId, "segment image qa", {
			totalImages: allImageUrls.length,
			uniqueImages: uniqueImageUrls.size,
			duplicates: duplicateImageCount,
		});
		if (imagePlanSummary.length) {
			logJob(jobId, "segment image plan", {
				count: imagePlanSummary.length,
				segments: imagePlanSummary,
			});
		}
		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				timeline,
				visualPlan: {
					presenterSegments: finalPresenterSegments,
					imageSegments: finalImageSegments,
				},
				imageQa: {
					total: allImageUrls.length,
					unique: uniqueImageUrls.size,
					duplicates: duplicateImageCount,
				},
			},
		});

		const presenterSegSet = new Set(finalPresenterSegments);
		const presenterOnlySegments = segments.filter((s) =>
			presenterSegSet.has(s.index)
		);
		const presenterVideoPlan = buildSubtleVideoExpressionPlan(
			presenterOnlySegments,
			tonePlan?.mood,
			jobId
		);
		const presenterPlanByIndex = new Map();
		presenterOnlySegments.forEach((seg, idx) => {
			presenterPlanByIndex.set(seg.index, presenterVideoPlan[idx] || "neutral");
		});
		segments = segments.map((s) => ({
			...s,
			videoExpression: presenterPlanByIndex.get(s.index) || "neutral",
		}));

		// Build topic-aligned overlays from segment cues (if no custom overlays provided)
		if (
			ENABLE_LONG_VIDEO_OVERLAYS &&
			(!overlayAssets || !overlayAssets.length)
		) {
			autoOverlayAssets = await buildOverlayAssetsFromSegments({
				segments,
				timeline,
				topics: topicPicks,
				maxOverlays: MAX_AUTO_OVERLAYS,
			});
			logJob(jobId, "auto overlays prepared", {
				count: autoOverlayAssets.length,
			});
		}

		// 9) Create baseline presenter videos (expression-aware)
		const baselinePresenterVideos = new Map();
		const pushBaselineVariant = (expr, clipPath) => {
			if (!clipPath) return;
			const list = baselinePresenterVideos.get(expr) || [];
			list.push(clipPath);
			baselinePresenterVideos.set(expr, list);
		};
		const pickBaselineVariant = (expr, seed = 0) => {
			const list =
				baselinePresenterVideos.get(expr) ||
				baselinePresenterVideos.get("neutral") ||
				[];
			if (!list.length) return null;
			const idx = Math.abs(Number(seed) || 0) % list.length;
			return list[idx];
		};
		const pickBaselineDefault = () => {
			const neutralList = baselinePresenterVideos.get("neutral") || [];
			if (neutralList.length) return neutralList[0];
			const first = baselinePresenterVideos.values().next().value;
			return Array.isArray(first) ? first[0] : first || null;
		};
		const expressionsNeeded = Array.from(
			new Set(
				[
					...segments.map((s) => s.videoExpression || s.expression),
					introExpression,
					outroExpression,
				].filter(Boolean)
			)
		);
		if (!expressionsNeeded.includes("neutral"))
			expressionsNeeded.unshift("neutral");

		if (presenterIsVideo) {
			pushBaselineVariant("neutral", presenterLocal);
			logJob(jobId, "presenter is video; baseline uses provided video");
		} else if (
			enableRunwayPresenterMotion &&
			ENABLE_RUNWAY_BASELINE &&
			RUNWAY_API_KEY
		) {
			for (const expr of expressionsNeeded) {
				for (let v = 0; v < BASELINE_VARIANTS; v++) {
					try {
						const runwayUri = await runwayCreateEphemeralUpload({
							filePath: presenterLocal,
							filename: `presenter_${expr}.png`,
						});
						const prompt = buildBaselinePrompt(expr, motionRefVideo, v);

						logJob(jobId, "runway baseline prompt", {
							expression: expr,
							variant: v + 1,
							duration: BASELINE_DUR_SEC,
							ratio: output.ratio,
						});

						const outUrl = await runwayImageToVideo({
							runwayImageUri: runwayUri,
							promptText: prompt,
							durationSec: BASELINE_DUR_SEC,
							ratio: output.ratio,
						});

						const outMp4 = path.join(
							tmpDir,
							`baseline_${jobId}_${expr}_v${v + 1}.mp4`
						);
						await downloadToFile(outUrl, outMp4, 120000, 2);

						// Prep baseline for sync input
						const prep = path.join(
							tmpDir,
							`baseline_sync_${jobId}_${expr}_v${v + 1}.mp4`
						);
						await spawnBin(
							ffmpegPath,
							[
								"-i",
								outMp4,
								"-t",
								BASELINE_DUR_SEC.toFixed(3),
								"-an",
								"-vf",
								`scale=${makeEven(output.w)}:${makeEven(
									output.h
								)}:force_original_aspect_ratio=increase:flags=lanczos,crop=${makeEven(
									output.w
								)}:${makeEven(
									output.h
								)},fps=${SYNC_SO_INPUT_FPS},format=yuv420p,setpts=PTS-STARTPTS`,
								"-c:v",
								"libx264",
								"-preset",
								"veryfast",
								"-crf",
								String(SYNC_SO_INPUT_CRF),
								"-pix_fmt",
								"yuv420p",
								"-movflags",
								"+faststart",
								"-y",
								prep,
							],
							"prepare_baseline",
							{ timeoutMs: 240000 }
						);

						const syncReady = await ensureUnderBytes(
							prep,
							SYNC_SO_MAX_BYTES,
							tmpDir,
							jobId,
							`baseline_sync_${expr}_v${v + 1}`
						);
						pushBaselineVariant(expr, syncReady);
						logJob(jobId, "baseline presenter ready", {
							expression: expr,
							variant: v + 1,
							path: path.basename(syncReady),
						});
					} catch (e) {
						logJob(jobId, "runway baseline failed (expression)", {
							expression: expr,
							variant: v + 1,
							error: e.message,
						});
					}
				}
			}
		}
		if (!baselinePresenterVideos.size) {
			// Fallback: convert image to a simple still video
			const still = path.join(tmpDir, `baseline_still_${jobId}.mp4`);
			await spawnBin(
				ffmpegPath,
				[
					"-loop",
					"1",
					"-i",
					presenterLocal,
					"-t",
					BASELINE_DUR_SEC.toFixed(3),
					"-an",
					"-vf",
					`scale=${makeEven(output.w)}:${makeEven(
						output.h
					)}:force_original_aspect_ratio=increase:flags=lanczos,crop=${makeEven(
						output.w
					)}:${makeEven(output.h)},fps=${SYNC_SO_INPUT_FPS},format=yuv420p`,
					"-c:v",
					"libx264",
					"-preset",
					"veryfast",
					"-crf",
					String(SYNC_SO_INPUT_CRF),
					"-pix_fmt",
					"yuv420p",
					"-movflags",
					"+faststart",
					"-y",
					still,
				],
				"baseline_still",
				{ timeoutMs: 180000 }
			);
			pushBaselineVariant("neutral", still);
		}

		const baselineDefault = pickBaselineDefault();

		updateJob(jobId, { progressPct: 50 });

		// 10) Intro (lipsync + title overlay)
		const introOffsetSeed = seedFromJobId(jobId) % 29;
		const introBaseline =
			pickBaselineVariant(introExpression, 0) || baselineDefault;
		const introBase = await renderLipsyncedSegment({
			jobId,
			tmpDir,
			output,
			baselineSource: introBaseline,
			segDur: introDurationSec,
			audioPath: introAudioPath,
			label: "intro",
			offsetSeed: introOffsetSeed,
			addFades: true,
		});
		const introPath = path.join(tmpDir, `intro_${jobId}.mp4`);
		const introTitle = INTRO_OVERLAY_TEXT;
		const introSubtitle = "";
		await createIntroClip({
			title: introTitle,
			subtitle: introSubtitle,
			bgImagePath: introBase,
			durationSec: introDurationSec,
			outCfg: output,
			outPath: introPath,
			disableVideoBlur: true,
		});
		safeUnlink(introBase);

		// 11) Content segment pipeline
		const segmentVideos = [introPath];
		for (const seg of timeline) {
			const segDur = Math.max(0.2, seg.endSec - seg.startSec);
			logJob(jobId, "segment start", {
				segment: seg.index,
				segDur: Number(segDur.toFixed(3)),
				visualType: seg.visualType || "presenter",
			});

			const exprKey = seg.videoExpression || seg.expression || "neutral";
			const baselineSource =
				pickBaselineVariant(exprKey, seg.index) || baselineDefault;
			let norm = null;
			if (seg.visualType === "image") {
				const imagePaths = segmentImagePaths.get(seg.index) || [];
				if (imagePaths.length) {
					try {
						norm = await renderImageSegment({
							jobId,
							tmpDir,
							output,
							segDur,
							audioPath: seg.audioPath,
							imagePaths,
							label: String(seg.index),
							addFades: ENABLE_SEGMENT_FADES,
						});
					} catch (e) {
						logJob(jobId, "image segment failed; fallback to presenter", {
							segment: seg.index,
							error: e.message,
						});
					}
				} else {
					logJob(jobId, "image segment missing assets; fallback to presenter", {
						segment: seg.index,
					});
				}
			}

			if (!norm) {
				norm = await renderLipsyncedSegment({
					jobId,
					tmpDir,
					output,
					baselineSource,
					segDur,
					audioPath: seg.audioPath,
					label: String(seg.index),
					offsetSeed: seg.index,
					addFades: ENABLE_SEGMENT_FADES,
				});
			}

			segmentVideos.push(norm);
			logJob(jobId, "segment ready", {
				segment: seg.index,
				visualType: seg.visualType || "presenter",
			});
		}

		// 12) Outro (lipsync + silent smile tail)
		const outroOffsetSeed = introOffsetSeed + 7;
		const outroBaseline =
			pickBaselineVariant(outroExpression, 1) || baselineDefault;
		const outroTalk = await renderLipsyncedSegment({
			jobId,
			tmpDir,
			output,
			baselineSource: outroBaseline,
			segDur: outroDurationSec,
			audioPath: outroAudioPath,
			label: "outro",
			offsetSeed: outroOffsetSeed,
			addFades: false,
		});

		// Calm tail after the outro line (silent + fade-out).
		const tailBaseline =
			pickBaselineVariant("neutral", 2) || outroBaseline || baselineDefault;
		const tailBaselineDur = await probeDurationSeconds(tailBaseline);
		const tailStart = Math.max(0, tailBaselineDur - OUTRO_SMILE_TAIL_SEC);
		const tailRaw = path.join(tmpDir, `outro_tail_raw_${jobId}.mp4`);
		await spawnBin(
			ffmpegPath,
			[
				"-ss",
				tailStart.toFixed(3),
				"-i",
				tailBaseline,
				"-t",
				OUTRO_SMILE_TAIL_SEC.toFixed(3),
				"-an",
				"-vf",
				"setpts=PTS-STARTPTS",
				"-c:v",
				"libx264",
				"-preset",
				INTERMEDIATE_PRESET,
				"-crf",
				String(INTERMEDIATE_VIDEO_CRF),
				"-pix_fmt",
				"yuv420p",
				"-movflags",
				"+faststart",
				"-y",
				tailRaw,
			],
			"outro_tail_cut",
			{ timeoutMs: 120000 }
		);
		const tailFit = path.join(tmpDir, `outro_tail_fit_${jobId}.mp4`);
		await fitVideoToDuration(tailRaw, OUTRO_SMILE_TAIL_SEC, tailFit);
		safeUnlink(tailRaw);

		const tailSilence = path.join(tmpDir, `outro_tail_silence_${jobId}.wav`);
		await createSilentWav({
			durationSec: OUTRO_SMILE_TAIL_SEC,
			outPath: tailSilence,
		});
		const tailWithAudio = path.join(tmpDir, `outro_tail_audio_${jobId}.mp4`);
		await mergeVideoWithAudio(tailFit, tailSilence, tailWithAudio);
		safeUnlink(tailSilence);
		safeUnlink(tailFit);

		const outroTail = path.join(tmpDir, `outro_tail_${jobId}.mp4`);
		await normalizeClip(tailWithAudio, outroTail, output, {
			zoomOut: CAMERA_ZOOM_OUT,
			fadeOutOnly: true,
		});
		safeUnlink(tailWithAudio);

		const outroPath = path.join(tmpDir, `outro_${jobId}.mp4`);
		await concatClips([outroTalk, outroTail], outroPath, output);
		segmentVideos.push(outroPath);

		updateJob(jobId, { progressPct: 72 });

		// 13) Concat intro + content + outro
		const concatPath = path.join(tmpDir, `concat_${jobId}.mp4`);
		await concatClips(segmentVideos, concatPath, output);
		logJob(jobId, "concat done", { clips: segmentVideos.length });

		// 14) Overlays (optional; static image segments provide visuals)
		let overlayedPath = concatPath;
		if (
			ENABLE_LONG_VIDEO_OVERLAYS &&
			((overlayAssets && overlayAssets.length) ||
				(autoOverlayAssets && autoOverlayAssets.length))
		) {
			const totalDurationSec = await probeDurationSeconds(concatPath);
			const overlaySource =
				overlayAssets && overlayAssets.length
					? overlayAssets
					: autoOverlayAssets;
			const normalizedOverlays = normalizeOverlayAssets(
				overlaySource,
				totalDurationSec
			);
			const overlaysToUse = normalizedOverlays;

			const overlayLocal = [];
			for (let i = 0; i < overlaysToUse.length; i++) {
				const ov = overlaysToUse[i];
				if (!ov?.url) continue;
				// Allow download when HEAD fails; only skip on explicit non-media types.
				const ct = await headContentType(ov.url, 7000);
				if (ct && !ct.startsWith("image/") && !ct.startsWith("video/"))
					continue;

				const ext = ov.type === "video" ? "mp4" : "png";
				const out = path.join(tmpDir, `ov_${jobId}_${i}.${ext}`);
				try {
					await downloadToFile(ov.url, out, 25000, 1);
					const dt = detectFileType(out);
					if (ov.type === "image" && dt?.kind !== "image") {
						safeUnlink(out);
						continue;
					}
					if (ov.type === "video" && dt?.kind !== "video") {
						safeUnlink(out);
						continue;
					}
					overlayLocal.push({ ...ov, localPath: out });
				} catch {
					safeUnlink(out);
				}
			}

			overlayedPath = path.join(tmpDir, `overlay_${jobId}.mp4`);
			try {
				overlayedPath = await applyOverlays(
					concatPath,
					overlayLocal,
					overlayedPath
				);
				logJob(jobId, "overlays applied", { count: overlayLocal.length });
			} catch (e) {
				logJob(jobId, "overlay failed (continuing without)", {
					error: e.message,
				});
				fs.copyFileSync(concatPath, overlayedPath);
			}
		} else {
			logJob(jobId, "overlays skipped", { reason: "segment visuals enabled" });
		}

		updateJob(jobId, { progressPct: 84 });

		// 15) Music mix (must)
		let mixedPath = overlayedPath;
		if (musicLocalPath) {
			const out = path.join(tmpDir, `mixed_${jobId}.mp4`);
			mixedPath = await mixBackgroundMusic(overlayedPath, musicLocalPath, out, {
				jobId,
			});
		}

		updateJob(jobId, { progressPct: 92 });

		// 16) Finalize (with fade-out)
		const outputName = `long_${jobId}.mp4`;
		const outputPath = LONG_VIDEO_PERSIST_OUTPUT
			? path.join(OUTPUT_DIR, outputName)
			: path.join(tmpDir, outputName);

		await finalizeVideoWithFadeOut({
			inputPath: mixedPath,
			outputPath,
			fadeOutSec: FINAL_FADE_OUT_SEC,
			outCfg: output,
		});

		// 16.5) YouTube upload (optional)
		let youtubeLink = "";
		let youtubeTokens = null;
		try {
			if (!hasYouTubeTokens) {
				logJob(jobId, "youtube upload skipped (no tokens)");
			} else {
				const youtubePayload = {
					youtubeAccessToken,
					youtubeRefreshToken,
					youtubeTokenExpiresAt,
				};
				youtubeTokens = await refreshYouTubeTokensIfNeeded(
					user,
					youtubePayload
				);
				if (youtubeTokens?.refresh_token) {
					youtubeLink = await uploadToYouTube(youtubeTokens, outputPath, {
						title: seoMeta?.seoTitle || script.title,
						description: seoMeta?.seoDescription || script.title,
						tags: seoMeta?.tags || [BRAND_TAG],
						category: youtubeCategoryFinal || LONG_VIDEO_YT_CATEGORY,
						thumbnailPath,
						jobId,
					});
					logJob(jobId, "youtube upload complete", { youtubeLink });
				} else {
					logJob(jobId, "youtube upload skipped (missing refresh token)");
				}
			}
		} catch (e) {
			logJob(jobId, "youtube upload skipped", { error: e.message });
		}

		const finalVideoUrl = LONG_VIDEO_PERSIST_OUTPUT
			? `${baseUrl}/uploads/videos/${outputName}`
			: youtubeLink || "";
		const outputUrl = LONG_VIDEO_PERSIST_OUTPUT
			? finalVideoUrl
			: youtubeLink || "";
		const localFilePath = LONG_VIDEO_PERSIST_OUTPUT ? outputPath : "";
		let videoDocId = null;
		try {
			if (user?._id) {
				const scriptText = [
					introTextFinal,
					...script.segments.map((s) => s.text),
					outroTextFinal,
				]
					.filter(Boolean)
					.join("\n");
				const durationValue = Math.round(contentTargetSec || 0);
				const allowedDurations = new Set([
					5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90,
					120, 180, 240, 300, 360, 420,
				]);
				const durationForDoc = allowedDurations.has(durationValue)
					? durationValue
					: undefined;
				const doc = await Video.create({
					user: user._id,
					category: categoryLabel,
					topic: topicSummary,
					topics: topicTitles,
					isLongVideo: true,
					seoTitle: seoMeta?.seoTitle || script.title,
					seoDescription: seoMeta?.seoDescription || script.title,
					tags: seoMeta?.tags || [BRAND_TAG],
					script: scriptText,
					ratio: output?.ratio,
					duration: durationForDoc,
					status: "SUCCEEDED",
					outputUrl: outputUrl || "",
					localFilePath: localFilePath || "",
					youtubeLink,
					language: languageLabel,
					country: LONG_VIDEO_TRENDS_GEO,
					youtubeEmail: user?.youtubeEmail || "",
					youtubeAccessToken:
						youtubeTokens?.access_token || youtubeAccessToken || "",
					youtubeRefreshToken:
						youtubeTokens?.refresh_token || youtubeRefreshToken || "",
					youtubeTokenExpiresAt: youtubeTokens?.expiry_date
						? new Date(youtubeTokens.expiry_date)
						: youtubeTokenExpiresAt
						? new Date(youtubeTokenExpiresAt)
						: undefined,
					presenterOutfit: presenterOutfit || "",
				});
				videoDocId = doc?._id ? String(doc._id) : null;
			}
		} catch (e) {
			logJob(jobId, "video doc save failed", { error: e.message });
		}
		updateJob(jobId, {
			status: "completed",
			progressPct: 100,
			finalVideoUrl: finalVideoUrl || null,
			meta: {
				...JOBS.get(jobId)?.meta,
				youtubeLink,
				videoId: videoDocId,
			},
		});
		logJob(jobId, "job completed", { finalVideoUrl, youtubeLink });
		if (!LONG_VIDEO_PERSIST_OUTPUT) safeUnlink(outputPath);
	} catch (err) {
		logJob(jobId, "job failed", {
			error: err?.message || "Long video job failed",
			stack: err?.stack || "",
		});
		updateJob(jobId, {
			status: "failed",
			error: err?.message || "Long video job failed",
		});
	} finally {
		if (!LONG_VIDEO_KEEP_TMP) safeRmRecursive(tmpDir);
		else logJob(jobId, "tmp kept", { tmpDir });
	}
}

/* ---------------------------------------------------------------
 * CONTROLLER: createLongVideo
 * ------------------------------------------------------------- */

exports.createLongVideo = async (req, res) => {
	const { errors, clean } = validateCreateBody(req.body || {});
	if (errors.length) return res.status(400).json({ error: errors.join(", ") });
	if (!isOwnerOnlyUser(req)) {
		return res.status(403).json({
			error: "Long video creation is temporarily restricted to the owner.",
		});
	}

	const jobId = crypto.randomUUID();
	const baseUrl = buildBaseUrl(req);

	const job = {
		jobId,
		status: "queued",
		progressPct: 0,
		topic: null,
		finalVideoUrl: null,
		error: null,
		createdAt: nowIso(),
		updatedAt: nowIso(),
		meta: {},
	};
	JOBS.set(jobId, job);

	res.status(202).json({
		jobId,
		status: "queued",
		statusUrl: `/api/long-video/${jobId}`,
	});

	logJob(jobId, "job queued", {
		statusUrl: `/api/long-video/${jobId}`,
		baseUrl,
	});

	// Optional scheduling (unchanged)
	const schedule = req.body?.schedule || null;
	const scheduleJobMeta =
		req.scheduleJobMeta || req.body?.scheduleJobMeta || null;
	const isScheduledJob = Boolean(scheduleJobMeta);

	if (schedule && !isScheduledJob && req.user?._id) {
		const { type, timeOfDay, startDate, endDate } = schedule;
		if (!["daily", "weekly", "monthly"].includes(String(type || ""))) {
			console.warn(
				"[LongVideo] Invalid schedule type; skipping schedule save."
			);
		} else if (!parseTimeOfDay(timeOfDay) || !startDate) {
			console.warn(
				"[LongVideo] Invalid schedule timing; skipping schedule save."
			);
		} else {
			const nextRun = computeNextRun({
				scheduleType: type,
				timeOfDay,
				startDate,
			});
			if (nextRun) {
				try {
					await Schedule.create({
						user: req.user._id,
						category: "LongVideo",
						scheduleType: type,
						timeOfDay,
						startDate: dayjs(startDate).toDate(),
						endDate: endDate ? dayjs(endDate).toDate() : undefined,
						nextRun,
						active: true,
						videoType: "long",
						longVideoConfig: { ...clean },
					});
				} catch (e) {
					console.warn("[LongVideo] Schedule creation failed", e.message);
				}
			}
		}
	}

	setImmediate(() => runLongVideoJob(jobId, clean, baseUrl, req.user || null));
};

/* ---------------------------------------------------------------
 * CONTROLLER: getLongVideoStatus
 * ------------------------------------------------------------- */

exports.getLongVideoStatus = async (req, res) => {
	const { jobId } = req.params;
	const job = JOBS.get(jobId);
	if (!job) return res.status(404).json({ error: "Job not found" });

	return res.json({
		jobId: job.jobId,
		status: job.status,
		progressPct: job.progressPct,
		topic: job.topic || null,
		finalVideoUrl: job.finalVideoUrl || null,
		error: job.error || null,
		meta: job.meta || {},
	});
};
