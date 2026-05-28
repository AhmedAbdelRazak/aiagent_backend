/** @format */
/**
 * videoControllerLonger.js (DROP-IN REPLACEMENT - QUALITY + STABILITY)
 *
 * Key improvements (mapped to Amad's requirements):
 * 1) No voice stutter / no silent gaps:
 *    - Generate TTS per segment -> convert to WAV -> remove leading/trailing silence
 *    - Apply ONE global atempo factor for consistent brisk narration (no padding)
 *    - Avoid aresample async drift correction (removes "stutter" artifacts)
 *
 * 2) Presenter looks natural:
 *    - Generate HeyGen photo-presenter clips from the approved presenter image
 *    - Merge presenter runs to keep paid renders cheaper and more coherent
 *    - Keep prompts restrained: natural lips, subtle reactions, stable identity
 *
 * 3) Presenter wardrobe selection (classy outfit):
 *    - Use the approved presenter outfit library after script planning
 *    - Keeps identity/studio consistent and avoids image-generation cost
 *
 * 4) Camera is slightly farther away:
 *    - Normalize presenter clips into the final frame with subtle motion polish
 *
 * 5) Professional intro/outro structure:
 *    - Intro opens with one brief natural greeting + topic, then the hook
 *    - Outro CTA beat with topic question + silent light-smile tail
 *    - Final fade-out for a clean finish
 *
 * 6-8) Script is "spicy" (American audience) + smooth transitions:
 *    - Strong prompt guidance: conversational, punchy, not formal
 *    - Explicit transition language between segments
 *
 * 9) No empty/silent parts:
 *    - Silence removed; no apad; pacing handled via global atempo
 *
 * 10) Code cleaned:
 *    - Removed unused/fragile paths and keeps paid presenter video on HeyGen
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
const xml2js = require("xml2js");
const dayjs = require("dayjs");
const { google } = require("googleapis");
const { OpenAI } = require("openai");
const cloudinary = require("cloudinary").v2;
const { generateThumbnailPackage } = require("../assets/thumbnailDesigner");
// Placeholder for the prior OpenAI wardrobe editor, kept for easy rollback:
// const { generatePresenterAdjustedImage } = require("../assets/presenterAdjustments");
const {
	generatePresenterAdjustedImage,
} = require("../assets/presenterAdjustments2");
const {
	assertOpenAIImageReady,
	editImageToPath,
} = require("../assets/openaiImageTools");
const Video = require("../models/Video");
const Schedule = require("../models/Schedule");
const {
	resolveFfprobePath: resolveSharedFfprobePath,
} = require("../utils/mediaBinaries");
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

const CHAT_MODEL = "gpt-5.4";
const OWNER_ONLY_USER_ID = "683e3a0329b0515ff5f7a1e1";
const LOG_STARTUP_DETAILS = ["1", "true", "yes", "on"].includes(
	String(process.env.LOG_STARTUP_DETAILS || "")
		.trim()
		.toLowerCase(),
);

function logStartupDetail(...args) {
	if (LOG_STARTUP_DETAILS) console.log(...args);
}

function isOwnerOnlyUser(req) {
	const userId = req?.user?._id || req?.user?.id || req?.userId;
	return String(userId || "") === OWNER_ONLY_USER_ID;
}

const ELEVEN_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ELEVEN_FIXED_VOICE_ID = "uKepyVD0sANZxUFnIoI2";
const ELEVEN_TTS_MODEL = "eleven_multilingual_v2";
const ELEVEN_TTS_MODEL_FALLBACKS = String(
	"eleven_flash_v2_5,eleven_turbo_v2_5,eleven_monolingual_v1",
)
	.split(",")
	.map((s) => s.trim())
	.filter(Boolean);
// TTS realism tuning: less locked, more human pacing and phrasing.
const ELEVEN_TTS_STABILITY = clampNumber(0.62, 0.1, 1);
const ELEVEN_TTS_SIMILARITY = clampNumber(0.88, 0.1, 1);
const ELEVEN_TTS_STYLE = clampNumber(0.2, 0, 1);
const ELEVEN_TTS_SPEED = clampNumber(
	process.env.LONG_VIDEO_ELEVEN_TTS_SPEED ?? 1.0,
	0.7,
	1.2,
);
const ELEVEN_TTS_SPEAKER_BOOST = true;
const UNIFORM_TTS_VOICE_SETTINGS = false;

const HEYGEN_API_KEY = process.env.HEYGEN_API_KEY || "";
const HEYGEN_API_BASE = "https://api.heygen.com";
const HEYGEN_CREATE_VIDEO_PATH = "/v3/videos";
const HEYGEN_DEFAULT_RESOLUTION =
	String(process.env.LONG_VIDEO_HEYGEN_RESOLUTION || "1080p").trim() || "1080p";
const HEYGEN_DEFAULT_ASPECT_RATIO =
	String(process.env.LONG_VIDEO_HEYGEN_ASPECT_RATIO || "16:9").trim() || "16:9";
const HEYGEN_DEFAULT_EXPRESSIVENESS =
	String(process.env.LONG_VIDEO_HEYGEN_EXPRESSIVENESS || "medium").trim() ||
	"medium";
const HEYGEN_DEFAULT_FIT =
	String(process.env.LONG_VIDEO_HEYGEN_FIT || "cover").trim() || "cover";
const HEYGEN_RETRY_EXPRESSIVENESS =
	String(process.env.LONG_VIDEO_HEYGEN_RETRY_EXPRESSIVENESS || "high").trim() ||
	"high";
const HEYGEN_POLL_INTERVAL_MS = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_POLL_INTERVAL_MS ?? 10000,
	3000,
	30000,
);
const HEYGEN_POLL_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_POLL_TIMEOUT_MS ?? 15 * 60 * 1000,
	2 * 60 * 1000,
	40 * 60 * 1000,
);

const GOOGLE_CSE_ID = process.env.GOOGLE_CSE_ID || null;

const GOOGLE_CSE_KEY = process.env.GOOGLE_CSE_KEY || null;
function looksLikeGoogleOAuthClientId(value = "") {
	return /\.apps\.googleusercontent\.com$/i.test(String(value || "").trim());
}
const GOOGLE_CSE_CONFIG_READY = Boolean(
	GOOGLE_CSE_ID &&
		GOOGLE_CSE_KEY &&
		!looksLikeGoogleOAuthClientId(GOOGLE_CSE_ID),
);
const GOOGLE_CSE_CONFIG_ISSUE =
	GOOGLE_CSE_ID && GOOGLE_CSE_KEY && !GOOGLE_CSE_CONFIG_READY
		? "GOOGLE_CSE_ID looks like an OAuth client id, not a Custom Search cx"
		: "";

const CLOUDINARY_ENABLED = Boolean(
	process.env.CLOUDINARY_CLOUD_NAME &&
	process.env.CLOUDINARY_API_KEY &&
	process.env.CLOUDINARY_API_SECRET,
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
const TRENDS_HTTP_TIMEOUT_MS = clampNumber(
	process.env.TRENDS_HTTP_TIMEOUT_MS ?? 240000,
	60000,
	600000,
);
const TRENDS_HTTP_MAX_ATTEMPTS = Math.floor(
	clampNumber(process.env.TRENDS_HTTP_MAX_ATTEMPTS ?? 1, 1, 2),
);
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
	if (baseUrl) {
		const trimmed = canonicalizeLoopbackBase(baseUrl);
		if (isPrivateHttpBase(trimmed)) add(`${trimmed}/api/google-trends`);
	}
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

function canonicalizeLoopbackBase(raw) {
	const cleaned = String(raw || "")
		.trim()
		.replace(/\/+$/, "");
	if (!cleaned) return "";
	return cleaned
		.replace(/:\/\/localhost(?=[:/]|$)/i, "://127.0.0.1")
		.replace(/:\/\/\[::1\](?=[:/]|$)/i, "://127.0.0.1");
}

function buildGoogleImagesApiCandidates(baseUrl) {
	const list = [];
	const bases = [
		deriveTrendsServiceBase(TRENDS_API_URL),
		String(baseUrl || "").trim(),
	].filter(Boolean);
	for (const base of bases) {
		const trimmed = canonicalizeLoopbackBase(base);
		if (trimmed && isPrivateHttpBase(trimmed))
			list.push(`${trimmed}/api/google-images`);
	}
	return Array.from(new Set(list));
}

function isPrivateHttpBase(raw) {
	try {
		const u = new URL(String(raw || ""));
		const host = String(u.hostname || "").toLowerCase();
		return (
			host === "localhost" ||
			host === "127.0.0.1" ||
			host === "::1" ||
			host.startsWith("10.") ||
			host.startsWith("192.168.") ||
			/^172\.(1[6-9]|2\d|3[01])\./.test(host)
		);
	} catch {
		return false;
	}
}

const LONG_VIDEO_YT_CATEGORY = "Entertainment";

const BRAND_TAG = "SereneJannat";
const BRAND_CREDIT = "Powered by Serene Jannat";
const CHANNEL_NAME = "Prime Time Brief";
const MERCH_INTRO =
	"Support the channel & customize your own merch:\n" +
	"https://serenejannat.com/our-products?category=candles/\n" +
	"https://www.serenejannat.com/custom-gifts\n" +
	"https://www.serenejannat.com/custom-gifts/67b7fb9c3d0cd90c4fc410e3\n\n";

const JAMENDO_CLIENT_ID = process.env.JAMENDO_CLIENT_ID || "";
const JAMENDO_BASE = "https://api.jamendo.com/v3.0";

const LONG_VIDEO_TMP_ROOT = String(
	process.env.LONG_VIDEO_TMP_ROOT || "",
).trim();
const TMP_ROOT = LONG_VIDEO_TMP_ROOT
	? path.resolve(LONG_VIDEO_TMP_ROOT)
	: path.join(__dirname, "../uploads/tmp/agentai_long_video");
const OUTPUT_DIR = path.join(__dirname, "../uploads/videos");
const THUMBNAIL_DIR = path.join(__dirname, "../uploads/thumbnails");
const LONG_VIDEO_PERSIST_OUTPUT =
	String(process.env.LONG_VIDEO_PERSIST_OUTPUT || "").toLowerCase() === "true";
const LONG_VIDEO_PERSIST_FOR_SHORTS =
	String(process.env.LONG_VIDEO_PERSIST_FOR_SHORTS || "true").toLowerCase() ===
	"true";
const SHOULD_PERSIST_LONG_VIDEO =
	LONG_VIDEO_PERSIST_OUTPUT || LONG_VIDEO_PERSIST_FOR_SHORTS;

// Your classy suit reference (also default presenter)
const DEFAULT_PRESENTER_ASSET_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767062842/aivideomatic/long_thumbnails/MyPhotoWithASuit_s1xay4.png";
const DEFAULT_PRESENTER_MOTION_VIDEO_PATHS = [
	path.join(__dirname, "../uploads/presenter_cache/motion_reference.mp4"),
	path.join(__dirname, "../uploads/presenter_cache/DemoVideo.mp4"),
];
const DEFAULT_PRESENTER_MOTION_VIDEO_URL =
	"https://res.cloudinary.com/infiniteapps/video/upload/v1766438047/aivideomatic/trend_seeds/aivideomatic/trend_seeds/MyVideoToReplicate_qlwrmu.mp4";
const STUDIO_EMPTY_PROMPT =
	"Studio is empty and locked; remove any background people from the reference; no people in the background, no passersby, no background figures or silhouettes, no reflections of people, no photos/posters/screens showing people, no mannequins or statues, no human-shaped shadows; background must be static with no moving elements, screens, mirrors, or window activity; if any windows or reflective surfaces exist, show only empty, still, blurred scenery with no human shapes; no candles, candle holders, or open flames anywhere; remove any candles from the reference.";
const PRESENTER_MOTION_STYLE =
	"locked-off tripod talking-head shot with a fixed frame; background edges stay perfectly locked with no camera shake, reframing, breathing zoom, rolling wobble, or drifting crop; human, credible seated presenter motion in the host's understated style; direct lens contact; head upright and centered with subtle conversational life, including tiny neck corrections, occasional soft chin dips, and one or two light emphasis nods across the shot; no forward/back head travel, no scale or zoom illusion, no side-to-side sway, and no jerky turns; shoulders and torso grounded but not frozen, with subtle breathing and small natural posture settling; hands low, relaxed, and mostly out of frame with only brief small emphasis gestures; natural blink cadence, mild brow movement, and visible speech-ready jaw and lip behavior that stays restrained and realistic; emotional read stays composed and even from start to finish; a trace smile only when appropriate, mostly closed-mouth or barely parted, with no toothy grin; avoid robotic motion, visible loops, frozen staring, surprise, skepticism, smirks, exaggerated sadness, exaggerated excitement, or repeated gesture patterns";

// Output defaults
const DEFAULT_OUTPUT_RATIO = "1280:720";
const DEFAULT_OUTPUT_FPS = 30;
const DEFAULT_SCALE_MODE = "cover";
const DEFAULT_IMAGE_SCALE_MODE = "blur";
const IMAGE_MONTAGE_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_IMAGE_MONTAGE_TIMEOUT_MS ?? 90000,
	30000,
	240000,
);
const IMAGE_MONTAGE_STATIC_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_IMAGE_MONTAGE_STATIC_TIMEOUT_MS ?? 90000,
	30000,
	180000,
);
const IMAGE_PLATE_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_IMAGE_PLATE_TIMEOUT_MS ?? 45000,
	15000,
	90000,
);
const IMAGE_MONTAGE_MAX_IMAGES = clampNumber(
	process.env.LONG_VIDEO_IMAGE_MONTAGE_MAX_IMAGES ?? 3,
	1,
	4,
);
const IMAGE_SEGMENT_RENDER_RESERVE = clampNumber(
	process.env.LONG_VIDEO_IMAGE_SEGMENT_RENDER_RESERVE ?? 2,
	0,
	4,
);
const ALLOW_PAID_IMAGE_SEGMENT_FALLBACK = envFlag(
	"LONG_VIDEO_ALLOW_PAID_IMAGE_SEGMENT_FALLBACK",
	false,
);
const INTERMEDIATE_VIDEO_CRF = clampNumber(16, 12, 24);
const FINAL_VIDEO_CRF = clampNumber(15, 10, 20);
const INTERMEDIATE_PRESET = "fast";
const FINAL_PRESET =
	String(process.env.FINAL_MASTER_PRESET || "slow").trim() || "slow";
const AUDIO_BITRATE = "256k";
const FINAL_LOUDNORM_FILTER = "loudnorm=I=-16:TP=-1.0:LRA=11";
const FINAL_MASTER_MAX_HEIGHT = clampNumber(
	process.env.FINAL_MASTER_MAX_HEIGHT ?? 2160,
	360,
	4320,
);
const FINAL_MASTER_MIN_HEIGHT = clampNumber(
	process.env.FINAL_MASTER_MIN_HEIGHT ?? 1080,
	0,
	FINAL_MASTER_MAX_HEIGHT,
);
const FINAL_MASTER_ALLOW_UPSCALE =
	String(process.env.FINAL_MASTER_ALLOW_UPSCALE || "false").toLowerCase() ===
	"true";
const FINAL_MASTER_MAX_UPSCALE_FACTOR = clampNumber(
	process.env.FINAL_MASTER_MAX_UPSCALE_FACTOR ?? 1.5,
	1,
	4,
);
// 0 disables the timeout so long 4K encodes can finish without being killed.
const FINAL_MASTER_TIMEOUT_MS = clampNumber(
	process.env.FINAL_MASTER_TIMEOUT_MS ?? 0,
	0,
	7 * 24 * 60 * 60 * 1000,
);
const FINAL_GOP_SECONDS = 2;
const FINAL_COLOR_SPACE = "bt709";
const FINAL_COLOR_RANGE = "tv";
const WATERMARK_TEXT = "https://serenejannat.com";
const WATERMARK_FONT_SIZE_PCT = 0.03;
const WATERMARK_MARGIN_PCT = 0.035;
const WATERMARK_OPACITY = 0.34;
const WATERMARK_SHADOW_OPACITY = 0.18;
const WATERMARK_SHADOW_PX = 2;
const CSE_PREFERRED_IMG_SIZE = "xlarge";
const CSE_FALLBACK_IMG_SIZE = "large";
const CSE_ULTRA_IMG_SIZE = "xxlarge";
const CSE_MIN_IMAGE_SHORT_EDGE = 720;
const CSE_MAX_PAGE_SIZE = 10;
const CSE_MAX_PAGES = 5;
const CSE_MAX_IMAGE_RESULTS = 40;
const CSE_RELAXED_MIN_IMAGE_SHORT_EDGE = 480;

// Intro (seconds). Keep the opening tight so the content rhythm starts quickly.
const INTRO_MIN_SEC = clampNumber(
	process.env.LONG_VIDEO_INTRO_MIN_SEC ?? 5.5,
	4,
	14,
);
const INTRO_MAX_SEC = clampNumber(
	process.env.LONG_VIDEO_INTRO_MAX_SEC ?? 8.5,
	INTRO_MIN_SEC,
	16,
);
const DEFAULT_INTRO_SEC = clampNumber(
	process.env.LONG_VIDEO_DEFAULT_INTRO_SEC ?? 6.5,
	INTRO_MIN_SEC,
	INTRO_MAX_SEC,
);
// Outro (seconds)
const OUTRO_MIN_SEC = clampNumber(
	process.env.LONG_VIDEO_OUTRO_MIN_SEC ?? 4.2,
	3,
	7,
);
const OUTRO_MAX_SEC = clampNumber(
	process.env.LONG_VIDEO_OUTRO_MAX_SEC ?? 7.2,
	OUTRO_MIN_SEC,
	9,
);
const DEFAULT_OUTRO_SEC = clampNumber(
	process.env.LONG_VIDEO_DEFAULT_OUTRO_SEC ?? 6.0,
	OUTRO_MIN_SEC,
	OUTRO_MAX_SEC,
);
const OUTRO_SMILE_TAIL_SEC = clampNumber(
	process.env.LONG_VIDEO_OUTRO_SMILE_TAIL_SEC ?? 2,
	1.8,
	2.4,
);
const REQUIRE_OUTRO_PRESENTER = true;
const INTRO_VIDEO_BLUR_SIGMA = clampNumber(2.6, 0, 8);
const INTRO_TEXT_FADE_IN_START = clampNumber(0.5, 0, 1.5);
const INTRO_TEXT_FADE_IN_DUR = clampNumber(0.55, 0.15, 1.2);
const INTRO_TEXT_X_PCT = clampNumber(0.12, 0.04, 0.2);
const INTRO_TEXT_Y_PCT = clampNumber(0.44, 0.2, 0.6);
const INTRO_SUBTITLE_Y_PCT = clampNumber(0.58, 0.3, 0.7);
const FINAL_FADE_OUT_SEC = clampNumber(0.5, 0, 1.2);

// Script pacing
const SCRIPT_VOICE_WPS = 2.65; // used only for word caps; target ~160 wpm
// Brisk news-presenter cadence: fast enough to feel alive, still coherent.
const SCRIPT_PACE_BIAS = clampNumber(1.02, 0.85, 1.35);
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
const OPENING_NO_FILLER_SEGMENT_INDICES = Object.freeze([0, 1, 2]);
const MAX_MICRO_EMOTES_PER_VIDEO = clampNumber(0, 0, 1);
const ENABLE_MICRO_EMOTES = true;
const ENABLE_MICRO_BREATHS = true;
const MAX_MICRO_BREATHS_PER_VIDEO = clampNumber(1, 0, 3);
const MICRO_BREATH_MIN_WORDS = clampNumber(26, 18, 40);
const MICRO_BREATH_TARGET_WORD = clampNumber(12, 8, 18);
// Shorts + engagement markers
const SHORTS_MIN_CANDIDATES = 3;
const SHORTS_MAX_CANDIDATES = 6;
const SHORTS_TARGET_SECONDS = [25, 35, 45];
const SHORTS_DEFAULT_TARGET_SECONDS = 35;
const SHORTS_DEFAULT_CTA_LINE = "Full breakdown on the channel.";
const SHORTS_EARLY_WINDOW_SEC = 28;
const SHORTS_OPEN_LOOP_WINDOW_SEC = 150;
const SHORTS_OPEN_LOOP_MIN_COUNT = 2;

// Audio processing
const AUDIO_SR = 48000;
const AUDIO_CHANNELS = 1; // mono voice for stability + smaller sync payload
const TRIM_LEADING_SILENCE = true;
const LEAD_SILENCE_MIN_SEC = clampNumber(0.04, 0.02, 0.2);
const LEAD_SILENCE_THRESHOLD_DB = clampNumber(-45, -60, -35);
const ALLOW_SLOW_NARRATION_TO_TARGET = envFlag(
	"LONG_VIDEO_ALLOW_SLOW_NARRATION_TO_TARGET",
	false,
);
const REWRITE_FOR_NARRATION_DURATION = envFlag(
	"LONG_VIDEO_REWRITE_FOR_NARRATION_DURATION",
	false,
);
const GLOBAL_ATEMPO_MIN = clampNumber(
	process.env.LONG_VIDEO_GLOBAL_ATEMPO_MIN ??
		(ALLOW_SLOW_NARRATION_TO_TARGET ? 0.95 : 1.0),
	0.85,
	1.05,
);
const GLOBAL_ATEMPO_MAX = clampNumber(
	process.env.LONG_VIDEO_GLOBAL_ATEMPO_MAX ?? 1.12,
	Math.max(1.0, GLOBAL_ATEMPO_MIN),
	1.18,
);
const INTRO_ATEMPO_MIN = clampNumber(
	process.env.LONG_VIDEO_INTRO_ATEMPO_MIN ?? 0.95,
	0.85,
	1.05,
);
const INTRO_ATEMPO_MAX = clampNumber(
	process.env.LONG_VIDEO_INTRO_ATEMPO_MAX ?? 1.14,
	1.0,
	1.25,
);
const OUTRO_ATEMPO_MIN = clampNumber(0.9, 0.9, 1.05);
const OUTRO_ATEMPO_MAX = clampNumber(1.06, 1.0, 1.15);
const SEGMENT_PAD_SEC = clampNumber(0.08, 0, 0.3);
const VOICE_SPEED_BOOST = clampNumber(
	process.env.LONG_VIDEO_VOICE_SPEED_BOOST ?? 1.02,
	1,
	1.12,
);
const FORCE_NEUTRAL_VOICEOVER = true;
const ALIGN_INTRO_OUTRO_ATEMPO = true;
const ALLOW_NARRATION_OVERRUN = true;
const MAX_NARRATION_OVERAGE_RATIO = clampNumber(1.5, 1.0, 1.8);
const MAX_NARRATION_OVERAGE_SEC = clampNumber(30, 5, 60);
const MAX_SUBTLE_VISUAL_EXPRESSIONS = clampNumber(
	process.env.LONG_VIDEO_MAX_SUBTLE_VISUAL_EXPRESSIONS ?? 2,
	0,
	4,
);
const SUBTLE_VISUAL_EDGE_BUFFER = clampNumber(1, 0, 3);
// Audio QA (quality-first voiceover)
const AUDIO_QA_ENABLED = true;
const AUDIO_QA_MAX_ATTEMPTS = clampNumber(4, 1, 4);
const AUDIO_QA_INTERNAL_SILENCE_SEC = clampNumber(0.58, 0.35, 2.5);
const AUDIO_QA_INTERNAL_SILENCE_DB = clampNumber(-40, -60, -25);
const AUDIO_QA_EDGE_BUFFER_SEC = clampNumber(0.08, 0, 0.2);
const AUDIO_QA_REPAIR_MAX_SILENCE_SEC = clampNumber(0.2, 0.12, 0.5);
const AUDIO_QA_TRANSCRIBE = true;
const AUDIO_QA_TRANSCRIBE_MODEL = "gpt-4o-mini-transcribe";
const AUDIO_QA_TRANSCRIBE_COOLDOWN_MS = clampNumber(
	process.env.LONG_VIDEO_AUDIO_QA_TRANSCRIBE_COOLDOWN_MS ?? 15 * 60 * 1000,
	60 * 1000,
	60 * 60 * 1000,
);
const AUDIO_QA_MIN_WORDS = clampNumber(7, 4, 14);
const AUDIO_QA_SIMILARITY_THRESHOLD = clampNumber(0.82, 0.7, 0.95);
const AUDIO_QA_STRICT_STABILITY_BOOST = clampNumber(0.08, 0, 0.2);
const AUDIO_QA_STRICT_STYLE_MAX = clampNumber(0.06, 0, 0.2);

// Presenter video QA/render prep
const PRESENTER_VIDEO_INPUT_FPS = 30;
const PRESENTER_VIDEO_INPUT_CRF = 18;
const REQUIRE_HEYGEN_PRESENTER_VIDEO = true;
// Short presenter beats are still visible enough that a static lip-sync output
// should retry instead of passing duration-only QA.
const PRESENTER_VIDEO_FREEZE_CHECK_MIN_SEC = clampNumber(0.6, 0.3, 12);
const PRESENTER_VIDEO_FREEZE_NOISE = clampNumber(0.0018, 0.0001, 0.01);
const PRESENTER_VIDEO_FREEZE_MIN_SEC = clampNumber(0.38, 0.15, 4);
const PRESENTER_VIDEO_MAX_FREEZE_RATIO = clampNumber(0.16, 0.05, 0.6);
const PRESENTER_VIDEO_MAX_FREEZE_SEC = clampNumber(0.75, 0.2, 4);
const PRESENTER_MOTION_QA_ENABLED = true;
const REQUIRE_REAL_PRESENTER_VIDEO = envFlag(
	"LONG_VIDEO_REQUIRE_REAL_PRESENTER_VIDEO",
	true,
);
const ALLOW_STATIC_PRESENTER_FALLBACK = envFlag(
	"LONG_VIDEO_ALLOW_STATIC_PRESENTER_FALLBACK",
	false,
);
const PRESENTER_MOTION_FREEZE_CHECK_MIN_SEC = clampNumber(0.35, 0.2, 3);
const PRESENTER_MOTION_FREEZE_MIN_SEC = clampNumber(0.24, 0.12, 2);
const PRESENTER_MOTION_FREEZE_NOISE = clampNumber(0.006, 0.0001, 0.01);
const PRESENTER_MOTION_MAX_FREEZE_RATIO = clampNumber(0.08, 0.03, 0.6);
const PRESENTER_MOTION_MAX_FREEZE_SEC = clampNumber(0.55, 0.15, 3);
const PRESENTER_BASELINE_MOTION_MAX_FREEZE_RATIO = clampNumber(
	process.env.LONG_VIDEO_BASELINE_MOTION_MAX_FREEZE_RATIO ?? 0.45,
	PRESENTER_MOTION_MAX_FREEZE_RATIO,
	0.6,
);
const PRESENTER_BASELINE_MOTION_MAX_FREEZE_SEC = clampNumber(
	process.env.LONG_VIDEO_BASELINE_MOTION_MAX_FREEZE_SEC ?? 1.2,
	PRESENTER_MOTION_MAX_FREEZE_SEC,
	2.5,
);
const PRESENTER_BASELINE_MOTION_NEAR_PASS_ENABLED = envFlag(
	"LONG_VIDEO_BASELINE_MOTION_NEAR_PASS",
	true,
);
const PRESENTER_RENDER_MOTION_NEAR_PASS_ENABLED = envFlag(
	"LONG_VIDEO_RENDER_MOTION_NEAR_PASS",
	true,
);
const PRESENTER_RENDER_MOTION_MAX_FREEZE_RATIO = clampNumber(
	process.env.LONG_VIDEO_RENDER_MOTION_MAX_FREEZE_RATIO ?? 0.48,
	PRESENTER_MOTION_MAX_FREEZE_RATIO,
	0.6,
);
const PRESENTER_RENDER_MOTION_MAX_FREEZE_SEC = clampNumber(
	process.env.LONG_VIDEO_RENDER_MOTION_MAX_FREEZE_SEC ?? 1.05,
	PRESENTER_MOTION_MAX_FREEZE_SEC,
	1.8,
);
const HEYGEN_REQUIRED_MOTION_MAX_FREEZE_RATIO = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_REQUIRED_MOTION_MAX_FREEZE_RATIO ?? 0.78,
	PRESENTER_RENDER_MOTION_MAX_FREEZE_RATIO,
	0.82,
);
const HEYGEN_REQUIRED_MOTION_MAX_FREEZE_SEC = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_REQUIRED_MOTION_MAX_FREEZE_SEC ?? 2.2,
	PRESENTER_RENDER_MOTION_MAX_FREEZE_SEC,
	4,
);
const HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_RATIO = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_RATIO ?? 0.58,
	PRESENTER_RENDER_MOTION_MAX_FREEZE_RATIO,
	0.72,
);
const HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_SEC = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_SEC ?? 1.4,
	PRESENTER_RENDER_MOTION_MAX_FREEZE_SEC,
	3,
);
const HEYGEN_CONTENT_MOTION_MAX_FREEZE_RATIO = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_CONTENT_MOTION_MAX_FREEZE_RATIO ??
		HEYGEN_REQUIRED_MOTION_MAX_FREEZE_RATIO,
	HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_RATIO,
	HEYGEN_REQUIRED_MOTION_MAX_FREEZE_RATIO,
);
const HEYGEN_CONTENT_MOTION_MAX_FREEZE_SEC = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_CONTENT_MOTION_MAX_FREEZE_SEC ??
		HEYGEN_REQUIRED_MOTION_MAX_FREEZE_SEC,
	HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_SEC,
	HEYGEN_REQUIRED_MOTION_MAX_FREEZE_SEC,
);
const HEYGEN_RENDER_MAX_ATTEMPTS = Math.floor(
	clampNumber(process.env.LONG_VIDEO_HEYGEN_RENDER_MAX_ATTEMPTS ?? 2, 1, 3),
);
const HEYGEN_FINAL_MOTION_FREEZE_NOISE = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_FINAL_MOTION_FREEZE_NOISE ?? 0.001,
	0.0001,
	0.01,
);
const HEYGEN_FINAL_MOTION_FREEZE_MIN_SEC = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_FINAL_MOTION_FREEZE_MIN_SEC ?? 1.0,
	0.5,
	3,
);
const HEYGEN_FINAL_MOTION_MAX_FREEZE_SEC = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_FINAL_MOTION_MAX_FREEZE_SEC ?? 3.0,
	1.0,
	6,
);
const HEYGEN_FINAL_MOTION_MAX_FREEZE_RATIO = clampNumber(
	process.env.LONG_VIDEO_HEYGEN_FINAL_MOTION_MAX_FREEZE_RATIO ?? 0.35,
	0.08,
	0.8,
);
const PRESENTER_VIDEO_MAX_SHORTFALL_SEC = clampNumber(0.18, 0.05, 1.5);
const PRESENTER_VIDEO_MIN_DURATION_RATIO = clampNumber(0.93, 0.5, 1);

// Presenter stability
const ENABLE_WARDROBE_EDIT = true;
const ENABLE_HEYGEN_PRESENTER_VIDEO = true;
const USE_MOTION_REF_BASELINE = false;
const BASELINE_DUR_SEC = clampNumber(
	process.env.LONG_VIDEO_BASELINE_DUR_SEC ?? 8,
	6,
	10,
);
// Generate a few extra attempts only if earlier local presenter takes fail QA. We still
// keep at most three accepted clips so presenter rotation stays coherent.
const BASELINE_VARIANTS = Math.floor(
	clampNumber(process.env.LONG_VIDEO_BASELINE_VARIANTS ?? 5, 1, 6),
);
const BASELINE_MAX_ACCEPTED_VARIANTS_PER_EXPRESSION = Math.floor(clampNumber(
	process.env.LONG_VIDEO_BASELINE_MAX_ACCEPTED_VARIANTS_PER_EXPRESSION ?? 3,
	1,
	4,
));
const BASELINE_IDENTITY_QA_ENABLED = envFlag(
	"LONG_VIDEO_BASELINE_IDENTITY_QA",
	true,
);
const BASELINE_IDENTITY_QA_MODEL =
	String(process.env.LONG_VIDEO_BASELINE_IDENTITY_QA_MODEL || CHAT_MODEL).trim() ||
	CHAT_MODEL;
const BASELINE_IDENTITY_QA_MIN_SCORE = clampNumber(
	process.env.LONG_VIDEO_BASELINE_IDENTITY_QA_MIN_SCORE ?? 0.78,
	0.5,
	0.98,
);
const BASELINE_IDENTITY_QA_MAX_DISTORTION_SCORE = clampNumber(
	process.env.LONG_VIDEO_BASELINE_IDENTITY_QA_MAX_DISTORTION_SCORE ?? 0.28,
	0,
	0.8,
);
const BASELINE_MAX_EXPRESSIONS = Math.floor(
	clampNumber(process.env.LONG_VIDEO_BASELINE_MAX_EXPRESSIONS ?? 1, 1, 4),
);
const LOCK_PRESENTER_VIDEO_EXPRESSION = envFlag(
	"LONG_VIDEO_LOCK_PRESENTER_VIDEO_EXPRESSION",
	true,
);
const BASELINE_STOP_AFTER_CLEAN_PASS = envFlag(
	"LONG_VIDEO_BASELINE_STOP_AFTER_CLEAN_PASS",
	false,
);
const CAMERA_ZOOM_OUT = clampNumber(
	process.env.LONG_VIDEO_CAMERA_ZOOM_OUT ?? 0.96,
	0.84,
	1.0,
);
const ENABLE_DYNAMIC_CAMERA_MOTION = envFlag(
	"LONG_VIDEO_DYNAMIC_CAMERA_MOTION",
	true,
);
const ENABLE_PRESENTER_DYNAMIC_CAMERA_MOTION = envFlag(
	"LONG_VIDEO_PRESENTER_DYNAMIC_CAMERA_MOTION",
	false,
);
const ENABLE_IMAGE_DYNAMIC_CAMERA_MOTION = envFlag(
	"LONG_VIDEO_IMAGE_DYNAMIC_CAMERA_MOTION",
	false,
);
const ENABLE_STILL_IMAGE_MOTION = envFlag(
	"LONG_VIDEO_STILL_IMAGE_MOTION",
	true,
);
const STILL_IMAGE_ZOOM_MAX = clampNumber(
	process.env.LONG_VIDEO_STILL_IMAGE_ZOOM_MAX ?? 1.006,
	1.0,
	1.04,
);
const STILL_IMAGE_ZOOM_STEP = clampNumber(
	process.env.LONG_VIDEO_STILL_IMAGE_ZOOM_STEP ?? 0.000018,
	0,
	0.0002,
);
const ENABLE_PRESENTER_DESHAKE = envFlag(
	"LONG_VIDEO_PRESENTER_DESHAKE",
	true,
);
const PRESENTER_DESHAKE_RX = normalizeDeshakeRange(
	process.env.LONG_VIDEO_PRESENTER_DESHAKE_RX ?? 16,
);
const PRESENTER_DESHAKE_RY = normalizeDeshakeRange(
	process.env.LONG_VIDEO_PRESENTER_DESHAKE_RY ?? 16,
);
const CAMERA_PUNCH_ZOOM_PRESENTER_MAX = clampNumber(
	process.env.LONG_VIDEO_CAMERA_PUNCH_ZOOM_PRESENTER_MAX ?? 1.035,
	1.005,
	1.07,
);
const CAMERA_PUNCH_ZOOM_IMAGE_MAX = clampNumber(
	process.env.LONG_VIDEO_CAMERA_PUNCH_ZOOM_IMAGE_MAX ?? 1.025,
	1.005,
	1.08,
);
const CAMERA_SLOW_ZOOM_PRESENTER_MAX = clampNumber(
	process.env.LONG_VIDEO_CAMERA_SLOW_ZOOM_PRESENTER_MAX ?? 1.012,
	1.001,
	1.04,
);
const CAMERA_SLOW_ZOOM_IMAGE_MAX = clampNumber(
	process.env.LONG_VIDEO_CAMERA_SLOW_ZOOM_IMAGE_MAX ?? 1.012,
	1.001,
	1.05,
);
const ENABLE_SEGMENT_FADES = false;
const ENABLE_SOFT_SEGMENT_TRANSITIONS = true;
const SEGMENT_TRANSITION_SEC = clampNumber(0.12, 0, 0.35);
const SEGMENT_TRANSITION_PAD_SEC = clampNumber(0.12, 0, 0.35);
const SEGMENT_TRANSITION_MIN_CLIP_SEC = clampNumber(1.6, 0.5, 5);

// Music
const MUSIC_VOLUME = clampNumber(
	process.env.LONG_VIDEO_MUSIC_VOLUME ?? 0.09,
	0.03,
	0.5,
);
const MUSIC_DUCK_THRESHOLD = clampNumber(
	process.env.LONG_VIDEO_MUSIC_DUCK_THRESHOLD ?? 0.06,
	0.02,
	0.3,
);
const MUSIC_DUCK_RATIO = clampNumber(
	process.env.LONG_VIDEO_MUSIC_DUCK_RATIO ?? 8,
	2,
	16,
);
const MUSIC_DUCK_ATTACK = clampNumber(
	process.env.LONG_VIDEO_MUSIC_DUCK_ATTACK ?? 12,
	5,
	200,
);
const MUSIC_DUCK_RELEASE = clampNumber(
	process.env.LONG_VIDEO_MUSIC_DUCK_RELEASE ?? 420,
	40,
	1600,
);
const MUSIC_DUCK_MAKEUP = clampNumber(
	process.env.LONG_VIDEO_MUSIC_DUCK_MAKEUP ?? 1.0,
	1,
	3,
);

const DEFAULT_MUSIC_URL = String(
	process.env.LONG_VIDEO_DEFAULT_MUSIC_URL || "",
).trim();
const DEFAULT_MUSIC_PATH = String(
	process.env.LONG_VIDEO_DEFAULT_MUSIC_PATH || "",
).trim();
const MUSIC_USE_DEFAULT_FIRST = envFlag(
	"LONG_VIDEO_MUSIC_DEFAULT_FIRST",
	false,
);
const MUSIC_RESOLVE_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_MUSIC_RESOLVE_TIMEOUT_MS ?? 120000,
	15000,
	300000,
);
const MUSIC_TRACK_DOWNLOAD_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_MUSIC_TRACK_DOWNLOAD_TIMEOUT_MS ?? 20000,
	8000,
	60000,
);
const MUSIC_MAX_DOWNLOAD_CANDIDATES = Math.round(
	clampNumber(process.env.LONG_VIDEO_MUSIC_MAX_DOWNLOAD_CANDIDATES ?? 5, 1, 10),
);

// Overlays
// Larger overlays by default; cap size relative to frame width.
const OVERLAY_SCALE = clampNumber(0.4, 0.14, 0.55);
const OVERLAY_MAX_WIDTH_PCT = clampNumber(0.45, 0.28, 0.5);
const OVERLAY_BORDER_PX = clampNumber(6, 0, 18);
const OVERLAY_MARGIN_PX = clampNumber(28, 6, 120);
const OVERLAY_DEFAULT_POSITION = "topRight";
const MAX_AUTO_OVERLAYS = clampNumber(10, 3, 16);

// Content visual mix (presenter vs static images). The opening and outro stay
// presenter-led, while the body leans on feed visuals unless one extra presenter
// beat is worth the paid render.
const CONTENT_PRESENTER_RATIO = clampNumber(
	process.env.LONG_VIDEO_PRESENTER_RATIO ?? 0.4,
	0.2,
	0.7,
);
const PRE_SCRIPT_VISUAL_RESEARCH_ENABLED = envFlag(
	"LONG_VIDEO_PRE_SCRIPT_VISUAL_RESEARCH",
	true,
);
const PRE_SCRIPT_VISUAL_RESEARCH_QUERY_LIMIT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_SCRIPT_VISUAL_QUERY_LIMIT ?? 4,
	0,
	8,
));
const PRE_SCRIPT_VISUAL_RESEARCH_RESULTS_PER_QUERY = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_SCRIPT_VISUAL_RESULTS_PER_QUERY ?? 28,
	8,
	60,
));
const PRE_SCRIPT_VISUAL_RESEARCH_TITLE_LIMIT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_SCRIPT_VISUAL_TITLE_LIMIT ?? 18,
	4,
	40,
));
const PRE_SCRIPT_VISUAL_BEAT_PLAN_ENABLED = envFlag(
	"LONG_VIDEO_PRE_SCRIPT_VISUAL_BEAT_PLAN",
	true,
);
const PRE_SCRIPT_VISUAL_BEAT_LIMIT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_SCRIPT_VISUAL_BEAT_LIMIT ?? 18,
	4,
	40,
));
const PRE_SCRIPT_VISUAL_VIDEO_BEAT_PROBE_ENABLED = envFlag(
	"LONG_VIDEO_PRE_SCRIPT_VISUAL_VIDEO_BEAT_PROBE",
	true,
);
const PRE_SCRIPT_VISUAL_VIDEO_BEAT_PROBE_LIMIT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_SCRIPT_VISUAL_VIDEO_BEAT_PROBE_LIMIT ?? 4,
	0,
	12,
));
const PRE_TTS_VISUAL_GROUNDING_ENABLED = envFlag(
	"LONG_VIDEO_PRE_TTS_VISUAL_GROUNDING",
	true,
);
const PRE_TTS_VISUAL_GROUNDING_IMAGE_LIMIT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_TTS_VISUAL_GROUNDING_IMAGES ?? 8,
	3,
	16,
));
const PRE_TTS_VISUAL_GROUNDING_VARIANT_LIMIT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_TTS_VISUAL_GROUNDING_VARIANTS ?? 4,
	1,
	8,
));
const PRE_TTS_VISUAL_GROUNDING_SEGMENT_LIMIT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_TTS_VISUAL_GROUNDING_SEGMENTS ?? 24,
	4,
	80,
));
const PRE_TTS_VISUAL_VIDEO_PROBE_ENABLED = envFlag(
	"LONG_VIDEO_PRE_TTS_VISUAL_VIDEO_PROBE",
	true,
);
const PRE_TTS_VISUAL_VIDEO_PROBE_SEGMENT_LIMIT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_PRE_TTS_VISUAL_VIDEO_PROBE_SEGMENTS ?? 6,
	0,
	16,
));
const FORCE_OPENING_PRESENTER_COUNT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_FORCE_OPENING_PRESENTER_COUNT ?? 2,
	0,
	4,
));
const OPENING_PRESENTER_CLUSTER_COUNT = Math.floor(clampNumber(
	process.env.LONG_VIDEO_OPENING_PRESENTER_CLUSTER_COUNT ?? 2,
	FORCE_OPENING_PRESENTER_COUNT,
	5,
));
const OPTIONAL_HEYGEN_CONTENT_CALLS_OVERRIDE = String(
	process.env.LONG_VIDEO_MAX_OPTIONAL_HEYGEN_CONTENT_SEGMENTS || "",
).trim();
const PREFERRED_HEYGEN_PRESENTER_SEGMENT_SEC = clampNumber(
	process.env.LONG_VIDEO_PREFERRED_HEYGEN_PRESENTER_SEGMENT_SEC ??
		process.env.LONG_VIDEO_MIN_HEYGEN_PRESENTER_SEGMENT_SEC ??
		10,
	5,
	18,
);
const PRESENTER_BODY_TARGET_SEC = clampNumber(
	process.env.LONG_VIDEO_PRESENTER_BODY_TARGET_SEC ??
		PREFERRED_HEYGEN_PRESENTER_SEGMENT_SEC,
	5,
	24,
);
const PRESENTER_BODY_MAX_SEC = clampNumber(
	process.env.LONG_VIDEO_PRESENTER_BODY_MAX_SEC ?? 20,
	PRESENTER_BODY_TARGET_SEC,
	30,
);
const OPENING_PRESENTER_USE_HERO_SYNC = envFlag(
	"LONG_VIDEO_OPENING_PRESENTER_HERO_SYNC",
	false,
);
const OPENING_PRESENTER_BASELINE_TRIES = Math.floor(clampNumber(
	process.env.LONG_VIDEO_OPENING_PRESENTER_BASELINE_TRIES ?? 3,
	1,
	3,
));
const OPENING_PRESENTER_SYNC_RETRIES = Math.floor(clampNumber(
	process.env.LONG_VIDEO_OPENING_PRESENTER_SYNC_RETRIES ?? 0,
	0,
	3,
));
const OPENING_PRESENTER_MIN_SEC = clampNumber(
	process.env.LONG_VIDEO_OPENING_PRESENTER_MIN_SEC ?? 10,
	6,
	30,
);
const OPENING_PRESENTER_MAX_SEC = clampNumber(
	process.env.LONG_VIDEO_OPENING_PRESENTER_MAX_SEC ?? 20,
	OPENING_PRESENTER_MIN_SEC,
	35,
);
const OPENING_PRESENTER_TARGET_SEC = clampNumber(
	process.env.LONG_VIDEO_OPENING_PRESENTER_TARGET_SEC ?? 16,
	OPENING_PRESENTER_MIN_SEC,
	OPENING_PRESENTER_MAX_SEC,
);
const MIN_ACTUAL_PRESENTER_SEGMENTS = Math.floor(clampNumber(
	process.env.LONG_VIDEO_MIN_ACTUAL_PRESENTER_SEGMENTS ??
		Math.max(2, FORCE_OPENING_PRESENTER_COUNT),
	0,
	20,
));
const MIN_ACTUAL_PRESENTER_PLAN_RATIO = clampNumber(
	process.env.LONG_VIDEO_MIN_ACTUAL_PRESENTER_PLAN_RATIO ?? 0.15,
	0,
	1,
);
const MIN_ACTUAL_PRESENTER_DURATION_RATIO = clampNumber(
	process.env.LONG_VIDEO_MIN_ACTUAL_PRESENTER_DURATION_RATIO ?? 0.04,
	0,
	0.5,
);
const ALLOW_ZERO_PRESENTER_OUTPUT = envFlag(
	"LONG_VIDEO_ALLOW_ZERO_PRESENTER_OUTPUT",
	false,
);
const FAIL_ON_LOW_PRESENTER_COVERAGE = envFlag(
	"LONG_VIDEO_FAIL_ON_LOW_PRESENTER_COVERAGE",
	false,
);
const REQUIRE_FORCED_OPENING_PRESENTERS = envFlag(
	"LONG_VIDEO_REQUIRE_FORCED_OPENING_PRESENTERS",
	false,
);
const FEED_VIDEO_ENABLED = envFlag("LONG_VIDEO_FEED_VIDEO_ENABLED", true);
const FEED_VIDEO_SEARCH_ENABLED = envFlag(
	"LONG_VIDEO_FEED_VIDEO_SEARCH",
	true,
);
const BING_FEED_VIDEO_SEARCH_ENABLED = envFlag(
	"LONG_VIDEO_BING_FEED_VIDEO_SEARCH",
	true,
);
const FEED_VIDEO_TRUSTED_SOURCES_ONLY = envFlag(
	"LONG_VIDEO_FEED_VIDEO_TRUSTED_ONLY",
	false,
);
const FEED_VIDEO_TARGET_FEED_SHARE = clampNumber(
	process.env.LONG_VIDEO_FEED_VIDEO_TARGET_FEED_SHARE ?? 0.35,
	0,
	0.75,
);
const FEED_VIDEO_MAX_SEGMENTS = Math.floor(
	clampNumber(process.env.LONG_VIDEO_FEED_VIDEO_MAX_SEGMENTS ?? 3, 0, 8),
);
const FEED_VIDEO_MAX_SEGMENT_ATTEMPTS = Math.floor(
	clampNumber(
		process.env.LONG_VIDEO_FEED_VIDEO_MAX_SEGMENT_ATTEMPTS ?? 8,
		0,
		24,
	),
);
const FEED_VIDEO_MIN_SEGMENT_SEC = clampNumber(
	process.env.LONG_VIDEO_FEED_VIDEO_MIN_SEGMENT_SEC ?? 3.2,
	1.5,
	12,
);
const FEED_VIDEO_MIN_SOURCE_SEC = clampNumber(
	process.env.LONG_VIDEO_FEED_VIDEO_MIN_SOURCE_SEC ?? 2.2,
	1,
	12,
);
const FEED_VIDEO_MAX_CLIP_SEC = clampNumber(
	process.env.LONG_VIDEO_FEED_VIDEO_MAX_CLIP_SEC ?? 8.5,
	2,
	12,
);
const FEED_VIDEO_MAX_BYTES = Math.floor(
	clampNumber(
		process.env.LONG_VIDEO_FEED_VIDEO_MAX_BYTES ?? 32_000_000,
		3_000_000,
		120_000_000,
	),
);
const FEED_VIDEO_CANDIDATE_LIMIT = Math.floor(
	clampNumber(process.env.LONG_VIDEO_FEED_VIDEO_CANDIDATES ?? 16, 4, 40),
);
const FEED_VIDEO_QUERY_LIMIT = Math.floor(
	clampNumber(process.env.LONG_VIDEO_FEED_VIDEO_QUERY_LIMIT ?? 4, 1, 8),
);
const FEED_VIDEO_DOWNLOAD_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_FEED_VIDEO_DOWNLOAD_TIMEOUT_MS ?? 30000,
	8000,
	90000,
);
const FEED_VIDEO_PAGE_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_FEED_VIDEO_PAGE_TIMEOUT_MS ?? 9000,
	4000,
	20000,
);
const FEED_VIDEO_MIN_WIDTH = Math.floor(
	clampNumber(process.env.LONG_VIDEO_FEED_VIDEO_MIN_WIDTH ?? 480, 240, 1920),
);
const FEED_VIDEO_MIN_HEIGHT = Math.floor(
	clampNumber(process.env.LONG_VIDEO_FEED_VIDEO_MIN_HEIGHT ?? 270, 160, 1080),
);
const FEED_VIDEO_MOTION_QA_ENABLED = envFlag(
	"LONG_VIDEO_FEED_VIDEO_MOTION_QA",
	true,
);
const FEED_VIDEO_FREEZE_NOISE = clampNumber(0.0025, 0.0001, 0.02);
const FEED_VIDEO_FREEZE_MIN_SEC = clampNumber(1.2, 0.4, 5);
const FEED_VIDEO_MAX_FREEZE_RATIO = clampNumber(0.82, 0.35, 0.98);
const FEED_VIDEO_MAX_FREEZE_SEC = clampNumber(5.5, 1.5, 12);
const IMAGE_SEGMENT_TARGET_SEC = clampNumber(3.8, 2.5, 8);
const IMAGE_SEGMENT_MIN_IMAGES = clampNumber(2, 1, 6);
const IMAGE_SEGMENT_MAX_IMAGES = clampNumber(
	process.env.LONG_VIDEO_IMAGE_SEGMENT_MAX_IMAGES ?? 3,
	2,
	6,
);
const STRICT_TOPIC_RELEVANT_FEED_IMAGES = envFlag(
	"LONG_VIDEO_STRICT_TOPIC_RELEVANT_FEED_IMAGES",
	true,
);
const IMAGE_SEGMENT_MULTI_MIN_SEC = clampNumber(4.8, 3, 12);
const IMAGE_SEGMENT_MIN_UNIQUE_RATIO = clampNumber(0.5, 0.4, 1);
const ENABLE_PRESENTER_RUN_MERGE = true;
const MERGE_REQUIRED_PRESENTER_RUNS = envFlag(
	"LONG_VIDEO_MERGE_REQUIRED_PRESENTER_RUNS",
	true,
);
const PRESENTER_RUN_MERGE_MAX_SEC = clampNumber(
	process.env.LONG_VIDEO_PRESENTER_RUN_MERGE_MAX_SEC ?? 28,
	8,
	36,
);
const PRESENTER_RUN_MERGE_MAX_SEGMENTS = clampNumber(
	process.env.LONG_VIDEO_PRESENTER_RUN_MERGE_MAX_SEGMENTS ?? 4,
	2,
	5,
);
const OPTIONAL_PRESENTER_CLUSTER_MIN_SEC = Math.min(
	PRESENTER_BODY_MAX_SEC,
	clampNumber(
		process.env.LONG_VIDEO_OPTIONAL_PRESENTER_CLUSTER_MIN_SEC ??
			Math.max(12, PREFERRED_HEYGEN_PRESENTER_SEGMENT_SEC),
		6,
		24,
	),
);
const OPTIONAL_PRESENTER_CLUSTER_TARGET_SEC = Math.min(
	PRESENTER_BODY_MAX_SEC,
	Math.max(
		OPTIONAL_PRESENTER_CLUSTER_MIN_SEC,
		clampNumber(
			process.env.LONG_VIDEO_OPTIONAL_PRESENTER_CLUSTER_TARGET_SEC ??
				Math.max(15, OPTIONAL_PRESENTER_CLUSTER_MIN_SEC),
			OPTIONAL_PRESENTER_CLUSTER_MIN_SEC,
			28,
		),
	),
);
const OPTIONAL_PRESENTER_CLUSTER_MAX_SEGMENTS = Math.floor(
	clampNumber(
		process.env.LONG_VIDEO_OPTIONAL_PRESENTER_CLUSTER_MAX_SEGMENTS ??
			Math.min(3, PRESENTER_RUN_MERGE_MAX_SEGMENTS),
		1,
		PRESENTER_RUN_MERGE_MAX_SEGMENTS,
	),
);
const IMAGE_SEARCH_MAX_QUERY_VARIANTS = clampNumber(12, 4, 16);
const IMAGE_SEARCH_CANDIDATE_MULTIPLIER = clampNumber(9, 2, 12);
const IMAGE_SEARCH_MIN_RANKED_POOL_MULTIPLIER = clampNumber(
	process.env.LONG_VIDEO_IMAGE_MIN_RANKED_POOL_MULTIPLIER ?? 6,
	2,
	12,
);
const GOOGLE_IMAGES_SEARCH_ENABLED = true;
const GOOGLE_IMAGES_VARIANT_LIMIT = clampNumber(6, 1, 8);
const GOOGLE_IMAGES_RESULTS_PER_QUERY = clampNumber(40, 8, 80);
const GOOGLE_IMAGES_MIN_POOL_MULTIPLIER = clampNumber(6, 1, 10);
const CSE_IMAGE_TOPUP_ENABLED = envFlag("LONG_VIDEO_CSE_IMAGE_TOPUP", true);
const CSE_IMAGE_LAST_RESORT_ONLY = envFlag(
	"LONG_VIDEO_CSE_IMAGE_LAST_RESORT_ONLY",
	true,
);
const CSE_IMAGE_FREE_POOL_FLOOR = clampNumber(
	process.env.LONG_VIDEO_CSE_IMAGE_FREE_POOL_FLOOR ?? 2,
	1,
	6,
);
const CSE_CONTEXT_FALLBACK_ENABLED = envFlag(
	"LONG_VIDEO_CSE_CONTEXT_FALLBACK",
	false,
);
const NEWS_IMAGE_FALLBACK_ENABLED = envFlag(
	"LONG_VIDEO_NEWS_IMAGE_FALLBACK",
	true,
);
const NEWS_IMAGE_FALLBACK_LIMIT = clampNumber(
	process.env.LONG_VIDEO_NEWS_IMAGE_FALLBACK_LIMIT ?? 8,
	2,
	12,
);
const NEWS_RSS_TIMEOUT_MS = clampNumber(
	process.env.LONG_VIDEO_NEWS_RSS_TIMEOUT_MS ?? 8000,
	3000,
	20000,
);
const PROMPT_TOPIC_NEWS_CONTEXT_ENABLED = envFlag(
	"LONG_VIDEO_PROMPT_NEWS_CONTEXT",
	true,
);
const PROMPT_TOPIC_NEWS_CONTEXT_LIMIT = clampNumber(
	process.env.LONG_VIDEO_PROMPT_NEWS_CONTEXT_LIMIT ?? 4,
	1,
	8,
);
const PROMPT_TOPIC_NEWS_QUERY_LIMIT = clampNumber(
	process.env.LONG_VIDEO_PROMPT_NEWS_QUERY_LIMIT ?? 3,
	1,
	6,
);
const PROMPT_TOPIC_SOURCE_MIN_LINKS = clampNumber(
	process.env.LONG_VIDEO_PROMPT_SOURCE_MIN_LINKS ?? 2,
	1,
	5,
);
const PROMPT_TOPIC_CSE_QUERY_LIMIT = clampNumber(
	process.env.LONG_VIDEO_PROMPT_CSE_QUERY_LIMIT ?? 4,
	1,
	8,
);
const PROMPT_TOPIC_CSE_RESULTS_PER_QUERY = clampNumber(
	process.env.LONG_VIDEO_PROMPT_CSE_RESULTS_PER_QUERY ?? 4,
	1,
	8,
);
const PROMPT_TOPIC_FREE_IMAGE_PREFETCH_ENABLED = envFlag(
	"LONG_VIDEO_PROMPT_IMAGE_PREFETCH",
	true,
);
const PROMPT_TOPIC_FREE_IMAGE_PREFETCH_QUERY_LIMIT = clampNumber(
	process.env.LONG_VIDEO_PROMPT_IMAGE_PREFETCH_QUERY_LIMIT ?? 3,
	1,
	6,
);
const PROMPT_TOPIC_FREE_IMAGE_PREFETCH_TARGET = clampNumber(
	process.env.LONG_VIDEO_PROMPT_IMAGE_PREFETCH_TARGET ?? 24,
	8,
	60,
);

const ENABLE_LONG_VIDEO_OVERLAYS = false;

const LONG_VIDEO_KEEP_TMP = false;

const ENABLE_TOPIC_DETAIL_CARDS = envFlag(
	"LONG_VIDEO_TOPIC_DETAIL_CARDS",
	true,
);
const TOPIC_DETAIL_CARD_MAX_PER_TOPIC = Math.floor(
	clampNumber(process.env.LONG_VIDEO_TOPIC_DETAIL_CARD_MAX_PER_TOPIC ?? 1, 0, 3),
);
const TITLE_PROMISE_QA_ENABLED = envFlag(
	"LONG_VIDEO_TITLE_PROMISE_QA",
	true,
);

const LONG_VIDEO_CONTROLLER_FINGERPRINT = (() => {
	try {
		const src = fs.readFileSync(__filename, "utf8");
		const hash = crypto
			.createHash("sha1")
			.update(src)
			.digest("hex")
			.slice(0, 12);
		return `long-video:${hash}`;
	} catch {
		return "long-video:unknown";
	}
})();

function getLongVideoRuntimeProfile() {
	return {
		fingerprint: LONG_VIDEO_CONTROLLER_FINGERPRINT,
		presenterVideoEngine: "heygen",
		heygenResolution: HEYGEN_DEFAULT_RESOLUTION,
		heygenExpressiveness: HEYGEN_DEFAULT_EXPRESSIVENESS,
		heygenAspectRatio: HEYGEN_DEFAULT_ASPECT_RATIO,
		heygenFit: HEYGEN_DEFAULT_FIT,
		requirePresenterVideo: REQUIRE_HEYGEN_PRESENTER_VIDEO,
		requireRealPresenterVideo: REQUIRE_REAL_PRESENTER_VIDEO,
		requireOutroPresenter: REQUIRE_OUTRO_PRESENTER,
		outroSmileTailSec: Number(OUTRO_SMILE_TAIL_SEC.toFixed(1)),
		openingPresenterTargetSec: Number(OPENING_PRESENTER_TARGET_SEC.toFixed(1)),
		openingPresenterMaxSec: Number(OPENING_PRESENTER_MAX_SEC.toFixed(1)),
		openingPresenterClusterCount: OPENING_PRESENTER_CLUSTER_COUNT,
		preferredHeyGenPresenterSegmentSec:
			PREFERRED_HEYGEN_PRESENTER_SEGMENT_SEC,
		optionalPresenterClusterMinSec: OPTIONAL_PRESENTER_CLUSTER_MIN_SEC,
		optionalPresenterClusterTargetSec: OPTIONAL_PRESENTER_CLUSTER_TARGET_SEC,
		presenterBodyTargetSec: PRESENTER_BODY_TARGET_SEC,
		presenterBodyMaxSec: PRESENTER_BODY_MAX_SEC,
		qualityFirstPresenterPlanning: true,
		heygenCallRules: {
			under3Min: 3,
			threeToUnder4Min: 4,
			fourMinOrMore: 5,
		},
		allowStaticPresenterFallback: ALLOW_STATIC_PRESENTER_FALLBACK,
		feedVideoEnabled: FEED_VIDEO_ENABLED,
		feedVideoMaxSegments: FEED_VIDEO_MAX_SEGMENTS,
		feedVideoMaxAttempts: FEED_VIDEO_MAX_SEGMENT_ATTEMPTS,
		heygenPollMaxSec: Number((HEYGEN_POLL_TIMEOUT_MS / 1000).toFixed(1)),
		heygenRenderMaxAttempts: HEYGEN_RENDER_MAX_ATTEMPTS,
		heygenFinalFreezeMaxSec: Number(
			HEYGEN_FINAL_MOTION_MAX_FREEZE_SEC.toFixed(2),
		),
		heygenFinalFreezeMaxRatio: Number(
			HEYGEN_FINAL_MOTION_MAX_FREEZE_RATIO.toFixed(2),
		),
		googleCseReady: GOOGLE_CSE_CONFIG_READY,
		googleCseIssue: GOOGLE_CSE_CONFIG_ISSUE || "",
	};
}

logStartupDetail("[LongVideo] controller loaded", getLongVideoRuntimeProfile());

/* ---------------------------------------------------------------
 * In-memory job store
 * ------------------------------------------------------------- */

const JOBS = new Map();
const MAX_JOBS_TO_KEEP = 250;
const JOB_TTL_MS = 1000 * 60 * 60 * 6;
let audioQaTranscribeDisabledUntil = 0;
let audioQaTranscribeDisabledReason = "";

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
				new Date(b[1].updatedAt || b[1].createdAt).getTime(),
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
if (SHOULD_PERSIST_LONG_VIDEO) ensureDir(OUTPUT_DIR);
if (SHOULD_PERSIST_LONG_VIDEO) ensureDir(THUMBNAIL_DIR);

function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function withTimeout(promise, timeoutMs, label = "operation") {
	const ms = Number(timeoutMs);
	if (!Number.isFinite(ms) || ms <= 0) return await promise;
	let timer = null;
	try {
		return await Promise.race([
			promise,
			new Promise((_, reject) => {
				timer = setTimeout(() => {
					reject(new Error(`${label} timed out after ${Math.round(ms)}ms`));
				}, ms);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
}

function clampNumber(n, min, max) {
	const x = Number(n);
	if (!Number.isFinite(x)) return min;
	return Math.max(min, Math.min(max, x));
}

function normalizeDeshakeRange(value) {
	const clamped = clampNumber(value, 0, 64);
	return Math.max(0, Math.min(64, Math.round(clamped / 16) * 16));
}

function envFlag(name, fallback = false) {
	const raw = process.env[name];
	if (raw === undefined || raw === null || raw === "") return Boolean(fallback);
	return !/^(0|false|no|off)$/i.test(String(raw).trim());
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
				(_m, _scheme, _www, domain) => `https://${domain}`,
			);
			s = s.replace(
				/(^|\s)([a-z0-9.-]+\.[a-z]{2,}[^\s)]*)/gi,
				(_m, prefix, url) =>
					`${prefix}https://${url.replace(/^https?:\/\//i, "")}`,
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
		(t) => !TOPIC_STOP_WORDS.has(t) && !GENERIC_TOPIC_TOKENS.has(t),
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
			"",
		);
		const cleaned = base.replace(/[^a-z0-9]+/g, " ").trim();
		if (cleaned) tokens.add(cleaned);
		if (lowered) tokens.add(lowered);
	}
	return Array.from(tokens);
}

function topicHasAttributionSource(topic = {}, topicContext = {}) {
	const ctx = Array.isArray(topicContext?.context)
		? topicContext.context
		: Array.isArray(topicContext)
			? topicContext
			: [];
	const hasContextLink = ctx.some(
		(item) => item && typeof item !== "string" && isHttpUrl(item.link || ""),
	);
	if (hasContextLink) return true;
	const articles = Array.isArray(topic?.trendStory?.articles)
		? topic.trendStory.articles
		: [];
	return articles.some((article) => isHttpUrl(article?.url || ""));
}

function buildTopicSourcePolicyPromptBlock(topics = [], topicContexts = []) {
	const safeTopics = Array.isArray(topics) ? topics : [];
	const lines = safeTopics.map((topic, idx) => {
		const label = cleanTopicLabel(
			topic?.displayTopic || topic?.topic || `Topic ${idx + 1}`,
		);
		const hasSources = topicHasAttributionSource(topic, topicContexts?.[idx]);
		return `- Topic ${idx + 1} (${label}): ${
			hasSources
				? "source links are available; attribute only those listed sources near factual or disputed claims."
				: 'no source links are available; do not cite named outlets, journals, "studies", "reviews", "researchers", or "reporting". Keep claims high-level and practical.'
		}`;
	});
	const anySources = safeTopics.some((topic, idx) =>
		topicHasAttributionSource(topic, topicContexts?.[idx]),
	);
	return {
		anySources,
		text: `Source policy:\n${
			lines.length
				? lines.join("\n")
				: '- No source links are available; do not invent named sources or reporting.'
		}`,
	};
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

const UNSUPPORTED_ATTRIBUTION_PATTERNS = [
	/\b(?:according\s+to|reported\s+by|as\s+reported\s+by|per|via|sources?\s+say)\b/i,
	/\b(?:reports?|reported|reporting)\s+(?:actually\s+)?(?:suggest|suggests|say|says|said|show|shows|confirm|confirms|support|supports)\b/i,
	/\b(?:key|latest|strongest)\s+reporting\b/i,
	/\breporting\s+(?:actually\s+)?(?:supports|confirms|suggests|shows|says)\b/i,
	/\b(?:reviews?|studies?)\s+in\s+[A-Z][A-Za-z&.\-\s]{2,90}\s+(?:suggest|show|shows|find|finds|say|says|warn|warns)\b/i,
	/\b(?:[A-Z][a-z]+\s+)?researchers?\s+(?:have\s+long\s+)?(?:warned|warn|say|said|suggest|suggested|found|find)\b/i,
	/\bCurrent\s+Opinion\s+in\s+Psychology\b/,
];

function segmentHasUnsupportedAttribution(text = "") {
	const raw = String(text || "");
	return UNSUPPORTED_ATTRIBUTION_PATTERNS.some((rx) => rx.test(raw));
}

function findUnsupportedAttributionSegments({
	script,
	topics = [],
	topicContexts = [],
} = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const out = [];
	for (let i = 0; i < segments.length; i++) {
		const segment = segments[i] || {};
		const topicIndex =
			Number.isFinite(Number(segment.topicIndex)) && Number(segment.topicIndex) >= 0
				? Number(segment.topicIndex)
				: 0;
		if (topicHasAttributionSource(topics?.[topicIndex], topicContexts?.[topicIndex]))
			continue;
		if (segmentHasUnsupportedAttribution(segment.text || "")) {
			out.push(Number.isFinite(Number(segment.index)) ? Number(segment.index) : i);
		}
	}
	return out;
}

function uppercaseFirstAlpha(text = "") {
	return String(text || "").replace(/^(\s*["'(\[]*)([a-z])/, (m, prefix, ch) =>
		`${prefix}${String(ch || "").toUpperCase()}`,
	);
}

function stripUnsupportedAttributionPhrasing(text = "") {
	let cleaned = sanitizeSegmentText(text || "");
	if (!cleaned) return cleaned;
	cleaned = cleaned
		.replace(
			/^\s*(?:according\s+to|as\s+reported\s+by|reported\s+by|per|via)\s+[^,]{2,110},\s*/i,
			"",
		)
		.replace(
			/\b(?:the\s+)?(?:latest|key|strongest)?\s*reporting\s+(?:actually\s+)?(?:supports|confirms|suggests|shows|says)\s+(?:that\s+)?/gi,
			"",
		)
		.replace(
			/\b(?:reports?|reported)\s+(?:suggest|suggests|say|says|said|show|shows|confirm|confirms|support|supports)\s+(?:that\s+)?/gi,
			"",
		)
		.replace(
			/\b(?:reviews?|studies?)\s+in\s+[A-Z][A-Za-z&.\-\s]{2,110}\s+(?:suggest|show|shows|find|finds|say|says|warn|warns)\s+(?:that\s+)?/gi,
			"",
		)
		.replace(
			/\b(?:[A-Z][a-z]+\s+)?researchers?\s+(?:have\s+long\s+)?(?:warned|warn|say|said|suggest|suggested|found|find)\s+(?:that\s+)?/gi,
			"",
		)
		.replace(/\s+/g, " ")
		.trim();
	cleaned = uppercaseFirstAlpha(cleaned);
	return sanitizeSegmentText(cleaned || text);
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
if (ffmpegPath) logStartupDetail(`[FFmpeg]  binary : ${ffmpegPath}`);
else
	console.warn(
		"[LongVideo] WARN - No valid FFmpeg binary found. Set FFMPEG_PATH or ensure ffmpeg is on PATH.",
	);

const ffprobePath = resolveSharedFfprobePath({ ffmpegPath });
if (ffprobePath) {
	logStartupDetail(`[FFprobe] binary : ${ffprobePath}`);
} else {
	console.warn(
		"[LongVideo] WARN - No valid FFprobe binary found. Install ffprobe, set FFPROBE_PATH, or install ffprobe-static.",
	);
}

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

		proc.on("close", (code, signal) => {
			if (killTimer) clearTimeout(killTimer);
			if (code === 0) return resolve({ stdout, stderr });
			const output = stderr || stdout || "";
			const excerpt =
				output.length > 4000 ? `...${output.slice(-4000)}` : output;
			const tailHint = killedByTimeout ? " (killed by timeout)" : "";
			const signalHint = signal ? `, signal ${signal}` : "";
			reject(
				new Error(
					`${label} failed (code ${code}${signalHint})${tailHint}: ${excerpt}`,
				),
			);
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
			},
		);
	});
}

async function probeDurationSeconds(filePath) {
	const info = await probeMedia(filePath);
	return info.duration || 0;
}

const durationCache = new Map();
async function probeDurationSecondsCached(filePath) {
	const key = String(filePath || "").trim();
	if (!key) return 0;
	if (durationCache.has(key)) return durationCache.get(key);
	const duration = await probeDurationSeconds(key);
	durationCache.set(key, duration || 0);
	return duration || 0;
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
	{ retries = 2, baseDelayMs = 600, label = "" } = {},
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
					String(e?.message || ""),
				);
			if (attempt >= retries || !retriable) throw e;
			const delay = Math.round(
				baseDelayMs * Math.pow(2, attempt) + Math.random() * 150,
			);
			if (label)
				console.warn(
					`[Retry] ${label} attempt ${attempt + 1}/${retries + 1} failed: ${
						e.message
					}. waiting ${delay}ms`,
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
				let settled = false;
				const streamTimeoutMs = Math.max(5000, Number(timeoutMs) + 5000);
				const streamTimer = setTimeout(() => {
					settle(
						new Error(
							`download stream timed out after ${Math.round(streamTimeoutMs)}ms`,
						),
					);
				}, streamTimeoutMs);
				function settle(err) {
					if (settled) return;
					settled = true;
					clearTimeout(streamTimer);
					if (err) {
						try {
							res.data.destroy(err);
						} catch {}
						try {
							ws.destroy(err);
						} catch {}
						reject(err);
						return;
					}
					resolve();
				}
				res.data.pipe(ws);
				res.data.on("error", settle);
				res.data.on("aborted", () => settle(new Error("download stream aborted")));
				ws.on("finish", () => settle());
				ws.on("error", settle);
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

async function downloadToFileWithLimit({
	url,
	outPath,
	timeoutMs = 30000,
	retries = 1,
	maxBytes = FEED_VIDEO_MAX_BYTES,
}) {
	ensureDir(path.dirname(outPath));
	let lastErr = null;

	for (let attempt = 0; attempt <= retries; attempt++) {
		try {
			const res = await axios.get(url, {
				responseType: "stream",
				timeout: timeoutMs,
				maxRedirects: 4,
				headers: {
					"User-Agent": "agentai-long-video/2.0",
					Accept: "video/mp4,video/webm,video/*,*/*;q=0.8",
				},
				validateStatus: (s) => s >= 200 && s < 400,
			});
			const declared = Number(res.headers?.["content-length"] || 0);
			if (declared && declared > maxBytes) {
				throw new Error(`remote file too large: ${declared}`);
			}

			await new Promise((resolve, reject) => {
				let bytes = 0;
				let settled = false;
				const ws = fs.createWriteStream(outPath);
				const fail = (err) => {
					if (settled) return;
					settled = true;
					try {
						res.data.destroy();
					} catch {}
					try {
						ws.destroy();
					} catch {}
					reject(err);
				};
				res.data.on("data", (chunk) => {
					bytes += chunk.length;
					if (bytes > maxBytes) {
						fail(new Error(`download exceeded ${maxBytes} bytes`));
					}
				});
				res.data.on("error", fail);
				ws.on("error", fail);
				ws.on("finish", () => {
					if (settled) return;
					settled = true;
					resolve();
				});
				res.data.pipe(ws);
			});

			const st = fs.statSync(outPath);
			if (!st || st.size < 1024) throw new Error("downloaded file too small");
			return outPath;
		} catch (e) {
			lastErr = e;
			safeUnlink(outPath);
			if (attempt < retries) {
				await sleep(350 * Math.pow(2, attempt));
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

	// WEBM/MKV
	if (head[0] === 0x1a && head[1] === 0x45 && head[2] === 0xdf && head[3] === 0xa3)
		return { kind: "video", ext: "webm" };

	return null;
}

function imageMimeTypeForPath(filePath) {
	const detected = detectFileType(filePath);
	const ext = String(detected?.ext || path.extname(filePath || ""))
		.toLowerCase()
		.replace(/^\./, "");
	if (ext === "png") return "image/png";
	if (ext === "jpg" || ext === "jpeg") return "image/jpeg";
	if (ext === "webp") return "image/webp";
	if (ext === "gif") return "image/gif";
	return "image/jpeg";
}

function imagePathToDataUrl(filePath) {
	if (!filePath || !fs.existsSync(filePath)) return "";
	const b64 = fs.readFileSync(filePath).toString("base64");
	return `data:${imageMimeTypeForPath(filePath)};base64,${b64}`;
}

async function hashFileSha1(filePath) {
	if (!filePath || !fs.existsSync(filePath))
		throw new Error("hash input missing");
	return new Promise((resolve, reject) => {
		const hash = crypto.createHash("sha1");
		const stream = fs.createReadStream(filePath);
		stream.on("error", reject);
		stream.on("data", (chunk) => hash.update(chunk));
		stream.on("end", () => resolve(hash.digest("hex")));
	});
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

function validateCreateBody(body = {}, controllerConfig = {}) {
	const cfg = normalizeLongVideoControllerConfig(controllerConfig);
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

	const presenterAssetUrl = resolveRequestedPresenterAsset(body, cfg);
	const voiceoverUrl = String(
		body.voiceoverUrl || body.narrationUrl || body.audioUrl || "",
	).trim();
	const skipPresenterAdjustments = Boolean(
		body.skipPresenterAdjustments ||
			body.disablePresenterAdjustments ||
			body.skipWardrobeEdit ||
			body.disableWardrobeEdit,
	);
	const enableHeyGenPresenterMotion =
		cfg.enableHeyGenPresenterMotion ?? true;
	const enableWardrobeEdit = cfg.enableWardrobeEdit && !skipPresenterAdjustments;
	const stopAfterThumbnail = Boolean(
		body.stopAfterThumbnail ||
			body.thumbnailOnly ||
			body.debugStopAfterThumbnail,
	);
	const disableMusic = false;

	return {
		errors,
		clean: {
			preferredTopicHint: String(body.preferredTopicHint || "").trim(),
			category: normalizeCategoryLabel(
				String(body.category || LONG_VIDEO_TRENDS_CATEGORY || "Entertainment"),
			).trim(),
			language: normalizeLanguageLabel(body.language || "English"),
			targetDurationSec: duration,
			introSec,
			outroSec,
			output: { ...outRatio, fps, scaleMode, imageScaleMode },
			presenterAssetUrl,
			voiceoverUrl,
			musicUrl: "",
			disableMusic,
			dryRun: Boolean(body.dryRun),
			orchestratorDryRun: Boolean(
				body.orchestratorDryRun ||
					body.dryRunOrchestrator ||
					body.planOnly ||
					body.scriptOnly,
			),
			stopAfterThumbnail,
			enableHeyGenPresenterMotion,
			enableWardrobeEdit,
			skipPresenterAdjustments,
			youtubeAccessToken: String(body.youtubeAccessToken || "").trim(),
			youtubeRefreshToken: String(body.youtubeRefreshToken || "").trim(),
			youtubeTokenExpiresAt: body.youtubeTokenExpiresAt || "",
			youtubeCategory: String(
				body.youtubeCategory || LONG_VIDEO_YT_CATEGORY || "Entertainment",
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

function normalizeWhitespace(value = "") {
	return String(value || "")
		.replace(/\s+/g, " ")
		.trim();
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
						.trim(),
				)
				.filter(Boolean),
		),
	);
}

function filterSpecificTopicTokens(tokens = []) {
	const norm = normalizeTopicTokens(tokens);
	const filtered = norm.filter(
		(t) => t.length >= 3 && !GENERIC_TOPIC_TOKENS.has(t),
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
		.replace(/\s+\|\s*[^|]{2,}$/g, "")
		.replace(/\s+-\s+[^-]{2,}$/g, "")
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
		story.topic || story.rawTitle || story.title || "",
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
			(s.interestOverTime && Number(s.interestOverTime.points) > 0),
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
			(t) => set.has(t),
		)
	)
		return "film";
	if (
		[
			"tv",
			"series",
			"season",
			"episode",
			"streaming",
			"reality",
			"survivor",
			"finale",
		].some((t) => set.has(t))
	)
		return "tv";
	if (
		["song", "album", "music", "tour", "concert", "singer", "rapper"].some(
			(t) => set.has(t),
		)
	)
		return "music";
	if (
		["celebrity", "actor", "actress", "influencer", "tiktok"].some((t) =>
			set.has(t),
		)
	)
		return "celebrity";
	return "general";
}

const CATEGORY_LABEL_ALIASES = {
	petsandanimals: "Pets and Animals",
	peopleblogs: "SocialIssues",
	"people&blogs": "SocialIssues",
	peopleandblogs: "SocialIssues",
	peopleblog: "SocialIssues",
	socialissues: "SocialIssues",
	socialissue: "SocialIssues",
	relationships: "SocialIssues",
	relationship: "SocialIssues",
};

function normalizeCategoryLabel(label) {
	const raw = String(label || "").trim();
	if (!raw) return "";
	const key = raw.toLowerCase().replace(/\s+/g, "");
	return CATEGORY_LABEL_ALIASES[key] || raw;
}

const PROMPT_CATEGORY_RULES = [
	{
		label: "Health",
		weight: 4,
		patterns: [
			/\b(public\s+health|world\s+health\s+organization|health|disease|outbreak|infection|infected|illness|hospital|symptoms?|transmission|pandemic|epidemic|vaccine|cdc|case\s+counts?|contact\s+tracing|mortality|treatment)\b/i,
			/\b[a-z0-9-]*virus\b/i,
		],
	},
	{
		label: "Lifestyle",
		weight: 5,
		patterns: [
			/\b(mental\s+fatigue|burnout|overloaded|always\s+tired|tired\s+after|tired\s+even|exhausted|drained|fake\s+rest|sleep|resting|rest|self\s+care|wellness|routine|mind\s+never\s+clocked\s+out|mental\s+noise)\b/i,
		],
	},
	{
		label: "SocialIssues",
		weight: 6,
		patterns: [
			/\b(making friends|make friends|adult friendship|friendships?|friendship feels|friends feels|making new friends|meeting people|social life|social connection|social isolation|loneliness|lonely|belonging|community|close friends|group chat|reach out|text first)\b/i,
			/\b(friendships?|friends?)\b[^.?!\n]{0,80}\b(hard|difficult|awkward|lonely|alone|rejection|rejected|ignored|disconnected|isolated|drifting|harder)\b/i,
			/\b(hard|difficult|awkward|lonely|alone|rejection|rejected|ignored|disconnected|isolated|drifting|harder)\b[^.?!\n]{0,80}\b(friendships?|friends?)\b/i,
		],
	},
	{
		label: "Travel",
		weight: 3,
		patterns: [
			/\b(top\s*\d+\s+)?(cities|city|countries|country|places|destinations?|travel|tourism|visit|vacation|hotels?|resorts?|beaches|landmarks?|airports?|cruise|road\s*trip)\b/i,
			/\btop\s*\d+\s+citites\b/i,
		],
	},
	{
		label: "Gaming",
		weight: 3,
		patterns: [
			/\b(video\s*games?|gaming|gameplay|console|playstation|xbox|nintendo|steam|rpg|trailer|demo|combat|open\s+world|patch|developer|studio|esports?)\b/i,
		],
	},
	{
		label: "Sports",
		weight: 5,
		patterns: [
			/\b(sports?|nascar|cup\s+series|race\s*car|stock\s*car|motorsports?|racing|driver|nba|nfl|mlb|nhl|soccer|football|basketball|baseball|hockey|ufc|boxing|tennis|golf|matchup|playoffs?|tournament|score|standings|draft|coach|player)\b/i,
		],
	},
	{
		label: "Finance",
		weight: 5,
		patterns: [
			/\b(stocks?|market|finance|bitcoin|crypto|inflation|fed|interest\s+rates?|earnings|revenue|profit|bank|economy|investment|investors?)\b/i,
			/\b(broke|paycheck|pay\s*check|cost\s+of\s+living|rent|renter|renters|bills?|subscriptions?|expenses?|spending|budget|grocer(?:y|ies)|wages?|salary|debt|autopay|overdraft|late\s+fees?|financial\s+stress|money\s+stress)\b/i,
		],
	},
	{
		label: "Technology",
		weight: 3,
		patterns: [
			/\b(technology|tech|ai|artificial\s+intelligence|software|startup|app|iphone|android|robotics?|cybersecurity|chip|semiconductor|openai|google|microsoft|apple|tesla)\b/i,
		],
	},
	{
		label: "Politics",
		weight: 3,
		patterns: [
			/\b(politics|political|election|president|prime\s+minister|senate|congress|parliament|governor|white\s+house|supreme\s+court|government|policy|lawmakers?|campaign|vote|diplomacy|sanctions|ceasefire|war)\b/i,
		],
	},
	{
		label: "Science",
		weight: 2,
		patterns: [
			/\b(science|space|nasa|astronomy|physics|climate\s+science|researchers?|study|scientists?|discovery|experiment)\b/i,
		],
	},
	{
		label: "Business",
		weight: 2,
		patterns: [
			/\b(business|company|companies|ceo|merger|acquisition|layoffs?|brand|retail|sales|industry|startup|deal)\b/i,
		],
	},
	{
		label: "FoodDrink",
		weight: 2,
		patterns: [
			/\b(food|drink|restaurant|recipe|coffee|tea|meal|diet|cooking|chef|fast\s+food|beverage)\b/i,
		],
	},
	{
		label: "Education",
		weight: 2,
		patterns: [
			/\b(education|school|college|university|students?|teachers?|learning|course|degree|campus)\b/i,
		],
	},
	{
		label: "Climate",
		weight: 2,
		patterns: [
			/\b(climate|weather|storm|hurricane|wildfire|flood|heatwave|emissions|renewable|environment)\b/i,
		],
	},
	{
		label: "Fashion",
		weight: 2,
		patterns: [
			/\b(fashion|style|outfit|runway|designer|makeup|beauty|skincare|luxury)\b/i,
		],
	},
	{
		label: "Pets and Animals",
		weight: 2,
		patterns: [
			/\b(pets?|animals?|dogs?|cats?|wildlife|zoo|veterinary|vet|rescue)\b/i,
		],
	},
	{
		label: "Entertainment",
		weight: 2,
		patterns: [
			/\b(movie|film|tv|series|streaming|celebrity|actor|actress|music|album|song|concert|trailer|box\s+office|netflix|disney|hollywood|reality\s+show|survivor|finale|episode|season\s+\d+)\b/i,
		],
	},
];

function promptCategoryText({ topics = [], promptText = "" } = {}) {
	const parts = [promptText];
	for (const topic of Array.isArray(topics) ? topics : []) {
		if (!topic) continue;
		parts.push(
			topic.displayTopic,
			topic.topic,
			topic.angle,
			topic.reason,
			topic.source,
			topic.topList?.subject,
			...(Array.isArray(topic.keywords) ? topic.keywords : []),
			...(Array.isArray(topic.searchHints) ? topic.searchHints : []),
			...(Array.isArray(topic.imageSearchHints) ? topic.imageSearchHints : []),
		);
	}
	return parts.filter(Boolean).join(" ");
}

function inferPromptCategoryLabel({ topics = [], promptText = "" } = {}) {
	const hay = promptCategoryText({ topics, promptText }).trim();
	if (!hay) return "";
	const scores = PROMPT_CATEGORY_RULES.map((rule, order) => {
		let score = 0;
		for (const pattern of rule.patterns || []) {
			const matches = hay.match(new RegExp(pattern.source, pattern.flags || "i"));
			if (matches) score += rule.weight || 1;
		}
		return { label: rule.label, score, order };
	})
		.filter((item) => item.score > 0)
		.sort((a, b) => b.score - a.score || a.order - b.order);
	return scores.length ? normalizeCategoryLabel(scores[0].label) : "";
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
				.toLowerCase() === target,
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

function normalizeContextItem(item) {
	if (typeof item === "string") {
		const text = normalizeWhitespace(item);
		return text ? text.slice(0, 320) : "";
	}
	if (!item || typeof item !== "object") return null;
	const title = normalizeWhitespace(item.title || "").slice(0, 180);
	const snippet = normalizeWhitespace(item.snippet || item.description || "").slice(
		0,
		260,
	);
	const link = isHttpUrl(item.link) ? String(item.link).trim() : "";
	const image = isHttpUrl(item.image) ? String(item.image).trim() : null;
	const source = normalizeWhitespace(item.source || "").slice(0, 120);
	if (!title && !snippet && !link) return null;
	return {
		...item,
		title,
		snippet,
		link,
		image,
		source,
	};
}

function uniqueContextItems(items = [], { limit = 0 } = {}) {
	const seen = new Set();
	const out = [];
	for (const raw of Array.isArray(items) ? items : []) {
		const item = normalizeContextItem(raw);
		if (!item) continue;
		const key =
			typeof item === "string"
				? `text:${item.toLowerCase()}`
				: item.link
					? `url:${normalizeImageUrlKey(item.link).toLowerCase()}`
					: `ctx:${item.title}|${item.snippet}`.toLowerCase();
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(item);
		if (limit && out.length >= limit) break;
	}
	return out;
}

function countContextSourceLinks(items = []) {
	return uniqueStrings(
		(Array.isArray(items) ? items : [])
			.map((item) => (typeof item === "string" ? "" : item?.link))
			.filter((u) => isHttpUrl(u)),
	).length;
}

function safeSlug(text = "", max = 60) {
	return String(text || "")
		.toLowerCase()
		.replace(/[^\w]+/g, "_")
		.replace(/^_+|_+$/g, "")
		.slice(0, max);
}

function normalizeTrendPotentialImages(list = []) {
	if (!Array.isArray(list)) return [];
	const out = [];
	const seen = new Set();
	for (const item of list) {
		if (!item) continue;
		const url = String(
			item.imageurl ||
				item.imageUrl ||
				item.url ||
				item.link ||
				item.originalUrl ||
				"",
		).trim();
		if (!isHttpUrl(url) || isLikelyThumbnailUrl(url)) continue;
		const key = normalizeImageUrlKey(url);
		if (seen.has(key)) continue;
		seen.add(key);
		out.push({
			url,
			source: String(
				item.source || item.pageUrl || item.contextLink || "",
			).trim(),
			title: String(
				item.description || item.title || item.caption || "",
			).trim(),
			width: item.width || null,
			height: item.height || null,
			origin: "trend",
		});
	}
	return out;
}

function normalizeTrendPotentialVideos(list = []) {
	if (!Array.isArray(list)) return [];
	const out = [];
	const seen = new Set();
	for (const item of list) {
		if (!item) continue;
		const obj = typeof item === "string" ? { url: item } : item;
		const url = String(
			obj.videoUrl ||
				obj.videoURL ||
				obj.contentUrl ||
				obj.contentURL ||
				obj.url ||
				obj.link ||
				obj.originalUrl ||
				"",
		).trim();
		const pageUrl = String(
			obj.pageUrl ||
				obj.contextLink ||
				obj.sourceUrl ||
				obj.hostPageUrl ||
				obj.embedUrl ||
				"",
		).trim();
		const usableUrl = isHttpUrl(url) ? url : isHttpUrl(pageUrl) ? pageUrl : "";
		if (!usableUrl) continue;
		const key = normalizeImageUrlKey(usableUrl);
		if (seen.has(key)) continue;
		seen.add(key);
		out.push({
			url: usableUrl,
			pageUrl: isHttpUrl(pageUrl) ? pageUrl : "",
			source: String(
				obj.source || obj.publisher || obj.siteName || obj.contextLink || "",
			).trim(),
			title: String(
				obj.description || obj.title || obj.caption || obj.name || "",
			).trim(),
			durationSec: Number(obj.durationSec || obj.duration || 0) || 0,
			width: obj.width || obj.w || null,
			height: obj.height || obj.h || null,
			origin: "trend",
		});
	}
	return out;
}

function normalizeTrendStory(raw = {}) {
	const baseTitle = String(
		raw.trendDialogTitle ||
			raw.title ||
			raw.rawTitle ||
			raw.dialogTitle ||
			raw.youtubeShortTitle ||
			raw.seoTitle ||
			"",
	).trim();
	const topic = cleanTopicCandidate(baseTitle);
	const rawTitle = String(raw.rawTitle || raw.title || baseTitle || "").trim();
	const relatedQueries = normalizeRelatedQueries(
		raw.relatedQueries || raw.trendSignals?.relatedQueries || null,
	);
	const interestOverTime = normalizeInterestOverTime(
		raw.interestOverTime || raw.trendSignals?.interestOverTime || null,
	);
	const relatedPhrases = uniqueStrings(
		[...relatedQueries.rising, ...relatedQueries.top],
		{ limit: 8 },
	);
	const searchPhrases = uniqueStrings(
		[
			topic,
			rawTitle,
			...relatedPhrases,
			...(raw.imageSearchQueries || []),
			...(raw.visualSearchQueries || []),
			...(raw.searchPhrases || []),
			...(raw.entityNames || []),
		],
		{ limit: 12 },
	);
	const imageSearchQueries = uniqueStrings(
		[
			...(raw.imageSearchQueries || []),
			...(raw.visualSearchQueries || []),
			...(raw.searchQueries || []),
		]
			.map((q) => cleanTopicCandidate(q))
			.filter(Boolean),
		{ limit: 8 },
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
		{ limit: 10 },
	).filter((u) => isHttpUrl(u));
	const potentialImages = normalizeTrendPotentialImages(raw.potentialImages);
	const potentialVideos = normalizeTrendPotentialVideos([
		...(Array.isArray(raw.potentialVideos) ? raw.potentialVideos : []),
		...(Array.isArray(raw.videoCandidates) ? raw.videoCandidates : []),
		...(Array.isArray(raw.videos) ? raw.videos : []),
	]);
	const videos = uniqueStrings(
		[
			raw.video,
			raw.videoUrl,
			...(Array.isArray(raw.videoUrls) ? raw.videoUrls : []),
			...potentialVideos.map((v) => v.url).filter(Boolean),
		],
		{ limit: 12 },
	).filter((u) => isHttpUrl(u));
	const keywords = uniqueStrings(
		[
			...searchPhrases,
			...relatedPhrases,
			...articles.slice(0, 4).map((a) => a.title),
			topic,
		],
		{ limit: 12 },
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
		imageSearchQueries,
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
		potentialImages,
		videos,
		potentialVideos,
		articles,
		keywords,
	};
}

async function fetchTrendsStories({
	categoryLabel = LONG_VIDEO_TRENDS_CATEGORY,
	geo = LONG_VIDEO_TRENDS_GEO,
	language = "English",
	baseUrl,
	topicCount,
} = {}) {
	const categoryId = resolveTrendsCategoryId(categoryLabel);
	const params = new URLSearchParams({
		geo,
		hours: "48",
		language,
		category: String(categoryId),
		includeImages: "1",
		includePotentialImages: "1",
		long: "1",
	});
	if (Number.isFinite(Number(topicCount))) {
		params.set("topics", String(Math.max(1, Math.min(3, Number(topicCount)))));
	}
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
	{
		num = 4,
		searchType = null,
		imgSize = null,
		start = 1,
		maxPages = 1,
		jobId = null,
		label = "cse",
	} = {},
) {
	if (!GOOGLE_CSE_CONFIG_READY) return [];
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
				const buildParams = ({ omitImageFilters = false } = {}) => ({
					key: GOOGLE_CSE_KEY,
					cx: GOOGLE_CSE_ID,
					q,
					num: pageNum,
					start: pageStart,
					safe: "active",
					gl: "us",
					hl: "en",
					...(searchType ? { searchType } : {}),
					...(searchType === "image" && !omitImageFilters
						? {
								imgType: "photo",
								imgSize: imgSize || CSE_PREFERRED_IMG_SIZE,
							}
						: {}),
				});
				const requestCse = (omitImageFilters = false) =>
					axios.get(GOOGLE_CSE_ENDPOINT, {
						params: buildParams({ omitImageFilters }),
						timeout: 12000,
						validateStatus: (s) => s < 500,
					});
				let { data } = await requestCse(false);
				const invalidImageFilter =
					searchType === "image" &&
					data?.error &&
					Number(data.error.code) === 400 &&
					/invalid argument/i.test(String(data.error.message || ""));
				if (invalidImageFilter) {
					if (jobId) {
						logJob(jobId, "cse image filter retry", {
							label,
							query: q,
							imgSize,
							message: data?.error?.message || "invalid image filter",
						});
					}
					({ data } = await requestCse(true));
				}

				if (!data || data.error) {
					if (jobId) {
						logJob(jobId, "cse fetch failed", {
							label,
							query: q,
							status: data?.error?.code || null,
							message: data?.error?.message || "empty response",
							searchType,
							imgSize,
						});
					}
					break;
				}

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
						displayLink: String(it.displayLink || "").trim(),
						mime: String(it.mime || "").trim(),
						fileFormat: String(it.fileFormat || "").trim(),
						pagemap: it.pagemap || null,
						image: it.image || null,
					});
				}
			} catch (e) {
				if (jobId) {
					logJob(jobId, "cse fetch error", {
						label,
						query: q,
						error: e.message,
						status: e.response?.status || null,
						message: e.response?.data?.error?.message || null,
						searchType,
						imgSize,
					});
				}
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
		isEntertainmentCandidate(it.title, it.snippet),
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
				`${idx + 1}) ${it.title}${it.snippet ? " | " + it.snippet : ""}`,
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
			resp?.choices?.[0]?.message?.content || "",
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
	const promptDirected = (Array.isArray(topics) ? topics : []).some((t) =>
		isUserPromptTopicPick(t),
	);
	const bareDirectAnswerPrompt = (Array.isArray(topics) ? topics : []).some((t) =>
		isBareDirectAnswerTopic(t),
	);
	const maxMultiplier = clampNumber(
		process.env.LONG_VIDEO_FLEX_MAX_MULTIPLIER ?? 2,
		1,
		2.5,
	);
	let effectiveMaxMultiplier = promptDirected
		? Math.min(
				maxMultiplier,
				clampNumber(
					process.env.LONG_VIDEO_PROMPT_MAX_MULTIPLIER ?? 2,
					1,
					2,
				),
			)
		: maxMultiplier;
	if (bareDirectAnswerPrompt) {
		effectiveMaxMultiplier = Math.min(
			effectiveMaxMultiplier,
			clampNumber(
				process.env.LONG_VIDEO_DIRECT_ANSWER_MAX_MULTIPLIER ?? 0.65,
				0.25,
				1,
			),
		);
	}
	const sensitivePrompt =
		promptDirected &&
		(Array.isArray(topics) ? topics : []).some((topic) =>
			isSensitiveTopicText(
				[
					topic?.displayTopic,
					topic?.topic,
					topic?.promptText,
					topic?.promptBrief?.title,
					...(Array.isArray(topic?.promptBrief?.briefLines)
						? topic.promptBrief.briefLines
						: []),
				]
					.filter(Boolean)
					.join(" "),
			),
		);
	if (sensitivePrompt && !bareDirectAnswerPrompt) {
		effectiveMaxMultiplier = Math.min(
			effectiveMaxMultiplier,
			clampNumber(
				process.env.LONG_VIDEO_SENSITIVE_PROMPT_MAX_MULTIPLIER ?? 2,
				1,
				2,
			),
		);
	}
	const minMultiplier = clampNumber(
		process.env.LONG_VIDEO_FLEX_MIN_MULTIPLIER ?? 0.5,
		0.25,
		maxMultiplier,
	);
	let effectiveMinMultiplier = promptDirected
		? clampNumber(
				process.env.LONG_VIDEO_PROMPT_MIN_MULTIPLIER ?? 0.25,
				0.25,
				effectiveMaxMultiplier,
			)
		: minMultiplier;
	if (bareDirectAnswerPrompt) {
		effectiveMinMultiplier = Math.min(effectiveMinMultiplier, 0.25);
	}
	const fullSignal = clampNumber(
		process.env.LONG_VIDEO_FLEX_FULL_SIGNAL ?? 16,
		4,
		40,
	);
	const neutralSignal = clampNumber(
		process.env.LONG_VIDEO_FLEX_NEUTRAL_SIGNAL ?? 0.42,
		0.1,
		0.9,
	);

	const minSec = Math.max(18, Math.round(requested * effectiveMinMultiplier));
	const maxSec = Math.max(minSec, Math.round(requested * effectiveMaxMultiplier));

	let totalSignal = 0;
	let promptQualitySignal = 0;
	const promptSignals = [];
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

		let signal =
			ctx.length * 1.0 +
			articles.length * 1.4 +
			phrases.length * 0.4 +
			entities.length * 0.3;
		if (promptDirected && isUserPromptTopicPick(t)) {
			const brief =
				t.promptBrief || parseStructuredPromptBrief(t.promptText || t.topic || "");
			const promptText = String(
				t.promptText || brief?.raw || t.displayTopic || t.topic || "",
			);
			const wordCount = countWords(promptText);
			const sectionCount = [
				brief?.title,
				brief?.openingLine,
				brief?.tone,
				brief?.audience,
				brief?.thumbnailText,
				brief?.outroLine,
				...(Array.isArray(brief?.mustInclude) ? brief.mustInclude : []),
				...(Array.isArray(brief?.avoid) ? brief.avoid : []),
				...(Array.isArray(brief?.visuals) ? brief.visuals : []),
				...(Array.isArray(brief?.structure) ? brief.structure : []),
			].filter(Boolean).length;
			const controversyHits = (
				promptText.match(
					/\b(secret|secretly|fail|failure|controvers|psycholog|money|lonel|fear|mistake|judg|shame|viral|why|truth|hidden|avoid|must include|memorable|structure)\b/gi,
				) || []
			).length;
			const visualHits = (
				promptText.match(
					/\b(visuals?|feed|image|video|graph|chart|data|park|screen|phone|laptop|office|public|comments?|journal|coffee|sunlight)\b/gi,
				) || []
			).length;
			const promptSignal =
				clampNumber(wordCount / 28, 0, 8) +
				clampNumber(sectionCount * 0.85, 0, 8) +
				clampNumber(controversyHits * 0.55, 0, 7) +
				clampNumber(visualHits * 0.35, 0, 5);
			promptQualitySignal += promptSignal;
			signal += promptSignal;
			promptSignals.push({
				topic: cleanTopicLabel(t.displayTopic || t.topic || "").slice(0, 80),
				words: wordCount,
				sections: sectionCount,
				controversyHits,
				visualHits,
				signal: Number(promptSignal.toFixed(2)),
			});
		}
		totalSignal += signal;
	}

	const avgSignal = totalSignal / topicCount;
	const normalized = clampNumber(avgSignal / fullSignal, 0, 1);

	let multiplier = 1;
	if (normalized <= neutralSignal) {
		const t = clampNumber(normalized / neutralSignal, 0, 1);
		multiplier =
			effectiveMinMultiplier +
			(1 - effectiveMinMultiplier) * Math.pow(t, 0.85);
	} else {
		const t = clampNumber(
			(normalized - neutralSignal) / Math.max(0.01, 1 - neutralSignal),
			0,
			1,
		);
		multiplier = 1 + (effectiveMaxMultiplier - 1) * Math.pow(t, 1.35);
	}

	let target = Math.round(requested * multiplier);

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
			multiplier: Number(multiplier.toFixed(3)),
			fullSignal: Number(fullSignal.toFixed(2)),
			neutralSignal: Number(neutralSignal.toFixed(2)),
			minMultiplier: Number(effectiveMinMultiplier.toFixed(2)),
			maxMultiplier: Number(maxMultiplier.toFixed(2)),
			effectiveMaxMultiplier: Number(effectiveMaxMultiplier.toFixed(2)),
			promptDirected,
			bareDirectAnswerPrompt,
			promptQualitySignal: Number(promptQualitySignal.toFixed(2)),
			promptSignals,
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
			"topic topics seoTitle",
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
			$or: [
				{ presenterOutfit: { $exists: true, $ne: "" } },
				{ presenterOutfitStyle: { $exists: true, $ne: "" } },
			],
		})
			.sort({ createdAt: -1 })
			.limit(Math.max(0, Number(limit) || 0))
			.select({ presenterOutfit: 1, presenterOutfitStyle: 1 })
			.lean();
		return (recent || [])
			.map((v) => ({
				presenterOutfit: String(v.presenterOutfit || "").trim(),
				presenterOutfitStyle: String(v.presenterOutfitStyle || "").trim(),
			}))
			.filter((v) => v.presenterOutfit || v.presenterOutfitStyle);
	} catch (e) {
		logJob(null, "recent outfits lookup failed", { error: e.message });
		return [];
	}
}

const PRIOR_LONG_VIDEO_LOOKBACK_LIMIT = Math.floor(
	clampNumber(process.env.LONG_VIDEO_PRIOR_LOOKBACK_LIMIT ?? 80, 10, 200),
);
const PRIOR_LONG_VIDEO_MAX_MATCHES = Math.floor(
	clampNumber(process.env.LONG_VIDEO_PRIOR_MAX_MATCHES ?? 3, 1, 6),
);
const PRIOR_LONG_VIDEO_SIMILARITY_THRESHOLD = clampNumber(
	process.env.LONG_VIDEO_PRIOR_SIMILARITY_THRESHOLD ?? 0.38,
	0.18,
	0.9,
);
const PRIOR_LONG_VIDEO_NOVELTY_REWRITE_ATTEMPTS = Math.floor(
	clampNumber(process.env.LONG_VIDEO_PRIOR_NOVELTY_REWRITES ?? 2, 1, 4),
);

function noveltyTokenSet(text = "") {
	const tokens = tokenizeLabel(text || "").filter(
		(t) =>
			t.length >= 3 &&
			!TOPIC_STOP_WORDS.has(t) &&
			!GENERIC_TOPIC_TOKENS.has(t),
	);
	return new Set(tokens);
}

function tokenSetSimilarity(a, b) {
	const setA = a instanceof Set ? a : noveltyTokenSet(a);
	const setB = b instanceof Set ? b : noveltyTokenSet(b);
	if (!setA.size || !setB.size) return 0;
	let overlap = 0;
	for (const token of setA) {
		if (setB.has(token)) overlap += 1;
	}
	const union = new Set([...setA, ...setB]).size || 1;
	const jaccard = overlap / union;
	const overlapCoeff = overlap / Math.min(setA.size, setB.size);
	return Number((jaccard * 0.35 + overlapCoeff * 0.65).toFixed(4));
}

function priorVideoTopicText(video = {}) {
	return normalizeWhitespace(
		[
			video.seoTitle,
			video.topic,
			...(Array.isArray(video.topics) ? video.topics : []),
			video.longVideoMeta?.promptTopic || "",
			video.longVideoMeta?.noveltyPlan?.noveltyAngle || "",
		]
			.filter(Boolean)
			.join(" "),
	);
}

function compactScriptExcerpt(script = "", maxChars = 900) {
	const text = normalizeWhitespace(script || "");
	if (!text) return "";
	const sentences = splitSentences(text).filter((s) => countWords(s) >= 5);
	const selected = uniqueStrings(
		[
			...sentences.slice(0, 4),
			...sentences.filter((s) =>
				/\b(step|rent|grocer|bill|subscription|paycheck|budget|debt|saving|hope|system|pressure|inflation|automatic|withdrawal)\b/i.test(
					s,
				),
			),
			...sentences.slice(-3),
		],
		{ limit: 10 },
	);
	return compactEvidenceText(selected.join(" "), maxChars);
}

function summarizePriorVideoForNovelty(video = {}, similarity = 0) {
	const id = String(video._id || "").trim();
	const title = formatHumanTitle(video.seoTitle || video.topic || "Previous video", 90);
	const url = String(video.youtubeLink || video.outputUrl || "").trim();
	const createdAt = video.createdAt
		? dayjs(video.createdAt).format("YYYY-MM-DD")
		: "";
	return {
		id,
		title,
		url,
		createdAt,
		category: String(video.category || "").trim(),
		similarity: Number((Number(similarity) || 0).toFixed(3)),
		excerpt: compactScriptExcerpt(video.script || "", 1100),
	};
}

async function loadSimilarPriorLongVideos({
	userId,
	topics = [],
	promptText = "",
	categoryLabel = "",
	jobId,
}) {
	if (!userId) return [];
	const topicLine = (Array.isArray(topics) ? topics : [])
		.map((t) => t?.displayTopic || t?.topic || "")
		.filter(Boolean)
		.join(" ");
	const targetText = normalizeWhitespace(
		`${promptText || ""} ${topicLine} ${categoryLabel || ""}`,
	);
	const targetSet = noveltyTokenSet(targetText);
	if (!targetSet.size) return [];
	try {
		const docs = await Video.find({
			user: userId,
			isLongVideo: true,
			status: "SUCCEEDED",
			$or: [
				{ youtubeLink: { $exists: true, $ne: "" } },
				{ outputUrl: { $exists: true, $ne: "" } },
			],
		})
			.sort({ createdAt: -1 })
			.limit(PRIOR_LONG_VIDEO_LOOKBACK_LIMIT)
			.select({
				seoTitle: 1,
				topic: 1,
				topics: 1,
				script: 1,
				youtubeLink: 1,
				outputUrl: 1,
				category: 1,
				createdAt: 1,
				longVideoMeta: 1,
			})
			.lean();
		const scored = [];
		for (const doc of docs || []) {
			const candidateText = priorVideoTopicText(doc);
			const baseScore = tokenSetSimilarity(targetSet, noveltyTokenSet(candidateText));
			const categoryBoost =
				categoryLabel &&
				String(doc.category || "").toLowerCase() ===
					String(categoryLabel || "").toLowerCase()
					? 0.04
					: 0;
			const score = Math.min(1, baseScore + categoryBoost);
			if (score < PRIOR_LONG_VIDEO_SIMILARITY_THRESHOLD) continue;
			scored.push({ doc, score });
		}
		scored.sort((a, b) => b.score - a.score);
		const matches = scored
			.slice(0, PRIOR_LONG_VIDEO_MAX_MATCHES)
			.map(({ doc, score }) => summarizePriorVideoForNovelty(doc, score));
		if (matches.length) {
			logJob(jobId, "prior similar long videos found", {
				count: matches.length,
				matches: matches.map((v) => ({
					id: v.id,
					title: v.title,
					similarity: v.similarity,
					hasUrl: Boolean(v.url),
				})),
			});
		}
		return matches;
	} catch (e) {
		logJob(jobId, "prior similar long video lookup failed", {
			error: e.message,
		});
		return [];
	}
}

function fallbackPriorVideoNoveltyPlan({ priorVideos = [], topics = [] } = {}) {
	const primary = priorVideos[0] || {};
	const topicLabel =
		(Array.isArray(topics) ? topics[0]?.displayTopic || topics[0]?.topic : "") ||
		primary.title ||
		"this topic";
	const refs = priorVideos.map((v) => ({
		id: v.id,
		title: v.title,
		url: v.url,
		createdAt: v.createdAt,
		similarity: v.similarity,
	}));
	return {
		hasPriorVideos: refs.length > 0,
		requiredNewnessPct: 65,
		shouldMentionPrior: refs.length > 0,
		spokenReferenceLine: refs.length
			? `If you saw the earlier breakdown on ${shortTopicLabel(
					topicLabel,
					6,
				)}, this one goes deeper into what to do next.`
			: "",
		noveltyAngle:
			"Treat this as a follow-up with fresh examples, different sequencing, and more practical audience value.",
		avoidRepeating: [
			"Do not reuse the same payoff order or the same examples from the prior video.",
			"Do not repeat full sentences or stock bridges from the prior video.",
			"Do not make the same video with only minor wording changes.",
		],
		newAngles: [
			"Open with a sharper audience tension.",
			"Use different examples and visual scenes.",
			"Add more concrete diagnosis and action steps.",
			"End with a new practical takeaway instead of the same wrap-up.",
		],
		segmentPlan: [],
		visualDifferentiators: [
			"Use different household-money visuals than the previous video.",
			"Avoid reused thumbnail-like B-roll.",
		],
		descriptionReferenceNote:
			"Related previous video(s) are listed below because this episode is a follow-up.",
		priorVideos: refs,
	};
}

function normalizePriorVideoNoveltyPlan(plan = {}, fallback = {}) {
	const refs = Array.isArray(fallback.priorVideos) ? fallback.priorVideos : [];
	const normalized = {
		...fallback,
		...(plan && typeof plan === "object" ? plan : {}),
	};
	normalized.hasPriorVideos = Boolean(refs.length);
	normalized.requiredNewnessPct = Math.max(
		60,
		Math.min(80, Number(normalized.requiredNewnessPct) || 65),
	);
	normalized.shouldMentionPrior = refs.length
		? normalized.shouldMentionPrior !== false
		: false;
	normalized.spokenReferenceLine = sanitizeSegmentText(
		normalizeWhitespace(normalized.spokenReferenceLine || ""),
	);
	if (normalized.spokenReferenceLine && countWords(normalized.spokenReferenceLine) > 28) {
		normalized.spokenReferenceLine = trimSegmentToCap(
			normalized.spokenReferenceLine,
			28,
		);
	}
	normalized.priorVideos = refs;
	for (const key of [
		"avoidRepeating",
		"newAngles",
		"segmentPlan",
		"visualDifferentiators",
	]) {
		normalized[key] = uniqueStrings(
			(Array.isArray(normalized[key]) ? normalized[key] : [normalized[key]])
				.map((line) => normalizeWhitespace(line))
				.filter(Boolean),
			{ limit: key === "segmentPlan" ? 10 : 8 },
		);
	}
	normalized.noveltyAngle = normalizeWhitespace(normalized.noveltyAngle || "");
	normalized.descriptionReferenceNote = normalizeWhitespace(
		normalized.descriptionReferenceNote || fallback.descriptionReferenceNote || "",
	);
	return normalized;
}

async function buildPriorVideoNoveltyPlan({
	jobId,
	promptText = "",
	topics = [],
	topicContexts = [],
	categoryLabel = "",
	priorVideos = [],
}) {
	const fallback = fallbackPriorVideoNoveltyPlan({ priorVideos, topics });
	if (!priorVideos.length) return fallback;
	if (!process.env.CHATGPT_API_TOKEN) return fallback;
	const topicLine = (Array.isArray(topics) ? topics : [])
		.map((t) => t?.displayTopic || t?.topic || "")
		.filter(Boolean)
		.join(" | ");
	const contextLine = (Array.isArray(topicContexts) ? topicContexts : [])
		.map((tc) => {
			const items = Array.isArray(tc?.context) ? tc.context : [];
			return `${tc?.topic || ""}: ${items
				.map((item) =>
					typeof item === "string"
						? item
						: `${item?.title || ""} ${item?.snippet || ""}`,
				)
				.filter(Boolean)
				.slice(0, 4)
				.join(" | ")}`;
		})
		.join("\n");
	const priorBlock = priorVideos
		.map(
			(v, i) => `Previous video ${i + 1}
Title: ${v.title}
Date: ${v.createdAt || "(unknown)"}
URL: ${v.url || "(local/no public URL)"}
Similarity: ${v.similarity}
Script excerpt: ${v.excerpt || "(no script excerpt)"}`,
		)
		.join("\n\n");
	const prompt = `
You are planning a new long-form YouTube video as a professional content strategist.

The user may send the same topic more than once. The goal is NOT to block the topic.
The goal is to make the new video feel like a fresh follow-up: at least 60-70% new value, examples, structure, wording, and visual direction, while preserving any mandatory user lines.

New requested topic(s): ${topicLine || "(none)"}
Category: ${categoryLabel || "General"}
Frontend prompt:
${promptText || "(none)"}

Fresh context:
${contextLine || "(none)"}

Already-published similar videos:
${priorBlock}

Return JSON only:
{
  "requiredNewnessPct": 65,
  "shouldMentionPrior": true,
  "spokenReferenceLine": "one natural presenter line, max 28 words, that references the earlier video without sounding repetitive",
  "noveltyAngle": "one sentence describing the fresh angle for this new video",
  "avoidRepeating": ["specific old angles, examples, structure, or phrases to avoid"],
  "newAngles": ["fresh angles/examples/sections to include"],
  "segmentPlan": ["short plan beats for this new video"],
  "visualDifferentiators": ["how visuals should feel different from prior videos"],
  "descriptionReferenceNote": "short note to put above related previous-video links"
}
`.trim();
	try {
		const resp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: prompt }],
		});
		const parsed = parseJsonFlexible(resp?.choices?.[0]?.message?.content || "");
		const plan = normalizePriorVideoNoveltyPlan(parsed, fallback);
		logJob(jobId, "prior video novelty plan", {
			hasPriorVideos: plan.hasPriorVideos,
			requiredNewnessPct: plan.requiredNewnessPct,
			shouldMentionPrior: plan.shouldMentionPrior,
			noveltyAngle: plan.noveltyAngle,
			avoidRepeating: plan.avoidRepeating?.slice(0, 4),
			newAngles: plan.newAngles?.slice(0, 4),
		});
		return plan;
	} catch (e) {
		logJob(jobId, "prior video novelty plan failed; using fallback", {
			error: e.message,
		});
		return fallback;
	}
}

function buildPriorVideoScriptGuide(priorVideoPlan = null) {
	if (!priorVideoPlan?.hasPriorVideos) {
		return "Prior-video novelty plan:\n- No similar published long videos found for this user/topic.";
	}
	const refs = Array.isArray(priorVideoPlan.priorVideos)
		? priorVideoPlan.priorVideos
		: [];
	const lines = [
		"Prior-video novelty plan:",
		`- Similar published long videos exist. This new script must feel like a fresh follow-up with at least ${priorVideoPlan.requiredNewnessPct || 65}% new content value.`,
		"- Preserve any required user-supplied lines, but change the structure, examples, explanations, visual cues, and payoff enough that returning viewers do not feel they are watching the same video.",
	];
	if (priorVideoPlan.shouldMentionPrior && priorVideoPlan.spokenReferenceLine) {
		lines.push(
			`- Mention the earlier video ONCE, naturally, preferably in segment 1 after the hook: "${priorVideoPlan.spokenReferenceLine}"`,
		);
	}
	if (priorVideoPlan.noveltyAngle)
		lines.push(`- Fresh angle: ${priorVideoPlan.noveltyAngle}`);
	if (priorVideoPlan.avoidRepeating?.length) {
		lines.push("- Avoid repeating:");
		for (const item of priorVideoPlan.avoidRepeating.slice(0, 6)) {
			lines.push(`  - ${item}`);
		}
	}
	if (priorVideoPlan.newAngles?.length) {
		lines.push("- Include fresh material such as:");
		for (const item of priorVideoPlan.newAngles.slice(0, 8)) {
			lines.push(`  - ${item}`);
		}
	}
	if (priorVideoPlan.segmentPlan?.length) {
		lines.push("- Follow-up content plan:");
		for (const item of priorVideoPlan.segmentPlan.slice(0, 10)) {
			lines.push(`  - ${item}`);
		}
	}
	if (priorVideoPlan.visualDifferentiators?.length) {
		lines.push("- Visual differentiation:");
		for (const item of priorVideoPlan.visualDifferentiators.slice(0, 6)) {
			lines.push(`  - ${item}`);
		}
	}
	if (refs.length) {
		lines.push("- Previous references:");
		for (const ref of refs.slice(0, 3)) {
			lines.push(
				`  - ${ref.title}${ref.createdAt ? ` (${ref.createdAt})` : ""}${
					ref.url ? ` - ${ref.url}` : ""
				}`,
			);
		}
	}
	return lines.join("\n");
}

function applyPriorVideoReferenceToScript({
	script = {},
	priorVideoPlan = null,
	wordCaps = [],
} = {}) {
	if (
		!priorVideoPlan?.hasPriorVideos ||
		!priorVideoPlan.shouldMentionPrior ||
		!priorVideoPlan.spokenReferenceLine ||
		!Array.isArray(script?.segments) ||
		!script.segments.length
	) {
		return script;
	}
	const line = sanitizeSegmentText(priorVideoPlan.spokenReferenceLine);
	if (!line) return script;
	const fullKey = normalizeQaText(script.segments.map((s) => s.text || "").join(" "));
	if (fullKey.includes(normalizeQaText(line))) return script;
	const segments = script.segments.map((s) => ({ ...s }));
	const targetIdx = segments.length > 1 ? 1 : 0;
	const current = sanitizeSegmentText(segments[targetIdx].text || "");
	const cap = Math.max(
		Number(wordCaps[segments[targetIdx].index] || wordCaps[targetIdx] || 0) || 0,
		countWords(current) + countWords(line),
		24,
	);
	const combined =
		targetIdx === 0
			? sanitizeSegmentText(`${current} ${line}`)
			: sanitizeSegmentText(`${line} ${current}`);
	segments[targetIdx].text = trimSegmentToCap(combined, cap + 10);
	segments[targetIdx].expression = segments[targetIdx].expression || "thoughtful";
	return { ...script, segments };
}

function estimatePriorNovelty(script = {}, priorVideoPlan = null) {
	if (!priorVideoPlan?.hasPriorVideos || !Array.isArray(priorVideoPlan.priorVideos))
		return null;
	const scriptText = buildScriptLogText(script);
	const scriptSet = noveltyTokenSet(scriptText);
	if (!scriptSet.size) return null;
	let maxSimilarity = 0;
	let closest = null;
	for (const prior of priorVideoPlan.priorVideos) {
		const score = tokenSetSimilarity(scriptSet, noveltyTokenSet(prior.excerpt || prior.title));
		if (score > maxSimilarity) {
			maxSimilarity = score;
			closest = prior;
		}
	}
	return {
		noveltyPct: Number(Math.max(0, (1 - maxSimilarity) * 100).toFixed(1)),
		maxSimilarity: Number(maxSimilarity.toFixed(3)),
		closestTitle: closest?.title || "",
	};
}

function buildPriorVideoDescriptionBlock(priorVideoPlan = null) {
	if (!priorVideoPlan?.hasPriorVideos) return "";
	const refs = (Array.isArray(priorVideoPlan.priorVideos)
		? priorVideoPlan.priorVideos
		: []
	).filter((ref) => ref?.url);
	if (!refs.length) return "";
	const note =
		priorVideoPlan.descriptionReferenceNote ||
		"Related previous video(s) mentioned in this follow-up:";
	const lines = [note];
	for (const ref of refs.slice(0, 3)) {
		lines.push(`- ${ref.title}: ${ref.url}`);
	}
	return lines.join("\n");
}

function compactPriorVideoPlanForMeta(priorVideoPlan = null) {
	if (!priorVideoPlan?.hasPriorVideos) return null;
	return {
		hasPriorVideos: true,
		requiredNewnessPct: priorVideoPlan.requiredNewnessPct || 65,
		shouldMentionPrior: Boolean(priorVideoPlan.shouldMentionPrior),
		spokenReferenceLine: priorVideoPlan.spokenReferenceLine || "",
		noveltyAngle: priorVideoPlan.noveltyAngle || "",
		avoidRepeating: Array.isArray(priorVideoPlan.avoidRepeating)
			? priorVideoPlan.avoidRepeating.slice(0, 8)
			: [],
		newAngles: Array.isArray(priorVideoPlan.newAngles)
			? priorVideoPlan.newAngles.slice(0, 8)
			: [],
		visualDifferentiators: Array.isArray(priorVideoPlan.visualDifferentiators)
			? priorVideoPlan.visualDifferentiators.slice(0, 8)
			: [],
		priorVideos: Array.isArray(priorVideoPlan.priorVideos)
			? priorVideoPlan.priorVideos.slice(0, 5).map((ref) => ({
					id: ref.id || "",
					title: ref.title || "",
					url: ref.url || "",
					createdAt: ref.createdAt || "",
					similarity: Number(ref.similarity) || 0,
				}))
			: [],
	};
}

function applyLongVideoScriptGuards({
	script,
	topics = [],
	topicContexts = [],
	wordCaps = [],
	priorVideoPlan = null,
	categoryLabel = "",
	mood = "neutral",
	includeOutro = true,
	outroText = "",
	injectOpeningLine = true,
} = {}) {
	let guarded = sanitizeScriptVisualCueLeaks(script, topics);
	guarded = applyPromptBriefToScript({
		script: guarded,
		topics,
		wordCaps,
		injectOpeningLine,
	});
	guarded = applyPriorVideoReferenceToScript({
		script: guarded,
		priorVideoPlan,
		wordCaps,
	});
	if (includeOutro) {
		guarded = removeContentCtasForSeparateOutro({
			script: guarded,
			topics,
			wordCaps,
			categoryLabel,
			mood,
			outroText,
		});
	}
	guarded = repairDirectAnswerOpening({
		script: guarded,
		topics,
		topicContexts,
		wordCaps,
	}).script;
	return guarded;
}

const DEFAULT_LONG_VIDEO_CONTROLLER_CONFIG = Object.freeze({
	controllerLabel: "long-video",
	statusPathBase: "/api/long-video",
	presenterAssetUrl: DEFAULT_PRESENTER_ASSET_URL,
	allowPresenterAssetOverride: false,
	enableHeyGenPresenterMotion: true,
	enableWardrobeEdit: ENABLE_WARDROBE_EDIT,
	disableYouTubeUpload: false,
});

function normalizeLongVideoControllerConfig(config = {}) {
	return {
		...DEFAULT_LONG_VIDEO_CONTROLLER_CONFIG,
		...(config && typeof config === "object" ? config : {}),
	};
}

function resolveRequestedPresenterAsset(body = {}, controllerConfig = {}) {
	const cfg = normalizeLongVideoControllerConfig(controllerConfig);
	const defaultAssetUrl = String(
		cfg.presenterAssetUrl || DEFAULT_PRESENTER_ASSET_URL,
	).trim();
	if (!cfg.allowPresenterAssetOverride) return defaultAssetUrl;

	const requested = String(
		body.presenterAssetUrl ||
			body.presenterImageUrl ||
			body.presenterVideoUrl ||
			"",
	).trim();
	return requested || defaultAssetUrl;
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

const PROMPT_CONTROL_TOKENS = new Set([
	"recommend",
	"recommendation",
	"suggest",
	"suggestion",
	"topic",
	"topics",
	"idea",
	"ideas",
	"video",
	"videos",
	"content",
	"script",
	"seo",
	"metadata",
	"title",
	"titles",
	"youtube",
	"friendly",
	"long",
	"short",
	"create",
	"make",
	"generate",
	"write",
	"produce",
	"build",
	"craft",
	"give",
	"show",
	"tell",
	"add",
	"need",
	"want",
	"please",
	"me",
	"my",
	"your",
	"you",
	"us",
	"we",
	"our",
	"someone",
	"anyone",
	"can",
	"could",
	"would",
	"should",
	"pick",
	"choose",
	"surprise",
	"anything",
	"something",
	"random",
	"best",
	"trending",
	"latest",
	"update",
	"updates",
	"news",
	"now",
	"today",
	"recommendations",
	"suggestions",
]);

const PROMPT_INTENT_TOKENS = new Set([
	"attract",
	"audience",
	"click",
	"clickable",
	"controversial",
	"debate",
	"engaging",
	"hook",
	"hooks",
	"interesting",
	"retention",
	"spicy",
	"thumbnail",
	"trend",
	"trending",
	"viral",
	"view",
	"views",
	"watch",
	"watchable",
]);

const TOP_LIST_NUMBER_WORDS = Object.freeze({
	one: 1,
	two: 2,
	three: 3,
	four: 4,
	five: 5,
	six: 6,
	seven: 7,
	eight: 8,
	nine: 9,
	ten: 10,
});

const PROMPT_QUESTION_TOKENS = new Set([
	"what",
	"who",
	"why",
	"how",
	"when",
	"where",
	"which",
]);

const PROMPT_RECOMMENDATION_PATTERNS = [
	/\b(recommend|suggest|pick|choose|surprise)\b/i,
	/\b(anything|something)\b/i,
	/\bwhat('s| is)\s+(trending|hot|popular|new)\b/i,
	/\bwhat\s+topic\b/i,
	/\btrending\s+topic\b/i,
	/\btopic\s+idea\b/i,
];

const PROMPT_SPLIT_RE = /[|;]+/;
const PROMPT_BRIEF_LABEL_RE =
	/^\s*(title|headline|seo\s*title(?:\s*request)?|title\s*request|reference\s*title(?:\s*idea)?|opening\s*line|hook|why\s+it\s+can\s+work|why\s+it\s+works|angle|tone|audience|must\s+include|avoid|ending|end|outro|sources?|stats?|facts?|thumbnail(?:\s*(?:text|idea))?|visuals?|b[-\s]?roll|feed\s*(?:images?|videos?)?|(?:\d+\s*[-\s]*)?minute\s*structure|structure|outline)\s*:\s*(.*)$/i;
const PROMPT_BRIEF_SINGLE_LABEL_RE =
	/\b(title|headline|seo\s*title(?:\s*request)?|title\s*request|reference\s*title(?:\s*idea)?|opening\s*line|hook|why\s+it\s+can\s+work|why\s+it\s+works|audience|must\s+include|avoid|ending|stats?|facts?|visuals?|b[-\s]?roll|thumbnail(?:\s*(?:text|idea))?|(?:\d+\s*[-\s]*)?minute\s*structure|structure|outline)\s*:/i;
const PROMPT_BRIEF_STRUCTURE_LINE_RE =
	/^\s*(?:minute|part|section|beat)\s*\d{1,2}\s*:\s*(.+)$/i;
const PROMPT_BRIEF_INLINE_LABEL_RE =
	/\s+(?=(?:title|headline|seo\s*title(?:\s*request)?|title\s*request|reference\s*title(?:\s*idea)?|opening\s*line|hook|why\s+it\s+can\s+work|why\s+it\s+works|angle|tone|audience|must\s+include|avoid|ending|end|outro|sources?|stats?|facts?|thumbnail(?:\s*(?:text|idea))?|visuals?|b[-\s]?roll|feed\s*(?:images?|videos?)?|(?:\d+\s*[-\s]*)?minute\s*structure|structure|outline|minute\s*\d{1,2}|part\s*\d{1,2}|section\s*\d{1,2}|beat\s*\d{1,2})\s*:)/gi;

function parseTopListNumberToken(token = "") {
	const raw = String(token || "")
		.toLowerCase()
		.trim();
	if (!raw) return 0;
	if (/^\d{1,2}$/.test(raw)) return Number(raw);
	return TOP_LIST_NUMBER_WORDS[raw] || 0;
}

function stripCreatorIntentForTopic(text = "") {
	let cleaned = String(text || "").trim();
	if (!cleaned) return cleaned;

	cleaned = cleaned
		.replace(/\b(?:that|which)\s+(?:would|will|can|could|should)\s+/gi, " ")
		.replace(
			/\b(?:attract|gain|get|drive|bring|pull)\s+(?:thousands|millions|lots|many|more|tons)\s+of\s+views?\b/gi,
			" ",
		)
		.replace(/\b(?:attract|gain|get|drive|bring|pull)\s+views?\b/gi, " ")
		.replace(/\b(?:go|get|make it|become)\s+viral\b/gi, " ")
		.replace(/\b(?:high|better)\s+retention\b/gi, " ")
		.replace(/\b(?:for|with)\s+(?:youtube|views?|retention|engagement)\b/gi, " ")
		.replace(/\s+/g, " ")
		.trim();

	const bridge =
		cleaned.match(
			/^(.*?)\b(?:about|on|regarding|covering|focused\s+on|based\s+on)\b\s+(.+)$/i,
		) || null;
	if (bridge) {
		const before = String(bridge[1] || "").toLowerCase();
		const after = String(bridge[2] || "").trim();
		const beforeTokens = tokenizeLabel(before);
		const looksLikeIntent =
			beforeTokens.length === 0 ||
			beforeTokens.some(
				(t) => PROMPT_CONTROL_TOKENS.has(t) || PROMPT_INTENT_TOKENS.has(t),
			);
		if (after && looksLikeIntent) cleaned = after;
	}

	return cleaned
		.replace(
			/^(?:controversial|spicy|viral|engaging|interesting|clickable|high\s+retention)\s+(?:topic|video|content)?\s*(?:about|on)?\s*/i,
			"",
		)
		.replace(/\s+/g, " ")
		.trim();
}

function stripPromptHeadingNumber(text = "") {
	return String(text || "")
		.trim()
		.replace(/^\s*(?:topic\s*)?(?:#?\d{1,2}|[A-Z])[\).:-]\s*/i, "")
		.trim();
}

function looksLikePromptStructureLabelOnly(text = "") {
	const raw = cleanTopicLabel(text).replace(/:+$/g, "").trim();
	return /^(?:minute|part|section|beat)\s*\d{1,2}$/i.test(raw);
}

function stripOuterQuotes(text = "") {
	let raw = String(text || "").trim();
	if (/^\?[\s\S]*\?$/.test(raw) && raw.length > 2) {
		raw = raw.slice(1, -1).trim();
	}
	return raw
		.replace(/^["'\u201c\u201d\u2018\u2019]+|["'\u201c\u201d\u2018\u2019]+$/g, "")
		.trim();
}

function isSeoTitleRequestText(text = "") {
	const raw = normalizeWhitespace(text).toLowerCase();
	if (!raw) return false;
	const hasTitle = /\btitles?\b/.test(raw);
	const hasSeoIntent =
		/\bseo(?:[-\s]?friendly)?\b/.test(raw) ||
		/\bsearch(?:able| optimized| friendly)?\b/.test(raw) ||
		/\byoutube\b/.test(raw);
	const hasAction =
		/\b(add|create|generate|write|choose|pick|make|give|use|find|recommend)\b/.test(
			raw,
		) || /\bbest\b/.test(raw);
	return hasTitle && hasSeoIntent && hasAction;
}

function stripSeoTitleRequestText(text = "") {
	let cleaned = String(text || "");
	if (!cleaned.trim()) return "";
	const requestFragment =
		/(^|[\n.?!]\s*)(?:please\s+)?(?:add|create|generate|write|choose|pick|make|give|use|find|recommend)\b[^.?!\n]{0,160}\b(?:seo(?:[-\s]?friendly)?|search(?:able| optimized| friendly)?|youtube)\b[^.?!\n]{0,100}\btitles?\b[^.?!\n]*(?=$|[\n.?!])/gi;
	const titleFirstFragment =
		/(^|[\n.?!]\s*)(?:please\s+)?(?:add|create|generate|write|choose|pick|make|give|use|find|recommend)\b[^.?!\n]{0,100}\btitles?\b[^.?!\n]{0,160}\b(?:seo(?:[-\s]?friendly)?|search(?:able| optimized| friendly)?|youtube)\b[^.?!\n]*(?=$|[\n.?!])/gi;
	cleaned = cleaned.replace(requestFragment, "$1");
	cleaned = cleaned.replace(titleFirstFragment, "$1");
	return cleaned.replace(/\s+/g, " ").trim();
}

function isLikelyPromptTitleLine(line = "") {
	const raw = stripOuterQuotes(stripPromptHeadingNumber(line));
	if (!raw || isSeoTitleRequestText(raw)) return false;
	if (PROMPT_BRIEF_STRUCTURE_LINE_RE.test(raw)) return false;
	if (looksLikePromptStructureLabelOnly(raw)) return false;
	const words = countWords(raw);
	if (words < 2 || words > 16) return false;
	if (/^(please|i\s+(?:want|need|would)|can\s+you|could\s+you|add|create|generate|write|make|give|use|find|recommend)\b/i.test(raw))
		return false;
	const tokens = tokenizeLabel(raw);
	const controlCount = tokens.filter((t) => PROMPT_CONTROL_TOKENS.has(t)).length;
	return controlCount <= Math.max(1, Math.floor(tokens.length * 0.35));
}

function normalizePromptThumbnailText(text = "") {
	const raw = stripOuterQuotes(text)
		.replace(/\s+/g, " ")
		.trim();
	if (!raw) return "";
	const normalized = normalizeBadgeText(raw);
	if (normalized) return normalized;
	return raw.replace(/[?]+/g, "").trim().toUpperCase().slice(0, 18);
}

function normalizePromptBriefKey(raw = "") {
	return String(raw || "")
		.toLowerCase()
		.replace(/\s+/g, "_")
		.replace(/-/g, "_")
		.trim();
}

function normalizePromptBriefInput(text = "") {
	return String(text || "")
		.replace(/\\r\\n|\\n|\\r/g, "\n")
		.replace(/\r\n?/g, "\n")
		.replace(PROMPT_BRIEF_INLINE_LABEL_RE, "\n")
		.replace(/\n{3,}/g, "\n\n")
		.trim();
}

function isPromptFactStatLine(line = "") {
	const raw = normalizeWhitespace(line);
	if (!raw) return false;
	if (PROMPT_BRIEF_STRUCTURE_LINE_RE.test(raw)) return false;
	const labelMatch = raw.match(PROMPT_BRIEF_LABEL_RE);
	if (labelMatch) {
		const key = normalizePromptBriefKey(labelMatch[1]);
		if (
			/^(title|headline|seo_title|seo_title_request|opening_line|hook|thumbnail|thumbnail_text|thumbnail_idea|structure|outline|audience|tone|avoid|ending|end|outro)$/.test(
				key,
			) ||
			/minute_structure$/i.test(key)
		) {
			return false;
		}
	}
	if (
		/^\s*(?:topic\s*)?(?:#?\d{1,2}|[A-Z])[\).:-]\s+/.test(raw) &&
		!/%|\$|\b(?:reported|according to|survey|study|data|outlook|census|bls|ftc|fed|federal reserve)\b/i.test(
			raw,
		)
	) {
		return false;
	}
	return /%|\$|\b\d+(?:\.\d+)?\s*(?:percent|million|billion|trillion|people|workers|households|americans)\b|\b(?:reported|according to|ftc|census|bls|fed|federal reserve|labor department|commerce department|stanford|survey|study|data|outlook)\b/i.test(
		raw,
	);
}

function extractPromptQuotedFragments(text = "") {
	const raw = String(text || "");
	const out = [];
	const re = /["\u201c]([^"\u201d]{8,260})["\u201d]|[\u2018]([^\u2019]{8,260})[\u2019]/g;
	let match;
	while ((match = re.exec(raw))) {
		const value = stripOuterQuotes(match[1] || match[2] || "");
		if (value) out.push(value);
	}
	return out;
}

function normalizePromptMustIncludeLines(text = "") {
	const raw = normalizeWhitespace(text);
	if (!raw) return [];
	const quoted = extractPromptQuotedFragments(raw);
	const candidates = quoted.length
		? quoted
		: splitSentences(
				raw
					.replace(
						/^(?:make\s+sure\s+to\s+)?(?:must\s+)?include(?:\s+the)?(?:\s+exact|\s+memorable|\s+important)?(?:\s+line|\s+sentence)?:?\s*/i,
						"",
					)
					.replace(/\binclude\s+the\s+(?:exact|memorable|important)\s+(?:line|sentence):\s*/i, ""),
			);
	return uniqueStrings(
		candidates
			.map((line) => sanitizeSegmentText(stripOuterQuotes(line)))
			.filter((line) => countWords(line) >= 4 && countWords(line) <= 42),
		{ limit: 4 },
	);
}

function normalizePromptOutroLine(text = "") {
	let raw = stripOuterQuotes(normalizeWhitespace(text));
	if (!raw) return "";
	const quoted = extractPromptQuotedFragments(raw);
	if (quoted.length) raw = quoted[quoted.length - 1];
	raw = sanitizeIntroOutroLine(raw)
		.replace(/\blet\s+know\s+know\b/gi, "let me know")
		.replace(/\blet\s+me\s+know\s+what\s+do\s+you\s+think\b/gi, "tell me what you think")
		.trim();
	if (!raw) return "";
	if (!/[.!?]$/.test(raw)) raw = `${raw}.`;
	if (countWords(raw) <= 24) return raw;
	const question = splitSentences(raw).find((sentence) => /\?/.test(sentence));
	if (question && countWords(question) <= 16) {
		return sanitizeIntroOutroLine(
			`Please like, subscribe, and tell me: ${question
				.replace(/^\s*(?:please\s+)?(?:like|subscribe|comment|tell\s+me)\b[^:?]*[:?]?\s*/i, "")
				.trim()}`,
		);
	}
	return sanitizeIntroOutroLine(trimSegmentToCap(raw, 24));
}

function splitPromptVisualHintList(text = "") {
	const raw = normalizeWhitespace(text);
	if (!raw) return [];
	const cleaned = raw
		.replace(/^(?:visuals?|b-?roll|feed images?|image ideas?)\s*:\s*/i, "")
		.replace(/\b(?:and|plus)\b/gi, ",");
	return uniqueStrings(
		cleaned
			.split(/[;,|/]+/)
			.map((part) =>
				cleanTopicLabel(
					part
						.replace(/\b(?:visuals?|shots?|scenes?|images?|photos?)\b/gi, "")
						.trim(),
				),
			)
			.filter((part) => part && countWords(part) >= 2 && countWords(part) <= 8),
		{ limit: 10 },
	);
}

function parseStructuredPromptBrief(promptText = "") {
	const rawOriginal = String(promptText || "").trim();
	const raw = normalizePromptBriefInput(rawOriginal);
	if (!raw) {
		return {
			isStructured: false,
			primaryTopic: "",
			title: "",
			titleLocked: false,
			openingLine: "",
			briefLines: [],
			factLines: [],
			searchHints: [],
			imageHints: [],
			seoTitleInstructions: [],
			wantsSeoTitle: false,
			thumbnailText: "",
			structureLines: [],
			mustIncludeLines: [],
			outroLine: "",
		};
	}

	const lines = raw
		.split(/\r?\n/)
		.map((line) => line.trim())
		.filter(Boolean);
	const fields = {};
	const unlabeled = [];
	const factLines = [];
	const seoTitleInstructions = [];
	const structureLines = [];
	let pendingKey = "";
	const appendFieldValue = (keyRaw, valueRaw, originalLine = "") => {
		const key = normalizePromptBriefKey(keyRaw);
		const value = String(valueRaw || "").trim();
		if (!key || !value) {
			pendingKey = key || pendingKey;
			return;
		}
		if (
			key === "seo_title" ||
			key === "seo_title_request" ||
			key === "title_request" ||
			key === "reference_title" ||
			key === "reference_title_idea"
		) {
			seoTitleInstructions.push(value || originalLine);
			return;
		}
		if (
			(key === "title" || key === "headline") &&
			isSeoTitleRequestText(value)
		) {
			seoTitleInstructions.push(value || originalLine);
			return;
		}
		if (
			key === "thumbnail" ||
			key === "thumbnail_text" ||
			key === "thumbnail_idea"
		) {
			fields.thumbnail_text = fields.thumbnail_text
				? `${fields.thumbnail_text} ${value}`.trim()
				: value;
			return;
		}
		if (
			key === "structure" ||
			key === "outline" ||
			/minute_structure$/i.test(key)
		) {
			structureLines.push(value);
			return;
		}
		fields[key] = fields[key] ? `${fields[key]} ${value}`.trim() : value;
		if (isPromptFactStatLine(value)) {
			factLines.push(value);
		}
	};
	for (const originalLine of lines) {
		let line = originalLine;
		const lineWithoutSeoInstruction = stripSeoTitleRequestText(line);
		if (
			isSeoTitleRequestText(line) &&
			lineWithoutSeoInstruction !== normalizeWhitespace(line)
		) {
			seoTitleInstructions.push(originalLine);
			line = lineWithoutSeoInstruction;
			if (!line) continue;
		}
		const match = line.match(PROMPT_BRIEF_LABEL_RE);
		if (match) {
			const key = normalizePromptBriefKey(match[1]);
			const value = String(match[2] || "").trim();
			pendingKey = value ? "" : key;
			if (value) appendFieldValue(key, value, originalLine);
			continue;
		}
		if (pendingKey) {
			appendFieldValue(pendingKey, line, originalLine);
			pendingKey = "";
			continue;
		}
		if (isSeoTitleRequestText(line)) {
			seoTitleInstructions.push(originalLine);
			continue;
		}
		const structureMatch = line.match(PROMPT_BRIEF_STRUCTURE_LINE_RE);
		if (structureMatch) {
			structureLines.push(line);
			continue;
		}
		unlabeled.push(line);
		if (isPromptFactStatLine(line)) {
			factLines.push(line);
		}
	}

	const explicitTitle = stripOuterQuotes(fields.title || fields.headline || "");
	const heading =
		unlabeled.find((line) => isLikelyPromptTitleLine(line)) || "";
	const promptHeading = stripOuterQuotes(stripPromptHeadingNumber(heading));
	const inferredTitle = promptHeading;
	const title = stripOuterQuotes(explicitTitle || inferredTitle);
	const wantsSeoTitle = seoTitleInstructions.length > 0;
	const titleLocked = Boolean(title && !wantsSeoTitle && (explicitTitle || inferredTitle));
	const openingLine = stripOuterQuotes(fields.opening_line || fields.hook || "");
	const thumbnailText = normalizePromptThumbnailText(fields.thumbnail_text || "");
	const mustIncludeLines = normalizePromptMustIncludeLines(
		fields.must_include || "",
	);
	const outroLine = normalizePromptOutroLine(fields.outro || "");
	const primaryTopicSeed =
		wantsSeoTitle && promptHeading ? promptHeading : title || promptHeading;
	const primaryTopic =
		cleanTopicLabel(primaryTopicSeed || stripPromptHeadingNumber(heading)) ||
		normalizePromptTopic(stripSeoTitleRequestText(raw)).slice(0, 120);
	const briefLines = uniqueStrings(
		[
			...(titleLocked && title ? [`Requested title: ${title}`] : []),
			...(!titleLocked && title ? [`Reference title/topic: ${title}`] : []),
			...(promptHeading && promptHeading !== title
				? [`Prompt topic heading: ${promptHeading}`]
				: []),
			...seoTitleInstructions.map(
				(line) => `SEO title instruction: ${line}`,
			),
			...(thumbnailText ? [`Thumbnail text: ${thumbnailText}`] : []),
			...(openingLine ? [`Requested opening line: ${openingLine}`] : []),
			...structureLines.map((line) => `Requested structure: ${line}`),
			...(fields.why_it_can_work
				? [`Why it can work: ${fields.why_it_can_work}`]
				: []),
			...(fields.why_it_works ? [`Why it works: ${fields.why_it_works}`] : []),
			...(fields.angle ? [`Angle: ${fields.angle}`] : []),
			...(fields.tone ? [`Tone: ${fields.tone}`] : []),
			...(fields.audience ? [`Audience: ${fields.audience}`] : []),
			...(fields.must_include ? [`Must include: ${fields.must_include}`] : []),
			...(fields.avoid ? [`Avoid: ${fields.avoid}`] : []),
			...(fields.ending || fields.end
				? [`Ending: ${fields.ending || fields.end}`]
				: []),
			...(fields.outro ? [`Outro: ${fields.outro}`] : []),
			...(fields.visuals ? [`Visuals: ${fields.visuals}`] : []),
			...(fields.b_roll ? [`B-roll: ${fields.b_roll}`] : []),
			...factLines.map((line) => `Fact/stat to verify: ${line}`),
		],
		{ limit: 18 },
	);
	const searchHints = uniqueStrings(
		[
			primaryTopic,
			title,
			promptHeading,
			...factLines,
			...(fields.sources ? [fields.sources] : []),
			...(fields.stats ? [fields.stats] : []),
			...(fields.facts ? [fields.facts] : []),
		].filter(Boolean),
		{ limit: 10 },
	);
	const imageHints = uniqueStrings(
		[
			primaryTopic ? `${primaryTopic} news photo` : "",
			primaryTopic ? `${primaryTopic} public warning` : "",
			primaryTopic ? `${primaryTopic} chart` : "",
			promptHeading ? `${promptHeading} news photo` : "",
			...(thumbnailText ? [`${primaryTopic} ${thumbnailText}`] : []),
			...(fields.visuals ? splitPromptVisualHintList(fields.visuals) : []),
			...(fields.feed_images
				? splitPromptVisualHintList(fields.feed_images)
				: []),
			...(fields.feed_videos
				? splitPromptVisualHintList(fields.feed_videos)
				: []),
		].filter(Boolean),
		{ limit: 10 },
	);
	const isStructured =
		PROMPT_BRIEF_SINGLE_LABEL_RE.test(raw) ||
		briefLines.length >= 2 ||
		(lines.length >= 3 && factLines.length > 0);
	return {
		isStructured,
		primaryTopic,
		title,
		titleLocked,
		openingLine,
		briefLines,
		factLines,
		searchHints,
		imageHints,
		seoTitleInstructions: uniqueStrings(seoTitleInstructions, { limit: 4 }),
		wantsSeoTitle,
		thumbnailText,
		structureLines: uniqueStrings(structureLines, { limit: 12 }),
		mustIncludeLines,
		outroLine,
		raw: rawOriginal.slice(0, 5000),
	};
}

function shouldTreatPromptAsSingleBrief(promptText = "") {
	const raw = String(promptText || "").trim();
	if (!raw) return false;
	const normalized = normalizePromptBriefInput(raw);
	if (PROMPT_BRIEF_SINGLE_LABEL_RE.test(normalized)) return true;
	if (raw.length >= 220 && /[.?!]\s+/.test(raw)) return true;
	const lines = normalized.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
	return lines.length >= 3 && lines.some((line) => /\d|%|\$/.test(line));
}

function detectPromptAngle(text = "") {
	const raw = String(text || "").toLowerCase();
	const angles = [];
	if (/\bcontrovers/i.test(raw))
		angles.push("controversial but fair; surface the real debate");
	if (/\bspicy|hot\s+take|provocative/i.test(raw))
		angles.push("sharper creator framing without unsupported claims");
	if (/\bviral|views?|retention|engag/i.test(raw))
		angles.push("strong hook and high-retention structure");
	if (/\blatest|update|updates|current|now|today/i.test(raw))
		angles.push("latest updates and current context");
	if (/\bemotional|suffering|struggling|feel their|care about|human\b/i.test(raw))
		angles.push("empathetic human framing without exaggeration");
	if (/\bhope|hopeful|optimistic|better|recovery|resilience\b/i.test(raw))
		angles.push("end with realistic hope grounded in facts");
	if (/\bstats?|statistics|data|charts?|numbers|reported\b/i.test(raw))
		angles.push("support key claims with stats and source attribution");
	return uniqueStrings(angles, { limit: 4 }).join("; ");
}

const DIRECT_ANSWER_QUERY_PATTERNS = [
	{
		type: "winner",
		answerLabel: "winner or result",
		pattern:
			/\b(?:who\s+(?:won|wins|is\s+the\s+winner|was\s+the\s+winner)|winner\s+of|who\s+took\s+home|who\s+came\s+first)\b/i,
	},
	{
		type: "elimination",
		answerLabel: "elimination or voted-off result",
		pattern:
			/\b(?:who\s+(?:was\s+)?(?:eliminated|voted\s+off|sent\s+home)|eliminated\s+from|voted\s+off)\b/i,
	},
	{
		type: "score",
		answerLabel: "score or final result",
		pattern:
			/\b(?:what\s+(?:was|is)\s+the\s+score|final\s+score|who\s+beat\s+who|who\s+defeated\s+who)\b/i,
	},
	{
		type: "status",
		answerLabel: "current status",
		pattern:
			/\b(?:is|are|was|were)\s+[^?]{2,80}\b(?:cancelled|canceled|renewed|dead|alive|available|confirmed|real)\b/i,
	},
	{
		type: "date",
		answerLabel: "date or timing",
		pattern:
			/\b(?:when\s+(?:is|was|does|did|will)|release\s+date|premiere\s+date|launch\s+date)\b/i,
	},
	{
		type: "price",
		answerLabel: "price or cost",
		pattern:
			/\b(?:how\s+much\s+(?:is|are|does|do|will)|price\s+of|cost\s+of|what\s+does\s+.+\s+cost)\b/i,
	},
	{
		type: "identity",
		answerLabel: "identity",
		pattern:
			/\b(?:who\s+(?:is|are|was|were)|what\s+(?:is|are|was|were)\s+the\s+name)\b/i,
	},
];

function extractDirectAnswerSubject(raw = "", type = "general_fact") {
	let text = normalizeWhitespace(raw)
		.replace(/[?]+$/g, "")
		.replace(/\b(?:please|can\s+you|tell\s+me|explain)\b/gi, " ")
		.trim();
	const replacements = [
		/^\s*who\s+(?:won|wins|is\s+the\s+winner\s+of|was\s+the\s+winner\s+of)\s+/i,
		/^\s*winner\s+of\s+/i,
		/^\s*who\s+took\s+home\s+/i,
		/^\s*who\s+came\s+first\s+(?:in|at|on)?\s*/i,
		/^\s*who\s+(?:was\s+)?(?:eliminated|voted\s+off|sent\s+home)\s+(?:from|on|in)?\s*/i,
		/^\s*what\s+(?:was|is)\s+the\s+score\s+(?:of|for|in)?\s*/i,
		/^\s*final\s+score\s+(?:of|for|in)?\s*/i,
		/^\s*when\s+(?:is|was|does|did|will)\s+/i,
		/^\s*how\s+much\s+(?:is|are|does|do|will)\s+/i,
		/^\s*price\s+of\s+/i,
		/^\s*cost\s+of\s+/i,
		/^\s*who\s+(?:is|are|was|were)\s+/i,
		/^\s*what\s+(?:is|are|was|were)\s+/i,
	];
	for (const rx of replacements) {
		text = text.replace(rx, " ").trim();
	}
	text = text
		.replace(/\b(?:right\s+now|today|latest|currently|now)\b/gi, " ")
		.replace(/\s+/g, " ")
		.trim();
	const cleaned = cleanTopicLabel(text);
	if (cleaned && countWords(cleaned) >= 1 && countWords(cleaned) <= 12)
		return cleaned;
	if (type === "winner") {
		const winnerMatch = raw.match(
			/\b(?:who\s+(?:won|wins)|winner\s+of)\s+(.{2,120})$/i,
		);
		if (winnerMatch) return cleanTopicLabel(winnerMatch[1]);
	}
	return cleanTopicLabel(raw).slice(0, 120);
}

function directAnswerSearchHints({ raw = "", subject = "", type = "" } = {}) {
	const base = cleanTopicLabel(subject || raw);
	const original = cleanTopicLabel(raw);
	const hints = [];
	const push = (value) => {
		const q = sanitizeOverlayQuery(value);
		if (q) hints.push(q);
	};
	push(original);
	push(base);
	if (type === "winner") {
		push(`${base} who won`);
		push(`${base} winner`);
		push(`${base} winner name`);
		push(`${base} champion name`);
		push(`${base} finale winner`);
		push(`${base} results`);
		push(`${base} winner confirmed`);
	} else if (type === "elimination") {
		push(`${base} eliminated`);
		push(`${base} voted off`);
		push(`${base} results`);
	} else if (type === "score") {
		push(`${base} final score`);
		push(`${base} result`);
	} else if (type === "date") {
		push(`${base} date`);
		push(`${base} release date`);
		push(`${base} latest update`);
	} else if (type === "price") {
		push(`${base} price`);
		push(`${base} cost`);
		push(`${base} latest price`);
	} else if (type === "status") {
		push(`${base} status`);
		push(`${base} confirmed`);
		push(`${base} latest update`);
	} else {
		push(`${base} answer`);
		push(`${base} latest`);
	}
	return uniqueStrings(hints, { limit: 10 });
}

function detectDirectAnswerQuery(promptText = "") {
	const raw = normalizeWhitespace(stripUrlsFromText(promptText || ""));
	if (!raw) return null;
	const lines = raw
		.split(/\r?\n/)
		.map((line) => normalizeWhitespace(line))
		.filter(Boolean);
	const firstMeaningful =
		lines.find((line) => !PROMPT_BRIEF_LABEL_RE.test(line)) || raw;
	const candidate = firstMeaningful.length <= 180 ? firstMeaningful : raw;
	const questionish =
		/[?]$/.test(candidate) ||
		/^(?:who|what|when|where|is|are|was|were|did|does|will|how\s+much)\b/i.test(
			candidate,
		);
	let matched = null;
	for (const rule of DIRECT_ANSWER_QUERY_PATTERNS) {
		if (rule.pattern.test(candidate) || rule.pattern.test(raw)) {
			matched = rule;
			break;
		}
	}
	if (!matched || !questionish) return null;
	if (
		matched.type === "identity" &&
		/\b(?:why|how\s+to|should|can\s+i|could\s+i|ways?|tips?|advice)\b/i.test(
			raw,
		)
	) {
		return null;
	}
	const subject = extractDirectAnswerSubject(candidate, matched.type);
	const bare = countWords(raw) <= 12 && lines.length <= 2;
	return {
		isDirectAnswer: true,
		type: matched.type,
		answerLabel: matched.answerLabel,
		answerFirst: true,
		bare,
		question: cleanTopicLabel(candidate).slice(0, 180),
		subject: subject || cleanTopicLabel(candidate).slice(0, 120),
		searchHints: directAnswerSearchHints({
			raw: candidate,
			subject: subject || candidate,
			type: matched.type,
		}),
	};
}

function getTopicDirectAnswerQuery(topic = {}) {
	const direct =
		topic?.directAnswerQuery || topic?.promptBrief?.directAnswerQuery || null;
	return direct?.isDirectAnswer ? direct : null;
}

function isDirectAnswerTopic(topic = {}) {
	return Boolean(getTopicDirectAnswerQuery(topic));
}

function isBareDirectAnswerTopic(topic = {}) {
	const direct = getTopicDirectAnswerQuery(topic);
	return Boolean(direct?.bare);
}

function detectTopListRequest(text = "") {
	const raw = cleanTopicLabel(text);
	if (!raw) return null;
	const match = raw.match(
		/\b(?:top|best|worst|most|least)\s*[-_ ]*(\d{1,2}|one|two|three|four|five|six|seven|eight|nine|ten)\b/i,
	);
	if (!match) return null;
	const count = clampNumber(parseTopListNumberToken(match[1]), 1, 10);
	if (!count || count < 2) return null;
	const after = raw.slice(match.index + match[0].length).trim();
	const subject =
		after.replace(/^(?:best|worst|most|least|controversial)\s+/i, "").trim() ||
		raw;
	return {
		count,
		subject: cleanTopicLabel(subject) || raw,
		countdown: true,
		label: `Top ${count}`,
	};
}

function normalizeTopListTopic(topic = "", topList = null) {
	const raw = cleanTopicLabel(topic);
	if (!raw || !topList?.count) return raw;
	if (new RegExp(`^top\\s*${topList.count}\\b`, "i").test(raw)) return raw;
	const subject = cleanTopicLabel(topList.subject || raw);
	return cleanTopicLabel(`Top ${topList.count} ${subject}`) || raw;
}

function buildPromptFactSearchQueries(promptText = "", topic = "") {
	const brief = parseStructuredPromptBrief(promptText);
	const directAnswer = detectDirectAnswerQuery(promptText || topic);
	const raw = String(promptText || "");
	const lines = raw
		.split(/\r?\n/)
		.map((line) => normalizeWhitespace(line))
		.filter(Boolean);
	const statLines = lines.filter(
		(line) => isPromptFactStatLine(line),
	);
	return uniqueStrings(
		[
			...(directAnswer?.searchHints || []),
			...brief.searchHints,
			...statLines,
			topic && /\bscam|fraud\b/i.test(`${topic} ${raw}`)
				? `${topic} FTC social media scam losses`
				: "",
			topic && /\beconomic|economy|rent|grocery|groceries|inflation|americans|broke|paycheck|bills?|subscriptions?|cost of living|budget|debt\b/i.test(`${topic} ${raw}`)
				? `${topic} BLS Census Federal Reserve household costs`
				: "",
			topic && /\bbroke|paycheck|rent|grocery|groceries|bills?|subscriptions?|cost of living|budget|debt\b/i.test(`${topic} ${raw}`)
				? "BLS CPI rent food wages household spending latest data"
				: "",
			topic && /\bbroke|paycheck|rent|grocery|groceries|bills?|subscriptions?|cost of living|budget|debt\b/i.test(`${topic} ${raw}`)
				? "Federal Reserve household economic well being paycheck to paycheck"
				: "",
			topic && /\brent|housing|cost of living|broke|paycheck\b/i.test(`${topic} ${raw}`)
				? "Census rent burden household income housing costs"
				: "",
		]
			.filter(Boolean)
			.map((q) => sanitizeOverlayQuery(q).slice(0, 140))
			.filter(Boolean),
		{ limit: 8 },
	);
}

function buildPromptSearchHints(topic = "", promptText = "", topList = null) {
	const cleanTopic = cleanTopicLabel(topic);
	const original = cleanTopicLabel(promptText);
	const base = cleanTopic || original;
	const brief = parseStructuredPromptBrief(promptText);
	const directAnswer = detectDirectAnswerQuery(promptText || base);
	const hints = [];
	const push = (value) => {
		const q = sanitizeOverlayQuery(value);
		if (q) hints.push(q);
	};
	push(base);
	for (const q of directAnswer?.searchHints || []) push(q);
	for (const q of buildPromptFactSearchQueries(promptText, base)) push(q);
	push(`${base} latest updates`);
	push(`${base} latest news`);
	push(`${base} explained`);
	push(`${base} controversy`);
	if (topList?.count) {
		push(`${base} ranking`);
		push(`${base} best list`);
		push(`${base} travel guide`);
		push(`${topList.subject || base} top ${topList.count}`);
	}
	if (
		!brief.isStructured &&
		original &&
		original.toLowerCase() !== base.toLowerCase()
	) {
		push(original);
	}
	return uniqueStrings(hints, { limit: 12 });
}

function buildPromptImageSearchHints(topic = "", promptText = "", topList = null) {
	const base = cleanTopicLabel(topic || promptText);
	const brief = parseStructuredPromptBrief(promptText);
	const directAnswer = detectDirectAnswerQuery(promptText || base);
	const hints = [];
	const push = (value) => {
		const q = sanitizeOverlayQuery(value);
		if (q) hints.push(q);
	};
	push(`${base} photo`);
	if (directAnswer?.type === "winner") {
		push(`${base} finale photo`);
		push(`${base} winner photo`);
		push(`${base} cast photo`);
	}
	push(`${base} news photo`);
	push(`${base} editorial photo`);
	push(`${base} landmark photo`);
	push(`${base} location photo`);
	if (topList?.count) {
		push(`${base} ranking photos`);
		push(`${topList.subject || base} travel photos`);
		push(`${topList.subject || base} skyline landmark`);
	}
	if (/\bscam|fraud\b/i.test(`${base} ${promptText}`)) {
		push(`${base} phone scam warning`);
		push(`${base} social media scam`);
		push(`${base} consumer protection`);
	}
	if (/\beconomic|economy|rent|grocery|inflation|americans\b/i.test(`${base} ${promptText}`)) {
		push(`${base} grocery prices`);
		push(`${base} rent prices`);
		push(`${base} household budget`);
		push(`${base} inflation chart`);
	}
	for (const q of brief.imageHints || []) push(q);
	return uniqueStrings(hints, { limit: 12 });
}

function isUserPromptTopicPick(topic = {}) {
	return (
		String(topic?.source || "").toLowerCase() === "user_prompt" ||
		Boolean(topic?.promptText)
	);
}

function normalizeUrlCandidate(raw = "") {
	const trimmed = String(raw || "").trim();
	if (!trimmed) return "";
	return trimmed.replace(/[),.;]+$/g, "");
}

function extractUrlsFromText(text = "") {
	const raw = String(text || "");
	const urls = [];
	const httpRe = /\bhttps?:\/\/[^\s<>()]+/gi;
	const wwwRe = /\bwww\.[^\s<>()]+/gi;
	let match;
	while ((match = httpRe.exec(raw))) {
		const cleaned = normalizeUrlCandidate(match[0]);
		if (cleaned) urls.push(cleaned);
	}
	while ((match = wwwRe.exec(raw))) {
		const cleaned = normalizeUrlCandidate(`https://${match[0]}`);
		if (cleaned) urls.push(cleaned);
	}
	return uniqueStrings(urls, { limit: 6 });
}

function stripUrlsFromText(text = "") {
	return String(text || "")
		.replace(/\bhttps?:\/\/[^\s<>()]+/gi, " ")
		.replace(/\bwww\.[^\s<>()]+/gi, " ")
		.replace(/\s+/g, " ")
		.trim();
}

const PROMPT_PREAMBLE_PATTERNS = [
	/^(please\s+)?(create|make|generate|write|produce|build|craft|plan)\b[^.?!]{0,80}?\b(video|script|story|content)\b\s*(?:about|on|for|regarding|re:)?\s*/i,
	/^(please\s+)?(give|show|tell|explain|summarize|break\s*down)\b[^.?!]{0,80}?\b(about|on|for|regarding|re:)\b\s*/i,
	/^(please\s+)?(i\s*(?:want|need|would\s+like|would\s+love|i'?d\s+like))\b[^.?!]{0,80}?\b(video|topic|content|script)\b\s*(?:about|on|for|regarding|re:)?\s*/i,
];

function stripPromptPreamble(text = "") {
	let cleaned = String(text || "").trim();
	if (!cleaned) return cleaned;
	for (const re of PROMPT_PREAMBLE_PATTERNS) {
		const match = cleaned.match(re);
		if (match) {
			cleaned = cleaned.slice(match[0].length).trim();
			break;
		}
	}
	return cleaned.replace(/^[\s:;-]+/g, "").trim();
}

function looksLikeRecommendationPrompt(text = "") {
	const raw = String(text || "").trim();
	if (!raw) return false;
	return PROMPT_RECOMMENDATION_PATTERNS.some((re) => re.test(raw));
}

function extractPromptSubjectTokens(text = "") {
	const tokens = tokenizeLabel(text);
	return tokens.filter(
		(t) =>
			!TOPIC_STOP_WORDS.has(t) &&
			!GENERIC_TOPIC_TOKENS.has(t) &&
			!PROMPT_CONTROL_TOKENS.has(t),
	);
}

function normalizePromptTopic(text = "") {
	const withoutProductionInstructions = stripSeoTitleRequestText(text);
	const stripped = stripPromptPreamble(withoutProductionInstructions);
	const base = stripCreatorIntentForTopic(
		stripped || withoutProductionInstructions || "",
	);
	const cleaned = cleanTopicCandidate(base) || base.trim();
	return cleaned.replace(/\s+/g, " ").trim();
}

function splitPromptTopics(text = "", opts = {}) {
	const raw = String(text || "").trim();
	if (!raw) return [];
	const brief = parseStructuredPromptBrief(raw);
	if (opts.singleBrief || brief.isStructured || shouldTreatPromptAsSingleBrief(raw)) {
		const topic = normalizePromptTopic(brief.primaryTopic || raw);
		return topic ? [topic] : [];
	}
	const parts = raw
		.split(PROMPT_SPLIT_RE)
		.map((chunk) => normalizePromptTopic(chunk))
		.filter(Boolean);
	if (parts.length) return parts;
	const fallback = normalizePromptTopic(raw);
	return fallback ? [fallback] : [];
}

function resolvePreferredTopicHint(raw = "") {
	const original = String(raw || "").trim();
	if (!original) {
		return {
			mode: "none",
			promptText: "",
			topicCandidates: [],
			imageUrls: [],
			videoUrls: [],
		};
	}
	const promptUrls = extractUrlsFromText(original).filter(isHttpUrl);
	const videoUrls = promptUrls.filter((u) => isProbablyDirectVideoUrl(u));
	const imageUrls = promptUrls.filter((u) => !videoUrls.includes(u));
	const cleanedPrompt = stripUrlsFromText(original);
	const promptText = String(cleanedPrompt || "").trim();
	const brief = parseStructuredPromptBrief(promptText);
	const directAnswerQuery = detectDirectAnswerQuery(promptText);
	if (!promptText) {
		return {
			mode: "none",
			promptText: "",
			topicCandidates: [],
			imageUrls,
			videoUrls,
			brief,
			directAnswerQuery,
		};
	}
	const subjectTokens = extractPromptSubjectTokens(promptText);
	const wantsRecommendation = looksLikeRecommendationPrompt(promptText);
	const effectiveTokens = wantsRecommendation
		? subjectTokens.filter((t) => !PROMPT_QUESTION_TOKENS.has(t))
		: subjectTokens;
	const mode = effectiveTokens.length ? "prompt" : "trends";
	const singleBrief = brief.isStructured || shouldTreatPromptAsSingleBrief(promptText);
	let topicCandidates =
		mode === "prompt" ? splitPromptTopics(promptText, { singleBrief }) : [];
	topicCandidates = topicCandidates.map((topic) => {
		const topList = detectTopListRequest(topic);
		return topList ? normalizeTopListTopic(topic, topList) : topic;
	});
	return {
		mode,
		promptText,
		topicCandidates,
		imageUrls,
		videoUrls,
		brief,
		singleBrief,
		directAnswerQuery,
	};
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
		const directAnswerQuery = detectDirectAnswerQuery(topic);
		return [
			{
				topic: topic.slice(0, 120),
				displayTopic,
				reason: dryRun ? "Dry run" : "preferredTopicHint",
				angle: "",
				keywords: topicTokensFromTitle(topic).slice(0, 8),
				directAnswerQuery,
			},
		];
	}

	const promptInfo = resolvePreferredTopicHint(preferredTopicHint);
	if (promptInfo.mode === "prompt") {
		const topics = [];
		const seen = new Set();
		const promptBrief = promptInfo.brief || parseStructuredPromptBrief(promptInfo.promptText);
		const candidates = promptInfo.singleBrief
			? [promptBrief.primaryTopic || promptInfo.topicCandidates[0] || promptInfo.promptText]
			: promptInfo.topicCandidates.length
			? promptInfo.topicCandidates
			: [promptInfo.promptText];
		const candidateLimit = promptInfo.singleBrief ? 1 : desired;
		for (const candidate of candidates) {
			if (topics.length >= candidateLimit) break;
			const normalized = normalizePromptTopic(candidate);
			if (!normalized) continue;
			const topList = detectTopListRequest(normalized);
			let finalTopic = topList
				? normalizeTopListTopic(normalized, topList)
				: normalized;
			if (looksLikePromptStructureLabelOnly(finalTopic)) {
				finalTopic =
					cleanTopicLabel(promptBrief.title || promptBrief.primaryTopic || "") ||
					finalTopic;
			}
			finalTopic = cleanTopicLabel(finalTopic) || finalTopic;
			const signature = topicSignature(finalTopic);
			if (signature && seen.has(signature)) continue;
			if (signature) seen.add(signature);
			const displayTopic = cleanTopicLabel(finalTopic) || finalTopic;
			const directAnswerQuery =
				detectDirectAnswerQuery(promptInfo.promptText) ||
				detectDirectAnswerQuery(finalTopic);
			const promptSearchHints = buildPromptSearchHints(
				finalTopic,
				promptInfo.promptText,
				topList,
			);
			const promptImageHints = buildPromptImageSearchHints(
				finalTopic,
				promptInfo.promptText,
				topList,
			);
			const exactPromptSearchHints = buildPromptFactSearchQueries(
				promptInfo.promptText,
				finalTopic,
			);
			topics.push({
				topic: finalTopic.slice(0, 120),
				displayTopic,
				reason: "preferredTopicHint",
				angle: detectPromptAngle(promptInfo.promptText),
				keywords: uniqueStrings(
					[
						...topicTokensFromTitle(finalTopic),
						...(directAnswerQuery?.type ? [directAnswerQuery.type] : []),
						...(directAnswerQuery?.answerLabel
							? topicTokensFromTitle(directAnswerQuery.answerLabel)
							: []),
						...promptSearchHints.flatMap((q) => topicTokensFromTitle(q)),
					],
					{ limit: 14 },
				),
				images: topics.length === 0 ? promptInfo.imageUrls : [],
				videos: topics.length === 0 ? promptInfo.videoUrls : [],
				source: "user_prompt",
				promptText: promptInfo.promptText,
				promptBrief: {
					...promptBrief,
					directAnswerQuery,
					searchHints: uniqueStrings(
						[
							...(directAnswerQuery?.searchHints || []),
							...(promptBrief.searchHints || []),
							...exactPromptSearchHints,
						],
						{ limit: 14 },
					),
					imageHints: uniqueStrings(
						[
							...(promptBrief.imageHints || []),
							...promptImageHints,
						],
						{ limit: 14 },
					),
				},
				directAnswerQuery,
				topList,
				searchHints: promptSearchHints,
				imageSearchHints: promptImageHints,
				trendStory: {
					searchPhrases: uniqueStrings(
						[
							...(directAnswerQuery?.searchHints || []),
							...promptSearchHints,
							...exactPromptSearchHints,
						],
						{ limit: 16 },
					),
					imageSearchQueries: promptImageHints,
					entityNames: uniqueStrings(
						[
							...(topList?.subject ? [topList.subject] : []),
							...(promptBrief.title ? [promptBrief.title] : []),
						],
						{ limit: 6 },
					),
					articles: [],
					images: [],
					videos: topics.length === 0 ? promptInfo.videoUrls : [],
					potentialVideos: [],
					potentialImages: [],
				},
			});
		}
		if (topics.length) {
			logJob(null, "preferred topic hint used (prompt mode)", {
				topics: topics.map((t) => t.displayTopic || t.topic),
				singleBrief: Boolean(promptInfo.singleBrief),
				requestedTitle: promptBrief.title || "",
				openingLine: promptBrief.openingLine || "",
				directAnswer: topics
					.map((t) => getTopicDirectAnswerQuery(t))
					.filter(Boolean)
					.map((dq) => ({
						type: dq.type,
						subject: dq.subject,
						bare: Boolean(dq.bare),
					})),
			});
			if (!promptInfo.singleBrief && promptInfo.topicCandidates.length > topics.length) {
				logJob(null, "preferred topic hint truncated to fit duration", {
					requested: promptInfo.topicCandidates.length,
					used: topics.length,
				});
			}
			return topics;
		}
	}

	if (promptInfo.mode === "trends" && promptInfo.promptText) {
		logJob(null, "preferred topic hint treated as recommendation", {
			hint: promptInfo.promptText.slice(0, 140),
		});
	}

	const topics = [];

	const trendStories = await fetchTrendsStories({
		categoryLabel: categoryLabel || LONG_VIDEO_TRENDS_CATEGORY,
		geo: LONG_VIDEO_TRENDS_GEO,
		language,
		baseUrl,
		topicCount: desired,
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
			primaryTrendStory.relatedQueries,
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
				resp?.choices?.[0]?.message?.content || "",
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
			"Unable to pick topics. Provide preferredTopicHint or ensure Google Trends is available.",
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
	const host = getUrlHost(url);
	if (/\b(ytimg\.com|img\.youtube\.com)\b/i.test(host)) return true;
	if (
		/\b(maxresdefault|hqdefault|mqdefault|sddefault)\.(?:jpg|jpeg|png|webp)(?:[?#]|$)/i.test(
			url,
		)
	) {
		return true;
	}
	if (/\/vi(?:_webp)?\/[^/]+\/[^/?#]+\.(?:jpg|jpeg|png|webp)(?:[?#]|$)/i.test(url))
		return true;
	if (/\b(video[-_]?thumbnail|youtube[-_]?thumbnail)\b/i.test(url))
		return true;
	return false;
}

function localPathKey(filePath = "") {
	try {
		return path.resolve(String(filePath || "")).toLowerCase();
	} catch {
		return String(filePath || "").trim().toLowerCase();
	}
}

function isReservedThumbnailVisualPath(filePath = "", thumbnailPath = "") {
	const raw = String(filePath || "").trim();
	if (!raw) return false;
	if (thumbnailPath && localPathKey(raw) === localPathKey(thumbnailPath)) {
		return true;
	}
	const base = path.basename(raw).toLowerCase();
	if (/^(thumb|thumbnail|yt_thumb|youtube_thumb)[_-]/i.test(base)) {
		return true;
	}
	return /\b(youtube[-_]?thumbnail|video[-_]?thumbnail)\b/i.test(base);
}

function isActualPresenterVisualType(visualType = "") {
	const type = String(visualType || "").toLowerCase();
	return type === "presenter" || type === "presenter_fallback";
}

function summarizePresenterCoverage(renderSummary = []) {
	const entries = Array.isArray(renderSummary) ? renderSummary : [];
	const presenterEntries = entries.filter((entry) =>
		String(entry?.plannedVisualType || "").toLowerCase() === "presenter",
	);
	const actualPresenterEntries = entries.filter((entry) =>
		isActualPresenterVisualType(entry?.actualVisualType),
	);
	const sumDuration = (list) =>
		list.reduce(
			(sum, entry) => sum + Math.max(0, Number(entry?.durationSec || 0)),
			0,
		);
	const totalDurationSec = sumDuration(entries);
	const plannedPresenterDurationSec = sumDuration(presenterEntries);
	const actualPresenterDurationSec = sumDuration(actualPresenterEntries);
	const forcedOpeningMisses = entries
		.filter(
			(entry) =>
				entry?.mustUsePresenter &&
				!isActualPresenterVisualType(entry?.actualVisualType),
		)
		.map((entry) => entry.label || entry.segment)
		.filter((label) => label !== undefined && label !== null);
	return {
		totalUnits: entries.length,
		plannedPresenterUnits: presenterEntries.length,
		actualPresenterUnits: actualPresenterEntries.length,
		totalDurationSec,
		plannedPresenterDurationSec,
		actualPresenterDurationSec,
		actualPresenterDurationRatio: totalDurationSec
			? actualPresenterDurationSec / totalDurationSec
			: 0,
		forcedOpeningMisses,
	};
}

async function fetchGoogleImagesFromService(
	query,
	{ limit = GOOGLE_IMAGES_RESULTS_PER_QUERY, baseUrl, jobId } = {},
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
				{ limit: Math.max(12, Number(limit) || 12) },
			);
			if (urls.length) {
				if (jobId)
					logJob(jobId, "free image search service hit", {
						query: q,
						endpoint,
						count: urls.length,
					});
				return urls;
			}
		} catch (e) {
			if (jobId)
				logJob(jobId, "free image search service failed", {
					query: q,
					endpoint,
					error: e.message,
				});
		}
	}
	if (GOOGLE_CSE_CONFIG_READY) {
		const cseUrls = await fetchCseImagesForQuery(
			q,
			[],
			Math.max(6, Math.min(Number(limit) || 12, CSE_MAX_IMAGE_RESULTS)),
			jobId,
			{
				maxPages: Math.min(2, CSE_MAX_PAGES),
				looseTopicRelevance: true,
				allowLooseResults: true,
				relaxedMinEdge: CSE_RELAXED_MIN_IMAGE_SHORT_EDGE,
			},
		);
		if (cseUrls.length) {
			if (jobId) {
				logJob(jobId, "free image search CSE fallback hit", {
					query: q,
					count: cseUrls.length,
				});
			}
			return cseUrls;
		}
	}
	const commonsUrls = await fetchWikimediaImageUrls(
		q,
		Math.max(3, Math.min(Number(limit) || 6, 8)),
	);
	if (commonsUrls.length) {
		if (jobId) {
			logJob(jobId, "free image search Wikimedia fallback hit", {
				query: q,
				count: commonsUrls.length,
			});
		}
		return commonsUrls;
	}
	return [];
}

function normalizeFreeImageMetadataItem(raw, query = "") {
	if (typeof raw === "string") {
		return {
			url: raw,
			title: "",
			sourcePage: "",
			provider: "",
			query,
		};
	}
	if (!raw || typeof raw !== "object") return null;
	const url = String(raw.url || raw.imageUrl || raw.src || "").trim();
	if (!url) return null;
	return {
		url,
		title: cleanTopicLabel(raw.title || raw.alt || raw.caption || ""),
		sourcePage: String(raw.sourcePage || raw.pageUrl || raw.contextUrl || "").trim(),
		provider: String(raw.provider || "").trim(),
		query,
	};
}

async function fetchCseImageMetadataForQuery(
	query,
	{ limit = PRE_SCRIPT_VISUAL_RESEARCH_RESULTS_PER_QUERY, jobId } = {},
) {
	const q = sanitizeOverlayQuery(query);
	if (!q || !GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) return [];
	const items = await fetchCseItems([q], {
		num: Math.max(6, Number(limit) || 12),
		maxPages: Math.min(2, CSE_MAX_PAGES),
		searchType: "image",
		imgSize: CSE_PREFERRED_IMG_SIZE,
		jobId,
		label: "pre_script_visual_metadata",
	});
	const seen = new Set();
	const out = [];
	for (const item of items || []) {
		const url = String(item?.link || "").trim();
		if (!isHttpUrl(url) || isLikelyThumbnailUrl(url)) continue;
		const key = normalizeImageUrlKey(url);
		if (!key || seen.has(key)) continue;
		seen.add(key);
		out.push({
			url,
			title: cleanTopicLabel(item?.title || ""),
			sourcePage: String(item?.image?.contextLink || item?.link || "").trim(),
			provider: getUrlHost(item?.displayLink || item?.link || "") || "cse",
			query: q,
		});
		if (out.length >= Math.max(12, Number(limit) || 12)) break;
	}
	if (jobId && out.length) {
		logJob(jobId, "pre-script visual CSE fallback hit", {
			query: q,
			count: out.length,
		});
	}
	return out;
}

async function fetchGoogleImageMetadataFromService(
	query,
	{ limit = PRE_SCRIPT_VISUAL_RESEARCH_RESULTS_PER_QUERY, baseUrl, jobId } = {},
) {
	const q = sanitizeOverlayQuery(query);
	if (!q) return [];
	const candidates = buildGoogleImagesApiCandidates(baseUrl);
	for (const endpoint of candidates) {
		try {
			const { data } = await axios.get(endpoint, {
				params: {
					q,
					limit: Math.max(6, Number(limit) || 12),
					includeMetadata: 1,
					bingOnly: 1,
				},
				timeout: 45000,
				validateStatus: (s) => s < 500,
			});
			const raw =
				(Array.isArray(data?.images) && data.images) ||
				(Array.isArray(data?.urls) && data.urls) ||
				(Array.isArray(data?.results) && data.results) ||
				[];
			const seen = new Set();
			const items = [];
			for (const item of raw) {
				const normalized = normalizeFreeImageMetadataItem(item, q);
				if (!normalized?.url) continue;
				if (!isHttpUrl(normalized.url) || isLikelyThumbnailUrl(normalized.url))
					continue;
				const key = normalizeImageUrlKey(normalized.url);
				if (!key || seen.has(key)) continue;
				seen.add(key);
				items.push(normalized);
				if (items.length >= Math.max(12, Number(limit) || 12)) break;
			}
			if (items.length) {
				if (jobId)
					logJob(jobId, "pre-script visual search hit", {
						query: q,
						endpoint,
						count: items.length,
						withTitles: items.filter((item) => item.title).length,
					});
				return items;
			}
		} catch (e) {
			if (jobId)
				logJob(jobId, "pre-script visual search failed", {
					query: q,
					endpoint,
					error: e.message,
				});
		}
	}
	const cseItems = await fetchCseImageMetadataForQuery(q, { limit, jobId });
	if (cseItems.length) return cseItems;
	const commonsUrls = await fetchWikimediaImageUrls(
		q,
		Math.min(Math.max(3, Number(limit) || 8), 8),
	);
	if (commonsUrls.length) {
		if (jobId) {
			logJob(jobId, "pre-script visual Wikimedia fallback hit", {
				query: q,
				count: commonsUrls.length,
			});
		}
		return commonsUrls.map((url) => ({
			url,
			title: "",
			sourcePage: "",
			provider: "wikimedia",
			query: q,
		}));
	}
	return [];
}

function buildPreScriptVisualQueries(topic = {}, category = "") {
	const story = topic.trendStory || {};
	const promptBrief = topic.promptBrief || parseStructuredPromptBrief(topic.promptText);
	const label = String(topic.displayTopic || topic.topic || "").trim();
	const keywordHints = Array.isArray(topic.keywords) ? topic.keywords : [];
	const articleTitles = Array.isArray(story.articles)
		? story.articles.map((a) => a?.title).filter(Boolean)
		: [];
	return uniqueStrings(
		[
			...(Array.isArray(topic.imageSearchHints) ? topic.imageSearchHints : []),
			...(Array.isArray(promptBrief?.imageHints) ? promptBrief.imageHints : []),
			...(Array.isArray(story.imageSearchQueries)
				? story.imageSearchQueries
				: []),
			...(promptBrief?.title ? [`${promptBrief.title} news photo`] : []),
			...buildTopicNearImageQueries(label, {
				topicKeywords: keywordHints,
				articleTitles,
				category,
			}),
			...(isConceptualOrMetaphoricalVisualTopic({
				category,
				topicLabel: label,
				text: [
					...(Array.isArray(promptBrief?.visuals) ? promptBrief.visuals : []),
					...(Array.isArray(promptBrief?.avoid) ? promptBrief.avoid : []),
					...(Array.isArray(promptBrief?.mustInclude)
						? promptBrief.mustInclude
						: []),
				].join(" "),
			})
				? buildConceptualVisualQueries({
						topicLabel: label,
						segmentText: [
							...(Array.isArray(promptBrief?.visuals)
								? promptBrief.visuals
								: []),
							...(Array.isArray(promptBrief?.avoid) ? promptBrief.avoid : []),
							...(Array.isArray(promptBrief?.mustInclude)
								? promptBrief.mustInclude
								: []),
						].join(" "),
						category,
						limit: 8,
					})
				: []),
		]
			.filter(Boolean)
			.map((q) => sanitizeOverlayQuery(q))
			.filter(Boolean),
		{ limit: PRE_SCRIPT_VISUAL_RESEARCH_QUERY_LIMIT },
	);
}

function summarizePreScriptVisualCandidates(candidates = []) {
	const titles = [];
	const urls = [];
	const seenTitles = new Set();
	const seenUrls = new Set();
	for (const item of candidates || []) {
		if (item?.url) {
			const key = normalizeImageUrlKey(item.url);
			if (key && !seenUrls.has(key)) {
				seenUrls.add(key);
				urls.push(item.url);
			}
		}
		const title = cleanTopicLabel(item?.title || "");
		if (!title || countWords(title) < 2) continue;
		const titleKey = normalizeQaText(title);
		if (!titleKey || seenTitles.has(titleKey)) continue;
		seenTitles.add(titleKey);
		titles.push({
			title,
			query: item.query || "",
			source: getUrlHost(item.sourcePage || item.url || ""),
		});
		if (titles.length >= PRE_SCRIPT_VISUAL_RESEARCH_TITLE_LIMIT) break;
	}
	return { titles, urls };
}

async function prefetchPreScriptVisualResearch({
	topics = [],
	baseUrl,
	jobId,
	category = "",
} = {}) {
	if (
		!PRE_SCRIPT_VISUAL_RESEARCH_ENABLED ||
		!GOOGLE_IMAGES_SEARCH_ENABLED ||
		!PRE_SCRIPT_VISUAL_RESEARCH_QUERY_LIMIT
	) {
		return [];
	}
	const summaries = [];
	for (let i = 0; i < (topics || []).length; i += 1) {
		const topic = topics[i] || {};
		const queries = buildPreScriptVisualQueries(topic, category);
		if (!queries.length) continue;
		const candidates = [];
		const seen = new Set();
		for (const query of queries) {
			const items = await fetchGoogleImageMetadataFromService(query, {
				limit: PRE_SCRIPT_VISUAL_RESEARCH_RESULTS_PER_QUERY,
				baseUrl,
				jobId,
			});
			for (const item of items) {
				const key = normalizeImageUrlKey(item.url);
				if (!key || seen.has(key)) continue;
				seen.add(key);
				candidates.push(item);
			}
		}
		const summary = summarizePreScriptVisualCandidates(candidates);
		const story = topic.trendStory || {};
		const existingPotential = Array.isArray(story.potentialImages)
			? story.potentialImages
			: [];
		const mergedPotential = [];
		const potentialSeen = new Set();
		for (const item of [...existingPotential, ...candidates]) {
			const normalized = normalizeFreeImageMetadataItem(item, item?.query || "");
			if (!normalized?.url) continue;
			const key = normalizeImageUrlKey(normalized.url);
			if (!key || potentialSeen.has(key)) continue;
			potentialSeen.add(key);
			mergedPotential.push(normalized);
		}
		topic.trendStory = {
			...story,
			potentialImages: mergedPotential.slice(0, 90),
			visualResearch: {
				queries,
				candidateCount: candidates.length,
				titles: summary.titles,
			},
		};
		summaries.push({
			topicIndex: i,
			topic: topic.displayTopic || topic.topic || "",
			queries,
			candidateCount: candidates.length,
			titleCount: summary.titles.length,
			titles: summary.titles,
			urlCount: summary.urls.length,
		});
		logJob(jobId, "pre-script visual research ready", {
			topic: topic.displayTopic || topic.topic || "",
			queries: queries.length,
			candidates: candidates.length,
			titles: summary.titles.length,
			potentialImages: topic.trendStory.potentialImages.length,
		});
	}
	return summaries;
}

const VISUAL_QUERY_EXTRA_STOP_TOKENS = new Set([
	"article",
	"articles",
	"editorial",
	"image",
	"images",
	"landmark",
	"news",
	"photo",
	"photos",
	"picture",
	"pictures",
	"press",
	"stock",
	"thumbnail",
	"update",
	"updates",
]);

function compactVisualSearchQuery(raw = "", topicLabel = "", fallback = "") {
	const topicTokens = filterSegmentImageMatchTokens(tokenizeLabel(topicLabel || ""));
	const rawTokens = filterSegmentImageMatchTokens(tokenizeLabel(raw || "")).filter(
		(t) => !VISUAL_QUERY_EXTRA_STOP_TOKENS.has(t),
	);
	const fallbackTokens = filterSegmentImageMatchTokens(
		tokenizeLabel(fallback || ""),
	).filter((t) => !VISUAL_QUERY_EXTRA_STOP_TOKENS.has(t));
	const chosen = [];
	const add = (tok) => {
		const clean = String(tok || "").trim();
		if (!clean || chosen.includes(clean)) return;
		chosen.push(clean);
	};
	for (const tok of rawTokens) add(tok);
	if (chosen.length < 2) {
		for (const tok of fallbackTokens) add(tok);
	}
	if (
		topicTokens.length &&
		!chosen.some((tok) => topicTokens.includes(tok))
	) {
		add(topicTokens[0]);
	}
	const query = sanitizeOverlayQuery(chosen.slice(0, 6).join(" "));
	if (tokenizeLabel(query).length >= 2) return query;
	const fallbackQuery = sanitizeOverlayQuery(fallback || topicLabel || raw);
	return fallbackQuery;
}

function mergeTopicPotentialImages(topic = {}, items = [], limit = 140) {
	if (!topic || typeof topic !== "object") return [];
	const story = topic.trendStory || {};
	const merged = [];
	const seen = new Set();
	for (const raw of [
		...(Array.isArray(story.potentialImages) ? story.potentialImages : []),
		...(Array.isArray(items) ? items : []),
	]) {
		const normalized = normalizeFreeImageMetadataItem(raw, raw?.query || "");
		if (!normalized?.url || !isHttpUrl(normalized.url)) continue;
		const key = normalizeImageUrlKey(normalized.url);
		if (!key || seen.has(key)) continue;
		seen.add(key);
		merged.push(normalized);
		if (merged.length >= limit) break;
	}
	topic.trendStory = {
		...story,
		potentialImages: merged,
	};
	return merged;
}

function mergeTopicPotentialVideos(topic = {}, items = [], limit = 48) {
	if (!topic || typeof topic !== "object") return [];
	const story = topic.trendStory || {};
	const merged = [];
	const seen = new Set();
	for (const raw of [
		...(Array.isArray(story.potentialVideos) ? story.potentialVideos : []),
		...(Array.isArray(items) ? items : []),
	]) {
		const entry = normalizeFeedVideoCandidateEntry(
			raw,
			raw?.sourceType || raw?.origin || "visual-grounding-video",
		);
		if (!entry?.url || !isHttpUrl(entry.url)) continue;
		const key = normalizeFeedVideoKey(entry.url || entry.pageUrl);
		if (!key || seen.has(key)) continue;
		seen.add(key);
		merged.push(entry);
		if (merged.length >= limit) break;
	}
	topic.trendStory = {
		...story,
		potentialVideos: merged,
	};
	return merged;
}

function buildPreScriptVisualBeatPlan({
	topics = [],
	segmentCount = 0,
	jobId = null,
} = {}) {
	if (!PRE_SCRIPT_VISUAL_BEAT_PLAN_ENABLED) return [];
	const allPlans = [];
	const targetPerTopic = Math.max(
		4,
		Math.ceil(
			Math.min(PRE_SCRIPT_VISUAL_BEAT_LIMIT, Number(segmentCount) || 12) /
				Math.max(1, (topics || []).length || 1),
		),
	);

	for (let topicIndex = 0; topicIndex < (topics || []).length; topicIndex += 1) {
		const topic = topics[topicIndex] || {};
		const story = topic.trendStory || {};
		const label = String(topic.displayTopic || topic.topic || "").trim();
		const groups = new Map();
		const addCandidate = (raw, fallbackQuery = "") => {
			const normalized = normalizeFreeImageMetadataItem(raw, fallbackQuery);
			if (!normalized?.url || !isHttpUrl(normalized.url)) return;
			const sourceQuery =
				sanitizeOverlayQuery(normalized.query || fallbackQuery || label) ||
				label;
			const key = normalizeQaText(sourceQuery || label || normalized.url);
			if (!key) return;
			const group = groups.get(key) || {
				sourceQuery,
				items: [],
				titles: [],
				hosts: new Set(),
			};
			group.items.push(normalized);
			const title = cleanTopicLabel(normalized.title || "");
			if (title && !group.titles.some((t) => normalizeQaText(t) === normalizeQaText(title))) {
				group.titles.push(title);
			}
			const host = getUrlHost(normalized.sourcePage || normalized.url || "");
			if (host) group.hosts.add(host);
			groups.set(key, group);
		};

		for (const item of Array.isArray(story.potentialImages)
			? story.potentialImages
			: []) {
			addCandidate(item, item?.query || label);
		}
		const topicTokens = topicTokensFromTitle(label);
		const beats = [...groups.values()]
			.map((group, idx) => {
				const titleQuery = compactVisualSearchQuery(
					group.titles[0] || "",
					label,
					group.sourceQuery,
				);
				const sourceQuery = compactVisualSearchQuery(
					group.sourceQuery || "",
					label,
					label,
				);
				const query =
					tokenizeLabel(titleQuery).length >= 2 ? titleQuery : sourceQuery;
				const titleMatches = topicMatchInfo(topicTokens, group.titles).count;
				const queryMatches = topicMatchInfo(topicTokens, [query, group.sourceQuery])
					.count;
				const imageUrls = uniqueStrings(
					group.items
						.map((item) => item.url)
						.filter((url) => isHttpUrl(url) && !isLikelyThumbnailUrl(url)),
					{ limit: PRE_TTS_VISUAL_GROUNDING_IMAGE_LIMIT },
				);
				return {
					id: `T${topicIndex + 1}V${idx + 1}`,
					topicIndex,
					topic: label,
					query,
					sourceQuery: sanitizeOverlayQuery(group.sourceQuery || query),
					imageCount: group.items.length,
					imageUrls,
					titleClues: group.titles.slice(0, 4),
					sourceHosts: [...group.hosts].slice(0, 4),
					score:
						group.items.length +
						group.titles.length * 1.5 +
						titleMatches * 2 +
						queryMatches,
				};
			})
			.filter(
				(beat) =>
					beat.imageCount > 0 &&
					beat.query &&
					tokenizeLabel(beat.query).length >= 2,
			)
			.sort((a, b) => b.score - a.score)
			.slice(0, targetPerTopic)
			.map((beat, idx) => ({
				...beat,
				id: `T${topicIndex + 1}V${idx + 1}`,
			}));

		topic.trendStory = {
			...story,
			visualBeatPlan: beats,
			visualResearch: {
				...(story.visualResearch || {}),
				beats: beats.map((beat) => ({
					id: beat.id,
					query: beat.query,
					sourceQuery: beat.sourceQuery,
					imageCount: beat.imageCount,
					titleClues: beat.titleClues,
					sourceHosts: beat.sourceHosts,
				})),
			},
		};
		allPlans.push({
			topicIndex,
			topic: label,
			beats,
		});
		if (beats.length) {
			logJob(jobId, "pre-script visual beat plan", {
				topic: label,
				beats: beats.length,
				queries: beats.map((beat) => beat.query).slice(0, 8),
				imageCandidates: beats.reduce(
					(sum, beat) => sum + Number(beat.imageCount || 0),
					0,
				),
			});
		}
	}
	return allPlans;
}

async function enrichPreScriptVisualBeatsWithFeedVideo({
	topics = [],
	topicContexts = [],
	category = "",
	jobId = null,
} = {}) {
	if (
		!PRE_SCRIPT_VISUAL_VIDEO_BEAT_PROBE_ENABLED ||
		!FEED_VIDEO_ENABLED ||
		PRE_SCRIPT_VISUAL_VIDEO_BEAT_PROBE_LIMIT <= 0
	) {
		return { probed: 0, videoCandidateBeats: 0, videoCandidates: 0 };
	}
	let probed = 0;
	let videoCandidateBeats = 0;
	let videoCandidates = 0;
	const summary = [];
	for (let topicIndex = 0; topicIndex < (topics || []).length; topicIndex += 1) {
		const topic = topics[topicIndex] || {};
		const beats = Array.isArray(topic.trendStory?.visualBeatPlan)
			? topic.trendStory.visualBeatPlan
			: [];
		if (!beats.length) continue;
		const meta = buildVisualGroundingTopicMeta(topic, topicContexts, topicIndex);
		const topicLabel =
			String(topic.displayTopic || topic.topic || "").trim() || meta.label || "";
		if (
			isEvergreenNonNewsVisualTopic({
				category,
				topicLabel,
				text: topicLabel,
			})
		) {
			continue;
		}
		const topicTokens = topicTokensFromTitle(topicLabel);
		for (const beat of beats) {
			if (probed >= PRE_SCRIPT_VISUAL_VIDEO_BEAT_PROBE_LIMIT) break;
			const query = sanitizeOverlayQuery(beat.query || beat.sourceQuery || "");
			if (!query) continue;
			probed += 1;
			const queryVariants = uniqueStrings(
				[query, beat.sourceQuery, topicLabel].filter(Boolean),
				{ limit: Math.max(2, FEED_VIDEO_QUERY_LIMIT) },
			);
			const queryTokens = filterSpecificTopicTokens(tokenizeLabel(query)).slice(
				0,
				16,
			);
			const segmentTokens = filterSegmentImageMatchTokens(tokenizeLabel(query))
				.filter((tok) => !topicTokens.includes(tok))
				.slice(0, 4);
			const candidates = await collectFeedVideoCandidateEntriesForSegment({
				query,
				topicLabel,
				queryVariants,
				topicTokens,
				segmentTokens,
				queryTokens,
				meta: {
					...meta,
					segmentText: query,
				},
				category,
				jobId,
			});
			beat.videoCandidateCount = candidates.length;
			beat.videoTitleClues = candidates
				.map((candidate) => cleanTopicLabel(candidate.title || candidate.snippet || ""))
				.filter(Boolean)
				.slice(0, 3);
			if (candidates.length) {
				videoCandidateBeats += 1;
				videoCandidates += candidates.length;
				mergeTopicPotentialVideos(topic, candidates);
			}
			summary.push({
				topic: topicLabel,
				beatId: beat.id,
				query,
				videoCandidates: candidates.length,
			});
		}
	}
	if (probed) {
		logJob(jobId, "pre-script visual video beat probe", {
			probed,
			videoCandidateBeats,
			videoCandidates,
			beats: summary.slice(0, 12),
		});
	}
	return { probed, videoCandidateBeats, videoCandidates, beats: summary };
}

function buildVisualBeatPlanPromptLines(topics = []) {
	const lines = [];
	for (let i = 0; i < (topics || []).length; i += 1) {
		const topic = topics[i] || {};
		const label =
			String(topic.displayTopic || topic.topic || "").trim() ||
			`Topic ${i + 1}`;
		const beats = Array.isArray(topic.trendStory?.visualBeatPlan)
			? topic.trendStory.visualBeatPlan
			: [];
		if (!beats.length) {
			lines.push(`Topic ${i + 1} (${label}): no validated visual beats.`);
			continue;
		}
		const beatLines = beats
			.slice(0, 12)
			.map((beat) => {
				const titles = Array.isArray(beat.titleClues)
					? beat.titleClues.slice(0, 2).join(" | ")
					: "";
				const hosts = Array.isArray(beat.sourceHosts)
					? beat.sourceHosts.slice(0, 2).join(", ")
					: "";
				return `- ${beat.id}: query="${beat.query}" images=${
					beat.imageCount || 0
				}${
					Number(beat.videoCandidateCount || 0)
						? ` videos=${beat.videoCandidateCount}`
						: ""
				}${hosts ? ` hosts=${hosts}` : ""}${
					titles ? ` title clues: ${titles}` : ""
				}`;
			})
			.join("\n");
		lines.push(`Topic ${i + 1} (${label}):\n${beatLines}`);
	}
	return lines.join("\n\n") || "- (none)";
}

function rssText(value) {
	if (value === undefined || value === null) return "";
	if (typeof value === "string" || typeof value === "number")
		return String(value);
	if (Array.isArray(value)) return rssText(value[0]);
	if (typeof value === "object") {
		if (value._ !== undefined) return rssText(value._);
		if (value.$?.url) return rssText(value.$.url);
	}
	return "";
}

function extractArticleUrlFromNewsLink(raw = "") {
	const link = String(raw || "").trim();
	if (!/^https?:\/\//i.test(link)) return "";
	try {
		const parsed = new URL(link);
		const direct = parsed.searchParams.get("url") || parsed.searchParams.get("u");
		if (direct && /^https?:\/\//i.test(direct)) return direct;
		if (/^news\.google\.com$/i.test(parsed.hostname)) return "";
	} catch {}
	return link;
}

async function fetchRssArticleUrls({
	endpoint,
	params = {},
	query,
	topicLabel,
	limit = NEWS_IMAGE_FALLBACK_LIMIT,
	jobId,
	label = "news",
}) {
	const q = sanitizeOverlayQuery(query || topicLabel || "");
	if (!endpoint || !q) return [];
	const topicTokens = filterSpecificTopicTokens(
		topicTokensFromTitle(topicLabel || query || ""),
	);
	const requiredTopicMatches = minImageTopicTokenMatches(topicTokens);
	try {
		const { data } = await axios.get(endpoint, {
			params,
			timeout: NEWS_RSS_TIMEOUT_MS,
			maxContentLength: 768 * 1024,
			maxBodyLength: 768 * 1024,
			headers: { "User-Agent": "agentai-long-video/2.0" },
			validateStatus: (s) => s >= 200 && s < 400,
		});
		const parsed = await xml2js.parseStringPromise(String(data || ""), {
			explicitArray: false,
			trim: true,
		});
		const rawItems =
			parsed?.rss?.channel?.item ||
			parsed?.feed?.entry ||
			parsed?.channel?.item ||
			[];
		const items = Array.isArray(rawItems)
			? rawItems
			: rawItems
				? [rawItems]
				: [];
		const urls = [];
		for (const item of items) {
			const title = rssText(item?.title);
			const link =
				rssText(item?.link?.href) || rssText(item?.link) || rssText(item?.id);
			const source =
				rssText(item?.source?.$?.url) ||
				rssText(item?.source?.url) ||
				rssText(item?.source);
			const description =
				rssText(item?.description) || rssText(item?.summary) || "";
			const fields = [title, link, source, description];
			if (
				requiredTopicMatches &&
				topicMatchInfo(topicTokens, fields).count < requiredTopicMatches
			) {
				continue;
			}
			const url = extractArticleUrlFromNewsLink(link);
			if (!isHttpUrl(url)) continue;
			urls.push(url);
			if (urls.length >= limit) break;
		}
		const unique = uniqueStrings(urls, { limit });
		if (jobId)
			logJob(jobId, "news rss article candidates", {
				source: label,
				query: q,
				count: unique.length,
			});
		return unique;
	} catch (e) {
		if (jobId)
			logJob(jobId, "news rss article fetch failed", {
				source: label,
				query: q,
				error: e.message,
			});
		return [];
	}
}

function cleanRssText(value = "", maxLen = 260) {
	return normalizeWhitespace(
		String(value || "")
			.replace(/<[^>]+>/g, " ")
			.replace(/&amp;|&#38;|&#038;/gi, "&")
			.replace(/&quot;|&#34;/gi, '"')
			.replace(/&#39;|&apos;/gi, "'")
			.replace(/&nbsp;/gi, " "),
	).slice(0, Math.max(20, Number(maxLen) || 260));
}

async function fetchRssArticleContextItems({
	endpoint,
	params = {},
	query,
	topicLabel,
	limit = PROMPT_TOPIC_NEWS_CONTEXT_LIMIT,
	jobId,
	label = "news",
}) {
	const q = sanitizeOverlayQuery(query || topicLabel || "");
	if (!endpoint || !q) return [];
	const target = clampNumber(Number(limit) || PROMPT_TOPIC_NEWS_CONTEXT_LIMIT, 1, 12);
	const topicTokens = filterSpecificTopicTokens(
		topicTokensFromTitle(topicLabel || query || ""),
	);
	const requiredTopicMatches = minImageTopicTokenMatches(topicTokens);
	try {
		const { data } = await axios.get(endpoint, {
			params,
			timeout: NEWS_RSS_TIMEOUT_MS,
			maxContentLength: 768 * 1024,
			maxBodyLength: 768 * 1024,
			headers: { "User-Agent": "agentai-long-video/2.0" },
			validateStatus: (s) => s >= 200 && s < 400,
		});
		const parsed = await xml2js.parseStringPromise(String(data || ""), {
			explicitArray: false,
			trim: true,
		});
		const rawItems =
			parsed?.rss?.channel?.item ||
			parsed?.feed?.entry ||
			parsed?.channel?.item ||
			[];
		const items = Array.isArray(rawItems)
			? rawItems
			: rawItems
				? [rawItems]
				: [];
		const contextItems = [];
		for (const item of items) {
			const title = cleanRssText(rssText(item?.title), 180);
			const rawLink =
				rssText(item?.link?.href) || rssText(item?.link) || rssText(item?.id);
			const source =
				rssText(item?.source?.$?.url) ||
				rssText(item?.source?.url) ||
				rssText(item?.source);
			const description = cleanRssText(
				rssText(item?.description) || rssText(item?.summary) || "",
				220,
			);
			const fields = [title, rawLink, source, description];
			if (
				requiredTopicMatches &&
				topicMatchInfo(topicTokens, fields).count < requiredTopicMatches
			) {
				continue;
			}
			const url =
				extractArticleUrlFromNewsLink(rawLink) ||
				extractArticleUrlFromNewsLink(source);
			if (!isHttpUrl(url)) continue;
			const sourceName = isHttpUrl(source)
				? getUrlHost(source)
				: cleanRssText(source, 80);
			const snippet = normalizeWhitespace(
				[description, sourceName ? `Source: ${sourceName}` : ""]
					.filter(Boolean)
					.join(" "),
			);
			contextItems.push({
				title,
				snippet,
				link: url,
				source: label,
			});
			if (contextItems.length >= target) break;
		}
		const unique = uniqueContextItems(contextItems, { limit: target });
		if (jobId)
			logJob(jobId, "prompt news context candidates", {
				source: label,
				query: q,
				count: unique.length,
			});
		return unique;
	} catch (e) {
		if (jobId)
			logJob(jobId, "prompt news context fetch failed", {
				source: label,
				query: q,
				error: e.message,
			});
		return [];
	}
}

async function fetchPromptTopicNewsContext({
	topic,
	searchHints = [],
	promptText = "",
	limit = PROMPT_TOPIC_NEWS_CONTEXT_LIMIT,
	jobId,
	forceAllQueries = false,
} = {}) {
	if (!PROMPT_TOPIC_NEWS_CONTEXT_ENABLED) return [];
	const topicLabel = cleanTopicLabel(topic || promptText);
	if (!topicLabel) return [];
	const target = clampNumber(Number(limit) || PROMPT_TOPIC_NEWS_CONTEXT_LIMIT, 1, 12);
	const queries = uniqueStrings(
		[
			topicLabel,
			...(Array.isArray(searchHints) ? searchHints : []),
			`${topicLabel} latest updates`,
			`${topicLabel} latest news`,
		],
		{ limit: PROMPT_TOPIC_NEWS_QUERY_LIMIT },
	);
	const out = [];
	for (const q of queries) {
		const searches = [
			fetchRssArticleContextItems({
				endpoint: "https://news.google.com/rss/search",
				params: { q, hl: "en-US", gl: "US", ceid: "US:en" },
				query: q,
				topicLabel,
				limit: target,
				jobId,
				label: "google_news",
			}),
			fetchRssArticleContextItems({
				endpoint: "https://www.bing.com/news/search",
				params: { q, format: "rss", mkt: "en-US" },
				query: q,
				topicLabel,
				limit: target,
				jobId,
				label: "bing_news",
			}),
		];
		const settled = await Promise.allSettled(searches);
		for (const result of settled) {
			if (result.status === "fulfilled") out.push(...(result.value || []));
		}
		if (!forceAllQueries && countContextSourceLinks(out) >= target) break;
	}
	return uniqueContextItems(out, { limit: target });
}

async function fetchNewsArticleUrlsForImages({
	query,
	topicLabel,
	limit = NEWS_IMAGE_FALLBACK_LIMIT,
	jobId,
} = {}) {
	if (!NEWS_IMAGE_FALLBACK_ENABLED) return [];
	const q = sanitizeOverlayQuery(query || topicLabel || "");
	if (!q) return [];
	const target = clampNumber(Number(limit) || NEWS_IMAGE_FALLBACK_LIMIT, 1, 16);
	const searches = [
		fetchRssArticleUrls({
			endpoint: "https://news.google.com/rss/search",
			params: { q, hl: "en-US", gl: "US", ceid: "US:en" },
			query: q,
			topicLabel,
			limit: target,
			jobId,
			label: "google_news",
		}),
		fetchRssArticleUrls({
			endpoint: "https://www.bing.com/news/search",
			params: { q, format: "rss", mkt: "en-US" },
			query: q,
			topicLabel,
			limit: target,
			jobId,
			label: "bing_news",
		}),
	];
	const settled = await Promise.allSettled(searches);
	const urls = [];
	for (const result of settled) {
		if (result.status === "fulfilled") urls.push(...(result.value || []));
	}
	return uniqueStrings(urls, { limit: target });
}

async function fetchCseContext(topic, extraTokens = [], opts = {}) {
	if (!topic) return [];
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const baseTokens = [...topicTokensFromTitle(topic), ...extra];
	const category = inferEntertainmentCategory(baseTokens);
	const queries = [
		`${topic}`,
		`${topic} latest news`,
		`${topic} latest updates`,
		`${topic} current`,
		`${topic} trending`,
		`${topic} explained`,
		`${topic} controversy`,
		`${topic} facts`,
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
			`${topic} ending`,
		);
	} else if (category === "music") {
		queries.push(`${topic} chart`, `${topic} music video`, `${topic} tour`);
	} else if (category === "celebrity") {
		queries.push(
			`${topic} interview`,
			`${topic} controversy`,
			`${topic} social media`,
		);
	}
	if (category === "film" || category === "tv" || category === "celebrity") {
		queries.push(`${topic} rumor`, `${topic} leak`);
	}
	const queryPool = uniqueStrings(
		[
			...(Array.isArray(opts?.queries) ? opts.queries : []),
			...queries,
		],
		{ limit: queries.length + 12 },
	);

	const maxQueries = clampNumber(
		opts?.maxQueries ?? queryPool.length,
		1,
		queryPool.length,
	);
	const num = clampNumber(opts?.num ?? 5, 1, 10);
	const maxPages = clampNumber(opts?.maxPages ?? 2, 1, 5);
	const limit = clampNumber(opts?.limit ?? 6, 1, 12);
	const items = await fetchCseItems(queryPool.slice(0, maxQueries), {
		num,
		maxPages,
		jobId: opts?.jobId || null,
		label: "cse_context",
	});
	const matchTokens = expandTopicTokens(filterSpecificTopicTokens(baseTokens));
	const minMatches = minTopicTokenMatches(matchTokens);
	return items
		.filter(
			(it) =>
				topicMatchInfo(matchTokens, [it.title, it.snippet, it.link]).count >=
				minMatches,
		)
		.slice(0, limit);
}

async function fetchCseImages(
	topic,
	extraTokens = [],
	jobId = null,
	opts = {},
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
		CSE_MAX_IMAGE_RESULTS,
	);
	const maxPages = clampNumber(Number(opts.maxPages) || CSE_MAX_PAGES, 1, 5);
	const relaxedMinEdge = clampNumber(
		Number(opts.relaxedMinEdge) || CSE_RELAXED_MIN_IMAGE_SHORT_EDGE,
		200,
		CSE_MIN_IMAGE_SHORT_EDGE,
	);
	const requestSize = Math.min(
		CSE_MAX_IMAGE_RESULTS,
		Math.max(12, maxResults * IMAGE_SEARCH_CANDIDATE_MULTIPLIER),
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
			`${searchLabel} premiere`,
		);
	} else if (category === "tv") {
		queries.unshift(
			`${searchLabel} episode still`,
			`${searchLabel} cast photo`,
		);
	} else if (category === "music") {
		queries.unshift(
			`${searchLabel} live performance`,
			`${searchLabel} stage photo`,
		);
	} else if (category === "celebrity") {
		queries.unshift(
			`${searchLabel} red carpet`,
			`${searchLabel} interview photo`,
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
		jobId,
		label: "cse_images_primary_ultra",
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
			jobId,
			label: "cse_images_primary_preferred",
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
			jobId,
			label: "cse_images_fallback_preferred",
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
			jobId,
			label: "cse_images_fallback_large",
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
			urlText.includes(tok),
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
		if (isDisfavoredImageSourceUrl(c.url)) continue;
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

function decodeHtmlEntitiesLite(value = "") {
	return String(value || "")
		.replace(/&amp;|&#38;|&#038;/gi, "&")
		.replace(/&quot;|&#34;|&#034;/gi, '"')
		.replace(/&#39;|&#039;|&apos;/gi, "'")
		.replace(/&lt;|&#60;|&#060;/gi, "<")
		.replace(/&gt;|&#62;|&#062;/gi, ">")
		.replace(/\\\//g, "/")
		.replace(/\\u002f/gi, "/")
		.replace(/\\u003a/gi, ":")
		.replace(/\\u0026/gi, "&");
}

function sanitizeFeedVideoUrl(raw = "", baseUrl = "") {
	let value = decodeHtmlEntitiesLite(raw).trim();
	if (!value) return "";
	value = value.replace(/[),.;]+$/g, "");
	if (!/^https?:\/\//i.test(value) && /%3a%2f%2f/i.test(value)) {
		try {
			value = decodeURIComponent(value);
		} catch {}
	}
	try {
		const resolved = new URL(value, baseUrl || undefined);
		if (!/^https?:$/i.test(resolved.protocol)) return "";
		return resolved.toString();
	} catch {
		return "";
	}
}

function normalizeFeedVideoKey(url = "") {
	try {
		const parsed = new URL(sanitizeFeedVideoUrl(url));
		parsed.hash = "";
		for (const key of [
			"utm_source",
			"utm_medium",
			"utm_campaign",
			"utm_term",
			"utm_content",
			"fbclid",
			"gclid",
		]) {
			parsed.searchParams.delete(key);
		}
		return parsed.toString().toLowerCase();
	} catch {
		return sanitizeFeedVideoUrl(url).split("#")[0].toLowerCase();
	}
}

function isProbablyDirectVideoUrl(url = "") {
	const raw = String(url || "").toLowerCase();
	return /\.(mp4|m4v|mov|webm)(?:[?#]|$)/i.test(raw);
}

function isUnsupportedFeedVideoHost(url = "") {
	const host = getUrlHost(url);
	return /\b(youtube\.com|youtu\.be|tiktok\.com|instagram\.com|facebook\.com|fb\.watch|twitter\.com|x\.com|threads\.net|snapchat\.com|etsy\.com|amazon\.com|ebay\.com|pinterest\.com|shopify\.com|redbubble\.com|zazzle\.com)\b/i.test(
		host,
	);
}

function isKnownBlockedFeedVideoPage(url = "") {
	const raw = String(url || "");
	if (!raw || isProbablyDirectVideoUrl(raw)) return false;
	let host = "";
	let pathAndQuery = "";
	try {
		const parsed = new URL(raw);
		host = parsed.hostname || "";
		pathAndQuery = `${parsed.pathname || ""}?${parsed.search || ""}`;
	} catch {
		host = getUrlHost(raw);
		pathAndQuery = raw;
	}
	if (
		/\b(stock\.adobe\.com|vecteezy\.com|storyblocks\.com|envato\.com|shutterstock\.com|istockphoto\.com|gettyimages\.com|depositphotos\.com|videvo\.net)\b/i.test(
			host,
		)
	)
		return true;
	if (
		/\b(pexels\.com|pixabay\.com)\b/i.test(host) &&
		/(?:\/search\/|\/search\b|[?&]q=|[?&]query=)/i.test(pathAndQuery)
	)
		return true;
	if (/\b(stock-footage|stock-video|stock_video|stockvideo)\b/i.test(pathAndQuery))
		return true;
	return false;
}

function isDisfavoredFeedVideoSourceUrl(url = "") {
	const raw = String(url || "");
	if (!raw) return true;
	if (isUnsupportedFeedVideoHost(raw)) return true;
	if (isKnownBlockedFeedVideoPage(raw)) return true;
	if (/\.(m3u8|mpd)(?:[?#]|$)/i.test(raw)) return true;
	if (/\b(ad|ads|promo|sponsored|thumbnail|poster|sprite|preview|watermark|stock-footage|stock_video|stockvideo)\b/i.test(raw))
		return true;
	if (isDisfavoredImageSourceUrl(raw)) return true;
	return false;
}

function feedVideoSourceTrustScore(url = "") {
	const host = getUrlHost(url);
	if (!host) return 0;
	let score = 0;
	if (/\.(gov|mil)$/i.test(host)) score += 6;
	if (/\b(courts?|supremecourt|judicial|sheriff|police|city|county|state)\b/i.test(host))
		score += 3;
	if (/\b(apnews|reuters|pbs|c-span|cspan|cnn|nbcnews|cbsnews|abcnews|foxnews|usatoday|npr|bbc|washingtonpost|nytimes)\./i.test(host))
		score += 2;
	if (/\b(vimeo\.com|dailymotion\.com)\b/i.test(host)) score -= 2;
	if (isUnsupportedFeedVideoHost(url)) score -= 20;
	if (isDisfavoredFeedVideoSourceUrl(url)) score -= 10;
	return score;
}

function extractOpenGraphVideoUrls(html = "", baseUrl = "") {
	const urls = [];
	const push = (raw) => {
		const url = sanitizeFeedVideoUrl(raw, baseUrl);
		if (url) urls.push(url);
	};
	const metaTags = String(html || "").match(/<meta[^>]+>/gi) || [];
	const priority = [
		"og:video:secure_url",
		"og:video:url",
		"og:video",
		"twitter:player:stream",
		"twitter:player",
	];
	for (const key of priority) {
		for (const tag of metaTags) {
			const attrs = parseMetaAttributes(tag);
			const prop = attrs.property || attrs.name || attrs.itemprop || "";
			if (!prop || prop.toLowerCase() !== key) continue;
			if (attrs.content) push(attrs.content);
		}
	}

	const itempropRe =
		/<(?:meta|link)[^>]+itemprop=["'](?:contentUrl|embedUrl|url)["'][^>]+>/gi;
	let itemMatch = null;
	while ((itemMatch = itempropRe.exec(String(html || "")))) {
		const attrs = parseMetaAttributes(itemMatch[0]);
		push(attrs.content || attrs.href || "");
	}

	const jsonLdRe =
		/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
	let jsonMatch = null;
	while ((jsonMatch = jsonLdRe.exec(String(html || "")))) {
		const rawJson = decodeHtmlEntitiesLite(jsonMatch[1] || "").trim();
		if (!rawJson) continue;
		try {
			const parsed = JSON.parse(rawJson);
			const stack = Array.isArray(parsed) ? parsed.slice() : [parsed];
			while (stack.length) {
				const item = stack.shift();
				if (!item || typeof item !== "object") continue;
				const typeText = Array.isArray(item["@type"])
					? item["@type"].join(" ")
					: String(item["@type"] || "");
				if (/VideoObject/i.test(typeText)) {
					push(item.contentUrl || item.embedUrl || item.url || "");
					if (Array.isArray(item.associatedMedia)) {
						for (const media of item.associatedMedia) {
							if (media && typeof media === "object")
								push(media.contentUrl || media.embedUrl || media.url || "");
						}
					}
				}
				for (const val of Object.values(item)) {
					if (Array.isArray(val)) stack.push(...val);
					else if (val && typeof val === "object") stack.push(val);
				}
			}
		} catch {}
	}

	return uniqueStrings(urls, { limit: FEED_VIDEO_CANDIDATE_LIMIT });
}

function extractVideoUrlsFromPagemap(pagemap = null) {
	if (!pagemap || typeof pagemap !== "object") return [];
	const urls = [];
	const push = (raw) => {
		const url = sanitizeFeedVideoUrl(raw);
		if (url) urls.push(url);
	};
	const videoObjects = Array.isArray(pagemap.videoobject)
		? pagemap.videoobject
		: [];
	for (const item of videoObjects) {
		if (!item || typeof item !== "object") continue;
		push(item.contenturl || item.contentUrl);
		push(item.embedurl || item.embedUrl);
		push(item.url);
	}
	const metaTags = Array.isArray(pagemap.metatags) ? pagemap.metatags : [];
	for (const meta of metaTags) {
		if (!meta || typeof meta !== "object") continue;
		push(meta["og:video:secure_url"]);
		push(meta["og:video:url"]);
		push(meta["og:video"]);
		push(meta["twitter:player:stream"]);
		push(meta["twitter:player"]);
	}
	return uniqueStrings(urls, { limit: FEED_VIDEO_CANDIDATE_LIMIT });
}

async function fetchOpenGraphVideoUrls(pageUrl, timeoutMs = FEED_VIDEO_PAGE_TIMEOUT_MS) {
	try {
		if (!isHttpUrl(pageUrl) || isUnsupportedFeedVideoHost(pageUrl)) return [];
		const res = await axios.get(pageUrl, {
			timeout: timeoutMs,
			maxContentLength: 1024 * 1024,
			maxBodyLength: 1024 * 1024,
			headers: { "User-Agent": "agentai-long-video/2.0" },
			validateStatus: (s) => s >= 200 && s < 400,
		});
		const html = String(res.data || "");
		if (!html) return [];
		return extractOpenGraphVideoUrls(html, pageUrl);
	} catch {
		return [];
	}
}

function extractBingRedirectTarget(rawUrl = "") {
	const url = sanitizeFeedVideoUrl(rawUrl);
	if (!url) return "";
	try {
		const parsed = new URL(url);
		if (!/bing\.com$/i.test(parsed.hostname.replace(/^www\./i, ""))) return url;
		const encoded = parsed.searchParams.get("u") || parsed.searchParams.get("url");
		if (!encoded) return "";
		let clean = encoded;
		if (/^a1/i.test(clean)) clean = clean.slice(2);
		try {
			clean = Buffer.from(clean, "base64").toString("utf8");
		} catch {}
		return sanitizeFeedVideoUrl(clean);
	} catch {
		return url;
	}
}

async function fetchBingVideoCandidates(query, { limit = 8, jobId = null } = {}) {
	if (!BING_FEED_VIDEO_SEARCH_ENABLED || !query) return [];
	try {
		const { data } = await axios.get("https://www.bing.com/videos/search", {
			params: { q: query, mkt: "en-US", safeSearch: "Strict" },
			timeout: FEED_VIDEO_PAGE_TIMEOUT_MS,
			maxContentLength: 1024 * 1024,
			maxBodyLength: 1024 * 1024,
			headers: { "User-Agent": "agentai-long-video/2.0" },
			validateStatus: (s) => s >= 200 && s < 500,
		});
		const html = decodeHtmlEntitiesLite(String(data || ""));
		const found = [];
		const push = (raw, sourceType = "bing-video") => {
			const resolved = extractBingRedirectTarget(raw) || sanitizeFeedVideoUrl(raw);
			if (!resolved || !isHttpUrl(resolved)) return;
			found.push({ url: resolved, pageUrl: resolved, sourceType, query });
		};
		const jsonUrlRe =
			/"(?:murl|contentUrl|contenturl|hostPageUrl|purl|webSearchUrl)"\s*:\s*"([^"]+)"/gi;
		let match = null;
		while ((match = jsonUrlRe.exec(html))) push(match[1]);
		const hrefRe = /<a[^>]+href=["']([^"']+)["']/gi;
		while ((match = hrefRe.exec(html))) {
			const href = match[1];
			if (/\/videos\/search/i.test(href)) continue;
			push(href, "bing-video-page");
		}
		const directRe = /https?:\/\/[^"'<>\\\s]+?\.(?:mp4|m4v|mov|webm)(?:[?#][^"'<>\\\s]*)?/gi;
		while ((match = directRe.exec(html))) push(match[0], "bing-video-direct");
		const out = [];
		const seen = new Set();
		for (const item of found) {
			const key = normalizeFeedVideoKey(item.url);
			if (!key || seen.has(key)) continue;
			seen.add(key);
			out.push(item);
			if (out.length >= limit) break;
		}
		return out;
	} catch (e) {
		if (jobId)
			logJob(jobId, "bing feed video search failed", {
				query,
				error: e.message,
			});
		return [];
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

const VISUAL_QUERY_BOUNDARY_STOP_TOKENS = new Set([
	"a",
	"an",
	"and",
	"for",
	"is",
	"of",
	"or",
	"please",
	"subscribe",
	"the",
	"to",
	"your",
]);

function sanitizeOverlayQuery(query = "") {
	const cleaned = String(query || "")
		.replace(/[^a-z0-9\s]/gi, " ")
		.replace(/\s+/g, " ")
		.trim()
		.slice(0, 120);
	const tokens = cleaned.split(/\s+/).filter(Boolean);
	while (
		tokens.length > 1 &&
		VISUAL_QUERY_BOUNDARY_STOP_TOKENS.has(tokens[tokens.length - 1].toLowerCase())
	) {
		tokens.pop();
	}
	while (
		tokens.length > 1 &&
		VISUAL_QUERY_BOUNDARY_STOP_TOKENS.has(tokens[0].toLowerCase())
	) {
		tokens.shift();
	}
	const normalized = tokens.join(" ").trim();
	if (normalized.length <= 80) return normalized;
	const clipped = normalized.slice(0, 80);
	const lastSpace = clipped.lastIndexOf(" ");
	return (lastSpace >= 48 ? clipped.slice(0, lastSpace) : clipped).trim();
}

function mergeImageQueryTerms(primary = "", secondary = "") {
	const left = sanitizeOverlayQuery(primary);
	const right = sanitizeOverlayQuery(secondary);
	if (!left) return right;
	if (!right) return left;
	const leftTokens = left.split(/\s+/).filter(Boolean);
	const rightTokens = right.split(/\s+/).filter(Boolean);
	if (!leftTokens.length) return right;
	if (!rightTokens.length) return left;
	const leftLower = leftTokens.map((t) => t.toLowerCase());
	const rightLower = rightTokens.map((t) => t.toLowerCase());
	if (rightLower.every((t) => leftLower.includes(t))) return left;
	if (leftLower.every((t) => rightLower.includes(t))) return right;
	let overlap = Math.min(leftTokens.length, rightTokens.length);
	while (overlap > 0) {
		const leftSuffix = leftLower.slice(leftLower.length - overlap).join(" ");
		const rightPrefix = rightLower.slice(0, overlap).join(" ");
		if (leftSuffix === rightPrefix) {
			return sanitizeOverlayQuery(
				[...leftTokens, ...rightTokens.slice(overlap)].join(" "),
			);
		}
		overlap -= 1;
	}
	return sanitizeOverlayQuery(`${left} ${right}`);
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

function ensureCompactTopicInQuery(query = "", topicLabel = "") {
	const base = sanitizeOverlayQuery(query);
	const topic = sanitizeOverlayQuery(topicLabel);
	if (!topic) return base;
	if (!base) return compactVisualSearchQuery(topic, topic, topic);
	const baseTokens = filterSegmentImageMatchTokens(tokenizeLabel(base));
	const topicTokens = filterSegmentImageMatchTokens(tokenizeLabel(topic));
	const hasTopicToken = topicTokens.some((t) => baseTokens.includes(t));
	if (hasTopicToken || baseTokens.length >= 3) {
		return compactVisualSearchQuery(base, topic, base);
	}
	const topicPrefix = topicTokens.slice(0, 2).join(" ");
	return compactVisualSearchQuery(
		[topicPrefix, base].filter(Boolean).join(" "),
		topic,
		ensureTopicInQuery(base, topic),
	);
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

const SEGMENT_IMAGE_STOP_TOKENS = new Set([
	...TOPIC_STOP_WORDS,
	...GENERIC_TOPIC_TOKENS,
	"actually",
	"accept",
	"accepted",
	"accepts",
	"another",
	"argument",
	"attention",
	"basic",
	"before",
	"because",
	"being",
	"blame",
	"blinks",
	"both",
	"breakthrough",
	"broader",
	"came",
	"careful",
	"carefully",
	"channel",
	"classic",
	"clear",
	"clearly",
	"confidence",
	"contained",
	"context",
	"constructive",
	"converging",
	"could",
	"cut",
	"debate",
	"deal",
	"detail",
	"details",
	"difference",
	"designed",
	"doubting",
	"early",
	"emergency",
	"enough",
	"enforcement",
	"enter",
	"exists",
	"explained",
	"fail",
	"finished",
	"gives",
	"genuine",
	"happening",
	"happened",
	"here",
	"holds",
	"immediate",
	"important",
	"improves",
	"limited",
	"keeps",
	"latest",
	"layer",
	"leverage",
	"matters",
	"meaning",
	"message",
	"messages",
	"moment",
	"moving",
	"narrower",
	"nearly",
	"now",
	"off",
	"offer",
	"people",
	"possible",
	"pressure",
	"provided",
	"public",
	"question",
	"quick",
	"quietly",
	"received",
	"sequencing",
	"really",
	"reason",
	"reasons",
	"report",
	"reported",
	"reporting",
	"reports",
	"response",
	"right",
	"says",
	"sequence",
	"signals",
	"simple",
	"skepticism",
	"sound",
	"spiked",
	"start",
	"started",
	"starts",
	"still",
	"story",
	"stress",
	"structured",
	"substance",
	"suggests",
	"surged",
	"takeaway",
	"terms",
	"testing",
	"tension",
	"thing",
	"theory",
	"timeline",
	"today",
	"trust",
	"two",
	"unclear",
	"unresolved",
	"update",
	"verification",
	"way",
	"ways",
	"want",
	"wanted",
	"wants",
	"what",
	"whether",
	"while",
	"wide",
	"yet",
]);

function filterSegmentImageMatchTokens(tokens = []) {
	return filterSpecificTopicTokens(normalizeTopicTokens(tokens)).filter(
		(t) => t && !SEGMENT_IMAGE_STOP_TOKENS.has(t),
	);
}

function buildOverlayQueryFallback(text = "", topic = "") {
	const base = compactVisualSearchQuery(cleanTopicCandidate(topic), topic, topic);
	const tokens = filterSegmentImageMatchTokens(tokenizeLabel(text)).slice(0, 4);
	const extras = tokens.filter(
		(t) => !base.toLowerCase().includes(String(t || "").toLowerCase()),
	);
	const parts = [base, ...extras].filter(Boolean);
	return ensureCompactTopicInQuery(parts.join(" "), topic);
}

function cleanImageQueryHint(hint = "", topicLabel = "") {
	const topicTokens = new Set(tokenizeLabel(topicLabel || ""));
	const cleaned = String(hint || "")
		.replace(/[’‘]/g, "'")
		.replace(/[“”]/g, '"')
		.replace(/[●•].*$/g, " ")
		.replace(/\b\d+\s*(?:minute|minutes|hour|hours|day|days|week|weeks)\s+ago\b.*$/i, " ")
		.replace(/^live\s+updates?\s*:\s*/i, " ")
		.replace(/\s+/g, " ")
		.trim();
	const tokens = tokenizeLabel(cleaned)
		.filter((t) => topicTokens.has(t) || !SEGMENT_IMAGE_STOP_TOKENS.has(t))
		.slice(0, 9);
	if (tokens.length < 2) return "";
	return ensureCompactTopicInQuery(tokens.join(" "), topicLabel);
}

const GENERIC_NEAR_IMAGE_MODIFIERS = [
	"news photo",
	"event photo",
	"press photo",
	"official photo",
	"official portrait",
	"location photo",
];

function isEvergreenNonNewsVisualTopic({
	category = "",
	topicLabel = "",
	text = "",
} = {}) {
	const hay = `${category || ""} ${topicLabel || ""} ${text || ""}`;
	return (
		isDigitalWellbeingTopic({ categoryLabel: category, text: hay }) ||
		isSocialConnectionTopic({ categoryLabel: category, text: hay }) ||
		isPersonalFinanceCostOfLivingTopic({ categoryLabel: category, text: hay })
	);
}

function isConceptualOrMetaphoricalVisualTopic({
	category = "",
	topicLabel = "",
	text = "",
} = {}) {
	const hay = `${category || ""} ${topicLabel || ""} ${text || ""}`.toLowerCase();
	if (!hay.trim()) return false;
	if (isEvergreenNonNewsVisualTopic({ category, topicLabel, text })) return true;
	return /\b(psychology|psychological|secretly|happiness|happier|loneliness|lonely|connection|attention|habit|habits|mindset|self\s*improvement|stress|anxiety|money|spending|failure|fail|failing|mistake|mistakes|judgment|judgement|humility|cruelty|shame|empathy|motivation|advice|life)\b/i.test(
		hay,
	);
}

function buildConceptualVisualQueries({
	topicLabel = "",
	segmentText = "",
	baseQuery = "",
	category = "",
	limit = 10,
} = {}) {
	const hay = `${category || ""} ${topicLabel || ""} ${segmentText || ""} ${baseQuery || ""}`.toLowerCase();
	const queries = [];
	const push = (raw) => {
		const q = sanitizeOverlayQuery(raw);
		if (q) queries.push(q);
	};

	if (/\b(failure|fail|failing|mistake|mistakes|shame|cruel|judg|humiliat)\b/i.test(hay)) {
		if (/\b(comment|online|internet|phone|screen|social)\b/i.test(hay)) {
			push("online comments phone screen photo");
			push("person reading comments laptop photo");
		}
		if (/\b(business|startup|brand|launch|pitch|executive|customer|office)\b/i.test(hay)) {
			push("business closed sign photo");
			push("office presentation mistake photo");
			push("startup meeting stress photo");
		}
		if (/\b(relief|not me|watch|viewer|attention|reaction)\b/i.test(hay)) {
			push("person watching laptop concerned photo");
			push("viewer laptop reflection photo");
		}
		push("person looking at laptop concerned photo");
		push("business closed sign photo");
		push("messy paperwork office photo");
		push("online comments phone screen photo");
		push("empty office desk stress photo");
		push("person thinking laptop photo");
	}

	if (/\b(happiness|happier|free|spending|money|gratitude|walk|walking|park|sunlight|coffee|journal)\b/i.test(hay)) {
		push("person walking park sunlight photo");
		push("gratitude journal desk photo");
		push("quiet coffee table sunlight photo");
		push("friend phone call coffee photo");
		push("public park bench sunlight photo");
		push("helping neighbor groceries photo");
	}

	if (/\b(lonely|loneliness|friendship|connection|call|message|relationship)\b/i.test(hay)) {
		push("person looking at phone alone photo");
		push("missed calls phone screen photo");
		push("friends talking coffee photo");
		push("empty chair cafe photo");
		push("video call laptop home photo");
	}

	if (/\b(stress|anxiety|mindset|attention|habit|self\s*improvement|advice|life)\b/i.test(hay)) {
		push("person journaling desk photo");
		push("person thinking window photo");
		push("notebook coffee desk photo");
		push("calm morning desk photo");
		push("person walking city sidewalk photo");
	}

	push("person thinking laptop photo");
	push("notebook reflection desk photo");
	push("phone comments screen photo");
	push("business meeting discussion photo");

	return uniqueStrings(queries, { limit: Math.max(1, Number(limit) || 10) });
}

function buildCategoryImageQueryModifiers(category = "", topicLabel = "") {
	const hay = `${category || ""} ${topicLabel || ""}`.toLowerCase();
	if (/\b(sports?|nba|nfl|mlb|nhl|wnba|basketball|football|baseball|hockey|soccer|ufc|boxing)\b/.test(hay)) {
		return [
			"game photo",
			"team photo",
			"official team photo",
			"press conference",
		];
	}
	if (/\b(movie|film|tv|television|series|streaming|episode)\b/.test(hay)) {
		return ["official still", "cast photo", "premiere photo"];
	}
	if (/\b(music|album|song|concert|tour|artist|singer|rapper)\b/.test(hay)) {
		return ["official portrait", "performance photo", "red carpet photo"];
	}
	if (isDigitalWellbeingTopic({ categoryLabel: category, text: topicLabel })) {
		return [
			"screen time photo",
			"phone notifications photo",
			"bedtime scrolling phone",
			"phone on table photo",
			"quiet phone photo",
		];
	}
	if (isSensitiveTopicText(topicLabel)) {
		return ["official portrait", "official statement", "tribute photo"];
	}
	return [];
}

function isUsefulVisualPhraseToken(token = "", topicTokens = new Set()) {
	const t = String(token || "").toLowerCase();
	if (!t) return false;
	if (topicTokens.has(t)) return true;
	if (/^\d+$/.test(t)) return false;
	if (t.length < 3) return false;
	if (SEGMENT_IMAGE_STOP_TOKENS.has(t)) return false;
	if (TOPIC_STOP_WORDS.has(t)) return false;
	if (GENERIC_TOPIC_TOKENS.has(t)) return false;
	return true;
}

function buildDynamicVisualPhrasesFromTexts(
	texts = [],
	topicLabel = "",
	{ limit = 10 } = {},
) {
	const topicTokens = new Set(tokenizeLabel(topicLabel || ""));
	const phrases = [];
	const pushTokens = (tokens = []) => {
		const phrase = uniqueStrings(tokens, { limit: 7 }).join(" ").trim();
		if (phrase && phrase.split(/\s+/).length >= 2) phrases.push(phrase);
	};

	for (const text of texts) {
		const tokens = normalizeTopicTokens(tokenizeLabel(text || "")).filter((t) =>
			isUsefulVisualPhraseToken(t, topicTokens),
		);
		if (tokens.length < 2) continue;
		pushTokens(tokens);
		if (tokens.length > 4) pushTokens(tokens.slice(-4));
		for (let i = 0; i < tokens.length - 1 && phrases.length < limit * 3; i += 1) {
			pushTokens(tokens.slice(i, i + 3));
		}
	}

	return uniqueStrings(phrases, { limit });
}

function buildTopicNearImageQueries(
	topicLabel = "",
	{ topicKeywords = [], articleTitles = [], category = "" } = {},
) {
	const base = cleanTopicCandidate(topicLabel);
	const evergreenNonNews = isEvergreenNonNewsVisualTopic({
		category,
		topicLabel,
		text: [
			...(Array.isArray(topicKeywords) ? topicKeywords : []),
			...(Array.isArray(articleTitles) ? articleTitles : []),
		].join(" "),
	});
	const rawContextTexts = [
		topicLabel,
		...(Array.isArray(topicKeywords) ? topicKeywords : []),
		...(Array.isArray(articleTitles) ? articleTitles : []),
	].filter(Boolean);
	const queries = [];
	const push = (raw) => {
		const q = sanitizeOverlayQuery(raw);
		if (q) queries.push(q);
	};

	if (base) {
		push(base);
		if (
			isConceptualOrMetaphoricalVisualTopic({
				category,
				topicLabel,
				text: rawContextTexts.join(" "),
			})
		) {
			buildConceptualVisualQueries({
				topicLabel,
				segmentText: rawContextTexts.join(" "),
				category,
				limit: 8,
			}).forEach(push);
		}
		for (const modifier of uniqueStrings(
			[
				...(evergreenNonNews ? [] : GENERIC_NEAR_IMAGE_MODIFIERS),
				...buildCategoryImageQueryModifiers(category, topicLabel),
			],
			{ limit: 10 },
		)) {
			push(`${base} ${modifier}`);
		}
	}

	const cleanedHints = uniqueStrings(
		[
			...(Array.isArray(topicKeywords) ? topicKeywords : []),
			...(Array.isArray(articleTitles) ? articleTitles : []),
		]
			.map((hint) => cleanImageQueryHint(hint, topicLabel))
			.filter(Boolean),
		{ limit: 8 },
	);
	cleanedHints.forEach(push);
	for (const phrase of buildDynamicVisualPhrasesFromTexts(rawContextTexts, topicLabel)) {
		if (base && !phrase.toLowerCase().includes(base.toLowerCase())) {
			push(mergeImageQueryTerms(base, phrase));
		}
		push(evergreenNonNews ? `${phrase} photo` : `${phrase} news photo`);
	}

	return uniqueStrings(queries, { limit: 16 });
}

function buildCategoryVideoQueryModifiers(category = "", topicLabel = "") {
	const hay = `${category} ${topicLabel}`.toLowerCase();
	if (isEvergreenNonNewsVisualTopic({ category, topicLabel })) return [];
	const mods = ["news video", "footage", "press conference video"];
	if (
		/\b(court|trial|lawsuit|legal|crime|murder|conviction|appeal|hearing|police|sheriff|judge|jury)\b/i.test(
			hay,
		)
	) {
		mods.unshift(
			"courtroom video",
			"court hearing video",
			"trial footage",
			"courthouse video",
		);
	}
	if (/\b(sports?|nba|nfl|mlb|nhl|soccer|football|basketball|player|team)\b/i.test(hay)) {
		mods.unshift("game footage", "press conference video", "practice video");
	}
	if (/\b(movie|film|tv|series|trailer|actor|actress|show)\b/i.test(hay)) {
		mods.unshift("official clip", "trailer clip", "interview video");
	}
	if (/\b(music|song|album|tour|concert|singer|rapper|band)\b/i.test(hay)) {
		mods.unshift("performance video", "interview video", "concert footage");
	}
	return uniqueStrings(mods, { limit: 8 });
}

function buildTopicNearVideoQueries(
	topicLabel = "",
	{ topicKeywords = [], articleTitles = [], category = "" } = {},
) {
	if (
		isEvergreenNonNewsVisualTopic({
			category,
			topicLabel,
			text: [
				...(Array.isArray(topicKeywords) ? topicKeywords : []),
				...(Array.isArray(articleTitles) ? articleTitles : []),
			].join(" "),
		})
	) {
		return [];
	}
	const base = cleanTopicCandidate(topicLabel);
	const queries = [];
	const push = (raw) => {
		const q = sanitizeOverlayQuery(raw);
		if (q) queries.push(q);
	};
	if (base) {
		for (const modifier of buildCategoryVideoQueryModifiers(category, topicLabel)) {
			push(`${base} ${modifier}`);
		}
		push(`${base} video`);
	}
	const hintPhrases = uniqueStrings(
		[
			...(Array.isArray(topicKeywords) ? topicKeywords : []),
			...(Array.isArray(articleTitles) ? articleTitles : []),
		]
			.map((hint) => cleanImageQueryHint(hint, topicLabel))
			.filter(Boolean),
		{ limit: 6 },
	);
	for (const phrase of hintPhrases) {
		push(`${phrase} video`);
		push(`${phrase} footage`);
	}
	return uniqueStrings(queries, { limit: 12 });
}

function buildSegmentVideoQueryVariants({
	baseQuery = "",
	topicLabel = "",
	segmentText = "",
	topicKeywords = [],
	articleTitles = [],
	category = "",
	maxVariants = FEED_VIDEO_QUERY_LIMIT,
} = {}) {
	if (
		isEvergreenNonNewsVisualTopic({
			category,
			topicLabel,
			text: `${baseQuery || ""} ${segmentText || ""}`,
		})
	) {
		return [];
	}
	const imageLike = buildSegmentImageQueryVariants({
		baseQuery,
		topicLabel,
		segmentText,
		topicKeywords,
		articleTitles,
		category,
		maxVariants: Math.max(maxVariants, 6),
	});
	const videoNear = buildTopicNearVideoQueries(topicLabel || baseQuery, {
		topicKeywords,
		articleTitles,
		category,
	});
	const imageVideoQueries = imageLike
		.map((q) => sanitizeOverlayQuery(String(q || "").replace(/\bphotos?\b/gi, " ")))
		.filter(Boolean);
	const raw = uniqueStrings(
		[
			baseQuery ? `${baseQuery} video` : "",
			baseQuery ? `${baseQuery} footage` : "",
			...videoNear,
			...imageVideoQueries.map((q) =>
				/\b(video|footage|clip)\b/i.test(q) ? q : `${q} video`,
			),
		].filter(Boolean),
		{ limit: Math.max(2, maxVariants) },
	);
	return raw.map((q) => sanitizeOverlayQuery(q)).filter(Boolean);
}

function normalizeFeedVideoCandidateEntry(raw = {}, sourceType = "feed-video") {
	if (!raw) return null;
	const obj = typeof raw === "string" ? { url: raw } : raw;
	const url = sanitizeFeedVideoUrl(
		obj.url ||
			obj.videoUrl ||
			obj.contentUrl ||
			obj.link ||
			obj.murl ||
			obj.src ||
			"",
	);
	const pageUrl = sanitizeFeedVideoUrl(
		obj.pageUrl ||
			obj.contextLink ||
			obj.hostPageUrl ||
			obj.sourceUrl ||
			obj.link ||
			"",
	);
	const usableUrl = url || pageUrl;
	if (!usableUrl || !isHttpUrl(usableUrl)) return null;
	return {
		url: usableUrl,
		pageUrl: pageUrl || usableUrl,
		sourceType: String(obj.sourceType || sourceType || "feed-video"),
		title: String(obj.title || obj.name || "").trim().slice(0, 180),
		snippet: String(obj.snippet || obj.description || "").trim().slice(0, 260),
		query: String(obj.query || "").trim(),
	};
}

function scoreFeedVideoCandidate(entry = {}, opts = {}) {
	const url = String(entry.url || "");
	const pageUrl = String(entry.pageUrl || "");
	const fields = [
		url,
		pageUrl,
		entry.title || "",
		entry.snippet || "",
		entry.query || "",
		entry.sourceType || "",
	];
	const topicTokens = filterSpecificTopicTokens(
		normalizeTopicTokens(opts.topicTokens || []),
	);
	const segmentTokens = filterSpecificTopicTokens(
		normalizeTopicTokens(opts.segmentTokens || []),
	);
	const queryTokens = filterSpecificTopicTokens(
		normalizeTopicTokens(opts.queryTokens || []),
	);
	const combinedTokens = uniqueStrings(
		[...topicTokens, ...segmentTokens, ...queryTokens],
		{ limit: 24 },
	);
	const topicInfo = topicMatchInfo(topicTokens, fields);
	const segmentInfo = topicMatchInfo(segmentTokens, fields);
	const queryInfo = topicMatchInfo(queryTokens, fields);
	const direct = isProbablyDirectVideoUrl(url);
	const trust = Math.max(feedVideoSourceTrustScore(url), feedVideoSourceTrustScore(pageUrl));
	let score =
		topicInfo.count * 3 +
		queryInfo.count * 2 +
		segmentInfo.count * 1.5 +
		trust +
		(direct ? 2 : 0);
	if (/trend|seed|article-og/i.test(entry.sourceType || "")) score += 2;
	if (/bing-video-direct|cse-video-direct/i.test(entry.sourceType || ""))
		score += 1;
	if (combinedTokens.length && !topicInfo.count && !queryInfo.count)
		score -= 4;
	if (isDisfavoredFeedVideoSourceUrl(url)) score -= 50;
	if (FEED_VIDEO_TRUSTED_SOURCES_ONLY && trust < 1) score -= 25;
	return { score, trust, direct };
}

async function collectFeedVideoCandidateEntriesForSegment({
	query = "",
	topicLabel = "",
	queryVariants = [],
	topicTokens = [],
	segmentTokens = [],
	queryTokens = [],
	meta = {},
	category = "",
	jobId = null,
} = {}) {
	if (!FEED_VIDEO_ENABLED) return [];
	const evergreenNonNews = isEvergreenNonNewsVisualTopic({
		category,
		topicLabel,
		text: `${query || ""} ${meta.segmentText || ""}`,
	});
	const entries = [];
	const push = (raw, sourceType) => {
		const entry = normalizeFeedVideoCandidateEntry(raw, sourceType);
		if (!entry) return;
		if (isDisfavoredFeedVideoSourceUrl(entry.url)) return;
		entries.push(entry);
	};

	for (const url of Array.isArray(meta.videoUrls) ? meta.videoUrls : []) {
		push({ url, title: topicLabel }, "trend-video");
	}
	for (const item of Array.isArray(meta.potentialVideos) ? meta.potentialVideos : []) {
		push(item, item?.origin || "trend-video");
	}

	const articleUrls = uniqueStrings(meta.articleUrls || [], { limit: 4 });
	for (const articleUrl of articleUrls) {
		const ogVideos = await fetchOpenGraphVideoUrls(articleUrl);
		for (const videoUrl of ogVideos) {
			push(
				{
					url: videoUrl,
					pageUrl: articleUrl,
					title: topicLabel,
					query,
				},
				"article-og-video",
			);
		}
	}

	const videoQueries = evergreenNonNews
		? []
		: uniqueStrings(
				[
					...buildSegmentVideoQueryVariants({
						baseQuery: query,
						topicLabel,
						segmentText: meta.segmentText || "",
						topicKeywords: meta.keywordHints,
						articleTitles: meta.articleTitles,
						category,
						maxVariants: FEED_VIDEO_QUERY_LIMIT,
					}),
					...(Array.isArray(queryVariants) ? queryVariants : []).map((q) =>
						/\b(video|footage|clip)\b/i.test(q) ? q : `${q} video`,
					),
				],
				{ limit: FEED_VIDEO_QUERY_LIMIT },
			);

	if (FEED_VIDEO_SEARCH_ENABLED && videoQueries.length) {
		for (const videoQuery of videoQueries) {
			if (GOOGLE_CSE_CONFIG_READY) {
				const items = await fetchCseItems([videoQuery], {
					num: FEED_VIDEO_CANDIDATE_LIMIT,
					maxPages: 1,
					jobId,
					label: "feed_video_cse",
				});
				for (const item of items) {
					const pageUrl = String(item.link || "").trim();
					for (const videoUrl of extractVideoUrlsFromPagemap(item.pagemap)) {
						push(
							{
								url: videoUrl,
								pageUrl,
								title: item.title,
								snippet: item.snippet,
								query: videoQuery,
							},
							isProbablyDirectVideoUrl(videoUrl)
								? "cse-video-direct"
								: "cse-video-meta",
						);
					}
					if (pageUrl && !isUnsupportedFeedVideoHost(pageUrl)) {
						push(
							{
								url: pageUrl,
								pageUrl,
								title: item.title,
								snippet: item.snippet,
								query: videoQuery,
							},
							"cse-video-page",
						);
					}
				}
			}

			const bingCandidates = await fetchBingVideoCandidates(videoQuery, {
				limit: Math.max(4, Math.floor(FEED_VIDEO_CANDIDATE_LIMIT / 2)),
				jobId,
			});
			for (const item of bingCandidates) push(item, item.sourceType);
		}
	}

	const scored = [];
	const seen = new Set();
	for (const entry of entries) {
		const key = normalizeFeedVideoKey(entry.url || entry.pageUrl);
		if (!key || seen.has(key)) continue;
		seen.add(key);
		const scoredEntry = {
			...entry,
			...scoreFeedVideoCandidate(entry, {
				topicTokens,
				segmentTokens,
				queryTokens,
			}),
		};
		if (FEED_VIDEO_TRUSTED_SOURCES_ONLY && scoredEntry.trust < 1) continue;
		if (scoredEntry.score < -10) continue;
		scored.push(scoredEntry);
	}
	scored.sort((a, b) => {
		if (b.score !== a.score) return b.score - a.score;
		if (Number(b.direct) !== Number(a.direct)) return Number(b.direct) - Number(a.direct);
		return b.trust - a.trust;
	});
	return scored.slice(0, FEED_VIDEO_CANDIDATE_LIMIT);
}

async function analyzeFeedVideoSourceMotion(videoPath, jobId, label) {
	if (!FEED_VIDEO_MOTION_QA_ENABLED) return { pass: true, issues: [] };
	try {
		const freezeInfo = await detectFrozenVideo(videoPath, {
			noise: FEED_VIDEO_FREEZE_NOISE,
			minFreezeSec: FEED_VIDEO_FREEZE_MIN_SEC,
		});
		const result = {
			pass: true,
			issues: [],
			durationSec: freezeInfo.durationSec || 0,
			maxFreezeSec: freezeInfo.maxFreezeSec || 0,
			freezeRatio: freezeInfo.freezeRatio || 0,
		};
		if (
			result.maxFreezeSec >= FEED_VIDEO_MAX_FREEZE_SEC ||
			result.freezeRatio >= FEED_VIDEO_MAX_FREEZE_RATIO
		) {
			result.pass = false;
			result.issues.push("feed_video_too_static");
		}
		return result;
	} catch (e) {
		if (jobId)
			logJob(jobId, "feed video motion qa failed", {
				label,
				error: e.message,
			});
		return { pass: true, issues: ["motion_qa_unavailable"] };
	}
}

async function expandFeedVideoCandidateUrls(entry = {}) {
	const urls = [];
	const push = (raw) => {
		const url = sanitizeFeedVideoUrl(raw);
		if (!url || isDisfavoredFeedVideoSourceUrl(url)) return;
		urls.push(url);
	};
	if (
		isProbablyDirectVideoUrl(entry.url) ||
		/\b(direct|article-og)\b/i.test(entry.sourceType || "")
	) {
		push(entry.url);
	}
	for (const pageUrl of uniqueStrings([entry.pageUrl, entry.url], { limit: 2 })) {
		if (
			!pageUrl ||
			isProbablyDirectVideoUrl(pageUrl) ||
			isKnownBlockedFeedVideoPage(pageUrl)
		)
			continue;
		const ogVideos = await fetchOpenGraphVideoUrls(pageUrl);
		ogVideos.forEach(push);
	}
	return uniqueStrings(urls, { limit: 5 });
}

async function downloadFeedVideoCandidates({
	candidates = [],
	tmpDir,
	jobId,
	segIndex,
	targetCount = 1,
}) {
	const localPaths = [];
	const usedUrls = [];
	const seen = new Set();
	for (let i = 0; i < candidates.length; i++) {
		if (localPaths.length >= targetCount) break;
		const candidate = candidates[i];
		const directUrls = await expandFeedVideoCandidateUrls(candidate);
		for (const directUrl of directUrls) {
			if (localPaths.length >= targetCount) break;
			const key = normalizeFeedVideoKey(directUrl);
			if (!key || seen.has(key)) continue;
			seen.add(key);
			if (FEED_VIDEO_TRUSTED_SOURCES_ONLY) {
				const trust = Math.max(
					feedVideoSourceTrustScore(directUrl),
					feedVideoSourceTrustScore(candidate.pageUrl || ""),
				);
				if (trust < 1) continue;
			}
			const extGuess = path
				.extname(String(directUrl).split("?")[0] || "")
				.toLowerCase();
			const ext = [".mp4", ".m4v", ".mov", ".webm"].includes(extGuess)
				? extGuess
				: ".mp4";
			const out = path.join(
				tmpDir,
				`seg_${jobId}_${segIndex}_feed_video_${i}_${crypto
					.randomUUID()
					.slice(0, 8)}${ext}`,
			);
			try {
				await downloadToFileWithLimit({
					url: directUrl,
					outPath: out,
					timeoutMs: FEED_VIDEO_DOWNLOAD_TIMEOUT_MS,
					retries: 1,
					maxBytes: FEED_VIDEO_MAX_BYTES,
				});
				const detected = detectFileType(out);
				if (!detected || detected.kind !== "video") {
					safeUnlink(out);
					continue;
				}
				const info = await probeMedia(out);
				const videoStream =
					(info.streams || []).find((s) => s.codec_type === "video") || {};
				const durationSec = Number(info.duration || videoStream.duration || 0);
				const width = Number(videoStream.width || 0);
				const height = Number(videoStream.height || 0);
				if (
					!info.hasVideo ||
					durationSec < FEED_VIDEO_MIN_SOURCE_SEC ||
					(width && width < FEED_VIDEO_MIN_WIDTH) ||
					(height && height < FEED_VIDEO_MIN_HEIGHT)
				) {
					safeUnlink(out);
					continue;
				}
				const motionQa = await analyzeFeedVideoSourceMotion(
					out,
					jobId,
					`seg_${segIndex}_feed_video`,
				);
				if (!motionQa.pass) {
					logJob(jobId, "feed video rejected by motion qa", {
						segment: segIndex,
						url: directUrl,
						issues: motionQa.issues,
						maxFreezeSec: Number((motionQa.maxFreezeSec || 0).toFixed(3)),
						freezeRatio: Number((motionQa.freezeRatio || 0).toFixed(3)),
					});
					safeUnlink(out);
					continue;
				}
				localPaths.push(out);
				usedUrls.push(directUrl);
			} catch (e) {
				logJob(jobId, "feed video candidate download failed", {
					segment: segIndex,
					sourceType: candidate.sourceType,
					url: directUrl,
					error: e.message,
				});
				safeUnlink(out);
			}
		}
	}
	return { localPaths, usedUrls };
}

async function fetchCseImagesForQuery(
	query,
	topicTokens = [],
	maxResults = 4,
	jobId = null,
	opts = {},
) {
	const q = sanitizeOverlayQuery(query);
	if (!q) return [];
	const target = clampNumber(Number(maxResults) || 4, 1, CSE_MAX_IMAGE_RESULTS);
	const maxPages = clampNumber(Number(opts.maxPages) || CSE_MAX_PAGES, 1, 5);
	const relaxedMinEdge = clampNumber(
		Number(opts.relaxedMinEdge) || CSE_RELAXED_MIN_IMAGE_SHORT_EDGE,
		200,
		CSE_MIN_IMAGE_SHORT_EDGE,
	);
	const requestSize = Math.min(
		CSE_MAX_IMAGE_RESULTS,
		Math.max(12, target * IMAGE_SEARCH_CANDIDATE_MULTIPLIER),
	);
	const looseTopicRelevance = Boolean(opts.looseTopicRelevance);
	const allowLooseResults = Boolean(opts.allowLooseResults);
	const strictTopicTokens = looseTopicRelevance
		? []
		: filterSpecificTopicTokens(topicTokens);
	const tokens = expandTopicTokens(
		filterSpecificTopicTokens([...tokenizeLabel(q), ...strictTopicTokens]),
	);
	const minMatches = looseTopicRelevance
		? Math.min(2, minTopicTokenMatches(tokens))
		: minTopicTokenMatches(tokens);
	const relaxedMinMatches = looseTopicRelevance
		? 1
		: Math.max(1, minMatches - 1);
	const requiredTopicMatches = minImageTopicTokenMatches(strictTopicTokens);
	const relaxedRequiredMatches = requiredTopicMatches ? 1 : 0;
	const attemptStats = [];
	let items = await fetchCseItems([q], {
		num: requestSize,
		maxPages,
		searchType: "image",
		imgSize: CSE_ULTRA_IMG_SIZE,
		jobId,
		label: "cse_image_query_ultra",
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
			jobId,
			label: "cse_image_query_preferred",
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
	if (!candidates.length && allowLooseResults) {
		for (const it of items) {
			const url = it.link || "";
			if (!url || !/^https:\/\//i.test(url)) continue;
			const w = Number(it.image?.width || 0);
			const h = Number(it.image?.height || 0);
			const shortEdge = w && h ? Math.min(w, h) : 0;
			if (shortEdge && shortEdge < relaxedMinEdge) continue;
			candidates.push({ url, score: 0, urlMatches: 0, w, h });
			if (candidates.length >= maxCandidates) break;
		}
	}
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
		if (isDisfavoredImageSourceUrl(c.url)) continue;
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
				"",
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
			text: seg.text || "",
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
		const holdSingleVisual = isHoldSingleVisualSegment({
			text: cue.text || "",
			overlayCues: [{ query: cue.query }],
		});
		const startPct = clampNumber(
			Number.isFinite(cue.startPct)
				? cue.startPct
				: holdSingleVisual
					? 0.05
					: 0.25,
			holdSingleVisual ? 0.02 : 0.2,
			holdSingleVisual ? 0.3 : 0.75,
		);
		const endPct = clampNumber(
			Number.isFinite(cue.endPct)
				? cue.endPct
				: holdSingleVisual
					? 0.95
					: 0.75,
			startPct + (holdSingleVisual ? 0.45 : 0.2),
			holdSingleVisual ? 0.98 : 0.9,
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

function resolveTargetHeyGenCallCount(videoDurationSec = 0) {
	const sec = Math.max(0, Number(videoDurationSec) || 0);
	if (sec >= 240) return 5;
	if (sec >= 180) return 4;
	return 3;
}

function resolveOptionalHeyGenContentCalls(videoDurationSec = 0) {
	if (OPTIONAL_HEYGEN_CONTENT_CALLS_OVERRIDE) {
		return Math.floor(clampNumber(
			Number(OPTIONAL_HEYGEN_CONTENT_CALLS_OVERRIDE),
			0,
			4,
		));
	}
	return Math.max(0, resolveTargetHeyGenCallCount(videoDurationSec) - 2);
}

function buildOptionalPresenterClusterPositions({
	bestIdx = -1,
	windowStart = 0,
	windowEnd = 0,
	bodyStart = 0,
	segmentDuration = () => 0,
	presenterPosSet = new Set(),
} = {}) {
	if (bestIdx < 0) {
		return { positions: [], durationSec: 0, expanded: false };
	}
	const maxSec = Math.max(
		OPTIONAL_PRESENTER_CLUSTER_MIN_SEC,
		Number(PRESENTER_BODY_MAX_SEC) || OPTIONAL_PRESENTER_CLUSTER_MIN_SEC,
	);
	const targetSec = clampNumber(
		OPTIONAL_PRESENTER_CLUSTER_TARGET_SEC,
		OPTIONAL_PRESENTER_CLUSTER_MIN_SEC,
		maxSec,
	);
	const maxSegments = Math.max(1, OPTIONAL_PRESENTER_CLUSTER_MAX_SEGMENTS);
	const positions = [bestIdx];
	let durationSec = Math.max(0, Number(segmentDuration(bestIdx)) || 0);

	const canUse = (idx) =>
		idx >= bodyStart &&
		idx >= windowStart &&
		idx < windowEnd &&
		!presenterPosSet.has(idx) &&
		!positions.includes(idx);

	const pickCandidate = () => {
		const candidates = [
			{ idx: positions[positions.length - 1] + 1, side: "right" },
			{ idx: positions[0] - 1, side: "left" },
		]
			.filter((candidate) => canUse(candidate.idx))
			.map((candidate) => {
				const dur = Math.max(0, Number(segmentDuration(candidate.idx)) || 0);
				const projected = durationSec + dur;
				return {
					...candidate,
					dur,
					projected,
					overMax: projected > maxSec,
					score:
						Math.abs(targetSec - projected) +
						(candidate.side === "left" ? 0.12 : 0) +
						(dur < 2.5 ? 0.4 : 0),
				};
			})
			.filter(
				(candidate) =>
					!candidate.overMax ||
					(durationSec < OPTIONAL_PRESENTER_CLUSTER_MIN_SEC &&
						candidate.projected <= PRESENTER_RUN_MERGE_MAX_SEC),
			);
		if (!candidates.length) return null;
		candidates.sort((a, b) => {
			if (a.overMax !== b.overMax) return a.overMax ? 1 : -1;
			return a.score - b.score;
		});
		return candidates[0];
	};

	while (
		positions.length < maxSegments &&
		durationSec < targetSec &&
		durationSec < maxSec
	) {
		const candidate = pickCandidate();
		if (!candidate) break;
		if (candidate.side === "left") positions.unshift(candidate.idx);
		else positions.push(candidate.idx);
		durationSec += candidate.dur;
		if (durationSec >= OPTIONAL_PRESENTER_CLUSTER_MIN_SEC) {
			const next = pickCandidate();
			if (
				!next ||
				Math.abs(targetSec - durationSec) <=
					Math.abs(targetSec - next.projected)
			) {
				break;
			}
		}
	}

	return {
		positions: positions.slice().sort((a, b) => a - b),
		durationSec,
		expanded: positions.length > 1,
	};
}

function computeContentVisualPlan(totalSegments, options = {}) {
	const count = Math.max(0, Math.floor(Number(totalSegments) || 0));
	const videoDurationSec = Math.max(0, Number(options.videoDurationSec) || 0);
	const rawDurations = Array.isArray(options.segmentDurations)
		? options.segmentDurations
		: [];
	const knownDurationTotal = rawDurations.reduce((sum, dur) => {
		const n = Math.max(0, Number(dur) || 0);
		return sum + n;
	}, 0);
	const fallbackSegmentSec =
		count > 0
			? Math.max(
					3,
					knownDurationTotal > 0
						? knownDurationTotal / count
						: videoDurationSec > 0
							? videoDurationSec / count
							: PRESENTER_BODY_TARGET_SEC,
				)
			: PRESENTER_BODY_TARGET_SEC;
	const segmentDuration = (idx) => {
		const n = Math.max(0, Number(rawDurations[idx]) || 0);
		return n > 0 ? n : fallbackSegmentSec;
	};
	const targetHeyGenCalls = resolveTargetHeyGenCallCount(videoDurationSec);
	const optionalHeyGenContentCalls = resolveOptionalHeyGenContentCalls(
		videoDurationSec,
	);
	const forcedOpeningPresenterCount = Math.min(
		FORCE_OPENING_PRESENTER_COUNT,
		count,
	);
	const openingPresenterCount = Math.min(
		count,
		Math.max(forcedOpeningPresenterCount, OPENING_PRESENTER_CLUSTER_COUNT),
	);
	const presenterPosSet = new Set();
	const presenterClusterIdByPosition = new Map();
	const optionalPresenterClusters = [];
	const addPosition = (idx, clusterId = "") => {
		if (idx < 0 || idx >= count || presenterPosSet.has(idx)) return false;
		presenterPosSet.add(idx);
		if (clusterId) presenterClusterIdByPosition.set(idx, clusterId);
		return true;
	};
	for (
		let idx = 0;
		idx < openingPresenterCount;
		idx += 1
	) {
		addPosition(idx, "opening");
	}
	const bodyStart = openingPresenterCount;
	const bodyCount = Math.max(0, count - bodyStart);
	for (
		let clusterIndex = 0;
		clusterIndex < optionalHeyGenContentCalls && bodyCount > 0;
		clusterIndex += 1
	) {
		const clusterId = `body_${clusterIndex + 1}`;
		const windowStart =
			bodyStart +
			Math.floor((clusterIndex * bodyCount) / optionalHeyGenContentCalls);
		const windowEnd =
			clusterIndex === optionalHeyGenContentCalls - 1
				? count
				: bodyStart +
					Math.floor(((clusterIndex + 1) * bodyCount) / optionalHeyGenContentCalls);
		const windowSize = Math.max(1, windowEnd - windowStart);
		let bestIdx = -1;
		let bestScore = Number.POSITIVE_INFINITY;
		const preferredSec = PREFERRED_HEYGEN_PRESENTER_SEGMENT_SEC;
		const idealPosition = windowStart + Math.floor(windowSize * 0.35);
		for (let idx = windowStart; idx < windowEnd; idx += 1) {
			if (idx < bodyStart || idx >= count || presenterPosSet.has(idx)) continue;
			const dur = segmentDuration(idx);
			const prefersLongEnough = dur >= preferredSec ? 0 : 1;
			const durationDistance = Math.abs(dur - preferredSec) / preferredSec;
			const positionDistance =
				Math.abs(idx - idealPosition) / Math.max(1, windowSize);
			const score = prefersLongEnough * 2 + durationDistance + positionDistance * 0.35;
			if (score < bestScore) {
				bestScore = score;
				bestIdx = idx;
			}
		}

		const clusterPlan = buildOptionalPresenterClusterPositions({
			bestIdx,
			windowStart,
			windowEnd,
			bodyStart,
			segmentDuration,
			presenterPosSet,
		});
		const clusterPositions = clusterPlan.positions;
		const clusterDurationSec = clusterPlan.durationSec;

		if (clusterPositions.length) {
			for (const idx of clusterPositions) addPosition(idx, clusterId);
			optionalPresenterClusters.push({
				id: clusterId,
				positions: clusterPositions.slice().sort((a, b) => a - b),
				durationSec: Number(clusterDurationSec.toFixed(3)),
				preferredDurationSec: preferredSec,
				durationMeetsPreference: clusterDurationSec >= preferredSec,
				heygenSafeMinSec: OPTIONAL_PRESENTER_CLUSTER_MIN_SEC,
				heygenTargetSec: OPTIONAL_PRESENTER_CLUSTER_TARGET_SEC,
				expandedForHeyGenMotion: Boolean(clusterPlan.expanded),
				qualityFirst: true,
			});
		}
	}
	const presenter = [];
	const image = [];
	const forcedPresenter = [];
	for (let idx = 0; idx < count; idx += 1) {
		if (idx < forcedOpeningPresenterCount) forcedPresenter.push(idx);
		if (presenterPosSet.has(idx)) presenter.push(idx);
		else image.push(idx);
	}
	return {
		totalSegments: count,
		presenterCount: presenter.length,
		imageCount: image.length,
		presenterPositions: presenter,
		imagePositions: image,
		forcedPresenterPositions: forcedPresenter,
		openingPresenterCount,
		targetHeyGenCalls,
		maxOptionalHeyGenContentSegments: optionalHeyGenContentCalls,
		optionalPresenterClusters,
		presenterPositionSet: presenterPosSet,
		presenterClusterIdByPosition,
		forcedPresenterPositionSet: new Set(forcedPresenter),
	};
}

function isHoldSingleVisualSegment(seg = {}) {
	const text = String(seg?.text || "");
	const cue = Array.isArray(seg?.overlayCues)
		? String(seg.overlayCues[0]?.query || "")
		: "";
	const hay = `${text} ${cue}`.toLowerCase();
	if (
		/\b(chart|graph|data|stat|statistics|census|bls|federal reserve|report|study|survey|paperwork|documents?|debt papers?|application|table|map|timeline|infographic|receipt|bill|invoice|budget sheet)\b/i.test(
			hay,
		)
	) {
		return true;
	}
	return countWords(text) >= 60;
}

function computeSegmentImageCount(segDur, seg = null) {
	const dur = Math.max(0, Number(segDur) || 0);
	if (seg && isHoldSingleVisualSegment(seg)) return 1;
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
			"",
	).trim();
	const countdownLabel = cleanTopicLabel(seg?.countdownLabel || "");
	const visualTopicLabel = countdownLabel || topicLabel;
	const cueRaw = Array.isArray(seg?.overlayCues) ? seg.overlayCues[0] : null;
	const fallbackQuery = buildOverlayQueryFallback(seg?.text || "", topicLabel);
	const baseQuery = String(cueRaw?.query || "").trim();
	const preferredQuery = isGenericOverlayQuery(baseQuery, topicLabel)
		? fallbackQuery
		: baseQuery || fallbackQuery;
	const query = ensureCompactTopicInQuery(preferredQuery, visualTopicLabel);
	return { query, topicLabel: visualTopicLabel || topicLabel };
}

function buildSegmentImageQueryVariants({
	baseQuery,
	topicLabel,
	segmentText,
	topicKeywords = [],
	articleTitles = [],
	category = "",
	maxVariants = IMAGE_SEARCH_MAX_QUERY_VARIANTS,
} = {}) {
	const variants = [];
	const push = (raw) => {
		const q = sanitizeOverlayQuery(raw);
		if (!q) return;
		variants.push(q);
	};

	push(baseQuery);
	if (
		isConceptualOrMetaphoricalVisualTopic({
			category,
			topicLabel,
			text: `${segmentText || ""} ${baseQuery || ""}`,
		})
	) {
		buildConceptualVisualQueries({
			topicLabel,
			segmentText,
			baseQuery,
			category,
			limit: Math.max(6, Number(maxVariants) || 6),
		}).forEach(push);
	}
	if (segmentText || topicLabel) {
		const fallback = buildOverlayQueryFallback(
			segmentText || "",
			topicLabel || "",
		);
		push(fallback);
	}
	buildTopicNearImageQueries(topicLabel, {
		topicKeywords,
		articleTitles,
		category,
	}).forEach(push);

	const topicTokens = new Set(tokenizeLabel(topicLabel || ""));
	const textTokens = filterSegmentImageMatchTokens(
		tokenizeLabel(segmentText || ""),
	).filter((t) => !topicTokens.has(t));
	if (topicLabel && textTokens.length) {
		push(ensureCompactTopicInQuery(textTokens.slice(0, 3).join(" "), topicLabel));
	}

	const hintList = uniqueStrings(
		[
			...(Array.isArray(topicKeywords) ? topicKeywords : []),
			...(Array.isArray(articleTitles) ? articleTitles : []),
		]
			.map((hint) => cleanImageQueryHint(hint, topicLabel))
			.filter(Boolean),
		{ limit: Math.max(6, Number(maxVariants) || 6) },
	);
	for (const hint of hintList) {
		if (variants.length >= maxVariants) break;
		const withTopic = ensureCompactTopicInQuery(hint, topicLabel);
		push(withTopic);
	}

	const unique = uniqueStrings(variants, { limit: maxVariants });
	const multiWord = unique.filter((v) => tokenizeLabel(v).length >= 2);
	return multiWord.length
		? uniqueStrings(multiWord, { limit: maxVariants })
		: unique;
}

function buildVisualGroundingTopicMeta(topic = {}, topicContexts = [], topicIndex = 0) {
	const story = topic.trendStory || {};
	const contextItems = topicContextItemsAt(topicContexts, topicIndex);
	const label = String(topic.displayTopic || topic.topic || "").trim();
	const keywordHints = uniqueStrings(
		[
			...(Array.isArray(topic.keywords) ? topic.keywords : []),
			...(story.imageSearchQueries || []),
			...(story.searchPhrases || []),
			...(story.entityNames || []),
		],
		{ limit: 10 },
	);
	const articleTitles = (story.articles || [])
		.map((a) => a.title)
		.filter(Boolean);
	const articleUrls = (story.articles || [])
		.map((a) => a.url)
		.filter((u) => isHttpUrl(u));
	const articleImageUrls = uniqueStrings(
		(story.articles || [])
			.map((a) => a.image)
			.filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u)),
		{ limit: 8 },
	);
	const potentialUrls = uniqueStrings(
		(Array.isArray(story.potentialImages) ? story.potentialImages : [])
			.map((p) => (typeof p === "string" ? p : p?.url))
			.filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u)),
		{ limit: 80 },
	);
	const seedUrls = uniqueStrings(
		[
			...(Array.isArray(topic.images) ? topic.images : []),
			topic.image,
			story.image,
			...(Array.isArray(story.images) ? story.images : []),
			...articleImageUrls,
		],
		{ limit: 18 },
	).filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u));
	const imageMetaByKey = new Map();
	const addImageMeta = (raw, queryHint = "") => {
		const normalized = normalizeFreeImageMetadataItem(raw, queryHint);
		if (!normalized?.url) return;
		const key = normalizeImageUrlKey(normalized.url);
		if (!key) return;
		imageMetaByKey.set(key, {
			...(imageMetaByKey.get(key) || {}),
			...normalized,
		});
	};
	for (const item of Array.isArray(story.potentialImages)
		? story.potentialImages
		: []) {
		addImageMeta(item, item?.query || label);
	}
	for (const article of Array.isArray(story.articles) ? story.articles : []) {
		if (!article?.image) continue;
		addImageMeta(
			{
				url: article.image,
				title: article.title,
				sourcePage: article.url,
				provider: article.source || "article",
			},
			label,
		);
	}
	for (const url of [...(Array.isArray(topic.images) ? topic.images : []), topic.image]) {
		if (!url) continue;
		addImageMeta({ url, title: label, query: label }, label);
	}
	return {
		label,
		contextItems,
		keywordHints,
		articleTitles,
		articleUrls,
		potentialUrls,
		seedUrls,
		trustedSeedUrls: articleImageUrls,
		imageMetaByKey,
		videoUrls: uniqueStrings(
			[
				...(Array.isArray(topic.videos) ? topic.videos : []),
				topic.video,
				...(Array.isArray(story.videos) ? story.videos : []),
				story.video,
				story.videoUrl,
				...(Array.isArray(story.potentialVideos)
					? story.potentialVideos.map((v) => v?.url).filter(Boolean)
					: []),
			],
			{ limit: 18 },
		).filter((u) => isHttpUrl(u)),
		potentialVideos: Array.isArray(story.potentialVideos)
			? story.potentialVideos
			: [],
		visualBeatQueries: uniqueStrings(
			(Array.isArray(story.visualBeatPlan) ? story.visualBeatPlan : [])
				.map((beat) => beat?.query)
				.filter(Boolean),
			{ limit: 12 },
		),
	};
}

async function groundScriptInValidatedVisuals({
	script,
	topics = [],
	topicContexts = [],
	category = "",
	baseUrl,
	jobId,
	plannedImagePositions = [],
} = {}) {
	if (!PRE_TTS_VISUAL_GROUNDING_ENABLED || !script?.segments?.length) {
		return { script, summary: null };
	}
	const segments = Array.isArray(script.segments) ? script.segments : [];
	const imagePositionSet = new Set(
		(Array.isArray(plannedImagePositions) ? plannedImagePositions : [])
			.map((idx) => Number(idx))
			.filter((idx) => Number.isFinite(idx)),
	);
	const targetPairs = segments
		.map((seg, position) => ({ seg, position }))
		.filter(({ position }) => !imagePositionSet.size || imagePositionSet.has(position))
		.slice(0, PRE_TTS_VISUAL_GROUNDING_SEGMENT_LIMIT);
	if (!targetPairs.length || !GOOGLE_IMAGES_SEARCH_ENABLED) {
		return { script, summary: null };
	}

	const topicMetaByIndex = new Map();
	for (let i = 0; i < (topics || []).length; i += 1) {
		topicMetaByIndex.set(
			i,
			buildVisualGroundingTopicMeta(topics[i] || {}, topicContexts, i),
		);
	}
	const imageMetadataCache = new Map();
	const videoCandidateCache = new Map();
	let groundedSegments = 0;
	let revisedQueries = 0;
	let attachedImages = 0;
	let feedVideoSegments = 0;
	let feedVideoCandidates = 0;
	let videoProbeUsed = 0;
	const segmentSummaries = [];

	const updatedSegments = segments.map((seg) => ({ ...seg }));

	for (const { seg, position } of targetPairs) {
		const topicIndex = Number.isFinite(Number(seg.topicIndex))
			? Number(seg.topicIndex)
			: 0;
		const topic = topics[topicIndex] || {};
		const meta = topicMetaByIndex.get(topicIndex) || {};
		const { query: currentQuery, topicLabel } = resolveSegmentImageQuery(
			seg,
			topics,
		);
		const effectiveTopicLabel = topicLabel || meta.label || "";
		const topicTokens = topicTokensFromTitle(effectiveTopicLabel || "");
		const segmentTokens = extractSegmentMatchTokens(
			seg.text || "",
			effectiveTopicLabel,
			4,
		);
		const queryVariants = uniqueStrings(
			[
				currentQuery,
				...(Array.isArray(meta.visualBeatQueries) ? meta.visualBeatQueries : []),
				...buildSegmentImageQueryVariants({
					baseQuery: currentQuery,
					topicLabel: effectiveTopicLabel,
					segmentText: seg.text,
					topicKeywords: meta.keywordHints,
					articleTitles: meta.articleTitles,
					category,
					maxVariants: IMAGE_SEARCH_MAX_QUERY_VARIANTS,
				}),
			],
			{ limit: PRE_TTS_VISUAL_GROUNDING_VARIANT_LIMIT },
		);
		const existingCandidates = [];
		for (const url of [
			...(Array.isArray(seg.imageUrls) ? seg.imageUrls : []),
			...(Array.isArray(meta.potentialUrls) ? meta.potentialUrls : []),
			...(Array.isArray(meta.seedUrls) ? meta.seedUrls : []),
		]) {
			if (isHttpUrl(url) && !isLikelyThumbnailUrl(url)) {
				existingCandidates.push({
					url,
					title: effectiveTopicLabel,
					query: currentQuery,
				});
			}
		}

		let best = {
			query: currentQuery,
			relevantUrls: [],
			items: [],
			score: -Infinity,
		};
		const evaluateItems = (items = [], q = currentQuery) => {
			const normalizedItems = [];
			for (const item of items) {
				const normalized = normalizeFreeImageMetadataItem(item, q);
				if (!normalized?.url || !isHttpUrl(normalized.url)) continue;
				const key = normalizeImageUrlKey(normalized.url);
				if (!key) continue;
				meta.imageMetaByKey?.set(key, {
					...(meta.imageMetaByKey.get(key) || {}),
					...normalized,
				});
				normalizedItems.push(normalized);
			}
			const candidateUrls = dedupeUrlsPreserveOrder(
				normalizedItems.map((item) => item.url),
			).filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u));
			const queryTokens = filterSpecificTopicTokens(tokenizeLabel(q)).slice(0, 16);
			const relevantUrls = filterRelevantImageCandidatePool(candidateUrls, {
				topicTokens,
				segmentTokens,
				queryTokens,
				trustedUrlKeys: new Set(),
				imageMetaByKey: meta.imageMetaByKey,
			});
			const score =
				relevantUrls.length * 4 +
				candidateUrls.length +
				topicMatchInfo(topicTokens, [q, ...normalizedItems.map((item) => item.title)]).count *
					2;
			if (score > best.score) {
				best = {
					query: q,
					relevantUrls,
					items: normalizedItems,
					score,
				};
			}
		};

		evaluateItems(existingCandidates, currentQuery);
		for (const q of queryVariants) {
			const normalizedQuery = sanitizeOverlayQuery(q);
			if (!normalizedQuery) continue;
			const cacheKey = `ground::${normalizedQuery}`;
			let items = imageMetadataCache.get(cacheKey);
			if (!items) {
				items = await fetchGoogleImageMetadataFromService(normalizedQuery, {
					limit: Math.max(12, PRE_TTS_VISUAL_GROUNDING_IMAGE_LIMIT * 2),
					baseUrl,
					jobId,
				});
				imageMetadataCache.set(cacheKey, items);
			}
			evaluateItems(items || [], normalizedQuery);
			if (best.relevantUrls.length >= PRE_TTS_VISUAL_GROUNDING_IMAGE_LIMIT) {
				break;
			}
		}

		const finalUrls = dedupeUrlsPreserveOrder(best.relevantUrls).slice(
			0,
			PRE_TTS_VISUAL_GROUNDING_IMAGE_LIMIT,
		);
		if (!finalUrls.length) {
			segmentSummaries.push({
				segment: seg.index,
				position,
				query: currentQuery,
				images: 0,
				videoCandidates: 0,
			});
			continue;
		}

		const segmentUpdate = updatedSegments[position] || { ...seg };
		const currentCue = Array.isArray(segmentUpdate.overlayCues)
			? segmentUpdate.overlayCues[0] || {}
			: {};
		const bestQuery = sanitizeOverlayQuery(best.query || currentQuery);
		if (bestQuery && bestQuery !== sanitizeOverlayQuery(currentQuery)) {
			revisedQueries += 1;
		}
		const holdSingleVisual = isHoldSingleVisualSegment({
			...segmentUpdate,
			overlayCues: [{ ...currentCue, query: bestQuery || currentQuery }],
		});
		const cueStartPct = holdSingleVisual
			? clampNumber(currentCue.startPct ?? 0.05, 0.02, 0.2)
			: clampNumber(currentCue.startPct ?? 0.25, 0.2, 0.65);
		const cueEndPct = holdSingleVisual
			? clampNumber(currentCue.endPct ?? 0.95, Math.max(0.75, cueStartPct + 0.45), 0.98)
			: clampNumber(
					currentCue.endPct ?? 0.75,
					Math.min(0.85, cueStartPct + 0.2),
					0.85,
				);
		segmentUpdate.overlayCues = [
			{
				...currentCue,
				query: bestQuery || currentQuery,
				startPct: cueStartPct,
				endPct: cueEndPct,
				position: "topRight",
			},
		];
		if (
			Number(segmentUpdate.overlayCues[0].endPct) -
				Number(segmentUpdate.overlayCues[0].startPct) <
			0.2
		) {
			segmentUpdate.overlayCues[0].endPct = Math.min(
				holdSingleVisual ? 0.98 : 0.85,
				Number(segmentUpdate.overlayCues[0].startPct) + 0.25,
			);
		}
		segmentUpdate.imageUrls = dedupeUrlsPreserveOrder([
			...(Array.isArray(segmentUpdate.imageUrls) ? segmentUpdate.imageUrls : []),
			...finalUrls,
		]).slice(0, PRE_TTS_VISUAL_GROUNDING_IMAGE_LIMIT);
		segmentUpdate.visualGrounding = {
			query: bestQuery || currentQuery,
			imageCount: finalUrls.length,
			validatedBeforeTts: true,
		};
		updatedSegments[position] = segmentUpdate;
		groundedSegments += 1;
		attachedImages += segmentUpdate.imageUrls.length;
		mergeTopicPotentialImages(topic, best.items);

		let segmentVideoCandidates = [];
		if (
			PRE_TTS_VISUAL_VIDEO_PROBE_ENABLED &&
			FEED_VIDEO_ENABLED &&
			!isEvergreenNonNewsVisualTopic({
				category,
				topicLabel: effectiveTopicLabel,
				text: `${bestQuery || currentQuery || ""} ${seg.text || ""}`,
			}) &&
			videoProbeUsed < PRE_TTS_VISUAL_VIDEO_PROBE_SEGMENT_LIMIT
		) {
			const videoKey = `video::${topicIndex}::${bestQuery || currentQuery}`;
			segmentVideoCandidates = videoCandidateCache.get(videoKey);
			if (!segmentVideoCandidates) {
				videoProbeUsed += 1;
				const queryTokens = filterSpecificTopicTokens(
					tokenizeLabel(bestQuery || currentQuery),
				).slice(0, 16);
				segmentVideoCandidates =
					await collectFeedVideoCandidateEntriesForSegment({
						query: bestQuery || currentQuery,
						topicLabel: effectiveTopicLabel,
						queryVariants,
						topicTokens,
						segmentTokens,
						queryTokens,
						meta: {
							...meta,
							segmentText: seg.text || "",
						},
						category,
						jobId,
					});
				videoCandidateCache.set(videoKey, segmentVideoCandidates);
			}
			segmentVideoCandidates = (segmentVideoCandidates || []).slice(
				0,
				FEED_VIDEO_CANDIDATE_LIMIT,
			);
			if (segmentVideoCandidates.length) {
				segmentUpdate.feedVideoCandidates = segmentVideoCandidates;
				mergeTopicPotentialVideos(topic, segmentVideoCandidates);
				feedVideoSegments += 1;
				feedVideoCandidates += segmentVideoCandidates.length;
			}
		}

		segmentSummaries.push({
			segment: seg.index,
			position,
			query: bestQuery || currentQuery,
			images: finalUrls.length,
			videoCandidates: segmentVideoCandidates.length,
		});
	}

	const summary = {
		targetSegments: targetPairs.length,
		groundedSegments,
		revisedQueries,
		attachedImages,
		feedVideoSegments,
		feedVideoCandidates,
		segments: segmentSummaries.slice(0, 24),
	};
	logJob(jobId, "script visual grounding ready", summary);
	return {
		script: {
			...script,
			segments: updatedSegments,
		},
		summary,
	};
}

function extractSegmentMatchTokens(
	segmentText = "",
	topicLabel = "",
	maxTokens = 4,
) {
	const topicTokens = new Set(tokenizeLabel(topicLabel || ""));
	const tokens = filterSegmentImageMatchTokens(
		tokenizeLabel(segmentText || ""),
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

const DISFAVORED_STOCK_IMAGE_HOST_RE =
	/(^|\.)((alamy|gettyimages|istockphoto|shutterstock|depositphotos|dreamstime|wireimage|agefotostock|123rf|bigstockphoto|pond5|pixtal)\.com|media\.gettyimages\.com|c8\.alamy\.com|c7\.alamy\.com)$/i;
const DISFAVORED_IMAGE_PATH_RE =
	/\b(watermark|watermarked|preview|comp|sample|stock-photo|stock_image|stockimage|gettyimages|alamy|shutterstock|istockphoto|maxresdefault|hqdefault|mqdefault|sddefault|youtube-thumbnail|video-thumbnail)\b/i;

function isDisfavoredImageSourceUrl(url = "") {
	const raw = String(url || "");
	if (!raw) return false;
	const host = getUrlHost(raw);
	if (host && DISFAVORED_STOCK_IMAGE_HOST_RE.test(host)) return true;
	try {
		const parsed = new URL(sanitizeImageUrl(raw));
		const hay = `${parsed.pathname} ${parsed.search}`.toLowerCase();
		return DISFAVORED_IMAGE_PATH_RE.test(hay);
	} catch {
		return DISFAVORED_IMAGE_PATH_RE.test(raw.toLowerCase());
	}
}

function sanitizeImageUrl(raw = "") {
	let url = String(raw || "").trim();
	if (!url) return "";
	url = url.replace(/&amp;|&#38;|&#038;|\\u0026/gi, "&");
	url = url.replace(/\s/g, "%20");
	return url;
}

function normalizeImageUrlKey(url = "") {
	try {
		const parsed = new URL(sanitizeImageUrl(url));
		parsed.hash = "";
		parsed.search = "";
		return parsed.toString().toLowerCase();
	} catch {
		return sanitizeImageUrl(url).split("?")[0].split("#")[0].toLowerCase();
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

function scoreTextTokenMatch(text = "", tokens = []) {
	const normTokens = normalizeTopicTokens(tokens);
	if (!normTokens.length) return 0;
	const hay = normalizeQaText(text);
	if (!hay) return 0;
	let count = 0;
	for (const tok of normTokens) {
		if (tok && hay.includes(tok)) count += 1;
	}
	return count;
}

function getImageUrlRelevanceTokens(opts = {}) {
	const topicTokens = filterSpecificTopicTokens(
		normalizeTopicTokens(opts.topicTokens || opts.preferTokens || []),
	);
	const segmentTokens = filterSpecificTopicTokens(
		normalizeTopicTokens(opts.segmentTokens || opts.requireTokens || []),
	);
	const queryTokens = filterSpecificTopicTokens(
		normalizeTopicTokens(opts.queryTokens || []),
	);
	return { topicTokens, segmentTokens, queryTokens };
}

function getTrustedImageUrlKeys(opts = {}) {
	if (opts?.trustedUrlKeys instanceof Set) return opts.trustedUrlKeys;
	return new Set();
}

function getValidatedImageUrlKeys(opts = {}) {
	if (opts?.validatedUrlKeys instanceof Set) return opts.validatedUrlKeys;
	return new Set();
}

function getImageRelevanceMeta(url = "", opts = {}) {
	const key = normalizeImageUrlKey(url);
	const byKey =
		opts?.imageMetaByKey instanceof Map ? opts.imageMetaByKey : new Map();
	const meta = byKey.get(key);
	if (!meta || typeof meta !== "object") return "";
	return [
		meta.title,
		meta.alt,
		meta.caption,
		meta.query,
		meta.source,
		meta.sourcePage,
		meta.pageUrl,
		meta.provider,
	]
		.filter(Boolean)
		.join(" ");
}

function scoreImageUrlQuality(url = "") {
	const raw = String(url || "");
	const lower = raw.toLowerCase();
	let score = 0;
	if (/\b(1200|1280|1600|1920|2048|2560|3000|3840|4096)\b/.test(lower))
		score += 4;
	if (/\b(original|xlarge|xxlarge|large|super|master|uploads)\b/.test(lower))
		score += 2;
	if (/\b(w=|width=|resize|fit=|crop=|quality=|format=)\b/.test(lower))
		score += 1;
	if (/\b(thumbnail|thumb|small|sprite|avatar|profile|logo|icon)\b/.test(lower))
		score -= 4;
	if (isLikelyThumbnailUrl(raw)) score -= 8;
	if (isDisfavoredImageSourceUrl(raw)) score -= 25;
	const host = getUrlHost(raw);
	if (/\b(cnn|bbc|cnbc|nytimes|reuters|apnews|cbsnews|usatoday)\./i.test(host))
		score += 2;
	return score;
}

function scoreImageUrlRelevance(url = "", opts = {}) {
	const key = normalizeImageUrlKey(url);
	const trustedUrlKeys = getTrustedImageUrlKeys(opts);
	const validatedUrlKeys = getValidatedImageUrlKeys(opts);
	const qualityScore = scoreImageUrlQuality(url);
	const { topicTokens, segmentTokens, queryTokens } =
		getImageUrlRelevanceTokens(opts);
	const metaText = getImageRelevanceMeta(url, opts);
	const scoreAnyTokenMatch = (tokens) =>
		Math.max(scoreUrlTokenMatch(url, tokens), scoreTextTokenMatch(metaText, tokens));
	const topicScore = scoreAnyTokenMatch(topicTokens);
	const segmentScore = scoreAnyTokenMatch(segmentTokens);
	const queryScore = scoreAnyTokenMatch(queryTokens);
	const hasRelevanceTokens =
		topicTokens.length || segmentTokens.length || queryTokens.length;
	if (
		isDisfavoredImageSourceUrl(url) &&
		!opts?.allowDisfavoredStockImages
	) {
		return {
			trusted: false,
			accepted: false,
			score: -100,
			qualityScore,
			topicScore: 0,
			segmentScore: 0,
			queryScore: 0,
		};
	}
	if (validatedUrlKeys.has(key)) {
		return {
			trusted: true,
			validated: true,
			accepted: true,
			score: 1200 + qualityScore,
			qualityScore,
			topicScore,
			segmentScore,
			queryScore,
		};
	}
	if (
		trustedUrlKeys.has(key) &&
		(!STRICT_TOPIC_RELEVANT_FEED_IMAGES || !hasRelevanceTokens)
	) {
		return {
			trusted: true,
			validated: false,
			accepted: true,
			score: 1000,
			qualityScore,
			topicScore: 0,
			segmentScore: 0,
			queryScore: 0,
		};
	}

	const requiredTopicMatches = minImageTopicTokenMatches(topicTokens);
	const queryRequired = queryTokens.length >= 3 ? 2 : queryTokens.length ? 1 : 0;
	const topical =
		requiredTopicMatches > 0 && topicScore >= requiredTopicMatches;
	const queryMatched = queryRequired > 0 && queryScore >= queryRequired;
	const segmentMatched = segmentTokens.length > 0 && segmentScore > 0;
	const accepted =
		topical ||
		queryMatched ||
		(topicScore > 0 && segmentMatched) ||
		(!requiredTopicMatches && (segmentMatched || queryMatched));
	return {
		trusted: trustedUrlKeys.has(key),
		validated: validatedUrlKeys.has(key),
		accepted,
		score: topicScore * 3 + queryScore * 2 + segmentScore + qualityScore * 0.25,
		qualityScore,
		topicScore,
		segmentScore,
		queryScore,
	};
}

function filterRelevantImageCandidatePool(pool = [], opts = {}) {
	const cleanPool = (Array.isArray(pool) ? pool : []).filter(
		(url) =>
			!isLikelyThumbnailUrl(url) &&
			(!isDisfavoredImageSourceUrl(url) || opts?.allowDisfavoredStockImages),
	);
	if (opts?.enforceRelevance === false) return cleanPool;
	const relevanceTokens = getImageUrlRelevanceTokens(opts);
	const hasSignals =
		relevanceTokens.topicTokens.length ||
		relevanceTokens.segmentTokens.length ||
		relevanceTokens.queryTokens.length ||
		getValidatedImageUrlKeys(opts).size ||
		getTrustedImageUrlKeys(opts).size;
	if (!hasSignals) return cleanPool;

	const scored = cleanPool
		.map((url) => ({ url, ...scoreImageUrlRelevance(url, opts) }))
		.filter((entry) => entry.accepted);
	if (!scored.length) return [];
	scored.sort((a, b) => {
		if (Number(Boolean(b.validated)) !== Number(Boolean(a.validated)))
			return Number(Boolean(b.validated)) - Number(Boolean(a.validated));
		if (Number(b.trusted) !== Number(a.trusted))
			return Number(b.trusted) - Number(a.trusted);
		if (b.topicScore !== a.topicScore) return b.topicScore - a.topicScore;
		if (b.queryScore !== a.queryScore) return b.queryScore - a.queryScore;
		if (b.segmentScore !== a.segmentScore)
			return b.segmentScore - a.segmentScore;
		if (b.qualityScore !== a.qualityScore)
			return b.qualityScore - a.qualityScore;
		return b.score - a.score;
	});
	return scored.map((entry) => entry.url);
}

function dedupeUrlsPreserveOrder(urls = []) {
	const out = [];
	const seen = new Set();
	for (const raw of Array.isArray(urls) ? urls : []) {
		const url = sanitizeImageUrl(raw);
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
	opts = {},
) {
	let pool = dedupeUrlsPreserveOrder(candidates);
	if (!pool.length) return [];

	const target = Math.max(1, Math.floor(desiredCount));
	const maxPicks = Math.max(
		target,
		Math.floor(Number(opts.maxPicks) || target),
	);
	const usedUrlsGlobal =
		opts && opts.usedUrlsGlobal instanceof Set ? opts.usedUrlsGlobal : null;
	const requireTokens = Array.isArray(opts.requireTokens)
		? normalizeTopicTokens(opts.requireTokens)
		: [];
	const preferTokens = Array.isArray(opts.preferTokens)
		? normalizeTopicTokens(opts.preferTokens)
		: [];
	pool = filterRelevantImageCandidatePool(pool, {
		...opts,
		topicTokens: opts.topicTokens || preferTokens,
		segmentTokens: opts.segmentTokens || requireTokens,
	});
	if (!pool.length) return [];
	if (requireTokens.length) {
		const matched = pool.filter(
			(url) => scoreUrlTokenMatch(url, requireTokens) > 0,
		);
		if (matched.length) {
			const matchedKeys = new Set(
				matched.map((url) => normalizeImageUrlKey(url)),
			);
			const rest = pool.filter(
				(url) => !matchedKeys.has(normalizeImageUrlKey(url)),
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
	targetCount = 0,
) {
	const localPaths = [];
	const usedUrls = [];
	const seen = new Set();
	for (let i = 0; i < urls.length; i++) {
		const url = sanitizeImageUrl(urls[i]);
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
				.slice(0, 8)}${ext}`,
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
	{ publicIdBase, output, jobId, segIndex } = {},
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
			/Maximum image size is 25 Megapixels|File size too large|image is too large|too large to process|first action resizes/i.test(
				msg,
			);
		if (!sizeIssue) throw e;

		const targetW = Math.max(640, Number(output?.w) || 1280);
		const targetH = Math.max(360, Number(output?.h) || 720);
		const scaledPath = path.join(
			path.dirname(localPath),
			`cloud_scaled_${jobId || "job"}_${segIndex || "seg"}_${crypto
				.randomUUID()
				.slice(0, 8)}.jpg`,
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
			{ timeoutMs: 120000 },
		);
		let result;
		try {
			result = await cloudinary.uploader.upload(scaledPath, {
				...baseOpts,
				quality: "auto:good",
				fetch_format: "auto",
			});
		} finally {
			safeUnlink(scaledPath);
		}
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

function isCloudinaryImageUrl(url = "") {
	return /res\.cloudinary\.com\/[^/]+\/image\/upload\//i.test(
		String(url || ""),
	);
}

function normalizeThumbnailSeedEntry(input, sourceType = "seed") {
	if (!input) return null;
	const obj = typeof input === "string" ? { url: input } : input;
	const url = String(
		obj.url ||
			obj.image ||
			obj.imageurl ||
			obj.imageUrl ||
			obj.link ||
			obj.originalUrl ||
			"",
	).trim();
	if (!isHttpUrl(url) || isLikelyThumbnailUrl(url)) return null;
	return {
		url,
		sourceType: String(obj.sourceType || sourceType || "seed"),
		source: String(obj.source || obj.pageUrl || obj.contextLink || "").trim(),
		title: String(obj.title || obj.description || obj.caption || "").trim(),
		description: String(obj.description || obj.title || obj.caption || "").trim(),
		width: Number(obj.width || obj.w || 0) || 0,
		height: Number(obj.height || obj.h || 0) || 0,
	};
}

function dedupeThumbnailSeedEntries(entries = [], { limit = Infinity } = {}) {
	const out = [];
	const seen = new Set();
	for (const raw of Array.isArray(entries) ? entries : []) {
		const entry = normalizeThumbnailSeedEntry(raw);
		if (!entry) continue;
		const key = normalizeImageUrlKey(entry.url);
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(entry);
		if (out.length >= limit) break;
	}
	return out;
}

function buildThumbnailSeedCandidateEntries(topicObj = {}) {
	const story = topicObj?.trendStory || {};
	const potentialEntries = (Array.isArray(story.potentialImages)
		? story.potentialImages
		: []
	)
		.map((p) =>
			normalizeThumbnailSeedEntry(
				{
					...p,
					sourceType: p?.source === "google-images" ? "google-images" : "trend",
				},
				"trend",
			),
		)
		.filter(Boolean);
	const topicSeeds = [
		topicObj?.image,
		...(Array.isArray(topicObj?.images) ? topicObj.images : []),
	]
		.map((url) => normalizeThumbnailSeedEntry({ url }, "topic"))
		.filter(Boolean);
	const storySeeds = [
		story.image,
		...(Array.isArray(story.images) ? story.images : []),
		...(Array.isArray(story.articles)
			? story.articles.map((a) => ({
					url: a?.image,
					source: a?.url,
					title: a?.title,
					description: a?.title,
					sourceType: "article-og",
				}))
			: []),
	]
		.map((item) => normalizeThumbnailSeedEntry(item, "article-og"))
		.filter(Boolean);
	return dedupeThumbnailSeedEntries(
		[
			...potentialEntries.slice(0, 12),
			...topicSeeds.slice(0, 8),
			...storySeeds.slice(0, 12),
		],
		{ limit: 24 },
	);
}

const SEARCH_ONLY_THUMBNAIL_SOURCE_TYPES = new Set([
	"cse",
	"cse-query",
	"google-images",
]);

function thumbnailSourceTrust(sourceType = "") {
	const type = String(sourceType || "").toLowerCase();
	if (
		type === "article-og" ||
		type === "news-og" ||
		type === "cse" ||
		type === "cse-query" ||
		type === "wikipedia" ||
		type === "wikimedia"
	) {
		return 4;
	}
	if (type === "google-images" || type === "topic") return 3;
	if (type === "article" || type === "fallback" || type === "trend") return 2;
	return 1;
}

function thumbnailCandidateUrlTokenHits(entry = {}, topicLabel = "") {
	const tokens = expandTopicTokens(
		filterSpecificTopicTokens(topicTokensFromTitle(topicLabel)),
	);
	if (!tokens.length) return 0;
	const text = normalizeWhitespace(`${entry.source || ""} ${entry.url || ""}`)
		.toLowerCase()
		.replace(/[_-]+/g, " ");
	let hits = 0;
	for (const tok of tokens) {
		if (text.includes(tok)) hits += 1;
	}
	return hits;
}

function hasKnownThumbnailMismatch(entry = {}, topicLabel = "") {
	const topic = normalizeWhitespace(topicLabel).toLowerCase();
	const text = normalizeWhitespace(
		`${entry.title || ""} ${entry.description || ""} ${entry.source || ""} ${
			entry.url || ""
		}`,
	)
		.toLowerCase()
		.replace(/[_-]+/g, " ");
	if (/\bdavid kendall\b/.test(topic) && /\bjenner\b/.test(text)) {
		return true;
	}
	return false;
}

function thumbnailEffectiveSourceTrust(entry = {}, topicLabel = "") {
	const baseTrust = thumbnailSourceTrust(entry.sourceType);
	const type = String(entry.sourceType || "").toLowerCase();
	if (!SEARCH_ONLY_THUMBNAIL_SOURCE_TYPES.has(type)) return baseTrust;
	const tokens = filterSpecificTopicTokens(topicTokensFromTitle(topicLabel));
	const requiredHits = Math.min(2, Math.max(1, tokens.length));
	const urlHits = thumbnailCandidateUrlTokenHits(entry, topicLabel);
	if (urlHits < requiredHits) return Math.min(baseTrust, 1);
	return baseTrust;
}

function thumbnailCandidateTextScore(entry = {}, topicLabel = "") {
	const tokens = expandTopicTokens(
		filterSpecificTopicTokens(topicTokensFromTitle(topicLabel)),
	);
	if (!tokens.length) return 0;
	const type = String(entry.sourceType || "").toLowerCase();
	const fields = SEARCH_ONLY_THUMBNAIL_SOURCE_TYPES.has(type)
		? [entry.source, entry.url].filter(Boolean)
		: [entry.title, entry.description, entry.source, entry.url].filter(Boolean);
	const info = topicMatchInfo(tokens, fields);
	let urlMatches = 0;
	const urlText = `${entry.url || ""} ${entry.source || ""}`.toLowerCase();
	for (const tok of tokens) {
		if (urlText.includes(tok)) urlMatches += 1;
	}
	return info.count + urlMatches * 0.75;
}

function thumbnailReferenceConfidence(entry = {}, topicLabel = "") {
	if (hasKnownThumbnailMismatch(entry, topicLabel)) return "weak";
	const trust = thumbnailEffectiveSourceTrust(entry, topicLabel);
	const textScore = thumbnailCandidateTextScore(entry, topicLabel);
	if (trust >= 4 && textScore >= 1) return "high";
	if (trust >= 4) return "medium";
	if (trust >= 3 && textScore >= 1) return "high";
	if (trust >= 3) return "medium";
	if (trust >= 2 && textScore >= 1) return "medium";
	return "weak";
}

function hasTrustedThumbnailEntries(entries = [], topicLabel = "") {
	return countTrustedThumbnailEntries(entries, topicLabel) > 0;
}

function countTrustedThumbnailEntries(entries = [], topicLabel = "") {
	return (Array.isArray(entries) ? entries : []).filter((entry) => {
		const confidence = thumbnailReferenceConfidence(entry, topicLabel);
		return confidence === "high" || confidence === "medium";
	}).length;
}

function buildThumbnailSearchQueries({ topicLabel = "", imageQueries = [] } = {}) {
	const topic = cleanTopicLabel(topicLabel);
	return uniqueStrings(
		[
			...(Array.isArray(imageQueries) ? imageQueries : []),
			topic ? `${topic} portrait` : "",
			topic ? `${topic} close up` : "",
			topic ? `${topic} press photo` : "",
			topic ? `${topic} news photo` : "",
			topic,
		].filter(Boolean),
		{ limit: 6 },
	).map(sanitizeOverlayQuery).filter(Boolean);
}

async function collectThumbnailSearchCandidateEntries({
	topicLabel,
	imageQueries = [],
	baseUrl,
	jobId,
	log,
	target = 6,
	allowCse = false,
}) {
	const topic = cleanTopicLabel(topicLabel);
	if (!topic) return [];
	const entries = [];
	const targetCount = clampNumber(Number(target) || 6, 2, 10);
	const topicTokens = filterSpecificTopicTokens(topicTokensFromTitle(topic));
	const queries = buildThumbnailSearchQueries({ topicLabel: topic, imageQueries });

	if (entries.length < 2) {
		const wiki = await fetchWikipediaPageImageUrl(topic);
		if (wiki) {
			entries.push(
				normalizeThumbnailSeedEntry(
					{
						url: wiki,
						sourceType: "wikipedia",
						title: topic,
						description: topic,
					},
					"wikipedia",
				),
			);
		}
	}

	if (entries.length < 2) {
		const commons = await fetchWikimediaImageUrls(topic, 3);
		entries.push(
			...commons.map((url) =>
				normalizeThumbnailSeedEntry(
					{
						url,
						sourceType: "wikimedia",
						title: topic,
						description: topic,
					},
					"wikimedia",
				),
			),
		);
	}

	if (entries.length < 2 && GOOGLE_IMAGES_SEARCH_ENABLED) {
		for (const query of queries.slice(0, 3)) {
			if (entries.length >= targetCount) break;
			const urls = await fetchGoogleImagesFromService(query, {
				limit: Math.max(12, targetCount * 3),
				baseUrl,
				jobId,
			});
			entries.push(
				...urls.map((url) =>
					normalizeThumbnailSeedEntry(
						{
							url,
							sourceType: "google-images",
							title: query,
							description: query,
						},
						"google-images",
					),
				),
			);
		}
	}

	if (allowCse && GOOGLE_CSE_CONFIG_READY) {
		for (const query of queries.slice(0, 3)) {
			if (entries.length >= targetCount) break;
			const urls = await fetchCseImagesForQuery(
				query,
				topicTokens,
				Math.max(3, targetCount - entries.length),
				jobId,
				{ maxPages: 2 },
			);
			entries.push(
				...urls.map((url) =>
					normalizeThumbnailSeedEntry(
						{ url, sourceType: "cse-query", title: query, description: query },
						"cse-query",
					),
				),
			);
		}
		if (entries.length < targetCount) {
			const urls = await fetchCseImages(topic, [], jobId, {
				maxResults: Math.max(4, targetCount - entries.length),
				maxPages: 2,
			});
			entries.push(
				...urls.map((url) =>
					normalizeThumbnailSeedEntry(
						{ url, sourceType: "cse", title: topic, description: topic },
						"cse",
					),
				),
			);
		}
	}

	const deduped = dedupeThumbnailSeedEntries(entries, { limit: targetCount });
	if (typeof log === "function" && deduped.length) {
		log("thumbnail seed smart topup", {
			topic,
			count: deduped.length,
			queries: queries.slice(0, 4),
			allowCse,
		});
	}
	return deduped;
}

async function collectThumbnailFeedImageEntries({
	topicLabel,
	articleUrls = [],
	storyArticles = [],
	jobId,
	limit = 8,
}) {
	const topic = cleanTopicLabel(topicLabel);
	if (!topic) return [];
	const target = clampNumber(Number(limit) || 8, 2, 12);
	const topicTokens = filterSpecificTopicTokens(topicTokensFromTitle(topic));
	const requiredTopicMatches = minImageTopicTokenMatches(topicTokens);
	const entries = [];
	const articleItems = [];
	for (const article of Array.isArray(storyArticles) ? storyArticles : []) {
		const pageUrl = String(article?.url || "").trim();
		const title = String(article?.title || "").trim();
		const image = String(article?.image || "").trim();
		const fields = [title, pageUrl];
		if (
			requiredTopicMatches &&
			topicMatchInfo(topicTokens, fields).count < requiredTopicMatches
		) {
			continue;
		}
		if (isHttpUrl(image) && !isLikelyThumbnailUrl(image)) {
			entries.push(
				normalizeThumbnailSeedEntry(
					{
						url: image,
						source: pageUrl,
						title,
						description: title,
						sourceType: "article-og",
					},
					"article-og",
				),
			);
		}
		if (isHttpUrl(pageUrl))
			articleItems.push({ url: pageUrl, title, sourceType: "article-og" });
	}
	for (const pageUrl of Array.isArray(articleUrls) ? articleUrls : []) {
		if (!isHttpUrl(pageUrl)) continue;
		if (
			requiredTopicMatches &&
			topicMatchInfo(topicTokens, [pageUrl]).count <
				Math.max(1, Math.min(requiredTopicMatches, 2))
		) {
			continue;
		}
		articleItems.push({ url: pageUrl, title: topic, sourceType: "article-og" });
	}
	if (entries.length < target) {
		const newsUrls = await fetchNewsArticleUrlsForImages({
			query: topic,
			topicLabel: topic,
			limit: NEWS_IMAGE_FALLBACK_LIMIT,
			jobId,
		});
		for (const newsUrl of newsUrls) {
			if (!isHttpUrl(newsUrl)) continue;
			articleItems.push({ url: newsUrl, title: topic });
		}
	}

	const seenPages = new Set();
	for (const item of articleItems) {
		if (entries.length >= target) break;
		const pageUrl = item?.url || "";
		const key = normalizeImageUrlKey(pageUrl);
		if (!pageUrl || seenPages.has(key)) continue;
		seenPages.add(key);
		const og = await fetchOpenGraphImageUrl(pageUrl);
		if (!og || isLikelyThumbnailUrl(og)) continue;
		if (!isProbablyDirectImageUrl(og)) {
			const ct = await headContentType(og, 7000);
			if (ct && !ct.startsWith("image/")) continue;
		}
		entries.push(
			normalizeThumbnailSeedEntry(
				{
					url: og,
					source: pageUrl,
					title: item?.title || topic,
					description: item?.title || topic,
					sourceType: item?.sourceType || "news-og",
				},
				"news-og",
			),
		);
	}

	return dedupeThumbnailSeedEntries(entries.filter(Boolean), { limit: target });
}

async function collectThumbnailFallbackUrls({
	topicLabel,
	articleUrls = [],
	seedUrls = [],
	limit = 6,
}) {
	const target = clampNumber(Number(limit) || 6, 1, 10);
	const urls = uniqueStrings(seedUrls, { limit: Math.max(6, target * 2) });
	const articleCandidates = uniqueStrings(articleUrls, { limit: 6 });
	for (const pageUrl of articleCandidates) {
		if (urls.length >= target) break;
		const og = await fetchOpenGraphImageUrl(pageUrl);
		if (!og || isLikelyThumbnailUrl(og)) continue;
		if (isProbablyDirectImageUrl(og)) {
			urls.push(og);
			continue;
		}
		const ct = await headContentType(og, 7000);
		if (ct && !ct.startsWith("image/")) continue;
		urls.push(og);
	}
	if (urls.length < target && topicLabel) {
		const wiki = await fetchWikipediaPageImageUrl(topicLabel);
		if (wiki && !isLikelyThumbnailUrl(wiki)) urls.push(wiki);
	}
	if (urls.length < target && topicLabel) {
		const commons = await fetchWikimediaImageUrls(
			topicLabel,
			Math.max(2, target - urls.length),
		);
		urls.push(...commons.filter((u) => !isLikelyThumbnailUrl(u)));
	}
	return uniqueStrings(urls, { limit: target });
}

async function selectBestThumbnailSeedCandidate({
	urls = [],
	tmpDir,
	jobId,
	topicLabel,
	maxDownloads = 6,
	minEdge = CSE_MIN_IMAGE_SHORT_EDGE,
	relaxedEdge = CSE_RELAXED_MIN_IMAGE_SHORT_EDGE,
}) {
	const candidates = dedupeThumbnailSeedEntries(urls).filter(
		(entry) => isHttpUrl(entry.url) && !isLikelyThumbnailUrl(entry.url),
	);
	const safeCandidates = candidates.filter(
		(entry) => !hasKnownThumbnailMismatch(entry, topicLabel),
	);
	const rankedCandidates = safeCandidates.length ? safeCandidates : candidates;
	rankedCandidates.sort((a, b) => {
		const trustDelta =
			thumbnailEffectiveSourceTrust(b, topicLabel) -
			thumbnailEffectiveSourceTrust(a, topicLabel);
		if (trustDelta) return trustDelta;
		return (
			thumbnailCandidateTextScore(b, topicLabel) -
			thumbnailCandidateTextScore(a, topicLabel)
		);
	});
	for (const entry of rankedCandidates) {
		if (isCloudinaryImageUrl(entry.url)) {
			return {
				url: entry.url,
				cloudinary: true,
				sourceType: entry.sourceType,
				confidence: thumbnailReferenceConfidence(entry, topicLabel),
				sourceTrust: thumbnailEffectiveSourceTrust(entry, topicLabel),
				textScore: thumbnailCandidateTextScore(entry, topicLabel),
			};
		}
	}
	let attempts = 0;
	let best = null;
	for (const entry of rankedCandidates) {
		if (attempts >= maxDownloads) break;
		const url = entry.url;
		const looksDirect = isProbablyDirectImageUrl(url);
		if (!looksDirect) {
			const ct = await headContentType(url, 7000);
			if (ct && !ct.startsWith("image/")) continue;
		}
		const extGuess = path
			.extname(String(url).split("?")[0] || "")
			.toLowerCase();
		const ext = extGuess && extGuess.length <= 5 ? extGuess : ".jpg";
		const outPath = path.join(
			tmpDir,
			`thumb_seed_${safeSlug(
				topicLabel || "topic",
				24,
			)}_${jobId}_${attempts}_${crypto.randomUUID().slice(0, 8)}${ext}`,
		);
		try {
			attempts += 1;
			await downloadToFile(url, outPath, 25000, 2);
			const detected = detectFileType(outPath);
			if (!detected || detected.kind !== "image") {
				safeUnlink(outPath);
				continue;
			}
			const info = await probeMedia(outPath);
			const stream = Array.isArray(info?.streams)
				? info.streams.find((s) => s.codec_type === "video")
				: null;
			const width = Number(stream?.width || 0);
			const height = Number(stream?.height || 0);
			if (!width || !height) {
				safeUnlink(outPath);
				continue;
			}
			const aspectRatio = height / width;
			const shortEdge = Math.min(width, height);
			const area = width * height;
			const tier = shortEdge >= minEdge ? 2 : shortEdge >= relaxedEdge ? 1 : 0;
			const portraitBoost =
				Number.isFinite(aspectRatio) && aspectRatio >= 1.08
					? Math.min((aspectRatio - 1) * 0.12, 0.22)
					: 0;
			const trust = thumbnailEffectiveSourceTrust(entry, topicLabel);
			const textScore = thumbnailCandidateTextScore(entry, topicLabel);
			const score =
				trust * 1e12 + textScore * 1e10 + tier * 1e9 + area * (1 + portraitBoost);
			if (!best || score > best.score) {
				if (best?.localPath) safeUnlink(best.localPath);
				best = {
					url,
					localPath: outPath,
					width,
					height,
					shortEdge,
					score,
					sourceType: entry.sourceType,
					confidence: thumbnailReferenceConfidence(entry, topicLabel),
					sourceTrust: trust,
					textScore,
				};
			} else {
				safeUnlink(outPath);
			}
		} catch {
			safeUnlink(outPath);
		}
	}
	return best;
}

function applyThumbnailSeedSelectionMeta(
	topic,
	selected = {},
	source = "seed",
	fallbackConfidence = "weak",
) {
	if (!topic || !selected) return;
	const textScore = Number(selected.textScore);
	const sourceTrust = Number(selected.sourceTrust);
	topic.thumbnailImageConfidence = selected.confidence || fallbackConfidence;
	topic.thumbnailImageSourceType = selected.sourceType || source || "seed";
	topic.thumbnailImageTextScore = Number.isFinite(textScore) ? textScore : 0;
	topic.thumbnailImageSourceTrust = Number.isFinite(sourceTrust)
		? sourceTrust
		: 0;
}

const SOFT_PERSON_THUMBNAIL_SOURCE_TYPES = new Set([
	"article",
	"existing",
	"fallback",
	"seed",
	"topic",
	"trend",
]);

function thumbnailTopicHasPersonAnchor(label = "") {
	if (looksLikeQuestionTopic(label)) return false;
	if (looksLikePersonName(label)) return true;
	const words = cleanTopicLabel(label)
		.replace(/[^a-zA-Z\s'.-]/g, " ")
		.replace(/\s+/g, " ")
		.trim()
		.split(/\s+/)
		.filter(Boolean);
	for (const count of [2, 3]) {
		if (
			words.length >= count &&
			looksLikePersonName(words.slice(0, count).join(" "))
		) {
			return true;
		}
	}
	return false;
}

function isSoftPersonThumbnailSelection(selected = {}, topicLabel = "") {
	if (!selected || !thumbnailTopicHasPersonAnchor(topicLabel)) return false;
	const sourceType = String(selected.sourceType || "").toLowerCase();
	const confidence = String(selected.confidence || "").toLowerCase();
	return (
		confidence !== "high" &&
		SOFT_PERSON_THUMBNAIL_SOURCE_TYPES.has(sourceType)
	);
}

async function ensureThumbnailSeedImages({
	topics = [],
	tmpDir,
	jobId,
	output,
	baseUrl,
	imageQueries = [],
	log,
}) {
	if (!Array.isArray(topics) || !topics.length) return;
	const outputCfg =
		output && typeof output === "object"
			? output
			: parseRatio(output || DEFAULT_OUTPUT_RATIO);
	const cseAvailable = GOOGLE_CSE_CONFIG_READY;
	for (let i = 0; i < topics.length; i++) {
		const t = topics[i];
		if (!t) continue;
		const topicLabel = String(t.displayTopic || t.topic || "").trim();
		const existingRaw = uniqueStrings(
			Array.isArray(t.thumbnailImageUrls) ? t.thumbnailImageUrls : [],
			{ limit: 4 },
		);
		const existingCloudinary = existingRaw.filter(isCloudinaryImageUrl);
		if (existingCloudinary.length) {
			t.thumbnailImageUrls = existingCloudinary;
			t.thumbnailImageConfidence = t.thumbnailImageConfidence || "medium";
			if (log)
				log("thumbnail seed image reused", {
					topic: topicLabel,
					count: existingCloudinary.length,
					confidence: t.thumbnailImageConfidence,
				});
			continue;
		}

		const candidateEntries = dedupeThumbnailSeedEntries(
			[
				...existingRaw.map((url) =>
					normalizeThumbnailSeedEntry(
						{ url, sourceType: "existing" },
						"existing",
					),
				),
				...buildThumbnailSeedCandidateEntries(t),
			],
			{ limit: 28 },
		);
		const story = t.trendStory || {};
		const storyArticles = Array.isArray(story.articles) ? story.articles : [];
		const articleUrls = (Array.isArray(story.articles) ? story.articles : [])
			.map((a) => a?.url)
			.filter((u) => isHttpUrl(u));
		const feedEntries = await collectThumbnailFeedImageEntries({
			topicLabel,
			articleUrls,
			storyArticles,
			jobId,
			limit: 8,
		});
		if (feedEntries.length && log) {
			log("thumbnail feed image candidates", {
				topic: topicLabel,
				count: feedEntries.length,
				trusted: countTrustedThumbnailEntries(feedEntries, topicLabel),
			});
		}
		const baseCandidateEntries = dedupeThumbnailSeedEntries(
			[...feedEntries, ...candidateEntries],
			{ limit: 34 },
		);

		const trustedSeedCount = countTrustedThumbnailEntries(
			baseCandidateEntries,
			topicLabel,
		);
		const shouldSmartTopUp =
			topicLabel && (baseCandidateEntries.length < 4 || trustedSeedCount < 2);
		let searchEntries = [];
		if (shouldSmartTopUp) {
			searchEntries = await collectThumbnailSearchCandidateEntries({
				topicLabel,
				imageQueries,
				baseUrl,
				jobId,
				log,
				target: 8,
			});
		}
		const trustedFeedAvailable = hasTrustedThumbnailEntries(
			feedEntries,
			topicLabel,
		);
		const preferredEntries = dedupeThumbnailSeedEntries(
			trustedFeedAvailable
				? [...feedEntries, ...searchEntries, ...candidateEntries]
				: hasTrustedThumbnailEntries(searchEntries, topicLabel)
					? [...searchEntries, ...baseCandidateEntries]
					: [...baseCandidateEntries, ...searchEntries],
			{ limit: 30 },
		);

		let selected = await selectBestThumbnailSeedCandidate({
			urls: preferredEntries,
			tmpDir,
			jobId,
			topicLabel,
		});
		let source = selected?.sourceType || "seed";
		if (isSoftPersonThumbnailSelection(selected, topicLabel)) {
			if (selected?.localPath) safeUnlink(selected.localPath);
			if (log)
				log("thumbnail seed image deferred", {
					topic: topicLabel,
					reason: "soft_person_reference",
					source,
					confidence: selected?.confidence || null,
				});
			selected = null;
		}

		if (!selected) {
			const fallbackUrls = await collectThumbnailFallbackUrls({
				topicLabel,
				articleUrls,
				seedUrls: preferredEntries
					.filter(
						(entry) =>
							!isSoftPersonThumbnailSelection(
								{
									sourceType: entry.sourceType,
									confidence: thumbnailReferenceConfidence(entry, topicLabel),
								},
								topicLabel,
							),
					)
					.map((entry) => entry.url),
				limit: 6,
			});
			if (fallbackUrls.length) {
				source = "fallback";
				selected = await selectBestThumbnailSeedCandidate({
					urls: fallbackUrls.map((url) =>
						normalizeThumbnailSeedEntry({ url, sourceType: "fallback" }, "fallback"),
					),
					tmpDir,
					jobId,
					topicLabel,
				});
				if (isSoftPersonThumbnailSelection(selected, topicLabel)) {
					if (selected?.localPath) safeUnlink(selected.localPath);
					if (log)
						log("thumbnail seed image deferred", {
							topic: topicLabel,
							reason: "soft_person_reference",
							source: selected?.sourceType || source,
							confidence: selected?.confidence || null,
						});
					selected = null;
				}
			}
		}

		if (!selected && cseAvailable && topicLabel) {
			const cseUrls = await fetchCseImages(
				topicLabel,
				Array.isArray(t.keywords) ? t.keywords : [],
				jobId,
				{ maxResults: 8, maxPages: 1 },
			);
			if (log && cseUrls.length) {
				log("thumbnail seed image cse fallback", {
					topic: topicLabel,
					count: cseUrls.length,
				});
			}
			source = "cse";
			selected = await selectBestThumbnailSeedCandidate({
				urls: cseUrls.map((url) =>
					normalizeThumbnailSeedEntry({ url, sourceType: "cse" }, "cse"),
				),
				tmpDir,
				jobId,
				topicLabel,
			});
		}

		if (!selected) {
			if (log)
				log("thumbnail seed image missing", {
					topic: topicLabel,
				});
			continue;
		}

		if (selected.cloudinary && selected.url) {
			t.thumbnailImageUrls = [selected.url];
			applyThumbnailSeedSelectionMeta(t, selected, source, "medium");
			if (log)
				log("thumbnail seed image selected", {
					topic: topicLabel,
					source: selected.sourceType || "cloudinary",
					confidence: t.thumbnailImageConfidence,
					url: selected.url,
				});
			continue;
		}

		if (selected.localPath && fs.existsSync(selected.localPath)) {
			const slug = safeSlug(topicLabel || "topic", 36) || "topic";
			const publicIdBase = `aivideomatic/long_feed/thumb_seed_${slug}_${jobId}_${
				i + 1
			}`;
			try {
				const uploaded = await uploadLocalImageToCloudinary(
					selected.localPath,
					{
						publicIdBase,
						output: outputCfg,
						jobId,
						segIndex: `thumb_${i + 1}`,
					},
				);
				if (uploaded?.url) {
					t.thumbnailImageUrls = [uploaded.url];
					applyThumbnailSeedSelectionMeta(t, selected, source, "medium");
					if (log)
						log("thumbnail seed image uploaded", {
							topic: topicLabel,
							source: selected.sourceType || source,
							confidence: t.thumbnailImageConfidence,
							url: uploaded.url,
							width: selected.width,
							height: selected.height,
						});
				} else if (selected.url) {
					t.thumbnailImageUrls = [selected.url];
					applyThumbnailSeedSelectionMeta(t, selected, source, "weak");
					if (log)
						log("thumbnail seed image upload missing; using raw url", {
							topic: topicLabel,
							source: selected.sourceType || source,
							confidence: t.thumbnailImageConfidence,
							url: selected.url,
						});
				}
			} catch (e) {
				if (selected.url) t.thumbnailImageUrls = [selected.url];
				if (selected.url)
					applyThumbnailSeedSelectionMeta(t, selected, source, "weak");
				if (log)
					log("thumbnail seed image upload failed", {
						topic: topicLabel,
						error: e.message,
					});
			} finally {
				safeUnlink(selected.localPath);
			}
			continue;
		}

		if (selected.url) {
			t.thumbnailImageUrls = [selected.url];
			applyThumbnailSeedSelectionMeta(t, selected, source, "weak");
			if (log)
				log("thumbnail seed image selected", {
					topic: topicLabel,
					source: selected.sourceType || source,
					confidence: t.thumbnailImageConfidence,
					url: selected.url,
				});
		}
	}
}

async function fetchFallbackImageUrlsForSegment({
	query,
	topicLabel,
	category = "",
	limit = 4,
	articleUrls = [],
	seedUrls = [],
	jobId,
	allowCseContext = CSE_CONTEXT_FALLBACK_ENABLED,
	allowNewsFallback = NEWS_IMAGE_FALLBACK_ENABLED,
}) {
	const target = clampNumber(Number(limit) || 4, 1, 8);
	const urls = [];
	const seeded = uniqueStrings(seedUrls, { limit: Math.max(6, target * 2) });
	urls.push(...seeded);
	if (urls.length >= target) return uniqueStrings(urls, { limit: target });

	const topicTokens = filterSpecificTopicTokens(
		topicTokensFromTitle(topicLabel || query || ""),
	);
	const requiredTopicMatches = minImageTopicTokenMatches(topicTokens);
	const relatedQueries = uniqueStrings(
		[
			query,
			...buildTopicNearImageQueries(topicLabel || query, { category }),
		].filter(Boolean),
		{ limit: 5 },
	);
	const newsArticleUrls = [];
	if (allowNewsFallback) {
		for (const relatedQuery of relatedQueries) {
			if (newsArticleUrls.length >= NEWS_IMAGE_FALLBACK_LIMIT) break;
			const urls = await fetchNewsArticleUrlsForImages({
				query: relatedQuery,
				topicLabel,
				limit: NEWS_IMAGE_FALLBACK_LIMIT,
				jobId,
			});
			newsArticleUrls.push(...urls);
		}
	}
	const directArticleUrls = (Array.isArray(articleUrls) ? articleUrls : []).filter(
		(u) => {
			if (!isHttpUrl(u)) return false;
			if (!requiredTopicMatches) return true;
			return (
				topicMatchInfo(topicTokens, [u]).count >=
				Math.max(1, Math.min(requiredTopicMatches, 2))
			);
		},
	);
	const contextArticleUrls = uniqueStrings(
		[...directArticleUrls, ...newsArticleUrls.filter(isHttpUrl)],
		{ limit: 12 },
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

	if (urls.length < target && allowCseContext) {
		const contextItems = await fetchCseContext(
			query || topicLabel,
			topicLabel ? [topicLabel] : [],
		);
		const strictContextItems = requiredTopicMatches
			? contextItems.filter(
					(it) =>
						topicMatchInfo(topicTokens, [it.title, it.snippet, it.link])
							.count >= requiredTopicMatches,
				)
			: contextItems;
		const cseArticleUrls = uniqueStrings(
			strictContextItems.map((c) => c?.link).filter(Boolean),
			{ limit: 8 },
		);
		for (const pageUrl of cseArticleUrls) {
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
		if (jobId)
			logJob(jobId, "segment fallback CSE context used", {
				query,
				topicLabel,
				articles: cseArticleUrls.length,
				total: urls.length,
			});
	}

	if (urls.length < target && topicLabel) {
		const wiki = await fetchWikipediaPageImageUrl(topicLabel);
		if (wiki) urls.push(wiki);
	}

	if (urls.length < target && topicLabel) {
		const commons = await fetchWikimediaImageUrls(topicLabel, target);
		urls.push(...commons);
	}

	if (urls.length < target) {
		for (const relatedQuery of relatedQueries) {
			if (urls.length >= target) break;
			const commons = await fetchWikimediaImageUrls(
				relatedQuery,
				target - urls.length,
			);
			urls.push(...commons);
		}
		if (jobId) {
			logJob(jobId, "segment fallback Wikimedia context used", {
				query,
				topicLabel,
				queries: relatedQueries,
				total: urls.length,
			});
		}
	}

	return uniqueStrings(urls, { limit: target });
}

function wrapDetailCardLine(text = "", maxChars = 42, maxLines = 2) {
	const words = normalizeWhitespace(text)
		.split(/\s+/)
		.filter(Boolean);
	if (!words.length) return [];
	const lines = [];
	let line = "";
	for (const word of words) {
		const next = line ? `${line} ${word}` : word;
		if (next.length <= maxChars) {
			line = next;
			continue;
		}
		if (line) lines.push(line);
		line = word;
		if (lines.length >= maxLines) break;
	}
	if (line && lines.length < maxLines) lines.push(line);
	if (lines.length > maxLines) lines.length = maxLines;
	const lastIdx = lines.length - 1;
	if (lastIdx >= 0 && words.join(" ").length > lines.join(" ").length + 4) {
		lines[lastIdx] = `${lines[lastIdx].replace(/[.,;:!?]+$/, "")}...`;
	}
	return lines;
}

function isDisplaySafeDetailCardBullet(text = "") {
	const clean = normalizeWhitespace(text);
	if (!clean || countWords(clean) < 4) return false;
	const lower = clean.toLowerCase();
	if (
		/^(requested|frontend prompt|prompt brief|title instruction|seo title|thumbnail|visuals?|avoid|tone|audience|outro|opening line|must include|minute\s+\d+|\d+\s*[- ]?minute\s*structure|structure|why it can work)\b/i.test(
			clean,
		)
	) {
		return false;
	}
	if (
		/\b(requested structure|requested title|requested opening|requested outro|thumbnail badge|user supplied|must include this line|the practical meaning is the part viewers can use|what viewers should actually think|title promise|repair sentence)\b/i.test(
			lower,
		)
	) {
		return false;
	}
	if (/[{}[\]<>]|=>|```/.test(clean)) return false;
	return true;
}

function cleanDetailCardBulletText(text = "") {
	return normalizeWhitespace(text)
		.replace(/^[-*]\s*/, "")
		.replace(/^User supplied fact\/stat to verify:\s*/i, "")
		.replace(/\s*\(?source:\s*[^)]+\)?\s*$/i, "")
		.replace(/\b(?:requested|must include)\s+structure\b.*$/i, "")
		.trim();
}

function extractTopicStatCardBullets(topic = {}, contextItems = []) {
	const promptBrief = topic?.promptBrief || parseStructuredPromptBrief(topic?.promptText);
	const candidates = [];
	const push = (value, source = "") => {
		let text = cleanDetailCardBulletText(value);
		if (!text) return;
		if (!isDisplaySafeDetailCardBullet(text)) return;
		if (!/(\d|%|\$|\bmillion\b|\bbillion\b|\btrillion\b|\bnearly\b|\babout\b|\broughly\b)/i.test(text))
			return;
		text = compactEvidenceText(text, 118);
		if (!isDisplaySafeDetailCardBullet(text)) return;
		const src = source ? formatHumanTitle(source.replace(/^www\./i, ""), 32) : "";
		candidates.push(src ? `${src}: ${text}` : text);
	};
	for (const line of promptBrief?.factLines || []) push(line);
	for (const item of Array.isArray(contextItems) ? contextItems : []) {
		if (typeof item === "string") {
			push(item);
			continue;
		}
		const source = getUrlHost(item?.link || "") || item?.source || "";
		push(`${item?.title || ""} ${item?.snippet || ""}`, source);
	}
	return uniqueStrings(candidates, { limit: 4 });
}

function buildTopicDetailCardPlan(topic = {}, contextItems = []) {
	if (!ENABLE_TOPIC_DETAIL_CARDS || TOPIC_DETAIL_CARD_MAX_PER_TOPIC <= 0)
		return null;
	const bullets = uniqueStrings(
		extractTopicStatCardBullets(topic, contextItems).filter(
			isDisplaySafeDetailCardBullet,
		),
		{ limit: 4 },
	);
	if (!bullets.length) return null;
	const title =
		formatHumanTitle(
			topic?.displayTopic || topic?.topic || topic?.rawTitle || "Key Details",
			62,
		) || "Key Details";
	return { title, bullets };
}

async function createTopicDetailCardImage({
	tmpDir,
	jobId,
	topicIndex = 0,
	title = "",
	bullets = [],
	output,
} = {}) {
	const outCfg = output && typeof output === "object" ? output : parseRatio();
	const w = makeEven(outCfg.w || 1280);
	const h = makeEven(outCfg.h || 720);
	const isVertical = h > w;
	const card = path.join(
		tmpDir,
		`topic_detail_card_${jobId}_${topicIndex}_${crypto.randomUUID()}.jpg`,
	);
	const fontFile = resolveFontFile();
	const fontOpt = fontFile ? `:fontfile='${escapeDrawtext(fontFile)}'` : "";
	const marginX = Math.round(w * (isVertical ? 0.08 : 0.095));
	const titleY = Math.round(h * (isVertical ? 0.16 : 0.18));
	const headingSize = Math.max(22, Math.round(h * (isVertical ? 0.026 : 0.032)));
	const titleSize = Math.max(30, Math.round(h * (isVertical ? 0.041 : 0.052)));
	const bodySize = Math.max(23, Math.round(h * (isVertical ? 0.028 : 0.038)));
	const lineGap = Math.round(bodySize * 1.35);
	const maxChars = isVertical ? 27 : 48;
	const titleLines = wrapDetailCardLine(title, isVertical ? 22 : 34, 2);
	const bodyLines = [];
	for (const bullet of bullets.slice(0, 4)) {
		const wrapped = wrapDetailCardLine(
			bullet.replace(/\.$/, ""),
			maxChars,
			isVertical ? 3 : 2,
		);
		if (!wrapped.length) continue;
		wrapped.forEach((line, idx) => {
			bodyLines.push(`${idx === 0 ? "- " : "  "}${line}`);
		});
	}

	const filters = [
		"format=yuv420p",
		`drawbox=x=0:y=0:w=iw:h=ih:color=0x111827:t=fill`,
		`drawbox=x=${Math.round(w * 0.055)}:y=${Math.round(
			h * 0.16,
		)}:w=${Math.max(6, Math.round(w * 0.009))}:h=${Math.round(
			h * 0.68,
		)}:color=0x38bdf8:t=fill`,
		`drawbox=x=${Math.round(w * 0.055)}:y=${Math.round(
			h * 0.16,
		)}:w=${Math.max(6, Math.round(w * 0.009))}:h=${Math.round(
			h * 0.22,
		)}:color=0xfacc15:t=fill`,
		`drawtext=text='${escapeDrawtext("KEY DETAILS")}'${fontOpt}:fontsize=${headingSize}:fontcolor=0x93c5fd:x=${marginX}:y=${Math.round(
			h * 0.105,
		)}:line_spacing=6`,
	];
	let y = titleY;
	for (const line of titleLines) {
		filters.push(
			`drawtext=text='${escapeDrawtext(
				line,
			)}'${fontOpt}:fontsize=${titleSize}:fontcolor=white:x=${marginX}:y=${y}:line_spacing=8`,
		);
		y += Math.round(titleSize * 1.22);
	}
	y += Math.round(h * 0.045);
	for (const line of bodyLines.slice(0, isVertical ? 9 : 8)) {
		filters.push(
			`drawtext=text='${escapeDrawtext(
				line,
			)}'${fontOpt}:fontsize=${bodySize}:fontcolor=0xe5e7eb:x=${marginX}:y=${y}:line_spacing=8`,
		);
		y += lineGap;
	}
	filters.push(
		`drawtext=text='${escapeDrawtext(
			CHANNEL_NAME,
		)}'${fontOpt}:fontsize=${Math.max(
			18,
			Math.round(h * 0.024),
		)}:fontcolor=white@0.52:x=${marginX}:y=h-th-${Math.round(h * 0.08)}`,
	);

	await spawnBin(
		ffmpegPath,
		[
			"-f",
			"lavfi",
			"-i",
			`color=c=0x111827:s=${w}x${h}:r=1`,
			"-frames:v",
			"1",
			"-vf",
			filters.join(","),
			"-q:v",
			"2",
			"-y",
			card,
		],
		"topic_detail_card",
		{ timeoutMs: IMAGE_PLATE_TIMEOUT_MS },
	);
	return card;
}

async function prepareImageSegments({
	timeline = [],
	topics = [],
	topicContexts = [],
	tmpDir,
	jobId,
	baseUrl,
	output,
	category = "",
}) {
	if (!timeline.length) {
		return {
			timeline,
			segmentImagePaths: new Map(),
			segmentFeedVideoPaths: new Map(),
			imagePlanSummary: [],
			feedVideoPlanSummary: [],
		};
	}

	const cseAvailable = GOOGLE_CSE_CONFIG_READY;
	if (!cseAvailable) {
		logJob(jobId, "image segments CSE disabled; using seed images only");
	}

	const queryCache = new Map();
	const topicCache = new Map();
	const fallbackCache = new Map();
	const googleImageCache = new Map();
	const feedVideoCandidateCache = new Map();
	const usedHosts = new Set();
	const usedUrlsGlobal = new Set();
	const usedFeedVideoUrlsGlobal = new Set();
	const usedUrlsByTopic = new Map();
	const detailCardCountByTopic = new Map();
	const topicMetaByIndex = new Map();
	const imageSegmentTotal = timeline.filter((seg) => seg.visualType === "image")
		.length;
	const feedVideoSegmentLimit =
		FEED_VIDEO_ENABLED && FEED_VIDEO_MAX_SEGMENTS > 0
			? Math.min(
					FEED_VIDEO_MAX_SEGMENTS,
					Math.max(
						0,
						Math.ceil(imageSegmentTotal * FEED_VIDEO_TARGET_FEED_SHARE),
					),
				)
			: 0;
	let feedVideoSegmentsUsed = 0;
	let feedVideoSegmentsAttempted = 0;
	const outputCfg =
		output && typeof output === "object"
			? output
			: parseRatio(output || DEFAULT_OUTPUT_RATIO);

	for (let i = 0; i < (topics || []).length; i++) {
		const t = topics[i] || {};
		const story = t.trendStory || {};
		const contextItems = topicContextItemsAt(topicContexts, i);
		const label = String(t.displayTopic || t.topic || "").trim();
		const keywordHints = uniqueStrings(
			[
				...(Array.isArray(t.keywords) ? t.keywords : []),
				...(story.imageSearchQueries || []),
				...(story.searchPhrases || []),
				...(story.entityNames || []),
			],
			{ limit: 10 },
		);
		const articleTitles = (story.articles || [])
			.map((a) => a.title)
			.filter(Boolean);
		const articleUrls = (story.articles || [])
			.map((a) => a.url)
			.filter((u) => isHttpUrl(u));
		const articleImageUrls = uniqueStrings(
			(story.articles || [])
				.map((a) => a.image)
				.filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u)),
			{ limit: 8 },
		);
		const potentialVideos = Array.isArray(story.potentialVideos)
			? story.potentialVideos
			: [];
		const videoUrls = uniqueStrings(
			[
				...(Array.isArray(t.videos) ? t.videos : []),
				t.video,
				...(Array.isArray(story.videos) ? story.videos : []),
				story.video,
				story.videoUrl,
				...potentialVideos.map((v) => v?.url).filter(Boolean),
			],
			{ limit: 18 },
		).filter((u) => isHttpUrl(u));
		let promptFreeImageUrls = [];
		const isPromptTopic = isUserPromptTopicPick(t);
		if (
			isPromptTopic &&
			GOOGLE_IMAGES_SEARCH_ENABLED &&
			PROMPT_TOPIC_FREE_IMAGE_PREFETCH_ENABLED
		) {
			const promptImageQueries = uniqueStrings(
				[
					...(Array.isArray(t.imageSearchHints) ? t.imageSearchHints : []),
					...(Array.isArray(story.imageSearchQueries)
						? story.imageSearchQueries
						: []),
					...buildTopicNearImageQueries(label, {
						topicKeywords: keywordHints,
						articleTitles,
						category,
					}),
				],
				{ limit: PROMPT_TOPIC_FREE_IMAGE_PREFETCH_QUERY_LIMIT },
			);
			for (const imageQuery of promptImageQueries) {
				const urls = await fetchGoogleImagesFromService(imageQuery, {
					limit: Math.max(
						8,
						Math.min(
							GOOGLE_IMAGES_RESULTS_PER_QUERY,
							PROMPT_TOPIC_FREE_IMAGE_PREFETCH_TARGET,
						),
					),
					baseUrl,
					jobId,
				});
				promptFreeImageUrls.push(...urls);
				if (
					uniqueStrings(promptFreeImageUrls, {
						limit: PROMPT_TOPIC_FREE_IMAGE_PREFETCH_TARGET,
					}).length >= PROMPT_TOPIC_FREE_IMAGE_PREFETCH_TARGET
				)
					break;
			}
			promptFreeImageUrls = uniqueStrings(promptFreeImageUrls, {
				limit: Math.max(24, PROMPT_TOPIC_FREE_IMAGE_PREFETCH_TARGET),
			});
			if (promptFreeImageUrls.length) {
				logJob(jobId, "prompt topic free image pool ready", {
					topic: label,
					queries: promptImageQueries.length,
					count: promptFreeImageUrls.length,
					target: PROMPT_TOPIC_FREE_IMAGE_PREFETCH_TARGET,
				});
			}
		}
		const potentialUrls = uniqueStrings(
			(Array.isArray(story.potentialImages) ? story.potentialImages : [])
				.map((p) => (typeof p === "string" ? p : p?.url))
				.filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u)),
			{ limit: 30 },
		);
		const mergedPotentialUrls = uniqueStrings(
			[...potentialUrls, ...promptFreeImageUrls],
			{ limit: 80 },
		);
		const imageMetaByKey = new Map();
		const addImageMeta = (raw, queryHint = "") => {
			const normalized = normalizeFreeImageMetadataItem(raw, queryHint);
			if (!normalized?.url) return;
			const key = normalizeImageUrlKey(normalized.url);
			if (!key) return;
			imageMetaByKey.set(key, {
				...(imageMetaByKey.get(key) || {}),
				...normalized,
			});
		};
		for (const item of Array.isArray(story.potentialImages)
			? story.potentialImages
			: []) {
			addImageMeta(item, item?.query || label);
		}
		for (const article of Array.isArray(story.articles) ? story.articles : []) {
			if (!article?.image) continue;
			addImageMeta(
				{
					url: article.image,
					title: article.title,
					sourcePage: article.url,
					provider: article.source || "article",
				},
				label,
			);
		}
		for (const url of [...(Array.isArray(t.images) ? t.images : []), t.image]) {
			if (!url) continue;
			addImageMeta({ url, title: label, query: label }, label);
		}
		const potentialKeys = new Set(
			mergedPotentialUrls.map((u) => normalizeImageUrlKey(u)),
		);
		const seedUrls = uniqueStrings(
			[
				...(Array.isArray(t.images) ? t.images : []),
				t.image,
				story.image,
				...(Array.isArray(story.images) ? story.images : []),
				...articleImageUrls,
			],
			{ limit: 18 },
		)
			.filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u))
			.filter((u) => !potentialKeys.has(normalizeImageUrlKey(u)));
		topicMetaByIndex.set(i, {
			label,
			keywordHints,
			articleTitles,
			articleUrls,
			potentialUrls: mergedPotentialUrls,
			seedUrls,
			trustedSeedUrls: articleImageUrls,
			imageMetaByKey,
			videoUrls,
			potentialVideos,
			detailCard: buildTopicDetailCardPlan(t, contextItems),
		});
	}

	const getUsedUrlKeys = (topicIndex) => {
		if (!usedUrlsByTopic.has(topicIndex)) {
			usedUrlsByTopic.set(topicIndex, new Set());
		}
		return usedUrlsByTopic.get(topicIndex);
	};

	const segmentImagePaths = new Map();
	const segmentFeedVideoPaths = new Map();
	const imagePlanSummary = [];
	const feedVideoPlanSummary = [];
	const updated = [];
	const imageSourceSummary = {
		segments: 0,
		plannedUrls: 0,
		potentialUrls: 0,
		seedUrls: 0,
		plannedAvailable: 0,
		cseQueryCalls: 0,
		cseTopicCalls: 0,
		cseCacheHits: 0,
		cseUrls: 0,
		pickedTotal: 0,
		pickedPlanned: 0,
		pickedPotential: 0,
		pickedCse: 0,
		pickedSeed: 0,
		pickedFallback: 0,
		feedVideoSegments: 0,
		feedVideoAttempts: 0,
		feedVideoCandidates: 0,
		feedVideoPicked: 0,
	};

	for (const seg of timeline) {
		if (seg.visualType !== "image") {
			updated.push(seg);
			continue;
		}

		const segDur = Math.max(0.2, Number(seg.endSec) - Number(seg.startSec));
		const desiredCount = computeSegmentImageCount(segDur, seg);
		const holdSingleVisual = isHoldSingleVisualSegment(seg);
		const renderTargetCount = Math.max(
			desiredCount,
			holdSingleVisual
				? desiredCount
				: Math.min(desiredCount + IMAGE_SEGMENT_RENDER_RESERVE, 5),
		);
		const topicIndex = Number(seg.topicIndex) || 0;
		const { query, topicLabel } = resolveSegmentImageQuery(seg, topics);
		const meta = topicMetaByIndex.get(topicIndex) || {};
		const effectiveTopicLabel = topicLabel || meta.label || "";
		const evergreenNonNewsVisual = isEvergreenNonNewsVisualTopic({
			category,
			topicLabel: effectiveTopicLabel,
			text: `${query || ""} ${seg.text || ""}`,
		});
		const topicTokens = topicTokensFromTitle(effectiveTopicLabel || "");
		const segmentTokens = extractSegmentMatchTokens(
			seg.text || "",
			effectiveTopicLabel,
			4,
		);
		const queryVariants = buildSegmentImageQueryVariants({
			baseQuery: query,
			topicLabel: effectiveTopicLabel,
			segmentText: seg.text,
			topicKeywords: meta.keywordHints,
			articleTitles: meta.articleTitles,
			category,
			maxVariants: IMAGE_SEARCH_MAX_QUERY_VARIANTS,
		});
		if (!queryVariants.length && query) queryVariants.push(query);
		if (!queryVariants.length && effectiveTopicLabel)
			queryVariants.push(effectiveTopicLabel);
		const queryTokens = filterSpecificTopicTokens(
			queryVariants.flatMap((q) => tokenizeLabel(q)),
		).slice(0, 16);

		logJob(jobId, "segment image search", {
			segment: seg.index,
			query,
			topicLabel: effectiveTopicLabel,
			desiredCount,
			variantCount: queryVariants.length,
			holdSingleVisual,
			segmentTokens,
			queryTokens: queryTokens.slice(0, 8),
		});

		let feedVideoDownload = { localPaths: [], usedUrls: [] };
		let feedVideoCandidates = [];
		const hasExplicitFeedVideoAssets = Boolean(
			(Array.isArray(meta.videoUrls) && meta.videoUrls.length) ||
				(Array.isArray(meta.potentialVideos) && meta.potentialVideos.length) ||
				(Array.isArray(seg.feedVideoCandidates) && seg.feedVideoCandidates.length),
		);
		const allowSegmentFeedVideo =
			!evergreenNonNewsVisual || hasExplicitFeedVideoAssets;
		if (
			allowSegmentFeedVideo &&
			feedVideoSegmentLimit > 0 &&
			feedVideoSegmentsUsed < feedVideoSegmentLimit &&
			feedVideoSegmentsAttempted < FEED_VIDEO_MAX_SEGMENT_ATTEMPTS &&
			segDur >= FEED_VIDEO_MIN_SEGMENT_SEC
		) {
			feedVideoSegmentsAttempted += 1;
			const feedVideoCacheKey = `${query}||${effectiveTopicLabel}||${seg.index}`;
			feedVideoCandidates = feedVideoCandidateCache.get(feedVideoCacheKey);
			if (!feedVideoCandidates) {
				feedVideoCandidates =
					await collectFeedVideoCandidateEntriesForSegment({
						query,
						topicLabel: effectiveTopicLabel,
						queryVariants,
						topicTokens,
						segmentTokens,
						queryTokens,
						meta: {
							...meta,
							segmentText: seg.text || "",
							potentialVideos: [
								...(Array.isArray(meta.potentialVideos)
									? meta.potentialVideos
									: []),
								...(Array.isArray(seg.feedVideoCandidates)
									? seg.feedVideoCandidates
									: []),
							],
						},
						category,
						jobId,
					});
				feedVideoCandidateCache.set(feedVideoCacheKey, feedVideoCandidates);
			}
			const availableFeedVideoCandidates = (feedVideoCandidates || []).filter(
				(item) =>
					!usedFeedVideoUrlsGlobal.has(
						normalizeFeedVideoKey(item.url || item.pageUrl || ""),
					),
			);
			if (availableFeedVideoCandidates.length) {
				feedVideoDownload = await downloadFeedVideoCandidates({
					candidates: availableFeedVideoCandidates,
					tmpDir,
					jobId,
					segIndex: seg.index,
					targetCount: 1,
				});
				if (feedVideoDownload.localPaths.length) {
					feedVideoSegmentsUsed += 1;
					segmentFeedVideoPaths.set(seg.index, feedVideoDownload.localPaths);
					for (const url of feedVideoDownload.usedUrls || []) {
						usedFeedVideoUrlsGlobal.add(normalizeFeedVideoKey(url));
					}
					feedVideoPlanSummary.push({
						segment: seg.index,
						videoCount: feedVideoDownload.localPaths.length,
						query,
						topicLabel: effectiveTopicLabel,
						sourceUrls: feedVideoDownload.usedUrls,
						candidates: availableFeedVideoCandidates.length,
					});
					logJob(jobId, "segment feed video ready", {
						segment: seg.index,
						query,
						topicLabel: effectiveTopicLabel,
						candidates: availableFeedVideoCandidates.length,
						downloaded: feedVideoDownload.localPaths.length,
					});
				} else {
					logJob(jobId, "segment feed video unavailable", {
						segment: seg.index,
						query,
						topicLabel: effectiveTopicLabel,
						candidates: availableFeedVideoCandidates.length,
					});
				}
			}
		}

		const plannedSegmentUrls = dedupeUrlsPreserveOrder(
			Array.isArray(seg.imageUrls) ? seg.imageUrls : [],
		).filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u));
		const potentialUrls = Array.isArray(meta.potentialUrls)
			? meta.potentialUrls.filter(
					(u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u),
				)
			: [];
		const seedUrls = Array.isArray(meta.seedUrls)
			? meta.seedUrls.filter((u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u))
			: [];
		const trustedSeedUrls = Array.isArray(meta.trustedSeedUrls)
			? meta.trustedSeedUrls.filter(
					(u) => isHttpUrl(u) && !isLikelyThumbnailUrl(u),
				)
			: [];
		const trustedSeedUrlKeys = new Set(
			trustedSeedUrls.map((u) => normalizeImageUrlKey(u)),
		);

		const plannedUrlKeys = new Set(
			plannedSegmentUrls.map((u) => normalizeImageUrlKey(u)),
		);
		const potentialUrlKeys = new Set(
			potentialUrls.map((u) => normalizeImageUrlKey(u)),
		);
		const seedUrlKeys = new Set(seedUrls.map((u) => normalizeImageUrlKey(u)));

		let candidates = dedupeUrlsPreserveOrder([
			...plannedSegmentUrls,
			...potentialUrls,
			...seedUrls,
		]);
		const plannedAvailable = candidates.filter((url) => {
			const key = normalizeImageUrlKey(url);
			return !usedUrlsGlobal.has(key);
		});
		let fallbackUrls = [];
		let fallbackUrlKeys = new Set();
		const baseRelevanceOpts = {
			topicTokens,
			segmentTokens,
			queryTokens,
			trustedUrlKeys: new Set([...plannedUrlKeys, ...trustedSeedUrlKeys]),
			validatedUrlKeys: plannedUrlKeys,
			imageMetaByKey: meta.imageMetaByKey,
		};

		const relevantBeforeFallback = filterRelevantImageCandidatePool(
			plannedAvailable,
			baseRelevanceOpts,
		);
		if (relevantBeforeFallback.length < renderTargetCount) {
			const fallbackKey = `${query}||${effectiveTopicLabel}||news:${
				NEWS_IMAGE_FALLBACK_ENABLED && !evergreenNonNewsVisual ? "1" : "0"
			}`;
			fallbackUrls = fallbackCache.get(fallbackKey);
			if (!fallbackUrls) {
				fallbackUrls = await fetchFallbackImageUrlsForSegment({
					query,
					topicLabel: effectiveTopicLabel,
					limit: Math.max(
						10,
						renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
					),
					articleUrls: meta.articleUrls,
					seedUrls: trustedSeedUrls,
					category,
					jobId,
					allowNewsFallback:
						NEWS_IMAGE_FALLBACK_ENABLED && !evergreenNonNewsVisual,
				});
				fallbackCache.set(fallbackKey, fallbackUrls);
			}
			fallbackUrlKeys = new Set(
				(fallbackUrls || []).map((u) => normalizeImageUrlKey(u)),
			);
			candidates = dedupeUrlsPreserveOrder([
				...candidates,
				...(fallbackUrls || []),
			]);
			logJob(jobId, "segment image candidates (fallback)", {
				segment: seg.index,
				query,
				topicLabel: effectiveTopicLabel,
				relevantBeforeFallback: relevantBeforeFallback.length,
				added: fallbackUrls?.length || 0,
				total: candidates.length,
			});
		}

		const trustedBeforeGoogleKeys = new Set([
			...plannedUrlKeys,
			...trustedSeedUrlKeys,
			...fallbackUrlKeys,
		]);
		const relevantBeforeGoogle = filterRelevantImageCandidatePool(
			candidates.filter((url) => {
				const key = normalizeImageUrlKey(url);
				return !usedUrlsGlobal.has(key);
			}),
			{
				topicTokens,
				segmentTokens,
				queryTokens,
				trustedUrlKeys: trustedBeforeGoogleKeys,
				validatedUrlKeys: plannedUrlKeys,
				imageMetaByKey: meta.imageMetaByKey,
			},
		);
		if (
			GOOGLE_IMAGES_SEARCH_ENABLED &&
			relevantBeforeGoogle.length <
				renderTargetCount * Math.max(1, GOOGLE_IMAGES_MIN_POOL_MULTIPLIER)
		) {
			const googleTargetPool = Math.max(
				renderTargetCount,
				renderTargetCount * Math.max(1, GOOGLE_IMAGES_MIN_POOL_MULTIPLIER),
			);
			const googleVariants = queryVariants.slice(
				0,
				Math.max(1, GOOGLE_IMAGES_VARIANT_LIMIT),
			);
			const googleUrls = [];
			for (const gQuery of googleVariants) {
				const cacheKey = `gimg::${gQuery}`;
				let urls = googleImageCache.get(cacheKey);
				if (!urls) {
					urls = await fetchGoogleImagesFromService(gQuery, {
						limit: Math.max(
							12,
							renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
							GOOGLE_IMAGES_RESULTS_PER_QUERY,
						),
						baseUrl,
						jobId,
					});
					googleImageCache.set(cacheKey, urls);
				}
				googleUrls.push(...(urls || []));
				const trialRelevant = filterRelevantImageCandidatePool(
					dedupeUrlsPreserveOrder([...candidates, ...googleUrls]).filter(
						(url) => {
							const key = normalizeImageUrlKey(url);
							return !usedUrlsGlobal.has(key);
						},
					),
					{
						topicTokens,
						segmentTokens,
						queryTokens,
						trustedUrlKeys: trustedBeforeGoogleKeys,
						validatedUrlKeys: plannedUrlKeys,
						imageMetaByKey: meta.imageMetaByKey,
					},
				);
				if (trialRelevant.length >= googleTargetPool) break;
			}
			if (googleUrls.length) {
				candidates = dedupeUrlsPreserveOrder([...candidates, ...googleUrls]);
				logJob(jobId, "segment google images added", {
					segment: seg.index,
					query,
					topicLabel: effectiveTopicLabel,
					variants: googleVariants.length,
					relevantBeforeGoogle: relevantBeforeGoogle.length,
					added: googleUrls.length,
					total: candidates.length,
				});
			}
		}

		const trustedBeforeCseKeys = new Set([
			...plannedUrlKeys,
			...trustedSeedUrlKeys,
			...fallbackUrlKeys,
		]);
		const availableBeforeCse = filterRelevantImageCandidatePool(
			candidates.filter((url) => {
				const key = normalizeImageUrlKey(url);
				return !usedUrlsGlobal.has(key);
			}),
			{
				topicTokens,
				segmentTokens,
				queryTokens,
				trustedUrlKeys: trustedBeforeCseKeys,
				validatedUrlKeys: plannedUrlKeys,
				imageMetaByKey: meta.imageMetaByKey,
			},
		);
		const allowCseTopUp =
			CSE_IMAGE_TOPUP_ENABLED &&
			cseAvailable &&
			(CSE_IMAGE_LAST_RESORT_ONLY
				? availableBeforeCse.length <
					Math.max(1, Math.min(CSE_IMAGE_FREE_POOL_FLOOR, renderTargetCount))
				: availableBeforeCse.length <
					renderTargetCount * IMAGE_SEARCH_MIN_RANKED_POOL_MULTIPLIER);

		let segmentCseQueryCalls = 0;
		let segmentCseTopicCalls = 0;
		let segmentCseCacheHits = 0;

		const fromQueryUrls = [];
		if (allowCseTopUp) {
			for (const qVariant of queryVariants) {
				const cacheKey = `q::${qVariant}`;
				let urls = queryCache.get(cacheKey);
				if (!urls) {
					segmentCseQueryCalls += 1;
					urls = await fetchCseImagesForQuery(
						qVariant,
						topicTokens,
						Math.max(
							12,
							renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
						),
						jobId,
						{ maxPages: CSE_MAX_PAGES },
					);
					queryCache.set(cacheKey, urls);
				} else {
					segmentCseCacheHits += 1;
				}
				fromQueryUrls.push(...(urls || []));
			}
		}

		let fromTopicUrls = [];
		if (allowCseTopUp && effectiveTopicLabel) {
			const topicKey = `topic::${topicIndex}`;
			fromTopicUrls = topicCache.get(topicKey) || [];
			if (!fromTopicUrls.length) {
				segmentCseTopicCalls += 1;
				const topicExtras = uniqueStrings(
					[...queryVariants, ...(segmentTokens || [])],
					{ limit: 12 },
				);
				fromTopicUrls = await fetchCseImages(
					effectiveTopicLabel,
					topicExtras,
					jobId,
					{
						maxResults: Math.max(
							12,
							renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
						),
						maxPages: CSE_MAX_PAGES,
					},
				);
				topicCache.set(topicKey, fromTopicUrls);
			} else {
				segmentCseCacheHits += 1;
			}
		}

		if (allowCseTopUp && (fromQueryUrls.length || fromTopicUrls.length)) {
			candidates = dedupeUrlsPreserveOrder([
				...candidates,
				...fromQueryUrls,
				...fromTopicUrls,
			]);
		}
		const cseUrlKeys = new Set(
			[...fromQueryUrls, ...fromTopicUrls].map((u) => normalizeImageUrlKey(u)),
		);
		const trustedUrlKeys = new Set([
			...plannedUrlKeys,
			...trustedSeedUrlKeys,
			...fallbackUrlKeys,
			...cseUrlKeys,
		]);

		logJob(jobId, "segment image candidates", {
			segment: seg.index,
			query,
			topicLabel: effectiveTopicLabel,
			queryVariants: queryVariants.length,
			planned: plannedSegmentUrls.length,
			potential: potentialUrls.length,
			seeded: seedUrls.length,
			fromQuery: fromQueryUrls.length,
			fromTopic: fromTopicUrls.length,
			relevantBeforeCse: availableBeforeCse.length,
			total: candidates.length,
			cseTopUp: allowCseTopUp,
		});

		const usedUrlKeys = getUsedUrlKeys(topicIndex);
		const picks = pickSegmentImageUrls(
			candidates,
			renderTargetCount,
			usedUrlKeys,
			usedHosts,
			{
				maxPicks: Math.max(
					renderTargetCount,
					renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
				),
				requireTokens: segmentTokens,
				preferTokens: topicTokens,
				queryTokens,
				topicTokens,
				segmentTokens,
				trustedUrlKeys,
				validatedUrlKeys: plannedUrlKeys,
				imageMetaByKey: meta.imageMetaByKey,
				usedUrlsGlobal,
			},
		);
		let download = await downloadSegmentImages(
			picks,
			tmpDir,
			jobId,
			seg.index,
			renderTargetCount,
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
			renderTargetCount,
			picked: picks.length,
			downloaded: localPaths.length,
		});

		if (localPaths.length < renderTargetCount) {
			const missing = Math.max(0, renderTargetCount - localPaths.length);
			const fallbackKey = `${query}||${effectiveTopicLabel}||news:${
				NEWS_IMAGE_FALLBACK_ENABLED && !evergreenNonNewsVisual ? "1" : "0"
			}`;
			fallbackUrls =
				fallbackCache.get(fallbackKey) ||
				(await fetchFallbackImageUrlsForSegment({
					query,
					topicLabel: effectiveTopicLabel,
					limit: Math.max(
						10,
						renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
					),
					articleUrls: meta.articleUrls,
					seedUrls: trustedSeedUrls,
					category,
					jobId,
					allowNewsFallback:
						NEWS_IMAGE_FALLBACK_ENABLED && !evergreenNonNewsVisual,
				}));
			fallbackCache.set(fallbackKey, fallbackUrls);
			for (const url of fallbackUrls || []) {
				trustedUrlKeys.add(normalizeImageUrlKey(url));
			}
			const fallbackPicks = pickSegmentImageUrls(
				fallbackUrls,
				missing || renderTargetCount,
				usedUrlKeys,
				usedHosts,
				{
					maxPicks: Math.max(
						missing || renderTargetCount,
						(missing || renderTargetCount) * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
					),
					requireTokens: segmentTokens,
					preferTokens: topicTokens,
					queryTokens,
					topicTokens,
					segmentTokens,
					trustedUrlKeys,
					validatedUrlKeys: plannedUrlKeys,
					imageMetaByKey: meta.imageMetaByKey,
					usedUrlsGlobal,
				},
			);
			download = await downloadSegmentImages(
				fallbackPicks,
				tmpDir,
				jobId,
				seg.index,
				missing || renderTargetCount,
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
				renderTargetCount,
				picked: fallbackPicks.length,
				downloaded: localPaths.length,
			});
		}

		if (!localPaths.length && GOOGLE_IMAGES_SEARCH_ENABLED) {
			const rescueQueries = uniqueStrings(
				[
					query,
					...buildTopicNearImageQueries(effectiveTopicLabel, {
						topicKeywords: meta.keywordHints,
						articleTitles: meta.articleTitles,
						category,
					}),
					...(evergreenNonNewsVisual
						? []
						: [
								`${effectiveTopicLabel} news photo`,
								`${effectiveTopicLabel} press photo`,
								`${effectiveTopicLabel} event photo`,
							]),
					effectiveTopicLabel,
				].filter(Boolean),
				{ limit: Math.max(3, GOOGLE_IMAGES_VARIANT_LIMIT) },
			);
			const rescueUrls = [];
			for (const rescueQuery of rescueQueries) {
				const cacheKey = `rescue::${rescueQuery}`;
				let urls = googleImageCache.get(cacheKey);
				if (!urls) {
					urls = await fetchGoogleImagesFromService(rescueQuery, {
						limit: Math.max(
							GOOGLE_IMAGES_RESULTS_PER_QUERY,
							renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
						),
						baseUrl,
						jobId,
					});
					googleImageCache.set(cacheKey, urls);
				}
				rescueUrls.push(...(urls || []));
			}
			const rescuePicks = pickSegmentImageUrls(
				rescueUrls,
				renderTargetCount,
				usedUrlKeys,
				usedHosts,
				{
					maxPicks: Math.max(
						renderTargetCount,
						renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
					),
					preferTokens: topicTokens,
					queryTokens,
					topicTokens,
					segmentTokens,
					trustedUrlKeys,
					validatedUrlKeys: plannedUrlKeys,
					imageMetaByKey: meta.imageMetaByKey,
					usedUrlsGlobal,
				},
			);
			download = await downloadSegmentImages(
				rescuePicks,
				tmpDir,
				jobId,
				seg.index,
				renderTargetCount,
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
			logJob(jobId, "segment image picks (google rescue)", {
				segment: seg.index,
				desiredCount,
				renderTargetCount,
				queries: rescueQueries,
				candidates: rescueUrls.length,
				picked: rescuePicks.length,
				downloaded: localPaths.length,
			});
		}

		if (
			!localPaths.length &&
			cseAvailable &&
			isConceptualOrMetaphoricalVisualTopic({
				category,
				topicLabel: effectiveTopicLabel,
				text: `${query || ""} ${seg.text || ""}`,
			})
		) {
			const conceptualQueries = buildConceptualVisualQueries({
				topicLabel: effectiveTopicLabel,
				segmentText: seg.text || "",
				baseQuery: query,
				category,
				limit: Math.max(6, GOOGLE_IMAGES_VARIANT_LIMIT),
			});
			const conceptualUrls = [];
			for (const conceptQuery of conceptualQueries) {
				const cacheKey = `concept::${conceptQuery}`;
				let urls = queryCache.get(cacheKey);
				if (!urls) {
					urls = await fetchCseImagesForQuery(
						conceptQuery,
						[],
						Math.max(
							renderTargetCount,
							renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
						),
						jobId,
						{
							maxPages: CSE_MAX_PAGES,
							looseTopicRelevance: true,
							allowLooseResults: true,
							relaxedMinEdge: CSE_RELAXED_MIN_IMAGE_SHORT_EDGE,
						},
					);
					queryCache.set(cacheKey, urls);
				} else {
					segmentCseCacheHits += 1;
				}
				conceptualUrls.push(...(urls || []));
				if (dedupeUrlsPreserveOrder(conceptualUrls).length >= renderTargetCount)
					break;
			}
			const conceptualPicks = pickSegmentImageUrls(
				conceptualUrls,
				renderTargetCount,
				usedUrlKeys,
				usedHosts,
				{
					maxPicks: Math.max(
						renderTargetCount,
						renderTargetCount * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
					),
					enforceRelevance: false,
					usedUrlsGlobal,
				},
			);
			download = await downloadSegmentImages(
				conceptualPicks,
				tmpDir,
				jobId,
				seg.index,
				renderTargetCount,
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
			logJob(jobId, "segment conceptual feed images", {
				segment: seg.index,
				topicLabel: effectiveTopicLabel,
				queries: conceptualQueries,
				candidates: conceptualUrls.length,
				picked: conceptualPicks.length,
				downloaded: localPaths.length,
			});
		}

		if (!STRICT_TOPIC_RELEVANT_FEED_IMAGES && localPaths.length < renderTargetCount) {
			const missing = Math.max(1, renderTargetCount - localPaths.length);
			const alreadyPickedKeys = new Set(
				pickedUrls.map((u) => normalizeImageUrlKey(u)),
			);
			const relaxedPool = dedupeUrlsPreserveOrder([
				...plannedSegmentUrls,
				...potentialUrls,
				...trustedSeedUrls,
				...seedUrls,
				...(fallbackUrls || []),
				...candidates,
			]).filter((u) => !alreadyPickedKeys.has(normalizeImageUrlKey(u)));
			let relaxedPicks = pickSegmentImageUrls(
				relaxedPool,
				missing,
				usedUrlKeys,
				null,
				{
					maxPicks: Math.max(
						missing,
						missing * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
					),
					enforceRelevance: false,
					usedUrlsGlobal,
				},
			);

			if (!relaxedPicks.length && relaxedPool.length) {
				relaxedPicks = pickSegmentImageUrls(
					relaxedPool,
					missing,
					new Set(),
					null,
					{
						maxPicks: Math.max(
							missing,
							missing * IMAGE_SEARCH_CANDIDATE_MULTIPLIER,
						),
						enforceRelevance: false,
					},
				);
			}

			if (relaxedPicks.length) {
				download = await downloadSegmentImages(
					relaxedPicks,
					tmpDir,
					jobId,
					seg.index,
					missing,
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
			}

			logJob(jobId, "segment image picks (ratio rescue)", {
				segment: seg.index,
				desiredCount,
				renderTargetCount,
				missing,
				candidates: relaxedPool.length,
				picked: relaxedPicks.length,
				downloaded: localPaths.length,
			});
		} else if (
			STRICT_TOPIC_RELEVANT_FEED_IMAGES &&
			localPaths.length < renderTargetCount
		) {
			logJob(jobId, "segment image relaxed rescue skipped", {
				segment: seg.index,
				desiredCount,
				renderTargetCount,
				downloaded: localPaths.length,
				reason: "strict_topic_relevance",
			});
		}

		const detailCardPlan = meta.detailCard || null;
		const detailCardsUsed = detailCardCountByTopic.get(topicIndex) || 0;
		const detailCardEligibleByTime = Number(seg.startSec || 0) >= 35;
		let detailCardPath = "";
		if (
			detailCardPlan &&
			detailCardsUsed < TOPIC_DETAIL_CARD_MAX_PER_TOPIC &&
			detailCardEligibleByTime &&
			segDur >= 2.2
		) {
			try {
				detailCardPath = await createTopicDetailCardImage({
					tmpDir,
					jobId,
					topicIndex,
					title: detailCardPlan.title,
					bullets: detailCardPlan.bullets,
					output: outputCfg,
				});
				detailCardCountByTopic.set(topicIndex, detailCardsUsed + 1);
				const keepImageCount = Math.max(0, renderTargetCount - 1);
				localPaths = [detailCardPath, ...localPaths.slice(0, keepImageCount)];
				logJob(jobId, "topic detail card added", {
					segment: seg.index,
					topicLabel: effectiveTopicLabel,
					bullets: detailCardPlan.bullets.length,
				});
			} catch (e) {
				logJob(jobId, "topic detail card failed", {
					segment: seg.index,
					topicLabel: effectiveTopicLabel,
					error: e.message,
				});
			}
		}

		let pickedPlanned = 0;
		let pickedPotential = 0;
		let pickedCse = 0;
		let pickedSeed = 0;
		let pickedFallback = 0;
		for (const url of pickedUrls) {
			const key = normalizeImageUrlKey(url);
			if (plannedUrlKeys.has(key)) pickedPlanned += 1;
			else if (potentialUrlKeys.has(key)) pickedPotential += 1;
			else if (cseUrlKeys.has(key)) pickedCse += 1;
			else if (seedUrlKeys.has(key)) pickedSeed += 1;
			else pickedFallback += 1;
		}

		logJob(jobId, "segment image source stats", {
			segment: seg.index,
			query,
			topicLabel: effectiveTopicLabel,
			desiredCount,
			renderTargetCount,
			planned: plannedSegmentUrls.length,
			potential: potentialUrls.length,
			seeded: seedUrls.length,
			plannedAvailable: plannedAvailable.length,
			cseQueryCalls: segmentCseQueryCalls,
			cseTopicCalls: segmentCseTopicCalls,
			cseCacheHits: segmentCseCacheHits,
			cseUrls: fromQueryUrls.length + fromTopicUrls.length,
			pickedTotal: pickedUrls.length,
			pickedPlanned,
			pickedPotential,
			pickedCse,
			pickedSeed,
			pickedFallback,
			detailCard: Boolean(detailCardPath),
		});

		imageSourceSummary.segments += 1;
		imageSourceSummary.plannedUrls += plannedSegmentUrls.length;
		imageSourceSummary.potentialUrls += potentialUrls.length;
		imageSourceSummary.seedUrls += seedUrls.length;
		imageSourceSummary.plannedAvailable += plannedAvailable.length;
		imageSourceSummary.cseQueryCalls += segmentCseQueryCalls;
		imageSourceSummary.cseTopicCalls += segmentCseTopicCalls;
		imageSourceSummary.cseCacheHits += segmentCseCacheHits;
		imageSourceSummary.cseUrls += fromQueryUrls.length + fromTopicUrls.length;
		imageSourceSummary.pickedTotal += pickedUrls.length;
		imageSourceSummary.pickedPlanned += pickedPlanned;
		imageSourceSummary.pickedPotential += pickedPotential;
		imageSourceSummary.pickedCse += pickedCse;
		imageSourceSummary.pickedSeed += pickedSeed;
		imageSourceSummary.pickedFallback += pickedFallback;
		imageSourceSummary.feedVideoCandidates += feedVideoCandidates?.length || 0;
		imageSourceSummary.feedVideoPicked += feedVideoDownload.usedUrls?.length || 0;
		if (feedVideoDownload.localPaths?.length) {
			imageSourceSummary.feedVideoSegments += 1;
		}

		let cloudinaryUrls = [];
		if (localPaths.length) {
			cloudinaryUrls = await uploadSegmentImagesToCloudinary({
				localPaths: localPaths.slice(0, desiredCount),
				jobId,
				segIndex: seg.index,
				topicLabel: effectiveTopicLabel,
				output: outputCfg,
			});
		}

		const hasFeedVideo = Boolean(feedVideoDownload.localPaths?.length);
		if (!localPaths.length && !hasFeedVideo) {
			if (!ALLOW_PAID_IMAGE_SEGMENT_FALLBACK) {
				logJob(jobId, "segment images missing; using local visual fallback", {
					segment: seg.index,
					query,
					topicLabel: effectiveTopicLabel,
				});
				imagePlanSummary.push({
					segment: seg.index,
					imageCount: 0,
					feedVideoCount: 0,
					detailCard: false,
					cloudinaryCount: 0,
					desiredCount,
					fallback: "local_visual",
				});
				updated.push({
					...seg,
					visualType: "image",
					imageUnavailable: true,
					imageFallbackReason: "no_image_assets",
				});
				continue;
			}
			logJob(jobId, "segment images missing; paid fallback to presenter", {
				segment: seg.index,
				query,
				topicLabel: effectiveTopicLabel,
			});
			updated.push({
				...seg,
				visualType: "presenter",
				imageFallbackReason: "paid_presenter_fallback_enabled",
			});
			continue;
		}

		if (localPaths.length) segmentImagePaths.set(seg.index, localPaths);
		imagePlanSummary.push({
			segment: seg.index,
			imageCount: localPaths.length,
			feedVideoCount: feedVideoDownload.localPaths?.length || 0,
			detailCard: Boolean(detailCardPath),
			cloudinaryCount: cloudinaryUrls.length,
			desiredCount,
			renderTargetCount,
			holdSingleVisual,
			query,
			topicLabel: effectiveTopicLabel,
		});
		updated.push({
			...seg,
			imageUrls: pickedUrls,
			imageCloudinaryUrls: cloudinaryUrls,
			feedVideoUrls: feedVideoDownload.usedUrls || [],
		});
	}

	imageSourceSummary.feedVideoAttempts = feedVideoSegmentsAttempted;
	if (imageSourceSummary.segments) {
		logJob(jobId, "segment image source summary", imageSourceSummary);
	}

	return {
		timeline: updated,
		segmentImagePaths,
		segmentFeedVideoPaths,
		imagePlanSummary,
		feedVideoPlanSummary,
	};
}

async function evaluateImageSegmentDiversity({
	timeline = [],
	segmentImagePaths,
	segmentFeedVideoPaths,
	jobId,
}) {
	const imageSegments = (timeline || []).filter(
		(seg) => seg.visualType === "image" && !seg.imageUnavailable,
	);
	const segmentCount = imageSegments.length;
	if (!segmentCount) {
		return {
			ok: true,
			segmentCount: 0,
			minUnique: 0,
			unique: 0,
			ratio: 1,
			perTopic: [],
		};
	}

	const segmentKeys = new Map();
	const primaryUrls = [];
	for (const seg of imageSegments) {
		const cloud = Array.isArray(seg.imageCloudinaryUrls)
			? seg.imageCloudinaryUrls
			: [];
		const raw = Array.isArray(seg.imageUrls) ? seg.imageUrls : [];
		const video = Array.isArray(seg.feedVideoUrls) ? seg.feedVideoUrls : [];
		const pick = cloud[0] || raw[0] || video[0] || "";
		if (pick) primaryUrls.push(pick);
	}
	const uniquePrimary = new Set(primaryUrls.map((u) => normalizeImageUrlKey(u)))
		.size;

	let uniqueHash = null;
	if (segmentImagePaths instanceof Map) {
		const hashes = [];
		for (const seg of imageSegments) {
			const paths = segmentImagePaths.get(seg.index) || [];
			const p = paths[0];
			if (!p) continue;
			try {
				const h = await hashFileSha1(p);
				hashes.push(h);
				segmentKeys.set(seg.index, `hash:${h}`);
			} catch (e) {
				logJob(jobId, "segment image hash failed", {
					segment: seg.index,
					error: e.message,
				});
			}
		}
		if (hashes.length) {
			uniqueHash = new Set(hashes).size;
		}
	}
	if (segmentFeedVideoPaths instanceof Map) {
		for (const seg of imageSegments) {
			if (segmentKeys.has(seg.index)) continue;
			const paths = segmentFeedVideoPaths.get(seg.index) || [];
			const p = paths[0];
			if (!p) continue;
			try {
				const h = await hashFileSha1(p);
				segmentKeys.set(seg.index, `video:${h}`);
			} catch (e) {
				logJob(jobId, "segment feed video hash failed", {
					segment: seg.index,
					error: e.message,
				});
			}
		}
	}

	for (const seg of imageSegments) {
		if (segmentKeys.has(seg.index)) continue;
		const cloud = Array.isArray(seg.imageCloudinaryUrls)
			? seg.imageCloudinaryUrls
			: [];
		const raw = Array.isArray(seg.imageUrls) ? seg.imageUrls : [];
		const video = Array.isArray(seg.feedVideoUrls) ? seg.feedVideoUrls : [];
		const pick = cloud[0] || raw[0] || video[0] || "";
		if (pick) {
			segmentKeys.set(seg.index, `url:${normalizeImageUrlKey(pick)}`);
		}
	}

	const topics = new Map();
	for (const seg of imageSegments) {
		const topicIndex = Number.isFinite(Number(seg.topicIndex))
			? Number(seg.topicIndex)
			: 0;
		const topicLabel = String(seg.topicLabel || "").trim();
		const key = `${topicIndex}:${topicLabel || "topic"}`;
		const list = topics.get(key) || {
			topicIndex,
			topicLabel,
			segments: [],
		};
		list.segments.push(seg);
		topics.set(key, list);
	}

	const perTopic = [];
	let ok = true;
	for (const [key, data] of topics.entries()) {
		const segs = data.segments || [];
		const segCount = segs.length;
		const minUnique = Math.max(
			1,
			Math.ceil(segCount * IMAGE_SEGMENT_MIN_UNIQUE_RATIO),
		);
		const uniqSet = new Set();
		for (const seg of segs) {
			const k = segmentKeys.get(seg.index);
			if (k) uniqSet.add(k);
		}
		const unique = uniqSet.size;
		const ratio = segCount ? unique / segCount : 1;
		const entry = {
			topicKey: key,
			topicIndex: data.topicIndex,
			topicLabel: data.topicLabel,
			segmentCount: segCount,
			minUnique,
			unique,
			ratio: Number(ratio.toFixed(3)),
		};
		if (unique < minUnique) ok = false;
		perTopic.push(entry);
	}

	const totalMinUnique = Math.max(
		1,
		Math.ceil(segmentCount * IMAGE_SEGMENT_MIN_UNIQUE_RATIO),
	);
	const unique = Number.isFinite(uniqueHash) ? uniqueHash : uniquePrimary;
	const ratio = segmentCount ? unique / segmentCount : 1;
	return {
		ok,
		segmentCount,
		minUnique: totalMinUnique,
		unique,
		uniquePrimary,
		uniqueHash,
		ratio: Number(ratio.toFixed(3)),
		perTopic,
	};
}

/* ---------------------------------------------------------------
 * Presenter handling
 * ------------------------------------------------------------- */

async function ensureLocalPresenterAsset(
	assetUrl,
	tmpDir,
	jobId,
	options = {},
) {
	const {
		defaultAssetUrl = DEFAULT_PRESENTER_ASSET_URL,
		allowOverride = false,
	} = options || {};
	const requested = String(assetUrl || "").trim();
	const fallbackUrl = String(
		defaultAssetUrl || DEFAULT_PRESENTER_ASSET_URL,
	).trim();
	let url = fallbackUrl;
	if (requested && allowOverride) {
		url = requested;
	} else if (requested && requested !== fallbackUrl) {
		logJob(jobId, "presenter asset override ignored (forced default)", {
			requested,
		});
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
			url = fallbackUrl;
		}
		const p = await downloadAndValidate(url);
		if (p) return p;
		if (fallbackUrl && url !== fallbackUrl) {
			const p2 = await downloadAndValidate(fallbackUrl);
			if (p2) return p2;
		}
		throw new Error("Presenter asset could not be downloaded/validated");
	}

	if (!fs.existsSync(url)) {
		logJob(jobId, "presenter local path missing; fallback to default", { url });
		if (fallbackUrl && url !== fallbackUrl) {
			const p2 = await downloadAndValidate(fallbackUrl);
			if (p2) return p2;
		}
		throw new Error("Presenter asset not found");
	}

	const detected = detectFileType(url);
	if (!detected || detected.kind === "text") {
		logJob(jobId, "presenter local invalid; fallback to default", { url });
		if (fallbackUrl && url !== fallbackUrl) {
			const p2 = await downloadAndValidate(fallbackUrl);
			if (p2) return p2;
		}
		throw new Error("Presenter asset invalid");
	}

	return url;
}

async function ensureLocalMotionReferenceVideo(tmpDir, jobId) {
	if (!USE_MOTION_REF_BASELINE) return null;
	for (const candidate of DEFAULT_PRESENTER_MOTION_VIDEO_PATHS) {
		try {
			if (candidate && fs.existsSync(candidate)) {
				const detected = detectFileType(candidate);
				if (detected?.kind === "video") return candidate;
			}
		} catch (e) {
			logJob(jobId, "local motion reference check failed (ignored)", {
				error: e.message,
				candidate,
			});
		}
	}

	const url = DEFAULT_PRESENTER_MOTION_VIDEO_URL;
	if (!url) return null;

	const downloadAndValidate = async (u) => {
		const extGuess = path.extname(u.split("?")[0] || "").toLowerCase();
		const ext = extGuess && extGuess.length <= 5 ? extGuess : ".mp4";
		const outPath = path.join(
			tmpDir,
			`motion_ref_${crypto.randomUUID()}${ext}`,
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

function buildPresenterReferenceMotionHint({ intro = false } = {}) {
	const handLine = intro
		? "hands low on the desk or just below frame"
		: "hands low near the torso, lightly clasped or relaxed";
	return [
		`Match DemoVideo.mp4 and motion_reference.mp4: locked tripod camera with a perfectly stable frame, upright seated posture, shoulders square, ${handLine}, calm direct eye contact, natural unhurried blinks, mild brow life, soft chin dips, subtle breathing and posture settling.`,
		"Keep visible human motion alive throughout the whole clip: a blink or eye refocus every few seconds, tiny jaw readiness, natural breathing in the shoulders, and one small conversational nod or micro-shift.",
		"Keep mostly direct lens contact; any eye refocus must be tiny and natural, not a side-looking performance. Warm beats may have only a super light, mostly closed-mouth smile, never a toothy grin.",
		"No camera shake, background drift, swaying, lunging, head tilts, looped nodding, wide eyes, theatrical reactions, motionless face, or frozen statue behavior.",
	].join(" ");
}

function httpErrorText(err) {
	if (!err) return "";
	if (typeof err === "string") return err;
	const parts = [];
	if (err.message) parts.push(err.message);
	if (err.response?.status) parts.push(String(err.response.status));
	const data = err.response?.data;
	if (typeof data === "string") parts.push(data);
	else if (data && typeof data === "object") {
		try {
			parts.push(JSON.stringify(data));
		} catch {}
	}
	return parts.filter(Boolean).join(" ");
}

function isOpenAiQuotaOrRateLimitError(err) {
	const status = Number(err?.status || err?.response?.status || 0);
	if (status === 429) return true;
	const text = httpErrorText(err).toLowerCase();
	return /\b(429|quota|billing|rate\s*limit|too many requests|insufficient_quota|exceeded your current quota)\b/i.test(
		text,
	);
}

function seedFromJobId(jobId) {
	// Deterministic 32-bit seed from uuid
	const h = crypto.createHash("sha256").update(String(jobId)).digest();
	return h.readUInt32BE(0);
}

function pickIntroExpression(jobId) {
	void jobId;
	return "calm, neutral expression with settled brows and relaxed eyes";
}

function buildBaselinePrompt(
	expression = "neutral",
	motionRefVideo,
	variant = 0,
) {
	const expr = normalizeExpression(expression);
	let expressionLine =
		"Expression: calm professional neutral, settled brows, relaxed eyes, no smile.";
	if (expr === "warm")
		expressionLine =
			"Expression: friendly approachable warmth with only a super light, mostly closed-mouth smile; no toothy grin, no cheek stretching, no big smile.";
	if (expr === "excited")
		expressionLine =
			"Expression: engaged and attentive but restrained, tiny mostly closed-mouth smile allowed; no teeth-baring grin, raised-brow surprise, or wide eyes.";
	if (expr === "serious")
		expressionLine =
			"Expression: neutral and steady, soft eye contact, no frown or exaggerated concern.";
	if (expr === "thoughtful")
		expressionLine =
			"Expression: thoughtful and composed, neutral mouth, gentle eye focus, settled brows.";

	let variantHint = "";
	if (variant === 1) {
		variantHint =
			"Variant one: calm anchor take with a natural blink cadence, one soft emphasis nod or chin dip, and one small shoulder-breath/posture reset. Keep movement natural, restrained, and never looped.";
	} else if (variant === 2) {
		variantHint =
			"Variant two: calm listening take with different blink timing, subtle eye refocus, tiny breathing in the shoulders, and one almost invisible posture settle. Keep direct lens contact; no side-looking performance.";
	} else if (variant === 3) {
		variantHint =
			"Variant three: calm explanatory take with a different natural rhythm, one tiny posture reset, a restrained micro nod, realistic breathing, and relaxed eye contact. No dramatic gestures.";
	} else if (variant === 4) {
		variantHint =
			"Variant four: calm rescue take focused on usable realism: continuous small blinks, slight shoulder breathing, one tiny chin dip, and stable direct eye contact. Prioritize identity lock over movement.";
	} else if (variant === 5) {
		variantHint =
			"Variant five: calm fallback take with very subtle human liveliness, different blink spacing, soft jaw readiness, and tiny torso settling. No pose freeze and no personality change.";
	}
	const variantLine = variantHint
		? `Motion variation: ${variantHint}`
		: "Motion variation: natural unique blink timing and tiny posture settling; avoid matching any prior clip exactly.";
	const motionHint = motionRefVideo
		? buildPresenterReferenceMotionHint()
		: PRESENTER_MOTION_STYLE;

	return `
Photorealistic talking-head video of the SAME man as the reference image. Treat the input image as an identity lock, not a loose inspiration. Preserve exact identity: shaved head, glasses shape and position, eye spacing, nose, beard line, mouth, jaw, skin texture, age, face width, and face proportions. Do not recast him, beautify him, slim or widen the face, change the glasses, change the beard, change age, change skin texture, or create a different-looking presenter.
All variants must look like the same presenter recorded in the same session. Variety is only blink timing, breathing, and tiny posture rhythm; never identity, wardrobe, lighting, camera, expression, or face geometry.
Keep the same studio, lighting, wardrobe, desk, and static empty background; no extra people, reflections, text, screens, candles, flames, or moving background.
Framing: medium shot, upper torso to mid torso, moderate headroom, camera at a comfortable distance.
This must be a real moving presenter video, not a still image, frozen photo, looping freeze-frame, or camera-only zoom. The face, eyes, jaw, shoulders, and breathing must show continuous subtle human motion in every second of the clip.
${expressionLine}
${variantLine}
Reality target: closest-to-real human motion possible, calm and premium, with no extravagant movement, no theatrical acting, no exaggerated gestures, and no obvious AI morphing.
Motion: ${motionHint}
Motion floor: never hold the same facial pose for more than half a second. If the presenter is listening silently, keep small blinks, eye refocus, gentle breathing, and tiny posture settling visible without becoming theatrical. If preserving the face conflicts with motion, choose smaller motion while still avoiding a frozen photo.
Mouth and jaw: lips mostly relaxed and lightly closed, with only tiny speech-ready jaw readiness; do not form syllables, lip-sync, over-open vowels, show a toothy smile, warp the mouth, stretch the cheeks, expose odd teeth, or make puppet-like motion.
Eyes/glasses: relaxed eyes with natural reflections and blink cadence; direct lens contact; glasses must stay stable and realistic; no glassy stare, wide eyes, frequent side glances, surprise, skepticism, smirks, dramatic brow lifts, drifting glasses, mismatched eyes, melted frames, or facial asymmetry.
Wardrobe/hands: clean collar/lapels/sleeves; hands low or out of frame, never covering the face.
Camera/framing: locked tripod shot; no camera shake, no frame vibration, no reframing, no breathing zoom, no drifting background edges, no rolling wobble. Do NOT try to lip-sync.
`.trim();
}

/* ---------------------------------------------------------------
 * Script generation (engaging US tone)
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
			Math.round(
				avg * SCRIPT_VOICE_WPS * SCRIPT_PACE_BIAS * hookBoost * endCut,
			),
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

const REAL_WORLD_NEWS_OVERRIDE_RE =
	/\b(iran|israel|hezbollah|hamas|gaza|ukraine|russia|china|taiwan|middle east|white house|congress|senate|parliament|supreme court|president|prime minister|governor|diplomacy|diplomatic|ceasefire|peace proposal|peace talks|sanctions|foreign minister|state department|united nations)\b/i;

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
	"live",
	"today",
	"yesterday",
	"vlog",
	"shorts",
	"reel",
	"clip",
	"stream",
	"watch",
	"highlights",
	"full",
]);
const ANCHOR_SHORT_TOKENS_KEEP = new Set([
	"us",
	"uk",
	"eu",
	"uae",
	"ai",
	"nba",
	"nfl",
	"mlb",
	"nhl",
]);
const CONTEXT_NOISE_TOKENS = new Set([
	"live",
	"today",
	"yesterday",
	"official",
	"breaking",
]);
const CONTEXT_SHORT_TOKENS_KEEP = new Set([
	"us",
	"uk",
	"eu",
	"uae",
	"ai",
	"nba",
	"nfl",
	"mlb",
	"nhl",
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
			"prime minister",
			"campaign",
			"vote",
			"policy",
			"governor",
			"mayor",
			"parliament",
			"iran",
			"israel",
			"hezbollah",
			"hamas",
			"gaza",
			"ukraine",
			"russia",
			"china",
			"taiwan",
			"middle east",
			"ceasefire",
			"peace proposal",
			"peace talks",
			"diplomacy",
			"diplomatic",
			"sanctions",
			"white house",
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
	if (REAL_WORLD_NEWS_OVERRIDE_RE.test(hay)) return false;
	const hasStrong = FICTIONAL_CONTEXT_STRONG_TOKENS.some((tok) =>
		hay.includes(tok),
	);
	const hasRealWorldOverride = REAL_WORLD_OVERRIDE_TOKENS.some((tok) =>
		hay.includes(tok),
	);
	const hasPersonCue = REAL_PERSON_CONTEXT_TOKENS.some((tok) =>
		hay.includes(tok),
	);
	const hasNamePattern =
		/\b[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?\b/.test(raw);
	if (hasStrong && (hasRealWorldOverride || hasPersonCue)) return false;
	if (hasStrong) return true;
	if (hasPersonCue) return false;
	if (hasNamePattern && hasRealWorldOverride) return false;
	const hasWeak = FICTIONAL_CONTEXT_WEAK_TOKENS.some((tok) =>
		hay.includes(tok),
	);
	if (!hasWeak) return false;
	const hasQuestionCue =
		/\b(did|does|do)\s+\w[\w\s]{0,40}\b(die|dies|died|killed|survive|survives|alive)\b/.test(
			hay,
		) ||
		/\bending explained\b/.test(hay) ||
		/\bwho\s+(dies|died|survives|survived)\b/.test(hay);
	return hasQuestionCue;
}

function looksLikeRealWorldNewsContext(text = "") {
	const hay = String(text || "");
	return REAL_WORLD_NEWS_OVERRIDE_RE.test(hay);
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
			0,
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

function normalizeContextLine(line = "") {
	let text = String(line || "")
		.replace(/\s+/g, " ")
		.trim();
	if (!text) return "";
	text = text.replace(/[|:]+/g, " ");
	text = text.replace(/\s*-\s*/g, " ");
	text = text.replace(/([a-z])[-\u2013\u2014](\s*)([a-z])/gi, "$1 $3");
	text = text.replace(/([a-z])([A-Z])/g, "$1 $2");
	text = text.replace(
		/\b(vlog|clip|shorts|reel)(today|yesterday)\b/gi,
		"$1 $2",
	);
	text = text.replace(/\b(today|yesterday)\b/gi, "");
	text = text.replace(/\s+/g, " ").trim();
	if (!text) return "";

	const tokens = text.split(/\s+/).filter(Boolean);
	const filtered = tokens.filter((tok, idx) => {
		const lower = tok.toLowerCase();
		if (CONTEXT_NOISE_TOKENS.has(lower)) return false;
		const isEdge = idx === 0 || idx === tokens.length - 1;
		if (
			isEdge &&
			tok.length <= 2 &&
			tokens.length > 3 &&
			!CONTEXT_SHORT_TOKENS_KEEP.has(lower)
		) {
			return false;
		}
		return true;
	});

	const cleaned = filtered.join(" ");
	if (filtered.length >= 2) return cleaned;
	return text;
}

function buildTopicContextStrings(topicObj, contextItems = []) {
	const list = [];
	const topicLabel = cleanTopicLabel(
		topicObj?.displayTopic || topicObj?.topic || "",
	);
	const pushLine = (value) => {
		const cleaned = normalizeContextLine(value);
		if (cleaned) list.push(cleaned);
	};
	if (topicLabel) pushLine(topicLabel);
	if (topicObj?.rawTitle) pushLine(String(topicObj.rawTitle));
	if (topicObj?.seoTitle) pushLine(String(topicObj.seoTitle));
	if (topicObj?.youtubeShortTitle) pushLine(String(topicObj.youtubeShortTitle));

	const story = topicObj?.trendStory || topicObj || {};
	const phrases = Array.isArray(story.searchPhrases) ? story.searchPhrases : [];
	const entities = Array.isArray(story.entityNames) ? story.entityNames : [];
	const articles = Array.isArray(story.articles) ? story.articles : [];
	const articleTitles = articles.map((a) => a?.title).filter(Boolean);
	const imageComment = story.imageComment || "";
	const related = normalizeRelatedQueries(
		story.relatedQueries || topicObj?.relatedQueries || null,
	);

	for (const value of [
		...phrases,
		...entities,
		...articleTitles,
		...related.rising,
		...related.top,
	]) {
		pushLine(value);
	}
	if (imageComment) pushLine(String(imageComment));

	for (const item of Array.isArray(contextItems) ? contextItems : []) {
		if (typeof item === "string") {
			pushLine(item);
			continue;
		}
		if (item?.title) pushLine(String(item.title));
		if (item?.snippet) pushLine(String(item.snippet));
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
	const baseSet = new Set(baseTokens);
	const extraTokens = tokens.filter(
		(t) =>
			!baseSet.has(t) &&
			!TOPIC_STOP_WORDS.has(t) &&
			!GENERIC_TOPIC_TOKENS.has(t),
	);
	const capWords = (candidate.match(/\b[A-Z][a-z]+\b/g) || []).length;
	const wordCount = tokens.length;
	let score =
		matchCount * 2 +
		capWords * 0.6 +
		Math.min(wordCount, 6) * 0.25 -
		Math.max(0, wordCount - 8) * 0.4;
	if (noiseHits) score -= Math.min(1.4, noiseHits * 0.7);
	if (extraTokens.length > 1)
		score -= Math.min(1.2, (extraTokens.length - 1) * 0.25);
	if (
		/^(did|does|do|is|are|was|were|will|can|could|should|would|has|have|had)\b/i.test(
			cleaned,
		)
	) {
		score -= 0.7;
	}
	const isGenericOnly = tokens.every(
		(t) => TOPIC_STOP_WORDS.has(t) || GENERIC_TOPIC_TOKENS.has(t),
	);
	if (isGenericOnly) score -= 2;
	return score;
}

function pickTopicAnchorLabel(topicLabel = "", contextStrings = []) {
	const baseLabel = normalizeTopicLabelForQuestion(topicLabel) || topicLabel;
	const baseTokens = filterSpecificTopicTokens(
		topicTokensFromTitle(baseLabel || topicLabel),
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
	topicLabel = "",
) {
	const lines = Array.isArray(contextStrings) ? contextStrings : [];
	const anchorLower = String(anchor || "").toLowerCase();
	if (anchorLower) {
		const hit = lines.find((l) =>
			String(l || "")
				.toLowerCase()
				.includes(anchorLower),
		);
		if (hit) return String(hit || "").slice(0, 160);
	}
	const baseTokens = topicTokensFromTitle(
		normalizeTopicLabelForQuestion(topicLabel) || topicLabel,
	);
	if (baseTokens.length) {
		const hit = lines.find((l) =>
			baseTokens.some((t) =>
				String(l || "")
					.toLowerCase()
					.includes(t),
			),
		);
		if (hit) return String(hit || "").slice(0, 160);
	}
	return lines.length ? String(lines[0] || "").slice(0, 160) : "";
}

function buildTopicIntentSummary(topicObj, contextItems = []) {
	const label = cleanTopicLabel(
		topicObj?.displayTopic || topicObj?.topic || "",
	);
	const contextStrings = buildTopicContextStrings(topicObj, contextItems);
	const contextText = contextStrings.join(" ");
	const domainInfo = inferTopicDomainFromText(contextText);
	const anchor = pickTopicAnchorLabel(
		label || topicObj?.topic || "",
		contextStrings,
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

const TITLE_PROMISE_RULES = Object.freeze([
	{
		key: "price",
		label: "price or cost",
		request:
			/\b(price|pricing|cost|retail|retails|how much|cheap|expensive|affordable)\b/i,
		evidence:
			/(?:US\$|\$|USD|EUR|GBP|\u00a3|\u20ac)\s*\d{2,6}|\b\d{2,6}\s*(?:dollars?|usd|euros?|eur|pounds?|gbp)\b|\b(?:price|pricing|cost)\s+(?:is|has)\s+not\s+(?:confirmed|disclosed|announced)\b/i,
		coverage:
			/(?:US\$|\$|USD|EUR|GBP|\u00a3|\u20ac)\s*\d{2,6}|\b(?:price|priced|pricing|retail|retails|costs?|dollars?|usd|euros?|eur|pounds?|gbp|not confirmed|not disclosed|has not been announced|hasn't been announced)\b/i,
	},
	{
		key: "release",
		label: "release date or launch timing",
		request:
			/\b(release date|release details|release|launch|launches|launched|drop|drops|dropped|debut|premiere|available as of|coming out|when (?:is|does|will|can))\b/i,
		evidence:
			/\b(?:available|availability|launch(?:es|ed|ing)?|release(?:s|d)?|drop(?:s|ped|ping)?|debut(?:s|ed)?|starts?|from|on|as of)\b.{0,80}\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:,\s*\d{4})?\b/i,
		coverage:
			/\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:,\s*\d{4})?\b|\b(?:release date|launch date|available|availability|drops?|not confirmed|not disclosed|has not been announced|hasn't been announced)\b/i,
	},
	{
		key: "availability",
		label: "availability or where to get it",
		request:
			/\b(availability|available|where to buy|how to buy|where to shop|stores?|shop\s+(?:now|online|for)|limited|one per|per person|drop details|release details|waitlist|queue|sell\s*out)\b/i,
		evidence:
			/\b(?:selected|select|worldwide|stores?|available(?:\s+online)?|availability|limited|one\s+(?:watch|piece|item)?\s*per\s+person|per\s+store|per\s+day|waitlist|queue|sell\s*out|shortage)\b/i,
		coverage:
			/\b(?:selected|select|worldwide|stores?|available(?:\s+online)?|availability|limited|one\s+(?:watch|piece|item)?\s*per\s+person|per\s+store|per\s+day|waitlist|queue|sell\s*out|not confirmed|not disclosed)\b/i,
	},
	{
		key: "expect",
		label: "what to expect next",
		request:
			/\b(what to expect|what happens next|what comes next|what's next|watch for|outlook|next steps?|next move|will happen)\b/i,
		evidence: /./,
		coverage:
			/\b(?:expect|watch for|next|could|likely|look for|should|will|open question|unresolved|what happens|what changes)\b/i,
	},
	{
		key: "meaning",
		label: "meaning, impact, or practical stakes",
		request:
			/\b(what it means|what this means|why it matters|impact|implication|stakes?|so what|practical)\b/i,
		evidence: /./,
		coverage:
			/\b(?:means|matters|impact|implication|stakes?|practical|for fans|for viewers|for buyers|for the team|for the case|for the brand|changes)\b/i,
	},
]);

function compactEvidenceText(text = "", maxChars = 180) {
	const cleaned = normalizeWhitespace(text)
		.replace(/\s+([,.!?])/g, "$1")
		.trim();
	if (cleaned.length <= maxChars) return cleaned;
	return `${cleaned.slice(0, Math.max(0, maxChars - 1)).trimEnd()}...`;
}

function topicContextItemsAt(topicContexts = [], index = 0) {
	if (!Array.isArray(topicContexts)) return [];
	const direct = topicContexts[index];
	if (Array.isArray(direct?.context)) return direct.context;
	return [];
}

function buildTitlePromiseRequestText(topicObj = {}, script = null) {
	const story = topicObj?.trendStory || {};
	const articles = Array.isArray(story.articles) ? story.articles : [];
	const parts = [
		script?.title,
		script?.shortTitle,
		topicObj?.displayTopic,
		topicObj?.topic,
		topicObj?.rawTitle,
		topicObj?.seoTitle,
		topicObj?.youtubeShortTitle,
		topicObj?.promptText,
		topicObj?.angle,
		...(Array.isArray(topicObj?.keywords) ? topicObj.keywords : []),
		...(Array.isArray(topicObj?.searchHints) ? topicObj.searchHints : []),
		...(Array.isArray(story.searchPhrases) ? story.searchPhrases : []),
		...(Array.isArray(story.entityNames) ? story.entityNames : []),
		...articles.map((a) => a?.title),
	];
	return normalizeWhitespace(parts.filter(Boolean).join(" "));
}

function buildTitlePromiseEvidenceText(topicObj = {}, contextItems = []) {
	const story = topicObj?.trendStory || {};
	const articles = Array.isArray(story.articles) ? story.articles : [];
	const contextParts = (Array.isArray(contextItems) ? contextItems : []).map((c) =>
		typeof c === "string"
			? c
			: `${c?.title || ""} ${c?.snippet || ""} ${c?.source || ""} ${
					c?.link || ""
				}`,
	);
	const parts = [
		topicObj?.displayTopic,
		topicObj?.topic,
		topicObj?.promptText,
		topicObj?.angle,
		...(Array.isArray(story.searchPhrases) ? story.searchPhrases : []),
		...(Array.isArray(story.entityNames) ? story.entityNames : []),
		...articles.map((a) => `${a?.title || ""} ${a?.snippet || ""}`),
		...contextParts,
	];
	return normalizeWhitespace(parts.filter(Boolean).join(" "));
}

function buildScriptCoverageTextForTopic(script = {}, topicIndex = 0) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const topicSegments = segments.filter(
		(s) => Number(s?.topicIndex || 0) === Number(topicIndex || 0),
	);
	const useSegments = topicSegments.length ? topicSegments : segments;
	return normalizeWhitespace(
		[
			script?.title,
			script?.shortTitle,
			...useSegments.map((s) => s?.text),
		]
			.filter(Boolean)
			.join(" "),
	);
}

function extractPriceValues(text = "") {
	const matches =
		String(text || "").match(
			/(?:US\$|\$|USD|EUR|GBP|\u00a3|\u20ac)\s*\d{2,6}(?:[,.]\d{2})?|\b\d{2,6}\s*(?:dollars?|usd|euros?|eur|pounds?|gbp)\b/gi,
		) || [];
	return uniqueStrings(
		matches.map((m) => normalizeWhitespace(m).replace(/\s+/g, " ")),
		{ limit: 4 },
	);
}

function extractDateValues(text = "") {
	const month =
		"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)";
	const rx = new RegExp(`\\b${month}\\s+\\d{1,2}(?:,\\s*\\d{4})?\\b`, "gi");
	return uniqueStrings(
		(String(text || "").match(rx) || []).map((m) => formatHumanTitle(m, 32)),
		{ limit: 3 },
	);
}

function extractAvailabilityPhrases(text = "") {
	const hay = String(text || "");
	const phrases = [];
	const push = (value) => {
		const cleaned = compactEvidenceText(value, 72);
		if (cleaned) phrases.push(cleaned);
	};
	const selectedStore = hay.match(/\b(?:selected|select)\s+[^.]{0,35}\bstores?\b/i);
	if (selectedStore) push(selectedStore[0]);
	const onePer = hay.match(
		/\bone\s+(?:watch|piece|item)?\s*per\s+person(?:\s*,?\s*(?:per|and per)\s+day)?(?:\s*,?\s*(?:per|and per)\s+store)?\b/i,
	);
	if (onePer) push(onePer[0]);
	const online = hay.match(/\baccessories\s+are\s+available\s+online\b/i);
	if (online) push(online[0]);
	if (!phrases.length && /\bavailable\b/i.test(hay)) push("availability is limited by the sourced details");
	return uniqueStrings(phrases, { limit: 3 });
}

function formatEvidenceList(items = []) {
	const list = uniqueStrings(items.filter(Boolean), { limit: 4 });
	if (!list.length) return "";
	if (list.length === 1) return list[0];
	if (list.length === 2) return `${list[0]} and ${list[1]}`;
	return `${list.slice(0, -1).join(", ")}, and ${list[list.length - 1]}`;
}

function buildPromiseRepairSentence({ key, topicLabel, evidenceText, hasEvidence }) {
	const label = cleanTopicLabel(topicLabel || "the story") || "the story";
	const prices = extractPriceValues(evidenceText);
	const dates = extractDateValues(evidenceText);
	const availability = extractAvailabilityPhrases(evidenceText);
	if (key === "price") {
		if (prices.length) return `Reported pricing is ${formatEvidenceList(prices)}.`;
		if (!hasEvidence)
			return `The sourced context does not confirm a price yet, so any number should be treated as speculation.`;
	}
	if (key === "release") {
		if (dates.length && availability.length)
			return `The key release detail is ${formatEvidenceList(
				dates.slice(0, 1),
			)} with ${formatEvidenceList(availability.slice(0, 2))}.`;
		if (dates.length)
			return `The key release timing in the sourced context is ${formatEvidenceList(
				dates.slice(0, 2),
			)}.`;
		if (!hasEvidence)
			return `The sourced context does not confirm a release date yet, so the timing should stay clearly labeled.`;
	}
	if (key === "availability") {
		if (availability.length)
			return `Availability is constrained around ${formatEvidenceList(
				availability,
			)}.`;
		if (!hasEvidence)
			return `The sourced context does not confirm exact availability yet, so the buying details should stay cautious.`;
	}
	if (key === "expect") {
		if (availability.length)
			return `What viewers should expect next is tight access first, then a louder reaction once buyers compare the idea with the real product.`;
		return `What to expect next is more reaction around ${label} as the practical details become clearer.`;
	}
	if (key === "meaning") {
		return `The practical meaning is the part viewers can use: what changes for the people, buyers, fans, or institutions tied to ${label}.`;
	}
	return "";
}

function analyzeTitlePromiseCoverage({
	script = null,
	topics = [],
	topicContexts = [],
} = {}) {
	if (!TITLE_PROMISE_QA_ENABLED) return { obligations: [], missing: [] };
	const obligations = [];
	const missing = [];
	const safeTopics = Array.isArray(topics) && topics.length ? topics : [];
	for (let i = 0; i < safeTopics.length; i++) {
		const topic = safeTopics[i] || {};
		const label = cleanTopicLabel(topic.displayTopic || topic.topic || "");
		const contextItems = topicContextItemsAt(topicContexts, i);
		const requestText = buildTitlePromiseRequestText(
			topic,
			safeTopics.length === 1 ? script : null,
		);
		const evidenceText = buildTitlePromiseEvidenceText(topic, contextItems);
		const scriptText = script ? buildScriptCoverageTextForTopic(script, i) : "";
		for (const rule of TITLE_PROMISE_RULES) {
			if (!rule.request.test(requestText)) continue;
			const hasEvidence = rule.evidence.test(evidenceText);
			const repairSentence = buildPromiseRepairSentence({
				key: rule.key,
				topicLabel: label,
				evidenceText,
				hasEvidence,
			});
			const obligation = {
				topicIndex: i,
				topicLabel: label,
				key: rule.key,
				label: rule.label,
				hasEvidence,
				evidenceHint: compactEvidenceText(repairSentence || evidenceText, 160),
				repairSentence,
			};
			obligations.push(obligation);
			if (script && !rule.coverage.test(scriptText)) {
				missing.push(obligation);
			}
		}
	}
	return { obligations, missing };
}

function buildTitlePromisePromptBlock({
	script = null,
	topics = [],
	topicContexts = [],
} = {}) {
	const coverage = analyzeTitlePromiseCoverage({ script, topics, topicContexts });
	if (!coverage.obligations.length) {
		return "Title-promise obligations:\n- None detected.";
	}
	const lines = coverage.obligations.slice(0, 8).map((item) => {
		const support = item.hasEvidence
			? `Use this sourced detail: ${item.evidenceHint || "(see context)"}`
			: "If the context does not confirm it, say that clearly instead of skipping it or inventing.";
		return `- Topic ${item.topicIndex + 1} (${item.topicLabel || "topic"}): the request/title promises ${item.label}. ${support}`;
	});
	return `Title-promise obligations (MUST satisfy in spoken narration, preferably in the first third of that topic):\n${lines.join(
		"\n",
	)}`;
}

function repairTitlePromiseCoverage({
	script = {},
	topics = [],
	topicContexts = [],
	wordCaps = [],
	log = null,
} = {}) {
	const coverage = analyzeTitlePromiseCoverage({ script, topics, topicContexts });
	if (!coverage.missing.length || !Array.isArray(script?.segments)) {
		return { script, repairs: [], coverage };
	}
	const segments = script.segments.map((s) => ({ ...s }));
	const usedTargets = new Set();
	const repairs = [];
	for (const item of coverage.missing) {
		const sentence = sanitizeSegmentText(item.repairSentence || "");
		if (!sentence || countWords(sentence) < 4) continue;
		const topicSegments = segments
			.map((s, idx) => ({ s, idx }))
			.filter(({ s }) => Number(s.topicIndex || 0) === Number(item.topicIndex));
		if (!topicSegments.length) continue;
		const preferredOffset =
			item.key === "expect" || item.key === "meaning"
				? Math.max(0, topicSegments.length - 2)
				: 0;
		const candidates = [
			...topicSegments.slice(preferredOffset),
			...topicSegments.slice(0, preferredOffset),
		];
		const target =
			candidates.find(({ idx }) => !usedTargets.has(idx)) || candidates[0];
		if (!target) continue;
		usedTargets.add(target.idx);
		const oldText = String(target.s.text || "").trim();
		if (normalizeQaText(oldText).includes(normalizeQaText(sentence))) continue;
		const cap = Math.max(
			Number(wordCaps[target.idx] || 0) || countWords(oldText) + 12,
			countWords(sentence) + 12,
		);
		const combined =
			item.key === "expect" || item.key === "meaning"
				? `${oldText} ${sentence}`.trim()
				: `${sentence} ${oldText}`.trim();
		target.s.text = sanitizeSegmentText(trimSegmentToCap(combined, cap + 8));
		repairs.push({
			topicIndex: item.topicIndex,
			segmentIndex: target.s.index,
			key: item.key,
			sentence,
		});
	}
	const repairedScript = { ...script, segments };
	if (log && repairs.length) log("script title-promise repaired", { repairs });
	return { script: repairedScript, repairs, coverage };
}

function directAnswerContextItems(topic = {}, topicContext = null) {
	const items = [];
	if (Array.isArray(topicContext?.context)) items.push(...topicContext.context);
	const story = topic?.trendStory || {};
	if (Array.isArray(story.articles)) {
		for (const article of story.articles) {
			items.push({
				title: article?.title || "",
				snippet: article?.snippet || article?.description || "",
				link: article?.url || article?.link || "",
				source: "article",
			});
		}
	}
	return uniqueContextItems(items, { limit: 16 });
}

function contextItemDirectAnswerText(item = {}) {
	if (typeof item === "string") return normalizeWhitespace(item);
	const title = normalizeWhitespace(item?.title || "");
	const snippet = normalizeWhitespace(item?.snippet || item?.description || "");
	const host = getUrlHost(item?.link || item?.url || "");
	return normalizeWhitespace([title, snippet, host].filter(Boolean).join(" | "));
}

function normalizeDirectAnswerCandidate(candidate = "", subject = "") {
	const clean = normalizeWhitespace(candidate)
		.replace(/^the\s+/i, "")
		.replace(
			/\s+(?:Reacts|Talks|Speaks|Opens|Explains|Reflects|Responds|Breaks)\b.*$/g,
			"",
		)
		.replace(/[,:;.!?]+$/g, "")
		.trim();
	if (!clean) return "";
	const words = clean.split(/\s+/).filter(Boolean);
	if (!words.length || words.length > 5) return "";
	if (
		/^(who|what|when|where|why|how|which|survivor|winner|finale|season)$/i.test(
			clean,
		)
	) {
		return "";
	}
	if (
		/^(who|what|when|where|why|how|which)\b/i.test(clean) ||
		/\b(won|wins|winner|winners|revealed|results?|finale|season|episode|spoiler|spoiled|source|sources?|reported|reports?|coverage|article|story|recap|exclusive)\b/i.test(
			clean,
		)
	) {
		return "";
	}
	if (
		/\b(survivor|winner|winners|season|episode|finale|recap|live|updates?|results?|spoilers?|finalists?|jury|vote|votes|cbs|news|tvline|insider|deadline|people|magazine|hollywood|reporter|usa\s+today|variety|yahoo|thewrap|seacoastonline)\b/i.test(
			clean,
		)
	) {
		return "";
	}
	const subjectTokens = new Set(topicTokensFromTitle(subject || ""));
	const candidateTokens = topicTokensFromTitle(clean);
	if (
		candidateTokens.length &&
		candidateTokens.every((token) => subjectTokens.has(token))
	) {
		return "";
	}
	if (!/[A-Z]/.test(clean[0] || "")) return "";
	return words
		.map((word) =>
			/[A-Z]/.test(word[0] || "")
				? word
				: word.charAt(0).toUpperCase() + word.slice(1),
		)
		.join(" ");
}

function expandDirectAnswerName(candidate = "", text = "") {
	const clean = normalizeWhitespace(candidate);
	if (!clean || countWords(clean) >= 2) return clean;
	const first = escapeRegExp(clean);
	const match = String(text || "").match(
		new RegExp(`\\b${first}\\s+([A-Z][A-Za-z'.-]{2,})\\b`),
	);
	if (!match) return clean;
	const next = match[1];
	if (
		/\b(wins?|won|winner|season|episode|finale|recap|news|live)\b/i.test(
			next,
		)
	) {
		return clean;
	}
	return `${clean} ${next}`.trim();
}

function directAnswerCandidatePatterns(type = "") {
	const name = "([A-Z][A-Za-z'.-]{1,}(?:\\s+[A-Z][A-Za-z'.-]{1,}){0,4})";
	if (type === "winner") {
		return [
			{ rx: new RegExp(`\\b${name}\\s+(?:wins|won)\\b`, "g"), weight: 5 },
			{
				rx: new RegExp(
					`\\b${name}\\s+(?:is|was)\\s+(?:the\\s+)?(?:winner|champion)\\b`,
					"g",
				),
				weight: 5,
			},
			{
				rx: new RegExp(
					`\\b${name}\\s+(?:was\\s+)?named\\s+(?:as\\s+)?(?:the\\s+)?(?:winner|champion)\\b`,
					"g",
				),
				weight: 5,
			},
			{
				rx: new RegExp(
					`\\b${name}\\s+(?:has\\s+been\\s+|was\\s+)?crowned\\b`,
					"g",
				),
				weight: 5,
			},
			{
				rx: new RegExp(
					`\\b(?:winner|champion)\\s+(?:is|was|revealed|named|announced)?\\s*[:\\-]?\\s*${name}\\b`,
					"gi",
				),
				weight: 4,
			},
			{
				rx: new RegExp(
					`\\b(?:winner|champ|champion)\\s+${name}\\b`,
					"gi",
				),
				weight: 6,
			},
		];
	}
	if (type === "elimination") {
		return [
			{
				rx: new RegExp(
					`\\b${name}\\s+(?:was\\s+)?(?:eliminated|voted\\s+off|sent\\s+home)\\b`,
					"g",
				),
				weight: 5,
			},
			{
				rx: new RegExp(
					`\\b(?:eliminated|voted\\s+off|sent\\s+home)\\s*[:\\-]?\\s*${name}\\b`,
					"gi",
				),
				weight: 4,
			},
		];
	}
	if (type === "identity") {
		return [
			{
				rx: new RegExp(`\\b(?:is|was)\\s+${name}\\b`, "g"),
				weight: 2,
			},
		];
	}
	return [];
}

function extractDirectAnswerFromContext(topic = {}, topicContext = null) {
	const direct = getTopicDirectAnswerQuery(topic);
	if (!direct) return null;
	if (direct.answer) {
		return {
			answer: direct.answer,
			sourceHost: direct.sourceHost || "",
			sourceUrl: direct.sourceUrl || "",
			sourceTitle: direct.sourceTitle || "",
			confidence: direct.confidence || "stored",
		};
	}
	const patterns = directAnswerCandidatePatterns(direct.type);
	if (!patterns.length) return null;
	const subject = direct.subject || topic.displayTopic || topic.topic || "";
	const items = directAnswerContextItems(topic, topicContext);
	const candidates = new Map();
	for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
		const item = items[itemIndex];
		const text = contextItemDirectAnswerText(item);
		if (!text) continue;
		for (const { rx, weight } of patterns) {
			rx.lastIndex = 0;
			let match;
			while ((match = rx.exec(text))) {
				const rawCandidate = expandDirectAnswerName(match[1] || "", text);
				const candidate = normalizeDirectAnswerCandidate(rawCandidate, subject);
				if (!candidate) continue;
				const key = candidate.toLowerCase();
				const sourceUrl =
					typeof item === "string" ? "" : String(item?.link || item?.url || "");
				const sourceHost = sourceUrl ? getUrlHost(sourceUrl) : "";
				const sourceTitle =
					typeof item === "string" ? "" : normalizeWhitespace(item?.title || "");
				const existing = candidates.get(key) || {
					answer: candidate,
					score: 0,
					sourceHost,
					sourceUrl,
					sourceTitle,
				};
				existing.score +=
					weight +
					(itemIndex === 0 ? 1.2 : 0) +
					(sourceHost ? 0.8 : 0) +
					(countWords(candidate) >= 2 ? 0.4 : 0);
				if (!existing.sourceUrl && sourceUrl) existing.sourceUrl = sourceUrl;
				if (!existing.sourceHost && sourceHost) existing.sourceHost = sourceHost;
				if (!existing.sourceTitle && sourceTitle)
					existing.sourceTitle = sourceTitle;
				candidates.set(key, existing);
			}
		}
	}
	const sorted = Array.from(candidates.values()).sort(
		(a, b) => b.score - a.score || countWords(b.answer) - countWords(a.answer),
	);
	const top = sorted[0];
	if (!top) return null;
	return {
		answer: top.answer,
		sourceHost: top.sourceHost || "",
		sourceUrl: top.sourceUrl || "",
		sourceTitle: top.sourceTitle || "",
		confidence: top.score >= 6 ? "high" : "medium",
	};
}

function enrichDirectAnswerTopicsFromContext(topics = [], topicContexts = [], jobId) {
	for (let i = 0; i < (Array.isArray(topics) ? topics.length : 0); i++) {
		const topic = topics[i];
		const direct = getTopicDirectAnswerQuery(topic);
		if (!direct) continue;
		const info = extractDirectAnswerFromContext(topic, topicContexts?.[i]);
		if (info?.answer) {
			topic.directAnswerQuery = {
				...direct,
				answer: info.answer,
				sourceHost: info.sourceHost || "",
				sourceUrl: info.sourceUrl || "",
				sourceTitle: info.sourceTitle || "",
				confidence: info.confidence || "medium",
			};
			if (topic.promptBrief) {
				topic.promptBrief = {
					...topic.promptBrief,
					directAnswerQuery: topic.directAnswerQuery,
				};
			}
		}
		if (jobId) {
			logJob(jobId, "direct-answer research", {
				topic: topic.displayTopic || topic.topic,
				type: direct.type,
				subject: direct.subject || "",
				answer: topic.directAnswerQuery?.answer || "",
				sourceHost: topic.directAnswerQuery?.sourceHost || "",
				sourceLinks: countContextSourceLinks(topicContexts?.[i]?.context || []),
			});
		}
	}
	return topics;
}

function directAnswerOpeningSentence(topic = {}, topicContext = null) {
	const direct = getTopicDirectAnswerQuery(topic);
	if (!direct) return "";
	const info = extractDirectAnswerFromContext(topic, topicContext);
	const subject = cleanTopicLabel(
		direct.subject || topic.displayTopic || topic.topic || "the question",
	);
	if (info?.answer) {
		let claim = "";
		if (direct.type === "winner") {
			claim = `${info.answer} won ${subject}.`;
		} else if (direct.type === "elimination") {
			claim = `${info.answer} was the reported elimination from ${subject}.`;
		} else if (direct.type === "identity") {
			claim = `The answer is ${info.answer}.`;
		} else {
			claim = `${info.answer} is the clearest answer in the available sources.`;
		}
		if (info.sourceHost) {
			return sanitizeSegmentText(`According to ${info.sourceHost}, ${claim}`);
		}
		return sanitizeSegmentText(claim);
	}
	if (!topicContext) return "";
	const sourceLinks = countContextSourceLinks(
		directAnswerContextItems(topic, topicContext),
	);
	if (sourceLinks > 0 && ["winner", "elimination", "identity"].includes(direct.type)) {
		const noun =
			direct.type === "winner"
				? "the winner"
				: direct.type === "elimination"
					? "who was eliminated"
					: "the exact answer";
		return sanitizeSegmentText(
			`The available source snippets do not clearly name ${noun}, so I would not call an exact answer confirmed yet.`,
		);
	}
	if (sourceLinks > 0) return "";
	return sanitizeSegmentText(
		`I could not verify the answer from the available sources yet, so the honest answer is that ${subject} is not confirmed here.`,
	);
}

function segmentAlreadyHasDirectAnswer(text = "", sentence = "") {
	const hay = normalizeQaText(text);
	const answerText = normalizeQaText(sentence);
	if (!hay || !answerText) return false;
	const sourceStripped = answerText.replace(/^according to [a-z0-9.-]+\s+/i, "");
	if (sourceStripped && hay.includes(sourceStripped)) return true;
	const answerTokens = tokenizeQaText(sourceStripped)
		.filter((token) => token.length > 2)
		.filter(
			(token) =>
				![
					"according",
					"won",
					"winner",
					"reported",
					"answer",
					"available",
					"sources",
					"confirmed",
					"honest",
					"question",
				].includes(token),
		);
	if (!answerTokens.length) return false;
	const haySet = new Set(tokenizeQaText(hay));
	const hits = answerTokens.filter((token) => haySet.has(token)).length;
	return hits >= Math.min(answerTokens.length, 2);
}

function repairDirectAnswerOpening({
	script = {},
	topics = [],
	topicContexts = [],
	wordCaps = [],
} = {}) {
	if (script?.directAnswerHandledByIntro) return { script, changed: false };
	if (!script || !Array.isArray(script.segments) || !script.segments.length) {
		return { script, changed: false };
	}
	const segments = script.segments.map((s) => ({ ...s }));
	let changed = false;
	for (let topicIndex = 0; topicIndex < topics.length; topicIndex++) {
		const topic = topics[topicIndex] || {};
		if (!isDirectAnswerTopic(topic)) continue;
		const targetIdx = segments.findIndex(
			(s) => Number(s.topicIndex || 0) === topicIndex,
		);
		if (targetIdx < 0) continue;
		const sentence = directAnswerOpeningSentence(
			topic,
			topicContexts?.[topicIndex],
		);
		if (!sentence) continue;
		const current = sanitizeSegmentText(segments[targetIdx].text || "");
		if (segmentAlreadyHasDirectAnswer(current, sentence)) continue;
		const uncertaintyAnswer = /\bdo not clearly name\b|\bnot confirmed\b/i.test(
			sentence,
		);
		const cap = Math.max(
			Number(wordCaps[segments[targetIdx].index] || wordCaps[targetIdx] || 0) ||
				0,
			uncertaintyAnswer
				? countWords(sentence) + 12
				: countWords(current) + countWords(sentence) + 4,
			countWords(sentence) + 16,
		);
		segments[targetIdx].text = sanitizeSegmentText(
			trimSegmentToCap(
				uncertaintyAnswer
					? `${sentence} The useful part is what the confirmed coverage does and does not show.`
					: `${sentence} ${current}`.trim(),
				cap + 4,
			),
		);
		segments[targetIdx].expression = normalizeExpression(
			segments[targetIdx].expression || "neutral",
			"neutral",
		);
		changed = true;
	}
	return changed ? { script: { ...script, segments }, changed } : { script, changed };
}

function introCarriesDirectAnswer({ introText = "", topics = [] } = {}) {
	const intro = sanitizeIntroOutroLine(introText);
	if (!intro) return false;
	for (const topic of Array.isArray(topics) ? topics : []) {
		if (!isDirectAnswerTopic(topic)) continue;
		const sentence = directAnswerOpeningSentence(topic);
		if (sentence && segmentAlreadyHasDirectAnswer(intro, sentence)) return true;
	}
	return false;
}

function buildDirectAnswerPromptBlock(topics = [], topicContexts = []) {
	const directTopics = (Array.isArray(topics) ? topics : [])
		.map((topic, idx) => ({ topic, idx, direct: getTopicDirectAnswerQuery(topic) }))
		.filter((item) => item.direct);
	if (!directTopics.length) return "Direct-answer prompt mode:\n- None.";
	const lines = directTopics.map(({ topic, idx, direct }) => {
		const contextItems = directAnswerContextItems(topic, topicContexts?.[idx]);
		const sources = uniqueStrings(
			contextItems
				.map((item) =>
					typeof item === "string" ? "" : getUrlHost(item?.link || item?.url || ""),
				)
				.filter(Boolean),
			{ limit: 4 },
		);
		const knownAnswer = direct.answer
			? ` Confirmed answer candidate from source context: ${direct.answer}${
					direct.sourceHost ? ` (${direct.sourceHost})` : ""
				}.`
			: " No exact answer candidate was extracted from the source snippets yet; do not invent one.";
		return `- Topic ${idx + 1} (${topic.displayTopic || topic.topic}): user asked a direct ${direct.answerLabel || "factual"} question: "${direct.question || topic.topic}". Answer it in the first spoken content sentence for this topic using source context.${knownAnswer} Do not tease, stall, or delay the answer for retention. For "who won" questions, include the winner's exact name; do not use vague substitutes like "the contestant" or "the player". If the sources here do not clearly confirm the answer, say that plainly instead of guessing. After the answer, make the video worth watching with context, why it matters, audience reaction, and what remains interesting. Sources seen: ${sources.length ? sources.join(", ") : "(none)"}.`;
	});
	return `Direct-answer prompt mode (MUST follow):\n${lines.join("\n")}`;
}

function primaryPromptBrief(topics = []) {
	const list = Array.isArray(topics) ? topics : [];
	return (
		list.map((t) => t?.promptBrief).find((brief) => brief && brief.isStructured) ||
		list.map((t) => t?.promptBrief).find(Boolean) ||
		null
	);
}

function shouldLockPromptBriefTitle(brief = null) {
	return Boolean(brief?.title && brief?.titleLocked);
}

function collectPromptSeoTitleInstructions(topics = []) {
	const lines = [];
	for (const topic of Array.isArray(topics) ? topics : []) {
		const brief = topic?.promptBrief || parseStructuredPromptBrief(topic?.promptText);
		if (Array.isArray(brief?.seoTitleInstructions)) {
			lines.push(...brief.seoTitleInstructions);
		}
	}
	return uniqueStrings(lines, { limit: 5 });
}

function collectPromptMustIncludeLines(topics = []) {
	const lines = [];
	for (const topic of Array.isArray(topics) ? topics : []) {
		const brief = topic?.promptBrief || parseStructuredPromptBrief(topic?.promptText);
		if (Array.isArray(brief?.mustIncludeLines)) {
			lines.push(...brief.mustIncludeLines);
		}
	}
	return uniqueStrings(lines, { limit: 6 });
}

function primaryPromptThumbnailText(topics = []) {
	for (const topic of Array.isArray(topics) ? topics : []) {
		const brief = topic?.promptBrief || parseStructuredPromptBrief(topic?.promptText);
		const thumbnailText = normalizePromptThumbnailText(brief?.thumbnailText || "");
		if (thumbnailText) return thumbnailText;
	}
	return "";
}

function secondaryPromptThumbnailBadge(topics = []) {
	const text = normalizeWhitespace(
		(Array.isArray(topics) ? topics : [])
			.map((topic) => {
				const brief = topic?.promptBrief || parseStructuredPromptBrief(topic?.promptText);
				return [
					...(Array.isArray(brief?.mustIncludeLines)
						? brief.mustIncludeLines
						: []),
					...(Array.isArray(brief?.briefLines) ? brief.briefLines : []),
					topic?.promptText || "",
				].join(" ");
			})
			.join(" "),
	);
	if (/\bmental noise\b/i.test(text)) return "MENTAL NOISE";
	if (/\bfake rest\b/i.test(text)) return "FAKE REST";
	if (/\boverloaded\b/i.test(text)) return "OVERLOADED";
	if (/\bburnout\b/i.test(text)) return "BURNOUT";
	return "";
}

function primaryPromptOutroText(topics = []) {
	for (const topic of Array.isArray(topics) ? topics : []) {
		const brief = topic?.promptBrief || parseStructuredPromptBrief(topic?.promptText);
		const outroLine = normalizePromptOutroLine(brief?.outroLine || "");
		if (outroLine) return outroLine;
	}
	return "";
}

function primaryPromptOpeningText(topics = []) {
	for (const topic of Array.isArray(topics) ? topics : []) {
		const brief = topic?.promptBrief || parseStructuredPromptBrief(topic?.promptText);
		const openingLine = sanitizeIntroOutroLine(
			stripOuterQuotes(brief?.openingLine || ""),
		);
		if (openingLine) return openingLine;
	}
	return "";
}

function isPsychologyFailureTopicText(text = "") {
	const hay = String(text || "");
	const hasFailureCore =
		/\b(fail(?:ed|ing|s|ure)?|mistakes?|business failures?|celebrity mistakes?|schadenfreude)\b/i.test(
			hay,
		) || /\bwatch(?:ing)?\s+people\s+(?:fail|fall)\b/i.test(hay);
	if (hasFailureCore) return true;
	const hasPsychologyFrame = /\b(psycholog|internet culture|drama|relief)\b/i.test(
		hay,
	);
	const hasJudgmentCluster =
		/\b(judg(?:e|ement|ment|ing)?|cruel(?:ty)?|mock(?:ing)?|humiliat(?:e|ing|ion)|suffering|humility)\b/i.test(
			hay,
		);
	return hasPsychologyFrame && hasJudgmentCluster;
}

function resolveOpeningPresenterExpression({
	topics = [],
	categoryLabel = "",
	title = "",
	mood = "neutral",
} = {}) {
	const hay = [
		categoryLabel || "",
		title || "",
		...(Array.isArray(topics)
			? topics.map((topic) =>
					[
						topic?.displayTopic,
						topic?.topic,
						topic?.promptText,
						topic?.promptBrief?.tone,
						...(Array.isArray(topic?.promptBrief?.briefLines)
							? topic.promptBrief.briefLines
							: []),
					]
						.filter(Boolean)
						.join(" "),
				)
			: []),
	].join(" ");
	if (
		isSensitiveTopicText(hay) ||
		/\b(politics|health|public\s+safety|tragedy|war|court|legal)\b/i.test(
			categoryLabel || "",
		)
	) {
		return "neutral";
	}
	if (isPsychologyFailureTopicText(hay)) return "thoughtful";
	if (
		isPersonalFinanceCostOfLivingTopic({ topics, categoryLabel, text: hay }) ||
		isSocialConnectionTopic({ topics, categoryLabel, text: hay }) ||
		isDigitalWellbeingTopic({ topics, categoryLabel, text: hay }) ||
		/\b(happy|happier|happiness|hopeful|gentle|free|gratitude|kind|calm|better day)\b/i.test(
			hay,
		)
	) {
		return "warm";
	}
	if (mood === "excited") return "warm";
	if (mood === "serious") return "neutral";
	return "neutral";
}

function buildOpeningContinuationFallback({
	topics = [],
	categoryLabel = "",
	title = "",
	mood = "neutral",
} = {}) {
	const hay = [
		title || "",
		categoryLabel || "",
		...(Array.isArray(topics)
			? topics.map((topic) =>
					[
						topic?.displayTopic,
						topic?.topic,
						topic?.promptText,
					]
						.filter(Boolean)
						.join(" "),
				)
			: []),
	].join(" ");
	if (isPsychologyFailureTopicText(hay)) {
		return "The uncomfortable question is whether watching failure makes us wiser, or just makes judgment feel harmless.";
	}
	if (isImpulseShoppingTopic({ topics, categoryLabel, text: hay })) {
		return "The interesting part is how a bored scroll turns into a purchase before you ever decide to go shopping.";
	}
	if (
		/\b(happy|happier|happiness|money|spending|gratitude|free)\b/i.test(hay)
	) {
		return "The useful part is not pretending money does not matter. It is noticing which ordinary moments still give something back.";
	}
	if (isSocialConnectionTopic({ topics, categoryLabel, text: hay })) {
		return "The useful part is that connection usually changes through small repeatable moments, not one perfect social reset.";
	}
	if (
		isSensitiveTopicText(hay) &&
		/\b(nascar|cup\s+series|motorsports?|racing|driver|kyle\s+busch)\b/i.test(hay)
	) {
		return "Start with the part that matters most: what is confirmed, what is not, and why his legacy is bigger than one headline.";
	}
	if (mood === "serious" || isSensitiveTopicText(hay)) {
		return "The important part is to separate what is confirmed from what the pattern might mean next.";
	}
	return "The interesting part is what the obvious answer hides, and why that changes how the whole topic feels.";
}

function removeIntroLineFromOpeningSegment({
	script = {},
	introText = "",
	topics = [],
	wordCaps = [],
	categoryLabel = "",
	mood = "neutral",
} = {}) {
	if (!script || !Array.isArray(script.segments) || !script.segments.length) {
		return { script, changed: false };
	}
	const introKey = normalizeOpeningForCompare(introText);
	if (!introKey) return { script, changed: false };
	const segments = script.segments.map((s) => ({ ...s }));
	const targetIdx = segments.findIndex((s) => Number(s.topicIndex || 0) === 0);
	const idx = targetIdx >= 0 ? targetIdx : 0;
	const current = sanitizeSegmentText(segments[idx]?.text || "");
	const sentences = splitSentences(current).filter(Boolean);
	if (!sentences.length) return { script, changed: false };
	const firstKey = normalizeOpeningForCompare(sentences[0]);
	const overlap =
		firstKey && introKey
			? overlapRatio(tokenizeQaText(firstKey), tokenizeQaText(introKey))
			: 0;
	const duplicatesIntro =
		firstKey === introKey ||
		firstKey.startsWith(introKey) ||
		introKey.startsWith(firstKey) ||
		overlap >= 0.82;
	if (!duplicatesIntro) return { script, changed: false };

	let nextText = sanitizeSegmentText(sentences.slice(1).join(" "));
	const nextKey = normalizeOpeningForCompare(nextText);
	const nextIntroOverlap =
		nextKey && introKey
			? overlapRatio(tokenizeQaText(nextKey), tokenizeQaText(introKey))
			: 0;
	if (countWords(nextText) < QA_MIN_SEGMENT_WORDS || nextIntroOverlap >= 0.58) {
		nextText = buildOpeningContinuationFallback({
			topics,
			categoryLabel,
			title: script.title || script.shortTitle || "",
			mood,
		});
	}
	const cap =
		Number(wordCaps?.[segments[idx].index] || wordCaps?.[idx] || 0) ||
		Math.max(countWords(nextText) + 4, QA_MIN_SEGMENT_WORDS + 8);
	segments[idx].text = sanitizeSegmentText(
		trimSegmentToCap(nextText, Math.max(cap + 4, QA_MIN_SEGMENT_WORDS + 8)),
	);
	return {
		script: { ...script, segments },
		changed: true,
		removed: sentences[0],
		replacement: segments[idx].text,
	};
}

function buildPromptBriefInstructionBlock(topics = []) {
	const promptTopics = (Array.isArray(topics) ? topics : []).filter((t) =>
		isUserPromptTopicPick(t),
	);
	if (!promptTopics.length) return "Frontend prompt brief:\n- None.";
	const lines = [];
	for (let i = 0; i < promptTopics.length; i++) {
		const topic = promptTopics[i] || {};
		const brief = topic.promptBrief || parseStructuredPromptBrief(topic.promptText);
		const label = topic.displayTopic || topic.topic || `Topic ${i + 1}`;
		const direct = getTopicDirectAnswerQuery(topic) || brief?.directAnswerQuery;
		lines.push(`- Topic ${i + 1}: ${label}`);
		if (direct?.isDirectAnswer) {
			lines.push(
				`  Direct factual question: answer-first mode (${direct.type || "fact"}). Question: "${direct.question || label}".`,
			);
			if (direct.answer) {
				lines.push(
					`  Sourced answer candidate: "${direct.answer}"${
						direct.sourceHost ? ` from ${direct.sourceHost}` : ""
					}.`,
				);
			}
		}
		if (shouldLockPromptBriefTitle(brief))
			lines.push(`  Requested title: ${brief.title}`);
		if (brief?.wantsSeoTitle)
			lines.push(
				"  Title instruction: generate the strongest SEO-friendly YouTube title supported by the script; do not use the instruction sentence itself as the title.",
			);
		if (brief?.thumbnailText)
			lines.push(`  Thumbnail badge/text must include: ${brief.thumbnailText}`);
		if (brief?.openingLine)
			lines.push(`  Requested opening line for the video opening: "${brief.openingLine}"`);
		if (Array.isArray(brief?.mustIncludeLines) && brief.mustIncludeLines.length) {
			for (const mustLine of brief.mustIncludeLines.slice(0, 4)) {
				lines.push(`  Must include this line or a very close spoken version: "${mustLine}"`);
			}
		}
		if (brief?.outroLine)
			lines.push(`  Requested outro/CTA: "${brief.outroLine}"`);
		if (Array.isArray(brief?.structureLines) && brief.structureLines.length) {
			lines.push(
				"  Requested structure: preserve this order; if the requested video duration is longer or shorter, scale the depth proportionally instead of changing the topic.",
			);
			for (const line of brief.structureLines.slice(0, 8)) {
				lines.push(`  - ${line}`);
			}
		}
		for (const line of (brief?.briefLines || []).slice(0, 8)) {
			lines.push(`  ${line}`);
		}
		if (brief?.raw) {
			lines.push(
				`  Full frontend prompt summary: ${compactEvidenceText(
					brief.raw,
					700,
				)}`,
			);
		}
	}
	return `Frontend prompt brief (MUST follow; keep the same topic and expand within it for the requested duration):\n${lines.join(
		"\n",
	)}`;
}

function normalizeOpeningForCompare(text = "") {
	return normalizeQaText(text)
		.replace(/\b(the|a|an)\b/g, "")
		.replace(/\s+/g, " ")
		.trim();
}

function combineOpeningLineWithSegment({
	openingLine = "",
	text = "",
	cap = 0,
} = {}) {
	let opening = sanitizeSegmentText(stripOuterQuotes(openingLine));
	const current = sanitizeSegmentText(text);
	if (!opening) return current;
	if (!endsWithTerminalPunctuation(opening)) opening = `${opening}.`;
	const openingKey = normalizeOpeningForCompare(opening);
	const currentKey = normalizeOpeningForCompare(current);
	if (openingKey && currentKey.startsWith(openingKey)) return current;
	const sentences = splitSentences(current).filter(Boolean);
	let rest = sentences
		.filter((sentence) => {
			const key = normalizeOpeningForCompare(sentence);
			return !openingKey || !key || overlapRatio(tokenizeQaText(openingKey), tokenizeQaText(key)) < 0.75;
		})
		.join(" ")
		.trim();
	const maxWords = Math.max(
		countWords(opening) + 8,
		Number(cap || 0) || countWords(opening) + countWords(rest),
	);
	if (rest && countWords(opening) + countWords(rest) > maxWords) {
		rest = trimSegmentToCap(rest, Math.max(8, maxWords - countWords(opening)));
	}
	return sanitizeSegmentText(`${opening}${rest ? ` ${rest}` : ""}`.trim());
}

function promptMustIncludeTargetIndex(segments = [], line = "") {
	const includeTokens = tokenizeQaText(line)
		.filter((t) => t.length > 2)
		.filter((t) => !["you", "your", "the", "and", "from", "that", "this"].includes(t));
	const financeNeedle = /\b(subscription|automatic|withdrawal|autopay|charge|bill|broke|spending|paycheck|budget|leak)\b/i.test(
		line,
	);
	let bestIdx = -1;
	let bestScore = -1;
	for (let i = 0; i < segments.length; i++) {
		const text = String(segments[i]?.text || "");
		const hayTokens = new Set(tokenizeQaText(text));
		let score = includeTokens.reduce(
			(sum, token) => sum + (hayTokens.has(token) ? 2 : 0),
			0,
		);
		if (
			financeNeedle &&
			/\b(subscription|automatic|autopay|recurring|charge|bill|spending|paycheck|budget|leak)\b/i.test(
				text,
			)
		) {
			score += 8;
		}
		if (i === 0) score -= 2;
		if (score > bestScore) {
			bestScore = score;
			bestIdx = i;
		}
	}
	if (bestIdx >= 0 && bestScore > 0) return bestIdx;
	return Math.max(0, Math.min(segments.length - 1, Math.floor(segments.length * 0.38)));
}

function insertPromptMustIncludeLine({
	text = "",
	mustLine = "",
	cap = 0,
} = {}) {
	const include = sanitizeSegmentText(stripOuterQuotes(mustLine));
	let current = stripBlockingScriptArtifacts(sanitizeSegmentText(text));
	if (!include) return current;
	if (normalizeQaText(current).includes(normalizeQaText(include))) return current;

	const sentences = splitSentences(current).filter(Boolean);
	const includeWords = countWords(include);
	const maxWords = Math.max(
		Number(cap || 0) || 0,
		countWords(current) + includeWords,
		includeWords + 10,
	);
	const shouldReplaceWeakTail =
		sentences.length > 1 &&
		(countWords(current) + includeWords > maxWords + 8 ||
			/\b(that'?s the takeaway|viewer takeaway|concrete next step|story moving|story is bigger)\b/i.test(
				sentences[sentences.length - 1] || "",
			));

	if (shouldReplaceWeakTail) {
		current = sentences.slice(0, -1).join(" ").trim();
	}
	let combined = sanitizeSegmentText(`${current} ${include}`.trim());
	if (countWords(combined) <= maxWords + 8) return combined;
	const roomForCurrent = Math.max(8, maxWords - includeWords);
	const trimmedCurrent = trimSegmentToCap(current, roomForCurrent);
	combined = sanitizeSegmentText(`${trimmedCurrent} ${include}`.trim());
	return combined;
}

function promptTopicHaystack(topics = [], extra = "") {
	const topicText = (Array.isArray(topics) ? topics : [])
		.map((topic) =>
			[
				topic?.topic,
				topic?.displayTopic,
				topic?.promptText,
				topic?.category,
				...(Array.isArray(topic?.keywords) ? topic.keywords : []),
			]
				.filter(Boolean)
				.join(" "),
		)
		.join(" ");
	return normalizeWhitespace(`${topicText} ${extra || ""}`).toLowerCase();
}

function isImpulseShoppingTopic({
	topics = [],
	categoryLabel = "",
	text = "",
} = {}) {
	const hay = promptTopicHaystack(topics, `${categoryLabel || ""} ${text || ""}`);
	const hasShoppingContext =
		/\b(tiktok\s+shop|social shopping|shopping app|online shopping|online shoppers?|shop\b|shopping\b|buy(?:ing)?|purchase|checkout|cart\b|creator ads?|reviews?|discounts?|coupons?|deals?|unboxing|packages?|deliveries?)\b/i.test(
			hay,
		);
	const hasImpulsePressure =
		/\b(impulse|addictive|addiction|bored|boredom|urge|scrolling|endless scroll|save money|spending|dont need|don't need|never needed|one purchase|buy trap)\b/i.test(
			hay,
		);
	return hasShoppingContext && hasImpulsePressure;
}

function isPersonalFinanceCostOfLivingTopic({
	topics = [],
	categoryLabel = "",
	text = "",
} = {}) {
	const hay = promptTopicHaystack(topics, `${categoryLabel || ""} ${text || ""}`);
	if (
		/\b(broke|paycheck|paycheque|rent|renter|renters|grocer(?:y|ies)|bills?|subscriptions?|automatic withdrawals?|inflation|cost of living|living paycheck|financial stress|money stress|budget|savings?|debt|fixed costs?|feel behind)\b/i.test(
			hay,
		)
	) {
		return true;
	}
	return (
		/\bfinance|personal finance|education\b/i.test(categoryLabel || "") &&
		/\b(job|work|income|pay|cost|money|budget|rent|bill)\b/i.test(hay)
	);
}

function isSocialConnectionTopic({
	topics = [],
	categoryLabel = "",
	text = "",
} = {}) {
	const hay = promptTopicHaystack(topics, `${categoryLabel || ""} ${text || ""}`);
	return (
		/\b(socialissues|people\s*&?\s*blogs?|relationships?)\b/i.test(
			categoryLabel || "",
		) ||
		/\b(making friends|make friends|adult friendship|friendships?|friendship feels|friends feels|making new friends|meeting people|social life|social connection|social isolation|loneliness|lonely|belonging|community|close friends|group chat|reach out|text first|fear of rejection|rejection)\b/i.test(
			hay,
		) ||
		/\b(friendships?|friends?)\b[^.?!\n]{0,80}\b(hard|difficult|awkward|lonely|alone|rejection|rejected|ignored|disconnected|isolated|drifting|harder)\b/i.test(
			hay,
		) ||
		/\b(hard|difficult|awkward|lonely|alone|rejection|rejected|ignored|disconnected|isolated|drifting|harder)\b[^.?!\n]{0,80}\b(friendships?|friends?)\b/i.test(
			hay,
		)
	);
}

function isDigitalWellbeingTopic({
	topics = [],
	categoryLabel = "",
	text = "",
} = {}) {
	const hay = promptTopicHaystack(topics, `${categoryLabel || ""} ${text || ""}`);
	const hasDeviceOrFeed =
		/\b(phone|smartphone|screen\s*time|scrolling|doomscrolling|notification|notifications|lock\s*screen|digital\s+detox|social\s+media|apps?|algorithm|feed|device|devices)\b/i.test(
			hay,
		);
	const hasWellbeingPressure =
		/\b(peace|calm|rest|mental\s+health|anxiety|stress|stressed|overwhelm|overstimulated|burnout|focus|attention|sleep|bedtime|dopamine|addict|addiction|habit|boundaries|distraction|mind|wellbeing|well-being|wellness|silence|quiet)\b/i.test(
			hay,
		);
	return hasDeviceOrFeed && hasWellbeingPressure;
}

function isWeakPromptOpeningSegment(text = "", topicLabel = "") {
	const clean = normalizeWhitespace(text);
	if (!clean) return true;
	const topicKey = normalizeQaText(topicLabel);
	const qa = normalizeQaText(clean);
	if (countWords(clean) < 16) return true;
	if (topicKey && qa.startsWith(topicKey.slice(0, Math.min(topicKey.length, 40))))
		return true;
	return /\b(starts with one contradiction|quick breakdown|what happened|key reporting|this story is bigger|that gives the story|that'?s the takeaway)\b/i.test(
		clean,
	);
}

function strengthenPromptOpeningRetention({
	script = {},
	topics = [],
	wordCaps = [],
	categoryLabel = "",
} = {}) {
	if (!script || !Array.isArray(script.segments)) return script;
	const brief = primaryPromptBrief(topics);
	if (!brief || !isPersonalFinanceCostOfLivingTopic({ topics, categoryLabel }))
		return script;
	const segments = script.segments.map((s) => ({ ...s }));
	const firstIdx = segments.findIndex((s) => Number(s.topicIndex || 0) === 0);
	const startIdx = firstIdx >= 0 ? firstIdx : 0;
	if (segments[startIdx]) {
		segments[startIdx].expression = "warm";
	}
	const secondIdx = startIdx + 1;
	const second = segments[secondIdx];
	if (!second) return { ...script, segments };
	const topic =
		topics?.[Number(second.topicIndex || 0)] ||
		topics?.[0] ||
		{};
	const topicLabel = String(
		second.topicLabel || topic?.displayTopic || topic?.topic || "",
	).trim();
	const protectedLines = collectPromptMustIncludeLines(topics)
		.map((line) => normalizeQaText(line))
		.filter(Boolean);
	const secondQa = normalizeQaText(second.text || "");
	if (protectedLines.some((line) => line && secondQa.includes(line))) {
		second.expression = second.expression || "thoughtful";
		return { ...script, segments };
	}
	if (!isWeakPromptOpeningSegment(second.text || "", topicLabel)) {
		second.expression = second.expression || "thoughtful";
		return { ...script, segments };
	}
	const replacement =
		"Here is the tension: a paycheck can be real and still disappear after rent, groceries, bills, and automatic payments clear. That is why this feels personal, not just financial.";
	const cap = Math.max(
		Number(wordCaps[second.index] || wordCaps[secondIdx] || 0) || 0,
		countWords(replacement) + 4,
	);
	second.text = sanitizeSegmentText(trimSegmentToCap(replacement, cap + 4));
	second.expression = "thoughtful";
	return { ...script, segments };
}

function applyPromptBriefToScript({
	script = {},
	topics = [],
	wordCaps = [],
	injectOpeningLine = true,
} = {}) {
	if (!script || !Array.isArray(script.segments)) return script;
	const brief = primaryPromptBrief(topics);
	if (!brief) return script;
	const next = {
		...script,
		segments: script.segments.map((s) => ({ ...s })),
	};
	if (shouldLockPromptBriefTitle(brief)) {
		next.title = formatHumanTitle(brief.title, 120) || next.title;
		next.shortTitle = shortTitleFromText(brief.title).slice(0, 60);
	}
	if (injectOpeningLine && brief.openingLine && next.segments.length) {
		const firstIdx = next.segments.findIndex((s) => Number(s.topicIndex || 0) === 0);
		const idx = firstIdx >= 0 ? firstIdx : 0;
		const cap = Math.max(
			Number(wordCaps[next.segments[idx].index] || wordCaps[idx] || 0) || 0,
			countWords(brief.openingLine) + 16,
		);
		next.segments[idx].text = combineOpeningLineWithSegment({
			openingLine: brief.openingLine,
			text: next.segments[idx].text,
			cap,
		});
	}
	const mustIncludeLines = collectPromptMustIncludeLines(topics);
	for (const mustLine of mustIncludeLines) {
		const key = normalizeQaText(mustLine);
		if (!key) continue;
		const fullText = normalizeQaText(
			next.segments.map((s) => s.text || "").join(" "),
		);
		if (fullText.includes(key)) continue;
		const idx = promptMustIncludeTargetIndex(next.segments, mustLine);
		if (idx < 0 || !next.segments[idx]) continue;
		const cap = Math.max(
			Number(wordCaps[next.segments[idx].index] || wordCaps[idx] || 0) || 0,
			countWords(next.segments[idx].text || "") + countWords(mustLine),
			countWords(mustLine) + 10,
		);
		next.segments[idx].text = insertPromptMustIncludeLine({
			text: next.segments[idx].text,
			mustLine,
			cap,
		});
	}
	return strengthenPromptOpeningRetention({ script: next, topics, wordCaps });
}

function inferTonePlan({ topic, topics, angle, liveContext }) {
	const topicLine =
		Array.isArray(topics) && topics.length
			? topics.map((t) => t.topic || "").join(" ")
			: topic || "";
	const contextLines = Array.isArray(liveContext)
		? liveContext.map((c) =>
				typeof c === "string" ? c : `${c.title || ""} ${c.snippet || ""}`,
			)
		: [];
	const hay = [topicLine, angle || "", ...contextLines].join(" ").toLowerCase();

	let seriousScore = 0;
	let excitedScore = 0;

	for (const tok of SERIOUS_TONE_TOKENS) {
		if (hay.includes(tok)) seriousScore += 2;
	}
	for (const tok of POLITICAL_TONE_TOKENS) {
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

const POLITICAL_TONE_TOKENS = [
	"politic",
	"election",
	"vote",
	"voting",
	"campaign",
	"government",
	"policy",
	"congress",
	"senate",
	"parliament",
	"president",
	"prime minister",
	"governor",
	"mayor",
	"legislation",
	"lawmakers",
	"bill",
	"referendum",
	"ballot",
	"protest",
	"protests",
	"war",
	"conflict",
	"invasion",
	"ceasefire",
	"sanction",
	"treaty",
	"security",
	"terror",
	"attack",
];

const ENTERTAINMENT_REACTION_CUES = [
	"honestly",
	"frankly",
	"to me",
	"i think",
	"i feel",
	"it feels",
	"that feels",
	"i like",
	"i love",
	"i'm into",
	"i am into",
	"i'm curious",
	"i am curious",
	"i'm surprised",
	"i am surprised",
	"i'll be honest",
];

function isEntertainmentTopicText(text = "") {
	const hay = String(text || "").toLowerCase();
	if (!hay) return false;
	return ENTERTAINMENT_KEYWORDS.some((k) => hay.includes(k));
}

function hasEntertainmentReactionCue(text = "") {
	const hay = String(text || "").toLowerCase();
	return ENTERTAINMENT_REACTION_CUES.some((tok) => hay.includes(tok));
}

function isSensitiveTopicText(text = "") {
	const t = String(text || "").toLowerCase();
	if (!t) return false;
	const has = (list) => list.some((tok) => t.includes(tok));
	if (
		/\b(death|dead|died|dies|passed\s+away|killed|fatal|cause\s+of\s+death|hospitali[sz]ed|severe\s+illness|mourning|tribute)\b/i.test(
			t,
		)
	)
		return true;
	return (
		has(SERIOUS_TONE_TOKENS) ||
		has(EXPLICIT_SERIOUS_CUES) ||
		has(POLITICAL_TONE_TOKENS)
	);
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

function coerceExpressionForNaturalness(
	rawExpression,
	text,
	mood = "neutral",
	topicLabel = "",
) {
	if (isSensitiveTopicText(`${text} ${topicLabel}`)) return "neutral";
	const base = normalizeExpression(rawExpression, mood);
	const explicit = inferExplicitExpression(text);
	if (explicit) {
		if (explicit === "serious") return "neutral";
		if (explicit === "excited") return "warm";
		return explicit;
	}
	const entertainment = isEntertainmentTopicText(`${text} ${topicLabel}`);
	if (entertainment && hasEntertainmentReactionCue(text)) return "warm";
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
	edgeBuffer = SUBTLE_VISUAL_EDGE_BUFFER,
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
	jobId,
) {
	if (!segments.length) return [];
	const total = segments.length;
	const plan = Array.from({ length: total }, () => "neutral");
	if (mood === "serious") return plan;

	const seed = jobId ? seedFromJobId(jobId) : 0;
	let indices = pickSubtleExpressionIndices(total, seed);
	const edgeBuffer = Math.max(
		0,
		Math.floor(Number(SUBTLE_VISUAL_EDGE_BUFFER) || 0),
	);
	const maxCount = Math.max(
		0,
		Math.floor(Number(MAX_SUBTLE_VISUAL_EXPRESSIONS) || 0),
	);
	const entertainmentIndices = [];
	for (let i = 0; i < segments.length; i++) {
		const seg = segments[i] || {};
		const hay = `${seg.text || ""} ${seg.topicLabel || ""}`;
		if (isSensitiveTopicText(hay)) continue;
		if (isEntertainmentTopicText(hay)) entertainmentIndices.push(i);
	}
	if (!indices.length && !entertainmentIndices.length) return plan;

	if (entertainmentIndices.length && maxCount > 0) {
		const eligibleEntertainment = entertainmentIndices.filter(
			(i) => i >= edgeBuffer && i <= total - edgeBuffer - 1,
		);
		const pickFrom = eligibleEntertainment.length
			? eligibleEntertainment
			: entertainmentIndices;
		const hasEntertainmentIndex = indices.some((i) =>
			entertainmentIndices.includes(i),
		);
		if (!hasEntertainmentIndex && pickFrom.length) {
			const pick =
				pickFrom[Math.abs(Number(seed) || 0) % pickFrom.length] ?? pickFrom[0];
			if (indices.length >= maxCount && indices.length > 0) {
				indices[indices.length - 1] = pick;
			} else {
				indices.push(pick);
			}
			indices = Array.from(new Set(indices)).sort((a, b) => a - b);
		}
	}

	const normalized = segments.map((s) =>
		normalizeExpression(s.expression, mood),
	);
	const entertainmentSet = new Set(entertainmentIndices);
	for (const idx of indices) {
		const seg = segments[idx] || {};
		const sensitive = isSensitiveTopicText(
			`${seg.text || ""} ${seg.topicLabel || ""}`,
		);
		if (sensitive) {
			plan[idx] = "neutral";
			continue;
		}
		const preferred = normalized[idx];
		if (preferred === "excited") plan[idx] = "warm";
		else if (preferred === "thoughtful") plan[idx] = "thoughtful";
		else if (preferred === "warm") plan[idx] = "warm";
		else if (entertainmentSet.has(idx)) plan[idx] = "warm";
		else plan[idx] = "neutral";
	}
	return plan;
}

function chooseLockedPresenterVideoExpression({
	segments = [],
	topics = [],
	categoryLabel = "",
	mood = "neutral",
} = {}) {
	const text = [
		categoryLabel,
		...(Array.isArray(topics)
			? topics.map((t) => [t?.displayTopic, t?.topic, t?.angle].filter(Boolean).join(" "))
			: []),
		...(Array.isArray(segments) ? segments.map((s) => s?.text || "") : []),
	]
		.filter(Boolean)
		.join(" ");
	if (mood === "serious" || isSensitiveTopicText(text)) return "neutral";
	if (
		isDigitalWellbeingTopic({ topics, categoryLabel, text }) ||
		isSocialConnectionTopic({ topics, categoryLabel, text }) ||
		isPersonalFinanceCostOfLivingTopic({ topics, categoryLabel, text })
	) {
		return "warm";
	}
	return "neutral";
}

const CAMERA_EMPHASIS_TEXT_TOKENS = [
	"breaking",
	"shocking",
	"controversy",
	"controversial",
	"drama",
	"scandal",
	"backlash",
	"heated",
	"viral",
	"explosive",
	"pressure",
	"major twist",
	"lawsuit",
	"charges",
	"feud",
	"clash",
	"stakes",
	"warning",
	"alert",
	"bombshell",
];

function textHasToken(text = "", tokens = []) {
	const hay = String(text || "").toLowerCase();
	if (!hay) return false;
	return tokens.some((tok) => hay.includes(tok));
}

function isPoliticalTopicText(text = "") {
	return textHasToken(text, POLITICAL_TONE_TOKENS);
}

function hasCameraEmphasisCue(text = "") {
	return textHasToken(text, CAMERA_EMPHASIS_TEXT_TOKENS);
}

function inferCameraMotionPlan({
	text = "",
	topicLabel = "",
	expression = "neutral",
	mood = "neutral",
	visualType = "presenter",
	durationSec = 0,
	index = 0,
	categoryLabel = "",
} = {}) {
	if (!ENABLE_DYNAMIC_CAMERA_MOTION) return { mode: "steady" };
	const dur = Math.max(0, Number(durationSec) || 0);
	if (dur < 2.6) return { mode: "steady" };

	const hay = [text, topicLabel, categoryLabel].filter(Boolean).join(" ");
	const expr = normalizeExpression(expression, mood);
	const isImage = String(visualType || "").toLowerCase() === "image";
	if (isImage && !ENABLE_IMAGE_DYNAMIC_CAMERA_MOTION) return { mode: "steady" };
	if (!isImage && !ENABLE_PRESENTER_DYNAMIC_CAMERA_MOTION)
		return { mode: "steady" };
	const political = isPoliticalTopicText(hay);
	const entertainment = isEntertainmentTopicText(hay);
	const emphasis =
		political ||
		hasCameraEmphasisCue(hay) ||
		(entertainment && hasEntertainmentReactionCue(text)) ||
		["warm", "excited"].includes(expr);

	if (emphasis && dur >= 3.2) {
		const zoomInSec = political ? 0.42 : 0.5;
		const zoomOutSec = political ? 1.15 : 1.3;
		const startSec = Number(index) % 3 === 1 ? 0.25 : 0.15;
		const availableHold = Math.max(0, dur - startSec - zoomInSec - zoomOutSec);
		return {
			mode: "punch",
			reason: political ? "political_or_serious" : "emphasis",
			maxZoom: isImage
				? CAMERA_PUNCH_ZOOM_IMAGE_MAX
				: CAMERA_PUNCH_ZOOM_PRESENTER_MAX,
			startSec,
			zoomInSec,
			holdSec: Math.min(4, availableHold),
			zoomOutSec,
		};
	}

	if (dur >= 4.0) {
		return {
			mode: "slow",
			reason: "calm_pacing",
			maxZoom: isImage
				? CAMERA_SLOW_ZOOM_IMAGE_MAX
				: CAMERA_SLOW_ZOOM_PRESENTER_MAX,
			startSec: 0,
			zoomInSec: Math.max(1.5, dur * 0.55),
			holdSec: 0,
			zoomOutSec: Math.max(1.1, dur * 0.45),
		};
	}

	return { mode: "steady" };
}

function cameraMotionKey(plan = {}) {
	const mode = String(plan?.mode || "steady").toLowerCase();
	if (mode === "steady") return "steady";
	return `${mode}:${String(plan?.reason || "")}`;
}

function summarizeCameraMotionPlan(timeline = []) {
	const counts = {};
	const samples = [];
	for (const seg of timeline || []) {
		const motion = seg?.cameraMotion || {};
		const mode = String(motion.mode || "steady").toLowerCase();
		counts[mode] = (counts[mode] || 0) + 1;
		if (mode !== "steady" && samples.length < 12) {
			samples.push({
				index: seg.index,
				visualType: seg.visualType || "presenter",
				mode,
				reason: motion.reason || "",
				maxZoom: Number((Number(motion.maxZoom) || 1).toFixed(3)),
			});
		}
	}
	return { counts, samples };
}

function shortTitleFromText(text = "") {
	const words = normalizeBareQuestionGrammar(String(text || ""))
		.replace(/["'(){}\[\]]/g, "")
		.replace(/[.,;:!?]+/g, " ")
		.trim()
		.split(/\s+/)
		.filter(Boolean);
	if (!words.length) return "Quick Update";
	return normalizeCommonTitleAcronyms(words.slice(0, 5).join(" "));
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
			{ limit: 12 },
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
	const promptBrief = t.promptBrief || parseStructuredPromptBrief(t.promptText);
	const displayTopic = String(
		t.displayTopic || t.topic || story.title || story.rawTitle || "",
	).trim();
	const normalizedDisplayTopic = cleanTopicLabel(displayTopic) || displayTopic;
	const keywords = Array.isArray(t.keywords)
		? t.keywords.map((s) => String(s || "").trim()).filter(Boolean)
		: [];
	const relatedQueries = normalizeRelatedQueriesAny(
		story.relatedQueries || t.relatedQueries,
	);
	const interestOverTime = normalizeInterestAny(
		story.interestOverTime || t.interestOverTime,
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
	const imageSearchQueries = Array.isArray(story.imageSearchQueries)
		? story.imageSearchQueries
		: Array.isArray(t.imageSearchQueries)
			? t.imageSearchQueries
			: [];
	const directImageHints = Array.isArray(t.imageSearchHints)
		? t.imageSearchHints
		: [];
	const promptImageHints = Array.isArray(promptBrief?.imageHints)
		? promptBrief.imageHints
		: [];
	const entityNames = Array.isArray(story.entityNames)
		? story.entityNames
		: Array.isArray(t.entityNames)
			? t.entityNames
			: [];

	return {
		displayTopic: normalizedDisplayTopic,
		keywords,
		topList: t.topList || story.topList || null,
		relatedQueries,
		interestOverTime,
		articleTitles,
		articleUrls,
		seedImages: seedImages.map((u) => String(u || "").trim()).filter(Boolean),
		searchPhrases: searchPhrases
			.map((s) => String(s || "").trim())
			.filter(Boolean),
		imageSearchQueries: uniqueStrings(
			[...imageSearchQueries, ...directImageHints, ...promptImageHints]
				.map((s) => String(s || "").trim())
				.filter(Boolean),
			{ limit: 14 },
		),
		entityNames: entityNames.map((s) => String(s || "").trim()).filter(Boolean),
		imageComment: String(story.imageComment || t.imageComment || "").trim(),
		angle: String(t.angle || "").trim(),
		reason: String(t.reason || "").trim(),
	};
}

const THUMBNAIL_SERIOUS_UPDATE_RE =
	/\b(concussion|injury|injured|health|brain|hospital|recovery|tbi|brain damage|medical|surgery|illness|diagnosis|death|dead|died|dies|mourn|mourning|tribute|tributes|devastated|passed away)\b/i;
const THUMBNAIL_IMAGE_STRIP_TOKENS = [
	"concussion",
	"injury",
	"injured",
	"health",
	"brain",
	"tbi",
	"brain damage",
	"hospital",
	"recovery",
	"medical",
	"surgery",
	"illness",
	"diagnosis",
];
const THUMBNAIL_GENERIC_HOOK_HEADLINES = new Set([
	"TRENDING NOW",
	"NEW UPDATE",
	"UPDATE",
	"TOP STORIES",
	"TOP STORY",
	"BREAKING",
]);
const THUMBNAIL_GENERIC_BADGES = new Set([
	"NEW DETAILS",
	"TOP STORY",
	"TOP STORIES",
	"BIG UPDATE",
	"NEW UPDATE",
	"UPDATE",
	"JUST DROPPED",
	"INSIDE LOOK",
]);
const THUMBNAIL_PERSON_EXCLUDE_TOKENS = new Set([
	"season",
	"episode",
	"documentary",
	"movie",
	"film",
	"show",
	"series",
	"trailer",
	"update",
	"news",
	"top",
	"stories",
	"trending",
	"super",
	"bowl",
	"mtv",
]);

const THUMBNAIL_INTENT_RULES = [
	{
		intent: "legal",
		re: /\b(lawsuit|court|judge|trial|appeal|charges|indict|arrest|police|investigation|filing|custody|conservatorship|bankruptcy)\b/i,
	},
	{
		intent: "finance",
		re: /\b(stock|shares|ipo|earnings|revenue|sec|market|inflation|interest rate|crypto|bitcoin|ethereum|paycheck|rent|grocer(?:y|ies)|broke|budget|debt|bills?|subscriptions?|autopay|cost of living)\b/i,
	},
	{
		intent: "politics",
		re: /\b(election|vote|president|prime minister|senator|congress|parliament|campaign|governor|white house|supreme court|iran|israel|hezbollah|hamas|gaza|ukraine|russia|china|taiwan|middle east|peace proposal|peace talks|ceasefire|diplomacy|diplomatic|sanctions|foreign minister|state department|united nations)\b/i,
	},
	{
		intent: "sports",
		re: /\b(nfl|nba|nhl|mlb|ufc|f1|world cup|champions league|premier league|playoffs|draft pick|trade deadline|transfer window|goal scored|quarterback|linebacker|pitcher|striker)\b/i,
	},
	{
		intent: "gaming",
		re: /\b(video game|gaming|gameplay|game trailer|pc game|console game|playstation|xbox|nintendo|steam|rpg|mmo|open world|esports?)\b/i,
	},
	{
		intent: "entertainment",
		re: /\b(trailer|season|episode|premiere|cast|box office|album|tour)\b/i,
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

	if (THUMBNAIL_SERIOUS_UPDATE_RE.test(hay)) return "serious_update";
	if (
		/\b(always tired|mental fatigue|burnout|fake rest|sleep|resting|rest|overloaded|drained|exhausted|mental noise|mind never clocked out|sunlight walk|unfinished tasks)\b/.test(
			hay,
		)
	) {
		return "general";
	}
	for (const rule of THUMBNAIL_INTENT_RULES) {
		if (rule.re.test(hay)) return rule.intent;
	}

	if (Number(io.slope) >= 15) return "general_trending";
	return "general";
}

function clampHeadline(text) {
	const t = String(text || "")
		.replace(/[?]+/g, "")
		.trim()
		.toUpperCase();
	if (!t) return "";
	return t.length > 18 ? t.slice(0, 18).trim() : t;
}

function stripImageQueryTokens(text = "") {
	let cleaned = String(text || "");
	if (!cleaned) return "";
	for (const token of THUMBNAIL_IMAGE_STRIP_TOKENS) {
		const re = new RegExp(`\\b${escapeRegExp(token)}\\b`, "gi");
		cleaned = cleaned.replace(re, "");
	}
	return cleaned.replace(/\s+/g, " ").trim();
}

function looksLikePersonName(text = "") {
	const cleaned = cleanTopicLabel(text)
		.replace(/[^a-zA-Z\s]/g, " ")
		.replace(/\s+/g, " ")
		.trim();
	const tokens = cleaned.split(/\s+/).filter(Boolean);
	if (tokens.length < 2 || tokens.length > 3) return false;
	if (tokens.some((t) => /\d/.test(t))) return false;
	const lowered = tokens.map((t) => t.toLowerCase());
	if (lowered.some((t) => THUMBNAIL_PERSON_EXCLUDE_TOKENS.has(t))) return false;
	return true;
}

function extractLastName(text = "") {
	const cleaned = cleanTopicLabel(text)
		.replace(/[^a-zA-Z\s]/g, " ")
		.replace(/\s+/g, " ")
		.trim();
	const tokens = cleaned.split(/\s+/).filter(Boolean);
	if (tokens.length < 2) return "";
	return tokens[tokens.length - 1].toUpperCase();
}

function buildPersonSpecificHeadline({ title = "", signals } = {}) {
	const rq = signals?.relatedQueries || { top: [], rising: [] };
	const hay = [
		title || "",
		signals?.displayTopic || "",
		(rq.top || []).join(" "),
		(rq.rising || []).join(" "),
		(signals?.articleTitles || []).join(" "),
	].join(" ");
	const lower = hay.toLowerCase();
	if (
		/\b(cast|co-?stars?|fans|tributes?|mourn|mourning|devastated)\b/.test(
			lower,
		) &&
		/\b(death|dead|died|mourn|mourning|tributes?)\b/.test(lower)
	) {
		return "CAST MOURNS";
	}
	if (/\b(cause of death|what happened|dead|died|death)\b/.test(lower))
		return "WHAT HAPPENED";
	if (/\b(steps back|stepping away)\b/.test(lower)) return "STEPS BACK";
	if (/\b(acting pause|pause from acting|acting break)\b/.test(lower))
		return "ACTING PAUSE";
	if (/\b(what she said|what he said)\b/.test(lower)) return "WHAT SHE SAID";
	if (/\b(health update|medical update)\b/.test(lower)) return "HEALTH NEWS";
	if (THUMBNAIL_SERIOUS_UPDATE_RE.test(lower)) return "WHAT CHANGED";
	return "";
}

function pickHookFromQueries({
	rqTop = [],
	rqRising = [],
	intent = "general",
	slope = 0,
}) {
	const hay = `${rqTop.join(" ")} ${rqRising.join(" ")}`.toLowerCase();

	if (/\bwhat happened\b|\bwhat happened to\b/.test(hay))
		return { headline: "WHAT HAPPENED", badge: "TIMELINE" };
	if (/\b(cause of death|dead|died|death)\b/.test(hay))
		return { headline: "WHAT HAPPENED", badge: "WHAT WE KNOW" };
	if (/\bwhy\b|\bexplained\b|\bmeaning\b/.test(hay))
		return { headline: "WHY IT MATTERS", badge: "BREAKDOWN" };
	if (/\breaction\b|\bresigns?\b|\bsteps down\b/.test(hay))
		return { headline: "BIG REACTION", badge: "JUST DROPPED" };

	if (intent === "serious_update") {
		if (/\bwhat happened\b|\bwhat happened to\b/.test(hay))
			return { headline: "WHAT HAPPENED", badge: "WHAT CHANGED" };
		if (/\bwhat she said\b|\bwhat he said\b/.test(hay))
			return { headline: "WHAT SHE SAID", badge: "THE QUOTE" };
		if (/\brecovery\b|\bhealth\b/.test(hay))
			return { headline: "HEALTH NEWS", badge: "WHAT CHANGED" };
		return { headline: "WHAT CHANGED", badge: "WHAT CHANGED" };
	}
	if (intent === "legal")
		return { headline: "LEGAL MOVE", badge: "COURT FILE" };
	if (intent === "finance")
		return {
			headline: "MARKET MOVE",
			badge: slope >= 15 ? "BIG SWING" : "MARKET WATCH",
		};
	if (intent === "sports") return { headline: "KEY MOMENT", badge: "BIG PLAY" };
	if (intent === "entertainment")
		return {
			headline: "INSIDE STORY",
			badge: slope >= 15 ? "JUST DROPPED" : "INSIDE LOOK",
		};
	if (intent === "politics")
		return { headline: "POWER MOVE", badge: "WHAT CHANGED" };
	if (intent === "weather") return { headline: "STORM TRACK", badge: "ALERT" };

	return {
		headline: slope >= 15 ? "TRENDING NOW" : "WHAT CHANGED",
		badge: slope >= 15 ? "TRENDING" : "BREAKDOWN",
	};
}

function normalizeBadgeText(text = "") {
	const clean = String(text || "")
		.replace(/[?]+/g, "")
		.trim()
		.toUpperCase();
	if (!clean) return "";
	const words = clean.split(/\s+/).filter(Boolean).slice(0, 3);
	let out = words.join(" ");
	while (out.length > 18 && words.length > 1) {
		words.pop();
		out = words.join(" ");
	}
	return out.slice(0, 18).trim();
}

function deriveThumbnailBadgeFromSignals({
	title = "",
	signals = {},
	intent = "general",
	slope = 0,
} = {}) {
	const rq = signals.relatedQueries || { top: [], rising: [] };
	const hay = [
		title,
		signals.displayTopic || "",
		signals.angle || "",
		signals.reason || "",
		(rq.top || []).join(" "),
		(rq.rising || []).join(" "),
		(signals.articleTitles || []).join(" "),
		(signals.searchPhrases || []).join(" "),
		(signals.imageSearchQueries || []).join(" "),
		(signals.keywords || []).join(" "),
		signals.imageComment || "",
	]
		.join(" ")
		.toLowerCase();

	const topCount = Number(signals.topList?.count || 0);
	if (Number.isFinite(topCount) && topCount >= 2) return `TOP ${topCount}`;
	if (/\b(controversy|controversial|debate|backlash|divided|critics|supporters)\b/.test(hay))
		return "THE DEBATE";
	if (/\b(ranking|ranked|top\s+\d|best|worst|most|least)\b/.test(hay))
		return "RANKED";
	if (/\b(why|explained|breakdown|analysis|what it means)\b/.test(hay))
		return "BREAKDOWN";
	if (/\b(reaction|reacts?|responds?|fans)\b/.test(hay)) return "REACTION";
	if (/\b(what she said|what he said|said|statement|interview)\b/.test(hay))
		return "THE QUOTE";
	if (/\b(release date|launch|delayed|delay|trailer|gameplay|demo)\b/.test(hay))
		return intent === "gaming" ? "GAME WATCH" : "LATEST";
	if (/\b(latest|update|updates|confirmed|announced|revealed)\b/.test(hay))
		return "LATEST";
	if (intent === "legal") return "COURT FILE";
	if (intent === "finance") return "MARKET WATCH";
	if (intent === "sports") return "BIG PLAY";
	if (intent === "weather") return "ALERT";
	if (intent === "gaming") return "GAME WATCH";
	if (intent === "serious_update") return "WHAT CHANGED";
	if (Number(slope) >= 15) return "TRENDING";
	return "";
}

function resolveThumbnailBadgeText({
	badge = "",
	title = "",
	signals = {},
	intent = "general",
	slope = 0,
} = {}) {
	const normalized = normalizeBadgeText(badge);
	if (normalized && !THUMBNAIL_GENERIC_BADGES.has(normalized)) {
		return normalized;
	}
	return normalizeBadgeText(
		deriveThumbnailBadgeFromSignals({ title, signals, intent, slope }) ||
			normalized,
	);
}

function buildTopicImageQueries({ signals }) {
	const topic = signals.displayTopic || "";
	const entities = (signals.entityNames || []).slice(0, 2);
	const imageSearchQueries = Array.isArray(signals.imageSearchQueries)
		? signals.imageSearchQueries.slice(0, 6)
		: [];
	const rqTop = (signals.relatedQueries?.top || []).slice(0, 4);
	const rqRising = (signals.relatedQueries?.rising || []).slice(0, 4);

	const base = [topic, ...entities, ...imageSearchQueries, ...rqRising, ...rqTop]
		.map((s) => String(s || "").trim())
		.filter(Boolean);
	const rawCore = base[0] || topic;
	const core = stripImageQueryTokens(rawCore) || rawCore;
	const contextTexts = [
		topic,
		...entities,
		...imageSearchQueries,
		...rqRising,
		...rqTop,
		...(Array.isArray(signals.articleTitles) ? signals.articleTitles : []),
		...(Array.isArray(signals.keywords) ? signals.keywords : []),
	].filter(Boolean);
	const dynamicPhrases = buildDynamicVisualPhrasesFromTexts(contextTexts, core, {
		limit: 6,
	});
	const dynamicQueries = [];
	for (const phrase of dynamicPhrases) {
		if (core && !phrase.toLowerCase().includes(core.toLowerCase())) {
			dynamicQueries.push(mergeImageQueryTerms(core, phrase));
		}
		dynamicQueries.push(`${phrase} news photo`);
		dynamicQueries.push(`${phrase} editorial photo`);
	}
	const directQueries = imageSearchQueries
		.map((q) => sanitizeOverlayQuery(q))
		.filter(Boolean);
	const genericQueries = [
		`${core} editorial photo`,
		`${core} news photo`,
		`${core} event photo`,
		`${core} press photo`,
		`${core} official photo`,
		`${core} location photo`,
	];
	if (looksLikePersonName(topic)) {
		genericQueries.unshift(`${core} portrait`, `${core} interview`);
	}

	return uniqueStrings([...directQueries, ...dynamicQueries, ...genericQueries], {
		limit: 8,
	});
}

function buildThumbnailHookPlan({ title, topicPicks }) {
	const topics = Array.isArray(topicPicks) ? topicPicks : [];
	const t0 = topics[0] || {};
	const signals = buildThumbnailSignalsFromTopicPick(t0);
	const intent = inferIntentFromSignals({ title, signals });
	const rq = signals.relatedQueries || { top: [], rising: [] };
	const slope = Number(signals.interestOverTime?.slope || 0);
	let { headline, badge } = pickHookFromQueries({
		rqTop: rq.top,
		rqRising: rq.rising,
		intent,
		slope,
	});
	const resolvedBadge = resolveThumbnailBadgeText({
		badge,
		title,
		signals,
		intent,
		slope,
	});
	let resolvedHeadline = clampHeadline(headline);
	const storySpecificHeadline = buildPersonSpecificHeadline({ title, signals });
	if (
		storySpecificHeadline &&
		(THUMBNAIL_GENERIC_HOOK_HEADLINES.has(resolvedHeadline) ||
			/\b(death|dead|died|mourn|mourning|tribute|devastated)\b/i.test(
				[
					title || "",
					signals?.displayTopic || "",
					(signals?.articleTitles || []).join(" "),
					(rq.top || []).join(" "),
					(rq.rising || []).join(" "),
				].join(" "),
			))
	) {
		resolvedHeadline = clampHeadline(storySpecificHeadline);
	}
	const isPerson = looksLikePersonName(signals.displayTopic || "");
	if (topics.length === 1 && isPerson) {
		const isGeneric = THUMBNAIL_GENERIC_HOOK_HEADLINES.has(resolvedHeadline);
		if (isGeneric) {
			const actionHeadline = buildPersonSpecificHeadline({ title, signals });
			const lastName = extractLastName(signals.displayTopic || "");
			const fallback =
				(actionHeadline && actionHeadline !== "WHAT CHANGED"
					? actionHeadline
					: lastName
						? `${lastName} DETAILS`
						: actionHeadline) || "";
			if (fallback) resolvedHeadline = clampHeadline(fallback);
		}
	}
	const promptBadge = primaryPromptThumbnailText(topics);
	const promptSecondaryBadge = secondaryPromptThumbnailBadge(topics);
	const topicHay = [
		title || "",
		signals.displayTopic || "",
		signals.angle || "",
		signals.reason || "",
		(signals.keywords || []).join(" "),
		promptBadge,
	]
		.join(" ")
		.toLowerCase();
	if (/\b(broke|paycheck|rent|grocery|groceries|bills?|budget|cost of living|subscriptions?)\b/.test(topicHay)) {
		if (/\bjob|paycheck|work|working|employed\b/.test(topicHay)) {
			resolvedHeadline = clampHeadline(promptBadge ? "WITH A JOB?" : "STILL BROKE?");
		} else if (/\brent\b/.test(topicHay)) {
			resolvedHeadline = clampHeadline("RENT SQUEEZE");
		} else {
			resolvedHeadline = clampHeadline("MONEY SQUEEZE");
		}
	}

	if (topics.length > 1) {
		return {
			intent: "multi",
			headline: "TOP PICKS",
			badgeText: "FAST ROUNDUP",
			imageQueries: [],
		};
	}

	return {
		intent,
		headline: promptBadge || resolvedHeadline,
		badgeText: promptBadge ? promptSecondaryBadge || resolvedBadge : resolvedBadge,
		imageQueries: buildTopicImageQueries({ signals, intent }),
	};
}

function cleanTopicLabel(text = "") {
	return normalizeBareQuestionGrammar(
		String(text || "")
			.replace(/["'(){}\[\]]/g, "")
			.replace(/\s+/g, " ")
			.replace(/[.!?]+$/g, "")
			.trim(),
	);
}

function stripAnchorNoise(label = "") {
	const tokens = cleanTopicLabel(label)
		.split(/\s+/)
		.filter(Boolean)
		.filter((t) => /[a-z0-9]/i.test(t));
	while (tokens.length) {
		const lower = tokens[0].toLowerCase();
		if (
			ANCHOR_NOISE_TOKENS.has(lower) ||
			(tokens.length > 3 &&
				tokens[0].length <= 2 &&
				!ANCHOR_SHORT_TOKENS_KEEP.has(lower))
		) {
			tokens.shift();
			continue;
		}
		break;
	}
	while (tokens.length) {
		const lower = tokens[tokens.length - 1].toLowerCase();
		if (
			ANCHOR_NOISE_TOKENS.has(lower) ||
			(tokens.length > 3 &&
				tokens[tokens.length - 1].length <= 2 &&
				!ANCHOR_SHORT_TOKENS_KEEP.has(lower))
		) {
			tokens.pop();
			continue;
		}
		break;
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
			t,
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
			"",
		)
		.replace(/^(what time does|what time do|when does|when do)\s+/i, "")
		.replace(
			/^(what is|who is|who are|how does|how do|why does|why is)\s+/i,
			"",
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
		/^(.*)\b(die|dies|died|death)\b\s*(?:in|on|at|during)?\s*(.*)$/i,
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

function normalizeBareQuestionGrammar(text = "") {
	let cleaned = String(text || "")
		.replace(/\s+/g, " ")
		.trim();
	if (!cleaned) return cleaned;

	cleaned = cleaned.replace(
		/\bdoes\s+([^?.,;:]{2,80}?)\s+ha(?:s|ve)\s+(?:a\s+)?mask(?:\s+on)?\b/gi,
		(_match, subject) => `is ${String(subject || "").trim()} wearing a mask`,
	);
	cleaned = cleaned.replace(
		/\bdoes\s+([^?.,;:]{2,80}?)\s+ha(?:s|ve)\s+(?:a\s+)?face\s+mask\b/gi,
		(_match, subject) => `is ${String(subject || "").trim()} wearing a face mask`,
	);
	cleaned = cleaned.replace(
		/\b(did|does|do)\s+([^?.,;:]{2,80}?)\s+has\b/gi,
		(_match, aux, subject) => `${aux} ${String(subject || "").trim()} have`,
	);
	cleaned = cleaned.replace(
		/\b(takes?|requires?|needs?)\s+(?:much|far)\s+more\s+than\./gi,
		(_match, verb) => `${verb} much more evidence.`,
	);
	return cleaned
		.replace(/\s+([,.!?;:])/g, "$1")
		.replace(/\s{2,}/g, " ")
		.trim();
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

function formatLabelList(labels = []) {
	const list = (labels || []).filter(Boolean).slice(0, 3);
	if (!list.length) return "";
	if (list.length === 1) return list[0];
	if (list.length === 2) return `${list[0]} and ${list[1]}`;
	return `${list[0]}, ${list[1]}, and ${list[2]}`;
}

function buildIntroTopicLabels(topics = [], maxWords = 4) {
	return (topics || [])
		.map((t) => shortTopicLabel(t?.displayTopic || t?.topic || t, maxWords))
		.filter(Boolean)
		.slice(0, 3);
}

const FILLER_WORD_REGEX = /\b(?:um+|uh+|uhm+|erm+|er|ah+|hmm+)\b/gi;
const LIKE_FILLER_REGEX = /([,.!?]\s+)like\s*,\s*/gi;
const MICRO_EMOTE_REGEX = /\b(?:heh|whew)\b/gi;

function cleanupSpeechText(text = "") {
	let t = String(text || "");
	t = t.replace(/([.!?])(?=[A-Z])/g, "$1 ");
	t = t.replace(/\s+([,.;:!?])/g, "$1");
	t = t.replace(/([,;:!?]){2,}/g, "$1");
	t = t.replace(/,\s*,/g, ", ");
	t = t.replace(/,\s*([.!?])/g, "$1");
	t = t.replace(/\s+/g, " ").trim();
	return t;
}

const MICRO_BREATH_BREAK_TOKENS = new Set([
	"and",
	"but",
	"so",
	"because",
	"while",
	"as",
	"when",
	"which",
	"that",
	"though",
	"however",
]);

function hasBreathPunctuation(text = "") {
	const t = String(text || "");
	return /[,;:]/.test(t) || /\.{3,}/.test(t) || /\s--\s/.test(t);
}

function injectMicroBreath(text = "", state) {
	if (!ENABLE_MICRO_BREATHS || !FORCE_NEUTRAL_VOICEOVER || !state)
		return String(text || "").trim();
	if (state.used >= MAX_MICRO_BREATHS_PER_VIDEO)
		return String(text || "").trim();

	const t = String(text || "").trim();
	if (!t) return t;
	if (countWords(t) < MICRO_BREATH_MIN_WORDS) return t;
	if (splitSentences(t).length !== 1) return t;
	if (hasBreathPunctuation(t)) return t;

	const words = t.split(/\s+/);
	const targetIdx = Math.min(
		words.length - 5,
		Math.max(MICRO_BREATH_TARGET_WORD, Math.floor(words.length * 0.45)),
	);
	const cleanWord = (w) => w.toLowerCase().replace(/[^a-z']/g, "");
	const scan = (start, end, step) => {
		for (let i = start; step > 0 ? i <= end : i >= end; i += step) {
			if (i <= 2 || i >= words.length - 2) continue;
			const w = cleanWord(words[i]);
			if (MICRO_BREATH_BREAK_TOKENS.has(w)) return i;
		}
		return -1;
	};

	let breakIdx = scan(targetIdx, Math.min(words.length - 3, targetIdx + 6), 1);
	if (breakIdx === -1) {
		breakIdx = scan(targetIdx, Math.max(3, targetIdx - 6), -1);
	}
	if (breakIdx === -1) breakIdx = targetIdx;

	if (/[,.!?;:]$/.test(words[breakIdx])) return t;
	words[breakIdx] = `${words[breakIdx]},`;
	state.used += 1;
	return cleanupSpeechText(words.join(" "));
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
			"",
		)
		.replace(/\b(next|this|that|first|second|third|final)\s+segment\b/gi, "")
		.replace(/\s+/g, " ")
		.trim();
	return cleanupSpeechText(softened);
}

function stripFillerAndEmotes(
	text = "",
	state,
	{ maxFillers = 0, maxEmotes = 0 } = {},
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

function stripPromptLabelArtifacts(text = "") {
	let t = String(text || "");
	if (!t) return t;
	t = t.replace(/([.!?])(?=[A-Z])/g, "$1 ");
	t = t.replace(
		/\b(?:memorable\s+line|must\s+include(?:\s+(?:this\s+)?(?:line|sentence))?|include\s+(?:this\s+)?(?:line|sentence))\s*[:?.-]?\s*/gi,
		"",
	);
	t = cleanupSpeechText(t);
	const sentences = splitSentences(t).filter(Boolean);
	if (sentences.length > 1) {
		const seen = new Set();
		const kept = [];
		for (const sentence of sentences) {
			const key = normalizeQaText(sentence);
			if (key && seen.has(key)) continue;
			if (key) seen.add(key);
			kept.push(sentence);
		}
		t = cleanupSpeechText(kept.join(" "));
	}
	return t;
}

function sanitizeIntroOutroLine(text = "") {
	const base = stripMetaNarration(text);
	const { text: cleaned } = stripFillerAndEmotes(
		base,
		{ fillers: 0, emotes: 0 },
		{ maxFillers: 0, maxEmotes: 0 },
	);
	return normalizeBareQuestionGrammar(stripPromptLabelArtifacts(cleaned));
}

function stripAllFillers(text = "") {
	const base = stripMetaNarration(text);
	const { text: cleaned } = stripFillerAndEmotes(
		base,
		{ fillers: 0, emotes: 0 },
		{ maxFillers: 0, maxEmotes: 0 },
	);
	return normalizeBareQuestionGrammar(stripPromptLabelArtifacts(cleaned));
}

function sanitizeSegmentText(text = "") {
	const cleaned = stripAllFillers(text);
	return cleaned || "Quick update.";
}

function visualCueLeakTokens(text = "") {
	return filterSpecificTopicTokens(tokenizeLabel(text || "")).filter(
		(t) =>
			t &&
			!SEGMENT_IMAGE_STOP_TOKENS.has(t) &&
			!GENERIC_TOPIC_TOKENS.has(t),
	);
}

function isLikelyVisualCueLeakLabel(label = "", cuePhrases = []) {
	const labelTokens = visualCueLeakTokens(label);
	if (labelTokens.length < 2) return false;
	for (const phrase of cuePhrases || []) {
		const cueTokens = visualCueLeakTokens(phrase);
		if (cueTokens.length < 2) continue;
		const overlap = labelTokens.filter((token) =>
			cueTokens.includes(token),
		).length;
		if (overlap >= Math.min(3, cueTokens.length, labelTokens.length)) {
			return true;
		}
	}
	return /\b(image|visual|photo|picture|search|query|cue)\b/i.test(label);
}

function collectVisualCuePhrasesForSegment(seg = {}, topics = []) {
	const topicIndex = Number(seg?.topicIndex) || 0;
	const topic = topics?.[topicIndex] || {};
	const story = topic?.trendStory || {};
	const cueQueries = Array.isArray(seg?.overlayCues)
		? seg.overlayCues.map((cue) => cue?.query).filter(Boolean)
		: [];
	return uniqueStrings(
		[
			...cueQueries,
			...(Array.isArray(story.imageSearchQueries)
				? story.imageSearchQueries
				: []),
		],
		{ limit: 20 },
	);
}

function stripVisualCueLeakFromText(text = "", cuePhrases = []) {
	let updated = String(text || "").trim();
	if (!updated || !Array.isArray(cuePhrases) || !cuePhrases.length) return updated;

	updated = updated.replace(
		/^((?:According to|Per)\s+[^,]{2,60},\s*)([^:]{6,140}):\s*/i,
		(match, prefix, label) =>
			isLikelyVisualCueLeakLabel(label, cuePhrases) ? prefix : match,
	);
	updated = updated.replace(/^([^:]{6,140}):\s*/i, (match, label) =>
		isLikelyVisualCueLeakLabel(label, cuePhrases) ? "" : match,
	);
	updated = updated.replace(
		/\b((?:that|this|which)\s+is\s+why)\s+([^,.]{6,160}?)\s+(?:has\s+become|became|is|was)\s+(?:the\s+)?(?:main\s+|lead\s+|anchor\s+)?(?:image|visual|photo|picture)\s*,?\s*(?:even\s+while|while)\s+/i,
		(match, intro, label) =>
			isLikelyVisualCueLeakLabel(label, cuePhrases) ? `${intro} ` : match,
	);

	return cleanupSpeechText(updated)
		.replace(/\s+([,.!?])/g, "$1")
		.replace(/\s{2,}/g, " ")
		.trim();
}

function sanitizeScriptVisualCueLeaks(script = {}, topics = []) {
	if (!script || !Array.isArray(script.segments)) return script;
	const segments = script.segments.map((seg) => {
		const cuePhrases = collectVisualCuePhrasesForSegment(seg, topics);
		const cleaned = stripVisualCueLeakFromText(seg.text || "", cuePhrases);
		return {
			...seg,
			text: sanitizeSegmentText(cleaned || seg.text || ""),
		};
	});
	return { ...script, segments };
}

const SHORTS_CLIP_TYPES = new Set([
	"hook",
	"twist",
	"controversy",
	"context_needed",
]);

const SHORTS_OPEN_LOOP_HINTS = [
	/\bbut\b/i,
	/\bhowever\b/i,
	/\byet\b/i,
	/\bstill\b/i,
	/\bwhether\b/i,
	/\bnot just\b/i,
	/\bthe detail\b/i,
	/\bthe twist\b/i,
	/\bwhat people missed\b/i,
	/\bwhat people ignore\b/i,
	/\bthe question\b/i,
	/\bthe unresolved\b/i,
	/\bnot clear\b/i,
	/\bnot proof\b/i,
	/\bnot evidence\b/i,
	/\bunclear\b/i,
	/\bnot confirmed\b/i,
];

const SHORTS_EARLY_RESOLUTION_HINTS = [
	/\bso the answer is\b/i,
	/\btherefore\b/i,
	/\bin short\b/i,
	/\bthe takeaway\b/i,
	/\bthis means\b/i,
	/\bthe reason is\b/i,
];

const SHORTS_ENDING_BLOCKLIST = [
	/\bwhat do you think\b/i,
	/\bthoughts on\b/i,
	/\bwhich topic stood out\b/i,
];

const SHORTS_FORWARD_LOOKING_HINTS = [
	/\bthat question\b/i,
	/\bthe question\b/i,
	/\bwhat happens next\b/i,
	/\bwhat comes next\b/i,
	/\bwatch for\b/i,
	/\bnext update\b/i,
	/\bnext piece\b/i,
	/\bnext decision\b/i,
	/\bstill unresolved\b/i,
	/\bmissing detail\b/i,
	/\bopen question\b/i,
	/\bthe real test\b/i,
	/\bwhether you\b/i,
	/\bleave smarter\b/i,
];

function textHasAnyRegex(text = "", list = []) {
	return list.some((rx) => rx.test(String(text || "")));
}

function segmentHasOpenLoop(text = "") {
	if (/\?/.test(String(text || ""))) return true;
	return textHasAnyRegex(text, SHORTS_OPEN_LOOP_HINTS);
}

function collectTextUpToSeconds(segments = [], seconds = 0) {
	if (!segments.length || !seconds) return "";
	let elapsed = 0;
	const parts = [];
	for (const seg of segments) {
		if (elapsed >= seconds) break;
		const text = String(seg?.text || "").trim();
		if (text) parts.push(text);
		elapsed += countWords(text) / SCRIPT_VOICE_WPS;
	}
	return parts.join(" ").trim();
}

function countOpenLoopsWithinSeconds(segments = [], seconds = 0) {
	if (!segments.length || !seconds) return 0;
	let elapsed = 0;
	let count = 0;
	for (const seg of segments) {
		const text = String(seg?.text || "").trim();
		if (!text) continue;
		if (elapsed > seconds) break;
		if (segmentHasOpenLoop(text)) count += 1;
		elapsed += countWords(text) / SCRIPT_VOICE_WPS;
	}
	return count;
}

function analyzeShortsGuardrails(script = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const issues = [];
	if (!segments.length) {
		return { pass: false, needsRewrite: true, issues: ["segments_missing"] };
	}

	const earlyText = collectTextUpToSeconds(segments, SHORTS_EARLY_WINDOW_SEC);
	const earlyHasGap =
		/\?/.test(earlyText) || textHasAnyRegex(earlyText, SHORTS_OPEN_LOOP_HINTS);
	const earlyHasResolution = textHasAnyRegex(
		earlyText,
		SHORTS_EARLY_RESOLUTION_HINTS,
	);
	if (!earlyHasGap) issues.push("early_curiosity_gap_missing");
	if (earlyHasResolution) issues.push("early_resolution_detected");

	const loopCount = countOpenLoopsWithinSeconds(
		segments,
		SHORTS_OPEN_LOOP_WINDOW_SEC,
	);
	if (loopCount < SHORTS_OPEN_LOOP_MIN_COUNT)
		issues.push("open_loops_under_target");

	const lastText = String(segments[segments.length - 1]?.text || "").trim();
	const endingBlocked = textHasAnyRegex(lastText, SHORTS_ENDING_BLOCKLIST);
	const endingForward =
		/\?/.test(lastText) ||
		textHasAnyRegex(lastText, SHORTS_FORWARD_LOOKING_HINTS) ||
		textHasAnyRegex(lastText, SHORTS_OPEN_LOOP_HINTS);
	if (endingBlocked) issues.push("ending_question_generic");
	if (!endingForward) issues.push("ending_open_loop_missing");

	return {
		pass: issues.length === 0,
		needsRewrite: issues.length > 0,
		issues,
		stats: {
			earlyHasGap,
			earlyHasResolution,
			openLoopCount: loopCount,
			endingBlocked,
			endingForward,
		},
	};
}

function normalizeShortsClipType(raw) {
	const t = String(raw || "")
		.trim()
		.toLowerCase();
	if (SHORTS_CLIP_TYPES.has(t)) return t;
	return "context_needed";
}

function normalizeShortsTargetSeconds(raw) {
	const n = Number(raw);
	if (SHORTS_TARGET_SECONDS.includes(n)) return n;
	return SHORTS_DEFAULT_TARGET_SECONDS;
}

const TITLE_SMALL_WORDS = new Set([
	"a",
	"an",
	"and",
	"as",
	"at",
	"but",
	"by",
	"for",
	"from",
	"in",
	"into",
	"nor",
	"of",
	"on",
	"or",
	"over",
	"per",
	"so",
	"the",
	"to",
	"up",
	"via",
	"vs",
	"with",
]);

function normalizeTitleWhitespace(text = "") {
	return String(text || "")
		.replace(/[\r\n]+/g, " ")
		.replace(/\s+/g, " ")
		.replace(/\s*([:|!?])/g, "$1")
		.replace(/([:|!?])(?=\S)/g, "$1 ")
		.replace(/\s+[\u2013\u2014]\s+/g, " - ")
		.replace(/\s+-\s+/g, " - ")
		.trim();
}

function normalizeCommonTitleAcronyms(text = "") {
	return String(text || "")
		.replace(/\bU\s+S(?=\s|$|[,;:!?])/g, "U.S.")
		.replace(/\bU\.s\.?(?=\s|$|[,;:!?])/g, "U.S.")
		.replace(/\bU\.S\.?(?=\s|$|[,;:!?])/g, "U.S.")
		.replace(/\bUS(?=\s|$|[,;:!?])/g, "U.S.")
		.replace(/\bU\s+K(?=\s|$|[,;:!?])/g, "U.K.")
		.replace(/\bU\.k\.?(?=\s|$|[,;:!?])/g, "U.K.")
		.replace(/\bU\.K\.?(?=\s|$|[,;:!?])/g, "U.K.")
		.replace(/\bUK(?=\s|$|[,;:!?])/g, "U.K.")
		.replace(/\bU\s+N(?=\s|$|[,;:!?])/g, "U.N.")
		.replace(/\bU\.n\.?(?=\s|$|[,;:!?])/g, "U.N.")
		.replace(/\bU\.N\.?(?=\s|$|[,;:!?])/g, "U.N.")
		.replace(/\bUN(?=\s|$|[,;:!?])/g, "U.N.");
}

function isAllCapsTitleToken(token = "") {
	return /^[A-Z0-9&+/'-]{2,}$/.test(token || "");
}

function headlineCaseToken(token = "", isBoundary = false) {
	const match = String(token || "").match(
		/^([^A-Za-z0-9]*)(.*?)([^A-Za-z0-9]*)$/,
	);
	if (!match) return token;
	const [, prefix, core, suffix] = match;
	if (!core) return token;
	if (isAllCapsTitleToken(core) || /\d/.test(core)) {
		return `${prefix}${core}${suffix}`;
	}
	if (/[a-z][A-Z]|[A-Z][a-z].*[A-Z]/.test(core)) {
		return `${prefix}${core}${suffix}`;
	}
	const lower = core.toLowerCase();
	if (!isBoundary && TITLE_SMALL_WORDS.has(lower)) {
		return `${prefix}${lower}${suffix}`;
	}
	const capitalized = lower.replace(/(^|[-/])([a-z])/g, (_m, lead, char) => {
		return `${lead}${char.toUpperCase()}`;
	});
	return `${prefix}${capitalized}${suffix}`;
}

function toHeadlineCase(text = "") {
	const normalized = normalizeTitleWhitespace(text);
	if (!normalized) return "";
	const tokens = normalized.split(" ");
	return tokens
		.map((token, index) => {
			const prev = tokens[index - 1] || "";
			const isBoundary =
				index === 0 || index === tokens.length - 1 || /[:|!-]$/.test(prev);
			return headlineCaseToken(token, isBoundary);
		})
		.join(" ")
		.replace(/\s+\|/g, " |")
		.replace(/\|\s+/g, " | ");
}

function formatHumanTitle(text = "", max = 95) {
	let cleaned = normalizeBareQuestionGrammar(normalizeTitleWhitespace(text))
		.replace(/^["'`]+|["'`]+$/g, "")
		.replace(/([!?]){2,}/g, "$1")
		.replace(/\.{2,}/g, "...")
		.replace(/[.]+$/g, "")
		.replace(/\s+[|:-]\s*$/g, "")
		.trim();
	if (!cleaned) return "";
	cleaned = toHeadlineCase(cleaned);
	cleaned = normalizeCommonTitleAcronyms(cleaned);
	return trimTitleToLimit(cleaned, max);
}

function buildTitleCandidates(baseTitle = "", shortTitle = "") {
	const base = String(shortTitle || baseTitle || "").trim();
	if (!base) return [];
	const variants = [
		base,
		`The detail people missed about ${base}`,
		`The twist in ${base}`,
		`Why ${base} is trending`,
		`What changed with ${base}`,
		`The real story behind ${base}`,
		`${base} - the missing piece`,
	];
	return uniqueStrings(variants, { limit: 8 }).map((t) =>
		formatHumanTitle(t, 95),
	);
}

function trimTitleToLimit(text = "", max = 95) {
	const cleaned = String(text || "")
		.replace(/\s+/g, " ")
		.trim();
	if (!cleaned) return "";
	if (cleaned.length <= max) return cleaned;
	const clipped = cleaned.slice(0, max);
	return clipped.replace(/\s+\S*$/, "").trim();
}

function cleanClipTitleBase(text = "") {
	let cleaned = String(text || "")
		.replace(/\s+/g, " ")
		.trim();
	if (!cleaned) return "";
	cleaned = cleaned.replace(/^["']+|["']+$/g, "");
	cleaned = cleaned.replace(
		/^(here's|here is|this is|there's|there is|today|right now)\b[:,-]?\s*/i,
		"",
	);
	cleaned = cleaned.replace(/^[\-\s]+/, "");
	cleaned = cleaned.replace(/[.!?]+$/g, "");
	cleaned = cleaned.replace(/^\s*(and|but|so)\s+/i, "");
	return cleaned.trim();
}

function buildClipTitleCandidates(line = "", fallbackBase = "") {
	const base = trimTitleToLimit(
		cleanClipTitleBase(line) || String(fallbackBase || "").trim(),
		95,
	);
	if (!base) return [];
	const variants = [
		base,
		`The key detail: ${base}`,
		`What changed: ${base}`,
		`Why it matters: ${base}`,
		`The quick update: ${base}`,
		`${base} | The detail people missed`,
	];
	return uniqueStrings(variants, { limit: 8 }).map((t) =>
		formatHumanTitle(t, 95),
	);
}

function buildThumbnailTextCandidates(baseTitle = "") {
	const base = String(baseTitle || "").trim();
	const tokens = base.split(/\s+/).slice(0, 2).join(" ");
	const variants = [
		"The detail",
		"Still unclear",
		"The twist",
		"People missed",
		"What changed",
		"The missing piece",
		"Why it shifted",
	];
	if (tokens) variants.unshift(tokens);
	return uniqueStrings(variants, { limit: 8 }).map((t) => t.trim());
}

function buildClipThumbnailTextCandidates(line = "", fallbackBase = "") {
	const base = cleanClipTitleBase(line) || String(fallbackBase || "").trim();
	const tokens = base.split(/\s+/).slice(0, 3).join(" ");
	const variants = [
		tokens || fallbackBase,
		"Key detail",
		"What changed",
		"Why it matters",
		"The twist",
		"Still unclear",
	];
	return uniqueStrings(variants, { limit: 8 }).map((t) => t.trim());
}

function scoreSegmentForClip(text = "", index = 0) {
	const t = String(text || "").toLowerCase();
	let score = 0;
	if (index === 0) score += 4;
	if (/\?/.test(t)) score += 3;
	if (textHasAnyRegex(t, SHORTS_OPEN_LOOP_HINTS)) score += 2;
	if (/\bbut\b|\bhowever\b|\bturns out\b|\bodd\b/.test(t)) score += 1;
	return score;
}

function inferClipTypeFromText(text = "", index = 0) {
	const t = String(text || "").toLowerCase();
	if (index === 0) return "hook";
	if (/\bcontroversy\b|\bbacklash\b|\bcritics\b|\bpolarizing\b/.test(t))
		return "controversy";
	if (/\btwist\b|\bturns out\b|\bbut\b|\bhowever\b|\bodd\b/.test(t))
		return "twist";
	return "context_needed";
}

function buildFallbackShortsDetails(script = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const title = String(script?.title || "").trim();
	const shortTitle = String(script?.shortTitle || "").trim();
	const scored = segments.map((s, idx) => ({
		idx,
		score: scoreSegmentForClip(s.text || "", idx),
	}));
	scored.sort((a, b) => b.score - a.score);
	const picked = scored.slice(0, SHORTS_MAX_CANDIDATES).map((s) => s.idx);
	const clipCandidates = picked.map((segIndex, i) => {
		const seg = segments[segIndex] || {};
		const line = String(seg.text || "").trim();
		const type = inferClipTypeFromText(line, segIndex);
		const targetSeconds =
			type === "hook"
				? SHORTS_TARGET_SECONDS[0]
				: type === "twist"
					? SHORTS_TARGET_SECONDS[1]
					: SHORTS_TARGET_SECONDS[2] || SHORTS_DEFAULT_TARGET_SECONDS;
		const fallbackBase = shortTitle || title;
		return {
			id: `short_${segIndex}_${i}`,
			type,
			segmentIndex: segIndex,
			line: line || "Quick update.",
			openLoop: segmentHasOpenLoop(line),
			ctaLine: SHORTS_DEFAULT_CTA_LINE,
			targetSeconds: normalizeShortsTargetSeconds(targetSeconds),
			titleCandidates: buildClipTitleCandidates(line, fallbackBase),
			thumbnailTextCandidates: buildClipThumbnailTextCandidates(
				line,
				fallbackBase,
			),
		};
	});

	return {
		angle: shortTitle || title,
		titleCandidates: buildTitleCandidates(title, shortTitle),
		thumbnailTextCandidates: buildThumbnailTextCandidates(shortTitle || title),
		clipCandidates,
	};
}

function normalizeShortsDetails(raw, script = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const safe = raw && typeof raw === "object" ? raw : {};
	const angle = String(safe.angle || "").trim();
	const titleCandidates = Array.isArray(
		safe.titleCandidates || safe.title_candidates,
	)
		? safe.titleCandidates || safe.title_candidates
		: [];
	const thumbnailTextCandidates = Array.isArray(
		safe.thumbnailTextCandidates || safe.thumbnail_text_candidates,
	)
		? safe.thumbnailTextCandidates || safe.thumbnail_text_candidates
		: [];
	const clipCandidatesRaw =
		safe.clipCandidates || safe.clip_candidates || safe.clipCandidates || [];
	const clipCandidates = (
		Array.isArray(clipCandidatesRaw) ? clipCandidatesRaw : []
	)
		.map((c, idx) => {
			const segIndex = Number(
				c?.segmentIndex ?? c?.segment_index ?? c?.index ?? c?.segment ?? idx,
			);
			if (!Number.isFinite(segIndex) || segIndex < 0) return null;
			if (segments.length && segIndex >= segments.length) return null;
			const baseText = segments[segIndex]?.text || "";
			const rawLine = String(c?.line || "").trim();
			const line = rawLine || String(baseText || "").trim();
			if (!line) return null;
			const type = normalizeShortsClipType(c?.type);
			const targetSeconds = normalizeShortsTargetSeconds(
				c?.targetSeconds ?? c?.target_seconds,
			);
			const openLoop =
				typeof c?.openLoop === "boolean"
					? c.openLoop
					: segmentHasOpenLoop(line);
			const ctaLine = String(c?.ctaLine || c?.cta_line || "").trim();
			const rawTitleCandidates = Array.isArray(
				c?.titleCandidates ||
					c?.title_candidates ||
					c?.seoTitleCandidates ||
					c?.seo_title_candidates,
			)
				? c.titleCandidates ||
					c.title_candidates ||
					c.seoTitleCandidates ||
					c.seo_title_candidates
				: [];
			let titleCandidates = uniqueStrings(
				(rawTitleCandidates || [])
					.map((t) => String(t || "").trim())
					.filter(Boolean),
				{ limit: 8 },
			);
			if (titleCandidates.length < 3) {
				const fallbackTitles = buildClipTitleCandidates(
					line,
					script?.shortTitle || script?.title || "",
				);
				titleCandidates = uniqueStrings(
					[...titleCandidates, ...fallbackTitles],
					{ limit: 8 },
				);
			}
			const rawThumbCandidates = Array.isArray(
				c?.thumbnailTextCandidates || c?.thumbnail_text_candidates,
			)
				? c.thumbnailTextCandidates || c.thumbnail_text_candidates
				: [];
			let thumbnailTextCandidates = uniqueStrings(
				(rawThumbCandidates || [])
					.map((t) => String(t || "").trim())
					.filter(Boolean),
				{ limit: 8 },
			);
			if (thumbnailTextCandidates.length < 3) {
				const fallbackThumbs = buildClipThumbnailTextCandidates(
					line,
					script?.shortTitle || script?.title || "",
				);
				thumbnailTextCandidates = uniqueStrings(
					[...thumbnailTextCandidates, ...fallbackThumbs],
					{ limit: 8 },
				);
			}
			return {
				id: String(c?.id || `short_${segIndex}_${idx}`),
				type,
				segmentIndex: segIndex,
				line,
				openLoop,
				ctaLine: ctaLine || SHORTS_DEFAULT_CTA_LINE,
				targetSeconds,
				titleCandidates,
				thumbnailTextCandidates,
			};
		})
		.filter(Boolean);

	const normalized = {
		angle,
		titleCandidates: uniqueStrings(
			(titleCandidates || [])
				.map((t) => String(t || "").trim())
				.filter(Boolean),
			{ limit: 8 },
		),
		thumbnailTextCandidates: uniqueStrings(
			(thumbnailTextCandidates || [])
				.map((t) => String(t || "").trim())
				.filter(Boolean),
			{ limit: 8 },
		),
		clipCandidates,
	};

	if (!normalized.angle) {
		normalized.angle = String(script?.shortTitle || script?.title || "").trim();
	}
	if (normalized.titleCandidates.length < 5) {
		const extra = buildTitleCandidates(script?.title, script?.shortTitle);
		normalized.titleCandidates = uniqueStrings(
			[...normalized.titleCandidates, ...extra],
			{ limit: 8 },
		);
	}
	if (normalized.thumbnailTextCandidates.length < 5) {
		const extra = buildThumbnailTextCandidates(
			script?.shortTitle || script?.title || "",
		);
		normalized.thumbnailTextCandidates = uniqueStrings(
			[...normalized.thumbnailTextCandidates, ...extra],
			{ limit: 8 },
		);
	}
	if (normalized.clipCandidates.length < SHORTS_MIN_CANDIDATES) {
		const fallback = buildFallbackShortsDetails(script);
		const merged = [];
		const seen = new Set();
		for (const item of [
			...normalized.clipCandidates,
			...fallback.clipCandidates,
		]) {
			const id = String(item?.id || "").trim();
			if (!id || seen.has(id)) continue;
			seen.add(id);
			merged.push(item);
			if (merged.length >= SHORTS_MAX_CANDIDATES) break;
		}
		normalized.clipCandidates = merged;
	}
	if (normalized.clipCandidates.length > SHORTS_MAX_CANDIDATES) {
		normalized.clipCandidates = normalized.clipCandidates.slice(
			0,
			SHORTS_MAX_CANDIDATES,
		);
	}

	return normalized;
}

async function generateShortsDetailsWithModel({ jobId, script, topics = [] }) {
	if (!process.env.CHATGPT_API_TOKEN) return null;
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return null;
	const topicLabels = (topics || [])
		.map((t) => t?.displayTopic || t?.topic || "")
		.filter(Boolean)
		.join(", ");
	const prompt = `
You are creating clip candidates for YouTube Shorts from a long-form script.
Keep the voice neutral and factual. Do NOT resolve everything in the clip.
Provide clip candidates that are cut-ready and end with an open loop.

Topics: ${topicLabels || "general"}

Return JSON ONLY:
{
  "angle": "one sentence, what this video is really about",
  "titleCandidates": ["5-8 options, max 95 chars each"],
  "thumbnailTextCandidates": ["5-8 options, 2-5 words each"],
  "clipCandidates": [
    {
      "type": "hook|twist|controversy|context_needed",
      "segmentIndex": 0,
      "line": "exact sentence(s) from that segment text",
      "openLoop": true,
      "ctaLine": "Full breakdown on the channel.",
      "targetSeconds": 25,
      "titleCandidates": ["3-6 clip-specific options, max 95 chars each"],
      "thumbnailTextCandidates": ["3-6 clip-specific options, 2-5 words each"]
    }
  ]
}

Rules:
- Return 3-6 clipCandidates.
- segmentIndex must map to the provided segment index.
- line must be copied verbatim from the segment text.
- openLoop=true only if the clip does NOT resolve the question.
- targetSeconds must be 25, 35, or 45.
- Each clip must include titleCandidates + thumbnailTextCandidates that are descriptive of that clip line (not generic).

Segments:
${segments.map((s) => `#${s.index}: ${s.text}`).join("\n")}
`.trim();

	try {
		const resp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: prompt }],
		});
		const parsed = parseJsonFlexible(
			resp?.choices?.[0]?.message?.content || "",
		);
		if (!parsed || typeof parsed !== "object") return null;
		return parsed;
	} catch (e) {
		if (jobId)
			logJob(jobId, "shorts details generation failed", { error: e.message });
		return null;
	}
}

async function ensureShortsDetails({ jobId, script, topics }) {
	const existing = script?.shortsDetails || null;
	const normalized = normalizeShortsDetails(existing, script);
	const needsMore =
		!normalized ||
		normalized.clipCandidates.length < SHORTS_MIN_CANDIDATES ||
		normalized.titleCandidates.length < 3;
	if (!needsMore) return normalized;
	const modelDetails = await generateShortsDetailsWithModel({
		jobId,
		script,
		topics,
	});
	if (!modelDetails) return normalized;
	return normalizeShortsDetails(modelDetails, script);
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

function formatAgendaList(items = []) {
	const list = (items || []).filter(Boolean).slice(0, 4);
	if (!list.length) return "";
	if (list.length === 1) return list[0];
	if (list.length === 2) return `${list[0]} and ${list[1]}`;
	if (list.length === 3) return `${list[0]}, ${list[1]}, and ${list[2]}`;
	return `${list[0]}, ${list[1]}, ${list[2]}, and ${list[3]}`;
}

function inferIntroAgendaProfile({ topics = [], shortTitle = "" } = {}) {
	const labels = buildIntroTopicLabels(topics, 6);
	const text = `${shortTitle || ""} ${labels.join(" ")}`.toLowerCase();
	if (isDigitalWellbeingTopic({ topics, text: shortTitle })) {
		return {
			beats: [
				"why the phone feels restful but keeps stimulation high",
				"where attention gets taxed before you notice",
				"the small boundaries that make quiet feel available again",
			],
			cardSubtitle: "Why It Drains You and What Helps",
		};
	}
	if (isSocialConnectionTopic({ topics, text: shortTitle })) {
		return {
			beats: [
				"why it feels personal",
				"what changed in adult routines",
				"the small moves that rebuild closeness",
			],
			cardSubtitle: "Why It Feels Hard and What Helps",
		};
	}
	if (/\b(divorce|split|breakup|separation|custody)\b/.test(text)) {
		return {
			beats: [
				"how this unfolded",
				"when things really started turning",
				"what the strongest reporting actually supports",
				"the detail that could change where this goes next",
			],
			cardSubtitle: "Timeline, Fallout, What's Next",
		};
	}
	if (
		/\b(backlash|controversy|scandal|lawsuit|court|trial|arrest|investigation|feud|accusation|claim)\b/.test(
			text,
		)
	) {
		return {
			beats: [
				"what set this off",
				"why people are so split on it",
				"what the strongest reporting actually supports",
				"the detail that could change where this goes next",
			],
			cardSubtitle: "What Set It Off and What's Next",
		};
	}
	if (
		/\b(election|vote|president|prime minister|senator|congress|parliament|campaign|governor|white house|supreme court|iran|israel|hezbollah|hamas|gaza|ukraine|russia|china|taiwan|middle east|peace proposal|peace talks|ceasefire|diplomacy|diplomatic|sanctions|foreign minister|state department|united nations)\b/.test(
			text,
		)
	) {
		return {
			beats: [
				"what the reporting confirms",
				"what remains unconfirmed or still moving",
				"why the timing matters",
				"what could change next",
			],
			cardSubtitle: "What Changed and What's Next",
		};
	}
	if (
		/\b(movie|film|show|series|episode|season|album|song|tour|cast|trailer|awards?)\b/.test(
			text,
		)
	) {
		return {
			beats: [
				"what changed",
				"why viewers are split on it",
				"what the latest reporting actually supports",
				"what to watch next",
			],
			cardSubtitle: "What Changed and What's Next",
		};
	}
	return {
		beats: [
			"what happened",
			"why people are reacting so strongly",
			"what the key reporting actually supports",
			"what could happen next",
		],
		cardSubtitle: "Why It Matters and What's Next",
	};
}

function pickIntroLead({ sensitive = false, topicCount = 1, jobId }) {
	const seed = jobId ? seedFromJobId(jobId) : 0;
	const casualSingle = [
		"Start with this:",
		"Here is the part that matters:",
		"The interesting question is this:",
	];
	const calmSingle = [
		"Look closely at this:",
		"The quiet part is this:",
	];
	const casualMulti = [
		"Start with these patterns:",
		"Here is what connects the story:",
	];
	const calmMulti = [
		"Look closely at these patterns:",
	];
	const pool =
		topicCount <= 1
			? sensitive
				? calmSingle
				: casualSingle
			: sensitive
				? calmMulti
				: casualMulti;
	return (
		pool[seed % pool.length] ||
		"Start with this:"
	);
}

function buildIntroCardSubtitle({ topics = [], shortTitle = "" } = {}) {
	const profile = inferIntroAgendaProfile({ topics, shortTitle });
	return formatHumanTitle(profile.cardSubtitle || "", 72);
}

function buildIntroCardTitle({ title = "", shortTitle = "" } = {}) {
	return formatHumanTitle(shortTitle || title || "Quick Update", 64);
}

function resolveIntroGreetingLabel({ topics = [], shortTitle = "" } = {}) {
	const topicLabels = buildIntroTopicLabels(topics, 7);
	const rawLabel =
		cleanTopicLabel(topicLabels[0] || "") ||
		cleanTopicLabel(shortTitle || "") ||
		"today's topic";
	const deathLike = /\b(dies?|dead|death|passed\s+away|sudden\s+passing|killed|fatal|illness)\b/i.test(
		`${rawLabel} ${shortTitle || ""}`,
	);
	const personLead =
		deathLike && rawLabel.includes(",")
			? cleanTopicLabel(rawLabel.split(",")[0])
			: "";
	const label = personLead || rawLabel;
	return formatHumanTitle(shortTopicLabel(label, 8), 90) || "today's topic";
}

function capitalizeSentenceStart(text = "") {
	return String(text || "").replace(/^(\s*)([a-z])/, (_m, lead, char) => {
		return `${lead}${char.toUpperCase()}`;
	});
}

function stripIntroTopicRestatement(line = "", { topics = [], shortTitle = "" } = {}) {
	let text = sanitizeIntroOutroLine(line) || String(line || "").trim();
	if (!text) return "";
	const candidates = uniqueStrings(
		[
			...buildIntroTopicLabels(topics, 8),
			...buildIntroTopicLabels(topics, 5),
			shortTitle,
		]
			.map((item) => cleanTopicLabel(item))
			.filter(Boolean),
		{ limit: 8 },
	);
	const leadMatch = text.match(/^([^.!?;:]{3,140})\s*[:;-]\s+(.+)$/);
	if (!leadMatch) return capitalizeSentenceStart(text);
	const leadKey = normalizeOpeningForCompare(leadMatch[1]);
	const rest = sanitizeIntroOutroLine(leadMatch[2]);
	if (!leadKey || countWords(rest) < 5) return capitalizeSentenceStart(text);
	const repeatsTopic = candidates.some((candidate) => {
		const candidateKey = normalizeOpeningForCompare(candidate);
		if (!candidateKey) return false;
		return (
			leadKey === candidateKey ||
			leadKey.startsWith(candidateKey) ||
			candidateKey.startsWith(leadKey) ||
			overlapRatio(tokenizeQaText(leadKey), tokenizeQaText(candidateKey)) >= 0.72
		);
	});
	return repeatsTopic ? capitalizeSentenceStart(rest) : capitalizeSentenceStart(text);
}

function addNaturalIntroGreeting({
	line = "",
	topics = [],
	shortTitle = "",
	mood = "neutral",
} = {}) {
	let text = stripIntroTopicRestatement(line, { topics, shortTitle });
	if (!text) return "";
	if (/^\s*(hi|hey|hello)\b/i.test(text)) return text;
	const greetingLabel = resolveIntroGreetingLabel({ topics, shortTitle });
	const hay = `${greetingLabel} ${shortTitle || ""} ${mood || ""} ${
		(Array.isArray(topics) ? topics : [])
			.map((topic) =>
				[
					topic?.displayTopic,
					topic?.topic,
					topic?.promptText,
					topic?.promptBrief?.tone,
				]
					.filter(Boolean)
					.join(" "),
			)
			.join(" ")
	}`.trim();
	const serious = isSensitiveTopicText(hay);
	const greeting = serious ? "Hi everyone" : "Hi guys";
	return sanitizeIntroOutroLine(`${greeting}. ${text}`);
}

function buildIntroLine({ topics = [], shortTitle, mood = "neutral", jobId }) {
	void jobId;
	const requestedOpening = primaryPromptOpeningText(topics);
	if (requestedOpening)
		return addNaturalIntroGreeting({
			line: requestedOpening,
			topics,
			shortTitle,
			mood,
		});
	const directTopic = (Array.isArray(topics) ? topics : []).find((topic) =>
		isDirectAnswerTopic(topic),
	);
	if (directTopic) {
		const answerLead = directAnswerOpeningSentence(directTopic);
		if (answerLead)
			return addNaturalIntroGreeting({
				line: answerLead,
				topics,
				shortTitle,
				mood,
			});
		return addNaturalIntroGreeting({
			line: "Here is the answer first, then the part that makes it worth understanding.",
			topics,
			shortTitle,
			mood,
		});
	}
	if (
		isDigitalWellbeingTopic({
			topics,
			text: `${shortTitle || ""} ${mood || ""}`,
		})
	) {
		return addNaturalIntroGreeting({
			line: "Phone peace sounds simple, until the break starts draining you. The real question is where your attention is leaking.",
			topics,
			shortTitle,
			mood,
		});
	}
	if (
		isSocialConnectionTopic({
			topics,
			text: `${shortTitle || ""} ${mood || ""}`,
		})
	) {
		return addNaturalIntroGreeting({
			line: "Friendship feels louder than ever, but somehow less close. That gap is the part worth paying attention to.",
			topics,
			shortTitle,
			mood,
		});
	}
	if (
		isPersonalFinanceCostOfLivingTopic({
			topics,
			text: `${shortTitle || ""} ${mood || ""}`,
		})
	) {
		return addNaturalIntroGreeting({
			line: "Payday should feel like relief, but sometimes it disappears immediately. The hidden math behind that squeeze changes the whole story.",
			topics,
			shortTitle,
			mood,
		});
	}
	const fallbackLabel = shortTopicLabel(shortTitle || "today's topic", 6);
	const topicLabels = buildIntroTopicLabels(topics, 6);
	const safeLabels = (topicLabels.length ? topicLabels : [fallbackLabel]).map(
		(label) => formatHumanTitle(label, 72) || cleanTopicLabel(label),
	);
	const topicList = formatLabelList(safeLabels) || fallbackLabel;
	const sensitive = isSensitiveTopicText(
		`${shortTitle || ""} ${safeLabels.join(" ")} ${mood || ""}`,
	);
	const profile = inferIntroAgendaProfile({ topics, shortTitle });
	const agenda = formatAgendaList((profile.beats || []).slice(0, 2));
	const primaryLabel = safeLabels[0] || topicList || fallbackLabel;
	const hay = `${primaryLabel} ${topicList}`.toLowerCase();
	if (/\b(court|lawsuit|legal|claims?|liability|case)\b/i.test(hay)) {
		return addNaturalIntroGreeting({
			line: "A public scare can fade quickly, but legal pressure is where the story starts getting harder to ignore.",
			topics,
			shortTitle,
			mood,
		});
	}
	if (/\b(chemical|leak|hazmat|exposure|evacuation|public safety|safety)\b/i.test(hay)) {
		return addNaturalIntroGreeting({
			line: "The first alert gets attention fast, but the real story is what people can prove afterward.",
			topics,
			shortTitle,
			mood,
		});
	}
	if (/\b(supply|supplier|factory|production|shipment|orders?|contracts?)\b/i.test(hay)) {
		return addNaturalIntroGreeting({
			line: "A local disruption can expose a much bigger business risk when customers depend on every step working.",
			topics,
			shortTitle,
			mood,
		});
	}
	if (/\b(fail|failure|mistake|secretly watch)\b/i.test(hay)) {
		return addNaturalIntroGreeting({
			line: "Failure videos get attention for a reason most of us would rather not admit. Sometimes we watch to learn; sometimes we watch to feel safe.",
			topics,
			shortTitle,
			mood,
		});
	}
	if (/\b(happy|happier|happiness|money|spending)\b/i.test(hay)) {
		return addNaturalIntroGreeting({
			line: "Happiness gets sold like something to buy. But some of the most useful parts of a better day still cost nothing.",
			topics,
			shortTitle,
			mood,
		});
	}
	if (/\b(lonely|loneliness|friendship|connection)\b/i.test(hay)) {
		return addNaturalIntroGreeting({
			line: "Connection looks easier than ever, but feeling close is a different problem. The quiet gap is what matters.",
			topics,
			shortTitle,
			mood,
		});
	}
	let teaser = sensitive
		? "the facts matter, but the pattern behind them matters too"
		: "the obvious answer is only the first layer";
	if (/\b(fail|failure|mistake|secretly watch)\b/i.test(hay)) {
		teaser =
			"failure videos pull us in for reasons most people do not want to admit";
	} else if (/\b(happy|happier|happiness|money|spending)\b/i.test(hay)) {
		teaser =
			"happiness gets sold like a product, but the useful part starts before you buy anything";
	} else if (/\b(lonely|loneliness|friendship|connection)\b/i.test(hay)) {
		teaser =
			"connection looks easier than ever, but feeling close is a different problem";
	}
	const payoff = agenda ? `Watch for ${agenda}.` : "The pattern changes how it looks.";
	const line = `${primaryLabel}: ${teaser}. ${payoff}`;
	return addNaturalIntroGreeting({ line, topics, shortTitle, mood });
}

function isSportsLikeTopicLabel(text = "") {
	const t = String(text || "").toLowerCase();
	return (
		/\bvs\.?\b|\bversus\b/.test(t) ||
		/\b(game|matchup|recap|highlights?|takeaways?|postgame|halftime|final four|sweet sixteen|elite eight|playoffs?|tournament)\b/.test(
			t,
		) ||
		/\b(nfl|nba|mlb|nhl|ncaa|ncaa tournament|college basketball|college football|march madness)\b/.test(
			t,
		)
	);
}

function buildTopicEngagementQuestionForLabel(
	topicLabel,
	mood = "neutral",
	{ compact = false, shortTitle = "" } = {},
) {
	const label = selectEngagementLabel({
		topicLabel,
		shortTitle,
		maxWords: compact ? 5 : 5,
	});
	const fallback = "What detail still feels unresolved?";
	if (!label) return fallback;
	if (isSportsLikeTopicLabel(label)) {
		return compact
			? `What was the turning point in ${label}?`
			: `What was the turning point for you in ${label}?`;
	}
	if (compact)
		return mood === "serious"
			? `Which detail about ${label} still feels unresolved?`
			: `What detail about ${label} still feels unresolved?`;
	if (mood === "serious")
		return `Which detail about ${label} still feels unresolved to you?`;
	return `What detail about ${label} still feels unresolved to you?`;
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

	if (compact) return "Which topic still feels unresolved to you?";

	const list = formatTopicList(labels);
	return `Which of these topics still feels unresolved to you: ${list}?`;
}

function buildOutroEngagementQuestionForLabel(topicLabel = "", mood = "neutral") {
	const label = selectEngagementLabel({
		topicLabel,
		maxWords: 5,
	});
	const hay = `${topicLabel || ""} ${label || ""}`.toLowerCase();
	if (/\bscam|fraud|phishing|fake|identity theft|con\b/.test(hay)) {
		return "what red flag would make you pause first?";
	}
	if (/\beconom|inflation|rent|grocery|groceries|prices|debt|wages?|cost of living|utility|utilities\b/.test(hay)) {
		return "which cost is hitting people hardest right now?";
	}
	if (/\bwatch|collab|release|price|product|brand|drop\b/.test(hay)) {
		return "does this make the idea feel more exciting or less special?";
	}
	if (isSportsLikeTopicLabel(hay)) {
		return "what was the real turning point?";
	}
	if (mood === "serious") {
		return "which detail still feels unresolved to you?";
	}
	return "what detail still feels unresolved to you?";
}

function buildOutroEngagementQuestion({
	topics = [],
	shortTitle = "",
	mood = "neutral",
} = {}) {
	const topicLabels = (topics || [])
		.map((t) => shortTopicLabel(t?.displayTopic || t?.topic || t, 5))
		.filter(Boolean);
	const label =
		topicLabels[0] ||
		(shortTitle ? shortTopicLabel(shortTitle, 5) : "") ||
		"this story";
	return buildOutroEngagementQuestionForLabel(label, mood);
}

function buildOutroLine({
	topics = [],
	shortTitle,
	mood = "neutral",
	includeQuestion = true,
	promptOutroText = "",
}) {
	void includeQuestion;
	const requestedOutro = normalizePromptOutroLine(
		promptOutroText || primaryPromptOutroText(topics),
	);
	if (requestedOutro) return sanitizeIntroOutroLine(requestedOutro);
	const question = buildOutroEngagementQuestion({ topics, shortTitle, mood });
	let line = `If this helped, please like and subscribe, and tell me: ${question}`;
	if (countWords(line) > 20) {
		line = `Please like and subscribe, and tell me: ${question}`;
	}
	if (countWords(line) > 18) {
		line = `Please like, subscribe, and tell me: ${question}`;
	}
	return sanitizeIntroOutroLine(line);
}

function resolveYoutubeCategoryLabelForPrompt({
	categoryLabel = "",
	topics = [],
	script = {},
} = {}) {
	const label = String(categoryLabel || "").trim();
	if (!label) return label;
	const text = [
		label,
		script?.title || "",
		script?.shortTitle || "",
		...(Array.isArray(topics)
			? topics.map((t) =>
					[
						t?.displayTopic,
						t?.topic,
						t?.angle,
						...(Array.isArray(t?.keywords) ? t.keywords : []),
						...(Array.isArray(t?.promptBrief?.briefLines)
							? t.promptBrief.briefLines
							: []),
					]
						.filter(Boolean)
						.join(" "),
				)
			: []),
		...(Array.isArray(script?.segments)
			? script.segments.map((s) => s?.text || "")
			: []),
	]
		.join(" ")
		.toLowerCase();
	if (
		label === "Finance" &&
		/\b(broke|paycheck|budget|rent|grocer(?:y|ies)|bills?|subscriptions?|spending|cost of living|personal finance|track one week|cancel leaks|delay emotional purchases)\b/.test(
			text,
		)
	) {
		return "Education";
	}
	if (isSocialConnectionTopic({ topics, categoryLabel: label, text })) {
		return "SocialIssues";
	}
	return label;
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
			? "That is the safest reading for now."
			: "That is the cleanest read for now.";
	const base = String(text || "").trim();
	if (!base) return closer;
	const needsPunct = /[.!?]["')\]]?$/.test(base) ? "" : ".";
	return `${base}${needsPunct} ${closer}`.trim();
}

function enforceCtaQuestion(text = "", mood = "neutral") {
	let t = String(text || "").trim();
	if (!t) t = "Quick final thought.";

	const hasSubscribe = /subscribe/i.test(t);
	const hasQuestionMark = /\?/.test(t);
	const subscribeStatement =
		mood === "serious" ? "Subscribe for updates." : "Subscribe for more.";
	const commentQuestion = "What detail still feels unresolved to you?";
	const combinedCta =
		mood === "serious"
			? "What detail still feels unresolved to you, and will you subscribe for updates?"
			: "What detail still feels unresolved to you, and will you subscribe for more?";

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

function sentenceLooksLikeContentCta(sentence = "") {
	const t = String(sentence || "").toLowerCase();
	if (!t) return false;
	const hasDirectCta =
		/\b(like|subscribe|comment|drop\s+a\s+comment|tap\s+like)\b/i.test(t);
	const hasTellMeCta =
		/\b(tell\s+me|let\s+me\s+know)\b.{0,60}\b(what|which|if|whether|your|you)\b/i.test(
			t,
		);
	const hasEngagementQuestion =
		/\b(what\s+(?:app|detail|part|habit|cost|moment|do\s+you\s+think)|which\s+(?:detail|app|cost|part)|does\s+this\s+make|would\s+you)\b/i.test(
			t,
		);
	return hasDirectCta || hasTellMeCta || (hasEngagementQuestion && /\?/.test(t));
}

function stripSeparateOutroCtaSentences(text = "") {
	const sentences = splitSentences(text);
	if (!sentences.length) return sanitizeSegmentText(text);
	const kept = sentences.filter((sentence) => !sentenceLooksLikeContentCta(sentence));
	return sanitizeSegmentText(kept.join(" "));
}

function buildSeparateOutroContentLanding({
	topics = [],
	categoryLabel = "",
	mood = "neutral",
} = {}) {
	const topicLabel = cleanTopicLabel(
		topics?.[0]?.displayTopic || topics?.[0]?.topic || "",
	);
	if (isDigitalWellbeingTopic({ topics, categoryLabel, text: topicLabel })) {
		return "The goal is not a perfect phone habit. It is protecting enough quiet for your mind to settle again.";
	}
	if (isSocialConnectionTopic({ topics, categoryLabel, text: topicLabel })) {
		return "The goal is not a perfect social life. It is one honest opening where connection can start to feel possible again.";
	}
	if (isPersonalFinanceCostOfLivingTopic({ topics, categoryLabel, text: topicLabel })) {
		return "The goal is not perfect discipline. It is seeing the pattern clearly enough to protect the next decision.";
	}
	if (mood === "serious") {
		return "For now, the cleanest read is to separate what is confirmed from what still needs time.";
	}
	return "That is the part worth watching next: whether the pattern keeps repeating, or finally starts to change.";
}

function removeContentCtasForSeparateOutro({
	script = {},
	topics = [],
	wordCaps = [],
	categoryLabel = "",
	mood = "neutral",
	outroText = "",
} = {}) {
	if (!script || !Array.isArray(script.segments) || !script.segments.length)
		return script;
	const outroKey = normalizeQaText(outroText);
	const segments = script.segments.map((seg, idx, arr) => {
		const original = sanitizeSegmentText(seg?.text || "");
		if (!original) return { ...seg, text: original };
		const originalKey = normalizeQaText(original);
		const isLast = idx === arr.length - 1;
		const sameAsOutro =
			Boolean(outroKey && originalKey) &&
			(originalKey === outroKey ||
				originalKey.includes(outroKey) ||
				outroKey.includes(originalKey));
		const stripped = stripSeparateOutroCtaSentences(original);
		const mostlyCta =
			sentenceLooksLikeContentCta(original) &&
			(countWords(stripped) < 6 || countWords(stripped) < countWords(original) / 2);
		let text = stripped;
		if (sameAsOutro || mostlyCta || (isLast && countWords(text) < 6)) {
			text = buildSeparateOutroContentLanding({
				topics,
				categoryLabel,
				mood,
			});
		}
		const cap =
			Number(wordCaps[seg.index] || wordCaps[idx] || 0) ||
			Math.max(18, countWords(text) + 2);
		return {
			...seg,
			text: sanitizeSegmentText(trimSegmentToCap(text, cap + 4)),
		};
	});
	return { ...script, segments };
}

function enforceSegmentCompleteness(
	segments = [],
	mood = "neutral",
	{ includeCta = true } = {},
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
				topics[topicIndex]?.displayTopic || topics[topicIndex]?.topic || "",
			).trim();
		let text = String(seg.text || "").trim();

		if (i === 0) {
			text = dropIntroTransitionSentence(text);
		} else if (topicIndex !== lastTopicIndex && topicLabel) {
			const lower = text.toLowerCase();
			const topicLower = topicLabel.toLowerCase();
			const hasTransition =
				/^(and now|next up|now|switching gears|turning to|moving on|pivoting)/i.test(
					text,
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
	{ anchor = "", allowInstallmentNumbers = false } = {},
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
				cleanedAnchor,
			)
			.replace(
				new RegExp(`\\b${escaped}\\s+episode\\s+\\d+\\b`, "gi"),
				cleanedAnchor,
			)
			.replace(
				new RegExp(`\\b${escaped}\\s+season\\s+\\d+\\b`, "gi"),
				cleanedAnchor,
			);
		if (!/\d/.test(cleanedAnchor)) {
			updated = updated.replace(
				new RegExp(`\\b${escaped}\\s+\\d+\\b`, "gi"),
				cleanedAnchor,
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
				"gi",
			),
			cleanedAnchor,
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
	topicIntents = [],
) {
	const topicMeta = new Map();
	for (let i = 0; i < (topics || []).length; i++) {
		const label = String(
			topics[i]?.displayTopic || topics[i]?.topic || "",
		).trim();
		const contextItems = Array.isArray(topicContexts?.[i]?.context)
			? topicContexts[i].context
			: [];
		const contextText = contextItems
			.map((c) =>
				typeof c === "string" ? c : `${c.title || ""} ${c.snippet || ""}`,
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
	wordCapsByIndex = [],
	opts = {},
) {
	const skipFinalTopicQuestion = Boolean(opts?.skipFinalTopicQuestion);
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
		if (skipFinalTopicQuestion) return seg;

		const text = String(seg.text || "").trim();
		if (/\?/.test(text)) return seg;

		const topicLabel =
			String(seg.topicLabel || "").trim() ||
			String(
				topics[topicIndex]?.displayTopic || topics[topicIndex]?.topic || "",
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
			`${baseText ? `${baseText}. ` : ""}${question}`.trim(),
		);
		return { ...seg, text: combined };
	});
}

function isSportsCategoryLabel(categoryLabel = "", topics = []) {
	const normalized = normalizeCategoryLabel(categoryLabel).toLowerCase();
	if (normalized === "sports") return true;
	return Array.isArray(topics)
		? topics.some((topic) =>
				isSportsLikeTopicLabel(
					topic?.displayTopic || topic?.topic || String(topic || ""),
				),
			)
		: false;
}

function isPoliticsCategoryLabel(categoryLabel = "", topics = []) {
	const normalized = normalizeCategoryLabel(categoryLabel).toLowerCase();
	if (/\b(politics|political|world news|news|government)\b/.test(normalized))
		return true;
	return Array.isArray(topics)
		? topics.some((topic) => {
				const label = String(
					topic?.displayTopic || topic?.topic || topic || "",
				).toLowerCase();
				return /\b(election|vote|president|prime minister|senator|congress|parliament|campaign|governor|white house|supreme court|iran|israel|hezbollah|hamas|gaza|ukraine|russia|china|taiwan|middle east|peace proposal|peace talks|ceasefire|diplomacy|diplomatic|sanctions|foreign minister|state department|united nations)\b/.test(
					label,
				);
			})
		: false;
}

function isHealthCategoryLabel(categoryLabel = "", topics = []) {
	const normalized = normalizeCategoryLabel(categoryLabel).toLowerCase();
	if (/\b(health|public health)\b/.test(normalized)) return true;
	return Array.isArray(topics)
		? topics.some((topic) => {
				const label = [
					topic?.displayTopic,
					topic?.topic,
					topic?.angle,
					...(Array.isArray(topic?.keywords) ? topic.keywords : []),
				]
					.filter(Boolean)
					.join(" ")
					.toLowerCase();
				return (
					/\b(public\s+health|world\s+health\s+organization|health|disease|outbreak|infection|infected|illness|hospital|symptoms?|transmission|pandemic|epidemic|vaccine|cdc|case\s+counts?|contact\s+tracing|mortality|treatment)\b/.test(
						label,
					) || /\b[a-z0-9-]*virus\b/.test(label)
				);
			})
		: false;
}

function buildCategoryScriptGuide(categoryLabel = "", topics = []) {
	const isSports = isSportsCategoryLabel(categoryLabel, topics);
	const isPolitics = isPoliticsCategoryLabel(categoryLabel, topics);
	const isHealth = isHealthCategoryLabel(categoryLabel, topics);
	const topicText = (Array.isArray(topics) ? topics : [])
		.map((topic) =>
			[
				topic?.displayTopic,
				topic?.topic,
				topic?.angle,
				...(Array.isArray(topic?.keywords) ? topic.keywords : []),
			]
				.filter(Boolean)
				.join(" "),
		)
		.join(" ");
	const isDigitalWellbeing = isDigitalWellbeingTopic({
		topics,
		categoryLabel,
		text: topicText,
	});
	const isSocial = isSocialConnectionTopic({ topics, categoryLabel });
	const isSensitiveSportsStory = isSports && isSensitiveTopicText(topicText);
	if (isPolitics) {
		return {
			isSports: false,
			isPolitics: true,
			isHealth: false,
			isSocial: false,
			isSerious: true,
			lines: [
				"- For politics, diplomacy, war, courts, or public safety, keep the voice measured and specific. No jokes, hype, or overly casual creator filler.",
				'- Avoid casual pivots like "real quick" and "here\'s the thing"; use precise transitions such as "the key question", "the pressure point", or "the unresolved part".',
				"- Attribute sensitive claims close to the claim, and separate confirmed reporting from interpretation.",
				"- Focus on the concrete stakes: timeline, people, institutions, negotiations, conflict risk, and what changes next.",
				"- Write for spoken delivery first. Translate keyword-style phrases into natural sentences a presenter would actually say.",
			],
		};
	}

	if (isHealth) {
		return {
			isSports: false,
			isPolitics: false,
			isHealth: true,
			isSocial: false,
			isSerious: true,
			lines: [
				"- For health, disease, outbreak, or public-safety topics, keep the voice calm, precise, and useful. No jokes, hype, or creator filler.",
				'- Avoid casual pivots like "real quick", "here\'s the thing", and "that\'s wild"; use measured transitions such as "the key distinction", "the public-health question", or "the unresolved test".',
				"- Attribute medical or public-health claims close to the claim, and separate confirmed reporting from uncertainty, monitoring, and interpretation.",
				"- Emphasize what is known, what is not known, and what viewers should watch next without implying panic or minimizing legitimate risk.",
				"- Write for spoken delivery first. Translate keyword-style phrases into natural sentences a presenter would actually say.",
			],
		};
	}

	if (isDigitalWellbeing) {
		return {
			isSports: false,
			isPolitics: false,
			isHealth: false,
			isSocial: true,
			isWellbeing: true,
			isSerious: false,
			lines: [
				"- For digital wellbeing, phone habits, screen time, attention, rest, or sleep topics, write with practical empathy. Make viewers feel understood, not judged.",
				"- Do not frame the topic like breaking news. Avoid generic phrases such as \"what happened\", \"why people are reacting\", \"key reporting\", or \"the headline\" unless there is an actual current event.",
				"- Use concrete everyday scenes: waking up, waiting in line, bedtime scrolling, lock-screen alerts, and transition moments that get filled by the phone.",
				"- Keep the tone adult and direct. Avoid schoolteacher reassurance, therapy-talk loops, or explaining obvious feelings too slowly.",
				"- Surface the uncomfortable but fair tradeoff: phones make life easier, but constant alerts and feeds can train attention away from quiet, sleep, and sustained thought.",
				"- Keep advice realistic and testable. Prefer small boundaries, experiments, and environmental changes over sweeping digital-detox promises.",
				"- Do not invent named studies, journals, researchers, or reporting. If source links are not provided, keep claims high-level and practical.",
			],
		};
	}

	if (isSocial) {
		return {
			isSports: false,
			isPolitics: false,
			isHealth: false,
			isSocial: true,
			isSerious: false,
			lines: [
				"- For social connection, friendship, loneliness, dating, family, or relationship topics, write with warmth and practical empathy. Make viewers feel seen, not diagnosed.",
				"- Do not frame the topic like breaking news. Avoid generic phrases such as \"what happened\", \"why people are reacting\", \"key reporting\", or \"the headline\" unless there is an actual current event.",
				"- Use concrete everyday scenes: unanswered texts, busy weekends, moving cities, remote work, awkward invitations, recurring plans, and small acts of reaching out.",
				"- Keep advice realistic and low-pressure. Prefer one small next action over sweeping life advice.",
				"- Let the opening feel intimate and human: a real contradiction, a familiar feeling, then a reason to keep watching.",
			],
		};
	}

	if (!isSports) {
		return {
			isSports: false,
			isPolitics: false,
			isHealth: false,
			isSocial: false,
			isSerious: false,
			lines: [
				"- Write for spoken delivery first. Translate keyword-style phrases into natural sentences a presenter would actually say.",
				"- When source context exists, attribute the reporting source, analyst, or outlet itself. Do not treat a platform host like YouTube, TikTok, Reddit, Instagram, or X as the authority.",
				"- When source context is missing, do not invent named outlets, journals, studies, researchers, or reporting. Keep claims high-level and clearly framed as analysis or practical advice.",
				"- Avoid search-query phrasing unless it is essential to the hook. After the opening, stay in story mode, not search-results mode.",
				"- Avoid symbol-heavy wording that sounds awkward in voiceover. Prefer naturally spoken phrasing over shorthand.",
			],
		};
	}

	return {
		isSports: true,
		isPolitics: false,
		isHealth: false,
		isSocial: false,
		isSerious: false,
		lines: [
			"- For sports topics, write like a sharp postgame or pregame breakdown, not a search-trends explainer.",
			...(isSensitiveSportsStory
				? [
						"- If the sports story involves death, injury, discipline, or legal issues, split the human/community impact from the competitive implication, and keep both sourced.",
					]
				: []),
			"- Open on the matchup tension, turning point, or strategic edge. Do NOT open on what people are searching for.",
			"- Use natural sports language for the actual sport. For NASCAR or motorsports, prefer terms like Cup Series, garage, pit road, track position, car control, setup, restart, team, rival, and fan base.",
			'- If a source is a preview, recap, or analyst hit, attribute the analyst and outlet. Say "Jon Rothstein on CBS Sports" rather than "According to YouTube".',
			'- Translate keyword phrases into normal speech. Never say quoted fragments like "where to watch" or "score today" as standalone ideas in the body of the script.',
			"- Mention odds or betting only if they clarify expectations, and state them plainly without sportsbook-promotional framing.",
			"- Focus on what actually changed the game: the matchup edge, the run that flipped it, the coaching adjustment, and what the result means next.",
			'- Keep sports scores and runs easy to read aloud. Prefer phrases like "an eight to nothing run" over symbol-heavy shorthand.',
		],
	};
}

const SCRIPT_SEARCH_META_PATTERNS = [
	/\bwhat(?:'s| is)\s+driving\s+searches\b/i,
	/\bwhat\s+people\s+are\s+searching\s+for\b/i,
	/\bwhere\s+to\s+watch\b/i,
	/\bscore\s+today\b/i,
	/\bwhat\s+people\s+are\s+asking\b/i,
	/\bshot\s+up\s+in\s+search(?:es)?\b/i,
	/\bspiked?\s+in\s+search(?:es)?\b/i,
	/\btrending\s+search(?:es)?\b/i,
	/\b(?:minute|part|section|beat)\s*\d{1,2}\s*:?\s+(?:bls|census|federal|visual|image|query|source)\b/i,
];

const SCRIPT_PLATFORM_ATTRIBUTION_PATTERNS = [
	/\baccording\s+to\s+youtube\b/i,
	/\baccording\s+to\s+tiktok\b/i,
	/\baccording\s+to\s+reddit\b/i,
	/\baccording\s+to\s+instagram\b/i,
	/\baccording\s+to\s+x\.com\b/i,
	/\baccording\s+to\s+x\b/i,
];

const SCRIPT_BETTING_PROMO_PATTERNS = [
	/\bhard\s+rock\s+bet\b/i,
	/\bbetting\s+preview(?:s)?\b/i,
	/\bsportsbook\b/i,
];

const SCRIPT_SPEECH_AWKWARD_PATTERNS = [
	/\bvs\.?\b/i,
	/\b\d{1,3}\s*[\u2013-]\s*\d{1,3}\b/,
	/^\s*[A-Z][A-Za-z'’.-]+(?:\s+[A-Z][A-Za-z'’.-]+){1,7}\s*:/,
	/\b(?:minute|part|section|beat)\s*\d{1,2}\s*:?\s*(?:[A-Z][A-Za-z&-]*\s*){1,5}:\s*/i,
	/^\s*(?:Mr|Mrs|Ms|Dr)\.?\s+(?:That|This|The|It)\b/i,
	/^\s*(?:Mr|Mrs|Ms|Dr)\.?\s*$/i,
	/\bloss\s+circle\b/i,
	/\bthe\s+angle\s+today\s+is\b/i,
];

const SCRIPT_STOCK_PHRASE_PATTERNS = [
	/^\s*quick\s+update[.!]?\s*/i,
	/^\s*memorable\s+line\s*\??\s*/i,
	/^\s*must\s+include\s*(?:this\s+)?(?:line|sentence)?\s*:?\s*/i,
	/\bthat\s+is\s+the\s+turn\b/i,
	/\bthat'?s\s+the\s+takeaway\b/i,
	/\bthat\s+is\s+the\s+part\s+worth\s+watching\s+next\b/i,
	/\bnext\s+detail\s+changes\s+how\b/i,
	/\bthe\s+next\s+detail\s+changes\s+how\b/i,
	/\bthe\s+confirmed\s+picture\s+is\s+still\s+narrow\b/i,
	/\bthe\s+answer\s+depends\s+on\s+the\s+next\s+detail\s+viewers\s+have\s+not\s+seen\s+yet\b/i,
	/\bwhat\s+changes\s+once\s+the\s+next\s+detail\s+lands\b/i,
	/\bthe\s+next\s+useful\s+(?:beat|detail|update)\b/i,
	/\bdetail\s+that\s+changes\s+what\s+viewers\s+should\s+actually\s+think\b/i,
	/\bwhat\s+viewers\s+should\s+actually\s+think\b/i,
	/\bthat\s+keeps\s+the\s+story\s+moving\s+toward\s+evidence\b/i,
	/\bthat\s+gives\s+the\s+story\s+a\s+concrete\s+next\s+step\b/i,
	/\bthe\s+story\s+is\s+bigger\s+than\s+one\s+paycheck\b/i,
	/\bconcrete\s+next\s+step\s+instead\s+of\s+leaving\s+people\s+with\s+guilt\b/i,
	/\bclearer\s+viewer\s+takeaway\b/i,
	/\bviewer\s+takeaway\s+is\s+simple\b/i,
	/\bstrongest\s+takeaway\s+is\s+the\s+small\s+change\b/i,
	/\buseful\s+shift\s+is\s+to\s+make\s+the\s+hidden\s+pressure\s+visible\b/i,
];

const SCRIPT_SERIOUS_CASUAL_PATTERNS = [
	/\breal\s+quick\b/i,
	/\bhere(?:'s| is)\s+the\s+thing\b/i,
	/\bhonestly\b/i,
];

const SCRIPT_INCOMPLETE_SENTENCE_PATTERNS = [
	/\b(?:said|says|reported|reports|suggested|suggests|claimed|claims|announced|warned|added)\s+(?:the\s+)?(?:u\.s\.|us|u\.k\.|uk|u\.n\.|un)\.?$/i,
	/\b(?:a|an|the|earlier|latest|new|proposed|first|second|next)\s+(?:u\.s\.|us|u\.k\.|uk|u\.n\.|un)\.?$/i,
	/\b(?:according\s+to|reported\s+by|because|while|after|before|although|but|and|or)\s*$/i,
	/\b(?:centers\s+on|focuses\s+on|points\s+to|starts\s+with|ends\s+with)\s*$/i,
];

function analyzeRepeatedScriptPhrases(segments = []) {
	const stockPhraseSegments = [];
	const sentenceMap = new Map();
	for (let i = 0; i < (segments || []).length; i++) {
		const seg = segments[i] || {};
		const text = String(seg.text || "");
		if (SCRIPT_STOCK_PHRASE_PATTERNS.some((rx) => rx.test(text))) {
			stockPhraseSegments.push(i);
		}
		for (const sentence of splitSentences(text)) {
			const key = normalizeQaText(sentence);
			if (countWords(key) < 7) continue;
			const list = sentenceMap.get(key) || [];
			list.push(i);
			sentenceMap.set(key, list);
		}
	}

	const repeatedSentenceGroups = [];
	for (const [sentence, indices] of sentenceMap.entries()) {
		const uniqueIndices = Array.from(new Set(indices));
		if (uniqueIndices.length < 2) continue;
		repeatedSentenceGroups.push({ sentence, segments: uniqueIndices });
	}

	return { stockPhraseSegments, repeatedSentenceGroups };
}

function analyzeScriptSpeakability({
	script,
	topics = [],
	categoryLabel = "",
}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	const isSports = isSportsCategoryLabel(categoryLabel, topics);
	const isPolitics = isPoliticsCategoryLabel(categoryLabel, topics);
	const searchMetaSegments = [];
	const platformAttributionSegments = [];
	const bettingPromoSegments = [];
	const speechAwkwardSegments = [];
	const repeatedPhrases = analyzeRepeatedScriptPhrases(segments);

	for (let i = 0; i < segments.length; i++) {
		const seg = segments[i] || {};
		const text = String(seg.text || "").trim();
		if (!text) continue;
		if (SCRIPT_SEARCH_META_PATTERNS.some((rx) => rx.test(text))) {
			searchMetaSegments.push(i);
		}
		if (SCRIPT_PLATFORM_ATTRIBUTION_PATTERNS.some((rx) => rx.test(text))) {
			platformAttributionSegments.push(i);
		}
		if (isSports && SCRIPT_BETTING_PROMO_PATTERNS.some((rx) => rx.test(text))) {
			bettingPromoSegments.push(i);
		}
		if (SCRIPT_SPEECH_AWKWARD_PATTERNS.some((rx) => rx.test(text))) {
			speechAwkwardSegments.push(i);
		} else if (
			SCRIPT_INCOMPLETE_SENTENCE_PATTERNS.some((rx) => rx.test(text))
		) {
			speechAwkwardSegments.push(i);
		} else if (
			isPolitics &&
			SCRIPT_SERIOUS_CASUAL_PATTERNS.some((rx) => rx.test(text))
		) {
			speechAwkwardSegments.push(i);
		}
	}

	const warnings = [];
	if (searchMetaSegments.length) warnings.push("search_meta_phrasing_detected");
	if (platformAttributionSegments.length)
		warnings.push("platform_attribution_detected");
	if (bettingPromoSegments.length) warnings.push("sportsbook_framing_detected");
	if (speechAwkwardSegments.length)
		warnings.push("tts_awkward_symbol_phrasing_detected");
	if (repeatedPhrases.stockPhraseSegments.length)
		warnings.push("stock_transition_phrase_detected");
	if (repeatedPhrases.repeatedSentenceGroups.length)
		warnings.push("repeated_sentence_detected");

	const needsRewrite =
		platformAttributionSegments.length > 0 ||
		searchMetaSegments.length >= (isSports ? 1 : 2) ||
		bettingPromoSegments.length > 0 ||
		speechAwkwardSegments.length > 0 ||
		repeatedPhrases.stockPhraseSegments.length > 0 ||
		repeatedPhrases.repeatedSentenceGroups.length > 0;

	return {
		needsRewrite,
		warnings,
		stats: {
			searchMetaSegments,
			platformAttributionSegments,
			bettingPromoSegments,
			speechAwkwardSegments,
			stockPhraseSegments: repeatedPhrases.stockPhraseSegments,
			repeatedSentenceGroups: repeatedPhrases.repeatedSentenceGroups,
		},
	};
}

function resolveTopListPlan(topics = [], categoryLabel = "") {
	const safeTopics = Array.isArray(topics) ? topics : [];
	const explicit = safeTopics.find((t) => t?.topList?.count)?.topList || null;
	const fromTopic =
		explicit ||
		detectTopListRequest(
			safeTopics
				.map((t) => t?.displayTopic || t?.topic || "")
				.filter(Boolean)
				.join(" "),
		);
	const categoryTop =
		!fromTopic && String(categoryLabel || "").toLowerCase() === "top5"
			? {
					count: 5,
					subject: safeTopics[0]?.displayTopic || safeTopics[0]?.topic || "",
				}
			: null;
	const plan = fromTopic || categoryTop;
	if (!plan?.count) return null;
	const count = Math.max(2, Math.min(10, Math.floor(Number(plan.count) || 0)));
	if (!count) return null;
	return {
		...plan,
		count,
		subject: cleanTopicLabel(plan.subject || safeTopics[0]?.displayTopic || ""),
	};
}

function buildTopListSegmentPlan(segmentCount, topList = null) {
	const total = Math.max(1, Math.floor(Number(segmentCount) || 1));
	const count = Math.max(2, Math.min(10, Math.floor(Number(topList?.count) || 0)));
	if (!count || total < count) return [];
	const base = Math.floor(total / count);
	let remainder = total - base * count;
	let cursor = 0;
	const out = [];
	for (let i = 0; i < count; i++) {
		const rank = count - i;
		const size = base + (remainder > 0 ? 1 : 0);
		remainder = Math.max(0, remainder - 1);
		const startIndex = cursor;
		const endIndex = Math.min(total - 1, cursor + Math.max(1, size) - 1);
		out.push({ rank, startIndex, endIndex });
		cursor = endIndex + 1;
	}
	return out;
}

function rankForTopListSegment(index, topListPlan = []) {
	const idx = Number(index);
	const hit = (topListPlan || []).find(
		(item) => idx >= item.startIndex && idx <= item.endIndex,
	);
	return hit?.rank || null;
}

function stripCountdownPrefix(text = "") {
	return String(text || "")
		.replace(/^\s*#?\s*(?:number\s*)?\d{1,2}\s*[-:.)]\s*/i, "")
		.trim();
}

function parseCountdownLabel(text = "") {
	const raw = String(text || "").trim();
	const match = raw.match(/^\s*#\s*\d{1,2}\s*[-:]\s*([^:.\n-]{2,80})/i);
	if (!match) return "";
	return cleanTopicLabel(match[1]).replace(/\s+$/, "").trim();
}

function enforceTopListCountdownStructure(segments = [], topList = null) {
	if (!topList?.count || !Array.isArray(segments) || !segments.length)
		return segments;
	const plan = buildTopListSegmentPlan(segments.length, topList);
	if (!plan.length) return segments;
	const firstByRank = new Map(plan.map((p) => [p.rank, p.startIndex]));
	return segments.map((seg, idx) => {
		const rank = rankForTopListSegment(idx, plan);
		if (!rank) return seg;
		const isRankStart = firstByRank.get(rank) === idx;
		let text = String(seg?.text || "").trim();
		let countdownLabel = cleanTopicLabel(seg?.countdownLabel || "");
		if (isRankStart) {
			const alreadyCorrect = new RegExp(`^\\s*#\\s*${rank}\\s*[-:]`, "i").test(
				text,
			);
			if (!countdownLabel) countdownLabel = parseCountdownLabel(text);
			if (!alreadyCorrect) {
				const body = stripCountdownPrefix(text);
				text = `#${rank}- ${body}`.trim();
			}
		}
		return {
			...seg,
			text,
			countdownRank: rank,
			countdownLabel,
		};
	});
}

function buildTopListGuideLines(topList = null, segmentCount = 0) {
	if (!topList?.count) return "";
	const plan = buildTopListSegmentPlan(segmentCount, topList);
	if (!plan.length) return "";
	const subject = cleanTopicLabel(topList.subject || "the topic");
	const lines = plan
		.map(
			(item) =>
				`- #${item.rank}: segments ${item.startIndex}-${item.endIndex}; segment ${item.startIndex} must start with "#${item.rank}- " followed by the ranked item name.`,
		)
		.join("\n");
	return `
Countdown structure:
This is a Top ${topList.count} ranked countdown about ${subject}.
Start the CONTENT immediately at #${topList.count}; do not add a separate content intro because the video intro is generated elsewhere.
Use descending order only: #${topList.count} down to #1.
${lines}
- First segment for each rank names one concrete item; following segments for that rank add descriptive facts, tradeoffs, controversy, visuals, or why viewers may disagree.
- #1 should feel like the payoff, with the strongest rationale and a short comment question.
- Do not use a rank prefix on non-start segments for the same item.
- Include countdownRank and countdownLabel in each segment object when possible.
`.trim();
}

function contextItemsToText(items = []) {
	return (Array.isArray(items) ? items : [])
		.map((item) => {
			if (typeof item === "string") return item;
			return [
				item?.title,
				item?.snippet,
				item?.source,
				getUrlHost(item?.link || ""),
			]
				.filter(Boolean)
				.join(" ");
		})
		.filter(Boolean)
		.join(" ");
}

function buildDynamicRetentionGuide({
	topics = [],
	topicContexts = [],
	categoryGuide = {},
	tonePlan = {},
	topListPlan = null,
	contentMode = "trends",
} = {}) {
	const topicText = (Array.isArray(topics) ? topics : [])
		.map((topic, idx) => {
			const label = [
				topic?.displayTopic,
				topic?.topic,
				topic?.angle,
				...(Array.isArray(topic?.keywords) ? topic.keywords : []),
				contextItemsToText(topicContexts?.[idx]?.context || []),
			]
				.filter(Boolean)
				.join(" ");
			return label;
		})
		.join(" ");
	const hay = `${topicText} ${contentMode || ""}`.toLowerCase();
	const mood = String(tonePlan?.mood || "neutral").toLowerCase();
	const isSensitive =
		mood === "serious" ||
		Boolean(categoryGuide?.isPolitics) ||
		Boolean(categoryGuide?.isHealth) ||
		Boolean(categoryGuide?.isSerious) ||
		isSensitiveTopicText(topicText);
	const isSports = Boolean(categoryGuide?.isSports);
	const isEntertainment = isEntertainmentTopicText(topicText);
	const isDigitalWellbeing = isDigitalWellbeingTopic({
		topics,
		text: topicText,
	});
	const isGaming =
		/\b(video\s*game|gaming|gameplay|trailer|demo|console|playstation|xbox|nintendo|steam|rpg|studio|developer|patch|combat|open world)\b/i.test(
			hay,
		);
	const hasSources = Array.isArray(topicContexts)
		? topicContexts.some((tc) =>
				(Array.isArray(tc?.context) ? tc.context : []).some(
					(item) => item && typeof item !== "string" && item.link,
				),
			)
		: false;

	const lines = [
		"- Retention should come from the facts and angle, not manufactured hype. Use a human creator voice: clear, opinion-aware, and curious.",
		"- Make the central tension feel adult and specific: who benefits, who pays the cost, what changes if the claim is true, and what the evidence does not prove yet.",
		"- Be controversial only where the facts or clearly labeled analysis support it. Use strong framing for real tradeoffs, not for unsupported accusations.",
		"- Vary the segment openings. Do not let multiple segments in a row start with the same connective style like \"That matters\", \"Still\", \"So\", or \"And\".",
		"- Every 3-4 segments, add a natural pattern interrupt: a contrast, a viewer-facing question, a consequence, or a sharper read that makes the next beat feel earned.",
		"- Keep the audience-oriented thread alive: why this matters, what changes if it is true, and what viewers are still waiting to see.",
		"- Make viewers feel included by naming the shared fan/viewer question, community reaction, or practical consequence without flattering them or forcing a catchphrase.",
	];

	if (topListPlan) {
		lines.push(
			"- For countdowns, make each rank justify why it belongs there; each lower rank should create anticipation for why the next one outranks it.",
		);
	} else {
		lines.push(
			"- For non-countdown stories, shape the arc as assumption -> complication -> evidence -> creator read -> unresolved test.",
		);
	}

	if (isSensitive) {
		lines.push(
			"- For sensitive, legal, political, conflict, tragedy, or public-safety topics, pattern interrupts must be sober reframes, not jokes or casual bits.",
		);
	} else if (isDigitalWellbeing) {
		lines.push(
			"- For digital wellbeing topics, retention should come from recognizable daily moments, not news framing: morning reach, waiting-line checks, notification tension, bedtime scrolling, and protected quiet.",
			"- Avoid therapy-like over-explaining. Assume an intelligent adult audience and name the uncomfortable tradeoff between convenience, connection, attention, sleep, and calm.",
		);
	} else if (isGaming || isEntertainment) {
		lines.push(
			"- For entertainment, gaming, creator, music, film, TV, and culture topics, include 1-2 grounded creator reads that sound like a real viewer reacting, while keeping sourced facts separate.",
		);
		if (isGaming) {
			lines.push(
				"- For gaming topics, translate reporting into player-facing stakes: trust, gameplay proof, polish, systems, launch risk, community expectations, and whether the footage answers the doubt.",
			);
		}
	} else if (isSports) {
		lines.push(
			"- For sports topics, rotate between turning point, matchup pressure, adjustment, consequence, and what the next test proves.",
		);
	}

	if (hasSources) {
		lines.push(
			"- Use source attribution like a human host would: quick and close to the claim, then immediately explain why the claim matters.",
		);
	}

	return `Dynamic retention and human-feel plan:
${lines.join("\n")}`;
}

function buildLocalFallbackScript({
	jobId,
	topics = [],
	segmentCount = 12,
	wordCaps = [],
	tonePlan = null,
	contentMode = "prompt",
} = {}) {
	const safeTopics =
		Array.isArray(topics) && topics.length
			? topics.filter((t) => t && (t.topic || t.displayTopic))
			: [{ topic: "today's topic" }];
	const topicLabelFor = (t) =>
		String(t?.displayTopic || t?.topic || "today's topic").trim();
	const brief = primaryPromptBrief(safeTopics);
	const title =
		formatHumanTitle(
			String(brief?.title || topicLabelFor(safeTopics[0]) || "Long Video"),
			120,
		) || "Long Video";
	const shortTitle = shortTitleFromText(title).slice(0, 60);
	const ranges = allocateTopicSegments(
		Math.max(1, Math.floor(Number(segmentCount) || 12)),
		safeTopics,
	);
	const mood = tonePlan?.mood || "neutral";
	const isPromptMode = String(contentMode || "").toLowerCase() === "prompt";
	const socialConnection = isSocialConnectionTopic({
		topics: safeTopics,
		text: title,
	});
	const genericBeats = [
		"Start with the central tension, then connect it to what people feel in daily life.",
		"The useful question is not whether the topic matters, but where the pressure shows up first.",
		"A good explanation separates the old promise from the new tradeoff people are actually weighing.",
		"The story gets clearer when the visible example is tied to a real decision, cost, or consequence.",
		"That is why the next detail matters: it shows who benefits, who carries risk, and what is still uncertain.",
		"The practical shift is to stop treating the topic like a slogan and look at the conditions around it.",
		"One concrete example can do more than a long list because viewers can picture the tradeoff immediately.",
		"The strongest point is usually the one that connects the headline to an ordinary choice.",
		"The first answer may sound simple, but the details make the decision harder.",
		"What helps is making the next step specific enough that viewers can test it against their own situation.",
		"The goal is not to force one opinion. It is to show the pattern clearly enough that the choice feels less vague.",
		"That is the useful part: a complicated topic becomes easier when the tradeoffs are visible.",
		"A balanced read starts by naming both the benefit and the cost without pretending either side disappears.",
		"Many people are reacting to real pressure, not just hype, and that context changes the tone.",
		"That makes practical framing more powerful than a dramatic promise.",
		"A clear comparison removes guesswork because it shows what changes under different choices.",
		"The pattern changes when viewers can see the consequence instead of only hearing the claim.",
		"Specific examples help because they carry the first abstract minutes of the explanation.",
		"The strongest version stays fair: name the upside, name the risk, and keep the advice grounded.",
		"People often need a clearer frame before they can decide what the headline means for them.",
		"That means the first impression is not the full test of the issue.",
		"The better test is whether the evidence makes the next choice easier to understand.",
		"Some details will stay uncertain, and that can be information instead of a reason to exaggerate.",
		"Not every angle has to become dramatic to be useful.",
		"Small signals still matter because they show where the bigger pattern may be heading.",
		"The most realistic approach is to compare the payoff, the risk, and the cheaper route before deciding.",
		"That is how the story becomes useful instead of just noisy.",
		"A small check can do what a big opinion cannot do.",
		"Once the next question is obvious, the topic stops depending on vague advice.",
		"The practical move is to lower the confusion before the decision gets expensive.",
		"That gives viewers a way to think clearly without pretending the pressure is easy.",
		"The hopeful ending is not certainty. It is a better way to ask the next question.",
	];
	const friendshipBeats = [
		"More contact tools did not automatically create more closeness. They mostly made it easier to send a message.",
		"Friendship still depends on repeated time, shared context, and the feeling that someone will show up again.",
		"Adult schedules make that harder because work, family, moving, and fatigue interrupt the old routines.",
		"Social media can make everyone look busy and already chosen, even when many people feel quietly disconnected.",
		"That pressure makes simple invitations feel risky, because nobody wants to seem needy or out of place.",
		"The fix is smaller than people think: make friendship easier to repeat, not more dramatic.",
		"Instead of one big plan, try a low-pressure rhythm like coffee after work or a Sunday walk.",
		"Early conversations can feel awkward because the relationship has not earned its shorthand yet.",
		"Consistency is what creates that shorthand. Familiarity turns effort into comfort.",
		"It also helps to name the plan clearly, because vague intentions usually disappear into busy weeks.",
		"Some friendships will not catch, and that is normal. The win is continuing without turning it into a verdict on you.",
		"The people who build community usually are not luckier. They are willing to initiate more than once.",
		"Over time, the repeated invitations become the proof that the relationship is safe.",
		"So the practical answer is not to chase everyone. It is to choose a few people and create a rhythm.",
		"A text that says, want to walk Saturday morning, is easier to answer than we should hang out sometime.",
		"Putting people on the calendar before life gets loud can protect the relationship from disappearing.",
		"Shared activities help because nobody has to carry the whole conversation alone.",
		"A small group can be easier than one-on-one when direct conversation feels too exposed.",
		"The first invitation is not a contract. It is just a signal that you are open.",
		"Following up once is normal, especially when the plan is clear and easy to decline.",
		"Notice who gives energy back, even if they are busy.",
		"Weak ties matter because they create the familiar faces that deeper friendship often grows from.",
		"The goal is a rhythm people can trust, not a perfect social performance.",
		"Friendship feels lighter when the next plan is already obvious.",
		"Try making the invite concrete, kind, and easy to answer.",
		"If someone says no, offer one alternate and then let the pattern reveal itself.",
		"The deeper change is treating connection like care, not proof of your value.",
		"You do not need everyone. You need a few repeated yeses.",
		"Modern life scatters people, so structure has to do some of what proximity used to do.",
		"That structure is not fake. It recreates the conditions friendship used to get for free.",
		"One recurring plan can matter more than five vague intentions.",
		"Awkwardness often means the relationship is new, not that it is doomed.",
		"Instead of asking whether you are interesting enough, ask whether the plan is easy enough.",
		"The clearest repair is boring in the best way: repeat, notice, follow through.",
		"Some people will not meet you halfway, and that tells you where not to spend your energy.",
		"The people who do respond are the ones worth building around.",
		"A real social life usually starts with small predictable contact before it becomes emotionally deep.",
		"The hopeful part is that friendship does not need a dramatic reset. It needs another ordinary chance.",
	];
	const beats = socialConnection ? friendshipBeats : genericBeats;
	const continuationBeats = socialConnection
		? [
				"Another practical test is whether the next invitation removes pressure instead of adding it.",
				"Another useful move is to make the setting simple enough that showing up is the main effort.",
				"Another honest detail is that comfort often appears after the calendar creates repetition.",
				"Another realistic step is to keep the door open without chasing people who never answer.",
			]
		: [
				"Another useful angle is to turn the pattern into one action people can actually repeat.",
				"Another practical test is whether the next step makes the pressure easier to understand.",
				"Another grounded detail is that small routines often change behavior before big motivation arrives.",
				"Another clear move is to make the next choice specific enough that it can be tested.",
			];
	const segments = [];
	for (let i = 0; i < Math.max(1, Number(segmentCount) || 12); i += 1) {
		const range =
			ranges.find((r) => i >= r.startIndex && i <= r.endIndex) || ranges[0];
		const topicIndex = Math.max(0, Number(range?.topicIndex || 0));
		const topicLabel = topicLabelFor(safeTopics[topicIndex] || safeTopics[0]);
		const cap = wordCaps[i] || 24;
		let text =
			beats[i] ||
			continuationBeats[i % continuationBeats.length] ||
			`The practical next step is to make ${topicLabel} easier to act on today.`;
		if (i === 0 && brief?.openingLine) {
			text = combineOpeningLineWithSegment({
				openingLine: brief.openingLine,
				text,
				cap: Math.max(cap, countWords(brief.openingLine) + 10),
			});
		} else {
			text = trimSegmentToCap(text, cap);
		}
		text = sanitizeSegmentText(text);
		const query =
			buildOverlayQueryFallback(text, topicLabel) ||
			sanitizeOverlayQuery(`${topicLabel} people conversation`);
		segments.push({
			index: i,
			topicIndex,
			topicLabel,
			text,
			expression:
				i === 0
					? "neutral"
					: normalizeExpression(
							i % 5 === 0 ? "warm" : i % 3 === 0 ? "thoughtful" : "neutral",
							mood,
						),
			countdownRank: null,
			countdownLabel: "",
			overlayCues: [
				{
					query,
					startPct: 0.25,
					endPct: 0.75,
					position: "topRight",
				},
			],
		});
	}
	const script = {
		title,
		shortTitle,
		segments,
		shortsDetails: null,
		localFallback: true,
	};
	script.shortsDetails = normalizeShortsDetails(null, script);
	logJob(jobId, "local fallback script ready", {
		reason: "openai_quota_or_rate_limit",
		mode: isPromptMode ? "prompt" : "trends",
		title,
		segments: segments.length,
		words: segments.reduce((sum, seg) => sum + countWords(seg.text), 0),
	});
	return script;
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
	categoryLabel = "",
	includeOutro = false,
	contentMode = "trends",
	priorVideoPlan = null,
}) {
	if (!process.env.CHATGPT_API_TOKEN)
		throw new Error("CHATGPT_API_TOKEN missing");

	const safeTopics =
		Array.isArray(topics) && topics.length
			? topics.filter((t) => t && t.topic)
			: [{ topic: "today's topic" }];
	const topicCount = safeTopics.length;
	const topicLabelFor = (t) => String(t?.displayTopic || t?.topic || "").trim();
	const isPromptMode = String(contentMode || "").toLowerCase() === "prompt";
	const categoryGuide = buildCategoryScriptGuide(categoryLabel, safeTopics);
	const topListPlan = resolveTopListPlan(safeTopics, categoryLabel);
	const topListGuide = buildTopListGuideLines(topListPlan, segmentCount);
	const retentionGuide = buildDynamicRetentionGuide({
		topics: safeTopics,
		topicContexts,
		categoryGuide,
		tonePlan,
		topListPlan,
		contentMode,
	});
	const briefLine = isPromptMode
		? topicCount > 1
			? "This is a multi-topic brief based on a user request."
			: "This is a user-requested topic brief."
		: "This is a multi-topic news brief.";
	const trendSignalLabel = isPromptMode
		? "Context signals (use if present; do NOT invent):"
		: categoryGuide.isSports
			? "Search signals (use as audience context for the hook; translate them into natural sports language and do NOT quote them verbatim):"
			: "Trending signals (address the #1 rising reason early if present):";
	const evidenceLine = isPromptMode
		? '- If a topic\'s evidence is "(none)", keep statements high-level and avoid specific claims; frame it as an open question.'
		: "- If a topic's evidence is \"(none)\", keep statements high-level and avoid specific claims; say it's trending and frame it as an open question.";
	const topicRanges = allocateTopicSegments(segmentCount, safeTopics);
	const capsLine = wordCaps.map((c, i) => `#${i}: <= ${c} words`).join(", ");
	const mood = tonePlan?.mood || "neutral";
	const deepDiveGuide =
		topListPlan
			? `Ranked-countdown deep dive: spend the narration budget on ${topListPlan.count} clear picks in descending order, with each pick getting enough context, evidence, and visual detail to feel earned.`
			: topicCount === 1
			? "Single-topic deep dive: spend more time on background, timeline, key evidence, and implications while staying concise and non-repetitive."
			: "";
	const outroGuide = includeOutro
		? topListPlan
			? "Last segment: finish the #1 payoff with a clean takeaway or open loop; leave space for the separate closing question."
			: "Last segment: clean wrap that naturally closes the story and leaves space for the closing line (no like/subscribe CTA)."
		: "Last segment: wrap + CTA question.";
	const toneGuide =
		topListPlan
			? `Segment 0 starts with #${topListPlan.count}- and a ranked item name; the countdown itself is the hook. ${outroGuide}`
			: mood === "serious"
			? `Segment 0: measured news-presenter cadence, clear pauses on confirmed facts. ${outroGuide}`
			: mood === "excited"
				? `Segment 0: confident, neutral hook with controlled energy (no shouty hype). ${outroGuide}`
				: `Segment 0: confident, neutral hook. ${outroGuide}`;
	const ctaLine = includeOutro
		? "Do NOT add like/subscribe or comment CTAs inside the content segments. Let the final content segment land with a clear takeaway, hope line, tension line, or open loop; the separate closing line handles the question."
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
			const rawHints = uniqueStrings(
				[
					...(Array.isArray(t.keywords) ? t.keywords : []),
					...(t.trendStory?.imageSearchQueries || []),
					...(t.trendStory?.searchPhrases || []),
					...(t.trendStory?.entityNames || []),
					...(Array.isArray(t.trendStory?.relatedQueries?.rising)
						? t.trendStory.relatedQueries.rising
						: []),
					...(Array.isArray(t.trendStory?.relatedQueries?.top)
						? t.trendStory.relatedQueries.top
						: []),
				],
				{ limit: 8 },
			);
			const hints = categoryGuide.isSports
				? rawHints.filter(
						(hint) =>
							!SCRIPT_SEARCH_META_PATTERNS.some((rx) =>
								rx.test(String(hint || "")),
							),
					)
				: rawHints;
			const articles = (t.trendStory?.articles || [])
				.map((a) => a.title)
				.filter(Boolean)
				.slice(0, 3);
			const angle = String(t.angle || "").trim();
			const imageHints = Array.isArray(t.imageSearchHints)
				? t.imageSearchHints.slice(0, 5)
				: [];
			return `Topic ${i + 1}: ${topicLabelFor(t) || t.topic}\n- Hints: ${
				hints.length ? hints.join(", ") : "(none)"
			}\n- User angle: ${angle || "(none)"}\n- Image search hints: ${
				imageHints.length ? imageHints.join(", ") : "(none)"
			}\n- Articles: ${articles.length ? articles.join(" | ") : "(none)"}`;
		})
		.join("\n\n");
	const visualResearchLines = safeTopics
		.map((t, i) => {
			const vr = t?.trendStory?.visualResearch || {};
			const titles = Array.isArray(vr.titles) ? vr.titles : [];
			const queries = Array.isArray(vr.queries) ? vr.queries : [];
			const label = topicLabelFor(t) || t.topic || `Topic ${i + 1}`;
			const titleLines = titles
				.slice(0, 12)
				.map((item) => {
					const title = cleanTopicLabel(item?.title || "");
					if (!title) return "";
					const source = item?.source ? ` (${item.source})` : "";
					const query = item?.query ? ` | query: ${item.query}` : "";
					return `- ${title}${source}${query}`;
				})
				.filter(Boolean);
			return `Topic ${i + 1} (${label}):\nQueries already searched: ${
				queries.length ? queries.slice(0, 6).join(" | ") : "(none)"
			}\nAvailable feed-image title clues:\n${
				titleLines.length ? titleLines.join("\n") : "- (none)"
			}`;
		})
		.join("\n\n");
	const visualBeatPlanLines = buildVisualBeatPlanPromptLines(safeTopics);
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
									typeof c === "string" ? "" : getUrlHost(c?.link || ""),
								)
								.filter(Boolean),
							{ limit: 6 },
						);
						return `Topic ${idx + 1} (${tc.topic}): ${
							sources.length ? sources.join(", ") : "(none)"
						}`;
					})
					.join("\n")
			: "- (none)";
	const sourcePolicy = buildTopicSourcePolicyPromptBlock(
		safeTopics,
		topicContexts,
	);
	const titlePromiseGuide = buildTitlePromisePromptBlock({
		topics: safeTopics,
		topicContexts,
	});
	const promptBriefGuide = buildPromptBriefInstructionBlock(safeTopics);
	const directAnswerGuide = buildDirectAnswerPromptBlock(safeTopics, topicContexts);
	const hasDirectAnswerTopic = safeTopics.some((topic) =>
		isDirectAnswerTopic(topic),
	);
	const answerLeadRule = hasDirectAnswerTopic
		? "For direct-answer prompts, answer the user's question in the first spoken content sentence for that topic; do not delay the answer, then build retention with context and stakes."
		: "Lead with the tension or contrast, then add context; delay the clear answer by at least one sentence.";
	const priorVideoGuide = buildPriorVideoScriptGuide(priorVideoPlan);

	const prompt = `
Current date: ${dayjs().format("YYYY-MM-DD")}

Write a YouTube talking-head script for a US audience.
${briefLine}
Language: ${languageLabel}
Category: ${categoryLabel || "General"}
Tone plan: ${mood} (${toneGuide})

Topics in order (do NOT change order):
${safeTopics
	.map((t, i) => `${i + 1}) ${topicLabelFor(t) || t.topic}`)
	.join("\n")}

Segment allocation (follow exactly):
${topicPlanLines}

${topListGuide}

${deepDiveGuide}

Quality-led narration budget (NOT counting intro/outro): ~${narrationTargetSec.toFixed(
		1,
	)}s
Segments: EXACTLY ${segmentCount}
Per-segment word caps: ${capsLine}

Use this background context as hints; do NOT pretend its real-time verified:
${contextLines}

Sources for attribution (use when referencing facts):
${sourceLines}

${sourcePolicy.text}

Topic context guidance:
${topicContextGuide}

Topic notes:
${topicHintLines}

Pre-script feed visual research:
${visualResearchLines}

Validated visual-first beat plan:
${visualBeatPlanLines}

${promptBriefGuide}

${directAnswerGuide}

${priorVideoGuide}

${trendSignalLabel}
${trendSignalLines}

Topic intent resolution (MUST follow; do NOT invent beyond this):
${topicIntentLines}

${titlePromiseGuide}

${retentionGuide}

Style rules (IMPORTANT):
- If Countdown structure is present above, it overrides generic hook rules: segment 0 starts with the highest rank prefix, not a separate intro.
- Keep pacing steady, brisk, and conversational; write for a natural fast presenter cadence without sounding rushed.
- Brisk, coherent American news-presenter delivery; avoid drawn-out phrasing, childish over-explaining, rushed clutter, or choppy sentence fragments.
- Write for spoken delivery, not article copy. Never open a normal segment with a headline-style label followed by a colon; countdown prefixes like "#5- Paris" are allowed when Countdown structure is present.
- Avoid abstract or unnatural phrases a real host would not say out loud, such as "loss circle" or stiff framing like "the angle today is".
- Keep the delivery composed and natural, not shouty. The writing should feel sharp, engaging, and lightly provocative when facts or clearly labeled analysis support it, but never reckless, insulting, or overhyped.
- Aim for intelligent adult viewers: skip obvious moralizing, avoid therapy-talk loops, and compress simple ideas into sharper, more memorable lines.
- Controversial but factual: name the real tradeoff, disagreement, incentive, or cost; then separate what is confirmed, what is inference, and what remains uncertain.
- Sound like a real creator, not a press release. No "Ladies and gentlemen", no "In conclusion", no corporate tone.
- Viewer-first editorial stance: stand with ordinary people affected by the issue. Evaluate companies, institutions, governments, schools, platforms, and authorities by their human impact. Do not shame viewers or blame people for pressure they did not create. For neutral general news or pure entertainment trends, stay fair and factual, but keep the human consequence visible.
- Keep it natural, not forced. For serious politics, diplomacy, legal stories, tragedies, or conflict, avoid casual filler like "real quick" or "here's the thing"; for lighter topics, use at most one friendly pivot per topic.
- For entertainment topics (film, TV, music, awards), add ONE or TWO short reactionary opinions per topic from the presenter (brief clauses only). Keep them grounded, fair, and clearly separate from sourced facts.
- Use contractions. Punchy sentences. A little playful, but not cringe.
- Avoid staccato punctuation. Do NOT put commas between single words.
- Keep punctuation light and flowing; prefer smooth, natural sentences.
- Avoid exclamation points; use calm, steady punctuation.
- Structure each topic as a mini-arc: HOOK -> TENSION -> CONTEXT -> PAYOFF -> OPEN LOOP.
- ${answerLeadRule}
- If the story is controversial, explicitly surface what people are arguing about, what the evidence supports, and where the uncertainty still is.
- Build short curiosity gaps that carry into the next segment; pay them off within 1-2 segments.
- Make at least one segment per topic clip-ready for Shorts: a standalone line that ends with an open loop, not a full resolution.
- Anchor each topic around one clear angle or implication; keep facts in service of that angle.
- Make each segment feel like a mini reveal: include one standout detail, twist, or implication likely new to viewers.
- Prefer curiosity pivots over soft transitions; when moving between points, hint at a consequence or open question instead of just explaining flow.
- Every 2-3 segments, add a brief stakes ratchet: one line that signals what changes if the point is true, without fully resolving it.
- Keep coherence tight: each segment should connect to the previous with a brief bridge or cause-effect line.
- Stay on one story. Do not jump into a different life-advice topic, generic friendship advice, unrelated politics, unrelated culture, or a second topic unless the user explicitly requested multiple topics.
- Use specific nouns (people, places, titles) over vague phrases like "big news" or "fans are excited".
- Keep the opening controlled, but make segment 0 feel like a real hook instead of a bland recap.
- If the frontend prompt includes an Opening line, treat it as the first spoken line of the overall opening unit unless it would be unsafe or factually false. Do not repeat it later; segment 0 should continue the thought.
- If the frontend prompt includes a Title, use it as the script title unless source verification clearly requires a more precise version.
- If the frontend prompt labels a sentence as "memorable line", "must include", or similar, include the sentence naturally. Do not say the label out loud.
- Prioritize genuinely interesting facts (history, timeline, behind-the-scenes, credible rumors, estimates) without overstating.
- If you mention a rumor or estimate from listed source context, label it clearly as unconfirmed and attribute it. Without source links, do not introduce rumors or estimates.
- Use source attribution only for topics marked with source links in the Source policy. Do not invent named outlets, journals, studies, researchers, or "reporting" for topics with no source links.
- Treat the frontend duration as a user preference signal, not a leash. The actual narration budget was chosen for retention and quality; never pad, stretch, slow down, or soften the script just to hit a number.
- If the topic has unusually strong hooks, controversy, history, practical stakes, or visual evidence, earn the extra time with sharper reveals. If the topic has thin material, stay short, punchy, and complete.
- YouTube retention is the priority: every segment must give the viewer a reason to keep watching, through curiosity, useful payoff, fresh context, fair tension, or a memorable line.
- Director rule: the first 8-12 seconds should use one brief natural greeting plus the topic, then immediately become a clean teaser. No host-name introduction, no slow welcome, no filler.
- Emotion should live in the words and pacing, not in stage directions. For happy topics, sound warmly human; for serious topics, sound steady and respectful; never overperform either direction.
- Avoid artificial dramatic pause writing in segments 0-2: no ellipses, no repeated dashes, no isolated one-word fragments, and no line that depends on a long silence to work.
- ElevenLabs delivery should be smooth on the first pass: use natural punctuation, short complete sentences, and only commas where a real presenter would take a tiny breath.
- Avoid repeating the topic question or using vague filler phrasing; be specific and helpful.
- Avoid repeating the headline or the same fact across segments; each segment must add a new detail or angle.
- No redundancy: do not restate the same fact or idea in different words.
- Do not repeat a word or short phrase back-to-back.
- Do not use stock transition templates such as "that is the turn", "the next detail changes how...", or "the confirmed picture is still narrow"; write a fresh human bridge tied to the actual topic.
- Do not reuse the same sentence across multiple segments, even if the idea is similar.
- Avoid exclamation points unless the script explicitly calls for excitement.
- Each segment should be 1-2 sentences. Do NOT switch topics mid-sentence.
- Stay close to the per-segment word caps (aim ~90-100% of each cap); do not be significantly shorter.
- Avoid specific dates, rankings, or stats unless they appear in the provided context above.
- Do NOT invent season/episode/part/chapter numbers. Only mention numbered installments if they appear in the topic label or provided context; otherwise say "the episode" or "the season" without numbers.
- Avoid filler words ("um", "uh", "umm", "uhm", "ah", "like"). Use zero filler words in the entire script, especially in segments 0-2.
- Opening unit discipline: the generated intro plus segments 0-2 must be clean, explicit, curiosity-driven, and entertaining without hesitation sounds, stall words, fake pauses, or annoying verbal padding.
- Do NOT introduce the presenter or host by name. A single brief greeting like "Hi guys, today we are talking about..." is allowed only in the generated intro, then open with the topic tension immediately. Segment 0 should not repeat the greeting.
- The opening should feel calm but magnetic: simple eye contact, clean sentences, one smooth emotional color that fits the topic, and no theatrical phrasing.
- Do NOT add micro vocalizations ("heh", "whew", "hmm").
- Do NOT mention "intro", "outro", "segment", "next segment", or say "in this video/clip".
- Segment 0 must be a strong hook that makes people stay.
- ${
		hasDirectAnswerTopic
			? 'For direct-answer prompts, segment 0 should continue after the answer with why the result matters, what changed, or what viewers are debating.'
			: 'Segment 0 should open with a tension, disagreement, contradiction, or "what people assume vs what the evidence or pattern suggests" line in the first sentence.'
	}
- Do NOT start segment 0 with "Quick update on..." or restate the intro line; the intro handles that.
- Segment 0 should read like the very next sentence after the intro, continuing the same thought without reintroducing the topic.
- Do NOT start segment 0 with transition phrases like "And now", "Now", "Next up", or "Let's talk about".
- Segment 0 follows the tone plan; middle segments stay conversational/neutral; last segment wraps with the tone plan.
- Each topic is its own mini story with clear transitions.
- Make topic handoffs feel smooth and coherent; use a brief bridge phrase to set up the next topic.
- For Topic 2+ only, the FIRST segment must START with an explicit transition line that names the topic. Do NOT use that transition for Topic 1.
- The FIRST segment for every topic must mention the topic name in the first sentence.
- The FIRST segment for every topic should sound like you're unpacking the angle viewers are actively debating, not just reciting background.
- Each segment should naturally flow into the next with a quick transition phrase.
- Each segment ends with a complete sentence and strong terminal punctuation. Do NOT end with "and", "but", "so", "because", "with", "to", "for", "that", or an open parenthetical.
- Let each segment land fully. If a line needs one more clause to sound finished in spoken delivery, use it instead of clipping the thought.
- Before the final closing line, end the content with a short tension line, hope line, or open loop; avoid soft wrap-ups or reflective closing phrases.
- No long lists. If you must list, cap at 3 items.
- If listed source context supports uncertainty, say "reports suggest" or "early signs"; without source links, avoid reporting language and keep the point general.
- ${ctaLine}
- Topic questions must be short and end with a single question mark.
- Provide "shortTitle": 2-5 words, punchy and easy to read.
- "title" must be a clean, human YouTube headline with proper capitalization and punctuation. Use natural headline case. If you use an emoji, use at most one and place it naturally.
- For each segment, include "expression" from: neutral, warm, serious, excited, thoughtful.
- Default to neutral for most segments. Use warm/thoughtful sparingly (1-2 middle segments max) and keep it subtle.
- If you include the entertainment reactionary aside, set expression to "warm" for that segment.
- If the topic is sad or serious, use neutral (no exaggerated sadness).
- If the topic is political or a real-world tragedy, keep expression neutral (no smiles).
- For death, injury, legal, health, or public-safety stories: start from confirmed reporting, name uncertainty plainly, avoid speculation, and keep the audience connection human rather than dramatic.
- If the title asks "what it means", answer both the human/community impact and the practical stakes; do not stop at "details are limited" unless no sourced implication exists.
- If the request, script title, or final title promises price, release details, availability, or what to expect, the spoken script must directly answer that promise with sourced details when available. If the source context does not confirm a detail, say it is not confirmed instead of skipping it.
- ONLY if a topic is about a TV show, film, or fictional character, frame it as plot/character discussion, not real-life tragedy.
- ONLY if a topic is marked as Fictional/Story, keep it in-universe and avoid real-world mourning language.
- If a topic is real-world, do NOT use in-universe/fictional framing or words like "in-universe", "fictional", "plotline", "storyline", "canon", "lore".
- Avoid phrasing like "sad news" unless it is a real-world tragedy.
- For non-countdown topics, use the topic anchor phrase in the FIRST segment of each topic. For countdown topics, use each ranked item's name instead.
${evidenceLine}
- Category-specific guidance:
${categoryGuide.lines.join("\n")}
- Voiceover clarity matters. Write lines that ElevenLabs can say naturally on the first pass.
- Avoid quote-like keyword strings, platform-led attribution, scoreboard shorthand, slashes, and compressed phrasing that sounds robotic when read aloud.
- If the line is happy, use a light smile; if very happy, a brief small smile with slight teeth (never a wide grin).
- Keep expressions coherent across segments; avoid abrupt mood flips and avoid exaggerated expressions.
- Each segment must include EXACTLY one overlayCues entry with a search query that matches that segment.
- For Top N countdowns, set countdownRank on every segment to the rank being discussed and countdownLabel to the ranked item name. The first segment for each rank must start with "#rank- Item Name".
- overlayCues.query must be 2-6 words, describe a real visual to search for (photo or video), include the topic name or a key subject from that segment, no punctuation or hashtags.
- overlayCues.query must name a concrete visual detail from the segment (person, work, location, event). Avoid generic words like "news", "update", "story".
- For metaphorical or psychological topics, translate the idea into concrete scenes. Example: "watching people fail" means public mistakes, business problems, online comments, reflection, judgment, relief, and learning; it does NOT mean a person literally falling down unless physical falling is the actual topic.
- For abstract topics, use searchable visual nouns like "business closed sign", "online comments phone screen", "person watching laptop", "person thinking laptop", "office presentation mistake", "gratitude journal desk", or "public park sunlight".
- Never put vague helper words like "why", "secretly", "part", "worth", "watch", or "thing" into overlayCues.query unless they are part of a named title. The query should read like something a search engine can actually find.
- Treat overlayCues.query as the downstream feed-search contract for images and possible B-roll video. It must stay within the topic and name a visible subject, place, action, object, institution, or scene from the segment/source context.
- Shape the story around the strongest available feed visuals above where possible. The image/video research comes before the script: use the validated visual-first beat plan to choose concrete beats, examples, and overlayCues.query values, but do not say "image title", "search result", "visual beat", or "visual research" in the spoken script.
- For visual-heavy segments, prefer overlayCues.query values from the validated visual-first beat plan exactly when they fit the segment. If you need a new query, keep it just as concrete and directly tied to the topic.
- Do not let visuals make the script generic. The writing still needs a strong, coherent editorial arc: clear hook, escalating tension, credible context, useful payoff, and strict topic focus.
- Never choose or imply an unrelated feed visual just because it is dramatic. If the available visual does not clearly belong to the topic, broaden to a directly adjacent topic visual or use a neutral explanatory visual; do not use unrelated people, unrelated events, or generic scenery.
- If a beat depends on a chart, graph, official data table, report, debt paperwork, application screen, map, timeline, or infographic, write that segment so one visual can stay on screen long enough to be understood. Explain what the viewer is looking at instead of rotating away too fast.
- For chart, graph, table, map, timeline, report, data, paperwork, or infographic segments, set overlayCues.startPct near 0.05 and endPct near 0.95 so the feed visual can stay up while the presenter finishes the explanation.
- Prefer official, public-facing, source-related visual subjects for overlayCues.query, such as official portraits, team photos, press conferences, venues, event stills, or source article subjects. Avoid stock-agency wording.
- Do NOT include overlayCues.query, image-search hints, visual cue labels, or anchor-image language in the spoken segment text. Those are metadata only.
- If exact photos are scarce, broaden only to adjacent visible context directly implied by the topic or source context; never use unrelated people, places, brands, or generic scenery.
- overlayCues.startPct and endPct usually sit between 0.2 and 0.85, with endPct at least 0.2 greater than startPct. For chart/graph/data/report/table/map/timeline/paperwork/infographic explanation segments, use approximately 0.05 to 0.95.
- overlayCues.position must be "topRight" only.
- Also return shortsDetails with clip candidates and packaging ideas.
- shortsDetails.clipCandidates must be 3-6 items with type, segmentIndex, line, openLoop, ctaLine, targetSeconds.
- clipCandidates.line must be copied verbatim from the segment text it references.
- targetSeconds must be 25, 35, or 45.
- Each clip must include titleCandidates + thumbnailTextCandidates that are descriptive of that clip line (not generic).

Return JSON ONLY:
{
  "title": "...",
  "shortTitle": "...",
  "shortsDetails": {
    "angle": "one sentence about what the video is really about",
    "titleCandidates": ["5-8 options, max 95 chars each"],
    "thumbnailTextCandidates": ["5-8 options, 2-5 words each"],
    "clipCandidates": [
      {
        "type": "hook|twist|controversy|context_needed",
        "segmentIndex": 0,
        "line": "exact sentence(s) from that segment text",
        "openLoop": true,
        "ctaLine": "Full breakdown on the channel.",
        "targetSeconds": 25,
        "titleCandidates": ["3-6 clip-specific options, max 95 chars each"],
        "thumbnailTextCandidates": ["3-6 clip-specific options, 2-5 words each"]
      }
    ]
  },
  "segments": [
	{
	  "index": 0,
	  "topicIndex": 0,
	  "topicLabel": "...",
	  "text": "...",
	  "expression": "neutral|warm|serious|excited|thoughtful",
	  "countdownRank": 5,
	  "countdownLabel": "ranked item name, only for Top N countdowns",
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
			(r) => Number(idx) >= r.startIndex && Number(idx) <= r.endIndex,
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
				countdownRank: Number.isFinite(Number(s.countdownRank || s.rank))
					? Number(s.countdownRank || s.rank)
					: null,
				countdownLabel: cleanTopicLabel(
					s.countdownLabel || s.label || s.item || "",
				),
				overlayCues: Array.isArray(s.overlayCues) ? s.overlayCues : [],
			};
		})
		.filter((s) => s.text);

	segments = segments.map((s) => ({
		...s,
		expression: coerceExpressionForNaturalness(
			s.expression,
			s.text,
			mood,
			s.topicLabel,
		),
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

	if (!topListPlan) {
		segments = ensureTopicTransitions(segments, safeTopics);
		segments = ensureTopicAnchors(segments, safeTopics, topicIntents);
		segments = enforceTopicSpecificityGuards(
			segments,
			safeTopics,
			topicContexts,
			topicIntents,
		);
	}
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
		wordCaps,
		{ skipFinalTopicQuestion: includeOutro },
	);
	segments = enforceTopListCountdownStructure(segments, topListPlan);

	// Ensure clean segment endings and CTA consistency.
	segments = enforceSegmentCompleteness(segments, mood, {
		includeCta: !includeOutro,
	});
	segments = limitFillerAndEmotesAcrossSegments(segments, {
		maxFillers: MAX_FILLER_WORDS_PER_VIDEO,
		maxFillersPerSegment: MAX_FILLER_WORDS_PER_SEGMENT,
		maxEmotes: MAX_MICRO_EMOTES_PER_VIDEO,
		maxEmotesPerSegment: MAX_MICRO_EMOTES_PER_VIDEO,
		noFillerSegmentIndices: OPENING_NO_FILLER_SEGMENT_INDICES,
	});
	segments = segments.map((s) => ({
		...s,
		text: sanitizeSegmentText(s.text),
	}));
	if (FORCE_NEUTRAL_VOICEOVER && segments[0]) {
		segments[0] = { ...segments[0], expression: "neutral" };
	}
	if (FORCE_NEUTRAL_VOICEOVER) {
		segments = segments.map((s) =>
			s.expression === "excited" ? { ...s, expression: "warm" } : s,
		);
	}
	// Smooth expressions so adjacent segments stay coherent.
	const smoothed = smoothExpressionPlan(
		segments.map((s) => s.expression),
		mood,
	);
	segments = segments.map((s, i) => ({ ...s, expression: smoothed[i] }));

	const fallbackTitle = safeTopics.map((t) => t.topic).join(" | ");
	let finalTitle =
		formatHumanTitle(String(parsed.title || fallbackTitle).trim(), 120) ||
		formatHumanTitle(fallbackTitle, 120) ||
		"Quick Update";
	let finalShortTitle = shortTitleFromText(
		String(parsed.shortTitle || "").trim() || finalTitle,
	).slice(0, 60);
	const promptAlignedScript = applyPromptBriefToScript({
		script: { title: finalTitle, shortTitle: finalShortTitle, segments },
		topics: safeTopics,
		wordCaps,
	});
	finalTitle = promptAlignedScript.title || finalTitle;
	finalShortTitle = promptAlignedScript.shortTitle || finalShortTitle;
	segments = Array.isArray(promptAlignedScript.segments)
		? promptAlignedScript.segments
		: segments;
	const priorAlignedScript = applyPriorVideoReferenceToScript({
		script: { title: finalTitle, shortTitle: finalShortTitle, segments },
		priorVideoPlan,
		wordCaps,
	});
	segments = Array.isArray(priorAlignedScript.segments)
		? priorAlignedScript.segments
		: segments;
	const directAnswerAlignedScript = repairDirectAnswerOpening({
		script: { title: finalTitle, shortTitle: finalShortTitle, segments },
		topics: safeTopics,
		topicContexts,
		wordCaps,
	});
	segments = Array.isArray(directAnswerAlignedScript.script?.segments)
		? directAnswerAlignedScript.script.segments
		: segments;
	const rawShortsDetails =
		parsed.shortsDetails || parsed.shorts_details || parsed.shorts || null;
	const normalizedShortsDetails = normalizeShortsDetails(rawShortsDetails, {
		title: finalTitle,
		shortTitle: finalShortTitle,
		segments,
	});

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
		shortsDetails: normalizedShortsDetails,
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
				t?.trendStory?.interestOverTime,
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
		{ limit: 12 },
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
	categoryLabel = "",
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
		(s) => countWords(s.text) < QA_MIN_SEGMENT_WORDS,
	);
	if (shortSegments.length) warnings.push("short_segments_detected");
	const veryShortWordLimit = Math.max(
		6,
		Math.floor(QA_MIN_SEGMENT_WORDS * 0.65),
	);
	const veryShortSegments = shortSegments.filter(
		(s) => countWords(s.text) <= veryShortWordLimit,
	);
	const tooManyShortSegments =
		shortSegments.length >= Math.max(2, Math.ceil(segments.length * 0.18));
	if (veryShortSegments.length) warnings.push("very_short_segments_detected");
	if (tooManyShortSegments) warnings.push("too_many_short_segments_detected");

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
			segmentHasAttribution(s.text || "", tokens),
		);
		if (!hasAttribution) missingAttributionTopics.push(i);
	}
	if (missingAttributionTopics.length)
		warnings.push("missing_attribution_by_topic");

	const trendCoverage = assessTrendSignalCoverage(script, topics);
	if (trendCoverage.missingTopics.length)
		warnings.push("missing_trend_signal_coverage");

	const speakability = analyzeScriptSpeakability({
		script,
		topics,
		categoryLabel,
	});
	for (const warning of speakability.warnings || []) {
		if (!warnings.includes(warning)) warnings.push(warning);
	}
	const unsupportedAttributionSegments = findUnsupportedAttributionSegments({
		script,
		topics,
		topicContexts,
	});
	if (unsupportedAttributionSegments.length) {
		warnings.push("unsupported_attribution_without_sources");
	}
	const titlePromiseCoverage = analyzeTitlePromiseCoverage({
		script,
		topics,
		topicContexts,
	});
	if (titlePromiseCoverage.missing.length)
		warnings.push("title_promise_missing_details");

	const needsRewrite =
		duplicatePairs.length > 0 ||
		missingAttributionTopics.length > 0 ||
		trendCoverage.missingTopics.length > 0 ||
		unsupportedAttributionSegments.length > 0 ||
		titlePromiseCoverage.missing.length > 0 ||
		veryShortSegments.length > 0 ||
		tooManyShortSegments ||
		speakability.needsRewrite;
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
			veryShortSegments: veryShortSegments.map((s) => s.index),
			tooManyShortSegments,
			missingAttributionTopics,
			missingTrendSignalTopics: trendCoverage.missingTopics,
			searchMetaSegments: speakability.stats?.searchMetaSegments || [],
			platformAttributionSegments:
				speakability.stats?.platformAttributionSegments || [],
			bettingPromoSegments: speakability.stats?.bettingPromoSegments || [],
			speechAwkwardSegments: speakability.stats?.speechAwkwardSegments || [],
			stockPhraseSegments: speakability.stats?.stockPhraseSegments || [],
			repeatedSentenceGroups: speakability.stats?.repeatedSentenceGroups || [],
			unsupportedAttributionSegments,
			titlePromiseMissing: titlePromiseCoverage.missing.map((item) => ({
				topicIndex: item.topicIndex,
				key: item.key,
				label: item.label,
				hasEvidence: item.hasEvidence,
				evidenceHint: item.evidenceHint,
			})),
		},
	};
}

function buildShortSegmentExtension({
	text = "",
	segment = {},
	topics = [],
	categoryGuide = {},
	segmentIndex = 0,
	avoidTexts = [],
} = {}) {
	const topic = topics?.[Number(segment?.topicIndex) || 0] || {};
	const topicLabel = String(
		segment?.topicLabel || topic?.displayTopic || topic?.topic || "the story",
	).trim();
	const isQuestion = /\?/.test(String(text || ""));
	const seed =
		(Number.isFinite(Number(segmentIndex)) ? Number(segmentIndex) : 0) +
		(Number(segment?.topicIndex) || 0);
	const avoid = new Set(
		(Array.isArray(avoidTexts) ? avoidTexts : [avoidTexts])
			.map((item) => normalizeQaText(item))
			.filter(Boolean),
	);
	const buildUniqueFallback = () => {
		const label = cleanTopicLabel(topicLabel) || "the issue";
		const genericFallbacks = [
			`A better move is to lower the friction around ${label} so the next step feels ordinary.`,
			`The practical test is whether ${label} becomes easier to act on after the first small choice.`,
			`That gives the segment a different job: name the pressure, then make the next move specific.`,
			`The useful detail is the repeatable action, because that is what turns insight into progress.`,
			`This matters because people need a step they can try without turning the whole thing into a verdict.`,
			`The clearer frame is to make the decision smaller, kinder, and easier to repeat.`,
			`That keeps the point grounded in daily behavior instead of another vague piece of advice.`,
			`The better takeaway is not intensity; it is one specific action that can happen again.`,
			`A useful next step is to remove guesswork before motivation fades.`,
			`That makes the advice more humane because it gives people a path, not just pressure.`,
			`The strongest version is practical: choose the next contact point and make it easy to answer.`,
			`This shifts the focus from proving yourself to creating conditions where trust can grow.`,
		];
		const start = Math.abs(seed) % genericFallbacks.length;
		for (let offset = 0; offset < genericFallbacks.length; offset++) {
			const candidate = genericFallbacks[(start + offset) % genericFallbacks.length];
			const key = normalizeQaText(candidate);
			if (key && !avoid.has(key)) return candidate;
		}
		return `The practical next step around ${label} is to make one low-pressure plan and notice what becomes easier.`;
	};
	const pick = (items) => {
		const list = (Array.isArray(items) ? items : []).filter(Boolean);
		if (!list.length) return buildUniqueFallback();
		const start = Math.abs(seed) % list.length;
		for (let offset = 0; offset < list.length; offset++) {
			const candidate = list[(start + offset) % list.length];
			const key = normalizeQaText(candidate);
			if (key && !avoid.has(key)) return candidate;
		}
		return buildUniqueFallback();
	};
	const sensitive = isSensitiveTopicText(`${topicLabel} ${text}`);
	if (sensitive) {
		return pick(
			isQuestion
				? [
						"The responsible answer has to come from confirmed reporting, not the rumor cycle.",
						"The next useful update is the one that separates confirmed facts from speculation.",
						"The human part matters here, so the wording has to stay precise.",
					]
				: [
						"Keep the focus on what is confirmed, what remains unclear, and who is directly affected.",
						"That restraint gives viewers a clearer line between fact, context, and speculation.",
						"The strongest read here is careful: name the verified facts and leave the unknowns open.",
						"The respectful version keeps the timeline separate from the tribute.",
						"The story can stay compelling without pretending the missing facts are known.",
						"The clearest frame is confirmed loss, unresolved details, and a legacy people are still debating.",
						"That keeps the human stakes visible without turning uncertainty into entertainment.",
						"The next useful beat is context, not guessing: the record, the reaction, and the unanswered pieces.",
						"The responsible path is to explain why it matters while leaving unconfirmed details alone.",
						"That gives viewers room to understand the impact without being pushed into rumor.",
					],
		);
	}
	if (categoryGuide?.isHealth) {
		return pick(
			isQuestion
				? [
						"The answer depends on evidence, timing, and what investigators can verify next.",
						"The next useful detail is the one health officials can actually verify.",
					]
				: [
						"That keeps the focus on evidence, timing, and what investigators can verify next.",
						"The important line is what is known now versus what still needs confirmation.",
					],
		);
	}
	if (categoryGuide?.isPolitics || categoryGuide?.isSerious) {
		return pick(
			isQuestion
				? [
						"The answer depends on what the record actually supports next.",
						"The next key point is whether the evidence catches up to the claim.",
					]
				: [
						"That keeps the focus on what the record actually supports, not the loudest interpretation.",
						"The useful line is evidence first, interpretation second.",
				],
		);
	}
	if (
		categoryGuide?.isWellbeing ||
		isDigitalWellbeingTopic({ topics, text: topicLabel })
	) {
		return pick(
			isQuestion
				? [
						"The useful question is which phone moment could become quiet again first.",
						"The next step is small: choose one transition your phone no longer gets by default.",
						"The practical test is whether one protected pocket of silence changes the day.",
					]
				: [
						"That keeps the advice realistic: protect one small pocket of quiet before trying to redesign your whole life.",
						"The useful shift is to treat quiet as something you schedule, not something the phone leaves behind.",
						"A small boundary works because it gives your attention a place to land before the next alert arrives.",
						"The pressure softens when the phone stops owning every transition by default.",
					],
		);
	}
	if (categoryGuide?.isSocial || isSocialConnectionTopic({ topics, text: topicLabel })) {
		return pick(
			isQuestion
				? [
						"The answer usually starts smaller than people expect: one clear invitation, repeated enough to become familiar.",
						"The useful question is who could become easier to reach if the plan became more specific.",
						"The next step is not becoming impressive; it is making connection easier to repeat.",
					]
				: [
						"That is the part people often misread: awkwardness can be the start of rhythm, not proof the friendship failed.",
						"The small shift is to make connection repeatable, so it does not depend on everyone magically feeling free.",
						"That keeps the advice realistic: send the clear invite, accept a little awkwardness, and let routine do some work.",
						"That is where hope comes in, because friendship often returns through ordinary repetition, not one perfect conversation.",
						"The pressure softens when reaching out becomes normal maintenance, not a test of your worth.",
						"A concrete plan lowers the emotional stakes because everyone knows what yes actually means.",
						"The first few moments can be stiff, but shared rhythm usually arrives after people have somewhere to return.",
						"Treat the follow-up as care, not a courtroom verdict on whether you are wanted.",
						"A recurring walk or meal gives the friendship a place to land before it has to feel deep.",
						"That makes the work feel smaller: choose the person, name the plan, and let repetition carry some weight.",
						"The healthier measure is not instant chemistry; it is whether the next contact feels a little easier.",
						"Connection becomes less fragile when it has a routine to protect it from busy weeks.",
						"The kindest strategy is to make the invitation specific and the response easy.",
						"That leaves room for normal life while still giving the relationship a real chance.",
						"The useful shift is to build around people who return some effort, even imperfectly.",
						"That turns friendship from a performance into a pattern people can keep choosing.",
					],
		);
	}
	if (categoryGuide?.isSports) {
		if (
			/\b(nascar|cup\s+series|motorsports?|stock\s*car|racing|pit\s+road|garage|restart|kyle\s+busch)\b/i.test(
				`${topicLabel} ${text}`,
			)
		) {
			return pick(
				isQuestion
					? [
							"The answer depends on what NASCAR confirms next and how the garage responds.",
							"The real test is whether the next confirmed detail adds clarity without fueling speculation.",
						]
				: [
							"That keeps the focus on the garage reaction, the record, and what remains unconfirmed.",
							"For NASCAR fans, the important line is legacy first, speculation last.",
							"The honest trackside read is simple: respect the loss, name the record, and wait for confirmed details.",
							"The better motorsports frame is the record, the rivalries, the fan reaction, and the facts still missing.",
							"That keeps the story on pit road, not in the rumor cycle.",
							"The garage context matters because his impact was competitive, emotional, and impossible to ignore.",
							"The clean NASCAR read is to hold the trophies and the tension in the same frame.",
						],
			);
		}
		return pick(
			isQuestion
				? [
						"The answer depends on the next adjustment and who handles the pressure.",
						"The real test is what changes when the next game puts pressure on the rotation.",
					]
				: [
						"The pressure point is the next adjustment: rotation, matchup, and who earns trust late.",
						"For fans, the interesting part is how this changes the next decision on the floor.",
					],
		);
	}
	if (/\b(video\s*game|gaming|gameplay|trailer|demo|studio|developer)\b/i.test(topicLabel)) {
		return pick(
			isQuestion
				? [
						"The answer depends on whether the next showing proves the promise.",
						"The next test is whether the footage answers the doubt instead of selling around it.",
					]
				: [
						"The useful read is whether the next showing proves the promise or exposes the gap.",
						"For players, the difference is simple: footage has to answer the doubt.",
					],
		);
	}
	if (
		/\b(broke|paycheck|rent|grocery|groceries|subscription|autopay|automatic|withdrawal|budget|debt|inflation|costs?|bills?|spending|savings?)\b/i.test(
			`${topicLabel} ${text}`,
		)
	) {
		return pick(
			isQuestion
				? [
						"The honest answer starts with separating pressure you caused from pressure the system added.",
						"The useful question is which expense quietly changed the month.",
						"The practical answer starts with making the invisible charges visible.",
					]
				: [
						"That matters because clarity lowers the shame and shows where one small change can start.",
						"The practical move is to make the pressure visible before deciding what to cut.",
						"For someone living close to the line, even one quiet leak can change the week.",
						"The goal is not a perfect budget; it is one decision that gives the paycheck more room.",
						"That keeps the advice humane: reduce the leak without pretending rent and groceries are easy.",
					],
		);
	}
	return pick(
		isQuestion
			? [
					"The useful answer starts with the part people can actually test today.",
					"The better question is what one small choice changes next.",
					"The clearest answer turns the pattern into a specific next step.",
					"The next move is to separate what feels urgent from what is genuinely useful.",
					"The better question is which choice gives people more control today.",
				]
			: [
					"That is where the pattern becomes useful: it points to one small choice people can test next.",
					"The consequence shows up in ordinary decisions, not just in the big emotional moments.",
					"That turns the point into something usable instead of another reason to feel blamed.",
					"The next decision matters because small repeated choices are what change the pattern.",
					"That gives people a clearer way to understand the pressure without turning it into shame.",
				],
	);
}

function removeRepeatedSentenceFromText(text = "", repeatedSentence = "") {
	const repeatedKey = normalizeQaText(repeatedSentence);
	if (!repeatedKey) return sanitizeSegmentText(text);
	const kept = splitSentences(text).filter((sentence) => {
		const key = normalizeQaText(sentence);
		return !key || key !== repeatedKey;
	});
	return sanitizeSegmentText(kept.join(" "));
}

function rememberRepairExtension(usedExtensions, extension = "") {
	const key = normalizeQaText(extension);
	if (key && usedExtensions instanceof Set) usedExtensions.add(key);
}

function sentenceHasBlockingScriptArtifact(sentence = "") {
	const text = String(sentence || "").trim();
	if (!text) return false;
	if (/^\s*(?:Mr|Mrs|Ms|Dr)\.?\s*$/i.test(text)) return true;
	if (/^\s*(?:Mr|Mrs|Ms|Dr)\.?\s+(?:That|This|The|It)\b/i.test(text))
		return true;
	return SCRIPT_STOCK_PHRASE_PATTERNS.some((rx) => rx.test(text));
}

function stripBlockingScriptArtifacts(text = "") {
	const sentences = splitSentences(text).filter(Boolean);
	const kept = sentences.filter(
		(sentence) => !sentenceHasBlockingScriptArtifact(sentence),
	);
	let cleaned = kept.length ? kept.join(" ") : "";
	cleaned = cleaned
		.replace(
			/^\s*[A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,7}\s*:\s*/g,
			"",
		)
		.replace(
			/\b(?:minute|part|section|beat)\s*\d{1,2}\s*:?\s*(?:[A-Z][A-Za-z&-]*\s*){1,5}:\s*/gi,
			" ",
		)
		.replace(/\b(?:visual|image|query|source)\s*:\s*/gi, " ")
		.replace(/^\s*(?:Mr|Mrs|Ms|Dr)\.?\s+(?=(?:That|This|The|It)\b)/i, "")
		.trim();
	for (const rx of SCRIPT_STOCK_PHRASE_PATTERNS) {
		cleaned = cleaned.replace(rx, " ").replace(/\s+/g, " ").trim();
	}
	return cleaned ? sanitizeSegmentText(cleaned) : "";
}

function nonStockSegmentFallback({
	segment = {},
	topics = [],
	segmentIndex = 0,
} = {}) {
	const topicIndex =
		Number.isFinite(Number(segment?.topicIndex)) && Number(segment.topicIndex) >= 0
			? Number(segment.topicIndex)
			: 0;
	const topic = topics?.[topicIndex] || topics?.[0] || {};
	const label = cleanTopicLabel(
		segment?.topicLabel || topic.displayTopic || topic.topic || "",
	);
	const pool = [
		"The useful line is evidence first, interpretation second, and the public reaction stays separate from proof.",
		"That keeps the story grounded: people can notice something unusual without treating a rumor as confirmation.",
		"The stronger version separates what viewers are asking from what the reporting can actually verify.",
		"The careful read is simple: name the visible question, then keep the claim inside the evidence.",
		"That framing lets the controversy breathe without turning uncertainty into a verdict.",
	];
	if (label && countWords(label) <= 5) {
		pool.push(
			`For ${label}, the safer read is to compare the public reaction with the confirmed record.`,
		);
	}
	return sanitizeSegmentText(pool[Math.abs(Number(segmentIndex) || 0) % pool.length]);
}

function ensureNonStockSegmentText({
	text = "",
	segment = {},
	topics = [],
	segmentIndex = 0,
} = {}) {
	let cleaned = sanitizeSegmentText(text);
	if (
		cleaned &&
		countWords(cleaned) >= QA_MIN_SEGMENT_WORDS &&
		!SCRIPT_STOCK_PHRASE_PATTERNS.some((rx) => rx.test(cleaned))
	) {
		return cleaned;
	}
	const stripped = stripBlockingScriptArtifacts(cleaned);
	if (
		stripped &&
		countWords(stripped) >= QA_MIN_SEGMENT_WORDS &&
		!SCRIPT_STOCK_PHRASE_PATTERNS.some((rx) => rx.test(stripped))
	) {
		return stripped;
	}
	return nonStockSegmentFallback({ segment, topics, segmentIndex });
}

function repairBlockingScriptArtifacts({
	script,
	topics = [],
	wordCaps = [],
	categoryLabel = "",
} = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return { script, changed: [] };
	const speakability = analyzeScriptSpeakability({
		script,
		topics,
		categoryLabel,
	});
	const badIndices = new Set([
		...(speakability.stats?.stockPhraseSegments || []),
		...(speakability.stats?.speechAwkwardSegments || []),
	]);
	const repeatedSentenceByIndex = new Map();
	for (const group of speakability.stats?.repeatedSentenceGroups || []) {
		const indices = Array.isArray(group?.segments) ? group.segments : [];
		const repeatedText = String(group?.sentence || "");
		if (SCRIPT_STOCK_PHRASE_PATTERNS.some((rx) => rx.test(repeatedText))) {
			indices.forEach((idx) => badIndices.add(idx));
		} else {
			indices.slice(1).forEach((idx) => {
				badIndices.add(idx);
				repeatedSentenceByIndex.set(idx, repeatedText);
			});
		}
	}
	if (!badIndices.size) return { script, changed: [] };

	const categoryGuide = buildCategoryScriptGuide(categoryLabel, topics);
	const changed = [];
	const usedExtensions = new Set(
		[...repeatedSentenceByIndex.values()].map((text) => normalizeQaText(text)),
	);
	const repaired = segments.map((segment, idx) => {
		if (!badIndices.has(idx)) return segment;
		const original = sanitizeSegmentText(segment?.text || "");
		let text = stripBlockingScriptArtifacts(original);
		const avoidTexts = [...repeatedSentenceByIndex.values()];
		if (repeatedSentenceByIndex.has(idx)) {
			text = removeRepeatedSentenceFromText(
				text,
				repeatedSentenceByIndex.get(idx),
			);
		}
		if (countWords(text) < QA_MIN_SEGMENT_WORDS) {
			const extension = buildShortSegmentExtension({
				text,
				segment,
				topics,
				categoryGuide,
				segmentIndex: idx,
				avoidTexts: [...avoidTexts, ...usedExtensions],
			});
			rememberRepairExtension(usedExtensions, extension);
			text = sanitizeSegmentText(`${text} ${extension}`.trim());
		}
		const cap = Number(wordCaps?.[idx] || 0);
		if (cap) text = trimSegmentToCap(text, Math.max(cap + 8, countWords(text)));
		text = sanitizeSegmentText(text);
		text = ensureNonStockSegmentText({
			text,
			segment,
			topics,
			segmentIndex: idx,
		});
		if (text && text !== original) {
			changed.push({
				index: Number.isFinite(Number(segment?.index)) ? Number(segment.index) : idx,
				from: original,
				to: text,
			});
			return { ...segment, text };
		}
		return segment;
	});
	return { script: { ...script, segments: repaired }, changed };
}

function repairUnsupportedAttributions({
	script,
	topics = [],
	topicContexts = [],
	wordCaps = [],
} = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return { script, changed: [] };
	const changed = [];
	const repaired = segments.map((segment, idx) => {
		const topicIndex =
			Number.isFinite(Number(segment?.topicIndex)) &&
			Number(segment.topicIndex) >= 0
				? Number(segment.topicIndex)
				: 0;
		if (topicHasAttributionSource(topics?.[topicIndex], topicContexts?.[topicIndex]))
			return segment;
		if (!segmentHasUnsupportedAttribution(segment?.text || "")) return segment;
		const original = sanitizeSegmentText(segment?.text || "");
		let text = stripUnsupportedAttributionPhrasing(original);
		const cap = Number(wordCaps?.[segment.index] || wordCaps?.[idx] || 0);
		if (cap) text = trimSegmentToCap(text, Math.max(cap, 12));
		text = sanitizeSegmentText(text);
		if (text && text !== original) {
			changed.push({
				index: Number.isFinite(Number(segment?.index)) ? Number(segment.index) : idx,
				from: original,
				to: text,
			});
			return { ...segment, text };
		}
		return segment;
	});
	return { script: { ...script, segments: repaired }, changed };
}

function repairShortScriptSegments({
	script,
	topics = [],
	wordCaps = [],
	categoryLabel = "",
} = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return { script, changed: [] };
	const categoryGuide = buildCategoryScriptGuide(categoryLabel, topics);
	const changed = [];
	const usedExtensions = new Set();
	const repairedSegments = segments.map((segment, idx) => {
		const text = sanitizeSegmentText(segment?.text || "");
		const words = countWords(text);
		if (words >= QA_MIN_SEGMENT_WORDS) return { ...segment, text };
		const extension = buildShortSegmentExtension({
			text,
			segment,
			topics,
			categoryGuide,
			segmentIndex: idx,
			avoidTexts: [...usedExtensions],
		});
		rememberRepairExtension(usedExtensions, extension);
		let updated = cleanupSpeechText(`${text} ${extension}`)
			.replace(/\s+([,.!?])/g, "$1")
			.replace(/\s{2,}/g, " ")
			.trim();
		const cap = Number(wordCaps?.[idx] || 0);
		const softCap = cap
			? Math.max(cap + 8, QA_MIN_SEGMENT_WORDS + 6, countWords(updated))
			: 0;
		if (softCap) updated = trimSegmentToCap(updated, softCap);
		updated = sanitizeSegmentText(updated);
		updated = ensureNonStockSegmentText({
			text: updated,
			segment,
			topics,
			segmentIndex: idx,
		});
		changed.push({
			index: Number.isFinite(Number(segment?.index)) ? Number(segment.index) : idx,
			fromWords: words,
			toWords: countWords(updated),
		});
		return { ...segment, text: updated };
	});
	return { script: { ...script, segments: repairedSegments }, changed };
}

function repairEarlyCuriosityGap({
	script,
	shortsGuardrails = null,
	topics = [],
	wordCaps = [],
	categoryLabel = "",
} = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return { script, changed: false };
	const guardrails = shortsGuardrails || analyzeShortsGuardrails(script);
	if (!guardrails?.issues?.includes("early_curiosity_gap_missing")) {
		return { script, changed: false };
	}
	const first = segments[0] || {};
	const categoryGuide = buildCategoryScriptGuide(categoryLabel, topics);
	const topic = topics?.[Number(first?.topicIndex) || 0] || {};
	const topicLabel = String(
		first?.topicLabel || topic?.displayTopic || topic?.topic || "this story",
	).trim();
	const baseText = sanitizeSegmentText(first.text || "");
	const sensitive = isSensitiveTopicText(`${topicLabel} ${baseText}`);
	const bridge = sensitive
		? `The important line is what has been confirmed about ${topicLabel}, and what still needs careful attribution.`
		: isImpulseShoppingTopic({ topics, categoryLabel, text: `${topicLabel} ${baseText}` })
			? "The interesting part is how a bored scroll turns into a purchase before you ever decide to go shopping."
		: categoryGuide?.isHealth
			? `The unresolved question is whether ${topicLabel} stays contained or the evidence points somewhere else.`
			: categoryGuide?.isSports
				? `The next pressure point is how ${topicLabel} changes the rotation, matchup, or locker room response.`
				: `The unresolved part is the consequence viewers should watch next.`;
	const updatedText = sanitizeSegmentText(`${baseText} ${bridge}`);
	const cap = Number(wordCaps?.[0] || 0);
	const softCap = cap
		? Math.max(cap + 12, countWords(updatedText), QA_MIN_SEGMENT_WORDS + 8)
		: 0;
	const finalText = softCap
		? trimSegmentToCap(updatedText, softCap)
		: updatedText;
	const repairedSegments = segments.map((segment, idx) =>
		idx === 0 ? { ...segment, text: sanitizeSegmentText(finalText) } : segment,
	);
	return { script: { ...script, segments: repairedSegments }, changed: true };
}

function repairEndingOpenLoop({
	script,
	shortsGuardrails = null,
	topics = [],
	wordCaps = [],
} = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return { script, changed: false };
	const guardrails = shortsGuardrails || analyzeShortsGuardrails(script);
	if (!guardrails?.issues?.includes("ending_open_loop_missing")) {
		return { script, changed: false };
	}
	const lastIdx = segments.length - 1;
	const last = segments[lastIdx] || {};
	const topic = topics?.[Number(last?.topicIndex) || 0] || {};
	const topicLabel = String(
		last?.topicLabel || topic?.displayTopic || topic?.topic || "this",
	).trim();
	const baseText = sanitizeSegmentText(last.text || "");
	const bridgeHay = `${topicLabel} ${baseText}`;
	const bridge = isPsychologyFailureTopicText(bridgeHay)
		? "But the open question is whether you leave with humility, or just another reason to feel above someone."
		: isImpulseShoppingTopic({ topics, text: bridgeHay })
			? "But the useful test is simple: A deal is not a deal if you never needed it."
			: "But the open question is what you notice next, and what you do with it.";
	let updatedText = sanitizeSegmentText(`${baseText} ${bridge}`);
	const cap = Number(wordCaps?.[last.index] || wordCaps?.[lastIdx] || 0);
	if (cap) {
		updatedText = trimSegmentToCap(
			updatedText,
			Math.max(cap + 12, countWords(baseText), QA_MIN_SEGMENT_WORDS + 8),
		);
	}
	const repairedSegments = segments.map((segment, idx) =>
		idx === lastIdx ? { ...segment, text: sanitizeSegmentText(updatedText) } : segment,
	);
	return { script: { ...script, segments: repairedSegments }, changed: true };
}

function buildForcedFinalOpenLoopLine({
	topics = [],
	categoryLabel = "",
	title = "",
} = {}) {
	const hay = [
		title || "",
		categoryLabel || "",
		...(Array.isArray(topics)
			? topics.map((topic) =>
					[topic?.displayTopic, topic?.topic, topic?.promptText]
						.filter(Boolean)
						.join(" "),
				)
			: []),
	].join(" ");
	if (isPsychologyFailureTopicText(hay)) {
		return "The open question is what you do after the clip ends: notice the lesson, or let the joke make you less human?";
	}
	if (isImpulseShoppingTopic({ topics, categoryLabel, text: hay })) {
		return "That is the test after the scroll slows down: A deal is not a deal if you never needed it.";
	}
	if (isPersonalFinanceCostOfLivingTopic({ topics, categoryLabel, text: hay })) {
		return "The open question is which small decision becomes easier once the pressure is visible?";
	}
	if (isSocialConnectionTopic({ topics, categoryLabel, text: hay })) {
		return "The open question is which ordinary invitation could make connection feel possible again?";
	}
	if (isSensitiveTopicText(hay)) {
		if (/\b(nascar|cup\s+series|racing|motorsports?|driver|kyle\s+busch)\b/i.test(hay)) {
			return "The open question is what confirmed details come next, and how NASCAR chooses to remember the full, complicated legacy.";
		}
		return "The open question is what confirmed details come next, and how people separate grief from speculation.";
	}
	return "The open question is what you notice next, and whether it changes the next choice you make.";
}

function forceFinalOpenLoopSegment({
	script = {},
	topics = [],
	categoryLabel = "",
	wordCaps = [],
} = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return { script, changed: false };
	const lastIdx = segments.length - 1;
	const line = buildForcedFinalOpenLoopLine({
		topics,
		categoryLabel,
		title: script.title || script.shortTitle || "",
	});
	const cap = Number(wordCaps?.[segments[lastIdx]?.index] || wordCaps?.[lastIdx] || 0);
	const text = sanitizeSegmentText(
		cap
			? trimSegmentToCap(line, Math.max(cap + 8, countWords(line) + 2))
			: line,
	);
	const repairedSegments = segments.map((segment, idx) =>
		idx === lastIdx ? { ...segment, text, expression: "thoughtful" } : segment,
	);
	return { script: { ...script, segments: repairedSegments }, changed: true };
}

function polishImpulseShoppingScript({
	script,
	topics = [],
	categoryLabel = "",
} = {}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return { script, changed: [] };
	const hay = [
		categoryLabel,
		script?.title,
		script?.shortTitle,
		...topics.map((topic) =>
			[topic?.displayTopic, topic?.topic, topic?.promptText]
				.filter(Boolean)
				.join(" "),
		),
	].join(" ");
	if (!isImpulseShoppingTopic({ topics, categoryLabel, text: hay })) {
		return { script, changed: [] };
	}

	const changed = [];
	const lastIdx = segments.length - 1;
	const repaired = segments.map((segment, idx) => {
		const original = sanitizeSegmentText(segment?.text || "");
		let text = original;
		if (/\byou\s+are\s+not\s+buying\s+an\s+item\s+first\.?$/i.test(text)) {
			text =
				"That is the practical trap: you are buying a mood, a shortcut, or a tiny promise before you are buying an item.";
		} else if (/\bthat\s+is\s+where\s+the\s+pattern\s+becomes\s+useful\b/i.test(text)) {
			text =
				"That is why endless scrolling matters. Every swipe gives the product another chance to feel normal before your budget gets a vote.";
		}
		if (idx === lastIdx) {
			text =
				"That is the test after the scroll slows down: A deal is not a deal if you never needed it.";
		}
		text = sanitizeSegmentText(text);
		if (text && text !== original) {
			changed.push({
				index: Number.isFinite(Number(segment?.index)) ? Number(segment.index) : idx,
				from: original,
				to: text,
			});
			return {
				...segment,
				text,
				expression: idx === lastIdx ? "thoughtful" : segment.expression,
			};
		}
		return segment;
	});
	return { script: { ...script, segments: repaired }, changed };
}

function repairResidualScriptQuality({
	script,
	topics = [],
	topicContexts = [],
	wordCaps = [],
	categoryLabel = "",
	shortsGuardrails = null,
} = {}) {
	let current = script;
	const repairs = [];
	const shortRepair = repairShortScriptSegments({
		script: current,
		topics,
		wordCaps,
		categoryLabel,
	});
	current = shortRepair.script;
	if (shortRepair.changed.length) {
		repairs.push({ type: "short_segments", changed: shortRepair.changed });
	}
	const artifactRepair = repairBlockingScriptArtifacts({
		script: current,
		topics,
		wordCaps,
		categoryLabel,
	});
	current = artifactRepair.script;
	if (artifactRepair.changed.length) {
		repairs.push({
			type: "blocking_script_artifacts",
			changed: artifactRepair.changed,
		});
	}
	const unsupportedAttributionRepair = repairUnsupportedAttributions({
		script: current,
		topics,
		topicContexts,
		wordCaps,
	});
	current = unsupportedAttributionRepair.script;
	if (unsupportedAttributionRepair.changed.length) {
		repairs.push({
			type: "unsupported_attributions",
			changed: unsupportedAttributionRepair.changed,
		});
	}
	const earlyRepair = repairEarlyCuriosityGap({
		script: current,
		shortsGuardrails,
		topics,
		wordCaps,
		categoryLabel,
	});
	current = earlyRepair.script;
	if (earlyRepair.changed) repairs.push({ type: "early_curiosity_gap" });
	const endingRepair = repairEndingOpenLoop({
		script: current,
		shortsGuardrails,
		topics,
		wordCaps,
	});
	current = endingRepair.script;
	if (endingRepair.changed) repairs.push({ type: "ending_open_loop" });
	const impulseRepair = polishImpulseShoppingScript({
		script: current,
		topics,
		categoryLabel,
	});
	current = impulseRepair.script;
	if (impulseRepair.changed.length) {
		repairs.push({
			type: "impulse_shopping_polish",
			changed: impulseRepair.changed,
		});
	}
	return { script: current, repairs };
}

function blockingScriptQualityIssues(qa = {}) {
	const stats = qa?.stats || {};
	const issues = [];
	if ((stats.speechAwkwardSegments || []).length)
		issues.push("speech_awkward_segments");
	if ((stats.stockPhraseSegments || []).length)
		issues.push("stock_transition_phrases");
	if ((stats.repeatedSentenceGroups || []).length)
		issues.push("repeated_sentences");
	if ((stats.platformAttributionSegments || []).length)
		issues.push("bad_platform_attribution");
	if ((stats.unsupportedAttributionSegments || []).length)
		issues.push("unsupported_source_attribution");
	if ((stats.searchMetaSegments || []).length >= 2)
		issues.push("search_meta_phrasing");
	return issues;
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
		/\?/.test(String(s.text || "")),
	).length;
	const fullText = segments
		.map((s) => String(s.text || "").toLowerCase())
		.join(" ");
	const countTokenHits = (tokens = []) =>
		(tokens || []).reduce(
			(count, tok) =>
				fullText.includes(String(tok || "").toLowerCase()) ? count + 1 : count,
			0,
		);
	return {
		segmentCount: segments.length,
		totalWords,
		avgWords: Number(avgWords.toFixed(1)),
		questionSegments,
		questionRatio: Number(
			(segments.length ? questionSegments / segments.length : 0).toFixed(2),
		),
		trendTokenHits: countTokenHits(TREND_SIGNAL_TOKENS),
		excitedTokenHits: countTokenHits(EXCITED_TONE_TOKENS),
		entertainmentTokenHits: countTokenHits(ENTERTAINMENT_KEYWORDS),
	};
}

const SOURCE_LABEL_OVERRIDES = new Map([
	["asatunews.co.id", "AsatuNews"],
	["bbc.com", "BBC"],
	["bbc.co.uk", "BBC"],
	["cbsnews.com", "CBS News"],
	["cnbc.com", "CNBC"],
	["cnn.com", "CNN"],
	["espn.com", "ESPN"],
	["hindustantimes.com", "Hindustan Times"],
	["ndtv.com", "NDTV"],
	["npr.org", "NPR"],
	["nytimes.com", "The New York Times"],
	["theguardian.com", "The Guardian"],
	["usatoday.com", "USA Today"],
	["washingtonpost.com", "The Washington Post"],
]);

function sourceLabelOverride(normalizedHost = "") {
	for (const [host, label] of SOURCE_LABEL_OVERRIDES) {
		if (normalizedHost === host || normalizedHost.endsWith(`.${host}`)) {
			return label;
		}
	}
	return "";
}

function formatSourceLabel(host = "", topicLabel = "") {
	const cleaned = String(host || "")
		.replace(/^www\./i, "")
		.trim();
	if (!cleaned) return "";
	const attributionSkipHosts = new Set([
		"youtube.com",
		"tiktok.com",
		"instagram.com",
		"reddit.com",
		"x.com",
		"twitter.com",
		"facebook.com",
	]);
	const normalizedHost = cleaned.toLowerCase();
	if (
		attributionSkipHosts.has(normalizedHost) ||
		Array.from(attributionSkipHosts).some((entry) =>
			normalizedHost.endsWith(`.${entry}`),
		)
	) {
		return "";
	}
	const overrideLabel = sourceLabelOverride(normalizedHost);
	if (overrideLabel) return overrideLabel;
	const base = cleaned.replace(
		/(?:\.co)?\.(com|net|org|us|uk|io|tv|info|biz|gov|id|in|au|ca)$/i,
		"",
	);
	const words = base
		.replace(/[^a-z0-9]+/gi, " ")
		.split(/\s+/)
		.filter(Boolean);
	if (!words.length) return cleaned;
	if (topicLabel) {
		const topicTokens = cleanTopicLabel(topicLabel)
			.toLowerCase()
			.split(/\s+/)
			.filter(Boolean);
		const slug = topicTokens.join("");
		const baseLower = base.toLowerCase();
		if (slug && baseLower.includes(slug)) {
			return topicTokens
				.map((t) =>
					t.length <= 3 ? t.toUpperCase() : t[0].toUpperCase() + t.slice(1),
				)
				.join(" ");
		}
		if (topicTokens.length >= 2) {
			const nameSlug = `${topicTokens[0]}${topicTokens[1]}`;
			if (baseLower.includes(nameSlug)) {
				return `${topicTokens[0][0].toUpperCase() + topicTokens[0].slice(1)} ${
					topicTokens[1][0].toUpperCase() + topicTokens[1].slice(1)
				}`;
			}
		}
	}
	return words
		.map((w) =>
			w.length <= 3 ? w.toUpperCase() : w[0].toUpperCase() + w.slice(1),
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
			"",
		);
		const cleaned = base.replace(/[^a-z0-9]+/g, " ").trim();
		if (cleaned) tokens.add(cleaned);
	}
	return Array.from(tokens);
}

function pickTopicSourceHosts(
	topic,
	topicContext = [],
	{ preferArticles = false } = {},
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
			topicContextFlags?.contentType || "",
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
			segmentHasAttribution(s.text || "", sourceTokens),
		);
		if (hasAttribution) continue;

		const promptOpening = String(topics?.[i]?.promptBrief?.openingLine || "").trim();
		const openingKey = normalizeOpeningForCompare(promptOpening);
		const topicSegmentEntries = segments
			.map((s, index) => ({ s, index }))
			.filter((entry) => Number(entry.s?.topicIndex) === i);
		const targetEntry =
			openingKey && topicSegmentEntries.length > 1
				? topicSegmentEntries.find((entry) => {
						const textKey = normalizeOpeningForCompare(entry.s?.text || "");
						return !textKey.startsWith(openingKey);
					}) || topicSegmentEntries[1]
				: topicSegmentEntries[0];
		const targetIndex = Number(targetEntry?.index);
		if (!Number.isFinite(targetIndex) || targetIndex < 0) continue;
		const sourceLabel = formatSourceLabel(
			sourceHosts[0],
			segments[targetIndex]?.topicLabel || topics?.[i]?.topic || "",
		);
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

async function rewriteSegmentsForPriorNovelty({
	jobId,
	script,
	topics = [],
	topicContexts = [],
	wordCaps = [],
	tonePlan,
	categoryLabel = "",
	includeOutro = true,
	priorVideoPlan = null,
}) {
	if (!priorVideoPlan?.hasPriorVideos) return script;
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return script;
	const mood = tonePlan?.mood || "neutral";
	const categoryGuide = buildCategoryScriptGuide(categoryLabel, topics);
	const priorGuide = buildPriorVideoScriptGuide(priorVideoPlan);
	const promptBriefGuide = buildPromptBriefInstructionBlock(topics);
	const directAnswerGuide = buildDirectAnswerPromptBlock(topics, topicContexts);
	const contextLines =
		Array.isArray(topicContexts) && topicContexts.length
			? topicContexts
					.map((tc, idx) => {
						const items = Array.isArray(tc.context) ? tc.context : [];
						const hints = items
							.map((item) =>
								typeof item === "string"
									? item
									: `${item?.title || ""} ${item?.snippet || ""}`,
							)
							.filter(Boolean)
							.slice(0, 5);
						return `Topic ${idx + 1} (${tc.topic || topics?.[idx]?.topic || ""}): ${
							hints.length ? hints.join(" | ") : "(none)"
						}`;
					})
					.join("\n")
			: "- (none)";
	const capsLine = wordCaps.map((c, i) => `#${i}: <= ${c} words`).join(", ");
	const sourcePolicy = buildTopicSourcePolicyPromptBlock(topics, topicContexts);
	const ctaRule = includeOutro
		? "Do not add like/subscribe/comment CTAs inside content segments; the separate outro handles that."
		: "The final segment may include one short engagement question.";
	const rewritePrompt = `
Rewrite this script as a genuinely fresh follow-up to previous videos on a similar topic.

${priorGuide}

Frontend prompt requirements:
${promptBriefGuide}

${directAnswerGuide}

Fresh context:
${contextLines}

${sourcePolicy.text}

Mood: ${mood}
Category: ${categoryLabel || "General"}
Per-segment word caps: ${capsLine}

Rules:
- Keep EXACTLY ${segments.length} segments and the same indexes.
- Keep topicIndex/topicLabel assignments.
- Preserve any frontend-requested opening line as the first spoken line of the overall opening unit, and do not repeat it inside segment 0 when the intro already uses it.
- For direct-answer prompts, preserve the answer in the first spoken content sentence for that topic; do not turn it into a tease.
- Preserve any mandatory "must include" lines, but change surrounding content so the video is not a duplicate. Do not say labels like "memorable line" or "must include" in the spoken script.
- Make roughly ${priorVideoPlan.requiredNewnessPct || 65}% of the content feel new: examples, ordering, explanations, visuals implied by overlayCues, practical advice, and payoff.
- If a prior-video reference line is specified, include it exactly once and naturally.
- Avoid repeating the same sentence, same stock bridge, or same step order from earlier videos.
- Use different concrete examples where possible.
- Keep the tone empathetic, useful, creator-like, and brisk; avoid slow reassurance or over-explaining obvious ideas.
- Keep the first beat direct and retention-led: one brief natural greeting is allowed in the generated intro, then no filler, no artificial pause writing, and no theatrical emotional cue.
- Use fair factual tension where supported: name the tradeoff, disagreement, incentive, or cost without inventing claims.
- Use source attribution only for topics marked with source links in the Source policy; otherwise do not invent named outlets, journals, studies, researchers, or reporting.
- ${ctaRule}
- Category-specific guidance:
${categoryGuide.lines.join("\n")}

Return JSON ONLY:
{ "segments":[{"index":0,"text":"...","expression":"neutral|warm|thoughtful|serious|excited"}] }

Current script:
${segments.map((s) => `#${s.index}: ${s.text}`).join("\n")}
`.trim();
	try {
		const resp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: rewritePrompt }],
		});
		const parsed = parseJsonFlexible(resp?.choices?.[0]?.message?.content || "");
		if (!parsed || !Array.isArray(parsed.segments)) return script;
		const byIndex = new Map();
		for (const seg of parsed.segments) {
			const idx = Number(seg?.index);
			if (!Number.isFinite(idx)) continue;
			const text = sanitizeSegmentText(seg?.text || "");
			if (!text) continue;
			byIndex.set(idx, {
				text,
				expression: normalizeExpression(seg?.expression, mood),
			});
		}
		const updated = segments.map((s, i) => {
			const next = byIndex.get(Number(s.index)) || byIndex.get(i);
			if (!next) return s;
			const cap = wordCaps[i] || wordCaps[s.index] || 24;
			return {
				...s,
				text: sanitizeSegmentText(trimSegmentToCap(next.text, cap + 8)),
				expression: next.expression || s.expression || "neutral",
			};
		});
		const nextScript = applyPriorVideoReferenceToScript({
			script: { ...script, segments: updated },
			priorVideoPlan,
			wordCaps,
		});
		logJob(jobId, "prior novelty rewrite applied", {
			segments: updated.length,
			estimate: estimatePriorNovelty(nextScript, priorVideoPlan),
		});
		const promptAligned = applyPromptBriefToScript({
			script: nextScript,
			topics,
			wordCaps,
		});
		const directAligned = repairDirectAnswerOpening({
			script: promptAligned,
			topics,
			topicContexts,
			wordCaps,
		}).script;
		return applyPriorVideoReferenceToScript({
			script: directAligned,
			priorVideoPlan,
			wordCaps,
		});
	} catch (e) {
		logJob(jobId, "prior novelty rewrite failed (continuing)", {
			error: e.message,
		});
		return script;
	}
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
	categoryLabel = "",
	includeOutro = true,
	contentMode = "trends",
	priorVideoPlan = null,
}) {
	const segments = Array.isArray(script?.segments) ? script.segments : [];
	if (!segments.length) return script;
	const mood = tonePlan?.mood || "neutral";
	const isPromptMode = String(contentMode || "").toLowerCase() === "prompt";
	const categoryGuide = buildCategoryScriptGuide(categoryLabel, topics);
	const topListPlan = resolveTopListPlan(topics, categoryLabel);
	const retentionGuide = buildDynamicRetentionGuide({
		topics,
		topicContexts,
		categoryGuide,
		tonePlan,
		topListPlan,
		contentMode,
	});
	const trendSignalLabel = isPromptMode
		? "Context signals (use if present; do NOT invent):"
		: categoryGuide.isSports
			? "Search signals (use as audience context for the hook; translate them into natural sports language and do NOT quote them verbatim):"
			: "Trending signals (address the #1 rising reason early if present):";

	const topicSummaries = (topics || []).map((t, idx) => {
		const ctx = Array.isArray(topicContexts?.[idx]?.context)
			? topicContexts[idx].context
			: [];
		const intent = buildTopicIntentSummary(t, ctx);
		const isFictional = Boolean(topicContextFlags?.[idx]?.isFictional);
		const contextLabel = isFictional ? "fictional" : "real-world";
		const sources = uniqueStrings(
			ctx.map((c) => getUrlHost(c?.link || "")).filter(Boolean),
			{ limit: 6 },
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
				})`,
		)
		.join(", ");
	const trendSignalLines = buildTrendSignalLines(topics);
	const titlePromiseGuide = buildTitlePromisePromptBlock({
		script,
		topics,
		topicContexts,
	});
	const promptBriefGuide = buildPromptBriefInstructionBlock(topics);
	const directAnswerGuide = buildDirectAnswerPromptBlock(topics, topicContexts);
	const hasDirectAnswerTopic = topics.some((topic) => isDirectAnswerTopic(topic));
	const answerRewriteRule = hasDirectAnswerTopic
		? "For direct-answer topics, keep the answer in the first spoken content sentence for that topic. Do not rewrite it into a tease."
		: "Preserve curiosity gaps: open with tension and delay the payoff by at least one sentence or segment.";
	const priorVideoGuide = buildPriorVideoScriptGuide(priorVideoPlan);
	const sourcePolicy = buildTopicSourcePolicyPromptBlock(topics, topicContexts);
	const rewriteCtaRule = includeOutro
		? "- Do NOT add engagement questions, like requests, subscribe requests, or comment CTAs inside the content; the separate closing line handles that. End with a clean takeaway, hopeful implication, or open loop."
		: "- End the last segment of each topic with a short engagement question. Do NOT add like/subscribe CTAs.";

	const rewritePrompt = `
Improve this script for clarity, concrete detail, and spoken flow.
Quality-led narration budget: ~${Number(narrationTargetSec || 0).toFixed(1)}s
Mood: ${mood}
Category: ${categoryLabel || "General"}
Topic summaries:
${topicSummaries.join("\n")}

${sourcePolicy.text}

${trendSignalLabel}
${trendSignalLines}

${titlePromiseGuide}

${promptBriefGuide}

${directAnswerGuide}

${priorVideoGuide}

${retentionGuide}

Topic assignment by segment (do NOT change):
${topicLine}

Per-segment word caps: ${capsLine}

Rules:
- Keep EXACTLY ${segments.length} segments with the same indexes.
- Keep the same topic order and assignments.
- No redundancy: each segment adds a new detail or angle with concrete, interesting facts.
- Add one fresh, concrete detail or implication per segment when possible.
- Structure each topic around one clear angle; keep facts in service of that angle.
- Keep the rewrite brisk and adult. Compress obvious reassurance, remove slow schoolteacher phrasing, and make the strongest supported tension arrive earlier.
- Treat duration as a quality-led budget, not a hard target. Do not add padding; keep only material that improves retention, clarity, curiosity, usefulness, or payoff.
- Let the quality-first expansion room help only when it improves the content. If the topic can earn a longer runtime, use extra time for sharper reveals, clearer stakes, better examples, or stronger payoff; never use it for filler or slow setup.
- Add controlled controversy where it is fair: name the tradeoff, incentive, disagreement, or cost, then clearly separate confirmed facts from analysis or uncertainty.
- Remove stock bridge phrases like "that is the turn", "the next detail changes how...", or repeated "the answer depends..." phrasing. Replace them with topic-specific, human transitions.
- Do not reuse any sentence verbatim across segments.
- ${answerRewriteRule}
- Keep at least one clip-ready line per topic that ends with an open loop.
- Prefer specific nouns over vague hype phrases.
- Keep the opening controlled, but make segment 0 genuinely hooky and curiosity-driven instead of flat.
- Preserve any frontend-requested opening line as the first spoken line of the overall opening unit, and do not repeat it inside segment 0 when the intro already uses it.
- Preserve any frontend-requested title unless a factual correction is necessary.
- Include mandatory or memorable lines naturally. Do not say labels like "memorable line" or "must include" in the spoken script.
- Keep the overall delivery controlled and professional; avoid excited phrasing and exclamation points, but let the writing feel sharp and engaging.
- If the story is controversial and source links exist, surface what people are arguing about, what the reporting supports, and what still is not fully settled. Without source links, frame the debate as a high-level tension, not sourced reporting.
- For death, injury, legal, health, or public-safety stories, keep the rewrite sober: confirmed facts first, uncertainty clearly labeled, no emotional overacting, and one human/community implication when sourced.
- If the headline promises "what it means", include a practical implication, not only a caution that details are limited.
- If the request, script title, or final title promises price, release details, availability, or what to expect, directly answer that promise with sourced details when available. If context does not confirm a detail, say that clearly instead of skipping it.
- Add source attribution only when that topic is marked with source links in the Source policy. Do not invent named outlets, journals, studies, researchers, or "reporting" for topics with no source links.
- Attribute the outlet or analyst, not the hosting platform. Never use phrases like "According to YouTube" or "According to TikTok".
- Rewrite keyword-style phrasing into natural spoken language that a presenter and ElevenLabs voice can deliver cleanly.
- Avoid scoreboard shorthand, symbol-heavy phrasing, and awkward query fragments.
- Write for spoken delivery, not article subheads. Never start a segment with a headline-style label plus a colon.
- Never include image-search hints, visual cue labels, overlay query text, or anchor-image language in spoken narration.
- Avoid abstract or unnatural spoken phrasing such as "loss circle" or stiff setups like "the angle today is".
- Let each segment land cleanly as a spoken beat; if a thought needs one more clause to feel finished, keep it complete instead of clipping it short.
- Do not repeat a word or short phrase back-to-back.
- If you mention rumors or estimates, label them clearly as unconfirmed.
- If a topic is real-world, do NOT use in-universe/fictional framing or words like "in-universe", "fictional", "plotline", "storyline", "canon", "lore".
- Keep it conversational and clear; no filler words.
- Opening unit discipline: the generated intro plus segments 0-2 must stay clean, explicit, curiosity-driven, and entertaining without hesitation sounds, stall words, fake pauses, or annoying verbal padding.
- Do NOT introduce the presenter or host by name. A single brief greeting like "Hi guys, today we are talking about..." is allowed only in the generated intro, then open with the topic tension immediately. Segment 0 should not repeat the greeting.
- Avoid artificial dramatic pause writing in segments 0-2: no ellipses, repeated dashes, isolated one-word fragments, or sentences that need a long silence to land.
- Keep the first beat simple and human: calm if serious, lightly warm if hopeful, always restrained and professional.
- For entertainment topics, allow brief grounded opinionated framing when it helps explain why people are divided, but keep it fair and separate from sourced facts.
${rewriteCtaRule}
- Category-specific guidance:
${categoryGuide.lines.join("\n")}

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
		topicIntents,
	);
	updated = enforceRealWorldFraming(updated, topicContextFlags);
	updated = ensureTopicEngagementQuestions(updated, topics, mood, wordCaps, {
		skipFinalTopicQuestion: includeOutro,
	});
	updated = enforceSegmentCompleteness(updated, mood, {
		includeCta: !includeOutro,
	});
	updated = limitFillerAndEmotesAcrossSegments(updated, {
		maxFillers: MAX_FILLER_WORDS_PER_VIDEO,
		maxFillersPerSegment: MAX_FILLER_WORDS_PER_SEGMENT,
		maxEmotes: MAX_MICRO_EMOTES_PER_VIDEO,
		maxEmotesPerSegment: MAX_MICRO_EMOTES_PER_VIDEO,
		noFillerSegmentIndices: OPENING_NO_FILLER_SEGMENT_INDICES,
	});
	updated = updated.map((s) => ({
		...s,
		text: sanitizeSegmentText(s.text),
	}));

	const promptAligned = applyPromptBriefToScript({
		script: { ...script, segments: updated },
		topics,
		wordCaps,
	});
	const directAligned = repairDirectAnswerOpening({
		script: promptAligned,
		topics,
		topicContexts,
		wordCaps,
	}).script;
	return applyPriorVideoReferenceToScript({
		script: directAligned,
		priorVideoPlan,
		wordCaps,
	});
}

/* ---------------------------------------------------------------
 * ElevenLabs TTS -> WAV (silence removed) -> global atempo
 * ------------------------------------------------------------- */

function buildVoiceSettingsForExpression(
	expression = "neutral",
	mood = "neutral",
	text = "",
	opts = {},
) {
	const uniform = Boolean(opts?.uniform);
	const forceNeutral = Boolean(opts?.forceNeutral);
	const naturalExpr = coerceExpressionForNaturalness(expression, text, mood);
	const expr = forceNeutral && naturalExpr === "excited" ? "warm" : naturalExpr;
	let stability = ELEVEN_TTS_STABILITY;
	let style = ELEVEN_TTS_STYLE;
	const expressionScale = forceNeutral ? 0.5 : 1;

	if (uniform) {
		stability += 0.04;
		style -= 0.03;
	}

	switch (expr) {
		case "warm":
			stability -= 0.05 * expressionScale;
			style += 0.05 * expressionScale;
			break;
		case "excited":
			stability -= 0.1 * expressionScale;
			style += 0.08 * expressionScale;
			break;
		case "serious":
			stability += 0.05 * expressionScale;
			style -= 0.04 * expressionScale;
			break;
		case "thoughtful":
			stability += 0.02 * expressionScale;
			style += 0.02 * expressionScale;
			break;
		default:
			stability -= uniform ? 0 : 0.02;
			style += uniform ? 0 : 0.01;
			break;
	}

	return {
		stability: clampNumber(stability, 0.1, 1),
		similarity_boost: clampNumber(ELEVEN_TTS_SIMILARITY, 0.1, 1),
		style: clampNumber(style, 0, 0.35),
		speed: ELEVEN_TTS_SPEED,
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
	ensureDir(tmpDir);
	const safeLabel = String(label || "tts").replace(/[^a-z0-9_-]/gi, "");
	const baseText = stripAllFillers(text);
	const maxAttempts = AUDIO_QA_ENABLED ? AUDIO_QA_MAX_ATTEMPTS : 1;
	let best = null;
	let bestScore = -Infinity;

	for (let attempt = 0; attempt < maxAttempts; attempt++) {
		const attemptLabel = attempt ? `${safeLabel}_a${attempt}` : safeLabel;
		const mp3 = path.join(tmpDir, `${attemptLabel}_${jobId}.mp3`);
		const wav = path.join(tmpDir, `${attemptLabel}_${jobId}.wav`);
		const ttsText = attempt === 0 ? baseText : tightenTtsText(baseText);
		const attemptVoiceSettings = tightenVoiceSettings(voiceSettings, attempt);

		const usedModelId = await elevenLabsTTS({
			text: ttsText,
			outMp3Path: mp3,
			voiceId,
			voiceSettings: attemptVoiceSettings,
			modelId,
			modelOrder,
		});
		await mp3ToCleanWav(mp3, wav);
		safeUnlink(mp3);
		const durationSec = await probeDurationSeconds(wav);
		let qa = AUDIO_QA_ENABLED
			? await analyzeAudioQuality({
					wavPath: wav,
					expectedText: ttsText,
					jobId,
					label: safeLabel,
				})
			: { pass: true, issues: [] };
		let activeWav = wav;
		let activeDurationSec = durationSec;

		if (jobId && AUDIO_QA_ENABLED) {
			logJob(jobId, "tts audio qa", {
				label: safeLabel,
				attempt,
				pass: qa.pass,
				issues: qa.issues,
				maxInternalSilenceSec: Number(
					(qa.maxInternalSilenceSec || 0).toFixed(3),
				),
				similarity: Number((qa.similarity || 0).toFixed(3)),
			});
		}

		if (!qa.pass && Array.isArray(qa.issues)) {
			if (qa.issues.includes("long_internal_silence")) {
				try {
					const tightened = await tightenInternalSilenceWav({
						wavPath: wav,
						tmpDir,
						jobId,
						label: attemptLabel,
					});
					if (tightened?.changed && tightened?.wavPath) {
						const tightenedQa = await analyzeAudioQuality({
							wavPath: tightened.wavPath,
							expectedText: ttsText,
							jobId,
							label: `${safeLabel}_tight`,
						});
						if (jobId && AUDIO_QA_ENABLED) {
							logJob(jobId, "tts audio qa tightened", {
								label: safeLabel,
								attempt,
								pass: tightenedQa.pass,
								issues: tightenedQa.issues,
								maxInternalSilenceSec: Number(
									(tightenedQa.maxInternalSilenceSec || 0).toFixed(3),
								),
								similarity: Number((tightenedQa.similarity || 0).toFixed(3)),
							});
						}
						if (tightenedQa.pass) {
							safeUnlink(wav);
							return {
								wavPath: tightened.wavPath,
								durationSec: tightened.durationSec || activeDurationSec,
								modelId: usedModelId,
								text: ttsText,
								qa: tightenedQa,
							};
						}
						const scoreOriginal =
							(qa.similarity || 0) -
							(qa.maxInternalSilenceSec || 0) * 0.5 -
							(qa.issues?.length || 0);
						const scoreTight =
							(tightenedQa.similarity || 0) -
							(tightenedQa.maxInternalSilenceSec || 0) * 0.5 -
							(tightenedQa.issues?.length || 0);
						if (scoreTight > scoreOriginal) {
							safeUnlink(wav);
							activeWav = tightened.wavPath;
							activeDurationSec = tightened.durationSec || activeDurationSec;
							qa = tightenedQa;
						} else {
							safeUnlink(tightened.wavPath);
						}
					}
				} catch (e) {
					if (jobId) {
						logJob(jobId, "tts audio tighten failed", {
							label: safeLabel,
							attempt,
							error: e?.message || String(e),
						});
					}
				}
			}
		}

		if (qa.pass) {
			if (best?.wavPath && best.wavPath !== activeWav) safeUnlink(best.wavPath);
			return {
				wavPath: activeWav,
				durationSec: activeDurationSec,
				modelId: usedModelId,
				text: ttsText,
				qa,
			};
		}

		const score =
			(qa.similarity || 0) -
			(qa.maxInternalSilenceSec || 0) * 0.5 -
			(qa.issues?.length || 0);
		if (!best || score > bestScore) {
			if (best?.wavPath && best.wavPath !== activeWav) safeUnlink(best.wavPath);
			best = {
				wavPath: activeWav,
				durationSec: activeDurationSec,
				modelId: usedModelId,
				text: ttsText,
				qa,
			};
			bestScore = score;
		} else {
			safeUnlink(activeWav);
		}
	}

	if (best) return best;
	return {
		wavPath: "",
		durationSec: 0,
		modelId: "",
		text: baseText,
		qa: { pass: false, issues: ["tts_failed"] },
	};
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
	ensureDir(tmpDir);
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
		{ timeoutMs: 60000 },
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
		process.env.YOUTUBE_REDIRECT_URI,
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
		new Set(list.map((t) => String(t || "").trim()).filter(Boolean)),
	).slice(0, 15);
}

const VALID_YOUTUBE_CATEGORY_IDS = new Set([
	"1",
	"2",
	"10",
	"15",
	"17",
	"19",
	"20",
	"22",
	"23",
	"24",
	"25",
	"26",
	"27",
	"28",
	"29",
]);

function resolveYouTubeUploadCategoryId(category) {
	const mapped = String(YT_CATEGORY_MAP[category] || "").trim();
	if (!mapped || mapped === "0") return "22";
	return VALID_YOUTUBE_CATEGORY_IDS.has(mapped) ? mapped : "22";
}

async function uploadToYouTube(
	u,
	fp,
	{ title, description, tags, category, thumbnailPath, jobId },
) {
	const o = buildYouTubeOAuth2Client(u);
	if (!o) throw new Error("YouTube OAuth missing");
	const yt = google.youtube({ version: "v3", auth: o });
	const safeTitle = String(title || "")
		.trim()
		.slice(0, 95);
	const safeDescription = ensureClickableLinks(description);
	const safeTags = normalizeYouTubeTags(tags);
	const categoryId = resolveYouTubeUploadCategoryId(category);
	if (YT_CATEGORY_MAP[category] && String(YT_CATEGORY_MAP[category]) !== categoryId) {
		logJob(jobId, "youtube category fallback", {
			category,
			mappedCategoryId: String(YT_CATEGORY_MAP[category]),
			categoryId,
		});
	}
	const { data } = await yt.videos.insert(
		{
			part: ["snippet", "status"],
			requestBody: {
				snippet: {
					title: safeTitle || "Untitled",
					description: safeDescription,
					tags: safeTags,
					categoryId,
				},
				status: { privacyStatus: "public", selfDeclaredMadeForKids: false },
			},
			media: { body: fs.createReadStream(fp) },
		},
		{ maxContentLength: Infinity, maxBodyLength: Infinity },
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

async function buildSeoMetadata({
	topics = [],
	scriptTitle,
	scriptText = "",
	languageLabel,
	lockTitle = false,
	titleInstructions = [],
	priorVideoPlan = null,
	categoryLabel = "",
	topicContexts = [],
}) {
	let seoTitle = String(scriptTitle || "").trim();
	const topicLine = topics
		.map((t) => t.displayTopic || t.topic)
		.filter(Boolean)
		.join(" | ");
	const scriptSupport = normalizeWhitespace(scriptText).slice(0, 2200);
	const isEvergreenExplainer = isEvergreenNonNewsVisualTopic({
		category: categoryLabel,
		topicLabel: topicLine,
		text: scriptSupport,
	});
	const formatLabel = isEvergreenExplainer
		? "educational explainer"
		: "long-form news brief";
	const sourcePolicy = buildTopicSourcePolicyPromptBlock(topics, topicContexts);
	const titleInstructionLines = uniqueStrings(
		(Array.isArray(titleInstructions) ? titleInstructions : [titleInstructions])
			.map((line) => normalizeWhitespace(line))
			.filter(Boolean),
		{ limit: 5 },
	);
	const frontendTitleGuide = titleInstructionLines.length
		? `\nFrontend title guidance: ${titleInstructionLines.join(" ")}\nTreat this as an instruction to generate a strong title, not as the literal title text.`
		: "";
	const priorDescriptionBlock = buildPriorVideoDescriptionBlock(priorVideoPlan);
	const priorSeoGuide = priorVideoPlan?.hasPriorVideos
		? `\nThis is a fresh follow-up to similar previously published video(s). Do not make the title or description sound like the same episode again. Fresh angle: ${
				priorVideoPlan.noveltyAngle || "new examples and practical value"
			}.`
		: "";

	if (process.env.CHATGPT_API_TOKEN && !lockTitle) {
		try {
			const titlePrompt = `Write ONE SEO-friendly YouTube title (max 90 characters) for a ${formatLabel} covering: ${topicLine}.
Use natural search phrasing, clean punctuation, and human headline case. No quotes, no hashtags.
The title must be fully supported by the script excerpt below. Do NOT promise price, release date, availability, allegations, scores, or "what to expect" unless the script explicitly covers that detail. If the script is mostly analysis/reaction, title it as analysis/reaction.
${frontendTitleGuide}
${priorSeoGuide}
Script excerpt:
${scriptSupport || "(none)"}`;
			const titleResp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: titlePrompt }],
			});
			const t = String(titleResp.choices?.[0]?.message?.content || "")
				.replace(/["']/g, "")
				.trim();
			if (t) seoTitle = formatHumanTitle(t, 90) || seoTitle;
		} catch {}
	}
	seoTitle =
		formatHumanTitle(seoTitle, 90) || formatHumanTitle(scriptTitle, 90);

	let seoDescription = "";
	if (process.env.CHATGPT_API_TOKEN) {
		try {
			const descPrompt = `Write a YouTube description (max 180 words) for a ${formatLabel} titled "${seoTitle}".
Make the first 2 lines keyword-rich for search. Use short sentences. Add a friendly CTA to comment and like (not pushy). End with 5-7 relevant hashtags.
${priorVideoPlan?.hasPriorVideos ? "This is a follow-up, so mention that the video adds a fresh angle without inventing details. A related-video links block will be appended separately." : ""}
${sourcePolicy.text}
Do not call this breaking news or cite named journals, studies, researchers, outlets, or reporting unless the source policy says that source links exist.
Only mention facts that are supported by this script excerpt:
${scriptSupport || "(none)"}`;
			const descResp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: descPrompt }],
			});
			const descRaw = String(descResp.choices?.[0]?.message?.content || "")
				.trim()
				.replace(/\n{3,}/g, "\n\n");
			const referenceBlock = priorDescriptionBlock
				? `\n\n${priorDescriptionBlock}`
				: "";
			seoDescription = ensureClickableLinks(
				`${MERCH_INTRO}${descRaw}${referenceBlock}\n\n${BRAND_CREDIT}`,
			);
		} catch {}
	}
	if (!seoDescription) {
		const referenceBlock = priorDescriptionBlock
			? `\n\n${priorDescriptionBlock}`
			: "";
		seoDescription = ensureClickableLinks(
			`${MERCH_INTRO}${seoTitle}\n\nTell me your take and tap like if this helped.${referenceBlock}\n\n${BRAND_CREDIT}`,
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
				stripCodeFence(tagResp.choices?.[0]?.message?.content || ""),
			);
			if (Array.isArray(parsed)) tags.push(...parsed);
		} catch {}
	}

	if (!tags.includes(BRAND_TAG)) tags.unshift(BRAND_TAG);
	tags = [...new Set(tags.filter(Boolean).map((t) => String(t).trim()))];

	return { seoTitle, seoDescription, tags, languageLabel };
}

const SMALL_NUMBER_WORDS = [
	"zero",
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
];
const TENS_NUMBER_WORDS = [
	"",
	"",
	"twenty",
	"thirty",
	"forty",
	"fifty",
	"sixty",
	"seventy",
	"eighty",
	"ninety",
];

function twoDigitNumberToWords(value) {
	const n = Number(value);
	if (!Number.isFinite(n)) return "";
	const v = Math.round(n);
	if (v < 0 || v >= 100) return "";
	if (v < 20) return SMALL_NUMBER_WORDS[v];
	const tens = Math.floor(v / 10);
	const ones = v % 10;
	return ones
		? `${TENS_NUMBER_WORDS[tens]} ${SMALL_NUMBER_WORDS[ones]}`
		: TENS_NUMBER_WORDS[tens];
}

function yearToWords(year) {
	const y = Number(year);
	if (!Number.isFinite(y)) return "";
	if (y >= 2000 && y <= 2009) {
		const tail = y % 100;
		return tail
			? `two thousand ${twoDigitNumberToWords(tail)}`
			: "two thousand";
	}
	if (y >= 2010 && y <= 2099) {
		const tail = y % 100;
		return `twenty ${twoDigitNumberToWords(tail)}`.trim();
	}
	if (y >= 1900 && y <= 1999) {
		const tail = y % 100;
		if (tail === 0) return "nineteen hundred";
		return `nineteen ${twoDigitNumberToWords(tail)}`.trim();
	}
	return String(year);
}

function numberToWordsUnder1000(value) {
	const n = Number(value);
	if (!Number.isFinite(n)) return String(value);
	const v = Math.round(n);
	if (v < 0 || v >= 1000) return String(v);
	if (v < 100) return twoDigitNumberToWords(v);
	const hundreds = Math.floor(v / 100);
	const rest = v % 100;
	const prefix = `${SMALL_NUMBER_WORDS[hundreds]} hundred`;
	return rest ? `${prefix} ${twoDigitNumberToWords(rest)}` : prefix;
}

function normalizeSymbolPhrasesForSpeech(text = "") {
	let t = String(text || "");
	t = t.replace(/[\u201C\u201D]/g, '"');
	t = t.replace(/[\u2018\u2019]/g, "'");
	t = t.replace(/\bvs\.?\b/gi, "versus");
	t = t.replace(/\bNo\.\s*(\d{1,3})\b/g, (_, value) => {
		return `number ${numberToWordsUnder1000(value)}`;
	});
	t = t.replace(/\b(\d{1,3})\s*[\u2013-]\s*(\d{1,3})\b/g, (_, left, right) => {
		return `${numberToWordsUnder1000(left)} to ${numberToWordsUnder1000(right)}`;
	});
	t = t.replace(/\b(\d{1,3})%\b/g, (_, value) => {
		return `${numberToWordsUnder1000(value)} percent`;
	});
	return t;
}

function normalizeNumbersForSpeech(text = "") {
	let t = String(text || "");
	t = normalizeSymbolPhrasesForSpeech(t);
	t = t.replace(/\b([A-Za-z]{2,})\+\b/g, "$1 plus");
	t = t.replace(/\b(19\d{2}|20\d{2})\b/g, (m) => yearToWords(m));
	return t;
}

function tightenTtsText(text = "") {
	let t = String(text || "");
	t = t.replace(/[;:]/g, ".");
	t = t.replace(/,\s*/g, " ");
	t = t.replace(/\s*--\s*/g, " ");
	t = t.replace(/[()]/g, " ");
	t = t.replace(/\s+/g, " ").trim();
	return t;
}

function cleanForTTS(text = "") {
	let t = String(text || "");
	if (FORCE_NEUTRAL_VOICEOVER) t = softenNeutralPunctuation(t);
	t = normalizeSymbolPhrasesForSpeech(t);
	// remove URLs/emails
	t = t.replace(/(https?:\/\/\S+|www\.[^\s]+|\S+@\S+\.\S+)/gi, " ");
	// normalize numeric speech cues (years, plus sign)
	t = normalizeNumbersForSpeech(t);
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

function softenNeutralPunctuation(text = "") {
	let t = String(text || "");
	t = t.replace(/!\?/g, "?");
	t = t.replace(/\?!/g, "?");
	t = t.replace(/!+/g, ".");
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
				ensureDir(path.dirname(outMp3Path));
				safeUnlink(outMp3Path);
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
	ensureDir(path.dirname(wavPath));
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
		{ timeoutMs: 120000 },
	);
}

async function trimLeadingSilenceWav(inWav, outWav) {
	const trimLead = `silenceremove=start_periods=1:start_duration=${LEAD_SILENCE_MIN_SEC}:start_threshold=${LEAD_SILENCE_THRESHOLD_DB}dB`;
	const af = [
		`aresample=${AUDIO_SR}`,
		"aformat=channel_layouts=mono",
		trimLead,
	].join(",");

	await spawnBin(
		ffmpegPath,
		[
			"-i",
			inWav,
			"-vn",
			"-af",
			af,
			"-acodec",
			"pcm_s16le",
			"-ar",
			String(AUDIO_SR),
			"-ac",
			String(AUDIO_CHANNELS),
			"-y",
			outWav,
		],
		"trim_lead_silence",
		{ timeoutMs: 120000 },
	);
	return outWav;
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
		{ timeoutMs: 120000 },
	);
	return outWav;
}

function tightenVoiceSettings(settings = {}, attempt = 0) {
	const base = settings || {};
	if (!attempt || UNIFORM_TTS_VOICE_SETTINGS) return base;
	const stability = Number(base.stability ?? ELEVEN_TTS_STABILITY);
	const style = Number(base.style ?? ELEVEN_TTS_STYLE);
	return {
		...base,
		stability: clampNumber(
			stability + AUDIO_QA_STRICT_STABILITY_BOOST * attempt,
			0.1,
			1,
		),
		style: clampNumber(Math.min(style, AUDIO_QA_STRICT_STYLE_MAX), 0, 0.35),
	};
}

function buildExpectedQaTokens(text = "") {
	const base = tokenizeQaText(text);
	const normalized = tokenizeQaText(normalizeNumbersForSpeech(text));
	const cleaned = tokenizeQaText(cleanForTTS(text));
	return uniqueStrings([...base, ...normalized, ...cleaned]);
}

function hasFillerWords(text = "") {
	if (!text) return false;
	const rx = new RegExp(FILLER_WORD_REGEX.source, "i");
	return rx.test(String(text || ""));
}

function estimateSpeechSliceWeight(text = "") {
	const cleaned = sanitizeSegmentText(text);
	const words = Math.max(1, countWords(cleaned));
	const sentencePauses = (cleaned.match(/[.!?]+/g) || []).length;
	const clausePauses = (cleaned.match(/[,:;]+/g) || []).length;
	return words + sentencePauses * 2.5 + clausePauses * 0.75;
}

function buildVoiceoverSliceDurations(segments = [], totalDurationSec = 0) {
	const safeSegments = Array.isArray(segments) ? segments : [];
	const total = Math.max(0.2, Number(totalDurationSec) || 0.2);
	if (!safeSegments.length) return [];

	const weights = safeSegments.map((seg) =>
		estimateSpeechSliceWeight(seg?.text || ""),
	);
	const weightSum =
		weights.reduce((sum, weight) => sum + (Number(weight) || 0), 0) ||
		safeSegments.length;
	const minSliceSec = Math.min(
		1.1,
		Math.max(0.35, total / Math.max(safeSegments.length * 2.8, 1)),
	);
	let durations = weights.map(
		(weight) => ((Number(weight) || 0) / weightSum) * total,
	);
	durations = durations.map((dur) => Math.max(minSliceSec, dur));

	const scaledTotal =
		durations.reduce((sum, dur) => sum + (Number(dur) || 0), 0) || total;
	const scale = total / scaledTotal;
	durations = durations.map((dur) => dur * scale);

	let assigned = 0;
	return durations.map((dur, idx) => {
		if (idx === durations.length - 1) {
			return Math.max(0.15, Number((total - assigned).toFixed(3)));
		}
		const rounded = Math.max(0.15, Number(dur.toFixed(3)));
		assigned += rounded;
		return rounded;
	});
}

async function transcribeAudioForQa(audioPath, jobId, label) {
	if (!AUDIO_QA_TRANSCRIBE) return "";
	if (!openai?.audio?.transcriptions?.create) return "";
	if (Date.now() < audioQaTranscribeDisabledUntil) {
		return "";
	}
	const models = [AUDIO_QA_TRANSCRIBE_MODEL, "whisper-1"]
		.filter(Boolean)
		.filter((v, i, arr) => arr.indexOf(v) === i);
	let lastErr = null;
	for (const model of models) {
		try {
			const resp = await openai.audio.transcriptions.create({
				file: fs.createReadStream(audioPath),
				model,
				response_format: "text",
				temperature: 0,
			});
			if (typeof resp === "string") return resp.trim();
			return String(resp?.text || "").trim();
		} catch (e) {
			lastErr = e;
		}
	}
	if (jobId && lastErr) {
		logJob(jobId, "audio qa transcription failed", {
			label,
			error: lastErr.message,
		});
	}
	if (lastErr && isOpenAiQuotaOrRateLimitError(lastErr)) {
		audioQaTranscribeDisabledUntil =
			Date.now() + AUDIO_QA_TRANSCRIBE_COOLDOWN_MS;
		audioQaTranscribeDisabledReason = lastErr.message || "openai_quota";
		if (jobId) {
			logJob(jobId, "audio qa transcription disabled temporarily", {
				label,
				cooldownSec: Math.round(AUDIO_QA_TRANSCRIBE_COOLDOWN_MS / 1000),
				error: audioQaTranscribeDisabledReason,
			});
		}
	}
	return "";
}

async function detectInternalSilence(
	wavPath,
	{
		minSilenceSec = AUDIO_QA_INTERNAL_SILENCE_SEC,
		noiseDb = AUDIO_QA_INTERNAL_SILENCE_DB,
		edgeBufferSec = AUDIO_QA_EDGE_BUFFER_SEC,
	} = {},
) {
	const durationSec = await probeDurationSeconds(wavPath);
	if (!durationSec || !Number.isFinite(durationSec)) {
		return { durationSec: 0, maxInternalSilenceSec: 0, internalSilences: [] };
	}
	const res = await spawnBin(
		ffmpegPath,
		[
			"-i",
			wavPath,
			"-af",
			`silencedetect=noise=${Number(noiseDb)}dB:d=${Number(minSilenceSec)}`,
			"-f",
			"null",
			"-",
		],
		"silence_detect",
		{ timeoutMs: 60000 },
	);
	const stderr = String(res?.stderr || "");
	const lines = stderr.split(/\r?\n/);
	const silences = [];
	let currentStart = null;

	for (const line of lines) {
		const startMatch = line.match(/silence_start:\s*([0-9.]+)/);
		if (startMatch) {
			currentStart = Number(startMatch[1]);
		}
		const endMatch = line.match(
			/silence_end:\s*([0-9.]+)\s*\|\s*silence_duration:\s*([0-9.]+)/,
		);
		if (endMatch) {
			const end = Number(endMatch[1]);
			const duration = Number(endMatch[2]);
			const start =
				Number.isFinite(currentStart) && currentStart >= 0
					? currentStart
					: end - duration;
			silences.push({ start, end, duration });
			currentStart = null;
		}
	}

	const edge = Math.max(0, Number(edgeBufferSec) || 0);
	const internal = silences.filter(
		(s) => s.start > edge && s.end < durationSec - edge,
	);
	const maxInternal = internal.reduce(
		(max, s) => Math.max(max, Number(s.duration) || 0),
		0,
	);

	return {
		durationSec,
		maxInternalSilenceSec: maxInternal,
		internalSilences: internal,
	};
}

function mergeSilenceIntervals(silences = []) {
	const sorted = (silences || [])
		.map((s) => ({
			start: Number(s?.start) || 0,
			end: Number(s?.end) || 0,
		}))
		.filter((s) => Number.isFinite(s.start) && Number.isFinite(s.end))
		.map((s) => ({
			start: Math.max(0, s.start),
			end: Math.max(0, s.end),
		}))
		.sort((a, b) => a.start - b.start);

	const merged = [];
	for (const s of sorted) {
		if (!merged.length) {
			merged.push({ ...s, duration: Math.max(0, s.end - s.start) });
			continue;
		}
		const last = merged[merged.length - 1];
		if (s.start <= last.end + 0.02) {
			last.end = Math.max(last.end, s.end);
			last.duration = Math.max(0, last.end - last.start);
		} else {
			merged.push({ ...s, duration: Math.max(0, s.end - s.start) });
		}
	}
	return merged;
}

async function tightenInternalSilenceWav({
	wavPath,
	tmpDir,
	jobId,
	label,
	maxSilenceSec = AUDIO_QA_REPAIR_MAX_SILENCE_SEC,
}) {
	const silenceInfo = await detectInternalSilence(wavPath);
	const durationSec = silenceInfo.durationSec || 0;
	const longSilences = mergeSilenceIntervals(silenceInfo.internalSilences || [])
		.filter((s) => (s.duration || 0) >= AUDIO_QA_INTERNAL_SILENCE_SEC)
		.filter((s) => s.start < s.end);

	if (!longSilences.length || !durationSec) {
		return { wavPath, durationSec, changed: false };
	}

	const safeLabel = String(label || "tts").replace(/[^a-z0-9_-]/gi, "");
	const outPath = path.join(
		tmpDir,
		`${safeLabel}_tight_${jobId || "audio"}.wav`,
	);
	const minSeg = 0.02;
	const keepSilence = clampNumber(maxSilenceSec, 0.08, 0.5);
	let cursor = 0;
	const segments = [];

	for (const silence of longSilences) {
		const start = Math.max(0, Math.min(durationSec, silence.start));
		const end = Math.max(start, Math.min(durationSec, silence.end));
		if (start - cursor >= minSeg) {
			segments.push({ type: "audio", start: cursor, end: start });
		}
		if (keepSilence >= minSeg) {
			segments.push({ type: "silence", duration: keepSilence });
		}
		cursor = end;
	}

	if (durationSec - cursor >= minSeg) {
		segments.push({ type: "audio", start: cursor, end: durationSec });
	}

	if (!segments.length || !segments.some((s) => s.type === "audio")) {
		return { wavPath, durationSec, changed: false };
	}

	const filterParts = [];
	const labels = [];
	let idx = 0;

	for (const seg of segments) {
		if (seg.type === "audio") {
			const aLabel = `a${idx}`;
			filterParts.push(
				`[0:a]atrim=start=${seg.start.toFixed(3)}:end=${seg.end.toFixed(
					3,
				)},asetpts=PTS-STARTPTS[${aLabel}]`,
			);
			labels.push(`[${aLabel}]`);
		} else {
			const sLabel = `s${idx}`;
			filterParts.push(
				`anullsrc=r=${AUDIO_SR}:cl=mono:d=${seg.duration.toFixed(3)}[${sLabel}]`,
			);
			labels.push(`[${sLabel}]`);
		}
		idx += 1;
	}

	filterParts.push(
		`${labels.join("")}concat=n=${
			labels.length
		}:v=0:a=1,aresample=${AUDIO_SR},aformat=channel_layouts=mono[clean]`,
	);

	await spawnBin(
		ffmpegPath,
		[
			"-i",
			wavPath,
			"-filter_complex",
			filterParts.join(";"),
			"-map",
			"[clean]",
			"-acodec",
			"pcm_s16le",
			"-ar",
			String(AUDIO_SR),
			"-ac",
			String(AUDIO_CHANNELS),
			"-y",
			outPath,
		],
		"tighten_internal_silence",
		{ timeoutMs: 120000 },
	);

	const outDuration = await probeDurationSeconds(outPath);
	return { wavPath: outPath, durationSec: outDuration, changed: true };
}

async function analyzeAudioQuality({ wavPath, expectedText, jobId, label }) {
	const result = {
		pass: true,
		issues: [],
		maxInternalSilenceSec: 0,
		similarity: 1,
		transcript: "",
	};
	try {
		const silence = await detectInternalSilence(wavPath);
		result.maxInternalSilenceSec = silence.maxInternalSilenceSec || 0;
		if (silence.maxInternalSilenceSec >= AUDIO_QA_INTERNAL_SILENCE_SEC) {
			result.issues.push("long_internal_silence");
		}
	} catch (e) {
		result.issues.push("silence_detect_failed");
	}

	try {
		if (AUDIO_QA_TRANSCRIBE && countWords(expectedText) >= AUDIO_QA_MIN_WORDS) {
			const transcript = await transcribeAudioForQa(wavPath, jobId, label);
			result.transcript = transcript;
			if (!transcript) {
				if (Date.now() < audioQaTranscribeDisabledUntil) {
					result.transcriptionSkipped = true;
					result.transcriptionSkipReason =
						audioQaTranscribeDisabledReason || "openai_quota";
				} else {
					result.issues.push("transcription_empty");
				}
			} else {
				if (hasFillerWords(transcript)) result.issues.push("filler_detected");
				const expectedTokens = buildExpectedQaTokens(expectedText);
				const actualTokens = tokenizeQaText(transcript);
				if (expectedTokens.length && actualTokens.length) {
					const similarity = overlapRatio(expectedTokens, actualTokens);
					result.similarity = similarity;
					if (similarity < AUDIO_QA_SIMILARITY_THRESHOLD) {
						result.issues.push("transcript_mismatch");
					}
				}
			}
		}
	} catch (e) {
		result.issues.push("transcription_failed");
	}

	result.pass = result.issues.length === 0;
	return result;
}

/* ---------------------------------------------------------------
 * Presenter video QA and HeyGen render helpers
 * ------------------------------------------------------------- */

async function detectFrozenVideo(
	videoPath,
	{
		noise = PRESENTER_VIDEO_FREEZE_NOISE,
		minFreezeSec = PRESENTER_VIDEO_FREEZE_MIN_SEC,
	} = {},
) {
	const durationSec = await probeDurationSeconds(videoPath);
	if (!durationSec || !Number.isFinite(durationSec)) {
		return { durationSec: 0, freezes: [], maxFreezeSec: 0, freezeRatio: 0 };
	}

	const res = await spawnBin(
		ffmpegPath,
		[
			"-i",
			videoPath,
			"-vf",
			`freezedetect=n=${Number(noise)}:d=${Number(minFreezeSec)}`,
			"-map",
			"0:v:0",
			"-f",
			"null",
			"-",
		],
		"freeze_detect",
		{ timeoutMs: 120000 },
	);

	const lines = String(res?.stderr || "").split(/\r?\n/);
	const freezes = [];
	let pending = null;

	const maybePushPending = () => {
		if (!pending) return;
		const duration =
			Number(pending.duration) ||
			(Number.isFinite(pending.start) && Number.isFinite(pending.end)
				? pending.end - pending.start
				: 0);
		if (duration > 0) {
			const start = Number.isFinite(pending.start)
				? pending.start
				: Math.max(0, Number(pending.end) - duration);
			const end = Number.isFinite(pending.end)
				? pending.end
				: Math.min(durationSec, start + duration);
			freezes.push({ start, end, duration });
		}
		pending = null;
	};

	for (const line of lines) {
		const startMatch = line.match(/freeze_start:\s*([0-9.]+)/i);
		if (startMatch) {
			maybePushPending();
			pending = { start: Number(startMatch[1]) };
		}
		const endMatch = line.match(/freeze_end:\s*([0-9.]+)/i);
		if (endMatch) {
			pending = pending || {};
			pending.end = Number(endMatch[1]);
		}
		const durationMatch = line.match(/freeze_duration:\s*([0-9.]+)/i);
		if (durationMatch) {
			pending = pending || {};
			pending.duration = Number(durationMatch[1]);
			if (pending.start != null || pending.end != null) maybePushPending();
		}
	}
	maybePushPending();

	const totalFrozenSec = freezes.reduce(
		(sum, item) => sum + Math.max(0, Number(item.duration) || 0),
		0,
	);
	const maxFreezeSec = freezes.reduce(
		(max, item) => Math.max(max, Number(item.duration) || 0),
		0,
	);

	return {
		durationSec,
		freezes,
		maxFreezeSec,
		freezeRatio: durationSec > 0 ? totalFrozenSec / durationSec : 0,
	};
}

async function analyzeLipsyncOutput({
	videoPath,
	expectedDurSec,
	jobId,
	label,
	requireMotion = false,
}) {
	const durationSec = await probeDurationSeconds(videoPath);
	const result = {
		pass: true,
		issues: [],
		durationSec: Number(durationSec || 0),
		durationDeltaSec: 0,
		maxFreezeSec: 0,
		freezeRatio: 0,
	};

	if (expectedDurSec && durationSec) {
		const delta = Math.abs(Number(expectedDurSec) - Number(durationSec));
		const ratio = Number(durationSec) / Math.max(0.01, Number(expectedDurSec));
		result.durationDeltaSec = delta;
		if (
			Number(expectedDurSec) - Number(durationSec) >
				PRESENTER_VIDEO_MAX_SHORTFALL_SEC ||
			ratio < PRESENTER_VIDEO_MIN_DURATION_RATIO
		) {
			result.issues.push("presenter_video_too_short");
		}
	}

	const motionRequired = Boolean(requireMotion && PRESENTER_MOTION_QA_ENABLED);
	const freezeCheckMinSec = motionRequired
		? PRESENTER_MOTION_FREEZE_CHECK_MIN_SEC
		: PRESENTER_VIDEO_FREEZE_CHECK_MIN_SEC;
	const freezeNoise = motionRequired
		? PRESENTER_MOTION_FREEZE_NOISE
		: PRESENTER_VIDEO_FREEZE_NOISE;
	const freezeMinSec = motionRequired
		? PRESENTER_MOTION_FREEZE_MIN_SEC
		: PRESENTER_VIDEO_FREEZE_MIN_SEC;
	const maxFreezeSec = motionRequired
		? PRESENTER_MOTION_MAX_FREEZE_SEC
		: PRESENTER_VIDEO_MAX_FREEZE_SEC;
	const maxFreezeRatio = motionRequired
		? PRESENTER_MOTION_MAX_FREEZE_RATIO
		: PRESENTER_VIDEO_MAX_FREEZE_RATIO;

	if (durationSec >= freezeCheckMinSec) {
		try {
			const freezeInfo = await detectFrozenVideo(videoPath, {
				noise: freezeNoise,
				minFreezeSec: freezeMinSec,
			});
			result.maxFreezeSec = Number(freezeInfo.maxFreezeSec || 0);
			result.freezeRatio = Number(freezeInfo.freezeRatio || 0);
			if (
				result.maxFreezeSec >= maxFreezeSec ||
				result.freezeRatio >= maxFreezeRatio
			) {
				result.issues.push("presenter_video_frozen");
			}
		} catch (e) {
			if (motionRequired) result.issues.push("presenter_motion_check_failed");
			if (jobId) {
				logJob(jobId, "presenter video freeze check failed", {
					label,
					requireMotion: motionRequired,
					error: e?.message || String(e),
				});
			}
		}
	}

	result.pass = !result.issues.length;
	return result;
}

function isPresenterFreezeNearPass(qa, mode = "rendered") {
	const issues = Array.isArray(qa?.issues) ? qa.issues : [];
	const freezeOnly =
		issues.length > 0 &&
		issues.every((issue) => issue === "presenter_video_frozen");
	if (!freezeOnly || Number(qa?.durationSec || 0) < 2) return false;

	const qaMode = String(mode || "rendered").toLowerCase();
	const maxFreezeSec =
		qaMode === "baseline"
			? PRESENTER_BASELINE_MOTION_MAX_FREEZE_SEC
			: PRESENTER_RENDER_MOTION_MAX_FREEZE_SEC;
	const maxFreezeRatio =
		qaMode === "baseline"
			? PRESENTER_BASELINE_MOTION_MAX_FREEZE_RATIO
			: PRESENTER_RENDER_MOTION_MAX_FREEZE_RATIO;

	return (
		Number(qa.maxFreezeSec || 0) <= maxFreezeSec &&
		Number(qa.freezeRatio || 0) <= maxFreezeRatio
	);
}

async function evaluatePresenterVideoMotion({
	videoPath,
	jobId,
	label,
	mode = "strict",
}) {
	if (!REQUIRE_REAL_PRESENTER_VIDEO || !PRESENTER_MOTION_QA_ENABLED) {
		return { pass: true, issues: [] };
	}
	const qaMode = String(mode || "strict").toLowerCase();
	const qa = await analyzeLipsyncOutput({
		videoPath,
		expectedDurSec: null,
		jobId,
		label,
		requireMotion: true,
	});
	const originalIssues = Array.isArray(qa.issues) ? [...qa.issues] : [];
	let acceptedNearPass = false;
	if (
		!qa.pass &&
		qaMode === "baseline" &&
		PRESENTER_BASELINE_MOTION_NEAR_PASS_ENABLED
	) {
		if (isPresenterFreezeNearPass(qa, "baseline")) {
			qa.pass = true;
			qa.issues = [];
			acceptedNearPass = true;
		}
	}
	if (
		!qa.pass &&
		qaMode === "rendered" &&
		PRESENTER_RENDER_MOTION_NEAR_PASS_ENABLED
	) {
		if (isPresenterFreezeNearPass(qa, "rendered")) {
			qa.pass = true;
			qa.issues = [];
			acceptedNearPass = true;
		}
	}
	logJob(jobId, "presenter source motion qa", {
		label,
		pass: qa.pass,
		issues: qa.issues,
		mode: qaMode,
		acceptedNearPass,
		...(acceptedNearPass ? { originalIssues } : {}),
		durationSec: Number((qa.durationSec || 0).toFixed(3)),
		maxFreezeSec: Number((qa.maxFreezeSec || 0).toFixed(3)),
		freezeRatio: Number((qa.freezeRatio || 0).toFixed(3)),
	});
	if (!qa.pass) {
		throw new Error(
			`presenter_motion_qa_failed:${label}:${qa.issues.join(",") || "unknown"}`,
		);
	}
	return { ...qa, acceptedNearPass, originalIssues };
}

async function analyzeHeyGenFinalPresenterMotion({
	videoPath,
	expectedDurSec,
	jobId,
	label,
} = {}) {
	const durationSec = await probeDurationSeconds(videoPath);
	const result = {
		pass: true,
		issues: [],
		durationSec: Number(durationSec || 0),
		durationDeltaSec: 0,
		maxFreezeSec: 0,
		freezeRatio: 0,
	};

	if (expectedDurSec && durationSec) {
		const delta = Math.abs(Number(expectedDurSec) - Number(durationSec));
		const ratio = Number(durationSec) / Math.max(0.01, Number(expectedDurSec));
		result.durationDeltaSec = delta;
		if (
			Number(expectedDurSec) - Number(durationSec) >
				PRESENTER_VIDEO_MAX_SHORTFALL_SEC ||
			ratio < PRESENTER_VIDEO_MIN_DURATION_RATIO
		) {
			result.issues.push("presenter_video_too_short");
		}
	}

	try {
		const freezeInfo = await detectFrozenVideo(videoPath, {
			noise: HEYGEN_FINAL_MOTION_FREEZE_NOISE,
			minFreezeSec: HEYGEN_FINAL_MOTION_FREEZE_MIN_SEC,
		});
		result.maxFreezeSec = Number(freezeInfo.maxFreezeSec || 0);
		result.freezeRatio = Number(freezeInfo.freezeRatio || 0);
		if (
			result.maxFreezeSec >= HEYGEN_FINAL_MOTION_MAX_FREEZE_SEC ||
			result.freezeRatio >= HEYGEN_FINAL_MOTION_MAX_FREEZE_RATIO
		) {
			result.issues.push("presenter_video_final_frozen");
		}
	} catch (e) {
		result.issues.push("presenter_final_motion_check_failed");
		if (jobId) {
			logJob(jobId, "heygen final presenter freeze check failed", {
				label,
				error: e?.message || String(e),
			});
		}
	}

	result.pass = !result.issues.length;
	return result;
}

async function extractVideoFrameForQa({
	videoPath,
	outPath,
	atSec,
	scaleWidth = 640,
}) {
	if (!videoPath || !fs.existsSync(videoPath)) {
		throw new Error("qa_video_missing");
	}
	const seekSec = Math.max(0, Number(atSec) || 0);
	await spawnBin(
		ffmpegPath,
		[
			"-ss",
			seekSec.toFixed(3),
			"-i",
			videoPath,
			"-frames:v",
			"1",
			"-vf",
			`scale=${Math.floor(scaleWidth)}:-2`,
			"-q:v",
			"3",
			"-y",
			outPath,
		],
		"extract_presenter_identity_frame",
		{ timeoutMs: 60000 },
	);
	if (!fs.existsSync(outPath)) throw new Error("qa_frame_missing");
	return outPath;
}

async function evaluatePresenterIdentityQa({
	referenceImagePath,
	videoPath,
	tmpDir,
	jobId,
	label,
	expression,
	variant,
}) {
	const baseResult = {
		pass: true,
		skipped: true,
		identityScore: 1,
		distortionScore: 0,
		reason: "",
	};
	if (!BASELINE_IDENTITY_QA_ENABLED) {
		return { ...baseResult, reason: "disabled" };
	}
	if (!process.env.CHATGPT_API_TOKEN) {
		return { ...baseResult, reason: "openai_key_missing" };
	}
	if (!referenceImagePath || !fs.existsSync(referenceImagePath)) {
		return { ...baseResult, reason: "reference_missing" };
	}
	if (!videoPath || !fs.existsSync(videoPath)) {
		throw new Error(`presenter_identity_qa_failed:${label}:video_missing`);
	}

	const safeLabel = String(label || "baseline").replace(/[^a-z0-9_-]/gi, "_");
	const framePaths = [];
	try {
		const durSec = (await probeDurationSecondsCached(videoPath)) || BASELINE_DUR_SEC;
		const sampleTimes = Array.from(
			new Set(
				[
					clampNumber(1.1, 0.1, Math.max(0.1, durSec - 0.2)),
					clampNumber(durSec * 0.58, 0.1, Math.max(0.1, durSec - 0.2)),
				].map((n) => Number((Number(n) || 0.1).toFixed(2))),
			),
		).slice(0, 2);
		for (let i = 0; i < sampleTimes.length; i++) {
			const framePath = path.join(
				tmpDir,
				`identity_${safeLabel}_${i + 1}_${jobId}.jpg`,
			);
			await extractVideoFrameForQa({
				videoPath,
				outPath: framePath,
				atSec: sampleTimes[i],
			});
			framePaths.push(framePath);
		}
		if (!framePaths.length) {
			return { ...baseResult, reason: "no_frames" };
		}

		const content = [
			{
				type: "text",
				text: `
You are a strict but practical visual QA reviewer for a YouTube presenter pipeline.
Compare the reference presenter image to the generated video frames.

Pass only if the generated frames clearly look like the same man from the reference and are usable in the same video:
- same shaved head, glasses shape/position, eye spacing, nose, beard line, mouth, jaw, face width/proportions, skin tone/texture, age, wardrobe, and studio feel
- no obvious AI distortion: warped mouth, drifting glasses, melted glasses, mismatched eyes, changed beard line, changed face shape, rubber skin, extra teeth, severe asymmetry, or a different-looking presenter

Ignore tiny normal differences from blinking, subtle expression, compression, and small pose movement.
Reject only meaningful identity drift or visible facial distortion.

Return JSON only:
{
  "pass": true,
  "identityScore": 0.0,
  "distortionScore": 0.0,
  "reason": "short practical reason"
}
identityScore is 0 to 1 where 1 is exact same person.
distortionScore is 0 to 1 where 0 is no visible distortion and 1 is severe distortion.
`.trim(),
			},
			{
				type: "image_url",
				image_url: { url: imagePathToDataUrl(referenceImagePath) },
			},
			...framePaths.map((framePath) => ({
				type: "image_url",
				image_url: { url: imagePathToDataUrl(framePath) },
			})),
		];

		const resp = await openai.chat.completions.create({
			model: BASELINE_IDENTITY_QA_MODEL,
			messages: [{ role: "user", content }],
		});
		const parsed = parseJsonFlexible(resp?.choices?.[0]?.message?.content || "");
		if (!parsed || typeof parsed !== "object") {
			logJob(jobId, "baseline identity qa skipped", {
				label,
				expression,
				variant,
				error: "identity_qa_parse_failed",
			});
			return { ...baseResult, reason: "identity_qa_parse_failed" };
		}
		const identityScore = clampNumber(
			Number(parsed?.identityScore ?? parsed?.identity_score ?? 0),
			0,
			1,
		);
		const distortionScore = clampNumber(
			Number(parsed?.distortionScore ?? parsed?.distortion_score ?? 1),
			0,
			1,
		);
		const parsedPass =
			parsed?.pass === true ||
			String(parsed?.pass || "")
				.trim()
				.toLowerCase() === "true";
		const pass =
			parsedPass &&
			identityScore >= BASELINE_IDENTITY_QA_MIN_SCORE &&
			distortionScore <= BASELINE_IDENTITY_QA_MAX_DISTORTION_SCORE;
		const reason = String(parsed?.reason || "").slice(0, 220);
		logJob(jobId, "baseline identity qa", {
			label,
			expression,
			variant,
			pass,
			identityScore: Number(identityScore.toFixed(3)),
			distortionScore: Number(distortionScore.toFixed(3)),
			minIdentityScore: BASELINE_IDENTITY_QA_MIN_SCORE,
			maxDistortionScore: BASELINE_IDENTITY_QA_MAX_DISTORTION_SCORE,
			reason,
		});
		if (!pass) {
			throw new Error(
				`presenter_identity_qa_failed:${label}:identity=${identityScore.toFixed(
					2,
				)}:distortion=${distortionScore.toFixed(2)}:${reason || "rejected"}`,
			);
		}
		return {
			pass,
			skipped: false,
			identityScore,
			distortionScore,
			reason,
		};
	} catch (e) {
		if (String(e?.message || "").startsWith("presenter_identity_qa_failed:")) {
			throw e;
		}
		logJob(jobId, "baseline identity qa skipped", {
			label,
			expression,
			variant,
			error: e?.message || String(e),
		});
		return { ...baseResult, reason: e?.message || String(e) };
	} finally {
		for (const framePath of framePaths) safeUnlink(framePath);
	}
}

async function assertPresenterVideoHasMotion({
	videoPath,
	jobId,
	label,
	mode = "strict",
}) {
	return await evaluatePresenterVideoMotion({
		videoPath,
		jobId,
		label,
		mode,
	});
}

function heygenHeaders(extra = {}) {
	return {
		"x-api-key": HEYGEN_API_KEY,
		"Content-Type": "application/json",
		...extra,
	};
}

function buildHeyGenMotionPrompt({
	role = "content",
	text = "",
	expression = "neutral",
	mood = "neutral",
	pace = "steady",
	durationSec = 0,
	silentSmileTailSec = 0,
	attempt = 1,
} = {}) {
	const cleanText = sanitizeSegmentText(text).slice(0, 700);
	const roleLabel = String(role || "content").toLowerCase();
	const expressionLabel = normalizeExpression(expression || mood || "neutral", mood);
	const paceLabel = String(pace || "steady").toLowerCase();
	const dur = Math.max(0, Number(durationSec) || 0);
	const motionAttempt = Math.max(1, Math.floor(Number(attempt) || 1));
	const emotionalDirection =
		expressionLabel === "serious" || expressionLabel === "thoughtful"
			? "more thoughtful than cheerful, with soft eyes and relaxed brows"
			: expressionLabel === "warm"
				? "warm and encouraging, with one tiny closed-mouth smile only when it feels natural"
				: "calm, attentive, and professional";
	const paceDirection =
		paceLabel === "slow" || paceLabel === "gentle"
			? "slightly slower, with clear phrase endings and comfortable pauses"
			: paceLabel === "urgent"
				? "clear and engaged, but never rushed or theatrical"
				: "clear, natural, and conversational";
	const outroTail =
		Number(silentSmileTailSec || 0) > 0.05
			? `In the final ${Number(silentSmileTailSec).toFixed(1)} seconds, stop speaking completely. Keep lips closed and relaxed, hold a light warm closed-mouth smile, show no teeth, do not mouth silent words, do not grin, and keep only tiny natural breathing and blinks.`
			: "";
	const openingDirection =
		roleLabel === "intro_first"
			? "Opening direction: begin with a brief natural greeting if the narration has one, then move straight into the topic. Keep calm attention, no long silent stare, no greeting performance, no big smile, and no exaggerated first-sentence reaction. The first seconds should feel like a professional teaser with smooth eye contact and one restrained emotional color."
			: "";
	const middlePresenterRole =
		roleLabel === "content_mid" || roleLabel === "content_required";
	const middleDirection =
		middlePresenterRole
			? "Middle presenter direction: this is a paid mid-video presenter beat, so it must stay visibly alive while still calm. Add small natural speech emphasis at phrase transitions: soft blinks, slight eye refocus, tiny bounded chin dips, subtle cheek and jaw motion, and quiet shoulder breathing. Keep the mouth and jaw visibly responding to every spoken phrase. Never hold the face like a photo."
			: "";
	const retryDirection =
		motionAttempt > 1
			? "Retry direction: the previous take was too still. Keep the same calm professional personality, but increase visible lip sync, blinks, eye refocus, tiny nods, cheek movement, and shoulder breathing throughout the entire clip. No second of the clip should look like a still photo."
			: "";
	const motionContinuity =
		roleLabel === "intro_first" || dur >= 18
			? "For a longer opening take, include two or three tiny naturally spaced chin dips, soft eye refocuses, or shoulder-breath posture settles so the clip never reads as a held photo."
			: roleLabel === "outro"
				? "For the outro, stay gentle but alive: small blinks, soft breathing, and a tiny relaxed posture settle before the final closed-mouth smile."
				: middlePresenterRole
					? "For this mid-video take, avoid stillness between sentences: keep a natural low-energy rhythm of blinks, micro nods, tiny posture settling, and restrained lip-sync movement across the full clip."
					: "For this short presenter beat, include at least one tiny natural emphasis cue while keeping the overall delivery calm.";

	return [
		`Photorealistic talking-head presenter delivery for a ${roleLabel} video segment.`,
		"Preserve the exact face, glasses, beard, head shape, skin texture, neck, shoulders, studio, lighting, and outfit from the source image.",
		"The presenter should look nice, simple, calm, credible, and human; never flashy, smug, exaggerated, theatrical, cartoonish, or overly expressive.",
		`Delivery should be ${paceDirection}. Expression should be ${emotionalDirection}.`,
		"Calm does not mean motionless: keep continuous, barely visible human micro-motion across the whole clip. Every second should show a natural cue from blinks, tiny eye refocus, jaw/cheek speech movement, neck correction, or quiet shoulder breathing.",
		middleDirection,
		motionContinuity,
		retryDirection,
		openingDirection,
		outroTail,
		"Keep expression intensity low to medium-low: no wide eyes, raised-eyebrow acting, big grin, sudden emotional jumps, or exaggerated reaction faces.",
		"Lip sync should look like normal human speech: restrained lips, smaller mouth openings, relaxed jaw, subtle chin and cheek movement on syllables, and brief closed-mouth rests at commas and periods.",
		"During natural audio pauses, keep tiny blinks, breathing, and eye focus alive; do not freeze into a still image, reset the face, stare blankly, or add a dramatic silent beat.",
		"Avoid oversized A/O vowel shapes, constant open-mouth talking, rubbery jaw travel, extra teeth, theatrical reactions, cartoon acting, warped glasses, face reshaping, neck stretching, or shoulder distortion.",
		"Use direct eye contact, soft blinks, relaxed shoulders, tiny bounded nods only when emphasis fits, and natural breathing. Keep hands low or out of frame; body movement should be subtle but not frozen.",
		cleanText ? `Narration context: ${cleanText}` : "",
	]
		.filter(Boolean)
		.join(" ");
}

function resolveHeyGenPace({ text = "", expression = "neutral", mood = "neutral" } = {}) {
	const haystack = `${text} ${expression} ${mood}`.toLowerCase();
	if (/\b(sad|grief|lonely|loneliness|struggl|worried|anxious|hard|hurt|advice|children|kids)\b/.test(haystack)) {
		return "gentle";
	}
	if (/\b(urgent|breaking|warning|danger|critical)\b/.test(haystack)) {
		return "urgent";
	}
	return "steady";
}

function buildHeyGenVideoPayload({
	title,
	imageUrl,
	audioUrl,
	resolution = HEYGEN_DEFAULT_RESOLUTION,
	aspectRatio = HEYGEN_DEFAULT_ASPECT_RATIO,
	expressiveness = HEYGEN_DEFAULT_EXPRESSIVENESS,
	fit = HEYGEN_DEFAULT_FIT,
	motionPrompt,
}) {
	if (!/^https:\/\//i.test(String(imageUrl || ""))) {
		throw new Error("HeyGen presenter image URL must be public HTTPS");
	}
	if (!/^https:\/\//i.test(String(audioUrl || ""))) {
		throw new Error("HeyGen audio URL must be public HTTPS");
	}
	return {
		type: "image",
		title: sanitizeSegmentText(title || "AgentAI presenter segment").slice(0, 120),
		resolution,
		aspect_ratio: aspectRatio,
		fit,
		output_format: "mp4",
		image: {
			type: "url",
			url: imageUrl,
		},
		audio_url: audioUrl,
		motion_prompt: String(motionPrompt || "").slice(0, 1800),
		expressiveness,
	};
}

async function uploadAudioToCloudinaryForHeyGen(audioPath, { jobId, label } = {}) {
	if (!CLOUDINARY_ENABLED) throw new Error("Cloudinary missing for HeyGen audio");
	if (!audioPath || !fs.existsSync(audioPath)) throw new Error("HeyGen audio missing");
	const safeLabel = String(label || "segment").replace(/[^a-z0-9_-]/gi, "_");
	const result = await cloudinary.uploader.upload(audioPath, {
		resource_type: "video",
		folder: "aivideomatic/long_presenter_audio",
		public_id: `heygen_${jobId || "job"}_${safeLabel}_${Date.now()}`,
		overwrite: true,
	});
	return {
		url: result.secure_url,
		publicId: result.public_id,
		bytes: result.bytes || fs.statSync(audioPath).size,
	};
}

async function createHeyGenVideo(payload, { jobId, label } = {}) {
	if (!HEYGEN_API_KEY) throw new Error("HEYGEN_API_KEY missing");
	const safeLabel = String(label || "segment").replace(/[^a-z0-9_-]/gi, "_");
	const res = await axios.post(
		`${HEYGEN_API_BASE}${HEYGEN_CREATE_VIDEO_PATH}`,
		payload,
		{
			headers: heygenHeaders({
				"Idempotency-Key": `agentai-long-${jobId || "job"}-${safeLabel}`,
			}),
			timeout: 60000,
			validateStatus: (status) => status < 500,
		},
	);
	if (res.status >= 300) {
		throw new Error(
			`HeyGen create failed (${res.status}): ${JSON.stringify(
				res.data || {},
			).slice(0, 1200)}`,
		);
	}
	const data = res.data?.data || {};
	if (!data.video_id) {
		throw new Error(`HeyGen create missing video_id: ${JSON.stringify(data)}`);
	}
	return data;
}

async function getHeyGenVideo(videoId) {
	if (!HEYGEN_API_KEY) throw new Error("HEYGEN_API_KEY missing");
	const res = await axios.get(
		`${HEYGEN_API_BASE}${HEYGEN_CREATE_VIDEO_PATH}/${encodeURIComponent(
			videoId,
		)}`,
		{
			headers: heygenHeaders(),
			timeout: 30000,
			validateStatus: (status) => status < 500,
		},
	);
	if (res.status >= 300) {
		throw new Error(
			`HeyGen poll failed (${res.status}): ${JSON.stringify(
				res.data || {},
			).slice(0, 1200)}`,
		);
	}
	return res.data?.data || {};
}

async function pollHeyGenVideo({ videoId, jobId, label }) {
	const startedAt = Date.now();
	let lastStatus = "";
	while (Date.now() - startedAt < HEYGEN_POLL_TIMEOUT_MS) {
		const data = await getHeyGenVideo(videoId);
		const status = String(data.status || "").toLowerCase();
		if (status !== lastStatus) {
			lastStatus = status;
			logJob(jobId, "heygen polling", {
				label,
				status: status || "unknown",
			});
		}
		if (status === "completed") return data;
		if (status === "failed") {
			throw new Error(
				`HeyGen video failed: ${data.failure_code || ""} ${
					data.failure_message || ""
				}`.trim(),
			);
		}
		await sleep(HEYGEN_POLL_INTERVAL_MS);
	}
	throw new Error(`HeyGen video polling timed out: ${label || videoId}`);
}

async function renderHeyGenPresenterSegment({
	jobId,
	tmpDir,
	output,
	presenterImageUrl,
	segDur,
	audioPath,
	label,
	addFades = false,
	cameraMotion = null,
	text = "",
	expression = "neutral",
	mood = "neutral",
	pace = "steady",
	role = "content",
	silentSmileTailSec = 0,
}) {
	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "_");
	if (!presenterImageUrl) throw new Error(`heygen_presenter_url_missing:${safeLabel}`);
	if (!audioPath || !fs.existsSync(audioPath))
		throw new Error(`heygen_audio_missing:${safeLabel}`);
	const dur = Math.max(0.2, Number(segDur) || (await probeDurationSeconds(audioPath)) || 0.2);
	const uploadedAudio = await uploadAudioToCloudinaryForHeyGen(audioPath, {
		jobId,
		label: safeLabel,
	});
	const roleKey = String(role || "").toLowerCase();
	const requiredPresenterRole = ["intro_first", "outro"].includes(roleKey);
	const contentPresenterRole = ["content_mid", "content_required"].includes(roleKey);
	const maxAllowedFreezeSec = requiredPresenterRole
		? HEYGEN_REQUIRED_MOTION_MAX_FREEZE_SEC
		: contentPresenterRole
			? HEYGEN_CONTENT_MOTION_MAX_FREEZE_SEC
			: HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_SEC;
	const maxAllowedFreezeRatio = requiredPresenterRole
		? HEYGEN_REQUIRED_MOTION_MAX_FREEZE_RATIO
		: contentPresenterRole
			? HEYGEN_CONTENT_MOTION_MAX_FREEZE_RATIO
			: HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_RATIO;
	let lastQaError = null;

	for (let attempt = 1; attempt <= HEYGEN_RENDER_MAX_ATTEMPTS; attempt++) {
		const attemptLabel = attempt === 1 ? safeLabel : `${safeLabel}_retry${attempt}`;
		const motionPrompt = buildHeyGenMotionPrompt({
			role,
			text,
			expression,
			mood,
			pace,
			durationSec: dur,
			silentSmileTailSec,
			attempt,
		});
		const payload = buildHeyGenVideoPayload({
			title: `AgentAI ${role} ${safeLabel}`,
			imageUrl: presenterImageUrl,
			audioUrl: uploadedAudio.url,
			motionPrompt,
			expressiveness:
				attempt > 1 ? HEYGEN_RETRY_EXPRESSIVENESS : HEYGEN_DEFAULT_EXPRESSIVENESS,
		});
		logJob(jobId, "heygen presenter request", {
			label: safeLabel,
			attempt,
			heygenLabel: attemptLabel,
			role,
			segDur: Number(dur.toFixed(3)),
			resolution: payload.resolution,
			expressiveness: payload.expressiveness,
			motionPromptChars: String(motionPrompt || "").length,
			audioPublicId: uploadedAudio.publicId,
		});
		const created = await createHeyGenVideo(payload, { jobId, label: attemptLabel });
		const completed = await pollHeyGenVideo({
			videoId: created.video_id,
			jobId,
			label: attemptLabel,
		});
		if (!completed.video_url) {
			throw new Error(`HeyGen completed without video_url:${attemptLabel}`);
		}
		const raw = path.join(tmpDir, `heygen_raw_${jobId}_${attemptLabel}.mp4`);
		await downloadToFile(completed.video_url, raw, 10 * 60 * 1000, 2);
		const qa = await analyzeLipsyncOutput({
			videoPath: raw,
			expectedDurSec: dur,
			jobId,
			label: attemptLabel,
			requireMotion: true,
		});
		const originalQaIssues = Array.isArray(qa.issues) ? [...qa.issues] : [];
		let acceptedSubtleHeyGenMotion = false;
		if (
			!qa.pass &&
			originalQaIssues.length > 0 &&
			originalQaIssues.every((issue) => issue === "presenter_video_frozen") &&
			Number(qa.durationDeltaSec || 0) <= PRESENTER_VIDEO_MAX_SHORTFALL_SEC &&
			Number(qa.maxFreezeSec || 0) <= maxAllowedFreezeSec &&
			Number(qa.freezeRatio || 0) <= maxAllowedFreezeRatio
		) {
			qa.pass = true;
			qa.issues = [];
			acceptedSubtleHeyGenMotion = true;
		}
		logJob(jobId, "heygen presenter qa", {
			label: safeLabel,
			attempt,
			heygenLabel: attemptLabel,
			pass: qa.pass,
			issues: qa.issues,
			acceptedSubtleHeyGenMotion,
			requiredPresenterRole,
			contentPresenterRole,
			...(acceptedSubtleHeyGenMotion ? { originalIssues: originalQaIssues } : {}),
			durationSec: Number((qa.durationSec || 0).toFixed(3)),
			durationDeltaSec: Number((qa.durationDeltaSec || 0).toFixed(3)),
			maxFreezeSec: Number((qa.maxFreezeSec || 0).toFixed(3)),
			freezeRatio: Number((qa.freezeRatio || 0).toFixed(3)),
			maxAllowedFreezeSec: Number(maxAllowedFreezeSec.toFixed(3)),
			maxAllowedFreezeRatio: Number(maxAllowedFreezeRatio.toFixed(3)),
			heygenVideoId: created.video_id,
		});
		if (!qa.pass) {
			lastQaError = new Error(
				`heygen_presenter_qa_failed:${safeLabel}:${qa.issues.join(",")}`,
			);
			safeUnlink(raw);
			if (attempt < HEYGEN_RENDER_MAX_ATTEMPTS) {
				logJob(jobId, "heygen presenter retry scheduled", {
					label: safeLabel,
					attempt,
					reason: qa.issues,
				});
				continue;
			}
			throw lastQaError;
		}

		const fit = path.join(tmpDir, `heygen_fit_${jobId}_${attemptLabel}.mp4`);
		await fitVideoToDuration(raw, dur, fit, SEGMENT_PAD_SEC);
		safeUnlink(raw);
		const withAudio = path.join(tmpDir, `heygen_audio_${jobId}_${attemptLabel}.mp4`);
		await mergeVideoWithAudio(fit, audioPath, withAudio);
		safeUnlink(fit);
		const norm = path.join(tmpDir, `heygen_norm_${jobId}_${attemptLabel}.mp4`);
		await normalizeClip(withAudio, norm, output, {
			zoomOut: CAMERA_ZOOM_OUT,
			addFades,
			cameraMotion: cameraMotion
				? { ...cameraMotion, visualType: "presenter" }
				: null,
		});
		safeUnlink(withAudio);

		const finalQa = await analyzeHeyGenFinalPresenterMotion({
			videoPath: norm,
			expectedDurSec: dur,
			jobId,
			label: attemptLabel,
		});
		logJob(jobId, "heygen presenter final qa", {
			label: safeLabel,
			attempt,
			heygenLabel: attemptLabel,
			pass: finalQa.pass,
			issues: finalQa.issues,
			durationSec: Number((finalQa.durationSec || 0).toFixed(3)),
			durationDeltaSec: Number((finalQa.durationDeltaSec || 0).toFixed(3)),
			maxFreezeSec: Number((finalQa.maxFreezeSec || 0).toFixed(3)),
			freezeRatio: Number((finalQa.freezeRatio || 0).toFixed(3)),
			maxAllowedFreezeSec: Number(HEYGEN_FINAL_MOTION_MAX_FREEZE_SEC.toFixed(3)),
			maxAllowedFreezeRatio: Number(
				HEYGEN_FINAL_MOTION_MAX_FREEZE_RATIO.toFixed(3),
			),
			heygenVideoId: created.video_id,
		});
		if (finalQa.pass) return norm;

		lastQaError = new Error(
			`heygen_presenter_final_qa_failed:${safeLabel}:${
				finalQa.issues.join(",") || "unknown"
			}`,
		);
		safeUnlink(norm);
		if (attempt < HEYGEN_RENDER_MAX_ATTEMPTS) {
			logJob(jobId, "heygen presenter retry scheduled", {
				label: safeLabel,
				attempt,
				reason: finalQa.issues,
				maxFreezeSec: Number((finalQa.maxFreezeSec || 0).toFixed(3)),
				freezeRatio: Number((finalQa.freezeRatio || 0).toFixed(3)),
			});
			continue;
		}
		throw lastQaError;
	}

	throw lastQaError || new Error(`heygen_presenter_qa_failed:${safeLabel}:unknown`);
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

function buildDynamicCameraMotionFilter({
	cameraMotion = null,
	durationSec = 0,
	fps = DEFAULT_OUTPUT_FPS,
	w = 1280,
	h = 720,
	visualType = "presenter",
} = {}) {
	if (!ENABLE_DYNAMIC_CAMERA_MOTION || !cameraMotion) return "";
	const mode = String(cameraMotion.mode || "steady").toLowerCase();
	if (!["punch", "slow"].includes(mode)) return "";

	const safeFps = Number(fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const dur = Math.max(0.2, Number(durationSec) || 0.2);
	const frameCount = Math.max(2, Math.round(dur * safeFps));
	const isImage = String(visualType || "").toLowerCase() === "image";
	const defaultMax =
		mode === "punch"
			? isImage
				? CAMERA_PUNCH_ZOOM_IMAGE_MAX
				: CAMERA_PUNCH_ZOOM_PRESENTER_MAX
			: isImage
				? CAMERA_SLOW_ZOOM_IMAGE_MAX
				: CAMERA_SLOW_ZOOM_PRESENTER_MAX;
	const maxZoom = clampNumber(
		cameraMotion.maxZoom || defaultMax,
		1.001,
		isImage ? 1.16 : 1.1,
	);
	const zoomDelta = Number((maxZoom - 1).toFixed(5));
	if (zoomDelta <= 0) return "";

	let startFrame = Math.max(
		0,
		Math.round((Number(cameraMotion.startSec) || 0) * safeFps),
	);
	startFrame = Math.min(startFrame, Math.max(0, frameCount - 2));

	let inFrames = Math.max(
		1,
		Math.round((Number(cameraMotion.zoomInSec) || dur * 0.5) * safeFps),
	);
	let holdFrames = Math.max(
		0,
		Math.round((Number(cameraMotion.holdSec) || 0) * safeFps),
	);
	let outFrames = Math.max(
		1,
		Math.round((Number(cameraMotion.zoomOutSec) || dur * 0.4) * safeFps),
	);

	if (mode === "slow") {
		startFrame = 0;
		inFrames = Math.max(2, Math.min(frameCount - 1, inFrames));
		holdFrames = 0;
		outFrames = Math.max(2, frameCount - inFrames);
	}

	const peakFrame = Math.min(frameCount - 1, startFrame + inFrames);
	const holdEndFrame = Math.min(frameCount - 1, peakFrame + holdFrames);
	const outEndFrame = Math.min(frameCount - 1, holdEndFrame + outFrames);
	const inDen = Math.max(1, peakFrame - startFrame);
	const outDen = Math.max(1, outEndFrame - holdEndFrame);
	const maxZoomText = maxZoom.toFixed(5);
	const deltaText = zoomDelta.toFixed(5);
	const zExpr =
		`if(lte(on,${startFrame}),1,` +
		`if(lte(on,${peakFrame}),1+${deltaText}*((on-${startFrame})/${inDen}),` +
		`if(lte(on,${holdEndFrame}),${maxZoomText},` +
		`if(lte(on,${outEndFrame}),${maxZoomText}-${deltaText}*((on-${holdEndFrame})/${outDen}),1))))`;

	return `zoompan=z='${zExpr}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:s=${makeEven(
		w,
	)}x${makeEven(h)}:fps=${safeFps},setsar=1,format=yuv420p`;
}

async function normalizeClip(
	inPath,
	outPath,
	outCfg,
	{
		zoomOut = 1.0,
		addFades = false,
		fadeOutOnly = false,
		cameraMotion = null,
	} = {},
) {
	const w = makeEven(outCfg.w);
	const h = makeEven(outCfg.h);
	const fps = Number(outCfg.fps || DEFAULT_OUTPUT_FPS);
	const scaleMode = outCfg.scaleMode || "cover";
	let durSec = 0;
	const needsDuration =
		Boolean(cameraMotion && cameraMotion.mode !== "steady") ||
		Boolean(addFades) ||
		Boolean(fadeOutOnly);
	if (needsDuration) {
		try {
			durSec = await probeDurationSeconds(inPath);
		} catch (_) {
			durSec = 0;
		}
	}

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

	const visualType = String(
		cameraMotion?.visualType || "presenter",
	).toLowerCase();
	let deshakeFilter = "";
	if (
		visualType === "presenter" &&
		ENABLE_PRESENTER_DESHAKE &&
		(PRESENTER_DESHAKE_RX > 0 || PRESENTER_DESHAKE_RY > 0)
	) {
		deshakeFilter = `deshake=rx=${PRESENTER_DESHAKE_RX}:ry=${PRESENTER_DESHAKE_RY}:edge=mirror`;
		vf += `,${deshakeFilter}`;
	}

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

	const cameraFilter = buildDynamicCameraMotionFilter({
		cameraMotion,
		durationSec: durSec,
		fps,
		w,
		h,
		visualType: cameraMotion?.visualType || "presenter",
	});
	if (cameraFilter) vf += `,${cameraFilter}`;

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

	const runNormalize = (videoFilter) =>
		spawnBin(
			ffmpegPath,
			[
				"-y",
				"-i",
				inPath,
				"-vf",
				videoFilter,
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
			{ timeoutMs: 240000 },
		);

	try {
		await runNormalize(vf);
	} catch (err) {
		if (!deshakeFilter) throw err;
		console.warn(
			`[LongVideo] presenter deshake fallback: ${
				err?.message || "unknown error"
			}`,
		);
		await runNormalize(vf.replace(`,${deshakeFilter}`, ""));
	}
}

function buildSubtleStillMotionFilter({
	idx = 0,
	fps = DEFAULT_OUTPUT_FPS,
	w = 1280,
	h = 720,
	mode = "blur",
} = {}) {
	const safeFps = Number(fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const W = makeEven(w);
	const H = makeEven(h);
	if (!ENABLE_STILL_IMAGE_MOTION || STILL_IMAGE_ZOOM_MAX <= 1.0005) {
		return `fps=${safeFps}`;
	}

	const soft = String(mode || "").toLowerCase() === "blur";
	const zoomInMax = Math.min(
		soft ? STILL_IMAGE_ZOOM_MAX : STILL_IMAGE_ZOOM_MAX + 0.004,
		1.025,
	).toFixed(5);
	const zoomOutStart = Math.min(Number(zoomInMax), 1.018).toFixed(5);
	const zoomInStep = Math.max(0, STILL_IMAGE_ZOOM_STEP).toFixed(7);
	const zoomOutStep = Math.max(0, STILL_IMAGE_ZOOM_STEP * 0.82).toFixed(7);
	const modeIndex = Math.abs(Number(idx) || 0) % 4;

	if (modeIndex === 1 || modeIndex === 3) {
		return `zoompan=z='max(1.0,${zoomOutStart}-on*${zoomOutStep})':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:s=${W}x${H}:fps=${safeFps}`;
	}
	return `zoompan=z='min(${zoomInMax},zoom+${zoomInStep})':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:s=${W}x${H}:fps=${safeFps}`;
}

async function createImageMontagePlateFrame({
	jobId,
	tmpDir,
	output,
	imgPath,
	label,
	idx = 0,
	mode = "blur",
}) {
	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "");
	const out = path.join(
		tmpDir,
		`seg_img_plate_${jobId}_${safeLabel}_${idx}.jpg`,
	);
	const w = makeEven(output.w);
	const h = makeEven(output.h);
	const blurMode = String(mode || "").toLowerCase() === "blur";
	const simpleVf = `scale=${w}:${h}:force_original_aspect_ratio=increase:flags=bicubic,crop=${w}:${h},setsar=1,format=yuv420p`;

	if (!blurMode) {
		await spawnBin(
			ffmpegPath,
			["-i", imgPath, "-vf", simpleVf, "-frames:v", "1", "-q:v", "3", "-y", out],
			"image_plate",
			{ timeoutMs: IMAGE_PLATE_TIMEOUT_MS },
		);
		return out;
	}

	const filter = [
		"[0:v]split=2[bg][fg]",
		`[bg]scale=${w}:${h}:force_original_aspect_ratio=increase:flags=bicubic,crop=${w}:${h},gblur=sigma=12[bg2]`,
		`[fg]scale=${w}:${h}:force_original_aspect_ratio=decrease:flags=bicubic[fg2]`,
		`[bg2][fg2]overlay=(W-w)/2:(H-h)/2,setsar=1,format=yuv420p[v]`,
	].join(";");

	try {
		await spawnBin(
			ffmpegPath,
			[
				"-i",
				imgPath,
				"-filter_complex",
				filter,
				"-map",
				"[v]",
				"-frames:v",
				"1",
				"-q:v",
				"3",
				"-y",
				out,
			],
			"image_plate",
			{ timeoutMs: IMAGE_PLATE_TIMEOUT_MS },
		);
		return out;
	} catch (e) {
		safeUnlink(out);
		const fallback = path.join(
			tmpDir,
			`seg_img_plate_${jobId}_${safeLabel}_${idx}_simple.jpg`,
		);
		await spawnBin(
			ffmpegPath,
			[
				"-i",
				imgPath,
				"-vf",
				simpleVf,
				"-frames:v",
				"1",
				"-q:v",
				"3",
				"-y",
				fallback,
			],
			"image_plate_simple",
			{ timeoutMs: IMAGE_PLATE_TIMEOUT_MS },
		);
		return fallback;
	}
}

async function prepareImageMontagePlateFrames({
	jobId,
	tmpDir,
	output,
	imagePaths = [],
	label,
	mode = "blur",
}) {
	const maxImages = Math.max(1, Math.floor(Number(IMAGE_MONTAGE_MAX_IMAGES) || 3));
	const selected = imagePaths.filter(Boolean).slice(0, maxImages);
	const plates = [];
	for (let i = 0; i < selected.length; i++) {
		plates.push(
			await createImageMontagePlateFrame({
				jobId,
				tmpDir,
				output,
				imgPath: selected[i],
				label,
				idx: i,
				mode,
			}),
		);
	}
	return plates;
}

async function createStaticImageFallbackClip({
	jobId,
	tmpDir,
	output,
	segDur,
	imagePaths = [],
	label,
}) {
	const sources = Array.isArray(imagePaths) ? imagePaths.filter(Boolean) : [];
	if (!sources.length) throw new Error("No images for static image fallback");
	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "");
	const dur = Math.max(0.2, Number(segDur) || 0.2);
	const w = makeEven(output.w);
	const h = makeEven(output.h);
	const fps = Number(output.fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const errors = [];
	for (let i = 0; i < sources.length; i++) {
		const source = sources[i];
		const raw = path.join(
			tmpDir,
			`seg_img_${jobId}_${safeLabel}_static_${i}.mp4`,
		);
		const labelNum = Number(label);
		const motion = buildSubtleStillMotionFilter({
			idx: Number.isFinite(labelNum) ? labelNum + i : i,
			fps,
			w,
			h,
			mode: "blur",
		});
		const vf = `scale=${w}:${h}:force_original_aspect_ratio=increase:flags=bicubic,crop=${w}:${h},${motion},trim=0:${dur.toFixed(3)},setpts=PTS-STARTPTS,setsar=1,format=yuv420p`;
		try {
			await spawnBin(
				ffmpegPath,
				[
					"-loop",
					"1",
					"-framerate",
					String(fps),
					"-i",
					source,
					"-t",
					dur.toFixed(3),
					"-vf",
					vf,
					"-c:v",
					"libx264",
					"-preset",
					"ultrafast",
					"-crf",
					String(Math.max(INTERMEDIATE_VIDEO_CRF, 18)),
					"-pix_fmt",
					"yuv420p",
					"-movflags",
					"+faststart",
					"-y",
					raw,
				],
				"image_montage_static",
				{ timeoutMs: IMAGE_MONTAGE_STATIC_TIMEOUT_MS },
			);
			if (i > 0) {
				logJob(jobId, "image static fallback used alternate feed image", {
					label: safeLabel,
					index: i,
				});
			}
			return raw;
		} catch (e) {
			safeUnlink(raw);
			errors.push(e?.message || String(e));
		}
	}
	throw new Error(
		`image_static_fallback_failed: ${errors.slice(0, 3).join(" | ")}`,
	);
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
		output.imageScaleMode || DEFAULT_IMAGE_SCALE_MODE,
	)
		.trim()
		.toLowerCase();
	let workingImagePaths = imagePaths
		.filter(Boolean)
		.slice(
			0,
			Math.max(1, Math.floor(Number(IMAGE_MONTAGE_MAX_IMAGES) || 3)),
		);
	if (!workingImagePaths.length) throw new Error("No usable images for segment");
	let cleanupImagePaths = [];
	let effectiveImageScaleMode = imageScaleMode;
	if (imageScaleMode === "blur") {
		try {
			workingImagePaths = await prepareImageMontagePlateFrames({
				jobId,
				tmpDir,
				output,
				imagePaths: workingImagePaths,
				label: safeLabel,
				mode: "blur",
			});
			cleanupImagePaths = workingImagePaths.slice();
			effectiveImageScaleMode = "plate";
		} catch (e) {
			logJob(jobId, "image montage plate prep failed; using simple path", {
				label: safeLabel,
				error: e.message,
			});
			effectiveImageScaleMode = "cover";
		}
	}
	const perDur = Math.max(0.2, dur / workingImagePaths.length);
	const labelNum = Number(label);
	const useCrossfade =
		workingImagePaths.length > 1 &&
		perDur >= 1.4 &&
		(!Number.isFinite(labelNum) || labelNum % 2 === 0);
	const crossfadeDur = useCrossfade ? clampNumber(perDur * 0.2, 0.25, 0.6) : 0;

	const inputs = [];
	const filterParts = [];
	const vLabels = [];

	workingImagePaths.forEach((imgPath, idx) => {
		inputs.push("-loop", "1", "-framerate", String(fps), "-i", imgPath);
		const outLabel = `v${idx}`;
		const trim = `trim=0:${perDur.toFixed(3)},setpts=PTS-STARTPTS`;
		if (effectiveImageScaleMode === "plate") {
			const motion = buildSubtleStillMotionFilter({
				idx: Number.isFinite(labelNum) ? labelNum + idx : idx,
				fps,
				w,
				h,
				mode: "blur",
			});
			filterParts.push(
				`[${idx}:v]${motion},${trim},setsar=1,format=yuv420p[${outLabel}]`,
			);
		} else if (effectiveImageScaleMode === "blur") {
			const bg = `bg${idx}`;
			const fg = `fg${idx}`;
			const bg2 = `bg2${idx}`;
			const fg2 = `fg2${idx}`;
			filterParts.push(`[${idx}:v]split=2[${bg}][${fg}]`);
			filterParts.push(
				`[${bg}]scale=${w}:${h}:force_original_aspect_ratio=increase:flags=lanczos,crop=${w}:${h},gblur=sigma=18[${bg2}]`,
			);
			filterParts.push(
				`[${fg}]scale=${w}:${h}:force_original_aspect_ratio=decrease:flags=lanczos[${fg2}]`,
			);
			const motion = buildSubtleStillMotionFilter({
				idx: Number.isFinite(labelNum) ? labelNum + idx : idx,
				fps,
				w,
				h,
				mode: "blur",
			});
			filterParts.push(
				`[${bg2}][${fg2}]overlay=(W-w)/2:(H-h)/2,${motion},${trim},setsar=1,format=yuv420p[${outLabel}]`,
			);
		} else {
			const scale =
				effectiveImageScaleMode === "contain"
					? `scale=${w}:${h}:force_original_aspect_ratio=decrease:flags=lanczos,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2:color=black`
					: `scale=${w}:${h}:force_original_aspect_ratio=increase:flags=lanczos,crop=${w}:${h}`;
			const motion = buildSubtleStillMotionFilter({
				idx: Number.isFinite(labelNum) ? labelNum + idx : idx,
				fps,
				w,
				h,
				mode: effectiveImageScaleMode,
			});
			filterParts.push(
				`[${idx}:v]${scale},${motion},${trim},setsar=1,format=yuv420p[${outLabel}]`,
			);
		}
		vLabels.push(`[${outLabel}]`);
	});

	if (useCrossfade && workingImagePaths.length > 1) {
		let last = "v0";
		let acc = perDur;
		for (let i = 1; i < workingImagePaths.length; i++) {
			const out = `xf${i}`;
			const offset = Math.max(0, acc - crossfadeDur);
			filterParts.push(
				`[${last}][v${i}]xfade=transition=fade:duration=${crossfadeDur.toFixed(
					3,
				)}:offset=${offset.toFixed(3)}[${out}]`,
			);
			acc += perDur - crossfadeDur;
			last = out;
		}
		filterParts.push(`[${last}]setsar=1,format=yuv420p[v]`);
	} else {
		filterParts.push(
			`${vLabels.join("")}concat=n=${workingImagePaths.length}:v=1:a=0[v]`,
		);
	}

	const raw = path.join(tmpDir, `seg_img_${jobId}_${safeLabel}_raw.mp4`);
	try {
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
			{ timeoutMs: IMAGE_MONTAGE_TIMEOUT_MS },
		);
	} finally {
		cleanupImagePaths.forEach((p) => safeUnlink(p));
	}

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
	cameraMotion = null,
}) {
	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "");
	let montage;
	try {
		montage = await createImageMontageClip({
			jobId,
			tmpDir,
			output,
			segDur,
			imagePaths,
			label: safeLabel,
		});
	} catch (e) {
		logJob(jobId, "image montage failed; using local static fallback", {
			label: safeLabel,
			error: e.message,
		});
		montage = await createStaticImageFallbackClip({
			jobId,
			tmpDir,
			output,
			segDur,
			imagePaths,
			label: safeLabel,
		});
	}

	const withAudio = path.join(tmpDir, `img_${jobId}_${safeLabel}_audio.mp4`);
	await mergeVideoWithAudio(montage, audioPath, withAudio);
	safeUnlink(montage);

	const norm = path.join(tmpDir, `img_${jobId}_${safeLabel}_norm.mp4`);
	await normalizeClip(withAudio, norm, output, {
		zoomOut: CAMERA_ZOOM_OUT,
		addFades,
		cameraMotion: cameraMotion ? { ...cameraMotion, visualType: "image" } : null,
	});
	safeUnlink(withAudio);
	return norm;
}

async function renderFeedVideoSegment({
	jobId,
	tmpDir,
	output,
	segDur,
	audioPath,
	videoPaths = [],
	label,
	addFades = false,
}) {
	const sources = Array.isArray(videoPaths) ? videoPaths.filter(Boolean) : [];
	if (!sources.length) throw new Error("No feed videos for segment");
	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "");
	const dur = Math.max(0.2, Number(segDur) || 0.2);
	const targetDur =
		dur <= FEED_VIDEO_MAX_CLIP_SEC + 0.5
			? dur
			: Math.max(0.2, FEED_VIDEO_MAX_CLIP_SEC);
	const w = makeEven(output.w);
	const h = makeEven(output.h);
	const fps = Number(output.fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const errors = [];

	for (let i = 0; i < sources.length; i++) {
		const source = sources[i];
		const info = await probeMedia(source);
		if (!info.hasVideo) continue;
		const sourceDur = Number(info.duration || 0);
		const startSeed = Math.abs(
			Number(
				crypto
					.createHash("sha1")
					.update(`${jobId}:${safeLabel}:${i}`)
					.digest()
					.readUInt32BE(0),
			),
		);
		const canTrim = sourceDur > targetDur + 0.8;
		const startSec = canTrim
			? Math.min(
					Math.max(0, sourceDur - targetDur - 0.2),
					(startSeed % 1000) / 1000 *
						Math.max(0, sourceDur - targetDur - 0.2),
				)
			: 0;
		const inputArgs = canTrim
			? ["-ss", startSec.toFixed(3), "-i", source]
			: sourceDur > 0 && sourceDur < targetDur - 0.15
				? ["-stream_loop", "-1", "-i", source]
				: ["-i", source];
		const raw = path.join(tmpDir, `seg_feed_video_${jobId}_${safeLabel}_${i}.mp4`);
		let fit = raw;
		const vf = `scale=${w}:${h}:force_original_aspect_ratio=increase:flags=lanczos,crop=${w}:${h},fps=${fps},setpts=PTS-STARTPTS,setsar=1,format=yuv420p`;
		try {
			await spawnBin(
				ffmpegPath,
				[
					...inputArgs,
					"-t",
					targetDur.toFixed(3),
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
					raw,
				],
				"feed_video_clip",
				{ timeoutMs: IMAGE_MONTAGE_TIMEOUT_MS },
			);
			fit =
				Math.abs(targetDur - dur) > 0.08
					? path.join(tmpDir, `seg_feed_video_${jobId}_${safeLabel}_${i}_fit.mp4`)
					: raw;
			if (fit !== raw) {
				await fitVideoToDuration(raw, dur, fit);
				safeUnlink(raw);
			}
			const withAudio = path.join(
				tmpDir,
				`feed_video_${jobId}_${safeLabel}_${i}_audio.mp4`,
			);
			await mergeVideoWithAudio(fit, audioPath, withAudio);
			if (fit !== raw) safeUnlink(fit);
			else safeUnlink(raw);
			const norm = path.join(
				tmpDir,
				`feed_video_${jobId}_${safeLabel}_${i}_norm.mp4`,
			);
			await normalizeClip(withAudio, norm, output, {
				zoomOut: 1,
				addFades,
				cameraMotion: null,
			});
			safeUnlink(withAudio);
			return norm;
		} catch (e) {
			safeUnlink(raw);
			if (fit !== raw) safeUnlink(fit);
			errors.push(e?.message || String(e));
			logJob(jobId, "feed video render candidate failed", {
				label: safeLabel,
				index: i,
				error: e.message,
			});
		}
	}
	throw new Error(
		`feed_video_render_failed: ${errors.slice(0, 3).join(" | ")}`,
	);
}

async function renderNoSyncVisualFallbackSegment({
	jobId,
	tmpDir,
	output,
	segDur,
	audioPath,
	label,
	addFades = false,
	cameraMotion = null,
}) {
	const safeLabel = String(label || "seg").replace(/[^a-z0-9_-]/gi, "");
	const dur = Math.max(0.2, Number(segDur) || 0.2);
	const w = makeEven(output.w);
	const h = makeEven(output.h);
	const fps = Number(output.fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const raw = path.join(tmpDir, `img_${jobId}_${safeLabel}_nosync_raw.mp4`);
	await spawnBin(
		ffmpegPath,
		[
			"-f",
			"lavfi",
			"-i",
			`color=c=0x111827:s=${w}x${h}:r=${fps}`,
			"-t",
			dur.toFixed(3),
			"-vf",
			"format=yuv420p",
			"-c:v",
			"libx264",
			"-preset",
			"ultrafast",
			"-crf",
			String(Math.max(INTERMEDIATE_VIDEO_CRF, 18)),
			"-pix_fmt",
			"yuv420p",
			"-movflags",
			"+faststart",
			"-y",
			raw,
		],
		"image_no_sync_fallback",
		{ timeoutMs: IMAGE_MONTAGE_STATIC_TIMEOUT_MS },
	);

	const withAudio = path.join(tmpDir, `img_${jobId}_${safeLabel}_nosync_audio.mp4`);
	await mergeVideoWithAudio(raw, audioPath, withAudio);
	safeUnlink(raw);

	const norm = path.join(tmpDir, `img_${jobId}_${safeLabel}_nosync_norm.mp4`);
	await normalizeClip(withAudio, norm, output, {
		zoomOut: CAMERA_ZOOM_OUT,
		addFades,
		cameraMotion: cameraMotion ? { ...cameraMotion, visualType: "image" } : null,
	});
	safeUnlink(withAudio);
	return norm;
}

async function fitVideoToDuration(inVideo, targetSec, outVideo, padSec = 0) {
	const pad = Math.max(0, Number(padSec) || 0);
	const target = Math.max(0.2, Number(targetSec) || 1) + pad;
	const vf = `setpts=PTS-STARTPTS,tpad=stop_mode=clone:stop_duration=${target.toFixed(
		3,
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
		{ timeoutMs: 180000 },
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
		{ timeoutMs: 180000 },
	);
	return outPath;
}

async function concatAudioClips(audioPaths = [], outPath) {
	if (!Array.isArray(audioPaths) || !audioPaths.length) {
		throw new Error("No audio clips to concat");
	}
	if (audioPaths.length === 1) {
		fs.copyFileSync(audioPaths[0], outPath);
		return outPath;
	}

	const args = [];
	audioPaths.forEach((p) => args.push("-i", p));

	const pre = audioPaths
		.map(
			(_, i) =>
				`[${i}:a:0]asetpts=PTS-STARTPTS,aresample=${AUDIO_SR},aformat=channel_layouts=mono:sample_fmts=s16[a${i}]`,
		)
		.join(";");
	const catInputs = audioPaths.map((_, i) => `[a${i}]`).join("");
	const filter = `${pre};${catInputs}concat=n=${audioPaths.length}:v=0:a=1[a]`;

	args.push(
		"-filter_complex",
		filter,
		"-map",
		"[a]",
		"-c:a",
		"pcm_s16le",
		"-ar",
		String(AUDIO_SR),
		"-ac",
		String(AUDIO_CHANNELS),
		"-y",
		outPath,
	);

	await spawnBin(ffmpegPath, args, "concat_audio", { timeoutMs: 180000 });
	return outPath;
}

function canMergePresenterRun(currentRun, seg) {
	if (!ENABLE_PRESENTER_RUN_MERGE) return false;
	if (!currentRun || !seg) return false;
	if (seg.visualType !== "presenter") return false;
	if (currentRun.visualType !== "presenter") return false;
	if (
		!MERGE_REQUIRED_PRESENTER_RUNS &&
		(currentRun.mustUsePresenter || seg.mustUsePresenter)
	) {
		return false;
	}
	if (currentRun.segments.length >= PRESENTER_RUN_MERGE_MAX_SEGMENTS) {
		return false;
	}
	const currentDur = Number(currentRun.segDur || 0);
	const nextDur = Math.max(
		0.2,
		Number(seg.endSec || 0) - Number(seg.startSec || 0),
	);
	const currentClusterId = String(currentRun.presenterClusterId || "").trim();
	const nextClusterId = String(seg.presenterClusterId || "").trim();
	if (currentClusterId && currentClusterId === nextClusterId) {
		return currentDur + nextDur <= PRESENTER_RUN_MERGE_MAX_SEC;
	}
	if (Number(seg.topicIndex) !== Number(currentRun.topicIndex)) return false;
	if (
		String(seg.videoExpression || seg.expression || "neutral") !==
		String(currentRun.videoExpression || currentRun.expression || "neutral")
	) {
		return false;
	}
	if (
		cameraMotionKey(seg.cameraMotion) !==
		cameraMotionKey(currentRun.cameraMotion)
	) {
		return false;
	}
	return currentDur + nextDur <= PRESENTER_RUN_MERGE_MAX_SEC;
}

async function buildRenderableTimelineUnits({
	timeline = [],
	tmpDir,
	jobId,
	premiumPresenterSegmentSet = new Set(),
}) {
	const units = [];
	let presenterRun = null;

	const pushSingleUnit = (seg) => {
		if (!seg) return;
		const segDur = Math.max(
			0.2,
			Number(seg.endSec || 0) - Number(seg.startSec || 0),
		);
		units.push({
			...seg,
			renderLabel: String(seg.index),
			renderSegmentIndices: [seg.index],
			segDur,
			syncTier: premiumPresenterSegmentSet.has(seg.index) ? "hero" : "standard",
			mergedPresenterRun: false,
		});
	};

	const flushPresenterRun = async () => {
		if (!presenterRun) return;
		if (presenterRun.segments.length <= 1) {
			pushSingleUnit(presenterRun.segments[0]);
			presenterRun = null;
			return;
		}

		const indices = presenterRun.segments.map((seg) => seg.index);
		const renderLabel = `${indices[0]}_${indices[indices.length - 1]}`;
		const mergedAudioPath = path.join(
			tmpDir,
			`seg_audio_${jobId}_${renderLabel}_merged.wav`,
		);

		try {
			await concatAudioClips(
				presenterRun.segments.map((seg) => seg.audioPath),
				mergedAudioPath,
			);
			units.push({
				...presenterRun.segments[0],
				endSec: presenterRun.segments[presenterRun.segments.length - 1].endSec,
				audioPath: mergedAudioPath,
				renderLabel,
				renderSegmentIndices: indices,
				segDur: presenterRun.segDur,
				syncTier: presenterRun.hasPremium ? "hero" : "standard",
				mergedPresenterRun: true,
				mergedPresenterCount: presenterRun.segments.length,
			});
		} catch {
			for (const seg of presenterRun.segments) {
				pushSingleUnit(seg);
			}
		}

		presenterRun = null;
	};

	for (const seg of timeline || []) {
		if (seg?.visualType !== "presenter") {
			await flushPresenterRun();
			pushSingleUnit(seg);
			continue;
		}

		const segDur = Math.max(
			0.2,
			Number(seg.endSec || 0) - Number(seg.startSec || 0),
		);
		if (!presenterRun) {
			presenterRun = {
				visualType: "presenter",
				topicIndex: seg.topicIndex,
				videoExpression: seg.videoExpression || seg.expression || "neutral",
				expression: seg.expression || "neutral",
				cameraMotion: seg.cameraMotion || { mode: "steady" },
				presenterClusterId: seg.presenterClusterId || "",
				segDur,
				mustUsePresenter: Boolean(seg.mustUsePresenter),
				hasPremium: premiumPresenterSegmentSet.has(seg.index),
				segments: [seg],
			};
			continue;
		}

		if (canMergePresenterRun(presenterRun, seg)) {
			presenterRun.segments.push(seg);
			presenterRun.segDur += segDur;
			if (seg.mustUsePresenter) presenterRun.mustUsePresenter = true;
			if (premiumPresenterSegmentSet.has(seg.index))
				presenterRun.hasPremium = true;
			continue;
		}

		await flushPresenterRun();
		presenterRun = {
			visualType: "presenter",
			topicIndex: seg.topicIndex,
			videoExpression: seg.videoExpression || seg.expression || "neutral",
			expression: seg.expression || "neutral",
			cameraMotion: seg.cameraMotion || { mode: "steady" },
			presenterClusterId: seg.presenterClusterId || "",
			segDur,
			mustUsePresenter: Boolean(seg.mustUsePresenter),
			hasPremium: premiumPresenterSegmentSet.has(seg.index),
			segments: [seg],
		};
	}

	await flushPresenterRun();
	return units;
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
	const fps = Number(outCfg?.fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const scaleFilter =
		w && h
			? `scale=${w}:${h}:force_original_aspect_ratio=increase:flags=lanczos,crop=${w}:${h},`
			: "";

	const runPlainConcat = async () => {
		const args = [];
		clips.forEach((p) => args.push("-i", p));

		const pre = clips
			.map(
				(_, i) =>
					`[${i}:v:0]${scaleFilter}setpts=PTS-STARTPTS,format=yuv420p,setsar=1[v${i}];` +
					`[${i}:a:0]asetpts=PTS-STARTPTS,aresample=${AUDIO_SR},aformat=channel_layouts=stereo:sample_fmts=fltp[a${i}]`,
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
			outPath,
		);

		await spawnBin(ffmpegPath, args, "concat", { timeoutMs: 420000 });
		return outPath;
	};

	const useSoftTransitions =
		ENABLE_SOFT_SEGMENT_TRANSITIONS &&
		clips.length > 1 &&
		outCfg?.softTransitions !== false;
	if (!useSoftTransitions) {
		return await runPlainConcat();
	}

	const requestedTransitionSec = clampNumber(
		Number(outCfg?.transitionSec || SEGMENT_TRANSITION_SEC),
		0,
		0.5,
	);
	const requestedPadSec = clampNumber(
		Number(outCfg?.transitionPadSec || SEGMENT_TRANSITION_PAD_SEC),
		0,
		0.5,
	);
	const minClipSec = clampNumber(
		Number(outCfg?.transitionMinClipSec || SEGMENT_TRANSITION_MIN_CLIP_SEC),
		0.25,
		12,
	);
	if (requestedTransitionSec <= 0) {
		return await runPlainConcat();
	}

	const formatFilterNum = (value) => {
		const num = Number(value) || 0;
		return Number(num.toFixed(3)).toString();
	};

	try {
		const durations = await Promise.all(
			clips.map((clipPath) => probeDurationSecondsCached(clipPath)),
		);
		if (
			durations.some(
				(dur) => !Number.isFinite(dur) || dur <= Math.max(minClipSec, 0.35),
			)
		) {
			return await runPlainConcat();
		}

		const args = [];
		clips.forEach((p) => args.push("-i", p));

		const preparedDurations = durations.map(
			(dur, i) =>
				Math.max(0, dur) + (i < clips.length - 1 ? requestedPadSec : 0),
		);

		const pre = clips
			.map((_, i) => {
				const addPad = i < clips.length - 1 && requestedPadSec > 0;
				const videoPad = addPad
					? `,tpad=stop_mode=clone:stop_duration=${formatFilterNum(
							requestedPadSec,
						)}`
					: "";
				const audioPad = addPad
					? `,apad=pad_dur=${formatFilterNum(requestedPadSec)}`
					: "";
				return (
					`[${i}:v:0]${scaleFilter}settb=AVTB,setpts=PTS-STARTPTS${videoPad},fps=${fps},format=yuv420p,setsar=1[v${i}]` +
					`;[${i}:a:0]asetpts=PTS-STARTPTS,aresample=${AUDIO_SR},aformat=channel_layouts=stereo:sample_fmts=fltp${audioPad}[a${i}]`
				);
			})
			.join(";");

		const chain = [pre];
		let currentVideo = "v0";
		let currentAudio = "a0";
		let currentDuration = preparedDurations[0];

		for (let i = 1; i < clips.length; i++) {
			const maxTransition = Math.min(
				requestedTransitionSec,
				Math.max(0, currentDuration - 0.05),
				Math.max(0, preparedDurations[i] - 0.05),
			);
			if (!Number.isFinite(maxTransition) || maxTransition < 0.05) {
				return await runPlainConcat();
			}

			const nextVideo = i === clips.length - 1 ? "v" : `vx${i}`;
			const nextAudio = i === clips.length - 1 ? "a" : `ax${i}`;
			const offset = Math.max(0, currentDuration - maxTransition);

			chain.push(
				`[${currentVideo}][v${i}]xfade=transition=fade:duration=${formatFilterNum(
					maxTransition,
				)}:offset=${formatFilterNum(offset)}[${nextVideo}]`,
			);
			chain.push(
				`[${currentAudio}][a${i}]acrossfade=d=${formatFilterNum(
					maxTransition,
				)}:c1=tri:c2=tri[${nextAudio}]`,
			);

			currentVideo = nextVideo;
			currentAudio = nextAudio;
			currentDuration += preparedDurations[i] - maxTransition;
		}

		args.push(
			"-filter_complex",
			chain.join(";"),
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
			outPath,
		);

		await spawnBin(ffmpegPath, args, "concat_soft", { timeoutMs: 420000 });
		return outPath;
	} catch (err) {
		console.warn(
			`[LongVideo] soft concat fallback: ${err?.message || "unknown error"}`,
		);
		return await runPlainConcat();
	}
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
		.replace(/,/g, "\\,")
		.replace(/:/g, "\\:")
		.replace(/'/g, "\\'")
		.replace(/%/g, "\\%")
		.replace(/\[/g, "\\[")
		.replace(/\]/g, "\\]")
		.replace(new RegExp(placeholder, "g"), "\\n")
		.trim();
}

function escapeFilterExpr(expr = "") {
	return String(expr || "")
		.replace(/\\/g, "\\\\")
		.replace(/,/g, "\\,")
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
		"[LongVideo] WARN - Watermark font not found. Falling back to default drawtext font.",
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

function computeFinalMasterSize(outCfg = {}, sourceCfg = {}) {
	const baseW = Number(outCfg?.w || 0) || 1280;
	const baseH = Number(outCfg?.h || 0) || 720;
	const sourceW = makeEven(
		Number(sourceCfg?.w || sourceCfg?.width || 0) || baseW,
	);
	const sourceH = makeEven(
		Number(sourceCfg?.h || sourceCfg?.height || 0) || baseH,
	);
	const fallbackRatio = baseW > 0 && baseH > 0 ? baseW / baseH : 16 / 9;
	const sourceRatio = sourceW > 0 && sourceH > 0 ? sourceW / sourceH : 0;
	const ratio =
		Number.isFinite(sourceRatio) && sourceRatio > 0
			? sourceRatio
			: fallbackRatio;

	let targetH = Math.min(sourceH, FINAL_MASTER_MAX_HEIGHT);
	if (FINAL_MASTER_ALLOW_UPSCALE) {
		const upscaleCap = makeEven(sourceH * FINAL_MASTER_MAX_UPSCALE_FACTOR);
		targetH = Math.min(
			FINAL_MASTER_MAX_HEIGHT,
			Math.max(sourceH, FINAL_MASTER_MIN_HEIGHT),
			upscaleCap,
		);
	}
	if (!Number.isFinite(targetH) || targetH <= 0) {
		targetH = Math.min(baseH, FINAL_MASTER_MAX_HEIGHT);
	}
	const targetW = makeEven(targetH * ratio);
	return {
		w: makeEven(targetW),
		h: makeEven(targetH),
		sourceW,
		sourceH,
	};
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
	{ baseMaxChars = 36, preferLines = 2, maxLines = 3 } = {},
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
		maxDur,
	);

	const fontFile = resolveFontFile();
	const fontOpt = fontFile ? `:fontfile='${escapeDrawtext(fontFile)}'` : "";

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
		maxLines: 1,
	});
	const safeTitle = escapeDrawtext(titleFit.text);
	const safeSub = escapeDrawtext(subFit.text);
	const titleFontSize = Math.max(
		16,
		Math.round(H * 0.048 * titleFit.fontScale),
	);
	const subFontSize = Math.max(12, Math.round(H * 0.032 * subFit.fontScale));
	const titleX = Math.round(W * INTRO_TEXT_X_PCT);
	const titleY = Math.round(H * INTRO_TEXT_Y_PCT);
	const subY = Math.round(H * INTRO_SUBTITLE_Y_PCT);
	const textInStart = INTRO_TEXT_FADE_IN_START;
	const textInDur = INTRO_TEXT_FADE_IN_DUR;
	const textInEnd = textInStart + textInDur;
	const alphaExprRaw = `if(lt(t,${textInStart.toFixed(
		2,
	)}),0, if(lt(t,${textInEnd.toFixed(2)}),(t-${textInStart.toFixed(
		2,
	)})/${textInDur.toFixed(2)}, 1))`;
	const alphaExpr = escapeFilterExpr(alphaExprRaw);

	const bgKind = detectFileType(bgImagePath)?.kind;
	const isVideoBg = bgKind === "video";
	const bgInfo = isVideoBg ? await probeMedia(bgImagePath) : null;
	const needsSilentAudio = !bgInfo?.hasAudio;
	// A subtle motion background + title fade in
	const blurSigma = disableVideoBlur ? 0 : INTRO_VIDEO_BLUR_SIGMA;
	const videoBlur = blurSigma > 0 ? `,gblur=sigma=${blurSigma.toFixed(2)}` : "";
	const base = isVideoBg
		? `scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H}${videoBlur},fps=${fps},format=yuv420p`
		: `scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H},gblur=sigma=18,` +
			`zoompan=z='min(1.12,zoom+0.0025)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:fps=${fps},format=yuv420p`;
	const filters = [
		base,
		`drawtext=text='${safeTitle}'${fontOpt}:fontsize=${titleFontSize}:fontcolor=white:x=${titleX}:y=${titleY}-(text_h/2):shadowcolor=black:shadowx=2:shadowy=2:alpha='${alphaExpr}'`,
	];
	if (safeSub) {
		filters.push(
			`drawtext=text='${safeSub}'${fontOpt}:fontsize=${subFontSize}:fontcolor=white:x=${titleX}:y=${subY}-(text_h/2):shadowcolor=black:shadowx=2:shadowy=2:alpha='${alphaExpr}'`,
		);
	}
	const vf = filters.join(",");

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
		{ timeoutMs: 180000 },
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
					Number(s.endSec) > Number(s.startSec),
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
				Math.floor(((i + 0.5) * segments.length) / n),
			);
			const seg = segments[idx];
			const segDur = Math.max(0.6, Number(seg.endSec) - Number(seg.startSec));
			const win = clampNumber(segDur * 0.5, 2.2, 4.2);
			startSec = Number(seg.startSec) + Math.max(0.2, segDur * 0.2);
			endSec = Math.min(Number(seg.endSec) - 0.2, startSec + win);
		} else {
			const available = Math.max(
				1,
				Number(totalDurationSec || 0) - Number(introSec || 0),
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
			}/TB[${prep}]`,
		);

		// Scale with a hard cap to avoid covering the presenter.
		filterParts.push(
			`[${prep}][${last}]scale2ref=w='min(iw*${ov.scale},main_w*${OVERLAY_MAX_WIDTH_PCT})':h='-1'[${scaled}][${baseRef}]`,
		);

		if (OVERLAY_BORDER_PX > 0) {
			filterParts.push(
				`[${scaled}]pad=iw+${OVERLAY_BORDER_PX * 2}:ih+${
					OVERLAY_BORDER_PX * 2
				}:${OVERLAY_BORDER_PX}:${OVERLAY_BORDER_PX}:color=black@0.25[${timed}]`,
			);
		}

		filterParts.push(
			`[${baseRef}][${timed}]overlay=${pos.x}:${
				pos.y
			}:enable='between(t,${ov.startSec.toFixed(3)},${ov.endSec.toFixed(
				3,
			)})'[${out}]`,
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
		{ timeoutMs: 360000 },
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
			"+",
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
		.filter((t) => t.audio && t.duration >= 30);
}

function buildBackgroundMusicSearchPlan({
	topic = "",
	categoryLabel = "",
	mood = "neutral",
	topics = [],
} = {}) {
	const personalFinance = isPersonalFinanceCostOfLivingTopic({
		topics,
		categoryLabel,
		text: topic,
	});
	const socialConnection = isSocialConnectionTopic({
		topics,
		categoryLabel,
		text: topic,
	});
	const digitalWellbeing = isDigitalWellbeingTopic({
		topics,
		categoryLabel,
		text: topic,
	});
	const serious = String(mood || "").toLowerCase() === "serious";
	if (personalFinance || socialConnection || digitalWellbeing || serious) {
		return {
			fuzzytags:
				socialConnection || digitalWellbeing
					? "calm, acoustic, ambient, hopeful, piano, soft, instrumental"
					: "documentary, calm, ambient, corporate, piano, hopeful, instrumental",
			speed: ["low", "medium"],
			preferTerms: [
				"ambient",
				"piano",
				"acoustic",
				"documentary",
				"corporate",
				"hope",
				"calm",
				"soft",
				"minimal",
				"warm",
			],
			avoidTerms: [
				"salsa",
				"dance",
				"party",
				"club",
				"latin",
				"techno",
				"house",
				"trance",
				"dubstep",
				"metal",
				"rap",
				"hip hop",
			],
		};
	}
	return {
		fuzzytags: `calm, ambient, soft, piano, acoustic, hopeful, instrumental, ${String(
			topic || "",
		).slice(0, 40)}`,
		speed: ["low", "medium"],
		preferTerms: [
			"calm",
			"ambient",
			"soft",
			"piano",
			"acoustic",
			"hope",
			"warm",
			"minimal",
			"documentary",
		],
		avoidTerms: [
			"salsa",
			"polka",
			"christmas",
			"lullaby",
			"dance",
			"party",
			"club",
			"techno",
			"house",
			"trance",
			"dubstep",
			"metal",
			"rap",
			"hip hop",
			"epic",
			"trailer",
			"energetic",
		],
	};
}

function scoreJamendoMusicCandidate(track = {}, plan = {}) {
	const hay = normalizeWhitespace(
		`${track.name || ""} ${track.artist || ""} ${track.shareurl || ""}`,
	).toLowerCase();
	let score = 0;
	for (const term of plan.preferTerms || []) {
		if (term && hay.includes(String(term).toLowerCase())) score += 4;
	}
	for (const term of plan.avoidTerms || []) {
		if (term && hay.includes(String(term).toLowerCase())) score -= 12;
	}
	const duration = Number(track.duration || 0);
	if (duration >= 90 && duration <= 360) score += 2;
	if (duration > 480) score -= 1;
	return score;
}

async function validateMusicFile(filePath) {
	if (!filePath || !fs.existsSync(filePath)) return false;
	const info = await probeMedia(filePath);
	if (!info.hasAudio) return false;
	if (!info.duration || info.duration < 10) return false;
	return true;
}

async function tryResolveDefaultBackgroundMusic(jobId) {
	if (DEFAULT_MUSIC_PATH && fs.existsSync(DEFAULT_MUSIC_PATH)) {
		logJob(jobId, "music default path check", { path: DEFAULT_MUSIC_PATH });
		if (await validateMusicFile(DEFAULT_MUSIC_PATH)) {
			logJob(jobId, "music ready (default path)", { path: DEFAULT_MUSIC_PATH });
			return DEFAULT_MUSIC_PATH;
		}
		logJob(jobId, "music default path invalid", { path: DEFAULT_MUSIC_PATH });
	}
	if (DEFAULT_MUSIC_URL) {
		const out = path.join(TMP_ROOT, `music_default_${jobId}.mp3`);
		let keepDownloadedDefault = false;
		try {
			logJob(jobId, "music default url download start", {
				timeoutMs: MUSIC_TRACK_DOWNLOAD_TIMEOUT_MS,
			});
			await downloadToFile(DEFAULT_MUSIC_URL, out, MUSIC_TRACK_DOWNLOAD_TIMEOUT_MS, 1);
			if (await validateMusicFile(out)) {
				keepDownloadedDefault = true;
				logJob(jobId, "music ready (default url)", { path: path.basename(out) });
				return out;
			}
			logJob(jobId, "music default url invalid", { path: path.basename(out) });
		} catch (err) {
			logJob(jobId, "music default url failed", {
				error: String(err?.message || err).slice(0, 240),
			});
		} finally {
			if (!keepDownloadedDefault) safeUnlink(out);
		}
	}
	return null;
}

async function resolveBackgroundMusic({
	jobId,
	topic,
	categoryLabel = "",
	mood = "neutral",
	topics = [],
	disableMusic,
	requestedMusicUrl,
}) {
	if (disableMusic) return null;

	// 1) explicit musicUrl
	const musicUrl = String(requestedMusicUrl || "").trim();
	logJob(jobId, "music resolve start", {
		requested: Boolean(musicUrl),
		defaultFirst: MUSIC_USE_DEFAULT_FIRST,
		hasDefaultPath: Boolean(DEFAULT_MUSIC_PATH),
		hasDefaultUrl: Boolean(DEFAULT_MUSIC_URL),
		hasJamendoClient: Boolean(JAMENDO_CLIENT_ID),
		timeoutMs: MUSIC_RESOLVE_TIMEOUT_MS,
	});
	if (musicUrl) {
		const out = path.join(TMP_ROOT, `music_req_${jobId}.mp3`);
		logJob(jobId, "music requested download start", {
			timeoutMs: MUSIC_TRACK_DOWNLOAD_TIMEOUT_MS,
		});
		await downloadToFile(musicUrl, out, MUSIC_TRACK_DOWNLOAD_TIMEOUT_MS, 1);
		if (await validateMusicFile(out)) {
			logJob(jobId, "music ready (requested)", { path: path.basename(out) });
			return out;
		}
		safeUnlink(out);
		throw new Error("Requested musicUrl downloaded but is not valid audio");
	}

	// 2) Optional fixed fallback first, only when explicitly enabled.
	if (MUSIC_USE_DEFAULT_FIRST) {
		const defaultMusic = await tryResolveDefaultBackgroundMusic(jobId);
		if (defaultMusic) return defaultMusic;
	}

	// 3) Jamendo based on topic/mood, preferred for organic long-video music.
	const musicPlan = buildBackgroundMusicSearchPlan({
		topic,
		categoryLabel,
		mood,
		topics,
	});
	logJob(jobId, "jamendo music search start", {
		fuzzytags: musicPlan.fuzzytags,
		speed: musicPlan.speed,
	});
	const candidates = await jamendoSearchTracks({
		fuzzytags: musicPlan.fuzzytags,
		speed: musicPlan.speed,
		instrumentalOnly: true,
	});
	logJob(jobId, "jamendo music search result", {
		candidates: candidates.length,
		maxDownloadCandidates: MUSIC_MAX_DOWNLOAD_CANDIDATES,
	});
	candidates.sort((a, b) => {
		const scoreDelta =
			scoreJamendoMusicCandidate(b, musicPlan) -
			scoreJamendoMusicCandidate(a, musicPlan);
		if (scoreDelta) return scoreDelta;
		return Math.min(Number(b.duration || 0), 360) - Math.min(Number(a.duration || 0), 360);
	});

	for (let i = 0; i < Math.min(MUSIC_MAX_DOWNLOAD_CANDIDATES, candidates.length); i++) {
		const c = candidates[i];
		try {
			const out = path.join(TMP_ROOT, `music_${jobId}_${c.id}.mp3`);
			logJob(jobId, "jamendo music download attempt", {
				index: i,
				id: c.id,
				name: c.name,
				duration: c.duration,
				timeoutMs: MUSIC_TRACK_DOWNLOAD_TIMEOUT_MS,
			});
			await downloadToFile(c.audio, out, MUSIC_TRACK_DOWNLOAD_TIMEOUT_MS, 1);
			if (await validateMusicFile(out)) {
				logJob(jobId, "jamendo picked track", {
					id: c.id,
					name: c.name,
					artist: c.artist,
					duration: c.duration,
					shareurl: c.shareurl,
					musicPlan: {
						fuzzytags: musicPlan.fuzzytags,
						speed: musicPlan.speed,
						score: scoreJamendoMusicCandidate(c, musicPlan),
					},
				});
				return out;
			}
			safeUnlink(out);
			logJob(jobId, "jamendo music candidate invalid", {
				index: i,
				id: c.id,
				name: c.name,
			});
		} catch (err) {
			logJob(jobId, "jamendo music candidate failed", {
				index: i,
				id: c.id,
				name: c.name,
				error: String(err?.message || err).slice(0, 240),
			});
			// try next
		}
	}

	const defaultMusic = await tryResolveDefaultBackgroundMusic(jobId);
	if (defaultMusic) return defaultMusic;

	throw new Error(
		"Background music is required but could not be resolved. Provide JAMENDO_CLIENT_ID or musicUrl or LONG_VIDEO_DEFAULT_MUSIC_URL/PATH.",
	);
}

async function mixBackgroundMusic(
	baseVideoPath,
	musicPath,
	outPath,
	{ jobId },
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
	const durationLimit = duration ? duration.toFixed(3) : "9999";
	const voicePrep = duration
		? `apad,atrim=0:${durationLimit},asetpts=N/SR/TB,`
		: "";
	const musicPrep = `atrim=0:${durationLimit},asetpts=N/SR/TB`;
	const filter =
		`[0:a]aresample=${AUDIO_SR},aformat=channel_layouts=stereo:sample_fmts=fltp,${voicePrep}asplit=2[vox][vox_sc];` +
		`[1:a]aresample=${AUDIO_SR},aformat=channel_layouts=stereo:sample_fmts=fltp,volume=${vol.toFixed(
			3,
		)},${musicPrep}[music];` +
		`[music][vox_sc]sidechaincompress=threshold=${threshold.toFixed(
			3,
		)}:ratio=${ratio.toFixed(2)}:attack=${attack.toFixed(
			0,
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
		...(duration ? ["-t", durationLimit] : []),
		"-movflags",
		"+faststart",
		"-y",
		outPath,
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
		dur > 0 ? Math.min(1.2, dur / 2) : 0,
	);
	const shouldFade = Boolean(fadeDur && dur && dur >= 0.4);
	const start = shouldFade ? Math.max(0, dur - fadeDur) : 0;

	const inputInfo = await probeMedia(inputPath);
	const inputVideoStream =
		(inputInfo?.streams || []).find((s) => s.codec_type === "video") || {};
	const sourceW = makeEven(
		Number(inputVideoStream?.width || inputVideoStream?.coded_width || 0) ||
			Number(outCfg?.w || 0) ||
			1280,
	);
	const sourceH = makeEven(
		Number(inputVideoStream?.height || inputVideoStream?.coded_height || 0) ||
			Number(outCfg?.h || 0) ||
			720,
	);
	const { w: outW, h: outH } = computeFinalMasterSize(outCfg, {
		w: sourceW,
		h: sourceH,
	});
	const fps = Number(outCfg?.fps || DEFAULT_OUTPUT_FPS) || DEFAULT_OUTPUT_FPS;
	const gop = Math.max(12, Math.round(fps * FINAL_GOP_SECONDS));

	const audioFilters = [
		`aresample=${AUDIO_SR}`,
		"aformat=channel_layouts=stereo:sample_fmts=fltp",
	];
	if (shouldFade) {
		audioFilters.push(
			`afade=t=out:st=${start.toFixed(3)}:d=${fadeDur.toFixed(3)}`,
		);
	}
	audioFilters.push(FINAL_LOUDNORM_FILTER);

	const buildFinalArgs = ({ width, height, preset, crf }) => {
		const videoFilters = [
			`scale=${width}:${height}:force_original_aspect_ratio=increase:flags=lanczos,crop=${width}:${height}`,
			`fps=${fps}`,
			buildWatermarkFilter(),
		];
		if (shouldFade) {
			videoFilters.push(
				`fade=t=out:st=${start.toFixed(3)}:d=${fadeDur.toFixed(3)}`,
			);
		}
		videoFilters.push("format=yuv420p");
		return [
			"-i",
			inputPath,
			"-vf",
			videoFilters.join(","),
			"-af",
			audioFilters.join(","),
			"-c:v",
			"libx264",
			"-preset",
			preset,
			"-crf",
			String(crf),
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
		];
	};

	try {
		await spawnBin(
			ffmpegPath,
			buildFinalArgs({
				width: outW,
				height: outH,
				preset: FINAL_PRESET,
				crf: FINAL_VIDEO_CRF,
			}),
			"final_master",
			{ timeoutMs: FINAL_MASTER_TIMEOUT_MS },
		);
	} catch (err) {
		const fallbackPreset = FINAL_PRESET === "fast" ? "veryfast" : "fast";
		const fallbackCrf = clampNumber(FINAL_VIDEO_CRF + 2, 10, 28);
		const fallbackW = Math.min(outW, sourceW);
		const fallbackH = Math.min(outH, sourceH);
		const shouldRetry =
			fallbackPreset !== FINAL_PRESET ||
			fallbackCrf !== FINAL_VIDEO_CRF ||
			fallbackW !== outW ||
			fallbackH !== outH;
		if (!shouldRetry) throw err;
		console.warn(
			`[LongVideo] final master fallback after primary failure: ${
				err?.message || "unknown error"
			}`,
		);
		await spawnBin(
			ffmpegPath,
			buildFinalArgs({
				width: fallbackW,
				height: fallbackH,
				preset: fallbackPreset,
				crf: fallbackCrf,
			}),
			"final_master_fallback",
			{ timeoutMs: FINAL_MASTER_TIMEOUT_MS },
		);
	}
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

async function runLongVideoJob(
	jobId,
	payload,
	baseUrl,
	user = null,
	controllerConfig = {},
) {
	const controllerOptions =
		normalizeLongVideoControllerConfig(controllerConfig);
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
			musicUrl,
			disableMusic,
			dryRun,
			orchestratorDryRun,
			stopAfterThumbnail,
			skipPresenterAdjustments,
			overlayAssets,
			youtubeAccessToken,
			youtubeRefreshToken,
			youtubeTokenExpiresAt,
			youtubeCategory,
		} = payload;
		const hasProvidedVoiceoverUrl = Boolean(voiceoverUrl);
		const voiceoverUrlLocked = String(voiceoverUrl || "").trim();
		const enableHeyGenPresenterMotion = Boolean(
			payload.enableHeyGenPresenterMotion ??
				controllerOptions.enableHeyGenPresenterMotion ??
				true,
		);
		const enableWardrobeEdit = Boolean(
			payload.enableWardrobeEdit ?? controllerOptions.enableWardrobeEdit,
		);
		const effectiveVoiceId = String(ELEVEN_FIXED_VOICE_ID || "").trim();
		const contentTargetSec = Number(targetDurationSec || 0);
		const requestedCategoryLabel =
			normalizeCategoryLabel(category) || LONG_VIDEO_TRENDS_CATEGORY;
		let categoryLabel = requestedCategoryLabel;
		const promptTextForCategory = String(preferredTopicHint || "").trim();
		const earlyPromptCategoryLabel = promptTextForCategory
			? inferPromptCategoryLabel({ promptText: promptTextForCategory })
			: "";
		if (earlyPromptCategoryLabel) categoryLabel = earlyPromptCategoryLabel;
		let introDurationSec = clampNumber(
			DEFAULT_INTRO_SEC,
			INTRO_MIN_SEC,
			INTRO_MAX_SEC,
		);
		let outroDurationSec = clampNumber(
			DEFAULT_OUTRO_SEC,
			OUTRO_MIN_SEC,
			OUTRO_MAX_SEC,
		);
		const totalTargetSec =
			introDurationSec + contentTargetSec + outroDurationSec;
		const youtubeUploadEnabled = !controllerOptions.disableYouTubeUpload;
		const hasYouTubeTokens =
			youtubeUploadEnabled &&
			Boolean(
				youtubeRefreshToken || youtubeAccessToken || user?.youtubeRefreshToken,
			);
		let thumbnailPath = "";
		let thumbnailUrl = "";
		let thumbnailPublicId = "";
		let topicSourceSummary = [];

		logJob(jobId, "job started", {
			controller: controllerOptions.controllerLabel,
			controllerRuntime: getLongVideoRuntimeProfile(),
			dryRun,
			orchestratorDryRun,
			requestedTargetSec: Number(targetDurationSec || 0),
			contentTargetSec,
			category: categoryLabel,
			introSec: introDurationSec,
			outroSec: outroDurationSec,
			totalTargetSec,
			output,
			presenterAssetUrl: presenterAssetUrl ? "(provided)" : "(none)",
			hasVoiceoverUrl: Boolean(voiceoverUrlLocked),
			voiceoverLocked: Boolean(voiceoverUrlLocked),
			hasMusicUrl: Boolean(musicUrl),
			hasCseKeys: GOOGLE_CSE_CONFIG_READY,
			cseConfigIssue: GOOGLE_CSE_CONFIG_ISSUE || "",
			hasHeyGen: Boolean(HEYGEN_API_KEY),
			enableHeyGenPresenterMotion: Boolean(enableHeyGenPresenterMotion),
			enableWardrobeEdit: Boolean(enableWardrobeEdit),
			skipPresenterAdjustments: Boolean(skipPresenterAdjustments),
			disableYouTubeUpload: Boolean(controllerOptions.disableYouTubeUpload),
			stopAfterThumbnail: Boolean(stopAfterThumbnail),
			voiceIdLocked: effectiveVoiceId,
			hasYouTubeTokens,
		});
		if (
			earlyPromptCategoryLabel &&
			earlyPromptCategoryLabel !== requestedCategoryLabel
		) {
			logJob(jobId, "prompt category inferred", {
				stage: "request",
				requestedCategory: requestedCategoryLabel,
				inferredCategory: earlyPromptCategoryLabel,
			});
		}
		if (hasProvidedVoiceoverUrl) {
			logJob(jobId, "external narration locked", {
				mode: "content_voiceover_url",
			});
		}

		if (dryRun) {
			const dummyUrl = SHOULD_PERSIST_LONG_VIDEO
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

		if (!ffmpegPath && !orchestratorDryRun)
			throw new Error("FFmpeg not found. Install ffmpeg or set FFMPEG_PATH.");
		if (!HEYGEN_API_KEY && !orchestratorDryRun)
			throw new Error("HEYGEN_API_KEY missing (required for presenter pipeline).");
		if (!process.env.CHATGPT_API_TOKEN)
			throw new Error("CHATGPT_API_TOKEN missing.");
		if (!ELEVEN_API_KEY && !orchestratorDryRun)
			throw new Error(
				"ELEVENLABS_API_KEY missing (required for intro/outro voice).",
			);
		if (!effectiveVoiceId && !orchestratorDryRun)
			throw new Error("ELEVENLABS voiceId missing.");

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
			.map((t) => {
				const label = cleanTopicLabel(t.displayTopic || t.topic || "");
				if (!looksLikePromptStructureLabelOnly(label)) return label;
				const brief = t.promptBrief || parseStructuredPromptBrief(t.promptText);
				return (
					cleanTopicLabel(brief?.title || brief?.primaryTopic || "") || label
				);
			})
			.filter(Boolean);
		const topicSummary = topicTitles.join(" / ");
		const contentMode = topicPicks.some(
			(t) => String(t?.source || "").toLowerCase() === "user_prompt",
		)
			? "prompt"
			: "trends";
		if (contentMode === "prompt" && !earlyPromptCategoryLabel) {
			const refinedPromptCategoryLabel = inferPromptCategoryLabel({
				topics: topicPicks,
				promptText: promptTextForCategory,
			});
			if (
				refinedPromptCategoryLabel &&
				refinedPromptCategoryLabel !== categoryLabel
			) {
				logJob(jobId, "prompt category inferred", {
					stage: "topics",
					requestedCategory: requestedCategoryLabel,
					previousCategory: categoryLabel,
					inferredCategory: refinedPromptCategoryLabel,
				});
				categoryLabel = refinedPromptCategoryLabel;
			}
		}
		if (
			contentMode === "prompt" &&
			isSocialConnectionTopic({
				topics: topicPicks,
				categoryLabel,
				text: promptTextForCategory,
			}) &&
			categoryLabel !== "SocialIssues"
		) {
			logJob(jobId, "prompt category inferred", {
				stage: "social-override",
				requestedCategory: requestedCategoryLabel,
				previousCategory: categoryLabel,
				inferredCategory: "SocialIssues",
			});
			categoryLabel = "SocialIssues";
		}
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
				{ limit: 6 },
			);
			const articleHosts = uniqueStrings(
				articleUrls.map((u) => getUrlHost(u)).filter(Boolean),
				{ limit: 6 },
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
				angle: t.angle || "",
				topList: t.topList || null,
				source: t.source || "",
				directAnswerQuery: getTopicDirectAnswerQuery(t)
					? {
							type: getTopicDirectAnswerQuery(t).type,
							subject: getTopicDirectAnswerQuery(t).subject || "",
							bare: Boolean(getTopicDirectAnswerQuery(t).bare),
						}
					: null,
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
					topList: t.topList || null,
					directAnswerQuery: getTopicDirectAnswerQuery(t)
						? {
								type: getTopicDirectAnswerQuery(t).type,
								subject: getTopicDirectAnswerQuery(t).subject || "",
								bare: Boolean(getTopicDirectAnswerQuery(t).bare),
							}
						: null,
				})),
				category: categoryLabel,
				topicReason: topicPicks[0]?.reason || "",
				topicAngle: topicPicks[0]?.angle || "",
			},
		});

		// 2) Presenter (forced default image, later animated by HeyGen)
		let presenterLocal = await ensureLocalPresenterAsset(
			presenterAssetUrl,
			tmpDir,
			jobId,
			{
				defaultAssetUrl: controllerOptions.presenterAssetUrl,
				allowOverride: controllerOptions.allowPresenterAssetOverride,
			},
		);
		let presenterThumbnailLocal = presenterLocal;
		let presenterOutfit = "";
		let presenterOutfitStyle = "";
		let presenterHeyGenImageUrl = "";
		const motionRefVideo = null;
		const detected = detectFileType(presenterLocal);
		let presenterIsVideo = detected?.kind === "video";
		let presenterIsImage = detected?.kind === "image";
		if (!presenterIsImage) {
			throw new Error("Presenter asset must be a valid image");
		}

		logJob(jobId, "presenter asset ready", {
			path: path.basename(presenterLocal),
			detected: detected?.kind || "unknown",
			presenterVideoEngine: "heygen",
			thumbnailSource: presenterThumbnailLocal
				? path.basename(presenterThumbnailLocal)
				: null,
		});

		updateJob(jobId, { progressPct: 12 });

		// 4) Context + images (optional)
		const topicContexts = [];
		let liveContext = [];
		for (const t of topicPicks) {
			const promptBrief = t.promptBrief || null;
			const extraTokens = uniqueStrings(
				[
					...(Array.isArray(t.keywords) ? t.keywords : []),
					...(Array.isArray(promptBrief?.searchHints)
						? promptBrief.searchHints.flatMap((q) => topicTokensFromTitle(q))
						: []),
				],
				{ limit: 24 },
			);
			const isPromptTopic = isUserPromptTopicPick(t);
			const promptBriefContext = isPromptTopic
				? uniqueStrings(
						[
							...(Array.isArray(promptBrief?.briefLines)
								? promptBrief.briefLines
								: []),
							...(Array.isArray(promptBrief?.factLines)
								? promptBrief.factLines.map((line) => `User supplied fact/stat to verify: ${line}`)
								: []),
							promptBrief?.openingLine
								? `User requested opening line: ${promptBrief.openingLine}`
								: "",
							shouldLockPromptBriefTitle(promptBrief)
								? `User requested title: ${promptBrief.title}`
								: promptBrief?.title
									? `User reference title/topic: ${promptBrief.title}`
								: "",
						].filter(Boolean),
						{ limit: 18 },
					)
				: [];
			const promptNewsContext = isPromptTopic
				? await fetchPromptTopicNewsContext({
						topic: t.topic,
						searchHints: uniqueStrings(
							[
								...(Array.isArray(promptBrief?.searchHints)
									? promptBrief.searchHints
									: []),
								...(Array.isArray(t.searchHints) ? t.searchHints : []),
							],
							{ limit: 16 },
						),
						promptText: t.promptText,
						limit: getTopicDirectAnswerQuery(t)
							? Math.max(PROMPT_TOPIC_NEWS_CONTEXT_LIMIT, 12)
							: PROMPT_TOPIC_NEWS_CONTEXT_LIMIT,
						jobId,
						forceAllQueries: Boolean(getTopicDirectAnswerQuery(t)),
					})
				: [];
			const needsLimitedCse =
				!isPromptTopic ||
				Boolean(getTopicDirectAnswerQuery(t)) ||
				Boolean(t.topList?.count) ||
				countContextSourceLinks(promptNewsContext) <
					PROMPT_TOPIC_SOURCE_MIN_LINKS;
			const ctx = needsLimitedCse
				? await fetchCseContext(
						t.topic,
						extraTokens,
						isPromptTopic
							? {
									queries: uniqueStrings(
										[
											...(Array.isArray(promptBrief?.searchHints)
												? promptBrief.searchHints
												: []),
											...(Array.isArray(t.searchHints)
												? t.searchHints
												: []),
										],
										{ limit: 14 },
									),
									maxQueries: getTopicDirectAnswerQuery(t)
										? Math.max(PROMPT_TOPIC_CSE_QUERY_LIMIT, 8)
										: PROMPT_TOPIC_CSE_QUERY_LIMIT,
									num: PROMPT_TOPIC_CSE_RESULTS_PER_QUERY,
									maxPages: 1,
									limit: getTopicDirectAnswerQuery(t) ? 10 : 6,
								}
							: {},
					)
				: [];
			const sourceContext = uniqueContextItems(
				[
					...promptBriefContext,
					...promptNewsContext,
					...(Array.isArray(ctx) ? ctx : []),
				],
				{
					limit:
						isPromptTopic && getTopicDirectAnswerQuery(t)
							? 16
							: isPromptTopic
								? 12
								: 8,
				},
			);
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
				{ limit: 8 },
			);
			const mergedContext = uniqueContextItems(
				sourceContext.concat(trendContext),
				{ limit: getTopicDirectAnswerQuery(t) ? 18 : 14 },
			);
			topicContexts.push({ topic: t.topic, context: mergedContext });
			liveContext = liveContext.concat(mergedContext || []);
			if (isPromptTopic) {
				logJob(jobId, "prompt topic context plan", {
					topic: t.topic,
					newsSources: countContextSourceLinks(promptNewsContext),
					usedCse: Boolean(needsLimitedCse),
					cseSources: countContextSourceLinks(ctx),
					totalSources: countContextSourceLinks(sourceContext),
				});
			}
		}
		const cseImages = [];
		logJob(jobId, "cse context", {
			count: liveContext.length,
			sourceLinks: countContextSourceLinks(liveContext),
			byTopic: topicContexts.map((tc) => ({
				topic: tc.topic,
				count: Array.isArray(tc.context) ? tc.context.length : 0,
				sourceLinks: countContextSourceLinks(tc.context),
			})),
		});
		logJob(jobId, "cse images", { count: cseImages.length });
		enrichDirectAnswerTopicsFromContext(topicPicks, topicContexts, jobId);
		topicSourceSummary = topicContexts.map((tc, idx) => {
			const contextItems = Array.isArray(tc.context) ? tc.context : [];
			const cseLinks = uniqueStrings(
				contextItems.map((c) => c?.link).filter((u) => isHttpUrl(u)),
				{ limit: 6 },
			);
			const cseHosts = uniqueStrings(
				cseLinks.map((u) => getUrlHost(u)).filter(Boolean),
				{ limit: 6 },
			);
			const story = topicPicks[idx]?.trendStory || {};
			const articleUrls = uniqueStrings(
				(Array.isArray(story.articles) ? story.articles : [])
					.map((a) => a?.url)
					.filter((u) => isHttpUrl(u)),
				{ limit: 6 },
			);
			const articleHosts = uniqueStrings(
				articleUrls.map((u) => getUrlHost(u)).filter(Boolean),
				{ limit: 6 },
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
		const preScriptVisualResearch = await prefetchPreScriptVisualResearch({
			topics: topicPicks,
			baseUrl,
			jobId,
			category: categoryLabel,
		});
		if (preScriptVisualResearch.length) {
			updateJob(jobId, {
				meta: {
					...JOBS.get(jobId)?.meta,
					preScriptVisualResearch,
				},
			});
		}
		const topicContextFlags = topicContexts.map((tc, idx) => {
			const items = Array.isArray(tc.context) ? tc.context : [];
			const topicObj = Array.isArray(topicPicks) ? topicPicks[idx] : null;
			const contextStrings = buildTopicContextStrings(topicObj || tc, items);
			const contextText = contextStrings.join(" ");
			const isClearlyRealWorld = looksLikeRealWorldNewsContext(contextText);
			return {
				topic: tc.topic,
				isFictional: isClearlyRealWorld
					? false
					: detectFictionalContext(contextText),
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
		const voiceTonePlan = FORCE_NEUTRAL_VOICEOVER
			? { ...tonePlan, mood: "neutral" }
			: tonePlan;
		if (FORCE_NEUTRAL_VOICEOVER && tonePlan.mood !== "neutral") {
			logJob(jobId, "voice tone forced to neutral", {
				originalMood: tonePlan.mood,
			});
		}
		logJob(jobId, "topic context flags", {
			contentType,
			topics: topicContextFlags.map((t) => ({
				topic: t.topic,
				isFictional: t.isFictional,
			})),
		});

		const priorVideos = await loadSimilarPriorLongVideos({
			userId: user?._id,
			topics: topicPicks,
			promptText: promptTextForCategory || topicSummary,
			categoryLabel,
			jobId,
		});
		if (!priorVideos.length) {
			logJob(jobId, "prior similar long videos found", { count: 0 });
		}
		const priorVideoPlan = await buildPriorVideoNoveltyPlan({
			jobId,
			promptText: promptTextForCategory || topicSummary,
			topics: topicPicks,
			topicContexts,
			categoryLabel,
			priorVideos,
		});
		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				noveltyPlan: compactPriorVideoPlanForMeta(priorVideoPlan),
			},
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
			Number(narrationPlan?.targetSec || contentTargetSec) || 0,
		);
		const requestedTopListPlan = resolveTopListPlan(topicPicks, categoryLabel);
		const segmentCountBase = computeSegmentCount(narrationTargetSec);
		const segmentCount = requestedTopListPlan?.count
			? Math.max(segmentCountBase, requestedTopListPlan.count)
			: segmentCountBase;
		const wordCaps = buildWordCaps(segmentCount, narrationTargetSec);
		logJob(jobId, "narration target planned", {
			requestedSec: Number(contentTargetSec || 0),
			targetSec: Number(narrationTargetSec || 0),
			minSec: narrationPlan?.minSec,
			maxSec: narrationPlan?.maxSec,
			mode: narrationPlan?.mode,
			signal: narrationPlan?.signal,
			segmentCountBase,
			segmentCount,
			topList: requestedTopListPlan || null,
		});
		const preScriptVisualBeatPlan = buildPreScriptVisualBeatPlan({
			topics: topicPicks,
			segmentCount,
			jobId,
		});
		const preScriptVisualVideoProbe =
			await enrichPreScriptVisualBeatsWithFeedVideo({
				topics: topicPicks,
				topicContexts,
				category: categoryLabel,
				jobId,
			});
		if (preScriptVisualBeatPlan.length) {
			updateJob(jobId, {
				meta: {
					...JOBS.get(jobId)?.meta,
					preScriptVisualBeatPlan: preScriptVisualBeatPlan.map((plan) => ({
						topicIndex: plan.topicIndex,
						topic: plan.topic,
						beats: (plan.beats || []).map((beat) => ({
							id: beat.id,
							query: beat.query,
							imageCount: beat.imageCount,
							videoCandidateCount: beat.videoCandidateCount || 0,
							titleClues: beat.titleClues,
							sourceHosts: beat.sourceHosts,
						})),
					})),
					preScriptVisualVideoProbe,
				},
			});
		}

		let script;
		try {
			script = await generateScript({
				jobId,
				topics: topicPicks,
				languageLabel: lang,
				narrationTargetSec,
				segmentCount,
				wordCaps,
				topicContexts,
				tonePlan: voiceTonePlan,
				topicContextFlags,
				categoryLabel,
				includeOutro: true,
				contentMode,
				priorVideoPlan,
			});
		} catch (e) {
			if (!isOpenAiQuotaOrRateLimitError(e)) throw e;
			logJob(jobId, "openai script generation unavailable; using local fallback", {
				error: e.message,
			});
			script = buildLocalFallbackScript({
				jobId,
				topics: topicPicks,
				segmentCount,
				wordCaps,
				tonePlan: voiceTonePlan,
				contentMode,
			});
		}
		script = applyLongVideoScriptGuards({
			script,
			topics: topicPicks,
			wordCaps,
			priorVideoPlan,
			categoryLabel,
			mood: voiceTonePlan?.mood,
		});
		if (priorVideoPlan?.hasPriorVideos) {
			let noveltyEstimate = null;
			for (
				let noveltyAttempt = 0;
				noveltyAttempt < PRIOR_LONG_VIDEO_NOVELTY_REWRITE_ATTEMPTS;
				noveltyAttempt++
			) {
				script = await rewriteSegmentsForPriorNovelty({
					jobId,
					script,
					topics: topicPicks,
					topicContexts,
					wordCaps,
					tonePlan: voiceTonePlan,
					categoryLabel,
					includeOutro: true,
					priorVideoPlan,
				});
				script = applyLongVideoScriptGuards({
					script,
					topics: topicPicks,
					wordCaps,
					priorVideoPlan,
					categoryLabel,
					mood: voiceTonePlan?.mood,
				});
				noveltyEstimate = estimatePriorNovelty(script, priorVideoPlan);
				logJob(jobId, "prior novelty estimate", {
					attempt: noveltyAttempt + 1,
					estimate: noveltyEstimate,
				});
				if (
					!noveltyEstimate ||
					Number(noveltyEstimate.noveltyPct || 0) >=
						(Number(priorVideoPlan.requiredNewnessPct) || 65) - 5
				) {
					break;
				}
			}
		}

		let qaResult = analyzeScriptQuality({
			script,
			topics: topicPicks,
			topicContexts,
			wordCaps,
			categoryLabel,
		});
		let shortsGuardrails = analyzeShortsGuardrails(script);
		if (shortsGuardrails.needsRewrite) qaResult.needsRewrite = true;
		logJob(jobId, "script qa", {
			pass: qaResult.pass,
			needsRewrite: qaResult.needsRewrite,
			issues: qaResult.issues,
			warnings: qaResult.warnings,
			stats: qaResult.stats,
		});
		logJob(jobId, "shorts guardrails", shortsGuardrails);
		const initialUnsupportedAttributionRepair = repairUnsupportedAttributions({
			script,
			topics: topicPicks,
			topicContexts,
			wordCaps,
		});
		if (initialUnsupportedAttributionRepair.changed.length) {
			script = applyLongVideoScriptGuards({
				script: initialUnsupportedAttributionRepair.script,
				topics: topicPicks,
				wordCaps,
				priorVideoPlan,
				categoryLabel,
				mood: voiceTonePlan?.mood,
			});
			qaResult = analyzeScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
				categoryLabel,
			});
			shortsGuardrails = analyzeShortsGuardrails(script);
			if (shortsGuardrails.needsRewrite) qaResult.needsRewrite = true;
			logJob(jobId, "script unsupported attribution repaired", {
				repairs: initialUnsupportedAttributionRepair.changed,
				qa: {
					pass: qaResult.pass,
					needsRewrite: qaResult.needsRewrite,
					issues: qaResult.issues,
					warnings: qaResult.warnings,
					stats: qaResult.stats,
				},
			});
		}

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
					tonePlan: voiceTonePlan,
					narrationTargetSec,
					categoryLabel,
					includeOutro: true,
					contentMode,
					priorVideoPlan,
				});
				script = applyLongVideoScriptGuards({
					script,
					topics: topicPicks,
					wordCaps,
					priorVideoPlan,
					categoryLabel,
					mood: voiceTonePlan?.mood,
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
				categoryLabel,
			});
			shortsGuardrails = analyzeShortsGuardrails(script);
			if (shortsGuardrails.needsRewrite) qaResult.needsRewrite = true;
			logJob(jobId, "script qa rewrite result", {
				attempt: qaAttempt + 1,
				pass: qaResult.pass,
				needsRewrite: qaResult.needsRewrite,
				issues: qaResult.issues,
				warnings: qaResult.warnings,
				stats: qaResult.stats,
			});
			logJob(jobId, "shorts guardrails rewrite result", {
				attempt: qaAttempt + 1,
				...shortsGuardrails,
			});
		}
		if (!qaResult.pass) {
			throw new Error(
				`script_qa_failed:${qaResult.issues.join("|") || "unknown"}`,
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
		script = applyLongVideoScriptGuards({
			script,
			topics: topicPicks,
			wordCaps,
			priorVideoPlan,
			categoryLabel,
			mood: voiceTonePlan?.mood,
		});
		if (attributionFix.didInsert) {
			qaResult = analyzeScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
				categoryLabel,
			});
			shortsGuardrails = analyzeShortsGuardrails(script);
			if (shortsGuardrails.needsRewrite) qaResult.needsRewrite = true;
			logJob(jobId, "script qa attribution fix", {
				inserted: attributionFix.inserted,
				qa: qaResult,
			});
		}

		const residualRepair = repairResidualScriptQuality({
			script,
			topics: topicPicks,
			topicContexts,
			wordCaps,
			categoryLabel,
			shortsGuardrails,
		});
		if (residualRepair.repairs.length) {
			script = applyLongVideoScriptGuards({
				script: residualRepair.script,
				topics: topicPicks,
				wordCaps,
				priorVideoPlan,
				categoryLabel,
				mood: voiceTonePlan?.mood,
			});
			qaResult = analyzeScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
				categoryLabel,
			});
			shortsGuardrails = analyzeShortsGuardrails(script);
			if (shortsGuardrails.needsRewrite) qaResult.needsRewrite = true;
			logJob(jobId, "script residual quality repaired", {
				repairs: residualRepair.repairs,
				qa: {
					pass: qaResult.pass,
					needsRewrite: qaResult.needsRewrite,
					issues: qaResult.issues,
					warnings: qaResult.warnings,
					stats: qaResult.stats,
				},
				shortsGuardrails,
			});
		}

		const titlePromiseRepair = repairTitlePromiseCoverage({
			script,
			topics: topicPicks,
			topicContexts,
			wordCaps,
			log: (message, payload) => logJob(jobId, message, payload),
		});
		if (titlePromiseRepair.repairs.length) {
			script = applyLongVideoScriptGuards({
				script: titlePromiseRepair.script,
				topics: topicPicks,
				wordCaps,
				priorVideoPlan,
				categoryLabel,
				mood: voiceTonePlan?.mood,
			});
			qaResult = analyzeScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
				categoryLabel,
			});
			shortsGuardrails = analyzeShortsGuardrails(script);
			if (shortsGuardrails.needsRewrite) qaResult.needsRewrite = true;
			logJob(jobId, "script title-promise repair result", {
				repairs: titlePromiseRepair.repairs,
				qa: {
					pass: qaResult.pass,
					needsRewrite: qaResult.needsRewrite,
					issues: qaResult.issues,
					warnings: qaResult.warnings,
					stats: qaResult.stats,
				},
				shortsGuardrails,
			});
		}

		qaResult = analyzeScriptQuality({
			script,
			topics: topicPicks,
			topicContexts,
			wordCaps,
			categoryLabel,
		});
		for (let repairPass = 0; repairPass < 3; repairPass += 1) {
			const blockingQa = blockingScriptQualityIssues(qaResult);
			if (!blockingQa.length) break;
			if (blockingQa.includes("unsupported_source_attribution")) {
				const finalUnsupportedRepair = repairUnsupportedAttributions({
					script,
					topics: topicPicks,
					topicContexts,
					wordCaps,
				});
				if (finalUnsupportedRepair.changed.length) {
					script = applyLongVideoScriptGuards({
						script: finalUnsupportedRepair.script,
						topics: topicPicks,
						wordCaps,
						priorVideoPlan,
						categoryLabel,
						mood: voiceTonePlan?.mood,
					});
					qaResult = analyzeScriptQuality({
						script,
						topics: topicPicks,
						topicContexts,
						wordCaps,
						categoryLabel,
					});
					logJob(jobId, "script final unsupported attribution repair", {
						repairPass: repairPass + 1,
						repairs: finalUnsupportedRepair.changed,
						qa: {
							pass: qaResult.pass,
							needsRewrite: qaResult.needsRewrite,
							issues: qaResult.issues,
							warnings: qaResult.warnings,
							stats: qaResult.stats,
						},
					});
					continue;
				}
			}
			const finalArtifactRepair = repairBlockingScriptArtifacts({
				script,
				topics: topicPicks,
				wordCaps,
				categoryLabel,
			});
			if (!finalArtifactRepair.changed.length) break;
			script = applyLongVideoScriptGuards({
				script: finalArtifactRepair.script,
				topics: topicPicks,
				wordCaps,
				priorVideoPlan,
				categoryLabel,
				mood: voiceTonePlan?.mood,
			});
			qaResult = analyzeScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
				categoryLabel,
			});
			logJob(jobId, "script final artifact repair", {
				repairPass: repairPass + 1,
				blocking: blockingQa,
				repairs: finalArtifactRepair.changed,
				qa: {
					pass: qaResult.pass,
					needsRewrite: qaResult.needsRewrite,
					issues: qaResult.issues,
					warnings: qaResult.warnings,
					stats: qaResult.stats,
				},
			});
		}
		const remainingBlockingQa = blockingScriptQualityIssues(qaResult);
		if (remainingBlockingQa.length) {
			logJob(jobId, "script quality hard stop before tts", {
				blocking: remainingBlockingQa,
				qa: qaResult,
			});
			throw new Error(`script_quality_blocked:${remainingBlockingQa.join("|")}`);
		}
		shortsGuardrails = analyzeShortsGuardrails(script);
		if (shortsGuardrails.needsRewrite) qaResult.needsRewrite = true;

		const plannedIntroOutroMood = voiceTonePlan?.mood || "neutral";
		const plannedIntroText =
			sanitizeIntroOutroLine(
				buildIntroLine({
					topics: topicPicks,
					shortTitle: script.shortTitle || script.title,
					mood: plannedIntroOutroMood,
					jobId,
				}),
			) || "";
		const plannedOutroText =
			sanitizeIntroOutroLine(
				buildOutroLine({
					topics: topicPicks,
					shortTitle: script.shortTitle || script.title,
					mood: plannedIntroOutroMood,
				}),
			) || "";
		const plannedIntroExpression = resolveOpeningPresenterExpression({
			topics: topicPicks,
			categoryLabel,
			title: script.title,
			mood: plannedIntroOutroMood,
		});
		const plannedOutroExpression = "warm";
		const plannedIntroCarriesDirectAnswer = introCarriesDirectAnswer({
			introText: plannedIntroText,
			topics: topicPicks,
		});
		if (plannedIntroCarriesDirectAnswer) {
			script = { ...script, directAnswerHandledByIntro: true };
		}
		const introDedupe = removeIntroLineFromOpeningSegment({
			script,
			introText: plannedIntroText,
			topics: topicPicks,
			wordCaps,
			categoryLabel,
			mood: plannedIntroOutroMood,
		});
		if (introDedupe.changed) {
			script = applyLongVideoScriptGuards({
				script: introDedupe.script,
				topics: topicPicks,
				wordCaps,
				priorVideoPlan,
				categoryLabel,
				mood: voiceTonePlan?.mood,
				injectOpeningLine: false,
			});
			qaResult = analyzeScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
				categoryLabel,
			});
			shortsGuardrails = analyzeShortsGuardrails(script);
			if (shortsGuardrails.needsRewrite) qaResult.needsRewrite = true;
			logJob(jobId, "opening line moved to intro", {
				removedFromSegment: introDedupe.removed,
				segment0Now: script.segments?.[0]?.text || "",
				introText: plannedIntroText,
				introExpression: plannedIntroExpression,
			});
		}
		const plannedIntroOutro = {
			mood: plannedIntroOutroMood,
			intro: {
				text: plannedIntroText,
				targetSec: introDurationSec,
				expression: plannedIntroExpression,
			},
			outro: {
				text: plannedOutroText,
				targetSec: outroDurationSec,
				expression: plannedOutroExpression,
				silentSmileTailSec: OUTRO_SMILE_TAIL_SEC,
			},
		};

		const preTtsVisualPlan = computeContentVisualPlan(
			Array.isArray(script.segments) ? script.segments.length : 0,
			{
				videoDurationSec:
					Number(narrationTargetSec || 0) +
					Number(introDurationSec || 0) +
					Number(outroDurationSec || 0),
			},
		);
		const visualGrounding = await groundScriptInValidatedVisuals({
			script,
			topics: topicPicks,
			topicContexts,
			category: categoryLabel,
			baseUrl,
			jobId,
			plannedImagePositions: preTtsVisualPlan.imagePositions,
		});
		if (visualGrounding?.script) {
			script = applyLongVideoScriptGuards({
				script: visualGrounding.script,
				topics: topicPicks,
				wordCaps,
				priorVideoPlan,
				categoryLabel,
				mood: voiceTonePlan?.mood,
				injectOpeningLine: false,
			});
		}
		if (visualGrounding?.summary) {
			updateJob(jobId, {
				meta: {
					...JOBS.get(jobId)?.meta,
					scriptVisualGrounding: visualGrounding.summary,
				},
			});
		}

		shortsGuardrails = analyzeShortsGuardrails(script);
		if (shortsGuardrails.needsRewrite) {
			const postVisualRepair = repairResidualScriptQuality({
				script,
				topics: topicPicks,
				topicContexts,
				wordCaps,
				categoryLabel,
				shortsGuardrails,
			});
			if (postVisualRepair.repairs.length) {
				script = applyLongVideoScriptGuards({
					script: postVisualRepair.script,
					topics: topicPicks,
					wordCaps,
					priorVideoPlan,
					categoryLabel,
					mood: voiceTonePlan?.mood,
					injectOpeningLine: false,
				});
				qaResult = analyzeScriptQuality({
					script,
					topics: topicPicks,
					topicContexts,
					wordCaps,
					categoryLabel,
				});
				shortsGuardrails = analyzeShortsGuardrails(script);
				logJob(jobId, "script post-visual guardrail repaired", {
					repairs: postVisualRepair.repairs,
					qa: {
						pass: qaResult.pass,
						needsRewrite: qaResult.needsRewrite,
						issues: qaResult.issues,
						warnings: qaResult.warnings,
						stats: qaResult.stats,
					},
					shortsGuardrails,
				});
			}
		}
		shortsGuardrails = analyzeShortsGuardrails(script);
		if (shortsGuardrails?.issues?.includes("ending_open_loop_missing")) {
			const forcedEndingRepair = forceFinalOpenLoopSegment({
				script,
				topics: topicPicks,
				categoryLabel,
				wordCaps,
			});
			if (forcedEndingRepair.changed) {
				script = forcedEndingRepair.script;
				qaResult = analyzeScriptQuality({
					script,
					topics: topicPicks,
					topicContexts,
					wordCaps,
					categoryLabel,
				});
				shortsGuardrails = analyzeShortsGuardrails(script);
				logJob(jobId, "script final open-loop forced", {
					qa: {
						pass: qaResult.pass,
						needsRewrite: qaResult.needsRewrite,
						issues: qaResult.issues,
						warnings: qaResult.warnings,
						stats: qaResult.stats,
					},
					shortsGuardrails,
					finalSegment: script.segments?.[script.segments.length - 1]?.text || "",
				});
			}
		}

		const shortsDetailsRaw = await ensureShortsDetails({
			jobId,
			script,
			topics: topicPicks,
		});
		const shortsDetails =
			shortsDetailsRaw && typeof shortsDetailsRaw === "object"
				? {
						...shortsDetailsRaw,
						status: shortsDetailsRaw.status || "planned",
						plannedAt: shortsDetailsRaw.plannedAt || nowIso(),
					}
				: null;
		script.shortsDetails = shortsDetails;

		const scriptEngagement = summarizeScriptEngagement(script);
		logJob(jobId, "script qa summary", {
			qa: qaResult,
			shortsGuardrails,
			engagement: scriptEngagement,
			novelty: estimatePriorNovelty(script, priorVideoPlan),
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
				shortsGuardrails,
				shortsDetails,
				introOutroPlan: plannedIntroOutro,
				script: { title: script.title, segments: script.segments },
			},
		});

		if (orchestratorDryRun) {
			const visualPlan = computeContentVisualPlan(
				Array.isArray(script.segments) ? script.segments.length : 0,
				{
					videoDurationSec:
						Number(narrationTargetSec || 0) +
						Number(introDurationSec || 0) +
						Number(outroDurationSec || 0),
				},
			);
			const estimatedWords = (script.segments || []).reduce(
				(sum, seg) => sum + countWords(seg.text || ""),
				0,
			);
			const dryRunMeta = {
				...JOBS.get(jobId)?.meta,
				orchestratorDryRun: true,
				title: script.title,
				shortTitle: script.shortTitle,
				requestedTargetSec: Number(contentTargetSec || 0),
				chosenNarrationSec: Number(narrationTargetSec || 0),
				estimatedWords,
				estimatedSpokenSec: Number(
					(estimatedWords / Math.max(0.1, SCRIPT_VOICE_WPS)).toFixed(1),
				),
				narrationPlan: {
					requestedSec: Number(contentTargetSec || 0),
					targetSec: Number(narrationTargetSec || 0),
					minSec: narrationPlan?.minSec,
					maxSec: narrationPlan?.maxSec,
					mode: narrationPlan?.mode,
					signal: narrationPlan?.signal,
				},
				visualPlan: {
					totalSegments: visualPlan.totalSegments,
					presenterSegments: visualPlan.presenterPositions,
					feedImageOrVideoSegments: visualPlan.imagePositions,
					forcedPresenterSegments: visualPlan.forcedPresenterPositions,
					openingPresenterCount: visualPlan.openingPresenterCount,
					targetHeyGenCalls: visualPlan.targetHeyGenCalls,
					maxOptionalHeyGenContentSegments:
						visualPlan.maxOptionalHeyGenContentSegments,
					optionalPresenterClusters: visualPlan.optionalPresenterClusters,
				},
				introOutroPlan: plannedIntroOutro,
				scriptQa: {
					pass: qaResult.pass,
					issues: qaResult.issues,
					warnings: qaResult.warnings,
					stats: qaResult.stats,
				},
				shortsGuardrails,
				shortsDetails,
				script: { title: script.title, segments: script.segments },
				scriptEngagement,
				thumbnailSkipped: true,
				heygenSkipped: true,
				ttsSkipped: true,
			};
			updateJob(jobId, {
				status: "completed",
				progressPct: 100,
				finalVideoUrl: null,
				meta: dryRunMeta,
			});
			logJob(jobId, "orchestrator dry run completed before paid/render steps", {
				requestedSec: Number(contentTargetSec || 0),
				chosenNarrationSec: Number(narrationTargetSec || 0),
				segments: script.segments?.length || 0,
				presenterSegments: visualPlan.presenterPositions,
				feedImageOrVideoSegments: visualPlan.imagePositions,
				openingPresenterCount: visualPlan.openingPresenterCount,
				targetHeyGenCalls: visualPlan.targetHeyGenCalls,
				maxOptionalHeyGenContentSegments:
					visualPlan.maxOptionalHeyGenContentSegments,
				optionalPresenterClusters: visualPlan.optionalPresenterClusters,
				thumbnailSkipped: true,
				heygenSkipped: true,
				ttsSkipped: true,
			});
			return;
		}

		// 5.5) Presenter wardrobe adjustment (post-script)
		if (enableWardrobeEdit && presenterIsImage) {
			try {
				const presenterTitle = String(
					script.title || topicSummary || topicTitles[0] || "",
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
							presenterResult.presenterOutfit || "",
						).trim();
						presenterOutfitStyle = String(
							presenterResult.presenterOutfitStyle || "",
						).trim();
						presenterHeyGenImageUrl = String(presenterResult.url || "").trim();
						logJob(jobId, "presenter adjustments ready", {
							path: path.basename(presenterLocal),
							method: presenterResult.method || "approved_presenter_outfit_library",
							cloudinary: Boolean(presenterResult.url),
							style: presenterOutfitStyle || "",
						});
						updateJob(jobId, {
							meta: {
								...JOBS.get(jobId)?.meta,
								presenterImageUrl: presenterResult.url || "",
								presenterOutfit,
								presenterOutfitStyle,
							},
						});
						presenterThumbnailLocal = presenterLocal;
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
		} else if (!enableWardrobeEdit && presenterIsImage) {
			logJob(jobId, "presenter adjustments skipped (disabled)", {
				reason: skipPresenterAdjustments ? "request" : "config",
				thumbnailSource: path.basename(presenterThumbnailLocal || presenterLocal),
			});
		}

		// 5.6) Thumbnail (script-aligned, uses adjusted presenter when available)
		try {
			const fallbackTitle = topicTitles[0] || topicSummary || "Quick Update";
			const thumbTitle = String(script.title || fallbackTitle).trim();
			const thumbShortTitle = String(
				script.shortTitle || shortTitleFromText(thumbTitle),
			).trim();
			const thumbLog = (message, payload) => logJob(jobId, message, payload);
			const promptThumbnailText = primaryPromptThumbnailText(topicPicks);
			let hookPlan = buildThumbnailHookPlan({
				title: thumbTitle,
				topicPicks,
			});
			if (promptThumbnailText) {
				hookPlan = {
					...(hookPlan || {}),
					headline: promptThumbnailText,
				};
				thumbLog("thumbnail prompt text override", {
					headline: promptThumbnailText,
					badgeText: hookPlan?.badgeText || "",
				});
			}
			if (hookPlan) thumbLog("thumbnail hook plan (computed)", hookPlan);
			await ensureThumbnailSeedImages({
				topics: topicPicks,
				tmpDir,
				jobId,
				output,
				baseUrl,
				imageQueries: hookPlan?.imageQueries || [],
				log: thumbLog,
			});
			let thumbExpression =
				script?.segments?.[0]?.expression || voiceTonePlan?.mood || "neutral";
			const hookHeadline = String(hookPlan?.headline || "").trim();
			const resolvedShortTitle = hookHeadline || thumbShortTitle;
			const thumbResult = await generateThumbnailPackage({
				jobId,
				tmpDir,
				presenterLocalPath: presenterThumbnailLocal,
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
				if (SHOULD_PERSIST_LONG_VIDEO) {
					const finalThumb = path.join(THUMBNAIL_DIR, `thumb_${jobId}.jpg`);
					fs.copyFileSync(thumbLocalPath, finalThumb);
					thumbnailPath = finalThumb;
				}
			}

			updateJob(jobId, {
				meta: {
					...JOBS.get(jobId)?.meta,
					thumbnailPath: SHOULD_PERSIST_LONG_VIDEO ? thumbnailPath : "",
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
				method: thumbResult?.method || null,
				comfy: thumbResult?.comfy || null,
			});
			if (stopAfterThumbnail) {
				updateJob(jobId, {
					status: "completed",
					progressPct: 35,
					finalVideoUrl: null,
					meta: {
						...JOBS.get(jobId)?.meta,
						thumbnailOnly: true,
						thumbnailUrl: thumbnailUrl || "",
						thumbnailPublicId: thumbnailPublicId || "",
					},
				});
				logJob(jobId, "thumbnail-only stop requested", {
					thumbnailUrl: thumbnailUrl || "",
					method: thumbResult?.method || null,
				});
				return;
			}
		} catch (e) {
			logJob(jobId, "thumbnail generation failed (hard stop)", {
				error: e.message,
			});
			throw e;
		}

		const seoMeta = await buildSeoMetadata({
			topics: topicPicks,
			scriptTitle: script.title,
			scriptText: buildScriptLogText(script),
			languageLabel: lang,
			lockTitle: shouldLockPromptBriefTitle(primaryPromptBrief(topicPicks)),
			titleInstructions: collectPromptSeoTitleInstructions(topicPicks),
			priorVideoPlan,
			categoryLabel,
			topicContexts,
		});
		const promptYoutubeCategoryLabel = resolveYoutubeCategoryLabelForPrompt({
			categoryLabel,
			topics: topicPicks,
			script,
		});
		const youtubeCategoryFinal =
			contentMode === "prompt" && YT_CATEGORY_MAP[promptYoutubeCategoryLabel]
				? promptYoutubeCategoryLabel
				: YT_CATEGORY_MAP[youtubeCategory]
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
		const introOutroMood =
			plannedIntroOutro?.mood || voiceTonePlan?.mood || "neutral";
		const introLine =
			plannedIntroOutro?.intro?.text ||
			buildIntroLine({
				topics: topicPicks,
				shortTitle: script.shortTitle || script.title,
				mood: introOutroMood,
				jobId,
			});
		const outroLine =
			plannedIntroOutro?.outro?.text ||
			buildOutroLine({
				topics: topicPicks,
				shortTitle: script.shortTitle || script.title,
				mood: introOutroMood,
			});
		const introText =
			sanitizeIntroOutroLine(introLine) || String(introLine || "").trim();
		const outroText =
			sanitizeIntroOutroLine(outroLine) || String(outroLine || "").trim();
		let introTextFinal = introText;
		let outroTextFinal = outroText;
		const contentCtaCleanedScript = removeContentCtasForSeparateOutro({
			script,
			topics: topicPicks,
			wordCaps,
			categoryLabel,
			mood: voiceTonePlan?.mood,
			outroText: outroTextFinal,
		});
		if (
			JSON.stringify((contentCtaCleanedScript.segments || []).map((s) => s.text)) !==
			JSON.stringify((script.segments || []).map((s) => s.text))
		) {
			script = contentCtaCleanedScript;
			logJob(jobId, "content CTA removed for separate outro", {
				outroText: outroTextFinal,
				lastSegment: script.segments?.[script.segments.length - 1]?.text || "",
			});
		}
		const introExpression =
			plannedIntroOutro?.intro?.expression ||
			resolveOpeningPresenterExpression({
				topics: topicPicks,
				categoryLabel,
				title: script.title,
				mood: introOutroMood,
			});
		const outroExpression = plannedIntroOutro?.outro?.expression || "warm";

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

		const lockedVoiceSettings = UNIFORM_TTS_VOICE_SETTINGS
			? buildVoiceSettingsForExpression("neutral", "neutral", "", {
					uniform: true,
					forceNeutral: FORCE_NEUTRAL_VOICEOVER,
				})
			: null;
		const resolveVoiceSettings = (expression, text) =>
			lockedVoiceSettings ||
			buildVoiceSettingsForExpression(expression, voiceTonePlan?.mood, text, {
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
				INTRO_MAX_SEC,
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
				OUTRO_MAX_SEC,
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
		let musicLocalPath = null;
		try {
			musicLocalPath = await withTimeout(
				resolveBackgroundMusic({
					jobId,
					topic: topicTitles[0] || topicSummary,
					categoryLabel,
					mood: voiceTonePlan?.mood || tonePlan?.mood || "neutral",
					topics: topicPicks,
					disableMusic,
					requestedMusicUrl: musicUrl,
				}),
				MUSIC_RESOLVE_TIMEOUT_MS,
				"background music resolution",
			);
		} catch (err) {
			logJob(jobId, "music resolve failed", {
				error: String(err?.message || err).slice(0, 320),
			});
			throw err;
		}
		logJob(jobId, "music resolve complete", {
			hasMusic: Boolean(musicLocalPath),
			path: musicLocalPath ? path.basename(musicLocalPath) : null,
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
				normalizeExpression(s.expression, voiceTonePlan?.mood),
				s.text,
				voiceTonePlan?.mood,
				s.topicLabel,
			),
			countdownRank: Number.isFinite(Number(s.countdownRank))
				? Number(s.countdownRank)
				: null,
			countdownLabel: cleanTopicLabel(s.countdownLabel || ""),
			overlayCues: Array.isArray(s.overlayCues) ? s.overlayCues : [],
			imageUrls: Array.isArray(s.imageUrls) ? s.imageUrls : [],
			feedVideoCandidates: Array.isArray(s.feedVideoCandidates)
				? s.feedVideoCandidates
				: [],
			visualGrounding:
				s.visualGrounding && typeof s.visualGrounding === "object"
					? s.visualGrounding
					: null,
		}));
		const smoothedExpressions = smoothExpressionPlan(
			segments.map((s) => s.expression),
			voiceTonePlan?.mood,
		);
		let segmentsWithExpressions = segments.map((s, i) => ({
			...s,
			expression: smoothedExpressions[i] || s.expression,
		}));
		if (FORCE_NEUTRAL_VOICEOVER) {
			segmentsWithExpressions = segmentsWithExpressions.map((s) =>
				s.expression === "excited" ? { ...s, expression: "warm" } : s,
			);
		}
		const videoExpressionPlan = buildSubtleVideoExpressionPlan(
			segmentsWithExpressions,
			voiceTonePlan?.mood,
			jobId,
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
						"",
				).trim() ||
				String(topicTitles[0] || "").trim(),
		}));

		let cleanedWavs = [];
		let sumCleanDur = 0;
		let globalAtempo = 1;
		let driftSec = 0;
		let autoOverlayAssets = [];
		let segmentImagePaths = new Map();
		let segmentFeedVideoPaths = new Map();
		const maxRewriteAttempts = voiceoverUrlLocked ? 0 : MAX_SCRIPT_REWRITES;

		for (let attempt = 0; attempt <= maxRewriteAttempts; attempt++) {
			cleanedWavs = [];
			sumCleanDur = 0;

			if (voiceoverUrlLocked) {
				// If you provide a full voiceoverUrl, we keep your narration as the source
				// and split it proportionally to the actual script instead of forcing TTS.
				const voicePath = path.join(tmpDir, `voice_${jobId}.wav`);
				await downloadToFile(voiceoverUrlLocked, voicePath, 45000, 2);

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
					{ timeoutMs: 180000 },
				);
				safeUnlink(voicePath);

				// Split narration proportionally to the actual script, not into equal chunks.
				const totalVoiceDur = await probeDurationSeconds(voiceWav);
				const sliceDurations = buildVoiceoverSliceDurations(
					segments,
					totalVoiceDur,
				);
				let cursor = 0;
				logJob(jobId, "external narration slicing", {
					totalVoiceDur: Number((totalVoiceDur || 0).toFixed(3)),
					sliceDurations: sliceDurations.map((dur) =>
						Number((dur || 0).toFixed(3)),
					),
				});
				for (let i = 0; i < segments.length; i++) {
					const start = cursor;
					const dur =
						i === segments.length - 1
							? Math.max(0.15, totalVoiceDur - cursor)
							: Math.max(0.15, Number(sliceDurations[i] || 0.15));
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
						{ timeoutMs: 120000 },
					);
					const d = await probeDurationSeconds(out);
					cleanedWavs.push({ index: i, wav: out, cleanDur: d });
					sumCleanDur += d;
					cursor += dur;
				}
				safeUnlink(voiceWav);
			} else {
				logJob(jobId, "eleven voice locked", {
					voiceId: effectiveVoiceId,
					attempt,
					modelId: ttsModelId || "auto",
				});

				const breathState = { used: 0 };
				for (const seg of segments) {
					const rawText = seg.text;
					const cleanText = sanitizeSegmentText(rawText);
					const breathedText = injectMicroBreath(cleanText, breathState);
					const textChanged =
						String(rawText || "").trim() !== String(breathedText || "").trim();
					seg.text = breathedText;
					const voiceSettings = resolveVoiceSettings(
						seg.expression,
						breathedText,
					);
					logJob(jobId, "tts segment start", {
						segment: seg.index,
						attempt,
						words: countWords(breathedText),
						text: breathedText,
						expression: seg.expression,
						voiceSettings,
						voiceId: effectiveVoiceId,
						modelId: ttsModelId || "auto",
						textChanged,
					});
					const tts = await synthesizeTtsWav({
						text: breathedText,
						tmpDir,
						jobId,
						label: `seg_${seg.index}`,
						voiceId: effectiveVoiceId,
						voiceSettings,
						modelId: ttsModelId || undefined,
						modelOrder: ttsModelOrder,
					});
					if (!ttsModelId && tts?.modelId) ttsModelId = tts.modelId;
					if (!tts?.wavPath)
						throw new Error("Voice audio generation failed (empty segment)");
					const d =
						Number(tts.durationSec) ||
						(await probeDurationSeconds(tts.wavPath));
					logJob(jobId, "tts segment ready", {
						segment: seg.index,
						attempt,
						cleanDur: Number(d.toFixed(3)),
						modelId: tts?.modelId || ttsModelId || "auto",
						qaPass: tts?.qa?.pass ?? null,
						qaIssues: Array.isArray(tts?.qa?.issues) ? tts.qa.issues : [],
					});
					cleanedWavs.push({ index: seg.index, wav: tts.wavPath, cleanDur: d });
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
				Math.max(1, narrationTargetSec * 0.07),
			);
			const ratioDelta = Math.abs(1 - rawAtempo);
			const overageSec = sumCleanDur - narrationTargetSec;
			const maxOverageSec = Math.max(
				0,
				Math.min(
					MAX_NARRATION_OVERAGE_SEC,
					narrationTargetSec * (MAX_NARRATION_OVERAGE_RATIO - 1),
				),
			);
			const allowOverage =
				ALLOW_NARRATION_OVERRUN &&
				overageSec > 0 &&
				overageSec <= maxOverageSec;
			const withinTolerance = driftSec <= toleranceSec || allowOverage;
			const rawCloseEnough =
				allowOverage ||
				ratioDelta <= REWRITE_CLOSE_RATIO_DELTA ||
				driftSec <= toleranceSec * REWRITE_CLOSE_DRIFT_MULT;
			const shouldSlowToTarget =
				ALLOW_SLOW_NARRATION_TO_TARGET &&
				rawAtempo < 1 &&
				(ratioDelta >= 0.04 || driftSec > toleranceSec);
			const shouldSpeedToTarget =
				rawAtempo > 1 && (ratioDelta >= 0.04 || driftSec > toleranceSec);
			const shouldTimeStretch =
				!voiceoverUrlLocked && (shouldSlowToTarget || shouldSpeedToTarget);
			globalAtempo = shouldTimeStretch
				? clampNumber(rawAtempo, GLOBAL_ATEMPO_MIN, GLOBAL_ATEMPO_MAX)
				: 1;
			const shouldApplyVoiceSpeedBoost =
				!voiceoverUrlLocked &&
				VOICE_SPEED_BOOST &&
				VOICE_SPEED_BOOST !== 1;
			if (shouldApplyVoiceSpeedBoost) {
				globalAtempo = clampNumber(
					globalAtempo * VOICE_SPEED_BOOST,
					GLOBAL_ATEMPO_MIN,
					GLOBAL_ATEMPO_MAX,
				);
			}
			const willApplyAtempo =
				globalAtempo > 0 && Math.abs(globalAtempo - 1) >= 0.0005;
			const projectedNarrationSec =
				willApplyAtempo
					? sumCleanDur / globalAtempo
					: sumCleanDur;
			const projectedDriftSec = Math.abs(
				projectedNarrationSec - narrationTargetSec,
			);
			const projectedOverageSec = projectedNarrationSec - narrationTargetSec;
			const projectedAllowOverage =
				ALLOW_NARRATION_OVERRUN &&
				projectedOverageSec > 0 &&
				projectedOverageSec <= maxOverageSec;
			const atempoCanRecover =
				willApplyAtempo &&
				(projectedDriftSec <= toleranceSec || projectedAllowOverage);
			const closeEnough = rawCloseEnough || atempoCanRecover;
			logJob(jobId, "global atempo computed", {
				sumCleanDur: Number(sumCleanDur.toFixed(3)),
				narrationTargetSec: Number(narrationTargetSec.toFixed(3)),
				atempo: Number(globalAtempo.toFixed(4)),
				rawAtempo: Number(rawAtempo.toFixed(4)),
				driftSec: Number(driftSec.toFixed(3)),
				projectedNarrationSec: Number(projectedNarrationSec.toFixed(3)),
				projectedDriftSec: Number(projectedDriftSec.toFixed(3)),
				withinTolerance,
				toleranceSec: Number(toleranceSec.toFixed(3)),
				ratioDelta: Number(ratioDelta.toFixed(3)),
				closeEnough,
				atempoCanRecover,
				shouldTimeStretch,
				shouldSlowToTarget,
				shouldSpeedToTarget,
				willApplyAtempo,
				allowOverage,
				overageSec: Number(overageSec.toFixed(3)),
				projectedOverageSec: Number(projectedOverageSec.toFixed(3)),
				maxOverageSec: Number(maxOverageSec.toFixed(3)),
				attempt,
				voiceSpeedBoost: VOICE_SPEED_BOOST,
				voiceSpeedBoostApplied: Boolean(shouldApplyVoiceSpeedBoost),
				durationRewriteEnabled: REWRITE_FOR_NARRATION_DURATION,
				allowSlowNarrationToTarget: ALLOW_SLOW_NARRATION_TO_TARGET,
			});

			const needsRewrite =
				!voiceoverUrlLocked &&
				REWRITE_FOR_NARRATION_DURATION &&
				!allowOverage &&
				!atempoCanRecover &&
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
				REWRITE_ADJUST_MAX,
			);
			const direction = ratio > 1 ? "LONGER" : "SHORTER";
			const adjustedCaps = wordCaps.map((c) =>
				Math.max(12, Math.round(c * dampedRatio)),
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
						})`,
				)
				.join(", ");
			const timingCategoryGuide = buildCategoryScriptGuide(
				categoryLabel,
				topicPicks,
			);
			const timingNeedsMeasuredTone = Boolean(
				timingCategoryGuide?.isPolitics ||
					timingCategoryGuide?.isHealth ||
					timingCategoryGuide?.isSerious ||
					voiceTonePlan?.mood === "serious",
			);
			const timingToneRule = timingNeedsMeasuredTone
				? "- Keep the same topic and tone for a US audience; use a brisk but measured news-presenter cadence."
				: "- Keep the same topic and tone for a US audience; make it natural, sharp, brisk, and audience-friendly.";
			const timingCasualRule = timingNeedsMeasuredTone
				? '- Do not use casual filler pivots like "real quick", "here\'s the thing", "that\'s wild", or exaggerated reactions.'
				: '- Keep it lightly conversational; use at most one friendly natural pivot per topic when it truly fits, and avoid repeated catchphrases.';
			const timingPromiseGuide = buildTitlePromisePromptBlock({
				script: { ...script, segments },
				topics: topicPicks,
				topicContexts,
			});
			const timingPromptBriefGuide = buildPromptBriefInstructionBlock(topicPicks);
			const timingDirectAnswerGuide = buildDirectAnswerPromptBlock(
				topicPicks,
				topicContexts,
			);
			const timingAnswerRule = topicPicks.some((topic) =>
				isDirectAnswerTopic(topic),
			)
				? "- For direct-answer topics, keep the verified answer in the first spoken content sentence for that topic; do not rewrite it into a tease."
				: "- Preserve curiosity gaps: open with tension and delay the payoff by at least one sentence or segment.";
			const timingSourcePolicy = buildTopicSourcePolicyPromptBlock(
				topicPicks,
				topicContexts,
			);

			const rewritePrompt = `
Rewrite this script to better fit ~${narrationTargetSec.toFixed(
				1,
			)}s of spoken narration.
Make the script about ${adjustPct}% ${direction} while keeping the same vibe.
Quality first: do not remove key details or clarity just to hit the target.
Per-segment word caps (updated): ${capsLine2}
Expressions by segment (keep these expressions, only adjust text): ${expressionsLine}
Topic assignment by segment (do NOT change order): ${topicsLine}

${timingPromiseGuide}

${timingPromptBriefGuide}

${timingDirectAnswerGuide}

${timingSourcePolicy.text}

Rules:
${timingToneRule}
${timingCasualRule}
- Keep the timing rewrite brisk and adult; do not add padding, slow explanatory loops, or childlike reassurance just to fill seconds.
- If the chosen narration budget expands beyond the frontend duration, spend it on retention: stronger examples, cleaner stakes, sharper transitions, and a better payoff. Do not add filler, repeated greetings, or slow recap.
- Preserve factual tension: supported tradeoffs, incentives, disagreement, and uncertainty should stay clear and specific.
- For entertainment topics (film, TV, music, awards), add ONE or TWO short grounded reactionary opinions per topic (brief clauses only). Keep them fair, specific, and clearly separate from sourced facts.
- Keep EXACTLY ${segments.length} segments.
- Preserve smooth transitions.
- Keep coherence tight: each segment should connect to the previous with a brief bridge or cause-effect line.
- Structure each topic around one clear angle; keep facts in service of that angle.
- ${timingAnswerRule}
- Keep at least one clip-ready line per topic that ends with an open loop.
- Make topic handoffs feel smooth and coherent; use a brief bridge phrase to set up the next topic.
- For Topic 2+ only, if a segment is the first for a new topic, start it with an explicit transition line naming the topic. Do NOT use that transition for Topic 1.
- Improve clarity and specificity; avoid vague filler phrasing or repeating the question.
- Avoid repeating the headline or the same fact across segments; each segment must add a new detail or angle.
- No redundancy: do not restate the same fact or idea in different words.
- Remove stock bridge phrases like "that is the turn", "the next detail changes how...", or repeated "the answer depends..." phrasing.
- Do not reuse any sentence verbatim across segments.
- Add one fresh, concrete detail or implication per segment when possible.
- Prefer specific nouns over vague hype phrases.
- Keep the opening controlled, but make segment 0 feel hooky and curiosity-driven instead of bland.
- Preserve any frontend-requested opening line as the first spoken line of the overall opening unit, and do not repeat it inside segment 0 when the intro already uses it.
- Preserve any frontend-requested title unless a factual correction is necessary.
- Include mandatory or memorable lines naturally. Do not say labels like "memorable line" or "must include" in the spoken script.
- Keep the overall delivery calm and professional; avoid excited phrasing and exclamation points, but let the writing feel sharp and engaging. For entertainment topics only, allow brief grounded reactionary asides.
- Stay close to the per-segment word caps (aim ~90-100% of each cap); do not be significantly shorter.
- Preserve source attributions only when that topic is marked with source links in the Source policy. Remove invented named outlets, journals, studies, researchers, or "reporting" from topics with no source links.
- If the story is controversial and source links exist, say what people are divided over and what the reporting actually supports. Without source links, frame the divide as a high-level tension.
- For death, injury, legal, health, or public-safety stories, keep confirmed facts and unknown details clearly separated; include human/community stakes without dramatic language.
- If the headline promises "what it means", include a practical implication when the provided context supports one.
- If the request, script title, or final title promises price, release details, availability, or what to expect, preserve those concrete details while adjusting length. If context does not confirm the detail, preserve the clear "not confirmed" wording.
- If you mention rumors or estimates, label them clearly as unconfirmed.
- Avoid filler words ("um", "uh", "umm", "uhm", "ah", "like"). Use zero filler words in the entire script, especially in segments 0-2.
- Opening unit discipline: the generated intro plus segments 0-2 must be clean, explicit, curiosity-driven, and entertaining without hesitation sounds, stall words, fake pauses, or annoying verbal padding.
- Do NOT introduce the presenter or host by name. A single brief greeting like "Hi guys, today we are talking about..." is allowed only in the generated intro, then open with the topic tension immediately. Segment 0 should not repeat the greeting.
- Avoid artificial dramatic pause writing in segments 0-2: no ellipses, repeated dashes, isolated one-word fragments, or sentences that need a long silence to land.
- Keep the first beat smooth and human: one restrained emotional color that fits the topic, with no exaggerated phrasing.
- Do NOT add micro vocalizations ("heh", "whew", "hmm").
- Do NOT mention "intro", "outro", "segment", "next segment", or say "in this video/clip".
- Do NOT start segment 0 with transition phrases like "And now", "Now", "Next up", or "Let's talk about".
- Do NOT ask engagement questions, likes, subscribes, or comments inside content; the separate closing line handles that.
- Last segment ends with a clean takeaway, hopeful implication, tension line, or open loop that leads into the closing line. Do NOT mention the outro or transitions to it.
- Category-specific guidance:
${timingCategoryGuide.lines.join("\n")}

Return JSON ONLY: { "segments":[{"index":0,"text":"..."}] }

Script:
${segments.map((s) => `#${s.index}: ${s.text}`).join("\n")}
`.trim();

			const resp2 = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: rewritePrompt }],
			});
			const parsed2 = parseJsonFlexible(
				resp2?.choices?.[0]?.message?.content || "",
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
				voiceTonePlan?.mood,
				{ includeCta: false },
			);
			const withTransitions = ensureTopicTransitions(fixedSegments, topicPicks);
			const withQuestions = ensureTopicEngagementQuestions(
				withTransitions,
				topicPicks,
				voiceTonePlan?.mood,
				adjustedCaps,
				{ skipFinalTopicQuestion: true },
			);
			const fillerLimited = limitFillerAndEmotesAcrossSegments(withQuestions, {
				maxFillers: MAX_FILLER_WORDS_PER_VIDEO,
				maxFillersPerSegment: MAX_FILLER_WORDS_PER_SEGMENT,
				maxEmotes: MAX_MICRO_EMOTES_PER_VIDEO,
				maxEmotesPerSegment: MAX_MICRO_EMOTES_PER_VIDEO,
				noFillerSegmentIndices: OPENING_NO_FILLER_SEGMENT_INDICES,
			});
			const timingPromiseRepair = repairTitlePromiseCoverage({
				script: { ...script, segments: fillerLimited },
				topics: topicPicks,
				topicContexts,
				wordCaps: adjustedCaps,
				log: (message, payload) => logJob(jobId, message, payload),
			});
			const timingReadySegments = timingPromiseRepair.repairs.length
				? timingPromiseRepair.script.segments
				: fillerLimited;
			const timingNoCta = removeContentCtasForSeparateOutro({
				script: { ...script, segments: timingReadySegments },
				topics: topicPicks,
				wordCaps: adjustedCaps,
				categoryLabel,
				mood: voiceTonePlan?.mood,
			});
			const timingDirectAnswerRepair = repairDirectAnswerOpening({
				script: timingNoCta,
				topics: topicPicks,
				topicContexts,
				wordCaps: adjustedCaps,
			}).script;
			segments.splice(
				0,
				segments.length,
				...(timingDirectAnswerRepair.segments || timingReadySegments),
			);
			segments = segments.map((s) => ({
				...s,
				text: sanitizeSegmentText(s.text),
			}));
		}

		const finalPromptBrief = primaryPromptBrief(topicPicks);
		if (shouldLockPromptBriefTitle(finalPromptBrief)) {
			script.title = formatHumanTitle(finalPromptBrief.title, 120) || script.title;
			script.shortTitle =
				shortTitleFromText(finalPromptBrief.title).slice(0, 60) ||
				script.shortTitle;
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

		if (TRIM_LEADING_SILENCE) {
			const introTight = path.join(tmpDir, `intro_tight_${jobId}.wav`);
			await trimLeadingSilenceWav(introAudioPath, introTight);
			safeUnlink(introAudioPath);
			introAudioPath = introTight;
			introDurationSec = await probeDurationSeconds(introAudioPath);

			const outroTight = path.join(tmpDir, `outro_tight_${jobId}.wav`);
			await trimLeadingSilenceWav(outroAudioPath, outroTight);
			safeUnlink(outroAudioPath);
			outroAudioPath = outroTight;
			outroDurationSec = await probeDurationSeconds(outroAudioPath);

			logJob(jobId, "intro/outro leading silence trimmed", {
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
			countdownRank: Number.isFinite(Number(s.countdownRank))
				? Number(s.countdownRank)
				: null,
			countdownLabel: cleanTopicLabel(s.countdownLabel || ""),
			overlayCues: Array.isArray(s.overlayCues) ? s.overlayCues : [],
			imageUrls: Array.isArray(s.imageUrls) ? s.imageUrls : [],
			feedVideoCandidates: Array.isArray(s.feedVideoCandidates)
				? s.feedVideoCandidates
				: [],
			visualGrounding:
				s.visualGrounding && typeof s.visualGrounding === "object"
					? s.visualGrounding
					: null,
		}));
		script.segments = finalScriptSegments;
		const finalQa = analyzeScriptQuality({
			script,
			topics: topicPicks,
			topicContexts,
			wordCaps,
			categoryLabel,
		});
		const finalShortsGuardrails = analyzeShortsGuardrails(script);
		const finalEngagement = summarizeScriptEngagement(script);
		logJob(jobId, "final script summary", {
			qa: finalQa,
			shortsGuardrails: finalShortsGuardrails,
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
			let finalWav = out;
			if (TRIM_LEADING_SILENCE && !voiceoverUrlLocked) {
				const trimmed = path.join(
					tmpDir,
					`seg_audio_${jobId}_${a.index}_tight.wav`,
				);
				await trimLeadingSilenceWav(out, trimmed);
				safeUnlink(out);
				finalWav = trimmed;
			}
			const d2 = await probeDurationSeconds(finalWav);
			logJob(jobId, "tts segment atempo applied", {
				segment: a.index,
				atempo: Number(globalAtempo.toFixed(4)),
				cleanDur: Number((a.cleanDur || 0).toFixed(3)),
				finalDur: Number((d2 || 0).toFixed(3)),
			});
			segmentAudio.push({ index: a.index, wav: finalWav, dur: d2 });
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
				imageUrls: Array.isArray(seg.imageUrls) ? seg.imageUrls : [],
				feedVideoCandidates: Array.isArray(seg.feedVideoCandidates)
					? seg.feedVideoCandidates
					: [],
				visualGrounding:
					seg.visualGrounding && typeof seg.visualGrounding === "object"
						? seg.visualGrounding
						: null,
				topicIndex: seg.topicIndex,
				topicLabel: seg.topicLabel,
				countdownRank: Number.isFinite(Number(seg.countdownRank))
					? Number(seg.countdownRank)
					: null,
				countdownLabel: cleanTopicLabel(seg.countdownLabel || ""),
				startSec,
				endSec,
				audioPath: a.wav,
			};
		});

		// Log target drift without forcing padding or slowdown; pacing is quality-first.
		if (timeline.length) {
			const finalEnd = timeline[timeline.length - 1].endSec;
			const desired = Number(
				(introDurationSec + narrationTargetSec).toFixed(3),
			);
			if (Math.abs(finalEnd - desired) > 0.08) {
				logJob(jobId, "timeline end differs from narration target", {
					finalEnd,
					desired,
					driftSec: Number(Math.abs(finalEnd - desired).toFixed(3)),
					qualityFirstPacing: !REWRITE_FOR_NARRATION_DURATION,
				});
			}
		}

		const narrationActualSec = segmentAudio.reduce(
			(sum, a) => sum + (Number(a.dur) || 0),
			0,
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
			atempo: Number(globalAtempo.toFixed(4)),
			qualityFirstPacing: !REWRITE_FOR_NARRATION_DURATION,
		});
		logJob(jobId, "final segment durations", {
			segments: timeline.map((seg) => ({
				index: seg.index,
				topicLabel: seg.topicLabel,
				startSec: seg.startSec,
				endSec: seg.endSec,
				durationSec: Number(
					Math.max(0, Number(seg.endSec) - Number(seg.startSec)).toFixed(3),
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

		// 8.5) Visual plan: budget-aware presenter vs static feed images (content only)
		const totalSegments = timeline.length;
		const contentVisualPlan = computeContentVisualPlan(totalSegments, {
			videoDurationSec: totalActualSec,
			segmentDurations: timeline.map((seg) =>
				Math.max(
					0,
					Number(seg.durationSec || 0) ||
						Number(seg.endSec || 0) - Number(seg.startSec || 0),
				),
			),
		});
		const presenterPosSet = contentVisualPlan.presenterPositionSet;
		const presenterSegments = [];
		const imageSegments = [];
		const forcedPresenterSegments = [];
		timeline = timeline.map((seg, idx) => {
			const visualType = presenterPosSet.has(idx) ? "presenter" : "image";
			const mustUsePresenter =
				contentVisualPlan.forcedPresenterPositionSet.has(idx);
			const presenterClusterId =
				contentVisualPlan.presenterClusterIdByPosition?.get(idx) || "";
			if (visualType === "presenter") presenterSegments.push(seg.index);
			else imageSegments.push(seg.index);
			if (mustUsePresenter) forcedPresenterSegments.push(seg.index);
			return {
				...seg,
				plannedVisualType: visualType,
				visualType,
				mustUsePresenter,
				presenterClusterId,
			};
		});
		logJob(jobId, "segment visual plan", {
			totalSegments,
			targetPresenterRatio: CONTENT_PRESENTER_RATIO,
			targetHeyGenCalls: contentVisualPlan.targetHeyGenCalls,
			openingPresenterCount: contentVisualPlan.openingPresenterCount,
			maxOptionalHeyGenContentSegments:
				contentVisualPlan.maxOptionalHeyGenContentSegments,
			optionalPresenterClusters:
				contentVisualPlan.optionalPresenterClusters || [],
			targetPresenterCount: presenterSegments.length,
			targetImageCount: imageSegments.length,
			forcedPresenterSegments,
			presenterSegments,
			imageSegments,
		});

		const imagePrep = await prepareImageSegments({
			timeline,
			topics: topicPicks,
			topicContexts,
			tmpDir,
			jobId,
			baseUrl,
			output,
			category,
		});
		timeline = imagePrep.timeline;
		segmentImagePaths = imagePrep.segmentImagePaths || new Map();
		segmentFeedVideoPaths = imagePrep.segmentFeedVideoPaths || new Map();
		const imagePlanSummary = imagePrep.imagePlanSummary || [];
		const feedVideoPlanSummary = imagePrep.feedVideoPlanSummary || [];

		const finalPresenterSegments = [];
		const finalImageSegments = [];
		const imageFallbackToPresenterSegments = [];
		for (const seg of timeline) {
			if (seg.visualType === "image") finalImageSegments.push(seg.index);
			else {
				finalPresenterSegments.push(seg.index);
				if (seg.plannedVisualType === "image")
					imageFallbackToPresenterSegments.push(seg.index);
			}
		}
		logJob(jobId, "segment visual plan final", {
			totalSegments,
			targetPresenterRatio: CONTENT_PRESENTER_RATIO,
			targetHeyGenCalls: contentVisualPlan.targetHeyGenCalls,
			openingPresenterCount: contentVisualPlan.openingPresenterCount,
			maxOptionalHeyGenContentSegments:
				contentVisualPlan.maxOptionalHeyGenContentSegments,
			optionalPresenterClusters:
				contentVisualPlan.optionalPresenterClusters || [],
			presenterCount: finalPresenterSegments.length,
			imageCount: finalImageSegments.length,
			presenterSegments: finalPresenterSegments,
			imageSegments: finalImageSegments,
			imageFallbackToPresenterSegments,
		});
		if (!presenterHeyGenImageUrl) {
			const presenterUpload = await uploadLocalImageToCloudinary(presenterLocal, {
				publicIdBase: `long_presenter_heygen_${jobId}`,
				output,
				jobId,
				segIndex: "presenter",
			});
			presenterHeyGenImageUrl = String(presenterUpload?.url || "").trim();
		}
		if (!presenterHeyGenImageUrl) {
			throw new Error("HeyGen presenter image upload failed");
		}
		logJob(jobId, "heygen presenter image ready", {
			url: presenterHeyGenImageUrl,
			outfitStyle: presenterOutfitStyle || "",
		});
		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				presenterVideoEngine: "heygen",
				heygenPresenter: {
					imageUrl: presenterHeyGenImageUrl,
					resolution: HEYGEN_DEFAULT_RESOLUTION,
					expressiveness: HEYGEN_DEFAULT_EXPRESSIVENESS,
					fit: HEYGEN_DEFAULT_FIT,
				},
			},
		});
		const presenterContentSec = timeline.reduce(
			(sum, seg) =>
				sum +
				(seg.visualType === "presenter"
					? Math.max(
							0,
							Number(seg.durationSec || 0) ||
								Number(seg.endSec || 0) - Number(seg.startSec || 0),
						)
					: 0),
			0,
		);
		const imageContentSec = timeline.reduce(
			(sum, seg) =>
				sum +
				(seg.visualType === "image"
					? Math.max(
							0,
							Number(seg.durationSec || 0) ||
								Number(seg.endSec || 0) - Number(seg.startSec || 0),
						)
					: 0),
			0,
		);
		const presenterExpressionCount = new Set(
			timeline
				.filter((seg) => seg.visualType === "presenter")
				.map((seg) =>
					normalizeExpression(
						seg.videoExpression || seg.expression || "neutral",
						voiceTonePlan?.mood,
					),
				),
		).size;
		const estimatedHeyGenPresenterSec =
			Number(introDurationSec || 0) +
			Number(outroDurationSec || 0) +
			presenterContentSec;
		logJob(jobId, "cost-sensitive render budget", {
			presenterRatio: CONTENT_PRESENTER_RATIO,
			presenterContentSec: Number(presenterContentSec.toFixed(3)),
			imageContentSec: Number(imageContentSec.toFixed(3)),
			estimatedHeyGenPresenterSec: Number(
				estimatedHeyGenPresenterSec.toFixed(3),
			),
			estimatedHeyGenPresenterMin: Number(
				(estimatedHeyGenPresenterSec / 60).toFixed(3),
			),
			baselineExpressions: presenterExpressionCount,
			presenterEngine: "heygen",
			heygenResolution: HEYGEN_DEFAULT_RESOLUTION,
			heygenExpressiveness: HEYGEN_DEFAULT_EXPRESSIVENESS,
		});
		const premiumPresenterSegments = [];
		for (const seg of timeline) {
			if (seg.mustUsePresenter) premiumPresenterSegments.push(seg.index);
		}
		if (finalPresenterSegments.length) {
			premiumPresenterSegments.push(finalPresenterSegments[0]);
			if (finalPresenterSegments.length > 1) {
				premiumPresenterSegments.push(
					finalPresenterSegments[finalPresenterSegments.length - 1],
				);
			}
		}
		const premiumPresenterSegmentSet = new Set(premiumPresenterSegments);
		const allImageUrls = [];
		for (const seg of timeline) {
			if (Array.isArray(seg.imageUrls)) allImageUrls.push(...seg.imageUrls);
			if (Array.isArray(seg.feedVideoUrls))
				allImageUrls.push(...seg.feedVideoUrls);
		}
		const uniqueImageUrls = new Set(
			allImageUrls.map((u) => normalizeImageUrlKey(u)),
		);
		const duplicateImageCount = Math.max(
			0,
			allImageUrls.length - uniqueImageUrls.size,
		);
		logJob(jobId, "segment image qa", {
			totalImages: allImageUrls.length,
			uniqueImages: uniqueImageUrls.size,
			duplicates: duplicateImageCount,
		});
		const imageDiversity = await evaluateImageSegmentDiversity({
			timeline,
			segmentImagePaths,
			segmentFeedVideoPaths,
			jobId,
		});
		logJob(jobId, "segment image diversity", imageDiversity);
		if (imagePlanSummary.length) {
			logJob(jobId, "segment image plan", {
				count: imagePlanSummary.length,
				segments: imagePlanSummary,
			});
		}
		if (feedVideoPlanSummary.length) {
			logJob(jobId, "segment feed video plan", {
				count: feedVideoPlanSummary.length,
				segments: feedVideoPlanSummary,
			});
		}
		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				timeline,
				visualPlan: {
					targetPresenterRatio: CONTENT_PRESENTER_RATIO,
					targetHeyGenCalls: contentVisualPlan.targetHeyGenCalls,
					presenterSegments: finalPresenterSegments,
					imageSegments: finalImageSegments,
					optionalPresenterClusters:
						contentVisualPlan.optionalPresenterClusters || [],
					feedVideoSegments: feedVideoPlanSummary.map((s) => s.segment),
				},
				imageQa: {
					total: allImageUrls.length,
					unique: uniqueImageUrls.size,
					duplicates: duplicateImageCount,
				},
				imageDiversity,
			},
		});
		logJob(jobId, "heygen presenter strategy", {
			resolution: HEYGEN_DEFAULT_RESOLUTION,
			expressiveness: HEYGEN_DEFAULT_EXPRESSIVENESS,
			fit: HEYGEN_DEFAULT_FIT,
			targetHeyGenCalls: contentVisualPlan.targetHeyGenCalls,
			openingPresenterCount: contentVisualPlan.openingPresenterCount,
			maxOptionalHeyGenContentSegments:
				contentVisualPlan.maxOptionalHeyGenContentSegments,
			preferredHeyGenPresenterSegmentSec:
				PREFERRED_HEYGEN_PRESENTER_SEGMENT_SEC,
			optionalPresenterClusterMinSec: OPTIONAL_PRESENTER_CLUSTER_MIN_SEC,
			optionalPresenterClusterTargetSec: OPTIONAL_PRESENTER_CLUSTER_TARGET_SEC,
			optionalPresenterClusterMaxSegments: OPTIONAL_PRESENTER_CLUSTER_MAX_SEGMENTS,
			bodyPresenterTargetSec: PRESENTER_BODY_TARGET_SEC,
			bodyPresenterMaxSec: PRESENTER_BODY_MAX_SEC,
			qualityFirstPresenterPlanning: true,
			optionalPresenterClusters:
				contentVisualPlan.optionalPresenterClusters || [],
			premiumPresenterSegments,
			requiredMotionMaxFreezeRatio: HEYGEN_REQUIRED_MOTION_MAX_FREEZE_RATIO,
			contentMotionMaxFreezeRatio: HEYGEN_CONTENT_MOTION_MAX_FREEZE_RATIO,
			optionalMotionMaxFreezeRatio: HEYGEN_OPTIONAL_MOTION_MAX_FREEZE_RATIO,
			fallbackPolicy: "no paid presenter fallbacks",
		});
		if (!imageDiversity.ok) {
			const failingTopics = (imageDiversity.perTopic || []).filter(
				(t) => t.unique < t.minUnique,
			);
			const failText = failingTopics
				.map(
					(t) =>
						`${t.topicLabel || `topic_${t.topicIndex}`}: ${t.unique}/${
							t.segmentCount
						} (min ${t.minUnique})`,
				)
				.join("; ");
			const message = `Not enough unique feed images per topic: ${failText || `${imageDiversity.unique}/${imageDiversity.segmentCount} unique (min ${imageDiversity.minUnique})`}.`;
			logJob(jobId, "segment image diversity failed", {
				...imageDiversity,
				message,
			});
			throw new Error(message);
		}

		const collectGlobalVisualFallbackPaths = ({
			exclude = [],
			limit = 12,
		} = {}) => {
			const excluded = new Set(
				(Array.isArray(exclude) ? exclude : [exclude])
					.filter(Boolean)
					.map(localPathKey),
			);
			const paths = [];
			const seen = new Set();
			const addPath = (p) => {
				if (!p || isReservedThumbnailVisualPath(p, thumbnailPath)) return;
				const key = localPathKey(p);
				if (excluded.has(key) || seen.has(key)) return;
				if (!fs.existsSync(p)) return;
				seen.add(key);
				paths.push(p);
			};
			if (segmentImagePaths instanceof Map) {
				for (const group of segmentImagePaths.values()) {
					for (const p of group || []) addPath(p);
				}
			}
			return paths.slice(0, Math.max(1, Number(limit) || 12));
		};
		const rotateFallbackPathsForLabel = (paths = [], label = "") => {
			const clean = (Array.isArray(paths) ? paths : []).filter(Boolean);
			if (clean.length <= 1) return clean;
			const hash = crypto
				.createHash("sha1")
				.update(`${jobId}:${label}`)
				.digest()
				.readUInt32BE(0);
			const offset = hash % clean.length;
			return clean.slice(offset).concat(clean.slice(0, offset));
		};
		const feedImageFallbackPaths = collectGlobalVisualFallbackPaths({
			limit: 14,
		});
		const renderNonPresenterFallbackSegment = async ({
			segDur,
			audioPath,
			label,
			addFades = false,
			cameraMotion = null,
			preferredImagePaths = [],
			fallbackText = "",
			reason = "presenter_motion_unavailable",
		}) => {
			const imagePaths = [];
			const seen = new Set();
			const addImagePath = (p) => {
				if (!p || isReservedThumbnailVisualPath(p, thumbnailPath)) return;
				const key = localPathKey(p);
				if (seen.has(key) || !fs.existsSync(p)) return;
				seen.add(key);
				imagePaths.push(p);
			};
			(Array.isArray(preferredImagePaths)
				? preferredImagePaths
				: [preferredImagePaths]
			).forEach(addImagePath);
			collectGlobalVisualFallbackPaths({
				exclude: imagePaths,
				limit: 10,
			}).forEach(addImagePath);
			if (!imagePaths.length) {
				try {
					const fallbackCard = await createTopicDetailCardImage({
						tmpDir,
						jobId,
						topicIndex: `fallback_${String(label || "segment").replace(/[^a-z0-9_-]/gi, "")}`,
						title:
							script.shortTitle ||
							script.title ||
							topicSummary ||
							"Key Idea",
						bullets: [
							formatHumanTitle(
								stripMetaNarration(fallbackText || script.title || topicSummary),
								92,
							) || "A clean visual fallback for this beat",
						],
						output,
					});
					addImagePath(fallbackCard);
				} catch (e) {
					logJob(jobId, "non-presenter fallback card failed", {
						label,
						reason,
						error: e?.message || String(e),
					});
				}
			}
			if (imagePaths.length) {
				const fallbackImages = rotateFallbackPathsForLabel(imagePaths, label);
				try {
					logJob(jobId, "non-presenter visual fallback using images", {
						label,
						reason,
						images: Math.min(fallbackImages.length, 8),
					});
					return await renderImageSegment({
						jobId,
						tmpDir,
						output,
						segDur,
						audioPath,
						imagePaths: fallbackImages.slice(0, 8),
						label,
						addFades,
						cameraMotion,
					});
				} catch (e) {
					logJob(jobId, "non-presenter image fallback failed; using local visual", {
						label,
						reason,
						images: Math.min(fallbackImages.length, 8),
						error: e.message,
					});
				}
			}
			logJob(jobId, "non-presenter local visual fallback used", {
				label,
				reason,
			});
			return await renderNoSyncVisualFallbackSegment({
				jobId,
				tmpDir,
				output,
				segDur,
				audioPath,
				label,
				addFades,
				cameraMotion,
			});
		};

		const presenterSegSet = new Set(finalPresenterSegments);
		const presenterOnlySegments = segments.filter((s) =>
			presenterSegSet.has(s.index),
		);
		const lockedPresenterExpression = LOCK_PRESENTER_VIDEO_EXPRESSION
			? chooseLockedPresenterVideoExpression({
					segments: presenterOnlySegments,
					topics: topicPicks,
					categoryLabel,
					mood: voiceTonePlan?.mood,
				})
			: "";
		const presenterVideoPlan = LOCK_PRESENTER_VIDEO_EXPRESSION
			? presenterOnlySegments.map(() => lockedPresenterExpression || "neutral")
			: buildSubtleVideoExpressionPlan(
					presenterOnlySegments,
					voiceTonePlan?.mood,
					jobId,
				);
		if (LOCK_PRESENTER_VIDEO_EXPRESSION && presenterOnlySegments.length) {
			logJob(jobId, "presenter video expression locked", {
				expression: lockedPresenterExpression || "neutral",
				segments: presenterOnlySegments.map((seg) => seg.index),
			});
		}
		const presenterPlanByIndex = new Map();
		presenterOnlySegments.forEach((seg, idx) => {
			presenterPlanByIndex.set(seg.index, presenterVideoPlan[idx] || "neutral");
		});
		segments = segments.map((s) => ({
			...s,
			videoExpression: presenterPlanByIndex.has(s.index)
				? presenterPlanByIndex.get(s.index) || "neutral"
				: s.videoExpression || s.expression || "neutral",
		}));
		const segmentMetaByIndex = new Map(segments.map((s) => [s.index, s]));
		timeline = timeline.map((seg) => {
			const meta = segmentMetaByIndex.get(seg.index) || {};
			const segDur = Math.max(
				0.2,
				Number(seg.endSec || 0) - Number(seg.startSec || 0),
			);
			const visualType = seg.visualType || "presenter";
			const expression = meta.expression || seg.expression || "neutral";
			const videoExpression =
				meta.videoExpression || seg.videoExpression || expression;
			const cameraMotion = inferCameraMotionPlan({
				text: meta.text || seg.text || "",
				topicLabel: seg.topicLabel || meta.topicLabel || "",
				expression: videoExpression,
				mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
				visualType,
				durationSec: segDur,
				index: seg.index,
				categoryLabel,
			});
			return {
				...seg,
				text: meta.text || seg.text,
				expression,
				videoExpression,
				cameraMotion,
			};
		});
		const cameraMotionSummary = summarizeCameraMotionPlan(timeline);
		logJob(jobId, "segment camera motion plan", cameraMotionSummary);
		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				timeline,
				cameraMotion: cameraMotionSummary,
			},
		});

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

		// 9) HeyGen uses the approved presenter image directly; no paid pre-baselines.
		logJob(jobId, "heygen presenter baselines skipped", {
			engine: "heygen",
			imageUrl: presenterHeyGenImageUrl,
		});

		updateJob(jobId, { progressPct: 50 });

		// 10) Intro + first content beats as one coherent HeyGen presenter segment.
		const segmentVideos = [];
		const segmentRenderSummary = [];
		const requiredPresenterSegmentSet = new Set(
			timeline
				.filter((seg) => seg.mustUsePresenter)
				.map((seg) => seg.index),
		);
		let renderUnits = await buildRenderableTimelineUnits({
			timeline,
			tmpDir,
			jobId,
			premiumPresenterSegmentSet,
		});
		const openingMinSec = OPENING_PRESENTER_MIN_SEC;
		const openingMaxSec = OPENING_PRESENTER_MAX_SEC;
		const openingTargetSec = OPENING_PRESENTER_TARGET_SEC;
		const openingUnits = [];
		let openingContentSec = 0;
		while (renderUnits.length) {
			const next = renderUnits[0];
			const nextDur = Math.max(0.2, Number(next.segDur || 0));
			const projected = introDurationSec + openingContentSec + nextDur;
			if (
				projected > openingMaxSec &&
				introDurationSec + openingContentSec >= openingMinSec
			) {
				break;
			}
			if (
				introDurationSec + openingContentSec >= openingTargetSec
			) {
				break;
			}
			openingUnits.push(renderUnits.shift());
			openingContentSec += nextDur;
			if (introDurationSec + openingContentSec >= openingMaxSec) break;
		}
		const openingAudioPath = path.join(tmpDir, `intro_first_audio_${jobId}.wav`);
		await concatAudioClips(
			[introAudioPath, ...openingUnits.map((unit) => unit.audioPath)].filter(
				Boolean,
			),
			openingAudioPath,
		);
		const openingDurationSec =
			(await probeDurationSeconds(openingAudioPath)) ||
			introDurationSec + openingContentSec;
		const openingText = [
			introTextFinal,
			...openingUnits.map((unit) => unit.text || ""),
		]
			.filter(Boolean)
			.join(" ");
		const openingCameraMotion = inferCameraMotionPlan({
			text: openingText,
			topicLabel: topicSummary,
			expression: introExpression,
			mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
			visualType: "presenter",
			durationSec: openingDurationSec,
			index: -1,
			categoryLabel,
		});
		const introPath = await renderHeyGenPresenterSegment({
			jobId,
			tmpDir,
			output,
			presenterImageUrl: presenterHeyGenImageUrl,
			segDur: openingDurationSec,
			audioPath: openingAudioPath,
			label: "intro_first",
			addFades: true,
			cameraMotion: openingCameraMotion,
			text: openingText,
			expression: introExpression,
			mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
			pace: resolveHeyGenPace({
				text: openingText,
				expression: introExpression,
				mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
			}),
			role: "intro_first",
		});
		segmentVideos.push(introPath);
		segmentRenderSummary.push({
			label: "intro_first",
			renderSegments: openingUnits.flatMap(
				(unit) => unit.renderSegmentIndices || [unit.index],
			),
			plannedVisualType: "presenter",
			actualVisualType: "presenter",
			durationSec: openingDurationSec,
			mustUsePresenter: true,
			combinedIntro: true,
		});
		logJob(jobId, "presenter render units", {
			totalUnits: renderUnits.length,
			openingCombined: {
				durationSec: Number((openingDurationSec || 0).toFixed(3)),
				segments: openingUnits.flatMap(
					(unit) => unit.renderSegmentIndices || [unit.index],
				),
			},
			mergedUnits: renderUnits
				.filter((unit) => unit.mergedPresenterRun)
				.map((unit) => ({
					label: unit.renderLabel,
					segments: unit.renderSegmentIndices,
					segDur: Number((unit.segDur || 0).toFixed(3)),
					engine: "heygen",
				})),
		});
		let lastGoodFeedImagePaths = feedImageFallbackPaths.slice(0, 5);
		for (const seg of renderUnits) {
			const segDur = Math.max(0.2, Number(seg.segDur || 0));
			logJob(jobId, "segment start", {
				segment: seg.index,
				renderLabel: seg.renderLabel || String(seg.index),
				renderSegments: seg.renderSegmentIndices || [seg.index],
				segDur: Number(segDur.toFixed(3)),
				visualType: seg.visualType || "presenter",
			});

			const renderSegmentIndices = seg.renderSegmentIndices || [seg.index];
			const mustUsePresenter = renderSegmentIndices.some((idx) =>
				requiredPresenterSegmentSet.has(idx),
			);
			const exprKey = seg.videoExpression || seg.expression || "neutral";
			const heygenPace = resolveHeyGenPace({
				text: seg.text || "",
				expression: exprKey,
				mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
			});
			if ((seg.visualType || "presenter") === "presenter") {
				logJob(jobId, "heygen presenter segment planned", {
					segment: seg.index,
					renderLabel: seg.renderLabel || String(seg.index),
					expression: exprKey,
					pace: heygenPace,
				});
			}
			let norm = null;
			const plannedVisualType = seg.visualType || "presenter";
			let actualVisualType = plannedVisualType;
			let fallbackReason = "";
			if (plannedVisualType === "image") {
				const feedVideoPaths = segmentFeedVideoPaths.get(seg.index) || [];
				const imagePaths = segmentImagePaths.get(seg.index) || [];
				const primaryPathSet = new Set(imagePaths);
				const renderFeedVideoClip = (paths, labelSuffix = "") =>
					renderFeedVideoSegment({
						jobId,
						tmpDir,
						output,
						segDur,
						audioPath: seg.audioPath,
						videoPaths: paths,
						label: labelSuffix
							? `${seg.index}_${labelSuffix}`
							: String(seg.index),
						addFades: ENABLE_SEGMENT_FADES,
					});
				const renderFeedImageClip = (paths, labelSuffix = "") =>
					renderImageSegment({
						jobId,
						tmpDir,
						output,
						segDur,
						audioPath: seg.audioPath,
						imagePaths: paths,
						label: labelSuffix
							? `${seg.index}_${labelSuffix}`
							: String(seg.index),
						addFades: ENABLE_SEGMENT_FADES,
						cameraMotion: seg.cameraMotion,
					});
				if (feedVideoPaths.length) {
					try {
						norm = await renderFeedVideoClip(feedVideoPaths);
						actualVisualType = "feed_video";
						logJob(jobId, "feed video segment rendered", {
							segment: seg.index,
							feedVideos: feedVideoPaths.length,
						});
					} catch (e) {
						logJob(jobId, "feed video segment render failed; trying images", {
							segment: seg.index,
							feedVideos: feedVideoPaths.length,
							error: e.message,
						});
						fallbackReason = "feed_video_render_failed";
					}
				}
				if (!norm) {
					if (imagePaths.length) {
						try {
							norm = await renderFeedImageClip(imagePaths);
							actualVisualType = "image";
							lastGoodFeedImagePaths = imagePaths.slice(0, 5);
						} catch (e) {
							logJob(jobId, "image segment render failed; trying feed-image rescue", {
								segment: seg.index,
								feedImages: imagePaths.length,
								error: e.message,
							});
							fallbackReason = fallbackReason
								? `${fallbackReason}_then_image_render_failed`
								: "image_render_failed";
						}
					} else {
						logJob(jobId, "image segment missing assets; trying feed-image rescue", {
							segment: seg.index,
						});
						fallbackReason = fallbackReason
							? `${fallbackReason}_then_image_assets_missing`
							: "image_assets_missing";
					}
				}

				if (!norm) {
					const rescuePaths = [];
					const rescuePathSet = new Set();
					const addRescuePath = (p) => {
						if (!p || rescuePathSet.has(p) || primaryPathSet.has(p)) return;
						rescuePathSet.add(p);
						rescuePaths.push(p);
					};
					lastGoodFeedImagePaths.forEach(addRescuePath);
					feedImageFallbackPaths.forEach(addRescuePath);
					if (rescuePaths.length) {
						try {
							norm = await renderFeedImageClip(
								rescuePaths.slice(0, 8),
								"feed_rescue",
							);
							actualVisualType = "image_rescue";
							fallbackReason = fallbackReason
								? `${fallbackReason}_rescued_with_feed_images`
								: "image_rescued_with_feed_images";
							lastGoodFeedImagePaths = rescuePaths.slice(0, 5);
							logJob(jobId, "image segment feed-image rescue ready", {
								segment: seg.index,
								rescueImages: Math.min(rescuePaths.length, 8),
							});
						} catch (e) {
							logJob(jobId, "image segment feed-image rescue failed", {
								segment: seg.index,
								rescueImages: Math.min(rescuePaths.length, 8),
								error: e.message,
							});
						}
					}
				}

				if (!norm) {
					try {
						logJob(jobId, "image segment feed images exhausted; using local visual fallback", {
							segment: seg.index,
						});
						norm = await renderNonPresenterFallbackSegment({
							segDur,
							audioPath: seg.audioPath,
							label: `${seg.index}_local_visual`,
							addFades: ENABLE_SEGMENT_FADES,
							cameraMotion: seg.cameraMotion,
							preferredImagePaths: feedImageFallbackPaths,
							fallbackText: seg.text || "",
							reason: fallbackReason || "image_assets_missing",
						});
						actualVisualType = "local_visual_fallback";
						fallbackReason = fallbackReason
							? `${fallbackReason}_used_local_visual`
							: "image_used_local_visual";
					} catch (e) {
						logJob(jobId, "image segment local no-sync fallback failed; fallback to presenter", {
							segment: seg.index,
							error: e.message,
						});
						actualVisualType = "presenter_fallback";
						fallbackReason = fallbackReason || "image_render_unavailable";
					}
				}
			}

			if (!norm) {
				if (
					plannedVisualType === "image" &&
					!ALLOW_PAID_IMAGE_SEGMENT_FALLBACK
				) {
					throw new Error(
						`planned image segment ${seg.index} failed all local visual fallbacks`,
					);
				}
				if (plannedVisualType === "image" && !fallbackReason) {
					actualVisualType = "presenter_fallback";
					fallbackReason = "image_render_unavailable";
				}
				let presenterRenderError = null;
				try {
					norm = await renderHeyGenPresenterSegment({
						jobId,
						tmpDir,
						output,
						presenterImageUrl: presenterHeyGenImageUrl,
						segDur,
						audioPath: seg.audioPath,
						label: seg.renderLabel || String(seg.index),
						addFades: ENABLE_SEGMENT_FADES,
						cameraMotion: seg.cameraMotion,
						text: seg.text || "",
						expression: exprKey,
						mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
						pace: heygenPace,
						role:
							plannedVisualType === "image"
								? "presenter_rescue"
								: mustUsePresenter
									? "content_required"
									: "content_mid",
					});
					actualVisualType = "presenter";
				} catch (e) {
					presenterRenderError = e;
					logJob(jobId, "heygen presenter segment render failed", {
						segment: seg.index,
						renderLabel: seg.renderLabel || String(seg.index),
						mustUsePresenter,
						error: e.message,
					});
				}
				if (!norm) {
					if (mustUsePresenter && REQUIRE_FORCED_OPENING_PRESENTERS) {
						const errorMessage =
							presenterRenderError?.message || "unknown presenter render error";
						logJob(jobId, "required presenter segment render failed", {
							segment: seg.index,
							renderLabel: seg.renderLabel || String(seg.index),
							error: errorMessage,
						});
						throw new Error(
							`required_presenter_segment_failed:${seg.renderLabel || seg.index}:${errorMessage}`,
						);
					}
					logJob(jobId, "presenter segment render failed; using non-presenter visual", {
						segment: seg.index,
						renderLabel: seg.renderLabel || String(seg.index),
						mustUsePresenter,
						error: presenterRenderError?.message || "unknown error",
					});
					norm = await renderNonPresenterFallbackSegment({
						segDur,
						audioPath: seg.audioPath,
						label: `${seg.renderLabel || seg.index}_presenter_failed`,
						addFades: ENABLE_SEGMENT_FADES,
						cameraMotion: seg.cameraMotion
							? { ...seg.cameraMotion, visualType: "image" }
							: null,
						preferredImagePaths: feedImageFallbackPaths,
						fallbackText: seg.text || "",
						reason: "presenter_render_failed",
					});
					actualVisualType = "image_rescue";
					fallbackReason = fallbackReason
						? `${fallbackReason}_presenter_render_failed`
						: "presenter_render_failed";
				}
			}

			segmentVideos.push(norm);
			segmentRenderSummary.push({
				label: seg.renderLabel || String(seg.index),
				segment: seg.index,
				renderSegments: seg.renderSegmentIndices || [seg.index],
				plannedVisualType,
				actualVisualType,
				durationSec: segDur,
				mustUsePresenter,
				...(fallbackReason ? { fallbackReason } : {}),
			});
			logJob(jobId, "segment ready", {
				segment: seg.index,
				renderLabel: seg.renderLabel || String(seg.index),
				renderSegments: seg.renderSegmentIndices || [seg.index],
				visualType: plannedVisualType,
				actualVisualType,
				...(fallbackReason ? { fallbackReason } : {}),
			});
		}

		// 12) Outro as one HeyGen segment with a silent closed-mouth smile tail.
		const outroCameraMotion = inferCameraMotionPlan({
			text: outroTextFinal,
			topicLabel: topicSummary,
			expression: outroExpression,
			mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
			visualType: "presenter",
			durationSec: outroDurationSec,
			index: timeline.length + 1,
			categoryLabel,
		});
		let outroPath = "";
		let outroActualVisualType = "presenter";
		let outroSmileTailAppliedSec = OUTRO_SMILE_TAIL_SEC;
		try {
			let outroHeyGenAudioPath = outroAudioPath;
			let outroHeyGenDurationSec = outroDurationSec;
			outroSmileTailAppliedSec = OUTRO_SMILE_TAIL_SEC;
			if (outroSmileTailAppliedSec > 0.05) {
				const outroTailSilence = path.join(
					tmpDir,
					`outro_silent_smile_${jobId}.wav`,
				);
				await createSilentWav({
					durationSec: outroSmileTailAppliedSec,
					outPath: outroTailSilence,
				});
				outroHeyGenAudioPath = path.join(
					tmpDir,
					`outro_with_smile_tail_${jobId}.wav`,
				);
				await concatAudioClips([outroAudioPath, outroTailSilence], outroHeyGenAudioPath);
				safeUnlink(outroTailSilence);
				outroHeyGenDurationSec =
					(await probeDurationSeconds(outroHeyGenAudioPath)) ||
					outroDurationSec + outroSmileTailAppliedSec;
				logJob(jobId, "outro silent smile tail prepared", {
					spokenOutroSec: Number((outroDurationSec || 0).toFixed(3)),
					silentSmileTailSec: Number(outroSmileTailAppliedSec.toFixed(1)),
					preferredHeyGenPresenterSegmentSec:
						PREFERRED_HEYGEN_PRESENTER_SEGMENT_SEC,
					qualityFirstPresenterPlanning: true,
					heygenOutroSec: Number((outroHeyGenDurationSec || 0).toFixed(3)),
				});
			}
			outroPath = await renderHeyGenPresenterSegment({
				jobId,
				tmpDir,
				output,
				presenterImageUrl: presenterHeyGenImageUrl,
				segDur: outroHeyGenDurationSec,
				audioPath: outroHeyGenAudioPath,
				label: "outro",
				addFades: true,
				cameraMotion: outroCameraMotion,
				text: outroTextFinal,
				expression: outroExpression,
				mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
				pace: resolveHeyGenPace({
					text: outroTextFinal,
					expression: outroExpression,
					mood: tonePlan?.mood || voiceTonePlan?.mood || "neutral",
				}),
				role: "outro",
				silentSmileTailSec: outroSmileTailAppliedSec,
			});
			outroDurationSec = outroHeyGenDurationSec;
		} catch (e) {
			logJob(jobId, "outro heygen presenter render failed; stopping final assembly", {
				error: e.message,
				requireOutroPresenter: REQUIRE_OUTRO_PRESENTER,
				outroSmileTailSec: Number(outroSmileTailAppliedSec.toFixed(1)),
			});
			throw new Error(`required_outro_presenter_failed:${e.message}`);
		}
		if (!outroPath || !fs.existsSync(outroPath)) {
			logJob(jobId, "outro presenter output missing; stopping final assembly", {
				outroPath: outroPath || "",
				requireOutroPresenter: REQUIRE_OUTRO_PRESENTER,
			});
			throw new Error("required_outro_presenter_failed:output_missing");
		}
		segmentVideos.push(outroPath);
		segmentRenderSummary.push({
			label: "outro",
			plannedVisualType: "presenter",
			actualVisualType: outroActualVisualType,
			durationSec: outroDurationSec,
			mustUsePresenter: true,
			silentSmileTailSec: outroSmileTailAppliedSec,
		});

		const presenterCoverage = summarizePresenterCoverage(segmentRenderSummary);
		const minPresenterUnits = Math.max(
			MIN_ACTUAL_PRESENTER_SEGMENTS,
			Math.ceil(
				presenterCoverage.plannedPresenterUnits *
					MIN_ACTUAL_PRESENTER_PLAN_RATIO,
			),
		);
		const presenterCoverageIssues = [];
		if (
			!ALLOW_ZERO_PRESENTER_OUTPUT &&
			presenterCoverage.plannedPresenterUnits > 0 &&
			presenterCoverage.actualPresenterUnits <= 0
		) {
			presenterCoverageIssues.push("no_presenter_segments");
		}
		if (presenterCoverage.actualPresenterUnits < minPresenterUnits) {
			presenterCoverageIssues.push("too_few_presenter_segments");
		}
		if (
			presenterCoverage.plannedPresenterUnits > 0 &&
			presenterCoverage.actualPresenterDurationRatio <
				MIN_ACTUAL_PRESENTER_DURATION_RATIO
		) {
			presenterCoverageIssues.push("presenter_duration_too_low");
		}
		if (
			REQUIRE_FORCED_OPENING_PRESENTERS &&
			presenterCoverage.forcedOpeningMisses.length
		) {
			presenterCoverageIssues.push("forced_opening_presenter_missing");
		}
		const presenterCoverageQa = {
			...presenterCoverage,
			totalDurationSec: Number(
				(presenterCoverage.totalDurationSec || 0).toFixed(3),
			),
			plannedPresenterDurationSec: Number(
				(presenterCoverage.plannedPresenterDurationSec || 0).toFixed(3),
			),
			actualPresenterDurationSec: Number(
				(presenterCoverage.actualPresenterDurationSec || 0).toFixed(3),
			),
			actualPresenterDurationRatio: Number(
				(presenterCoverage.actualPresenterDurationRatio || 0).toFixed(3),
			),
			minPresenterUnits,
			minPresenterDurationRatio: MIN_ACTUAL_PRESENTER_DURATION_RATIO,
			minPresenterPlanRatio: MIN_ACTUAL_PRESENTER_PLAN_RATIO,
			failOnLowCoverage: FAIL_ON_LOW_PRESENTER_COVERAGE,
			pass: presenterCoverageIssues.length === 0,
			issues: presenterCoverageIssues,
		};
		logJob(jobId, "presenter coverage qa", presenterCoverageQa);
		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				presenterCoverage: presenterCoverageQa,
				actualVisualPlan: segmentRenderSummary,
			},
		});
		if (!presenterCoverageQa.pass) {
			if (FAIL_ON_LOW_PRESENTER_COVERAGE) {
				throw new Error(
					`presenter_coverage_failed:${presenterCoverageIssues.join(",")}`,
				);
			}
			logJob(jobId, "presenter coverage degraded; continuing with image rescues", {
				issues: presenterCoverageIssues,
				actualPresenterUnits: presenterCoverage.actualPresenterUnits,
				actualPresenterDurationRatio: Number(
					(presenterCoverage.actualPresenterDurationRatio || 0).toFixed(3),
				),
			});
		}

		updateJob(jobId, { progressPct: 72 });

		// 13) Concat intro + content + outro
		const concatPath = path.join(tmpDir, `concat_${jobId}.mp4`);
		await concatClips(segmentVideos, concatPath, {
			...output,
			softTransitions: true,
		});
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
				totalDurationSec,
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
					overlayedPath,
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
		const outputSlug =
			safeSlug(
				seoMeta?.seoTitle || script.title || topicSummary || "long_video",
				56,
			) || "long_video";
		const outputName = `long_${outputSlug}_${jobId}.mp4`;
		const outputPath = SHOULD_PERSIST_LONG_VIDEO
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
			if (!youtubeUploadEnabled) {
				logJob(jobId, "youtube upload skipped (disabled for controller)");
			} else if (!hasYouTubeTokens) {
				logJob(jobId, "youtube upload skipped (no tokens)");
			} else {
				const youtubePayload = {
					youtubeAccessToken,
					youtubeRefreshToken,
					youtubeTokenExpiresAt,
				};
				youtubeTokens = await refreshYouTubeTokensIfNeeded(
					user,
					youtubePayload,
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

		const finalVideoUrl = SHOULD_PERSIST_LONG_VIDEO
			? `${baseUrl}/uploads/videos/${outputName}`
			: youtubeLink || "";
		const outputUrl = SHOULD_PERSIST_LONG_VIDEO
			? finalVideoUrl
			: youtubeLink || "";
		const localFilePath = SHOULD_PERSIST_LONG_VIDEO ? outputPath : "";
		let videoDocId = null;
		try {
			if (user?._id) {
				const contentScriptLines = (script.segments || [])
					.map((s) => String(s.text || "").trim())
					.filter(Boolean);
				const outroDocKey = normalizeQaText(outroTextFinal);
				const scriptText = [
					introTextFinal,
					...contentScriptLines.filter((line, idx) => {
						if (idx !== contentScriptLines.length - 1) return true;
						const lineKey = normalizeQaText(line);
						return !outroDocKey || (lineKey !== outroDocKey && !lineKey.includes(outroDocKey));
					}),
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
				const actualVisualBySegment = new Map();
				for (const item of segmentRenderSummary || []) {
					for (const idx of item.renderSegments || []) {
						actualVisualBySegment.set(idx, {
							actualVisualType: item.actualVisualType,
							fallbackReason: item.fallbackReason || "",
						});
					}
				}
				const timelineForMeta = Array.isArray(timeline)
					? timeline.map((seg) => ({
							index: seg.index,
							topicIndex: seg.topicIndex,
							topicLabel: seg.topicLabel,
							text: seg.text,
							startSec: seg.startSec,
							endSec: seg.endSec,
							visualType: seg.visualType || "presenter",
							actualVisualType:
								actualVisualBySegment.get(seg.index)?.actualVisualType ||
								seg.visualType ||
								"presenter",
							fallbackReason:
								actualVisualBySegment.get(seg.index)?.fallbackReason || "",
							expression: seg.expression || "neutral",
							videoExpression: seg.videoExpression || seg.expression || "neutral",
							cameraMotion: seg.cameraMotion
								? {
										mode: seg.cameraMotion.mode || "steady",
										reason: seg.cameraMotion.reason || "",
										maxZoom: Number(seg.cameraMotion.maxZoom) || 1,
									}
								: { mode: "steady" },
							countdownRank: seg.countdownRank || null,
							countdownLabel: seg.countdownLabel || "",
						}))
					: [];
				const shortsDetailsForDoc = script?.shortsDetails || null;
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
					longVideoMeta: {
						promptTopic: promptTextForCategory || topicSummary,
						noveltyPlan: compactPriorVideoPlanForMeta(priorVideoPlan),
						segments: script?.segments || [],
						timeline: timelineForMeta,
						presenterCoverage: presenterCoverageQa,
						actualVisualPlan: segmentRenderSummary,
						preScriptVisualResearch,
					},
					shortsDetails: shortsDetailsForDoc,
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
					presenterOutfitStyle: presenterOutfitStyle || "",
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
		if (!SHOULD_PERSIST_LONG_VIDEO) safeUnlink(outputPath);
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

function createLongVideoController(controllerConfig = {}) {
	const cfg = normalizeLongVideoControllerConfig(controllerConfig);
	return async (req, res) => {
		const { errors, clean } = validateCreateBody(req.body || {}, cfg);
		if (errors.length)
			return res.status(400).json({ error: errors.join(", ") });
		if (!isOwnerOnlyUser(req)) {
			return res.status(403).json({
				error: "Long video creation is temporarily restricted to the owner.",
			});
		}

		const jobId = crypto.randomUUID();
		const baseUrl = buildBaseUrl(req);

		const job = {
			jobId,
			userId: req.user?._id ? String(req.user._id) : "",
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

		const statusUrl = `${cfg.statusPathBase}/${jobId}`;
		res.status(202).json({
			jobId,
			status: "queued",
			statusUrl,
		});

		logJob(jobId, "job queued", {
			controller: cfg.controllerLabel,
			statusUrl,
			baseUrl,
		});

		const schedule = req.body?.schedule || null;
		const scheduleJobMeta =
			req.scheduleJobMeta || req.body?.scheduleJobMeta || null;
		const isScheduledJob = Boolean(scheduleJobMeta);

		if (schedule && !isScheduledJob && req.user?._id) {
			const { type, timeOfDay, startDate, endDate } = schedule;
			if (!["daily", "weekly", "monthly"].includes(String(type || ""))) {
				console.warn(
					"[LongVideo] Invalid schedule type; skipping schedule save.",
				);
			} else if (!parseTimeOfDay(timeOfDay) || !startDate) {
				console.warn(
					"[LongVideo] Invalid schedule timing; skipping schedule save.",
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

		setImmediate(() =>
			runLongVideoJob(jobId, clean, baseUrl, req.user || null, cfg),
		);
	};
}

exports.createLongVideoController = createLongVideoController;
exports.createLongVideo = createLongVideoController();
exports.longVideoControllerFingerprint = LONG_VIDEO_CONTROLLER_FINGERPRINT;
exports.getLongVideoRuntimeProfile = getLongVideoRuntimeProfile;

/* ---------------------------------------------------------------
 * CONTROLLER: getLongVideoStatus
 * ------------------------------------------------------------- */

exports.getLongVideoStatus = async (req, res) => {
	const { jobId } = req.params;
	const job = JOBS.get(jobId);
	if (!job) return res.status(404).json({ error: "Job not found" });
	if (
		job.userId &&
		String(job.userId) !== String(req.user?._id || "") &&
		req.user?.role !== "admin"
	) {
		return res.status(403).json({ error: "Not authorized" });
	}

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
