/** @format */
/**
 * videoControllerLonger.js (DROP-IN REPLACEMENT - QUALITY + STABILITY)
 *
 * Key improvements (mapped to Ahmed's requirements):
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

const GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1";

const TRENDS_API_URL =
	process.env.TRENDS_API_URL || "http://localhost:8102/api/google-trends";
const TRENDS_HTTP_TIMEOUT_MS = 60000;
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
	"Studio is empty; remove any background people from the reference; no people in the background, no passersby, no background figures or silhouettes, no reflections of people, no movement behind the presenter.";
const PRESENTER_MOTION_STYLE =
	"natural head and neck movement, human blink rate with slight variation (every few seconds), soft eyelid closures, subtle breathing, soft micro-expressions, natural jaw movement, relaxed eyes, natural forehead movement; smile, if any, is very subtle; no exaggerated grin";

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
const CSE_MIN_IMAGE_SHORT_EDGE = 720;

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
// Slightly faster default pacing (~10-12% more words).
const SCRIPT_PACE_BIAS = clampNumber(1.12, 0.85, 1.35);
const SEGMENT_TARGET_SEC = 8;
const MAX_SEGMENTS = 45;
const SCRIPT_TOLERANCE_SEC = clampNumber(4.5, 2, 5);
const MAX_SCRIPT_REWRITES = clampNumber(4, 0, 5);
const MAX_FILLER_WORDS_PER_VIDEO = clampNumber(0, 0, 2);
const MAX_FILLER_WORDS_PER_SEGMENT = clampNumber(0, 0, 2);
const MAX_MICRO_EMOTES_PER_VIDEO = clampNumber(0, 0, 1);
const ENABLE_MICRO_EMOTES = true;

// Audio processing
const AUDIO_SR = 48000;
const AUDIO_CHANNELS = 1; // mono voice for stability + smaller sync payload
const GLOBAL_ATEMPO_MIN = 0.97;
const GLOBAL_ATEMPO_MAX = 1.05;
const INTRO_ATEMPO_MIN = clampNumber(0.97, 0.9, 1.05);
const INTRO_ATEMPO_MAX = clampNumber(1.06, 1.0, 1.15);
const OUTRO_ATEMPO_MIN = clampNumber(0.97, 0.9, 1.05);
const OUTRO_ATEMPO_MAX = clampNumber(1.06, 1.0, 1.15);
const SEGMENT_PAD_SEC = clampNumber(0.08, 0, 0.3);
const VOICE_SPEED_BOOST = clampNumber(1.0, 0.98, 1.08);

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
const USE_MOTION_REF_BASELINE = true;
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
const CONTENT_PRESENTER_RATIO = clampNumber(0.5, 0.2, 0.8);
const IMAGE_SEGMENT_TARGET_SEC = clampNumber(4.6, 2.5, 8);
const IMAGE_SEGMENT_MIN_IMAGES = clampNumber(2, 1, 6);
const IMAGE_SEGMENT_MAX_IMAGES = clampNumber(4, 2, 8);
const IMAGE_SEGMENT_MULTI_MIN_SEC = clampNumber(5.5, 3, 12);

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
		? clampNumber(targetDurationSec, 20, 300)
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
	const searchPhrases = uniqueStrings(
		[topic, rawTitle, ...(raw.searchPhrases || []), ...(raw.entityNames || [])],
		{ limit: 10 }
	);
	const articles = Array.isArray(raw.articles)
		? raw.articles
				.map((a) => ({
					title: String(a.title || "").trim(),
					url: a.url || null,
				}))
				.filter((a) => a.title)
		: [];
	const keywords = uniqueStrings(
		[...searchPhrases, ...articles.slice(0, 4).map((a) => a.title), topic],
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
	});
	const candidates = buildTrendsApiCandidates(baseUrl);
	for (let i = 0; i < candidates.length; i++) {
		const url = `${candidates[i]}?${params.toString()}`;
		try {
			logJob(null, "trends fetch", { url, attempt: i + 1 });
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
		} catch (e) {
			logJob(null, "trends fetch failed", { error: e.message, url });
		}
	}
	return [];
}

async function fetchCseItems(
	queries,
	{ num = 4, searchType = null, imgSize = null } = {}
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
	if (sec <= 90) return 1;
	if (sec <= 120) return 2;
	if (sec <= 180) return 3;
	return 4;
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
		if (isDuplicateTopic(hint, topics, usedSet)) {
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

	for (const story of trendStories) {
		if (topics.length >= desired) break;
		if (!story?.topic) continue;
		if (isDuplicateTopic(story.topic, topics, usedSet)) continue;
		const displayTopic = cleanTopicLabel(story.topic) || story.topic;
		topics.push({
			topic: story.topic,
			displayTopic,
			angle: "",
			reason: "Google Trends",
			keywords: topicTokensFromTitle(story.topic)
				.concat(topicTokensFromTitle(story.rawTitle || ""))
				.slice(0, 10),
			trendStory: story,
		});
		addUsedTopicVariants(usedSet, story.topic);
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
	];
	if (category === "film") {
		queries.push(`${topic} trailer`, `${topic} cast`, `${topic} box office`);
	} else if (category === "tv") {
		queries.push(`${topic} episode`, `${topic} season`, `${topic} streaming`);
	} else if (category === "music") {
		queries.push(`${topic} chart`, `${topic} music video`, `${topic} tour`);
	} else if (category === "celebrity") {
		queries.push(
			`${topic} interview`,
			`${topic} controversy`,
			`${topic} social media`
		);
	}

	const items = await fetchCseItems(queries, { num: 3 });
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

async function fetchCseImages(topic, extraTokens = []) {
	if (!topic) return [];
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const baseTokens = [...topicTokensFromTitle(topic), ...extra];
	const category = inferEntertainmentCategory(baseTokens);

	const queries = [
		`${topic} press photo`,
		`${topic} news photo`,
		`${topic} photo`,
	];
	if (category === "film") {
		queries.unshift(
			`${topic} official still`,
			`${topic} movie still`,
			`${topic} premiere`
		);
	} else if (category === "tv") {
		queries.unshift(`${topic} episode still`, `${topic} cast photo`);
	} else if (category === "music") {
		queries.unshift(`${topic} live performance`, `${topic} stage photo`);
	} else if (category === "celebrity") {
		queries.unshift(`${topic} red carpet`, `${topic} interview photo`);
	}

	const fallbackQueries = [
		`${topic} photo`,
		`${topic} press`,
		`${topic} red carpet`,
		`${topic} still`,
		`${topic} interview`,
	];
	const keyPhrase = filterSpecificTopicTokens(baseTokens).slice(0, 2).join(" ");
	if (keyPhrase) {
		fallbackQueries.push(`${keyPhrase} photo`, `${keyPhrase} press`);
	}

	let items = await fetchCseItems(queries, {
		num: 8,
		searchType: "image",
		imgSize: CSE_PREFERRED_IMG_SIZE,
	});
	if (!items.length) {
		items = await fetchCseItems(queries, {
			num: 8,
			searchType: "image",
			imgSize: CSE_FALLBACK_IMG_SIZE,
		});
	}
	if (!items.length) {
		items = await fetchCseItems(fallbackQueries, {
			num: 8,
			searchType: "image",
			imgSize: CSE_PREFERRED_IMG_SIZE,
		});
	}
	if (!items.length) {
		items = await fetchCseItems(fallbackQueries, {
			num: 8,
			searchType: "image",
			imgSize: CSE_FALLBACK_IMG_SIZE,
		});
	}
	const matchTokens = expandTopicTokens(filterSpecificTopicTokens(baseTokens));
	const minMatches = minTopicTokenMatches(matchTokens);

	const candidates = [];
	for (const it of items) {
		const url = it.link || "";
		if (!url || !/^https:\/\//i.test(url)) continue;
		const info = topicMatchInfo(matchTokens, [
			it.title,
			it.snippet,
			it.link,
			it.image?.contextLink || "",
		]);
		if (info.count < minMatches) continue;
		const w = Number(it.image?.width || 0);
		const h = Number(it.image?.height || 0);
		if (w && h && Math.min(w, h) < CSE_MIN_IMAGE_SHORT_EDGE) continue;
		const urlText = `${it.link || ""} ${
			it.image?.contextLink || ""
		}`.toLowerCase();
		const urlMatches = matchTokens.filter((tok) =>
			urlText.includes(tok)
		).length;
		const score = info.count + urlMatches * 0.75;
		candidates.push({ url, score, urlMatches, w, h });
		if (candidates.length >= 14) break;
	}

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
		if (!c?.url || seen.has(c.url)) continue;
		seen.add(c.url);
		if (!isProbablyDirectImageUrl(c.url)) continue;
		const ct = await headContentType(c.url, 7000);
		if (ct && !ct.startsWith("image/")) continue;
		filtered.push(c.url);
		if (filtered.length >= 6) break;
	}
	return filtered;
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

function buildOverlayQueryFallback(text = "", topic = "") {
	const base = cleanTopicCandidate(topic);
	const tokens = filterSpecificTopicTokens(tokenizeLabel(text)).slice(0, 4);
	const extras = tokens.filter(
		(t) => !base.toLowerCase().includes(String(t || "").toLowerCase())
	);
	const parts = [base, ...extras].filter(Boolean);
	return sanitizeOverlayQuery(parts.join(" "));
}

async function fetchCseImagesForQuery(query, topicTokens = [], maxResults = 4) {
	const q = sanitizeOverlayQuery(query);
	if (!q) return [];
	const target = clampNumber(Number(maxResults) || 4, 1, 12);
	const tokens = expandTopicTokens(
		filterSpecificTopicTokens([...tokenizeLabel(q), ...topicTokens])
	);
	const minMatches = minTopicTokenMatches(tokens);
	let items = await fetchCseItems([q], {
		num: Math.min(10, Math.max(8, target * 2)),
		searchType: "image",
		imgSize: CSE_PREFERRED_IMG_SIZE,
	});
	if (!items.length) {
		items = await fetchCseItems([q], {
			num: Math.min(10, Math.max(8, target * 2)),
			searchType: "image",
			imgSize: CSE_FALLBACK_IMG_SIZE,
		});
	}
	const candidates = [];
	const maxCandidates = Math.max(10, target * 3);

	for (const it of items) {
		const url = it.link || "";
		if (!url || !/^https:\/\//i.test(url)) continue;
		const info = topicMatchInfo(tokens, [
			it.title,
			it.snippet,
			it.link,
			it.image?.contextLink || "",
		]);
		if (info.count < minMatches) continue;
		const w = Number(it.image?.width || 0);
		const h = Number(it.image?.height || 0);
		if (w && h && Math.min(w, h) < CSE_MIN_IMAGE_SHORT_EDGE) continue;
		const urlText = `${it.link || ""} ${
			it.image?.contextLink || ""
		}`.toLowerCase();
		const urlMatches = tokens.filter((tok) => urlText.includes(tok)).length;
		const score = info.count + urlMatches * 0.75;
		candidates.push({ url, score, urlMatches, w, h });
		if (candidates.length >= maxCandidates) break;
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
		if (!c?.url || seen.has(c.url)) continue;
		seen.add(c.url);
		if (!isProbablyDirectImageUrl(c.url)) continue;
		const ct = await headContentType(c.url, 7000);
		if (ct && !ct.startsWith("image/")) continue;
		filtered.push(c.url);
		if (filtered.length >= target) break;
	}

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
	const cueQuery =
		cueRaw?.query || buildOverlayQueryFallback(seg?.text || "", topicLabel);
	const query = ensureTopicInQuery(cueQuery, topicLabel);
	return { query, topicLabel };
}

function pickSegmentImageUrls(candidates = [], desiredCount = 1, usedUrls) {
	const pool = [];
	const seen = new Set();
	for (const u of candidates) {
		const url = String(u || "").trim();
		if (!url || seen.has(url)) continue;
		seen.add(url);
		pool.push(url);
	}
	if (!pool.length) return [];

	const target = Math.max(1, Math.floor(desiredCount));
	const picks = [];
	const picked = new Set();

	for (const url of pool) {
		if (picks.length >= target) break;
		if (usedUrls && usedUrls.has(url)) continue;
		picks.push(url);
		picked.add(url);
	}

	if (picks.length < target) {
		for (const url of pool) {
			if (picks.length >= target) break;
			if (picked.has(url)) continue;
			picks.push(url);
			picked.add(url);
		}
	}

	if (usedUrls) {
		for (const url of picks) usedUrls.add(url);
	}
	return picks;
}

async function downloadSegmentImages(urls, tmpDir, jobId, segIndex) {
	const localPaths = [];
	for (let i = 0; i < urls.length; i++) {
		const url = urls[i];
		if (!url) continue;
		const extGuess = path
			.extname(String(url).split("?")[0] || "")
			.toLowerCase();
		const ext = extGuess && extGuess.length <= 5 ? extGuess : ".jpg";
		const out = path.join(tmpDir, `seg_${jobId}_${segIndex}_img_${i}${ext}`);
		try {
			await downloadToFile(url, out, 25000, 1);
			const detected = detectFileType(out);
			if (!detected || detected.kind !== "image") {
				safeUnlink(out);
				continue;
			}
			localPaths.push(out);
		} catch {
			safeUnlink(out);
		}
	}
	return localPaths;
}

async function prepareImageSegments({
	timeline = [],
	topics = [],
	tmpDir,
	jobId,
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
	const usedUrls = new Set();
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
		const { query, topicLabel } = resolveSegmentImageQuery(seg, topics);
		const cacheKey = `${query}||${topicLabel}`;

		let candidates = [];
		if (query && queryCache.has(cacheKey)) {
			candidates = queryCache.get(cacheKey) || [];
		} else if (query) {
			const topicTokens = topicTokensFromTitle(topicLabel || "");
			const fromQuery = await fetchCseImagesForQuery(
				query,
				topicTokens,
				Math.max(6, desiredCount * 2)
			);
			let fromTopic = [];
			if (topicLabel && fromQuery.length < desiredCount) {
				fromTopic = await fetchCseImages(topicLabel, [query]);
			}
			candidates = Array.from(new Set([...(fromQuery || []), ...fromTopic]));
			queryCache.set(cacheKey, candidates);
			if (topicLabel && candidates.length)
				topicCache.set(topicLabel, candidates);
		}

		if (!candidates.length && topicLabel && topicCache.has(topicLabel)) {
			candidates = topicCache.get(topicLabel) || [];
		}

		const picks = pickSegmentImageUrls(candidates, desiredCount, usedUrls);
		const localPaths = await downloadSegmentImages(
			picks,
			tmpDir,
			jobId,
			seg.index
		);

		if (!localPaths.length) {
			logJob(jobId, "segment images missing; fallback to presenter", {
				segment: seg.index,
				query,
				topicLabel,
			});
			updated.push({ ...seg, visualType: "presenter" });
			continue;
		}

		segmentImagePaths.set(seg.index, localPaths);
		imagePlanSummary.push({
			segment: seg.index,
			imageCount: localPaths.length,
			query,
			topicLabel,
		});
		updated.push({ ...seg, imageUrls: picks });
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
		"Expression: calm and professional; mouth near-neutral with a very subtle smile (barely noticeable, not constant).";
	if (expr === "warm")
		expressionLine =
			"Expression: friendly and approachable with a very subtle, light smile (barely there, not constant).";
	if (expr === "excited")
		expressionLine =
			"Expression: energized and engaged, minimal smile only, no exaggerated grin.";
	if (expr === "serious")
		expressionLine =
			"Expression: serious but calm, neutral mouth, low-energy delivery, soft eye contact.";
	if (expr === "thoughtful")
		expressionLine =
			"Expression: thoughtful and attentive, relaxed mouth, gentle eye focus, minimal smile.";

	const variantHint =
		variant === 1
			? "Add tiny head tilts and micro shifts in posture; keep movement subtle."
			: variant === 2
			? "Use a slightly different blink cadence and a soft head turn or two."
			: "";
	const motionHint = motionRefVideo
		? "Match the natural motion style from the reference performance: gentle head nods, human blink rate with slight variation, small hand movements."
		: PRESENTER_MOTION_STYLE;

	return `
Photorealistic talking-head video of the SAME person as the reference image.
Keep identity, studio background, lighting, and wardrobe consistent. ${STUDIO_EMPTY_PROMPT}
Props: keep all existing props exactly as in the reference; do not add or remove objects. If a candle is visible, keep it subtle and unchanged with a calm flame; no extra candles.
Framing: medium shot (not too close, not too far), upper torso to mid torso, moderate headroom; desk visible; camera at a comfortable distance.
${expressionLine}
Motion: ${motionHint} ${variantHint}
Mouth and jaw: natural, human movement; avoid robotic or stiff mouth shapes.
Forehead: natural skin texture and subtle movement; avoid waxy smoothing.
Eyes: relaxed, comfortable, natural reflections and blink cadence; avoid glassy or robotic eyes.
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
	if (explicit) return explicit;
	if (mood === "serious" && base === "serious") return "serious";
	if (mood === "excited" && base === "excited") return "excited";
	if (base === "warm") return "warm";
	if (base === "neutral" && mood !== "serious") return "warm";
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

function cleanTopicLabel(text = "") {
	return String(text || "")
		.replace(/["'(){}\[\]]/g, "")
		.replace(/\s+/g, " ")
		.replace(/[.!?]+$/g, "")
		.trim();
}

function shortTopicLabel(text = "", maxWords = 4) {
	const base = cleanTopicLabel(text);
	const words = base.split(/\s+/).filter(Boolean);
	if (!words.length) return "today's topic";
	if (words.length <= maxWords) return words.join(" ");
	return words.slice(0, maxWords).join(" ");
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

function buildIntroLine({ topics = [], shortTitle }) {
	const subject = shortTitle
		? shortTopicLabel(shortTitle, 4)
		: formatTopicList(topics);
	const line = `Hi there, my name is Ahmed. Quick update on ${subject}.`;
	return sanitizeIntroOutroLine(line);
}

function buildTopicEngagementQuestionForLabel(
	topicLabel,
	mood = "neutral",
	{ compact = false } = {}
) {
	const label = shortTopicLabel(topicLabel, compact ? 3 : 4);
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
		return buildTopicEngagementQuestionForLabel(labels[0], mood, { compact });

	if (compact)
		return mood === "serious"
			? "Which topic matters most to you?"
			: "Which topic stood out to you most?";

	const list = formatTopicList(labels);
	return mood === "serious"
		? `Which of these topics matters most to you: ${list}?`
		: `Which of these stood out to you most: ${list}?`;
}

function buildOutroLine({ topics = [], shortTitle, mood = "neutral" }) {
	const question = buildTopicEngagementQuestion({
		topics,
		shortTitle,
		mood,
		compact: true,
	});
	let line = `${question} Thanks for watching. Like the video, and see you next time.`;
	if (countWords(line) > 18) {
		line = `${question} Thanks for watching, like the video, see you next time.`;
	}
	if (countWords(line) > 16) {
		line = `${question} Thanks for watching. Like the video. See you next time.`;
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

const TOPIC_TRANSITION_TEMPLATES = [
	"Alright, switching gears to {topic}. Here's the quick read.",
	"Next up: {topic}. Here's the key update.",
	"Now pivoting to {topic}. Here's what matters.",
	"Alright, moving on to {topic}. Here's the latest.",
	"Turning to {topic}. Here's the headline.",
];

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

		if (i > 0 && topicIndex !== lastTopicIndex && topicLabel) {
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
			const baseWords = baseText.split(/\s+/).filter(Boolean);
			const questionWords = question.split(/\s+/).filter(Boolean);
			const allowedBaseWords = Math.max(0, cap - questionWords.length);
			if (baseWords.length > allowedBaseWords) {
				baseText = baseWords.slice(0, allowedBaseWords).join(" ");
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
	includeOutro = false,
}) {
	if (!process.env.CHATGPT_API_TOKEN)
		throw new Error("CHATGPT_API_TOKEN missing");

	const safeTopics =
		Array.isArray(topics) && topics.length
			? topics.filter((t) => t && t.topic)
			: [{ topic: "today's topic" }];
	const topicLabelFor = (t) => String(t?.displayTopic || t?.topic || "").trim();
	const topicRanges = allocateTopicSegments(segmentCount, safeTopics);
	const capsLine = wordCaps.map((c, i) => `#${i}: <= ${c} words`).join(", ");
	const mood = tonePlan?.mood || "neutral";
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
		? "End the LAST segment of EACH topic with one short, topic-specific engagement question for comments. Do NOT add like/subscribe in content; the closing line handles thanks and likes."
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

	const contextLines =
		Array.isArray(topicContexts) && topicContexts.length
			? topicContexts
					.map((tc, idx) => {
						const items = Array.isArray(tc.context) ? tc.context : [];
						const lineItems = items
							.map((c) =>
								typeof c === "string"
									? c
									: `${c.title}${c.snippet ? " | " + c.snippet : ""}`
							)
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

Target narration duration (NOT counting intro/outro): ~${narrationTargetSec.toFixed(
		1
	)}s
Segments: EXACTLY ${segmentCount}
Per-segment word caps: ${capsLine}

Use this background context as hints; do NOT pretend its real-time verified:
${contextLines}

Topic notes:
${topicHintLines}

Style rules (IMPORTANT):
- Keep pacing steady and conversational; no sudden speed-ups.
- Slightly brisk, natural American delivery; avoid drawn-out phrasing.
- Sound like a real creator, not a press release. No "Ladies and gentlemen", no "In conclusion", no corporate tone.
- Keep it lightly casual: a few friendly, natural phrases like "real quick" or "here's the thing" (max 1 per topic), but stay professional.
- Use contractions. Punchy sentences. A little playful, but not cringe.
- Avoid staccato punctuation. Do NOT put commas between single words.
- Keep punctuation light and flowing; prefer smooth, natural sentences.
- Lead with the answer, then add context (what happened, why it matters, what to watch for).
- Avoid repeating the topic question or using vague filler phrasing; be specific and helpful.
- Avoid exclamation points unless the script explicitly calls for excitement.
- Each segment should be 1-2 sentences. Do NOT switch topics mid-sentence.
- Avoid specific dates, rankings, or stats unless they appear in the provided context above.
- Avoid filler words ("um", "uh", "umm", "uhm", "ah", "like"). Use zero filler words in the entire script, especially in segments 0-2.
- Do NOT add micro vocalizations ("heh", "whew", "hmm").
- Do NOT mention "intro", "outro", "segment", "next segment", or say "in this video/clip".
- Segment 0 must be a strong hook that makes people stay.
- Segment 0 follows the tone plan; middle segments stay conversational/neutral; last segment wraps with the tone plan.
- Each topic is its own mini story with clear transitions.
- Make topic handoffs feel smooth and coherent; use a brief bridge phrase to set up the next topic.
- The FIRST segment of Topic 2+ must START with an explicit transition line that names the topic, like "And now, let's talk about {topic}."
- The FIRST segment for every topic must mention the topic name in the first sentence.
- Each segment should naturally flow into the next with a quick transition phrase.
- Each segment ends with a complete sentence and strong terminal punctuation. Do NOT end with "and", "but", "so", "because", "with", "to", "for", "that", or an open parenthetical.
- No long lists. If you must list, cap at 3 items.
- If you are unsure about a detail, say "reports suggest" or "early signs".
- ${ctaLine}
- Topic questions must be short and end with a single question mark.
- Provide "shortTitle": 2-5 words, punchy and easy to read.
- For each segment, include "expression" from: neutral, warm, serious, excited, thoughtful.
- Default to warm (light smile) for regular news; use serious for sad/hard news, neutral for somber or low-tone lines, thoughtful when reflective.
- Keep expressions coherent across segments; avoid abrupt mood flips. Use warm smiles lightly and very subtle (barely noticeable), never exaggerated.
- Each segment must include EXACTLY one overlayCues entry with a search query that matches that segment.
- overlayCues.query must be 2-6 words, describe a real photo to search for, include the topic name or a key subject from that segment, no punctuation or hashtags.
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

	// Enforce caps (hard trim if needed - keeps flow and avoids a second model call)
	segments = segments.map((s, i) => {
		const cap = wordCaps[i] || 22;
		const words = s.text.split(/\s+/).filter(Boolean);
		if (words.length <= cap) return s;
		return {
			...s,
			text: words
				.slice(0, cap)
				.join(" ")
				.replace(/[,;:]?$/, "."),
		};
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
	const expr = coerceExpressionForNaturalness(expression, text, mood);
	let stability = Math.max(ELEVEN_TTS_STABILITY, uniform ? 0.72 : 0.6);
	let style = Math.min(ELEVEN_TTS_STYLE, uniform ? 0.16 : 0.22);

	if (uniform) {
		if (mood === "serious") stability += 0.04;
		if (mood === "excited") style += 0.04;
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
	return { wavPath: wav, durationSec, modelId: usedModelId };
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
		? "Match the natural motion style from the reference performance: gentle head nods, human blink rate with slight variation, subtle hand gestures."
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

		logJob(jobId, "job started", {
			dryRun,
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
			topicContexts.push({ topic: t.topic, context: ctx });
			liveContext = liveContext.concat(ctx || []);
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
		const tonePlan = inferTonePlan({
			topics: topicPicks,
			liveContext,
		});

		// 5) Script (content duration excludes intro/outro)
		const lang = languageLabel || String(language || "en");
		const narrationTargetSec = Math.max(18, Number(contentTargetSec) || 0);
		const segmentCount = computeSegmentCount(narrationTargetSec);
		const wordCaps = buildWordCaps(segmentCount, narrationTargetSec);

		const script = await generateScript({
			jobId,
			topics: topicPicks,
			languageLabel: lang,
			narrationTargetSec,
			segmentCount,
			wordCaps,
			topicContexts,
			tonePlan,
			includeOutro: true,
		});

		updateJob(jobId, {
			progressPct: 18,
			meta: {
				...JOBS.get(jobId)?.meta,
				title: script.title,
				shortTitle: script.shortTitle,
				script: { title: script.title, segments: script.segments },
			},
		});

		// 5.5) Thumbnail (script-aligned, fail-fast)
		try {
			const fallbackTitle = topicTitles[0] || topicSummary || "Quick Update";
			const thumbTitle = String(script.title || fallbackTitle).trim();
			const thumbShortTitle = String(
				script.shortTitle || shortTitleFromText(thumbTitle)
			).trim();
			const thumbExpression =
				script?.segments?.[0]?.expression || tonePlan?.mood || "warm";
			const thumbLog = (message, payload) => logJob(jobId, message, payload);
			const thumbResult = await generateThumbnailPackage({
				jobId,
				tmpDir,
				presenterLocalPath: presenterLocal,
				title: thumbTitle,
				shortTitle: thumbShortTitle,
				seoTitle: "",
				topics: topicPicks,
				expression: thumbExpression,
				openai,
				log: thumbLog,
				requireTopicImages: true,
			});
			const thumbLocalPath = thumbResult?.localPath || "";
			const thumbCloudUrl = thumbResult?.url || "";
			const thumbPublicId = thumbResult?.publicId || "";
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
				},
			});
			logJob(jobId, "thumbnail ready", {
				path: thumbnailPath ? path.basename(thumbnailPath) : null,
				cloudinary: Boolean(thumbnailUrl),
				pose: thumbResult?.pose || null,
				accent: thumbResult?.accent || null,
			});
		} catch (e) {
			logJob(jobId, "thumbnail generation failed (hard stop)", {
				error: e.message,
			});
			throw e;
		}

		// 5.6) Presenter wardrobe adjustment (post-script/thumbnail)
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
						logJob(jobId, "presenter adjustments ready", {
							path: path.basename(presenterLocal),
							method: presenterResult.method || "runway",
							cloudinary: Boolean(presenterResult.url),
						});
						updateJob(jobId, {
							meta: {
								...JOBS.get(jobId)?.meta,
								presenterImageUrl: presenterResult.url || "",
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
		const introOutroMood = "neutral";
		const introLine = buildIntroLine({
			topics: topicPicks,
			shortTitle: script.shortTitle || script.title,
		});
		const outroLine = buildOutroLine({
			topics: topicPicks,
			shortTitle: script.shortTitle || script.title,
			mood: introOutroMood,
		});
		const introExpression = "neutral";
		const outroExpression = "warm";

		logJob(jobId, "orchestrator plan", {
			mood: introOutroMood,
			contentTargetSec,
			intro: {
				text: introLine,
				targetSec: introDurationSec,
				expression: introExpression,
			},
			outro: {
				text: outroLine,
				targetSec: outroDurationSec,
				expression: outroExpression,
			},
		});

		updateJob(jobId, {
			meta: {
				...JOBS.get(jobId)?.meta,
				intro: { text: introLine, targetSec: introDurationSec },
				outro: { text: outroLine, targetSec: outroDurationSec },
			},
		});

		const lockedVoiceSettings = UNIFORM_TTS_VOICE_SETTINGS
			? buildVoiceSettingsForExpression("neutral", tonePlan?.mood, "", {
					uniform: true,
			  })
			: null;
		const resolveVoiceSettings = (expression, text) =>
			lockedVoiceSettings ||
			buildVoiceSettingsForExpression(expression, tonePlan?.mood, text);
		const ttsModelOrder = [
			ELEVEN_TTS_MODEL,
			...ELEVEN_TTS_MODEL_FALLBACKS,
		].filter(Boolean);
		let ttsModelId = "";

		const introVoiceSettings = resolveVoiceSettings(introExpression, introLine);
		const introTts = await synthesizeTtsWav({
			text: introLine,
			tmpDir,
			jobId,
			label: "intro",
			voiceId: effectiveVoiceId,
			voiceSettings: introVoiceSettings,
			modelId: ttsModelId || undefined,
			modelOrder: ttsModelOrder,
		});
		if (introTts?.modelId) ttsModelId = introTts.modelId;
		const introFit = await fitWavToTargetDuration({
			wavPath: introTts.wavPath,
			targetSec: introDurationSec,
			minAtempo: INTRO_ATEMPO_MIN,
			maxAtempo: INTRO_ATEMPO_MAX,
			tmpDir,
			jobId,
			label: "intro",
		});
		if (!introFit.durationSec)
			throw new Error("Intro voice generation failed (empty duration)");
		const introAudioPath = introFit.wavPath;
		introDurationSec = introFit.durationSec || introDurationSec;
		if (introDurationSec < INTRO_MIN_SEC || introDurationSec > INTRO_MAX_SEC) {
			logJob(jobId, "intro duration outside target range", {
				introDurationSec: Number(introDurationSec.toFixed(3)),
				targetMin: INTRO_MIN_SEC,
				targetMax: INTRO_MAX_SEC,
			});
		}
		logJob(jobId, "intro voice ready", {
			durationSec: Number((introFit.durationSec || 0).toFixed(3)),
			atempo: Number(introFit.atempo.toFixed(3)),
			rawAtempo: Number(introFit.rawAtempo.toFixed(3)),
		});

		const outroVoiceSettings = resolveVoiceSettings(outroExpression, outroLine);
		const outroTts = await synthesizeTtsWav({
			text: outroLine,
			tmpDir,
			jobId,
			label: "outro",
			voiceId: effectiveVoiceId,
			voiceSettings: outroVoiceSettings,
			modelId: ttsModelId || undefined,
			modelOrder: ttsModelOrder,
		});
		if (outroTts?.modelId) ttsModelId = outroTts.modelId;
		const outroFit = await fitWavToTargetDuration({
			wavPath: outroTts.wavPath,
			targetSec: outroDurationSec,
			minAtempo: OUTRO_ATEMPO_MIN,
			maxAtempo: OUTRO_ATEMPO_MAX,
			tmpDir,
			jobId,
			label: "outro",
		});
		if (!outroFit.durationSec)
			throw new Error("Outro voice generation failed (empty duration)");
		const outroAudioPath = outroFit.wavPath;
		outroDurationSec = outroFit.durationSec || outroDurationSec;
		if (outroDurationSec < OUTRO_MIN_SEC || outroDurationSec > OUTRO_MAX_SEC) {
			logJob(jobId, "outro duration outside target range", {
				outroDurationSec: Number(outroDurationSec.toFixed(3)),
				targetMin: OUTRO_MIN_SEC,
				targetMax: OUTRO_MAX_SEC,
			});
		}
		logJob(jobId, "outro voice ready", {
			durationSec: Number((outroFit.durationSec || 0).toFixed(3)),
			atempo: Number(outroFit.atempo.toFixed(3)),
			rawAtempo: Number(outroFit.rawAtempo.toFixed(3)),
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
		segments = segments.map((s, i) => ({
			...s,
			expression: smoothedExpressions[i] || s.expression,
			videoExpression: "neutral",
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
					logJob(jobId, "tts segment start", {
						segment: seg.index,
						words: countWords(seg.text),
					});

					const mp3 = path.join(tmpDir, `tts_${jobId}_${seg.index}.mp3`);
					const cleanWav = path.join(
						tmpDir,
						`tts_clean_${jobId}_${seg.index}.wav`
					);

					const cleanText = sanitizeSegmentText(seg.text);
					const voiceSettings = resolveVoiceSettings(seg.expression, cleanText);
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
					cleanedWavs.push({ index: seg.index, wav: cleanWav, cleanDur: d });
					sumCleanDur += d;
				}
			}

			if (sumCleanDur < 3)
				throw new Error("Voice audio generation failed (empty duration)");

			const rawAtempo = sumCleanDur / narrationTargetSec;
			driftSec = Math.abs(sumCleanDur - narrationTargetSec);
			const toleranceSec = Math.min(
				SCRIPT_TOLERANCE_SEC,
				Math.max(1, narrationTargetSec * 0.07)
			);
			const ratioDelta = Math.abs(1 - rawAtempo);
			const withinTolerance = driftSec <= toleranceSec;
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
				attempt,
				voiceSpeedBoost: VOICE_SPEED_BOOST,
			});

			const needsRewrite =
				!voiceoverUrl &&
				(!withinTolerance ||
					rawAtempo < GLOBAL_ATEMPO_MIN ||
					rawAtempo > GLOBAL_ATEMPO_MAX);
			if (!needsRewrite || attempt >= maxRewriteAttempts) break;

			// cleanup current audio before rewrite
			for (const a of cleanedWavs) safeUnlink(a.wav);

			const ratio = narrationTargetSec / sumCleanDur;
			const adjustPct = clampNumber(
				Math.round(Math.abs(1 - ratio) * 100 + 4),
				8,
				24
			);
			const direction = ratio > 1 ? "LONGER" : "SHORTER";
			const adjustedCaps = wordCaps.map((c) =>
				Math.max(12, Math.round(c * ratio))
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
Per-segment word caps (updated): ${capsLine2}
Expressions by segment (keep these expressions, only adjust text): ${expressionsLine}
Topic assignment by segment (do NOT change order): ${topicsLine}

Rules:
- Keep the same topic and tone (US audience, fun, not formal).
- Keep it lightly casual: a few friendly, natural phrases like "real quick" or "here's the thing" (max 1 per topic).
- Keep EXACTLY ${segments.length} segments.
- Preserve smooth transitions.
- Make topic handoffs feel smooth and coherent; use a brief bridge phrase to set up the next topic.
- If a segment is the first for a new topic, start it with an explicit transition line naming the topic (example: "And now, let's talk about {topic}.").
- Improve clarity and specificity; avoid vague filler phrasing or repeating the question.
- Avoid filler words ("um", "uh", "umm", "uhm", "ah", "like"). Use zero filler words in the entire script, especially in segments 0-2.
- Do NOT add micro vocalizations ("heh", "whew", "hmm").
- Do NOT mention "intro", "outro", "segment", "next segment", or say "in this video/clip".
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

		// Apply global atempo to each segment (may be 1.0 within tolerance)
		const segmentAudio = [];
		for (const a of cleanedWavs.sort((x, y) => x.index - y.index)) {
			const out = path.join(tmpDir, `seg_audio_${jobId}_${a.index}.wav`);
			await applyGlobalAtempoToWav(a.wav, out, globalAtempo);
			const d2 = await probeDurationSeconds(out);
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
		if (OUTRO_SMILE_TAIL_SEC > 0 && !expressionsNeeded.includes("warm"))
			expressionsNeeded.push("warm");
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

		// Smile tail after the outro line (silent + fade-out).
		const tailBaseline =
			pickBaselineVariant("warm", 2) || outroBaseline || baselineDefault;
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
					introLine,
					...script.segments.map((s) => s.text),
					outroLine,
				]
					.filter(Boolean)
					.join("\n");
				const durationValue = Math.round(contentTargetSec || 0);
				const allowedDurations = new Set([
					5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90,
					120, 180, 240, 300,
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
