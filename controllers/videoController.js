/** @format */
/*  videoController.js  — high‑motion, trends‑driven edition (enhanced)
 *  ✅ Uses Google Trends images per segment (non‑redundant where possible)
 *  ✅ Local high‑quality preprocessing for all seed images (optional Real‑ESRGAN super‑resolution)
 *  ✅ Cloudinary normalises aspect ratio & upscales/enhances images before Runway
 *  ✅ Runway image‑to‑video as the primary path; safety-only fallback to original static image
 *  ✅ Static fallback now uses best‑quality source (original URL first) + high‑quality lanczos scaling
 *  ✅ Runway clips always ≥ segment duration (no big freeze‑frame padding)
 *  ✅ OpenAI plans narration + visuals dynamically from Trends + article links
 *  ✅ Prompts emphasise clear, human‑like motion in every segment
 *  ✅ ElevenLabs voice picked dynamically via /voices + GPT, with American accent for English
 *  ✅ NEW: Orchestrator avoids reusing the last ElevenLabs voice for the same user when possible
 *  ✅ NEW: Voice planning nudged towards clear, motivated, brisk American‑style delivery (non‑sensitive topics)
 *  ✅ Background music planned via GPT (search term + voice/music gains) & metadata saved on Video
 *  ✅ Script timing recomputed from words → far fewer long pauses
 *  ✅ NEW: GPT retargets the script so natural spoken time lands within (duration - 2) .. duration seconds
 *  ✅ Phases kept in sync with GenerationModal (INIT → … → COMPLETED / ERROR)
 */

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

/* ───────────────────────────────────────────────────────────────
 *  Mongoose models & shared utils
 * ───────────────────────────────────────────────────────────── */
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

/* ───────────────────────────────────────────────────────────────
 *  Runtime guards + ffmpeg bootstrap
 * ───────────────────────────────────────────────────────────── */
function assertExists(cond, msg) {
	if (!cond) {
		console.error(`[Startup] FATAL – ${msg}`);
		process.exit(1);
	}
}

/* ffmpeg / ffprobe discovery */
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
console.log(`[FFmpeg]   lavfi  → ${hasLavfi}`);

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

/* Optional Real‑ESRGAN (local AI super‑resolution for seed images) */
function resolveRealEsrganPath() {
	const p = process.env.REAL_ESRGAN_PATH;
	if (!p) return null;

	// 1) Confirm the file actually exists
	if (!fs.existsSync(p)) {
		console.warn(
			"[RealESRGAN] REAL_ESRGAN_PATH is set but file does not exist →",
			p
		);
		return null;
	}

	// 2) Try to launch it once with "-h".
	//    We treat ANY exit code as "OK" as long as it starts without ENOENT.
	try {
		const res = child_process.spawnSync(p, ["-h"], {
			windowsHide: true,
			stdio: "ignore",
		});

		if (res.error && res.error.code === "ENOENT") {
			console.warn(
				"[RealESRGAN] REAL_ESRGAN_PATH is set but executable not found →",
				res.error.message
			);
			return null;
		}

		// If we get here, the process started successfully.
		return p;
	} catch (e) {
		console.warn(
			"[RealESRGAN] REAL_ESRGAN_PATH is set but binary failed to launch →",
			e.message
		);
		return null;
	}
}

const REAL_ESRGAN_PATH = resolveRealEsrganPath();
const HAS_REAL_ESRGAN = !!REAL_ESRGAN_PATH;
console.log(
	`[RealESRGAN] ${
		HAS_REAL_ESRGAN ? `enabled at ${REAL_ESRGAN_PATH}` : "not configured"
	}`
);

/* ───────────────────────────────────────────────────────────────
 *  Global constants
 * ───────────────────────────────────────────────────────────── */
const RUNWAY_VERSION = "2024-11-06";
const POLL_INTERVAL_MS = 2000;
const MAX_POLL_ATTEMPTS = 90;

const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });
const JAMENDO_ID = process.env.JAMENDO_CLIENT_ID;
const RUNWAY_ADMIN_KEY = process.env.RUNWAYML_API_SECRET;
const ELEVEN_API_KEY = process.env.ELEVENLABS_API_KEY;

const VALID_RATIOS = [
	"1280:720",
	"720:1280",
	"1104:832",
	"832:1104",
	"960:960",
	"1584:672",
];

/**
 * WORDS_PER_SEC: conservative cap used when asking GPT for max words.
 * NATURAL_WPS: more realistic speed used when recomputing durations from script.
 */
const WORDS_PER_SEC = 2.35;
const NATURAL_WPS = 2.45;

const MAX_SILENCE_PAD = 0.35;
const MIN_ATEMPO = 0.9;
const MAX_ATEMPO = 1.18;

/* Gen‑4 Turbo everywhere for speed + fidelity */
const T2V_MODEL = "gen4_turbo";
const ITV_MODEL = "gen4_turbo";
const TTI_MODEL = "gen4_image";

const QUALITY_BONUS =
	"photorealistic, ultra‑detailed, HDR, 8K, cinema lighting, award‑winning, cinematic camera movement, smooth parallax, subtle subject motion, emotional body language";

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
	"awkward pose",
	"mismatched gaze",
	"crossed eyes",
	"wall‑eyed",
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
	"anatomically correct, natural human faces, one natural‑looking head, two eyes, normal limbs, realistic body proportions, natural head position, natural skin texture, sharp and in‑focus facial features, no distortion, no warping, no blurring";

const BRAND_ENHANCEMENT_HINT =
	"subtle global brightness and contrast boost, slightly brighter and clearer faces while preserving natural skin tones, consistent AiVideomatic brand color grading";

const CHAT_MODEL = "gpt-5.1";

const ELEVEN_VOICES = {
	English: "21m00Tcm4TlvDq8ikWAM",
	العربية: "CYw3kZ02Hs0563khs1Fj",
	Français: "gqjD3Awy6ZnJf2el9DnG",
	Deutsch: "IFHEeWG1IGkfXpxmB1vN",
	हिंदी: "ykoxtvL6VZTyas23mE9F",
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
	Sports: "Use an energetic, but professional broadcast tone.",
	Politics:
		"Maintain an authoritative yet neutral tone, like a high‑end documentary voiceover.",
	Finance: "Speak in a confident, analytical tone.",
	Entertainment: "Keep it upbeat and engaging.",
	Technology: "Adopt a forward‑looking, curious tone.",
	Health: "Stay reassuring and informative.",
	Lifestyle: "Be friendly and encouraging.",
	Science: "Convey wonder and clarity.",
	World: "Maintain an objective, international outlook.",
	Top5: "Keep each item snappy and clearly ranked.",
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

const BRAND_TAG = "AiVideomatic";
const BRAND_CREDIT = "Powered by AiVideomatic";

const PROMPT_CHAR_LIMIT = 220;

/* ───────────────────────────────────────────────────────────────
 *  Small helpers
 * ───────────────────────────────────────────────────────────── */
const norm = (p) => (p ? p.replace(/\\/g, "/") : p);
const choose = (a) => a[Math.floor(Math.random() * a.length)];

// remove surrounding markdown code fence if present, without using literal ```
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

/** Download image from URL to a temp file (used for Trends and static clips) */
async function downloadImageToTemp(url, ext = ".jpg") {
	const tmp = tmpFile("trend_raw", ext);
	const writer = fs.createWriteStream(tmp);
	const resp = await axios.get(url, { responseType: "stream" });
	await new Promise((resolve, reject) => {
		resp.data.pipe(writer).on("finish", resolve).on("error", reject);
	});
	return tmp;
}

function spokenSeconds(words) {
	return +(words / WORDS_PER_SEC).toFixed(2);
}

/* ----------  TITLE HELPERS  ---------- */
function toTitleCase(str = "") {
	return str
		.toLowerCase()
		.replace(/(^\w|\s\w)/g, (m) => m.toUpperCase())
		.trim();
}

function fallbackSeoTitle(topic, category) {
	const base = toTitleCase(topic || "Breaking Update");
	if (category === "Top5") return `${base} | Top 5`;
	if (category === "Sports") return `${base} | Highlights & Preview`;
	return `${base} | Update`;
}

/* smoother numbers for TTS */
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
	let openaiSpeed = 1.06;

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
			openaiSpeed = 1.12;
		} else {
			style = Math.min(1, baseStyle + 0.15);
			stability = 0.17;
			openaiSpeed = 1.08;
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

/* simple word counter */
function countWordsInText(text) {
	return String(text || "")
		.trim()
		.split(/\s+/)
		.filter(Boolean).length;
}

/* Recompute segment durations from final script to avoid long pauses */
function recomputeSegmentDurationsFromScript(segments, targetTotalSeconds) {
	if (
		!Array.isArray(segments) ||
		!segments.length ||
		!targetTotalSeconds ||
		!Number.isFinite(targetTotalSeconds)
	)
		return null;

	const MIN_SEGMENT_SECONDS = 2.5;

	const est = segments.map((s, idx) => {
		const words = countWordsInText(s.scriptText || "");
		const basePause = idx === segments.length - 1 ? 0.35 : 0.25;
		const raw = (words || 1) / NATURAL_WPS + basePause;
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
			sd = candidate;
			scaled[idx] = sd;
			diff -= step;
		}
		idx--;
		if (idx < 0 && Math.abs(diff) > 0.05) idx = scaled.length - 1;
	}

	return scaled.map((v) => +v.toFixed(2));
}

/* Retarget whole script so natural spoken time ≈ requested duration */
async function retargetScriptToDuration(
	segments,
	targetTotalSeconds,
	segWordCaps,
	category,
	language
) {
	if (
		!Array.isArray(segments) ||
		!segments.length ||
		!targetTotalSeconds ||
		!Number.isFinite(targetTotalSeconds)
	) {
		return segments;
	}

	const wordCounts = segments.map((s) => countWordsInText(s.scriptText));
	const totalWords = wordCounts.reduce((a, b) => a + b, 0);
	if (!totalWords) return segments;

	const estSeconds = totalWords / NATURAL_WPS;
	const minSeconds = Math.max(5, targetTotalSeconds - 2);
	const maxSeconds = targetTotalSeconds;

	if (estSeconds >= minSeconds && estSeconds <= maxSeconds) {
		return segments;
	}

	const minWords = Math.floor(minSeconds * NATURAL_WPS);
	const maxWords = Math.floor(maxSeconds * NATURAL_WPS);
	const targetWords = Math.round((minWords + maxWords) / 2);

	const caps =
		Array.isArray(segWordCaps) && segWordCaps.length === segments.length
			? segWordCaps.map((c) =>
					Number.isFinite(c) && c > 0 ? Math.floor(c) : null
			  )
			: segments.map(() => null);

	const payloadSegments = segments.map((s, idx) => ({
		index: typeof s.index === "number" ? s.index : idx + 1,
		scriptText: String(s.scriptText || "").trim(),
	}));

	const toneHint = TONE_HINTS[category] || "";
	const ask = `
You are editing the narration for a short-form ${category} video.

The script is split into ${
		segments.length
	} segments, which will be read aloud at about ${NATURAL_WPS.toFixed(
		2
	)} words per second.

Current script:
- Approximate total words: ${totalWords}
- Estimated reading time: ~${estSeconds.toFixed(1)} seconds.

Target:
- Video length: ${targetTotalSeconds} seconds.
- Desired reading time window: ${minSeconds.toFixed(1)}–${maxSeconds.toFixed(
		1
	)} seconds.
- So the total word count across ALL segments should be between ${minWords} and ${maxWords} words (ideally near ${targetWords}).

Constraints:
- Keep the same number of segments and the same order.
- Preserve all important facts and logical flow, but you may compress or expand phrasing.
- Each segment must NOT exceed its max word cap from this array (treat these as hard limits):

wordCaps = [${caps.map((c) => (c === null ? "null" : c)).join(", ")}]

Additional style:
${toneHint ? `- ${toneHint}` : ""}
- All narration must remain in ${language}.
- Segment 1 must remain a strong hook.
- The final segment must still feel like a natural ending.

Return ONLY JSON with this exact shape:
{
  "segments": [
    { "index": 1, "scriptText": "..." },
    ...
  ]
}

Here are the current segments as JSON:
${JSON.stringify(payloadSegments).slice(0, 12000)}
`.trim();

	for (let attempt = 1; attempt <= 2; attempt++) {
		try {
			const { choices } = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: ask }],
			});
			const raw = strip(choices[0].message.content);
			const parsed = JSON.parse(raw);
			if (!parsed || !Array.isArray(parsed.segments)) continue;

			const incoming = parsed.segments;
			if (incoming.length !== segments.length) continue;

			const merged = segments.map((s, idx) => ({
				...s,
				scriptText: String(
					incoming[idx]?.scriptText || s.scriptText || ""
				).trim(),
			}));

			const newWordsTotal = merged.reduce(
				(acc, seg) => acc + countWordsInText(seg.scriptText),
				0
			);
			const newEstSeconds = newWordsTotal / NATURAL_WPS;
			console.log("[Timing] retargetScriptToDuration result", {
				oldWords: totalWords,
				newWords: newWordsTotal,
				oldSecs: estSeconds.toFixed(2),
				newSecs: newEstSeconds.toFixed(2),
			});
			return merged;
		} catch (e) {
			console.warn(
				`[Timing] retargetScriptToDuration attempt ${attempt} failed →`,
				e.message
			);
		}
	}

	return segments;
}

/* ───────────────────────────────────────────────────────────────
 *  Cloudinary + resolution helpers
 * ───────────────────────────────────────────────────────────── */
function ratioToCloudinaryAspect(ratio) {
	switch (ratio) {
		case "1280:720":
		case "1584:672":
			return "16:9";
		case "720:1280":
		case "832:1104":
			return "9:16";
		case "960:960":
			return "1:1";
		case "1104:832":
			return "4:3";
		default:
			return "16:9";
	}
}

/** Target display resolution per ratio – avoids tiny / stretched frames */
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

/** Cloudinary transform tuned for crisp Runway seeds */
function buildCloudinaryTransformForRatio(ratio) {
	const aspect = ratioToCloudinaryAspect(ratio);
	const { width, height } = targetResolutionForRatio(ratio);

	const base = {
		crop: "fill",
		gravity: "auto",
		quality: "auto:best",
		fetch_format: "auto",
		dpr: "auto",
	};
	if (width && height) {
		base.width = width;
		base.height = height;
	} else {
		base.aspect_ratio = aspect;
	}

	return [
		base,
		{ effect: "upscale" },
		{ effect: "improve" },
		{ effect: "sharpen:80" },
	];
}

/* ───────────────────────────────────────────────────────────────
 *  ffmpeg helpers
 * ───────────────────────────────────────────────────────────── */
function ffmpegPromise(cfg) {
	return new Promise((res, rej) => {
		const p = cfg(ffmpeg()) || ffmpeg();
		p.on("start", (cmd) => console.log(`[FFmpeg] ${cmd}`))
			.on("end", () => res())
			.on("error", (e) => rej(e));
	});
}

/** local super‑resolution using Real‑ESRGAN if available */
async function maybeUpscaleWithRealEsrgan(srcPath, ratio) {
	if (!HAS_REAL_ESRGAN || !srcPath) return srcPath;

	try {
		const meta = await new Promise((resolve, reject) =>
			ffmpeg.ffprobe(srcPath, (err, data) =>
				err ? reject(err) : resolve(data)
			)
		);
		const stream = Array.isArray(meta.streams)
			? meta.streams.find((s) => s.width && s.height)
			: null;
		const inW = stream?.width;
		const inH = stream?.height;
		if (!inW || !inH) return srcPath;

		const targetRes = targetResolutionForRatio(ratio);
		const shortTarget = Math.min(
			targetRes?.width || inW,
			targetRes?.height || inH
		);
		const shortIn = Math.min(inW, inH);

		if (!shortTarget || shortIn >= shortTarget * 0.9) {
			return srcPath;
		}

		const rawFactor = shortTarget / shortIn;
		let scaleFactor = rawFactor <= 2.3 ? 2 : 4;
		if (scaleFactor <= 1.2) return srcPath;
		if (scaleFactor > 4) scaleFactor = 4;

		const outPath = tmpFile("esr_up", path.extname(srcPath) || ".png");
		console.log(
			`[RealESRGAN] Upscaling seed from ${inW}x${inH} by x${scaleFactor}`
		);

		await new Promise((resolve, reject) => {
			const args = ["-i", srcPath, "-o", outPath, "-s", String(scaleFactor)];
			const child = child_process.spawn(REAL_ESRGAN_PATH, args, {
				stdio: "ignore",
			});
			child.on("error", reject);
			child.on("close", (code) => {
				if (code === 0) resolve();
				else reject(new Error(`RealESRGAN exited with non‑zero code ${code}`));
			});
		});

		return outPath;
	} catch (e) {
		console.warn("[RealESRGAN] upscale failed →", e.message);
		return srcPath;
	}
}

/**
 * Prepare a still image for use as a high‑quality video seed:
 * download → (optional Real‑ESRGAN) → scale/crop → subtle sharpen/contrast.
 * Returns a local file path.
 */
async function prepareImageForVideo(url, ratio, tag = "img") {
	const rawPath = await downloadImageToTemp(url, ".jpg");
	let workPath = rawPath;
	let scaledPath = null;

	try {
		const maybeUpscaled = await maybeUpscaleWithRealEsrgan(workPath, ratio);
		if (maybeUpscaled && maybeUpscaled !== workPath) {
			workPath = maybeUpscaled;
		}

		const { width, height } = targetResolutionForRatio(ratio);
		if (width && height) {
			scaledPath = tmpFile(`${tag}_prep`, ".jpg");
			const vf = [
				`scale=${width}:${height}:force_original_aspect_ratio=increase:flags=lanczos+accurate_rnd+full_chroma_int`,
				`crop=${width}:${height}`,
				"unsharp=lx=5:ly=5:la=0.75:cx=3:cy=3:ca=0.45",
				"eq=contrast=1.06:saturation=1.04:gamma=0.99",
			];

			await ffmpegPromise((c) =>
				c
					.input(norm(workPath))
					.videoFilters(vf.join(","))
					.outputOptions("-frames:v", "1", "-y")
					.save(norm(scaledPath))
			);
		}
	} catch (e) {
		console.warn("[ImagePrep] preprocessing failed →", e.message);
	}

	const outPath = scaledPath || workPath;
	if (outPath !== rawPath) {
		try {
			if (workPath && workPath !== outPath && workPath !== rawPath) {
				fs.unlinkSync(workPath);
			}
		} catch (_) {}
		try {
			fs.unlinkSync(rawPath);
		} catch (_) {}
	}
	return outPath;
}

/**
 * exactLen – normalises video clips:
 * - optional upscale/crop to canonical resolution per ratio
 * - optional light sharpen/contrast
 * - enforce or pad duration with tpad
 */
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
				vf.push(`tpad=stop_duration=${diff.toFixed(3)}`);
			}
		}

		if (enhance) {
			vf.push(
				"unsharp=lx=5:ly=5:la=0.9:cx=3:cy=3:ca=0.45",
				"eq=contrast=1.06:saturation=1.04:gamma=0.99"
			);
		}

		if (vf.length) {
			cmd.videoFilters(vf.join(","));
		}

		const preset = enhance ? "slow" : "veryfast";
		const crf = enhance ? 17 : 18;

		return cmd
			.outputOptions(
				"-c:v",
				"libx264",
				"-preset",
				preset,
				"-crf",
				String(crf),
				"-pix_fmt",
				"yuv420p",
				"-y"
			)
			.save(norm(out));
	});
}

/**
 * exactLenAudio – keeps audio tightly aligned with target while avoiding big silent gaps.
 */
async function exactLenAudio(src, target, out) {
	const meta = await new Promise((resolve, reject) =>
		ffmpeg.ffprobe(src, (err, data) => (err ? reject(err) : resolve(data)))
	);

	const inDur = meta.format?.duration || target;
	const diff = +(target - inDur).toFixed(3);

	await ffmpegPromise((cmd) => {
		cmd.input(norm(src));

		const filters = [];

		if (Math.abs(diff) <= 0.12) {
		} else if (diff < -0.12) {
			const ratio = inDur / target;

			if (ratio <= 2.0) {
				let tempo = ratio;
				if (tempo > MAX_ATEMPO) tempo = MAX_ATEMPO;
				if (tempo < MIN_ATEMPO) tempo = MIN_ATEMPO;
				filters.push(`atempo=${tempo.toFixed(3)}`);
			} else if (ratio <= 4.0) {
				const r = Math.min(MAX_ATEMPO, Math.sqrt(ratio));
				filters.push(`atempo=${r.toFixed(3)},atempo=${r.toFixed(3)}`);
			} else {
				cmd.outputOptions("-t", String(target));
			}
		} else {
			const padDur = Math.min(MAX_SILENCE_PAD, diff);
			if (padDur > 0.05) {
				filters.push(`apad=pad_dur=${padDur.toFixed(3)}`);
			}
		}

		if (filters.length) {
			cmd.audioFilters(filters.join(","));
		}

		return cmd.outputOptions("-y").save(norm(out));
	});
}

/* ───────────────────────────────────────────────────────────────
 *  Google Trends helpers & SEO title
 * ───────────────────────────────────────────────────────────── */
function resolveTrendsCategoryId(label) {
	const e = googleTrendingCategoriesId.find((c) => c.category === label);
	return e ? e.ids[0] : 0;
}

const TRENDS_API_URL =
	process.env.TRENDS_API_URL || "http://localhost:8102/api/google-trends";

async function fetchTrendingStory(category, geo = "US") {
	const id = resolveTrendsCategoryId(category);
	const url =
		`${TRENDS_API_URL}?` + qs.stringify({ geo, category: id, hours: 168 });

	try {
		const { data } = await axios.get(url, { timeout: 12000 });
		const stories = Array.isArray(data?.stories) ? data.stories : [];
		if (!stories.length) throw new Error("empty trends payload");

		const s = stories[0];
		const articles = Array.isArray(s.articles) ? s.articles : [];

		const imgSet = new Set();
		if (s.image) imgSet.add(s.image);
		for (const a of articles) {
			if (a.image) imgSet.add(a.image);
		}

		const images = Array.from(imgSet);

		return {
			title: String(s.title || "").trim(),
			images,
			articles: articles.map((a) => ({
				title: String(a.title || "").trim(),
				url: a.url || null,
				image: a.image || null,
			})),
		};
	} catch (e) {
		console.warn("[Trending] fetch failed →", e.message);
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
				} catch (_) {}
			}
		}
		return null;
	}
}

async function scrapeArticleText(url) {
	if (!url) return null;
	try {
		const { data: html } = await axios.get(url, { timeout: 10000 });
		const $ = cheerio.load(html);
		const body = $("article").text() || $("body").text();
		const cleaned = body
			.replace(/\s+/g, " ")
			.replace(/(Advertisement|Subscribe now|Sign up for.*?newsletter).*/gi, "")
			.trim();
		return cleaned.slice(0, 12000) || null;
	} catch (e) {
		console.warn("[Scrape] article failed →", e.message);
		if (e.response) {
			console.warn("[Scrape] HTTP status:", e.response.status);
			if (e.response.data) {
				try {
					console.warn(
						"[Scrape] response data snippet:",
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

/* SEO title – official, search‑friendly */
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

Write ONE highly searchable, professional YouTube Shorts title.

Hard constraints:
- Maximum 65 characters.
- Title Case.
- No emojis.
- No hashtags.
- No quotation marks.
- No over-hyped or tabloid adjectives like "Insane", "Crazy", "Wild", "Lightning Burst".
- The style must feel ${
		isSports
			? "like ESPN or an official league/NFL/NBA channel, not a meme or fan channel."
			: "like a major newspaper or broadcaster, not a clickbait channel."
	}

SEO / search behaviour:
- Include the core subject or matchup once.
- Prefer phrases that match how people actually search, such as:
  ${
		isSports
			? '"Highlights", "Gameday Preview", "How To Watch", "Full Recap".'
			: '"Explained", "Update", "Analysis", "What To Know".'
	}
- You may use a short descriptor after a separator like "|" or "–"
  (for example: "Team A vs Team B | Gameday Preview").

Context from Google Trends and linked articles:
${context || "(no extra context)"}

${
	language !== DEFAULT_LANGUAGE
		? `Respond in ${language}, keeping any team or person names in their original language.`
		: ""
}

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
		console.warn("[SEO title] generation failed →", e.message);
		return "";
	}
}

/* ───────────────────────────────────────────────────────────────
 *  Topic helpers for non‑Trends / Top‑5 mode
 * ───────────────────────────────────────────────────────────── */
const CURRENT_MONTH_YEAR = dayjs().format("MMMM YYYY");
const CURRENT_YEAR = dayjs().year();

async function topicFromCustomPrompt(text) {
	const make = (a) =>
		`
Attempt ${a}:
Give one click‑worthy title (at most 70 characters, no hashtags, no quotes) set in ${CURRENT_MONTH_YEAR}.
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

/* Top‑5 outline helper */
async function generateTop5Outline(topic, language = DEFAULT_LANGUAGE) {
	const ask = `
Current date: ${dayjs().format("YYYY-MM-DD")}

You are planning a Top 5 countdown video.

Title: ${topic}

Return a strict JSON array of exactly 5 objects, one per rank from 5 down to 1.
Each object must have:
- "rank": a number 5, 4, 3, 2 or 1
- "label": a short name for the item (maximum 8 words)
- "oneLine": one punchy sentence (maximum 18 words) explaining why it deserves this rank.

Use real‑world facts and widely known names when appropriate, avoid speculation.
Keep everything in ${language}. Do not include any other keys or free‑text.
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
				`[GPT] Top‑5 outline attempt ${attempt} failed → ${err.message}`
			);
		}
	}
	return null;
}

/* ───────────────────────────────────────────────────────────────
 *  Cloudinary helpers for Trends images
 * ───────────────────────────────────────────────────────────── */
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

	const prepped = await prepareImageForVideo(url, ratio, "trend_seed");

	try {
		const result = await cloudinary.uploader.upload(prepped, {
			...baseOpts,
			transformation: transform,
		});
		console.log("[Cloudinary] Seed image uploaded →", {
			public_id: result.public_id,
			width: result.width,
			height: result.height,
			format: result.format,
		});
		return {
			public_id: result.public_id,
			url: result.secure_url,
		};
	} finally {
		try {
			fs.unlinkSync(prepped);
		} catch (_) {}
	}
}

/* ───────────────────────────────────────────────────────────────
 *  Runway poll + retry (hard‑fail on 4xx, with richer logging)
 * ───────────────────────────────────────────────────────────── */
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
			throw new Error(
				`${lbl} failed (Runway: ${data.failureCode || "FAILED"})`
			);
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
				} → ${e.message}`
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
			if (status && status >= 400 && status < 500 && status !== 429) break;
		}
	}
	throw last;
}

/* ───────────────────────────────────────────────────────────────
 *  Runway helpers for clips
 * ───────────────────────────────────────────────────────────── */
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

	const idVid = await retry(
		async () => {
			const { data } = await axios.post(
				"https://api.dev.runwayml.com/v1/image_to_video",
				{
					model: ITV_MODEL,
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
	const ttiLabel = `tti_seg${segmentIndex}`;
	const pollTtiLabel = `poll_tti_seg${segmentIndex}`;
	const itvLabel = `itv_from_tti_seg${segmentIndex}`;
	const pollItvLabel = `poll_itv_from_tti_seg${segmentIndex}`;

	const ttiId = await retry(
		async () => {
			const { data } = await axios.post(
				"https://api.dev.runwayml.com/v1/text_to_image",
				{
					model: TTI_MODEL,
					promptText,
					ratio,
					promptStrength: 0.9,
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
		ttiLabel
	);

	const imgUrl = await retry(
		() => pollRunway(ttiId, RUNWAY_ADMIN_KEY, pollTtiLabel),
		3,
		pollTtiLabel
	);

	const idVid = await retry(
		async () => {
			const { data } = await axios.post(
				"https://api.dev.runwayml.com/v1/image_to_video",
				{
					model: ITV_MODEL,
					promptImage: imgUrl,
					promptText,
					ratio,
					duration: runwayDuration,
					promptStrength: 0.85,
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
		() => pollRunway(idVid, RUNWAY_ADMIN_KEY, pollItvLabel),
		3,
		pollItvLabel
	);

	const p = tmpFile(`seg_tti_itv_${segmentIndex}`, ".mp4");
	await new Promise((r, j) =>
		axios
			.get(vidUrl, { responseType: "stream" })
			.then(({ data }) =>
				data.pipe(fs.createWriteStream(p)).on("finish", r).on("error", j)
			)
	);
	return p;
}

/**
 * generateStaticClipFromImage – NON‑AI fallback when Runway flags SAFETY / HumanSafety
 */
async function generateStaticClipFromImage({
	segmentIndex,
	imgUrlOriginal,
	imgUrlCloudinary,
	ratio,
	targetDuration,
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
			const localPath = await prepareImageForVideo(
				url,
				ratio,
				`static_${segmentIndex}`
			);
			const out = tmpFile(`seg_static_${segmentIndex}`, ".mp4");

			await ffmpegPromise((c) => {
				c.input(norm(localPath)).inputOptions("-loop", "1");

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

			try {
				fs.unlinkSync(localPath);
			} catch (_) {}

			console.log(
				`[Seg ${segmentIndex}] Static fallback clip created from ${url}`
			);

			return out;
		} catch (e) {
			lastErr = e;
			console.warn(
				`[Seg ${segmentIndex}] Static fallback failed for ${url} →`,
				e.message
			);
		}
	}

	throw lastErr || new Error("Failed to build static clip from any image URL");
}

/* ───────────────────────────────────────────────────────────────
 *  YouTube & Jamendo helpers
 * ───────────────────────────────────────────────────────────── */
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

/* Background music planning – GPT picks Jamendo search + gains */
async function planBackgroundMusic(category, language, script) {
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

Return JSON:
{
  "jamendoSearch": "one concise search term for Jamendo, including genre and mood, must imply no vocals",
  "fallbackSearchTerms": ["term1", "term2"],
  "voiceGain": 1.4,
  "musicGain": 0.14
}

Constraints:
- "fallbackSearchTerms" must be an array of exactly 2 short strings.
- "voiceGain" between 1.2 and 1.7 (voice louder).
- "musicGain" between 0.08 and 0.22 (music softer).
- Use English for search terms so Jamendo search works well, even if the narration language is different.
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
		console.warn("[MusicPlan] planning failed →", e.message);
		return null;
	}
}

/* ───────────────────────────────────────────────────────────────
 *  ElevenLabs helpers – dynamic voice selection + TTS
 * ───────────────────────────────────────────────────────────── */
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
		console.warn("[Eleven] fetch voices failed →", e.message);
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
- Pacing should feel clear and slightly brisk, not sleepy or dragged out, unless the topic is clearly tragic or sensitive.

${
	language === "English"
		? "- IMPORTANT: Only select a voice with a clearly American / US English accent.\n- Do NOT pick British, Australian or other non-US accents."
		: ""
}

${avoidText}

You are given a JSON array called "voices" with candidate voices from the ElevenLabs /voices API.
Pick ONE best "id" to use. Prefer:
- Narrator / storyteller voices over comedic, meme, or character voices.
- Voices that fit the language/accent when possible.
- Calmer, more neutral voices if the tone is sensitive.
- Otherwise, choose a clear, warm, motivated broadcast voice that sounds confident and engaging.

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
		console.warn("[Eleven] GPT voice selection failed →", e.message);
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

/* ───────────────────────────────────────────────────────────────
 *  OpenAI “director” – build full video plan (segments + visuals)
 * ─────────────────────────────────────────────────────────────── */
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
	const segDescLines = segLens
		.map(
			(sec, i) =>
				`Segment ${i + 1}: ~${sec.toFixed(1)}s, ≤ ${
					segWordCaps[i]
				} spoken words.`
		)
		.join("\n");

	const categoryTone = TONE_HINTS[category] || "";

	const baseIntro = `
Current date: ${dayjs().format("YYYY-MM-DD")}

You are an expert short‑form video editor and sports/news producer.

We need a ${duration}s ${category} YouTube Shorts video titled "${topic}",
split into ${segCnt} sequential segments.

Segment timing:
${segDescLines}

Narration rules (for all segments):
- Use natural spoken language, like a professional commentator.
- Stay accurate to the real‑world topic. Do NOT invent fake scores, injuries, or quotes.
- Avoid generic filler like "In this video", "Smash that like button", or "subscribe".
- Segment 1 must hook the viewer immediately.
- Later segments should deepen the context (stakes, key players, what to watch, etc.).
- Keep within the word caps above so that the voice‑over can fit the timing.
- All narration MUST be in ${language}.
- For non‑tragic topics, pacing should feel clear and slightly brisk, like a motivated American TV host, without awkward long pauses.
- For clearly tragic or sensitive stories, slow the pacing slightly but keep it clear and respectful.
${categoryTone ? `- Tone: ${categoryTone}` : ""}
`.trim();

	let promptText;
	if (hasImages) {
		const imgCount = images.length;
		promptText = `
${baseIntro}

You also have ${imgCount} REAL photos from Google Trends for this story.

Google Trends context:
- Story title: ${trendStory?.title || topic}
- Article headlines (for factual grounding):
  ${articleTitles.map((t) => `- ${t}`).join("\n  ") || "- (none)"}

Article text snippet (may be truncated, use for facts only):
${snippet || "(no article text available)"}

Images:
I have attached the ${imgCount} images to this message, in order.
The FIRST attached image is imageIndex 0, the second is 1, etc.
The video engine will receive an upscaled, cropped version of these photos (via Cloudinary),
but it is still the exact same real shot and the same real people.
Minor global adjustments such as a subtle brightness/contrast lift and slightly brighter faces
for a consistent brand look are allowed, but the underlying people and setting must remain identical.

Your job:
1) Write the voice‑over script for each segment.
2) Decide which imageIndex to animate for each segment.
3) For each segment, write one concise "runwayPrompt" telling a video model how to animate THAT exact real photo, as if you are a professional video director.
4) For each segment, also write a "negativePrompt" listing visual problems that the video model must avoid for that shot.

Critical policy and visual rules:
- The video model ALREADY SEES the real Google Trends photo. The "runwayPrompt" is ONLY there to describe camera motion and subtle, realistic movement in the existing scene.
- Do NOT change who the people are. Do not add new people that are not implied by the original image.
- Do NOT change the basic setting, stadium, or composition in a drastic way. Subtle global color/brightness changes and slightly brighter faces for branding are fine, but the scene must still clearly look like the same photo.
- NEVER mention any real person names, team names, club names, jersey numbers, or brand names in "runwayPrompt". Names are fine in the narration, but NOT in the visual prompt.
- When referring to people or teams in "runwayPrompt", use generic roles such as "the head coach", "star quarterback", "home team in red jerseys", "away team in white jerseys", "fans in the crowd".
- EVERY segment must have clear, visible motion: no "still photograph" look.
- Use camera movement (slow zoom, dolly, pan, tilt) AND/OR subject motion (players walking, crowd cheering, lights flickering, flags waving).
- Use each imageIndex at most once before reusing any image.
- Keep the subject sharp and in focus. Avoid soft focus, heavy motion blur, or smeared details.
- Faces must remain human and natural: no distortion, no duplicated or missing facial features, no melted or blurry faces.
- Frame as if the video will be watched full‑screen on a phone: main subject large and centered, never tiny in the distance.
- Never morph faces into different people.
- No surreal or abstract effects.

For each "runwayPrompt":
- Describe motion that is consistent with what you actually see in that attached photo.
- Explicitly mention at least ONE motion verb, such as "slow zoom in", "camera pans left", "coach glances toward the field", "fans wave towels", "stadium lights flicker".
- Do NOT ask for still images; it must describe a short moving shot.
- Do NOT restate the news headline or player names; focus purely on what the camera sees and how it moves.

For each "negativePrompt":
- Explicitly list all visual defects the model must avoid, especially those that make people look "unhuman":
  extra limbs, extra heads, missing limbs, mutated or fused fingers, distorted anatomy, broken or twisted joints,
  twisted necks, wall‑eyed or crossed‑eyed gazes, melted or blurry faces, stretched or glitched bodies.
- Also include technical artifacts to avoid: lowres, pixelated, blur, out of focus, soft focus, heavy motion blur,
  overexposed, underexposed, watermark, logo, text overlay, frozen frame, static frame, no motion, surreal or abstract effects, gore, nsfw content.
- Keep it a single comma‑separated string tailored for that segment (you may reuse core phrases across segments).

Return a single JSON object with this exact shape:

{
  "segments": [
    {
      "index": 1,
      "scriptText": "spoken narration for segment 1",
      "imageIndex": 0,
      "runwayPrompt": "how to animate the first attached photo",
      "negativePrompt": "comma‑separated list of things to avoid for this shot"
    }
    // exactly ${segCnt} segments, index 1..${segCnt}
  ]
}

No extra commentary.
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

This is a Top 5 style countdown. Use this outline:

${outlineText}

Rules:
- Segment 1 should tease the countdown and hook the viewer.
- Segments 2–6 must correspond to ranks #5, #4, #3, #2, and #1 respectively.
- Each of those segments MUST start with "#5:", "#4:", "#3:", "#2:" or "#1:" followed by the item label.
- After the rank label, you may add one concise sentence explaining why that item deserves this rank.
- No images are provided, so you must imagine visuals.

For each segment, output:
- "index"
- "scriptText"
- "runwayPrompt": a vivid but grounded description of the scene to generate from scratch. Focus on symbols (arenas, trophies, jerseys), not specific copyrighted logos.
- "negativePrompt": a comma‑separated list of visual defects to avoid for that scene.

Visual rules:
- Keep scenes realistic and grounded in today's world.
- Keep the focal subject large and sharp; avoid tiny subjects, chaotic camera moves and heavy motion blur.
- Avoid specific trademarks or team logos; describe them generically (for example "home team in dark jerseys").
- If people are visible, keep faces natural and undistorted.
- EVERY runwayPrompt must describe clear motion (camera or subject), never a static pose.
- NEVER mention real person names, team names, club names, jersey numbers, or brand names in "runwayPrompt". Use generic roles such as "star player", "coach", "home team", "away team".
- Faces must remain human and natural: no extra or missing limbs, no distorted anatomy, no melted or glitched faces.

For each "negativePrompt":
- Explicitly list human‑anatomy defects and artifacts to avoid (extra limbs, extra heads, mutated or fused fingers, broken joints, twisted necks, distorted faces, etc.).
- Also include lowres, pixelated, blur, out of focus, soft focus, heavy motion blur, overexposed, underexposed, watermark, logo, text overlay, static frame, no motion, gore, nsfw.
- Keep it as one comma‑separated string.

Return JSON of the form:
{
  "segments": [
    { "index": 1, "scriptText": "...", "runwayPrompt": "...", "negativePrompt": "..." },
    ...
  ]
}
`.trim();
	} else {
		promptText = `
${baseIntro}

No reliable Google Trends images are available for this topic.

You must imagine the visuals from scratch. For each segment, output:
- "index"
- "scriptText"
- "runwayPrompt": a short scene description that a text‑to‑image model can turn into one keyframe.
- "negativePrompt": a comma‑separated list of visual defects to avoid for that scene.

Visual rules:
- Keep scenes realistic and grounded in today's world.
- Favour crisp, well‑lit compositions; avoid heavy motion blur or extreme camera moves.
- Avoid specific trademarks or team logos; describe them generically (for example "home team in dark jerseys").
- If people are visible, keep faces natural and undistorted.
- Prefer one clear focal subject per segment.
- EVERY runwayPrompt must include explicit motion: for example "camera slowly pushes in", "athlete jogs toward camera", "crowd claps and waves", "scoreboard lights pulse".
- Do NOT mention any real person names, team names, club names, jersey numbers, or brand names in "runwayPrompt". Use generic roles instead.
- Faces must remain human and natural: no extra or missing limbs, no distorted anatomy, no melted or glitched faces.

For each "negativePrompt":
- Explicitly list human‑anatomy defects and artifacts to avoid (extra limbs, extra heads, mutated or fused fingers, broken joints, twisted necks, distorted faces, etc.).
- Also include lowres, pixelated, blur, out of focus, soft focus, heavy motion blur, overexposed, underexposed, watermark, logo, text overlay, static frame, no motion, gore, nsfw.
- Keep it as one comma‑separated string.

Return JSON of the form:
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

	const segments = plan.segments.map((s, idx) => {
		const runwayPrompt = String(s.runwayPrompt || "").trim();
		const negativePromptRaw = String(s.negativePrompt || "").trim();

		const base = {
			index: typeof s.index === "number" ? s.index : idx + 1,
			scriptText: String(s.scriptText || "").trim(),
			runwayPrompt,
			runwayNegativePrompt: negativePromptRaw,
		};

		if (hasImages) {
			const imgIdxRaw = Number.isInteger(s.imageIndex) ? s.imageIndex : 0;
			const imgIdxSafe =
				imgIdxRaw >= 0 && imgIdxRaw < images.length ? imgIdxRaw : 0;
			return { ...base, imageIndex: imgIdxSafe };
		}
		return { ...base, imageIndex: null };
	});

	return { segments };
}

/* ───────────────────────────────────────────────────────────────
 *  Main controller – createVideo
 * ───────────────────────────────────────────────────────────── */
exports.createVideo = async (req, res) => {
	const { category, ratio: ratioIn, duration: durIn } = req.body;

	if (!category || !YT_CATEGORY_MAP[category])
		return res.status(400).json({ error: "Bad category" });
	if (!VALID_RATIOS.includes(ratioIn))
		return res.status(400).json({ error: "Bad ratio" });
	if (!goodDur(durIn)) return res.status(400).json({ error: "Bad duration" });

	const ratio = ratioIn;
	const duration = +durIn;

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
	console.log("[Phase] INIT → Starting pipeline");
	res.setTimeout(0);

	try {
		const {
			language: langIn,
			country: countryIn,
			customPrompt: customPromptRaw = "",
			videoImage,
			schedule,
			youtubeEmail,
		} = req.body;

		const user = req.user;
		const language = (langIn || DEFAULT_LANGUAGE).trim();
		const country =
			countryIn && countryIn.toLowerCase() !== "all countries"
				? countryIn.trim()
				: "US";
		const customPrompt = customPromptRaw.trim();

		console.log(
			`[Job] user=${user.email}  cat=${category}  dur=${duration}s  geo=${country}`
		);

		/* 1. Topic resolution & Trends */
		let topic = "";
		let trendStory = null;
		let trendArticleText = null;

		const userOverrides = Boolean(videoImage) || customPrompt.length > 0;

		if (!userOverrides && category !== "Top5") {
			trendStory = await fetchTrendingStory(category, country);
			if (trendStory && trendStory.title) {
				topic = trendStory.title;
				console.log(`[Trending] candidate topic="${topic}"`);
			}
		}

		if (topic) {
			const dup = await Video.findOne({
				user: user._id,
				category,
				topic,
			}).select("_id");
			if (dup) {
				console.warn("[Duplicate] topic already used – picking new one");
				topic = "";
				trendStory = null;
			}
		}

		if (customPrompt && !topic) {
			try {
				topic = await topicFromCustomPrompt(customPrompt);
			} catch {}
		}

		if (!topic) {
			if (category === "Top5") {
				const used = new Set(
					(
						await Video.find({ user: user._id, category: "Top5" }).select(
							"topic"
						)
					).map((v) => v.topic)
				);
				const remaining = ALL_TOP5_TOPICS.filter((t) => !used.has(t));
				topic = remaining.length ? remaining[0] : choose(ALL_TOP5_TOPICS);
			} else {
				const list = await pickTrendingTopicFresh(category, language, country);
				const used = new Set(
					(await Video.find({ user: user._id, category }).select("topic")).map(
						(v) => v.topic
					)
				);
				topic = list.find((t) => !used.has(t)) || list[0];
			}
		}

		console.log(`[Job] final topic="${topic}"`);

		if (trendStory && trendStory.articles && trendStory.articles.length) {
			trendArticleText = await scrapeArticleText(
				trendStory.articles[0].url || null
			);
		}

		/* 2. Segment timing */
		const INTRO = 3;
		let segCnt =
			category === "Top5" ? 6 : Math.ceil((duration - INTRO) / 10) + 1;

		let segLens;
		if (category === "Top5") {
			const r = duration - INTRO;
			const base = Math.floor(r / 5);
			const extra = r % 5;
			segLens = [
				INTRO,
				...Array.from({ length: 5 }, (_, i) => base + (i < extra ? 1 : 0)),
			];
		} else {
			const r = duration - INTRO;
			const n = Math.ceil(r / 10);
			segLens = [
				INTRO,
				...Array.from({ length: n }, (_, i) =>
					i === n - 1 ? r - 10 * (n - 1) : 10
				),
			];
		}

		const delta = duration - segLens.reduce((a, b) => a + b, 0);
		if (Math.abs(delta) >= 1) segLens[segLens.length - 1] += delta;

		const segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));
		console.log("[Timing] initial segment lengths", {
			segLens,
			segWordCaps,
		});

		/* 3. Top‑5 outline if needed */
		let top5Outline = null;
		if (category === "Top5") {
			top5Outline = await generateTop5Outline(topic, language);
		}

		/* 4. Upload Trends images to Cloudinary */
		let trendImagePairs = [];
		const canUseTrendsImages =
			category !== "Top5" &&
			!userOverrides &&
			trendStory &&
			Array.isArray(trendStory.images) &&
			trendStory.images.length > 0;

		if (canUseTrendsImages) {
			const slugBase = topic
				.toLowerCase()
				.replace(/[^\w]+/g, "_")
				.replace(/^_+|_+$/g, "")
				.slice(0, 40);
			for (let i = 0; i < trendStory.images.length; i++) {
				const url = trendStory.images[i];
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
					console.warn("[Cloudinary] upload failed →", e.message);
				}
			}
			if (!trendImagePairs.length) {
				console.warn(
					"[Cloudinary] All Trends uploads failed – falling back to prompt‑only mode"
				);
			}
		}

		const hasTrendImages = trendImagePairs.length > 0;

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
		});

		let segments = plan.segments;

		console.log("[GPT] buildVideoPlanWithGPT → plan ready", {
			segments: segments.length,
			hasImages: hasTrendImages,
		});

		// Per‑segment hard cap tightening
		await Promise.all(
			segments.map((s, i) =>
				countWordsInText(s.scriptText) <= segWordCaps[i]
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

		// Global script retargeting so natural spoken time ≈ duration
		try {
			const adjusted = await retargetScriptToDuration(
				segments,
				duration,
				segWordCaps,
				category,
				language
			);
			if (adjusted) {
				segments = adjusted;
			}
		} catch (e) {
			console.warn("[Timing] retargetScriptToDuration failed →", e.message);
		}

		const fullScript = segments.map((s) => s.scriptText.trim()).join(" ");

		// Recompute segment durations from final script
		const recomputed = recomputeSegmentDurationsFromScript(segments, duration);
		if (recomputed && recomputed.length === segLens.length) {
			console.log("[Timing] Recomputed segment durations from script:", {
				before: segLens,
				after: recomputed,
			});
			segLens = recomputed;
		}
		segCnt = segLens.length;

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
			console.warn("[GPT] global style generation failed →", e.message);
		}

		let seoTitle = "";
		try {
			const seedHeadlines =
				trendStory && trendStory.articles && trendStory.articles.length
					? trendStory.articles.map((a) => a.title).filter(Boolean)
					: [topic];
			const snippet = trendArticleText ? trendArticleText.slice(0, 800) : "";
			seoTitle = await generateSeoTitle(
				seedHeadlines,
				category,
				language,
				snippet
			);
		} catch (e) {
			console.warn("[SEO title] generation outer failed →", e.message);
		}
		if (!seoTitle) seoTitle = fallbackSeoTitle(topic, category);

		const descResp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [
				{
					role: "user",
					content: `Write a YouTube description (at most 150 words) for the video titled "${seoTitle}". End with 5–7 relevant hashtags.`,
				},
			],
		});
		const seoDescription = `${descResp.choices[0].message.content.trim()}\n\n${BRAND_CREDIT}`;

		let tags = ["shorts"];
		try {
			const tagResp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [
					{
						role: "user",
						content: `Return a JSON array of 5–8 tags for the YouTube video "${seoTitle}".`,
					},
				],
			});
			const parsed = JSON.parse(strip(tagResp.choices[0].message.content));
			if (Array.isArray(parsed)) tags.push(...parsed);
		} catch (e) {
			console.warn("[Tags] generation failed →", e.message);
		}
		if (category === "Top5") tags.unshift("Top5");
		if (!tags.includes(BRAND_TAG)) tags.unshift(BRAND_TAG);
		tags = [...new Set(tags)];

		/* 7. Load last ElevenLabs voice */
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
				"[TTS] Unable to load last ElevenLabs voice metadata →",
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
			console.warn("[TTS] Voice selection failed →", e.message);
		}

		/* 9. Optional background music */
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
			console.warn("[MusicPlan] planning failed →", e.message);
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
			console.warn("[Music] Jamendo failed →", e.message);
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

		/* 10. Per‑segment video generation */
		const clips = [];
		sendPhase("GENERATING_CLIPS", {
			msg: "Generating clips",
			total: segCnt,
			done: 0,
		});
		console.log("[Phase] GENERATING_CLIPS → Generating clips");

		for (let i = 0; i < segCnt; i++) {
			const d = segLens[i];
			const seg = segments[i];
			const segIndex = i + 1;

			const rw = d <= 5 ? 5 : 10;

			console.log(
				`[Seg ${segIndex}/${segCnt}] targetDuration=${d.toFixed(
					2
				)}s runwayDuration=${rw}s`
			);

			const promptBase = `${
				seg.runwayPrompt || ""
			}, ${globalStyle}, ${QUALITY_BONUS}, ${HUMAN_SAFETY}, ${BRAND_ENHANCEMENT_HINT}`;
			const promptText =
				promptBase.length > PROMPT_CHAR_LIMIT
					? promptBase.slice(0, PROMPT_CHAR_LIMIT)
					: promptBase;

			const negativeBase =
				seg.runwayNegativePrompt && seg.runwayNegativePrompt.trim().length
					? seg.runwayNegativePrompt.trim()
					: RUNWAY_NEGATIVE_PROMPT;
			const negativePrompt =
				negativeBase.length > PROMPT_CHAR_LIMIT
					? negativeBase.slice(0, PROMPT_CHAR_LIMIT)
					: negativeBase;

			let clipPath = null;

			if (hasTrendImages && seg.imageIndex !== null) {
				const pair =
					trendImagePairs[seg.imageIndex] || trendImagePairs[0] || null;
				const imgUrlCloudinary = pair?.cloudinaryUrl;
				const imgUrlOriginal = pair?.originalUrl || imgUrlCloudinary;

				if (!imgUrlCloudinary)
					throw new Error("No Cloudinary Trends image available for Runway");

				console.log("[Runway] prompt preview", {
					segment: segIndex,
					promptPreview: promptText.slice(0, 160),
					hasTrendImage: true,
				});

				try {
					clipPath = await generateItvClipFromImage({
						segmentIndex: segIndex,
						imgUrl: imgUrlCloudinary,
						promptText,
						negativePrompt,
						ratio,
						runwayDuration: rw,
					});
				} catch (e) {
					const msg = String(e?.message || "");
					const failureCode =
						e?.response?.data?.failureCode || e?.response?.data?.code || "";
					const isSafety =
						/SAFETY/i.test(msg) ||
						/SAFETY/i.test(String(failureCode || "")) ||
						/HUMAN/i.test(String(failureCode || ""));

					console.error(
						`[Seg ${segIndex}] Runway image_to_video failed with Trends image →`,
						msg,
						failureCode ? `(${failureCode})` : ""
					);

					if (isSafety || imgUrlOriginal || imgUrlCloudinary) {
						console.warn(
							`[Seg ${segIndex}] Falling back to highest‑quality static image due to Runway failure${
								isSafety ? " (safety-related)." : "."
							}`
						);
						clipPath = await generateStaticClipFromImage({
							segmentIndex: segIndex,
							imgUrlOriginal,
							imgUrlCloudinary,
							ratio,
							targetDuration: d,
						});
					} else {
						throw e;
					}
				}
			} else {
				console.log("[Runway] prompt preview", {
					segment: segIndex,
					promptPreview: promptText.slice(0, 160),
					hasTrendImage: false,
				});
				clipPath = await generateTtiItvClip({
					segmentIndex: segIndex,
					promptText,
					negativePrompt,
					ratio,
					runwayDuration: rw,
				});
			}

			const fixed = tmpFile(`fx_${segIndex}`, ".mp4");
			await exactLen(clipPath, d, fixed, { ratio, enhance: true });
			try {
				fs.unlinkSync(clipPath);
			} catch {}

			clips.push(fixed);

			sendPhase("GENERATING_CLIPS", {
				msg: `Rendering segment ${segIndex}/${segCnt}`,
				total: segCnt,
				done: segIndex,
			});
			console.log("[Phase] GENERATING_CLIPS → Rendering segment", segIndex);
		}

		/* 11. Concatenate silent video */
		sendPhase("ASSEMBLING_VIDEO", { msg: "Concatenating clips…" });
		console.log("[Phase] ASSEMBLING_VIDEO → Concatenating clips");

		const listFile = tmpFile("list", ".txt");
		fs.writeFileSync(
			listFile,
			clips.map((p) => `file '${norm(p)}'`).join("\n")
		);

		const silent = tmpFile("silent", ".mp4");
		await ffmpegPromise((c) =>
			c
				.input(norm(listFile))
				.inputOptions("-f", "concat", "-safe", "0")
				.outputOptions("-c", "copy", "-y")
				.save(norm(silent))
		);
		fs.unlinkSync(listFile);
		clips.forEach((p) => {
			try {
				fs.unlinkSync(p);
			} catch {}
		});

		const silentFixed = tmpFile("silent_fix", ".mp4");
		await exactLen(silent, duration, silentFixed, {
			ratio,
			enhance: false,
		});
		try {
			fs.unlinkSync(silent);
		} catch {}

		/* 12. Voice‑over & music */
		sendPhase("ADDING_VOICE_MUSIC", { msg: "Creating audio layer" });
		console.log("[Phase] ADDING_VOICE_MUSIC → Creating audio layer");

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
					`[TTS] ElevenLabs failed for seg ${i + 1}, falling back to OpenAI →`,
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

			await exactLenAudio(raw, segLens[i], fixed);
			try {
				fs.unlinkSync(raw);
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
					.outputOptions("-t", String(duration), "-y")
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
						`
						[0:a]volume=${voiceGain.toFixed(3)}[a0]`,
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

		await exactLenAudio(mixedRaw, duration, mixed);
		try {
			fs.unlinkSync(mixedRaw);
		} catch {}

		/* 13. Mux audio + video */
		sendPhase("SYNCING_VOICE_MUSIC", { msg: "Muxing final video" });
		console.log("[Phase] SYNCING_VOICE_MUSIC → Muxing final video");

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
					String(duration),
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

		/* 14. YouTube upload (best‑effort) */
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
			console.warn("[YouTube] upload skipped →", e.message);
		}

		/* 15. Prepare voice + music metadata */
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  Controller: Get All Videos for a User                                      */
/* ────────────────────────────────────────────────────────────────────────── */
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  Controller: Get Single Video by ID                                         */
/* ────────────────────────────────────────────────────────────────────────── */
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  Controller: Update Video                                                   */
/* ────────────────────────────────────────────────────────────────────────── */
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  Controller: Delete Video                                                   */
/* ────────────────────────────────────────────────────────────────────────── */
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
