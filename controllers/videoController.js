/** @format */
/*  videoController.js  — trends‑driven, OpenAI‑orchestrated edition
 *  ✅ Uses Google Trends images per segment (non‑redundant where possible)
 *  ✅ OpenAI plans narration + visuals dynamically from Trends + article links
 *  ✅ Cloudinary normalises ratio & lightly enhances images before Runway
 *  ✅ Runway image‑to‑video as the primary path; hard‑fail on rejection
 *  ✅ Falls back to text‑to‑image→video only when no Trends images exist (e.g. Top5)
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
const WORDS_PER_SEC = 1.8;

/* Gen‑4 Turbo everywhere for speed + fidelity */
const T2V_MODEL = "gen4_turbo";
const ITV_MODEL = "gen4_turbo";
const TTI_MODEL = "gen4_image";

const QUALITY_BONUS =
	"photorealistic, ultra‑detailed, HDR, 8K, cinema lighting, award‑winning, trending on artstation";

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
	"wall‑eyed",
	"sliding feet",
].join(", ");

const HUMAN_SAFETY =
	"anatomically correct, one natural‑looking head, two eyes, normal limbs, realistic proportions, natural head position";

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

/* remove surrounding markdown code fence if present, without using literal ``` */
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

/* Voice tone classification – excited, varied, but respectful on tragedy */
function deriveVoiceSettings(text, category = "Other") {
	const baseStyle = ELEVEN_STYLE_BY_CATEGORY[category] ?? 0.7;
	const lower = String(text || "").toLowerCase();

	const isSensitive = SENSITIVE_TONE_RE.test(lower);

	let style = baseStyle;
	let stability = 0.15;
	let similarityBoost = 0.92;
	let openaiSpeed = 1.03;

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
			style = Math.min(1, baseStyle + 0.25);
			stability = 0.13;
			openaiSpeed = 1.07;
		} else {
			style = Math.min(1, baseStyle + 0.1);
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

async function exactLen(src, target, out) {
	const meta = await new Promise((r, j) =>
		ffmpeg.ffprobe(src, (e, d) => (e ? j(e) : r(d)))
	);
	const diff = +(target - meta.format.duration).toFixed(3);
	await ffmpegPromise((c) => {
		c.input(norm(src));
		if (Math.abs(diff) < 0.05) {
			/* keep length */
		} else if (diff < 0) {
			c.outputOptions("-t", String(target));
		} else {
			c.videoFilters(`tpad=stop_duration=${diff}`);
		}
		return c
			.outputOptions(
				"-c:v",
				"libx264",
				"-preset",
				"veryfast",
				"-crf",
				"21",
				"-pix_fmt",
				"yuv420p",
				"-y"
			)
			.save(norm(out));
	});
}

async function exactLenAudio(src, target, out) {
	const meta = await new Promise((r, j) =>
		ffmpeg.ffprobe(src, (e, d) => (e ? j(e) : r(d)))
	);
	const inDur = meta.format.duration;
	const diff = +(target - inDur).toFixed(3);
	await ffmpegPromise((c) => {
		c.input(norm(src));
		if (Math.abs(diff) <= 0.05) {
			/* leave */
		} else if (diff > 0.05) {
			c.audioFilters(`apad=pad_dur=${diff}`);
		} else {
			const ratio = inDur / target;
			if (ratio <= 2.0) {
				c.audioFilters(`atempo=${ratio.toFixed(3)}`);
			} else if (ratio <= 4.0) {
				const r = Math.sqrt(ratio).toFixed(3);
				c.audioFilters(`atempo=${r},atempo=${r}`);
			} else {
				c.outputOptions("-t", String(target));
			}
		}
		return c.outputOptions("-y").save(norm(out));
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
		return null;
	}
}

/* SEO title – official, search‑friendly, non‑clickbait */
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
			? "like ESPN or an official league/NHL channel, not a meme or fan channel."
			: "like a major newspaper or broadcaster, not a clickbait channel."
	}

SEO / search behaviour:
- Include the core subject or matchup once (for example: "Golden Knights vs Ducks").
- Prefer phrases that match how people actually search, such as:
  ${
		isSports
			? '"Highlights", "Gameday Preview", "How To Watch", "Full Recap".'
			: '"Explained", "Update", "Analysis", "What To Know".'
	}
- You may use a short descriptor after a separator like "|" or "–"
  (for example: "Golden Knights vs Ducks | Gameday Preview").

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

/* Top‑5 outline helper so GPT is “smart” about which items it uses */
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

async function uploadTrendImageToCloudinary(url, ratio, slugBase) {
	if (!url) throw new Error("Missing Trends image URL");
	const aspect = ratioToCloudinaryAspect(ratio);
	const publicIdBase =
		slugBase || `aivideomatic/trend_seeds/${Date.now()}_${crypto.randomUUID()}`;
	const opts = {
		public_id: publicIdBase,
		resource_type: "image",
		overwrite: false,
		folder: "aivideomatic/trend_seeds",
		transformation: [
			{
				aspect_ratio: aspect,
				crop: "fill",
				gravity: "auto",
			},
			{ effect: "sharpen:60" },
			{ effect: "contrast:20" },
		],
	};
	const result = await cloudinary.uploader.upload(url, opts);
	console.log("[Cloudinary] Seed image uploaded →", result.public_id);
	return {
		public_id: result.public_id,
		url: result.secure_url,
	};
}

/* ───────────────────────────────────────────────────────────────
 *  Runway poll + retry (hard‑fail on 4xx, as requested)
 * ───────────────────────────────────────────────────────────── */
async function pollRunway(id, tk, lbl) {
	const url = `https://api.dev.runwayml.com/v1/tasks/${id}`;
	for (let i = 0; i < MAX_POLL_ATTEMPTS; i++) {
		await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
		const { data } = await axios.get(url, {
			headers: {
				Authorization: `Bearer ${tk}`,
				"X-Runway-Version": RUNWAY_VERSION,
			},
		});
		if (data.status === "SUCCEEDED") return data.output[0];
		if (data.status === "FAILED")
			throw new Error(`${lbl} failed (Runway reported FAILED)`);
	}
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
			last = e;
			// Hard 4xx (except 429) are unrecoverable → do not keep retrying
			if (status && status >= 400 && status < 500 && status !== 429) break;
		}
	}
	throw last;
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

/* ───────────────────────────────────────────────────────────────
 *  ElevenLabs TTS helper (with per‑segment tone)
 * ───────────────────────────────────────────────────────────── */
async function elevenLabsTTS(text, language, outPath, category = "Other") {
	if (!ELEVEN_API_KEY) throw new Error("ELEVENLABS_API_KEY missing");
	const voiceId = ELEVEN_VOICES[language] || ELEVEN_VOICES[DEFAULT_LANGUAGE];

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
 * ───────────────────────────────────────────────────────────── */
async function buildVideoPlanWithGPT({
	topic,
	category,
	language,
	duration,
	segLens,
	trendStory,
	articleText,
	top5Outline,
}) {
	const segCnt = segLens.length;
	const segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));
	const hasImages =
		trendStory &&
		Array.isArray(trendStory.images) &&
		trendStory.images.length > 0;
	const images = hasImages ? trendStory.images.slice(0, 8) : [];
	const articleTitles = (trendStory?.articles || [])
		.map((a) => a.title)
		.filter(Boolean);
	const snippet = articleText ? articleText.slice(0, 1800) : "";
	const segDescLines = segLens
		.map(
			(sec, i) =>
				`Segment ${i + 1}: ~${sec}s, ≤ ${segWordCaps[i]} spoken words.`
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
${categoryTone ? `- Tone: ${categoryTone}` : ""}
`.trim();

	let promptText;
	if (hasImages) {
		const imgCount = images.length;
		promptText = `
${baseIntro}

You also have ${imgCount} REAL photos from Google Trends for this story.

Google Trends context:
- Story title: ${trendStory.title || topic}
- Article headlines (for factual grounding):
  ${articleTitles.map((t) => `- ${t}`).join("\n  ") || "- (none)"}

Article text snippet (may be truncated, use for facts only):
${snippet || "(no article text available)"}

Images:
I have attached the ${imgCount} images to this message, in order.
The FIRST attached image is imageIndex 0, the second is 1, etc.

Your job:
1) Write the voice‑over script for each segment.
2) Decide which imageIndex to animate for each segment.
3) For each segment, write one concise "runwayPrompt" telling a video model how to animate THAT exact real photo.

Visual rules:
- Use each imageIndex at most once before reusing any image.
- Aim for subtle, realistic animation: camera push‑in or pan, shallow depth‑of‑field, arena lights flickering, slow‑motion crowd, gentle parallax.
- Do NOT radically change the scene: keep team colours, logos and basic composition.
- Never morph faces into different people.
- No surreal or abstract effects.

Return a single JSON object with this exact shape:

{
  "segments": [
    {
      "index": 1,
      "scriptText": "spoken narration for segment 1",
      "imageIndex": 0,
      "runwayPrompt": "how to animate the first attached photo"
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
- After the rank label, you may add one concise sentence explaining why that item deserves its rank.
- No images are provided, so you must imagine visuals.

For each segment, output:
- "index"
- "scriptText"
- "runwayPrompt": a vivid but grounded description of the scene to generate from scratch. Focus on symbols (arenas, trophies, jerseys), not specific copyrighted logos.

Return JSON of the form:
{
  "segments": [
    { "index": 1, "scriptText": "...", "runwayPrompt": "..." },
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

Visual rules:
- Keep scenes realistic and grounded in today's world.
- Avoid specific trademarks or team logos; describe them generically (for example "home team in dark jerseys").
- If people are visible, keep faces natural and undistorted.
- Prefer one clear focal subject per segment.

Return JSON of the form:
{
  "segments": [
    { "index": 1, "scriptText": "...", "runwayPrompt": "..." },
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

	// Normalise & clamp
	const segments = plan.segments.map((s, idx) => {
		const base = {
			index: typeof s.index === "number" ? s.index : idx + 1,
			scriptText: String(s.scriptText || "").trim(),
			runwayPrompt: String(s.runwayPrompt || "").trim(),
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

		/* ─────────────────────────────────────────────────────────
		 *  1.  Topic resolution & Google Trends
		 * ───────────────────────────────────────────────────────── */
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
			} catch {
				/* ignore – fallback below */
			}
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
				topic = remaining.length ? choose(remaining) : choose(ALL_TOP5_TOPICS);
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

		// If we have a Trends story, grab a little article text for better grounding
		if (trendStory && trendStory.articles && trendStory.articles.length) {
			trendArticleText = await scrapeArticleText(
				trendStory.articles[0].url || null
			);
		}

		/* ─────────────────────────────────────────────────────────
		 *  2.  Segment timing (Intro + body segments)
		 * ───────────────────────────────────────────────────────── */
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

		/* ─────────────────────────────────────────────────────────
		 *  3.  Top‑5 outline if needed
		 * ───────────────────────────────────────────────────────── */
		let top5Outline = null;
		if (category === "Top5") {
			top5Outline = await generateTop5Outline(topic, language);
		}

		/* ─────────────────────────────────────────────────────────
		 *  4.  Upload Trends images to Cloudinary (if available)
		 * ───────────────────────────────────────────────────────── */
		let trendCloudinaryImages = [];
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
					trendCloudinaryImages.push(up.url);
				} catch (e) {
					console.warn("[Cloudinary] upload failed →", e.message);
				}
			}
			if (!trendCloudinaryImages.length) {
				console.warn(
					"[Cloudinary] All Trends uploads failed – falling back to prompt‑only mode"
				);
			}
		}

		const hasTrendImages = trendCloudinaryImages.length > 0;

		/* ─────────────────────────────────────────────────────────
		 *  5.  Let OpenAI orchestrate segments + visuals
		 * ───────────────────────────────────────────────────────── */
		console.log("[GPT] building full video plan …");

		const plan = await buildVideoPlanWithGPT({
			topic,
			category,
			language,
			duration,
			segLens,
			trendStory: hasTrendImages ? trendStory : null,
			articleText: trendArticleText,
			top5Outline,
		});

		let segments = plan.segments;

		// Tighten narration to fit word caps using GPT when necessary
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

		const fullScript = segments.map((s) => s.scriptText.trim()).join(" ");

		/* ─────────────────────────────────────────────────────────
		 *  6.  Global style, SEO title, tags
		 * ───────────────────────────────────────────────────────── */
		let globalStyle = "";
		try {
			const g = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [
					{
						role: "user",
						content: `Give one short cinematic style phrase for the video topic "${topic}".`,
					},
				],
			});
			globalStyle = g.choices[0].message.content
				.replace(/^[-–•\s]+/, "")
				.trim();
		} catch {
			/* ignore */
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
		} catch {
			/* ignore */
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
		} catch {}
		if (category === "Top5") tags.unshift("Top5");
		if (!tags.includes(BRAND_TAG)) tags.unshift(BRAND_TAG);
		tags = [...new Set(tags)];

		/* ─────────────────────────────────────────────────────────
		 *  7.  Optional background music
		 * ───────────────────────────────────────────────────────── */
		let music = null;
		try {
			const jam =
				(await jamendo(topic.split(" ")[0])) || (await jamendo("ambient"));
			if (jam) {
				music = tmpFile("bg", ".mp3");
				const ws = fs.createWriteStream(music);
				const { data } = await axios.get(jam, { responseType: "stream" });
				await new Promise((r, j) =>
					data.pipe(ws).on("finish", r).on("error", j)
				);
			}
		} catch (e) {
			console.warn("[Music] Jamendo failed →", e.message);
		}

		/* ─────────────────────────────────────────────────────────
		 *  8.  PER‑SEGMENT VIDEO GENERATION (Runway image‑to‑video first)
		 * ───────────────────────────────────────────────────────── */
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

			// Runway supports discrete durations; we’ll pick the closest of 5 or 10s
			const rw = Math.abs(5 - d) <= Math.abs(10 - d) ? 5 : 10;

			console.log(
				`[Seg ${i + 1}/${segCnt}] targetDuration=${d}s runwayDuration=${rw}s`
			);

			const promptBase = `${
				seg.runwayPrompt || ""
			}, ${globalStyle}, ${QUALITY_BONUS}, ${HUMAN_SAFETY}`;
			const promptText =
				promptBase.length > PROMPT_CHAR_LIMIT
					? promptBase.slice(0, PROMPT_CHAR_LIMIT)
					: promptBase;

			let clipPath = null;

			if (hasTrendImages && seg.imageIndex !== null) {
				const imgUrl =
					trendCloudinaryImages[seg.imageIndex] || trendCloudinaryImages[0];
				if (!imgUrl)
					throw new Error("No Cloudinary Trends image available for Runway");

				// Primary path: image_to_video using real Trends image
				const idVid = await retry(
					async () => {
						const { data } = await axios.post(
							"https://api.dev.runwayml.com/v1/image_to_video",
							{
								model: ITV_MODEL,
								promptImage: imgUrl,
								promptText,
								ratio,
								duration: rw,
								promptStrength: 0.4, // mild changes only
								negativePrompt: RUNWAY_NEGATIVE_PROMPT,
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
					`itv_seg${i + 1}`
				);

				const vidUrl = await retry(
					() => pollRunway(idVid, RUNWAY_ADMIN_KEY, `poll_itv_seg${i + 1}`),
					3,
					`poll_itv_seg${i + 1}`
				);

				const p = tmpFile(`seg_itv_${i + 1}`, ".mp4");
				await new Promise((r, j) =>
					axios
						.get(vidUrl, { responseType: "stream" })
						.then(({ data }) =>
							data.pipe(fs.createWriteStream(p)).on("finish", r).on("error", j)
						)
				);
				clipPath = p;
			} else {
				// Fallback: no Trends images – text_to_image + image_to_video
				const ttiId = await retry(
					async () => {
						const { data } = await axios.post(
							"https://api.dev.runwayml.com/v1/text_to_image",
							{
								model: TTI_MODEL,
								promptText,
								ratio,
								promptStrength: 0.9,
								negativePrompt: RUNWAY_NEGATIVE_PROMPT,
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
					`tti_seg${i + 1}`
				);

				const imgUrl = await retry(
					() => pollRunway(ttiId, RUNWAY_ADMIN_KEY, `poll_tti_seg${i + 1}`),
					3,
					`poll_tti_seg${i + 1}`
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
								duration: rw,
								promptStrength: 0.85,
								negativePrompt: RUNWAY_NEGATIVE_PROMPT,
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
					`itv_from_tti_seg${i + 1}`
				);

				const vidUrl = await retry(
					() =>
						pollRunway(
							idVid,
							RUNWAY_ADMIN_KEY,
							`poll_itv_from_tti_seg${i + 1}`
						),
					3,
					`poll_itv_from_tti_seg${i + 1}`
				);

				const p = tmpFile(`seg_tti_itv_${i + 1}`, ".mp4");
				await new Promise((r, j) =>
					axios
						.get(vidUrl, { responseType: "stream" })
						.then(({ data }) =>
							data.pipe(fs.createWriteStream(p)).on("finish", r).on("error", j)
						)
				);
				clipPath = p;
			}

			// Adjust to exact segment duration
			const fixed = tmpFile(`fx_${i + 1}`, ".mp4");
			await exactLen(clipPath, d, fixed);
			try {
				fs.unlinkSync(clipPath);
			} catch {}

			clips.push(fixed);

			sendPhase("GENERATING_CLIPS", {
				msg: `Rendering segment ${i + 1}/${segCnt}`,
				total: segCnt,
				done: i + 1,
			});
			console.log("[Phase] GENERATING_CLIPS → Rendering segment", i + 1);
		}

		/* ─────────────────────────────────────────────────────────
		 *  9.  Concatenate silent video
		 * ───────────────────────────────────────────────────────── */
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
		await exactLen(silent, duration, silentFixed);
		try {
			fs.unlinkSync(silent);
		} catch {}

		/* ─────────────────────────────────────────────────────────
		 * 10.  Voice‑over & music
		 * ───────────────────────────────────────────────────────── */
		sendPhase("ADDING_VOICE_MUSIC", { msg: "Creating audio layer" });
		console.log("[Phase] ADDING_VOICE_MUSIC → Creating audio layer");

		const fixedPieces = [];
		for (let i = 0; i < segCnt; i++) {
			const raw = tmpFile(`tts_raw_${i + 1}`, ".mp3");
			const fixed = tmpFile(`tts_fix_${i + 1}`, ".wav");
			const txt = improveTTSPronunciation(segments[i].scriptText);

			let tone;
			try {
				tone = await elevenLabsTTS(txt, language, raw, category);
			} catch (e) {
				console.warn(
					`[TTS] ElevenLabs failed for seg ${i + 1}, falling back to OpenAI →`,
					e.message
				);
				const t = deriveVoiceSettings(txt, category);
				const tts = await openai.audio.speech.create({
					model: "tts-1-hd",
					voice: "shimmer",
					speed: t.openaiSpeed,
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

		fs.writeFileSync(
			listFile,
			fixedPieces.map((p) => `file '${norm(p)}'`).join("\n")
		);
		const ttsJoin = tmpFile("tts_join", ".wav");
		await ffmpegPromise((c) =>
			c
				.input(norm(listFile))
				.inputOptions("-f", "concat", "-safe", "0")
				.outputOptions("-c", "copy", "-y")
				.save(norm(ttsJoin))
		);
		try {
			fs.unlinkSync(listFile);
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
						"[0:a]volume=1.4[a0]",
						"[1:a]volume=0.12[a1]",
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

		/* ─────────────────────────────────────────────────────────
		 * 11.  Mux audio + video
		 * ───────────────────────────────────────────────────────── */
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
					"veryfast",
					"-crf",
					"18",
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

		/* ─────────────────────────────────────────────────────────
		 * 12.  YouTube upload (best‑effort)
		 * ───────────────────────────────────────────────────────── */
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

		/* ─────────────────────────────────────────────────────────
		 * 13.  Persist to Mongo
		 * ───────────────────────────────────────────────────────── */
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
		});

		/* optional scheduling */
		if (schedule) {
			const { type, timeOfDay, startDate, endDate } = schedule;
			const [hh, mm] = timeOfDay.split(":").map(Number);

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

		/* ─────────────────────────────────────────────────────────
		 * 14.  DONE
		 * ───────────────────────────────────────────────────────── */
		sendPhase("COMPLETED", {
			id: doc._id,
			youtubeLink,
			phases: JSON.parse(JSON.stringify(history)),
		});
		console.log("[Phase] COMPLETED", doc._id, youtubeLink);
		res.end();
	} catch (err) {
		console.error("[createVideo] ERROR", err);
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
