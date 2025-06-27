/*  videoController.js  — dynamic, music‑safe, full‑log edition 2025‑06‑23
 *  🔄 100 %‑synced release – fixes timing drift, Top‑5 overlay,
 *  richer prompts, explicit fallback phases, complete phase history.
 */
/* eslint-disable no-await-in-loop, camelcase, max-len */
/**
 * controllers/videoController.js
 * Last updated: 2025‑06‑25
 *
 * – Gen‑2 text‑to‑video first, then image pipeline fallback
 * – Up‑level prompt engineering & negative prompts
 * – Fully hoisted function declarations (no ReferenceErrors)
 * – All original console logs / sendPhase events kept for debugging
 */

/* ─────────────────────────────────────────────────────────────── */
/*  BASIC DEPENDENCIES                                             */
/* ─────────────────────────────────────────────────────────────── */
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const child_process = require("child_process");

const axios = require("axios");
const dayjs = require("dayjs");
const cheerio = require("cheerio");
const qs = require("querystring");

const { google } = require("googleapis");
const { OpenAI } = require("openai");
const ffmpeg = require("fluent-ffmpeg");

/* ───────────────────────────────────────────────────────────────
 *  1. Mongoose models & shared utils
 * ───────────────────────────────────────────────────────────── */
const Video = require("../models/Video");
const Schedule = require("../models/Schedule");
const {
	ALL_TOP5_TOPICS,
	googleTrendingCategoriesId,
} = require("../assets/utils");

const {
	safeDescribeSeedImage,
	injectSeedDescription,
	uploadWithVariation,
} = require("../assets/helper");

/* ───────────────────────────────────────────────────────────────
 *  2.  Runtime guards + ffmpeg bootstrap
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
	"FFmpeg binary not found – install ffmpeg or set FFMPEG_PATH."
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

/* font discovery (for Top‑5 overlays) */
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
 *  3.  Global constants
 * ───────────────────────────────────────────────────────────── */
const RUNWAY_VERSION = "2024-11-06";
const POLL_INTERVAL_MS = 2_000;
const MAX_POLL_ATTEMPTS = 180;

const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });
const JAMENDO_ID = process.env.JAMENDO_CLIENT_ID;
const RUNWAY_ADMIN_KEY = process.env.RUNWAYML_API_SECRET;
const ELEVEN_API_KEY = process.env.ELEVENLABS_API_KEY;

/* generation models */
const VALID_RATIOS = [
	"1280:720",
	"720:1280",
	"1104:832",
	"832:1104",
	"960:960",
	"1584:672",
];
const WORDS_PER_SEC = 1.8;

const T2V_MODEL = "gen2_turbo"; // realistic motion
const ITV_MODEL = "gen4_turbo"; // image‑to‑video
const TTI_MODEL = "gen4_image"; // text‑to‑image

/* prompt tuning */
const QUALITY_BONUS =
	"photorealistic, ultra‑detailed, HDR, 8K, cinema lighting, award‑winning, trending on artstation";
const RUNWAY_NEGATIVE_PROMPT = [
	"duplicate",
	"mirror",
	"reverse",
	"backwards walk",
	"extra limbs",
	"broken fingers",
	"contorted",
	"bad anatomy",
	"dislocated joints",
	"lowres",
	"blur",
	"watermark",
	"nsfw",
	"awkward pose",
	"mismatched gaze",
	"sliding feet",
].join(", ");
const HUMAN_SAFETY =
	"anatomically correct, two eyes, one head, normal limbs, realistic proportions, natural waist";

const CHAT_MODEL = "gpt-4o";

/* ElevenLabs voices + style‑by‑category */
const ELEVEN_VOICES = {
	English: "21m00Tcm4TlvDq8ikWAM",
	العربية: "CYw3kZ02Hs0563khs1Fj",
	Français: "gqjD3Awy6ZnJf2el9DnG",
	Deutsch: "IFHEeWG1IGkfXpxmB1vN",
	हिंदी: "ykoxtvL6VZTyas23mE9F",
};
const ELEVEN_STYLE_BY_CATEGORY = {
	Sports: 1.0,
	Politics: 0.6,
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

const DEFAULT_LANGUAGE = "English";
const TONE_HINTS = {
	Sports: "Use an energetic, motivational tone and sprinkle light humour.",
	Politics: "Maintain an authoritative yet neutral tone.",
	Finance: "Speak in a confident, analytical tone.",
	Entertainment: "Keep it upbeat and engaging.",
	Technology: "Adopt a forward‑looking, curious tone.",
	Health: "Stay reassuring and informative.",
	Lifestyle: "Be friendly and encouraging.",
	Science: "Convey wonder and clarity.",
	World: "Maintain an objective, international outlook.",
	Top5: "Keep each item snappy, thrilling, and hype‑driven.",
};

const TOPIC_RULES = [
	{
		test: /football|soccer/i,
		positive: "single soccer ball",
		negative: "two balls, duplicate ball, extra ball",
	},
	{
		test: /tennis/i,
		positive: "",
		negative: "table tennis, ping‑pong table, paddles",
	},
	{
		test: /basketball/i,
		positive: "single orange basketball",
		negative: "two balls, duplicate ball",
	},
	{
		test: /judge|courtroom/i,
		positive: "",
		negative: "crossed eyes, lazy eye, wonky eyes",
	},
	{
		test: /handshake|meeting|business deal/i,
		positive:
			"two executives shaking hands, thumbs locked, eye‑contact, center frame",
		negative:
			"broken handshake, disjointed fingers, hands not touching, twisted wrist",
	},
	{
		test: /walking|runner|jogger/i,
		positive: "subject strides forward, heel‑to‑toe, natural gait",
		negative: "backwards walk, sliding feet, floating, disconnected shadow",
	},
	// gaming, politics, etc. can follow
];

function tunePromptForTopic(prompt, topicRaw) {
	for (const rule of TOPIC_RULES) {
		if (rule.test.test(topicRaw)) {
			const plus = rule.positive ? `${rule.positive}, ` : "";
			return `${plus}${prompt.replace(
				RUNWAY_NEGATIVE_PROMPT,
				`${rule.negative}, ${RUNWAY_NEGATIVE_PROMPT}`
			)}`;
		}
	}
	return prompt;
}

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

const EMOTIONS = [
	"smiling",
	"laughing",
	"serious",
	"angry",
	"sad",
	"surprised",
];

/* ───────────────────────────────────────────────────────────────
 *  4.  Helper utilities
 * ───────────────────────────────────────────────────────────── */
const norm = (p) => (p ? p.replace(/\\/g, "/") : p);
const choose = (a) => a[Math.floor(Math.random() * a.length)];
const strip = (s) =>
	(s.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/m) || [, ""])[1] || s;
const goodDur = (n) =>
	Number.isInteger(+n) && +n >= 5 && +n <= 90 && +n % 5 === 0;
const escTxt = (t) =>
	t
		.replace(/\\/g, "\\\\")
		.replace(/[’']/g, "\\'")
		.replace(/:/g, "\\:")
		.replace(/,/g, "\\,");

function tmpFile(tag, ext = "") {
	return path.join(os.tmpdir(), `${tag}_${crypto.randomUUID()}${ext}`);
}

function pickEmotion(t) {
	const l = t.toLowerCase();
	if (/laugh|celebrat|cheer|joy|win/.test(l)) return "laughing";
	if (/angry|rage|furious|protest/.test(l)) return "angry";
	if (/sad|mourn|grief|tear/.test(l)) return "sad";
	if (/shock|wow|astonish|surpris/.test(l)) return "surprised";
	if (/happy|delight|smile/.test(l)) return "smiling";
	return "serious";
}

function spokenSeconds(words) {
	return +(words / WORDS_PER_SEC).toFixed(2);
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

const PROMPT_CHAR_LIMIT = 220;

/* ───────────────────────────────────────────────────────────────
 *  5.  JSON‑safe segment parser
 * ───────────────────────────────────────────────────────────── */
function parseSegmentsSafe(raw) {
	raw = strip(raw.trim());
	if (!raw.trim().startsWith("[")) raw = `[${raw}]`;
	try {
		return JSON.parse(raw);
	} catch {}
	try {
		const j = JSON.parse(
			raw.replace(/(['`])([^'`]*?)\1/g, (m, q, s) => `"${s}"`)
		);
		return Array.isArray(j) ? j : Object.values(j);
	} catch {
		return null;
	}
}

async function getSegments(prompt, cnt) {
	for (let a = 1; a <= 3; a++) {
		const rsp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: prompt }],
		});
		const seg = parseSegmentsSafe(rsp.choices[0].message.content);
		if (seg && seg.length === cnt) return seg;
		console.warn(`[GPT] segments parse failed on attempt ${a}`);
	}
	throw new Error("GPT segment JSON malformed after 3 attempts");
}

/* ───────────────────────────────────────────────────────────────
 *  6.  ffmpeg helpers
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
		} else if (diff < 0) c.outputOptions("-t", String(target));
		else c.videoFilters(`tpad=stop_duration=${diff}`);
		return c
			.outputOptions(
				"-c:v",
				"libx264",
				"-preset",
				"veryfast",
				"-crf",
				"23",
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
		} else if (diff > 0.05) c.audioFilters(`apad=pad_dur=${diff}`);
		else {
			const ratio = inDur / target;
			if (ratio <= 2.0) c.audioFilters(`atempo=${ratio.toFixed(3)}`);
			else if (ratio <= 4.0) {
				const r = Math.sqrt(ratio).toFixed(3);
				c.audioFilters(`atempo=${r},atempo=${r}`);
			} else c.outputOptions("-t", String(target));
		}
		return c.outputOptions("-y").save(norm(out));
	});
}
async function checkOverlay(filter, w, h, d) {
	if (!hasLavfi) return;
	const vf = filter.replace(/\[vout\]$/, "");
	const tmp = tmpFile("chk", ".mp4");
	await ffmpegPromise((c) =>
		c
			.input(`color=c=black:s=${w}x${h}:d=${d}`)
			.inputOptions("-f", "lavfi")
			.complexFilter([vf])
			.outputOptions("-frames:v", "1", "-f", "null", "-")
			.save(norm(tmp))
	);
	fs.unlinkSync(tmp);
}
async function makeDummyClip(w, h, d) {
	if (!hasLavfi)
		throw new Error("FFmpeg without lavfi – cannot create dummy clip");
	const out = tmpFile("dummy", ".mp4");
	await ffmpegPromise((c) =>
		c
			.input(`color=c=black:s=${w}x${h}:d=${d}`)
			.inputOptions("-f", "lavfi")
			.outputOptions(
				"-c:v",
				"libx264",
				"-t",
				String(d),
				"-pix_fmt",
				"yuv420p",
				"-y"
			)
			.save(norm(out))
	);
	return out;
}

async function validateClipStill(stillPath, yesNoChecks = []) {
	// Build the prompt: one Yes/No question per rule
	const prompt = yesNoChecks.map((r) => `Yes/No: ${r.q}`).join("\n");

	const { choices } = await openai.chat.completions.create({
		model: CHAT_MODEL,
		messages: [
			{
				role: "user",
				content: [
					{ type: "text", text: prompt },
					{
						type: "image_url",
						image_url: {
							url: `data:image/jpeg;base64,${fs.readFileSync(
								stillPath,
								"base64"
							)}`,
						},
					},
				],
			},
		],
	});

	/* We accept if *all* answers contain “yes” (case‑insensitive) */
	return choices[0].message.content
		.toLowerCase()
		.split(/[\n\r]+/)
		.every((l) => l.includes("yes"));
}

/* ───────────────────────────────────────────────────────────────
 *  7.  GPT prompt helpers
 * ───────────────────────────────────────────────────────────── */
async function refineRunwayPrompt(initialPrompt, scriptText) {
	/* ---------- 1.  derive context‑sensitive extra tags ---------- */
	const mustHaveUniqueObj =
		/\b(soccer ball|tennis ball|football|basketball)\b/i.test(initialPrompt);

	const needFwdMotion = /\bwalk|run|jog|march|drive|cycle|skate\b/i.test(
		scriptText
	);

	const involvesHandshake = /\bhandshake|deal|agreement|congratulate\b/i.test(
		scriptText
	);

	const spatial = /\b(pitch|court|arena|stadium|dock|bench|field)\b/i.test(
		initialPrompt
	)
		? ""
		: "center frame";

	const extrasArr = [
		spatial,
		mustHaveUniqueObj ? "single object in view" : "",
		needFwdMotion ? "natural forward motion, heel‑to‑toe gait" : "",
		involvesHandshake
			? "firm professional handshake, thumbs locked, eye‑contact"
			: "",
		"face‑preserving",
	].filter(Boolean);

	/* ---------- 2.  build the GPT instruction ---------- */
	const ask = `
Rewrite the following as a *production‑ready* RUNWAY Gen‑2 prompt.

Rules
• 20‑25 words (hard cap).  
• Start with ONE vivid atmosphere adjective.  
• Use **present‑tense, imperative verbs** (“Observe…”, “Tracking shot of…”, “Close‑up showing…”).  
• No quotes, brand names, or proper nouns.  
• Keep only indispensable subjects & actions; drop camera jargon.  
• If motion is implied, specify “forward”, “left‑to‑right”, etc.  
• If two subjects interact, ensure “mutual eye‑contact” and describe the physical connection.  
• Finish with a concise style tag (e.g. “cinematic HDR”).  

Input
«${initialPrompt}»
---
Context (what happens in the scene)
${scriptText}

Return ONLY the rewritten prompt.
  `.trim();

	/* ---------- 3.  call GPT – retry max 2 ---------- */
	for (let attempt = 1; attempt <= 2; attempt++) {
		try {
			const rsp = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: ask }],
			});

			let out = rsp.choices[0].message.content.replace(/["“”]/g, "").trim();

			/* hard‑truncate to 75 tokens  */
			const tokens = out.split(/\s+/);
			if (tokens.length > 75) out = tokens.slice(0, 75).join(" ");

			/* post‑pend extras & return */
			return `${extrasArr.join(", ")}, ${out}`;
		} catch (err) {
			console.warn(`[GPT refine] attempt ${attempt} failed → ${err.message}`);
			if (attempt === 2) return initialPrompt; // fallback
		}
	}
}

async function generateFallbackPrompt(topic, category) {
	const WORD_LIMIT = 12;
	const MAX_ATTEMPTS = 2;

	const baseAsk = (tryNo) => `
Attempt ${tryNo} — respond with ONE ${WORD_LIMIT}-word prompt (count words, 8‑${WORD_LIMIT} accepted).

• Start with a mood adjective (“Brooding”, “Vibrant”…).  
• Use concrete nouns & present‑tense active verbs.  
• Exactly one clear subject + action.  
• No names, brands, hashtags, camera jargon.  
• Avoid ambiguous directions (say “forward” not “backwards”).  
• End with a concise style tag (“cinematic HDR”, “neon noir” …).

Scene must capture today’s hottest ${category} topic: **${topic}**

Return ONLY the prompt.`;

	for (let t = 1; t <= MAX_ATTEMPTS; t++) {
		try {
			const { choices } = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: baseAsk(t) }],
			});

			let out = choices[0].message.content.replace(/["“”]/g, "").trim();
			const words = out.split(/\s+/);
			if (words.length <= WORD_LIMIT && words.length >= 8) {
				return `${out}, ${QUALITY_BONUS}`;
			}
		} catch (err) {
			console.warn(`[fallbackPrompt] attempt ${t} failed → ${err.message}`);
		}
	}
	/* ultimate fallback — safe generic */
	return `Atmospheric silhouette strides forward under stadium lights, cinematic HDR, ${QUALITY_BONUS}`;
}

async function describeHuman(language, country) {
	const MAX_ATTEMPTS = 2;
	const WORD_CAP = 15;
	const locale =
		country && country.toLowerCase() !== "all countries"
			? `from ${country}`
			: "US";

	const baseAsk = (n) => `
Attempt ${n}: In ≤${WORD_CAP} words, describe **ONE** photorealistic human ${locale}.

Must include:
• age range & gender impression  
• skin tone & build  
• attire & mood  
• lens / lighting style

No brands, no names, no guess about unseen parts.
Return ONLY the description.`;

	for (let n = 1; n <= MAX_ATTEMPTS; n++) {
		try {
			const { choices } = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: baseAsk(n) }],
			});
			let out = choices[0].message.content.replace(/["“”]/g, "").trim();
			if (out.split(/\s+/).length <= WORD_CAP) return out;
		} catch (err) {
			console.warn(`[describeHuman] attempt ${n} failed → ${err.message}`);
		}
	}
	return "mid‑30s athletic male, olive skin, casual jacket, relaxed smile, soft rim light";
}

async function describePerson(name) {
	const MAX_ATTEMPTS = 2;
	const WORD_CAP = 20;
	const ask = (tryNo) => `
Attempt ${tryNo}: In ≤${WORD_CAP} words depict a photorealistic person who could *evoke* ${name}.

Include:
• face shape, skin tone, hair colour & style  
• eye shape, build, age range  
• attire, mood  
• lens & lighting

No names, brands or trademark features.  
Return ONLY the description.`;

	for (let a = 1; a <= MAX_ATTEMPTS; a++) {
		try {
			const { choices } = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: ask(a) }],
			});
			let out = choices[0].message.content.replace(/["“”]/g, "").trim();
			if (out.split(/\s+/).length <= WORD_CAP) return out;
		} catch (err) {
			console.warn(`[describePerson] attempt ${a} failed → ${err.message}`);
		}
	}
	/* safe default */
	return "early‑40s medium build, warm brown eyes, short wavy chestnut hair, calm expression, soft key light";
}

async function injectHumanIfNeeded(
	runwayPrompt,
	scriptText,
	language,
	country,
	cache
) {
	const hasHuman = /\b(man|woman|person|portrait)\b/i.test(runwayPrompt);
	const name = scriptText.match(/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b/);

	/* celebrity path */
	if (name) {
		const celeb = name[1];
		if (!cache[celeb]) cache[celeb] = await describePerson(celeb);
		if (!runwayPrompt.startsWith(cache[celeb]))
			return `${cache[celeb]}, ${HUMAN_SAFETY}, ${runwayPrompt}`;
		return runwayPrompt;
	}

	/* inject generic */
	if (
		!hasHuman &&
		/\b(he|she|they|man|woman|person|people)\b/i.test(scriptText)
	) {
		if (!cache.humanDesc)
			cache.humanDesc = await describeHuman(language, country);
		const emo = pickEmotion(scriptText);
		return `${emo} ${cache.humanDesc}, ${HUMAN_SAFETY}, ${runwayPrompt}`;
	}
	return runwayPrompt;
}

/* ───────────────────────────────────────────────────────────────
 *  8.  Google‑Trends helpers & SEO title
 * ───────────────────────────────────────────────────────────── */
function resolveTrendsCategoryId(label) {
	const e = googleTrendingCategoriesId.find((c) => c.category === label);
	return e ? e.ids[0] : 0;
}
const TRENDS_API_URL =
	process.env.TRENDS_API_URL || "http://localhost:8102/api/google-trends";

async function generateSeoTitle(headlines, category, language) {
	try {
		const ask = `Give ONE irresistible YouTube title in Title Case (≤ 70 chars, no #, no “quotes”).
Use a power verb + intrigue + keyword. Avoid click‑bait filler.

Must summarise: ${headlines.join(" | ")}${
			language !== DEFAULT_LANGUAGE ? `\nRespond in ${language}.` : ""
		}`;
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: ask }],
		});
		return choices[0].message.content.replace(/["“”]/g, "").trim();
	} catch {
		return null;
	}
}

async function fetchTrendingStory(category, geo = "US") {
	const id = resolveTrendsCategoryId(category);
	const url =
		`${TRENDS_API_URL}?` + qs.stringify({ geo, category: id, hours: 168 });

	try {
		const { data } = await axios.get(url, { timeout: 12_000 });
		if (
			Array.isArray(data?.stories) &&
			data.stories.length &&
			data.stories[0]?.title
		) {
			const s = data.stories[0];
			const first =
				Array.isArray(s.articles) && s.articles.length ? s.articles[0] : null;
			return {
				title: String(s.title).trim(),
				image: s.image || first?.image || null,
				articleUrl: first?.url || null,
				articleTitles: (s.articles || [])
					.slice(0, 3)
					.map((a) => String(a.title).trim()),
			};
		}
		throw new Error("empty trends payload");
	} catch (e) {
		console.warn(`[Trending] fetch failed → ${e.message}`);
		return null;
	}
}

async function scrapeArticleText(url) {
	if (!url) return null;
	try {
		const { data: html } = await axios.get(url, { timeout: 10_000 });
		const $ = cheerio.load(html);
		const body = $("article").text() || $("body").text();
		const cleaned = body
			.replace(/\s+/g, " ")
			.replace(/(Advertisement|Subscribe now|Sign up for.*?newsletter).*/gi, "")
			.trim();
		return cleaned.slice(0, 12_000) || null;
	} catch (e) {
		console.warn(`[Scrape] article failed → ${e.message}`);
		return null;
	}
}

/* fresh topic list via GPT */
const CURRENT_MONTH_YEAR = dayjs().format("MMMM YYYY");
const CURRENT_YEAR = dayjs().year();

async function topicFromCustomPrompt(text) {
	const make = (a) =>
		`
Attempt ${a}:
Give one click‑worthy title (≤70 chars, no hashtags, no quotes) set in ${CURRENT_MONTH_YEAR}.
Do NOT mention years before ${CURRENT_YEAR}.
<<<${text}>>>`.trim();

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
Attempt ${a}:
Return JSON array of 10 trending ${category} titles (${CURRENT_MONTH_YEAR}${loc}), no hashtags, ≤70 chars.${langLn}`.trim();

	for (let a = 1; a <= 2; a++) {
		try {
			const g = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: base(a) }],
			});
			const list = JSON.parse(
				strip(g.choices[0].message.content.trim()) || "[]"
			);
			if (Array.isArray(list) && list.length) return list;
		} catch {
			/* ignore */
		}
	}
	return [`Breaking ${category} Story – ${CURRENT_MONTH_YEAR}`];
}

/* ───────────────────────────────────────────────────────────────
 *  9.  Runway poll + retry
 * ───────────────────────────────────────────────────────────── */
async function pollRunway(id, tk, seg, lbl) {
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

async function retry(fn, max, seg, lbl) {
	let last;
	for (let a = 1; a <= max; a++) {
		try {
			console.log(`[Retry] ${lbl} attempt ${a}/${max}`);
			return await fn();
		} catch (e) {
			console.warn(`[Retry] ${lbl} attempt ${a} failed → ${e.message}`);
			last = e;
		}
	}
	throw last;
}

/* ───────────────────────────────────────────────────────────────
 * 10.  YouTube & Jamendo helpers  (unchanged)
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
			if (user.isModified() && user.role !== "admin") await user.save();
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
 * 11.  ElevenLabs TTS helper  (unchanged)
 * ───────────────────────────────────────────────────────────── */
async function elevenLabsTTS(text, language, outPath, category = "Other") {
	if (!ELEVEN_API_KEY) throw new Error("ELEVENLABS_API_KEY missing");
	const voiceId = ELEVEN_VOICES[language] || ELEVEN_VOICES[DEFAULT_LANGUAGE];
	const style = ELEVEN_STYLE_BY_CATEGORY[category] ?? 0.7;
	const payload = {
		text,
		model_id: "eleven_multilingual_v2",
		voice_settings: {
			stability: 0.15,
			similarity_boost: 0.92,
			style,
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
}

/* ───────────────────────────────────────────────────────────────
 * 12.  Main controller – createVideo
 * ───────────────────────────────────────────────────────────── */
exports.createVideo = async (req, res) => {
	const { category, ratio: ratioIn, duration: durIn } = req.body;

	/* quick validation */
	if (!category || !YT_CATEGORY_MAP[category])
		return res.status(400).json({ error: "Bad category" });
	if (!VALID_RATIOS.includes(ratioIn))
		return res.status(400).json({ error: "Bad ratio" });
	if (!goodDur(durIn)) return res.status(400).json({ error: "Bad duration" });

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
		if (!res.headersSent) res.status(500).json({ error: m });
	};

	sendPhase("INIT");
	console.log("send phaste `INIT`");
	res.setTimeout(0);

	try {
		/* pull fields */
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
		const country = (
			countryIn === "all countries" || !countryIn ? "US" : countryIn
		).trim();
		const customPrompt = customPromptRaw.trim();
		const ratio = ratioIn;
		const duration = +durIn;
		const [w, h] = ratio.split(":").map(Number);

		console.log(
			`[Job] user=${user.email}  cat=${category}  dur=${duration}s  geo=${country}`
		);

		/* ─────────────────────────────────────────────────────────
		 *  1.  Topic resolution
		 * ───────────────────────────────────────────────────────── */
		let topic = "",
			trendImage = null,
			trendArticleUrl = null,
			trendArticleTitles = null;

		const userOverrides = Boolean(videoImage) || customPrompt.length > 0;

		/* try Google‑Trends first if allowed */
		if (!userOverrides && category !== "Top5") {
			const story = await fetchTrendingStory(category, country);
			if (story) {
				topic = story.title;
				trendImage = story.image;
				trendArticleUrl = story.articleUrl;
				trendArticleTitles = story.articleTitles;
				console.log(`[Trending] candidate topic="${topic}"`);
			}
		}

		/* avoid per‑user duplicates */
		if (topic) {
			const dup = await Video.findOne({
				user: user._id,
				category,
				topic,
			}).select("_id");
			if (dup) {
				console.warn("[Duplicate] topic already used – picking new one");
				topic = "";
				trendImage = trendArticleUrl = trendArticleTitles = null;
			}
		}

		/* custom prompt path */
		if (customPrompt && !topic) {
			try {
				topic = await topicFromCustomPrompt(customPrompt);
			} catch {}
			trendArticleTitles = null; // prevent mismatch
		}

		/* fallback topic list */
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
			trendArticleTitles = null; // ensure coherence
		}
		console.log(`[Job] final topic="${topic}"`);

		/* ─────────────────────────────────────────────────────────
		 *  2.  Vision seed + article scrape
		 * ───────────────────────────────────────────────────────── */
		let seedImageUrl = videoImage?.url || trendImage || null;
		let seedImageDesc = null;
		let cldVariants = null; // keep IDs if you need them later

		if (seedImageUrl) {
			console.log("[Variation] creating cloudinary variant …");
			cldVariants = await uploadWithVariation(seedImageUrl, {
				folder: "aivideomatic",
			});
			/* use the 90 %-similar image for the rest of the pipeline */
			seedImageUrl = cldVariants.variant.url;
			console.log(
				`[Variation] orig=${cldVariants.original.public_id}  ` +
					`variant=${cldVariants.variant.public_id}`
			);

			console.log("[Vision] describing seed image …");
			seedImageDesc = await safeDescribeSeedImage(seedImageUrl);
			console.log("[Vision] →", seedImageDesc);
		}

		const articleText = await scrapeArticleText(trendArticleUrl);

		/* ─────────────────────────────────────────────────────────
		 *  3.  Segment plan & GPT scripts
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

		/* ensure exact sum */
		const delta = duration - segLens.reduce((a, b) => a + b, 0);
		if (Math.abs(delta) >= 1) segLens[segLens.length - 1] += delta;

		let segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));

		/* GPT segment prompt */
		const allowExplain = duration >= 25;
		const capTable = segWordCaps
			.map((w, i) => `Segment ${i + 1} ≤ ${w} words`)
			.join("  •  ");

		const segPrompt = `
Current date: ${dayjs().format("YYYY-MM-DD")}
We need a ${duration}s ${category} video titled "${topic}" split into ${segCnt} segments (${segLens.join(
			"/"
		)}). 
${capTable}
${
	category === "Top5"
		? `Segments 2‑6 must start with "#5:" … "#1:"${
				allowExplain
					? " followed by ≤6 extra words on why it ranks there."
					: " and contain only the concise label."
		  }`
		: ""
}
Return *strict* JSON array of objects with exactly two keys: "runwayPrompt" and "scriptText". Do NOT wrap the JSON in markdown. 
${TONE_HINTS[category] || ""}${
			language !== DEFAULT_LANGUAGE
				? `\nAll output must be in ${language}.`
				: ""
		}${articleText ? `\n---\nReference article:\n${articleText}` : ""}`.trim();

		console.log("[GPT] requesting segments …");
		const segments = await getSegments(segPrompt, segCnt);

		/* normalise possible plain‑string returns */
		for (let i = 0; i < segments.length; i++) {
			const s = segments[i];
			if (typeof s === "string")
				segments[i] = { runwayPrompt: "", scriptText: s };
			else {
				segments[i].runwayPrompt = s.runwayPrompt ?? "";
				segments[i].scriptText = s.scriptText ?? "";
			}
		}

		if (seedImageDesc) {
			segments.forEach((s) => {
				s.runwayPrompt = injectSeedDescription(
					s.runwayPrompt || "",
					seedImageDesc
				);
			});
		}

		/* shrink overlong lines */
		await Promise.all(
			segments.map((s, i) =>
				s.scriptText.trim().split(/\s+/).length <= segWordCaps[i]
					? s
					: (async () => {
							const ask = `
Rewrite in active voice, keep all facts, ≤ ${segWordCaps[i]} words.
One sentence only. No filler words.

“${s.scriptText}”`.trim();
							const { choices } = await openai.chat.completions.create({
								model: CHAT_MODEL,
								messages: [{ role: "user", content: ask }],
							});
							s.scriptText = choices[0].message.content.trim();
					  })()
			)
		);

		/* fine‑tune Top‑5 segment durations */
		if (category === "Top5") {
			const introLen = segLens[0];
			const newLens = [introLen];
			let total = introLen;
			for (let i = 1; i < segCnt; i++) {
				const words = segments[i].scriptText.trim().split(/\s+/).length;
				const min = Math.ceil(spokenSeconds(words) + 0.6);
				newLens.push(Math.max(segLens[i], min));
				total += newLens[i];
			}
			if (total > duration) {
				const scale = (duration - introLen) / (total - introLen + 1e-3);
				for (let i = 1; i < segCnt; i++) {
					newLens[i] = Math.max(
						Math.ceil(newLens[i] * scale),
						Math.ceil(
							spokenSeconds(segments[i].scriptText.trim().split(/\s+/).length)
						)
					);
				}
			}
			segLens = newLens;
			segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));
		}

		/* ─────────────────────────────────────────────────────────
		 *  4.  Global style, SEO title, tags
		 * ───────────────────────────────────────────────────────── */
		let globalStyle = "";
		try {
			const g = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [
					{
						role: "user",
						content: `Give a short cinematic style for "${topic}".`,
					},
				],
			});
			globalStyle = g.choices[0].message.content
				.replace(/^[-–•\s]+/, "")
				.trim();
		} catch {}

		/* If article titles don't match the final topic, discard them */
		if (
			trendArticleTitles?.length &&
			!trendArticleTitles.some((t) =>
				t.toLowerCase().includes(topic.toLowerCase().slice(0, 8))
			)
		)
			trendArticleTitles = null;

		let seoTitle = "";
		if (trendArticleTitles?.length) {
			seoTitle =
				(await generateSeoTitle(trendArticleTitles, category, language)) || "";
		}
		if (!seoTitle)
			seoTitle =
				category === "Top5"
					? /^top\s*5/i.test(topic)
						? topic
						: `Top 5: ${topic}`
					: `${category} Highlights: ${topic}`;

		const descResp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [
				{
					role: "user",
					content: `Write YouTube description (≤150 words) for "${seoTitle}", end with 5‑7 hashtags.`,
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
						content: `Return JSON array of 5‑8 tags for "${seoTitle}".`,
					},
				],
			});
			tags.push(
				...JSON.parse(strip(tagResp.choices[0].message.content.trim()))
			);
		} catch {}
		if (category === "Top5") tags.unshift("Top5");
		if (!tags.includes(BRAND_TAG)) tags.unshift(BRAND_TAG);

		/* ─────────────────────────────────────────────────────────
		 *  5.  Enhance runway prompts (human injection, style, etc.)
		 * ───────────────────────────────────────────────────────── */
		const humanCache = {};
		const prependCustom = (p) => (customPrompt ? `${customPrompt}, ${p}` : p);

		for (let i = 0; i < segCnt; i++) {
			let prompt = `${
				segments[i].runwayPrompt || ""
			}, ${globalStyle}, ${QUALITY_BONUS}`;
			prompt = await injectHumanIfNeeded(
				prompt,
				segments[i].scriptText,
				language,
				country,
				humanCache
			);

			prompt = await refineRunwayPrompt(prompt, segments[i].scriptText);
			prompt = `${prompt}, ${RUNWAY_NEGATIVE_PROMPT}`; // append global negatives first
			prompt = tunePromptForTopic(prompt, topic); // now topic helper can replace
			// finalise and store
			segments[i].runwayPrompt = prependCustom(prompt.replace(/^,\s*/, ""));
			if (!segments[i].negativePromptFull) {
				const matchedRule = TOPIC_RULES.find((r) => r.test.test(topic));
				const extraNeg =
					matchedRule && matchedRule.negative ? matchedRule.negative : "";
				segments[i].negativePromptFull =
					(extraNeg ? `${extraNeg}, ` : "") + RUNWAY_NEGATIVE_PROMPT;
			}
		}

		const fullScript = segments.map((s) => s.scriptText.trim()).join(" ");

		/* ─────────────────────────────────────────────────────────
		 *  6.  Build overlay for Top‑5
		 * ───────────────────────────────────────────────────────── */
		let overlay = "";
		if (category === "Top5") {
			let t = segLens[0];
			const draw = [];
			for (let i = 1; i < segCnt; i++) {
				const d = segLens[i];
				let label = segments[i].scriptText;
				const m = label.match(/^#\s*\d\s*:\s*(.+)$/i);
				if (m) label = m[1].trim();
				if (label.length > 60) label = label.slice(0, 57) + "…";
				const spoken = spokenSeconds(label.split(/\s+/).length);
				const showFrom = t.toFixed(2);
				const showTo = Math.min(t + spoken + 0.25, t + d - 0.1).toFixed(2);

				draw.push(
					`drawtext=fontfile='${FONT_PATH_FFMPEG}':text='${escTxt(label)}'` +
						`:fontsize=32:fontcolor=white:box=1:boxcolor=black@0.4:boxborderw=15:` +
						`x=(w-text_w)/2:y=(h-text_h)/2:enable='between(t,${showFrom},${showTo})'`
				);
				t += d;
			}
			overlay = `[0:v]${draw.join(",")}[vout]`;
			await checkOverlay(overlay, w, h, duration);
		}

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
		} catch {}

		/* ─────────────────────────────────────────────────────────
		 *  8.  PER‑SEGMENT VIDEO GENERATION
		 * ───────────────────────────────────────────────────────── */
		const clips = [];
		sendPhase("GENERATING_CLIPS", {
			msg: "Generating clips",
			total: segCnt,
			done: 0,
		});
		console.log("send phaste `GENERATING_CLIPS`");

		// Pre‑seed with the Cloudinary variant so doTtiItv()
		// skips the “text‑to‑image” call and goes straight to ITV.
		let reusableFallbackImage = seedImageUrl; // NEW

		for (let i = 0; i < segCnt; i++) {
			const d = segLens[i];
			const rw = Math.abs(5 - d) <= Math.abs(10 - d) ? 5 : 10;
			let clip = null;

			const announceFallback = (type, reason) =>
				sendPhase("FALLBACK", { segment: i + 1, type, reason });

			/* helpers ------------------------------------------------ */
			async function doTextToVideo(promptTextRaw, label, img = null) {
				/* --- 1. guard against prompt truncation ----------------------- */
				const promptText =
					promptTextRaw.length > PROMPT_CHAR_LIMIT
						? promptTextRaw.slice(0, PROMPT_CHAR_LIMIT)
						: promptTextRaw;

				/* --- 2. build payload ----------------------------------------- */
				const payload = {
					model: T2V_MODEL,
					promptText,
					ratio,
					duration: rw, // ‘rw’ & ‘ratio’ come from outer scope
					promptStrength: 0.85, // ✨ keeps the text in firm control
					negativePrompt: segments[i].negativePromptFull,
				};
				if (img) payload.promptImage = img;

				/* --- 3. fire → poll → download -------------------------------- */
				const id = await retry(
					async () => {
						const { data } = await axios.post(
							"https://api.dev.runwayml.com/v1/text_to_video",
							payload,
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
					i + 1,
					`t2v${label}`
				);

				const vidUrl = await retry(
					() => pollRunway(id, RUNWAY_ADMIN_KEY, i + 1, `poll(t2v${label})`),
					3,
					i + 1,
					`poll(t2v${label})`
				);

				const p = tmpFile(`seg_t2v${label}${i + 1}`, ".mp4");
				await new Promise((r, j) =>
					axios
						.get(vidUrl, { responseType: "stream" })
						.then(({ data }) =>
							data.pipe(fs.createWriteStream(p)).on("finish", r).on("error", j)
						)
				);
				return p;
			}

			async function doTtiItv(promptTextRaw, label) {
				/* -------- 1. clamp prompt length ------------ */
				const promptText =
					promptTextRaw.length > PROMPT_CHAR_LIMIT
						? promptTextRaw.slice(0, PROMPT_CHAR_LIMIT)
						: promptTextRaw;

				/* -------- 2. Step A – get / reuse an image --- */
				let imgUrl = reusableFallbackImage;
				if (!imgUrl) {
					const idImg = await retry(
						async () => {
							const { data } = await axios.post(
								"https://api.dev.runwayml.com/v1/text_to_image",
								{
									model: TTI_MODEL,
									promptText,
									ratio,
									promptStrength: 0.9, // ✨ tighter adherence
									negativePrompt: segments[i].negativePromptFull,
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
						i + 1,
						`tti${label}`
					);

					imgUrl = await retry(
						() =>
							pollRunway(idImg, RUNWAY_ADMIN_KEY, i + 1, `poll(img${label})`),
						3,
						i + 1,
						`poll(img${label})`
					);

					/* store for subsequent segments */
					reusableFallbackImage = imgUrl;
				}

				/* -------- 3. Step B – animate that image ----- */
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
								negativePrompt: segments[i].negativePromptFull,
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
					i + 1,
					`itv${label}`
				);

				const vidUrl = await retry(
					() => pollRunway(idVid, RUNWAY_ADMIN_KEY, i + 1, `poll(vid${label})`),
					3,
					i + 1,
					`poll(vid${label})`
				);

				/* -------- 4. download & hand back ---------- */
				const p = tmpFile(`seg_${label}${i + 1}`, ".mp4");
				await new Promise((r, j) =>
					axios
						.get(vidUrl, { responseType: "stream" })
						.then(({ data }) =>
							data.pipe(fs.createWriteStream(p)).on("finish", r).on("error", j)
						)
				);
				return p;
			}

			/* tier A – t2v with seed */
			try {
				if (seedImageUrl)
					clip = await doTextToVideo(
						segments[i].runwayPrompt,
						"_seed",
						seedImageUrl
					);
			} catch (e) {
				console.warn(`[Seg ${i + 1}] t2v‑seed failed → ${e.message}`);
				announceFallback("t2v_seed", e.message);
			}

			/* tier B – pure t2v */
			if (!clip) {
				try {
					clip = await doTextToVideo(segments[i].runwayPrompt, "");
				} catch (e) {
					console.warn(`[Seg ${i + 1}] t2v failed → ${e.message}`);
					announceFallback("t2v", e.message);
				}
			}

			/* tier C – tti + itv */
			if (!clip) {
				try {
					clip = await doTtiItv(segments[i].runwayPrompt, "");
				} catch (e) {
					console.warn(`[Seg ${i + 1}] tti+itv failed → ${e.message}`);
					announceFallback("tti_itv", e.message);
				}
			}

			/* tier D – safe fallback prompt */
			if (!clip) {
				try {
					const safe = await generateFallbackPrompt(topic, category);
					console.log(`[Seg ${i + 1}] safe prompt →`, safe);
					clip = await doTtiItv(safe, "_fallback");
				} catch (e) {
					console.warn(`[Seg ${i + 1}] safe prompt failed → ${e.message}`);
					announceFallback("safePrompt", e.message);
				}
			}

			/* tier E – black dummy */
			if (!clip) {
				console.warn(`[Seg ${i + 1}] using dummy clip`);
				announceFallback("dummy", "black clip");
				clip = await makeDummyClip(w, h, rw);
			}

			/* fix length */
			const fixed = tmpFile(`fx_${i + 1}`, ".mp4");
			await exactLen(clip, d, fixed);

			/* ───────── single‑frame QA  ───────── */
			const still = tmpFile("frame", ".jpg");
			await ffmpegPromise((c) =>
				c
					.input(norm(fixed))
					.outputOptions("-ss", (d / 2).toString(), "-frames:v", "1", "-y")
					.save(norm(still))
			);

			// Build rule set **once per topic** (customise as you grow)
			const qaRules = [];
			if (/football|soccer/i.test(topic)) {
				qaRules.push({ q: "Exactly one soccer ball visible?" });
			}
			if (
				/judge/i.test(topic) ||
				/person|player|man|woman/i.test(segments[i].scriptText)
			) {
				qaRules.push({ q: "Is the person's face normal (no crossed eyes)?" });
			}

			let passed = true;
			if (qaRules.length) {
				try {
					passed = await validateClipStill(still, qaRules);
				} catch (e) {
					console.warn("[Vision QA] skipped ->", e.message);
				}
			}
			fs.unlinkSync(still);

			/* If the still fails, regenerate **once** with a safe prompt */
			if (!passed) {
				console.warn(
					`[Seg ${i + 1}] QA failed – regenerating with fallback prompt`
				);
				try {
					const safePrompt = await generateFallbackPrompt(topic, category);
					const redo = await doTtiItv(safePrompt, "_qa_retry");
					await exactLen(redo, d, fixed); // overwrite previous fix
					fs.unlinkSync(redo);
				} catch (e) {
					console.warn(`[Seg ${i + 1}] QA retry failed – keeping original`);
				}
			}

			fs.unlinkSync(clip);
			clips.push(fixed);

			sendPhase("GENERATING_CLIPS", {
				msg: `Rendering segment ${i + 1}/${segCnt}`,
				total: segCnt,
				done: i + 1,
			});

			console.log("send phaste `GENERATING_CLIPS`");
		}

		/* ─────────────────────────────────────────────────────────
		 *  9.  Concatenate video
		 * ───────────────────────────────────────────────────────── */
		sendPhase("ASSEMBLING_VIDEO", { msg: "Concatenating clips…" });
		console.log("send phaste `ASSEMBLING_VIDEO`");

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
		clips.forEach((p) => fs.unlinkSync(p));

		/* exact duration */
		const silentFixed = tmpFile("silent_fix", ".mp4");
		await exactLen(silent, duration, silentFixed);
		fs.unlinkSync(silent);

		/* ─────────────────────────────────────────────────────────
		 * 10.  Voice‑over & music
		 * ───────────────────────────────────────────────────────── */
		sendPhase("ADDING_VOICE_MUSIC", { msg: "Creating audio layer" });
		console.log("send phase `ADDING_VOICE_MUSIC`");
		const fixedPieces = [];
		for (let i = 0; i < segCnt; i++) {
			const raw = tmpFile(`tts_raw_${i + 1}`, ".mp3");
			const fixed = tmpFile(`tts_fix_${i + 1}`, ".wav");
			const txt = improveTTSPronunciation(segments[i].scriptText);
			try {
				await elevenLabsTTS(txt, language, raw, category);
			} catch {
				/* fallback to OpenAI shimmer */
				const tts = await openai.audio.speech.create({
					model: "tts-1-hd",
					voice: "shimmer",
					speed: 1.0,
					input: txt,
					format: "mp3",
				});
				fs.writeFileSync(raw, Buffer.from(await tts.arrayBuffer()));
			}
			await exactLenAudio(raw, segLens[i], fixed);
			fs.unlinkSync(raw);
			fixedPieces.push(fixed);
		}

		/* concat audio segments */
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
		fs.unlinkSync(listFile);
		fixedPieces.forEach((p) => fs.unlinkSync(p));

		/* mix with bg music */
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
			fs.unlinkSync(music);

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
			fs.unlinkSync(trim);
		} else {
			await ffmpegPromise((c) =>
				c
					.input(norm(ttsJoin))
					.audioFilters("volume=1.4")
					.outputOptions("-c:a", "pcm_s16le", "-y")
					.save(norm(mixedRaw))
			);
		}
		fs.unlinkSync(ttsJoin);
		await exactLenAudio(mixedRaw, duration, mixed);
		fs.unlinkSync(mixedRaw);

		/* ─────────────────────────────────────────────────────────
		 * 11.  Mux audio + video
		 * ───────────────────────────────────────────────────────── */
		sendPhase("SYNCING_VOICE_MUSIC", { msg: "Muxing final video" });
		console.log("send phase `SYNCING_VOICE_MUSIC`");
		const safeTitle = seoTitle
			.toLowerCase()
			.replace(/[^\w\d]+/g, "_")
			.replace(/^_+|_+$/g, "");
		const finalPath = tmpFile(safeTitle, ".mp4");

		await ffmpegPromise((c) => {
			c.input(norm(silentFixed)).input(norm(mixed));
			if (category === "Top5") {
				c.complexFilter([overlay]).outputOptions(
					"-map",
					"[vout]",
					"-map",
					"1:a",
					"-c:v",
					"libx264",
					"-preset",
					"veryfast",
					"-crf",
					"20",
					"-c:a",
					"aac",
					"-t",
					String(duration),
					"-y"
				);
			} else {
				c.outputOptions(
					"-map",
					"0:v",
					"-map",
					"1:a",
					"-c:v",
					"copy",
					"-c:a",
					"aac",
					"-t",
					String(duration),
					"-y"
				);
			}
			return c.save(norm(finalPath));
		});
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
								tags: [...new Set(tags)].slice(0, 15),
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
				console.log("[YouTube] video uploaded →", youtubeLink);
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
			tags: [...new Set(tags)],
			script: fullScript,
			ratio,
			duration,
			model: T2V_MODEL,
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
			let next = dayjs(startDate)
				.hour(+timeOfDay.split(":")[0])
				.minute(+timeOfDay.split(":")[1])
				.second(0);

			if (next.isBefore(dayjs())) {
				if (type === "daily") next = next.add(1, "day");
				if (type === "weekly") next = next.add(1, "week");
				if (type === "monthly") next = next.add(1, "month");
			}

			await new Schedule({
				user: user._id,
				video: doc._id,
				scheduleType: type,
				timeOfDay,
				startDate: dayjs(startDate).toDate(),
				endDate: endDate ? dayjs(endDate).toDate() : undefined,
				nextRun: next.toDate(),
				active: true,
			}).save();

			doc.scheduled = true;
			await doc.save();
			sendPhase("VIDEO_SCHEDULED", { msg: "Scheduled" });
			console.log("[Schedule] video scheduled for");
		}

		/* ─────────────────────────────────────────────────────────
		 * 14.  DONE
		 * ───────────────────────────────────────────────────────── */
		sendPhase("COMPLETED", {
			id: doc._id,
			youtubeLink,
			phases: JSON.parse(JSON.stringify(history)),
		});
		console.log("[createVideo] DONE", doc._id, youtubeLink);
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
