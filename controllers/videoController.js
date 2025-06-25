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

const Video = require("../models/Video");
const Schedule = require("../models/Schedule");
const {
	ALL_TOP5_TOPICS,
	googleTrendingCategoriesId,
} = require("../assets/utils");

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
	} catch {}
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

const RUNWAY_VERSION = "2024-11-06";
const POLL_INTERVAL_MS = 2_000;
const MAX_POLL_ATTEMPTS = 180;

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

const T2V_MODEL = "gen2_turbo";
const ITV_MODEL = "gen4_turbo";
const TTI_MODEL = "gen4_image";

const QUALITY_BONUS =
	"photorealistic, ultra‑detailed, HDR, 8K, cinema lighting, award‑winning, trending on artstation";
const MOTION_QUALITY =
	"natural motion, realistic physics, nuanced facial expressions, authentic gestures";
const RUNWAY_NEGATIVE_PROMPT = [
	"duplicate",
	"bad anatomy",
	"deformed hands",
	"extra fingers",
	"extra limbs",
	"missing limbs",
	"multiple heads",
	"mutated",
	"disfigured",
	"doll‑like skin",
	"oversaturated",
	"poor lighting",
	"low contrast",
	"grainy",
	"lowres",
	"jpeg artifacts",
	"blur",
	"watermark",
	"text",
	"logo",
	"nsfw",
	"nudity",
	"violence",
	"gore",
	"blood",
	"backward motion",
	"reversed walk",
	"unnatural gait",
	"physics‑defying",
	"distorted perspective",
].join(", ");
const HUMAN_SAFETY =
	"anatomically correct, two eyes, one head, normal limbs, realistic proportions, natural waist";

const CHAT_MODEL = "gpt-4o";

const ELEVEN_VOICES = {
	English: [
		"21m00Tcm4TlvDq8ikWAM",
		"AZnzlk1XvdvUeBnXmlld",
		"EXAVITQu4vr4xnSDxMaL",
	],
	العربية: ["CYw3kZ02Hs0563khs1Fj", "TX3LPaxmHKxFdv7QDePo"],
	Français: ["gqjD3Awy6ZnJf2el9DnG", "MF3mGyEYCl7XYWbV9V6O"],
	Deutsch: ["IFHEeWG1IGkfXpxmB1vN", "ErXwobaYiN019PkySvjV"],
	हिंदी: ["ykoxtvL6VZTyas23mE9F", "LcfcDJNUP1GQjkzn1xUU"],
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

async function refineRunwayPrompt(initialPrompt, scriptText) {
	const ask = `
Rewrite the following as a *production‑ready* text‑to‑video prompt.

Rules
• ≤ 25 words.  
• Begin with one vivid atmosphere adjective.  
• Use present‑tense, imperative verbs (e.g. “Soaring drone shot of …”).  
• NO quote marks, NO brand names, NO proper names.  
• Keep only essential subjects & actions; drop metadata, camera jargon.  
• Inject strong cinematic language (lighting, mood, lens) where helpful.  
• End with a concise style tag if absent.  

Input
«${initialPrompt}»
---
Context
${scriptText}`.trim();

	try {
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: ask }],
		});
		return choices[0].message.content.replace(/["“”]/g, "").trim();
	} catch (e) {
		console.warn("[GPT] refineRunwayPrompt failed – using original");
		return initialPrompt;
	}
}

async function generateFallbackPrompt(topic, category) {
	const ask = `
In ≤ 12 words craft a TOP‑TIER, film‑grade scene (no names, no brands) that visually
captures today’s hottest ${category} topic: “${topic}”.
Start with a mood adjective, use concrete nouns & active verbs.`.trim();

	const { choices } = await openai.chat.completions.create({
		model: CHAT_MODEL,
		messages: [{ role: "user", content: ask }],
	});
	return `${choices[0].message.content.trim()}, ${QUALITY_BONUS}`;
}

async function describeHuman(language, country) {
	const locale =
		country && country !== "all countries" ? `from ${country}` : "western";
	const prompt = `Describe a real human ${locale} in ≤15 words; include attire, mood, lens, lighting.`;

	const { choices } = await openai.chat.completions.create({
		model: CHAT_MODEL,
		messages: [{ role: "user", content: prompt }],
	});
	return choices[0].message.content.trim();
}

async function describePerson(name) {
	const prompt = `
In ≤ 20 words depict a photorealistic person who could be mistaken for ${name}.
Detail: face shape, skin tone, hair colour & style, eye shape, age range, build,
attire, mood, lens & lighting. No names.`.trim();

	const { choices } = await openai.chat.completions.create({
		model: CHAT_MODEL,
		messages: [{ role: "user", content: prompt }],
	});
	return choices[0].message.content.trim();
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

	if (name) {
		const celeb = name[1];
		if (!cache[celeb]) cache[celeb] = await describePerson(celeb);
		if (!runwayPrompt.startsWith(cache[celeb]))
			return `${cache[celeb]}, ${HUMAN_SAFETY}, ${runwayPrompt}`;
		return runwayPrompt;
	}

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

async function describeSeedImage(url) {
	const ask = `
Describe *everything* visible in a single sentence (≤ 70 words).

Include
• age range, gender, ethnicity, build, facial features, hair colour & style  
• attire & accessories  
• pose, expression & camera angle  
• environment, colour palette, lighting style, depth of field, mood

Use present tense. No names, no brands, no guessing unseen parts.`.trim();

	try {
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [
				{
					role: "user",
					content: [
						{ type: "text", text: ask },
						{ type: "image_url", image_url: { url } },
					],
				},
			],
		});
		return choices[0].message.content.trim();
	} catch (e) {
		console.warn("[Vision] image description failed:", e.message);
		return null;
	}
}

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
		`${TRENDS_API_URL}?` + qs.stringify({ geo, category: id, hours: 24 });

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
			: "";
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
		} catch {}
	}
	return [`Breaking ${category} Story – ${CURRENT_MONTH_YEAR}`];
}

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

function chooseVoice(language) {
	const list = ELEVEN_VOICES[language] || ELEVEN_VOICES[DEFAULT_LANGUAGE];
	return Array.isArray(list) ? choose(list) : list;
}
function jitterStyle(base) {
	const j = base * (0.9 + Math.random() * 0.3);
	return Math.max(0.3, Math.min(1.4, +j.toFixed(2)));
}

async function elevenLabsTTS(text, language, outPath, category = "Other") {
	if (!ELEVEN_API_KEY) throw new Error("ELEVENLABS_API_KEY missing");
	const voiceId = chooseVoice(language);
	const style = jitterStyle(ELEVEN_STYLE_BY_CATEGORY[category] ?? 0.7);
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

exports.createVideo = async (req, res) => {
	const { category, ratio: ratioIn, duration: durIn } = req.body;

	if (!category || !YT_CATEGORY_MAP[category])
		return res.status(400).json({ error: "Bad category" });
	if (!VALID_RATIOS.includes(ratioIn))
		return res.status(400).json({ error: "Bad ratio" });
	if (!goodDur(durIn)) return res.status(400).json({ error: "Bad duration" });

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
		const country = (countryIn || "US").trim();
		const customPrompt = customPromptRaw.trim();
		const ratio = ratioIn;
		const duration = +durIn;
		const [w, h] = ratio.split(":").map(Number);

		console.log(
			`[Job] user=${user.email}  cat=${category}  dur=${duration}s  geo=${country}`
		);

		let topic = "",
			trendImage = null,
			trendArticleUrl = null,
			trendArticleTitles = null;

		const userOverrides = Boolean(videoImage) || customPrompt.length > 0;

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

		if (customPrompt && !topic) {
			try {
				topic = await topicFromCustomPrompt(customPrompt);
			} catch {}
			trendArticleTitles = null;
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
			trendArticleTitles = null;
		}
		console.log(`[Job] final topic="${topic}"`);

		let seedImageUrl = videoImage?.url || trendImage || null;
		let seedImageDesc = null;
		if (seedImageUrl) {
			console.log("[Vision] describing seed image …");
			seedImageDesc = await describeSeedImage(seedImageUrl);
			console.log("[Vision] →", seedImageDesc);
		}

		const articleText = await scrapeArticleText(trendArticleUrl);

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

		let segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));

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

		for (let i = 0; i < segments.length; i++) {
			const s = segments[i];
			if (typeof s === "string")
				segments[i] = { runwayPrompt: "", scriptText: s };
			else {
				segments[i].runwayPrompt = s.runwayPrompt ?? "";
				segments[i].scriptText = s.scriptText ?? "";
			}
		}

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

		const humanCache = {};
		const prependCustom = (p) => (customPrompt ? `${customPrompt}, ${p}` : p);

		for (let i = 0; i < segCnt; i++) {
			let prompt = `${
				segments[i].runwayPrompt || ""
			}, ${globalStyle}, ${QUALITY_BONUS}, ${MOTION_QUALITY}`;
			prompt = await injectHumanIfNeeded(
				prompt,
				segments[i].scriptText,
				language,
				country,
				humanCache
			);
			prompt = await refineRunwayPrompt(prompt, segments[i].scriptText);
			segments[i].runwayPrompt = prependCustom(
				`${prompt}, ${RUNWAY_NEGATIVE_PROMPT}`.replace(/^,\s*/, "")
			);
		}

		const fullScript = segments.map((s) => s.scriptText.trim()).join(" ");

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

		const clips = [];
		sendPhase("GENERATING_CLIPS", {
			msg: "Generating clips",
			total: segCnt,
			done: 0,
		});

		for (let i = 0; i < segCnt; i++) {
			const d = segLens[i];
			const rw = Math.abs(5 - d) <= Math.abs(10 - d) ? 5 : 10;
			let clip = null;

			const announceFallback = (type, reason) =>
				sendPhase("FALLBACK", { segment: i + 1, type, reason });

			async function doTextToVideo(promptText, label, img = null) {
				const payload = {
					model: T2V_MODEL,
					promptText,
					ratio,
					duration: rw,
					negativePrompt: RUNWAY_NEGATIVE_PROMPT,
				};
				if (img) payload.promptImage = img;

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
			async function doTtiItv(promptText, label) {
				const idImg = await retry(
					async () => {
						const { data } = await axios.post(
							"https://api.dev.runwayml.com/v1/text_to_image",
							{ model: TTI_MODEL, promptText, ratio },
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

				const imgUrl = await retry(
					() => pollRunway(idImg, RUNWAY_ADMIN_KEY, i + 1, `poll(img${label})`),
					3,
					i + 1,
					`poll(img${label})`
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

			if (!clip) {
				try {
					clip = await doTextToVideo(segments[i].runwayPrompt, "");
				} catch (e) {
					console.warn(`[Seg ${i + 1}] t2v failed → ${e.message}`);
					announceFallback("t2v", e.message);
				}
			}

			if (!clip) {
				try {
					clip = await doTtiItv(segments[i].runwayPrompt, "");
				} catch (e) {
					console.warn(`[Seg ${i + 1}] tti+itv failed → ${e.message}`);
					announceFallback("tti_itv", e.message);
				}
			}

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

			if (!clip) {
				console.warn(`[Seg ${i + 1}] using dummy clip`);
				announceFallback("dummy", "black clip");
				clip = await makeDummyClip(w, h, rw);
			}

			const fixed = tmpFile(`fx_${i + 1}`, ".mp4");
			await exactLen(clip, d, fixed);
			fs.unlinkSync(clip);
			clips.push(fixed);

			sendPhase("GENERATING_CLIPS", {
				msg: `Rendering segment ${i + 1}/${segCnt}`,
				total: segCnt,
				done: i + 1,
			});
		}

		sendPhase("ASSEMBLING_VIDEO", { msg: "Concatenating clips…" });
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

		const silentFixed = tmpFile("silent_fix", ".mp4");
		await exactLen(silent, duration, silentFixed);
		fs.unlinkSync(silent);

		sendPhase("ADDING_VOICE_MUSIC", { msg: "Creating audio layer" });

		const fixedPieces = [];
		for (let i = 0; i < segCnt; i++) {
			const raw = tmpFile(`tts_raw_${i + 1}`, ".mp3");
			const fixed = tmpFile(`tts_fix_${i + 1}`, ".wav");
			const txt = improveTTSPronunciation(segments[i].scriptText);
			try {
				await elevenLabsTTS(txt, language, raw, category);
			} catch {
				const tts = await openai.audio.speech.create({
					model: "tts-1-hd",
					voice: choose(["alloy", "fable", "onyx", "nova", "shimmer"]),
					speed: 0.95 + Math.random() * 0.1,
					input: txt,
					format: "mp3",
				});
				fs.writeFileSync(raw, Buffer.from(await tts.arrayBuffer()));
			}
			await exactLenAudio(raw, segLens[i], fixed);
			fs.unlinkSync(raw);
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
		fs.unlinkSync(listFile);
		fixedPieces.forEach((p) => fs.unlinkSync(p));

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

		sendPhase("SYNCING_VOICE_MUSIC", { msg: "Muxing final video" });

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

		let youtubeLink = "";
		try {
			const tokens = await refreshYouTubeTokensIfNeeded(user, req);
			const oauth2 = buildYouTubeOAuth2Client(tokens);
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
			}
		} catch (e) {
			console.warn("[YouTube] upload skipped →", e.message);
		}

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
		});

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
		}

		sendPhase("COMPLETED", {
			id: doc._id,
			youtubeLink,
			phases: JSON.parse(JSON.stringify(history)),
		});
		res.end();
	} catch (err) {
		console.error("[createVideo] ERROR", err);
		sendErr(err.message || "Internal error");
	}
};

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
