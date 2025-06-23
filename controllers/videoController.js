/*  videoController.js  — dynamic, music‑safe, full‑log edition 2025‑06‑23  */
/* eslint-disable no-await-in-loop, camelcase, max-len */
"use strict";

/* ────────────────────────────────────────────────────────────────────────── */
/*  DEPENDENCIES                                                             */
/* ────────────────────────────────────────────────────────────────────────── */
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const child_process = require("child_process");

const axios = require("axios");
const dayjs = require("dayjs");
const cheerio = require("cheerio"); // ← HTML scraping
const qs = require("querystring"); // ← query‑string helper

const { OpenAI } = require("openai");
const { google } = require("googleapis");
const ffmpeg = require("fluent-ffmpeg");

/* Mongoose models ---------------------------------------------------------- */
const Video = require("../models/Video");
const Schedule = require("../models/Schedule");
const {
	ALL_TOP5_TOPICS,
	googleTrendingCategoriesId,
} = require("../assets/utils");

/* ────────────────────────────────────────────────────────────────────────── */
/*  RUNTIME DEPENDENCY GUARDS                                                */
/* ────────────────────────────────────────────────────────────────────────── */
function assertExists(condition, msg) {
	if (!condition) {
		console.error(`[Startup] FATAL – ${msg}`);
		process.exit(1);
	}
}

/* ────────────────────────────────────────────────────────────────────────── */
/*  FFmpeg bootstrap                                                         */
/* ────────────────────────────────────────────────────────────────────────── */
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  FONT discovery                                                           */
/* ────────────────────────────────────────────────────────────────────────── */
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  CONSTANTS & CONFIG                                                       */
/* ────────────────────────────────────────────────────────────────────────── */
const RUNWAY_VERSION = "2024-11-06";
const POLL_INTERVAL_MS = 2_000;
const MAX_POLL_ATTEMPTS = 180;

const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });
const JAMENDO_ID = process.env.JAMENDO_CLIENT_ID;
const RUNWAY_ADMIN_KEY = process.env.RUNWAYML_API_SECRET;
const ELEVEN_API_KEY = process.env.ELEVENLABS_API_KEY;

/* video + prompt config */
const VALID_RATIOS = [
	"1280:720",
	"720:1280",
	"1104:832",
	"832:1104",
	"960:960",
	"1584:672",
];
const WORDS_PER_SEC = 1.8;
const QUALITY_BONUS =
	"photorealistic, ultra‑detailed, HDR, 8K, cinema lighting, bokeh, volumetric fog";
const RUNWAY_NEGATIVE_PROMPT =
	"duplicate, extra limbs, multiple heads, distorted, blurry, watermark, text, logo, lowres, malformed";
const HUMAN_SAFETY =
	"anatomically correct, two eyes, one head, normal limbs, realistic proportions, natural waist";

/* ElevenLabs voices */
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

const CHAT_MODEL = "gpt-4o";

/* misc helpers */
function spokenSeconds(words) {
	return +(words / WORDS_PER_SEC).toFixed(2);
}
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
	Health: "22",
	World: "0",
	Lifestyle: "0",
	Science: "0",
	Other: "0",
	Top5: "0",
};
const BRAND_TAG = "AiVideomatic";
const BRAND_CREDIT = "Powered by AiVideomatic";

/* ────────────────────────────────────────────────────────────────────────── */
/*  UTILS                                                                    */
/* ────────────────────────────────────────────────────────────────────────── */
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

/* pronunciation smoother for TTS */
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  GPT JSON segment parser                                                  */
/* ────────────────────────────────────────────────────────────────────────── */
function parseSegmentsSafe(raw, expectedCount) {
	raw = strip(raw.trim());
	if (!raw.trim().startsWith("[")) raw = `[${raw}]`;
	try {
		const j = JSON.parse(raw);
		return Array.isArray(j) ? j : Object.values(j);
	} catch {}
	try {
		const j = JSON.parse(
			raw.replace(/(['`])([^'`]*?)\1/g, (m, q, s) => `"${s}"`)
		);
		return Array.isArray(j) ? j : Object.values(j);
	} catch {}
	return null;
}
async function getSegments(segPrompt, segCnt) {
	for (let attempt = 1; attempt <= 3; attempt++) {
		const rsp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: segPrompt }],
		});
		let rawResponse = rsp.choices[0].message.content;
		let seg = parseSegmentsSafe(rawResponse, segCnt);
		if (seg && seg.length === segCnt) return seg;
		console.warn(`[GPT] segments parse failed on attempt ${attempt}`);
	}
	throw new Error("GPT segment JSON malformed after 3 attempts");
}

/* create a solid‑colour placeholder clip (used on failures) */
async function makeDummyClip(width, height, duration) {
	if (!hasLavfi) {
		throw new Error("FFmpeg built without lavfi – cannot create dummy clip");
	}
	const out = tmpFile("dummy", ".mp4");
	await ffmpegPromise((c) =>
		c
			.input(`color=c=black:s=${width}x${height}:d=${duration}`)
			.inputOptions("-f", "lavfi")
			.outputOptions(
				"-c:v",
				"libx264",
				"-t",
				String(duration),
				"-pix_fmt",
				"yuv420p",
				"-y"
			)
			.save(norm(out))
	);
	return out;
}

/* ────────────────────────────────────────────────────────────────────────── */
/*  FFmpeg helpers                                                           */
/* ────────────────────────────────────────────────────────────────────────── */
function ffmpegPromise(cfg) {
	return new Promise((res, rej) => {
		const proc = cfg(ffmpeg()) || ffmpeg();
		proc
			.on("start", (cmd) => console.log(`[FFmpeg] ${cmd}`))
			.on("end", () => res())
			.on("error", (e) => rej(e));
	});
}

/* length helpers */
async function exactLen(src, target, out) {
	const meta = await new Promise((r, j) =>
		ffmpeg.ffprobe(src, (e, d) => (e ? j(e) : r(d)))
	);
	const diff = +(target - meta.format.duration).toFixed(3);
	await ffmpegPromise((c) => {
		c.input(norm(src));
		if (diff < -0.05) c.outputOptions("-t", String(target));
		else if (diff > 0.05) c.videoFilters(`tpad=stop_duration=${diff}`);
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
		} else if (diff > 0.05) {
			c.audioFilters(`apad=pad_dur=${diff}`);
		} else {
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

/* overlay dry‑run (throws on bad filter) */
async function checkOverlay(filter, w, h, d) {
	if (!hasLavfi) return;
	const vf = filter.replace(/\[vout\]$/, "");
	const tmp = tmpFile("dummy", ".mp4");
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  GPT helpers                                                              */
/* ────────────────────────────────────────────────────────────────────────── */
async function refineRunwayPrompt(initialPrompt, scriptText) {
	const ask = `Refine to ≤25 words, no quotes:\n${initialPrompt}\n---\n${scriptText}`;
	console.log("[GPT] refineRunwayPrompt");
	try {
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: ask }],
		});
		return choices[0].message.content.trim();
	} catch (e) {
		console.warn("[GPT] refineRunwayPrompt failed → using original");
		return initialPrompt;
	}
}

async function generateFallbackPrompt(topic, category) {
	const ask = `In ≤12 words, give a cinematic, vivid scene (no names) illustrating latest ${category} topic: "${topic}".`;
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
async function describePerson(name, language) {
	const prompt = `Describe a person similar to ${name} in ≤15 words; build, hair, eyes, ethnicity, attire, lens, lighting.`;
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
	const nameMatch = scriptText.match(/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b/);
	if (nameMatch) {
		const celeb = nameMatch[1];
		if (!cache[celeb]) cache[celeb] = await describePerson(celeb, language);
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
		return `${cache.humanDesc}, ${HUMAN_SAFETY}, ${runwayPrompt}`;
	}
	return runwayPrompt;
}

async function describeSeedImage(imageUrl) {
	const ask =
		"Describe every visible subject (include attire, build, hair colour), background and mood in ≤40 words, no proper names.";

	try {
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [
				{
					role: "user",
					content: [
						{ type: "text", text: ask },
						{ type: "image_url", image_url: { url: imageUrl } },
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  TRENDING story helpers                                                   */
/* ────────────────────────────────────────────────────────────────────────── */
function resolveTrendsCategoryId(label) {
	const entry = googleTrendingCategoriesId.find((c) => c.category === label);
	return entry ? entry.ids[0] : 0;
}

const TRENDS_API_URL =
	process.env.TRENDS_API_URL || "http://localhost:8102/api/google-trends";

async function fetchTrendingStory(categoryLabel, geo = "US") {
	const categoryId = resolveTrendsCategoryId(categoryLabel);
	const url =
		`${TRENDS_API_URL}?` +
		qs.stringify({ geo, category: categoryId, hours: 24 });
	try {
		const { data } = await axios.get(url, { timeout: 12_000 });
		if (
			Array.isArray(data?.stories) &&
			data.stories.length &&
			data.stories[0]?.title
		) {
			const s = data.stories[0];
			const firstArticle =
				Array.isArray(s.articles) && s.articles.length ? s.articles[0] : null;
			return {
				title: String(s.title).trim(),
				image: s.image || firstArticle?.image || null,
				articleUrl: firstArticle?.url || null,
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  Topic helpers (GPT title logic)                                          */
/* ────────────────────────────────────────────────────────────────────────── */
const CURRENT_MONTH_YEAR = dayjs().format("MMMM YYYY");
const CURRENT_YEAR = dayjs().year();

async function topicFromCustomPrompt(text) {
	const basePrompt = (attempt) =>
		`
Attempt ${attempt}:
Give one click‑worthy title (≤70 chars, no hashtags, no quotes) set in ${CURRENT_MONTH_YEAR}.
Do NOT mention years before ${CURRENT_YEAR}.
<<<${text}>>>`.trim();
	for (let a = 1; a <= 2; a++) {
		const { choices } = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [{ role: "user", content: basePrompt(a) }],
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
		language && language !== DEFAULT_LANGUAGE ? ` Respond in ${language}.` : "";
	const basePrompt = (attempt) =>
		`
Attempt ${attempt}:
Return JSON array of 10 trending ${category} titles (${CURRENT_MONTH_YEAR}${loc}), no hashtags, ≤70 chars.${langLn}`.trim();
	for (let attempt = 1; attempt <= 2; attempt++) {
		try {
			const g = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: basePrompt(attempt) }],
			});
			const list = JSON.parse(
				strip(g.choices[0].message.content.trim()) || "[]"
			);
			if (Array.isArray(list) && list.length) return list;
		} catch {}
	}
	return [`Breaking ${category} Story – ${CURRENT_MONTH_YEAR}`];
}

/* ────────────────────────────────────────────────────────────────────────── */
/*  Runway poll + retry helpers                                              */
/* ────────────────────────────────────────────────────────────────────────── */
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

/* ────────────────────────────────────────────────────────────────────────── */
/*  YouTube + Jamendo helpers                                                */
/* ────────────────────────────────────────────────────────────────────────── */
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

/* ───────────────────────────────────────────────────────────── */
/*  ElevenLabs TTS                                               */
/* ───────────────────────────────────────────────────────────── */
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

/* ───────────────────────────────────────────────────────────── */
/*  MAIN CONTROLLER                                              */
/* ───────────────────────────────────────────────────────────── */
exports.createVideo = async (req, res) => {
	res.setHeader("Content-Type", "text/event-stream");
	res.setHeader("Cache-Control", "no-cache");
	res.setHeader("Connection", "keep-alive");
	const sendPhase = (phase, extra = {}) =>
		res.write(`data:${JSON.stringify({ phase, extra })}\n\n`);
	sendPhase("INIT");
	res.setTimeout(0);

	try {
		/* ─── Parse & validate input ─────────────────────────── */
		const {
			category,
			ratio: ratioIn,
			duration: durIn,
			language: langIn,
			country: countryIn,
			customPrompt: customPromptRaw = "",
			videoImage,
			schedule,
			youtubeEmail,
		} = req.body;
		const user = req.user;
		const language = langIn?.trim() || DEFAULT_LANGUAGE;
		const country = countryIn?.trim() || "US";
		const customPrompt = customPromptRaw.trim();

		if (!category || !YT_CATEGORY_MAP[category])
			return res.status(400).json({ error: "Bad category" });
		if (!VALID_RATIOS.includes(ratioIn))
			return res.status(400).json({ error: "Bad ratio" });
		if (!goodDur(durIn)) return res.status(400).json({ error: "Bad duration" });

		const ratio = ratioIn;
		const duration = +durIn;
		const [w, h] = ratio.split(":").map(Number);

		console.log(
			`[Job] user=${user.email}  cat=${category}  dur=${duration}s  geo=${country}`
		);

		/* ─── Determine topic & trend details ───────────────── */
		let topic = "";
		let trendImage = null;
		let trendArticleUrl = null;

		if (!customPrompt && category !== "Top5") {
			const story = await fetchTrendingStory(category, country);
			if (story) {
				topic = story.title;
				trendImage = story.image;
				trendArticleUrl = story.articleUrl;
				console.log(`[Trending] candidate topic="${topic}"`);
			}
		}

		/* avoid duplicates per user/category */
		if (topic) {
			const dup = await Video.findOne({
				user: user._id,
				category,
				topic,
			}).select("_id");
			if (dup) {
				console.warn("[Duplicate] topic already used for this user");
				topic = "";
			}
		}

		/* fallbacks */
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

		/* ─── seed image priority: user > trend > none ──────── */
		let seedImageUrl = videoImage?.url || trendImage || null;

		let seedImageDesc = null;
		if (seedImageUrl) {
			console.log("[Vision] describing seed image …");
			seedImageDesc = await describeSeedImage(seedImageUrl);
			console.log("[Vision] →", seedImageDesc);
		}

		/* ─── scrape article text if available ──────────────── */
		const articleText = await scrapeArticleText(trendArticleUrl);

		/* ─── Duration plan ──────────────────────────────────── */
		const intro = 3;
		const segCnt =
			category === "Top5" ? 6 : Math.ceil((duration - intro) / 10) + 1;

		/* initial rough split */
		let segLens = (() => {
			if (category === "Top5") {
				const r = duration - intro;
				const base = Math.floor(r / 5);
				const extra = r % 5;
				return [
					intro,
					...Array.from({ length: 5 }, (_, i) => base + (i < extra ? 1 : 0)),
				];
			}
			const r = duration - intro;
			const n = Math.ceil(r / 10);
			return [
				intro,
				...Array.from({ length: n }, (_, i) =>
					i === n - 1 ? r - 10 * (n - 1) : 10
				),
			];
		})();
		let segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));

		/* ─── Ask GPT for segment scripts ───────────────────── */
		const allowExplain = duration >= 25;
		const capTable = segWordCaps
			.map((w, i) => `Segment ${i + 1} ≤ ${w} words`)
			.join("  •  ");

		const articleSection = articleText
			? `\n---\nReference article (verbatim, may truncate):\n${articleText}`
			: "";

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
Return *strict* JSON array of objects with exactly two keys: "runwayPrompt" and "scriptText". Do not wrap the JSON in markdown. ${
			TONE_HINTS[category] || ""
		}${articleSection}`.trim();

		console.log("[GPT] requesting segments …");
		const segments = await getSegments(segPrompt, segCnt);

		/* shrink any over‑long lines */
		const shorten = async (seg, cap) => {
			if (seg.scriptText.trim().split(/\s+/).length <= cap) return seg;
			const ask = `Shorten to ≤${cap} words:\n"${seg.scriptText}"`;
			const { choices } = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [{ role: "user", content: ask }],
			});
			seg.scriptText = choices[0].message.content.trim();
			return seg;
		};
		await Promise.all(segments.map((s, i) => shorten(s, segWordCaps[i])));

		/* Top‑5 segment length refinement */
		if (category === "Top5") {
			const introLen = segLens[0];
			const newLens = [introLen];
			let totalNeeded = introLen;
			for (let i = 1; i < segCnt; i++) {
				const words = segments[i].scriptText.trim().split(/\s+/).length;
				const minimum = Math.ceil(spokenSeconds(words) + 0.6);
				newLens.push(Math.max(segLens[i], minimum));
				totalNeeded += newLens[i];
			}
			if (totalNeeded > duration) {
				const scale = (duration - introLen) / (totalNeeded - introLen + 0.0001);
				for (let i = 1; i < segCnt; i++) {
					newLens[i] = Math.max(
						Math.ceil(newLens[i] * scale),
						Math.ceil(spokenSeconds(segments[i].scriptText.split(/\s+/).length))
					);
				}
			}
			segLens = newLens;
			segWordCaps = segLens.map((s) => Math.floor(s * WORDS_PER_SEC));
		}

		/* ─── Style helper ──────────────────────────────────── */
		let globalStyle = "";
		try {
			const s = await openai.chat.completions.create({
				model: CHAT_MODEL,
				messages: [
					{
						role: "user",
						content: `Give a short cinematic style for "${topic}".`,
					},
				],
			});
			globalStyle = s.choices[0].message.content
				.replace(/^[-–•\s]+/, "")
				.trim();
		} catch {}

		/* ─── SEO helpers ───────────────────────────────────── */
		const seoTitle =
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

		/* ─── Enhance Runway prompts ────────────────────────── */
		const humanCache = {};
		const prependCustom = (p) => (customPrompt ? `${customPrompt}, ${p}` : p);
		const neg = RUNWAY_NEGATIVE_PROMPT;

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
			segments[i].runwayPrompt = prependCustom(
				`${prompt}, ${neg}`.replace(/^,\s*/, "")
			);
		}
		const fullScript = segments.map((s) => s.scriptText.trim()).join(" ");

		/* ─── Prepare overlay for Top‑5 ─────────────────────── */
		let overlay = "";
		if (category === "Top5") {
			let t = segLens[0];
			const draw = [];
			for (let i = 1; i < segCnt; i++) {
				const d = segLens[i];
				const labelRaw = segments[i].scriptText
					.replace(/…$/, "")
					.split(/[.!?\n]/)[0]
					.trim();
				const label =
					labelRaw.length > 60 ? labelRaw.slice(0, 57) + "…" : labelRaw;
				const spoken = spokenSeconds(label.split(/\s+/).length);
				const showFrom = t.toFixed(2);
				const showTo = Math.min(t + spoken + 0.25, t + d - 0.1).toFixed(2);
				draw.push(
					`drawtext=fontfile='${FONT_PATH_FFMPEG}':text='${escTxt(
						label
					)}':fontsize=32:fontcolor=white:box=1:boxcolor=black@0.4:boxborderw=15:x=(w-text_w)/2:y=(h-text_h)/2:enable='between(t,${showFrom},${showTo})'`
				);
				t += d;
			}
			overlay = `[0:v]${draw.join(",")}[vout]`;
			await checkOverlay(overlay, w, h, duration);
		}

		/* ─── Background music (best‑effort) ───────────────── */
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

		/* ──────────────────────────────────────────────────── */
		/*  SEGMENT‑BY‑SEGMENT VIDEO GENERATION                 */
		/* ──────────────────────────────────────────────────── */
		const clips = [];
		sendPhase("GENERATING_CLIPS", { msg: "Generating clips", total: segCnt });

		for (let i = 0; i < segCnt; i++) {
			sendPhase("ASSEMBLING_VIDEO", {
				msg: `Rendering segment ${i + 1}/${segCnt}`,
				total: segCnt,
				done: i,
			});
			const d = segLens[i];
			const rw = Math.abs(5 - d) <= Math.abs(10 - d) ? 5 : 10;
			let clip = null;

			/* -------- tier 1 : seed image (if any) ------------ */
			try {
				if (seedImageUrl) {
					const idVid = await retry(
						async () => {
							const { data } = await axios.post(
								"https://api.dev.runwayml.com/v1/image_to_video",
								{
									model: "gen4_turbo",
									promptImage: seedImageUrl,
									promptText: segments[i].runwayPrompt,
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
						"itv(seed)"
					);

					const vidUrl = await retry(
						() => pollRunway(idVid, RUNWAY_ADMIN_KEY, i + 1, "poll(seed)"),
						3,
						i + 1,
						"poll(seed)"
					);
					clip = tmpFile(`seed_${i + 1}`, ".mp4");
					await new Promise((r, j) =>
						axios
							.get(vidUrl, { responseType: "stream" })
							.then(({ data }) =>
								data
									.pipe(fs.createWriteStream(clip))
									.on("finish", r)
									.on("error", j)
							)
					);
				}
			} catch (e) {
				console.warn(`[Segment ${i + 1}] seed‑path failed → ${e.message}`);
				if (seedImageDesc)
					segments[
						i
					].runwayPrompt = `${seedImageDesc}, ${segments[i].runwayPrompt}`;
			}

			/* -------- tier 2 : normal tti → itv --------------- */
			const doTextToVid = async (promptText, label) => {
				const idImg = await retry(
					async () => {
						const { data } = await axios.post(
							"https://api.dev.runwayml.com/v1/text_to_image",
							{
								model: "gen4_image",
								promptText,
								ratio,
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
								model: "gen4_turbo",
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
			};

			if (!clip) {
				try {
					clip = await doTextToVid(segments[i].runwayPrompt, "");
				} catch (e) {
					console.error(`[Segment ${i + 1}] gen‑prompt failed → ${e.message}`);
				}
			}

			/* -------- tier 3 : auto‑generated safe prompt ------ */
			if (!clip) {
				try {
					const safePrompt = await generateFallbackPrompt(topic, category);
					console.log(`[Segment ${i + 1}] fallback prompt →`, safePrompt);
					clip = await doTextToVid(safePrompt, "_fallback");
				} catch (e) {
					console.error(
						`[Segment ${i + 1}] fallback‑prompt failed → ${e.message}`
					);
				}
			}

			/* -------- tier 4 : black dummy --------------------- */
			if (!clip) {
				console.warn(`[Segment ${i + 1}] using fallback black clip`);
				clip = await makeDummyClip(w, h, rw);
			}

			/* normalise length */
			const fixed = tmpFile(`fx_${i + 1}`, ".mp4");
			await exactLen(clip, d, fixed);
			fs.unlinkSync(clip);
			clips.push(fixed);

			sendPhase("GENERATING_CLIPS", {
				total: segCnt,
				done: i + 1,
			});
		}

		/* ─── Concat clips ─────────────────────────────────── */
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

		sendPhase("ADDING_VOICE_MUSIC", { msg: "Creating audio layer" });

		/* ─── Text‑to‑speech & mix ─────────────────────────── */
		const ttsPath = tmpFile("tts", ".mp3");
		try {
			await elevenLabsTTS(
				improveTTSPronunciation(fullScript),
				language,
				ttsPath,
				category
			);
		} catch {
			const tts = await openai.audio.speech.create({
				model: "tts-1-hd",
				voice: "shimmer",
				speed: 1.0,
				input: improveTTSPronunciation(fullScript),
				format: "mp3",
			});
			fs.writeFileSync(ttsPath, Buffer.from(await tts.arrayBuffer()));
		}

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
					.input(norm(ttsPath))
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
					.input(norm(ttsPath))
					.audioFilters("volume=1.4")
					.outputOptions("-c:a", "pcm_s16le", "-y")
					.save(norm(mixedRaw))
			);
		}
		fs.unlinkSync(ttsPath);
		await exactLenAudio(mixedRaw, duration, mixed);
		fs.unlinkSync(mixedRaw);

		sendPhase("SYNCING_VOICE_MUSIC", { msg: "Muxing final video" });

		/* ─── Mux audio + video ─────────────────────────────── */
		const safeTitle = seoTitle
			.toLowerCase()
			.replace(/[^\w\d]+/g, "_")
			.replace(/^_+|_+$/g, "");
		const finalPath = tmpFile(safeTitle, ".mp4");
		await ffmpegPromise((c) => {
			c.input(norm(silent)).input(norm(mixed));
			if (category === "Top5")
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
			else
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
			return c.save(norm(finalPath));
		});
		try {
			fs.unlinkSync(silent);
		} catch {}
		try {
			fs.unlinkSync(mixed);
		} catch {}

		/* ─── YouTube upload (best‑effort) ───────────────────── */
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

		/* ─── Persist in DB ─────────────────────────────────── */
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
			model: "gen4_turbo",
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

		sendPhase("COMPLETED", { id: doc._id, youtubeLink });
		res.end();
	} catch (err) {
		console.error("[createVideo] ERROR", err);
		if (!res.headersSent) res.status(500).json({ error: err.message });
	}
};

/* ───────────────────────────────────────────────────────────── */
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
