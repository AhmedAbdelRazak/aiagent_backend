/* routes/googleTrendsPuppeteer.js â€” bulletâ€‘proof, updated 2025â€‘11â€‘28 */
/* eslint-disable no-console, max-len */

require("dotenv").config();
const express = require("express");
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const child_process = require("child_process");
const axios = require("axios");
const puppeteer = require("puppeteer-extra");
const Stealth = require("puppeteer-extra-plugin-stealth");
const OpenAI = require("openai");
const googleTrends = require("google-trends-api");

puppeteer.use(Stealth());

const router = express.Router();

const ROW_LIMIT = 8; // how many risingâ€‘search rows we scrape
const ROW_TIMEOUT_MS = 12_000; // perâ€‘row timeout (ms)
const PROTOCOL_TIMEOUT = 120_000; // wholeâ€‘browser cap (ms)
const ARTICLE_IMAGE_FETCH_TIMEOUT_MS = 8_000; // cap for fetching article HTML
const log = (...m) => console.log("[Trends]", ...m);
const ffmpegPath =
	process.env.FFMPEG_PATH ||
	process.env.FFMPEG ||
	process.env.FFMPEG_BIN ||
	"ffmpeg";
const BROWSER_UA =
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const GOOGLE_IMAGES_DEFAULT_LIMIT = 30;
const GOOGLE_IMAGES_MAX_RESULTS = 80;
const GOOGLE_IMAGES_SCROLLS = 7;
const GOOGLE_IMAGES_SCROLL_DELAY_MS = 650;
const GOOGLE_IMAGES_SELECTOR_TIMEOUT_MS = 15000;
const TRENDS_SIGNAL_WINDOW_HOURS = 48;
const TRENDS_SIGNAL_MAX_STORIES = 8;
const TRENDS_SIGNAL_TIMEOUT_MS = 9000;
const RELATED_QUERIES_LIMIT = 12;

function tmpFile(tag, ext = "") {
	return path.join(os.tmpdir(), `${tag}_${crypto.randomUUID()}${ext}`);
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

function clampInt(value, min, max) {
	const n = Number(value);
	if (!Number.isFinite(n)) return min;
	return Math.max(min, Math.min(max, Math.round(n)));
}

function delay(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function isLikelyThumbnailUrl(u = "") {
	const url = String(u || "").toLowerCase();
	if (!url) return true;
	if (url.startsWith("data:image/")) return true;
	if (url.includes("encrypted-tbn0") || url.includes("tbn:")) return true;
	if (url.includes("gstatic.com/images?q=tbn")) return true;
	return false;
}

/* â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ OpenAI client + helpers */

let openai = null;
const CHATGPT_API_TOKEN =
	process.env.CHATGPT_API_TOKEN || process.env.OPENAI_API_KEY || null;

if (CHATGPT_API_TOKEN) {
	try {
		openai = new OpenAI({ apiKey: CHATGPT_API_TOKEN });
		log("OpenAI client initialized");
	} catch (err) {
		console.error("[Trends] Failed to init OpenAI client:", err.message);
		openai = null;
	}
}

function normaliseImageBriefs(briefs = [], topic = "") {
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

function stripCodeFences(raw = "") {
	const trimmed = String(raw || "").trim();
	if (!trimmed.startsWith("```")) return trimmed;
	const match = trimmed.match(/^```[a-zA-Z]*\s*([\s\S]*?)\s*```$/);
	return match ? String(match[1] || "").trim() : trimmed;
}

function extractJsonObject(raw = "") {
	const text = String(raw || "");
	const first = text.indexOf("{");
	const last = text.lastIndexOf("}");
	if (first === -1 || last === -1 || last <= first) return text;
	return text.slice(first, last + 1);
}

function repairJsonString(raw = "") {
	return String(raw || "")
		.replace(/[\u201c\u201d]/g, '"')
		.replace(/[\u2018\u2019]/g, "'")
		.replace(/,\s*([}\]])/g, "$1");
}

function safeParseOpenAiJson(raw = "") {
	const trimmed = String(raw || "").trim();
	if (!trimmed) return null;
	const base = stripCodeFences(trimmed);
	const candidates = uniqueStrings(
		[trimmed, base, extractJsonObject(base), extractJsonObject(trimmed)],
		{ limit: 6 }
	);
	for (const candidate of candidates) {
		if (!candidate) continue;
		for (const variant of [candidate, repairJsonString(candidate)]) {
			try {
				return JSON.parse(variant);
			} catch {
				// try next
			}
		}
	}
	return null;
}

function safeParseTrendsJson(raw = "") {
	const trimmed = String(raw || "").trim();
	if (!trimmed) return null;
	try {
		return JSON.parse(trimmed);
	} catch {
		return null;
	}
}

function withTimeout(promise, timeoutMs) {
	let timeoutId;
	const timeout = new Promise((_, reject) => {
		timeoutId = setTimeout(() => reject(new Error("timeout")), timeoutMs);
	});
	return Promise.race([promise, timeout]).finally(() => {
		if (timeoutId) clearTimeout(timeoutId);
	});
}

function parseRelatedQueries(payload, limit = RELATED_QUERIES_LIMIT) {
	const ranked = Array.isArray(payload?.default?.rankedList)
		? payload.default.rankedList
		: [];
	const topRaw = Array.isArray(ranked?.[0]?.rankedKeyword)
		? ranked[0].rankedKeyword
		: [];
	const risingRaw = Array.isArray(ranked?.[1]?.rankedKeyword)
		? ranked[1].rankedKeyword
		: [];
	const toQueries = (list) =>
		uniqueStrings(
			(list || [])
				.map((it) => it?.query || it?.topic?.title || "")
				.filter(Boolean),
			{ limit: clampInt(limit, 4, 20) }
		);
	return {
		top: toQueries(topRaw),
		rising: toQueries(risingRaw),
	};
}

function parseInterestOverTime(payload) {
	const timeline = Array.isArray(payload?.default?.timelineData)
		? payload.default.timelineData
		: [];
	const values = timeline
		.map((t) =>
			Number(
				Array.isArray(t?.value) ? t.value[0] : t?.value ?? t?.formattedValue
			)
		)
		.filter((n) => Number.isFinite(n));
	if (!values.length) {
		return { points: 0, avg: 0, latest: 0, peak: 0, slope: 0 };
	}
	const sum = values.reduce((a, b) => a + b, 0);
	const avg = sum / values.length;
	const latest = values[values.length - 1];
	const peak = Math.max(...values);
	const slope = latest - values[0];
	return {
		points: values.length,
		avg: Number(avg.toFixed(1)),
		latest,
		peak,
		slope: Number(slope.toFixed(1)),
	};
}

async function fetchTrendSignalsForKeyword(keyword, { geo, hours } = {}) {
	const safeKeyword = String(keyword || "").trim();
	if (!safeKeyword) return null;
	const windowHours = clampInt(hours || TRENDS_SIGNAL_WINDOW_HOURS, 12, 168);
	const endTime = new Date();
	const startTime = new Date(endTime.getTime() - windowHours * 60 * 60 * 1000);
	const opts = { keyword: safeKeyword, startTime, endTime, geo };

	const [relatedRaw, interestRaw] = await Promise.all([
		withTimeout(
			googleTrends.relatedQueries(opts),
			TRENDS_SIGNAL_TIMEOUT_MS
		).catch(() => null),
		withTimeout(
			googleTrends.interestOverTime(opts),
			TRENDS_SIGNAL_TIMEOUT_MS
		).catch(() => null),
	]);

	const related = parseRelatedQueries(safeParseTrendsJson(relatedRaw) || {});
	const interest = parseInterestOverTime(
		safeParseTrendsJson(interestRaw) || {}
	);
	return { relatedQueries: related, interestOverTime: interest };
}

async function enrichStoriesWithTrendSignals(stories, { geo, hours } = {}) {
	if (!Array.isArray(stories) || !stories.length) return stories;
	const limit = clampInt(TRENDS_SIGNAL_MAX_STORIES, 1, stories.length);
	const windowHours = clampInt(hours || TRENDS_SIGNAL_WINDOW_HOURS, 12, 168);
	log("Trends API enrichment start", {
		stories: stories.length,
		enrichCount: limit,
		hours: windowHours,
	});
	const out = [];

	for (let i = 0; i < stories.length; i++) {
		const story = stories[i];
		if (i >= limit) {
			out.push(story);
			continue;
		}
		const keyword =
			story?.title ||
			story?.rawTitle ||
			story?.trendSearchTerm ||
			story?.term ||
			"";
		try {
			const signals = await fetchTrendSignalsForKeyword(keyword, {
				geo,
				hours: windowHours,
			});
			if (signals) {
				log("Trends API signals", {
					term: keyword,
					relatedTop: signals.relatedQueries?.top?.length || 0,
					relatedRising: signals.relatedQueries?.rising?.length || 0,
					interest: signals.interestOverTime || {},
				});
				out.push({ ...story, ...signals });
			} else {
				log("Trends API signals empty", { term: keyword });
				out.push(story);
			}
		} catch (err) {
			log("Trends API signals failed", {
				term: keyword,
				error: err.message || String(err),
			});
			out.push(story);
		}
	}

	return out;
}
/**
 * Use GPTâ€‘5.1 to generate SEOâ€‘optimized blog + Shorts titles per story.
 * If anything fails, we just return the original stories unchanged.
 */
async function enhanceStoriesWithOpenAI(
	stories,
	{ geo, hours, category, language = "English" }
) {
	if (!openai || !stories.length) return stories;

	const payload = {
		geo,
		hours,
		category,
		language,
		topics: stories.map((s) => ({
			term: s.title,
			articleTitles: s.articles.map((a) => a.title),
		})),
	};

	try {
		const response = await openai.responses.create({
			model: "gpt-5.1",
			instructions:
				"You are an expert SEO copywriter and YouTube Shorts strategist. " +
				"Given trending search topics and their news article titles, " +
				"for EACH topic create:\n" +
				"1) A compelling yet honest blog post title (no clickbait, max ~80 chars).\n" +
				"2) A highCTR YouTube Shorts title (max ~60 chars, must stay factual).\n" +
				"3) EXACTLY TWO viral thumbnail/image directives: one for aspectRatio 1280:720 (landscape) and one for 720:1280 (vertical). Each directive should include:\n" +
				'   - "aspectRatio": one of those two strings (landscape MUST be 1280:720, vertical MUST be 720:1280)\n' +
				'   - "visualHook": a vivid, specific description of what the image shows (clear subject, camera framing, and motion-friendly composition, no text overlays, no logos)\n' +
				'   - "emotion": the main emotion the image should evoke\n' +
				'   - "rationale": a short note for the video orchestrator about why this hook works for that aspect ratio\n' +
				"4) One short 'imageComment' that plainly describes what the recommended hero shot should look like so downstream video generation can pick the right image for Runway (mention subject + setting, no extra fluff).\n\n" +
				`ALL text must be in ${language}, even if the country/geo differs. No other languages or scripts are allowed.\n` +
				"Your entire reply MUST be valid JSON, no extra commentary.",
			input:
				"Here is the data as JSON:\n\n" +
				JSON.stringify(payload) +
				"\n\n" +
				"Respond with a JSON object of the form:\n" +
				'{ "topics": [ { "term": string, "blogTitle": string, "youtubeShortTitle": string, "imageComment": string, "imageDirectives": [ { "aspectRatio": "1280:720", "visualHook": string, "emotion": string, "rationale": string }, { "aspectRatio": "720:1280", "visualHook": string, "emotion": string, "rationale": string } ] } ] }',
			// We skip response_format to avoid model/version compatibility errors
			// and just force JSON via instructions.
		});

		const raw = response.output_text || "";
		const parsed = safeParseOpenAiJson(raw);
		if (!parsed) {
			log("Failed to parse OpenAI JSON:", "unable to recover JSON output");
			return stories;
		}
		const byTerm = new Map();
		if (parsed && Array.isArray(parsed.topics)) {
			for (const t of parsed.topics) {
				if (!t || typeof t.term !== "string") continue;
				byTerm.set(t.term.toLowerCase(), t);
			}
		}

		return stories.map((s) => {
			const key = s.title.toLowerCase();
			const match =
				byTerm.get(key) ||
				// fallback: GPT sometimes normalizes whitespace/case
				Array.from(byTerm.values()).find(
					(t) => t.term && t.term.toLowerCase().trim() === key.trim()
				);

			if (!match) return s;

			const briefs = normaliseImageBriefs(
				match.imageDirectives || match.viralImageBriefs || [],
				s.title
			);
			const imageComment =
				String(match.imageComment || match.imageHook || "").trim() ||
				(briefs[0]?.visualHook
					? `Lead image for ${s.title}: ${briefs[0].visualHook}`
					: `Lead image for ${s.title}, framed for ${
							briefs[0]?.aspectRatio === "720:1280" ? "vertical" : "landscape"
					  } video.`);

			return {
				...s,
				seoTitle: match.blogTitle || s.title,
				youtubeShortTitle: match.youtubeShortTitle || s.title,
				imageComment,
				viralImageBriefs: briefs,
			};
		});
	} catch (err) {
		console.error("[Trends] OpenAI enhancement failed:", err.message || err);
		return stories;
	}
}

/* â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ helpers */

const urlFor = ({ geo, hours, category, sort }) => {
	// Clamp hours 1â€“168 and actually use the requested window.
	// const hrs = Math.min(Math.max(Number(hours) || 24, 1), 168);

	const params = new URLSearchParams({
		geo,
		hl: "en-US", // matches the UI you pasted
		hours: String(48),
		status: "active",
		// sort:"search-volume"
	});

	if (category) params.set("category", String(category).trim());

	// If caller passes an explicit sort, forward it.
	// Otherwise we let Trends use its default sort (UI default).
	if (sort && String(sort).trim()) {
		params.set("sort", String(sort).trim());
	}

	return `https://trends.google.com/trending?${params.toString()}`;
};

/* â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ cached browser */

let browser; // one instance per container / PM2 worker

async function getBrowser() {
	if (browser) return browser;

	browser = await puppeteer.launch({
		headless: true,
		protocolTimeout: PROTOCOL_TIMEOUT,
		executablePath:
			process.env.CHROME_BIN || // production path
			puppeteer.executablePath(), // dev fallback
		args: [
			"--no-sandbox",
			"--disable-setuid-sandbox",
			"--disable-gpu",
			"--disable-dev-shm-usage",
		],
	});

	return browser;
}

/* â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ article image helpers (Node side) */

async function fetchHtmlWithTimeout(url, timeoutMs) {
	// Older Node may not have global fetch; if so we just skip enrichment.
	if (typeof fetch !== "function") return null;

	const supportsAbort =
		typeof AbortController !== "undefined" &&
		typeof AbortController === "function";

	const controller = supportsAbort ? new AbortController() : null;
	let timeoutId;

	try {
		const fetchPromise = fetch(url, {
			redirect: "follow",
			signal: controller ? controller.signal : undefined,
			headers: {
				"User-Agent":
					"Mozilla/5.0 (compatible; TrendsScraper/1.0; +https://trends.google.com)",
				Accept: "text/html,application/xhtml+xml",
			},
		});

		let response;
		if (controller) {
			const timeoutPromise = new Promise((_, reject) => {
				timeoutId = setTimeout(() => {
					controller.abort();
					reject(new Error("timeout"));
				}, timeoutMs);
			});
			response = await Promise.race([fetchPromise, timeoutPromise]);
		} else {
			response = await fetchPromise;
		}

		if (!response || !response.ok) return null;

		const contentType = response.headers.get("content-type") || "";
		if (!contentType.includes("text/html")) return null;

		return await response.text();
	} catch (err) {
		log("Article fetch failed:", url, err.message || String(err));
		return null;
	} finally {
		if (timeoutId) clearTimeout(timeoutId);
	}
}

function extractBestImageFromHtml(html) {
	if (!html) return null;

	const candidates = [];

	const pushMatch = (regex) => {
		const m = regex.exec(html);
		if (m && m[1]) candidates.push(m[1]);
	};

	// Try common Open Graph / Twitter meta tags.
	pushMatch(
		/<meta[^>]+property=["']og:image:secure_url["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);
	pushMatch(
		/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);
	pushMatch(
		/<meta[^>]+name=["']og:image["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);
	pushMatch(
		/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);
	pushMatch(
		/<meta[^>]+name=["']twitter:image:src["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);

	const httpCandidates = candidates.filter((u) => /^https?:\/\//i.test(u));
	return httpCandidates[0] || null;
}

async function fetchBestImageForArticle(url, fallbackImage) {
	try {
		const html = await fetchHtmlWithTimeout(
			url,
			ARTICLE_IMAGE_FETCH_TIMEOUT_MS
		);
		const ogImage = extractBestImageFromHtml(html);
		return ogImage || fallbackImage || null;
	} catch (err) {
		log("Image enrichment failed:", url, err.message || String(err));
		return fallbackImage || null;
	}
}

/**
 * For each article we scraped from Google Trends, try to replace the tiny
 * Google thumbnail with the article's OpenGraph hero image. This runs in
 * Node, not in the browser, so CORS is not an issue.
 */
async function hydrateArticleImages(stories) {
	if (!stories.length) return stories;

	// If fetch is missing we just return the original data.
	if (typeof fetch !== "function") {
		log("Global fetch not available, skipping article image hydration");
		return stories;
	}

	return Promise.all(
		stories.map(async (story) => {
			const enrichedArticles = await Promise.all(
				story.articles.map(async (art) => {
					const bestImage = await fetchBestImageForArticle(art.url, art.image);
					return { ...art, image: bestImage };
				})
			);

			const hero =
				enrichedArticles.find((a) => a.image && typeof a.image === "string")
					?.image || story.image;

			return {
				...story,
				image: hero,
				articles: enrichedArticles,
			};
		})
	);
}

/* â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ scraper */

/* ---------------------------------------------------------------
 * Google Images scraping helper
 * ------------------------------------------------------------- */

async function autoScrollPage(page, { scrolls, delayMs } = {}) {
	const steps = clampInt(scrolls ?? GOOGLE_IMAGES_SCROLLS, 1, 12);
	const pause = clampInt(delayMs ?? GOOGLE_IMAGES_SCROLL_DELAY_MS, 200, 2000);
	for (let i = 0; i < steps; i++) {
		await page.evaluate(() => {
			window.scrollBy(0, window.innerHeight * 1.2);
		});
		// eslint-disable-next-line no-await-in-loop
		await delay(pause);
	}
}

async function scrapeGoogleImages({
	query,
	limit = GOOGLE_IMAGES_DEFAULT_LIMIT,
}) {
	const page = await (await getBrowser()).newPage();
	page.setDefaultNavigationTimeout(PROTOCOL_TIMEOUT);
	await page.setUserAgent(BROWSER_UA);

	// Block only fonts for speed; keep images so lazy-loaded URLs hydrate.
	await page.setRequestInterception(true);
	page.on("request", (req) => {
		const type = req.resourceType();
		if (type === "font") return req.abort();
		return req.continue();
	});

	const targetUrl = `https://www.google.com/search?tbm=isch&q=${encodeURIComponent(
		query
	)}`;
	log("Google images navigate:", targetUrl);

	try {
		await page.goto(targetUrl, { waitUntil: "domcontentloaded" });
		try {
			await page.waitForSelector("img", {
				timeout: GOOGLE_IMAGES_SELECTOR_TIMEOUT_MS,
			});
		} catch {
			// Continue even if images are slow to load.
		}

		await autoScrollPage(page, {
			scrolls: GOOGLE_IMAGES_SCROLLS,
			delayMs: GOOGLE_IMAGES_SCROLL_DELAY_MS,
		});

		const rawUrls = await page.evaluate(() => {
			const out = [];
			const push = (u) => {
				if (u) out.push(u);
			};

			const anchors = Array.from(
				document.querySelectorAll('a[href^="/imgres?"]')
			);
			for (const a of anchors) {
				const href = a.getAttribute("href") || "";
				const qIndex = href.indexOf("?");
				if (qIndex === -1) continue;
				try {
					const params = new URLSearchParams(href.slice(qIndex + 1));
					const imgurl = params.get("imgurl");
					if (imgurl) push(imgurl);
				} catch {
					// ignore URLSearchParams failures
				}
			}

			const imgs = Array.from(document.querySelectorAll("img"));
			for (const img of imgs) {
				const candidate =
					img.getAttribute("data-iurl") ||
					img.getAttribute("data-src") ||
					img.getAttribute("data-lsrc") ||
					img.src ||
					"";
				if (candidate) push(candidate);
			}

			return out;
		});

		const filtered = (rawUrls || [])
			.map((u) => String(u || "").trim())
			.filter((u) => /^https?:\/\//i.test(u))
			.filter((u) => !isLikelyThumbnailUrl(u));

		return uniqueStrings(filtered, {
			limit: clampInt(limit, 6, GOOGLE_IMAGES_MAX_RESULTS),
		});
	} finally {
		await page.close().catch(() => {});
	}
}

async function scrape({ geo, hours, category, sort }) {
	const page = await (await getBrowser()).newPage();
	page.setDefaultNavigationTimeout(PROTOCOL_TIMEOUT);

	// Relay browser console messages, but drop noisy network errors.
	page.on("console", (msg) => {
		const text = msg.text();
		if (/Failed to load resource/i.test(text)) return;
		log("Page>", text);
	});

	// Block only fonts for speed; keep images and CSS so layout stays stable.
	await page.setRequestInterception(true);
	page.on("request", (req) => {
		const type = req.resourceType();
		if (type === "font") {
			return req.abort();
		}
		return req.continue();
	});

	const targetURL = urlFor({ geo, hours, category, sort });
	log("Navigate:", targetURL);

	const stories = [];
	const seenTerms = new Set();

	try {
		await page.goto(targetURL, { waitUntil: "domcontentloaded" });

		await page.waitForSelector('tr[role="row"][data-row-id]', {
			timeout: 60_000,
		});

		// Extract the first ROW_LIMIT keywords exactly as shown in the table.
		const rows = await page.$$eval(
			'tr[role="row"][data-row-id]',
			(trs, LIM) =>
				trs.slice(0, LIM).map((tr) => ({
					id: tr.getAttribute("data-row-id"),
					term: tr.querySelector("td:nth-child(2)")?.innerText.trim() ?? "",
				})),
			ROW_LIMIT
		);
		log("Row list:", rows);

		for (const { id, term } of rows) {
			if (!term) continue;
			const normTerm = term.toLowerCase().trim();
			if (seenTerms.has(normTerm)) {
				log(`Skip duplicate term "${term}"`);
				continue;
			}

			const result = await page.evaluate(
				// eslint-disable-next-line no-undef
				async (rowId, rowTerm, rowMs) => {
					const sleep = (ms) =>
						new Promise((resolve) => setTimeout(resolve, ms));
					const deadline = Date.now() + rowMs;

					const clickRow = () => {
						const tr = document.querySelector(`tr[data-row-id="${rowId}"]`);
						const cell = tr?.querySelector("td:nth-child(2)");
						if (!cell) return false;
						cell.scrollIntoView({ block: "center" });
						cell.click();
						console.log(`Clicked ${rowTerm}`);
						return true;
					};

					if (!clickRow()) return { status: "row-not-found" };

					while (Date.now() < deadline) {
						const dialog = document.querySelector(
							'div[aria-modal="true"][role="dialog"][aria-label]'
						);

						if (dialog) {
							const label = dialog.getAttribute("aria-label") || "";
							const heading =
								dialog.querySelector('[role="heading"]')?.textContent.trim() ||
								dialog.querySelector("h1,h2,h3")?.textContent.trim() ||
								"";
							const normalizedLabel = label.trim().toLowerCase();
							const normalizedTerm = rowTerm.trim().toLowerCase();

							// Some dialogs prepend extra words; accept prefix match.
							const isMatch =
								normalizedLabel === normalizedTerm ||
								normalizedLabel.startsWith(normalizedTerm);

							if (isMatch) {
								let anchors = [];
								const t2 = Date.now() + 1_000;
								while (Date.now() < t2) {
									anchors = [
										...dialog.querySelectorAll(
											'a[target="_blank"][href^="http"]'
										),
									];
									if (anchors.length) break;
									// eslint-disable-next-line no-await-in-loop
									await sleep(120);
								}

								const arts = anchors.slice(0, 6).map((a) => ({
									title:
										a.querySelector('[role="heading"]')?.textContent.trim() ||
										a.querySelector("div.Q0LBCe")?.textContent.trim() ||
										a.textContent.trim(),
									url: a.href,
									image: a.querySelector("img")?.src || null,
								}));

								// Close the dialog (handle layout variants).
								(
									dialog.querySelector(
										'div[aria-label="Close search"], div[aria-label="Close"], button.pYTkkf-Bz112c-LgbsSe'
									) || document.querySelector('div[aria-label="Close"]')
								)?.click() ||
									document.dispatchEvent(
										new KeyboardEvent("keydown", { key: "Escape" })
									);

								return {
									status: "ok",
									dialogTitle: heading || label || rowTerm,
									image: arts[0]?.image || null,
									articles: arts,
								};
							}
						}

						// If dialog vanished (virtual scroll) â†’ reclick.
						if (!dialog) clickRow();
						// eslint-disable-next-line no-await-in-loop
						await sleep(200);
					}

					return { status: "timeout" };
				},
				id,
				term,
				ROW_TIMEOUT_MS
			);

			log(`Result for "${term}":`, result.status);

			if (result.status === "ok") {
				seenTerms.add(normTerm);

				const rawTitle = term;
				const dialogTitle = String(result.dialogTitle || "").trim();
				const primaryTitle = dialogTitle || rawTitle;
				const articles = Array.isArray(result.articles)
					? result.articles.map((a) => ({
							title: String(a.title || "").trim(),
							url: a.url,
							image: a.image || null,
					  }))
					: [];
				const searchPhrases = uniqueStrings(
					[primaryTitle, rawTitle, ...articles.map((a) => a.title)],
					{ limit: 6 }
				);
				const entityNames = uniqueStrings([primaryTitle, rawTitle]);

				stories.push({
					title: primaryTitle,
					rawTitle,
					trendDialogTitle: dialogTitle || null,
					searchPhrases,
					image: result.image,
					entityNames,
					articles,
				});
			}

			// Wait until dialog truly closed before next loop.
			try {
				await page.waitForFunction(
					() =>
						!document.querySelector('div[aria-modal="true"][role="dialog"]'),
					{ timeout: 5_000 }
				);
				await page.waitForTimeout(250);
			} catch (e) {
				// ignored
			}
		}
	} finally {
		await page.close().catch(() => {});
	}

	return stories;
}

/* â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ express API */

router.get("/google-images", async (req, res) => {
	const query = String(req.query.q || req.query.query || "").trim();
	if (!query) {
		return res.status(400).json({
			error: "`q` query param is required",
		});
	}

	const rawLimit = Number(req.query.limit);
	const limit = Number.isFinite(rawLimit)
		? clampInt(rawLimit, 6, GOOGLE_IMAGES_MAX_RESULTS)
		: GOOGLE_IMAGES_DEFAULT_LIMIT;

	try {
		const images = await scrapeGoogleImages({ query, limit });
		return res.json({
			query,
			count: images.length,
			images,
		});
	} catch (err) {
		console.error(
			"[Trends] Google Images scraping failed:",
			err.message || err
		);
		return res.status(500).json({
			error: "Google Images scraping failed",
			detail: err.message || String(err),
		});
	}
});

router.get("/google-trends", async (req, res) => {
	const geo = (req.query.geo || "").toUpperCase();
	if (!/^[A-Z]{2}$/.test(geo)) {
		return res.status(400).json({
			error: "`geo` must be an ISOâ€‘3166 alphaâ€‘2 country code",
		});
	}

	const hours = Number(req.query.hours) || 24;
	const category = req.query.category ?? null;
	const sort = req.query.sort ?? null;
	const includeImages = ["1", "true", "yes", "on"].includes(
		String(req.query.includeImages || "").toLowerCase()
	);

	try {
		let stories = await scrape({
			geo,
			hours,
			category,
			sort,
		});

		// Ask GPTâ€‘5.1 for better blog + YouTube titles.
		//    This is optional and skipped if CHATGPT_API_TOKEN / OPENAI_API_KEY
		//    is not configured or the call fails.
		stories = await enhanceStoriesWithOpenAI(stories, {
			geo,
			hours,
			category,
		});

		stories = await enrichStoriesWithTrendSignals(stories, {
			geo,
			hours: TRENDS_SIGNAL_WINDOW_HOURS,
		});

		if (includeImages) {
			stories = await hydrateArticleImages(stories);
		}

		// Strip images here; downstream orchestrator will search high-quality images per ratio.
		stories = stories.map((s) => ({
			...s,
			trendSearchTerm:
				s.trendSearchTerm || s.rawTitle || s.title || s.trendDialogTitle || "",
			image: includeImages ? s.image || null : null,
			images: includeImages
				? uniqueStrings(
						[
							s.image || null,
							...(s.articles || []).map((a) => a.image).filter(Boolean),
						],
						{ limit: 8 }
				  )
				: [],
			articles: (s.articles || []).map((a) => ({
				title: a.title,
				url: a.url,
				...(includeImages && a.image ? { image: a.image } : {}),
			})),
		}));

		return res.json({
			generatedAt: new Date().toISOString(),
			requestedGeo: geo,
			effectiveGeo: geo, // Trends may redirect, but we fix geo
			hours,
			category,
			stories,
		});
	} catch (err) {
		console.error(err);
		return res.status(500).json({
			error: "Google Trends scraping failed",
			detail: err.message || String(err),
		});
	}
});

module.exports = router;
