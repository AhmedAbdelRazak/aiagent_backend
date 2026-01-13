/* routes/googleTrendsPuppeteer.js — bullet‑proof, updated 2025‑11‑28 */
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

const ROW_LIMIT = 8; // how many rising‑search rows we scrape
const ROW_TIMEOUT_MS = 12_000; // per‑row timeout (ms)
const PROTOCOL_TIMEOUT = 120_000; // whole‑browser cap (ms)
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
const TRENDS_SIGNAL_FALLBACK_HOURS = 168;
const TRENDS_SIGNAL_MAX_STORIES = 8;
const TRENDS_SIGNAL_TIMEOUT_MS = 9000;
const TRENDS_SIGNAL_RETRY_DELAY_MS = 350;
const TRENDS_SIGNAL_TIME_FALLBACKS = ["now 7-d", "today 1-m"];
const RELATED_QUERIES_LIMIT = 12;
const TRENDS_CACHE_TTL_MS = 5 * 60 * 1000;
const POTENTIAL_IMAGE_TOPIC_LIMIT = ROW_LIMIT;
const POTENTIAL_IMAGE_MIN_PER_STORY = 4;
const POTENTIAL_IMAGE_TOPUP_TOPIC_LIMIT = 5;
const POTENTIAL_IMAGE_ARTICLE_LIMIT = 3;
const POTENTIAL_IMAGE_PER_ARTICLE_LIMIT = 6;
const POTENTIAL_IMAGE_MAX_PER_STORY = 10;
const POTENTIAL_IMAGE_MIN_WIDTH = 320;
const POTENTIAL_IMAGE_MIN_HEIGHT = 180;
const POTENTIAL_IMAGE_SELECTOR_TIMEOUT_MS = 8000;
const POTENTIAL_IMAGE_TIMEOUT_MS = 25_000;
const POTENTIAL_IMAGE_SCROLLS = 3;
const POTENTIAL_IMAGE_SCROLL_DELAY_MS = 450;
const POTENTIAL_IMAGE_VALIDATE_TIMEOUT_MS = 4500;
const VOGUE_SEARCH_SCROLLS = 3;
const VOGUE_SEARCH_SCROLL_DELAY_MS = 650;
const VOGUE_SEARCH_SELECTOR_TIMEOUT_MS = 10_000;
const VOGUE_SEARCH_MAX_RESULTS = 10;
const VOGUE_SEARCH_BASE_URL = "https://www.vogue.com/search";
const VOGUE_SEARCH_SORT = "score desc";
const BBC_SEARCH_SCROLLS = 3;
const BBC_SEARCH_SCROLL_DELAY_MS = 650;
const BBC_SEARCH_SELECTOR_TIMEOUT_MS = 10_000;
const BBC_SEARCH_MAX_RESULTS = 12;
const BBC_SEARCH_MAX_ARTICLES = 3;
const BBC_SEARCH_MIN_SCORE = 2;
const BBC_SEARCH_BASE_URL = "https://www.bbc.com/search";
const CBS_SEARCH_SCROLLS = 3;
const CBS_SEARCH_SCROLL_DELAY_MS = 650;
const CBS_SEARCH_SELECTOR_TIMEOUT_MS = 12_000;
const CBS_SEARCH_MAX_RESULTS = 12;
const CBS_SEARCH_MAX_ARTICLES = 3;
const CBS_SEARCH_MIN_SCORE = 2;
const CBS_SEARCH_MAX_AGE_DAYS = 365;
const CBS_SEARCH_BASE_URL = "https://www.cbsnews.com/search/";
const POTENTIAL_IMAGE_CAPTCHA_HINTS = [
	"captcha",
	"verify you are a human",
	"unusual traffic",
	"are you a robot",
	"robot check",
	"access denied",
	"blocked",
	"cloudflare",
	"attention required",
];
const POTENTIAL_IMAGE_BAD_HINTS = [
	"logo",
	"icon",
	"sprite",
	"favicon",
	"avatar",
	"profile",
	"badge",
	"button",
	"thumbnail",
	"thumb",
	"placeholder",
	"spacer",
	"pixel",
	"banner",
	"ads",
	"advert",
	"promo",
	"tracking",
	"loader",
	"spinner",
	"live_cards",
	"video-door",
	"author",
	"bio",
	"byline",
	"headshot",
	"staff",
	"newsletter",
	"subscribe",
];
const ENTERTAINMENT_CATEGORY_IDS = new Set(["4"]);
const IMAGE_STOPWORDS = new Set([
	"the",
	"and",
	"or",
	"of",
	"in",
	"on",
	"for",
	"to",
	"a",
	"an",
	"with",
	"at",
	"by",
	"from",
	"about",
	"into",
	"after",
	"before",
	"over",
	"under",
	"new",
	"latest",
	"update",
	"updates",
	"official",
	"photo",
	"photos",
	"video",
]);
const trendsCache = new Map();

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

function buildTrendsCacheKey({
	geo,
	hours,
	category,
	sort,
	includeImages,
	includePotentialImages,
	skipOpenAI,
	skipSignals,
}) {
	return JSON.stringify({
		geo,
		hours,
		category: category || "",
		sort: sort || "",
		includeImages: Boolean(includeImages),
		includePotentialImages: Boolean(includePotentialImages),
		skipOpenAI: Boolean(skipOpenAI),
		skipSignals: Boolean(skipSignals),
	});
}

function isLikelyThumbnailUrl(u = "") {
	const url = String(u || "").toLowerCase();
	if (!url) return true;
	if (url.startsWith("data:image/")) return true;
	if (url.includes("encrypted-tbn0") || url.includes("tbn:")) return true;
	if (url.includes("gstatic.com/images?q=tbn")) return true;
	return false;
}

/* ───────────────────────────────────────────── OpenAI client + helpers */

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
	if (!raw) return null;
	if (typeof raw === "object") return raw;
	const trimmed = String(raw || "").trim();
	if (!trimmed) return null;
	const objIdx = trimmed.indexOf("{");
	const arrIdx = trimmed.indexOf("[");
	let start = objIdx;
	if (arrIdx !== -1 && (start === -1 || arrIdx < start)) start = arrIdx;
	const candidate = start > 0 ? trimmed.slice(start) : trimmed;
	try {
		return JSON.parse(candidate);
	} catch {
		return null;
	}
}

function normalizeTrendKeyword(keyword = "") {
	return String(keyword || "")
		.replace(/[\u2018\u2019\u201c\u201d"'`]/g, "")
		.replace(/[^a-z0-9\s-]/gi, " ")
		.replace(/\s+/g, " ")
		.trim();
}

function buildTrendKeywordVariants(keyword = "") {
	const normalized = normalizeTrendKeyword(keyword);
	return uniqueStrings([keyword, normalized], { limit: 3 }).filter(Boolean);
}

function normalizeMatchText(value = "") {
	return normalizeTrendKeyword(value).toLowerCase();
}

function sanitizeHeadlineForMatch(raw = "") {
	return String(raw || "")
		.replace(/\u00a0/g, " ")
		.replace(/\b(?:today|yesterday|tomorrow)\b/gi, "")
		.replace(
			/\d+\s*(?:minutes?|mins?|min|hours?|hrs?|hr|days?|day|weeks?|week)\s*ago\b/gi,
			""
		)
		.replace(/\s*[\u2022\u00b7\u25cf\|\-\u2013\u2014]\s+.+$/g, "")
		.replace(/\s+/g, " ")
		.trim();
}

function normalizeSearchQuery(raw = "") {
	const cleaned = sanitizeHeadlineForMatch(raw);
	return cleaned.replace(/\s+/g, " ").trim();
}

function limitQueryLength(value = "", maxLen = 100) {
	const trimmed = String(value || "").trim();
	if (!trimmed) return "";
	if (trimmed.length <= maxLen) return trimmed;
	const sliced = trimmed.slice(0, maxLen);
	return sliced.replace(/\s+\S*$/, "").trim();
}

function buildVogueSearchQueriesForStory(story) {
	const candidates = [];
	const add = (value) => {
		const normalized = limitQueryLength(normalizeSearchQuery(value), 100);
		if (normalized) candidates.push(normalized);
	};

	const articles = Array.isArray(story?.articles) ? story.articles : [];
	for (const article of articles.slice(0, 2)) {
		add(article?.title);
	}
	add(story?.title);
	add(story?.rawTitle);
	add(story?.trendDialogTitle);

	return uniqueStrings(candidates, { limit: 5 });
}

function buildVogueSearchUrl(query = "") {
	const params = new URLSearchParams({
		q: query,
		sort: VOGUE_SEARCH_SORT,
	});
	return `${VOGUE_SEARCH_BASE_URL}?${params.toString()}`;
}

function buildBbcSearchQueriesForStory(story) {
	const candidates = [];
	const add = (value) => {
		const normalized = limitQueryLength(normalizeSearchQuery(value), 90);
		if (normalized) candidates.push(normalized);
	};

	add(story?.title);
	add(story?.rawTitle);
	add(story?.trendDialogTitle);
	const articles = Array.isArray(story?.articles) ? story.articles : [];
	for (const article of articles.slice(0, 2)) {
		add(article?.title);
	}

	return uniqueStrings(candidates, { limit: 4 });
}

function buildBbcSearchUrl(query = "") {
	const params = new URLSearchParams({
		q: query,
	});
	return `${BBC_SEARCH_BASE_URL}?${params.toString()}`;
}

function sanitizeCbsSearchQuery(raw = "", maxLen = 90) {
	const cleaned = normalizeSearchQuery(raw);
	if (!cleaned) return "";
	let value = cleaned
		.replace(/#[A-Za-z0-9_]+/g, " ")
		.replace(/\s+/g, " ")
		.trim()
		.replace(/(?:today|yesterday|tomorrow)$/i, "")
		.trim();
	value = limitQueryLength(value, maxLen);
	const tokens = value.split(" ").filter(Boolean);
	if (tokens.length > 10) {
		value = tokens.slice(0, 10).join(" ");
	}
	return value;
}

function buildCbsSearchQueriesForStory(story) {
	const candidates = [];
	const add = (value) => {
		const normalized = sanitizeCbsSearchQuery(value, 90);
		if (normalized) candidates.push(normalized);
	};

	add(story?.title);
	add(story?.rawTitle);
	add(story?.trendDialogTitle);
	const articles = Array.isArray(story?.articles) ? story.articles : [];
	for (const article of articles.slice(0, 2)) {
		add(article?.title);
	}

	return uniqueStrings(candidates, { limit: 4 });
}

function buildCbsSearchUrl(query = "") {
	const params = new URLSearchParams({
		q: query,
	});
	return `${CBS_SEARCH_BASE_URL}?${params.toString()}`;
}

function isLikelyBbcArticleUrl(rawUrl = "") {
	const lower = String(rawUrl || "").toLowerCase();
	if (
		!lower.startsWith("https://www.bbc.com/") &&
		!lower.startsWith("https://www.bbc.co.uk/")
	) {
		return false;
	}
	if (lower.includes("/search")) return false;
	if (lower.includes("/account")) return false;
	return true;
}

function isLikelyCbsArticleUrl(rawUrl = "") {
	const lower = String(rawUrl || "").toLowerCase();
	if (!lower.startsWith("https://www.cbsnews.com/")) return false;
	if (lower.includes("/search")) return false;
	if (lower.includes("#search-form")) return false;
	return true;
}

function isCbsVideoUrl(rawUrl = "") {
	const lower = String(rawUrl || "").toLowerCase();
	return lower.includes("/video/") || lower.includes("/videos/");
}

function scoreTextMatch(text = "", terms = {}) {
	const normalized = normalizeMatchText(text);
	if (!normalized) return 0;
	let score = 0;
	for (const phrase of terms.phrases || []) {
		if (phrase && normalized.includes(phrase)) score += 5;
	}
	for (const token of terms.tokens || []) {
		if (token && normalized.includes(token)) score += 1;
	}
	return score;
}

function countTokenHits(text = "", tokens = []) {
	const normalized = normalizeMatchText(text);
	if (!normalized) return 0;
	const hits = new Set();
	for (const token of tokens || []) {
		if (!token) continue;
		if (token.length < 3) continue;
		if (normalized.includes(token)) hits.add(token);
	}
	return hits.size;
}

function hasPhraseMatch(text = "", phrases = []) {
	const normalized = normalizeMatchText(text);
	if (!normalized) return false;
	for (const phrase of phrases || []) {
		if (phrase && normalized.includes(phrase)) return true;
	}
	return false;
}

function parseCbsDateToTimestamp(value = "") {
	const raw = String(value || "").trim();
	if (!raw) return null;
	const lower = raw.toLowerCase();
	if (lower.includes("just now")) return Date.now();
	if (lower.includes("today")) return Date.now();
	if (lower.includes("yesterday")) {
		return Date.now() - 24 * 60 * 60 * 1000;
	}

	const relMatch = lower.match(
		/(\d+)\s*(mins?|minutes?|m|hrs?|hours?|h|days?|d|weeks?|w)\s*ago/
	);
	if (relMatch) {
		const valueNum = Number(relMatch[1]);
		if (!Number.isFinite(valueNum)) return null;
		const unit = relMatch[2];
		let minutes = valueNum;
		if (/^h/.test(unit)) minutes = valueNum * 60;
		else if (/^d/.test(unit)) minutes = valueNum * 60 * 24;
		else if (/^w/.test(unit)) minutes = valueNum * 60 * 24 * 7;
		return Date.now() - minutes * 60 * 1000;
	}

	let parsed = Date.parse(raw);
	if (!Number.isNaN(parsed)) return parsed;
	const year = new Date().getFullYear();
	parsed = Date.parse(`${raw} ${year}`);
	if (!Number.isNaN(parsed)) return parsed;
	return null;
}

function isEntertainmentCategory(category) {
	const key = String(category || "").trim();
	return ENTERTAINMENT_CATEGORY_IDS.has(key);
}

function buildMatchTerms(...values) {
	const list = [];
	for (const value of values) {
		if (Array.isArray(value)) {
			list.push(...value);
		} else {
			list.push(value);
		}
	}

	const phrases = uniqueStrings(
		list
			.map((val) => normalizeMatchText(sanitizeHeadlineForMatch(val)))
			.filter(Boolean),
		{ limit: 24 }
	);

	const tokens = [];
	for (const phrase of phrases) {
		for (const token of phrase.split(" ")) {
			const trimmed = token.trim();
			if (!trimmed) continue;
			if (trimmed.length < 2) continue;
			if (IMAGE_STOPWORDS.has(trimmed)) continue;
			tokens.push(trimmed);
		}
	}

	return {
		phrases,
		tokens: uniqueStrings(tokens, { limit: 60 }),
	};
}

function matchesAnyTerm(text = "", terms = {}) {
	const normalized = normalizeMatchText(text);
	if (!normalized) return false;
	for (const phrase of terms.phrases || []) {
		if (phrase && normalized.includes(phrase)) return true;
	}
	for (const token of terms.tokens || []) {
		if (token && normalized.includes(token)) return true;
	}
	return false;
}

function matchesAnyTermStrict(text = "", terms = {}) {
	const normalized = normalizeMatchText(text);
	if (!normalized) return false;
	for (const phrase of terms.phrases || []) {
		if (phrase && normalized.includes(phrase)) return true;
	}
	let hitCount = 0;
	for (const token of terms.tokens || []) {
		if (!token) continue;
		if (token.length < 3) continue;
		if (normalized.includes(token)) {
			hitCount += 1;
			if (hitCount >= 2) return true;
		}
	}
	return false;
}

function getUrlPathForMatch(rawUrl = "") {
	try {
		const parsed = new URL(rawUrl);
		return `${parsed.pathname}`.toLowerCase();
	} catch {
		return String(rawUrl || "").toLowerCase();
	}
}

function classifyImageUrl(rawUrl = "") {
	if (!rawUrl) return "bad";
	let parsed;
	try {
		parsed = new URL(rawUrl);
	} catch {
		return "bad";
	}
	const lower = rawUrl.toLowerCase();
	if (lower.startsWith("data:") || lower.startsWith("blob:")) return "bad";
	const path = parsed.pathname.toLowerCase();
	const extMatch = /\.(jpe?g|png|webp|gif|avif|bmp|tiff|svg)$/.test(path);
	if (extMatch) return "good";

	const host = parsed.hostname.toLowerCase();
	const hostHints = [
		"cdn",
		"static",
		"media",
		"img",
		"image",
		"images",
		"assets",
		"akamai",
		"cloudfront",
		"fastly",
		"gstatic",
		"ggpht",
		"fbcdn",
		"instagram",
		"twimg",
		"scontent",
		"ytimg",
	];
	const pathHints = [
		"/images/",
		"/image/",
		"/img/",
		"/photos/",
		"/photo/",
		"/media/",
		"/uploads/",
		"/upload/",
		"/thumb",
		"/thmb/",
		"/assets/",
		"/static/",
		"/cdn/",
		"/gcdn/",
		"/authoring/",
	];
	const hasHostHint = hostHints.some((hint) => host.includes(hint));
	const hasPathHint = pathHints.some((hint) => path.includes(hint));
	if (hasHostHint || hasPathHint) return "maybe";

	const hasFormat =
		parsed.searchParams.has("format") ||
		parsed.searchParams.has("fm") ||
		parsed.searchParams.has("auto") ||
		parsed.searchParams.has("quality");
	const looksLikeArticlePath =
		/(\/|^)(story|news|article|video|entertainment|sports|tv|movies|celebrity|politics)(\/|$)/.test(
			path
		);
	if (looksLikeArticlePath && !hasFormat) return "bad";
	if (looksLikeArticlePath && hasFormat) return "maybe";
	return hasFormat ? "maybe" : "bad";
}

async function validateImageUrl(
	rawUrl,
	timeoutMs = POTENTIAL_IMAGE_VALIDATE_TIMEOUT_MS
) {
	if (typeof fetch !== "function") return true;
	const supportsAbort =
		typeof AbortController !== "undefined" &&
		typeof AbortController === "function";

	const fetchWithTimeout = async (url, options = {}) => {
		const controller = supportsAbort ? new AbortController() : null;
		let timeoutId;
		try {
			const promise = fetch(url, {
				redirect: "follow",
				signal: controller ? controller.signal : undefined,
				...options,
			});
			let response;
			if (controller) {
				const timeoutPromise = new Promise((_, reject) => {
					timeoutId = setTimeout(() => {
						controller.abort();
						reject(new Error("timeout"));
					}, timeoutMs);
				});
				response = await Promise.race([promise, timeoutPromise]);
			} else {
				response = await promise;
			}
			return response;
		} catch {
			return null;
		} finally {
			if (timeoutId) clearTimeout(timeoutId);
		}
	};

	const head = await fetchWithTimeout(rawUrl, {
		method: "HEAD",
		headers: {
			Accept: "image/*",
		},
	});
	if (head && head.ok) {
		const contentType = head.headers.get("content-type") || "";
		if (contentType.toLowerCase().startsWith("image/")) return true;
	}

	const rangeGet = await fetchWithTimeout(rawUrl, {
		method: "GET",
		headers: {
			Accept: "image/*",
			Range: "bytes=0-0",
		},
	});
	if (rangeGet && rangeGet.ok) {
		const contentType = rangeGet.headers.get("content-type") || "";
		if (contentType.toLowerCase().startsWith("image/")) return true;
	}
	return false;
}

async function validatePotentialImages(images = []) {
	if (!Array.isArray(images) || !images.length) return [];
	if (typeof fetch !== "function") return images;
	const out = [];
	for (const img of images) {
		const quality = img.urlQuality || classifyImageUrl(img.imageurl);
		if (quality === "bad") continue;
		if (quality === "good") {
			out.push(img);
			continue;
		}
		// eslint-disable-next-line no-await-in-loop
		const ok = await validateImageUrl(img.imageurl);
		if (ok) out.push(img);
	}
	return out;
}

function truncateText(value = "", maxLen = 140) {
	const trimmed = String(value || "")
		.replace(/\s+/g, " ")
		.trim();
	if (!trimmed) return "";
	if (trimmed.length <= maxLen) return trimmed;
	return `${trimmed.slice(0, Math.max(0, maxLen - 3)).trim()}...`;
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

function hasTrendSignalData(signals) {
	if (!signals) return false;
	const related = signals.relatedQueries || {};
	const interest = signals.interestOverTime || {};
	return Boolean(
		(Array.isArray(related.top) && related.top.length) ||
			(Array.isArray(related.rising) && related.rising.length) ||
			Number(interest.points) > 0
	);
}

async function fetchTrendSignalsWithOpts(opts) {
	let [relatedRaw, interestRaw] = await Promise.all([
		withTimeout(
			googleTrends.relatedQueries(opts),
			TRENDS_SIGNAL_TIMEOUT_MS
		).catch(() => null),
		withTimeout(
			googleTrends.interestOverTime(opts),
			TRENDS_SIGNAL_TIMEOUT_MS
		).catch(() => null),
	]);
	if (!relatedRaw && !interestRaw) {
		await delay(TRENDS_SIGNAL_RETRY_DELAY_MS);
		[relatedRaw, interestRaw] = await Promise.all([
			withTimeout(
				googleTrends.relatedQueries(opts),
				TRENDS_SIGNAL_TIMEOUT_MS
			).catch(() => null),
			withTimeout(
				googleTrends.interestOverTime(opts),
				TRENDS_SIGNAL_TIMEOUT_MS
			).catch(() => null),
		]);
	}

	const related = parseRelatedQueries(safeParseTrendsJson(relatedRaw) || {});
	const interest = parseInterestOverTime(
		safeParseTrendsJson(interestRaw) || {}
	);
	return { relatedQueries: related, interestOverTime: interest };
}

async function fetchTrendSignalsForKeyword(keyword, { geo, hours } = {}) {
	const safeKeyword = String(keyword || "").trim();
	if (!safeKeyword) return null;
	const windowHours = clampInt(hours || TRENDS_SIGNAL_WINDOW_HOURS, 12, 168);
	const endTime = new Date();
	const startTime = new Date(endTime.getTime() - windowHours * 60 * 60 * 1000);
	const keywordVariants = buildTrendKeywordVariants(safeKeyword);
	let lastSignals = null;

	for (const variant of keywordVariants) {
		const primary = await fetchTrendSignalsWithOpts({
			keyword: variant,
			startTime,
			endTime,
			geo,
		});
		if (!lastSignals) lastSignals = primary;
		if (hasTrendSignalData(primary)) return primary;

		const fallbackHours = clampInt(
			Math.max(windowHours, TRENDS_SIGNAL_FALLBACK_HOURS),
			12,
			168
		);
		if (fallbackHours > windowHours) {
			const fallbackStart = new Date(
				endTime.getTime() - fallbackHours * 60 * 60 * 1000
			);
			const fallbackSignals = await fetchTrendSignalsWithOpts({
				keyword: variant,
				startTime: fallbackStart,
				endTime,
				geo,
			});
			if (hasTrendSignalData(fallbackSignals)) {
				log("Trends API signals fallback window", {
					term: variant,
					hours: fallbackHours,
				});
				return fallbackSignals;
			}
		}

		const defaultSignals = await fetchTrendSignalsWithOpts({
			keyword: variant,
			geo,
		});
		if (hasTrendSignalData(defaultSignals)) {
			log("Trends API signals fallback default window", { term: variant });
			return defaultSignals;
		}

		for (const time of TRENDS_SIGNAL_TIME_FALLBACKS) {
			const timeSignals = await fetchTrendSignalsWithOpts({
				keyword: variant,
				geo,
				time,
			});
			if (hasTrendSignalData(timeSignals)) {
				log("Trends API signals fallback time", { term: variant, time });
				return timeSignals;
			}
		}

		if (geo) {
			const globalSignals = await fetchTrendSignalsWithOpts({
				keyword: variant,
			});
			if (hasTrendSignalData(globalSignals)) {
				log("Trends API signals fallback global", { term: variant });
				return globalSignals;
			}
		}
	}

	return lastSignals;
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
			story?.rawTitle ||
			story?.trendSearchTerm ||
			story?.term ||
			story?.trendDialogTitle ||
			story?.title ||
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
 * Use GPT‑5.1 to generate SEO‑optimized blog + Shorts titles per story.
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
		topics: stories.map((s, idx) => ({
			id: String(idx),
			term: s.title,
			articleTitles: s.articles.map((a) => a.title),
		})),
	};

	try {
		const response = await openai.responses.create({
			model: "gpt-5.2",
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
				"Each topic MUST include the original id field provided.\n" +
				`ALL text must be in ${language}, even if the country/geo differs. No other languages or scripts are allowed.\n` +
				"Your entire reply MUST be valid JSON, no extra commentary.",
			input:
				"Here is the data as JSON:\n\n" +
				JSON.stringify(payload) +
				"\n\n" +
				"Respond with a JSON object of the form:\n" +
				'{ "topics": [ { "id": string, "term": string, "blogTitle": string, "youtubeShortTitle": string, "imageComment": string, "imageDirectives": [ { "aspectRatio": "1280:720", "visualHook": string, "emotion": string, "rationale": string }, { "aspectRatio": "720:1280", "visualHook": string, "emotion": string, "rationale": string } ] } ] }',
			// We skip response_format to avoid model/version compatibility errors
			// and just force JSON via instructions.
		});

		const raw = response.output_text || "";
		const parsed = safeParseOpenAiJson(raw);
		if (!parsed) {
			log("Failed to parse OpenAI JSON:", "unable to recover JSON output");
			return stories;
		}
		const byId = new Map();
		const byTerm = new Map();
		if (parsed && Array.isArray(parsed.topics)) {
			for (const t of parsed.topics) {
				if (!t) continue;
				if (typeof t.id === "string" && t.id.trim()) byId.set(t.id.trim(), t);
				if (typeof t.term === "string" && t.term.trim())
					byTerm.set(t.term.toLowerCase(), t);
			}
		}

		return stories.map((s, idx) => {
			const idKey = String(idx);
			const key = s.title.toLowerCase();
			const match =
				byId.get(idKey) ||
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

/* ───────────────────────────────────────────────────────────── helpers */

const urlFor = ({ geo, hours, category, sort }) => {
	// Clamp hours 1–168 and actually use the requested window.
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

/* ───────────────────────────────────────────────────── cached browser */

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

/* ───────────────────────────────────── article image helpers (Node side) */

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

/* ───────────────────────────────────────────────────────────── scraper */

/* potential image helpers (Puppeteer) */

function normalizeImageUrlKey(rawUrl = "") {
	try {
		const parsed = new URL(rawUrl);
		return `${parsed.origin}${parsed.pathname}`.toLowerCase();
	} catch {
		return String(rawUrl || "").toLowerCase();
	}
}

function normalizePossibleUrl(rawUrl = "", baseUrl = "") {
	const value = String(rawUrl || "").trim();
	if (!value) return "";
	if (value.startsWith("//")) return `https:${value}`;
	if (/^https?:\/\//i.test(value)) return value;
	if (!baseUrl) return value;
	try {
		return new URL(value, baseUrl).toString();
	} catch {
		return value;
	}
}

function selectBestSrcFromSrcset(srcset = "") {
	const entries = String(srcset || "")
		.split(",")
		.map((entry) => entry.trim())
		.filter(Boolean);
	let bestUrl = "";
	let bestScore = 0;
	for (const entry of entries) {
		const parts = entry.split(/\s+/);
		const url = parts[0];
		if (!url) continue;
		let score = 0;
		const size = (parts[1] || "").toLowerCase();
		if (size) {
			const parsed = parseFloat(size);
			if (Number.isFinite(parsed)) {
				score = size.endsWith("w") ? parsed : parsed * 1000;
			}
		}
		if (!bestUrl || score >= bestScore) {
			bestUrl = url;
			bestScore = score;
		}
	}
	return bestUrl;
}

function pickBestImageUrl(raw = {}, baseUrl = "") {
	const candidates = [];
	const srcsetBest =
		selectBestSrcFromSrcset(raw.srcset) ||
		selectBestSrcFromSrcset(raw.dataSrcset);
	if (srcsetBest) candidates.push(srcsetBest);
	if (raw.dataSrc) candidates.push(raw.dataSrc);
	if (raw.src) candidates.push(raw.src);

	for (const candidate of candidates) {
		const normalized = normalizePossibleUrl(candidate, baseUrl);
		if (!normalized) continue;
		if (normalized.startsWith("data:") || normalized.startsWith("blob:")) {
			continue;
		}
		if (/^https?:\/\//i.test(normalized)) return normalized;
	}
	return "";
}

function guessDimensionsFromUrl(rawUrl = "") {
	const output = { width: 0, height: 0 };
	if (!rawUrl) return output;
	const sizeMatch = String(rawUrl).match(/(\d{2,4})[xX](\d{2,4})/);
	if (sizeMatch) {
		const width = Number(sizeMatch[1]);
		const height = Number(sizeMatch[2]);
		if (Number.isFinite(width)) output.width = width;
		if (Number.isFinite(height)) output.height = height;
	}
	try {
		const parsed = new URL(rawUrl);
		const readInt = (key) => {
			const val = parsed.searchParams.get(key);
			const parsedVal = Number(val);
			return Number.isFinite(parsedVal) ? parsedVal : 0;
		};
		output.width =
			output.width ||
			readInt("w") ||
			readInt("width") ||
			readInt("mw") ||
			readInt("maxw") ||
			readInt("resize");
		output.height =
			output.height || readInt("h") || readInt("height") || readInt("mh");
	} catch {
		// ignore URL parse failures
	}
	return output;
}

function resolveImageDimensions(raw = {}, rawUrl = "") {
	let width = Number(raw.width) || 0;
	let height = Number(raw.height) || 0;
	if (!width || !height) {
		const guessed = guessDimensionsFromUrl(rawUrl);
		if (!width && guessed.width) width = guessed.width;
		if (!height && guessed.height) height = guessed.height;
	}
	return { width, height };
}

function isLikelyNonContentImage({ url, alt, className, id } = {}) {
	const haystack = [url, alt, className, id]
		.map((val) => String(val || "").toLowerCase())
		.join(" ");
	if (!haystack) return false;
	if (haystack.includes(".svg") || haystack.includes("image/svg")) return true;
	return POTENTIAL_IMAGE_BAD_HINTS.some((hint) => haystack.includes(hint));
}

function buildImageContext(raw = {}) {
	const parts = [
		raw.alt,
		raw.title,
		raw.ariaLabel,
		raw.figcaption,
		raw.dataCaption,
	];
	if (raw.figureText && String(raw.figureText).trim().length <= 160) {
		parts.push(raw.figureText);
	}
	return parts
		.map((val) =>
			String(val || "")
				.replace(/\s+/g, " ")
				.trim()
		)
		.filter(Boolean)
		.join(" ");
}

function buildImageDescription(context, { storyTitle, articleTitle } = {}) {
	const cleaned = truncateText(context, 140);
	if (cleaned) return cleaned;
	const fallback = truncateText(articleTitle || storyTitle || "", 120);
	return fallback ? `Related image: ${fallback}` : "Related image";
}

function scorePotentialImageCandidate({
	contextMatch,
	inArticle,
	width,
	height,
	hasCaption,
} = {}) {
	let score = 0;
	if (contextMatch) score += 4;
	if (inArticle) score += 2;
	if (hasCaption) score += 1;
	const maxDim = Math.max(Number(width) || 0, Number(height) || 0);
	if (maxDim >= 1600) score += 3;
	else if (maxDim >= 1200) score += 2;
	else if (maxDim >= 800) score += 1;
	return score;
}

function selectPotentialImagesFromRaw(rawImages, options = {}) {
	const {
		articleUrl,
		baseUrl,
		sourceUrl,
		matchTerms,
		focusTerms,
		storyTitle,
		articleTitle,
		storySeen,
		globalSeen,
		perArticleLimit = POTENTIAL_IMAGE_PER_ARTICLE_LIMIT,
	} = options;
	if (!Array.isArray(rawImages) || !rawImages.length) return [];

	const resolvedBaseUrl = baseUrl || articleUrl;
	const defaultSource = normalizePossibleUrl(
		sourceUrl || articleUrl,
		resolvedBaseUrl
	);

	const candidates = [];
	for (const raw of rawImages) {
		const imageUrl = pickBestImageUrl(raw, resolvedBaseUrl);
		if (!imageUrl) continue;
		if (!/^https?:\/\//i.test(imageUrl)) continue;
		if (isLikelyThumbnailUrl(imageUrl)) continue;
		if (
			isLikelyNonContentImage({
				url: imageUrl,
				alt: raw.alt,
				className: raw.className,
				id: raw.id,
			})
		) {
			continue;
		}

		const urlQuality = classifyImageUrl(imageUrl);
		if (urlQuality === "bad") continue;

		const { width, height } = resolveImageDimensions(raw, imageUrl);
		if (
			width &&
			height &&
			(width < POTENTIAL_IMAGE_MIN_WIDTH || height < POTENTIAL_IMAGE_MIN_HEIGHT)
		) {
			continue;
		}

		const context = buildImageContext(raw);
		const contextMatch = matchesAnyTermStrict(context, matchTerms);
		const urlMatch = matchesAnyTermStrict(
			getUrlPathForMatch(imageUrl),
			matchTerms
		);
		const focusActive = Boolean(
			focusTerms &&
				((focusTerms.phrases || []).length || (focusTerms.tokens || []).length)
		);
		const focusMatch = !focusActive
			? true
			: matchesAnyTermStrict(context, focusTerms) ||
			  matchesAnyTermStrict(getUrlPathForMatch(imageUrl), focusTerms);
		if (!focusMatch) continue;
		if (!contextMatch && !urlMatch) continue;

		const score = scorePotentialImageCandidate({
			contextMatch: contextMatch || urlMatch,
			inArticle: raw.inArticle,
			width,
			height,
			hasCaption: Boolean(raw.figcaption),
		});

		candidates.push({
			imageurl: imageUrl,
			description: buildImageDescription(context, { storyTitle, articleTitle }),
			source:
				normalizePossibleUrl(raw.sourceUrl || defaultSource, resolvedBaseUrl) ||
				defaultSource,
			width: width ? String(width) : "",
			height: height ? String(height) : "",
			score,
			urlQuality,
		});
	}

	candidates.sort((a, b) => {
		if (b.score !== a.score) return b.score - a.score;
		return (Number(b.width) || 0) - (Number(a.width) || 0);
	});

	const results = [];
	const pageSeen = new Set();
	for (const candidate of candidates) {
		const key = normalizeImageUrlKey(candidate.imageurl);
		if (pageSeen.has(key)) continue;
		if (storySeen && storySeen.has(key)) continue;
		if (globalSeen && globalSeen.has(key)) continue;
		pageSeen.add(key);
		if (storySeen) storySeen.add(key);
		if (globalSeen) globalSeen.add(key);
		const { score, urlQuality, ...payload } = candidate;
		results.push(payload);
		if (results.length >= perArticleLimit) break;
	}

	return results;
}

async function scrapePotentialImagesFromArticle(
	articleUrl,
	{
		matchTerms,
		focusTerms,
		storyTitle,
		articleTitle,
		storySeen,
		globalSeen,
		perArticleLimit,
	} = {}
) {
	if (!articleUrl) return [];
	const page = await (await getBrowser()).newPage();
	page.setDefaultNavigationTimeout(POTENTIAL_IMAGE_TIMEOUT_MS);
	await page.setUserAgent(BROWSER_UA);

	await page.setRequestInterception(true);
	page.on("request", (req) => {
		const type = req.resourceType();
		if (type === "font") return req.abort();
		return req.continue();
	});

	try {
		const response = await page.goto(articleUrl, {
			waitUntil: "domcontentloaded",
			timeout: POTENTIAL_IMAGE_TIMEOUT_MS,
		});
		log("Potential images navigate", {
			url: articleUrl,
			status: response ? response.status() : "no-response",
		});

		try {
			await page.waitForSelector("img", {
				timeout: POTENTIAL_IMAGE_SELECTOR_TIMEOUT_MS,
			});
		} catch {
			// Continue even if images are slow to load.
		}

		await autoScrollPage(page, {
			scrolls: POTENTIAL_IMAGE_SCROLLS,
			delayMs: POTENTIAL_IMAGE_SCROLL_DELAY_MS,
		});
		await delay(250);

		const blockSignal = await page
			.evaluate(() => {
				const title = document.title || "";
				const bodyText = document.body?.innerText || "";
				return {
					title: title.slice(0, 140),
					text: bodyText.slice(0, 800),
				};
			})
			.catch(() => null);
		if (blockSignal) {
			const combined = `${blockSignal.title} ${blockSignal.text}`.toLowerCase();
			const blocked = POTENTIAL_IMAGE_CAPTCHA_HINTS.some((hint) =>
				combined.includes(hint)
			);
			if (blocked) {
				log("Potential images blocked hint", {
					url: articleUrl,
					title: blockSignal.title,
				});
			}
		}

		const rawImages = await page.evaluate(() => {
			const out = [];
			const tidy = (value, limit = 260) =>
				String(value || "")
					.replace(/\s+/g, " ")
					.trim()
					.slice(0, limit);
			const imgs = Array.from(document.images || []);
			for (const img of imgs) {
				const rect = img.getBoundingClientRect();
				const figure = img.closest("figure");
				const figcaption = figure
					? tidy(figure.querySelector("figcaption")?.innerText || "", 240)
					: "";
				const figureText = figure ? tidy(figure.innerText || "", 240) : "";
				out.push({
					src: img.currentSrc || img.src || "",
					alt: tidy(img.getAttribute("alt") || "", 240),
					title: tidy(img.getAttribute("title") || "", 180),
					ariaLabel: tidy(img.getAttribute("aria-label") || "", 180),
					dataCaption: tidy(img.getAttribute("data-caption") || "", 240),
					width: img.naturalWidth || Math.round(rect.width) || 0,
					height: img.naturalHeight || Math.round(rect.height) || 0,
					dataSrc:
						img.getAttribute("data-src") ||
						img.getAttribute("data-lazy-src") ||
						img.getAttribute("data-original") ||
						img.getAttribute("data-url") ||
						"",
					srcset: img.getAttribute("srcset") || "",
					dataSrcset:
						img.getAttribute("data-srcset") ||
						img.getAttribute("data-lazy-srcset") ||
						"",
					className: img.className || "",
					id: img.id || "",
					inArticle: Boolean(img.closest("article") || img.closest("main")),
					figcaption,
					figureText,
				});
			}
			return out;
		});

		let filtered = selectPotentialImagesFromRaw(rawImages, {
			articleUrl,
			matchTerms,
			focusTerms,
			storyTitle,
			articleTitle,
			storySeen,
			globalSeen,
			perArticleLimit,
		});
		log("Potential images filter", {
			url: articleUrl,
			story: storyTitle || "",
			article: articleTitle || "",
			raw: rawImages.length,
			kept: filtered.length,
		});

		const validated = await validatePotentialImages(filtered);
		if (validated.length !== filtered.length) {
			log("Potential images validate", {
				url: articleUrl,
				kept: validated.length,
				dropped: filtered.length - validated.length,
			});
		}
		filtered = validated;

		if (!filtered.length) {
			const meta = await page
				.evaluate(() => {
					const readMeta = (selector) =>
						document.querySelector(selector)?.getAttribute("content") || "";
					return {
						title:
							readMeta('meta[property="og:title"]') ||
							readMeta('meta[name="twitter:title"]') ||
							document.title ||
							"",
						ogImage:
							readMeta('meta[property="og:image:secure_url"]') ||
							readMeta('meta[property="og:image"]') ||
							readMeta('meta[name="twitter:image:src"]') ||
							readMeta('meta[name="twitter:image"]') ||
							"",
					};
				})
				.catch(() => null);
			const metaTitle = sanitizeHeadlineForMatch(meta?.title || "");
			const ogImage = meta?.ogImage || "";
			const focusActive = Boolean(
				focusTerms &&
					((focusTerms.phrases || []).length ||
						(focusTerms.tokens || []).length)
			);
			const metaTerms = focusActive ? focusTerms : matchTerms;
			if (metaTitle && ogImage && matchesAnyTermStrict(metaTitle, metaTerms)) {
				const resolved = normalizePossibleUrl(ogImage, articleUrl);
				const key = normalizeImageUrlKey(resolved);
				const alreadySeen =
					(storySeen && storySeen.has(key)) ||
					(globalSeen && globalSeen.has(key));
				if (
					resolved &&
					!alreadySeen &&
					!isLikelyThumbnailUrl(resolved) &&
					!isLikelyNonContentImage({ url: resolved })
				) {
					const quality = classifyImageUrl(resolved);
					let ok = quality !== "bad";
					if (ok && quality === "maybe" && typeof fetch === "function") {
						ok = await validateImageUrl(resolved);
					}
					if (ok) {
						if (storySeen) storySeen.add(key);
						if (globalSeen) globalSeen.add(key);
						filtered = [
							{
								imageurl: resolved,
								description: buildImageDescription(metaTitle, {
									storyTitle,
									articleTitle,
								}),
								source: articleUrl,
								width: "",
								height: "",
							},
						];
						log("Potential images og fallback", {
							url: articleUrl,
							title: metaTitle,
						});
					}
				}
			}
		}
		return filtered;
	} catch (err) {
		log(
			"Potential images scrape failed:",
			articleUrl,
			err.message || String(err)
		);
		return [];
	} finally {
		await page.close().catch(() => {});
	}
}

async function scrapeVogueSearchImages({
	query,
	matchTerms,
	focusTerms,
	storyTitle,
	storySeen,
	globalSeen,
	limit = VOGUE_SEARCH_MAX_RESULTS,
} = {}) {
	const safeQuery = String(query || "").trim();
	if (!safeQuery) return [];

	const page = await (await getBrowser()).newPage();
	page.setDefaultNavigationTimeout(POTENTIAL_IMAGE_TIMEOUT_MS);
	await page.setUserAgent(BROWSER_UA);

	await page.setRequestInterception(true);
	page.on("request", (req) => {
		const type = req.resourceType();
		if (type === "font") return req.abort();
		return req.continue();
	});

	const searchUrl = buildVogueSearchUrl(safeQuery);
	log("Vogue search start", { query: safeQuery, url: searchUrl });

	try {
		const response = await page.goto(searchUrl, {
			waitUntil: "domcontentloaded",
			timeout: POTENTIAL_IMAGE_TIMEOUT_MS,
		});
		log("Vogue search navigate", {
			url: searchUrl,
			status: response ? response.status() : "no-response",
		});

		try {
			await page.waitForSelector("img", {
				timeout: VOGUE_SEARCH_SELECTOR_TIMEOUT_MS,
			});
		} catch {
			// Continue even if images are slow to load.
		}

		await autoScrollPage(page, {
			scrolls: VOGUE_SEARCH_SCROLLS,
			delayMs: VOGUE_SEARCH_SCROLL_DELAY_MS,
		});
		await delay(250);

		const blockSignal = await page
			.evaluate(() => {
				const title = document.title || "";
				const bodyText = document.body?.innerText || "";
				return {
					title: title.slice(0, 140),
					text: bodyText.slice(0, 800),
				};
			})
			.catch(() => null);
		if (blockSignal) {
			const combined = `${blockSignal.title} ${blockSignal.text}`.toLowerCase();
			const blocked = POTENTIAL_IMAGE_CAPTCHA_HINTS.some((hint) =>
				combined.includes(hint)
			);
			if (blocked) {
				log("Vogue search blocked hint", {
					url: searchUrl,
					title: blockSignal.title,
				});
			}
		}

		const rawImages = await page.evaluate(() => {
			const out = [];
			const tidy = (value, limit = 260) =>
				String(value || "")
					.replace(/\s+/g, " ")
					.trim()
					.slice(0, limit);
			const imgs = Array.from(document.images || []);
			for (const img of imgs) {
				const rect = img.getBoundingClientRect();
				const card =
					img.closest("article") ||
					img.closest('[data-testid*="search"]') ||
					img.closest('[data-testid*="Search"]') ||
					img.closest("div");
				const link = card?.querySelector("a[href]") || img.closest("a[href]");
				const headline = card?.querySelector("h1,h2,h3")?.textContent || "";
				const kicker = card?.querySelector("p")?.textContent || "";
				const caption = tidy(headline || kicker, 240);
				out.push({
					src: img.currentSrc || img.src || "",
					alt: tidy(img.getAttribute("alt") || "", 240),
					title: tidy(img.getAttribute("title") || "", 180),
					ariaLabel: tidy(img.getAttribute("aria-label") || "", 180),
					dataCaption: caption,
					width: img.naturalWidth || Math.round(rect.width) || 0,
					height: img.naturalHeight || Math.round(rect.height) || 0,
					dataSrc:
						img.getAttribute("data-src") ||
						img.getAttribute("data-lazy-src") ||
						img.getAttribute("data-original") ||
						img.getAttribute("data-url") ||
						"",
					srcset: img.getAttribute("srcset") || "",
					dataSrcset:
						img.getAttribute("data-srcset") ||
						img.getAttribute("data-lazy-srcset") ||
						"",
					className: img.className || "",
					id: img.id || "",
					inArticle: Boolean(card),
					figcaption: "",
					figureText: caption,
					sourceUrl: link ? link.href : "",
				});
			}
			return out;
		});

		let filtered = selectPotentialImagesFromRaw(rawImages, {
			articleUrl: searchUrl,
			baseUrl: searchUrl,
			sourceUrl: searchUrl,
			matchTerms,
			focusTerms,
			storyTitle,
			articleTitle: safeQuery,
			storySeen,
			globalSeen,
			perArticleLimit: clampInt(limit, 1, VOGUE_SEARCH_MAX_RESULTS),
		});
		log("Vogue search filter", {
			url: searchUrl,
			raw: rawImages.length,
			kept: filtered.length,
		});

		const validated = await validatePotentialImages(filtered);
		if (validated.length !== filtered.length) {
			log("Vogue search validate", {
				url: searchUrl,
				kept: validated.length,
				dropped: filtered.length - validated.length,
			});
		}
		filtered = validated;

		return filtered;
	} catch (err) {
		log("Vogue search failed", {
			url: searchUrl,
			error: err.message || String(err),
		});
		return [];
	} finally {
		await page.close().catch(() => {});
	}
}

async function scrapeBbcSearchResults(query) {
	const safeQuery = String(query || "").trim();
	if (!safeQuery) return [];

	const page = await (await getBrowser()).newPage();
	page.setDefaultNavigationTimeout(POTENTIAL_IMAGE_TIMEOUT_MS);
	await page.setUserAgent(BROWSER_UA);

	await page.setRequestInterception(true);
	page.on("request", (req) => {
		const type = req.resourceType();
		if (type === "font") return req.abort();
		return req.continue();
	});

	const searchUrl = buildBbcSearchUrl(safeQuery);
	log("BBC search start", { query: safeQuery, url: searchUrl });

	try {
		const response = await page.goto(searchUrl, {
			waitUntil: "domcontentloaded",
			timeout: POTENTIAL_IMAGE_TIMEOUT_MS,
		});
		log("BBC search navigate", {
			url: searchUrl,
			status: response ? response.status() : "no-response",
		});

		try {
			await page.waitForSelector("a[href]", {
				timeout: BBC_SEARCH_SELECTOR_TIMEOUT_MS,
			});
		} catch {
			// Continue even if results are slow to load.
		}

		await autoScrollPage(page, {
			scrolls: BBC_SEARCH_SCROLLS,
			delayMs: BBC_SEARCH_SCROLL_DELAY_MS,
		});
		await delay(250);

		const blockSignal = await page
			.evaluate(() => {
				const title = document.title || "";
				const bodyText = document.body?.innerText || "";
				return {
					title: title.slice(0, 140),
					text: bodyText.slice(0, 800),
				};
			})
			.catch(() => null);
		if (blockSignal) {
			const combined = `${blockSignal.title} ${blockSignal.text}`.toLowerCase();
			const blocked = POTENTIAL_IMAGE_CAPTCHA_HINTS.some((hint) =>
				combined.includes(hint)
			);
			if (blocked) {
				log("BBC search blocked hint", {
					url: searchUrl,
					title: blockSignal.title,
				});
			}
		}

		const rawResults = await page.evaluate(() => {
			const out = [];
			const tidy = (value, limit = 220) =>
				String(value || "")
					.replace(/\s+/g, " ")
					.trim()
					.slice(0, limit);

			const cards = Array.from(
				document.querySelectorAll('[data-testid="newport-card"]')
			);
			for (const card of cards) {
				const link = card.querySelector("a[href]");
				const headline =
					card.querySelector('[data-testid="card-headline"]') ||
					card.querySelector("h1,h2,h3");
				const title = tidy(headline?.textContent || "", 200);
				const href = link?.href || "";
				if (href && title) out.push({ title, url: href });
			}

			const anchors = Array.from(document.querySelectorAll("main a[href]"));
			for (const anchor of anchors) {
				const headline =
					anchor.querySelector("h1,h2,h3") || anchor.querySelector("span");
				const title = tidy(headline?.textContent || anchor.textContent, 200);
				const href = anchor.href || "";
				if (href && title && title.length >= 6) {
					out.push({ title, url: href });
				}
			}

			return out;
		});

		const seen = new Set();
		const results = [];
		for (const item of rawResults) {
			const url = String(item?.url || "");
			const title = String(item?.title || "").trim();
			if (!url || !title) continue;
			if (!isLikelyBbcArticleUrl(url)) continue;
			const key = url.toLowerCase();
			if (seen.has(key)) continue;
			seen.add(key);
			results.push({ title, url });
			if (results.length >= BBC_SEARCH_MAX_RESULTS) break;
		}

		log("BBC search results", {
			url: searchUrl,
			count: results.length,
		});

		return results;
	} catch (err) {
		log("BBC search failed", {
			url: searchUrl,
			error: err.message || String(err),
		});
		return [];
	} finally {
		await page.close().catch(() => {});
	}
}

async function scrapeBbcSearchImages({
	query,
	matchTerms,
	focusTerms,
	storyTitle,
	storySeen,
	globalSeen,
	limit = POTENTIAL_IMAGE_MIN_PER_STORY,
} = {}) {
	const safeQuery = String(query || "").trim();
	if (!safeQuery) return [];

	const results = await scrapeBbcSearchResults(safeQuery);
	if (!results.length) return [];

	const queryTerms = buildMatchTerms(safeQuery);
	const strongQueryTokens = (queryTerms.tokens || []).filter(
		(token) => token && token.length >= 3
	);
	const minQueryHits = strongQueryTokens.length
		? strongQueryTokens.length >= 2
			? 2
			: 1
		: 0;
	const baseTerms =
		matchTerms && (matchTerms.phrases || matchTerms.tokens)
			? matchTerms
			: queryTerms;
	const scored = results.map((item) => {
		const title = sanitizeHeadlineForMatch(item.title);
		const timestamp = parseCbsDateToTimestamp(item.dateText);
		const ageMinutes = Number.isFinite(timestamp)
			? Math.max(0, Math.round((Date.now() - timestamp) / 60000))
			: null;
		const phraseMatch = hasPhraseMatch(title, queryTerms.phrases);
		const tokenHits = countTokenHits(title, strongQueryTokens);
		const queryOk =
			phraseMatch || (minQueryHits > 0 && tokenHits >= minQueryHits);
		const score = queryOk
			? scoreTextMatch(title, baseTerms) +
			  scoreTextMatch(title, focusTerms || {})
			: 0;
		return { ...item, title, score, tokenHits, ageMinutes };
	});
	scored.sort((a, b) => {
		if (b.score !== a.score) return b.score - a.score;
		if (a.ageMinutes == null && b.ageMinutes != null) return 1;
		if (a.ageMinutes != null && b.ageMinutes == null) return -1;
		if (a.ageMinutes != null && b.ageMinutes != null) {
			if (a.ageMinutes !== b.ageMinutes) return a.ageMinutes - b.ageMinutes;
		}
		return 0;
	});

	const minScore = BBC_SEARCH_MIN_SCORE;
	const candidates = scored.filter((item) => item.score >= minScore);
	if (!candidates.length) {
		log("BBC search skip", {
			query: safeQuery,
			reason: "no-strong-match",
			maxScore: scored[0]?.score || 0,
			maxTokenHits: scored[0]?.tokenHits || 0,
		});
		return [];
	}
	const finalCandidates = candidates.slice(0, BBC_SEARCH_MAX_ARTICLES);

	log("BBC search pick", {
		query: safeQuery,
		picked: finalCandidates.map((item) => ({
			title: item.title,
			url: item.url,
			score: item.score,
		})),
	});

	const images = [];
	for (const item of finalCandidates) {
		if (images.length >= limit) break;
		const searchMatchTerms = buildMatchTerms(safeQuery, item.title, storyTitle);
		// eslint-disable-next-line no-await-in-loop
		const scraped = await scrapePotentialImagesFromArticle(item.url, {
			matchTerms: searchMatchTerms,
			focusTerms,
			storyTitle,
			articleTitle: item.title,
			storySeen,
			globalSeen,
			perArticleLimit: Math.max(1, limit - images.length),
		});
		for (const img of scraped) {
			images.push(img);
			if (images.length >= limit) break;
		}
	}

	return images;
}

async function scrapeCbsSearchResults(query) {
	const safeQuery = sanitizeCbsSearchQuery(query, 90);
	if (!safeQuery) return [];

	const page = await (await getBrowser()).newPage();
	page.setDefaultNavigationTimeout(POTENTIAL_IMAGE_TIMEOUT_MS);
	await page.setUserAgent(BROWSER_UA);

	await page.setRequestInterception(true);
	page.on("request", (req) => {
		const type = req.resourceType();
		if (type === "font") return req.abort();
		return req.continue();
	});

	const searchUrl = buildCbsSearchUrl(safeQuery);
	log("CBS search start", { query: safeQuery, url: searchUrl });

	try {
		const response = await page.goto(searchUrl, {
			waitUntil: "domcontentloaded",
			timeout: POTENTIAL_IMAGE_TIMEOUT_MS,
		});
		log("CBS search navigate", {
			url: searchUrl,
			status: response ? response.status() : "no-response",
		});
		try {
			await page.waitForSelector(".search-results", {
				timeout: CBS_SEARCH_SELECTOR_TIMEOUT_MS,
			});
		} catch {
			// Continue even if results are slow to load.
		}

		await autoScrollPage(page, {
			scrolls: CBS_SEARCH_SCROLLS,
			delayMs: CBS_SEARCH_SCROLL_DELAY_MS,
		});
		await delay(250);

		const blockSignal = await page
			.evaluate(() => {
				const title = document.title || "";
				const bodyText = document.body?.innerText || "";
				return {
					title: title.slice(0, 140),
					text: bodyText.slice(0, 800),
				};
			})
			.catch(() => null);
		if (blockSignal) {
			const combined = `${blockSignal.title} ${blockSignal.text}`.toLowerCase();
			const blocked = POTENTIAL_IMAGE_CAPTCHA_HINTS.some((hint) =>
				combined.includes(hint)
			);
			if (blocked) {
				log("CBS search blocked hint", {
					url: searchUrl,
					title: blockSignal.title,
				});
			}
		}

		const rawResults = await page.evaluate(() => {
			const out = [];
			const tidy = (value, limit = 220) =>
				String(value || "")
					.replace(/\s+/g, " ")
					.trim()
					.slice(0, limit);

			const container =
				document.querySelector("section.search-results") ||
				document.querySelector(".search-results") ||
				document.querySelector("main");
			const cards = Array.from(
				container?.querySelectorAll("article.item") || []
			);
			for (const card of cards) {
				const link =
					card.querySelector("a.item__anchor[href]") ||
					card.querySelector("a[href]");
				const headline =
					card.querySelector(".item__hed") || card.querySelector("h4,h3,h2,h1");
				const dateNode =
					card.querySelector(".item__date") ||
					card.querySelector(".item__metadata .item__date");
				const title = tidy(headline?.textContent || "", 200);
				const dateText = tidy(dateNode?.textContent || "", 60);
				const href = link?.href || "";
				if (href && title) out.push({ title, url: href, dateText });
			}

			const anchors = Array.from(
				(container || document).querySelectorAll("article.item a[href]")
			);
			for (const anchor of anchors) {
				const card = anchor.closest("article.item");
				const headline =
					card?.querySelector(".item__hed") ||
					anchor.querySelector("h4,h3,h2,h1") ||
					anchor.querySelector("span");
				const dateNode =
					card?.querySelector(".item__date") ||
					card?.querySelector(".item__metadata .item__date");
				const title = tidy(headline?.textContent || anchor.textContent, 200);
				const dateText = tidy(dateNode?.textContent || "", 60);
				const href = anchor.href || "";
				if (href && title && title.length >= 6) {
					out.push({ title, url: href, dateText });
				}
			}

			return out;
		});

		const seen = new Set();
		const results = [];
		for (const item of rawResults) {
			const url = String(item?.url || "");
			const title = String(item?.title || "").trim();
			const dateText = String(item?.dateText || "").trim();
			if (!url || !title) continue;
			if (!isLikelyCbsArticleUrl(url)) continue;
			const key = url.toLowerCase();
			if (seen.has(key)) continue;
			seen.add(key);
			results.push({ title, url, dateText });
			if (results.length >= CBS_SEARCH_MAX_RESULTS) break;
		}

		const nonVideo = results.filter((item) => !isCbsVideoUrl(item.url));
		const filteredResults = nonVideo.length ? nonVideo : results;

		log("CBS search results", {
			url: searchUrl,
			count: filteredResults.length,
			nonVideo: nonVideo.length,
		});

		return filteredResults;
	} catch (err) {
		log("CBS search failed", {
			url: searchUrl,
			error: err.message || String(err),
		});
		return [];
	} finally {
		await page.close().catch(() => {});
	}
}

async function scrapeCbsSearchImages({
	query,
	matchTerms,
	focusTerms,
	storyTitle,
	storySeen,
	globalSeen,
	limit = POTENTIAL_IMAGE_MIN_PER_STORY,
} = {}) {
	const safeQuery = sanitizeCbsSearchQuery(query, 90);
	if (!safeQuery) return [];

	const results = await scrapeCbsSearchResults(safeQuery);
	if (!results.length) return [];

	const queryTerms = buildMatchTerms(safeQuery);
	const strongQueryTokens = (queryTerms.tokens || []).filter(
		(token) => token && token.length >= 3
	);
	const minQueryHits = strongQueryTokens.length
		? strongQueryTokens.length >= 2
			? 2
			: 1
		: 0;
	const baseTerms =
		matchTerms && (matchTerms.phrases || matchTerms.tokens)
			? matchTerms
			: queryTerms;
	const scored = results.map((item) => {
		const title = sanitizeHeadlineForMatch(item.title);
		const timestamp = parseCbsDateToTimestamp(item.dateText);
		const ageMinutes = Number.isFinite(timestamp)
			? Math.max(0, Math.round((Date.now() - timestamp) / 60000))
			: null;
		const phraseMatch = hasPhraseMatch(title, queryTerms.phrases);
		const tokenHits = countTokenHits(title, strongQueryTokens);
		const queryOk =
			phraseMatch || (minQueryHits > 0 && tokenHits >= minQueryHits);
		const score = queryOk
			? scoreTextMatch(title, baseTerms) +
			  scoreTextMatch(title, focusTerms || {})
			: 0;
		return { ...item, title, score, tokenHits, ageMinutes };
	});
	const scoredFiltered = (() => {
		const maxAgeMinutes = CBS_SEARCH_MAX_AGE_DAYS * 24 * 60;
		const fresh = scored.filter(
			(item) => item.ageMinutes == null || item.ageMinutes <= maxAgeMinutes
		);
		const nonVideo = fresh.filter((item) => !isCbsVideoUrl(item.url));
		return nonVideo.length ? nonVideo : fresh;
	})();
	scoredFiltered.sort((a, b) => {
		if (b.score !== a.score) return b.score - a.score;
		if (a.ageMinutes == null && b.ageMinutes != null) return 1;
		if (a.ageMinutes != null && b.ageMinutes == null) return -1;
		if (a.ageMinutes != null && b.ageMinutes != null) {
			if (a.ageMinutes !== b.ageMinutes) return a.ageMinutes - b.ageMinutes;
		}
		return 0;
	});

	const minScore = CBS_SEARCH_MIN_SCORE;
	const candidates = scoredFiltered.filter((item) => item.score >= minScore);
	if (!candidates.length) {
		log("CBS search skip", {
			query: safeQuery,
			reason: "no-strong-match",
			maxScore: scoredFiltered[0]?.score || 0,
			maxTokenHits: scoredFiltered[0]?.tokenHits || 0,
		});
		return [];
	}
	const finalCandidates = candidates.slice(0, CBS_SEARCH_MAX_ARTICLES);

	log("CBS search pick", {
		query: safeQuery,
		picked: finalCandidates.map((item) => ({
			title: item.title,
			url: item.url,
			score: item.score,
			date: item.dateText,
			ageMinutes: item.ageMinutes,
		})),
	});

	const images = [];
	for (const item of finalCandidates) {
		if (images.length >= limit) break;
		const searchMatchTerms = buildMatchTerms(safeQuery, item.title, storyTitle);
		// eslint-disable-next-line no-await-in-loop
		const scraped = await scrapePotentialImagesFromArticle(item.url, {
			matchTerms: searchMatchTerms,
			focusTerms,
			storyTitle,
			articleTitle: item.title,
			storySeen,
			globalSeen,
			perArticleLimit: Math.max(1, limit - images.length),
		});
		for (const img of scraped) {
			images.push(img);
			if (images.length >= limit) break;
		}
	}

	return images;
}

async function enrichStoriesWithPotentialImages(
	stories,
	{
		topicLimit,
		minImagesPerStory = 0,
		targetImagesPerStory = POTENTIAL_IMAGE_MAX_PER_STORY,
		topUpTopics = 0,
		enableVogueFallback = false,
		enableBbcFallback = false,
		bbcTopUpTopics = 0,
		enableCbsFallback = false,
		cbsTopUpTopics = 0,
	} = {}
) {
	if (!Array.isArray(stories) || !stories.length) return stories;
	const limit = clampInt(
		topicLimit || POTENTIAL_IMAGE_TOPIC_LIMIT,
		1,
		stories.length
	);
	const minImages = clampInt(
		minImagesPerStory || 0,
		0,
		POTENTIAL_IMAGE_MAX_PER_STORY
	);
	const targetImages = clampInt(
		targetImagesPerStory || POTENTIAL_IMAGE_MAX_PER_STORY,
		minImages,
		POTENTIAL_IMAGE_MAX_PER_STORY
	);
	const topUpLimit = clampInt(topUpTopics || 0, 0, stories.length);
	const bbcTopUpLimit = clampInt(bbcTopUpTopics || 0, 0, stories.length);
	const cbsTopUpLimit = clampInt(cbsTopUpTopics || 0, 0, stories.length);
	const globalSeen = new Set();
	const out = [];

	log("Potential images enrichment start", {
		stories: stories.length,
		enrichCount: limit,
		minImages,
		targetImages,
		topUpLimit,
		vogueFallback: enableVogueFallback,
		bbcTopUpLimit,
		bbcFallback: enableBbcFallback,
		cbsTopUpLimit,
		cbsFallback: enableCbsFallback,
	});

	for (let i = 0; i < stories.length; i++) {
		const story = stories[i];
		if (i >= limit) {
			out.push({ ...story, potentialImages: [] });
			continue;
		}

		try {
			const storySeen = new Set();
			const articles = Array.isArray(story?.articles)
				? story.articles.slice(0, POTENTIAL_IMAGE_ARTICLE_LIMIT)
				: [];
			const potentialImages = [];
			const focusTerms = buildMatchTerms(
				story?.title,
				story?.rawTitle,
				story?.trendDialogTitle,
				story?.entityNames || []
			);
			let articleIndex = 0;

			for (const article of articles) {
				if (!article?.url) continue;
				articleIndex += 1;
				log("Potential images article start", {
					story: story?.title,
					storyIndex: i + 1,
					articleIndex,
					url: article.url,
				});
				const matchTerms = buildMatchTerms(
					story?.title,
					story?.rawTitle,
					story?.trendDialogTitle,
					story?.entityNames || [],
					article?.title
				);

				const scraped = await scrapePotentialImagesFromArticle(article.url, {
					matchTerms,
					focusTerms,
					storyTitle: story?.title,
					articleTitle: article?.title,
					storySeen,
					globalSeen,
					perArticleLimit: POTENTIAL_IMAGE_PER_ARTICLE_LIMIT,
				});
				log("Potential images article done", {
					story: story?.title,
					storyIndex: i + 1,
					articleIndex,
					url: article.url,
					count: scraped.length,
				});

				for (const img of scraped) {
					potentialImages.push(img);
					if (potentialImages.length >= POTENTIAL_IMAGE_MAX_PER_STORY) break;
				}
				if (potentialImages.length >= POTENTIAL_IMAGE_MAX_PER_STORY) break;
				// eslint-disable-next-line no-await-in-loop
				await delay(150);
			}

			const shouldTopUp =
				enableVogueFallback &&
				targetImages > 0 &&
				i < topUpLimit &&
				potentialImages.length < targetImages;
			if (shouldTopUp) {
				const queries = buildVogueSearchQueriesForStory(story);
				let remaining = targetImages - potentialImages.length;
				log("Vogue search topup start", {
					story: story?.title,
					storyIndex: i + 1,
					remaining,
					queries,
				});

				for (const query of queries) {
					if (remaining <= 0) break;
					if (potentialImages.length >= targetImages) break;
					const searchMatchTerms = buildMatchTerms(
						query,
						story?.title,
						story?.rawTitle,
						story?.trendDialogTitle,
						story?.entityNames || []
					);
					// eslint-disable-next-line no-await-in-loop
					const searchImages = await scrapeVogueSearchImages({
						query,
						matchTerms: searchMatchTerms,
						focusTerms,
						storyTitle: story?.title,
						storySeen,
						globalSeen,
						limit: remaining,
					});
					for (const img of searchImages) {
						potentialImages.push(img);
						if (potentialImages.length >= POTENTIAL_IMAGE_MAX_PER_STORY) break;
					}
					remaining = targetImages - potentialImages.length;
				}

				log("Vogue search topup done", {
					story: story?.title,
					storyIndex: i + 1,
					count: potentialImages.length,
				});
			}

			const shouldBbcTopUp =
				enableBbcFallback &&
				targetImages > 0 &&
				i < bbcTopUpLimit &&
				potentialImages.length < targetImages;
			if (shouldBbcTopUp) {
				const queries = buildBbcSearchQueriesForStory(story);
				let remaining = targetImages - potentialImages.length;
				log("BBC search topup start", {
					story: story?.title,
					storyIndex: i + 1,
					remaining,
					queries,
				});

				for (const query of queries) {
					if (remaining <= 0) break;
					if (potentialImages.length >= targetImages) break;
					// eslint-disable-next-line no-await-in-loop
					const searchImages = await scrapeBbcSearchImages({
						query,
						matchTerms: buildMatchTerms(
							query,
							story?.title,
							story?.rawTitle,
							story?.trendDialogTitle,
							story?.entityNames || []
						),
						focusTerms,
						storyTitle: story?.title,
						storySeen,
						globalSeen,
						limit: remaining,
					});
					for (const img of searchImages) {
						potentialImages.push(img);
						if (potentialImages.length >= POTENTIAL_IMAGE_MAX_PER_STORY) break;
					}
					remaining = targetImages - potentialImages.length;
				}

				log("BBC search topup done", {
					story: story?.title,
					storyIndex: i + 1,
					count: potentialImages.length,
				});
			}

			const shouldCbsTopUp =
				enableCbsFallback &&
				targetImages > 0 &&
				i < cbsTopUpLimit &&
				potentialImages.length < targetImages;
			if (shouldCbsTopUp) {
				const queries = buildCbsSearchQueriesForStory(story);
				let remaining = targetImages - potentialImages.length;
				log("CBS search topup start", {
					story: story?.title,
					storyIndex: i + 1,
					remaining,
					queries,
				});

				for (const query of queries) {
					if (remaining <= 0) break;
					if (potentialImages.length >= targetImages) break;
					// eslint-disable-next-line no-await-in-loop
					const searchImages = await scrapeCbsSearchImages({
						query,
						matchTerms: buildMatchTerms(
							query,
							story?.title,
							story?.rawTitle,
							story?.trendDialogTitle,
							story?.entityNames || []
						),
						focusTerms,
						storyTitle: story?.title,
						storySeen,
						globalSeen,
						limit: remaining,
					});
					for (const img of searchImages) {
						potentialImages.push(img);
						if (potentialImages.length >= POTENTIAL_IMAGE_MAX_PER_STORY) break;
					}
					remaining = targetImages - potentialImages.length;
				}

				log("CBS search topup done", {
					story: story?.title,
					storyIndex: i + 1,
					count: potentialImages.length,
				});
			}

			log("Potential images collected", {
				term: story?.title,
				count: potentialImages.length,
			});

			out.push({ ...story, potentialImages });
		} catch (err) {
			log("Potential images failed", {
				term: story?.title,
				error: err.message || String(err),
			});
			out.push({ ...story, potentialImages: [] });
		}
	}

	return out;
}

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

						// If dialog vanished (virtual scroll) → reclick.
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

/* ───────────────────────────────────────────────────────────── express API */

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
			error: "`geo` must be an ISO‑3166 alpha‑2 country code",
		});
	}

	const hours = Number(req.query.hours) || 24;
	const category = req.query.category ?? null;
	const sort = req.query.sort ?? null;
	const isEntertainment = isEntertainmentCategory(category);
	const includeImages = ["1", "true", "yes", "on"].includes(
		String(req.query.includeImages || "").toLowerCase()
	);
	const includePotentialImages = !["0", "false", "no", "off"].includes(
		String(req.query.includePotentialImages || "").toLowerCase()
	);
	const skipOpenAI = ["1", "true", "yes", "on"].includes(
		String(req.query.skipOpenAI || "").toLowerCase()
	);
	const skipSignals = ["1", "true", "yes", "on"].includes(
		String(req.query.skipSignals || "").toLowerCase()
	);
	const cacheKey = buildTrendsCacheKey({
		geo,
		hours,
		category,
		sort,
		includeImages,
		includePotentialImages,
		skipOpenAI,
		skipSignals,
	});
	const now = Date.now();
	for (const [key, entry] of trendsCache.entries()) {
		if (!entry || entry.expiresAt <= now) trendsCache.delete(key);
	}
	const cached = trendsCache.get(cacheKey);
	if (cached && cached.expiresAt > now) {
		log("Trends cache hit", { geo, hours, category, sort });
		return res.json(cached.payload);
	}

	try {
		let stories = await scrape({
			geo,
			hours,
			category,
			sort,
		});

		// Ask GPT‑5.1 for better blog + YouTube titles.
		//    This is optional and skipped if CHATGPT_API_TOKEN / OPENAI_API_KEY
		//    is not configured or the call fails.
		if (!skipOpenAI) {
			stories = await enhanceStoriesWithOpenAI(stories, {
				geo,
				hours,
				category,
			});
		}

		if (!skipSignals) {
			stories = await enrichStoriesWithTrendSignals(stories, {
				geo,
				hours: TRENDS_SIGNAL_WINDOW_HOURS,
			});
		}

		if (includeImages) {
			stories = await hydrateArticleImages(stories);
		}

		if (includePotentialImages) {
			stories = await enrichStoriesWithPotentialImages(stories, {
				topicLimit: POTENTIAL_IMAGE_TOPIC_LIMIT,
				minImagesPerStory: POTENTIAL_IMAGE_MIN_PER_STORY,
				targetImagesPerStory: POTENTIAL_IMAGE_MAX_PER_STORY,
				topUpTopics: isEntertainment ? POTENTIAL_IMAGE_TOPUP_TOPIC_LIMIT : 0,
				enableVogueFallback: isEntertainment,
				enableBbcFallback: true,
				bbcTopUpTopics: POTENTIAL_IMAGE_TOPIC_LIMIT,
				enableCbsFallback: true,
				cbsTopUpTopics: POTENTIAL_IMAGE_TOPIC_LIMIT,
			});
		} else {
			stories = stories.map((s) => ({ ...s, potentialImages: [] }));
		}

		// Strip images here; downstream orchestrator will search high-quality images per ratio.
		stories = stories.map((s) => ({
			...s,
			trendSearchTerm:
				s.trendSearchTerm || s.rawTitle || s.title || s.trendDialogTitle || "",
			potentialImages: Array.isArray(s.potentialImages)
				? s.potentialImages
				: [],
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

		const payload = {
			generatedAt: new Date().toISOString(),
			requestedGeo: geo,
			effectiveGeo: geo, // Trends may redirect, but we fix geo
			hours,
			category,
			stories,
		};
		trendsCache.set(cacheKey, {
			expiresAt: Date.now() + TRENDS_CACHE_TTL_MS,
			payload,
		});
		return res.json(payload);
	} catch (err) {
		console.error(err);
		return res.status(500).json({
			error: "Google Trends scraping failed",
			detail: err.message || String(err),
		});
	}
});

module.exports = router;
