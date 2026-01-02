/** @format */

const fs = require("fs");
const path = require("path");
const child_process = require("child_process");
const crypto = require("crypto");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;
const { TOPIC_STOP_WORDS, GENERIC_TOPIC_TOKENS } = require("./utils");

let ffmpegPath = "";
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
}

const RUNWAY_API_KEY = process.env.RUNWAYML_API_SECRET || "";
const RUNWAY_VERSION = "2024-11-06";
const RUNWAY_IMAGE_MODEL = "gen4_image";
const RUNWAY_IMAGE_POLL_INTERVAL_MS = 2000;
const RUNWAY_IMAGE_MAX_POLL_ATTEMPTS = 120;
const RUNWAY_THUMBNAIL_RATIO = "1920:1080";
const MAX_INPUT_IMAGES = 4;
const DEFAULT_CANVAS_WIDTH = 1280;
const DEFAULT_CANVAS_HEIGHT = 720;
const LEFT_PANEL_PCT = 0.48;
const PANEL_MARGIN_PCT = 0.035;
const PRESENTER_OVERLAP_PCT = 0.06;

const THUMBNAIL_RATIO = "1280:720";
const THUMBNAIL_WIDTH = 1280;
const THUMBNAIL_HEIGHT = 720;
const THUMBNAIL_TEXT_MAX_WORDS = 4;
const THUMBNAIL_TEXT_BASE_MAX_CHARS = 12;
const THUMBNAIL_VARIANT_B_LEFT_PCT = 0.42;
const THUMBNAIL_VARIANT_B_OVERLAP_PCT = 0.1;
const THUMBNAIL_VARIANT_B_PANEL_PCT = 0.18;
const THUMBNAIL_VARIANT_B_TEXT_BOX_PCT = 0.38;
const THUMBNAIL_VARIANT_B_TEXT_MAX_WORDS = 3;
const THUMBNAIL_VARIANT_B_CONTRAST = 1.12;
const THUMBNAIL_VARIANT_B_BASE_CHARS = 10;
const THUMBNAIL_PRESENTER_CUTOUT_DIR = path.resolve(
	__dirname,
	"../uploads/presenter_cutouts"
);
const THUMBNAIL_PRESENTER_SMILE_PATH = "";
const THUMBNAIL_PRESENTER_NEUTRAL_PATH = "";
const THUMBNAIL_PRESENTER_SURPRISED_PATH = "";
const THUMBNAIL_PRESENTER_LIKENESS_MIN = clampNumber(0.82, 0.5, 0.98);
const THUMBNAIL_PRESENTER_FACE_REGION = {
	x: 0.3,
	y: 0.05,
	w: 0.4,
	h: 0.55,
};
const THUMBNAIL_PRESENTER_EYES_REGION = {
	x: 0.32,
	y: 0.08,
	w: 0.36,
	h: 0.22,
};
const QA_PREVIEW_WIDTH = 320;
const QA_PREVIEW_HEIGHT = 180;
const QA_LUMA_MIN = 0.34;
const QA_LUMA_LEFT_MIN = 0.33;
const THUMBNAIL_TEXT_MARGIN_PCT = 0.05;
const THUMBNAIL_TEXT_SIZE_PCT = 0.12;
const THUMBNAIL_TEXT_LINE_SPACING_PCT = 0.2;
const THUMBNAIL_TEXT_Y_OFFSET_PCT = 0.12;
const THUMBNAIL_BADGE_FONT_PCT = 0.045;
const THUMBNAIL_BADGE_X_PCT = 0.05;
const THUMBNAIL_BADGE_Y_PCT = 0.05;
const THUMBNAIL_BADGE_MAX_CHARS = 18;
const THUMBNAIL_TOPIC_MAX_IMAGES = 1;
const THUMBNAIL_TOPIC_MIN_EDGE = 900;
const THUMBNAIL_TOPIC_MIN_BYTES = 60000;
const THUMBNAIL_TOPIC_MAX_DOWNLOADS = 8;
const THUMBNAIL_MIN_BYTES = 12000;
const THUMBNAIL_HOOK_WORDS = [
	"trailer",
	"finale",
	"ending",
	"cast",
	"update",
	"explained",
	"revealed",
	"confirmed",
	"return",
	"comeback",
	"recap",
	"review",
	"reaction",
	"release",
	"season",
	"episode",
	"tour",
	"album",
	"single",
];
const QUESTION_START_TOKENS = new Set([
	"did",
	"does",
	"do",
	"is",
	"are",
	"was",
	"were",
	"will",
	"can",
	"could",
	"should",
	"would",
	"has",
	"have",
	"had",
	"who",
	"what",
	"why",
	"how",
	"when",
	"where",
	"which",
]);
const QUESTION_CONTEXT_SPLIT_TOKENS = new Set([
	"in",
	"on",
	"at",
	"for",
	"with",
	"from",
	"about",
	"into",
	"over",
	"under",
	"after",
	"before",
	"of",
	"vs",
	"vs.",
]);
const IMAGE_DEEMPHASIS_TOKENS = new Set([
	"die",
	"dies",
	"died",
	"dead",
	"death",
	"kill",
	"killed",
	"killing",
	"funeral",
	"murder",
	"suicide",
	"shooting",
]);
const THUMBNAIL_CLOUDINARY_FOLDER = "aivideomatic/long_thumbnails";
const THUMBNAIL_CLOUDINARY_PUBLIC_PREFIX = "long_thumb";
const ACCENT_PALETTE = {
	default: "0xFFC700",
	tech: "0x00D1FF",
	business: "0x00E676",
	entertainment: "0xFF2E63",
};

const GOOGLE_CSE_ID = process.env.GOOGLE_CSE_ID || null;
const GOOGLE_CSE_KEY = process.env.GOOGLE_CSE_KEY || null;
const GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1";
const TRENDS_API_URL =
	process.env.TRENDS_API_URL || "http://localhost:8102/api/google-trends";
const CSE_PREFERRED_IMG_SIZE = "xlarge";
const CSE_FALLBACK_IMG_SIZE = "large";
const CSE_PREFERRED_IMG_COLOR = "color";
const CSE_MIN_IMAGE_SHORT_EDGE = 720;
const CSE_MAX_PAGE_SIZE = 10;
const CSE_MAX_PAGES = 2;
const GOOGLE_IMAGES_SEARCH_ENABLED = true;
const GOOGLE_IMAGES_RESULTS_PER_QUERY = 28;
const GOOGLE_IMAGES_VARIANT_LIMIT = 3;
const GOOGLE_IMAGES_MIN_POOL_MULTIPLIER = 2;
const REQUIRE_THUMBNAIL_TOPIC_IMAGES = true;
const WIKIPEDIA_FALLBACK_ENABLED = true;
const WIKIMEDIA_FALLBACK_ENABLED = true;
const WIKIPEDIA_LANG = "en";
const WIKIPEDIA_API_BASE = `https://${WIKIPEDIA_LANG}.wikipedia.org/w/api.php`;
const WIKIMEDIA_API_BASE = "https://commons.wikimedia.org/w/api.php";

const WATERMARK_URL_TOKENS = [
	"gettyimages",
	"getty",
	"alamy",
	"shutterstock",
	"istock",
	"istockphoto",
	"adobestock",
	"depositphotos",
	"dreamstime",
	"123rf",
	"bigstock",
	"bigstockphoto",
	"fotolia",
	"pond5",
	"envato",
	"stockphoto",
	"stockphotography",
	"imagebroker",
	"imago-images",
	"historicimages",
	"historic-images",
	"wireimage",
	"pressphoto",
	"newscom",
	"pixelsquid",
	"watermark",
];
const THUMBNAIL_MERCH_DISALLOWED_TOKENS = [
	"funko",
	"funko pop",
	"pop vinyl",
	"vinyl figure",
	"vinyl figurine",
	"figurine",
	"action figure",
	"collectible",
	"collectable",
	"toy",
	"unboxing",
	"box set",
	"boxset",
	"packaging",
	"plush",
	"doll",
	"statue",
	"keychain",
	"tshirt",
	"hoodie",
	"mug",
	"sticker",
];
const THUMBNAIL_MERCH_PENALTY_TOKENS = [
	"poster",
	"wallpaper",
	"fanart",
	"fan art",
	"keyart",
	"concept art",
	"illustration",
	"vector",
	"collage",
	"render",
];
const THUMBNAIL_MERCH_DISALLOWED_HOSTS = [
	"amazon.com",
	"ebay.com",
	"etsy.com",
	"walmart.com",
	"target.com",
	"mercari.com",
	"poshmark.com",
	"aliexpress.com",
	"temu.com",
	"funko.com",
	"popinabox.com",
	"popinabox.us",
	"popcultcha.com.au",
	"boxlunch.com",
	"gamestop.com",
	"shopdisney.com",
	"hottopic.com",
	"entertainmentearth.com",
	"bigbadtoystore.com",
];

const RUNWAY_PROMPT_CHAR_LIMIT = 520;

function cleanThumbnailText(text = "") {
	return String(text || "")
		.replace(/([a-z])([A-Z])/g, "$1 $2")
		.replace(/[‘’]/g, "'")
		.replace(/[“”]/g, '"')
		.replace(/[^a-z0-9\s']/gi, " ")
		.replace(/\s+/g, " ")
		.trim();
}

function sanitizeThumbnailContext(text = "") {
	const banned = [
		"death",
		"dead",
		"died",
		"dies",
		"killed",
		"shooting",
		"murder",
		"suicide",
		"funeral",
		"tragedy",
		"tragic",
		"memorial",
		"accident",
		"crash",
		"assault",
		"abuse",
		"lawsuit",
		"arrest",
		"charged",
		"trial",
		"court",
		"injury",
		"hospital",
	];
	let cleaned = String(text || "");
	for (const word of banned) {
		cleaned = cleaned.replace(new RegExp(`\\b${word}\\b`, "gi"), "");
	}
	return cleaned.replace(/\s+/g, " ").trim();
}

function ensureDir(dirPath) {
	if (!dirPath) return;
	if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
}

ensureDir(THUMBNAIL_PRESENTER_CUTOUT_DIR);

function safeUnlink(p) {
	try {
		if (p && fs.existsSync(p)) fs.unlinkSync(p);
	} catch {}
}

function clampNumber(val, min, max) {
	const n = Number(val);
	if (!Number.isFinite(n)) return min;
	if (n < min) return min;
	if (n > max) return max;
	return n;
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

const CONTEXT_STOP_TOKENS = new Set([
	...TOPIC_STOP_WORDS,
	"what",
	"know",
	"known",
	"why",
	"how",
	"reports",
	"report",
	"reported",
	"dies",
	"died",
	"death",
	"dead",
	"age",
	"aged",
	"year",
	"years",
	"says",
	"said",
	"say",
	"details",
	"return",
	"returns",
	"returning",
	"back",
	"revival",
	"reboot",
	"comeback",
	"cast",
	"watch",
	"watching",
	"hulu",
	"netflix",
	"prime",
	"amazon",
	"disney",
	"disneyplus",
	"hbo",
	"hbomax",
	"peacock",
	"paramount",
	"paramountplus",
	"apple",
]);

function filterContextTokens(tokens = []) {
	const norm = normalizeTopicTokens(tokens);
	const filtered = norm.filter(
		(t) =>
			t.length >= 3 &&
			!GENERIC_TOPIC_TOKENS.has(t) &&
			!CONTEXT_STOP_TOKENS.has(t)
	);
	return filtered.length ? filtered : [];
}

function minImageTokenMatches(tokens = []) {
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

function extractQuestionSegments(rawText = "") {
	const raw = String(rawText || "").trim();
	if (!raw) return null;
	const cleaned = cleanThumbnailText(raw);
	if (!cleaned) return null;
	const hasQuestionMark = /\?/.test(raw);
	const tokens = cleaned.toLowerCase().split(" ").filter(Boolean);
	if (!tokens.length) return null;
	const first = tokens[0];
	const hasQuestionWord = QUESTION_START_TOKENS.has(first);
	if (!hasQuestionWord && !hasQuestionMark) return null;
	const questionWord = hasQuestionWord ? first : "";
	const rest = hasQuestionWord ? tokens.slice(1) : tokens;
	if (!rest.length) return null;
	const splitIdx = rest.findIndex((t) => QUESTION_CONTEXT_SPLIT_TOKENS.has(t));
	const subjectTokens = splitIdx > 0 ? rest.slice(0, splitIdx) : rest.slice(0);
	const contextTokens = splitIdx > 0 ? rest.slice(splitIdx + 1) : [];
	return {
		questionWord,
		subjectTokens: subjectTokens.filter(Boolean),
		contextTokens: contextTokens.filter(Boolean),
		hasQuestionMark,
	};
}

function filterImageSearchTokens(tokens = []) {
	const norm = normalizeTopicTokens(tokens);
	if (!norm.length) return [];
	const filtered = norm.filter((t) => !IMAGE_DEEMPHASIS_TOKENS.has(t));
	return filtered.length ? filtered : norm;
}

function filterQuestionSubjectTokens(tokens = []) {
	const filtered = filterImageSearchTokens(filterSpecificTopicTokens(tokens));
	return filtered.length ? filtered : filterImageSearchTokens(tokens);
}

function buildQuestionSubjectLabel(questionInfo, maxWords = 4) {
	if (!questionInfo?.subjectTokens?.length) return "";
	const tokens = filterQuestionSubjectTokens(questionInfo.subjectTokens);
	if (!tokens.length) return "";
	const phrase = tokens.slice(0, maxWords).join(" ");
	return phrase ? titleCaseIfLower(phrase) : "";
}

function buildQuestionSubjectContextLabel(questionInfo, maxWords = 5) {
	if (!questionInfo) return "";
	const subject = filterQuestionSubjectTokens(questionInfo.subjectTokens || []);
	const context = filterQuestionSubjectTokens(questionInfo.contextTokens || []);
	const combined = [...subject, ...context].filter(Boolean).slice(0, maxWords);
	if (!combined.length) return "";
	return titleCaseIfLower(combined.join(" "));
}

function buildSearchLabelTokens(topic = "", extraTokens = []) {
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const baseTokens = filterSpecificTopicTokens([
		...topicTokensFromTitle(topic),
		...extra,
	]);
	const filtered = filterImageSearchTokens(baseTokens);
	const tokens = filtered.length ? filtered : baseTokens;
	return normalizeTopicTokens(tokens).slice(0, 6);
}

function buildTopicIdentityLabel(topic = "", extraTokens = []) {
	const questionInfo = extractQuestionSegments(topic);
	let tokens = [];
	if (questionInfo?.subjectTokens?.length) {
		const subjectContext = buildQuestionSubjectContextLabel(questionInfo, 5);
		if (subjectContext) return subjectContext;
		const subjectOnly = buildQuestionSubjectLabel(questionInfo, 4);
		if (subjectOnly) return subjectOnly;
	}
	if (questionInfo?.contextTokens?.length) {
		tokens = questionInfo.contextTokens;
	} else if (questionInfo?.subjectTokens?.length) {
		tokens = questionInfo.subjectTokens;
	} else {
		tokens = buildSearchLabelTokens(topic, extraTokens);
	}
	const filtered = filterImageSearchTokens(tokens);
	const cleaned = filterSpecificTopicTokens(filtered);
	if (!cleaned.length) return "";
	const phrase = cleaned.slice(0, 4).join(" ");
	return phrase ? titleCaseIfLower(phrase) : "";
}

function buildImageMatchCriteria(topic = "", extraTokens = []) {
	const rawTokens = tokenizeLabel(topic);
	const baseTokens = topicTokensFromTitle(topic);
	const wordSource = baseTokens.length >= 2 ? baseTokens : rawTokens;
	const specificWords = filterSpecificTopicTokens(wordSource);
	const filteredWords = filterImageSearchTokens(
		specificWords.length ? specificWords : wordSource
	);
	const wordTokens = filteredWords.length
		? filteredWords
		: specificWords.length
		? specificWords
		: wordSource;
	const phraseSource = wordTokens.length >= 2 ? wordTokens : rawTokens;
	const phraseToken = phraseSource.length >= 2 ? phraseSource.join(" ") : "";
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const wordSet = new Set(normalizeTopicTokens(wordTokens));
	const contextTokens = filterContextTokens(
		extra.filter((tok) => !wordSet.has(String(tok).toLowerCase()))
	);
	const questionInfo = extractQuestionSegments(topic);
	const subjectTokens = questionInfo?.subjectTokens?.length
		? filterQuestionSubjectTokens(questionInfo.subjectTokens)
		: [];
	return {
		wordTokens,
		phraseToken,
		contextTokens: contextTokens.slice(0, 6),
		minWordMatches: minImageTokenMatches(wordTokens),
		rawTokenCount: rawTokens.length,
		searchTokens: wordTokens,
		subjectTokens,
		minSubjectMatches: subjectTokens.length ? 1 : 0,
	};
}

const THUMBNAIL_PREFERRED_SOURCE_TOKENS = [
	"imdb",
	"wikipedia",
	"wikimedia",
	"disney",
	"netflix",
	"hbomax",
	"hbo",
	"primevideo",
	"paramount",
	"warnerbros",
	"universal",
	"sony",
	"marvel",
	"starwars",
	"reuters",
	"apnews",
	"bbc",
	"cnn",
	"variety",
	"hollywoodreporter",
	"deadline",
	"rollingstone",
];

function scoreSourceAffinity(url = "", contextLink = "") {
	const hay = `${url} ${contextLink}`.toLowerCase();
	for (const token of THUMBNAIL_PREFERRED_SOURCE_TOKENS) {
		if (hay.includes(token)) return 0.35;
	}
	return 0;
}

function scoreThumbnailTopicMatch(url = "", contextLink = "", criteria = null) {
	if (!criteria)
		return { score: 0, wordMatches: 0, contextMatches: 0, subjectMatches: 0 };
	const fields = [url, contextLink];
	const wordInfo = topicMatchInfo(criteria.wordTokens, fields);
	const contextInfo = topicMatchInfo(criteria.contextTokens, fields);
	const subjectInfo = topicMatchInfo(criteria.subjectTokens || [], fields);
	const phraseHit = criteria.phraseToken
		? fields.join(" ").toLowerCase().includes(criteria.phraseToken)
		: false;
	const score =
		wordInfo.count * 1.2 +
		contextInfo.count * 0.6 +
		subjectInfo.count * 1.4 +
		(phraseHit ? 1.2 : 0);
	return {
		score,
		wordMatches: wordInfo.count,
		contextMatches: contextInfo.count,
		subjectMatches: subjectInfo.count,
		phraseHit,
	};
}

function sanitizeOverlayQuery(query = "") {
	return String(query || "")
		.replace(/[^a-z0-9\s]/gi, " ")
		.replace(/\s+/g, " ")
		.trim()
		.slice(0, 80);
}

function normalizeTrendsApiUrl(raw) {
	return String(raw || "")
		.trim()
		.replace(/\/+$/, "");
}

function deriveTrendsServiceBase(raw) {
	const cleaned = normalizeTrendsApiUrl(raw);
	if (!cleaned) return "";
	return cleaned.replace(/\/api\/google-trends$/i, "");
}

function buildGoogleImagesApiCandidates() {
	const list = [];
	const base = deriveTrendsServiceBase(TRENDS_API_URL);
	if (base) {
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

function sanitizeImageQuery(query = "") {
	return String(query || "")
		.replace(/[^a-z0-9\s]/gi, " ")
		.replace(/\s+/g, " ")
		.trim()
		.slice(0, 100);
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
	{ limit = GOOGLE_IMAGES_RESULTS_PER_QUERY, tokens = [] } = {}
) {
	if (!GOOGLE_IMAGES_SEARCH_ENABLED) return [];
	const q = sanitizeImageQuery(query);
	if (!q) return [];
	const endpoints = buildGoogleImagesApiCandidates();
	if (!endpoints.length) return [];
	const matchTokens = filterSpecificTopicTokens(tokens);
	const minMatches = minImageTokenMatches(matchTokens);

	for (const endpoint of endpoints) {
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
			const urls = (raw || [])
				.map((u) => String(u || "").trim())
				.filter((u) => /^https?:\/\//i.test(u))
				.filter((u) => !isLikelyThumbnailUrl(u))
				.filter((u) => !isMerchDisallowedCandidate({ url: u }));
			if (!urls.length) continue;

			let pool = urls;
			if (matchTokens.length) {
				const matched = urls.filter(
					(u) => topicMatchInfo(matchTokens, [u]).count >= minMatches
				);
				if (matched.length) {
					const matchedSet = new Set(matched);
					const rest = urls.filter((u) => !matchedSet.has(u));
					pool = matched.concat(rest);
				}
			}

			return uniqueStrings(pool, {
				limit: Math.max(6, Number(limit) || 12),
			});
		} catch {
			// ignore and try next endpoint
		}
	}

	return [];
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

async function fetchCseItems(
	queries,
	{
		num = 4,
		searchType = null,
		imgSize = null,
		imgColorType = null,
		start = 1,
		maxPages = 1,
	} = {}
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
		let pagesFetched = 0;
		let pageStart = baseStart;
		while (pagesFetched < pageCap) {
			try {
				const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
					params: {
						key: GOOGLE_CSE_KEY,
						cx: GOOGLE_CSE_ID,
						q,
						num: pageSize,
						start: pageStart,
						safe: "active",
						gl: "us",
						hl: "en",
						...(searchType ? { searchType } : {}),
						...(searchType === "image"
							? {
									imgType: "photo",
									imgSize: imgSize || CSE_PREFERRED_IMG_SIZE,
									...(imgColorType ? { imgColorType } : {}),
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
			pagesFetched += 1;
			pageStart += pageSize;
		}
	}
	return results;
}

function isLikelyWatermarkedSource(url = "", contextLink = "") {
	const hay = `${url} ${contextLink}`.toLowerCase();
	return WATERMARK_URL_TOKENS.some((token) => hay.includes(token));
}

function matchesAnyToken(hay = "", tokens = []) {
	if (!hay || !tokens || !tokens.length) return false;
	return tokens.some((token) => hay.includes(token));
}

function isMerchDisallowedCandidate({
	url = "",
	source = "",
	title = "",
} = {}) {
	const hay = `${url} ${source} ${title}`.toLowerCase();
	if (matchesAnyToken(hay, THUMBNAIL_MERCH_DISALLOWED_TOKENS)) return true;
	if (url) {
		try {
			const host = new URL(url).hostname.toLowerCase();
			if (
				THUMBNAIL_MERCH_DISALLOWED_HOSTS.some(
					(h) => host === h || host.endsWith(`.${h}`)
				)
			)
				return true;
		} catch {
			// ignore URL parse errors
		}
	}
	return false;
}

function merchPenaltyScore({ url = "", source = "", title = "" } = {}) {
	const hay = `${url} ${source} ${title}`.toLowerCase();
	return matchesAnyToken(hay, THUMBNAIL_MERCH_PENALTY_TOKENS) ? 0.7 : 0;
}

function isProbablyDirectImageUrl(u) {
	const url = String(u || "").trim();
	if (!/^https?:\/\//i.test(url)) return false;
	return /\.(png|jpe?g|webp)(\?|#|$)/i.test(url);
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

async function headContentType(url, timeoutMs = 8000) {
	try {
		const res = await axios.head(url, {
			timeout: timeoutMs,
			validateStatus: (s) => s >= 200 && s < 400,
			headers: { "User-Agent": "agentai-thumbnail/1.0" },
		});
		const ct = String(res.headers?.["content-type"] || "").toLowerCase();
		return ct || null;
	} catch {
		return null;
	}
}

function buildWikiTitleCandidates(topic = "") {
	const raw = String(topic || "").trim();
	const cleaned = cleanThumbnailText(raw);
	return uniqueStrings([raw, cleaned].filter(Boolean), { limit: 2 });
}

async function fetchWikipediaPageImageUrl(topic = "") {
	if (!WIKIPEDIA_FALLBACK_ENABLED) return null;
	const candidates = buildWikiTitleCandidates(topic);
	if (!candidates.length) return null;
	const tokens = filterSpecificTopicTokens(topicTokensFromTitle(topic));
	const minMatches = Math.max(1, Math.min(2, tokens.length));

	for (const title of candidates) {
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
				headers: { "User-Agent": "agentai-thumbnail/1.0" },
			});

			const pages = data?.query?.pages || {};
			const page = Object.values(pages)[0];
			if (!page || page.missing) continue;
			const pageTitle = String(page.title || "");
			if (
				tokens.length &&
				topicMatchInfo(tokens, [pageTitle]).count < minMatches
			)
				continue;
			const imageUrl = page.original?.source || page.thumbnail?.source || "";
			if (!imageUrl) continue;
			if (isLikelyWatermarkedSource(imageUrl, page.fullurl || "")) continue;
			return imageUrl;
		} catch {
			// ignore and try next
		}
	}

	return null;
}

async function fetchWikimediaImageCandidates(
	query = "",
	{ limit = 3, tokens = [] } = {}
) {
	if (!WIKIMEDIA_FALLBACK_ENABLED) return [];
	const q = sanitizeOverlayQuery(query);
	if (!q) return [];
	const matchTokens = filterSpecificTopicTokens(tokens);
	const minMatches = Math.max(1, Math.min(2, matchTokens.length));
	const target = clampNumber(Number(limit) || 3, 1, 8);

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
			headers: { "User-Agent": "agentai-thumbnail/1.0" },
		});

		const pages = data?.query?.pages || {};
		const items = [];
		for (const page of Object.values(pages)) {
			const title = String(page.title || "");
			if (
				matchTokens.length &&
				topicMatchInfo(matchTokens, [title]).count < minMatches
			)
				continue;
			const info = Array.isArray(page.imageinfo) ? page.imageinfo[0] : null;
			const url = String(info?.url || info?.thumburl || "").trim();
			if (!url) continue;
			const mime = String(info?.mime || "").toLowerCase();
			if (mime && !mime.startsWith("image/")) continue;
			if (isLikelyWatermarkedSource(url, "")) continue;
			items.push({
				url,
				title,
				width: Number(info?.width || 0),
				height: Number(info?.height || 0),
				mime,
			});
			if (items.length >= target * 2) break;
		}
		return items;
	} catch {
		return [];
	}
}

function scoreWikimediaCandidate(candidate) {
	const title = String(candidate?.title || "").toLowerCase();
	let score = 0;
	if (/(logo|title card|title|poster)\b/.test(title)) score += 2;
	if (/(cast|promo|promotional|publicity)\b/.test(title)) score += 1.5;
	if (/(still|screencap|scene|episode)\b/.test(title)) score += 0.5;
	const minEdge = Math.min(
		Number(candidate?.width || 0),
		Number(candidate?.height || 0)
	);
	if (minEdge >= 1200) score += 1;
	else if (minEdge >= 900) score += 0.5;
	return score;
}

async function fetchWikimediaImageUrlsSmart(topic = "", limit = 3) {
	const baseTokens = topicTokensFromTitle(topic);
	const queries = [
		`${topic} logo`,
		`${topic} cast`,
		`${topic} promotional photo`,
		`${topic} title card`,
		`${topic} poster`,
		topic,
	];
	const seen = new Set();
	const scored = [];
	const target = clampNumber(Number(limit) || 3, 1, 8);
	for (const q of queries) {
		const candidates = await fetchWikimediaImageCandidates(q, {
			limit: Math.max(5, target * 2),
			tokens: baseTokens,
		});
		for (const c of candidates) {
			const key = String(c.url || "").toLowerCase();
			if (!key || seen.has(key)) continue;
			seen.add(key);
			scored.push({
				url: c.url,
				score: scoreWikimediaCandidate(c),
				width: c.width,
				height: c.height,
			});
		}
		if (scored.length >= target * 4) break;
	}
	scored.sort((a, b) => {
		if (b.score !== a.score) return b.score - a.score;
		const aSize = (a.width || 0) * (a.height || 0);
		const bSize = (b.width || 0) * (b.height || 0);
		return bSize - aSize;
	});
	return scored.slice(0, target).map((c) => c.url);
}

async function fetchWikimediaImageUrls(topic = "", limit = 3) {
	if (!WIKIMEDIA_FALLBACK_ENABLED) return [];
	const category = inferEntertainmentCategory(topicTokensFromTitle(topic));
	if (category === "tv" || category === "film") {
		const smart = await fetchWikimediaImageUrlsSmart(topic, limit);
		if (smart.length) return smart;
	}
	const candidates = await fetchWikimediaImageCandidates(topic, {
		limit,
		tokens: topicTokensFromTitle(topic),
	});
	return uniqueStrings(candidates.map((c) => c.url).filter(Boolean), {
		limit: clampNumber(Number(limit) || 3, 1, 8),
	});
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
			headers: { "User-Agent": "agentai-thumbnail/1.0" },
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

async function downloadToFile(
	url,
	outPath,
	timeoutMs = 20000,
	retries = 1,
	maxBytes = 8 * 1024 * 1024
) {
	let lastErr = null;
	for (let attempt = 0; attempt <= retries; attempt++) {
		try {
			const res = await axios.get(url, {
				responseType: "stream",
				timeout: timeoutMs,
				headers: { "User-Agent": "agentai-thumbnail/1.0" },
				validateStatus: (s) => s >= 200 && s < 400,
			});
			await new Promise((resolve, reject) => {
				const ws = fs.createWriteStream(outPath);
				let bytes = 0;
				let settled = false;
				const done = (err) => {
					if (settled) return;
					settled = true;
					if (err) {
						ws.destroy();
						reject(err);
						return;
					}
					resolve();
				};
				res.data.on("data", (chunk) => {
					bytes += chunk.length;
					if (bytes > maxBytes) {
						res.data.destroy(new Error("download exceeded maxBytes"));
					}
				});
				res.data.on("error", done);
				ws.on("error", done);
				ws.on("finish", () => done());
				res.data.pipe(ws);
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

function buildThumbnailPrompt({ title, topics, topicImageCount = 0 }) {
	const topicLine = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic)
				.filter(Boolean)
				.join(" / ")
		: "";
	const keywordLine = Array.isArray(topics)
		? topics
				.flatMap((t) => (Array.isArray(t.keywords) ? t.keywords : []))
				.filter(Boolean)
				.slice(0, 10)
				.join(", ")
		: "";
	const contextRaw = [title, topicLine]
		.filter(Boolean)
		.join(" | ")
		.slice(0, 240);
	const safeContext =
		sanitizeThumbnailContext(contextRaw) ||
		cleanThumbnailText(topicLine || title || "");
	const safeKeywords =
		sanitizeThumbnailContext(keywordLine) || cleanThumbnailText(keywordLine);
	const topicFocusRaw = [safeContext, safeKeywords].filter(Boolean).join(" | ");
	const topicFocus =
		topicFocusRaw || cleanThumbnailText(title || "") || "the topic";
	const topicImageLine =
		topicImageCount > 0
			? `Incorporate ${Math.min(
					2,
					Math.max(1, topicImageCount)
			  )} provided topic reference images as clean panels on the left side (soft shadow, slight depth, no clutter). Keep them clearly about ${topicFocus}.`
			: `Add subtle, tasteful visual cues related to: ${topicFocus}.`;

	return `
Create a YouTube thumbnail image (no text in the image).
Use the provided person reference; keep identity, face shape, and wardrobe consistent with the studio desk setup and lighting.
Composition: presenter on the right third (face and shoulders fully inside the right third), leave the left ~40% clean for headline text.
${topicImageLine}
Style: ultra sharp, clean, premium, high contrast, cinematic studio lighting, shallow depth of field, crisp subject separation.
Expression: confident, intrigued, camera-ready.
No candles, no logos, no watermarks, no extra people, no extra hands, no distortion, no text.
`.trim();
}

function buildRunwayThumbnailPrompt({ title, topics }) {
	const topicLine = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic)
				.filter(Boolean)
				.join(" / ")
		: "";
	const keywordLine = Array.isArray(topics)
		? topics
				.flatMap((t) => (Array.isArray(t.keywords) ? t.keywords : []))
				.filter(Boolean)
				.slice(0, 10)
				.join(", ")
		: "";
	const contextRaw = [title, topicLine]
		.filter(Boolean)
		.join(" | ")
		.slice(0, 240);
	const safeContext =
		sanitizeThumbnailContext(contextRaw) ||
		cleanThumbnailText(topicLine || title || "");
	const safeKeywords =
		sanitizeThumbnailContext(keywordLine) || cleanThumbnailText(keywordLine);
	const topicFocusRaw = [safeContext, safeKeywords].filter(Boolean).join(" | ");
	const topicFocus =
		topicFocusRaw || cleanThumbnailText(title || "") || "the topic";

	const prompt = `
Premium studio background plate for a YouTube thumbnail.
No people, no faces, no text, no logos, no watermarks, no candles.
Lighting: BRIGHT, high-key studio lighting with lifted shadows (avoid deep blacks), clean highlights, crisp detail, balanced contrast (not moody, not low-key).
Left ~40% is clean AND BRIGHTER for headline text and panels; keep it uncluttered with a smooth gradient backdrop.
Subtle, tasteful topic atmosphere inspired by: ${topicFocus}.
No dark corners, no heavy vignette, no gloomy cinematic look.
Elegant, vibrant but controlled color palette, clean subject separation feel.
`.trim();

	return prompt.length > RUNWAY_PROMPT_CHAR_LIMIT
		? prompt.slice(0, RUNWAY_PROMPT_CHAR_LIMIT)
		: prompt;
}

function upscaleRunwayRatio(ratio) {
	const norm = String(ratio || "").trim();
	if (!norm) return RUNWAY_THUMBNAIL_RATIO;
	if (norm === "1280:720") return RUNWAY_THUMBNAIL_RATIO;
	if (norm === "720:1280") return "1080:1920";
	if (norm === "720:720") return "1080:1080";
	return norm;
}

async function generateRunwayThumbnailBase({
	jobId,
	tmpDir,
	prompt,
	ratio,
	log,
}) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	const cleanPrompt = String(prompt || "").trim();
	if (!cleanPrompt) throw new Error("thumbnail_prompt_missing");
	ensureDir(tmpDir);

	const runwayRatio = upscaleRunwayRatio(ratio);
	const seed = seedFromText(`${jobId || "thumb"}_${cleanPrompt}`);
	if (log)
		log("thumbnail runway prompt", {
			prompt: cleanPrompt.slice(0, 200),
			ratio: runwayRatio,
		});
	const outputUri = await runwayTextToImage({
		promptText: cleanPrompt,
		ratio: runwayRatio,
		seed,
	});
	const outPath = path.join(
		tmpDir,
		`thumb_runway_${jobId || crypto.randomUUID()}.png`
	);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	const dt = detectFileType(outPath);
	if (dt?.kind !== "image") {
		safeUnlink(outPath);
		throw new Error("thumbnail_runway_invalid_output");
	}
	return outPath;
}

function collectThumbnailInputImages({
	presenterLocalPath,
	candleLocalPath,
	topicImagePaths = [],
}) {
	const list = [];
	const add = (p) => {
		if (!p) return;
		if (!fs.existsSync(p)) return;
		if (!list.includes(p)) list.push(p);
	};
	add(presenterLocalPath);
	add(candleLocalPath);
	for (const p of topicImagePaths) add(p);
	return list.slice(0, MAX_INPUT_IMAGES);
}

function readFileHeader(filePath, bytes = 12) {
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

	return null;
}

function parsePngSize(buf) {
	if (!buf || buf.length < 24) return null;
	return {
		width: buf.readUInt32BE(16),
		height: buf.readUInt32BE(20),
	};
}

function parseJpegSize(buf) {
	if (!buf || buf.length < 4) return null;
	let offset = 2;
	while (offset < buf.length) {
		if (buf[offset] !== 0xff) {
			offset += 1;
			continue;
		}
		const marker = buf[offset + 1];
		// SOF markers
		if (
			(marker >= 0xc0 && marker <= 0xc3) ||
			(marker >= 0xc5 && marker <= 0xc7) ||
			(marker >= 0xc9 && marker <= 0xcb) ||
			(marker >= 0xcd && marker <= 0xcf)
		) {
			if (offset + 8 >= buf.length) return null;
			const height = buf.readUInt16BE(offset + 5);
			const width = buf.readUInt16BE(offset + 7);
			return { width, height };
		}
		if (offset + 4 >= buf.length) break;
		const length = buf.readUInt16BE(offset + 2);
		if (!length) break;
		offset += 2 + length;
	}
	return null;
}

function probeImageDimensions(filePath) {
	try {
		const dt = detectFileType(filePath);
		if (!dt || dt.kind !== "image") return { width: 0, height: 0 };
		const head = readFileHeader(filePath, 256 * 1024);
		if (!head) return { width: 0, height: 0 };
		if (dt.ext === "png") {
			const size = parsePngSize(head);
			return size || { width: 0, height: 0 };
		}
		if (dt.ext === "jpg") {
			const size = parseJpegSize(head);
			return size || { width: 0, height: 0 };
		}
		return { width: 0, height: 0 };
	} catch {
		return { width: 0, height: 0 };
	}
}

function ffprobeDimensions(filePath) {
	try {
		const ffprobePath = resolveFfprobePath();
		const out = child_process
			.execSync(
				`"${ffprobePath}" -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x "${filePath}"`,
				{ stdio: ["ignore", "pipe", "ignore"] }
			)
			.toString()
			.trim();
		const [w, h] = out.split("x").map((n) => Number(n) || 0);
		return { width: w || 0, height: h || 0 };
	} catch {
		return { width: 0, height: 0 };
	}
}

function resolveFfprobePath() {
	let ffprobePath = "ffprobe";
	if (ffmpegPath) {
		const candidate = ffmpegPath.replace(/ffmpeg(\.exe)?$/i, "ffprobe$1");
		if (candidate && candidate !== ffmpegPath) ffprobePath = candidate;
	}
	return ffprobePath;
}

function hasAlphaChannel(filePath) {
	try {
		const ffprobePath = resolveFfprobePath();
		const fmt = child_process
			.execSync(
				`"${ffprobePath}" -v error -select_streams v:0 -show_entries stream=pix_fmt -of default=nw=1:nk=1 "${filePath}"`,
				{ stdio: ["ignore", "pipe", "ignore"] }
			)
			.toString()
			.trim()
			.toLowerCase();
		if (!fmt) return false;
		return fmt.includes("a");
	} catch {
		return false;
	}
}

function inferImageMime(filePath) {
	const head = readFileHeader(filePath, 12);
	if (head && head.length >= 4) {
		// PNG
		if (
			head[0] === 0x89 &&
			head[1] === 0x50 &&
			head[2] === 0x4e &&
			head[3] === 0x47
		)
			return "image/png";
		// JPEG
		if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff)
			return "image/jpeg";
		// WEBP
		if (
			head.toString("ascii", 0, 4) === "RIFF" &&
			head.toString("ascii", 8, 12) === "WEBP"
		)
			return "image/webp";
	}
	const ext = path.extname(filePath).toLowerCase();
	if (ext === ".png") return "image/png";
	if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
	if (ext === ".webp") return "image/webp";
	return null;
}

function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function seedFromText(value) {
	const h = crypto
		.createHash("sha256")
		.update(String(value || ""))
		.digest();
	return h.readUInt32BE(0);
}

function runwayHeadersJson() {
	return {
		Authorization: `Bearer ${RUNWAY_API_KEY}`,
		"X-Runway-Version": RUNWAY_VERSION,
		"Content-Type": "application/json",
	};
}

async function pollRunwayTask(taskId, label) {
	const url = `https://api.dev.runwayml.com/v1/tasks/${taskId}`;
	for (let i = 0; i < RUNWAY_IMAGE_MAX_POLL_ATTEMPTS; i++) {
		await sleep(RUNWAY_IMAGE_POLL_INTERVAL_MS);
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

async function runwayTextToImage({ promptText, ratio, referenceImages, seed }) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	const payload = {
		model: RUNWAY_IMAGE_MODEL,
		promptText: String(promptText || "").slice(0, 1000),
		ratio: String(ratio || RUNWAY_THUMBNAIL_RATIO),
		...(Array.isArray(referenceImages) && referenceImages.length
			? { referenceImages }
			: {}),
		...(Number.isFinite(seed) ? { seed } : {}),
	};

	const res = await axios.post(
		"https://api.dev.runwayml.com/v1/text_to_image",
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
			`Runway text_to_image failed (${res.status}): ${msg.slice(0, 700)}`
		);
	}
	return await pollRunwayTask(res.data.id, "runway_text_to_image");
}

async function downloadRunwayImageToPath({ uri, outPath }) {
	if (!uri) throw new Error("runway output missing");
	const target = String(uri);
	if (target.startsWith("data:image/")) {
		const base64 = target.split(",")[1] || "";
		const buf = Buffer.from(base64, "base64");
		fs.writeFileSync(outPath, buf);
		return outPath;
	}
	if (!/^https?:\/\//i.test(target))
		throw new Error(`unsupported runway output uri: ${target.slice(0, 50)}`);
	const res = await axios.get(target, {
		responseType: "arraybuffer",
		timeout: 30000,
		validateStatus: (s) => s < 500,
	});
	if (res.status >= 300)
		throw new Error(`runway output download failed (${res.status})`);
	fs.writeFileSync(outPath, Buffer.from(res.data));
	return outPath;
}

function sha1(s) {
	return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 12);
}

function fileHash(p) {
	const buf = fs.readFileSync(p);
	return crypto.createHash("sha1").update(buf).digest("hex").slice(0, 12);
}

function poseFromExpression(expression = "", text = "") {
	const expr = String(expression || "").toLowerCase();
	if (expr === "thoughtful") return "thoughtful";
	if (expr === "serious" || expr === "neutral") return "neutral";
	if (expr === "warm" || expr === "excited") return "smile";
	const lower = String(text || "").toLowerCase();
	const hasNegative =
		/(death|died|dead|killed|suicide|murder|arrest|charged|trial|lawsuit|injury|accident|crash|sad|tragic|funeral|hospital|scandal)/.test(
			lower
		);
	const hasPositive =
		/(returns|revival|win|wins|won|success|smile|happy|laugh|funny|hilarious|amazing|great|best|top|comeback|surprise)/.test(
			lower
		);
	if (hasNegative) return "neutral";
	if (hasPositive) return "smile";
	return "smile";
}

function planThumbnailStyle({
	title,
	shortTitle,
	seoTitle,
	topics,
	expression,
}) {
	const topicText = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join(" ")
		: "";
	const text = `${title || ""} ${shortTitle || ""} ${
		seoTitle || ""
	} ${topicText}`
		.trim()
		.toLowerCase();
	const tokens = tokenizeLabel(text);
	const cat = inferEntertainmentCategory(tokens);
	let accent = ACCENT_PALETTE.default;
	if (cat === "film" || cat === "tv" || cat === "celebrity")
		accent = ACCENT_PALETTE.entertainment;
	else if (/(ai|software|code|developer|programming|tech)/i.test(text))
		accent = ACCENT_PALETTE.tech;
	else if (/(money|finance|business|startup)/i.test(text))
		accent = ACCENT_PALETTE.business;

	return {
		pose: poseFromExpression(expression, text),
		accent,
	};
}

function presenterPosePrompt({ pose }) {
	if (pose === "surprised") {
		return `
Isolate the same person from the reference photo as a clean cutout (transparent background).
Same identity, glasses, beard, suit and shirt.
Expression: mild surprised with a soft smile, NOT exaggerated, not screaming.
Eyes: natural, forward-looking, aligned; no crossed eyes or odd gaze.
No distortions, no extra people, sharp and clean.
`.trim();
	}
	if (pose === "thoughtful") {
		return `
Isolate the same person from the reference photo as a clean cutout (transparent background).
Same identity, glasses, beard, suit and shirt.
Expression: thoughtful and calm, natural focus, closed mouth, not exaggerated.
Eyes: natural, forward-looking, aligned; no crossed eyes or odd gaze.
Sharp, clean edges, no distortions.
`.trim();
	}
	if (pose === "neutral") {
		return `
Isolate the same person from the reference photo as a clean cutout (transparent background).
Same identity, glasses, beard, suit and shirt.
Expression: neutral but engaged (calm, confident).
Eyes: natural, forward-looking, aligned; no crossed eyes or odd gaze.
Sharp, clean edges, no distortions.
`.trim();
	}
	return `
Isolate the same person from the reference photo as a clean cutout (transparent background).
Same identity, glasses, beard, suit and shirt.
Expression: warm natural smile (subtle, friendly, not exaggerated).
Eyes: natural, forward-looking, aligned; no crossed eyes or odd gaze.
Sharp, clean edges, no distortions.
`.trim();
}

function presenterCutoutKey(pose = "") {
	const norm = String(pose || "").toLowerCase();
	if (norm === "surprised") return "surprised";
	if (norm === "neutral" || norm === "thoughtful") return "neutral";
	return "smile";
}

function resolvePresenterCutoutPath(pose = "", fallbackDir = "") {
	const key = presenterCutoutKey(pose);
	const direct =
		key === "smile"
			? THUMBNAIL_PRESENTER_SMILE_PATH
			: key === "surprised"
			? THUMBNAIL_PRESENTER_SURPRISED_PATH
			: THUMBNAIL_PRESENTER_NEUTRAL_PATH;
	if (direct && fs.existsSync(direct)) {
		const dt = detectFileType(direct);
		if (dt?.kind === "image") return direct;
	}
	const searchDirs = [];
	if (THUMBNAIL_PRESENTER_CUTOUT_DIR)
		searchDirs.push(THUMBNAIL_PRESENTER_CUTOUT_DIR);
	if (fallbackDir) searchDirs.push(fallbackDir);
	if (!searchDirs.length) return "";

	const names = [
		`presenter_${key}.png`,
		`presenter_${key}.webp`,
		`presenter_${key}.jpg`,
		`${key}.png`,
		`${key}.webp`,
		`${key}.jpg`,
	];
	for (const dir of searchDirs) {
		for (const name of names) {
			const candidate = path.join(dir, name);
			if (!fs.existsSync(candidate)) continue;
			const dt = detectFileType(candidate);
			if (dt?.kind === "image") return candidate;
		}
	}
	return "";
}

function makeEven(n) {
	const x = Math.round(Number(n) || 0);
	if (!Number.isFinite(x) || x <= 0) return 2;
	return x % 2 === 0 ? x : x + 1;
}

function runFfmpeg(args, label = "ffmpeg") {
	return new Promise((resolve, reject) => {
		if (!ffmpegPath) return reject(new Error("ffmpeg not available"));
		const proc = child_process.spawn(ffmpegPath, args, {
			stdio: ["ignore", "pipe", "pipe"],
			windowsHide: true,
		});
		let stderr = "";
		proc.stderr.on("data", (d) => (stderr += d.toString()));
		proc.on("error", reject);
		proc.on("close", (code) => {
			if (code === 0) return resolve();
			const head = stderr.slice(0, 4000);
			reject(new Error(`${label} failed (code ${code}): ${head}`));
		});
	});
}

function runFfmpegBuffer(args, label = "ffmpeg_buffer") {
	if (!ffmpegPath) throw new Error("ffmpeg not available");
	const res = child_process.spawnSync(ffmpegPath, args, {
		encoding: null,
		windowsHide: true,
	});
	if (res.status === 0) return res.stdout || Buffer.alloc(0);
	const err = (res.stderr || Buffer.alloc(0)).toString().slice(0, 4000);
	throw new Error(`${label} failed (code ${res.status}): ${err}`);
}

function computeImageHash(filePath, regionPct = null) {
	if (!ffmpegPath) return null;
	const dims = ffprobeDimensions(filePath);
	if (!dims.width || !dims.height) return null;
	let crop = "";
	if (regionPct && dims.width && dims.height) {
		const rx = Math.max(0, Math.round(dims.width * (regionPct.x || 0)));
		const ry = Math.max(0, Math.round(dims.height * (regionPct.y || 0)));
		const rw = Math.max(1, Math.round(dims.width * (regionPct.w || 1)));
		const rh = Math.max(1, Math.round(dims.height * (regionPct.h || 1)));
		crop = `crop=${rw}:${rh}:${rx}:${ry},`;
	}
	const filter = `${crop}scale=9:8:flags=area,format=gray`;
	const args = [
		"-hide_banner",
		"-loglevel",
		"error",
		"-i",
		filePath,
		"-vf",
		filter,
		"-frames:v",
		"1",
		"-f",
		"rawvideo",
		"pipe:1",
	];
	try {
		const buf = runFfmpegBuffer(args, "thumbnail_hash");
		if (!buf || buf.length < 72) return null;
		const bits = new Array(64);
		let idx = 0;
		for (let y = 0; y < 8; y++) {
			for (let x = 0; x < 8; x++) {
				const left = buf[y * 9 + x];
				const right = buf[y * 9 + x + 1];
				bits[idx] = left > right ? 1 : 0;
				idx += 1;
			}
		}
		return bits;
	} catch {
		return null;
	}
}

function hashSimilarity(a, b) {
	if (!a || !b || a.length !== b.length) return null;
	let diff = 0;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) diff += 1;
	}
	return 1 - diff / a.length;
}

function comparePresenterSimilarity(originalPath, candidatePath) {
	const regions = [
		THUMBNAIL_PRESENTER_EYES_REGION,
		THUMBNAIL_PRESENTER_FACE_REGION,
	];
	const scores = [];
	for (const region of regions) {
		const a = computeImageHash(originalPath, region);
		const b = computeImageHash(candidatePath, region);
		const score = hashSimilarity(a, b);
		if (Number.isFinite(score)) scores.push(score);
	}
	if (!scores.length) return null;
	const sum = scores.reduce((acc, v) => acc + v, 0);
	return sum / scores.length;
}

function samplePreviewLuma(filePath, region = null) {
	const w = QA_PREVIEW_WIDTH;
	const h = QA_PREVIEW_HEIGHT;
	let crop = "";
	if (region) {
		const rx = Math.max(0, Math.round(region.x || 0));
		const ry = Math.max(0, Math.round(region.y || 0));
		const rw = Math.max(1, Math.round(region.w || 1));
		const rh = Math.max(1, Math.round(region.h || 1));
		crop = `crop=${rw}:${rh}:${rx}:${ry},`;
	}
	const filter = `scale=${w}:${h}:flags=area,${crop}scale=1:1:flags=area,format=rgb24`;
	const args = [
		"-hide_banner",
		"-loglevel",
		"error",
		"-i",
		filePath,
		"-vf",
		filter,
		"-frames:v",
		"1",
		"-f",
		"rawvideo",
		"pipe:1",
	];
	try {
		const buf = runFfmpegBuffer(args, "thumbnail_luma");
		if (!buf || buf.length < 3) return null;
		const r = buf[0];
		const g = buf[1];
		const b = buf[2];
		return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
	} catch {
		return null;
	}
}

function samplePreviewRgb(filePath, region = null) {
	const w = QA_PREVIEW_WIDTH;
	const h = QA_PREVIEW_HEIGHT;
	let crop = "";
	if (region) {
		const rx = Math.max(0, Math.round(region.x || 0));
		const ry = Math.max(0, Math.round(region.y || 0));
		const rw = Math.max(1, Math.round(region.w || 1));
		const rh = Math.max(1, Math.round(region.h || 1));
		crop = `crop=${rw}:${rh}:${rx}:${ry},`;
	}
	const filter = `scale=${w}:${h}:flags=area,${crop}scale=1:1:flags=area,format=rgb24`;
	const args = [
		"-hide_banner",
		"-loglevel",
		"error",
		"-i",
		filePath,
		"-vf",
		filter,
		"-frames:v",
		"1",
		"-f",
		"rawvideo",
		"pipe:1",
	];
	try {
		const buf = runFfmpegBuffer(args, "thumbnail_rgb");
		if (!buf || buf.length < 3) return null;
		return { r: buf[0], g: buf[1], b: buf[2] };
	} catch {
		return null;
	}
}

function analyzeImageTone(filePath) {
	const rgb = samplePreviewRgb(filePath);
	if (!rgb) return null;
	const total = rgb.r + rgb.g + rgb.b;
	const bRatio = total > 0 ? rgb.b / total : 0;
	const rgOverB = rgb.b > 0 ? (rgb.r + rgb.g) / (2 * rgb.b) : null;
	const luma = (0.2126 * rgb.r + 0.7152 * rgb.g + 0.0722 * rgb.b) / 255;
	return { rgb, luma, bRatio, rgOverB };
}

function scoreTopicImageCandidate(candidate) {
	const minEdge = Math.min(candidate.width || 0, candidate.height || 0);
	const sizeScore = clampNumber(minEdge / 1200, 0, 1);
	const luma = candidate?.tone?.luma;
	const lumaScore = Number.isFinite(luma)
		? 1 - Math.min(Math.abs(luma - 0.45) / 0.35, 1)
		: 0.4;
	const bRatio = candidate?.tone?.bRatio;
	let colorScore = 0.5;
	if (Number.isFinite(bRatio)) {
		const target = 0.3;
		colorScore = 1 - Math.min(Math.abs(bRatio - target) / 0.12, 1);
	}
	const rgOverB = candidate?.tone?.rgOverB;
	const warmPenalty = Number.isFinite(rgOverB) && rgOverB > 1.7 ? 0.2 : 0;
	const matchScoreRaw = Number.isFinite(candidate?.matchScore)
		? candidate.matchScore
		: 0.6;
	const matchScore = clampNumber(matchScoreRaw / 4, 0, 1);
	const sourceScore = Number.isFinite(candidate?.sourceScore)
		? clampNumber(candidate.sourceScore, 0, 1)
		: 0;
	return (
		sizeScore * 0.35 +
		lumaScore * 0.2 +
		colorScore * 0.1 +
		matchScore * 0.3 +
		sourceScore * 0.05 -
		warmPenalty
	);
}

function shouldNeutralizeTopicImage(tone) {
	if (!tone) return false;
	if (!Number.isFinite(tone.bRatio) || !Number.isFinite(tone.rgOverB))
		return false;
	return tone.bRatio < 0.23 && tone.rgOverB > 1.7;
}

async function normalizeTopicImageIfNeeded({
	inputPath,
	tone,
	tmpDir,
	jobId,
	index = 0,
	log,
}) {
	if (!ffmpegPath) return inputPath;
	if (!shouldNeutralizeTopicImage(tone)) return inputPath;
	const outPath = path.join(tmpDir, `thumb_topic_norm_${jobId}_${index}.jpg`);
	const filter =
		"colorchannelmixer=rr=0.95:gg=0.97:bb=1.10," +
		"eq=contrast=1.02:saturation=0.95:brightness=0.02";
	await runFfmpeg(
		[
			"-i",
			inputPath,
			"-vf",
			filter,
			"-frames:v",
			"1",
			"-q:v",
			"2",
			"-y",
			outPath,
		],
		"thumbnail_topic_colorfix"
	);
	safeUnlink(inputPath);
	if (log)
		log("thumbnail topic image neutralized", {
			path: path.basename(outPath),
			bRatio: Number(tone.bRatio.toFixed(3)),
			rgOverB: Number(tone.rgOverB.toFixed(2)),
		});
	return outPath;
}

function getQaLeftSampleRegion() {
	return {
		x: 0,
		y: Math.round(QA_PREVIEW_HEIGHT * 0.45),
		w: Math.round(QA_PREVIEW_WIDTH * 0.45),
		h: Math.round(QA_PREVIEW_HEIGHT * 0.25),
	};
}

async function applyThumbnailQaAdjustments(filePath, { log } = {}) {
	if (!ffmpegPath) return { applied: false };
	const beforeOverall = samplePreviewLuma(filePath);
	const beforeLeft = samplePreviewLuma(filePath, getQaLeftSampleRegion());
	if (beforeOverall == null || beforeLeft == null) return { applied: false };

	const targetOverall = QA_LUMA_MIN;
	const targetLeft = QA_LUMA_LEFT_MIN;
	if (log) {
		log("thumbnail qa precheck", {
			beforeOverall: Number(beforeOverall.toFixed(3)),
			beforeLeft: Number(beforeLeft.toFixed(3)),
			targetOverall,
			targetLeft,
		});
	}
	if (beforeOverall >= targetOverall && beforeLeft >= targetLeft) {
		return {
			applied: false,
			before: { overall: beforeOverall, left: beforeLeft },
			after: { overall: beforeOverall, left: beforeLeft },
			passes: 0,
		};
	}

	const passes = [
		{ contrast: 1.05, saturation: 1.1, brightness: 0.07, gamma: 0.92 },
		{ contrast: 1.08, saturation: 1.15, brightness: 0.1, gamma: 0.9 },
	];

	let afterOverall = beforeOverall;
	let afterLeft = beforeLeft;
	let used = 0;

	for (const p of passes) {
		used += 1;
		const tmp = path.join(
			path.dirname(filePath),
			`thumb_qalift_${used}_${path.basename(filePath)}`
		);
		await runFfmpeg(
			[
				"-i",
				filePath,
				"-vf",
				`eq=contrast=${p.contrast}:saturation=${p.saturation}:brightness=${p.brightness}:gamma=${p.gamma}`,
				"-frames:v",
				"1",
				"-q:v",
				"2",
				"-y",
				tmp,
			],
			"thumbnail_qa_lift"
		);
		safeUnlink(filePath);
		fs.renameSync(tmp, filePath);

		const nextOverall = samplePreviewLuma(filePath);
		const nextLeft = samplePreviewLuma(filePath, getQaLeftSampleRegion());
		if (nextOverall == null || nextLeft == null) break;
		afterOverall = nextOverall;
		afterLeft = nextLeft;
		if (afterOverall >= targetOverall && afterLeft >= targetLeft) break;
	}

	if (log) {
		log("thumbnail qa lift applied", {
			beforeOverall: Number(beforeOverall.toFixed(3)),
			beforeLeft: Number(beforeLeft.toFixed(3)),
			afterOverall: Number(afterOverall.toFixed(3)),
			afterLeft: Number(afterLeft.toFixed(3)),
			passes: used,
		});
	}

	return {
		applied: used > 0,
		before: { overall: beforeOverall, left: beforeLeft },
		after: { overall: afterOverall, left: afterLeft },
		passes: used,
	};
}

function scoreThumbnailLuma({ overall, left }) {
	if (!Number.isFinite(overall) || !Number.isFinite(left)) return -Infinity;
	const targetOverall = clampNumber(QA_LUMA_MIN + 0.02, 0.32, 0.42);
	const targetLeft = clampNumber(QA_LUMA_LEFT_MIN + 0.01, 0.3, 0.4);
	const overallScore = 1 - Math.min(Math.abs(overall - targetOverall) / 0.1, 1);
	const leftScore = 1 - Math.min(Math.abs(left - targetLeft) / 0.08, 1);
	let score = overallScore * 0.65 + leftScore * 0.35;
	const overLimit = targetOverall + 0.1;
	if (overall > overLimit) score -= (overall - overLimit) * 2;
	if (left < QA_LUMA_LEFT_MIN) score -= (QA_LUMA_LEFT_MIN - left) * 2;
	return score;
}

async function generateFallbackBackground({
	sourcePath,
	outPath,
	width = DEFAULT_CANVAS_WIDTH,
	height = DEFAULT_CANVAS_HEIGHT,
	log,
}) {
	const W = makeEven(Math.max(2, Math.round(width)));
	const H = makeEven(Math.max(2, Math.round(height)));
	const baseColor = "0x1b1b1b";
	const baseArgs = [
		"-f",
		"lavfi",
		"-i",
		`color=c=${baseColor}:s=${W}x${H}`,
		"-frames:v",
		"1",
		"-q:v",
		"2",
		"-y",
		outPath,
	];

	if (sourcePath && fs.existsSync(sourcePath)) {
		try {
			const args = [
				"-f",
				"lavfi",
				"-i",
				`color=c=${baseColor}:s=${W}x${H}`,
				"-i",
				sourcePath,
				"-filter_complex",
				`[1:v]scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H},format=rgba,boxblur=24:1,eq=brightness=0.02:saturation=1.05[blur];[0:v][blur]overlay=0:0[outv]`,
				"-map",
				"[outv]",
				"-frames:v",
				"1",
				"-q:v",
				"2",
				"-y",
				outPath,
			];
			await runFfmpeg(args, "thumbnail_fallback_bg");
			return outPath;
		} catch (e) {
			if (log)
				log("thumbnail fallback bg failed; using solid color", {
					error: e?.message || String(e),
				});
		}
	}

	await runFfmpeg(baseArgs, "thumbnail_fallback_solid");
	return outPath;
}

async function composeThumbnailBase({
	baseImagePath,
	presenterImagePath,
	topicImagePaths = [],
	outPath,
	width = DEFAULT_CANVAS_WIDTH,
	height = DEFAULT_CANVAS_HEIGHT,
	accentColor = ACCENT_PALETTE.default,
	layout = {},
}) {
	const W = makeEven(Math.max(2, Math.round(width)));
	const H = makeEven(Math.max(2, Math.round(height)));
	const leftPct = Number.isFinite(Number(layout.leftPanelPct))
		? Number(layout.leftPanelPct)
		: LEFT_PANEL_PCT;
	const marginPct = Number.isFinite(Number(layout.panelMarginPct))
		? Number(layout.panelMarginPct)
		: PANEL_MARGIN_PCT;
	const overlapPct = Number.isFinite(Number(layout.presenterOverlapPct))
		? Number(layout.presenterOverlapPct)
		: PRESENTER_OVERLAP_PCT;
	const leftW = Math.max(1, Math.round(W * leftPct));
	const margin = Math.max(4, Math.round(W * marginPct));
	const overlap = Math.max(0, Math.round(W * overlapPct));
	let presenterW = Math.max(2, W - leftW + overlap);
	presenterW = makeEven(Math.min(W, presenterW));
	const presenterX = Math.max(0, Math.min(W - presenterW, leftW - overlap));

	const topics = Array.isArray(topicImagePaths)
		? topicImagePaths.filter(Boolean).slice(0, 2)
		: [];
	const panelCount = topics.length;
	const hasSinglePanel = panelCount === 1;
	const panelMargin = hasSinglePanel ? 0 : margin;
	const topPad = hasSinglePanel
		? 0
		: panelCount === 1
		? Math.round(H * 0.22)
		: 0;
	const panelW = makeEven(Math.max(2, leftW - panelMargin * 2));
	const panelH =
		panelCount > 1
			? makeEven(Math.max(2, Math.round((H - panelMargin * 3) / 2)))
			: makeEven(Math.max(2, H - panelMargin * 2 - topPad));
	const panelBorder = Math.max(4, Math.round(W * 0.004));
	const panelInnerW = makeEven(Math.max(2, panelW - panelBorder * 2));
	const panelInnerH = makeEven(Math.max(2, panelH - panelBorder * 2));
	const presenterHasAlpha = hasAlphaChannel(presenterImagePath);

	const inputs = [baseImagePath, presenterImagePath, ...topics];
	const filters = [];
	filters.push(
		`[0:v]scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H}[base]`
	);
	filters.push(
		`[1:v]scale=${presenterW}:${H}:force_original_aspect_ratio=increase:flags=lanczos,` +
			`crop=${presenterW}:${H}:(iw-ow)/2:(ih-oh)/2,format=rgba,` +
			`eq=contrast=1.05:saturation=1.04:brightness=0.035:gamma=0.95,` +
			`unsharp=3:3:0.45[presenter]`
	);
	if (presenterHasAlpha) {
		filters.push("[presenter]split=2[p][ps]");
		filters.push(
			"[ps]colorchannelmixer=rr=0:gg=0:bb=0:aa=0.55,boxblur=18:1[shadow]"
		);
	} else {
		filters.push("[presenter]null[p]");
	}

	let current = "[base]";

	if (panelCount >= 1) {
		const panel1Idx = 2;
		const panelY =
			panelCount > 1
				? panelMargin
				: Math.max(0, Math.round(panelMargin + topPad));
		const panelCropX = "(iw-ow)/2";
		const panelCropY = hasSinglePanel ? "(ih-oh)*0.22" : "(ih-oh)*0.35";
		if (hasSinglePanel) {
			filters.push(
				`[${panel1Idx}:v]scale=${panelInnerW}:${panelInnerH}:force_original_aspect_ratio=increase:flags=lanczos,` +
					`crop=${panelInnerW}:${panelInnerH}:${panelCropX}:${panelCropY},boxblur=12:1,` +
					`eq=contrast=1.02:saturation=1.02:brightness=0.02,` +
					`format=rgba[panel1bg]`
			);
			filters.push(
				`[${panel1Idx}:v]scale=${panelInnerW}:${panelInnerH}:force_original_aspect_ratio=increase:flags=lanczos,` +
					`crop=${panelInnerW}:${panelInnerH}:${panelCropX}:${panelCropY},` +
					`eq=contrast=1.07:saturation=1.10:brightness=0.06:gamma=0.95,` +
					`unsharp=3:3:0.35,format=rgba[panel1fg]`
			);
			filters.push("[panel1bg][panel1fg]overlay=0:0[panel1i]");
		} else {
			filters.push(
				`[${panel1Idx}:v]scale=${panelInnerW}:${panelInnerH}:force_original_aspect_ratio=increase:flags=lanczos,` +
					`crop=${panelInnerW}:${panelInnerH}:${panelCropX}:${panelCropY},` +
					`eq=contrast=1.07:saturation=1.10:brightness=0.06:gamma=0.95,` +
					`unsharp=3:3:0.35[panel1i]`
			);
		}
		filters.push(
			`[panel1i]pad=${panelW}:${panelH}:${panelBorder}:${panelBorder}:color=${accentColor}@0.55[panel1]`
		);
		filters.push(`${current}[panel1]overlay=${panelMargin}:${panelY}[tmp1]`);
		current = "[tmp1]";
	}

	if (panelCount >= 2) {
		const panel2Idx = 3;
		const panel2Y = Math.max(0, panelMargin * 2 + panelH);
		const panelCropX = "(iw-ow)/2";
		const panelCropY = "(ih-oh)*0.35";
		filters.push(
			`[${panel2Idx}:v]scale=${panelInnerW}:${panelInnerH}:force_original_aspect_ratio=increase:flags=lanczos,` +
				`crop=${panelInnerW}:${panelInnerH}:${panelCropX}:${panelCropY},` +
				`eq=contrast=1.07:saturation=1.10:brightness=0.06:gamma=0.95,` +
				`unsharp=3:3:0.35[panel2i]`
		);
		filters.push(
			`[panel2i]pad=${panelW}:${panelH}:${panelBorder}:${panelBorder}:color=${accentColor}@0.55[panel2]`
		);
		filters.push(`${current}[panel2]overlay=${margin}:${panel2Y}[tmp2]`);
		current = "[tmp2]";
	}

	if (presenterHasAlpha) {
		filters.push(`${current}[shadow]overlay=${presenterX + 10}:12[tmpS]`);
		filters.push(`[tmpS][p]overlay=${presenterX}:0[outv]`);
	} else {
		filters.push(`${current}[p]overlay=${presenterX}:0[outv]`);
	}

	const outExt = path.extname(outPath).toLowerCase();
	const useJpegQ = outExt === ".jpg" || outExt === ".jpeg";
	const args = [];
	for (const input of inputs) {
		args.push("-i", input);
	}
	args.push(
		"-filter_complex",
		filters.join(";"),
		"-map",
		"[outv]",
		"-frames:v",
		"1"
	);
	if (useJpegQ) args.push("-q:v", "2");
	args.push("-y", outPath);

	await runFfmpeg(args, "thumbnail_compose");
	return outPath;
}

async function generateThumbnailCompositeBase({
	jobId,
	tmpDir,
	presenterImagePath,
	topicImagePaths = [],
	title,
	topics,
	ratio = THUMBNAIL_RATIO,
	width = DEFAULT_CANVAS_WIDTH,
	height = DEFAULT_CANVAS_HEIGHT,
	accentColor = ACCENT_PALETTE.default,
	log,
}) {
	if (!presenterImagePath)
		throw new Error("thumbnail_presenter_missing_or_invalid");

	let baseImagePath = presenterImagePath;
	let usedRunway = false;
	const prompt = buildRunwayThumbnailPrompt({ title, topics });
	try {
		baseImagePath = await generateRunwayThumbnailBase({
			jobId,
			tmpDir,
			prompt,
			ratio,
			log,
		});
		usedRunway = true;
		if (log)
			log("thumbnail runway background ready", {
				path: path.basename(baseImagePath),
			});
	} catch (e) {
		if (log)
			log("thumbnail runway failed; using fallback background", {
				error: e.message,
			});
	}
	if (!usedRunway) {
		const topicSource =
			Array.isArray(topicImagePaths) && topicImagePaths.length
				? topicImagePaths[0]
				: null;
		const presenterHasAlpha = hasAlphaChannel(presenterImagePath);
		const fallbackSource =
			topicSource || (presenterHasAlpha ? null : presenterImagePath);
		const fallbackPath = path.join(tmpDir, `thumb_bg_${jobId}.jpg`);
		baseImagePath = await generateFallbackBackground({
			sourcePath: fallbackSource,
			outPath: fallbackPath,
			width,
			height,
			log,
		});
		if (log)
			log("thumbnail fallback background ready", {
				path: path.basename(baseImagePath),
			});
	}

	const outPath = path.join(tmpDir, `thumb_composite_${jobId}.png`);
	return await composeThumbnailBase({
		baseImagePath,
		presenterImagePath,
		topicImagePaths,
		outPath,
		width,
		height,
		accentColor,
	});
}

async function resolveThumbnailBackground({
	jobId,
	tmpDir,
	title,
	topics,
	ratio,
	width,
	height,
	presenterImagePath,
	topicImagePaths = [],
	log,
}) {
	let baseImagePath = "";
	const prompt = buildRunwayThumbnailPrompt({ title, topics });
	try {
		baseImagePath = await generateRunwayThumbnailBase({
			jobId,
			tmpDir,
			prompt,
			ratio,
			log,
		});
		if (log)
			log("thumbnail runway background ready", {
				path: path.basename(baseImagePath),
			});
		return { path: baseImagePath, usedRunway: true };
	} catch (e) {
		if (log)
			log("thumbnail runway failed; using fallback background", {
				error: e.message,
			});
	}

	const topicSource =
		Array.isArray(topicImagePaths) && topicImagePaths.length
			? topicImagePaths[0]
			: null;
	const presenterHasAlpha = hasAlphaChannel(presenterImagePath);
	const fallbackSource =
		topicSource || (presenterHasAlpha ? null : presenterImagePath);
	const fallbackPath = path.join(tmpDir, `thumb_bg_${jobId}.jpg`);
	baseImagePath = await generateFallbackBackground({
		sourcePath: fallbackSource,
		outPath: fallbackPath,
		width,
		height,
		log,
	});
	if (log)
		log("thumbnail fallback background ready", {
			path: path.basename(baseImagePath),
		});
	return { path: baseImagePath, usedRunway: false };
}

function escapeDrawtext(s = "") {
	const placeholder = "__NL__";
	return String(s || "")
		.replace(/\r\n|\r|\n/g, placeholder)
		.replace(/\\/g, "\\\\")
		.replace(/:/g, "\\:")
		.replace(/'/g, "\\'")
		.replace(/%/g, "\\%")
		.replace(/,/g, "\\,")
		.replace(/\[/g, "\\[")
		.replace(/\]/g, "\\]")
		.replace(new RegExp(placeholder, "g"), "\\\\n")
		.trim();
}

function resolveThumbnailFontFile() {
	const candidates = [
		"C:/Windows/Fonts/impact.ttf",
		"C:/Windows/Fonts/arialbd.ttf",
		"C:/Windows/Fonts/arialblack.ttf",
		"C:/Windows/Fonts/arial.ttf",
		"/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
		"/Library/Fonts/Arial Bold.ttf",
	];
	for (const p of candidates) {
		try {
			if (p && fs.existsSync(p)) return p;
		} catch {}
	}
	return "";
}

const THUMBNAIL_FONT_FILE = resolveThumbnailFontFile();

function hardTruncateText(text = "", maxChars = 40) {
	const t = String(text || "").trim();
	if (t.length <= maxChars) return t;
	return t.slice(0, Math.max(0, maxChars)).trimEnd();
}

const WRAP_AVOID_END_WORDS = new Set([
	"a",
	"an",
	"and",
	"at",
	"by",
	"for",
	"from",
	"in",
	"of",
	"on",
	"or",
	"the",
	"to",
	"with",
]);

function wrapText(text = "", maxCharsPerLine = 36, maxLines = 2) {
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
		line = line ? `${line} ${word}` : word;
	}

	if (line) lines.push(line);
	if (lines.length > 1) {
		for (let i = 0; i < lines.length - 1; i++) {
			const parts = lines[i].split(" ").filter(Boolean);
			if (parts.length < 2) continue;
			const last = parts[parts.length - 1];
			if (!WRAP_AVOID_END_WORDS.has(last.toLowerCase())) continue;
			const combined = `${last} ${lines[i + 1]}`.trim();
			if (combined.length <= maxCharsPerLine) {
				lines[i] = parts.slice(0, -1).join(" ");
				lines[i + 1] = combined;
			}
		}
	}
	const maxLineLen = lines.reduce((m, l) => Math.max(m, l.length), 0);
	return {
		text: lines.join("\n").trim(),
		lines: lines.length,
		maxLineLen,
		overflow: maxLineLen > maxCharsPerLine,
	};
}

function fitHeadlineText(
	text = "",
	{ baseMaxChars = 36, preferLines = 1, maxLines = 1 } = {}
) {
	const clean = String(text || "").trim();
	if (!clean) return { text: "", fontScale: 1, lines: 0, truncated: false };

	let fontScale = 1.0;
	let maxChars = baseMaxChars;
	let wrap = wrapText(clean, maxChars, preferLines);

	if (wrap.overflow || wrap.lines > preferLines) {
		wrap = wrapText(clean, maxChars, maxLines);
	}

	if (wrap.overflow) {
		const scales = [0.94, 0.9, 0.86];
		for (const scale of scales) {
			fontScale = scale;
			maxChars = Math.round(baseMaxChars / scale);
			wrap = wrapText(clean, maxChars, maxLines);
			if (!wrap.overflow) break;
		}
	}

	let truncated = false;
	if (wrap.overflow) {
		const maxTotal = maxChars * maxLines;
		const cut = hardTruncateText(clean, maxTotal);
		wrap = wrapText(cut, maxChars, maxLines);
		truncated = cut.length < clean.length;
	}

	return { text: wrap.text, fontScale, lines: wrap.lines, truncated };
}

function titleCaseIfLower(text = "") {
	const cleaned = String(text || "").trim();
	if (!cleaned) return "";
	if (/[A-Z]/.test(cleaned)) return cleaned;
	return cleaned.replace(/\b[a-z]/g, (m) => m.toUpperCase());
}

function shouldAppendQuestionMark(text = "") {
	const info = extractQuestionSegments(text);
	return Boolean(info && (info.questionWord || info.hasQuestionMark));
}

function ensureQuestionMark(text = "") {
	if (!text) return text;
	const cleaned = String(text).replace(/\?+$/, "").trim();
	return cleaned ? `${cleaned}?` : text;
}

function buildThumbnailText(
	title = "",
	{
		maxWords = THUMBNAIL_TEXT_MAX_WORDS,
		baseChars = THUMBNAIL_TEXT_BASE_MAX_CHARS,
	} = {}
) {
	const cleaned = cleanThumbnailText(title);
	if (!cleaned) return { text: "", fontScale: 1 };
	const pretty = titleCaseIfLower(cleaned);
	const words = pretty.split(" ").filter(Boolean);
	const trimmedWords = words.slice(0, Math.max(1, maxWords));
	const wantsQuestionMark = shouldAppendQuestionMark(title);
	let trimmed = trimmedWords.join(" ");
	if (wantsQuestionMark) trimmed = ensureQuestionMark(trimmed);
	const fit = fitHeadlineText(trimmed, {
		baseMaxChars: baseChars,
		preferLines: 2,
		maxLines: 2,
	});
	const fitted = fit.text || trimmed;
	const finalText = wantsQuestionMark ? ensureQuestionMark(fitted) : fitted;
	return {
		text: finalText,
		fontScale: fit.fontScale || 1,
	};
}

async function renderThumbnailOverlay({
	inputPath,
	outputPath,
	title,
	accentColor = ACCENT_PALETTE.default,
	overlayOptions = {},
}) {
	const maxWords = Number.isFinite(Number(overlayOptions.maxWords))
		? Number(overlayOptions.maxWords)
		: THUMBNAIL_TEXT_MAX_WORDS;
	const baseChars = Number.isFinite(Number(overlayOptions.baseChars))
		? Number(overlayOptions.baseChars)
		: THUMBNAIL_TEXT_BASE_MAX_CHARS;
	const contrast = Number.isFinite(Number(overlayOptions.contrast))
		? Number(overlayOptions.contrast)
		: 1.05;
	const saturation = Number.isFinite(Number(overlayOptions.saturation))
		? Number(overlayOptions.saturation)
		: 1.1;
	const brightness = Number.isFinite(Number(overlayOptions.brightness))
		? Number(overlayOptions.brightness)
		: 0.06;
	const gamma = Number.isFinite(Number(overlayOptions.gamma))
		? Number(overlayOptions.gamma)
		: 0.96;
	const panelOpacity = Number.isFinite(Number(overlayOptions.panelOpacity))
		? Number(overlayOptions.panelOpacity)
		: 0;
	const textBoxOpacity = Number.isFinite(Number(overlayOptions.textBoxOpacity))
		? Number(overlayOptions.textBoxOpacity)
		: 0.28;
	const vignetteStrength = Number.isFinite(Number(overlayOptions.vignette))
		? Number(overlayOptions.vignette)
		: 0.04;
	const leftPanelPct = Number.isFinite(Number(overlayOptions.leftPanelPct))
		? clampNumber(Number(overlayOptions.leftPanelPct), 0.3, 0.65)
		: LEFT_PANEL_PCT;
	const leftLift = Number.isFinite(Number(overlayOptions.leftLift))
		? clampNumber(Number(overlayOptions.leftLift), 0, 0.18)
		: 0.06;
	const leftLiftHeight = Number.isFinite(Number(overlayOptions.leftLiftHeight))
		? clampNumber(Number(overlayOptions.leftLiftHeight), 0.2, 1)
		: 0.6;
	const leftFeatherPct = Number.isFinite(Number(overlayOptions.leftFeatherPct))
		? clampNumber(Number(overlayOptions.leftFeatherPct), 0, 0.12)
		: Math.min(0.05, leftPanelPct * 0.25);
	const badgeTextRaw =
		typeof overlayOptions.badgeText === "string"
			? overlayOptions.badgeText.trim()
			: "";
	const badgeText = badgeTextRaw
		? hardTruncateText(badgeTextRaw.toUpperCase(), THUMBNAIL_BADGE_MAX_CHARS)
		: "";
	const { text, fontScale } = buildThumbnailText(title, {
		maxWords,
		baseChars,
	});
	const hasText = Boolean(text);
	const hasBadge = Boolean(badgeText);
	const fontFile = THUMBNAIL_FONT_FILE
		? `:fontfile='${escapeDrawtext(THUMBNAIL_FONT_FILE)}'`
		: "";
	const fontSize = Math.max(
		42,
		Math.round(THUMBNAIL_HEIGHT * THUMBNAIL_TEXT_SIZE_PCT * fontScale)
	);
	const boxBorder = Math.round(fontSize * 0.45);
	const lineSpacing = Math.round(fontSize * THUMBNAIL_TEXT_LINE_SPACING_PCT);
	const textFilePath = hasText
		? path.join(
				path.dirname(outputPath),
				`thumb_text_${path.basename(outputPath, path.extname(outputPath))}.txt`
		  )
		: "";
	const badgeFilePath = hasBadge
		? path.join(
				path.dirname(outputPath),
				`thumb_badge_${path.basename(outputPath, path.extname(outputPath))}.txt`
		  )
		: "";
	if (hasText) fs.writeFileSync(textFilePath, text, "utf8");
	if (hasBadge) fs.writeFileSync(badgeFilePath, badgeText, "utf8");

	const filters = [
		`scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}`,
		`eq=contrast=${contrast.toFixed(2)}:saturation=${saturation.toFixed(
			2
		)}:brightness=${brightness.toFixed(3)}:gamma=${gamma.toFixed(3)}`,
		"unsharp=5:5:0.7",
	];
	if (panelOpacity > 0) {
		filters.push(
			`drawbox=x=0:y=0:w=iw*${leftPanelPct.toFixed(
				3
			)}:h=ih:color=black@${panelOpacity.toFixed(3)}:t=fill`
		);
		const panelFeatherOpacity = Math.min(panelOpacity, panelOpacity * 0.35);
		if (leftFeatherPct > 0 && panelFeatherOpacity > 0) {
			filters.push(
				`drawbox=x=iw*${leftPanelPct.toFixed(
					4
				)}:y=0:w=iw*${leftFeatherPct.toFixed(
					4
				)}:h=ih:color=black@${panelFeatherOpacity.toFixed(3)}:t=fill`
			);
		}
	}
	if (leftLift > 0) {
		const seg = leftPanelPct / 3;
		const segStart2 = seg * 2;
		const liftHeight = leftLiftHeight.toFixed(2);
		filters.push(
			`drawbox=x=0:y=0:w=iw*${seg.toFixed(
				4
			)}:h=ih*${liftHeight}:color=white@${leftLift.toFixed(3)}:t=fill`,
			`drawbox=x=iw*${seg.toFixed(4)}:y=0:w=iw*${seg.toFixed(
				4
			)}:h=ih*${liftHeight}:color=white@${(leftLift * 0.65).toFixed(3)}:t=fill`,
			`drawbox=x=iw*${segStart2.toFixed(4)}:y=0:w=iw*${seg.toFixed(
				4
			)}:h=ih*${liftHeight}:color=white@${(leftLift * 0.35).toFixed(3)}:t=fill`
		);
		const featherAlpha = leftLift * 0.12;
		if (leftFeatherPct > 0 && featherAlpha > 0) {
			filters.push(
				`drawbox=x=iw*${leftPanelPct.toFixed(
					4
				)}:y=0:w=iw*${leftFeatherPct.toFixed(
					4
				)}:h=ih*${liftHeight}:color=white@${featherAlpha.toFixed(3)}:t=fill`
			);
		}
	}
	filters.push(
		`drawbox=x=0:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.012:t=fill`,
		`drawbox=x=iw*0.2:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.01:t=fill`,
		`drawbox=x=iw*0.4:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.007:t=fill`,
		`drawbox=x=iw*0.6:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.004:t=fill`,
		`drawbox=x=iw*0.8:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.002:t=fill`,
		`drawbox=x=0:y=0:w=iw*0.018:h=ih:color=${accentColor}@0.25:t=fill`,
		`drawbox=x=0:y=0:w=iw:h=ih:color=${accentColor}@0.08:t=4`,
		`vignette=${vignetteStrength.toFixed(2)}`
	);
	if (hasBadge) {
		const badgeFontSize = Math.max(
			18,
			Math.round(THUMBNAIL_HEIGHT * THUMBNAIL_BADGE_FONT_PCT)
		);
		const badgeBorder = Math.round(badgeFontSize * 0.55);
		filters.push(
			`drawtext=textfile='${escapeDrawtext(
				badgeFilePath
			)}'${fontFile}:fontsize=${badgeFontSize}:fontcolor=white:box=1:boxcolor=${accentColor}@0.85:boxborderw=${badgeBorder}:shadowcolor=black@0.35:shadowx=1:shadowy=1:x=w*${THUMBNAIL_BADGE_X_PCT}:y=h*${THUMBNAIL_BADGE_Y_PCT}`
		);
	}
	if (hasText) {
		filters.push(
			`drawtext=textfile='${escapeDrawtext(
				textFilePath
			)}'${fontFile}:fontsize=${fontSize}:fontcolor=white:borderw=3:bordercolor=black@0.6:box=1:boxcolor=black@${textBoxOpacity.toFixed(
				2
			)}:boxborderw=${boxBorder}:shadowcolor=black@0.45:shadowx=2:shadowy=2:line_spacing=${lineSpacing}:x=w*${THUMBNAIL_TEXT_MARGIN_PCT}:y=h*${THUMBNAIL_TEXT_Y_OFFSET_PCT}`
		);
	}

	try {
		await runFfmpeg(
			[
				"-i",
				inputPath,
				"-vf",
				filters.join(","),
				"-frames:v",
				"1",
				"-q:v",
				"2",
				"-y",
				outputPath,
			],
			"thumbnail_render"
		);
	} finally {
		if (textFilePath) safeUnlink(textFilePath);
		if (badgeFilePath) safeUnlink(badgeFilePath);
	}

	return outputPath;
}

function ensureThumbnailFile(filePath, minBytes = THUMBNAIL_MIN_BYTES) {
	if (!filePath || !fs.existsSync(filePath))
		throw new Error("thumbnail_missing");
	const st = fs.statSync(filePath);
	if (!st || st.size < minBytes) throw new Error("thumbnail_too_small");
	const dt = detectFileType(filePath);
	if (!dt || dt.kind !== "image") throw new Error("thumbnail_invalid");
	return filePath;
}

async function fetchCseContext(topic, extraTokens = []) {
	if (!topic || !GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) return [];
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const baseTokens = [...topicTokensFromTitle(topic), ...extra];
	const queries = [
		`${topic} news`,
		`${topic} interview`,
		`${topic} official`,
		`${topic} cast`,
		`${topic} trailer`,
	];

	const items = await fetchCseItems(queries, { num: 3 });
	const matchTokens = filterSpecificTopicTokens(baseTokens);
	const minMatches = Math.max(1, Math.min(2, matchTokens.length));
	return items
		.filter(
			(it) =>
				topicMatchInfo(matchTokens, [it.title, it.snippet, it.link]).count >=
				minMatches
		)
		.slice(0, 6);
}

async function fetchCseImages(topic, extraTokens = []) {
	if (!topic || !GOOGLE_CSE_ID || !GOOGLE_CSE_KEY) return [];
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const baseTokens = [...topicTokensFromTitle(topic), ...extra];
	const category = inferEntertainmentCategory(baseTokens);
	const topicTokensBase = filterSpecificTopicTokens(
		topicTokensFromTitle(topic)
	);
	const topicTokens = filterImageSearchTokens(topicTokensBase);
	const searchTokens = buildSearchLabelTokens(topic, extraTokens);
	const searchLabel = searchTokens.slice(0, 4).join(" ") || topic;

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
	const keyPhrase = searchTokens.slice(0, 2).join(" ");
	if (keyPhrase) {
		fallbackQueries.push(`${keyPhrase} photo`, `${keyPhrase} press`);
	}

	let items = await fetchCseItems(queries, {
		num: 10,
		searchType: "image",
		imgSize: CSE_PREFERRED_IMG_SIZE,
		imgColorType: CSE_PREFERRED_IMG_COLOR,
		maxPages: CSE_MAX_PAGES,
	});
	if (!items.length) {
		items = await fetchCseItems(queries, {
			num: 10,
			searchType: "image",
			imgSize: CSE_FALLBACK_IMG_SIZE,
			imgColorType: CSE_PREFERRED_IMG_COLOR,
			maxPages: CSE_MAX_PAGES,
		});
	}
	if (!items.length) {
		items = await fetchCseItems(fallbackQueries, {
			num: 10,
			searchType: "image",
			imgSize: CSE_PREFERRED_IMG_SIZE,
			imgColorType: CSE_PREFERRED_IMG_COLOR,
			maxPages: CSE_MAX_PAGES,
		});
	}
	if (!items.length) {
		items = await fetchCseItems(fallbackQueries, {
			num: 10,
			searchType: "image",
			imgSize: CSE_FALLBACK_IMG_SIZE,
			imgColorType: CSE_PREFERRED_IMG_COLOR,
			maxPages: CSE_MAX_PAGES,
		});
	}

	const matchSourceTokens = topicTokens.length ? topicTokens : topicTokensBase;
	const matchTokens = expandTopicTokens(matchSourceTokens);
	const minMatches = minImageTokenMatches(matchTokens);

	const candidates = [];
	const pushCandidate = (it, { minEdge, minTokenMatches }) => {
		const url = it.link || "";
		if (!url || !/^https:\/\//i.test(url)) return;
		if (
			isMerchDisallowedCandidate({
				url,
				source: it.image?.contextLink || it.displayLink || "",
				title: it.title || "",
			})
		)
			return;
		const info = topicMatchInfo(matchTokens, [
			it.title,
			it.snippet,
			it.link,
			it.image?.contextLink || "",
		]);
		if (info.count < minTokenMatches) return;
		const w = Number(it.image?.width || 0);
		const h = Number(it.image?.height || 0);
		if (w && h && Math.min(w, h) < minEdge) return;
		const urlText = `${it.link || ""} ${
			it.image?.contextLink || ""
		}`.toLowerCase();
		const urlMatches = matchTokens.filter((tok) =>
			urlText.includes(tok)
		).length;
		const score = info.count + urlMatches * 0.75;
		const mime = String(it.image?.mime || "").toLowerCase();
		candidates.push({
			url,
			score,
			urlMatches,
			w,
			h,
			mime,
			source: it.image?.contextLink || it.displayLink || "",
			title: it.title || "",
		});
	};

	for (const it of items) {
		pushCandidate(it, {
			minEdge: CSE_MIN_IMAGE_SHORT_EDGE,
			minTokenMatches: minMatches,
		});
		if (candidates.length >= 14) break;
	}
	if (!candidates.length && items.length) {
		const relaxedMinEdge = Math.min(CSE_MIN_IMAGE_SHORT_EDGE, 600);
		const relaxedMatches = Math.max(1, minMatches - 1);
		for (const it of items) {
			pushCandidate(it, {
				minEdge: relaxedMinEdge,
				minTokenMatches: relaxedMatches,
			});
			if (candidates.length >= 12) break;
		}
	}

	candidates.sort((a, b) => {
		if (b.score !== a.score) return b.score - a.score;
		if (b.w !== a.w) return b.w - a.w;
		return b.h - a.h;
	});

	if (
		GOOGLE_IMAGES_SEARCH_ENABLED &&
		candidates.length < GOOGLE_IMAGES_MIN_POOL_MULTIPLIER * 2
	) {
		const googleQueries = [];
		const seenGoogle = new Set();
		const pushQuery = (raw) => {
			const cleaned = sanitizeImageQuery(raw);
			if (!cleaned) return;
			const key = cleaned.toLowerCase();
			if (seenGoogle.has(key)) return;
			seenGoogle.add(key);
			googleQueries.push(cleaned);
		};
		pushQuery(searchLabel);
		pushQuery(topic);
		queries.forEach((q) => pushQuery(q));
		fallbackQueries.forEach((q) => pushQuery(q));
		const limited = googleQueries.slice(0, GOOGLE_IMAGES_VARIANT_LIMIT);
		for (const gQuery of limited) {
			const googleUrls = await fetchGoogleImagesFromService(gQuery, {
				limit: GOOGLE_IMAGES_RESULTS_PER_QUERY,
				tokens: matchTokens,
			});
			for (const url of googleUrls) {
				if (!url) continue;
				const info = topicMatchInfo(matchTokens, [url]);
				if (info.count < Math.max(1, minMatches - 1)) continue;
				candidates.push({
					url,
					score: info.count,
					urlMatches: info.count,
					w: 0,
					h: 0,
					mime: "",
					source: "google-images",
					title: "",
				});
				if (candidates.length >= 18) break;
			}
			if (candidates.length >= 18) break;
		}
	}

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
		if (!isProbablyDirectImageUrl(c.url) && !c.mime) continue;
		filtered.push(c);
		if (filtered.length >= 6) break;
	}
	return filtered;
}

async function collectThumbnailTopicImages({
	topics = [],
	tmpDir,
	jobId,
	title = "",
	shortTitle = "",
	seoTitle = "",
	maxImages = THUMBNAIL_TOPIC_MAX_IMAGES,
	requireTopicImages = REQUIRE_THUMBNAIL_TOPIC_IMAGES,
	log,
}) {
	const target = Math.max(0, Math.floor(maxImages));
	if (!target) return [];
	const hasCSE = !!(GOOGLE_CSE_ID && GOOGLE_CSE_KEY);
	if (!hasCSE && log)
		log("thumbnail topic images: CSE missing, using wiki/commons only");
	const topicList = Array.isArray(topics) ? topics : [];
	if (!topicList.length) return [];

	const combinedContext = `${title || ""} ${shortTitle || ""} ${
		seoTitle || ""
	}`.trim();
	const contextQuery = sanitizeOverlayQuery(
		sanitizeThumbnailContext(combinedContext)
	);
	const contextTokens =
		contextQuery && contextQuery.length >= 4 ? [contextQuery] : [];

	const urlCandidates = [];
	const seen = new Set();
	const maxUrls = Math.max(target * 4, THUMBNAIL_TOPIC_MAX_DOWNLOADS);
	const pushCandidate = (
		url,
		{ source = "", title = "", priority = 0, criteria } = {}
	) => {
		if (!url) return;
		const key = normalizeImageUrlKey(url);
		if (seen.has(key)) return;
		if (isLikelyThumbnailUrl(url)) return;
		if (isLikelyWatermarkedSource(url, source)) return;
		if (isMerchDisallowedCandidate({ url, source, title })) return;
		const merchPenalty = merchPenaltyScore({ url, source, title });
		const match = scoreThumbnailTopicMatch(url, source, criteria);
		const minWordMatches = Number(criteria?.minWordMatches || 0);
		const minSubjectMatches = Number(criteria?.minSubjectMatches || 0);
		const relaxed =
			(minWordMatches && match.wordMatches < minWordMatches) ||
			(minSubjectMatches && match.subjectMatches < minSubjectMatches);
		urlCandidates.push({
			url,
			source,
			matchScore: match.score,
			wordMatches: match.wordMatches,
			contextMatches: match.contextMatches,
			subjectMatches: match.subjectMatches,
			phraseHit: match.phraseHit,
			sourceScore: scoreSourceAffinity(url, source),
			merchPenalty,
			priority,
			relaxed: relaxed || merchPenalty >= 0.6,
		});
		seen.add(key);
	};

	for (const t of topicList) {
		if (urlCandidates.length >= maxUrls) break;
		const label = t?.displayTopic || t?.topic || "";
		if (!label) continue;
		const extraTokens = Array.isArray(t?.keywords) ? t.keywords : [];
		const trendHints = uniqueStrings(
			[
				...(Array.isArray(t?.trendStory?.searchPhrases)
					? t.trendStory.searchPhrases
					: []),
				...(Array.isArray(t?.trendStory?.entityNames)
					? t.trendStory.entityNames
					: []),
				...(Array.isArray(t?.trendStory?.articles)
					? t.trendStory.articles.map((a) => a?.title)
					: []),
				t?.trendStory?.imageComment,
				...(Array.isArray(t?.trendStory?.viralImageBriefs)
					? t.trendStory.viralImageBriefs.map((b) =>
							String(
								b?.visualHook || b?.idea || b?.hook || b?.description || ""
							).trim()
					  )
					: []),
			].filter(Boolean),
			{ limit: 12 }
		);
		const questionInfo = extractQuestionSegments(label);
		const questionSubjectLabel = buildQuestionSubjectLabel(questionInfo, 4);
		const questionSubjectContextLabel = buildQuestionSubjectContextLabel(
			questionInfo,
			5
		);
		const mergedTokens = uniqueStrings(
			[
				...(contextTokens.length ? contextTokens : []),
				...extraTokens,
				...trendHints,
				questionSubjectLabel,
				questionSubjectContextLabel,
			],
			{ limit: 18 }
		);
		const criteria = buildImageMatchCriteria(label, mergedTokens);
		const identityLabel = buildTopicIdentityLabel(label, mergedTokens);
		const seedUrls = uniqueStrings(
			[
				t?.trendStory?.image,
				...(Array.isArray(t?.trendStory?.images) ? t.trendStory.images : []),
				...(Array.isArray(t?.trendStory?.articles)
					? t.trendStory.articles.map((a) => a?.image)
					: []),
			].filter(Boolean),
			{ limit: 8 }
		);
		for (const url of seedUrls) {
			pushCandidate(url, {
				source: t?.trendStory?.articles?.[0]?.url || "",
				title: t?.trendStory?.articles?.[0]?.title || "",
				priority: 0.7,
				criteria,
			});
		}

		const searchLabel =
			questionSubjectContextLabel || questionSubjectLabel || label;
		let hits = hasCSE ? await fetchCseImages(searchLabel, mergedTokens) : [];
		if (hits.length) {
			for (const hit of hits) {
				const url = typeof hit === "string" ? hit : hit?.url;
				if (!url) continue;
				pushCandidate(url, {
					source: hit?.source || "",
					title: hit?.title || "",
					priority: 0.55,
					criteria,
				});
			}
		}

		if (!hits.length && hasCSE) {
			const ctxItems = await fetchCseContext(searchLabel, mergedTokens);
			const articleUrls = uniqueStrings(
				[
					...(Array.isArray(t?.trendStory?.articles)
						? t.trendStory.articles.map((a) => a?.url)
						: []),
					...ctxItems.map((c) => c?.link),
				],
				{ limit: 6 }
			);
			const ogHits = [];
			for (const pageUrl of articleUrls) {
				if (ogHits.length >= 3) break;
				if (!pageUrl || ogHits.includes(pageUrl)) continue;
				const og = await fetchOpenGraphImageUrl(pageUrl);
				if (!og) continue;
				if (isLikelyWatermarkedSource(og, pageUrl)) continue;
				const ct = await headContentType(og, 7000);
				if (ct && !ct.startsWith("image/")) continue;
				ogHits.push(og);
			}
			if (ogHits.length && log)
				log("thumbnail topic images fallback og", {
					topic: label,
					count: ogHits.length,
				});
			for (const og of ogHits) {
				pushCandidate(og, {
					source: label,
					title: label,
					priority: 0.5,
					criteria,
				});
			}
		}

		if (
			GOOGLE_IMAGES_SEARCH_ENABLED &&
			urlCandidates.length < maxUrls &&
			label
		) {
			const googleQueries = uniqueStrings(
				[
					questionSubjectContextLabel,
					questionSubjectLabel,
					label,
					identityLabel,
					contextQuery,
				].filter(Boolean),
				{ limit: GOOGLE_IMAGES_VARIANT_LIMIT }
			);
			for (const gQuery of googleQueries) {
				const googleUrls = await fetchGoogleImagesFromService(gQuery, {
					limit: GOOGLE_IMAGES_RESULTS_PER_QUERY,
					tokens: criteria.wordTokens,
				});
				for (const url of googleUrls) {
					pushCandidate(url, {
						source: "google-images",
						title: gQuery,
						priority: 0.25,
						criteria,
					});
					if (urlCandidates.length >= maxUrls) break;
				}
				if (urlCandidates.length >= maxUrls) break;
			}
		}

		if (urlCandidates.length < maxUrls) {
			const wikiLabel = identityLabel || label;
			const wikiUrl = await fetchWikipediaPageImageUrl(wikiLabel);
			if (wikiUrl) {
				if (log)
					log("thumbnail topic images fallback wiki", {
						topic: wikiLabel,
					});
				pushCandidate(wikiUrl, {
					source: wikiLabel,
					title: wikiLabel,
					priority: 0.4,
					criteria,
				});
			}
		}
		if (urlCandidates.length < maxUrls) {
			const commonsLabel = identityLabel || label;
			const commons = await fetchWikimediaImageUrls(commonsLabel, 3);
			if (commons.length && log)
				log("thumbnail topic images fallback commons", {
					topic: commonsLabel,
					count: commons.length,
				});
			for (const url of commons) {
				pushCandidate(url, {
					source: label,
					title: commonsLabel,
					priority: 0.35,
					criteria,
				});
				if (urlCandidates.length >= maxUrls) break;
			}
		}
	}

	if (!urlCandidates.length) {
		if (requireTopicImages) throw new Error("thumbnail_topic_images_missing");
		if (log)
			log("thumbnail topic images none", {
				reason: "no_urls",
				target,
			});
		return [];
	}

	const candidates = [];
	const smallCandidates = [];
	const strictCandidates = urlCandidates.filter((c) => !c.relaxed);
	const candidatePool =
		strictCandidates.length >= target ? strictCandidates : urlCandidates;
	const signalCandidates = candidatePool.filter(
		(c) => (c.wordMatches || 0) + (c.contextMatches || 0) > 0 || c.phraseHit
	);
	const filteredPool =
		signalCandidates.length >= target ? signalCandidates : candidatePool;
	const ranked = filteredPool
		.map((c) => ({
			...c,
			relevanceScore:
				(c.matchScore || 0) +
				(c.sourceScore || 0) +
				(c.priority || 0) -
				(c.merchPenalty || 0) -
				(c.relaxed ? 0.6 : 0),
		}))
		.sort((a, b) => b.relevanceScore - a.relevanceScore);
	const downloadCount = Math.min(ranked.length, THUMBNAIL_TOPIC_MAX_DOWNLOADS);
	for (let i = 0; i < downloadCount; i++) {
		const candidate = ranked[i];
		const url = candidate.url;
		const extGuess = path
			.extname(String(url).split("?")[0] || "")
			.toLowerCase();
		const ext = extGuess && extGuess.length <= 5 ? extGuess : ".jpg";
		const out = path.join(tmpDir, `thumb_topic_${jobId}_${i}${ext}`);
		try {
			await downloadToFile(url, out, 25000, 1);
			const detected = detectFileType(out);
			if (!detected || detected.kind !== "image") {
				safeUnlink(out);
				continue;
			}
			const st = fs.statSync(out);
			if (!st?.size || st.size < 4096) {
				safeUnlink(out);
				continue;
			}
			const dims = ffprobeDimensions(out);
			const tone = analyzeImageTone(out);
			const minEdge = Math.min(dims.width || 0, dims.height || 0);
			if (minEdge && minEdge < CSE_MIN_IMAGE_SHORT_EDGE) {
				smallCandidates.push({
					path: out,
					size: st.size,
					width: dims.width || 0,
					height: dims.height || 0,
					tone,
					score: scoreTopicImageCandidate({
						width: dims.width || 0,
						height: dims.height || 0,
						tone,
					}),
				});
				continue;
			}
			candidates.push({
				path: out,
				size: st.size,
				width: dims.width || 0,
				height: dims.height || 0,
				tone,
				matchScore: candidate.matchScore,
				sourceScore: candidate.sourceScore,
				url,
				source: candidate.source,
				score: scoreTopicImageCandidate({
					width: dims.width || 0,
					height: dims.height || 0,
					tone,
					matchScore: candidate.matchScore,
					sourceScore: candidate.sourceScore,
				}),
			});
		} catch {
			safeUnlink(out);
		}
	}

	let usableCandidates = candidates;
	if (!usableCandidates.length && smallCandidates.length) {
		usableCandidates = smallCandidates;
		if (log)
			log("thumbnail topic images fallback small", {
				count: usableCandidates.length,
			});
	}

	if (!usableCandidates.length) {
		if (requireTopicImages) throw new Error("thumbnail_topic_images_missing");
		if (log)
			log("thumbnail topic images none", {
				reason: "no_candidates",
				target,
			});
		return [];
	}

	const preferred = usableCandidates.filter((c) => {
		const minEdge = Math.min(c.width || 0, c.height || 0);
		if (!minEdge) return false;
		if (minEdge && minEdge < THUMBNAIL_TOPIC_MIN_EDGE) return false;
		return c.size >= THUMBNAIL_TOPIC_MIN_BYTES;
	});

	const pickPool = preferred.length ? preferred : usableCandidates;
	pickPool.sort((a, b) => {
		if (Number.isFinite(a.score) && Number.isFinite(b.score)) {
			if (b.score !== a.score) return b.score - a.score;
		}
		if (b.size !== a.size) return b.size - a.size;
		const aPixels = (a.width || 0) * (a.height || 0);
		const bPixels = (b.width || 0) * (b.height || 0);
		return bPixels - aPixels;
	});
	const selectedCandidates = pickPool.slice(0, target);
	const selected = [];
	for (let i = 0; i < selectedCandidates.length; i++) {
		const candidate = selectedCandidates[i];
		const normalized = await normalizeTopicImageIfNeeded({
			inputPath: candidate.path,
			tone: candidate.tone,
			tmpDir,
			jobId,
			index: i,
			log,
		});
		selected.push(normalized);
	}

	for (const c of [...candidates, ...smallCandidates]) {
		if (!selected.includes(c.path)) safeUnlink(c.path);
	}

	if (log)
		log("thumbnail topic images selected", {
			count: selected.length,
			target,
		});

	return selected;
}

function extractHookWord(texts = []) {
	for (const raw of texts) {
		const words = cleanThumbnailText(raw || "")
			.toLowerCase()
			.split(" ")
			.filter(Boolean);
		for (const hook of THUMBNAIL_HOOK_WORDS) {
			if (words.includes(hook)) return hook.toUpperCase();
		}
	}
	return "";
}

function selectHeadlineWords(text = "", maxWords = 4) {
	const words = cleanThumbnailText(text).split(" ").filter(Boolean);
	if (words.length <= maxWords) return words;
	const filtered = words.filter(
		(w) =>
			!TOPIC_STOP_WORDS.has(w.toLowerCase()) &&
			!GENERIC_TOPIC_TOKENS.has(w.toLowerCase())
	);
	if (filtered.length) return filtered.slice(0, maxWords);
	return words.slice(0, maxWords);
}

function buildQuestionHeadline(questionInfo, maxWords = 4) {
	if (!questionInfo) return "";
	const questionWord = questionInfo.questionWord;
	let subjectTokens = questionInfo.subjectTokens.filter(
		(t) => !TOPIC_STOP_WORDS.has(t)
	);
	if (!subjectTokens.length) subjectTokens = questionInfo.subjectTokens;
	const maxSubject = Math.max(1, maxWords - (questionWord ? 1 : 0));
	subjectTokens = subjectTokens.slice(0, maxSubject);
	const headTokens = questionWord
		? [questionWord, ...subjectTokens]
		: subjectTokens;
	let headline = headTokens.join(" ").trim();
	headline = titleCaseIfLower(headline);
	if (!headline) return "";
	if (questionInfo.hasQuestionMark || questionWord) {
		headline = headline.replace(/\?+$/, "");
		headline = `${headline}?`;
	}
	return headline;
}

function buildQuestionBadge(questionInfo) {
	if (!questionInfo?.contextTokens?.length) return "";
	const hookSet = new Set(THUMBNAIL_HOOK_WORDS);
	const filtered = questionInfo.contextTokens
		.map((t) => String(t || "").toLowerCase())
		.filter(Boolean)
		.filter(
			(t) =>
				!TOPIC_STOP_WORDS.has(t) &&
				!GENERIC_TOPIC_TOKENS.has(t) &&
				!IMAGE_DEEMPHASIS_TOKENS.has(t) &&
				!hookSet.has(t)
		);
	if (!filtered.length) return "";
	const phrase = filtered.slice(0, 3).join(" ");
	return titleCaseIfLower(phrase);
}

function deriveQuestionHeadlinePlan({
	title,
	shortTitle,
	seoTitle,
	topics,
	maxWords = 4,
	punchyMaxWords = 3,
} = {}) {
	const sources = [
		topics?.[0]?.displayTopic,
		topics?.[0]?.topic,
		title,
		shortTitle,
		seoTitle,
	]
		.map((s) => String(s || "").trim())
		.filter(Boolean);
	for (const src of sources) {
		const info = extractQuestionSegments(src);
		if (!info) continue;
		const headline = buildQuestionHeadline(info, maxWords);
		if (!headline) continue;
		const punchy = buildQuestionHeadline(info, punchyMaxWords) || headline;
		const badgeText = buildQuestionBadge(info);
		return { headline, punchy, badgeText };
	}
	return null;
}

function buildTopicPhrase(topics = [], maxWords = 3) {
	const list = Array.isArray(topics) ? topics : [];
	const primary = list[0]?.displayTopic || list[0]?.topic || "";
	const secondary = list[1]?.displayTopic || list[1]?.topic || "";
	const primaryWords = selectHeadlineWords(primary, maxWords);
	if (!primaryWords.length) return "";
	if (secondary) {
		const secondaryWords = selectHeadlineWords(secondary, maxWords);
		const combinedCount =
			primaryWords.length +
			secondaryWords.length +
			(secondaryWords.length ? 1 : 0);
		if (
			primaryWords.length === 1 &&
			secondaryWords.length === 1 &&
			combinedCount <= maxWords
		) {
			return `${primaryWords[0]} & ${secondaryWords[0]}`;
		}
	}
	return primaryWords.join(" ");
}

function buildSeoHeadline({
	title,
	shortTitle,
	seoTitle,
	topics,
	maxWords = 4,
} = {}) {
	const topicPhrase = buildTopicPhrase(topics, Math.max(2, maxWords));
	const hookWord = extractHookWord([shortTitle, seoTitle, title, topicPhrase]);
	const hookToken = hookWord ? hookWord.toLowerCase() : "";
	if (topicPhrase) {
		const topicWords = topicPhrase.split(" ").filter(Boolean);
		if (hookWord && topicWords.length < maxWords) {
			return `${topicPhrase} ${hookWord}`;
		}
		return topicPhrase;
	}
	const candidates = [shortTitle, seoTitle, title];
	for (const candidate of candidates) {
		const words = selectHeadlineWords(candidate || "", maxWords);
		if (!words.length) continue;
		const normalizedWords = words.map((w) => w.toLowerCase());
		if (
			hookWord &&
			words.length < maxWords &&
			!normalizedWords.includes(hookToken)
		) {
			return `${words.join(" ")} ${hookWord}`;
		}
		return words.join(" ");
	}
	return "Quick Update";
}

function selectThumbnailTitle({ title, shortTitle, seoTitle, topics }) {
	return buildSeoHeadline({
		title,
		shortTitle,
		seoTitle,
		topics,
		maxWords: THUMBNAIL_TEXT_MAX_WORDS,
	});
}

function buildShortHookTitle({ title, shortTitle, seoTitle, topics }) {
	return buildSeoHeadline({
		title,
		shortTitle,
		seoTitle,
		topics,
		maxWords: THUMBNAIL_VARIANT_B_TEXT_MAX_WORDS,
	});
}

function assertCloudinaryReady() {
	if (
		!process.env.CLOUDINARY_CLOUD_NAME ||
		!process.env.CLOUDINARY_API_KEY ||
		!process.env.CLOUDINARY_API_SECRET
	) {
		throw new Error(
			"Cloudinary credentials missing (CLOUDINARY_CLOUD_NAME/API_KEY/API_SECRET)."
		);
	}
	cloudinary.config({
		cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
		api_key: process.env.CLOUDINARY_API_KEY,
		api_secret: process.env.CLOUDINARY_API_SECRET,
	});
}

async function uploadThumbnailToCloudinary(filePath, jobId) {
	assertCloudinaryReady();
	ensureThumbnailFile(filePath);
	const publicId = `${THUMBNAIL_CLOUDINARY_PUBLIC_PREFIX}_${jobId}_${Date.now()}`;
	const result = await cloudinary.uploader.upload(filePath, {
		public_id: publicId,
		folder: THUMBNAIL_CLOUDINARY_FOLDER,
		resource_type: "image",
		overwrite: true,
	});
	return {
		public_id: result.public_id,
		url: result.secure_url,
		width: result.width,
		height: result.height,
	};
}

async function generateThumbnailPackage({
	jobId,
	tmpDir,
	presenterLocalPath,
	title,
	shortTitle,
	seoTitle,
	topics = [],
	expression,
	log,
	requireTopicImages = REQUIRE_THUMBNAIL_TOPIC_IMAGES,
}) {
	if (!presenterLocalPath)
		throw new Error("thumbnail_presenter_missing_or_invalid");
	ensureDir(tmpDir);
	const presenterDetected = detectFileType(presenterLocalPath);
	if (presenterDetected?.kind !== "image")
		throw new Error("thumbnail_presenter_missing_or_invalid");
	const stylePlan = planThumbnailStyle({
		title,
		shortTitle,
		seoTitle,
		topics,
		expression,
	});
	const resolvedCutout = resolvePresenterCutoutPath(stylePlan.pose);
	const presenterForCompose = resolvedCutout || presenterLocalPath;
	if (log)
		log(
			resolvedCutout
				? "presenter cutout selected"
				: "presenter fixed (no emotion variants)",
			{
				path: path.basename(presenterForCompose),
				pose: stylePlan.pose,
			}
		);
	const chosenDetected = detectFileType(presenterForCompose);
	if (!chosenDetected || chosenDetected.kind !== "image")
		throw new Error("thumbnail_presenter_missing_or_invalid");
	if (chosenDetected.ext !== "png" && log) {
		log("thumbnail presenter not png", {
			ext: chosenDetected.ext || null,
		});
	}

	const questionPlan = deriveQuestionHeadlinePlan({
		title,
		shortTitle,
		seoTitle,
		topics,
		maxWords: THUMBNAIL_TEXT_MAX_WORDS,
		punchyMaxWords: THUMBNAIL_VARIANT_B_TEXT_MAX_WORDS,
	});
	const thumbTitle =
		questionPlan?.headline ||
		selectThumbnailTitle({
			title,
			shortTitle,
			seoTitle,
			topics,
		});
	const punchyTitle =
		questionPlan?.punchy ||
		buildShortHookTitle({
			title,
			shortTitle,
			seoTitle,
			topics,
		});
	const topicCount = Array.isArray(topics) ? topics.length : 0;
	const badgeText =
		topicCount > 1
			? `${Math.min(topicCount, 9)} STORIES`
			: questionPlan?.badgeText || "";

	const topicImagePaths = await collectThumbnailTopicImages({
		topics,
		tmpDir,
		jobId,
		title: title || seoTitle || "",
		shortTitle,
		seoTitle,
		maxImages: THUMBNAIL_TOPIC_MAX_IMAGES,
		requireTopicImages,
		log,
	});
	const bg = await resolveThumbnailBackground({
		jobId,
		tmpDir,
		title: thumbTitle,
		topics,
		ratio: THUMBNAIL_RATIO,
		width: THUMBNAIL_WIDTH,
		height: THUMBNAIL_HEIGHT,
		presenterImagePath: presenterForCompose,
		topicImagePaths,
		log,
	});

	const variantBOverlayOptions = {
		maxWords: THUMBNAIL_VARIANT_B_TEXT_MAX_WORDS,
		baseChars: THUMBNAIL_VARIANT_B_BASE_CHARS,
		contrast: THUMBNAIL_VARIANT_B_CONTRAST,
		panelOpacity: THUMBNAIL_VARIANT_B_PANEL_PCT,
		textBoxOpacity: THUMBNAIL_VARIANT_B_TEXT_BOX_PCT,
		badgeText,
	};
	const variantPlans = [
		{
			key: "a",
			title: thumbTitle,
			layout: {},
			overlayOptions: { badgeText },
		},
		{
			key: "b",
			title: punchyTitle,
			layout: {
				leftPanelPct: THUMBNAIL_VARIANT_B_LEFT_PCT,
				presenterOverlapPct: THUMBNAIL_VARIANT_B_OVERLAP_PCT,
			},
			overlayOptions: variantBOverlayOptions,
		},
		{
			key: "c",
			title: punchyTitle,
			layout: {
				leftPanelPct: THUMBNAIL_VARIANT_B_LEFT_PCT,
				presenterOverlapPct: THUMBNAIL_VARIANT_B_OVERLAP_PCT,
			},
			overlayOptions: {
				...variantBOverlayOptions,
				brightness: 0.1,
				gamma: 0.9,
				saturation: 1.15,
				vignette: 0.03,
				leftLift: 0.08,
				textBoxOpacity: 0.26,
			},
		},
	];

	const variantResults = [];
	for (const variant of variantPlans) {
		try {
			const baseImage = path.join(
				tmpDir,
				`thumb_comp_${jobId}_${variant.key}.png`
			);
			await composeThumbnailBase({
				baseImagePath: bg.path,
				presenterImagePath: presenterForCompose,
				topicImagePaths,
				outPath: baseImage,
				width: THUMBNAIL_WIDTH,
				height: THUMBNAIL_HEIGHT,
				accentColor: stylePlan.accent,
				layout: variant.layout,
			});

			const finalPath = path.join(tmpDir, `thumb_${jobId}_${variant.key}.jpg`);
			const overlayOptions = {
				...variant.overlayOptions,
				leftPanelPct: Number.isFinite(Number(variant.layout?.leftPanelPct))
					? Number(variant.layout.leftPanelPct)
					: LEFT_PANEL_PCT,
			};
			await renderThumbnailOverlay({
				inputPath: baseImage,
				outputPath: finalPath,
				title: variant.title,
				accentColor: stylePlan.accent,
				overlayOptions,
			});
			ensureThumbnailFile(finalPath);
			const qa = await applyThumbnailQaAdjustments(finalPath, { log });
			if (log) log("thumbnail qa result", { variant: variant.key, ...qa });
			ensureThumbnailFile(finalPath);

			const maxBytes = 2 * 1024 * 1024;
			let didReencode = false;
			if (fs.statSync(finalPath).size > maxBytes) {
				const qualitySteps = [3, 4, 5, 6];
				for (const q of qualitySteps) {
					const smaller = path.join(
						tmpDir,
						`thumb_${jobId}_${variant.key}_q${q}.jpg`
					);
					await runFfmpeg(
						[
							"-i",
							finalPath,
							"-q:v",
							String(q),
							"-frames:v",
							"1",
							"-y",
							smaller,
						],
						"thumbnail_reencode_smaller"
					);
					safeUnlink(finalPath);
					fs.renameSync(smaller, finalPath);
					ensureThumbnailFile(finalPath);
					didReencode = true;
					if (fs.statSync(finalPath).size <= maxBytes) break;
				}
				if (fs.statSync(finalPath).size > maxBytes && log) {
					log("thumbnail size still above max", {
						variant: variant.key,
						size: fs.statSync(finalPath).size,
						maxBytes,
					});
				}
			}

			let overallLuma = qa?.after?.overall;
			let leftLuma = qa?.after?.left;
			if (
				didReencode ||
				!Number.isFinite(overallLuma) ||
				!Number.isFinite(leftLuma)
			) {
				overallLuma = samplePreviewLuma(finalPath);
				leftLuma = samplePreviewLuma(finalPath, getQaLeftSampleRegion());
			}
			const lumaScore = scoreThumbnailLuma({
				overall: overallLuma,
				left: leftLuma,
			});
			const overallLog = Number.isFinite(overallLuma)
				? Number(overallLuma.toFixed(3))
				: null;
			const leftLog = Number.isFinite(leftLuma)
				? Number(leftLuma.toFixed(3))
				: null;
			const scoreLog = Number.isFinite(lumaScore)
				? Number(lumaScore.toFixed(3))
				: null;
			if (log)
				log("thumbnail luma score", {
					variant: variant.key,
					overall: overallLog,
					left: leftLog,
					score: scoreLog,
				});

			variantResults.push({
				variant: variant.key,
				localPath: finalPath,
				url: "",
				publicId: "",
				width: THUMBNAIL_WIDTH,
				height: THUMBNAIL_HEIGHT,
				title: variant.title,
				luma: {
					overall: overallLuma,
					left: leftLuma,
					score: lumaScore,
				},
			});
		} catch (e) {
			if (log)
				log("thumbnail variant failed", {
					variant: variant.key,
					error: e?.message || String(e),
				});
		}
	}

	if (!variantResults.length) throw new Error("thumbnail_generation_failed");
	const scoredVariants = variantResults.filter((v) =>
		Number.isFinite(v?.luma?.score)
	);
	const preferred =
		scoredVariants.reduce((best, next) => {
			if (!best) return next;
			return next.luma.score > best.luma.score ? next : best;
		}, null) ||
		variantResults.find((v) => v.variant === "b") ||
		variantResults[0];
	const uploaded = await uploadThumbnailToCloudinary(
		preferred.localPath,
		jobId
	);
	preferred.url = uploaded.url;
	preferred.publicId = uploaded.public_id;
	preferred.width = uploaded.width;
	preferred.height = uploaded.height;
	return {
		localPath: preferred.localPath,
		url: preferred.url,
		publicId: preferred.publicId,
		width: preferred.width,
		height: preferred.height,
		title: preferred.title,
		pose: stylePlan.pose,
		accent: stylePlan.accent,
		variants: variantResults,
	};
}

module.exports = {
	buildThumbnailPrompt,
	buildRunwayThumbnailPrompt,
	generateThumbnailCompositeBase,
	generateThumbnailPackage,
};
