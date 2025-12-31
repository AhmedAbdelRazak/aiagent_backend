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
const QA_LUMA_MIN = 0.36;
const QA_LUMA_LEFT_MIN = 0.33;
const THUMBNAIL_TEXT_MARGIN_PCT = 0.05;
const THUMBNAIL_TEXT_SIZE_PCT = 0.12;
const THUMBNAIL_TEXT_LINE_SPACING_PCT = 0.2;
const THUMBNAIL_TEXT_Y_OFFSET_PCT = 0.22;
const THUMBNAIL_TOPIC_MAX_IMAGES = 1;
const THUMBNAIL_TOPIC_MIN_EDGE = 900;
const THUMBNAIL_TOPIC_MIN_BYTES = 60000;
const THUMBNAIL_TOPIC_MAX_DOWNLOADS = 6;
const THUMBNAIL_MIN_BYTES = 12000;
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
const CSE_PREFERRED_IMG_SIZE = "xlarge";
const CSE_FALLBACK_IMG_SIZE = "large";
const CSE_PREFERRED_IMG_COLOR = "color";
const CSE_MIN_IMAGE_SHORT_EDGE = 720;
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

const RUNWAY_PROMPT_CHAR_LIMIT = 520;

function cleanThumbnailText(text = "") {
	return String(text || "")
		.replace(/([a-z])([A-Z])/g, "$1 $2")
		.replace(/["'`]/g, "")
		.replace(/[^a-z0-9\s]/gi, " ")
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

function buildImageMatchCriteria(topic = "", extraTokens = []) {
	const rawTokens = tokenizeLabel(topic);
	const baseTokens = topicTokensFromTitle(topic);
	const wordSource = baseTokens.length >= 2 ? baseTokens : rawTokens;
	const specificWords = filterSpecificTopicTokens(wordSource);
	const wordTokens = specificWords.length ? specificWords : wordSource;
	const phraseToken = rawTokens.length >= 2 ? rawTokens.join(" ") : "";
	const extra = Array.isArray(extraTokens)
		? extraTokens.flatMap((t) => tokenizeLabel(t))
		: [];
	const wordSet = new Set(normalizeTopicTokens(wordTokens));
	const contextTokens = filterContextTokens(
		extra.filter((tok) => !wordSet.has(String(tok).toLowerCase()))
	);
	return {
		wordTokens,
		phraseToken,
		contextTokens: contextTokens.slice(0, 6),
		minWordMatches: minImageTokenMatches(wordTokens),
		rawTokenCount: rawTokens.length,
	};
}

function sanitizeOverlayQuery(query = "") {
	return String(query || "")
		.replace(/[^a-z0-9\s]/gi, " ")
		.replace(/\s+/g, " ")
		.trim()
		.slice(0, 80);
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
	{ num = 4, searchType = null, imgSize = null, imgColorType = null } = {}
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
								...(imgColorType ? { imgColorType } : {}),
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

function isLikelyWatermarkedSource(url = "", contextLink = "") {
	const hay = `${url} ${contextLink}`.toLowerCase();
	return WATERMARK_URL_TOKENS.some((token) => hay.includes(token));
}

function isProbablyDirectImageUrl(u) {
	const url = String(u || "").trim();
	if (!/^https?:\/\//i.test(url)) return false;
	return /\.(png|jpe?g|webp)(\?|#|$)/i.test(url);
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
Premium cinematic studio background plate for a YouTube thumbnail.
No people, no faces, no text, no logos, no watermarks, no candles.
Left ~40% is clean and brighter for headline text and panels; keep it uncluttered with a smooth gradient backdrop.
Subtle, tasteful topic atmosphere inspired by: ${topicFocus}.
Classy, upscale editorial lighting, crisp detail, balanced contrast, shallow depth of field, soft vignette, rich but elegant color palette.
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

async function applyThumbnailQaAdjustments(filePath, { log } = {}) {
	if (!ffmpegPath) return { applied: false };
	const overall = samplePreviewLuma(filePath);
	const left = samplePreviewLuma(filePath, {
		x: 0,
		y: 0,
		w: Math.round(QA_PREVIEW_WIDTH * 0.45),
		h: Math.round(QA_PREVIEW_HEIGHT * 0.35),
	});
	if (overall == null || left == null) return { applied: false };
	if (overall >= QA_LUMA_MIN && left >= QA_LUMA_LEFT_MIN)
		return { applied: false, overall, left };
	const tmp = path.join(
		path.dirname(filePath),
		`thumb_qalift_${path.basename(filePath)}`
	);
	await runFfmpeg(
		[
			"-i",
			filePath,
			"-vf",
			"eq=contrast=1.02:saturation=1.06:brightness=0.05:gamma=0.96",
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
	if (log)
		log("thumbnail qa lift applied", {
			overall: Number(overall.toFixed(3)),
			left: Number(left.toFixed(3)),
		});
	return { applied: true, overall, left };
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
}) {
	const W = makeEven(Math.max(2, Math.round(width)));
	const H = makeEven(Math.max(2, Math.round(height)));
	const leftW = Math.max(1, Math.round(W * LEFT_PANEL_PCT));
	const margin = Math.max(4, Math.round(W * PANEL_MARGIN_PCT));
	const overlap = Math.max(0, Math.round(W * PRESENTER_OVERLAP_PCT));
	let presenterW = Math.max(2, W - leftW + overlap);
	presenterW = makeEven(Math.min(W, presenterW));
	const presenterX = Math.max(0, Math.min(W - presenterW, leftW - overlap));

	const topics = Array.isArray(topicImagePaths)
		? topicImagePaths.filter(Boolean).slice(0, 2)
		: [];
	const panelCount = topics.length;
	const panelW = makeEven(Math.max(2, leftW - margin * 2));
	const topPad = panelCount === 1 ? Math.round(H * 0.22) : 0;
	const panelH =
		panelCount > 1
			? makeEven(Math.max(2, Math.round((H - margin * 3) / 2)))
			: makeEven(Math.max(2, H - margin * 2 - topPad));
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
		`[1:v]scale=${presenterW}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${presenterW}:${H}:(iw-ow)/2:(ih-oh)/2,format=rgba[presenter]`
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
			panelCount > 1 ? margin : Math.max(0, Math.round(margin + topPad));
		const panelCropX = "(iw-ow)/2";
		const panelCropY = "(ih-oh)/2";
		filters.push(
			`[${panel1Idx}:v]scale=${panelInnerW}:${panelInnerH}:force_original_aspect_ratio=increase:flags=lanczos,crop=${panelInnerW}:${panelInnerH}:${panelCropX}:${panelCropY}[panel1i]`
		);
		filters.push(
			`[panel1i]pad=${panelW}:${panelH}:${panelBorder}:${panelBorder}:color=${accentColor}@0.55[panel1]`
		);
		filters.push(`${current}[panel1]overlay=${margin}:${panelY}[tmp1]`);
		current = "[tmp1]";
	}

	if (panelCount >= 2) {
		const panel2Idx = 3;
		const panel2Y = Math.max(0, margin * 2 + panelH);
		const panelCropX = "(iw-ow)/2";
		const panelCropY = "(ih-oh)/2";
		filters.push(
			`[${panel2Idx}:v]scale=${panelInnerW}:${panelInnerH}:force_original_aspect_ratio=increase:flags=lanczos,crop=${panelInnerW}:${panelInnerH}:${panelCropX}:${panelCropY}[panel2i]`
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

function buildThumbnailText(title = "") {
	const cleaned = cleanThumbnailText(title);
	if (!cleaned) return { text: "", fontScale: 1 };
	const pretty = titleCaseIfLower(cleaned);
	const words = pretty.split(" ").filter(Boolean);
	const trimmedWords = words.slice(0, THUMBNAIL_TEXT_MAX_WORDS);
	const trimmed = trimmedWords.join(" ");
	const baseChars = THUMBNAIL_TEXT_BASE_MAX_CHARS;
	const fit = fitHeadlineText(trimmed, {
		baseMaxChars: baseChars,
		preferLines: 2,
		maxLines: 2,
	});
	return {
		text: fit.text || trimmed,
		fontScale: fit.fontScale || 1,
	};
}

async function renderThumbnailOverlay({
	inputPath,
	outputPath,
	title,
	accentColor = ACCENT_PALETTE.default,
}) {
	const { text, fontScale } = buildThumbnailText(title);
	const hasText = Boolean(text);
	const fontFile = THUMBNAIL_FONT_FILE
		? `:fontfile='${escapeDrawtext(THUMBNAIL_FONT_FILE)}'`
		: "";
	const fontSize = Math.max(
		42,
		Math.round(THUMBNAIL_HEIGHT * THUMBNAIL_TEXT_SIZE_PCT * fontScale)
	);
	const boxBorder = Math.round(fontSize * 0.55);
	const lineSpacing = Math.round(fontSize * THUMBNAIL_TEXT_LINE_SPACING_PCT);
	const textFilePath = hasText
		? path.join(
				path.dirname(outputPath),
				`thumb_text_${path.basename(outputPath, path.extname(outputPath))}.txt`
		  )
		: "";
	if (hasText) fs.writeFileSync(textFilePath, text, "utf8");

	const filters = [
		`scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}`,
		"eq=contrast=1.05:saturation=1.10:brightness=0.06:gamma=0.96",
		"unsharp=5:5:0.7",
		`drawbox=x=0:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.012:t=fill`,
		`drawbox=x=iw*0.2:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.01:t=fill`,
		`drawbox=x=iw*0.4:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.007:t=fill`,
		`drawbox=x=iw*0.6:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.004:t=fill`,
		`drawbox=x=iw*0.8:y=0:w=iw*0.2:h=ih:color=${accentColor}@0.002:t=fill`,
		`drawbox=x=0:y=0:w=iw*0.018:h=ih:color=${accentColor}@0.25:t=fill`,
		`drawbox=x=0:y=0:w=iw:h=ih:color=${accentColor}@0.08:t=4`,
		"vignette=0.08",
	];
	if (hasText) {
		filters.push(
			`drawtext=textfile='${escapeDrawtext(
				textFilePath
			)}'${fontFile}:fontsize=${fontSize}:fontcolor=white:borderw=3:bordercolor=black@0.6:box=1:boxcolor=black@0.35:boxborderw=${boxBorder}:shadowcolor=black@0.45:shadowx=2:shadowy=2:line_spacing=${lineSpacing}:x=w*${THUMBNAIL_TEXT_MARGIN_PCT}:y=h*0.12`
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
	const minMatches = minImageTokenMatches(matchTokens);

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
		const mime = String(it.image?.mime || "").toLowerCase();
		candidates.push({ url, score, urlMatches, w, h, mime });
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
		if (!isProbablyDirectImageUrl(c.url) && !c.mime) continue;
		filtered.push(c.url);
		if (filtered.length >= 6) break;
	}
	return filtered;
}

async function collectThumbnailTopicImages({
	topics = [],
	tmpDir,
	jobId,
	title = "",
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

	const contextQuery = sanitizeOverlayQuery(
		sanitizeThumbnailContext(title || "")
	);
	const contextTokens =
		contextQuery && contextQuery.length >= 4 ? [contextQuery] : [];

	const urls = [];
	const seen = new Set();
	const maxUrls = Math.max(target * 4, THUMBNAIL_TOPIC_MAX_DOWNLOADS);
	for (const t of topicList) {
		if (urls.length >= maxUrls) break;
		const label = t?.displayTopic || t?.topic || "";
		if (!label) continue;
		const extraTokens = Array.isArray(t?.keywords) ? t.keywords : [];
		const mergedTokens = contextTokens.length
			? [...contextTokens, ...extraTokens]
			: extraTokens;
		let hits = hasCSE ? await fetchCseImages(label, mergedTokens) : [];
		if (!hits.length && hasCSE) {
			const ctxItems = await fetchCseContext(label, mergedTokens);
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
			if (ogHits.length) {
				if (log)
					log("thumbnail topic images fallback og", {
						topic: label,
						count: ogHits.length,
					});
				hits = ogHits;
			}
		}
		if (!hits.length) {
			const wikiUrl = await fetchWikipediaPageImageUrl(label);
			if (wikiUrl) {
				if (log)
					log("thumbnail topic images fallback wiki", {
						topic: label,
					});
				hits = [wikiUrl];
			}
		}
		if (!hits.length) {
			const commons = await fetchWikimediaImageUrls(label, 3);
			if (commons.length) {
				if (log)
					log("thumbnail topic images fallback commons", {
						topic: label,
						count: commons.length,
					});
				hits = commons;
			}
		}
		for (const url of hits) {
			if (urls.length >= maxUrls) break;
			if (!url || seen.has(url)) continue;
			seen.add(url);
			urls.push(url);
		}
		if (urls.length < maxUrls) {
			const wikiUrl = await fetchWikipediaPageImageUrl(label);
			if (wikiUrl && !seen.has(wikiUrl)) {
				if (log)
					log("thumbnail topic images fallback wiki", {
						topic: label,
					});
				seen.add(wikiUrl);
				urls.push(wikiUrl);
			}
		}
		if (urls.length < maxUrls) {
			const commons = await fetchWikimediaImageUrls(label, 3);
			if (commons.length) {
				if (log)
					log("thumbnail topic images fallback commons", {
						topic: label,
						count: commons.length,
					});
				for (const url of commons) {
					if (urls.length >= maxUrls) break;
					if (!url || seen.has(url)) continue;
					seen.add(url);
					urls.push(url);
				}
			}
		}
	}

	if (!urls.length) {
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
	const downloadCount = Math.min(urls.length, THUMBNAIL_TOPIC_MAX_DOWNLOADS);
	for (let i = 0; i < downloadCount; i++) {
		const url = urls[i];
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
			const minEdge = Math.min(dims.width || 0, dims.height || 0);
			if (minEdge && minEdge < CSE_MIN_IMAGE_SHORT_EDGE) {
				smallCandidates.push({
					path: out,
					size: st.size,
					width: dims.width || 0,
					height: dims.height || 0,
				});
				continue;
			}
			candidates.push({
				path: out,
				size: st.size,
				width: dims.width || 0,
				height: dims.height || 0,
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
	pickPool.sort((a, b) => b.size - a.size);
	const selected = pickPool.slice(0, target).map((c) => c.path);

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

function selectThumbnailTitle({ title, shortTitle, seoTitle, topics }) {
	const topicLabel =
		Array.isArray(topics) && topics.length
			? topics[0]?.displayTopic || topics[0]?.topic || ""
			: "";
	const topicWords = cleanThumbnailText(topicLabel).split(" ").filter(Boolean);
	const shortWords = cleanThumbnailText(shortTitle || "")
		.split(" ")
		.filter(Boolean);
	if (topicWords.length && topicWords.length <= 3) return topicLabel;
	if (shortWords.length && shortWords.length <= 3) return shortTitle || "";
	return shortTitle || seoTitle || title || topicLabel;
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
	const presenterForCompose = presenterLocalPath;
	if (log)
		log("presenter fixed (no emotion variants)", {
			path: path.basename(presenterForCompose),
		});
	const chosenDetected = detectFileType(presenterForCompose);
	if (!chosenDetected || chosenDetected.kind !== "image")
		throw new Error("thumbnail_presenter_missing_or_invalid");
	if (chosenDetected.ext !== "png" && log) {
		log("thumbnail presenter not png", {
			ext: chosenDetected.ext || null,
		});
	}

	const thumbTitle = selectThumbnailTitle({
		title,
		shortTitle,
		seoTitle,
		topics,
	});

	const topicImagePaths = await collectThumbnailTopicImages({
		topics,
		tmpDir,
		jobId,
		title: title || seoTitle || "",
		maxImages: THUMBNAIL_TOPIC_MAX_IMAGES,
		requireTopicImages,
		log,
	});

	const baseImage = await generateThumbnailCompositeBase({
		jobId,
		tmpDir,
		presenterImagePath: presenterForCompose,
		topicImagePaths,
		title: thumbTitle,
		topics,
		ratio: THUMBNAIL_RATIO,
		width: THUMBNAIL_WIDTH,
		height: THUMBNAIL_HEIGHT,
		accentColor: stylePlan.accent,
		log,
	});

	const finalPath = path.join(tmpDir, `thumb_${jobId}.jpg`);
	await renderThumbnailOverlay({
		inputPath: baseImage,
		outputPath: finalPath,
		title: thumbTitle,
		accentColor: stylePlan.accent,
	});
	ensureThumbnailFile(finalPath);
	const qa = await applyThumbnailQaAdjustments(finalPath, { log });
	if (log) log("thumbnail qa result", qa);
	ensureThumbnailFile(finalPath);
	const maxBytes = 2 * 1024 * 1024;
	if (fs.statSync(finalPath).size > maxBytes) {
		const qualitySteps = [3, 4, 5, 6];
		for (const q of qualitySteps) {
			const smaller = path.join(tmpDir, `thumb_${jobId}_q${q}.jpg`);
			await runFfmpeg(
				["-i", finalPath, "-q:v", String(q), "-frames:v", "1", "-y", smaller],
				"thumbnail_reencode_smaller"
			);
			safeUnlink(finalPath);
			fs.renameSync(smaller, finalPath);
			ensureThumbnailFile(finalPath);
			if (fs.statSync(finalPath).size <= maxBytes) break;
		}
		if (fs.statSync(finalPath).size > maxBytes && log) {
			log("thumbnail size still above max", {
				size: fs.statSync(finalPath).size,
				maxBytes,
			});
		}
	}
	const uploaded = await uploadThumbnailToCloudinary(finalPath, jobId);
	return {
		localPath: finalPath,
		url: uploaded.url,
		publicId: uploaded.public_id,
		width: uploaded.width,
		height: uploaded.height,
		title: thumbTitle,
		pose: stylePlan.pose,
		accent: stylePlan.accent,
	};
}

module.exports = {
	buildThumbnailPrompt,
	buildRunwayThumbnailPrompt,
	generateThumbnailCompositeBase,
	generateThumbnailPackage,
};
