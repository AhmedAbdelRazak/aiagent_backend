/** @format */

const fs = require("fs");
const path = require("path");
const child_process = require("child_process");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;
const { editImageToPath, pickOpenAIImageSize } = require("./openaiImageTools");

let ffmpegPath = "";
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
}

const THUMBNAIL_WIDTH = 1280;
const THUMBNAIL_HEIGHT = 720;
const THUMBNAIL_MIN_BYTES = 12000;
const THUMBNAIL_CLOUDINARY_FOLDER = "aivideomatic/long_thumbnails";
const THUMBNAIL_CLOUDINARY_PUBLIC_PREFIX = "long_thumb";
const MAX_TOPIC_REFERENCE_IMAGES = 2;
const DEFAULT_LEFT_TEXT_PCT = 0.54;
const THUMBNAIL_TOPIC_PANEL_W = 740;
const THUMBNAIL_PRESENTER_PANEL_W = THUMBNAIL_WIDTH - THUMBNAIL_TOPIC_PANEL_W;
const THUMBNAIL_FINISH_FILTER =
	"eq=contrast=1.025:saturation=1.025:brightness=0.004,unsharp=5:5:0.38:3:3:0.05";
const SOFT_THUMBNAIL_REFERENCE_SOURCES = new Set([
	"article",
	"existing",
	"fallback",
	"seed",
	"topic",
	"trend",
]);

const ACCENT_PALETTE = {
	default: "0xFFC700",
	tech: "0x00C2FF",
	business: "0x48C67A",
	legal: "0xFFB020",
	sports: "0x7CFF6B",
	politics: "0xFF4D4D",
	entertainment: "0xFFC700",
};

const THUMBNAIL_STYLE_PROFILES = [
	{
		id: "cinematic_gold",
		accent: "0xFFC700",
		tagColor: "0x27305F",
		lowerPanelOpacity: 0.34,
		brief:
			"cinematic editorial contrast, warm gold accent, clean dark lower panel, premium entertainment-news energy",
	},
	{
		id: "electric_cyan",
		accent: "0x00C2FF",
		tagColor: "0x102A43",
		lowerPanelOpacity: 0.32,
		brief:
			"cool tech-forward contrast, cyan accent, crisp edge lighting, modern product/news energy",
	},
	{
		id: "market_green",
		accent: "0x48C67A",
		tagColor: "0x12392B",
		lowerPanelOpacity: 0.31,
		brief:
			"polished business-news contrast, green accent, controlled highlights, clean financial-news energy",
	},
	{
		id: "serious_amber",
		accent: "0xFFB020",
		tagColor: "0x2B2118",
		lowerPanelOpacity: 0.38,
		brief:
			"serious mainstream-news contrast, amber accent, restrained dramatic depth, respectful editorial energy",
	},
	{
		id: "sports_lime",
		accent: "0xA6FF3D",
		tagColor: "0x17310F",
		lowerPanelOpacity: 0.33,
		brief:
			"fast sports-broadcast contrast, lime accent, action-focused clarity, energetic but uncluttered",
	},
	{
		id: "magenta_pop",
		accent: "0xFF4DB8",
		tagColor: "0x3A1438",
		lowerPanelOpacity: 0.32,
		brief:
			"glossy pop-culture contrast, magenta accent, bright subject separation, entertainment energy",
	},
];

const PERSON_NAME_STOPWORDS = new Set([
	"update",
	"details",
	"news",
	"story",
	"case",
	"trial",
	"court",
	"movie",
	"film",
	"show",
	"season",
	"series",
	"episode",
	"album",
	"song",
	"interview",
	"recap",
	"breakdown",
	"explained",
	"latest",
	"official",
]);

function normalizeWhitespace(value = "") {
	return String(value || "")
		.replace(/\s+/g, " ")
		.trim();
}

function ensureDir(dirPath) {
	if (dirPath) fs.mkdirSync(dirPath, { recursive: true });
}

function safeUnlink(filePath) {
	try {
		if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
	} catch {}
}

function uniqueStrings(values = [], { limit = Infinity } = {}) {
	const out = [];
	const seen = new Set();
	for (const value of Array.isArray(values) ? values : []) {
		const normalized = normalizeWhitespace(value);
		if (!normalized || seen.has(normalized)) continue;
		seen.add(normalized);
		out.push(normalized);
		if (out.length >= limit) break;
	}
	return out;
}

function isHttpUrl(value = "") {
	return /^https?:\/\//i.test(String(value || "").trim());
}

function safeSlug(value = "", maxLen = 48) {
	const slug = normalizeWhitespace(value)
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-+|-+$/g, "");
	return (slug || "item").slice(0, maxLen);
}

function runFfmpeg(args, label = "ffmpeg") {
	if (!ffmpegPath) throw new Error("ffmpeg_unavailable");
	try {
		child_process.execFileSync(
			ffmpegPath,
			["-hide_banner", "-loglevel", "error", ...args],
			{
				maxBuffer: 64 * 1024 * 1024,
				stdio: ["ignore", "pipe", "pipe"],
				windowsHide: true,
			},
		);
	} catch (error) {
		const stderr = String(error?.stderr || error?.message || "").trim();
		throw new Error(`${label}_failed${stderr ? `:${stderr}` : ""}`);
	}
}

function resolveThumbnailFontFile() {
	const candidates = [
		String(process.env.FFMPEG_FONT_PATH || "").trim(),
		"C:/Windows/Fonts/impact.ttf",
		"C:/Windows/Fonts/arialbd.ttf",
		"C:/Windows/Fonts/arial.ttf",
		"/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
		"/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
		"/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
		"/Library/Fonts/Arial Bold.ttf",
		"/Library/Fonts/Arial.ttf",
	].filter(Boolean);
	for (const candidate of candidates) {
		try {
			if (candidate && fs.existsSync(candidate)) return candidate;
		} catch {}
	}
	return null;
}

function escapeDrawtext(value = "") {
	const placeholder = "__NL__";
	return String(value || "")
		.replace(/\r\n|\r|\n/g, placeholder)
		.replace(/\\/g, "\\\\")
		.replace(/,/g, "\\,")
		.replace(/:/g, "\\:")
		.replace(/'/g, "\\'")
		.replace(/%/g, "\\%")
		.replace(/\[/g, "\\[")
		.replace(/\]/g, "\\]")
		.replace(new RegExp(placeholder, "g"), "\\n")
		.trim();
}

function normalizeAccentColor(value = "") {
	const raw = String(value || "")
		.trim()
		.replace(/^0x/i, "")
		.replace(/^#/, "");
	if (/^[0-9a-f]{6}$/i.test(raw)) return `0x${raw.toUpperCase()}`;
	return "0xFFC700";
}

function wrapTextToLines(text = "", maxCharsPerLine = 14, maxLines = 2) {
	const words = normalizeWhitespace(text).split(/\s+/).filter(Boolean);
	if (!words.length) return { text: "", lines: 0, overflow: false };

	const lines = [];
	let line = "";
	for (const word of words) {
		const next = line ? `${line} ${word}` : word;
		if (next.length <= maxCharsPerLine) {
			line = next;
			continue;
		}
		if (line) lines.push(line);
		line = word;
	}
	if (line) lines.push(line);

	if (lines.length <= maxLines) {
		return { text: lines.join("\n"), lines: lines.length, overflow: false };
	}

	const kept = lines.slice(0, maxLines);
	kept[maxLines - 1] = normalizeWhitespace(
		`${kept[maxLines - 1]} ${lines.slice(maxLines).join(" ")}`,
	);
	return {
		text: kept.join("\n"),
		lines: kept.length,
		overflow: true,
	};
}

function fitThumbnailText(
	text = "",
	{ baseMaxChars = 14, maxLines = 2, maxChars = 30 } = {},
) {
	const clean = trimTextToMaxChars(text, maxChars);
	if (!clean) return { text: "", fontScale: 1, truncated: false };

	const scales = [1, 0.94, 0.88, 0.82, 0.76];
	for (const scale of scales) {
		const wrapped = wrapTextToLines(
			clean,
			Math.max(baseMaxChars, Math.round(baseMaxChars / scale)),
			maxLines,
		);
		if (!wrapped.overflow) {
			return {
				text: wrapped.text,
				fontScale: scale,
				truncated: false,
			};
		}
	}

	const wrapped = wrapTextToLines(clean, baseMaxChars + 3, maxLines);
	return {
		text: wrapped.text,
		fontScale: 0.76,
		truncated: wrapped.overflow,
	};
}

function trimTextToMaxChars(text = "", maxChars = 30) {
	const clean = normalizeWhitespace(text);
	const limit = Number(maxChars) || 0;
	if (!limit || clean.length <= limit) return clean;
	const clipped = clean.slice(0, limit).trim();
	const wordSafe = clipped.replace(/\s+\S*$/, "").trim();
	return wordSafe || clipped;
}

function estimateDrawtextWidth(text = "", fontSize = 32) {
	let units = 0;
	for (const char of String(text || "")) {
		if (char === " ") units += 0.34;
		else if (/[MW]/.test(char)) units += 1;
		else if (/[A-Z]/.test(char)) units += 0.78;
		else if (/[a-z]/.test(char)) units += 0.58;
		else if (/[0-9]/.test(char)) units += 0.64;
		else units += 0.36;
	}
	return units * fontSize;
}

function fitFontSizeToWidth(
	lines = [],
	baseFontSize = 32,
	{ maxWidth = 640, minFontSize = 20 } = {},
) {
	let size = Math.round(Number(baseFontSize) || 32);
	const floor = Math.max(12, Math.round(Number(minFontSize) || 20));
	const widthLimit = Math.max(120, Number(maxWidth) || 640);
	const safeLines = (Array.isArray(lines) ? lines : [lines])
		.map((line) => normalizeWhitespace(line))
		.filter(Boolean);
	while (
		size > floor &&
		safeLines.some((line) => estimateDrawtextWidth(line, size) > widthLimit)
	) {
		size -= 2;
	}
	return Math.max(floor, size);
}

function detectBufferType(buffer) {
	if (!buffer || buffer.length < 12) return null;
	if (
		buffer[0] === 0x89 &&
		buffer[1] === 0x50 &&
		buffer[2] === 0x4e &&
		buffer[3] === 0x47
	) {
		return { kind: "image", ext: "png" };
	}
	if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) {
		return { kind: "image", ext: "jpg" };
	}
	if (
		buffer.toString("ascii", 0, 4) === "RIFF" &&
		buffer.toString("ascii", 8, 12) === "WEBP"
	) {
		return { kind: "image", ext: "webp" };
	}
	return null;
}

function detectFileType(filePath) {
	if (!filePath || !fs.existsSync(filePath)) return null;
	try {
		const fd = fs.openSync(filePath, "r");
		const head = Buffer.alloc(32);
		fs.readSync(fd, head, 0, 32, 0);
		fs.closeSync(fd);
		return detectBufferType(head);
	} catch {
		return null;
	}
}

function ensureImageFile(filePath, minBytes = THUMBNAIL_MIN_BYTES) {
	if (!filePath || !fs.existsSync(filePath)) {
		throw new Error("thumbnail_image_missing");
	}
	const stat = fs.statSync(filePath);
	if (!stat || stat.size < minBytes) {
		throw new Error("thumbnail_image_too_small");
	}
	const detected = detectFileType(filePath);
	if (!detected || detected.kind !== "image") {
		throw new Error("thumbnail_image_invalid");
	}
	return filePath;
}

function ensureThumbnailFile(filePath, minBytes = THUMBNAIL_MIN_BYTES) {
	return ensureImageFile(filePath, minBytes);
}

async function downloadImageToPath({ url, tmpDir, basename }) {
	if (!isHttpUrl(url)) throw new Error("thumbnail_reference_url_invalid");
	ensureDir(tmpDir);
	const response = await axios.get(url, {
		responseType: "arraybuffer",
		timeout: 30000,
		validateStatus: (status) => status >= 200 && status < 400,
	});
	const buffer = Buffer.from(response.data || []);
	const detected = detectBufferType(buffer);
	if (!detected || detected.kind !== "image") {
		throw new Error("thumbnail_reference_not_image");
	}
	const outPath = path.join(tmpDir, `${basename}.${detected.ext}`);
	fs.writeFileSync(outPath, buffer);
	return outPath;
}

function topicLabelHasPersonAnchor(label = "") {
	const normalized = normalizeWhitespace(label);
	if (!normalized) return false;
	if (looksLikeLikelyPersonName(normalized)) return true;
	const words = normalized
		.split(/\s+/)
		.map((word) => word.replace(/[^A-Za-z'.-]/g, ""))
		.filter(Boolean);
	for (const count of [2, 3]) {
		if (
			words.length >= count &&
			looksLikeLikelyPersonName(words.slice(0, count).join(" "))
		) {
			return true;
		}
	}
	return false;
}

async function collectTopicReferenceImages({
	topics = [],
	tmpDir,
	jobId,
	maxImages = MAX_TOPIC_REFERENCE_IMAGES,
	log,
}) {
	const urls = [];
	for (const topic of Array.isArray(topics) ? topics : []) {
		const confidence = normalizeWhitespace(topic?.thumbnailImageConfidence)
			.toLowerCase()
			.trim();
		if (confidence === "weak") {
			if (typeof log === "function") {
				log("thumbnail topic reference skipped", {
					reason: "weak_confidence",
					topic: topic?.displayTopic || topic?.topic || "",
				});
			}
			continue;
		}
		const sourceType = normalizeWhitespace(
			topic?.thumbnailImageSourceType || topic?.thumbnailImageSource || "",
		).toLowerCase();
		const topicLabel = topic?.displayTopic || topic?.topic || "";
		if (
			confidence !== "high" &&
			sourceType &&
			SOFT_THUMBNAIL_REFERENCE_SOURCES.has(sourceType) &&
			topicLabelHasPersonAnchor(topicLabel)
		) {
			if (typeof log === "function") {
				log("thumbnail topic reference skipped", {
					reason: "soft_person_reference",
					topic: topicLabel,
					source: sourceType,
					confidence,
				});
			}
			continue;
		}
		for (const url of uniqueStrings(topic?.thumbnailImageUrls || [])) {
			if (!isHttpUrl(url)) continue;
			urls.push(url);
			if (urls.length >= maxImages * 2) break;
		}
		if (urls.length >= maxImages * 2) break;
	}

	const paths = [];
	for (
		let index = 0;
		index < urls.length && paths.length < maxImages;
		index++
	) {
		try {
			const localPath = await downloadImageToPath({
				url: urls[index],
				tmpDir,
				basename: `thumb_topic_ref_${safeSlug(jobId || "job")}_${index + 1}`,
			});
			ensureImageFile(localPath, 5000);
			paths.push(localPath);
		} catch (error) {
			if (typeof log === "function") {
				log("thumbnail topic reference skipped", {
					url: urls[index],
					error: error?.message || String(error),
				});
			}
		}
	}

	if (typeof log === "function") {
		log("thumbnail topic references ready", {
			count: paths.length,
			paths: paths.map((item) => path.basename(item)),
		});
	}

	return paths;
}

function buildContextText({ title, shortTitle, seoTitle, topics }) {
	const topicLabels = (Array.isArray(topics) ? topics : [])
		.map((topic) => topic?.displayTopic || topic?.topic || "")
		.filter(Boolean)
		.join(" ");
	return normalizeWhitespace(
		`${title || ""} ${shortTitle || ""} ${seoTitle || ""} ${topicLabels}`,
	).toLowerCase();
}

function inferThumbnailIntent({ title, shortTitle, seoTitle, topics }) {
	const text = buildContextText({ title, shortTitle, seoTitle, topics });
	if (
		/\b(death|dead|died|dies|mourn|mourning|tribute|tributes|devastated|passed away|hospital|injury|illness|diagnosis)\b/.test(
			text,
		)
	) {
		return "serious_update";
	}
	if (
		/\b(court|trial|lawsuit|charged|indictment|arrest|investigation|probe|police|crime|case|suspect|murder|killed|death|dead)\b/.test(
			text,
		)
	) {
		return "legal";
	}
	if (/\b(market|finance|stock|earnings|business|startup|money)\b/.test(text)) {
		return "business";
	}
	if (
		/\b(ai|tech|software|developer|code|app|innovation|product)\b/.test(text)
	) {
		return "tech";
	}
	if (
		/\b(movie|film|tv|series|music|album|song|artist|actor|celebrity|trailer|season|show)\b/.test(
			text,
		)
	) {
		return "entertainment";
	}
	return "general";
}

function hashText(value = "") {
	let hash = 0;
	for (const char of String(value || "")) {
		hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
	}
	return hash;
}

function chooseThumbnailStyleProfile(intent = "general", text = "") {
	const hay = String(text || "").toLowerCase();
	if (
		intent === "serious_update" ||
		intent === "legal" ||
		/\b(death|dead|died|court|lawsuit|trial|arrest|investigation|tribute|mourn|mourning|devastated|hospital|injury)\b/.test(
			hay,
		)
	) {
		return { ...THUMBNAIL_STYLE_PROFILES[3] };
	}
	if (intent === "tech" || /\b(ai|tech|software|app|device|product)\b/.test(hay)) {
		return { ...THUMBNAIL_STYLE_PROFILES[1] };
	}
	if (
		intent === "business" ||
		intent === "finance" ||
		/\b(stock|market|earnings|business|startup|money|crypto|shares)\b/.test(
			hay,
		)
	) {
		return { ...THUMBNAIL_STYLE_PROFILES[2] };
	}
	if (
		intent === "sports" ||
		/\b(nfl|nba|nhl|mlb|ufc|match|playoffs|trade|goal|draft)\b/.test(hay)
	) {
		return { ...THUMBNAIL_STYLE_PROFILES[4] };
	}
	if (/\b(album|tour|song|music|artist|pop|concert)\b/.test(hay)) {
		return { ...THUMBNAIL_STYLE_PROFILES[5] };
	}
	if (intent === "entertainment") {
		const variants = [
			THUMBNAIL_STYLE_PROFILES[0],
			THUMBNAIL_STYLE_PROFILES[5],
		];
		return { ...variants[hashText(hay) % variants.length] };
	}
	const variants = [
		THUMBNAIL_STYLE_PROFILES[0],
		THUMBNAIL_STYLE_PROFILES[1],
		THUMBNAIL_STYLE_PROFILES[2],
		THUMBNAIL_STYLE_PROFILES[5],
	];
	return { ...variants[hashText(hay) % variants.length] };
}

function chooseAccentColor(intent, text = "") {
	return chooseThumbnailStyleProfile(intent, text).accent || ACCENT_PALETTE.default;
}

function safeOpacity(value, fallback = 0.34) {
	const n = Number(value);
	if (!Number.isFinite(n)) return fallback;
	return Math.min(0.5, Math.max(0.18, n));
}

function buildStyleDirectionLine(styleProfile = {}) {
	const profile = styleProfile || {};
	return normalizeWhitespace(
		`Dynamic style profile: ${profile.id || "balanced_editorial"}. ${
			profile.brief ||
			"premium editorial contrast, topic-matched accent color, clean depth, and clear mobile hierarchy"
		}. Use this as a direction, not a rigid template. The designer may choose refined shapes, curved or angled editorial panels, tasteful topic-appropriate symbolic accents, and depth effects when they improve click appeal.`,
	);
}

function chooseLockedTextOverlayLayout(styleProfile = {}) {
	const id = String(styleProfile?.id || "").toLowerCase();
	if (id === "serious_amber") {
		return {
			id: "respectful_editorial_anchor",
			panelX: 42,
			panelY: 376,
			panelW: 650,
			panelH: 264,
			headlineX: 62,
			headlineY: 448,
			badgeX: 64,
			badgeY: 398,
			maxTextWidth: 596,
			panelOpacity: 0.22,
			badgeBox: false,
			accentRail: true,
		};
	}
	if (id === "magenta_pop") {
		return {
			id: "pop_editorial_stack",
			panelX: 38,
			panelY: 380,
			panelW: 660,
			panelH: 254,
			headlineX: 66,
			headlineY: 442,
			badgeX: 66,
			badgeY: 394,
			maxTextWidth: 600,
			panelOpacity: 0.2,
			badgeBox: true,
			accentRail: true,
		};
	}
	if (id === "electric_cyan") {
		return {
			id: "clean_tech_anchor",
			panelX: 40,
			panelY: 386,
			panelW: 648,
			panelH: 248,
			headlineX: 64,
			headlineY: 448,
			badgeX: 64,
			badgeY: 400,
			maxTextWidth: 590,
			panelOpacity: 0.18,
			badgeBox: false,
			accentRail: true,
		};
	}
	return {
		id: "cinematic_editorial_anchor",
		panelX: 42,
		panelY: 382,
		panelW: 652,
		panelH: 252,
		headlineX: 64,
		headlineY: 446,
		badgeX: 64,
		badgeY: 398,
		maxTextWidth: 596,
		panelOpacity: 0.2,
		badgeBox: false,
		accentRail: true,
	};
}

function chooseThumbnailPose({
	expression = "",
	intent = "general",
	contextText = "",
}) {
	const expr = String(expression || "").toLowerCase();
	const text = String(contextText || "").toLowerCase();
	if (
		intent === "serious_update" ||
		intent === "legal" ||
		/\b(death|dead|killed|murder|arrest|charged|trial|lawsuit|injury|crash|tragic)\b/.test(
			text,
		)
	) {
		return expr === "thoughtful" ? "thoughtful" : "serious";
	}
	if (expr === "thoughtful") return "thoughtful";
	if (expr === "serious") return "serious";
	if (expr === "warm" || expr === "excited") return "smile";
	if (
		/\b(revealed|shocking|surprise|unexpected|what happened|wait what|leaked)\b/.test(
			text,
		)
	) {
		return "surprised";
	}
	return intent === "entertainment" ? "thoughtful" : "neutral";
}

function buildExpressionPrompt(pose = "neutral") {
	if (pose === "serious") {
		return "serious and composed, natural relaxed eyes, closed mouth, eyebrows almost unchanged, only a tiny shift in expression, not intense, not exaggerated, not dramatic";
	}
	if (pose === "thoughtful") {
		return "thoughtful and lightly engaged, natural relaxed eyes, eyebrows unchanged or barely lifted, mouth relaxed, only a tiny shift in expression, subtle and natural, never surprised";
	}
	if (pose === "surprised") {
		return "very mild controlled reaction, brows only barely raised and still natural, eyes not widened, never shocked, never exaggerated";
	}
	if (pose === "smile") {
		return "soft professional smile, tiny reaction only, friendly and subtle, eyebrows natural, no broad grin and no big cheek movement";
	}
	return "neutral, attentive, lightly engaged, calm and natural, almost no facial change";
}

function buildThumbnailArtDirection(intent = "general") {
	if (intent === "serious_update") {
		return "Keep the design simple and highly clickable: one strong presenter on the right, one respectful story cue on the left, bold clean typography, serious contrast, minimal clutter, and a premium mainstream-news tribute feel.";
	}
	if (intent === "legal") {
		return "Keep the design simple and highly clickable: one strong presenter on the right, one clear story cue on the left, bold clean typography, strong contrast, minimal clutter, and a premium mainstream-news feel.";
	}
	if (intent === "business") {
		return "Keep the design simple and highly clickable: one strong presenter on the right, one clean business cue on the left, bold typography, premium contrast, minimal clutter, and a polished financial-news feel.";
	}
	if (intent === "tech") {
		return "Keep the design simple and highly clickable: one strong presenter on the right, one clear modern tech cue on the left, crisp typography, bright contrast, minimal clutter, and a premium product-launch feel.";
	}
	if (intent === "entertainment") {
		return "Keep the design simple and highly clickable: one strong presenter on the right, one clear entertainment cue on the left, bold clean typography, bright contrast, minimal clutter, and a glossy editorial feel.";
	}
	return "Keep the design simple and highly clickable: one strong presenter on the right, one clear story cue on the left, bold clean typography, premium contrast, and minimal clutter.";
}

const TOPIC_DISPLAY_ACRONYMS = new Set([
	"ai",
	"api",
	"bbc",
	"cbs",
	"cnn",
	"dc",
	"espn",
	"fbi",
	"mlb",
	"nba",
	"nfl",
	"nhl",
	"nyc",
	"snl",
	"ucla",
	"ufc",
	"uk",
	"us",
	"usa",
	"wwe",
]);

const TOPIC_DISPLAY_LOWERCASE_WORDS = new Set([
	"a",
	"an",
	"and",
	"as",
	"at",
	"but",
	"by",
	"for",
	"from",
	"in",
	"of",
	"on",
	"or",
	"the",
	"to",
	"vs",
	"with",
]);

function formatTopicDisplayWord(word = "", index = 0) {
	const raw = String(word || "");
	const leading = raw.match(/^[^A-Za-z0-9]+/)?.[0] || "";
	const trailing = raw.match(/[^A-Za-z0-9]+$/)?.[0] || "";
	const core = raw.slice(leading.length, raw.length - trailing.length);
	if (!core) return raw;
	const lower = core.toLowerCase();
	if (/^[A-Z0-9]{2,}$/.test(core) || TOPIC_DISPLAY_ACRONYMS.has(lower)) {
		return `${leading}${core.toUpperCase()}${trailing}`;
	}
	if (index > 0 && TOPIC_DISPLAY_LOWERCASE_WORDS.has(lower)) {
		return `${leading}${lower}${trailing}`;
	}
	return `${leading}${core.charAt(0).toUpperCase()}${core
		.slice(1)
		.toLowerCase()}${trailing}`;
}

function formatTopicDisplay(label = "") {
	const normalized = normalizeWhitespace(label);
	if (!normalized) return "";
	return normalized
		.split(" ")
		.map((word, index) => formatTopicDisplayWord(word, index))
		.join(" ");
}

function primaryTopicLabel(topics = []) {
	return normalizeWhitespace(
		topics?.[0]?.displayTopic || topics?.[0]?.topic || "",
	);
}

function buildTopicFocus({ title, shortTitle, seoTitle, topics }) {
	const primary = primaryTopicLabel(topics);
	const parts = uniqueStrings([
		formatTopicDisplay(primary),
		normalizeWhitespace(title),
		normalizeWhitespace(shortTitle),
		normalizeWhitespace(seoTitle),
	]);
	return parts.filter(Boolean).slice(0, 3).join(" | ");
}

const TITLE_STOP_WORDS = new Set([
	"the",
	"a",
	"an",
	"we",
	"what",
	"actually",
	"know",
	"about",
	"this",
	"that",
	"right",
	"now",
	"why",
	"how",
	"is",
	"are",
	"was",
	"were",
	"to",
	"of",
	"for",
	"on",
	"in",
	"and",
	"or",
	"with",
	"from",
	"at",
	"latest",
]);

const GENERIC_THUMBNAIL_HEADLINES = new Set([
	"BIG UPDATE",
	"NEW DETAILS",
	"NEW UPDATE",
	"TOP STORY",
	"TOP STORIES",
	"TRENDING NOW",
	"WHAT WE KNOW",
	"WHAT CHANGED",
	"EXPLAINED",
]);

function normalizeDisplayWords(text = "") {
	return normalizeWhitespace(text)
		.replace(/[_|:;/\\-]+/g, " ")
		.replace(/[^\w\s!?']/g, " ")
		.split(/\s+/)
		.filter(Boolean);
}

function buildSignificantPhrase(text = "", maxWords = 4) {
	const words = normalizeDisplayWords(text).filter((word) => {
		const lower = word.toLowerCase();
		return (
			!TITLE_STOP_WORDS.has(lower) && (word.length > 2 || /[0-9]/.test(word))
		);
	});
	return words.slice(0, maxWords).join(" ");
}

function deriveHeadlineFromTitle({
	title,
	shortTitle,
	seoTitle,
	topics,
	intent,
}) {
	const primaryTopic = formatTopicDisplay(primaryTopicLabel(topics));
	const context = buildContextText({ title, shortTitle, seoTitle, topics });

	if (
		primaryTopic &&
		/\b(investigation|probe|court|trial|lawsuit|charged|indictment|arrest|case)\b/.test(
			context,
		)
	) {
		return `${primaryTopic} CASE`;
	}
	if (
		primaryTopic &&
		/\b(update|latest|new details|confirmed)\b/.test(context)
	) {
		return `${primaryTopic} UPDATE`;
	}
	if (primaryTopic && intent === "legal") return `${primaryTopic} CASE`;

	const significant =
		buildSignificantPhrase(title, 4) ||
		buildSignificantPhrase(shortTitle, 4) ||
		buildSignificantPhrase(seoTitle, 4);
	if (significant) return significant;
	if (primaryTopic) return `${primaryTopic} NEWS`;
	return intent === "legal" ? "NEW DETAILS" : "BIG UPDATE";
}

function normalizeHeadlineText(text = "", maxWords = 4) {
	const words = normalizeDisplayWords(text).slice(0, maxWords);
	let out = words.join(" ").trim();
	if (!out) out = "BIG UPDATE";
	while (out.length > 26 && words.length > 1) {
		words.pop();
		out = words.join(" ").trim();
	}
	return out.toUpperCase();
}

function chooseBadgeText({ intent, overrideBadgeText = "" }) {
	if (overrideBadgeText) return normalizeHeadlineText(overrideBadgeText, 3);
	if (intent === "legal") return "COURT FILE";
	if (intent === "business") return "MARKET WATCH";
	if (intent === "tech") return "BIG UPDATE";
	if (intent === "entertainment") return "TRENDING NOW";
	return "TOP STORY";
}

function headlineHasTopic(headline = "", topicDisplay = "") {
	const h = normalizeWhitespace(headline).toLowerCase();
	const t = normalizeWhitespace(topicDisplay).toLowerCase();
	return Boolean(h && t && h.includes(t));
}

function compactThumbnailSubline(topicDisplay = "") {
	const words = normalizeWhitespace(topicDisplay).split(/\s+/).filter(Boolean);
	if (words.length <= 3) return words.join(" ");
	const firstTwoLookLikeName = words
		.slice(0, 2)
		.every((word) => /^[A-Z][A-Za-z'.-]+$/.test(word));
	if (firstTwoLookLikeName) return words.slice(0, 2).join(" ");
	return words.slice(0, 3).join(" ");
}

function buildThumbnailTextPlan({
	title,
	shortTitle,
	seoTitle,
	topics,
	intent,
	overrideHeadline,
	overrideBadgeText,
}) {
	const primaryTopic = formatTopicDisplay(primaryTopicLabel(topics));
	const derivedHeadline = deriveHeadlineFromTitle({
		title,
		shortTitle,
		seoTitle,
		topics,
		intent,
	});
	const overrideClean = normalizeHeadlineText(overrideHeadline || "", 4);
	const derivedClean = normalizeHeadlineText(derivedHeadline, 4);
	const preferredHeadline =
		overrideHeadline &&
		!GENERIC_THUMBNAIL_HEADLINES.has(overrideClean) &&
		overrideClean.length >= 6
			? overrideHeadline
			: derivedHeadline || overrideHeadline;
	const primaryHeadline = normalizeHeadlineText(
		preferredHeadline || derivedClean,
		4,
	);
	const badgeText = chooseBadgeText({
		intent,
		overrideBadgeText,
	});
	const sublineText =
		primaryTopic && !headlineHasTopic(primaryHeadline, primaryTopic)
			? compactThumbnailSubline(primaryTopic)
			: "";
	return {
		primaryHeadline,
		badgeText,
		sublineText,
		primaryTopic,
	};
}

function buildThumbnailPrompt({
	title,
	shortTitle,
	seoTitle,
	topics,
	headline = "",
	badgeText = "",
	sublineText = "",
	pose = "neutral",
	intent = "general",
	topicReferenceCount = 0,
	styleProfile = {},
}) {
	const topicFocus = buildTopicFocus({ title, shortTitle, seoTitle, topics });
	const artDirection = buildThumbnailArtDirection(intent);
	const styleLine = buildStyleDirectionLine(styleProfile);
	const moodLine =
		intent === "legal"
			? "Keep the visual tone premium, serious, editorial, mainstream, bright enough for a high-performing YouTube thumbnail, and never horror-like."
			: "Keep the visual tone premium, modern, high-end, clean, bright, and clearly click-worthy for a long-form YouTube thumbnail.";
	const topicFigureLine =
		topicReferenceCount > 0
			? "Use any topic reference images only as contextual inspiration for the left side story cue. Never replace or duplicate the presenter."
			: "CRITICAL: no trusted topic reference image was provided. Do not invent or depict real people, cast members, celebrities, the deceased person, fake archival photos, or human faces on the left. Use environmental, object, studio, screen, script, memorial, or symbolic story cues instead.";
	return normalizeWhitespace(`
Create one premium 16:9 YouTube thumbnail visual foundation for a long-form video.
The first input image is the draft visual layout and presenter placement reference.
Use the first input image as the composition reference, then redesign and polish the visuals like a top-performing YouTube editorial thumbnail.
Improve clarity, contrast, lighting, color separation, depth, polish, and click appeal while preserving the same clear story.
Use YouTube best practices for a thumbnail meant to attract very high click-through: instantly readable at mobile size, bold hierarchy, clean negative space, strong focal contrast, no clutter, no muddy lighting, and no tiny decorative details.
Keep all important faces inside safe margins so the final 16:9 crop cannot cut anything off.
Use the presenter reference image to preserve the same presenter identity, glasses, beard, hair, overall facial appearance, shoulders, and current dark outfit.
The presenter must stay on the right side in the same position, same scale, and same framing established by the draft image, with the upper torso visible, camera-facing, photorealistic, crisp, and premium.
Do not zoom in on the presenter face, do not crop tighter, do not reframe the presenter, and do not change the presenter scale relative to the frame.
Do not change the presenter expression. Treat the presenter as locked: same face, same eyes, same eyebrows, same mouth, same glasses, same beard, same hairline, same body position.
Do not damage, redraw, redesign, relight, sharpen, smooth, stylize, or beautify the presenter. Design around him.
Use the left side for the story setup and leave clear negative space for final text overlays.
Topic direction: ${topicFocus || "current trending story"}.
${artDirection}
${styleLine}
${moodLine}
${topicFigureLine}
Keep the composition clean and uncluttered.
Use one dominant left-side visual cue only, not a busy collage.
Create a topic-specific design, not the same repeated template. Vary the image treatment, accent color, lighting, depth, and energy based on the story.
The design should feel punchy and clickable through contrast, focus, and hierarchy rather than heavy effects.
You have creative freedom to build a polished editorial text-safe area on the left using subtle curves, angled panels, layered shadows, light streaks, tasteful icon-like symbols, or story-matched graphic accents. Use these only when they fit the topic; for a memorial or death story, keep accents restrained and respectful.
Avoid plain full-width dark bars. Prefer an intentional designer-made area with depth, shape, and negative space.
Make the left topic/feed image feel intentionally chosen and sharpened. Avoid letting an unrelated celebrity, fashion, red-carpet, or generic stock-like image become the story cue unless that is truly the topic.
If the topic/feed image contains a face, crop and position that face so it stays clear and recognizable, but keep a clean face-free area for final text. Never put the final text-safe zone directly over any eyes, mouth, or important facial features in the topic image.
Make the thumbnail instantly understandable at a glance on mobile.
Do not render any readable text, letters, captions, labels, signs, logos, watermarks, lower thirds, badges, or duplicated words. The final text will be added separately after this visual edit.
Keep the left-side lower/mid area clean enough for bold thumbnail text to be added later, but do not place a face underneath that clean area. The final text may overlap abstract background or clothing, but it must not cover the presenter face, presenter glasses, topic-face eyes, topic-face mouth, or the main identity cue.
Do not add logos, watermarks, extra people, extra hands, deformed anatomy, broken glasses, damaged facial features, or muddy lighting.
The final thumbnail should look like a finished production thumbnail, not a rough draft.
	`);
}

function buildThumbnailDesignerPrompt({
	title,
	shortTitle,
	seoTitle,
	topics,
	headline = "",
	badgeText = "",
	sublineText = "",
	pose = "neutral",
	intent = "general",
	topicReferenceCount = 0,
	styleProfile = {},
}) {
	const topicFocus = buildTopicFocus({ title, shortTitle, seoTitle, topics });
	const primaryTopic = buildTopicFocus({
		title: "",
		shortTitle: "",
		seoTitle: "",
		topics,
	});
	const artDirection = buildThumbnailArtDirection(intent);
	const styleLine = buildStyleDirectionLine(styleProfile);
	const suggestedHeadline = normalizeWhitespace(headline || "NEW DETAILS");
	const suggestedBadge = normalizeWhitespace(badgeText || "JUST DROPPED");
	const suggestedSubline = normalizeWhitespace(sublineText || primaryTopic);
	const textSafeWidth = THUMBNAIL_TOPIC_PANEL_W - 96;
	const textSafeRight = THUMBNAIL_TOPIC_PANEL_W - 48;
	const topicReferenceLine =
		topicReferenceCount > 0
			? "The left-side topic image is the story reference. You may crop, sharpen, relight, and simplify it, but keep it clearly related to the topic."
			: "No trusted topic image was provided. Use the left side as a symbolic story visual area only: no invented people, no fake cast photos, no celebrity lookalikes, no fabricated portraits, and no human faces.";
	return normalizeWhitespace(`
You are an expert YouTube thumbnail designer creating one complete premium 16:9 thumbnail intended to earn very high click-through.
The input image is a clean draft: topic/feed image on the left, presenter on the right. Use it as the base composition.
Redesign the thumbnail fully: sharpen it, improve contrast, lighting, subject separation, color, text hierarchy, and click appeal.
Keep it clean, modern, editorial, and easy to understand at mobile size. Avoid clutter and avoid too much text.
Do not repeat one fixed template. Use the topic, mood, and source imagery to choose a fresh composition, color accent, sharpness level, lighting style, and visual energy for this specific video.
${styleLine}
Topic direction: ${topicFocus || "current trending story"}.
${artDirection}
${topicReferenceLine}

Creative direction:
- You are not locked to a fixed badge-on-top template. Choose the strongest design for this topic.
- You may use subtle curved panels, angled editorial shapes, tasteful light beams, small topic-appropriate icon-like symbols, depth layers, or accent strokes when they improve clarity.
- If the story is serious, death-related, memorial, legal, or sensitive, keep symbols respectful and minimal. Do not use playful icons or anything that trivializes the topic.
- Avoid generic full-width translucent black bars. Make the text area feel intentionally designed, sharp, and premium.
- If the topic/feed image includes a face, preserve the face and keep readable text away from eyes, mouth, and key identity features. Recompose the image so text lands on clean negative space, not on a face.

Recommended on-image text from the orchestrator:
- Main headline idea: "${suggestedHeadline}"
- Small badge idea: "${suggestedBadge}"
${suggestedSubline ? `- Optional small subject label: "${suggestedSubline}"` : ""}

Typography direction:
- Use bold condensed YouTube-style display lettering, similar to Anton, Bebas Neue, Impact, or a heavy editorial sans.
- Use high-contrast text with clean stroke/shadow/box treatment when needed.
- Use one dominant headline that gives the viewer a fast story promise, not a vague quiz. Prefer concrete hooks like impact, reveal, fallout, reaction, decision, warning, tribute, or what changed.
- Use at most two dominant text elements, plus one tiny subject label only if it improves clarity.
- Keep the total readable text short. Do not add paragraphs, duplicate lines, watermarks, logos, or extra captions.
- You may improve the text wording only if it is shorter, clearer, more clickable, and still truthful to the topic. Avoid boring generic text like "Top Story", "Update", or "New Details" unless the subject label makes the story instantly clear.
- Hard text safe zone: every readable word, badge, label, shadow, stroke, and text box must fit completely inside the left story panel only, from x=48 to x=${textSafeRight}, within roughly ${textSafeWidth}px of usable width.
- Never place text across the vertical divider, over the center seam, on the presenter, or in the right 42% presenter panel.
- Keep the main headline to one or two short lines on the left panel. If needed, make the font smaller rather than letting text run under the presenter.
- Keep all text at least 36px away from every frame edge. No cropped words, no half-visible badges, and no text touching the top, bottom, left, or divider edge.

Presenter guardrails:
- Preserve the presenter identity from the right side and presenter reference: same person, glasses, beard, hairline, facial proportions, skin texture, and outfit feel.
- The presenter must remain on the right side, upper torso visible, camera-facing, same scale and framing as the draft.
- Treat the presenter as locked. Do not change his face, expression, eyebrows, eyes, mouth, glasses, beard, hairline, body position, outfit, or lighting.
- Do not add a reaction to the presenter. Put the click appeal into the composition, color, typography, contrast, and story cue instead.
- Do not distort the face, glasses, beard, hands, anatomy, or clothing.
- Important production note: after your design is generated, the original presenter panel on the right will be composited back over the image to lock the presenter. Anything you put on the right side will be hidden. Keep the right side clean and text-free.

Final result:
- One finished production-ready YouTube thumbnail.
- Super clean, sharp, attractive, premium, and clickable.
- No cropped text, no misspelled text, no extra people, no broken facial features, no muddy lighting.
	`);
}

function buildThumbnailBackgroundPrompt({
	title,
	shortTitle,
	seoTitle,
	topics,
	headline = "",
	badgeText = "",
	sublineText = "",
	intent = "general",
}) {
	const topicFocus = buildTopicFocus({ title, shortTitle, seoTitle, topics });
	return normalizeWhitespace(`
Create a premium 16:9 YouTube thumbnail background scene for a long-form video.
No people, no body parts, no readable incidental text, no logos, and no watermarks.
Leave the right side open enough for a presenter and keep the left side strong enough for a headline.
Topic direction: ${topicFocus || "current trending story"}.
Headline concept: ${headline || "BIG UPDATE"}.
Badge concept: ${badgeText || "TOP STORY"}.
${sublineText ? `Subline concept: ${sublineText}.` : ""}
Keep the scene bright, premium, high-contrast, editorial, and YouTube-friendly.
Intent: ${intent}.
	`);
}

function looksLikeLikelyPersonName(text = "") {
	const tokens = normalizeWhitespace(text)
		.split(/\s+/)
		.filter(Boolean)
		.map((token) => token.replace(/[^A-Za-z'.-]/g, ""))
		.filter(Boolean);
	if (tokens.length < 2 || tokens.length > 4) return false;
	return tokens.every(
		(token) =>
			token.length >= 2 &&
			/^[A-Za-z][A-Za-z'.-]+$/.test(token) &&
			!PERSON_NAME_STOPWORDS.has(token.toLowerCase()),
	);
}

function shouldPreferTopicLeadThumbnail({
	topics = [],
	intent = "general",
	contextText = "",
	topicReferencePaths = [],
}) {
	if (!Array.isArray(topicReferencePaths) || !topicReferencePaths.length)
		return false;
	const primaryTopic = primaryTopicLabel(topics);
	const confidences = (Array.isArray(topics) ? topics : [])
		.map((topic) =>
			normalizeWhitespace(topic?.thumbnailImageConfidence).toLowerCase(),
		)
		.filter(Boolean);
	const weakOnly =
		confidences.length > 0 &&
		!confidences.some((item) => item === "high" || item === "medium");
	if (weakOnly) return false;
	const text = String(contextText || "").toLowerCase();
	return (
		looksLikeLikelyPersonName(primaryTopic) ||
		/\b(actor|actress|singer|rapper|artist|celebrity|star|host|comedian|influencer|player|coach|fighter|quarterback|president|senator|governor)\b/.test(
			text,
		) ||
		intent === "entertainment"
	);
}

function isOpenAiSafetyRejection(error) {
	return /rejected by the safety system/i.test(
		String(error?.message || error || ""),
	);
}

function renderTopicLeadThumbnailPlate({
	jobId,
	tmpDir,
	presenterLocalPath,
	topicReferencePaths = [],
	headline,
	badgeText,
	sublineText,
	accent = ACCENT_PALETTE.default,
	styleProfile = {},
	log,
}) {
	const heroSource = topicReferencePaths[0] || presenterLocalPath;
	ensureImageFile(heroSource, 5000);
	ensureImageFile(presenterLocalPath, 5000);

	const outputPath = path.join(tmpDir, `thumb_plate_${jobId}.jpg`);
	const fontFile = resolveThumbnailFontFile();
	const fontOpt = fontFile ? `:fontfile='${escapeDrawtext(fontFile)}'` : "";
	const accentColor = normalizeAccentColor(accent);
	const tagColor = normalizeAccentColor(styleProfile.tagColor || "0x27305F");
	const lowerPanelOpacity = safeOpacity(styleProfile.lowerPanelOpacity, 0.34);
	const headlineFit = fitThumbnailText(headline || "BIG UPDATE", {
		baseMaxChars: 12,
		maxLines: 2,
		maxChars: 30,
	});
	const sublineFit = fitThumbnailText(sublineText || "", {
		baseMaxChars: 28,
		maxLines: 1,
		maxChars: 42,
	});
	const safeBadge = escapeDrawtext(
		normalizeWhitespace(badgeText || "TOP STORY"),
	);
	const safeSubline = escapeDrawtext(sublineFit.text);
	let headlineFontSize = Math.max(58, Math.round(98 * headlineFit.fontScale));
	const headlineLines = String(headlineFit.text || "")
		.split(/\n+/)
		.map((line) => normalizeWhitespace(line))
		.filter(Boolean)
		.slice(0, 2);
	const headlineX = 58;
	headlineFontSize = fitFontSizeToWidth(headlineLines, headlineFontSize, {
		maxWidth: THUMBNAIL_TOPIC_PANEL_W - headlineX - 54,
		minFontSize: 54,
	});
	const sublineTagFontSize = fitFontSizeToWidth(
		[sublineFit.text],
		Math.max(22, Math.round(34 * sublineFit.fontScale)),
		{
			maxWidth: THUMBNAIL_TOPIC_PANEL_W - 112,
			minFontSize: 21,
		},
	);
	const headlineLineGap = Math.max(8, Math.round(headlineFontSize * 0.1));
	const lowerBoxY = headlineLines.length > 1 ? 344 : 368;
	const headlineStartY = headlineLines.length > 1 ? 406 : 448;
	const badgeY = lowerBoxY + 20;
	const filters = [
		`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.05:saturation=0.92:brightness=-0.015,gblur=sigma=18,setsar=1[bg]`,
		`[1:v]scale=${THUMBNAIL_TOPIC_PANEL_W}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_TOPIC_PANEL_W}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.08:saturation=1.06:brightness=0.015,unsharp=5:5:0.60:5:5:0.0,setsar=1[topic]`,
		`[2:v]scale=${THUMBNAIL_PRESENTER_PANEL_W + 32}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_PRESENTER_PANEL_W}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.05:saturation=1.02,setsar=1[presenter]`,
		`[bg][topic]overlay=0:0[tmp0]`,
		`[tmp0]drawbox=x=0:y=0:w=${THUMBNAIL_TOPIC_PANEL_W}:h=${THUMBNAIL_HEIGHT}:color=black@0.14:t=fill[tmp1]`,
		`[tmp1][presenter]overlay=${THUMBNAIL_TOPIC_PANEL_W}:0[tmp2]`,
		`[tmp2]drawbox=x=${THUMBNAIL_TOPIC_PANEL_W - 8}:y=0:w=8:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.96:t=fill[tmp3]`,
		`[tmp3]drawbox=x=0:y=${lowerBoxY}:w=${THUMBNAIL_TOPIC_PANEL_W}:h=${
			THUMBNAIL_HEIGHT - lowerBoxY
		}:color=black@${lowerPanelOpacity.toFixed(2)}:t=fill[tmp4]`,
		`[tmp4]drawbox=x=0:y=${lowerBoxY}:w=${THUMBNAIL_TOPIC_PANEL_W}:h=3:color=${accentColor}@0.48:t=fill[tmp5]`,
	];
	const textFilters = [];
	if (safeSubline) {
		textFilters.push(
			`drawtext=text='${safeSubline}'${fontOpt}:fontsize=${sublineTagFontSize}:fontcolor=white:x=24:y=8:box=1:boxcolor=${tagColor}@0.92:boxborderw=8:borderw=1:bordercolor=white@0.22:shadowcolor=black@0.35:shadowx=2:shadowy=2`,
		);
	}
	textFilters.push(
		`drawtext=text='${safeBadge}'${fontOpt}:fontsize=31:fontcolor=${accentColor}:x=58:y=${badgeY}:box=1:boxcolor=black@0.72:boxborderw=10:borderw=1:bordercolor=${accentColor}@0.72:shadowcolor=black@0.55:shadowx=2:shadowy=2`,
	);
	for (let i = 0; i < headlineLines.length; i++) {
		textFilters.push(
			`drawtext=text='${escapeDrawtext(
				headlineLines[i],
			)}'${fontOpt}:fontsize=${headlineFontSize}:fontcolor=white:x=${headlineX}:y=${
				headlineStartY + i * (headlineFontSize + headlineLineGap)
			}:borderw=5:bordercolor=black@0.84:shadowcolor=black@0.68:shadowx=4:shadowy=4`,
		);
	}
	filters.push(`[tmp5]${textFilters.join(",")},${THUMBNAIL_FINISH_FILTER}[outv]`);

	runFfmpeg(
		[
			"-i",
			heroSource,
			"-i",
			heroSource,
			"-i",
			presenterLocalPath,
			"-filter_complex",
			filters.join(";"),
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-q:v",
			"1",
			"-y",
			outputPath,
		],
		"thumbnail_topic_lead_compose",
	);
	ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
	if (typeof log === "function") {
		log("thumbnail topic-lead plate ready", {
			path: path.basename(outputPath),
			usesTopicHero: Boolean(topicReferencePaths.length),
			font: fontFile ? path.basename(fontFile) : "default",
		});
	}
	return {
		path: outputPath,
		method: "local_topic_lead",
	};
}

function renderTopicLeadVisualSeed({
	jobId,
	tmpDir,
	presenterLocalPath,
	topicReferencePaths = [],
	accent = ACCENT_PALETTE.default,
	log,
}) {
	const heroSource = topicReferencePaths[0] || presenterLocalPath;
	ensureImageFile(heroSource, 5000);
	ensureImageFile(presenterLocalPath, 5000);

	const outputPath = path.join(tmpDir, `thumb_visual_seed_${jobId}.jpg`);
	const accentColor = normalizeAccentColor(accent);
	const filters = [
		`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.05:saturation=0.92:brightness=-0.015,gblur=sigma=18,setsar=1[bg]`,
		`[1:v]scale=${THUMBNAIL_TOPIC_PANEL_W}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_TOPIC_PANEL_W}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.08:saturation=1.06:brightness=0.015,unsharp=5:5:0.60:5:5:0.0,setsar=1[topic]`,
		`[2:v]scale=${THUMBNAIL_PRESENTER_PANEL_W + 32}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_PRESENTER_PANEL_W}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.05:saturation=1.02,setsar=1[presenter]`,
		`[bg][topic]overlay=0:0[tmp0]`,
		`[tmp0]drawbox=x=0:y=0:w=${THUMBNAIL_TOPIC_PANEL_W}:h=${THUMBNAIL_HEIGHT}:color=black@0.08:t=fill[tmp1]`,
		`[tmp1][presenter]overlay=${THUMBNAIL_TOPIC_PANEL_W}:0[tmp2]`,
		`[tmp2]drawbox=x=${THUMBNAIL_TOPIC_PANEL_W - 8}:y=0:w=8:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.96:t=fill[outv]`,
	];
	runFfmpeg(
		[
			"-i",
			heroSource,
			"-i",
			heroSource,
			"-i",
			presenterLocalPath,
			"-filter_complex",
			filters.join(";"),
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-q:v",
			"1",
			"-y",
			outputPath,
		],
		"thumbnail_visual_seed_compose",
	);
	ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
	if (typeof log === "function") {
		log("thumbnail visual seed ready", {
			path: path.basename(outputPath),
			usesTopicHero: Boolean(topicReferencePaths.length),
		});
	}
	return {
		path: outputPath,
		method: "local_visual_seed",
	};
}

function renderLockedThumbnailTextOverlay({
	jobId,
	tmpDir,
	basePath,
	headline,
	badgeText,
	sublineText,
	accent = ACCENT_PALETTE.default,
	styleProfile = {},
	log,
}) {
	ensureImageFile(basePath, 5000);
	const outputPath = path.join(tmpDir, `thumb_plate_${jobId}.jpg`);
	const fontFile = resolveThumbnailFontFile();
	const fontOpt = fontFile ? `:fontfile='${escapeDrawtext(fontFile)}'` : "";
	const accentColor = normalizeAccentColor(accent);
	const tagColor = normalizeAccentColor(styleProfile.tagColor || "0x27305F");
	const layout = chooseLockedTextOverlayLayout(styleProfile);
	const panelOpacity = safeOpacity(
		styleProfile.textPanelOpacity ?? layout.panelOpacity,
		layout.panelOpacity,
	);
	const badgeFit = fitThumbnailText(badgeText || "TOP STORY", {
		baseMaxChars: 16,
		maxLines: 1,
		maxChars: 20,
	});
	const headlineFit = fitThumbnailText(headline || "BIG UPDATE", {
		baseMaxChars: 12,
		maxLines: 2,
		maxChars: 30,
	});
	const sublineFit = fitThumbnailText(sublineText || "", {
		baseMaxChars: 18,
		maxLines: 1,
		maxChars: 24,
	});
	const safeBadge = escapeDrawtext(badgeFit.text || "TOP STORY");
	const safeSubline = escapeDrawtext(sublineFit.text);
	const badgeFontSize = fitFontSizeToWidth(
		[badgeFit.text || "TOP STORY"],
		32,
		{
			maxWidth: layout.maxTextWidth,
			minFontSize: 22,
		},
	);
	const headlineLines = String(headlineFit.text || "")
		.split(/\n+/)
		.map((line) => normalizeWhitespace(line))
		.filter(Boolean)
		.slice(0, 2);
	let headlineFontSize = Math.max(
		56,
		Math.round((headlineLines.length > 1 ? 86 : 96) * headlineFit.fontScale),
	);
	headlineFontSize = fitFontSizeToWidth(headlineLines, headlineFontSize, {
		maxWidth: layout.maxTextWidth,
		minFontSize: 52,
	});
	const sublineTagFontSize = fitFontSizeToWidth(
		[sublineFit.text],
		Math.max(20, Math.round(30 * sublineFit.fontScale)),
		{
			maxWidth: layout.maxTextWidth,
			minFontSize: 19,
		},
	);
	const headlineLineGap = Math.max(8, Math.round(headlineFontSize * 0.1));
	const headlineStartY =
		headlineLines.length > 1 ? layout.headlineY - 28 : layout.headlineY;
	const chainFilters = [
		`scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos`,
		`crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2`,
		"setsar=1",
		`drawbox=x=${layout.panelX}:y=${layout.panelY}:w=${layout.panelW}:h=${layout.panelH}:color=black@${panelOpacity.toFixed(2)}:t=fill`,
		`drawbox=x=${layout.panelX}:y=${layout.panelY}:w=${layout.panelW}:h=2:color=${accentColor}@0.70:t=fill`,
		`drawbox=x=${layout.panelX}:y=${layout.panelY + layout.panelH - 2}:w=${layout.panelW}:h=2:color=white@0.16:t=fill`,
	];
	if (layout.accentRail) {
		chainFilters.push(
			`drawbox=x=${layout.panelX}:y=${layout.panelY}:w=8:h=${layout.panelH}:color=${accentColor}@0.92:t=fill`,
			`drawbox=x=${layout.panelX + 20}:y=${layout.panelY + 20}:w=46:h=5:color=${accentColor}@0.78:t=fill`,
			`drawbox=x=${layout.panelX + 20}:y=${layout.panelY + 32}:w=24:h=5:color=white@0.32:t=fill`,
		);
	}
	if (safeSubline) {
		chainFilters.push(
			`drawtext=text='${safeSubline}'${fontOpt}:fontsize=${sublineTagFontSize}:fontcolor=white:x=${
				layout.panelX + 78
			}:y=${layout.panelY + 15}:box=1:boxcolor=${tagColor}@0.78:boxborderw=7:borderw=1:bordercolor=white@0.18:shadowcolor=black@0.35:shadowx=2:shadowy=2`,
		);
	}
	const badgeBox = layout.badgeBox
		? `:box=1:boxcolor=black@0.56:boxborderw=8:borderw=1:bordercolor=${accentColor}@0.62`
		: ":borderw=2:bordercolor=black@0.78";
	chainFilters.push(
		`drawtext=text='${safeBadge}'${fontOpt}:fontsize=${badgeFontSize}:fontcolor=${accentColor}:x=${layout.badgeX}:y=${layout.badgeY}${badgeBox}:shadowcolor=black@0.55:shadowx=2:shadowy=2`,
	);
	for (let i = 0; i < headlineLines.length; i++) {
		chainFilters.push(
			`drawtext=text='${escapeDrawtext(
				headlineLines[i],
			)}'${fontOpt}:fontsize=${headlineFontSize}:fontcolor=white:x=${layout.headlineX}:y=${
				headlineStartY + i * (headlineFontSize + headlineLineGap)
			}:borderw=5:bordercolor=black@0.84:shadowcolor=black@0.68:shadowx=4:shadowy=4`,
		);
	}
	chainFilters.push(
		"drawbox=x=0:y=0:w=iw:h=ih:color=white@0.08:t=2",
		THUMBNAIL_FINISH_FILTER,
	);
	const filters = [`[0:v]${chainFilters.join(",")}[outv]`];
	runFfmpeg(
		[
			"-i",
			basePath,
			"-filter_complex",
			filters.join(";"),
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-q:v",
			"1",
			"-y",
			outputPath,
		],
		"thumbnail_locked_text_overlay",
	);
	ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
	if (typeof log === "function") {
		log("thumbnail locked text overlay ready", {
			path: path.basename(outputPath),
			headline: headlineFit.text,
			badgeText: normalizeWhitespace(badgeText || "TOP STORY"),
			subline: sublineFit.text || null,
			layout: layout.id,
			font: fontFile ? path.basename(fontFile) : "default",
		});
	}
	return {
		path: outputPath,
		method: "openai_edit_text_locked",
	};
}

function renderSeedCompositeFallback({
	jobId,
	tmpDir,
	presenterLocalPath,
	topicReferencePaths = [],
	log,
}) {
	const seedBasePath = composeThumbnailSeedBase({
		jobId,
		tmpDir,
		presenterLocalPath,
		topicReferencePaths,
	});
	const outputPath = path.join(tmpDir, `thumb_seed_fallback_${jobId}.jpg`);
	try {
		normalizeThumbnailOutput({
			candidatePath: seedBasePath,
			outPath: outputPath,
		});
		ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
		if (typeof log === "function") {
			log("thumbnail seed fallback ready", {
				path: path.basename(outputPath),
			});
		}
		return {
			path: outputPath,
			method: "seed_composite_fallback",
		};
	} finally {
		safeUnlink(seedBasePath);
	}
}

function composeThumbnailSeedBase({
	jobId,
	tmpDir,
	presenterLocalPath,
	topicReferencePaths = [],
}) {
	ensureImageFile(presenterLocalPath, 5000);
	const backgroundSource = topicReferencePaths[0] || presenterLocalPath;
	ensureImageFile(backgroundSource, 5000);
	const outputPath = path.join(tmpDir, `thumb_seed_base_${jobId}.png`);
	runFfmpeg(
		[
			"-i",
			backgroundSource,
			"-i",
			presenterLocalPath,
			"-filter_complex",
			[
				`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,` +
					`crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1,` +
					`boxblur=18:2,eq=contrast=1.02:saturation=0.92:brightness=0.02:gamma=0.98,format=rgba[bg0]`,
				`[bg0]drawbox=x=0:y=0:w=iw*${DEFAULT_LEFT_TEXT_PCT}:h=ih:color=black@0.10:t=fill[bg1]`,
				`[1:v]scale=654:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,` +
					`crop=654:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1,format=rgba[presenter]`,
				`[bg1][presenter]overlay=${THUMBNAIL_WIDTH - 654}:0[outv]`,
			].join(";"),
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-y",
			outputPath,
		],
		"thumbnail_seed_compose",
	);
	ensureImageFile(outputPath, 5000);
	return outputPath;
}

function normalizeThumbnailOutput({ candidatePath, outPath }) {
	runFfmpeg(
		[
			"-i",
			candidatePath,
			"-vf",
			`scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,` +
				`crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1`,
			"-frames:v",
			"1",
			"-q:v",
			"1",
			"-y",
			outPath,
		],
		"thumbnail_normalize",
	);
	return outPath;
}

function lockPresenterPanel({
	jobId,
	tmpDir,
	basePath,
	presenterLocalPath,
	label = "presenter_locked",
	log,
}) {
	ensureImageFile(basePath, 5000);
	ensureImageFile(presenterLocalPath, 5000);
	const outputPath = path.join(
		tmpDir,
		`thumb_${safeSlug(label, 24)}_${jobId}.jpg`,
	);
	runFfmpeg(
		[
			"-i",
			basePath,
			"-i",
			presenterLocalPath,
			"-filter_complex",
			[
				`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,` +
					`crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1[base]`,
				`[1:v]scale=${THUMBNAIL_PRESENTER_PANEL_W + 32}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,` +
					`crop=${THUMBNAIL_PRESENTER_PANEL_W}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1[presenter]`,
				`[base][presenter]overlay=${THUMBNAIL_TOPIC_PANEL_W}:0[outv]`,
			].join(";"),
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-q:v",
			"1",
			"-y",
			outputPath,
		],
		"thumbnail_presenter_lock",
	);
	ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
	if (typeof log === "function") {
		log("thumbnail presenter locked", {
			path: path.basename(outputPath),
			label,
		});
	}
	return outputPath;
}

function assertCloudinaryReady() {
	if (
		!process.env.CLOUDINARY_CLOUD_NAME ||
		!process.env.CLOUDINARY_API_KEY ||
		!process.env.CLOUDINARY_API_SECRET
	) {
		throw new Error(
			"Cloudinary credentials missing (CLOUDINARY_CLOUD_NAME/API_KEY/API_SECRET).",
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

async function generateOpenAiDesignerThumbnailPlate({
	jobId,
	tmpDir,
	presenterLocalPath,
	topicReferencePaths = [],
	title,
	shortTitle,
	seoTitle,
	topics,
	headline,
	badgeText,
	sublineText,
	pose,
	intent,
	styleProfile = {},
	log,
}) {
	const contextText = buildContextText({ title, shortTitle, seoTitle, topics });
	const effectiveStyleProfile =
		styleProfile && styleProfile.id
			? styleProfile
			: chooseThumbnailStyleProfile(intent, contextText);
	const accent =
		effectiveStyleProfile.accent ||
		chooseThumbnailStyleProfile(intent, contextText).accent;
	const prompt = buildThumbnailDesignerPrompt({
		title,
		shortTitle,
		seoTitle,
		topics,
		headline,
		badgeText,
		sublineText,
		pose,
		intent,
		topicReferenceCount: topicReferencePaths.length,
		styleProfile: effectiveStyleProfile,
	});
	const visualSeed = renderTopicLeadVisualSeed({
		jobId: `${jobId || "thumbnail"}_designer_seed`,
		tmpDir,
		presenterLocalPath,
		topicReferencePaths,
		accent,
		log,
	});
	const rawPath = path.join(tmpDir, `thumb_designer_raw_${jobId}.png`);
	const outputPath = path.join(tmpDir, `thumb_designer_${jobId}.jpg`);
	let lockedPath = "";
	if (typeof log === "function") {
		log("thumbnail openai designer seed ready", {
			path: path.basename(visualSeed.path || ""),
			topicReferenceCount: topicReferencePaths.length,
		});
		log("thumbnail openai designer prompt", {
			attempt: 1,
			pose,
			headline,
			badgeText,
			sublineText: sublineText || null,
			prompt: prompt.slice(0, 260),
		});
	}
	try {
		const imagePaths = [
			visualSeed.path,
			presenterLocalPath,
			...topicReferencePaths.slice(0, 1),
		].filter(Boolean);
		await editImageToPath({
			prompt,
			imagePaths,
			outPath: rawPath,
			size: pickOpenAIImageSize({
				width: THUMBNAIL_WIDTH,
				height: THUMBNAIL_HEIGHT,
				preferLandscape: true,
			}),
			quality: "high",
			background: "opaque",
			inputFidelity: "high",
			user: `${jobId || "thumbnail"}_designer_1`,
		});
		normalizeThumbnailOutput({
			candidatePath: rawPath,
			outPath: outputPath,
		});
		ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
		lockedPath = lockPresenterPanel({
			jobId,
			tmpDir,
			basePath: outputPath,
			presenterLocalPath,
			label: "designer_presenter_locked",
			log,
		});
		if (typeof log === "function") {
			log("thumbnail openai designer ready", {
				attempt: 1,
				path: path.basename(lockedPath),
			});
		}
		return {
			path: lockedPath,
			method: "openai_full_designer",
		};
	} finally {
		safeUnlink(visualSeed.path);
		safeUnlink(rawPath);
		safeUnlink(outputPath);
	}
}

async function generateOpenAiThumbnailPlate({
	jobId,
	tmpDir,
	presenterLocalPath,
	topicReferencePaths = [],
	title,
	shortTitle,
	seoTitle,
	topics,
	headline,
	badgeText,
	sublineText,
	pose,
	intent,
	styleProfile = {},
	log,
}) {
	const contextText = buildContextText({ title, shortTitle, seoTitle, topics });
	const effectiveStyleProfile =
		styleProfile && styleProfile.id
			? styleProfile
			: chooseThumbnailStyleProfile(intent, contextText);
	const prompt = buildThumbnailPrompt({
		title,
		shortTitle,
		seoTitle,
		topics,
		headline,
		badgeText,
		sublineText,
		pose,
		intent,
		topicReferenceCount: topicReferencePaths.length,
		styleProfile: effectiveStyleProfile,
	});
	const accent = effectiveStyleProfile.accent || chooseAccentColor(intent, contextText);
	const visualSeed = renderTopicLeadVisualSeed({
		jobId: `${jobId || "thumbnail"}_openai_seed`,
		tmpDir,
		presenterLocalPath,
		topicReferencePaths,
		accent,
		log,
	});
	const rawPath = path.join(tmpDir, `thumb_plate_raw_${jobId}.png`);
	const polishedPath = path.join(tmpDir, `thumb_polished_${jobId}.jpg`);
	let lockedPolishedPath = "";
	if (typeof log === "function") {
		log("thumbnail openai visual seed ready", {
			path: path.basename(visualSeed.path || ""),
			topicReferenceCount: topicReferencePaths.length,
		});
		log("thumbnail openai plate prompt", {
			attempt: 1,
			pose,
			prompt: prompt.slice(0, 240),
		});
	}
	try {
		const imagePaths = [
			visualSeed.path,
			presenterLocalPath,
			...topicReferencePaths.slice(0, 1),
		].filter(Boolean);
		await editImageToPath({
			prompt,
			imagePaths,
			outPath: rawPath,
			size: pickOpenAIImageSize({
				width: THUMBNAIL_WIDTH,
				height: THUMBNAIL_HEIGHT,
				preferLandscape: true,
			}),
			quality: "high",
			background: "opaque",
			inputFidelity: "high",
			user: `${jobId || "thumbnail"}_plate_1`,
		});
		normalizeThumbnailOutput({
			candidatePath: rawPath,
			outPath: polishedPath,
		});
		ensureThumbnailFile(polishedPath, THUMBNAIL_MIN_BYTES);
		if (typeof log === "function") {
			log("thumbnail openai plate ready", {
				attempt: 1,
				path: path.basename(polishedPath),
			});
		}
		lockedPolishedPath = lockPresenterPanel({
			jobId,
			tmpDir,
			basePath: polishedPath,
			presenterLocalPath,
			label: "polished_presenter_locked",
			log,
		});
		return renderLockedThumbnailTextOverlay({
			jobId,
			tmpDir,
			basePath: lockedPolishedPath,
			headline,
			badgeText,
			sublineText,
			accent,
			styleProfile: effectiveStyleProfile,
			log,
		});
	} finally {
		safeUnlink(visualSeed.path);
		safeUnlink(rawPath);
		safeUnlink(polishedPath);
		safeUnlink(lockedPolishedPath);
	}
}

async function generateThumbnailCompositeBase({
	jobId,
	tmpDir,
	presenterLocalPath,
	topics = [],
	log,
}) {
	const topicReferencePaths = await collectTopicReferenceImages({
		topics,
		tmpDir,
		jobId,
		log,
	});
	return composeThumbnailSeedBase({
		jobId,
		tmpDir,
		presenterLocalPath,
		topicReferencePaths,
	});
}

async function generateThumbnailPackage({
	jobId,
	tmpDir,
	presenterLocalPath,
	title,
	shortTitle,
	seoTitle,
	topics = [],
	expression = "neutral",
	log,
	overrideHeadline,
	overrideBadgeText,
	overrideIntent,
}) {
	if (!presenterLocalPath) {
		throw new Error("thumbnail_presenter_missing_or_invalid");
	}
	ensureDir(tmpDir);
	ensureImageFile(presenterLocalPath, 5000);
	assertCloudinaryReady();

	const contextText = buildContextText({ title, shortTitle, seoTitle, topics });
	const intent =
		normalizeWhitespace(overrideIntent) ||
		inferThumbnailIntent({ title, shortTitle, seoTitle, topics });
	const textPlan = buildThumbnailTextPlan({
		title,
		shortTitle,
		seoTitle,
		topics,
		intent,
		overrideHeadline,
		overrideBadgeText,
	});
	const styleContextText = normalizeWhitespace(
		`${contextText} ${textPlan.primaryHeadline} ${textPlan.badgeText} ${
			textPlan.sublineText || ""
		}`,
	);
	const pose = chooseThumbnailPose({
		expression,
		intent,
		contextText: styleContextText,
	});
	const styleProfile = chooseThumbnailStyleProfile(intent, styleContextText);
	const accent = styleProfile.accent || chooseAccentColor(intent, styleContextText);

	if (typeof log === "function") {
		log("thumbnail plan", {
			intent,
			pose,
			headline: textPlan.primaryHeadline,
			badgeText: textPlan.badgeText,
			subline: textPlan.sublineText || null,
			style: styleProfile.id,
		});
	}

	const topicReferencePaths = await collectTopicReferenceImages({
		topics,
		tmpDir,
		jobId,
		log,
	});
	const preferTopicLead = shouldPreferTopicLeadThumbnail({
		topics,
		intent,
		contextText,
		topicReferencePaths,
	});
	if (typeof log === "function") {
		log("thumbnail route selected", {
			primaryRoute: "openai_edit_text_locked",
			fallbackRoutes:
				preferTopicLead || topicReferencePaths.length
					? ["topic_lead_local", "seed_fallback"]
					: ["seed_fallback"],
			preferTopicLead,
			intent,
			topicReferenceCount: topicReferencePaths.length,
			primaryTopic: primaryTopicLabel(topics),
		});
	}

	const sharedThumbArgs = {
		jobId,
		tmpDir,
		presenterLocalPath,
		topicReferencePaths,
		title,
		shortTitle,
		seoTitle,
		topics,
		headline: textPlan.primaryHeadline,
		badgeText: textPlan.badgeText,
		sublineText: textPlan.sublineText,
		pose,
		intent,
		styleProfile,
		log,
	};

	let plate = null;
	let firstError = null;

	const tryTopicLead = () =>
		renderTopicLeadThumbnailPlate({
			jobId,
			tmpDir,
			presenterLocalPath,
			topicReferencePaths,
			headline: textPlan.primaryHeadline,
			badgeText: textPlan.badgeText,
			sublineText: textPlan.sublineText,
			accent,
			styleProfile,
			log,
		});

	const tryOpenAi = () => generateOpenAiThumbnailPlate(sharedThumbArgs);

	const trySeedFallback = () =>
		renderSeedCompositeFallback({
			jobId,
			tmpDir,
			presenterLocalPath,
			topicReferencePaths,
			log,
		});

	const topicLeadStep = { name: "topic_lead_local", run: tryTopicLead };
	const openAiStep = { name: "openai_edit_text_locked", run: tryOpenAi };
	const seedStep = { name: "seed_fallback", run: trySeedFallback };
	const route =
		preferTopicLead || topicReferencePaths.length
			? [openAiStep, topicLeadStep, seedStep]
			: [openAiStep, seedStep];

	for (const step of route) {
		try {
			plate = await step.run();
			if (plate) {
				if (typeof log === "function") {
					log("thumbnail route succeeded", {
						strategy: step.name,
						method: plate.method || step.name,
						path: path.basename(plate.path || ""),
					});
				}
				break;
			}
		} catch (error) {
			if (!firstError) firstError = error;
			if (typeof log === "function") {
				log("thumbnail route failed", {
					strategy: step.name,
					error: error?.message || String(error),
					safetyRejected: isOpenAiSafetyRejection(error),
				});
			}
		}
	}

	if (!plate?.path) {
		throw firstError || new Error("thumbnail_generation_failed");
	}

	const uploaded = await uploadThumbnailToCloudinary(plate.path, jobId);

	const variant = {
		variant: plate.method === "local_topic_lead" ? "topic_lead" : "a",
		localPath: plate.path,
		url: uploaded.url || "",
		publicId: uploaded.public_id || "",
		width: uploaded.width || THUMBNAIL_WIDTH,
		height: uploaded.height || THUMBNAIL_HEIGHT,
		title: textPlan.primaryHeadline,
		method: plate.method || "unknown",
	};

	return {
		localPath: plate.path,
		url: uploaded.url || "",
		publicId: uploaded.public_id || "",
		width: uploaded.width || THUMBNAIL_WIDTH,
		height: uploaded.height || THUMBNAIL_HEIGHT,
		title: textPlan.primaryHeadline,
		pose,
		accent,
		style: styleProfile.id,
		method: plate.method || "unknown",
		variants: [variant],
	};
}

module.exports = {
	buildThumbnailPrompt,
	buildRunwayThumbnailPrompt: buildThumbnailBackgroundPrompt,
	buildThumbnailBackgroundPrompt,
	generateThumbnailCompositeBase,
	generateThumbnailPackage,
};
