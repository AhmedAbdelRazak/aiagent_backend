/** @format */

require("dotenv").config();

const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");
const axios = require("axios");

const PRESENTER_MIN_BYTES = 12000;
const PRESENTER_METHOD = "approved_cloudinary_presenter_library";
const RECENT_OUTFIT_HARD_BLOCK_COUNT = 1;
const RECENT_OUTFIT_SOFT_BLOCK_COUNT = 6;

const APPROVED_PRESENTER_LIBRARY = [
	{
		id: "entertainment_black_satin_bomber_black_crew",
		label: "black satin-leather bomber with black crew shirt",
		description:
			"Glossy black satin-leather bomber jacket over a black crew shirt; premium entertainment-host look.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1779317268/aivideomatic/long_presenters/presenter_master_9ca36222-ef4e-4737-b1e0-82f98e7bca07_1779317267532.png",
		modes: ["entertainment", "modern"],
		keywords: ["movie", "music", "celebrity", "culture", "creator", "launch"],
	},
	{
		id: "formal_black_blazer_black_button_shirt",
		label: "black blazer with black button-up shirt",
		description:
			"Matte black blazer layered over a black button-up shirt; serious, polished, studio-ready.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1779316470/aivideomatic/long_presenters/presenter_master_cc62c6a4-099d-40c6-a786-0d09574024e8_1779316469701.png",
		modes: ["formal", "business", "modern"],
		keywords: ["court", "policy", "finance", "business", "election", "government"],
	},
	{
		id: "modern_black_overshirt_black_mockneck",
		label: "black overshirt with black mock-neck layer",
		description:
			"Structured black overshirt with flap pockets over a dark mock-neck layer; modern tech-presenter look.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1779316018/aivideomatic/long_presenters/presenter_master_8510ccda-d7fe-4ec1-aefe-4d2fd8ae011f_1779316017879.png",
		modes: ["modern", "tech", "formal"],
		keywords: ["ai", "tech", "software", "startup", "product", "science"],
	},
	{
		id: "entertainment_black_silk_open_collar",
		label: "black silk-blend open-collar shirt",
		description:
			"Black silk-blend open-collar shirt with subtle sheen; stylish but still classy and covered.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1779311766/aivideomatic/long_presenters/presenter_master_fa5ed73c-4890-4e21-aa8a-e1b54037c43a_1779311765352.png",
		modes: ["entertainment", "modern"],
		keywords: ["entertainment", "celebrity", "film", "music", "style", "host"],
	},
	{
		id: "formal_black_blazer_black_crew",
		label: "black blazer with black crew shirt",
		description:
			"Soft black blazer over a plain black crew shirt; refined, calm, and versatile for serious stories.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1779203075/aivideomatic/long_presenters/presenter_master_61ad1912-eb25-42e0-9df3-f9aaa4a4e023_1779203074540.png",
		modes: ["formal", "modern", "entertainment"],
		keywords: ["analysis", "business", "news", "crime", "investigation", "serious"],
	},
	{
		id: "approachable_charcoal_crew_sweater",
		label: "charcoal crew-neck sweater",
		description:
			"Charcoal crew-neck sweater with relaxed fit; warm, approachable, and conversational.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1778883528/aivideomatic/long_presenters/presenter_master_a6bed5b8-eb3b-431a-9770-96672b2f7dd5_1778883527282.png",
		modes: ["approachable", "modern"],
		keywords: ["friendship", "money stress", "budget", "life", "relationships", "health"],
	},
	{
		id: "approachable_heather_gray_tee",
		label: "heather gray crew-neck t-shirt",
		description:
			"Heather gray crew-neck t-shirt; simple, casual, relatable, and low-pressure.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1778869316/aivideomatic/long_presenters/presenter_master_08b2b7c0-ce6e-4564-8427-149e9c7697da_1778869316077.png",
		modes: ["approachable"],
		keywords: ["personal", "everyday", "rent", "groceries", "friends", "stress"],
	},
	{
		id: "approachable_dark_charcoal_long_sleeve",
		label: "dark charcoal long-sleeve crew shirt",
		description:
			"Dark charcoal long-sleeve crew shirt; understated, friendly, and clean on camera.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1778867413/aivideomatic/long_presenters/presenter_master_8f489318-0564-4cb1-aa5d-e65f013fb38a_1778867413057.png",
		modes: ["approachable", "modern"],
		keywords: ["family", "friendship", "social", "wellness", "budget", "work"],
	},
	{
		id: "formal_black_three_piece_suit_tie",
		label: "black three-piece suit with white shirt and black tie",
		description:
			"Black three-piece suit, white dress shirt, black tie, and pocket square; highest-formality news look.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1777061349/aivideomatic/long_presenters/presenter_master_3997d103-d886-4695-bb8e-b1dd8e0c6ba3_1777061346365.png",
		modes: ["formal"],
		keywords: ["court", "trial", "war", "diplomacy", "government", "death", "crisis"],
	},
	{
		id: "modern_black_structured_overshirt",
		label: "black structured overshirt with dark shirt",
		description:
			"Black structured overshirt or shirt-jacket over a dark shirt; sharp, minimal, and contemporary.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1777055600/aivideomatic/long_presenters/presenter_master_eec5df3c-d695-41bf-a682-801c5568bf7b_1777055599516.png",
		modes: ["modern", "formal", "tech"],
		keywords: ["ai", "tech", "startup", "sports", "analysis", "product"],
	},
	{
		id: "approachable_charcoal_short_sleeve_button_shirt",
		label: "charcoal short-sleeve button shirt",
		description:
			"Charcoal short-sleeve button shirt with chest pocket; approachable, grounded, and neat.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1769012778/aivideomatic/long_presenters/presenter_master_38aeb3b8-bef6-4065-97e1-39f96964a5ce_1769012777889.png",
		modes: ["approachable", "entertainment"],
		keywords: ["social", "community", "friends", "everyday", "local", "culture"],
	},
	{
		id: "approachable_deep_green_overshirt",
		label: "deep green overshirt with button front",
		description:
			"Deep green overshirt with button front and chest pocket; relaxed but polished.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1768197655/aivideomatic/long_presenters/presenter_master_eae39d0a-ca89-49ca-aa82-604cfdbd3b24_1768197654677.png",
		modes: ["approachable", "modern"],
		keywords: ["environment", "health", "lifestyle", "community", "work", "money"],
	},
	{
		id: "approachable_graphite_open_collar_short_sleeve",
		label: "graphite open-collar short-sleeve shirt",
		description:
			"Graphite short-sleeve open-collar shirt; casual, neat, and friendly without looking plain.",
		url: "https://res.cloudinary.com/infiniteapps/image/upload/v1768196633/aivideomatic/long_presenters/presenter_master_27257249-20bf-428e-b434-456398133e59_1768196632943.png",
		modes: ["approachable", "entertainment"],
		keywords: ["relationships", "social", "culture", "lifestyle", "friends", "personal"],
	},
];

function normalizeWhitespace(value = "") {
	return String(value || "").replace(/\s+/g, " ").trim();
}

function createLogger(log, jobId) {
	const prefix = `[presenter_adjustments2${jobId ? `:${jobId}` : ""}]`;
	return (message, data = null) => {
		try {
			if (typeof log === "function") log(message, data || {});
		} catch {}
		try {
			if (data && Object.keys(data).length) console.log(prefix, message, data);
			else console.log(prefix, message);
		} catch {}
	};
}

function ensureDir(dirPath) {
	if (dirPath) fs.mkdirSync(dirPath, { recursive: true });
}

function readFileHeader(filePath, bytes = 65536) {
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

function detectImageType(filePath) {
	const head = readFileHeader(filePath, 12);
	if (!head || head.length < 4) return null;
	if (
		head[0] === 0x89 &&
		head[1] === 0x50 &&
		head[2] === 0x4e &&
		head[3] === 0x47
	) {
		return "png";
	}
	if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff) return "jpg";
	if (
		head.toString("ascii", 0, 4) === "RIFF" &&
		head.toString("ascii", 8, 12) === "WEBP"
	) {
		return "webp";
	}
	return null;
}

function parsePngSize(buf) {
	if (!buf || buf.length < 24) return null;
	if (buf[0] !== 0x89 || buf[1] !== 0x50 || buf[2] !== 0x4e || buf[3] !== 0x47)
		return null;
	return {
		width: buf.readUInt32BE(16),
		height: buf.readUInt32BE(20),
	};
}

function parseJpegSize(buf) {
	if (!buf || buf.length < 4) return null;
	if (buf[0] !== 0xff || buf[1] !== 0xd8) return null;
	let offset = 2;
	while (offset + 1 < buf.length) {
		if (buf[offset] !== 0xff) {
			offset += 1;
			continue;
		}
		while (buf[offset] === 0xff) offset += 1;
		const marker = buf[offset];
		offset += 1;
		if (marker === 0xd9 || marker === 0xda) break;
		if (offset + 1 >= buf.length) break;
		const length = buf.readUInt16BE(offset);
		if (length < 2) break;
		if (
			(marker >= 0xc0 && marker <= 0xc3) ||
			(marker >= 0xc5 && marker <= 0xc7) ||
			(marker >= 0xc9 && marker <= 0xcb) ||
			(marker >= 0xcd && marker <= 0xcf)
		) {
			if (offset + 7 >= buf.length) break;
			return {
				width: buf.readUInt16BE(offset + 5),
				height: buf.readUInt16BE(offset + 3),
			};
		}
		offset += length;
	}
	return null;
}

function getImageDimensions(filePath) {
	const head = readFileHeader(filePath);
	if (!head) return { width: 0, height: 0 };
	return parsePngSize(head) || parseJpegSize(head) || { width: 0, height: 0 };
}

function ensurePresenterFile(filePath) {
	if (!filePath || !fs.existsSync(filePath)) {
		throw new Error("presenter_image_missing");
	}
	const stat = fs.statSync(filePath);
	if (!stat || stat.size < PRESENTER_MIN_BYTES) {
		throw new Error("presenter_image_too_small");
	}
	if (!detectImageType(filePath)) {
		throw new Error("presenter_image_invalid");
	}
	return filePath;
}

function buildOriginalFallback(presenterLocalPath) {
	return {
		localPath: presenterLocalPath,
		url: "",
		publicId: "",
		width: 0,
		height: 0,
		method: "strict_fallback_original",
		presenterOutfit: "",
		presenterOutfitStyle: "",
	};
}

function buildTopicContext({ title, topics = [], categoryLabel = "" }) {
	const topicText = Array.isArray(topics)
		? topics
				.map((topic) =>
					[
						topic?.displayTopic,
						topic?.topic,
						topic?.angle,
						topic?.reason,
						topic?.promptText,
						topic?.promptBrief?.title,
						topic?.promptBrief?.primaryTopic,
					]
						.filter(Boolean)
						.join(" "),
				)
				.filter(Boolean)
				.join(" ")
		: "";
	return `${title || ""} ${topicText} ${categoryLabel || ""}`
		.replace(/\s+/g, " ")
		.trim()
		.toLowerCase();
}

function inferPresentationMode({ title, topics, categoryLabel }) {
	const context = buildTopicContext({ title, topics, categoryLabel });
	if (
		/\b(broke|paycheck|paycheque|rent|renter|renters|grocer(?:y|ies)|bills?|subscriptions?|automatic withdrawals?|inflation|cost of living|living paycheck|financial stress|money stress|budget|savings?|debt|fixed costs?|feel behind|making friends|make friends|adult friendship|friendships?|friends feels|loneliness|lonely|social connection|social isolation|belonging|community|relationships?|dating|family pressure|reach out|text first|group chat|wellness|lifestyle)\b/.test(
			context,
		)
	) {
		return "approachable";
	}
	if (
		/\b(arrest|court|trial|lawsuit|legal|investigation|crime|murder|victim|charged|hearing|government|policy|finance|business|election|president|prime minister|congress|senate|parliament|governor|white house|supreme court|iran|israel|hezbollah|hamas|gaza|ukraine|russia|china|taiwan|middle east|peace proposal|peace talks|ceasefire|diplomacy|diplomatic|sanctions|foreign minister|state department|united nations|health|public health|world health organization|disease|outbreak|infection|infected|illness|hospital|symptom|transmission|pandemic|epidemic|vaccine|cdc|case count|contact tracing|mortality|treatment)\b/.test(
			context,
		) ||
		/\b[a-z0-9-]*virus\b/.test(context)
	) {
		return "formal";
	}
	if (
		/\b(sports?|game|matchup|playoff|tournament|tech|software|startup|product|ai|artificial intelligence|science|space|device|app)\b/.test(
			context,
		)
	) {
		return "modern";
	}
	return "entertainment";
}

function normalizeOutfitKey(value = "") {
	return String(value || "")
		.trim()
		.toLowerCase()
		.replace(/https?:\/\/[^/]+\/image\/upload\/(?:v\d+\/)?/g, "")
		.replace(/\.[a-z0-9]+$/i, "")
		.replace(/[^a-z0-9]+/g, "_")
		.replace(/^_+|_+$/g, "");
}

function inferOutfitModeFromValue(value = "") {
	const key = normalizeOutfitKey(value);
	if (!key) return "";
	if (key.startsWith("approachable_") || /\bapproachable\b/.test(key))
		return "approachable";
	if (key.startsWith("formal_") || /\bformal\b/.test(key)) return "formal";
	if (
		key.startsWith("modern_") ||
		key.startsWith("tech_") ||
		/\b(modern|tech)\b/.test(key)
	) {
		return "modern";
	}
	if (
		key.startsWith("entertainment_") ||
		/\b(entertainment|host|satin|silk|bomber)\b/.test(key)
	) {
		return "entertainment";
	}
	return "";
}

function normalizeRecentOutfitHistory(recentOutfits = []) {
	return (Array.isArray(recentOutfits) ? recentOutfits : [])
		.map((entry) => {
			if (!entry) return null;
			if (typeof entry === "string") {
				const presenterOutfit = String(entry || "").trim();
				const key = normalizeOutfitKey(presenterOutfit);
				if (!key) return null;
				return {
					presenterOutfit,
					presenterOutfitStyle: "",
					key,
					mode: inferOutfitModeFromValue(presenterOutfit),
				};
			}
			const presenterOutfit = String(
				entry.presenterOutfit || entry.styleLabel || entry.outfit || "",
			).trim();
			const presenterOutfitStyle = String(
				entry.presenterOutfitStyle ||
					entry.styleId ||
					entry.style ||
					entry.publicId ||
					entry.url ||
					"",
			).trim();
			const key = normalizeOutfitKey(presenterOutfitStyle || presenterOutfit);
			if (!key) return null;
			return {
				presenterOutfit,
				presenterOutfitStyle,
				key,
				mode: inferOutfitModeFromValue(presenterOutfitStyle || presenterOutfit),
			};
		})
		.filter(Boolean);
}

function presenterKeys(option) {
	return [
		option.id,
		option.label,
		option.description,
		option.url,
		cloudinaryPublicIdFromUrl(option.url),
	]
		.map(normalizeOutfitKey)
		.filter(Boolean);
}

function matchesRecent(option, recentItem) {
	if (!recentItem?.key) return false;
	const keys = presenterKeys(option);
	return keys.some(
		(key) =>
			key === recentItem.key ||
			key.endsWith(`_${recentItem.key}`) ||
			recentItem.key.endsWith(`_${key}`),
	);
}

function countKeywordHits(option, context) {
	if (!context) return 0;
	return (option.keywords || []).reduce((count, keyword) => {
		const normalized = normalizeWhitespace(keyword).toLowerCase();
		if (!normalized) return count;
		return context.includes(normalized) ? count + 1 : count;
	}, 0);
}

function stableHash(value = "") {
	const digest = crypto.createHash("sha1").update(String(value)).digest();
	return digest.readUInt32BE(0);
}

function scorePresenterOption({ option, mode, context, recentHistory, jobId, title }) {
	const hardBlocked = recentHistory
		.slice(0, RECENT_OUTFIT_HARD_BLOCK_COUNT)
		.some((item) => matchesRecent(option, item));
	const softIndex = recentHistory
		.slice(0, RECENT_OUTFIT_SOFT_BLOCK_COUNT)
		.findIndex((item) => matchesRecent(option, item));
	const modeMatch = (option.modes || []).includes(mode);
	const keywordHits = countKeywordHits(option, context);
	const softPenalty = softIndex >= 0 ? 25 - Math.min(20, softIndex * 4) : 0;
	const score =
		(hardBlocked ? 10000 : 0) +
		(modeMatch ? 0 : 80) +
		softPenalty -
		keywordHits * 12;
	const hash = stableHash(`${jobId || ""}|${title || ""}|${mode}|${option.id}`);
	return { score, hash, hardBlocked, softIndex, keywordHits };
}

function chooseApprovedPresenter({
	title = "",
	topics = [],
	categoryLabel = "",
	jobId = "",
	recentOutfits = [],
	preferredStyleId = "",
}) {
	const mode = inferPresentationMode({ title, topics, categoryLabel });
	const context = buildTopicContext({ title, topics, categoryLabel });
	const recentHistory = normalizeRecentOutfitHistory(recentOutfits);
	const preferredKey = normalizeOutfitKey(preferredStyleId);
	const preferred = preferredKey
		? APPROVED_PRESENTER_LIBRARY.find((option) =>
				presenterKeys(option).some((key) => key === preferredKey),
			)
		: null;
	const hardBlocked = (option) =>
		recentHistory
			.slice(0, RECENT_OUTFIT_HARD_BLOCK_COUNT)
			.some((item) => matchesRecent(option, item));
	if (preferred && !hardBlocked(preferred)) {
		return {
			...preferred,
			mode,
			recentHistory,
			selectionReason: "preferred_style",
		};
	}

	const modePool = APPROVED_PRESENTER_LIBRARY.filter((option) =>
		(option.modes || []).includes(mode),
	);
	const firstPool = modePool.length ? modePool : APPROVED_PRESENTER_LIBRARY;
	let eligible = firstPool.filter((option) => !hardBlocked(option));
	if (!eligible.length) {
		eligible = APPROVED_PRESENTER_LIBRARY.filter((option) => !hardBlocked(option));
	}
	if (!eligible.length) eligible = [...APPROVED_PRESENTER_LIBRARY];

	const ranked = eligible
		.map((option) => ({
			option,
			...scorePresenterOption({
				option,
				mode,
				context,
				recentHistory,
				jobId,
				title,
			}),
		}))
		.sort((a, b) => a.score - b.score || a.hash - b.hash);
	const selected = ranked[0]?.option || APPROVED_PRESENTER_LIBRARY[0];
	return {
		...selected,
		mode,
		recentHistory,
		selectionReason: ranked[0]?.keywordHits
			? "topic_keyword_match"
			: "topic_mode_rotation",
	};
}

function cloudinaryPublicIdFromUrl(url = "") {
	const raw = String(url || "");
	const match = raw.match(/\/image\/upload\/(?:[^/]+\/)?(.+?)\.[a-z0-9]+(?:[?#].*)?$/i);
	return match ? match[1] : "";
}

function extensionFromUrl(url = "") {
	const match = String(url || "").match(/\.([a-z0-9]+)(?:[?#].*)?$/i);
	const ext = match ? match[1].toLowerCase() : "png";
	return ["png", "jpg", "jpeg", "webp"].includes(ext) ? ext : "png";
}

async function downloadToFile(url, outPath, timeoutMs = 90000) {
	ensureDir(path.dirname(outPath));
	const partPath = `${outPath}.part`;
	const response = await axios({
		method: "GET",
		url,
		responseType: "stream",
		timeout: timeoutMs,
		validateStatus: (status) => status >= 200 && status < 300,
	});
	await new Promise((resolve, reject) => {
		const writer = fs.createWriteStream(partPath);
		response.data.pipe(writer);
		writer.on("finish", resolve);
		writer.on("error", reject);
	});
	fs.renameSync(partPath, outPath);
	return outPath;
}

async function materializeApprovedPresenter({ option, tmpDir, jobId }) {
	const ext = extensionFromUrl(option.url);
	const safeJobId = normalizeOutfitKey(jobId || "job") || "job";
	const outPath = path.join(
		tmpDir,
		`presenter_library_${safeJobId}_${option.id}.${ext}`,
	);
	if (!fs.existsSync(outPath)) {
		await downloadToFile(option.url, outPath);
	}
	ensurePresenterFile(outPath);
	return outPath;
}

async function generatePresenterAdjustedImage({
	jobId,
	tmpDir,
	presenterLocalPath,
	title,
	topics = [],
	categoryLabel,
	recentOutfits = [],
	preferredStyleId = "",
	log,
}) {
	const logger = createLogger(log, jobId);
	const workingDir =
		tmpDir || path.join(os.tmpdir(), "presenter_adjustments2", jobId || "job");
	ensureDir(workingDir);

	const selected = chooseApprovedPresenter({
		title,
		topics,
		categoryLabel,
		jobId,
		recentOutfits,
		preferredStyleId,
	});
	logger("presenterAdjustments2 approved presenter selected", {
		topicMode: selected.mode,
		style: selected.id,
		outfit: selected.label,
		reason: selected.selectionReason,
		lastOutfit:
			selected.recentHistory[0]?.presenterOutfitStyle ||
			selected.recentHistory[0]?.presenterOutfit ||
			"",
	});

	try {
		const localPath = await materializeApprovedPresenter({
			option: selected,
			tmpDir: workingDir,
			jobId: jobId || "job",
		});
		const dims = getImageDimensions(localPath);
		return {
			localPath,
			url: selected.url,
			publicId: cloudinaryPublicIdFromUrl(selected.url),
			width: dims.width || 0,
			height: dims.height || 0,
			method: PRESENTER_METHOD,
			presenterOutfit: selected.label,
			presenterOutfitStyle: selected.id,
			presenterOutfitDescription: selected.description,
		};
	} catch (error) {
		logger("presenterAdjustments2 approved presenter unavailable; using original", {
			error: error?.message || String(error),
			style: selected.id,
		});
		return buildOriginalFallback(presenterLocalPath);
	}
}

function argValue(name, fallback = "") {
	const index = process.argv.indexOf(name);
	if (index < 0 || index >= process.argv.length - 1) return fallback;
	return process.argv[index + 1];
}

async function runCli() {
	if (!process.argv.includes("--test")) return;
	const jobId = argValue("--job-id", `presenter2_library_${Date.now()}`);
	const tmpDir = path.resolve(
		argValue("--tmp-dir", path.join(os.tmpdir(), "presenter_adjustments2", jobId)),
	);
	const recentStyle = argValue("--recent-style", "");
	const result = await generatePresenterAdjustedImage({
		jobId,
		tmpDir,
		presenterLocalPath: argValue("--source", ""),
		title: argValue("--title", "Modern AI presenter outfit test"),
		categoryLabel: argValue("--category", ""),
		preferredStyleId: argValue("--style-id", ""),
		recentOutfits: recentStyle ? [{ presenterOutfitStyle: recentStyle }] : [],
		log: (event, data = {}) =>
			console.log(`[presenterAdjustments2] ${event} ${JSON.stringify(data)}`),
	});
	console.log(JSON.stringify(result, null, 2));
}

if (require.main === module) {
	runCli().catch((error) => {
		console.error(error?.stack || error?.message || String(error));
		process.exit(1);
	});
}

module.exports = {
	generatePresenterAdjustedImage,
	__internals: {
		APPROVED_PRESENTER_LIBRARY,
		chooseApprovedPresenter,
		inferPresentationMode,
		normalizeRecentOutfitHistory,
	},
};
