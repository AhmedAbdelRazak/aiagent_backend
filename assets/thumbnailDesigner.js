/** @format */

const fs = require("fs");
const path = require("path");
const child_process = require("child_process");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;
const {
	assertOpenAIImageReady,
	editImageToPath,
	pickOpenAIImageSize,
} = require("./openaiImageTools");

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

const ACCENT_PALETTE = {
	default: "0xFFC700",
	tech: "0x00C2FF",
	business: "0x48C67A",
};

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

async function collectTopicReferenceImages({
	topics = [],
	tmpDir,
	jobId,
	maxImages = MAX_TOPIC_REFERENCE_IMAGES,
	log,
}) {
	const urls = [];
	for (const topic of Array.isArray(topics) ? topics : []) {
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

function chooseAccentColor(intent, text = "") {
	if (intent === "tech") return ACCENT_PALETTE.tech;
	if (intent === "business") return ACCENT_PALETTE.business;
	if (/\b(ai|tech|software|developer|code)\b/i.test(text)) {
		return ACCENT_PALETTE.tech;
	}
	if (/\b(business|finance|money|market|startup)\b/i.test(text)) {
		return ACCENT_PALETTE.business;
	}
	return ACCENT_PALETTE.default;
}

function chooseThumbnailPose({
	expression = "",
	intent = "general",
	contextText = "",
}) {
	const expr = String(expression || "").toLowerCase();
	const text = String(contextText || "").toLowerCase();
	if (
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
		return "serious and composed, natural relaxed eyes, closed mouth, only a tiny shift in expression, not intense, not exaggerated, not dramatic";
	}
	if (pose === "thoughtful") {
		return "thoughtful, mildly curious, slightly wondering, only a tiny shift in expression, subtle and natural";
	}
	if (pose === "surprised") {
		return "very mild surprise with slightly raised brows and a controlled soft reaction, never shocked, never exaggerated";
	}
	if (pose === "smile") {
		return "soft professional smile, tiny reaction only, friendly and subtle, no broad grin and no big cheek movement";
	}
	return "neutral, attentive, lightly engaged, calm and natural, almost no facial change";
}

function buildThumbnailArtDirection(intent = "general") {
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

function formatTopicDisplay(label = "") {
	const normalized = normalizeWhitespace(label);
	if (!normalized) return "";
	if (/[0-9]/.test(normalized) || normalized.length <= 6) {
		return normalized.toUpperCase();
	}
	return normalized
		.split(" ")
		.map((word) =>
			word.length <= 3
				? word.toUpperCase()
				: word.charAt(0).toUpperCase() + word.slice(1).toLowerCase(),
		)
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
	const primaryHeadline = normalizeHeadlineText(
		overrideHeadline ||
			deriveHeadlineFromTitle({ title, shortTitle, seoTitle, topics, intent }),
		4,
	);
	const badgeText = chooseBadgeText({
		intent,
		overrideBadgeText,
	});
	const sublineText =
		primaryTopic && !headlineHasTopic(primaryHeadline, primaryTopic)
			? primaryTopic
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
}) {
	const topicFocus = buildTopicFocus({ title, shortTitle, seoTitle, topics });
	const artDirection = buildThumbnailArtDirection(intent);
	const moodLine =
		intent === "legal"
			? "Keep the visual tone premium, serious, editorial, mainstream, bright enough for a high-performing YouTube thumbnail, and never horror-like."
			: "Keep the visual tone premium, modern, high-end, clean, bright, and clearly click-worthy for a long-form YouTube thumbnail.";
	const topicFigureLine =
		topicReferenceCount > 0
			? "Use the topic reference image only as contextual inspiration for the left side story cue. Never replace or duplicate the presenter."
			: "Use environmental or symbolic topic cues on the left side and never add extra people.";
	return normalizeWhitespace(`
Create one complete 16:9 YouTube thumbnail for a long-form video.
Use the first input image as the seed composition.
Respect the existing composition of the seed image instead of inventing a new crop.
Use the presenter reference image to preserve the exact presenter identity, beard, glasses, hair, skin tone, nose, jawline, neck, shoulders, and the current dark outfit.
The presenter must stay on the right side in the same position, same scale, and same framing established by the seed image, with the upper torso visible, camera-facing, photorealistic, crisp, and premium.
Do not zoom in on the presenter face, do not crop tighter, do not reframe the presenter, and do not change the presenter scale relative to the frame.
Allow only a tiny facial reaction: ${buildExpressionPrompt(pose)}.
Do not damage or redesign the presenter.
Use the left side for the story setup and typography.
Topic direction: ${topicFocus || "current trending story"}.
${artDirection}
${moodLine}
${topicFigureLine}
Keep the composition clean and uncluttered.
Use one dominant left-side visual cue only, not a busy collage.
Make the thumbnail instantly understandable at a glance on mobile.
Render large, bold, clean, readable headline text exactly as: "${headline || "BIG UPDATE"}".
Render a smaller badge exactly as: "${badgeText || "TOP STORY"}".
${sublineText ? `Render a smaller supporting subline exactly as: "${sublineText}".` : ""}
Typography must look deliberate, sharp, high-contrast, premium, and readable on YouTube.
Use a clean visual hierarchy: headline first, badge second, supporting cue third.
Do not add any extra readable text beyond those exact phrases.
Do not add logos, watermarks, extra people, extra hands, deformed anatomy, broken glasses, damaged facial features, or muddy lighting.
The final thumbnail should already look polished, bright, and production-ready with no post-processing required.
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
	log,
}) {
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
	});
	const seedBasePath = composeThumbnailSeedBase({
		jobId,
		tmpDir,
		presenterLocalPath,
		topicReferencePaths,
	});
	const rawPath = path.join(tmpDir, `thumb_plate_raw_${jobId}.png`);
	const outputPath = path.join(tmpDir, `thumb_plate_${jobId}.jpg`);
	if (typeof log === "function") {
		log("thumbnail openai plate prompt", {
			attempt: 1,
			pose,
			prompt: prompt.slice(0, 240),
		});
	}
	try {
		const imagePaths = [
			seedBasePath,
			presenterLocalPath,
			...topicReferencePaths.slice(0, MAX_TOPIC_REFERENCE_IMAGES),
		];
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
			outPath: outputPath,
		});
		ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
		if (typeof log === "function") {
			log("thumbnail openai plate ready", {
				attempt: 1,
				path: path.basename(outputPath),
			});
		}
		return {
			path: outputPath,
		};
	} finally {
		safeUnlink(seedBasePath);
		safeUnlink(rawPath);
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
	assertOpenAIImageReady();
	assertCloudinaryReady();

	const contextText = buildContextText({ title, shortTitle, seoTitle, topics });
	const intent =
		normalizeWhitespace(overrideIntent) ||
		inferThumbnailIntent({ title, shortTitle, seoTitle, topics });
	const pose = chooseThumbnailPose({
		expression,
		intent,
		contextText,
	});
	const accent = chooseAccentColor(intent, contextText);
	const textPlan = buildThumbnailTextPlan({
		title,
		shortTitle,
		seoTitle,
		topics,
		intent,
		overrideHeadline,
		overrideBadgeText,
	});

	if (typeof log === "function") {
		log("thumbnail plan", {
			intent,
			pose,
			headline: textPlan.primaryHeadline,
			badgeText: textPlan.badgeText,
			subline: textPlan.sublineText || null,
		});
	}

	const topicReferencePaths = await collectTopicReferenceImages({
		topics,
		tmpDir,
		jobId,
		log,
	});

	const plate = await generateOpenAiThumbnailPlate({
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
		log,
	});

	const uploaded = await uploadThumbnailToCloudinary(plate.path, jobId);

	const variant = {
		variant: "a",
		localPath: plate.path,
		url: uploaded.url || "",
		publicId: uploaded.public_id || "",
		width: uploaded.width || THUMBNAIL_WIDTH,
		height: uploaded.height || THUMBNAIL_HEIGHT,
		title: textPlan.primaryHeadline,
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
