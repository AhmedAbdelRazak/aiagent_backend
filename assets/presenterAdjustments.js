/** @format */

const fs = require("fs");
const os = require("os");
const path = require("path");
const child_process = require("child_process");
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

const PRESENTER_MIN_BYTES = 12000;
const PRESENTER_CLOUDINARY_FOLDER = "aivideomatic/long_presenters";
const PRESENTER_CLOUDINARY_PUBLIC_PREFIX = "presenter_master";
const PRESENTER_METHOD = "openai_image_edit_prompt_only";
const RECENT_OUTFIT_HARD_BLOCK_COUNT = 1;
const RECENT_OUTFIT_SOFT_BLOCK_COUNT = 6;
const MAX_OUTFIT_EDIT_ATTEMPTS = 3;

function createLogger(log, jobId) {
	const prefix = `[presenter_adjustments${jobId ? `:${jobId}` : ""}]`;
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

function safeUnlink(filePath) {
	try {
		if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
	} catch {}
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

function parseWebpSize(buf) {
	if (!buf || buf.length < 30) return null;
	if (
		buf.toString("ascii", 0, 4) !== "RIFF" ||
		buf.toString("ascii", 8, 12) !== "WEBP"
	) {
		return null;
	}
	const chunk = buf.toString("ascii", 12, 16);
	if (chunk === "VP8X") {
		return {
			width: 1 + buf.readUIntLE(24, 3),
			height: 1 + buf.readUIntLE(27, 3),
		};
	}
	if (chunk === "VP8 ") {
		return {
			width: buf.readUInt16LE(26) & 0x3fff,
			height: buf.readUInt16LE(28) & 0x3fff,
		};
	}
	if (chunk === "VP8L") {
		const b0 = buf[21];
		const b1 = buf[22];
		const b2 = buf[23];
		const b3 = buf[24];
		return {
			width: 1 + (b0 | ((b1 & 0x3f) << 8)),
			height: 1 + (((b1 & 0xc0) >> 6) | (b2 << 2) | ((b3 & 0x0f) << 10)),
		};
	}
	return null;
}

function getImageDimensions(filePath) {
	const head = readFileHeader(filePath);
	if (!head) return { width: 0, height: 0 };
	return (
		parsePngSize(head) ||
		parseJpegSize(head) ||
		parseWebpSize(head) || { width: 0, height: 0 }
	);
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

function runFfmpeg(args, label = "presenter_ffmpeg") {
	if (!ffmpegPath) throw new Error("ffmpeg_not_available");
	try {
		child_process.execFileSync(
			ffmpegPath,
			["-hide_banner", "-loglevel", "error", ...args],
			{
				windowsHide: true,
				maxBuffer: 64 * 1024 * 1024,
				stdio: ["ignore", "pipe", "pipe"],
			},
		);
	} catch (error) {
		const stderr = String(error?.stderr || error?.message || "").trim();
		throw new Error(`${label}_failed${stderr ? `:${stderr}` : ""}`);
	}
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
				.map((topic) => topic?.displayTopic || topic?.topic || "")
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
		/\b(arrest|court|trial|lawsuit|legal|investigation|crime|murder|victim|charged|hearing|government|policy|finance|business)\b/.test(
			context,
		)
	) {
		return "formal";
	}
	if (
		/\b(sports?|game|matchup|playoff|tournament|tech|software|startup|product|ai)\b/.test(
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
		.replace(/[^a-z0-9]+/g, "_")
		.replace(/^_+|_+$/g, "");
}

function inferOutfitModeFromValue(value = "") {
	const key = normalizeOutfitKey(value);
	if (!key) return "";
	if (key.startsWith("formal_") || /\bformal\b/.test(key)) return "formal";
	if (key.startsWith("modern_") || /\bmodern\b/.test(key)) return "modern";
	if (
		key.startsWith("entertainment_") ||
		/\bentertainment\b/.test(key) ||
		/\bhost\b/.test(key)
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
				const presenterOutfitStyle = "";
				const key = normalizeOutfitKey(presenterOutfit);
				if (!key) return null;
				return {
					presenterOutfit,
					presenterOutfitStyle,
					key,
					mode: inferOutfitModeFromValue(presenterOutfit),
				};
			}
			const presenterOutfit = String(
				entry.presenterOutfit || entry.styleLabel || entry.outfit || "",
			).trim();
			const presenterOutfitStyle = String(
				entry.presenterOutfitStyle || entry.styleId || entry.style || "",
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

const OUTFIT_VARIANTS_BY_MODE = {
	formal: [
		{
			id: "formal_charcoal_peak_lapel_black_open_collar",
			label: "charcoal peak-lapel blazer with black open-collar shirt",
			promptLine:
				"a tailored charcoal peak-lapel blazer over a black open-collar shirt with premium wool texture, crisp structure, and an elegant newsroom silhouette",
		},
		{
			id: "formal_midnight_double_breasted_graphite_shirt",
			label: "midnight double-breasted blazer with graphite shirt",
			promptLine:
				"a midnight double-breasted blazer over a graphite dress shirt with a refined structured fit, premium fabric, and a polished serious tone",
		},
		{
			id: "formal_black_textured_jacket_dark_stand_collar",
			label: "black textured jacket with dark stand-collar shirt",
			promptLine:
				"a black subtly textured formal jacket over a dark stand-collar shirt with a sleek premium finish and a restrained high-end broadcast feel",
		},
		{
			id: "formal_graphite_single_breasted_satin_trim",
			label: "graphite single-breasted blazer with satin-trim shirt look",
			promptLine:
				"a graphite single-breasted blazer with a dark satin-trim shirt styling, clean lines, premium tailoring, and a composed professional presence",
		},
		{
			id: "formal_deep_navy_soft_shoulder_black_shirt",
			label: "deep navy soft-shoulder blazer with black shirt",
			promptLine:
				"a deep navy soft-shoulder blazer over a black dress shirt with understated luxury, refined drape, and a calm formal-news look",
		},
		{
			id: "formal_black_three_piece_visible_vest",
			label: "black three-piece look with visible vest",
			promptLine:
				"a premium black three-piece formal look with a visible vest layer, sharp tailoring, and an expensive restrained newsroom finish",
		},
	],
	modern: [
		{
			id: "modern_black_mockneck_soft_jacket",
			label: "black mock-neck with soft dark jacket",
			promptLine:
				"a premium black mock-neck layered under a soft dark tailored jacket with clean minimal lines and a polished modern studio feel",
		},
		{
			id: "modern_graphite_overshirt_black_crew",
			label: "graphite overshirt with black crew shirt",
			promptLine:
				"a structured graphite overshirt layered over a clean black crew shirt with premium fabric, modern restraint, and a sharp on-camera silhouette",
		},
		{
			id: "modern_collarless_jacket_jet_black_shirt",
			label: "collarless dark jacket with jet black shirt",
			promptLine:
				"a collarless dark modern jacket over a jet black shirt with sleek architectural lines, refined texture, and an elevated contemporary studio look",
		},
		{
			id: "modern_dark_bomber_minimal_black_shirt",
			label: "minimal dark bomber with black shirt",
			promptLine:
				"a minimal premium dark bomber layered over a fitted black shirt with clean structure, subtle luxury, and a smart modern presenter style",
		},
		{
			id: "modern_slate_shirt_jacket_tonal_layer",
			label: "slate shirt-jacket with tonal dark layer",
			promptLine:
				"a slate shirt-jacket over a tonal dark inner layer with crisp seams, premium texture, and a polished contemporary studio profile",
		},
		{
			id: "modern_black_zip_front_jacket_graphite_layer",
			label: "black zip-front jacket with graphite layer",
			promptLine:
				"a premium black zip-front jacket over a graphite base layer with sleek modern styling, clean tailoring, and a believable high-end studio finish",
		},
	],
	entertainment: [
		{
			id: "entertainment_midnight_satin_bomber_black_shirt",
			label: "midnight satin bomber with black shirt",
			promptLine:
				"a midnight satin-finish bomber over a black shirt with a premium entertainment-host feel, tasteful shine, and a polished believable on-camera look",
		},
		{
			id: "entertainment_open_collar_silk_blend_black_shirt",
			label: "open-collar silk-blend black shirt look",
			promptLine:
				"a refined black silk-blend open-collar shirt look with premium drape, subtle texture, and a classy expressive host energy",
		},
		{
			id: "entertainment_deep_navy_soft_blazer_tonal_shirt",
			label: "deep navy soft blazer with tonal shirt",
			promptLine:
				"a deep navy soft blazer over a tonal dark shirt with entertainment-host polish, premium fabric, and a stylish but believable studio presence",
		},
		{
			id: "entertainment_black_tonal_layered_shirt_jacket",
			label: "black tonal layered shirt-jacket look",
			promptLine:
				"a black tonal layered shirt-jacket look with premium fabric contrast, subtle fashion-forward styling, and a polished host silhouette",
		},
		{
			id: "entertainment_graphite_textured_blazer_open_neck",
			label: "graphite textured blazer with open-neck dark shirt",
			promptLine:
				"a graphite textured blazer over an open-neck dark shirt with classy entertainment coverage energy, premium texture, and a believable studio-host finish",
		},
		{
			id: "entertainment_dark_suede_jacket_black_crew",
			label: "dark suede-style jacket with black crew layer",
			promptLine:
				"a dark suede-style jacket over a black crew layer with rich premium texture, tasteful host styling, and a clean flattering on-camera fit",
		},
	],
};

function outfitVariantScore({
	option,
	recentHistory,
	mode,
	jobId = "",
	title = "",
}) {
	const optionKey = normalizeOutfitKey(option.id || option.label);
	const hardBlocked = recentHistory
		.slice(0, RECENT_OUTFIT_HARD_BLOCK_COUNT)
		.some((item) => item.key === optionKey);
	const softBlocked = recentHistory
		.slice(0, RECENT_OUTFIT_SOFT_BLOCK_COUNT)
		.some((item) => item.key === optionKey);
	const modePenalty =
		recentHistory.length &&
		recentHistory[0]?.mode &&
		recentHistory[0].mode === mode &&
		hardBlocked
			? 1000
			: 0;
	const score = modePenalty + (hardBlocked ? 100 : 0) + (softBlocked ? 10 : 0);
	const hashSource = `${jobId}|${title || ""}|${mode}|${option.id}|${option.label}`;
	let hash = 0;
	for (const char of hashSource) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
	return { score, hash };
}

function chooseOutfitDirection({
	title,
	topics,
	categoryLabel,
	jobId = "",
	recentOutfits = [],
}) {
	const mode = inferPresentationMode({ title, topics, categoryLabel });
	const options =
		OUTFIT_VARIANTS_BY_MODE[mode] || OUTFIT_VARIANTS_BY_MODE.entertainment;
	const recentHistory = normalizeRecentOutfitHistory(recentOutfits);
	const ordered = [...options]
		.map((option) => ({
			option,
			...outfitVariantScore({
				option,
				recentHistory,
				mode,
				jobId,
				title,
			}),
		}))
		.sort((a, b) => a.score - b.score || a.hash - b.hash)
		.map((item) => item.option);
	const selected = ordered[0] || options[0];
	return {
		mode,
		styleFamily: mode,
		styleId: selected.id,
		styleLabel: selected.label,
		promptLine: selected.promptLine,
		candidateStyles: ordered.map((option) => ({
			mode,
			styleFamily: mode,
			styleId: option.id,
			styleLabel: option.label,
			promptLine: option.promptLine,
		})),
		recentHistory: recentHistory.slice(0, RECENT_OUTFIT_SOFT_BLOCK_COUNT),
	};
}

function buildWardrobePrompt({
	outfitDirection,
	title,
	topics = [],
	categoryLabel,
	recentOutfits = [],
}) {
	const topicLine =
		(Array.isArray(topics)
			? topics
					.map((topic) => topic?.displayTopic || topic?.topic || "")
					.filter(Boolean)
					.join(" / ")
			: "") ||
		title ||
		categoryLabel ||
		"the video topic";
	const recentHistory = normalizeRecentOutfitHistory(recentOutfits);
	const recentLabels = recentHistory
		.slice(0, RECENT_OUTFIT_SOFT_BLOCK_COUNT)
		.map((item) => item.presenterOutfit || item.presenterOutfitStyle || "")
		.filter(Boolean);
	const recentLine = recentLabels.length
		? `This new outfit must be clearly different from these recently used presenter outfits: ${recentLabels.join("; ")}.`
		: "This new outfit must feel like a clearly fresh wardrobe change for a new video.";
	return [
		"Edit this exact presenter photo.",
		"Keep the exact same presenter in the exact same shot.",
		"Keep the exact same face, beard, glasses, hair, skin tone, neck, shoulders, hands, body proportions, desk, studio background, lighting, shadows, framing, camera angle, and overall realism.",
		"Keep the presenter in the exact same position and location within the frame.",
		"Do not zoom in, do not zoom out, do not crop, do not reframe, do not change camera distance, and do not change composition.",
		"Keep the same head size, eye line, shoulder placement, and torso scale relative to the frame.",
		"Change only the presenter outfit.",
		`Replace the full visible outfit with ${outfitDirection.promptLine}.`,
		`The outfit must feel classy, dark, polished, photorealistic, and appropriate for ${topicLine}.`,
		recentLine,
		"Do not reuse the same jacket shape, neckline, shirt styling, layering, lapel treatment, or silhouette from the recent outfits.",
		"Make the outfit fully coherent and continuous from the collar to the bottom of the visible torso.",
		"Both shoulders, lapels, collar lines, sleeves, and seams must look clean, symmetrical, and believable.",
		"No straps, harness details, scarves, shoulder drapes, floating fabric pieces, broken lapels, mismatched collars, or unexplained accessories.",
		"Do not leave any part of the original clothing visible.",
		"Do not change the dimensions of the image.",
		"Do not add text, captions, logos, graphics, jewelry, props, extra people, or background changes.",
		"Do not restyle the face, do not change the expression, and do not alter the identity in any way.",
		"The result must look like the same real presenter in the same real studio image, with only the outfit changed.",
	].join(" ");
}

function normalizeEditedPresenter({ sourcePath, candidatePath, outPath }) {
	const sourceDims = getImageDimensions(sourcePath);
	const candidateDims = getImageDimensions(candidatePath);
	if (!sourceDims.width || !sourceDims.height) {
		throw new Error("presenter_source_dimensions_unavailable");
	}
	if (
		sourceDims.width === candidateDims.width &&
		sourceDims.height === candidateDims.height
	) {
		return candidatePath;
	}
	runFfmpeg(
		[
			"-i",
			sourcePath,
			"-i",
			candidatePath,
			"-filter_complex",
			[
				`[1:v]scale=${sourceDims.width}:${sourceDims.height}:force_original_aspect_ratio=decrease:flags=lanczos,setsar=1[candidate]`,
				"[0:v][candidate]overlay=(W-w)/2:(H-h)/2:format=auto[outv]",
			].join(";"),
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-y",
			outPath,
		],
		"presenter_normalize",
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

async function uploadPresenterToCloudinary(filePath, jobId) {
	assertCloudinaryReady();
	const publicId = `${PRESENTER_CLOUDINARY_PUBLIC_PREFIX}_${jobId}_${Date.now()}`;
	const result = await cloudinary.uploader.upload(filePath, {
		public_id: publicId,
		folder: PRESENTER_CLOUDINARY_FOLDER,
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

async function generatePresenterAdjustedImage({
	jobId,
	tmpDir,
	presenterLocalPath,
	title,
	topics = [],
	categoryLabel,
	recentOutfits = [],
	log,
}) {
	const logger = createLogger(log, jobId);
	const workingDir = tmpDir || path.join(os.tmpdir(), "presenter_adjustments");
	ensureDir(workingDir);
	ensurePresenterFile(presenterLocalPath);

	try {
		assertOpenAIImageReady();
	} catch (error) {
		logger("openai image edit unavailable; using original presenter", {
			error: error?.message || String(error),
		});
		return buildOriginalFallback(presenterLocalPath);
	}

	try {
		assertCloudinaryReady();
	} catch (error) {
		logger("cloudinary upload unavailable; using original presenter", {
			error: error?.message || String(error),
		});
		return buildOriginalFallback(presenterLocalPath);
	}

	const sourceDims = getImageDimensions(presenterLocalPath);
	const rawOutputPath = path.join(
		workingDir,
		`presenter_outfit_${jobId || "job"}_raw.png`,
	);
	const normalizedOutputPath = path.join(
		workingDir,
		`presenter_outfit_${jobId || "job"}_normalized.png`,
	);
	const outfitPlan = chooseOutfitDirection({
		title,
		topics,
		categoryLabel,
		jobId,
		recentOutfits,
	});

	logger("presenter source wardrobe analysis", {
		topicMode: outfitPlan.mode,
		styleFamily: outfitPlan.styleFamily,
		selectedStyle: outfitPlan.styleId,
		recentOutfits: outfitPlan.recentHistory.map(
			(item) => item.presenterOutfitStyle || item.presenterOutfit,
		),
	});

	let lastError = null;
	const attemptStyles = Array.isArray(outfitPlan.candidateStyles)
		? outfitPlan.candidateStyles.slice(0, MAX_OUTFIT_EDIT_ATTEMPTS)
		: [
				{
					mode: outfitPlan.mode,
					styleFamily: outfitPlan.styleFamily,
					styleId: outfitPlan.styleId,
					styleLabel: outfitPlan.styleLabel,
					promptLine: outfitPlan.promptLine,
				},
			];
	for (
		let attemptIndex = 0;
		attemptIndex < attemptStyles.length;
		attemptIndex++
	) {
		const outfitDirection = attemptStyles[attemptIndex];
		const prompt = buildWardrobePrompt({
			outfitDirection,
			title,
			topics,
			categoryLabel,
			recentOutfits,
		});
		logger("presenter edit attempt", {
			attempt: attemptIndex + 1,
			outfit: outfitDirection.styleLabel,
			style: outfitDirection.styleId,
		});
		try {
			await editImageToPath({
				prompt,
				imagePaths: [presenterLocalPath],
				outPath: rawOutputPath,
				size: pickOpenAIImageSize({
					width: sourceDims.width,
					height: sourceDims.height,
					preferLandscape: true,
				}),
				quality: "high",
				background: "opaque",
				inputFidelity: "high",
				user: String(jobId || "presenter_adjustment"),
			});

			const finalPath = normalizeEditedPresenter({
				sourcePath: presenterLocalPath,
				candidatePath: rawOutputPath,
				outPath: normalizedOutputPath,
			});
			ensurePresenterFile(finalPath);

			const uploaded = await uploadPresenterToCloudinary(
				finalPath,
				jobId || "job",
			);

			logger("presenter edit ready", {
				outfit: outfitDirection.styleLabel,
				style: outfitDirection.styleId,
				url: uploaded.url || "",
			});

			return {
				localPath: finalPath,
				url: uploaded.url || "",
				publicId: uploaded.public_id || "",
				width: uploaded.width || sourceDims.width || 0,
				height: uploaded.height || sourceDims.height || 0,
				method: PRESENTER_METHOD,
				presenterOutfit: outfitDirection.styleLabel,
				presenterOutfitStyle: outfitDirection.styleId,
			};
		} catch (error) {
			lastError = error;
			logger("presenter edit attempt failed", {
				attempt: attemptIndex + 1,
				outfit: outfitDirection.styleLabel,
				style: outfitDirection.styleId,
				error: error?.message || String(error),
			});
		} finally {
			safeUnlink(rawOutputPath);
		}
	}

	logger("presenter strict fallback to original", {
		error: lastError?.message || "presenter_edit_failed",
	});
	return buildOriginalFallback(presenterLocalPath);
}

module.exports = {
	generatePresenterAdjustedImage,
};
