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

function chooseOutfitDirection({ title, topics, categoryLabel, jobId = "" }) {
	const mode = inferPresentationMode({ title, topics, categoryLabel });
	const optionsByMode = {
		formal: [
			"a classy dark formal-news outfit that feels authoritative and premium, choosing either a tailored blazer over a coordinated dark open-collar shirt or a refined dark jacket over a matching dark shirt, whichever looks most natural in this exact image",
			"a polished dark newsroom look with a premium layered outfit in dark tones, choosing the exact dark blazer-or-jacket combination yourself so it feels elegant, serious, and camera-ready",
			"a sharp dark professional outfit for a serious story, with one coherent layered look in dark tones that feels expensive, restrained, and naturally integrated into the shot",
		],
		modern: [
			"a modern clean dark studio outfit, choosing either a premium dark shirt-only look or a streamlined dark jacket over a dark shirt, whichever looks most natural in this exact image",
			"a sleek dark presenter outfit with a contemporary studio feel, choosing the exact dark shirt or lightweight jacket styling yourself so it feels polished and natural",
			"a crisp modern dark on-camera look that feels smart and premium, with one coherent dark outfit designed to look natural in the existing shot",
		],
		entertainment: [
			"a polished dark entertainment-host outfit, choosing either a refined dark shirt-only look or a subtle dark jacket look, whichever looks most natural in this exact image",
			"a classy dark presenter outfit for entertainment coverage, choosing the exact dark styling yourself so it feels expressive, premium, and believable on camera",
			"a tasteful dark studio-host look with premium fabric and clean styling, choosing one natural-looking dark outfit that flatters the presenter without changing the shot",
		],
	};
	const options = optionsByMode[mode] || optionsByMode.entertainment;
	const hashSource = `${jobId}|${title || ""}|${mode}`;
	let hash = 0;
	for (const char of hashSource) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
	const promptLine = options[hash % options.length];
	return {
		mode,
		styleId: mode,
		styleLabel:
			mode === "formal"
				? "dark formal look"
				: mode === "modern"
					? "dark modern studio look"
					: "dark entertainment-host look",
		promptLine,
	};
}

function buildWardrobePrompt({
	outfitDirection,
	title,
	topics = [],
	categoryLabel,
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
		"Make the outfit fully coherent and continuous from the collar to the bottom of the visible torso.",
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
	const outfitDirection = chooseOutfitDirection({
		title,
		topics,
		categoryLabel,
		jobId,
	});
	const prompt = buildWardrobePrompt({
		outfitDirection,
		title,
		topics,
		categoryLabel,
	});

	logger("presenter source wardrobe analysis", {
		topicMode: outfitDirection.mode,
		styleFamily: outfitDirection.styleId,
	});

	try {
		logger("presenter edit attempt", {
			attempt: 1,
			outfit: outfitDirection.styleLabel,
			style: outfitDirection.styleId,
		});

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
		logger("presenter edit attempt failed", {
			attempt: 1,
			outfit: outfitDirection.styleLabel,
			style: outfitDirection.styleId,
			error: error?.message || String(error),
		});
		logger("presenter strict fallback to original", {
			error: error?.message || String(error),
		});
		return buildOriginalFallback(presenterLocalPath);
	} finally {
		safeUnlink(rawOutputPath);
	}
}

module.exports = {
	generatePresenterAdjustedImage,
};
