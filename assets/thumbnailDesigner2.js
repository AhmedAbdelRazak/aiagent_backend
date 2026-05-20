/** @format */

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const axios = require("axios");
const FormData = require("form-data");
const baseDesigner = require("./thumbnailDesigner");

const {
	ACCENT_PALETTE,
	THUMBNAIL_HEIGHT,
	THUMBNAIL_MIN_BYTES,
	THUMBNAIL_WIDTH,
	buildContextText,
	buildThumbnailTextPlan,
	chooseAccentColor,
	chooseThumbnailPose,
	chooseThumbnailStyleProfile,
	collectTopicReferenceImages,
	ensureDir,
	ensureImageFile,
	ensureThumbnailFile,
	inferThumbnailIntent,
	lockPresenterPanel,
	normalizeWhitespace,
	primaryTopicLabel,
	renderLockedThumbnailTextOverlay,
	renderTopicLeadVisualSeed,
	safeUnlink,
	uploadThumbnailToCloudinary,
} = baseDesigner.__internals || {};

const DEFAULT_COMFY_URL = "http://127.0.0.1:8188";
const DEFAULT_MODEL = "Realistic_Vision_V6.0_NV_B1_fp16.safetensors";

let ffmpegPath = "";
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "/usr/bin/ffmpeg";
}

function requireInternals() {
	const missing = [
		["buildContextText", buildContextText],
		["buildThumbnailTextPlan", buildThumbnailTextPlan],
		["collectTopicReferenceImages", collectTopicReferenceImages],
		["renderTopicLeadVisualSeed", renderTopicLeadVisualSeed],
		["lockPresenterPanel", lockPresenterPanel],
		["renderLockedThumbnailTextOverlay", renderLockedThumbnailTextOverlay],
		["uploadThumbnailToCloudinary", uploadThumbnailToCloudinary],
	].filter(([, value]) => typeof value !== "function");
	if (missing.length) {
		throw new Error(
			`thumbnail_designer2_missing_base_helpers:${missing
				.map(([name]) => name)
				.join(",")}`,
		);
	}
}

function truthyEnv(value, fallback = false) {
	const raw = String(value ?? "").trim().toLowerCase();
	if (!raw) return fallback;
	return ["1", "true", "yes", "on"].includes(raw);
}

function numberEnv(name, fallback, min, max) {
	const parsed = Number(process.env[name]);
	if (!Number.isFinite(parsed)) return fallback;
	return Math.max(min, Math.min(max, parsed));
}

function getComfyConfig() {
	return {
		enabled: truthyEnv(process.env.THUMBNAIL_DESIGNER2_COMFY_ENABLED, true),
		strict: truthyEnv(process.env.THUMBNAIL_DESIGNER2_STRICT_COMFY, false),
		url: normalizeWhitespace(process.env.COMFYUI_URL || DEFAULT_COMFY_URL).replace(
			/\/+$/,
			"",
		),
		model: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_MODEL || DEFAULT_MODEL,
		),
		width: numberEnv("THUMBNAIL_DESIGNER2_COMFY_WIDTH", 768, 384, 1024),
		height: numberEnv("THUMBNAIL_DESIGNER2_COMFY_HEIGHT", 432, 216, 768),
		steps: numberEnv("THUMBNAIL_DESIGNER2_COMFY_STEPS", 4, 1, 24),
		cfg: numberEnv("THUMBNAIL_DESIGNER2_COMFY_CFG", 4.4, 1, 9),
		denoise: numberEnv("THUMBNAIL_DESIGNER2_COMFY_DENOISE", 0.38, 0.15, 0.7),
		sampler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_SAMPLER || "euler",
		),
		scheduler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_SCHEDULER || "normal",
		),
		feedEnabled: truthyEnv(
			process.env.THUMBNAIL_DESIGNER2_COMFY_FEED_ENABLED,
			true,
		),
		feedWidth: numberEnv("THUMBNAIL_DESIGNER2_COMFY_FEED_WIDTH", 768, 384, 1024),
		feedHeight: numberEnv(
			"THUMBNAIL_DESIGNER2_COMFY_FEED_HEIGHT",
			432,
			216,
			768,
		),
		feedSteps: numberEnv("THUMBNAIL_DESIGNER2_COMFY_FEED_STEPS", 2, 1, 12),
		feedCfg: numberEnv("THUMBNAIL_DESIGNER2_COMFY_FEED_CFG", 5.2, 1, 9),
		feedSampler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_FEED_SAMPLER ||
				process.env.THUMBNAIL_DESIGNER2_COMFY_SAMPLER ||
				"euler",
		),
		feedScheduler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_FEED_SCHEDULER ||
				process.env.THUMBNAIL_DESIGNER2_COMFY_SCHEDULER ||
				"normal",
		),
		timeoutMs: numberEnv(
			"THUMBNAIL_DESIGNER2_COMFY_TIMEOUT_MS",
			20 * 60 * 1000,
			60 * 1000,
			60 * 60 * 1000,
		),
		pollMs: numberEnv("THUMBNAIL_DESIGNER2_COMFY_POLL_MS", 2500, 1000, 10000),
		inputDir: normalizeWhitespace(
			process.env.COMFYUI_INPUT_DIR ||
				(process.platform === "win32"
					? ""
					: "/home/ahmedadmin/ai-lab/ComfyUI/input"),
		),
		outputDir: normalizeWhitespace(
			process.env.COMFYUI_OUTPUT_DIR ||
				(process.platform === "win32"
					? ""
					: "/home/ahmedadmin/ai-lab/ComfyUI/output"),
		),
	};
}

function runFfmpeg(args, label = "ffmpeg") {
	if (!ffmpegPath) throw new Error("ffmpeg_unavailable");
	try {
		require("child_process").execFileSync(
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

function normalizeAccentColor(value = "") {
	const raw = String(value || "")
		.trim()
		.replace(/^0x/i, "")
		.replace(/^#/, "");
	if (/^[0-9a-f]{6}$/i.test(raw)) return `0x${raw.toUpperCase()}`;
	return "0x00C2FF";
}

function enhanceDesigner2StyleProfile(styleProfile = {}, contextText = "") {
	const hay = normalizeWhitespace(contextText).toLowerCase();
	if (
		/\b(tired|fatigue|burnout|sleep|resting|rest|overloaded|mental noise|drained|exhausted|worrying|scrolling)\b/.test(
			hay,
		)
	) {
		return {
			...styleProfile,
			id: "soft_cyan_wellness",
			accent: "0x00C2FF",
			tagColor: "0x102A43",
			textPanelOpacity: 0.97,
			brief:
				"cinematic wellness/editorial contrast, cyan night-to-morning glow, polished mental-fatigue story energy, clean premium YouTube frame",
		};
	}
	return styleProfile;
}

function buildComfyPrompt({
	title,
	shortTitle,
	seoTitle,
	topics = [],
	headline = "",
	badgeText = "",
	sublineText = "",
	intent = "general",
	styleProfile = {},
	hasFeedImage = false,
	feedSource = "",
}) {
	const topicText =
		normalizeWhitespace(primaryTopicLabel?.(topics)) ||
		normalizeWhitespace(shortTitle) ||
		normalizeWhitespace(title) ||
		normalizeWhitespace(seoTitle) ||
		"current story";
	const styleBrief = normalizeWhitespace(styleProfile.brief || "");
	const safeHeadline = normalizeWhitespace(headline || "main story");
	const safeBadge = normalizeWhitespace(badgeText || "spotlight");
	const safeSubline = normalizeWhitespace(sublineText || topicText);
	const sourceType = normalizeWhitespace(feedSource).toLowerCase();
	const feedLine = hasFeedImage
		? sourceType === "comfy_generated_fallback"
			? "The left side contains an AI-generated topic reference used only because no reliable orchestrator feed image was available. Keep it symbolic, non-fabricated, and visually clear; polish it into a premium thumbnail story cue."
			: "The left side already contains the orchestrator-provided feed/story image. Preserve its main subject and context, then relight, sharpen, simplify, and frame it as a premium thumbnail story cue."
		: "No reliable feed image is present. Keep the left side symbolic and non-fabricated; use environment, objects, color, and editorial lighting instead of inventing real people.";

	return normalizeWhitespace(`
		Image-to-image polish of one complete 16:9 YouTube thumbnail visual plate.
		Use the input image as the layout blueprint: story/feed visual on the left, presenter on the right.
		${feedLine}
		The text is already planned separately: headline "${safeHeadline}", badge "${safeBadge}", optional subject "${safeSubline}".
		Do not render text. Leave a clean readable left-side text area for those exact words to be added after generation.
		Remove or paint over any words, captions, screenshot fragments, labels, or text-like artifacts already visible in the source feed image, especially near the lower-left text-safe panel.
		Do not hallucinate a new left-side person or scene. Keep the source feed image recognizable; enhance lighting and depth only.
		Keep the right-side presenter in the same position and scale. Preserve identity, glasses, beard, hairline, face shape, expression, shoulders, dark outfit, and camera-facing pose.
		Do not redraw, beautify, age, distort, crop, or change the presenter face. The original presenter panel will be restored after this step.
		Polish the whole visual plate: stronger contrast, richer depth, cleaner lighting, crisp subject separation, premium editorial color, high-end YouTube thumbnail energy, mobile-readable composition.
		Make the left feed image dominant, bright enough to understand, and visually specific to the topic. Add tasteful cyan/blue editorial glow, curved light streaks, depth, a premium divider, and a designed lower-left text-safe panel like a sophisticated news/sports YouTube thumbnail.
		Keep the main story subject visible above/behind the text-safe panel, not buried in darkness. Avoid busy collage, random body-part crops, fake faces, fake screenshots, text blocks, signs, logos, or watermarks.
		Intent: ${intent}. Topic: ${topicText}. Style: ${styleBrief || "premium editorial thumbnail"}.
	`);
}

function buildComfyFeedPrompt({
	title,
	shortTitle,
	seoTitle,
	topics = [],
	contextText = "",
	styleProfile = {},
}) {
	const topicText =
		normalizeWhitespace(primaryTopicLabel?.(topics)) ||
		normalizeWhitespace(shortTitle) ||
		normalizeWhitespace(title) ||
		normalizeWhitespace(seoTitle) ||
		"current story";
	const styleBrief = normalizeWhitespace(styleProfile.brief || "");
	return normalizeWhitespace(`
		Create one photorealistic editorial feed image for the left side of a YouTube thumbnail.
		No presenter, no host, no text, no typography, no watermark, no UI.
		Topic: ${topicText}.
		Context and visual hints: ${normalizeWhitespace(contextText).slice(0, 1200)}.
		For a tiredness, burnout, mental fatigue, sleep, rest, or overloaded-life topic, show a relatable cinematic scene:
		a tired adult near a laptop at night, coffee cup, messy desk, notebook or unfinished task list, soft morning light or window glow, calm realistic mood, practical not medical.
		Make it feel like a high-quality news/editorial feed photo with clear subject, strong depth, cinematic lighting, premium contrast, and space near the lower-left for later headline text.
		Avoid hospital scenes, medical diagnosis, horror, melodrama, fake celebrities, extra limbs, distorted faces, words, letters, captions, logos, and screenshots.
		Style: ${styleBrief || "premium editorial photo, cinematic cyan highlights"}.
	`);
}

function buildNegativePrompt() {
	return normalizeWhitespace(`
		text, letters, words, subtitles, captions, logo, watermark, signature,
		UI screenshot, fake interface, poster text, misspelled text, duplicated text,
		deformed face, changed presenter identity, different glasses, missing glasses,
		bad eyes, bad beard, bad mouth, bad anatomy, extra fingers, extra limbs,
		duplicate people, cropped head, out of frame, waxy skin, plastic skin,
		cartoon, anime, illustration, painting, low quality, blurry, noisy,
		muddy lighting, cluttered composition, oversaturated, overexposed,
		underexposed, random celebrity, fabricated portrait
	`);
}

function randomSeed() {
	return Math.floor(Math.random() * 999999999999999);
}

function buildWorkflow(config, prompt, uploadedImageName) {
	return {
		"4": {
			class_type: "CheckpointLoaderSimple",
			inputs: {
				ckpt_name: config.model,
			},
		},
		"10": {
			class_type: "LoadImage",
			inputs: {
				image: uploadedImageName,
			},
		},
		"11": {
			class_type: "ImageScale",
			inputs: {
				upscale_method: "lanczos",
				width: config.width,
				height: config.height,
				crop: "center",
				image: ["10", 0],
			},
		},
		"6": {
			class_type: "CLIPTextEncode",
			inputs: {
				text: prompt,
				clip: ["4", 1],
			},
		},
		"7": {
			class_type: "CLIPTextEncode",
			inputs: {
				text: buildNegativePrompt(),
				clip: ["4", 1],
			},
		},
		"12": {
			class_type: "VAEEncode",
			inputs: {
				pixels: ["11", 0],
				vae: ["4", 2],
			},
		},
		"3": {
			class_type: "KSampler",
			inputs: {
				seed: randomSeed(),
				steps: config.steps,
				cfg: config.cfg,
				sampler_name: config.sampler,
				scheduler: config.scheduler,
				denoise: config.denoise,
				model: ["4", 0],
				positive: ["6", 0],
				negative: ["7", 0],
				latent_image: ["12", 0],
			},
		},
		"8": {
			class_type: "VAEDecode",
			inputs: {
				samples: ["3", 0],
				vae: ["4", 2],
			},
		},
		"9": {
			class_type: "SaveImage",
			inputs: {
				filename_prefix: "agentai_thumbnail2_comfy_plate",
				images: ["8", 0],
			},
		},
	};
}

function buildTextToImageWorkflow(config, prompt, options = {}) {
	const width = options.width || config.feedWidth || config.width;
	const height = options.height || config.feedHeight || config.height;
	const steps = options.steps || config.feedSteps || config.steps;
	const cfg = options.cfg || config.feedCfg || config.cfg;
	const sampler = options.sampler || config.feedSampler || config.sampler;
	const scheduler = options.scheduler || config.feedScheduler || config.scheduler;
	const prefix = options.prefix || "agentai_thumbnail2_feed";
	return {
		"4": {
			class_type: "CheckpointLoaderSimple",
			inputs: {
				ckpt_name: config.model,
			},
		},
		"6": {
			class_type: "CLIPTextEncode",
			inputs: {
				text: prompt,
				clip: ["4", 1],
			},
		},
		"7": {
			class_type: "CLIPTextEncode",
			inputs: {
				text: buildNegativePrompt(),
				clip: ["4", 1],
			},
		},
		"5": {
			class_type: "EmptyLatentImage",
			inputs: {
				width,
				height,
				batch_size: 1,
			},
		},
		"3": {
			class_type: "KSampler",
			inputs: {
				seed: randomSeed(),
				steps,
				cfg,
				sampler_name: sampler,
				scheduler,
				denoise: 1,
				model: ["4", 0],
				positive: ["6", 0],
				negative: ["7", 0],
				latent_image: ["5", 0],
			},
		},
		"8": {
			class_type: "VAEDecode",
			inputs: {
				samples: ["3", 0],
				vae: ["4", 2],
			},
		},
		"9": {
			class_type: "SaveImage",
			inputs: {
				filename_prefix: prefix,
				images: ["8", 0],
			},
		},
	};
}

async function comfyRequest(config, method, pathname, data, options = {}) {
	const response = await axios({
		method,
		url: `${config.url}${pathname}`,
		data,
		timeout: options.timeout || 30000,
		validateStatus: (status) => status >= 200 && status < 300,
	});
	return response.data;
}

async function uploadComfyInput(config, filePath) {
	const form = new FormData();
	form.append("image", fs.createReadStream(filePath), {
		filename: `agentai_thumbnail2_draft_${crypto.randomUUID()}.png`,
	});
	form.append("type", "input");
	form.append("overwrite", "true");
	const response = await axios.post(`${config.url}/upload/image`, form, {
		headers: form.getHeaders(),
		timeout: 60000,
		validateStatus: (status) => status >= 200 && status < 300,
	});
	return response.data;
}

async function waitForComfyImage(config, promptId) {
	const deadline = Date.now() + config.timeoutMs;
	while (Date.now() < deadline) {
		await new Promise((resolve) => setTimeout(resolve, config.pollMs));
		const history = await comfyRequest(
			config,
			"GET",
			`/history/${encodeURIComponent(promptId)}`,
		);
		const item = history?.[promptId];
		if (!item?.outputs) continue;
		for (const output of Object.values(item.outputs)) {
			if (Array.isArray(output?.images) && output.images[0]) {
				return output.images[0];
			}
		}
	}
	throw new Error("comfyui_thumbnail_plate_timeout");
}

async function freeComfyMemory(config, log) {
	try {
		await comfyRequest(config, "POST", "/free", {
			unload_models: true,
			free_memory: true,
		});
		if (typeof log === "function") {
			log("thumbnailDesigner2 comfy memory released", {
				unloadModels: true,
				freeMemory: true,
			});
		}
	} catch (error) {
		if (typeof log === "function") {
			log("thumbnailDesigner2 comfy memory release skipped", {
				error: error?.message || String(error),
			});
		}
	}
}

function comfyImageUrl(config, image) {
	const params = new URLSearchParams({
		filename: image.filename || "",
		subfolder: image.subfolder || "",
		type: image.type || "output",
	});
	return `${config.url}/view?${params.toString()}`;
}

function resolveComfyFilePath(config, image = {}) {
	const type = image.type || "output";
	const rootDir = type === "input" ? config.inputDir : config.outputDir;
	if (!rootDir || !image.filename) return "";
	const root = path.resolve(rootDir);
	const resolved = path.resolve(root, image.subfolder || "", image.filename);
	return resolved === root || !resolved.startsWith(`${root}${path.sep}`)
		? ""
		: resolved;
}

function cleanupComfyFile(filePath, log, label) {
	if (!filePath) return;
	try {
		if (fs.existsSync(filePath)) {
			fs.unlinkSync(filePath);
			if (typeof log === "function") {
				log(label, { file: path.basename(filePath) });
			}
		}
	} catch (error) {
		if (typeof log === "function") {
			log(`${label} skipped`, {
				error: error?.message || String(error),
			});
		}
	}
}

async function downloadComfyImage(config, image, outPath) {
	const response = await axios.get(comfyImageUrl(config, image), {
		responseType: "stream",
		timeout: 60000,
		validateStatus: (status) => status >= 200 && status < 300,
	});
	await new Promise((resolve, reject) => {
		const writer = fs.createWriteStream(outPath);
		response.data.pipe(writer);
		writer.on("finish", resolve);
		writer.on("error", reject);
	});
	return outPath;
}

async function resolveComfyOutputToLocalPath(config, image, tmpDir, jobId) {
	const resolved = resolveComfyFilePath(config, image);
	if (resolved && fs.existsSync(resolved)) return resolved;
	const outPath = path.join(tmpDir, `thumb_comfy_plate_${jobId}.png`);
	await downloadComfyImage(config, image, outPath);
	return outPath;
}

async function generateComfyThumbnailPlate({
	jobId,
	tmpDir,
	draftPath,
	title,
	shortTitle,
	seoTitle,
	topics,
	headline,
	badgeText,
	sublineText,
	intent,
	styleProfile,
	hasFeedImage,
	feedSource = "",
	log,
}) {
	const config = getComfyConfig();
	if (!config.enabled) return null;
	ensureImageFile(draftPath, 5000);
	const prompt = buildComfyPrompt({
		title,
		shortTitle,
		seoTitle,
		topics,
		headline,
		badgeText,
		sublineText,
		intent,
		styleProfile,
		hasFeedImage,
		feedSource,
	});
	if (typeof log === "function") {
		log("thumbnailDesigner2 comfy plate starting", {
			url: config.url,
			model: config.model,
			width: config.width,
			height: config.height,
			steps: config.steps,
			cfg: config.cfg,
			denoise: config.denoise,
			sampler: config.sampler,
			scheduler: config.scheduler,
			hasFeedImage,
			feedSource,
		});
	}

	await comfyRequest(config, "GET", "/system_stats", null, { timeout: 8000 });
	const uploadedInput = await uploadComfyInput(config, draftPath);
	const inputPath = resolveComfyFilePath(config, {
		filename: uploadedInput.name || uploadedInput.filename,
		subfolder: uploadedInput.subfolder || "",
		type: uploadedInput.type || "input",
	});
	try {
		const queued = await comfyRequest(config, "POST", "/prompt", {
			client_id: crypto.randomUUID(),
			prompt: buildWorkflow(config, prompt, uploadedInput.name || uploadedInput.filename),
		});
		const promptId = queued?.prompt_id;
		if (!promptId) throw new Error("comfyui_prompt_id_missing");
		const image = await waitForComfyImage(config, promptId);
		const localPath = await resolveComfyOutputToLocalPath(config, image, tmpDir, jobId);
		await freeComfyMemory(config, log);

		if (typeof log === "function") {
			log("thumbnailDesigner2 comfy plate ready", {
				promptId,
				filename: image.filename || "",
				subfolder: image.subfolder || "",
				type: image.type || "output",
				path: path.basename(localPath),
			});
		}

		return {
			path: localPath,
			method: "comfyui_img2img_plate",
			model: config.model,
			width: config.width,
			height: config.height,
			steps: config.steps,
			cfg: config.cfg,
			denoise: config.denoise,
			outputPath: resolveComfyFilePath(config, image),
			inputPath,
		};
	} catch (error) {
		await freeComfyMemory(config, log);
		throw error;
	} finally {
		cleanupComfyFile(
			inputPath,
			log,
			"thumbnailDesigner2 comfy input cleaned",
		);
	}
}

async function generateComfyFeedReference({
	jobId,
	tmpDir,
	title,
	shortTitle,
	seoTitle,
	topics,
	contextText,
	styleProfile,
	log,
}) {
	const config = getComfyConfig();
	if (!config.enabled || !config.feedEnabled) return null;
	const prompt = buildComfyFeedPrompt({
		title,
		shortTitle,
		seoTitle,
		topics,
		contextText,
		styleProfile,
	});
	if (typeof log === "function") {
		log("thumbnailDesigner2 comfy feed starting", {
			url: config.url,
			model: config.model,
			width: config.feedWidth,
			height: config.feedHeight,
			steps: config.feedSteps,
			cfg: config.feedCfg,
			sampler: config.feedSampler,
			scheduler: config.feedScheduler,
		});
	}
	await comfyRequest(config, "GET", "/system_stats", null, { timeout: 8000 });
	const queued = await comfyRequest(config, "POST", "/prompt", {
		client_id: crypto.randomUUID(),
		prompt: buildTextToImageWorkflow(config, prompt, {
			width: config.feedWidth,
			height: config.feedHeight,
			steps: config.feedSteps,
			cfg: config.feedCfg,
			sampler: config.feedSampler,
			scheduler: config.feedScheduler,
			prefix: "agentai_thumbnail2_feed",
		}),
	});
	const promptId = queued?.prompt_id;
	if (!promptId) throw new Error("comfyui_feed_prompt_id_missing");
	const image = await waitForComfyImage(config, promptId);
	const localPath = await resolveComfyOutputToLocalPath(
		config,
		image,
		tmpDir,
		`${jobId}_feed`,
	);
	if (typeof log === "function") {
		log("thumbnailDesigner2 comfy feed ready", {
			promptId,
			filename: image.filename || "",
			subfolder: image.subfolder || "",
			type: image.type || "output",
			path: path.basename(localPath),
		});
	}
	return {
		path: localPath,
		outputPath: resolveComfyFilePath(config, image),
		method: "comfyui_feed_reference",
	};
}

function renderDesigner2VisualSeed({
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
	const outputPath = path.join(tmpDir, `thumb_designer2_seed_${jobId}.jpg`);
	const accentColor = normalizeAccentColor(accent || "0x00C2FF");
	const filters = [
		`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.05:saturation=0.96:brightness=-0.015,gblur=sigma=24,setsar=1[bg]`,
		`[0:v]scale=740:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=740:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.12:saturation=1.10:brightness=0.012,unsharp=5:5:0.62:5:5:0.02,setsar=1[topic]`,
		`[1:v]scale=572:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=540:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.05:saturation=1.03,setsar=1[presenter]`,
		`[bg][topic]overlay=0:0[tmp0]`,
		`[tmp0]drawbox=x=0:y=0:w=740:h=${THUMBNAIL_HEIGHT}:color=black@0.05:t=fill[tmp1]`,
		`[tmp1]drawbox=x=36:y=334:w=664:h=368:color=black@0.74:t=fill[tmp2]`,
		`[tmp2]drawbox=x=36:y=334:w=664:h=3:color=${accentColor}@0.95:t=fill[tmp3]`,
		`[tmp3]drawbox=x=36:y=334:w=7:h=368:color=${accentColor}@0.96:t=fill[tmp4]`,
		`[tmp4]drawbox=x=0:y=0:w=740:h=7:color=${accentColor}@0.18:t=fill[tmp5]`,
		`[tmp5]drawbox=x=0:y=0:w=7:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.18:t=fill[tmp6]`,
		`[tmp6]drawbox=x=682:y=0:w=58:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.14:t=fill[tmp7]`,
		`[tmp7][presenter]overlay=740:0[tmp8]`,
		`[tmp8]drawbox=x=732:y=0:w=8:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.98:t=fill[tmp9]`,
		`[tmp9]drawbox=x=726:y=0:w=20:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.18:t=fill[outv]`,
	];
	runFfmpeg(
		[
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
		"thumbnail_designer2_seed_compose",
	);
	ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
	if (typeof log === "function") {
		log("thumbnailDesigner2 visual seed ready", {
			path: path.basename(outputPath),
			usesTopicHero: Boolean(topicReferencePaths.length),
		});
	}
	return {
		path: outputPath,
		method: "designer2_visual_seed",
	};
}

function restoreSharpFeedPanel({
	jobId,
	tmpDir,
	basePath,
	topicReferencePath,
	accent = ACCENT_PALETTE.default,
	log,
}) {
	if (!topicReferencePath) return "";
	ensureImageFile(basePath, 5000);
	ensureImageFile(topicReferencePath, 5000);
	const outputPath = path.join(tmpDir, `thumb_feed_sharp_${jobId}.jpg`);
	const accentColor = normalizeAccentColor(accent || "0x00C2FF");
	const filters = [
		`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1[base]`,
		`[1:v]scale=740:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=740:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.08:saturation=1.06:brightness=0.01,unsharp=5:5:0.70:5:5:0.02,setsar=1[feed]`,
		`[base][feed]overlay=0:0[tmp0]`,
		`[tmp0]drawbox=x=0:y=0:w=740:h=${THUMBNAIL_HEIGHT}:color=black@0.03:t=fill[tmp1]`,
		`[tmp1]drawbox=x=0:y=0:w=740:h=7:color=${accentColor}@0.16:t=fill[tmp2]`,
		`[tmp2]drawbox=x=0:y=0:w=7:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.16:t=fill[tmp3]`,
		`[tmp3]drawbox=x=682:y=0:w=58:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.10:t=fill[tmp4]`,
		`[tmp4]drawbox=x=732:y=0:w=8:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.98:t=fill[outv]`,
	];
	runFfmpeg(
		[
			"-i",
			basePath,
			"-i",
			topicReferencePath,
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
		"thumbnail_designer2_feed_restore",
	);
	ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
	if (typeof log === "function") {
		log("thumbnailDesigner2 feed image restored sharp", {
			path: path.basename(outputPath),
			source: path.basename(topicReferencePath),
		});
	}
	return outputPath;
}

async function generateComfyFirstThumbnailPackage(args = {}) {
	requireInternals();
	const {
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
	} = args;

	if (!presenterLocalPath) {
		throw new Error("thumbnail_presenter_missing_or_invalid");
	}
	ensureDir(tmpDir);
	ensureImageFile(presenterLocalPath, 5000);

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
	const styleProfile = enhanceDesigner2StyleProfile(
		chooseThumbnailStyleProfile(intent, styleContextText),
		styleContextText,
	);
	const accent =
		styleProfile.accent ||
		chooseAccentColor(intent, styleContextText) ||
		ACCENT_PALETTE.default;

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

	let topicReferencePaths = await collectTopicReferenceImages({
		topics,
		tmpDir,
		jobId,
		log,
	});
	let topicReferenceSource = topicReferencePaths.length ? "orchestrator" : "none";
	let generatedFeed = null;
	if (!topicReferencePaths.length) {
		try {
			generatedFeed = await generateComfyFeedReference({
				jobId,
				tmpDir,
				title,
				shortTitle,
				seoTitle,
				topics,
				contextText,
				styleProfile,
				log,
			});
			if (generatedFeed?.path) {
				topicReferencePaths = [generatedFeed.path];
				topicReferenceSource = "comfy_generated_fallback";
				if (typeof log === "function") {
					log("thumbnailDesigner2 comfy feed selected", {
						path: path.basename(generatedFeed.path),
					});
				}
			}
		} catch (error) {
			if (typeof log === "function") {
				log("thumbnailDesigner2 comfy feed unavailable", {
					error: error?.message || String(error),
				});
			}
		}
	}

	if (typeof log === "function") {
		log("thumbnail route selected", {
			primaryRoute: "comfyui_img2img_text_locked",
			fallbackRoutes: getComfyConfig().strict
				? []
				: ["thumbnailDesigner_openai_original"],
			preferTopicLead: Boolean(topicReferencePaths.length),
			intent,
			topicReferenceCount: topicReferencePaths.length,
			topicReferenceSource,
			primaryTopic: primaryTopicLabel(topics),
		});
	}

	const draft = renderDesigner2VisualSeed({
		jobId: `${jobId}_comfy_draft`,
		tmpDir,
		presenterLocalPath,
		topicReferencePaths,
		accent,
		log,
	});
	let comfyPlate = null;
	let feedRestoredPath = "";
	let presenterLockedPath = "";
	let finalPlate = null;
	try {
		comfyPlate = await generateComfyThumbnailPlate({
			jobId,
			tmpDir,
			draftPath: draft.path,
			title,
			shortTitle,
			seoTitle,
			topics,
			headline: textPlan.primaryHeadline,
			badgeText: textPlan.badgeText,
			sublineText: textPlan.sublineText,
			intent,
			styleProfile,
			hasFeedImage: Boolean(topicReferencePaths.length),
			feedSource: topicReferenceSource,
			log,
		});
		if (!comfyPlate?.path) throw new Error("comfyui_plate_missing");

		feedRestoredPath = restoreSharpFeedPanel({
			jobId,
			tmpDir,
			basePath: comfyPlate.path,
			topicReferencePath: topicReferencePaths[0],
			accent,
			log,
		});
		presenterLockedPath = lockPresenterPanel({
			jobId,
			tmpDir,
			basePath: feedRestoredPath || comfyPlate.path,
			presenterLocalPath,
			label: "comfy_presenter_locked",
			log,
		});
		finalPlate = renderLockedThumbnailTextOverlay({
			jobId,
			tmpDir,
			basePath: presenterLockedPath,
			headline: textPlan.primaryHeadline,
			badgeText: textPlan.badgeText,
			sublineText: textPlan.sublineText,
			accent,
			styleProfile,
			log,
		});
		finalPlate.method = "comfyui_img2img_text_locked";
		ensureThumbnailFile(finalPlate.path, THUMBNAIL_MIN_BYTES);

		if (typeof log === "function") {
			log("thumbnail route succeeded", {
				strategy: "comfyui_img2img_text_locked",
				method: finalPlate.method,
				path: path.basename(finalPlate.path || ""),
			});
		}
	} finally {
		cleanupComfyFile(
			comfyPlate?.outputPath,
			log,
			"thumbnailDesigner2 comfy plate cleaned",
		);
		if (comfyPlate?.path && comfyPlate.path !== comfyPlate.outputPath) {
			safeUnlink(comfyPlate.path);
		}
		safeUnlink(draft.path);
		if (feedRestoredPath && finalPlate?.path !== feedRestoredPath) {
			safeUnlink(feedRestoredPath);
		}
		if (presenterLockedPath && finalPlate?.path !== presenterLockedPath) {
			safeUnlink(presenterLockedPath);
		}
		if (generatedFeed?.outputPath) {
			cleanupComfyFile(
				generatedFeed.outputPath,
				log,
				"thumbnailDesigner2 comfy feed cleaned",
			);
		}
		if (generatedFeed?.path && generatedFeed.path !== generatedFeed.outputPath) {
			safeUnlink(generatedFeed.path);
		}
	}

	const uploaded = await uploadThumbnailToCloudinary(finalPlate.path, jobId);
	const variant = {
		variant: "comfyui",
		localPath: finalPlate.path,
		url: uploaded.url || "",
		publicId: uploaded.public_id || "",
		width: uploaded.width || THUMBNAIL_WIDTH,
		height: uploaded.height || THUMBNAIL_HEIGHT,
		title: textPlan.primaryHeadline,
		method: finalPlate.method,
	};

	return {
		localPath: finalPlate.path,
		url: uploaded.url || "",
		publicId: uploaded.public_id || "",
		width: uploaded.width || THUMBNAIL_WIDTH,
		height: uploaded.height || THUMBNAIL_HEIGHT,
		title: textPlan.primaryHeadline,
		pose,
		accent,
		style: styleProfile.id,
		method: finalPlate.method,
		variants: [variant],
		comfy: {
			model: comfyPlate.model,
			width: comfyPlate.width,
			height: comfyPlate.height,
			steps: comfyPlate.steps,
			cfg: comfyPlate.cfg,
			denoise: comfyPlate.denoise,
			feedSource: topicReferenceSource,
		},
	};
}

async function generateThumbnailPackage(args = {}) {
	try {
		return await generateComfyFirstThumbnailPackage(args);
	} catch (error) {
		const config = getComfyConfig();
		if (typeof args.log === "function") {
			args.log("thumbnailDesigner2 comfy route failed", {
				error: error?.message || String(error),
				fallback: !config.strict,
			});
		}
		if (config.strict) throw error;
		const result = await baseDesigner.generateThumbnailPackage(args);
		return {
			...result,
			designer: "thumbnailDesigner",
			method: result.method || "thumbnailDesigner_openai_original",
			comfy: null,
		};
	}
}

module.exports = {
	...baseDesigner,
	generateThumbnailPackage,
};
