/** @format */

const crypto = require("crypto");
const axios = require("axios");
const baseDesigner = require("./thumbnailDesigner");

const DEFAULT_COMFY_URL = "http://127.0.0.1:8188";
const DEFAULT_MODEL = "Realistic_Vision_V6.0_NV_B1_fp16.safetensors";

function normalizeWhitespace(value = "") {
	return String(value || "")
		.replace(/\s+/g, " ")
		.trim();
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
		height: numberEnv("THUMBNAIL_DESIGNER2_COMFY_HEIGHT", 432, 256, 768),
		steps: numberEnv("THUMBNAIL_DESIGNER2_COMFY_STEPS", 22, 8, 36),
		cfg: numberEnv("THUMBNAIL_DESIGNER2_COMFY_CFG", 6.2, 3, 10),
		sampler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_SAMPLER || "dpmpp_2m",
		),
		scheduler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_SCHEDULER || "karras",
		),
		timeoutMs: numberEnv(
			"THUMBNAIL_DESIGNER2_COMFY_TIMEOUT_MS",
			35 * 60 * 1000,
			60 * 1000,
			60 * 60 * 1000,
		),
		pollMs: numberEnv("THUMBNAIL_DESIGNER2_COMFY_POLL_MS", 2500, 1000, 10000),
	};
}

function buildComfyPrompt({ title, shortTitle, seoTitle, topics = [] }) {
	const labels = (Array.isArray(topics) ? topics : [])
		.map((topic) => topic?.displayTopic || topic?.topic || topic?.title || "")
		.map(normalizeWhitespace)
		.filter(Boolean)
		.slice(0, 4);
	const topicText = labels.length ? labels.join(", ") : "current news story";
	const titleText =
		normalizeWhitespace(shortTitle) ||
		normalizeWhitespace(title) ||
		normalizeWhitespace(seoTitle) ||
		topicText;

	return normalizeWhitespace(`
		Premium photorealistic 16:9 YouTube thumbnail visual foundation.
		Topic: ${topicText}.
		Editorial angle: ${titleText}.
		Create a clean, high-contrast, dramatic but realistic scene with one obvious focal subject,
		strong depth, professional lighting, crisp details, premium news/editorial composition,
		vibrant but natural color, clean negative space for later headline text, cinematic lens,
		sharp focus, high-end commercial photography, mobile-readable composition.
		Do not include captions, labels, logos, watermarks, UI screenshots, posters, typography,
		random letters, misspelled text, or text blocks.
	`);
}

function buildNegativePrompt() {
	return normalizeWhitespace(`
		text, letters, words, logo, watermark, signature, subtitles, captions, UI,
		poster, flyer, magazine cover, fake screenshot, blurry, low quality, muddy,
		low contrast, noisy, deformed face, bad eyes, bad hands, bad anatomy, extra fingers,
		extra limbs, cropped subject, out of frame, duplicate people, plastic skin,
		waxy skin, cartoon, painting, illustration, anime, distorted perspective,
		oversaturated, overexposed, underexposed, cluttered composition
	`);
}

function randomSeed() {
	return Math.floor(Math.random() * 999999999999999);
}

function buildWorkflow(config, prompt) {
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
				width: config.width,
				height: config.height,
				batch_size: 1,
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
				filename_prefix: "agentai_thumbnail2_realistic",
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
	throw new Error("comfyui_thumbnail_seed_timeout");
}

function comfyImageUrl(config, image) {
	const params = new URLSearchParams({
		filename: image.filename || "",
		subfolder: image.subfolder || "",
		type: image.type || "output",
	});
	return `${config.url}/view?${params.toString()}`;
}

async function generateComfySeedReference({
	jobId,
	title,
	shortTitle,
	seoTitle,
	topics,
	log,
}) {
	const config = getComfyConfig();
	if (!config.enabled) return null;

	const prompt = buildComfyPrompt({ title, shortTitle, seoTitle, topics });
	if (typeof log === "function") {
		log("thumbnailDesigner2 comfy seed starting", {
			url: config.url,
			model: config.model,
			width: config.width,
			height: config.height,
			steps: config.steps,
			cfg: config.cfg,
			sampler: config.sampler,
			scheduler: config.scheduler,
		});
	}

	await comfyRequest(config, "GET", "/system_stats", null, { timeout: 8000 });
	const queued = await comfyRequest(config, "POST", "/prompt", {
		client_id: crypto.randomUUID(),
		prompt: buildWorkflow(config, prompt),
	});
	const promptId = queued?.prompt_id;
	if (!promptId) throw new Error("comfyui_prompt_id_missing");
	const image = await waitForComfyImage(config, promptId);
	const url = comfyImageUrl(config, image);

	if (typeof log === "function") {
		log("thumbnailDesigner2 comfy seed ready", {
			promptId,
			filename: image.filename || "",
			subfolder: image.subfolder || "",
			type: image.type || "output",
		});
	}

	return {
		url,
		model: config.model,
		width: config.width,
		height: config.height,
		method: "comfyui_realistic_vision_seed",
	};
}

function withComfySeedTopic(topics = [], seed) {
	if (!seed?.url) return topics;
	return [
		{
			displayTopic: "AI-generated thumbnail visual foundation",
			topic: "AI-generated thumbnail visual foundation",
			thumbnailImageUrls: [seed.url],
			thumbnailImageConfidence: "high",
			thumbnailImageSourceType: "comfyui",
		},
		...(Array.isArray(topics) ? topics : []),
	];
}

async function generateThumbnailPackage(args = {}) {
	let seed = null;
	try {
		seed = await generateComfySeedReference(args);
	} catch (error) {
		if (typeof args.log === "function") {
			args.log("thumbnailDesigner2 comfy seed unavailable", {
				error: error?.message || String(error),
				fallback: !getComfyConfig().strict,
			});
		}
		if (getComfyConfig().strict) throw error;
	}

	const result = await baseDesigner.generateThumbnailPackage({
		...args,
		topics: withComfySeedTopic(args.topics, seed),
	});

	return {
		...result,
		method: seed?.method ? `designer2_${result.method || "thumbnail"}` : result.method,
		designer: seed?.method ? "thumbnailDesigner2" : "thumbnailDesigner",
		comfySeed: seed
			? {
					model: seed.model,
					width: seed.width,
					height: seed.height,
					method: seed.method,
				}
			: null,
	};
}

module.exports = {
	...baseDesigner,
	generateThumbnailPackage,
};
