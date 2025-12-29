/** @format */

const fs = require("fs");
const path = require("path");
const axios = require("axios");
const { OpenAI, toFile } = require("openai");

const DEFAULT_IMAGE_MODEL = "gpt-image-1";
const DEFAULT_IMAGE_SIZE = "1536x1024";
const DEFAULT_IMAGE_QUALITY = "high";
const DEFAULT_IMAGE_INPUT_FIDELITY = "high";
const MAX_INPUT_IMAGES = 4;

function cleanThumbnailText(text = "") {
	return String(text || "")
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
Composition: presenter on the right third (face and shoulders fully inside the right third), leave the left ~45% clean for headline text.
${topicImageLine}
Style: ultra sharp, clean, premium, high contrast, cinematic studio lighting, shallow depth of field, crisp subject separation.
Expression: confident, intrigued, camera-ready.
No candles, no logos, no watermarks, no extra people, no extra hands, no distortion, no text.
`.trim();
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
		if (head.toString("ascii", 0, 4) === "RIFF" &&
			head.toString("ascii", 8, 12) === "WEBP")
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

async function withRetries(fn, { retries = 1, baseDelayMs = 900, log } = {}) {
	let lastErr = null;
	for (let attempt = 0; attempt <= retries; attempt++) {
		try {
			return await fn(attempt);
		} catch (e) {
			lastErr = e;
			if (attempt >= retries) throw e;
			const delay = Math.round(
				baseDelayMs * Math.pow(2, attempt) + Math.random() * 150
			);
			if (log) log("thumbnail openai retry", { attempt: attempt + 1, delay });
			await sleep(delay);
		}
	}
	throw lastErr || new Error("thumbnail openai retry failed");
}

async function downloadUrlToFile(url, outPath) {
	const res = await axios.get(url, {
		responseType: "arraybuffer",
		timeout: 60000,
	});
	if (!res?.data) throw new Error("thumbnail download empty");
	fs.writeFileSync(outPath, res.data);
	return outPath;
}

async function generateOpenAiThumbnailBase({
	jobId,
	tmpDir,
	prompt,
	imagePaths = [],
	openai,
	model = DEFAULT_IMAGE_MODEL,
	size = DEFAULT_IMAGE_SIZE,
	quality = DEFAULT_IMAGE_QUALITY,
	inputFidelity = DEFAULT_IMAGE_INPUT_FIDELITY,
	log,
}) {
	const cleanPrompt = String(prompt || "").trim();
	if (!cleanPrompt) throw new Error("thumbnail_prompt_missing");
	const apiKey = process.env.CHATGPT_API_TOKEN;
	if (!apiKey) throw new Error("CHATGPT_API_TOKEN missing");
	const client = openai || new OpenAI({ apiKey });

	const outPath = path.join(tmpDir, `thumb_openai_${jobId}.png`);

	const doRequest = async () => {
		if (imagePaths.length) {
			const inputs = [];
			for (const p of imagePaths) {
				try {
					const buf = fs.readFileSync(p);
					const mime = inferImageMime(p);
					if (!mime) continue;
					inputs.push(await toFile(buf, path.basename(p), { type: mime }));
				} catch {}
			}
			if (inputs.length) {
				return await client.images.edit({
					model,
					image: inputs.length === 1 ? inputs[0] : inputs,
					prompt: cleanPrompt,
					size,
					quality,
					output_format: "png",
					background: "auto",
					input_fidelity: inputFidelity,
				});
			}
		}
		return await client.images.generate({
			model,
			prompt: cleanPrompt,
			size,
			quality,
			output_format: "png",
			background: "auto",
		});
	};

	const resp = await withRetries(doRequest, {
		retries: 1,
		baseDelayMs: 900,
		log,
	});

	const image = resp?.data?.[0];
	if (image?.b64_json) {
		const buf = Buffer.from(String(image.b64_json), "base64");
		fs.writeFileSync(outPath, buf);
		return outPath;
	}
	if (image?.url) {
		await downloadUrlToFile(image.url, outPath);
		return outPath;
	}

	throw new Error("thumbnail_openai_empty");
}

module.exports = {
	buildThumbnailPrompt,
	collectThumbnailInputImages,
	generateOpenAiThumbnailBase,
};
