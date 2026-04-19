/** @format */

const fs = require("fs");
const path = require("path");
const axios = require("axios");
const { OpenAI, toFile } = require("openai");

const OPENAI_IMAGE_MODEL = "gpt-image-1.5";
const OPENAI_IMAGE_DEFAULT_QUALITY = "high";
const OPENAI_IMAGE_DEFAULT_OUTPUT_FORMAT = "png";

let client = null;

function getOpenAIApiKey() {
	return (
		String(process.env.CHATGPT_API_TOKEN || "").trim() ||
		String(process.env.OPENAI_API_KEY || "").trim()
	);
}

function getOpenAIImageClient() {
	const apiKey = getOpenAIApiKey();
	if (!apiKey) return null;
	if (!client) client = new OpenAI({ apiKey });
	return client;
}

function assertOpenAIImageReady() {
	const apiKey = getOpenAIApiKey();
	if (!apiKey) {
		throw new Error(
			"OpenAI API key missing (set CHATGPT_API_TOKEN or OPENAI_API_KEY).",
		);
	}
	return getOpenAIImageClient();
}

function mimeTypeFromPath(filePath = "") {
	const ext = path.extname(String(filePath || "")).toLowerCase();
	if (ext === ".png") return "image/png";
	if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
	if (ext === ".webp") return "image/webp";
	return "application/octet-stream";
}

async function toUploadableFromPath(filePath) {
	if (!filePath || !fs.existsSync(filePath)) {
		throw new Error(`image_file_missing:${filePath || "unknown"}`);
	}
	return await toFile(fs.createReadStream(filePath), path.basename(filePath), {
		type: mimeTypeFromPath(filePath),
	});
}

async function writeGeneratedImageToPath(imageData, outPath) {
	if (!outPath) throw new Error("openai_image_output_path_missing");
	if (!imageData) throw new Error("openai_image_response_missing");
	fs.mkdirSync(path.dirname(outPath), { recursive: true });

	if (imageData.b64_json) {
		fs.writeFileSync(outPath, Buffer.from(imageData.b64_json, "base64"));
		return outPath;
	}

	if (imageData.url) {
		const res = await axios.get(imageData.url, {
			responseType: "arraybuffer",
			timeout: 30000,
			validateStatus: (status) => status < 500,
		});
		if (res.status >= 300) {
			throw new Error(`openai_image_download_failed:${res.status}`);
		}
		fs.writeFileSync(outPath, Buffer.from(res.data));
		return outPath;
	}

	throw new Error("openai_image_data_missing");
}

async function saveImageResultToPath(result, outPath, index = 0) {
	const imageData = Array.isArray(result?.data) ? result.data[index] : null;
	return await writeGeneratedImageToPath(imageData, outPath);
}

function pickOpenAIImageSize({
	width,
	height,
	preferLandscape = true,
} = {}) {
	const w = Number(width) || 0;
	const h = Number(height) || 0;
	if (w && h) {
		if (w / h > 1.05) return "1536x1024";
		if (h / w > 1.05) return "1024x1536";
		return "1024x1024";
	}
	return preferLandscape ? "1536x1024" : "1024x1536";
}

async function generateImageToPath({
	prompt,
	outPath,
	size = "1536x1024",
	quality = OPENAI_IMAGE_DEFAULT_QUALITY,
	background = "opaque",
	moderation = "auto",
	outputFormat = OPENAI_IMAGE_DEFAULT_OUTPUT_FORMAT,
	user,
}) {
	const openai = assertOpenAIImageReady();
	const result = await openai.images.generate({
		model: OPENAI_IMAGE_MODEL,
		prompt: String(prompt || "").trim(),
		size,
		quality,
		background,
		moderation,
		output_format: outputFormat,
		user,
	});
	await saveImageResultToPath(result, outPath);
	return {
		path: outPath,
		model: OPENAI_IMAGE_MODEL,
	};
}

async function editImageToPath({
	prompt,
	imagePaths,
	maskPath,
	outPath,
	size = "auto",
	quality = OPENAI_IMAGE_DEFAULT_QUALITY,
	background = "opaque",
	moderation = "auto",
	outputFormat = OPENAI_IMAGE_DEFAULT_OUTPUT_FORMAT,
	inputFidelity = "high",
	user,
}) {
	const openai = assertOpenAIImageReady();
	const rawPaths = Array.isArray(imagePaths) ? imagePaths : [imagePaths];
	const uploadables = [];
	for (const filePath of rawPaths.filter(Boolean)) {
		uploadables.push(await toUploadableFromPath(filePath));
	}
	if (!uploadables.length) throw new Error("openai_edit_missing_source_images");

	const request = {
		model: OPENAI_IMAGE_MODEL,
		image: uploadables.length === 1 ? uploadables[0] : uploadables,
		prompt: String(prompt || "").trim(),
		size,
		quality,
		background,
		moderation,
		output_format: outputFormat,
		input_fidelity: inputFidelity,
		user,
	};
	if (maskPath) request.mask = await toUploadableFromPath(maskPath);

	const result = await openai.images.edit(request);
	await saveImageResultToPath(result, outPath);
	return {
		path: outPath,
		model: OPENAI_IMAGE_MODEL,
	};
}

module.exports = {
	OPENAI_IMAGE_MODEL,
	assertOpenAIImageReady,
	editImageToPath,
	generateImageToPath,
	getOpenAIApiKey,
	getOpenAIImageClient,
	pickOpenAIImageSize,
	toUploadableFromPath,
	writeGeneratedImageToPath,
};
