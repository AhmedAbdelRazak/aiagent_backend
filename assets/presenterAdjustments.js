/** @format */

const fs = require("fs");
const os = require("os");
const path = require("path");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;
const { OpenAI } = require("openai");

let FormDataNode = null;
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	FormDataNode = require("form-data");
} catch {
	FormDataNode = null;
}

const RUNWAY_API_KEY = process.env.RUNWAYML_API_SECRET || "";
const RUNWAY_VERSION = "2024-11-06";
const RUNWAY_IMAGE_MODEL = "gen4_image";
const RUNWAY_IMAGE_POLL_INTERVAL_MS = 2000;
const RUNWAY_IMAGE_MAX_POLL_ATTEMPTS = 120;
const PRESENTER_MIN_BYTES = 12000;
const PRESENTER_CLOUDINARY_FOLDER = "aivideomatic/long_presenters";
const PRESENTER_CLOUDINARY_PUBLIC_PREFIX = "presenter_master";
const PRESENTER_OUTFIT_PREFIX = "presenter_outfit";
const CHAT_MODEL = "gpt-5.2";
const ORCHESTRATOR_PRESENTER_REF_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767066355/aivideomatic/long_presenters/presenter_master_4b76c718-6a2a-4749-895e-e05bd2b2ecfc_1767066355424.png";
const ORCHESTRATOR_CANDLE_REF_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767142335/aivideomatic/PresenterWithCandle_f6t83r.png";
const ORCHESTRATOR_CANDLE_PRODUCT_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767134899/aivideomatic/MyCandle_u9skio.png";

const openai = process.env.CHATGPT_API_TOKEN
	? new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN })
	: null;

function ensureDir(dirPath) {
	if (!dirPath) return;
	if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
}

function safeUnlink(p) {
	try {
		if (p && fs.existsSync(p)) fs.unlinkSync(p);
	} catch {}
}

function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function readFileHeader(filePath, bytes = 16) {
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
	)
		return "png";
	if (head[0] === 0xff && head[1] === 0xd8 && head[2] === 0xff) return "jpg";
	if (
		head.toString("ascii", 0, 4) === "RIFF" &&
		head.toString("ascii", 8, 12) === "WEBP"
	)
		return "webp";
	return null;
}

function ensurePresenterFile(filePath) {
	if (!filePath || !fs.existsSync(filePath))
		throw new Error("presenter_image_missing");
	const st = fs.statSync(filePath);
	if (!st || st.size < PRESENTER_MIN_BYTES)
		throw new Error("presenter_image_too_small");
	const kind = detectImageType(filePath);
	if (!kind) throw new Error("presenter_image_invalid");
	return filePath;
}

function buildTopicLine({ title, topics = [] }) {
	const topicLine = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join(" / ")
		: "";
	const raw = [title, topicLine].filter(Boolean).join(" | ");
	return String(raw || topicLine || title || "the topic").slice(0, 220);
}

function fallbackWardrobePrompt({ topicLine }) {
	return `
Use @presenter_ref for exact framing, pose, lighting, desk, and studio environment.
Change ONLY the outfit on the torso/upper body area to a dark, classy button-up shirt (open collar), optional open blazer.
Outfit colors must be dark only (charcoal, black, deep navy). No bright or light colors.
Face, glasses, beard, hairline, skin texture, and all facial features must remain EXACTLY the same. Do NOT touch or alter the face in any way. Single face only, no ghosting.
Studio background, desk, lighting, camera angle, and all props must remain EXACTLY the same.
No candles, no extra objects, no text, no logos. Topic context: ${topicLine}.
`.trim();
}

function fallbackCandlePrompt({ topicLine }) {
	return `
Use @presenter_ref for exact framing, pose, lighting, desk, and studio environment. Keep the outfit exactly the same as @presenter_ref.
Add @candle_ref candle on the desk to the right side behind the presenter, near the right edge, fully visible and grounded on the tabletop.
The candle jar is OPEN with NO lid visible. The candle is LIT with a tiny calm flame; no exaggerated glow.
Keep face identity EXACTLY the same; do NOT touch or alter the face in any way; single face only, no double exposure or ghosting.
No transparency on the candle; label and glass must be solid and crisp, with a soft natural shadow on the desk.
No other changes, no extra objects, no text, no logos. Topic context: ${topicLine}.
`.trim();
}

function parseJsonObject(text = "") {
	const raw = String(text || "").trim();
	if (!raw) return null;
	try {
		return JSON.parse(raw);
	} catch {}
	const start = raw.indexOf("{");
	const end = raw.lastIndexOf("}");
	if (start >= 0 && end > start) {
		const slice = raw.slice(start, end + 1);
		try {
			return JSON.parse(slice);
		} catch {}
	}
	return null;
}

async function buildOrchestratedPrompts({ title, topics, categoryLabel, log }) {
	const topicLine = buildTopicLine({ title, topics });
	if (!openai) {
		return {
			wardrobePrompt: fallbackWardrobePrompt({ topicLine }),
			candlePrompt: fallbackCandlePrompt({ topicLine }),
		};
	}

	const system = `
You write precise, regular descriptive prompts for Runway gen4_image.
Return JSON only with keys: wardrobePrompt, candlePrompt.
Rules:
- Use @presenter_ref as the only person reference.
- Study the provided reference images to match the studio framing and candle placement.
- Face is strictly locked: do NOT alter the face or any facial features in any way; no double face, no ghosting, no artifacts.
- Keep studio/desk/background/camera/lighting unchanged.
- Wardrobe: dark classy button-up (open collar), optional open blazer, dark colors only.
- Candle: add @candle_ref on the right side of the desk near the edge, fully visible, lid removed, lit with a tiny calm flame, natural shadow, no transparency, sitting on the tabletop (not floating).
- Candle must match the product reference (label, jar shape, proportions) while being normal size in scene.
- No extra objects, no text, no logos, no watermarks.
`.trim();

	const userText = `
Title: ${String(title || "").trim()}
Topics: ${topicLine}
Category: ${String(categoryLabel || "").trim()}
Study the reference images: 1) original presenter studio, 2) desired candle placement, 3) candle product reference.
Output JSON only.
`.trim();

	try {
		const resp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [
				{ role: "system", content: system },
				{
					role: "user",
					content: [
						{ type: "text", text: userText },
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_PRESENTER_REF_URL },
						},
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_CANDLE_REF_URL },
						},
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_CANDLE_PRODUCT_URL },
						},
					],
				},
			],
			temperature: 0.4,
			max_tokens: 500,
		});
		const content = String(resp?.choices?.[0]?.message?.content || "").trim();
		const parsed = parseJsonObject(content);
		if (parsed && parsed.wardrobePrompt && parsed.candlePrompt) {
			return {
				wardrobePrompt: String(parsed.wardrobePrompt).trim(),
				candlePrompt: String(parsed.candlePrompt).trim(),
			};
		}
	} catch (e) {
		if (log)
			log("prompt orchestrator failed; using fallback", {
				error: e?.message || String(e),
			});
	}

	return {
		wardrobePrompt: fallbackWardrobePrompt({ topicLine }),
		candlePrompt: fallbackCandlePrompt({ topicLine }),
	};
}

function runwayHeadersJson() {
	return {
		Authorization: `Bearer ${RUNWAY_API_KEY}`,
		"X-Runway-Version": RUNWAY_VERSION,
		"Content-Type": "application/json",
	};
}

async function runwayCreateEphemeralUpload({ filePath, filename }) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	if (!fs.existsSync(filePath))
		throw new Error("file missing for runway upload");

	const baseName = filename || path.basename(filePath || "asset.bin");
	const init = await axios.post(
		"https://api.dev.runwayml.com/v1/uploads",
		{ filename: baseName, type: "ephemeral" },
		{
			headers: runwayHeadersJson(),
			timeout: 20000,
			validateStatus: (s) => s < 500,
		}
	);
	if (init.status >= 300) {
		const msg =
			typeof init.data === "string"
				? init.data
				: JSON.stringify(init.data || {});
		throw new Error(
			`Runway upload init failed (${init.status}): ${msg.slice(0, 500)}`
		);
	}
	const { uploadUrl, fields, runwayUri } = init.data || {};
	if (!uploadUrl || !fields || !runwayUri)
		throw new Error("Runway upload init returned incomplete response");

	if (FormDataNode) {
		const form = new FormDataNode();
		Object.entries(fields || {}).forEach(([k, v]) => form.append(k, v));
		form.append("file", fs.createReadStream(filePath));
		const r = await axios.post(uploadUrl, form, {
			headers: form.getHeaders(),
			maxBodyLength: Infinity,
			maxContentLength: Infinity,
			timeout: 30000,
			validateStatus: (s) => s < 500,
		});
		if (r.status >= 300) throw new Error(`Runway upload failed (${r.status})`);
		return runwayUri;
	}

	if (typeof fetch === "function" && typeof FormData !== "undefined") {
		const form = new FormData();
		Object.entries(fields || {}).forEach(([k, v]) => form.append(k, v));
		const buf = fs.readFileSync(filePath);
		const blob = new Blob([buf]);
		form.append("file", blob, baseName);
		const resp = await fetch(uploadUrl, { method: "POST", body: form });
		if (!resp.ok) throw new Error(`Runway upload failed (${resp.status})`);
		return runwayUri;
	}

	throw new Error(
		"Runway upload requires Node 18+ (fetch/FormData) or install 'form-data'"
	);
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

async function runwayTextToImage({ promptText, referenceImages }) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	const payload = {
		model: RUNWAY_IMAGE_MODEL,
		promptText: String(promptText || "").slice(0, 1000),
		ratio: "1920:1080",
		...(Array.isArray(referenceImages) && referenceImages.length
			? { referenceImages }
			: {}),
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

async function downloadUrlToFile(url, outPath) {
	const res = await axios.get(url, {
		responseType: "arraybuffer",
		timeout: 60000,
	});
	if (!res?.data) throw new Error("download empty");
	fs.writeFileSync(outPath, res.data);
	return outPath;
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

async function uploadPresenterToCloudinary(filePath, jobId, prefix) {
	assertCloudinaryReady();
	const publicId = `${prefix}_${jobId}_${Date.now()}`;
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

async function generateRunwayOutfitStage({
	jobId,
	tmpDir,
	presenterLocalPath,
	wardrobePrompt,
	log,
}) {
	const presenterUri = await runwayCreateEphemeralUpload({
		filePath: presenterLocalPath,
		filename: path.basename(presenterLocalPath),
	});
	if (log)
		log("runway wardrobe prompt", {
			prompt: String(wardrobePrompt || "").slice(0, 200),
		});
	const outputUri = await runwayTextToImage({
		promptText: wardrobePrompt,
		referenceImages: [{ uri: presenterUri, tag: "presenter_ref" }],
	});
	const outPath = path.join(tmpDir, `presenter_outfit_${jobId}.png`);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	return outPath;
}

async function generateRunwayCandleStage({
	jobId,
	tmpDir,
	presenterCloudUrl,
	candleLocalPath,
	candlePrompt,
	log,
}) {
	const presenterPath = path.join(tmpDir, `presenter_cloud_${jobId}.png`);
	await downloadUrlToFile(presenterCloudUrl, presenterPath);
	const presenterUri = await runwayCreateEphemeralUpload({
		filePath: presenterPath,
		filename: path.basename(presenterPath),
	});
	const candleUri = await runwayCreateEphemeralUpload({
		filePath: candleLocalPath,
		filename: path.basename(candleLocalPath),
	});

	if (log)
		log("runway candle prompt", {
			prompt: String(candlePrompt || "").slice(0, 200),
		});
	const outputUri = await runwayTextToImage({
		promptText: candlePrompt,
		referenceImages: [
			{ uri: presenterUri, tag: "presenter_ref" },
			{ uri: candleUri, tag: "candle_ref" },
		],
	});
	const outPath = path.join(tmpDir, `presenter_final_${jobId}.png`);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	safeUnlink(presenterPath);
	return outPath;
}

async function generatePresenterAdjustedImage({
	jobId,
	tmpDir,
	presenterLocalPath,
	candleLocalPath,
	title,
	topics = [],
	categoryLabel,
	log,
}) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	if (!presenterLocalPath || !fs.existsSync(presenterLocalPath))
		throw new Error("presenter_base_missing");
	if (!candleLocalPath || !fs.existsSync(candleLocalPath))
		throw new Error("candle_base_missing");

	const workingDir = tmpDir || path.join(os.tmpdir(), "presenter_adjustments");
	ensureDir(workingDir);
	ensurePresenterFile(presenterLocalPath);

	const prompts = await buildOrchestratedPrompts({
		title,
		topics,
		categoryLabel,
		log,
	});

	let outfitPath = null;
	let outfitUpload = null;
	let finalPath = null;
	let finalUpload = null;

	try {
		outfitPath = await generateRunwayOutfitStage({
			jobId,
			tmpDir: workingDir,
			presenterLocalPath,
			wardrobePrompt: prompts.wardrobePrompt,
			log,
		});
		ensurePresenterFile(outfitPath);
		outfitUpload = await uploadPresenterToCloudinary(
			outfitPath,
			jobId,
			PRESENTER_OUTFIT_PREFIX
		);
	} catch (e) {
		if (log)
			log("runway wardrobe stage failed", {
				error: e?.message || String(e),
			});
		throw e;
	}

	try {
		finalPath = await generateRunwayCandleStage({
			jobId,
			tmpDir: workingDir,
			presenterCloudUrl: outfitUpload.url,
			candleLocalPath,
			candlePrompt: prompts.candlePrompt,
			log,
		});
		ensurePresenterFile(finalPath);
		finalUpload = await uploadPresenterToCloudinary(
			finalPath,
			jobId,
			PRESENTER_CLOUDINARY_PUBLIC_PREFIX
		);
	} catch (e) {
		if (log)
			log("runway candle stage failed; using wardrobe image", {
				error: e?.message || String(e),
			});
		finalPath = outfitPath;
		finalUpload = outfitUpload;
	}

	return {
		localPath: finalPath,
		url: finalUpload?.url || "",
		publicId: finalUpload?.public_id || "",
		width: finalUpload?.width || 0,
		height: finalUpload?.height || 0,
		method: "runway_two_stage",
	};
}

module.exports = {
	generatePresenterAdjustedImage,
};
