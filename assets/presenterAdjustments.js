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
const PRESENTER_CANDLE_PREFIX = "presenter_candle";
const PRESENTER_BASE_PREFIX = "presenter_base";

// OpenAI
const CHAT_MODEL = "gpt-5.2";

// Reference images (Cloudinary URLs)
const ORCHESTRATOR_PRESENTER_REF_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767066355/aivideomatic/long_presenters/presenter_master_4b76c718-6a2a-4749-895e-e05bd2b2ecfc_1767066355424.png";
const ORCHESTRATOR_CANDLE_REF_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767142335/aivideomatic/PresenterWithCandle_f6t83r.png";
const ORCHESTRATOR_CANDLE_PRODUCT_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767134899/aivideomatic/MyCandle_u9skio.png";

// Candle placement calibrated from ORCHESTRATOR_CANDLE_REF_URL relative to ORCHESTRATOR_PRESENTER_REF_URL.
// Coordinates are in the 1280x720 reference frame (top-left origin).
// These values were selected to match the candle's position on the right/back desk.
const CANDLE_PLACEMENT_REF = {
	baseW: 1280,
	baseH: 720,
	// top-left corner for candle overlay
	x: 1021,
	y: 441,
	// target overlay size (approx jar+flame), used for scaling
	w: 160,
	h: 210,
};

// How many times to refine Cloudinary compositing placement (cheap, deterministic)
const COMPOSITE_MAX_ATTEMPTS = 3;

// If Cloudinary background removal returns 423 while preparing derived assets, retry download
const CLOUDINARY_DERIVED_423_MAX_RETRIES = 10;
const CLOUDINARY_DERIVED_423_SLEEP_MS = 1200;

const WARDROBE_VARIANTS = [
	"dark charcoal matte button-up, open collar, no blazer",
	"deep navy textured button-up, open collar, unstructured dark blazer",
	"black band-collar button-up, no blazer",
	"dark graphite micro-pattern button-up, open collar, soft knit blazer",
	"dark slate button-up, open collar, open blazer with subtle texture",
	"black button-up with subtle sheen, open collar, slim dark blazer",
	"deep navy oxford button-up, open collar, no blazer",
	"charcoal button-up with thin pinstripe, open collar, open blazer",
	"black button-up with hidden placket, open collar, no blazer",
	"midnight-blue button-up, open collar, relaxed dark blazer",
];

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

function clampInt(n, min, max) {
	const v = Number.isFinite(Number(n)) ? Number(n) : 0;
	return Math.max(min, Math.min(max, Math.round(v)));
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

function ensureImageFile(filePath, minBytes = 2000) {
	if (!filePath || !fs.existsSync(filePath)) throw new Error("image_missing");
	const st = fs.statSync(filePath);
	if (!st || st.size < minBytes) throw new Error("image_too_small");
	const kind = detectImageType(filePath);
	if (!kind) throw new Error("image_invalid");
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

function hashStringToInt(value = "") {
	let hash = 0;
	const str = String(value || "");
	for (let i = 0; i < str.length; i++) {
		hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
	}
	return hash;
}

function pickWardrobeVariant({ jobId, title, topics }) {
	const topicText = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join("|")
		: "";
	const jitter = `${Date.now()}-${Math.random()}`;
	const seed = `${jobId || ""}|${title || ""}|${topicText}|${jitter}`;
	const idx = hashStringToInt(seed) % WARDROBE_VARIANTS.length;
	return WARDROBE_VARIANTS[idx] || WARDROBE_VARIANTS[0];
}

function fallbackWardrobePrompt({ topicLine, wardrobeVariant }) {
	return `
Use @presenter_ref for exact framing, pose, lighting, desk, and studio environment.
Change ONLY the outfit on the torso/upper body area to a dark, classy outfit. Outfit spec (use exactly): ${wardrobeVariant}.
Outfit colors must be dark only (charcoal, black, deep navy). No bright or light colors.
Do NOT alter the face or head at all. Keep glasses, beard, hairline, skin texture, and facial features exactly as in @presenter_ref. Single face only, no ghosting.
Studio background, desk, lighting, camera angle, and all props must remain EXACTLY the same.
DO NOT add borders, frames, vignettes, letterboxing, or extra processing.
No candles, no extra objects, no text, no logos. Topic context: ${topicLine}.
`.trim();
}

function fallbackCandleProductPrompt() {
	return `
Use @candle_ref to create a clean candle CUTOUT that matches the reference candle jar/label.
HARD REQUIREMENTS:
- Lid/cap must be COMPLETELY REMOVED and NOT visible anywhere in frame.
- Candle is LIT with a tiny calm flame (no large flame, no glow).
- Label/branding must remain EXACT, sharp, readable, and undistorted; do NOT redraw or change label art/text.
- Output MUST be a PNG with TRANSPARENT BACKGROUND (alpha). No white/gray backdrop, no shadows.
- Candle centered, upright, normal proportions (no warping).
No extra objects or text.
`.trim();
}

function fallbackFinalPrompt({ topicLine }) {
	return `
Use @presenter_ref for exact framing, pose, lighting, desk, and studio environment. Keep the outfit exactly the same as @presenter_ref.
Match the candle placement and size to the provided candle placement reference image (same relative offset and scale).
Add @candle_ref candle on the back table/desk to the right side behind the presenter, near the right edge, fully visible and grounded on the tabletop.
The candle jar is OPEN with NO lid visible. The candle is LIT with a tiny calm flame; no exaggerated glow.
Do NOT alter the face or head at all; keep it exactly as in @presenter_ref. Single face only, no double exposure or ghosting.
No transparency on the candle; label text/logo must remain EXACT and crisp, glass must be solid, with a soft natural shadow on the desk.
Keep candle size natural and slightly smaller than the presenter; do not exaggerate scale.
Only add the candle; do NOT change any other pixels or elements in the scene.
No other changes, no extra objects, and no added text/logos beyond the candle label. Topic context: ${topicLine}.
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

async function buildOrchestratedPrompts({
	jobId,
	title,
	topics,
	categoryLabel,
	log,
}) {
	const topicLine = buildTopicLine({ title, topics });
	const wardrobeVariant = pickWardrobeVariant({ jobId, title, topics });
	if (log)
		log("wardrobe variation selected", {
			variant: wardrobeVariant,
		});
	if (!openai) {
		const fallback = {
			wardrobePrompt: fallbackWardrobePrompt({
				topicLine,
				wardrobeVariant,
			}),
			candleProductPrompt: fallbackCandleProductPrompt(),
			finalPrompt: fallbackFinalPrompt({ topicLine }),
		};
		if (log)
			log("orchestrator prompts (fallback)", {
				wardrobe: fallback.wardrobePrompt.slice(0, 300),
				candleProduct: fallback.candleProductPrompt.slice(0, 300),
				final: fallback.finalPrompt.slice(0, 300),
			});
		return fallback;
	}

	const system = `
You write precise, regular descriptive prompts for Runway gen4_image.
Return JSON only with keys: wardrobePrompt, candleProductPrompt, finalPrompt.
Rules:
- Use @presenter_ref as the only person reference.
- Study the provided reference images to match the studio framing and the candle placement.
- Face/head are strictly locked: do NOT alter the face/head, hairline, glasses, beard, skin texture, expression. No double face, no ghosting.
- Keep studio/desk/background/camera/framing/lighting unchanged; DO NOT add borders/frames/vignettes/letterboxing.
- Wardrobe: vary the outfit each run using the provided wardrobe variation cue; include it exactly. Dark colors only.
- Candle product: use @candle_ref to create a clean candle CUTOUT with lid removed, tiny calm flame, exact label/branding. Output must be PNG with transparent background (alpha). No shadows.
- Final: (fallback-only) add the candle on the right/back desk matching the placement reference. Only add the candle; do not change any other pixels.
- No extra objects and no added text/logos beyond the candle label.
`.trim();

	const userText = `
Title: ${String(title || "").trim()}
Topics: ${topicLine}
Category: ${String(categoryLabel || "").trim()}
Wardrobe variation cue (use exactly): ${wardrobeVariant}
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
			max_completion_tokens: 500,
		});
		const content = String(resp?.choices?.[0]?.message?.content || "").trim();
		const parsed = parseJsonObject(content);
		if (
			parsed &&
			parsed.wardrobePrompt &&
			parsed.candleProductPrompt &&
			parsed.finalPrompt
		) {
			const result = {
				wardrobePrompt: String(parsed.wardrobePrompt).trim(),
				candleProductPrompt: String(parsed.candleProductPrompt).trim(),
				finalPrompt: String(parsed.finalPrompt).trim(),
			};
			if (log)
				log("orchestrator prompts", {
					wardrobe: result.wardrobePrompt.slice(0, 300),
					candleProduct: result.candleProductPrompt.slice(0, 300),
					final: result.finalPrompt.slice(0, 300),
				});
			return result;
		}
	} catch (e) {
		if (log)
			log("prompt orchestrator failed; using fallback", {
				error: e?.message || String(e),
			});
	}

	const fallback = {
		wardrobePrompt: fallbackWardrobePrompt({
			topicLine,
			wardrobeVariant,
		}),
		candleProductPrompt: fallbackCandleProductPrompt(),
		finalPrompt: fallbackFinalPrompt({ topicLine }),
	};
	if (log)
		log("orchestrator prompts (fallback)", {
			wardrobe: fallback.wardrobePrompt.slice(0, 300),
			candleProduct: fallback.candleProductPrompt.slice(0, 300),
			final: fallback.finalPrompt.slice(0, 300),
		});
	return fallback;
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

function stableSeedFrom(jobId, stage, attempt = 1) {
	// Keep seeds stable-ish per job/stage so retries are more comparable.
	const base = `${jobId || ""}|${stage || ""}|${attempt || 1}`;
	return hashStringToInt(base) % 2147483647;
}

async function runwayTextToImage({ promptText, referenceImages, ratio, seed }) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	const payload = {
		model: RUNWAY_IMAGE_MODEL,
		promptText: String(promptText || "").slice(0, 1000),
		ratio: String(ratio || "1920:1080"),
		...(Number.isInteger(seed) ? { seed } : {}),
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

async function downloadUrlToFile(url, outPath, opts = {}) {
	const {
		timeoutMs = 60000,
		maxRetries = 0,
		retrySleepMs = 1000,
		retryOnStatuses = [],
	} = opts || {};

	let lastErr = null;
	for (let attempt = 0; attempt <= maxRetries; attempt++) {
		try {
			const res = await axios.get(url, {
				responseType: "arraybuffer",
				timeout: timeoutMs,
				validateStatus: (s) => s < 500,
			});
			if (res.status >= 300) {
				const shouldRetry = retryOnStatuses.includes(res.status);
				if (shouldRetry && attempt < maxRetries) {
					await sleep(retrySleepMs);
					continue;
				}
				throw new Error(`download failed (${res.status})`);
			}
			if (!res?.data) throw new Error("download empty");
			fs.writeFileSync(outPath, res.data);
			return outPath;
		} catch (e) {
			lastErr = e;
			if (attempt < maxRetries) {
				await sleep(retrySleepMs);
				continue;
			}
		}
	}
	throw lastErr || new Error("download failed");
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

async function deleteCloudinaryAsset(publicId, log) {
	if (!publicId) return;
	assertCloudinaryReady();
	try {
		await cloudinary.uploader.destroy(publicId, { resource_type: "image" });
		if (log)
			log("cloudinary asset removed", {
				publicId,
			});
	} catch (e) {
		if (log)
			log("cloudinary asset delete failed", {
				publicId,
				error: e?.message || String(e),
			});
	}
}

function cloudinaryOverlayId(publicId) {
	// Cloudinary overlay URLs replace / with :
	return String(publicId || "").replace(/\//g, ":");
}

function computeScaledCandlePlacement({ targetW, targetH, tweak = {} }) {
	const baseW = CANDLE_PLACEMENT_REF.baseW;
	const baseH = CANDLE_PLACEMENT_REF.baseH;
	const scaleW = targetW / baseW;
	const scaleH = targetH / baseH;
	const scale =
		Number.isFinite(scaleW) && Number.isFinite(scaleH)
			? Math.min(scaleW, scaleH)
			: 1;
	const w = clampInt(
		CANDLE_PLACEMENT_REF.w * (tweak.scale || 1) * scale,
		16,
		Math.max(16, targetW)
	);
	const h = clampInt(
		CANDLE_PLACEMENT_REF.h * (tweak.scale || 1) * scale,
		16,
		Math.max(16, targetH)
	);
	// Keep the candle fully in-frame
	const maxX = Math.max(0, targetW - w);
	const maxY = Math.max(0, targetH - h);
	const x = clampInt(
		(CANDLE_PLACEMENT_REF.x + (tweak.dx || 0)) * scale,
		0,
		maxX
	);
	const y = clampInt(
		(CANDLE_PLACEMENT_REF.y + (tweak.dy || 0)) * scale,
		0,
		maxY
	);
	return { x, y, w, h, scale };
}

async function reviewCandleProduct({ candleUrl, promptUsed, attempt, log }) {
	if (!openai) {
		return {
			accept: true,
			reason: "review_skipped_no_openai",
			improvedPrompt: "",
		};
	}

	const system = `
You are a strict quality reviewer for a candle CUTOUT image meant for compositing.
Return JSON only with keys: accept (boolean), reason (string), improvedPrompt (string).
Accept only if:
- Lid/cap is fully removed (NO lid visible anywhere).
- Candle is centered, upright, normal proportions (no warping).
- Flame is tiny and calm (no large flame, no big glow).
- Label/branding is readable and not obviously mangled (prioritize legibility).
- Background is transparent (alpha) OR very clean and easy to remove; prefer transparent PNG.
- No extra objects or text; no watermarks.
IMPORTANT: The product reference image may have a lid — that is OK. Your requirement is LID REMOVED in the generated candle.
If reject, provide a revised candle product prompt to fix the issues.
`.trim();

	const userText = `
Attempt: ${Number(attempt || 1)}
Prompt used: ${String(promptUsed || "").slice(0, 700)}
Review the generated candle image against the product reference (jar/label), BUT enforce lid removed. Output JSON only.
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
						{ type: "image_url", image_url: { url: candleUrl } },
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_CANDLE_PRODUCT_URL },
						},
					],
				},
			],
			temperature: 0.2,
			max_completion_tokens: 300,
		});
		const content = String(resp?.choices?.[0]?.message?.content || "").trim();
		const parsed = parseJsonObject(content);
		if (parsed && typeof parsed.accept === "boolean") {
			return {
				accept: Boolean(parsed.accept),
				reason: String(parsed.reason || "").trim(),
				improvedPrompt: String(parsed.improvedPrompt || "").trim(),
			};
		}
		if (log)
			log("candle product review parse failed", {
				attempt,
				response: content.slice(0, 500),
			});
	} catch (e) {
		if (log)
			log("candle product review failed", {
				error: e?.message || String(e),
			});
	}

	return {
		accept: false,
		reason: "review_parse_failed",
		improvedPrompt: `${String(
			promptUsed || ""
		).trim()}\nAdjust to: lid removed, tiny flame, crisp label text, and output PNG with transparent background.`,
	};
}

async function reviewOutfitIntegrity({
	baseUrl,
	outfitUrl,
	promptUsed,
	attempt,
	log,
}) {
	if (!openai) {
		return {
			accept: true,
			reason: "review_skipped_no_openai",
			improvedPrompt: "",
		};
	}

	const system = `
You are a strict reviewer for a presenter wardrobe-change image.
Return JSON only with keys: accept (boolean), reason (string), improvedPrompt (string).
Accept only if:
- Presenter face/head (hairline, glasses, beard, skin texture, expression) is unchanged vs base.
- Studio, desk, camera angle, framing, lighting are unchanged.
- NO borders, vignettes, letterboxing, added frames, or weird post-processing.
- Only the wardrobe/clothing changed on the torso area, consistent with the prompt.
If reject, provide an improved wardrobe prompt that emphasizes strict locking and removing borders/artifacts.
`.trim();

	const userText = `
Attempt: ${Number(attempt || 1)}
Prompt used: ${String(promptUsed || "").slice(0, 700)}
Review the wardrobe result vs the base presenter. Output JSON only.
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
						{ type: "image_url", image_url: { url: outfitUrl } },
						{ type: "image_url", image_url: { url: baseUrl } },
					],
				},
			],
			temperature: 0.2,
			max_completion_tokens: 260,
		});
		const content = String(resp?.choices?.[0]?.message?.content || "").trim();
		const parsed = parseJsonObject(content);
		if (parsed && typeof parsed.accept === "boolean") {
			return {
				accept: Boolean(parsed.accept),
				reason: String(parsed.reason || "").trim(),
				improvedPrompt: String(parsed.improvedPrompt || "").trim(),
			};
		}
		if (log)
			log("wardrobe review parse failed", {
				attempt,
				response: content.slice(0, 500),
			});
	} catch (e) {
		if (log)
			log("wardrobe review failed", {
				error: e?.message || String(e),
			});
	}

	return {
		accept: false,
		reason: "review_parse_failed",
		improvedPrompt: "",
	};
}

async function reviewCompositePlacement({ finalUrl, attempt, log }) {
	if (!openai) {
		return {
			accept: true,
			reason: "review_skipped_no_openai",
			deltaX: 0,
			deltaY: 0,
			scaleMultiplier: 1,
		};
	}

	const system = `
You are a strict reviewer for candle placement in a presenter studio image.
Return JSON only with keys: accept (boolean), reason (string), deltaX (integer), deltaY (integer), scaleMultiplier (number).
Goals:
- Candle placement must closely match the reference placement image (right/back desk near edge).
- Candle size must match the reference (not tiny, not oversized).
- Presenter and studio must remain unchanged.
If ACCEPT = false, suggest small adjustments:
- deltaX: move candle right (+) or left (-) by pixels.
- deltaY: move candle down (+) or up (-) by pixels.
- scaleMultiplier: e.g., 1.05 to slightly enlarge, 0.95 to slightly shrink.
Keep adjustments small: |deltaX| <= 60, |deltaY| <= 60, scaleMultiplier between 0.85 and 1.20.
`.trim();

	const userText = `
Attempt: ${Number(attempt || 1)}
Compare the generated image to the candle placement reference. Output JSON only.
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
						{ type: "image_url", image_url: { url: finalUrl } },
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_CANDLE_REF_URL },
						},
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_PRESENTER_REF_URL },
						},
					],
				},
			],
			temperature: 0.2,
			max_completion_tokens: 260,
		});
		const content = String(resp?.choices?.[0]?.message?.content || "").trim();
		const parsed = parseJsonObject(content);
		if (parsed && typeof parsed.accept === "boolean") {
			const dx = clampInt(parsed.deltaX ?? 0, -60, 60);
			const dy = clampInt(parsed.deltaY ?? 0, -60, 60);
			let sm = Number(parsed.scaleMultiplier ?? 1);
			if (!Number.isFinite(sm)) sm = 1;
			sm = Math.max(0.85, Math.min(1.2, sm));
			return {
				accept: Boolean(parsed.accept),
				reason: String(parsed.reason || "").trim(),
				deltaX: dx,
				deltaY: dy,
				scaleMultiplier: sm,
			};
		}
		if (log)
			log("composite placement review parse failed", {
				attempt,
				response: content.slice(0, 400),
			});
	} catch (e) {
		if (log)
			log("composite placement review failed", {
				error: e?.message || String(e),
			});
	}

	return {
		accept: true,
		reason: "review_failed_openai_error",
		deltaX: 0,
		deltaY: 0,
		scaleMultiplier: 1,
	};
}

async function generateRunwayOutfitStage({
	jobId,
	tmpDir,
	presenterLocalPath,
	wardrobePrompt,
	attempt = 1,
	log,
}) {
	const presenterUri = await runwayCreateEphemeralUpload({
		filePath: presenterLocalPath,
		filename: path.basename(presenterLocalPath),
	});
	if (log)
		log("runway wardrobe prompt", {
			attempt,
			prompt: String(wardrobePrompt || "").slice(0, 220),
		});
	const outputUri = await runwayTextToImage({
		promptText: wardrobePrompt,
		referenceImages: [{ uri: presenterUri, tag: "presenter_ref" }],
		ratio: "1280:720",
		seed: stableSeedFrom(jobId, "wardrobe", attempt),
	});
	const outPath = path.join(tmpDir, `presenter_outfit_${jobId}_${attempt}.png`);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	return outPath;
}

async function generateRunwayCandleProductStage({
	jobId,
	tmpDir,
	candleLocalPath,
	candleProductPrompt,
	attempt = 1,
	log,
}) {
	const candleUri = await runwayCreateEphemeralUpload({
		filePath: candleLocalPath,
		filename: path.basename(candleLocalPath),
	});

	if (log)
		log("runway candle product prompt", {
			attempt,
			prompt: String(candleProductPrompt || "").slice(0, 220),
		});
	const outputUri = await runwayTextToImage({
		promptText: candleProductPrompt,
		referenceImages: [{ uri: candleUri, tag: "candle_ref" }],
		ratio: "1024:1024",
		seed: stableSeedFrom(jobId, "candle", attempt),
	});
	const outPath = path.join(tmpDir, `candle_product_${jobId}_${attempt}.png`);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	return outPath;
}

async function generateRunwayFinalStage({
	jobId,
	tmpDir,
	presenterCloudUrl,
	candleCloudUrl,
	finalPrompt,
	attempt = 1,
	log,
}) {
	const presenterPath = path.join(
		tmpDir,
		`presenter_cloud_${jobId}_${attempt}.png`
	);
	await downloadUrlToFile(presenterCloudUrl, presenterPath);
	const candlePath = path.join(tmpDir, `candle_cloud_${jobId}_${attempt}.png`);
	await downloadUrlToFile(candleCloudUrl, candlePath);
	const presenterUri = await runwayCreateEphemeralUpload({
		filePath: presenterPath,
		filename: path.basename(presenterPath),
	});
	const candleUri = await runwayCreateEphemeralUpload({
		filePath: candlePath,
		filename: path.basename(candlePath),
	});

	if (log)
		log("runway final prompt", {
			attempt,
			prompt: String(finalPrompt || "").slice(0, 220),
		});
	const outputUri = await runwayTextToImage({
		promptText: finalPrompt,
		referenceImages: [
			{ uri: presenterUri, tag: "presenter_ref" },
			{ uri: candleUri, tag: "candle_ref" },
		],
		ratio: "1280:720",
		seed: stableSeedFrom(jobId, "final", attempt),
	});
	const outPath = path.join(tmpDir, `presenter_final_${jobId}_${attempt}.png`);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	safeUnlink(presenterPath);
	safeUnlink(candlePath);
	return outPath;
}

async function buildCloudinaryCompositeUrl({
	basePublicId,
	candlePublicId,
	placement,
	useBackgroundRemoval = true,
}) {
	assertCloudinaryReady();

	const overlayId = cloudinaryOverlayId(candlePublicId);

	const t = [];
	// Place the candle overlay with a predictable size and location.
	// All transformations between overlay and layer_apply affect the overlay.
	const overlayTransform = {
		overlay: overlayId,
		width: placement.w,
		height: placement.h,
		crop: "scale",
	};
	if (useBackgroundRemoval) {
		// Requires Cloudinary background removal add-on. On first request can return 423.
		overlayTransform.effect = "background_removal";
	}

	t.push(overlayTransform);
	t.push({
		flags: "layer_apply",
		gravity: "north_west",
		x: placement.x,
		y: placement.y,
	});

	// Force PNG output to preserve any alpha in the overlay result
	return cloudinary.url(basePublicId, {
		secure: true,
		resource_type: "image",
		format: "png",
		transformation: t,
	});
}

async function generateCloudinaryCompositeStage({
	jobId,
	tmpDir,
	baseUpload,
	candleUpload,
	log,
}) {
	if (!baseUpload?.public_id || !candleUpload?.public_id) {
		throw new Error("composite_missing_inputs");
	}

	const targetW = Number(baseUpload.width || 1280);
	const targetH = Number(baseUpload.height || 720);

	let tweak = { dx: 0, dy: 0, scale: 1 };
	let best = null;

	for (let attempt = 1; attempt <= COMPOSITE_MAX_ATTEMPTS; attempt++) {
		const placement = computeScaledCandlePlacement({
			targetW,
			targetH,
			tweak,
		});

		// Strategy: try with background removal first (handles non-transparent candle outputs).
		let compositeUrl = null;
		let usedBgRemoval = true;
		try {
			compositeUrl = await buildCloudinaryCompositeUrl({
				basePublicId: baseUpload.public_id,
				candlePublicId: candleUpload.public_id,
				placement,
				useBackgroundRemoval: true,
			});
		} catch {
			compositeUrl = null;
		}

		let outPath = path.join(
			tmpDir,
			`presenter_composite_${jobId}_${attempt}.png`
		);
		let downloaded = false;
		if (compositeUrl) {
			try {
				await downloadUrlToFile(compositeUrl, outPath, {
					timeoutMs: 60000,
					maxRetries: CLOUDINARY_DERIVED_423_MAX_RETRIES,
					retrySleepMs: CLOUDINARY_DERIVED_423_SLEEP_MS,
					retryOnStatuses: [423, 420],
				});
				downloaded = true;
			} catch (e) {
				if (log)
					log("cloudinary composite download failed (bg removal)", {
						attempt,
						error: e?.message || String(e),
					});
				downloaded = false;
			}
		}

		if (!downloaded) {
			// Fallback strategy: no background removal. This will ONLY work if the candle image already has transparency.
			usedBgRemoval = false;
			try {
				compositeUrl = await buildCloudinaryCompositeUrl({
					basePublicId: baseUpload.public_id,
					candlePublicId: candleUpload.public_id,
					placement,
					useBackgroundRemoval: false,
				});
				await downloadUrlToFile(compositeUrl, outPath, {
					timeoutMs: 60000,
				});
				downloaded = true;
			} catch (e) {
				if (log)
					log("cloudinary composite download failed (no bg removal)", {
						attempt,
						error: e?.message || String(e),
					});
				downloaded = false;
			}
		}

		if (!downloaded) {
			safeUnlink(outPath);
			continue;
		}

		ensurePresenterFile(outPath);
		const upload = await uploadPresenterToCloudinary(
			outPath,
			jobId,
			PRESENTER_CLOUDINARY_PUBLIC_PREFIX
		);

		if (log)
			log("cloudinary composite created", {
				attempt,
				usedBgRemoval,
				placement,
				url: upload.url,
			});

		// Review placement; if rejected, adjust tweak and retry
		const placementReview = await reviewCompositePlacement({
			finalUrl: upload.url,
			attempt,
			log,
		});

		if (log)
			log("composite placement review", {
				attempt,
				accept: placementReview?.accept,
				reason: placementReview?.reason || "",
				deltaX: placementReview?.deltaX || 0,
				deltaY: placementReview?.deltaY || 0,
				scaleMultiplier: placementReview?.scaleMultiplier || 1,
			});

		best = { upload, outPath, placement, usedBgRemoval };

		if (placementReview?.accept) {
			return {
				upload,
				outPath,
				method: "cloudinary_composite",
			};
		}

		// Prepare tweak for next attempt
		tweak = {
			dx:
				(tweak.dx || 0) +
				(placementReview?.deltaX || 0) / Math.max(placement.scale || 1, 0.001),
			dy:
				(tweak.dy || 0) +
				(placementReview?.deltaY || 0) / Math.max(placement.scale || 1, 0.001),
			scale: (tweak.scale || 1) * (placementReview?.scaleMultiplier || 1),
		};

		// Keep tweak sane
		tweak.dx = clampInt(tweak.dx, -120, 120);
		tweak.dy = clampInt(tweak.dy, -120, 120);
		tweak.scale = Math.max(0.85, Math.min(1.2, tweak.scale));
	}

	// If we never accepted, return best attempt (if any)
	if (best?.upload) {
		return {
			upload: best.upload,
			outPath: best.outPath,
			method: "cloudinary_composite_best_effort",
		};
	}

	throw new Error("cloudinary_composite_failed");
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
		jobId,
		title,
		topics,
		categoryLabel,
		log,
	});

	let baseUpload = null;
	try {
		baseUpload = await uploadPresenterToCloudinary(
			presenterLocalPath,
			jobId,
			PRESENTER_BASE_PREFIX
		);
		if (log)
			log("presenter base uploaded", {
				url: baseUpload.url,
				publicId: baseUpload.public_id,
			});
	} catch (e) {
		// Base upload is only for review/compositing. If it fails, we can still proceed with local paths.
		baseUpload = null;
		if (log)
			log("presenter base upload failed", {
				error: e?.message || String(e),
			});
	}

	let outfitPath = null;
	let outfitUpload = null;
	let wardrobePrompt = prompts.wardrobePrompt;

	// Wardrobe stage with integrity review + retry.
	for (let attempt = 1; attempt <= 2; attempt++) {
		try {
			outfitPath = await generateRunwayOutfitStage({
				jobId,
				tmpDir: workingDir,
				presenterLocalPath,
				wardrobePrompt,
				attempt,
				log,
			});
			ensurePresenterFile(outfitPath);
			outfitUpload = await uploadPresenterToCloudinary(
				outfitPath,
				jobId,
				PRESENTER_OUTFIT_PREFIX
			);

			// Review against base presenter (prefer job-specific base upload if available).
			const baseUrlForReview =
				baseUpload?.url || ORCHESTRATOR_PRESENTER_REF_URL;
			const wardrobeReview = await reviewOutfitIntegrity({
				baseUrl: baseUrlForReview,
				outfitUrl: outfitUpload.url,
				promptUsed: wardrobePrompt,
				attempt,
				log,
			});
			if (log)
				log("wardrobe review", {
					attempt,
					accept: wardrobeReview?.accept,
					reason: wardrobeReview?.reason || "",
				});

			if (
				wardrobeReview?.accept ||
				wardrobeReview?.reason === "review_parse_failed"
			) {
				break;
			}

			// Retry: delete the bad outfit asset
			await deleteCloudinaryAsset(outfitUpload?.public_id, log);
			safeUnlink(outfitPath);
			outfitPath = null;
			outfitUpload = null;

			wardrobePrompt =
				wardrobeReview?.improvedPrompt ||
				`${prompts.wardrobePrompt}\nAdjustment: ${
					wardrobeReview?.reason || "preserve face/studio and remove borders"
				}`;
			if (log)
				log("wardrobe retry prompt", {
					nextAttempt: attempt + 1,
					prompt: String(wardrobePrompt).slice(0, 260),
				});
		} catch (e) {
			if (log)
				log("runway wardrobe stage failed", {
					error: e?.message || String(e),
					attempt,
				});
			// If wardrobe generation fails entirely, fallback to base presenter.
			break;
		}
	}

	// If wardrobe stage didn't produce a clean outfit, fallback to base presenter.
	if (!outfitUpload) {
		if (log)
			log("wardrobe fallback to base presenter", {
				reason: "wardrobe_failed_or_rejected",
			});
		if (baseUpload) {
			outfitUpload = baseUpload;
			outfitPath = presenterLocalPath;
		} else {
			// upload base again under outfit prefix
			outfitUpload = await uploadPresenterToCloudinary(
				presenterLocalPath,
				jobId,
				PRESENTER_OUTFIT_PREFIX
			);
			outfitPath = presenterLocalPath;
		}
	}

	// Candle product stage
	let candleProductPath = null;
	let candleUpload = null;
	let candlePrompt = prompts.candleProductPrompt;
	let candleReview = null;

	for (let attempt = 1; attempt <= 3; attempt++) {
		try {
			candleProductPath = await generateRunwayCandleProductStage({
				jobId,
				tmpDir: workingDir,
				candleLocalPath,
				candleProductPrompt: candlePrompt,
				attempt,
				log,
			});
			ensureImageFile(candleProductPath);
			candleUpload = await uploadPresenterToCloudinary(
				candleProductPath,
				jobId,
				PRESENTER_CANDLE_PREFIX
			);
		} catch (e) {
			if (log)
				log("runway candle product stage failed", {
					error: e?.message || String(e),
					attempt,
				});
			candleUpload = null;
			candleProductPath = null;
			break;
		}

		candleReview = await reviewCandleProduct({
			candleUrl: candleUpload?.url || "",
			promptUsed: candlePrompt,
			attempt,
			log,
		});
		if (log)
			log("candle product review", {
				attempt,
				accept: candleReview?.accept,
				reason: candleReview?.reason || "",
			});

		if (
			candleReview?.accept ||
			candleReview?.reason === "review_parse_failed"
		) {
			break;
		}

		if (attempt < 3) {
			await deleteCloudinaryAsset(candleUpload?.public_id, log);
			safeUnlink(candleProductPath);
			candleUpload = null;
			candleProductPath = null;
		}

		candlePrompt =
			candleReview?.improvedPrompt ||
			`${prompts.candleProductPrompt}\nAdjustment: ${
				candleReview?.reason ||
				"lid removed, crisp label, tiny flame, transparent PNG"
			}`;
		if (attempt < 3 && log)
			log("candle product retry prompt", {
				nextAttempt: attempt + 1,
				prompt: String(candlePrompt).slice(0, 320),
			});
	}

	if (!candleUpload) {
		// Last resort: upload the provided candleLocalPath and try compositing with background removal.
		if (log)
			log("candle fallback to provided asset", {
				reason: "candle_generation_failed",
			});
		try {
			candleUpload = await uploadPresenterToCloudinary(
				candleLocalPath,
				jobId,
				PRESENTER_CANDLE_PREFIX
			);
		} catch (e) {
			candleUpload = null;
			if (log)
				log("candle fallback upload failed", {
					error: e?.message || String(e),
				});
		}
	}

	let finalUpload = null;
	let finalPath = null;
	let method = "";

	// Preferred: deterministic Cloudinary compositing (preserves presenter/studio perfectly)
	if (candleUpload) {
		try {
			const composite = await generateCloudinaryCompositeStage({
				jobId,
				tmpDir: workingDir,
				baseUpload: outfitUpload,
				candleUpload,
				log,
			});
			finalUpload = composite.upload;
			finalPath = composite.outPath;
			method = composite.method;
		} catch (e) {
			if (log)
				log("cloudinary composite stage failed", {
					error: e?.message || String(e),
				});
			finalUpload = null;
			finalPath = null;
		}
	}

	// Fallback: last-resort Runway final generation if compositing fails
	if (!finalUpload && candleUpload) {
		if (log)
			log("fallback to runway final stage", {
				reason: "cloudinary_composite_failed",
			});
		let finalPrompt = prompts.finalPrompt;
		for (let attempt = 1; attempt <= 2; attempt++) {
			try {
				const tmpFinal = await generateRunwayFinalStage({
					jobId,
					tmpDir: workingDir,
					presenterCloudUrl: outfitUpload.url,
					candleCloudUrl: candleUpload.url,
					finalPrompt,
					attempt,
					log,
				});
				ensurePresenterFile(tmpFinal);
				finalPath = tmpFinal;
				finalUpload = await uploadPresenterToCloudinary(
					finalPath,
					jobId,
					PRESENTER_CLOUDINARY_PUBLIC_PREFIX
				);
				method = "runway_final_fallback";
				break;
			} catch (e) {
				if (log)
					log("runway final stage failed", {
						error: e?.message || String(e),
						attempt,
					});
				finalUpload = null;
				finalPath = null;
				finalPrompt = `${prompts.finalPrompt}\nAdjustment: preserve presenter exactly; fix candle placement to match reference; no borders.`;
			}
		}
	}

	// Final fallback: return the outfit (no candle) instead of hard failing the whole job.
	if (!finalUpload) {
		if (log)
			log("final fallback to outfit", {
				reason: "final_generation_failed",
			});
		finalUpload = await uploadPresenterToCloudinary(
			outfitPath,
			jobId,
			PRESENTER_CLOUDINARY_PUBLIC_PREFIX
		);
		finalPath = outfitPath;
		method = method || "outfit_only_fallback";
	}

	return {
		localPath: finalPath,
		url: finalUpload?.url || "",
		publicId: finalUpload?.public_id || "",
		width: finalUpload?.width || 0,
		height: finalUpload?.height || 0,
		method: method || "runway_three_stage",
	};
}

module.exports = {
	generatePresenterAdjustedImage,
};
