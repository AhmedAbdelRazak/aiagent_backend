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
	// Tuned to keep the candle on the BACK/right desk surface (not the front edge).
	// This is the starting placement; the placement reviewer can nudge it per attempt.
	x: 960,
	y: 308,
	w: 200,
	h: 214,
};

// How many times to refine Cloudinary compositing placement (cheap, deterministic)
const COMPOSITE_MAX_ATTEMPTS = 5;

// Candle overlay strategy:
// - "placement_ref" (default): extract the candle directly from the placement reference image.
//   Best realism (correct perspective + flame) and avoids label re-generation issues.
// - "generated": use the generated candle image (Runway) and rely on background removal.
// - "provided": use the provided candleLocalPath upload (no Runway candle gen).
const CANDLE_OVERLAY_STRATEGY = String(
	process.env.CANDLE_OVERLAY_STRATEGY || "placement_ref"
).toLowerCase();

// If strict, a candle will ONLY be used when placement review accepts it.
const STRICT_CANDLE_PLACEMENT =
	String(process.env.STRICT_CANDLE_PLACEMENT || "1") !== "0";

// If true, keep the last failed composite on Cloudinary for debugging.
const KEEP_FAILED_CANDLE_COMPOSITES =
	String(process.env.KEEP_FAILED_CANDLE_COMPOSITES || "0") === "1";

// If false, reject a bad candle render and do NOT place it.
const ALLOW_CANDLE_WITH_ISSUES =
	String(process.env.ALLOW_CANDLE_WITH_ISSUES || "0") === "1";

// Disable by default: Runway "final" stage can mutate presenter/studio.
// Enable only if you explicitly want it.
const ENABLE_RUNWAY_FINAL_FALLBACK =
	String(process.env.ENABLE_RUNWAY_FINAL_FALLBACK || "0") === "1";

// Public ID for the placement reference (same asset as ORCHESTRATOR_CANDLE_REF_URL).
// URL example: https://res.cloudinary.com/<cloud>/image/upload/v.../aivideomatic/PresenterWithCandle_f6t83r.png
const ORCHESTRATOR_CANDLE_REF_PUBLIC_ID =
	process.env.ORCHESTRATOR_CANDLE_REF_PUBLIC_ID ||
	"aivideomatic/PresenterWithCandle_f6t83r";

// Crop region (pixels) inside ORCHESTRATOR_CANDLE_REF_PUBLIC_ID that tightly frames the candle + contact area.
// Used only when CANDLE_OVERLAY_STRATEGY === "placement_ref".
const PLACEMENT_REF_CANDLE_CROP = {
	x: 1020,
	y: 420,
	w: 220,
	h: 235,
};

// Subtle layer effects to "seat" the candle on the desk (shadow + slight DOF blur).
const CANDLE_LAYER_SHADOW = {
	strength: 32,
	x: 0,
	y: 14,
	color: "black",
};

const CANDLE_LAYER_BLUR = 10;

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
Do NOT add borders/frames/vignettes/letterboxing or change image processing.
No candles, no extra objects, no text, no logos. Topic context: ${topicLine}.
`.trim();
}

function fallbackCandleProductPrompt() {
	return `
Use @candle_ref to generate a single clean candle cutout of the same candle.
HARD REQUIREMENT: the jar must be OPEN with NO lid or cap visible anywhere.
The candle is LIT with a TINY calm flame; no exaggerated glow.
Label/branding must remain EXACT and fully readable; avoid mangled microtext.
Output MUST be a PNG with a TRANSPARENT background (alpha). No shadows, no props, no extra text.
Keep the candle centered, upright, normal proportions (no warping), and fill most of the frame.
`.trim();
}

function fallbackFinalPrompt({ topicLine }) {
	// NOTE: We no longer rely on Runway for final compositing by default.
	// This prompt is kept for last-resort fallback usage.
	return `
Use @presenter_ref as the ONLY base image. Face/head strictly locked; do not alter the presenter in any way.
Keep the studio/desk/background/camera/framing/lighting unchanged.
Add only @candle_ref candle on the back table/desk on the right side near the edge, matching the reference candle placement.
Candle must be lid removed and lit with a tiny calm flame. Label must stay readable.
Do NOT add borders, vignettes, color grading, or any new objects.
Topic context: ${topicLine}.
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
	const raw = `${String(jobId || "")}::${String(stage || "")}::${attempt}`;
	// simple deterministic 32-bit hash
	let h = 2166136261;
	for (let i = 0; i < raw.length; i++) {
		h ^= raw.charCodeAt(i);
		h = Math.imul(h, 16777619);
		h >>>= 0;
	}
	return h;
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
				if (retryOnStatuses.includes(res.status) && attempt < maxRetries) {
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
			break;
		}
	}
	throw lastErr || new Error("download_failed");
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

	// IMPORTANT: Product reference image includes a lid, but our requirement is LID REMOVED.
	// The reviewer must NOT reject a candle for lacking the lid.
	const system = `
You are a strict quality reviewer for a candle CUTOUT image that will be composited into a presenter scene.
Return JSON only with keys: accept (boolean), reason (string), improvedPrompt (string).
Accept only if ALL are true:
- Lid/cap is fully removed (NO lid visible anywhere).
- Candle is centered, upright, normal proportions (no warping).
- Flame is tiny and calm (no big flame, no dramatic glow).
- Label/branding is readable and not mangled/garbled.
- Background is transparent (alpha) OR is a perfectly clean single-color background that can be removed.
- No extra objects or extra text.
If reject, provide a revised candle product prompt that fixes the issue.
`.trim();

	const userText = `
Attempt: ${Number(attempt || 1)}
Prompt used: ${String(promptUsed || "").slice(0, 700)}
Review the generated candle image against the candle product reference label/jar shape. The reference product photo includes a lid, but the REQUIRED output must have the lid removed.
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
						{ type: "image_url", image_url: { url: candleUrl } },
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_CANDLE_PRODUCT_URL },
						},
						{
							type: "image_url",
							image_url: { url: ORCHESTRATOR_CANDLE_REF_URL },
						},
					],
				},
			],
			temperature: 0.2,
			max_completion_tokens: 320,
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
		improvedPrompt: `${String(promptUsed || "").trim()}
Adjustment: ensure lid is removed, label is crisp, flame is tiny, and output is transparent PNG.`,
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
You are a strict quality reviewer for a presenter wardrobe adjustment image.
Return JSON only with keys: accept (boolean), reason (string), improvedPrompt (string).
Accept only if:
- Presenter face/head (hairline, beard, glasses, skin tone, expression) are unchanged from the base image.
- Studio/desk/background/camera angle/framing/lighting are unchanged.
- No borders/frames/vignettes/letterboxing.
- Only the wardrobe/clothing changed (dark outfit).
If reject, provide an improved wardrobe prompt that emphasizes preserving the base image and removing borders.
`.trim();

	const userText = `
Attempt: ${Number(attempt || 1)}
Prompt used: ${String(promptUsed || "").slice(0, 700)}
Compare the wardrobe result to the base presenter image. Output JSON only.
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
			log("wardrobe review parse failed", {
				attempt,
				response: content.slice(0, 600),
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
		improvedPrompt: `${String(promptUsed || "").trim()}
Adjustment: Do NOT change face/head or studio; NO borders/letterboxing; ONLY change clothing.`,
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
		accept: STRICT_CANDLE_PLACEMENT ? false : true,
		reason: STRICT_CANDLE_PLACEMENT
			? "review_failed_openai_error"
			: "review_skipped_openai_error",
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
	overlayCrop = null,
	overlayLayerEffects = [],
}) {
	assertCloudinaryReady();

	if (!basePublicId) {
		throw new Error("buildCloudinaryCompositeUrl: basePublicId missing");
	}
	if (!candlePublicId) {
		throw new Error("buildCloudinaryCompositeUrl: candlePublicId missing");
	}
	if (!placement) {
		throw new Error("buildCloudinaryCompositeUrl: placement missing");
	}

	const overlayId = cloudinaryOverlayId(candlePublicId);

	// Layer transformations are applied in order until `flags: layer_apply`.
	// Anything before `layer_apply` affects the overlay layer (the candle).
	const t = [];

	// 1) Start layer
	t.push({ overlay: overlayId });

	// 2) Optional crop inside the overlay source (used for placement_ref strategy)
	if (
		overlayCrop &&
		Number.isFinite(overlayCrop.x) &&
		Number.isFinite(overlayCrop.y) &&
		Number.isFinite(overlayCrop.w) &&
		Number.isFinite(overlayCrop.h)
	) {
		t.push({
			crop: "crop",
			gravity: "north_west",
			x: Math.round(overlayCrop.x),
			y: Math.round(overlayCrop.y),
			width: Math.round(overlayCrop.w),
			height: Math.round(overlayCrop.h),
		});
	}

	// 3) Optional background removal (turns into a cutout)
	if (useBackgroundRemoval) {
		t.push({ effect: "background_removal" });
	}

	// 4) Optional additional layer effects (shadow/blur/color matching, etc.)
	for (const eff of Array.isArray(overlayLayerEffects)
		? overlayLayerEffects
		: []) {
		if (eff && typeof eff === "object") {
			t.push(eff);
		}
	}

	// 5) Resize the layer to match target placement box
	t.push({
		width: placement.w,
		height: placement.h,
		crop: "scale",
	});

	// 6) Apply layer onto the base
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
	targetW,
	targetH,
	log,
}) {
	assertCloudinaryReady();

	if (!baseUpload?.public_id) {
		throw new Error("generateCloudinaryCompositeStage: baseUpload missing");
	}

	const overlayStrategy = CANDLE_OVERLAY_STRATEGY;
	const isPlacementRefOverlay = overlayStrategy === "placement_ref";

	let overlayPublicId = null;
	let overlayCrop = null;

	if (isPlacementRefOverlay) {
		overlayPublicId = ORCHESTRATOR_CANDLE_REF_PUBLIC_ID;
		overlayCrop = PLACEMENT_REF_CANDLE_CROP;
	} else {
		if (!candleUpload?.public_id) {
			throw new Error(
				`generateCloudinaryCompositeStage: candleUpload missing (strategy=${overlayStrategy})`
			);
		}
		overlayPublicId = candleUpload.public_id;
		overlayCrop = null;
	}

	// Subtle layer effects to improve realism (shadow + slight DOF blur).
	const overlayLayerEffects = [];
	if (CANDLE_LAYER_SHADOW?.strength) {
		overlayLayerEffects.push({
			effect: `shadow:${Math.round(CANDLE_LAYER_SHADOW.strength)}`,
			color: CANDLE_LAYER_SHADOW.color || "black",
			x: Math.round(CANDLE_LAYER_SHADOW.x || 0),
			y: Math.round(CANDLE_LAYER_SHADOW.y || 0),
		});
	}
	if (
		CANDLE_LAYER_BLUR &&
		Number.isFinite(CANDLE_LAYER_BLUR) &&
		CANDLE_LAYER_BLUR > 0
	) {
		overlayLayerEffects.push({
			effect: `blur:${Math.round(CANDLE_LAYER_BLUR)}`,
		});
	}

	let tweak = { dx: 0, dy: 0, scale: 1 };
	let lastReview = null;

	for (let attempt = 1; attempt <= COMPOSITE_MAX_ATTEMPTS; attempt++) {
		const placement = computeScaledCandlePlacement(targetW, targetH, tweak);

		// For placement_ref overlay we ALWAYS need background removal.
		// For generated/provided candles, background removal is also recommended (most are RGB).
		const usedBgRemoval = true;

		const compositeUrl = await buildCloudinaryCompositeUrl({
			basePublicId: baseUpload.public_id,
			candlePublicId: overlayPublicId,
			placement,
			useBackgroundRemoval: usedBgRemoval,
			overlayCrop,
			overlayLayerEffects,
		});

		const outPath = path.join(
			tmpDir,
			`presenter_composite_${jobId}_${attempt}.png`
		);
		await downloadUrlToFile(compositeUrl, outPath, log);

		const upload = await uploadPresenterToCloudinary({
			localPath: outPath,
			publicId: `${PRESENTER_MASTER_PREFIX}${jobId}_${Date.now()}_${attempt}`,
			folder: PRESENTER_CLOUDINARY_FOLDER,
			log,
		});

		if (log) {
			log(`[LongVideo][${jobId}] cloudinary composite created`, {
				attempt,
				overlayStrategy,
				usedBgRemoval,
				placement,
				url: upload.url,
			});
		}

		const review = await reviewCompositePlacement({
			finalUrl: upload.url,
			log,
		});
		lastReview = review;

		if (log) {
			log(`[LongVideo][${jobId}] composite placement review`, {
				attempt,
				accept: !!review?.accept,
				reason: review?.reason || "unknown",
				deltaX: review?.deltaX ?? 0,
				deltaY: review?.deltaY ?? 0,
				scaleMultiplier: review?.scaleMultiplier ?? 1,
			});
		}

		if (review?.accept) {
			return {
				upload,
				outPath,
				method: isPlacementRefOverlay
					? "cloudinary_composite_placement_ref"
					: "cloudinary_composite",
			};
		}

		// If this was the last attempt:
		if (attempt === COMPOSITE_MAX_ATTEMPTS) {
			if (STRICT_CANDLE_PLACEMENT) {
				// Delete the last candidate unless debugging is enabled
				if (!KEEP_FAILED_CANDLE_COMPOSITES) {
					await deleteCloudinaryAsset(upload.public_id, log);
				}
				safeUnlink(outPath);
				throw new Error(
					`cloudinary_composite_not_accepted: ${
						review?.reason || "placement_mismatch"
					}`
				);
			}

			// Best-effort mode: keep it.
			return {
				upload,
				outPath,
				method: isPlacementRefOverlay
					? "cloudinary_composite_placement_ref_best_effort"
					: "cloudinary_composite_best_effort",
			};
		}

		// Otherwise: apply reviewer deltas and retry.
		const dx = clamp(Math.round(review?.deltaX ?? 0), -120, 120);
		const dy = clamp(Math.round(review?.deltaY ?? 0), -120, 120);
		const sm = clamp(Number(review?.scaleMultiplier ?? 1), 0.85, 1.2);

		tweak.dx += dx;
		tweak.dy += dy;
		tweak.scale *= sm;

		// Cleanup this rejected candidate to avoid Cloudinary temp clutter
		if (!KEEP_FAILED_CANDLE_COMPOSITES) {
			await deleteCloudinaryAsset(upload.public_id, log);
		}
		safeUnlink(outPath);
	}

	// Should never reach here
	throw new Error(
		`cloudinary_composite_not_accepted: ${lastReview?.reason || "unknown"}`
	);
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
	ensureImageFile(candleLocalPath);

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

	// Candle handling depends on the overlay strategy:
	// - placement_ref: we extract the candle directly from ORCHESTRATOR_CANDLE_REF_PUBLIC_ID (no Runway candle generation).
	// - generated: we try to generate a clean candle cutout via Runway (then composite).
	// - provided: we skip Runway and just upload the provided candleLocalPath (then composite with bg removal).
	if (CANDLE_OVERLAY_STRATEGY === "placement_ref") {
		if (log)
			log(
				`[LongVideo][${jobId}] candle overlay strategy=placement_ref; skipping candle product generation`,
				{}
			);
	} else if (CANDLE_OVERLAY_STRATEGY === "provided") {
		candleUpload = await uploadPresenterToCloudinary({
			localPath: candleLocalPath,
			publicId: `${PRESENTER_CANDLE_PREFIX}${jobId}_${Date.now()}`,
			folder: PRESENTER_CLOUDINARY_FOLDER,
			log,
		});
		candleProductPath = candleLocalPath;
	} else {
		for (let attempt = 1; attempt <= 3; attempt++) {
			try {
				const candle = await generateRunwayCandleProductStage({
					jobId,
					tmpDir: workingDir,
					candleLocalPath,
					candleProductPrompt: candlePrompt,
					attempt,
					log,
				});

				candleProductPath = candle.path;

				// Upload this candidate candle
				candleUpload = await uploadPresenterToCloudinary({
					localPath: candleProductPath,
					publicId: `${PRESENTER_CANDLE_PREFIX}${jobId}_${Date.now()}_${attempt}`,
					folder: PRESENTER_CLOUDINARY_FOLDER,
					log,
				});

				// Review candle product quality
				candleReview = await reviewCandleProduct({
					candleUrl: candleUpload.url,
					log,
				});

				if (log)
					log(`[LongVideo][${jobId}] candle product review`, {
						attempt,
						accept: !!candleReview?.accept,
						reason: candleReview?.reason || "unknown",
					});

				if (candleReview?.accept) break;

				// Not accepted -> delete & retry (avoid clutter)
				try {
					await deleteCloudinaryAsset(candleUpload.public_id, log);
				} catch (e) {}

				if (candleProductPath) safeUnlink(candleProductPath);
				candleUpload = null;
				candleProductPath = null;

				// Update prompt with retry guidance if provided by the reviewer
				if (candleReview?.retryPrompt) {
					candlePrompt = candleReview.retryPrompt;
				}
			} catch (e) {
				if (log)
					log(`[LongVideo][${jobId}] runway candle product stage failed`, {
						attempt,
						error: String(e?.message || e),
					});
			}
		}

		// If the candle was generated but rejected by the reviewer, discard it unless explicitly allowed.
		if (
			candleUpload &&
			candleReview &&
			candleReview.accept === false &&
			!ALLOW_CANDLE_WITH_ISSUES
		) {
			if (log)
				log("candle product rejected; discarding", {
					reason: candleReview.reason || "rejected",
				});
			try {
				await deleteCloudinaryAsset(candleUpload.public_id, log);
			} catch (e) {}
			if (candleProductPath) safeUnlink(candleProductPath);
			candleUpload = null;
			candleProductPath = null;
		}

		// Last resort: upload the provided candleLocalPath and try compositing with background removal
		if (!candleUpload) {
			candleUpload = await uploadPresenterToCloudinary({
				localPath: candleLocalPath,
				publicId: `${PRESENTER_CANDLE_PREFIX}${jobId}_${Date.now()}`,
				folder: PRESENTER_CLOUDINARY_FOLDER,
				log,
			});
			candleProductPath = candleLocalPath;
		}
	}
	let finalUpload = null;
	let finalPath = null;
	let method = "";

	// Preferred: deterministic Cloudinary compositing (preserves presenter/studio perfectly)
	// We can composite even without a generated candle when using the placement_ref strategy.
	const wantComposite =
		CANDLE_OVERLAY_STRATEGY === "placement_ref" || Boolean(candleUpload);

	let compositeNotAccepted = false;

	if (!finalUpload && wantComposite) {
		try {
			const composite = await generateCloudinaryCompositeStage({
				jobId,
				tmpDir: workingDir,
				baseUpload: outfitUpload,
				// candleUpload may be null when using placement_ref overlay
				candleUpload,
				targetW: outfitUpload.width || TARGET_OUTPUT.width,
				targetH: outfitUpload.height || TARGET_OUTPUT.height,
				log,
			});
			finalUpload = composite.upload;
			finalPath = composite.outPath;
			method = composite.method;
		} catch (e) {
			compositeNotAccepted = String(e?.message || "").includes(
				"cloudinary_composite_not_accepted"
			);
			if (log)
				log(`[LongVideo][${jobId}] cloudinary composite failed`, {
					error: String(e?.message || e),
					compositeNotAccepted,
					overlayStrategy: CANDLE_OVERLAY_STRATEGY,
				});
		}
	}

	// Optional fallback: Runway "final" stage (risky; may mutate presenter/studio).
	// Disabled by default; set ENABLE_RUNWAY_FINAL_FALLBACK=1 to turn on.
	if (
		!finalUpload &&
		candleUpload &&
		ENABLE_RUNWAY_FINAL_FALLBACK &&
		!compositeNotAccepted
	) {
		try {
			const final = await generateRunwayFinalStage({
				jobId,
				tmpDir: workingDir,
				presenterCloudUrl: outfitUpload.url,
				candleCloudUrl: candleUpload.url,
				finalPrompt: prompts.final,
				attempt: 1,
				log,
			});
			finalPath = final.path;
			finalUpload = await uploadPresenterToCloudinary({
				localPath: finalPath,
				publicId: `${PRESENTER_MASTER_PREFIX}${jobId}_${Date.now()}`,
				folder: PRESENTER_CLOUDINARY_FOLDER,
				log,
			});
			method = final.method;
		} catch (e) {
			if (log)
				log(`[LongVideo][${jobId}] runway final stage failed`, {
					error: String(e?.message || e),
				});
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
