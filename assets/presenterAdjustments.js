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
const CHAT_MODEL = "gpt-5.2";
const ORCHESTRATOR_PRESENTER_REF_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767066355/aivideomatic/long_presenters/presenter_master_4b76c718-6a2a-4749-895e-e05bd2b2ecfc_1767066355424.png";
const ORCHESTRATOR_CANDLE_REF_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767142335/aivideomatic/PresenterWithCandle_f6t83r.png";
const ORCHESTRATOR_CANDLE_PRODUCT_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767134899/aivideomatic/MyCandle_u9skio.png";

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
No candles, no extra objects, no text, no logos. Topic context: ${topicLine}.
`.trim();
}

function fallbackCandleProductPrompt() {
	return `
Use @candle_ref to generate a single clean product image of the same candle.
Jar must be OPEN with NO lid or cap visible. Candle is LIT with a tiny calm flame; no exaggerated glow.
Label text/logo must remain EXACT, sharp, readable, and undistorted; do NOT redraw or change the label artwork or typography.
Keep the candle centered, upright, and normal size (not oversized); no distortion.
Isolate on a clean neutral background with no shadows or extra objects.
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
- Study the provided reference images to match the studio framing and candle placement.
- Face is strictly locked: do NOT alter the face or head in any way; no double face, no ghosting, no artifacts.
- Keep studio/desk/background/camera/lighting unchanged.
- Wardrobe: vary the outfit each run using the provided wardrobe variation cue; the prompt must include the cue explicitly and match it exactly (dark colors only, open collar, optional open blazer).
- Candle product: use @candle_ref to generate a clean candle product image with lid removed, tiny calm flame, exact label/branding, no distortion; do not redraw or alter label art/text.
- Final: add the candle (from the candle product prompt) on the back table/desk to the right side near the edge, fully visible, lid removed, tiny calm flame, natural shadow, no transparency, sitting on the tabletop (not floating), normal size in scene. Match the candle placement/size to the reference image (same relative offset/scale). Candle label/branding must remain EXACT and readable. Only add the candle; do not change any other pixels.
- Candle must match the product reference (label, jar shape, proportions) while being normal size in scene.
- Keep prompts concise and avoid phrasing that implies identity manipulation or deepfakes.
- No extra objects and no added text/logos beyond the candle label; no watermarks.
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

async function runwayTextToImage({ promptText, referenceImages, ratio }) {
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	const payload = {
		model: RUNWAY_IMAGE_MODEL,
		promptText: String(promptText || "").slice(0, 1000),
		ratio: String(ratio || "1920:1080"),
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

async function reviewCandleProduct({ candleUrl, promptUsed, attempt, log }) {
	if (!openai) {
		return {
			accept: true,
			reason: "review_skipped_no_openai",
			improvedPrompt: "",
		};
	}

	const system = `
You are a strict quality reviewer for a candle product image.
Return JSON only with keys: accept (boolean), reason (string), improvedPrompt (string).
Accept only if:
- Candle lid/cap is fully removed (no lid visible).
- Candle is centered, upright, normal size, with no warping.
- Flame is tiny and calm (no large glow).
- Label/branding is mostly readable and not obviously mangled; do not reject solely for minor label differences.
- No extra objects or text; clean neutral background.
If reject, provide a revised candle product prompt that fixes the issue.
`.trim();

	const userText = `
Attempt: ${Number(attempt || 1)}
Prompt used: ${String(promptUsed || "").slice(0, 600)}
Review the generated candle image against the product reference. Output JSON only.
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
							image_url: { url: candleUrl },
						},
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
		).trim()}\nAdjust to remove lid, preserve exact label text/art, and avoid distortion.`,
	};
}

async function reviewFinalPlacement({ finalUrl, promptUsed, attempt, log }) {
	if (!openai) {
		return {
			accept: true,
			reason: "review_skipped_no_openai",
			improvedPrompt: "",
		};
	}

	const system = `
You are a strict quality reviewer for a presenter image with a branded candle.
Return JSON only with keys: accept (boolean), reason (string), improvedPrompt (string).
Accept only if:
- Candle placement closely matches the reference placement image (position on right desk, same relative size/offset).
- Candle is fully visible, lid removed, tiny calm flame, natural shadow, no transparency.
- Presenter face/head and studio are unchanged.
- Do not reject solely for minor label/branding differences; prioritize placement/scale and presenter integrity.
If reject, provide a revised final prompt that keeps all constraints and fixes placement/size/label clarity.
`.trim();

	const userText = `
Attempt: ${Number(attempt || 1)}
Prompt used: ${String(promptUsed || "").slice(0, 600)}
Review the generated image against the references. Output JSON only.
`.trim();

	const runReview = async (sys, label) => {
		const resp = await openai.chat.completions.create({
			model: CHAT_MODEL,
			messages: [
				{ role: "system", content: sys },
				{
					role: "user",
					content: [
						{ type: "text", text: userText },
						{ type: "image_url", image_url: { url: finalUrl } },
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
			temperature: 0.2,
			max_completion_tokens: 400,
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
			log("presenter final review parse failed", {
				attempt,
				label,
				response: content.slice(0, 600),
			});
		return null;
	};

	try {
		const primary = await runReview(system, "primary");
		if (primary) return primary;
	} catch (e) {
		if (log)
			log("presenter final review failed", {
				error: e?.message || String(e),
			});
	}

	const fallbackSystem = `
Return JSON only with keys: accept, reason, improvedPrompt.
Be strict about candle placement vs reference. If unsure, reject.
`.trim();
	try {
		const fallback = await runReview(fallbackSystem, "fallback");
		if (fallback) return fallback;
	} catch (e) {
		if (log)
			log("presenter final review failed (fallback)", {
				error: e?.message || String(e),
			});
	}

	return {
		accept: true,
		reason: "review_parse_failed",
		improvedPrompt: "",
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

async function generateRunwayCandleProductStage({
	jobId,
	tmpDir,
	candleLocalPath,
	candleProductPrompt,
	log,
}) {
	const candleUri = await runwayCreateEphemeralUpload({
		filePath: candleLocalPath,
		filename: path.basename(candleLocalPath),
	});

	if (log)
		log("runway candle product prompt", {
			prompt: String(candleProductPrompt || "").slice(0, 200),
		});
	const outputUri = await runwayTextToImage({
		promptText: candleProductPrompt,
		referenceImages: [{ uri: candleUri, tag: "candle_ref" }],
		ratio: "1024:1024",
	});
	const outPath = path.join(tmpDir, `candle_product_${jobId}.png`);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	return outPath;
}

async function generateRunwayFinalStage({
	jobId,
	tmpDir,
	presenterCloudUrl,
	candleCloudUrl,
	finalPrompt,
	log,
}) {
	const presenterPath = path.join(tmpDir, `presenter_cloud_${jobId}.png`);
	await downloadUrlToFile(presenterCloudUrl, presenterPath);
	const candlePath = path.join(tmpDir, `candle_cloud_${jobId}.png`);
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
			prompt: String(finalPrompt || "").slice(0, 200),
		});
	const outputUri = await runwayTextToImage({
		promptText: finalPrompt,
		referenceImages: [
			{ uri: presenterUri, tag: "presenter_ref" },
			{ uri: candleUri, tag: "candle_ref" },
		],
	});
	const outPath = path.join(tmpDir, `presenter_final_${jobId}.png`);
	await downloadRunwayImageToPath({ uri: outputUri, outPath });
	safeUnlink(presenterPath);
	safeUnlink(candlePath);
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
		jobId,
		title,
		topics,
		categoryLabel,
		log,
	});

	let outfitPath = null;
	let outfitUpload = null;
	let candleProductPath = null;
	let candleUpload = null;
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

	let candlePrompt = prompts.candleProductPrompt;
	let candleReview = null;
	for (let attempt = 1; attempt <= 3; attempt++) {
		try {
			candleProductPath = await generateRunwayCandleProductStage({
				jobId,
				tmpDir: workingDir,
				candleLocalPath,
				candleProductPrompt: candlePrompt,
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
			throw e;
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
			if (candleReview?.reason === "review_parse_failed" && log) {
				log("candle product review skipped", {
					attempt,
					reason: "review_parse_failed",
				});
			}
			break;
		}

		if (attempt < 3) {
			await deleteCloudinaryAsset(candleUpload?.public_id, log);
			safeUnlink(candleProductPath);
			candleProductPath = null;
			candleUpload = null;
		}

		const nextPrompt =
			candleReview?.improvedPrompt ||
			`${prompts.candleProductPrompt}\nAdjustment: ${
				candleReview?.reason || "remove lid and preserve exact label"
			}`;
		if (attempt < 3 && log)
			log("candle product retry prompt", {
				nextAttempt: attempt + 1,
				prompt: String(nextPrompt || "").slice(0, 300),
			});
		candlePrompt = nextPrompt;
	}

	if (candleReview && candleReview.accept === false) {
		if (log)
			log("candle product accepted with issues", {
				reason: candleReview.reason || "label/lid mismatch",
			});
	}

	let finalPrompt = prompts.finalPrompt;
	let finalReview = null;
	for (let attempt = 1; attempt <= 3; attempt++) {
		try {
			finalPath = await generateRunwayFinalStage({
				jobId,
				tmpDir: workingDir,
				presenterCloudUrl: outfitUpload.url,
				candleCloudUrl: candleUpload.url,
				finalPrompt,
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
				log("runway final stage failed", {
					error: e?.message || String(e),
					attempt,
				});
			throw e;
		}

		finalReview = await reviewFinalPlacement({
			finalUrl: finalUpload?.url || "",
			promptUsed: finalPrompt,
			attempt,
			log,
		});
		if (log)
			log("presenter final review", {
				attempt,
				accept: finalReview?.accept,
				reason: finalReview?.reason || "",
			});
		if (finalReview?.reason === "review_parse_failed" && log) {
			log("presenter final review skipped", {
				attempt,
				reason: "review_parse_failed",
			});
		}
		if (finalReview?.accept) break;

		await deleteCloudinaryAsset(finalUpload?.public_id, log);
		safeUnlink(finalPath);
		finalPath = null;
		finalUpload = null;

		if (attempt < 3) {
			const nextPrompt =
				finalReview?.improvedPrompt ||
				`${prompts.finalPrompt}\nAdjustment: ${
					finalReview?.reason || "fix candle placement to match reference"
				}`;
			if (log)
				log("presenter final retry prompt", {
					nextAttempt: attempt + 1,
					prompt: String(nextPrompt || "").slice(0, 300),
				});
			finalPrompt = nextPrompt;
		}
	}

	if (finalReview && finalReview.accept === false) {
		if (log)
			log("presenter final rejected", {
				reason: finalReview.reason || "placement mismatch",
			});
		throw new Error(
			`presenter final rejected: ${finalReview.reason || "placement mismatch"}`
		);
	}

	return {
		localPath: finalPath,
		url: finalUpload?.url || "",
		publicId: finalUpload?.public_id || "",
		width: finalUpload?.width || 0,
		height: finalUpload?.height || 0,
		method: "runway_three_stage",
	};
}

module.exports = {
	generatePresenterAdjustedImage,
};
