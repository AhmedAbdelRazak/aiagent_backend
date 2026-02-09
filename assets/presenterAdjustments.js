/** @format */

const fs = require("fs");
const os = require("os");
const path = require("path");
const child_process = require("child_process");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;
const { OpenAI } = require("openai");
const { EXPLICIT_SERIOUS_CUES } = require("./utils");

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
const CHAT_MODEL = "gpt-5.2";
const ORCHESTRATOR_PRESENTER_REF_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767066355/aivideomatic/long_presenters/presenter_master_4b76c718-6a2a-4749-895e-e05bd2b2ecfc_1767066355424.png";

let ffmpegPath = "";
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
}

const WARDROBE_VARIANTS = [
	"dark charcoal matte button-up, open collar, no blazer",
	"black band-collar button-up, no blazer",
	"deep navy oxford button-up, open collar, no blazer",
	"black button-up with standard placket, open collar, no blazer",
	"black button-up with hidden placket, open collar, no blazer",
	"deep forest green button-up, open collar, no blazer",
	"dark burgundy button-up, open collar, no blazer",
	"midnight teal button-up, open collar, no blazer",
	"dark aubergine button-up, open collar, no blazer",
	"graphite twill button-up, open collar, no blazer",
	"charcoal herringbone button-up, open collar, no blazer",
	"ink navy poplin button-up, open collar, no blazer",
	"deep espresso button-up, open collar, no blazer",
	"near-black button-up, open collar, no blazer",
	"black micro-texture button-up, open collar, no blazer",
	"deep slate button-up, open collar, no blazer",
	"deep navy textured button-up, open collar, unstructured dark blazer",
	"dark graphite micro-pattern button-up, open collar, soft knit blazer",
	"dark slate button-up, open collar, open blazer with subtle texture",
	"black button-up with subtle sheen, open collar, slim dark blazer",
	"charcoal button-up with thin pinstripe, open collar, open blazer",
	"midnight-blue button-up, open collar, relaxed dark blazer",
	"dark espresso button-up, open collar, tailored black blazer",
	"deep charcoal twill button-up, open collar, structured dark blazer",
	"black micro-texture button-up, open collar, clean dark blazer",
	"midnight navy button-up, open collar, matte charcoal blazer",
	"graphite button-up, open collar, dark windowpane blazer",
	"deep slate button-up, open collar, minimalist black blazer",
	"black poplin button-up, open collar, dark subtle-check blazer",
	"charcoal oxford button-up, open collar, slim dark blazer",
	"dark navy button-up, open collar, soft-structured black blazer",
	"near-black button-up, open collar, clean dark blazer",
];
const NO_BLAZER_PATTERN = /\bno blazer\b/i;
const FORMAL_CATEGORY_KEYWORDS = new Set([
	"politics",
	"world",
	"health",
	"social",
	"socialissues",
	"crime",
	"law",
	"justice",
	"government",
	"public safety",
]);
const FORMAL_CONTEXT_PHRASES = [
	"official statement",
	"official report",
	"official announcement",
	"press conference",
	"court",
	"trial",
	"verdict",
	"sentenced",
	"indictment",
	"charged",
	"arrest",
	"lawsuit",
	"sued",
	"investigation",
	"police",
	"government",
	"parliament",
	"congress",
	"senate",
	"white house",
	"policy",
	"regulation",
	"minister",
	"president",
	"prime minister",
	"military",
	"war",
	"conflict",
];
const SERIOUS_CONTEXT_TOKENS = Array.from(
	new Set([
		...(EXPLICIT_SERIOUS_CUES || []),
		"funeral",
		"shooting",
		"murder",
		"assault",
		"violence",
		"cancer",
		"illness",
		"crash",
		"fatal",
		"fatalities",
		"victim",
		"victims",
		"hospitalized",
		"critical",
		"emergency",
	])
).map((token) => String(token || "").toLowerCase());

const PRESENTER_FACE_LOCK_ENABLED = true;
const PRESENTER_FACE_LOCK_ALWAYS = true;
const PRESENTER_FACE_SIMILARITY_MIN = 0.92;
const PRESENTER_FACE_LOCK_FEATHER_PCT = 0.03;
const PRESENTER_FACE_LOCK_REGION = {
	x: 0.28,
	y: 0.04,
	w: 0.44,
	h: 0.5,
};
const PRESENTER_FACE_LOCK_EYES_REGION = {
	x: 0.32,
	y: 0.08,
	w: 0.36,
	h: 0.2,
};

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

function clampNumber(n, min, max) {
	const x = Number(n);
	if (!Number.isFinite(x)) return min;
	return Math.max(min, Math.min(max, x));
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

function resolveFfprobePath() {
	let ffprobePath = "ffprobe";
	if (ffmpegPath) {
		const candidate = ffmpegPath.replace(/ffmpeg(\.exe)?$/i, "ffprobe$1");
		if (candidate && candidate !== ffmpegPath) ffprobePath = candidate;
	}
	return ffprobePath;
}

function ffprobeDimensions(filePath) {
	try {
		const ffprobePath = resolveFfprobePath();
		const out = child_process
			.execSync(
				`"${ffprobePath}" -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x "${filePath}"`,
				{ stdio: ["ignore", "pipe", "ignore"] },
			)
			.toString()
			.trim();
		const [w, h] = out.split("x").map((n) => Number(n) || 0);
		return { width: w || 0, height: h || 0 };
	} catch {
		return { width: 0, height: 0 };
	}
}

function runFfmpeg(args, label = "ffmpeg") {
	return new Promise((resolve, reject) => {
		if (!ffmpegPath) return reject(new Error("ffmpeg not available"));
		const proc = child_process.spawn(ffmpegPath, args, {
			stdio: ["ignore", "pipe", "pipe"],
			windowsHide: true,
		});
		let stderr = "";
		proc.stderr.on("data", (d) => (stderr += d.toString()));
		proc.on("error", reject);
		proc.on("close", (code) => {
			if (code === 0) return resolve();
			const head = stderr.slice(0, 4000);
			reject(new Error(`${label} failed (code ${code}): ${head}`));
		});
	});
}

function runFfmpegBuffer(args, label = "ffmpeg_buffer") {
	if (!ffmpegPath) throw new Error("ffmpeg not available");
	const res = child_process.spawnSync(ffmpegPath, args, {
		encoding: null,
		windowsHide: true,
	});
	if (res.status === 0) return res.stdout || Buffer.alloc(0);
	const err = (res.stderr || Buffer.alloc(0)).toString().slice(0, 4000);
	throw new Error(`${label} failed (code ${res.status}): ${err}`);
}

function computeImageHash(filePath, regionPct = null) {
	if (!ffmpegPath) return null;
	const dims = ffprobeDimensions(filePath);
	if (!dims.width || !dims.height) return null;
	let crop = "";
	if (regionPct && dims.width && dims.height) {
		const rx = Math.max(0, Math.round(dims.width * (regionPct.x || 0)));
		const ry = Math.max(0, Math.round(dims.height * (regionPct.y || 0)));
		const rw = Math.max(1, Math.round(dims.width * (regionPct.w || 1)));
		const rh = Math.max(1, Math.round(dims.height * (regionPct.h || 1)));
		crop = `crop=${rw}:${rh}:${rx}:${ry},`;
	}
	const filter = `${crop}scale=9:8:flags=area,format=gray`;
	const args = [
		"-hide_banner",
		"-loglevel",
		"error",
		"-i",
		filePath,
		"-vf",
		filter,
		"-frames:v",
		"1",
		"-f",
		"rawvideo",
		"pipe:1",
	];
	try {
		const buf = runFfmpegBuffer(args, "presenter_hash");
		if (!buf || buf.length < 72) return null;
		const bits = new Array(64);
		let idx = 0;
		for (let y = 0; y < 8; y++) {
			for (let x = 0; x < 8; x++) {
				const left = buf[y * 9 + x];
				const right = buf[y * 9 + x + 1];
				bits[idx] = left > right ? 1 : 0;
				idx += 1;
			}
		}
		return bits;
	} catch {
		return null;
	}
}

function hashSimilarity(a, b) {
	if (!a || !b || a.length !== b.length) return null;
	let diff = 0;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) diff += 1;
	}
	return 1 - diff / a.length;
}

function comparePresenterSimilarity(originalPath, candidatePath) {
	const regions = [
		PRESENTER_FACE_LOCK_EYES_REGION,
		PRESENTER_FACE_LOCK_REGION,
	];
	const scores = [];
	for (const region of regions) {
		const a = computeImageHash(originalPath, region);
		const b = computeImageHash(candidatePath, region);
		const score = hashSimilarity(a, b);
		if (Number.isFinite(score)) scores.push(score);
	}
	if (!scores.length) return null;
	const sum = scores.reduce((acc, v) => acc + v, 0);
	return sum / scores.length;
}

async function applyPresenterFaceLock({
	basePath,
	editedPath,
	outPath,
	region = PRESENTER_FACE_LOCK_REGION,
	featherPct = PRESENTER_FACE_LOCK_FEATHER_PCT,
}) {
	const dims = ffprobeDimensions(basePath);
	if (!dims.width || !dims.height)
		throw new Error("face_lock_dimensions_missing");
	const w = Math.max(2, Math.round(dims.width));
	const h = Math.max(2, Math.round(dims.height));
	const rx = Math.max(0, Math.round(w * (region.x || 0)));
	const ry = Math.max(0, Math.round(h * (region.y || 0)));
	const rw = Math.max(2, Math.round(w * (region.w || 1)));
	const rh = Math.max(2, Math.round(h * (region.h || 1)));
	const feather = Math.max(
		2,
		Math.round(h * clampNumber(featherPct, 0, 0.2)),
	);
	const mask = `color=black:s=${w}x${h},format=gray,drawbox=x=${rx}:y=${ry}:w=${rw}:h=${rh}:color=white@1:t=fill,boxblur=luma_radius=${feather}:luma_power=1[mask]`;
	const filter = [
		`[0:v]scale=${w}:${h}:flags=lanczos,format=rgba[base]`,
		`[1:v]scale=${w}:${h}:flags=lanczos,format=rgba[edit]`,
		mask,
		`[base][mask]alphamerge[basea]`,
		`[edit][basea]overlay=0:0:format=auto[out]`,
	].join(";");
	await runFfmpeg(
		[
			"-hide_banner",
			"-loglevel",
			"error",
			"-i",
			basePath,
			"-i",
			editedPath,
			"-filter_complex",
			filter,
			"-map",
			"[out]",
			"-frames:v",
			"1",
			"-y",
			outPath,
		],
		"presenter_face_lock",
	);
	return outPath;
}

async function enforcePresenterFaceLock({
	jobId,
	tmpDir,
	basePath,
	editedPath,
	log,
}) {
	if (!PRESENTER_FACE_LOCK_ENABLED || !editedPath) {
		return { path: editedPath, applied: false, scoreBefore: null };
	}
	const scoreBefore = comparePresenterSimilarity(basePath, editedPath);
	const threshold = clampNumber(PRESENTER_FACE_SIMILARITY_MIN, 0.7, 0.99);
	if (!PRESENTER_FACE_LOCK_ALWAYS && Number.isFinite(scoreBefore)) {
		if (scoreBefore >= threshold) {
			return { path: editedPath, applied: false, scoreBefore };
		}
	}
	if (!ffmpegPath) {
		if (Number.isFinite(scoreBefore) && scoreBefore < threshold) {
			throw new Error("face_lock_unavailable_low_similarity");
		}
		return { path: editedPath, applied: false, scoreBefore };
	}
	const outPath = path.join(
		tmpDir || os.tmpdir(),
		`presenter_face_lock_${jobId || "job"}.png`,
	);
	await applyPresenterFaceLock({
		basePath,
		editedPath,
		outPath,
	});
	const scoreAfter = comparePresenterSimilarity(basePath, outPath);
	if (log)
		log("presenter face lock applied", {
			scoreBefore,
			scoreAfter,
			threshold,
		});
	if (Number.isFinite(scoreAfter) && scoreAfter < threshold) {
		throw new Error("face_lock_similarity_too_low");
	}
	return {
		path: outPath,
		applied: true,
		scoreBefore,
		scoreAfter,
	};
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

function hashStringToInt(value = "") {
	let hash = 0;
	const str = String(value || "");
	for (let i = 0; i < str.length; i++) {
		hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
	}
	return hash;
}

function buildWardrobeContextText({ title, topics, categoryLabel }) {
	const topicText = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join(" ")
		: "";
	return `${title || ""} ${topicText} ${categoryLabel || ""}`
		.trim()
		.toLowerCase();
}

function isFormalCategoryLabel(label = "") {
	const normalized = String(label || "")
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, " ")
		.trim();
	if (!normalized) return false;
	for (const keyword of FORMAL_CATEGORY_KEYWORDS) {
		if (normalized.includes(keyword)) return true;
	}
	return false;
}

function isFormalContext({ title, topics, categoryLabel }) {
	const context = buildWardrobeContextText({ title, topics, categoryLabel });
	if (!context) return false;
	if (isFormalCategoryLabel(categoryLabel)) return true;
	if (SERIOUS_CONTEXT_TOKENS.some((token) => token && context.includes(token)))
		return true;
	if (FORMAL_CONTEXT_PHRASES.some((phrase) => context.includes(phrase)))
		return true;
	return false;
}

function pickWardrobeVariant({
	jobId,
	title,
	topics,
	categoryLabel,
	avoidOutfits = [],
}) {
	const topicText = Array.isArray(topics)
		? topics
				.map((t) => t.displayTopic || t.topic || "")
				.filter(Boolean)
				.join("|")
		: "";
	const normalizedAvoid = new Set(
		(avoidOutfits || [])
			.map((v) =>
				String(v || "")
					.trim()
					.toLowerCase()
			)
			.filter(Boolean)
	);
	const prefersFormal = isFormalContext({ title, topics, categoryLabel });
	const noBlazerVariants = WARDROBE_VARIANTS.filter((variant) =>
		NO_BLAZER_PATTERN.test(variant)
	);
	const blazerVariants = WARDROBE_VARIANTS.filter(
		(variant) => !NO_BLAZER_PATTERN.test(variant)
	);
	const primaryPool = prefersFormal ? blazerVariants : noBlazerVariants;
	const candidates = primaryPool.filter(
		(v) => !normalizedAvoid.has(String(v).toLowerCase())
	);
	const fallbackPool = WARDROBE_VARIANTS.filter(
		(v) => !normalizedAvoid.has(String(v).toLowerCase())
	);
	const pool = candidates.length
		? candidates
		: fallbackPool.length
		? fallbackPool
		: WARDROBE_VARIANTS;
	const jitter = `${Date.now()}-${Math.random()}`;
	const seed = `${jobId || ""}|${title || ""}|${topicText}|${jitter}`;
	const idx = hashStringToInt(seed) % pool.length;
	return pool[idx] || WARDROBE_VARIANTS[0];
}

function fallbackWardrobePrompt({ topicLine, wardrobeVariant }) {
	return `
Use @presenter_ref for exact framing, pose, lighting, desk, and studio environment.
Change ONLY the outfit on the torso/upper body area to a dark, classy outfit. Outfit spec (use exactly): ${wardrobeVariant}.
Outfit colors must be dark only (charcoal, black, deep navy, deep forest green, dark burgundy, oxblood, deep teal, dark aubergine). No bright or light colors.
Outfit must be intact: no rips, tears, holes, or missing fabric; shirt placket straight and buttons aligned.
Do NOT alter any pixels of the face or head at all. Keep glasses, beard, hairline, skin texture, and facial features exactly as in @presenter_ref. Single face only, no ghosting.
Face is LOCKED: no edits above the collarbone, no smoothing, no retouching, no lighting changes on the face, no eye or mouth changes.
Studio background, desk, lighting, camera angle, and all props must remain EXACTLY the same.
No crop/zoom, no borders/letterboxing/vignettes, no added blur or beautification; keep exact framing and processing.
No candles, no extra objects, no text, no logos. Topic context: ${topicLine}.
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
	avoidOutfits = [],
	log,
}) {
	const topicLine = buildTopicLine({ title, topics });
	const wardrobeVariant = pickWardrobeVariant({
		jobId,
		title,
		topics,
		categoryLabel,
		avoidOutfits,
	});
	if (log)
		log("wardrobe variation selected", {
			variant: wardrobeVariant,
			avoided: (avoidOutfits || []).length,
			formalContext: isFormalContext({ title, topics, categoryLabel }),
		});
	if (!openai) {
		const fallback = {
			wardrobePrompt: fallbackWardrobePrompt({
				topicLine,
				wardrobeVariant,
			}),
		};
		if (log)
			log("orchestrator prompts (fallback)", {
				wardrobe: fallback.wardrobePrompt.slice(0, 300),
			});
		return fallback;
	}

	const system = `
You write precise, regular descriptive prompts for Runway gen4_image.
Return JSON only with key: wardrobePrompt.
	Rules:
	- Use @presenter_ref as the only person reference.
	- Face is strictly locked: do NOT alter any pixels of the face or head in any way; no double face, no ghosting, no artifacts, no retouching or smoothing. Treat everything above the collarbone as read-only.
	- Keep studio/desk/background/camera/framing/lighting unchanged; no crop/zoom, no borders/letterboxing/vignettes, no added blur or processing.
	- Wardrobe: vary the outfit each run using the provided wardrobe variation cue; include it exactly. If the cue says "no blazer", do not add a blazer. Use dark colors only and an open collar.
- Outfit must be intact: no rips, tears, holes, missing fabric, or broken seams; shirt placket straight and buttons aligned.
- Keep prompts concise and avoid phrasing that implies identity manipulation or deepfakes.
- No extra objects and no added text/logos; no watermarks.
`.trim();

	const userText = `
Title: ${String(title || "").trim()}
Topics: ${topicLine}
	Category: ${String(categoryLabel || "").trim()}
	Wardrobe variation cue (use exactly): ${wardrobeVariant}
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
					],
				},
			],
			temperature: 0.4,
			max_completion_tokens: 500,
		});
		const content = String(resp?.choices?.[0]?.message?.content || "").trim();
		const parsed = parseJsonObject(content);
		if (parsed && parsed.wardrobePrompt) {
			const integrityLine =
				"Outfit must be intact: no rips, tears, holes, or missing fabric; shirt placket straight and buttons aligned.";
			const promptBase = String(parsed.wardrobePrompt).trim();
			const promptWithIntegrity = promptBase.toLowerCase().includes("rips")
				? promptBase
				: `${promptBase}\n${integrityLine}`;
			const result = {
				wardrobePrompt: promptWithIntegrity,
				wardrobeVariant,
			};
			if (log)
				log("orchestrator prompts", {
					wardrobe: result.wardrobePrompt.slice(0, 300),
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
		wardrobeVariant,
	};
	if (log)
		log("orchestrator prompts (fallback)", {
			wardrobe: fallback.wardrobePrompt.slice(0, 300),
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
	if (!RUNWAY_API_KEY) throw new Error("RUNWAY_API_KEY missing");
	if (!presenterLocalPath || !fs.existsSync(presenterLocalPath))
		throw new Error("presenter_base_missing");

	const workingDir = tmpDir || path.join(os.tmpdir(), "presenter_adjustments");
	ensureDir(workingDir);
	ensurePresenterFile(presenterLocalPath);

	const prompts = await buildOrchestratedPrompts({
		jobId,
		title,
		topics,
		categoryLabel,
		avoidOutfits: recentOutfits,
		log,
	});
	const presenterOutfit = String(prompts.wardrobeVariant || "").trim();

	let outfitPath = null;
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
		if (PRESENTER_FACE_LOCK_ENABLED) {
			try {
				const lockResult = await enforcePresenterFaceLock({
					jobId,
					tmpDir: workingDir,
					basePath: presenterLocalPath,
					editedPath: outfitPath,
					log,
				});
				if (lockResult?.path && lockResult.path !== outfitPath) {
					safeUnlink(outfitPath);
					outfitPath = lockResult.path;
					ensurePresenterFile(outfitPath);
				}
			} catch (e) {
				if (log)
					log("presenter face lock failed; using original presenter", {
						error: e?.message || String(e),
					});
				safeUnlink(outfitPath);
				return {
					localPath: presenterLocalPath,
					url: "",
					publicId: "",
					width: 0,
					height: 0,
					method: "face_lock_fallback_original",
					presenterOutfit: "",
				};
			}
		}
		finalUpload = await uploadPresenterToCloudinary(
			outfitPath,
			jobId,
			PRESENTER_CLOUDINARY_PUBLIC_PREFIX
		);
	} catch (e) {
		if (log)
			log("runway wardrobe stage failed", {
				error: e?.message || String(e),
			});
		throw e;
	}

	return {
		localPath: outfitPath,
		url: finalUpload?.url || "",
		publicId: finalUpload?.public_id || "",
		width: finalUpload?.width || 0,
		height: finalUpload?.height || 0,
		method: "runway_outfit",
		presenterOutfit,
	};
}

module.exports = {
	generatePresenterAdjustedImage,
};
