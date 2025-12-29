/** @format */

const fs = require("fs");
const path = require("path");
const child_process = require("child_process");
const axios = require("axios");
const { OpenAI, toFile } = require("openai");

let ffmpegPath = process.env.FFMPEG_PATH || "";
if (!ffmpegPath) {
	try {
		// eslint-disable-next-line import/no-extraneous-dependencies
		ffmpegPath = require("ffmpeg-static");
	} catch {
		ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "ffmpeg";
	}
}

const DEFAULT_IMAGE_MODEL = "gpt-image-1";
const DEFAULT_IMAGE_SIZE = "1536x1024";
const DEFAULT_IMAGE_QUALITY = "high";
const DEFAULT_IMAGE_INPUT_FIDELITY = "high";
const MAX_INPUT_IMAGES = 4;
const DEFAULT_CANVAS_WIDTH = 1280;
const DEFAULT_CANVAS_HEIGHT = 720;
const LEFT_PANEL_PCT = 0.48;
const PANEL_MARGIN_PCT = 0.035;
const PRESENTER_OVERLAP_PCT = 0.06;

const SORA_MODEL = process.env.SORA_MODEL || "sora-2-pro";
const SORA_THUMBNAIL_ENABLED =
	String(process.env.SORA_THUMBNAIL_ENABLED ?? "true").toLowerCase() !==
	"false";
const SORA_THUMBNAIL_SECONDS = String(
	process.env.SORA_THUMBNAIL_SECONDS || "4"
);
const SORA_POLL_INTERVAL_MS = 2000;
const SORA_MAX_POLL_ATTEMPTS = 120;
const SORA_PROMPT_CHAR_LIMIT = 320;

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
Composition: presenter on the right third (face and shoulders fully inside the right third), leave the left ~40% clean for headline text.
${topicImageLine}
Style: ultra sharp, clean, premium, high contrast, cinematic studio lighting, shallow depth of field, crisp subject separation.
Expression: confident, intrigued, camera-ready.
No candles, no logos, no watermarks, no extra people, no extra hands, no distortion, no text.
`.trim();
}

function buildSoraThumbnailPrompt({ title, topics }) {
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

	const prompt = `
Cinematic studio background plate for a YouTube thumbnail.
No people, no faces, no text, no logos, no watermarks.
Left ~40% is clean, brighter, and ready for headline text + one hero image; right side darker and unobtrusive.
Subtle topic-related props or atmosphere inspired by: ${topicFocus}.
High contrast, crisp detail, premium lighting, shallow depth of field, soft vignette, rich but tasteful color.
`.trim();

	return prompt.length > SORA_PROMPT_CHAR_LIMIT
		? prompt.slice(0, SORA_PROMPT_CHAR_LIMIT)
		: prompt;
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
		if (
			head.toString("ascii", 0, 4) === "RIFF" &&
			head.toString("ascii", 8, 12) === "WEBP"
		)
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

function makeEven(n) {
	const x = Math.round(Number(n) || 0);
	if (!Number.isFinite(x) || x <= 0) return 2;
	return x % 2 === 0 ? x : x + 1;
}

function soraSizeForRatio(ratio) {
	switch (ratio) {
		case "720:1280":
		case "832:1104":
			return "720x1280";
		default:
			return "1280x720";
	}
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

async function downloadUrlToFile(url, outPath) {
	const res = await axios.get(url, {
		responseType: "arraybuffer",
		timeout: 60000,
	});
	if (!res?.data) throw new Error("thumbnail download empty");
	fs.writeFileSync(outPath, res.data);
	return outPath;
}

async function extractFrameFromVideo({ videoPath, outPath }) {
	const ext = path.extname(outPath).toLowerCase();
	const useJpegQ = ext === ".jpg" || ext === ".jpeg";
	const args = [
		"-ss",
		"0.4",
		"-i",
		videoPath,
		"-frames:v",
		"1",
	];
	if (useJpegQ) {
		args.push("-q:v", "2");
	}
	args.push("-y", outPath);
	await runFfmpeg(args, "sora_thumbnail_frame");
	return outPath;
}

async function generateSoraThumbnailFrame({
	jobId,
	tmpDir,
	prompt,
	ratio = "1280:720",
	openai,
	model = SORA_MODEL,
	log,
}) {
	if (!SORA_THUMBNAIL_ENABLED) return null;
	const apiKey = process.env.CHATGPT_API_TOKEN;
	if (!apiKey) throw new Error("CHATGPT_API_TOKEN missing");
	const client = openai || new OpenAI({ apiKey });
	const size = soraSizeForRatio(ratio);

	if (log) log("thumbnail sora prompt", { prompt: prompt.slice(0, 200), size });

	let job = null;
	try {
		job = await client.videos.create({
			model,
			prompt,
			seconds: SORA_THUMBNAIL_SECONDS,
			size,
		});
	} catch (err) {
		if (log)
			log("thumbnail sora create failed", {
				message: err?.message,
				code: err?.code || err?.response?.data?.error?.code || null,
				status: err?.response?.status || null,
			});
		throw err;
	}

	const running = new Set(["queued", "in_progress", "processing"]);
	let attempts = 0;
	while (
		running.has(String(job?.status || "")) &&
		attempts < SORA_MAX_POLL_ATTEMPTS
	) {
		await sleep(SORA_POLL_INTERVAL_MS);
		try {
			const updated = await client.videos.retrieve(job.id);
			if (updated) job = updated;
		} catch (pollErr) {
			if (log)
				log("thumbnail sora poll failed", {
					attempt: attempts + 1,
					message: pollErr?.message,
					code: pollErr?.code || pollErr?.response?.data?.error?.code || null,
					status: pollErr?.response?.status || null,
				});
		}
		attempts++;
	}

	if (String(job?.status) !== "completed") {
		const jobErr = job?.error || job?.last_error || job?.failure || null;
		const code = jobErr?.code || jobErr?.type || null;
		const message =
			jobErr?.message || jobErr?.error?.message || job?.failure_reason || null;
		const err = new Error(
			message ||
				`Sora job ${job?.id} failed (status=${job?.status || "unknown"})`
		);
		err.code = code;
		err.jobId = job?.id;
		err.status = job?.status;
		throw err;
	}

	let response = null;
	try {
		response = await client.videos.downloadContent(job.id, {
			variant: "video",
		});
	} catch {
		response = await client.videos.downloadContent(job.id, {
			variant: "mp4",
		});
	}

	const buf = Buffer.from(await response.arrayBuffer());
	const videoPath = path.join(tmpDir, `thumb_sora_${jobId}.mp4`);
	fs.writeFileSync(videoPath, buf);
	const framePath = path.join(tmpDir, `thumb_sora_${jobId}.png`);
	await extractFrameFromVideo({ videoPath, outPath: framePath });
	return framePath;
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

async function composeThumbnailBase({
	baseImagePath,
	presenterImagePath,
	topicImagePaths = [],
	outPath,
	width = DEFAULT_CANVAS_WIDTH,
	height = DEFAULT_CANVAS_HEIGHT,
}) {
	const W = makeEven(Math.max(2, Math.round(width)));
	const H = makeEven(Math.max(2, Math.round(height)));
	const leftW = Math.max(1, Math.round(W * LEFT_PANEL_PCT));
	const margin = Math.max(4, Math.round(W * PANEL_MARGIN_PCT));
	const overlap = Math.max(0, Math.round(W * PRESENTER_OVERLAP_PCT));
	let presenterW = Math.max(2, W - leftW + overlap);
	presenterW = makeEven(Math.min(W, presenterW));
	const presenterX = Math.max(
		0,
		Math.min(W - presenterW, leftW - overlap)
	);

	const topics = Array.isArray(topicImagePaths)
		? topicImagePaths.filter(Boolean).slice(0, 2)
		: [];
	const panelCount = topics.length;
	const panelW = Math.max(1, leftW - margin * 2);
	const panelH =
		panelCount > 1
			? Math.max(1, Math.round((H - margin * 3) / 2))
			: Math.max(1, H - margin * 2);

	const inputs = [baseImagePath, presenterImagePath, ...topics];
	const filters = [];
	filters.push(
		`[0:v]scale=${W}:${H}:force_original_aspect_ratio=increase:flags=lanczos,crop=${W}:${H}[base]`
	);
	filters.push(
		`[1:v]scale=${presenterW}:${H}:force_original_aspect_ratio=decrease:flags=lanczos,pad=${presenterW}:${H}:(ow-iw)/2:(oh-ih)/2[presenter]`
	);

	let current = "[base]";

	if (panelCount >= 1) {
		const panel1Idx = 2;
		const panelY =
			panelCount > 1 ? margin : Math.max(0, Math.round((H - panelH) / 2));
		filters.push(
			`[${panel1Idx}:v]scale=${panelW}:${panelH}:force_original_aspect_ratio=increase:flags=lanczos,crop=${panelW}:${panelH}[panel1]`
		);
		filters.push(`${current}[panel1]overlay=${margin}:${panelY}[tmp1]`);
		current = "[tmp1]";
	}

	if (panelCount >= 2) {
		const panel2Idx = 3;
		const panel2Y = Math.max(0, margin * 2 + panelH);
		filters.push(
			`[${panel2Idx}:v]scale=${panelW}:${panelH}:force_original_aspect_ratio=increase:flags=lanczos,crop=${panelW}:${panelH}[panel2]`
		);
		filters.push(`${current}[panel2]overlay=${margin}:${panel2Y}[tmp2]`);
		current = "[tmp2]";
	}

	filters.push(`${current}[presenter]overlay=${presenterX}:0[outv]`);

	const outExt = path.extname(outPath).toLowerCase();
	const useJpegQ = outExt === ".jpg" || outExt === ".jpeg";
	const args = [];
	for (const input of inputs) {
		args.push("-i", input);
	}
	args.push(
		"-filter_complex",
		filters.join(";"),
		"-map",
		"[outv]",
		"-frames:v",
		"1"
	);
	if (useJpegQ) args.push("-q:v", "2");
	args.push("-y", outPath);

	await runFfmpeg(args, "thumbnail_compose");
	return outPath;
}

async function generateThumbnailCompositeBase({
	jobId,
	tmpDir,
	presenterImagePath,
	topicImagePaths = [],
	title,
	topics,
	ratio = "1280:720",
	width = DEFAULT_CANVAS_WIDTH,
	height = DEFAULT_CANVAS_HEIGHT,
	openai,
	log,
	useSora = true,
}) {
	if (!presenterImagePath)
		throw new Error("thumbnail_presenter_missing_or_invalid");

	let baseImagePath = presenterImagePath;
	if (useSora && SORA_THUMBNAIL_ENABLED) {
		const prompt = buildSoraThumbnailPrompt({ title, topics });
		try {
			const soraFrame = await generateSoraThumbnailFrame({
				jobId,
				tmpDir,
				prompt,
				ratio,
				openai,
				log,
			});
			if (soraFrame) baseImagePath = soraFrame;
		} catch (e) {
			if (log)
				log("thumbnail sora failed; using presenter base", {
					error: e.message,
				});
		}
	}

	const outPath = path.join(tmpDir, `thumb_composite_${jobId}.png`);
	return await composeThumbnailBase({
		baseImagePath,
		presenterImagePath,
		topicImagePaths,
		outPath,
		width,
		height,
	});
}

module.exports = {
	buildThumbnailPrompt,
	buildSoraThumbnailPrompt,
	generateThumbnailCompositeBase,
};
