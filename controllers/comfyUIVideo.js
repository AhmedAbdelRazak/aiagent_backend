require("dotenv").config();

const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { execFileSync } = require("child_process");
const axios = require("axios");
const FormData = require("form-data");

let ffmpegPath = "";
try {
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "/usr/bin/ffmpeg";
}

const DEFAULT_COMFY_URL = "http://127.0.0.1:8188";
const DEFAULT_PRESENTER_ASSET_URL =
	"https://res.cloudinary.com/infiniteapps/image/upload/v1767062842/aivideomatic/long_thumbnails/MyPhotoWithASuit_s1xay4.png";

function normalizeWhitespace(value = "") {
	return String(value || "").replace(/\s+/g, " ").trim();
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

function clampNumber(value, fallback, min, max) {
	const parsed = Number(value);
	const n = Number.isFinite(parsed) ? parsed : fallback;
	return Math.max(min, Math.min(max, n));
}

function getComfyVideoConfig(overrides = {}) {
	const root =
		normalizeWhitespace(process.env.COMFYUI_ROOT_DIR) ||
		(process.platform === "win32" ? "" : "/home/ahmedadmin/ai-lab/ComfyUI");
	return {
		enabled: truthyEnv(process.env.COMFY_VIDEO_ENABLED, true),
		url: normalizeWhitespace(process.env.COMFYUI_URL || DEFAULT_COMFY_URL).replace(
			/\/+$/,
			"",
		),
		rootDir: root,
		inputDir:
			normalizeWhitespace(process.env.COMFYUI_INPUT_DIR) ||
			(root ? path.join(root, "input") : ""),
		outputDir:
			normalizeWhitespace(process.env.COMFYUI_OUTPUT_DIR) ||
			(root ? path.join(root, "output") : ""),
		width: numberEnv("COMFY_VIDEO_WIDTH", 768, 256, 1280),
		height: numberEnv("COMFY_VIDEO_HEIGHT", 432, 256, 1280),
		fps: numberEnv("COMFY_VIDEO_FPS", 15, 6, 30),
		crf: Math.round(numberEnv("COMFY_VIDEO_CRF", 20, 15, 35)),
		durationSec: numberEnv("COMFY_VIDEO_DURATION_SEC", 6, 2, 20),
		timeoutMs: numberEnv("COMFY_VIDEO_TIMEOUT_MS", 35 * 60 * 1000, 60 * 1000, 90 * 60 * 1000),
		pollMs: numberEnv("COMFY_VIDEO_POLL_MS", 2500, 1000, 10000),
		maxTempC: numberEnv("COMFY_VIDEO_MAX_TEMP_C", 93, 70, 105),
		preflightMaxTempC: numberEnv("COMFY_VIDEO_PREFLIGHT_MAX_TEMP_C", 88, 50, 100),
		preflightCooldownMs: numberEnv(
			"COMFY_VIDEO_PREFLIGHT_COOLDOWN_MS",
			2 * 60 * 1000,
			0,
			15 * 60 * 1000,
		),
		maxDiskUsedPercent: numberEnv("COMFY_VIDEO_MAX_DISK_USED_PERCENT", 40, 10, 95),
		cropFactor: numberEnv("COMFY_VIDEO_CROP_FACTOR", 1.7, 1.5, 2.5),
		retargetingEyes: numberEnv("COMFY_VIDEO_RETARGETING_EYES", 0, 0, 1),
		retargetingMouth: numberEnv("COMFY_VIDEO_RETARGETING_MOUTH", 0, 0, 1),
		keepComfyOutputs: truthyEnv(process.env.COMFY_VIDEO_KEEP_OUTPUTS, false),
		keepComfyInputs: truthyEnv(process.env.COMFY_VIDEO_KEEP_INPUTS, false),
		...overrides,
	};
}

function safeUnlink(filePath) {
	try {
		if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
	} catch {}
}

function ensureDir(dir) {
	if (dir) fs.mkdirSync(dir, { recursive: true });
}

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

function sanitizeName(value = "comfy_video") {
	const clean = String(value || "comfy_video")
		.toLowerCase()
		.replace(/[^a-z0-9._-]+/g, "_")
		.replace(/^_+|_+$/g, "")
		.slice(0, 80);
	return clean || "comfy_video";
}

function runFfmpeg(args, label = "ffmpeg") {
	if (!ffmpegPath) throw new Error("ffmpeg_unavailable");
	try {
		execFileSync(ffmpegPath, ["-hide_banner", "-loglevel", "error", ...args], {
			maxBuffer: 64 * 1024 * 1024,
			stdio: ["ignore", "pipe", "pipe"],
			windowsHide: true,
		});
	} catch (error) {
		const stderr = String(error?.stderr || error?.message || "").trim();
		throw new Error(`${label}_failed${stderr ? `:${stderr}` : ""}`);
	}
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

function readTempFileC(filePath) {
	try {
		const raw = Number(fs.readFileSync(filePath, "utf8").trim());
		if (!Number.isFinite(raw)) return null;
		const celsius = raw > 1000 ? raw / 1000 : raw;
		return celsius >= 15 && celsius <= 125 ? celsius : null;
	} catch {
		return null;
	}
}

function readMaxCpuTemperatureC() {
	if (process.platform === "win32") return null;
	const values = [];
	try {
		for (const entry of fs.readdirSync("/sys/class/thermal")) {
			if (!/^thermal_zone\d+$/.test(entry)) continue;
			const dir = path.join("/sys/class/thermal", entry);
			const type = normalizeWhitespace(
				fs.existsSync(path.join(dir, "type"))
					? fs.readFileSync(path.join(dir, "type"), "utf8")
					: "",
			).toLowerCase();
			if (type && !/cpu|pkg|package|x86|core|acpi|thermal|pch|k10|zen/i.test(type)) {
				continue;
			}
			const temp = readTempFileC(path.join(dir, "temp"));
			if (temp != null) values.push(temp);
		}
	} catch {}
	try {
		for (const hwmon of fs.readdirSync("/sys/class/hwmon")) {
			const dir = path.join("/sys/class/hwmon", hwmon);
			for (const entry of fs.readdirSync(dir)) {
				const match = entry.match(/^temp(\d+)_input$/);
				if (!match) continue;
				const labelPath = path.join(dir, `temp${match[1]}_label`);
				const label = normalizeWhitespace(
					fs.existsSync(labelPath) ? fs.readFileSync(labelPath, "utf8") : "",
				).toLowerCase();
				if (label && !/cpu|package|core|tdie|tctl|x86|k10|zen|sensor/i.test(label)) {
					continue;
				}
				const temp = readTempFileC(path.join(dir, entry));
				if (temp != null) values.push(temp);
			}
		}
	} catch {}
	return values.length ? Math.max(...values) : null;
}

function readRootDiskUsedPercent(config = {}) {
	if (process.platform === "win32") return null;
	try {
		const target = config.rootDir || config.outputDir || "/";
		const output = execFileSync("df", ["-P", target], {
			encoding: "utf8",
			stdio: ["ignore", "pipe", "ignore"],
		});
		const line = output.trim().split(/\n/)[1] || "";
		const parts = line.trim().split(/\s+/);
		const used = Number(String(parts[4] || "").replace("%", ""));
		return Number.isFinite(used) ? used : null;
	} catch {
		return null;
	}
}

function systemSnapshot(config = {}) {
	return {
		tempC: readMaxCpuTemperatureC(),
		memAvailableMb: Math.round(os.freemem() / 1024 / 1024),
		diskUsedPercent: readRootDiskUsedPercent(config),
	};
}

async function interruptComfy(config, log, reason = {}) {
	try {
		await comfyRequest(config, "POST", "/interrupt", {}, { timeout: 8000 });
		if (typeof log === "function") log("comfy video interrupted", reason);
	} catch (error) {
		if (typeof log === "function") {
			log("comfy video interrupt failed", {
				...reason,
				error: error?.message || String(error),
			});
		}
	}
}

async function waitForSafeComfyTemperature(config, log, stage = "video") {
	const started = Date.now();
	while (true) {
		const snap = systemSnapshot(config);
		if (snap.diskUsedPercent != null && snap.diskUsedPercent > config.maxDiskUsedPercent) {
			throw new Error(
				`comfy_video_disk_guard:${snap.diskUsedPercent}>${config.maxDiskUsedPercent}`,
			);
		}
		if (snap.tempC == null || snap.tempC < config.preflightMaxTempC) return;
		if (Date.now() - started >= config.preflightCooldownMs) {
			throw new Error(`comfy_video_preflight_hot:${snap.tempC.toFixed(1)}C`);
		}
		if (typeof log === "function") {
			log("comfy video cooling before queue", {
				stage,
				tempC: Number(snap.tempC.toFixed(1)),
				preflightMaxTempC: config.preflightMaxTempC,
				waitSec: Math.round((Date.now() - started) / 1000),
			});
		}
		await sleep(Math.min(30000, Math.max(5000, config.pollMs * 3)));
	}
}

async function freeComfyMemory(config, log) {
	try {
		await comfyRequest(
			config,
			"POST",
			"/free",
			{ unload_models: true, free_memory: true },
			{ timeout: 10000 },
		);
		if (typeof log === "function") log("comfy video memory released", {});
	} catch (error) {
		if (typeof log === "function") {
			log("comfy video memory release skipped", {
				error: error?.message || String(error),
			});
		}
	}
}

async function uploadComfyInput(config, filePath, filename) {
	if (!fs.existsSync(filePath)) throw new Error(`comfy_video_input_missing:${filePath}`);
	const form = new FormData();
	form.append("image", fs.createReadStream(filePath), filename || path.basename(filePath));
	form.append("type", "input");
	form.append("overwrite", "true");
	const response = await axios.post(`${config.url}/upload/image`, form, {
		headers: form.getHeaders(),
		maxBodyLength: Infinity,
		timeout: 120000,
		validateStatus: (status) => status >= 200 && status < 300,
	});
	return response.data || {};
}

function resolveComfyFilePath(config, file = {}) {
	const type = String(file.type || "output").toLowerCase();
	const base = type === "input" ? config.inputDir : config.outputDir;
	if (!base || !file.filename) return "";
	const subfolder = String(file.subfolder || "").replace(/^[/\\]+|[/\\]+$/g, "");
	return path.resolve(base, subfolder, file.filename);
}

async function downloadComfyFile(config, file, outPath) {
	ensureDir(path.dirname(outPath));
	const resolved = resolveComfyFilePath(config, file);
	if (resolved && fs.existsSync(resolved)) {
		fs.copyFileSync(resolved, outPath);
		return outPath;
	}
	const params = new URLSearchParams({
		filename: file.filename,
		subfolder: file.subfolder || "",
		type: file.type || "output",
	});
	const response = await axios({
		method: "GET",
		url: `${config.url}/view?${params.toString()}`,
		responseType: "stream",
		timeout: 120000,
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

function collectVideoFiles(value, found = []) {
	if (!value) return found;
	if (Array.isArray(value)) {
		value.forEach((item) => collectVideoFiles(item, found));
		return found;
	}
	if (typeof value === "object") {
		const filename = String(value.filename || "");
		if (/\.(mp4|mov|mkv|webm)$/i.test(filename)) {
			found.push({
				filename,
				subfolder: value.subfolder || "",
				type: value.type || "output",
				format: value.format || "",
				frame_rate: value.frame_rate || value.fps || null,
			});
		}
		Object.values(value).forEach((item) => collectVideoFiles(item, found));
	}
	return found;
}

async function waitForComfyVideo(config, promptId, log) {
	const started = Date.now();
	let lastLog = 0;
	while (Date.now() - started < config.timeoutMs) {
		const snap = systemSnapshot(config);
		if (snap.diskUsedPercent != null && snap.diskUsedPercent > config.maxDiskUsedPercent) {
			await interruptComfy(config, log, {
				reason: "disk_guard",
				diskUsedPercent: snap.diskUsedPercent,
			});
			throw new Error(
				`comfy_video_disk_guard:${snap.diskUsedPercent}>${config.maxDiskUsedPercent}`,
			);
		}
		if (snap.tempC != null && snap.tempC >= config.maxTempC) {
			await interruptComfy(config, log, {
				reason: "thermal_guard",
				tempC: Number(snap.tempC.toFixed(1)),
				maxTempC: config.maxTempC,
			});
			throw new Error(`comfy_video_thermal_guard:${snap.tempC.toFixed(1)}C`);
		}
		if (typeof log === "function" && Date.now() - lastLog > 20000) {
			lastLog = Date.now();
			log("comfy video monitor", {
				promptId,
				elapsedSec: Math.round((Date.now() - started) / 1000),
				...snap,
			});
		}

		const history = await comfyRequest(
			config,
			"GET",
			`/history/${encodeURIComponent(promptId)}`,
			null,
			{ timeout: 15000 },
		);
		const item = history?.[promptId];
		const files = collectVideoFiles(item?.outputs || item);
		if (files.length) return files[files.length - 1];
		const statusText = normalizeWhitespace(item?.status?.status_str).toLowerCase();
		if (statusText === "error") {
			throw new Error(
				`comfy_video_failed:${JSON.stringify(item?.status || {}).slice(0, 700)}`,
			);
		}
		await sleep(config.pollMs);
	}
	await interruptComfy(config, log, { reason: "timeout", promptId });
	throw new Error(`comfy_video_timeout:${promptId}`);
}

async function downloadToFile(url, outPath, timeoutMs = 60000) {
	ensureDir(path.dirname(outPath));
	const response = await axios({
		method: "GET",
		url,
		responseType: "stream",
		timeout: timeoutMs,
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

function preparePresenterImage({ sourcePath, outputPath, width, height }) {
	ensureDir(path.dirname(outputPath));
	runFfmpeg(
		[
			"-y",
			"-i",
			sourcePath,
			"-vf",
			`scale=${width}:${height}:force_original_aspect_ratio=increase:flags=lanczos,crop=${width}:${height}:(iw-ow)/2:(ih-oh)/2,setsar=1`,
			"-frames:v",
			"1",
			outputPath,
		],
		"comfy_video_prepare_presenter",
	);
	return outputPath;
}

function normalizeExpression(expression = "") {
	const raw = normalizeWhitespace(expression).toLowerCase();
	if (/\b(warm|friendly|smile|approachable)\b/.test(raw)) return "warm";
	if (/\b(excited|engaged|energy|surprised)\b/.test(raw)) return "excited";
	if (/\b(serious|concern|warning|urgent)\b/.test(raw)) return "serious";
	if (/\b(thoughtful|reflect|consider|listen)\b/.test(raw)) return "thoughtful";
	return "neutral";
}

function inferExpressionFromPrompt(promptText = "", fallback = "neutral") {
	const prompt = normalizeWhitespace(promptText).toLowerCase();
	if (/\b(warm|friendly|approachable|light smile)\b/.test(prompt)) return "warm";
	if (/\b(excited|engaged|high energy|attentive)\b/.test(prompt)) return "excited";
	if (/\b(serious|concern|warning|steady|no smile)\b/.test(prompt)) return "serious";
	if (/\b(thoughtful|composed|listening|reflective)\b/.test(prompt)) return "thoughtful";
	return normalizeExpression(fallback);
}

function expressionPresets(expression = "neutral") {
	const expr = normalizeExpression(expression);
	const base = [
		{ rotate_pitch: 0, rotate_yaw: -0.8, blink: 1.5, eyebrow: 0, aaa: 1, smile: 0 },
		{ rotate_pitch: -0.8, rotate_yaw: 0.8, blink: 0, eyebrow: 0.3, aaa: 2, smile: 0.02 },
		{ rotate_pitch: 0.6, rotate_yaw: 0, blink: 2.5, eyebrow: 0, aaa: 0, smile: 0 },
	];
	if (expr === "warm") {
		return base.map((p, i) => ({
			...p,
			smile: [0.08, 0.12, 0.06][i] || 0.08,
			eyebrow: [0.2, 0.4, 0.1][i] || 0.2,
		}));
	}
	if (expr === "excited") {
		return base.map((p, i) => ({
			...p,
			rotate_pitch: [-1.2, -0.4, 0.5][i] || p.rotate_pitch,
			eyebrow: [0.8, 1.1, 0.5][i] || 0.8,
			aaa: [3, 4, 1][i] || 2,
			smile: [0.05, 0.08, 0.04][i] || 0.05,
		}));
	}
	if (expr === "serious") {
		return base.map((p, i) => ({
			...p,
			rotate_pitch: [0.4, -0.4, 0.2][i] || p.rotate_pitch,
			eyebrow: [-0.4, -0.2, -0.3][i] || -0.3,
			aaa: [0, 1, 0][i] || 0,
			smile: -0.02,
		}));
	}
	if (expr === "thoughtful") {
		return base.map((p, i) => ({
			...p,
			rotate_yaw: [-1.2, 0.4, 0.9][i] || p.rotate_yaw,
			pupil_x: [-0.8, 0, 0.5][i] || 0,
			eyebrow: [0, 0.2, 0][i] || 0,
			smile: 0,
		}));
	}
	return base;
}

function buildMotionCommand({ durationSec, fps, expressionCount }) {
	const targetFrames = Math.max(12, Math.round(durationSec * fps));
	const sequence = [];
	let frames = 0;
	let idx = 1;
	while (frames < targetFrames) {
		const change = Math.min(5, Math.max(2, targetFrames - frames));
		const hold = Math.min(8, Math.max(0, targetFrames - frames - change));
		sequence.push(`${idx} = ${change}:${hold}`);
		frames += change + hold;
		idx = idx >= expressionCount ? 0 : idx + 1;
		if (idx === 0 && frames < targetFrames) {
			const backChange = Math.min(4, Math.max(2, targetFrames - frames));
			const backHold = Math.min(7, Math.max(0, targetFrames - frames - backChange));
			sequence.push(`0 = ${backChange}:${backHold}`);
			frames += backChange + backHold;
			idx = 1;
		}
	}
	return sequence.join("\n");
}

function editorInputs(preset, config, previousMotionLink, srcImageLink) {
	return {
		rotate_pitch: Number(preset.rotate_pitch || 0),
		rotate_yaw: Number(preset.rotate_yaw || 0),
		rotate_roll: Number(preset.rotate_roll || 0),
		blink: Number(preset.blink || 0),
		eyebrow: Number(preset.eyebrow || 0),
		wink: Number(preset.wink || 0),
		pupil_x: Number(preset.pupil_x || 0),
		pupil_y: Number(preset.pupil_y || 0),
		aaa: Number(preset.aaa || 0),
		eee: Number(preset.eee || 0),
		woo: Number(preset.woo || 0),
		smile: Number(preset.smile || 0),
		src_ratio: 1,
		sample_ratio: 1,
		sample_parts: "All",
		crop_factor: config.cropFactor,
		...(srcImageLink ? { src_image: srcImageLink } : {}),
		...(previousMotionLink ? { motion_link: previousMotionLink } : {}),
	};
}

function buildAdvancedLivePortraitWorkflow({
	uploadedImageName,
	config,
	expression = "neutral",
	durationSec,
	fps,
	filenamePrefix,
}) {
	const presets = expressionPresets(expression);
	const workflow = {
		"1": {
			class_type: "LoadImage",
			inputs: { image: uploadedImageName },
		},
	};
	let previousMotionLink = null;
	presets.forEach((preset, index) => {
		const id = String(10 + index);
		workflow[id] = {
			class_type: "ExpressionEditor",
			inputs: editorInputs(
				preset,
				config,
				previousMotionLink,
				index === 0 ? ["1", 0] : null,
			),
		};
		previousMotionLink = [id, 1];
	});
	workflow["50"] = {
		class_type: "AdvancedLivePortrait",
		inputs: {
			retargeting_eyes: config.retargetingEyes,
			retargeting_mouth: config.retargetingMouth,
			crop_factor: config.cropFactor,
			turn_on: true,
			tracking_src_vid: false,
			animate_without_vid: true,
			command: buildMotionCommand({
				durationSec,
				fps,
				expressionCount: presets.length,
			}),
			motion_link: previousMotionLink,
		},
	};
	workflow["60"] = {
		class_type: "VHS_VideoCombine",
		inputs: {
			images: ["50", 0],
			frame_rate: fps,
			loop_count: 0,
			filename_prefix: filenamePrefix,
			format: "video/h264-mp4",
			pix_fmt: "yuv420p",
			crf: config.crf,
			save_metadata: false,
			trim_to_audio: false,
			pingpong: false,
			save_output: true,
		},
	};
	return workflow;
}

function parseComfyUri(value = "") {
	const raw = String(value || "");
	const match = raw.match(/^comfy:\/\/(input|output)\/(.+)$/i);
	if (!match) return null;
	return {
		type: match[1].toLowerCase(),
		filename: decodeURIComponent(match[2]),
		subfolder: "",
	};
}

async function comfyCreateEphemeralUpload({ filePath, filename, config: cfg } = {}) {
	const config = getComfyVideoConfig(cfg);
	const baseName = sanitizeName(filename || path.basename(filePath || "presenter.png"));
	const uploaded = await uploadComfyInput(config, filePath, baseName);
	const name = uploaded.name || uploaded.filename || baseName;
	return {
		runwayUri: `comfy://input/${encodeURIComponent(name)}`,
		comfyUri: `comfy://input/${encodeURIComponent(name)}`,
		filename: name,
		name,
		type: uploaded.type || "input",
		subfolder: uploaded.subfolder || "",
	};
}

async function comfyImageToVideo({
	runwayImageUri,
	comfyUri,
	imagePath,
	promptText = "",
	durationSec,
	ratio = "1280:720",
	jobId = crypto.randomUUID(),
	label = "comfy_video",
	expression,
	outputPath,
	tmpDir,
	config: cfg,
	log,
} = {}) {
	const config = getComfyVideoConfig(cfg);
	if (!config.enabled) throw new Error("COMFY_VIDEO_ENABLED=false");
	await comfyRequest(config, "GET", "/system_stats", null, { timeout: 8000 });
	await waitForSafeComfyTemperature(config, log, label);

	const duration = clampNumber(durationSec, config.durationSec, 2, 20);
	const fps = Math.round(clampNumber(config.fps, 15, 6, 30));
	const expr = inferExpressionFromPrompt(promptText, expression || "neutral");
	const workDir =
		tmpDir || path.join(os.tmpdir(), "agentai_comfy_video", sanitizeName(jobId));
	ensureDir(workDir);

	let uploaded = null;
	let uploadedInputPath = "";
	const parsedUri = parseComfyUri(comfyUri || runwayImageUri || "");
	if (parsedUri) {
		uploaded = parsedUri;
	} else {
		if (!imagePath || !fs.existsSync(imagePath)) {
			throw new Error("comfy_video_image_missing");
		}
		const prepared = path.join(workDir, `comfy_video_source_${sanitizeName(jobId)}.png`);
		preparePresenterImage({
			sourcePath: imagePath,
			outputPath: prepared,
			width: config.width,
			height: config.height,
		});
		uploaded = await uploadComfyInput(
			config,
			prepared,
			`comfy_video_${sanitizeName(jobId)}.png`,
		);
		uploadedInputPath = resolveComfyFilePath(config, {
			filename: uploaded.name || uploaded.filename,
			type: uploaded.type || "input",
			subfolder: uploaded.subfolder || "",
		});
	}

	const filenamePrefix = `agentai_comfy_video_${sanitizeName(label)}_${sanitizeName(jobId)}`;
	if (typeof log === "function") {
		log("comfy video starting", {
			jobId,
			label,
			expression: expr,
			durationSec: duration,
			fps,
			ratio,
			width: config.width,
			height: config.height,
			maxTempC: config.maxTempC,
			preflightMaxTempC: config.preflightMaxTempC,
			maxDiskUsedPercent: config.maxDiskUsedPercent,
		});
	}

	let outputFile = null;
	try {
		const queued = await comfyRequest(config, "POST", "/prompt", {
			client_id: crypto.randomUUID(),
			prompt: buildAdvancedLivePortraitWorkflow({
				uploadedImageName: uploaded.name || uploaded.filename,
				config,
				expression: expr,
				durationSec: duration,
				fps,
				filenamePrefix,
			}),
		});
		const promptId = queued?.prompt_id;
		if (!promptId) throw new Error("comfy_video_prompt_id_missing");
		outputFile = await waitForComfyVideo(config, promptId, log);
		const finalPath =
			outputPath ||
			path.join(workDir, `${filenamePrefix}_${Date.now()}.mp4`);
		await downloadComfyFile(config, outputFile, finalPath);
		await freeComfyMemory(config, log);
		if (typeof log === "function") {
			log("comfy video ready", {
				promptId,
				path: finalPath,
				filename: outputFile.filename,
				subfolder: outputFile.subfolder || "",
				type: outputFile.type || "output",
			});
		}
		return {
			url: finalPath,
			path: finalPath,
			method: "comfyui_advanced_liveportrait",
			model: "ComfyUI-AdvancedLivePortrait",
			durationSec: duration,
			fps,
			expression: expr,
			promptText,
			comfyOutput: outputFile,
			comfyInputPath: uploadedInputPath,
		};
	} catch (error) {
		await freeComfyMemory(config, log);
		throw error;
	} finally {
		if (!config.keepComfyInputs && uploadedInputPath) {
			safeUnlink(uploadedInputPath);
			if (typeof log === "function") {
				log("comfy video input cleaned", { file: path.basename(uploadedInputPath) });
			}
		}
		if (!config.keepComfyOutputs && outputFile) {
			const resolvedOutput = resolveComfyFilePath(config, outputFile);
			safeUnlink(resolvedOutput);
			if (typeof log === "function") {
				log("comfy video output cleaned", {
					file: path.basename(resolvedOutput || outputFile.filename || ""),
				});
			}
		}
	}
}

function argValue(name, fallback = "") {
	const index = process.argv.indexOf(name);
	if (index < 0 || index >= process.argv.length - 1) return fallback;
	return process.argv[index + 1];
}

async function runCli() {
	if (!process.argv.includes("--test")) return;
	const jobId = argValue("--job-id", `comfy_test_${Date.now()}`);
	const tmpDir = path.resolve(argValue("--tmp-dir", path.join(os.tmpdir(), jobId)));
	ensureDir(tmpDir);
	let source = argValue("--source", "");
	const sourceUrl = argValue("--source-url", source.startsWith("http") ? source : DEFAULT_PRESENTER_ASSET_URL);
	if (!source || /^https?:\/\//i.test(source)) {
		source = path.join(tmpDir, `presenter_source_${sanitizeName(jobId)}.png`);
		await downloadToFile(sourceUrl, source, 90000);
	}
	const outputPath = path.resolve(
		argValue("--out", path.join(tmpDir, `comfy_video_test_${sanitizeName(jobId)}.mp4`)),
	);
	const log = (event, data = {}) => {
		console.log(`[comfyUIVideo] ${event} ${JSON.stringify(data)}`);
	};
	const result = await comfyImageToVideo({
		imagePath: source,
		promptText: argValue("--prompt", "calm professional presenter, natural blinks and tiny nod"),
		durationSec: Number(argValue("--duration", "6")),
		expression: argValue("--expression", "warm"),
		jobId,
		label: argValue("--label", "advanced_liveportrait_test"),
		outputPath,
		tmpDir,
		log,
	});
	console.log(JSON.stringify(result, null, 2));
}

if (require.main === module) {
	runCli().catch((error) => {
		console.error(error?.stack || error?.message || String(error));
		process.exit(1);
	});
}

module.exports = {
	getComfyVideoConfig,
	comfyCreateEphemeralUpload,
	comfyImageToVideo,
	buildAdvancedLivePortraitWorkflow,
	buildMotionCommand,
	inferExpressionFromPrompt,
	normalizeExpression,
};
