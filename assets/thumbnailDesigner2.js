/** @format */

const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");
const axios = require("axios");
const FormData = require("form-data");
const baseDesigner = require("./thumbnailDesigner");

const {
	ACCENT_PALETTE,
	THUMBNAIL_HEIGHT,
	THUMBNAIL_MIN_BYTES,
	THUMBNAIL_WIDTH,
	buildContextText,
	buildThumbnailTextPlan,
	chooseAccentColor,
	chooseThumbnailPose,
	chooseThumbnailStyleProfile,
	collectTopicReferenceImages,
	ensureDir,
	ensureImageFile,
	ensureThumbnailFile,
	inferThumbnailIntent,
	lockPresenterPanel,
	normalizeWhitespace,
	primaryTopicLabel,
	renderLockedThumbnailTextOverlay,
	renderTopicLeadVisualSeed,
	safeUnlink,
	uploadThumbnailToCloudinary,
} = baseDesigner.__internals || {};

const DEFAULT_COMFY_URL = "http://127.0.0.1:8188";
const DEFAULT_MODEL = "Realistic_Vision_V6.0_NV_B1_fp16.safetensors";
const DESIGNER2_TOPIC_PANEL_W = 740;
const DESIGNER2_BROADCAST_OVERLAY_MIN_BYTES = 1024 * 1024;

let ffmpegPath = "";
try {
	// eslint-disable-next-line import/no-extraneous-dependencies
	ffmpegPath = require("ffmpeg-static");
} catch {
	ffmpegPath = process.platform === "win32" ? "ffmpeg.exe" : "/usr/bin/ffmpeg";
}

function requireInternals() {
	const missing = [
		["buildContextText", buildContextText],
		["buildThumbnailTextPlan", buildThumbnailTextPlan],
		["collectTopicReferenceImages", collectTopicReferenceImages],
		["renderTopicLeadVisualSeed", renderTopicLeadVisualSeed],
		["lockPresenterPanel", lockPresenterPanel],
		["renderLockedThumbnailTextOverlay", renderLockedThumbnailTextOverlay],
		["uploadThumbnailToCloudinary", uploadThumbnailToCloudinary],
	].filter(([, value]) => typeof value !== "function");
	if (missing.length) {
		throw new Error(
			`thumbnail_designer2_missing_base_helpers:${missing
				.map(([name]) => name)
				.join(",")}`,
		);
	}
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
		height: numberEnv("THUMBNAIL_DESIGNER2_COMFY_HEIGHT", 432, 216, 768),
		steps: numberEnv("THUMBNAIL_DESIGNER2_COMFY_STEPS", 8, 1, 24),
		cfg: numberEnv("THUMBNAIL_DESIGNER2_COMFY_CFG", 4.4, 1, 9),
		denoise: numberEnv("THUMBNAIL_DESIGNER2_COMFY_DENOISE", 0.28, 0.15, 0.7),
		sampler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_SAMPLER || "dpmpp_2m",
		),
		scheduler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_SCHEDULER || "karras",
		),
		feedEnabled: truthyEnv(
			process.env.THUMBNAIL_DESIGNER2_COMFY_FEED_ENABLED,
			true,
		),
		feedWidth: numberEnv("THUMBNAIL_DESIGNER2_COMFY_FEED_WIDTH", 768, 384, 1024),
		feedHeight: numberEnv(
			"THUMBNAIL_DESIGNER2_COMFY_FEED_HEIGHT",
			432,
			216,
			768,
		),
		feedSteps: numberEnv("THUMBNAIL_DESIGNER2_COMFY_FEED_STEPS", 6, 1, 12),
		feedCfg: numberEnv("THUMBNAIL_DESIGNER2_COMFY_FEED_CFG", 4.8, 1, 9),
		feedSampler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_FEED_SAMPLER ||
				process.env.THUMBNAIL_DESIGNER2_COMFY_SAMPLER ||
				"dpmpp_2m",
		),
		feedScheduler: normalizeWhitespace(
			process.env.THUMBNAIL_DESIGNER2_COMFY_FEED_SCHEDULER ||
				process.env.THUMBNAIL_DESIGNER2_COMFY_SCHEDULER ||
				"karras",
		),
		timeoutMs: numberEnv(
			"THUMBNAIL_DESIGNER2_COMFY_TIMEOUT_MS",
			20 * 60 * 1000,
			60 * 1000,
			60 * 60 * 1000,
		),
		pollMs: numberEnv("THUMBNAIL_DESIGNER2_COMFY_POLL_MS", 2500, 1000, 10000),
		maxTempC: numberEnv("THUMBNAIL_DESIGNER2_MAX_TEMP_C", 92, 70, 92),
		preflightMaxTempC: numberEnv(
			"THUMBNAIL_DESIGNER2_PREFLIGHT_MAX_TEMP_C",
			86,
			50,
			90,
		),
		preflightCooldownMs: numberEnv(
			"THUMBNAIL_DESIGNER2_PREFLIGHT_COOLDOWN_MS",
			2 * 60 * 1000,
			0,
			15 * 60 * 1000,
		),
		inputDir: normalizeWhitespace(
			process.env.COMFYUI_INPUT_DIR ||
				(process.platform === "win32"
					? ""
					: "/home/ahmedadmin/ai-lab/ComfyUI/input"),
		),
		outputDir: normalizeWhitespace(
			process.env.COMFYUI_OUTPUT_DIR ||
				(process.platform === "win32"
					? ""
					: "/home/ahmedadmin/ai-lab/ComfyUI/output"),
		),
		maxDiskUsedPercent: numberEnv(
			"THUMBNAIL_DESIGNER2_MAX_DISK_USED_PERCENT",
			40,
			10,
			95,
		),
		maxMemUsedPercent: numberEnv(
			"THUMBNAIL_DESIGNER2_MAX_MEM_USED_PERCENT",
			90,
			50,
			99,
		),
		minMemAvailableMb: numberEnv(
			"THUMBNAIL_DESIGNER2_MIN_MEM_AVAILABLE_MB",
			4096,
			512,
			14000,
		),
	};
}

function runFfmpeg(args, label = "ffmpeg") {
	if (!ffmpegPath) throw new Error("ffmpeg_unavailable");
	try {
		require("child_process").execFileSync(
			ffmpegPath,
			["-hide_banner", "-loglevel", "error", ...args],
			{
				maxBuffer: 64 * 1024 * 1024,
				stdio: ["ignore", "pipe", "pipe"],
				windowsHide: true,
			},
		);
	} catch (error) {
		const stderr = String(error?.stderr || error?.message || "").trim();
		throw new Error(`${label}_failed${stderr ? `:${stderr}` : ""}`);
	}
}

function normalizeAccentColor(value = "") {
	const raw = String(value || "")
		.trim()
		.replace(/^0x/i, "")
		.replace(/^#/, "");
	if (/^[0-9a-f]{6}$/i.test(raw)) return `0x${raw.toUpperCase()}`;
	return "0x00C2FF";
}

function clampNumber(value, min, max) {
	const n = Number(value);
	if (!Number.isFinite(n)) return min;
	return Math.max(min, Math.min(max, n));
}

function clampByte(value) {
	return Math.round(clampNumber(value, 0, 255));
}

function parseRgbColor(value = "", fallback = "0x00C2FF") {
	const normalized = normalizeAccentColor(value || fallback).replace(/^0x/i, "");
	const raw = /^[0-9a-f]{6}$/i.test(normalized)
		? normalized
		: normalizeAccentColor(fallback).replace(/^0x/i, "");
	return {
		r: parseInt(raw.slice(0, 2), 16),
		g: parseInt(raw.slice(2, 4), 16),
		b: parseInt(raw.slice(4, 6), 16),
	};
}

function mixRgb(a, b, amount = 0.5) {
	const t = clampNumber(amount, 0, 1);
	return {
		r: clampByte(a.r * (1 - t) + b.r * t),
		g: clampByte(a.g * (1 - t) + b.g * t),
		b: clampByte(a.b * (1 - t) + b.b * t),
	};
}

function brightenRgb(color, amount = 0.35) {
	return mixRgb(color, { r: 255, g: 255, b: 255 }, amount);
}

function deepenRgb(color, amount = 0.35) {
	return mixRgb(color, { r: 0, g: 0, b: 0 }, amount);
}

function blendOverlayPixel(buffer, width, height, x, y, color, alpha = 1) {
	const px = Math.round(x);
	const py = Math.round(y);
	if (px < 0 || py < 0 || px >= width || py >= height) return;
	const srcA = clampNumber(alpha, 0, 1);
	if (srcA <= 0) return;
	const index = (py * width + px) * 4;
	const dstA = buffer[index + 3] / 255;
	const outA = srcA + dstA * (1 - srcA);
	if (outA <= 0) return;
	buffer[index] = clampByte(
		(color.r * srcA + buffer[index] * dstA * (1 - srcA)) / outA,
	);
	buffer[index + 1] = clampByte(
		(color.g * srcA + buffer[index + 1] * dstA * (1 - srcA)) / outA,
	);
	buffer[index + 2] = clampByte(
		(color.b * srcA + buffer[index + 2] * dstA * (1 - srcA)) / outA,
	);
	buffer[index + 3] = clampByte(outA * 255);
}

function drawSoftBrush(buffer, width, height, cx, cy, radius, color, alpha, power = 1.6) {
	const r = Math.max(1, Number(radius) || 1);
	const minX = Math.max(0, Math.floor(cx - r));
	const maxX = Math.min(width - 1, Math.ceil(cx + r));
	const minY = Math.max(0, Math.floor(cy - r));
	const maxY = Math.min(height - 1, Math.ceil(cy + r));
	const r2 = r * r;
	for (let y = minY; y <= maxY; y++) {
		for (let x = minX; x <= maxX; x++) {
			const dx = x - cx;
			const dy = y - cy;
			const d2 = dx * dx + dy * dy;
			if (d2 > r2) continue;
			const falloff = Math.pow(1 - Math.sqrt(d2) / r, power);
			blendOverlayPixel(buffer, width, height, x, y, color, alpha * falloff);
		}
	}
}

function drawSoftLine(
	buffer,
	width,
	height,
	x1,
	y1,
	x2,
	y2,
	color,
	{ alpha = 0.8, thickness = 3, glow = 0 } = {},
) {
	const distance = Math.max(1, Math.hypot(x2 - x1, y2 - y1));
	const steps = Math.ceil(distance / Math.max(1.5, thickness * 0.65));
	if (glow > 0) {
		for (let i = 0; i <= steps; i++) {
			const t = i / steps;
			drawSoftBrush(
				buffer,
				width,
				height,
				x1 + (x2 - x1) * t,
				y1 + (y2 - y1) * t,
				glow,
				color,
				alpha * 0.2,
				2.4,
			);
		}
	}
	for (let i = 0; i <= steps; i++) {
		const t = i / steps;
		drawSoftBrush(
			buffer,
			width,
			height,
			x1 + (x2 - x1) * t,
			y1 + (y2 - y1) * t,
			thickness,
			color,
			alpha,
			0.55,
		);
	}
}

function drawArc(
	buffer,
	width,
	height,
	cx,
	cy,
	radius,
	startDeg,
	endDeg,
	color,
	{ alpha = 0.8, thickness = 3, glow = 0 } = {},
) {
	const sweep = Math.abs(endDeg - startDeg);
	const steps = Math.max(24, Math.ceil((sweep / 360) * radius * 2.8));
	const start = (startDeg * Math.PI) / 180;
	const end = (endDeg * Math.PI) / 180;
	if (glow > 0) {
		for (let i = 0; i <= steps; i++) {
			const t = i / steps;
			const angle = start + (end - start) * t;
			drawSoftBrush(
				buffer,
				width,
				height,
				cx + Math.cos(angle) * radius,
				cy + Math.sin(angle) * radius,
				glow,
				color,
				alpha * 0.18,
				2.4,
			);
		}
	}
	for (let i = 0; i <= steps; i++) {
		const t = i / steps;
		const angle = start + (end - start) * t;
		drawSoftBrush(
			buffer,
			width,
			height,
			cx + Math.cos(angle) * radius,
			cy + Math.sin(angle) * radius,
			thickness,
			color,
			alpha,
			0.6,
		);
	}
}

function drawRectFrame(
	buffer,
	width,
	height,
	x,
	y,
	w,
	h,
	color,
	{ alpha = 0.65, thickness = 2, glow = 0 } = {},
) {
	drawSoftLine(buffer, width, height, x, y, x + w, y, color, {
		alpha,
		thickness,
		glow,
	});
	drawSoftLine(buffer, width, height, x, y, x, y + h, color, {
		alpha,
		thickness,
		glow,
	});
	drawSoftLine(buffer, width, height, x, y + h, x + w, y + h, color, {
		alpha: alpha * 0.45,
		thickness,
		glow: glow * 0.5,
	});
	drawSoftLine(buffer, width, height, x + w, y, x + w, y + h, color, {
		alpha: alpha * 0.28,
		thickness,
		glow: glow * 0.45,
	});
}

function fillSoftRect(buffer, width, height, x, y, w, h, color, alpha = 0.2) {
	const minX = Math.max(0, Math.round(x));
	const maxX = Math.min(width - 1, Math.round(x + w));
	const minY = Math.max(0, Math.round(y));
	const maxY = Math.min(height - 1, Math.round(y + h));
	for (let py = minY; py <= maxY; py++) {
		for (let px = minX; px <= maxX; px++) {
			blendOverlayPixel(buffer, width, height, px, py, color, alpha);
		}
	}
}

function drawLeftPanelVignette(buffer, width, height, topicPanelW) {
	for (let y = 0; y < height; y++) {
		const bottom = Math.max(0, (y - height * 0.42) / (height * 0.58));
		const top = Math.max(0, 1 - y / (height * 0.42));
		for (let x = 0; x < topicPanelW; x++) {
			const edge = Math.max(0, 1 - x / 150);
			const seam = Math.max(0, (x - (topicPanelW - 96)) / 96);
			const alpha = bottom * 0.16 + top * edge * 0.1 + seam * 0.12;
			if (alpha > 0.006) {
				blendOverlayPixel(
					buffer,
					width,
					height,
					x,
					y,
					{ r: 0, g: 0, b: 0 },
					alpha,
				);
			}
		}
	}
}

function panelGeometryForDesigner2(styleProfile = {}) {
	const id = String(styleProfile?.id || "").toLowerCase();
	if (id === "soft_cyan_wellness") {
		return { x: 38, y: 320, w: 662, h: 400 };
	}
	if (id === "magenta_pop") {
		return { x: 38, y: 380, w: 660, h: 254 };
	}
	if (id === "electric_cyan" || id === "gaming_teal_ember") {
		return { x: 40, y: 386, w: 648, h: 248 };
	}
	return { x: 42, y: 372, w: 650, h: 268 };
}

function writePamRgba(filePath, width, height, rgbaBuffer) {
	const header = Buffer.from(
		`P7\nWIDTH ${width}\nHEIGHT ${height}\nDEPTH 4\nMAXVAL 255\nTUPLTYPE RGB_ALPHA\nENDHDR\n`,
		"ascii",
	);
	fs.writeFileSync(filePath, Buffer.concat([header, rgbaBuffer]));
	const stat = fs.statSync(filePath);
	if (!stat || stat.size < DESIGNER2_BROADCAST_OVERLAY_MIN_BYTES) {
		throw new Error("thumbnail_designer2_overlay_too_small");
	}
	return filePath;
}

function createDesigner2BroadcastOverlay({
	jobId,
	tmpDir,
	accent = ACCENT_PALETTE.default,
	styleProfile = {},
	stage = "background",
}) {
	const width = THUMBNAIL_WIDTH;
	const height = THUMBNAIL_HEIGHT;
	const topicPanelW = DESIGNER2_TOPIC_PANEL_W;
	const overlayPath = path.join(
		tmpDir,
		`thumb_designer2_${stage}_overlay_${jobId}.pam`,
	);
	const pixels = Buffer.alloc(width * height * 4);
	const accentRgb = parseRgbColor(accent || "0x00C2FF");
	const bright = brightenRgb(accentRgb, 0.48);
	const hot = brightenRgb(accentRgb, 0.72);
	const deep = deepenRgb(accentRgb, 0.48);
	const white = { r: 255, g: 255, b: 255 };
	const panel = panelGeometryForDesigner2(styleProfile);

	if (stage === "background") {
		drawLeftPanelVignette(pixels, width, height, topicPanelW);
		drawSoftBrush(pixels, width, height, 64, 34, 170, bright, 0.26, 1.95);
		drawSoftBrush(pixels, width, height, 710, 240, 210, accentRgb, 0.16, 2.15);
		drawSoftBrush(pixels, width, height, 286, 690, 250, deep, 0.2, 1.8);
		drawSoftBrush(pixels, width, height, 602, 624, 210, accentRgb, 0.12, 2.25);
		fillSoftRect(
			pixels,
			width,
			height,
			panel.x - 2,
			panel.y - 14,
			panel.w + 18,
			panel.h + 20,
			{ r: 0, g: 0, b: 0 },
			0.12,
		);
		drawArc(pixels, width, height, 740, 342, 720, 154, 254, bright, {
			alpha: 0.44,
			thickness: 3,
			glow: 16,
		});
		drawArc(pixels, width, height, 758, 346, 646, 158, 250, hot, {
			alpha: 0.3,
			thickness: 2,
			glow: 11,
		});
		drawArc(pixels, width, height, 420, 378, 336, -52, 42, bright, {
			alpha: 0.62,
			thickness: 4,
			glow: 22,
		});
		drawArc(pixels, width, height, 426, 382, 356, -52, 42, deep, {
			alpha: 0.22,
			thickness: 6,
			glow: 8,
		});
		drawSoftLine(pixels, width, height, 735, 0, 735, height, accentRgb, {
			alpha: 0.66,
			thickness: 3,
			glow: 15,
		});
		drawSoftLine(pixels, width, height, 742, 0, 742, height, white, {
			alpha: 0.13,
			thickness: 1,
			glow: 4,
		});
		drawSoftLine(pixels, width, height, 0, 24, 338, 24, bright, {
			alpha: 0.44,
			thickness: 3,
			glow: 9,
		});
		drawSoftLine(pixels, width, height, 0, 42, 252, 42, white, {
			alpha: 0.14,
			thickness: 2,
			glow: 5,
		});
	} else {
		drawRectFrame(pixels, width, height, panel.x, panel.y, panel.w, panel.h, bright, {
			alpha: 0.62,
			thickness: 2,
			glow: 7,
		});
		drawSoftLine(
			pixels,
			width,
			height,
			panel.x,
			panel.y,
			panel.x,
			panel.y + panel.h,
			accentRgb,
			{ alpha: 0.92, thickness: 4, glow: 10 },
		);
		drawSoftLine(
			pixels,
			width,
			height,
			panel.x + 18,
			panel.y + 22,
			panel.x + 68,
			panel.y + 22,
			hot,
			{ alpha: 0.58, thickness: 3, glow: 6 },
		);
		drawSoftLine(
			pixels,
			width,
			height,
			panel.x + 18,
			panel.y + 36,
			panel.x + 44,
			panel.y + 36,
			white,
			{ alpha: 0.22, thickness: 2, glow: 4 },
		);
		drawArc(pixels, width, height, 450, 360, 306, -44, 50, accentRgb, {
			alpha: 0.42,
			thickness: 3,
			glow: 14,
		});
		drawSoftLine(pixels, width, height, 735, 0, 735, height, accentRgb, {
			alpha: 0.86,
			thickness: 4,
			glow: 14,
		});
		drawSoftLine(pixels, width, height, 744, 0, 744, height, { r: 0, g: 0, b: 0 }, {
			alpha: 0.26,
			thickness: 5,
			glow: 0,
		});
		drawSoftLine(pixels, width, height, 0, 0, topicPanelW, 0, white, {
			alpha: 0.16,
			thickness: 1,
			glow: 0,
		});
		drawSoftLine(pixels, width, height, 0, height - 1, topicPanelW, height - 1, white, {
			alpha: 0.14,
			thickness: 1,
			glow: 0,
		});
	}

	return writePamRgba(overlayPath, width, height, pixels);
}

function applyDesigner2BroadcastOverlay({
	jobId,
	tmpDir,
	basePath,
	accent = ACCENT_PALETTE.default,
	styleProfile = {},
	stage = "background",
	label = "broadcast",
	log,
}) {
	ensureImageFile(basePath, 5000);
	const outputPath = path.join(
		tmpDir,
		`thumb_designer2_${safeOverlaySlug(label)}_${jobId}.jpg`,
	);
	const overlayPath = createDesigner2BroadcastOverlay({
		jobId,
		tmpDir,
		accent,
		styleProfile,
		stage,
	});
	try {
		runFfmpeg(
			[
				"-i",
				basePath,
				"-i",
				overlayPath,
				"-filter_complex",
				[
					`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,` +
						`crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1,format=rgba[base]`,
					"[1:v]format=rgba[fx]",
					"[base][fx]overlay=0:0,format=yuv420p[outv]",
				].join(";"),
				"-map",
				"[outv]",
				"-frames:v",
				"1",
				"-q:v",
				"1",
				"-y",
				outputPath,
			],
			`thumbnail_designer2_${label}`,
		);
		ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
		if (typeof log === "function") {
			log("thumbnailDesigner2 broadcast overlay ready", {
				stage,
				path: path.basename(outputPath),
			});
		}
		return outputPath;
	} finally {
		safeUnlink(overlayPath);
	}
}

function safeOverlaySlug(value = "") {
	return String(value || "overlay")
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "_")
		.replace(/^_+|_+$/g, "")
		.slice(0, 32);
}

function enhanceDesigner2StyleProfile(styleProfile = {}, contextText = "") {
	const hay = normalizeWhitespace(contextText).toLowerCase();
	if (
		/\b(ai|chatbot|companions?|robot|artificial intelligence)\b/.test(hay) &&
		/\b(love|romance|romantic|relationship|girlfriend|boyfriend|lonely|intimacy|attachment|dependence|falling|emotional)\b/.test(
			hay,
		)
	) {
		return {
			...styleProfile,
			id: "magenta_pop",
			accent: "0xFF3EA5",
			tagColor: "0x32133E",
			textPanelOpacity: 0.66,
			brief:
				"premium AI-companion editorial contrast, hot magenta and electric cyan glow, intimate tech-story energy, clean broadcast thumbnail frame",
		};
	}
	if (
		/\b(tired|fatigue|burnout|sleep|resting|rest|overloaded|mental noise|drained|exhausted|worrying|scrolling)\b/.test(
			hay,
		)
	) {
		return {
			...styleProfile,
			id: "soft_cyan_wellness",
			accent: "0x00C2FF",
			tagColor: "0x102A43",
			textPanelOpacity: 1,
			brief:
				"cinematic wellness/editorial contrast, cyan night-to-morning glow, polished mental-fatigue story energy, clean premium YouTube frame",
			};
	}
	if (hasDesigner2FreeHappinessSignal(hay)) {
		return {
			...styleProfile,
			id: "warm_green_wellbeing",
			accent: "0x22C55E",
			tagColor: "0x163326",
			textPanelOpacity: 0.8,
			brief:
				"warm optimistic self-improvement editorial contrast, clean green and soft golden sunlight, crisp practical wellbeing story cue, premium YouTube frame",
		};
	}
	return styleProfile;
}

function hasDesigner2AiCompanionSignal(contextText = "") {
	const hay = normalizeWhitespace(contextText).toLowerCase();
	return (
		/\b(ai|chatbot|companions?|robot|artificial intelligence)\b/.test(hay) &&
		/\b(love|romance|romantic|relationship|girlfriend|boyfriend|lonely|intimacy|attachment|dependence|falling|emotional)\b/.test(
			hay,
		)
	);
}

function hasDesigner2FreeHappinessSignal(contextText = "") {
	const hay = normalizeWhitespace(contextText).toLowerCase();
	return (
		/\b(happier|happiness|happy|free|gratitude|grateful|simple living|walk|walking|call someone|calling a friend|help someone|journaling|journal|loneliness|lonely|self-improvement|feel better)\b/.test(
			hay,
		) &&
		/\b(money|spending|spend|buy|purchase|budget|\$0|zero dollars|without spending|cost of living)\b/.test(
			hay,
		)
	);
}

function isDesigner2WeakHeadline(headline = "") {
	const words = normalizeWhitespace(headline)
		.toUpperCase()
		.split(/\s+/)
		.filter(Boolean);
	if (!words.length) return true;
	const normalized = words.join(" ");
	const last = words[words.length - 1];
	if (
		/^(A|AN|AND|ARE|AS|AT|BE|BY|FOR|FROM|IN|INTO|IS|OF|ON|OR|THE|TO|WITH|WITHOUT|YOUR|MY|OUR|THEIR)$/i.test(
			last,
		)
	) {
		return true;
	}
	if (/^WHAT MAKES\b/.test(normalized) && words.length >= 5) return true;
	if (/^HOW TO\b/.test(normalized) && words.length <= 3) return true;
	return false;
}

function refineDesigner2TextPlan({
	textPlan = {},
	title,
	shortTitle,
	seoTitle,
	topics = [],
	intent,
} = {}) {
	const contextText = buildContextText({ title, shortTitle, seoTitle, topics });
	const headline = normalizeWhitespace(textPlan.primaryHeadline || "");
	if (!isDesigner2WeakHeadline(headline)) return textPlan;

	if (hasDesigner2AiCompanionSignal(contextText)) {
		return {
			...textPlan,
			primaryHeadline: "FALLING FOR AI",
			badgeText: textPlan.badgeText || "BREAKDOWN",
			sublineText: "Why It Feels Real",
		};
	}

	if (/\b(why|what makes|how it works|explained|breakdown)\b/i.test(contextText)) {
		return {
			...textPlan,
			primaryHeadline:
				intent === "tech" || /\b(ai|tech|app|robot|software)\b/i.test(contextText)
					? "WHY IT WORKS"
					: "WHAT CHANGED",
			sublineText: textPlan.sublineText || textPlan.primaryTopic || "",
		};
	}

	return textPlan;
}

function buildComfyPrompt({
	title,
	shortTitle,
	seoTitle,
	topics = [],
	headline = "",
	badgeText = "",
	sublineText = "",
	intent = "general",
	styleProfile = {},
	hasFeedImage = false,
	feedSource = "",
}) {
	const topicText =
		normalizeWhitespace(primaryTopicLabel?.(topics)) ||
		normalizeWhitespace(shortTitle) ||
		normalizeWhitespace(title) ||
		normalizeWhitespace(seoTitle) ||
		"current story";
	const styleBrief = normalizeWhitespace(styleProfile.brief || "");
	const safeHeadline = normalizeWhitespace(headline || "main story");
	const safeBadge = normalizeWhitespace(badgeText || "spotlight");
	const safeSubline = normalizeWhitespace(sublineText || topicText);
	const sourceType = normalizeWhitespace(feedSource).toLowerCase();
	const aiCompanionFeed = sourceType === "comfy_generated_ai_companion";
	const feedLine = hasFeedImage
		? aiCompanionFeed
			? "The left side contains an AI-generated device-focused AI companion reference. Preserve the phone/laptop/glow story cue; do not introduce people, faces, hands, readable text, or screenshot UI."
			: sourceType === "comfy_generated_conceptual"
			? "The left side contains an AI-generated object-led conceptual reference. Preserve the crisp symbolic story cue, clean negative space, and practical emotional context; do not introduce people, faces, hands, readable text, or clutter."
			: sourceType === "comfy_generated_fallback"
			? "The left side contains an AI-generated topic reference used only because no reliable orchestrator feed image was available. Keep it symbolic, non-fabricated, and visually clear; polish it into a premium thumbnail story cue."
			: "The left side already contains the orchestrator-provided feed/story image. Preserve its main subject and context, then relight, sharpen, simplify, and frame it as a premium thumbnail story cue."
		: "No reliable feed image is present. Keep the left side symbolic and non-fabricated; use environment, objects, color, and editorial lighting instead of inventing real people.";
	const aiCompanionGuard = aiCompanionFeed
		? "For this AI companion thumbnail, keep the left story side object-led: glowing phone, laptop, dark desk, reflections, and luminous abstract chat shapes only. No human figure, no portrait, no cropped face, no hands."
		: "";

	return normalizeWhitespace(`
		Image-to-image polish of one complete 16:9 YouTube thumbnail visual plate.
		Use the input image as the layout blueprint: story/feed visual on the left, presenter on the right.
		${feedLine}
		${aiCompanionGuard}
		The text is already planned separately: headline "${safeHeadline}", badge "${safeBadge}", optional subject "${safeSubline}".
		Do not render text. Leave a clean readable left-side text area for those exact words to be added after generation.
		Remove or paint over any words, captions, screenshot fragments, labels, or text-like artifacts already visible in the source feed image, especially near the lower-left text-safe panel.
		Do not hallucinate a new left-side person or scene. Keep the source feed image recognizable; enhance lighting and depth only.
		Keep the right-side presenter in the same position and scale. Preserve identity, glasses, beard, hairline, face shape, expression, shoulders, dark outfit, and camera-facing pose.
		Do not redraw, beautify, age, distort, crop, or change the presenter face. The original presenter panel will be restored after this step.
		Polish the whole visual plate: stronger contrast, richer depth, cleaner lighting, crisp subject separation, premium editorial color, high-end YouTube thumbnail energy, mobile-readable composition.
		Make the left feed image dominant, bright enough to understand, and visually specific to the topic. Aim for a designed broadcast-package left side: curved neon arcs, topic-matched accent glows, subtle gradients, depth, a premium divider, and a dark glass lower-left text-safe shelf like a sophisticated news/sports/tech YouTube thumbnail.
		Use the accent implied by the style instead of forcing one repeated color. The deterministic compositor will add final arcs, rails, and divider treatment after this step, so keep those areas clean and compatible.
		Keep the main story subject visible above/behind the text-safe panel, not buried in darkness. Avoid busy collage, random body-part crops, fake faces, fake screenshots, text blocks, signs, logos, or watermarks.
		Intent: ${intent}. Topic: ${topicText}. Style: ${styleBrief || "premium editorial thumbnail"}.
	`);
}

function buildComfyFeedPrompt({
	title,
	shortTitle,
	seoTitle,
	topics = [],
	contextText = "",
	styleProfile = {},
}) {
	const topicText =
		normalizeWhitespace(primaryTopicLabel?.(topics)) ||
		normalizeWhitespace(shortTitle) ||
		normalizeWhitespace(title) ||
		normalizeWhitespace(seoTitle) ||
		"current story";
	const styleBrief = normalizeWhitespace(styleProfile.brief || "");
	const aiCompanionLine = hasDesigner2AiCompanionSignal(
		`${topicText} ${contextText}`,
	)
		? "For an AI companion, chatbot romance, loneliness, or emotional attachment topic, show a sharp cinematic but non-branded object-only scene: glowing phone and laptop on a dark desk at night, empty chair, glass reflections, luminous abstract chat-bubble light and soft heart-shaped glow, subtle robot/AI presence only through reflections or light, emotional tech tension, crisp subject separation, no people, no faces, no hands, no readable text, not a blurred silhouette."
		: "";
	const freeHappinessLine = hasDesigner2FreeHappinessSignal(
		`${topicText} ${contextText}`,
	)
		? "For a happiness without spending money, simple living, gratitude, or money-stress topic, make the image object-led and crystal clear: warm sunlight on a public park bench or small kitchen table, blank gratitude journal, simple coffee mug, phone placed face down or with an unreadable dark screen, a few coins or closed wallet as a subtle money cue, soft greenery in the background, hopeful practical mood. No faces, no hands, no family group, no readable text, no shopping scene, no luxury products, no fake UI."
		: "";
	return normalizeWhitespace(`
		Create one photorealistic editorial feed image for the left side of a YouTube thumbnail.
		No presenter, no host, no text, no typography, no watermark, no UI.
		Topic: ${topicText}.
		Context and visual hints: ${normalizeWhitespace(contextText).slice(0, 1200)}.
		${aiCompanionLine}
		${freeHappinessLine}
		For a tiredness, burnout, mental fatigue, sleep, rest, or overloaded-life topic, show a relatable cinematic scene:
		a tired adult near a laptop at night, coffee cup, messy desk, notebook or unfinished task list, soft morning light or window glow, calm realistic mood, practical not medical.
		Make it feel like a high-quality news/editorial feed photo with clear subject, strong depth, cinematic lighting, premium contrast, and space near the lower-left for later headline text.
		Avoid hospital scenes, medical diagnosis, horror, melodrama, fake celebrities, extra limbs, distorted faces, words, letters, captions, logos, and screenshots.
		Style: ${styleBrief || "premium editorial photo, cinematic cyan highlights"}.
	`);
}

function buildNegativePrompt(extra = "") {
	return normalizeWhitespace(`
		text, letters, words, subtitles, captions, logo, watermark, signature,
		UI screenshot, fake interface, poster text, misspelled text, duplicated text,
		deformed face, changed presenter identity, different glasses, missing glasses,
		bad eyes, bad beard, bad mouth, bad anatomy, extra fingers, extra limbs,
		duplicate people, cropped head, out of frame, waxy skin, plastic skin,
		cartoon, anime, illustration, painting, low quality, blurry, noisy,
		faceless silhouette, indistinct figure, muddy lighting, cluttered composition, oversaturated, overexposed,
		underexposed, random celebrity, fabricated portrait,
		${extra}
	`);
}

function randomSeed() {
	return Math.floor(Math.random() * 999999999999999);
}

function buildWorkflow(config, prompt, uploadedImageName) {
	return {
		"4": {
			class_type: "CheckpointLoaderSimple",
			inputs: {
				ckpt_name: config.model,
			},
		},
		"10": {
			class_type: "LoadImage",
			inputs: {
				image: uploadedImageName,
			},
		},
		"11": {
			class_type: "ImageScale",
			inputs: {
				upscale_method: "lanczos",
				width: config.width,
				height: config.height,
				crop: "center",
				image: ["10", 0],
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
		"12": {
			class_type: "VAEEncode",
			inputs: {
				pixels: ["11", 0],
				vae: ["4", 2],
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
				denoise: config.denoise,
				model: ["4", 0],
				positive: ["6", 0],
				negative: ["7", 0],
				latent_image: ["12", 0],
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
				filename_prefix: "agentai_thumbnail2_comfy_plate",
				images: ["8", 0],
			},
		},
	};
}

function buildTextToImageWorkflow(config, prompt, options = {}) {
	const width = options.width || config.feedWidth || config.width;
	const height = options.height || config.feedHeight || config.height;
	const steps = options.steps || config.feedSteps || config.steps;
	const cfg = options.cfg || config.feedCfg || config.cfg;
	const sampler = options.sampler || config.feedSampler || config.sampler;
	const scheduler = options.scheduler || config.feedScheduler || config.scheduler;
	const prefix = options.prefix || "agentai_thumbnail2_feed";
	const negativePromptExtra = normalizeWhitespace(options.negativePromptExtra || "");
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
				text: buildNegativePrompt(negativePromptExtra),
				clip: ["4", 1],
			},
		},
		"5": {
			class_type: "EmptyLatentImage",
			inputs: {
				width,
				height,
				batch_size: 1,
			},
		},
		"3": {
			class_type: "KSampler",
			inputs: {
				seed: randomSeed(),
				steps,
				cfg,
				sampler_name: sampler,
				scheduler,
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
				filename_prefix: prefix,
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
		const thermalRoot = "/sys/class/thermal";
		for (const entry of fs.readdirSync(thermalRoot)) {
			if (!/^thermal_zone\d+$/.test(entry)) continue;
			const zoneDir = path.join(thermalRoot, entry);
			const type = normalizeWhitespace(
				fs.existsSync(path.join(zoneDir, "type"))
					? fs.readFileSync(path.join(zoneDir, "type"), "utf8")
					: "",
			).toLowerCase();
			if (
				type &&
				!/cpu|pkg|package|x86|core|acpi|thermal|pch|k10|zen/i.test(type)
			) {
				continue;
			}
			const temp = readTempFileC(path.join(zoneDir, "temp"));
			if (temp != null) values.push(temp);
		}
	} catch {}
	try {
		const hwmonRoot = "/sys/class/hwmon";
		for (const hwmon of fs.readdirSync(hwmonRoot)) {
			const dir = path.join(hwmonRoot, hwmon);
			for (const entry of fs.readdirSync(dir)) {
				const match = entry.match(/^temp(\d+)_input$/);
				if (!match) continue;
				const labelPath = path.join(dir, `temp${match[1]}_label`);
				const label = normalizeWhitespace(
					fs.existsSync(labelPath) ? fs.readFileSync(labelPath, "utf8") : "",
				).toLowerCase();
				if (
					label &&
					!/cpu|package|core|tdie|tctl|x86|k10|zen|sensor/i.test(label)
				) {
					continue;
				}
				const temp = readTempFileC(path.join(dir, entry));
				if (temp != null) values.push(temp);
			}
		}
	} catch {}
	if (!values.length) return null;
	return Math.max(...values);
}

function readMemorySnapshot() {
	if (process.platform !== "win32") {
		try {
			const raw = fs.readFileSync("/proc/meminfo", "utf8");
			const getKb = (key) => {
				const match = raw.match(new RegExp(`^${key}:\\s+(\\d+)\\s+kB`, "m"));
				return match ? Number(match[1]) : 0;
			};
			const totalKb = getKb("MemTotal");
			const availableKb = getKb("MemAvailable") || getKb("MemFree");
			if (totalKb > 0 && availableKb > 0) {
				return {
					memAvailableMb: Math.round(availableKb / 1024),
					memUsedPercent:
						Math.round(((totalKb - availableKb) / totalKb) * 1000) / 10,
				};
			}
		} catch {}
	}
	const total = os.totalmem();
	const free = os.freemem();
	return {
		memAvailableMb: Math.round(free / 1024 / 1024),
		memUsedPercent:
			total > 0 ? Math.round(((total - free) / total) * 1000) / 10 : null,
	};
}

function readDiskUsedPercent(config = {}) {
	const candidates = [
		config.outputDir,
		config.inputDir,
		process.cwd(),
	].filter(Boolean);
	const target = candidates[0] || process.cwd();
	if (process.platform === "win32") return null;
	try {
		const out = require("child_process")
			.execFileSync("df", ["-P", target], {
				encoding: "utf8",
				stdio: ["ignore", "pipe", "ignore"],
			})
			.trim()
			.split(/\r?\n/)
			.pop();
		const parts = String(out || "").trim().split(/\s+/);
		const used = parts[4] || "";
		const parsed = Number(String(used).replace("%", ""));
		return Number.isFinite(parsed) ? parsed : null;
	} catch {
		return null;
	}
}

function systemSnapshot(config = {}) {
	return {
		tempC: readMaxCpuTemperatureC(),
		...readMemorySnapshot(),
		diskUsedPercent: readDiskUsedPercent(config),
	};
}

function assertSystemGuards(config, snap = {}) {
	if (
		snap.diskUsedPercent != null &&
		snap.diskUsedPercent > config.maxDiskUsedPercent
	) {
		throw new Error(
			`comfyui_disk_guard:${snap.diskUsedPercent}>${config.maxDiskUsedPercent}`,
		);
	}
	if (
		snap.memAvailableMb != null &&
		snap.memAvailableMb < config.minMemAvailableMb
	) {
		throw new Error(
			`comfyui_memory_guard:${snap.memAvailableMb}<${config.minMemAvailableMb}`,
		);
	}
	if (
		snap.memUsedPercent != null &&
		snap.memUsedPercent >= config.maxMemUsedPercent
	) {
		throw new Error(
			`comfyui_memory_used_guard:${snap.memUsedPercent}>=${config.maxMemUsedPercent}`,
		);
	}
}

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

async function interruptComfy(config, log, reason = {}) {
	try {
		await comfyRequest(config, "POST", "/interrupt", {});
		if (typeof log === "function") {
			log("thumbnailDesigner2 comfy interrupted", reason);
		}
	} catch (error) {
		if (typeof log === "function") {
			log("thumbnailDesigner2 comfy interrupt failed", {
				...reason,
				error: error?.message || String(error),
			});
		}
	}
}

async function waitForSafeComfyTemperature(config, log, label = "comfy") {
	const initialSnap = systemSnapshot(config);
	assertSystemGuards(config, initialSnap);
	const initialTemp = initialSnap.tempC;
	if (initialTemp == null) return;
	if (initialTemp < config.preflightMaxTempC) return;
	const started = Date.now();
	if (typeof log === "function") {
		log("thumbnailDesigner2 temperature cooldown starting", {
			label,
			tempC: Number(initialTemp.toFixed(1)),
			targetC: config.preflightMaxTempC,
			maxTempC: config.maxTempC,
			cooldownMs: config.preflightCooldownMs,
		});
	}
	let temp = initialTemp;
	while (
		temp != null &&
		temp >= config.preflightMaxTempC &&
		Date.now() - started < config.preflightCooldownMs
	) {
		await sleep(Math.min(15000, Math.max(3000, config.pollMs)));
		const snap = systemSnapshot(config);
		assertSystemGuards(config, snap);
		temp = snap.tempC;
	}
	if (temp != null && temp >= config.preflightMaxTempC) {
		throw new Error(
			`comfyui_temperature_preflight_too_hot:${temp.toFixed(1)}C`,
		);
	}
	if (typeof log === "function") {
		log("thumbnailDesigner2 temperature cooldown complete", {
			label,
			tempC: temp == null ? null : Number(temp.toFixed(1)),
		});
	}
}

async function uploadComfyInput(config, filePath) {
	const form = new FormData();
	form.append("image", fs.createReadStream(filePath), {
		filename: `agentai_thumbnail2_draft_${crypto.randomUUID()}.png`,
	});
	form.append("type", "input");
	form.append("overwrite", "true");
	const response = await axios.post(`${config.url}/upload/image`, form, {
		headers: form.getHeaders(),
		timeout: 60000,
		validateStatus: (status) => status >= 200 && status < 300,
	});
	return response.data;
}

async function waitForComfyImage(config, promptId, log, label = "plate") {
	const deadline = Date.now() + config.timeoutMs;
	while (Date.now() < deadline) {
		await new Promise((resolve) => setTimeout(resolve, config.pollMs));
		const snap = systemSnapshot(config);
		try {
			assertSystemGuards(config, snap);
		} catch (error) {
			await interruptComfy(config, log, {
				label,
				promptId,
				...snap,
				reason: "system_guard",
			});
			throw error;
		}
		const tempC = snap.tempC;
		if (tempC != null && tempC >= config.maxTempC) {
			await interruptComfy(config, log, {
				label,
				promptId,
				tempC: Number(tempC.toFixed(1)),
				maxTempC: config.maxTempC,
			});
			throw new Error(`comfyui_temperature_limit:${tempC.toFixed(1)}C`);
		}
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
	throw new Error("comfyui_thumbnail_plate_timeout");
}

async function freeComfyMemory(config, log) {
	try {
		await comfyRequest(config, "POST", "/free", {
			unload_models: true,
			free_memory: true,
		});
		if (typeof log === "function") {
			log("thumbnailDesigner2 comfy memory released", {
				unloadModels: true,
				freeMemory: true,
			});
		}
	} catch (error) {
		if (typeof log === "function") {
			log("thumbnailDesigner2 comfy memory release skipped", {
				error: error?.message || String(error),
			});
		}
	}
}

function comfyImageUrl(config, image) {
	const params = new URLSearchParams({
		filename: image.filename || "",
		subfolder: image.subfolder || "",
		type: image.type || "output",
	});
	return `${config.url}/view?${params.toString()}`;
}

function resolveComfyFilePath(config, image = {}) {
	const type = image.type || "output";
	const rootDir = type === "input" ? config.inputDir : config.outputDir;
	if (!rootDir || !image.filename) return "";
	const root = path.resolve(rootDir);
	const resolved = path.resolve(root, image.subfolder || "", image.filename);
	return resolved === root || !resolved.startsWith(`${root}${path.sep}`)
		? ""
		: resolved;
}

function cleanupComfyFile(filePath, log, label) {
	if (!filePath) return;
	try {
		if (fs.existsSync(filePath)) {
			fs.unlinkSync(filePath);
			if (typeof log === "function") {
				log(label, { file: path.basename(filePath) });
			}
		}
	} catch (error) {
		if (typeof log === "function") {
			log(`${label} skipped`, {
				error: error?.message || String(error),
			});
		}
	}
}

async function downloadComfyImage(config, image, outPath) {
	const response = await axios.get(comfyImageUrl(config, image), {
		responseType: "stream",
		timeout: 60000,
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

async function resolveComfyOutputToLocalPath(config, image, tmpDir, jobId) {
	const resolved = resolveComfyFilePath(config, image);
	if (resolved && fs.existsSync(resolved)) return resolved;
	const outPath = path.join(tmpDir, `thumb_comfy_plate_${jobId}.png`);
	await downloadComfyImage(config, image, outPath);
	return outPath;
}

async function generateComfyThumbnailPlate({
	jobId,
	tmpDir,
	draftPath,
	title,
	shortTitle,
	seoTitle,
	topics,
	headline,
	badgeText,
	sublineText,
	intent,
	styleProfile,
	hasFeedImage,
	feedSource = "",
	log,
}) {
	const config = getComfyConfig();
	if (!config.enabled) return null;
	ensureImageFile(draftPath, 5000);
	const prompt = buildComfyPrompt({
		title,
		shortTitle,
		seoTitle,
		topics,
		headline,
		badgeText,
		sublineText,
		intent,
		styleProfile,
		hasFeedImage,
		feedSource,
	});
	if (typeof log === "function") {
		log("thumbnailDesigner2 comfy plate starting", {
			url: config.url,
			model: config.model,
			width: config.width,
			height: config.height,
			steps: config.steps,
			cfg: config.cfg,
			denoise: config.denoise,
			sampler: config.sampler,
			scheduler: config.scheduler,
			maxTempC: config.maxTempC,
			preflightMaxTempC: config.preflightMaxTempC,
			hasFeedImage,
			feedSource,
		});
	}

	await comfyRequest(config, "GET", "/system_stats", null, { timeout: 8000 });
	await waitForSafeComfyTemperature(config, log, "plate");
	const uploadedInput = await uploadComfyInput(config, draftPath);
	const inputPath = resolveComfyFilePath(config, {
		filename: uploadedInput.name || uploadedInput.filename,
		subfolder: uploadedInput.subfolder || "",
		type: uploadedInput.type || "input",
	});
	try {
		const queued = await comfyRequest(config, "POST", "/prompt", {
			client_id: crypto.randomUUID(),
			prompt: buildWorkflow(config, prompt, uploadedInput.name || uploadedInput.filename),
		});
		const promptId = queued?.prompt_id;
		if (!promptId) throw new Error("comfyui_prompt_id_missing");
		const image = await waitForComfyImage(config, promptId, log, "plate");
		const localPath = await resolveComfyOutputToLocalPath(config, image, tmpDir, jobId);
		await freeComfyMemory(config, log);

		if (typeof log === "function") {
			log("thumbnailDesigner2 comfy plate ready", {
				promptId,
				filename: image.filename || "",
				subfolder: image.subfolder || "",
				type: image.type || "output",
				path: path.basename(localPath),
			});
		}

		return {
			path: localPath,
			method: "comfyui_img2img_plate",
			model: config.model,
			width: config.width,
			height: config.height,
			steps: config.steps,
			cfg: config.cfg,
			denoise: config.denoise,
			outputPath: resolveComfyFilePath(config, image),
			inputPath,
		};
	} catch (error) {
		await freeComfyMemory(config, log);
		throw error;
	} finally {
		cleanupComfyFile(
			inputPath,
			log,
			"thumbnailDesigner2 comfy input cleaned",
		);
	}
}

async function generateComfyFeedReference({
	jobId,
	tmpDir,
	title,
	shortTitle,
	seoTitle,
	topics,
	contextText,
	styleProfile,
	qualityProfile = "",
	log,
}) {
	const config = getComfyConfig();
	if (!config.enabled || !config.feedEnabled) return null;
	const highQualityAbstractFeed = qualityProfile === "abstract_ai_companion";
	const highQualityConceptualFeed = qualityProfile === "conceptual_free_happiness";
	const feedWidth = highQualityConceptualFeed
		? Math.max(config.feedWidth, 896)
		: config.feedWidth;
	const feedHeight = highQualityConceptualFeed
		? Math.max(config.feedHeight, 768)
		: config.feedHeight;
	const feedSteps = highQualityAbstractFeed
		? Math.max(config.feedSteps, 6)
		: highQualityConceptualFeed
		? Math.max(config.feedSteps, 10)
		: config.feedSteps;
	const feedCfg = highQualityAbstractFeed
		? Math.min(config.feedCfg, 4.8)
		: highQualityConceptualFeed
		? Math.min(Math.max(config.feedCfg, 5.2), 6)
		: config.feedCfg;
	const feedSampler =
		highQualityAbstractFeed || highQualityConceptualFeed
			? "dpmpp_2m"
			: config.feedSampler;
	const feedScheduler =
		highQualityAbstractFeed || highQualityConceptualFeed
			? "karras"
			: config.feedScheduler;
	const feedNegativePromptExtra = highQualityAbstractFeed
		? "people, person, human, woman, man, child, teen, face, portrait, eyes, mouth, hair, hands, fingers, arms, body, bare shoulder, bedroom portrait, sleeping face, sensual pose, cropped body, distorted hands"
		: highQualityConceptualFeed
		? "people, person, human, family, group, child, face, portrait, eyes, mouth, hair, hands, fingers, arms, body, shopping mall, luxury store, brand, product logo, readable phone screen, receipt text, distorted objects"
		: "";
	const prompt = buildComfyFeedPrompt({
		title,
		shortTitle,
		seoTitle,
		topics,
		contextText,
		styleProfile,
	});
	if (typeof log === "function") {
		log("thumbnailDesigner2 comfy feed starting", {
			url: config.url,
			model: config.model,
			width: feedWidth,
			height: feedHeight,
			steps: feedSteps,
			cfg: feedCfg,
			sampler: feedSampler,
			scheduler: feedScheduler,
			maxTempC: config.maxTempC,
			preflightMaxTempC: config.preflightMaxTempC,
			qualityProfile: qualityProfile || null,
		});
	}
	await comfyRequest(config, "GET", "/system_stats", null, { timeout: 8000 });
	await waitForSafeComfyTemperature(config, log, "feed");
	const queued = await comfyRequest(config, "POST", "/prompt", {
		client_id: crypto.randomUUID(),
		prompt: buildTextToImageWorkflow(config, prompt, {
			width: feedWidth,
			height: feedHeight,
			steps: feedSteps,
			cfg: feedCfg,
			sampler: feedSampler,
			scheduler: feedScheduler,
			prefix: "agentai_thumbnail2_feed",
			negativePromptExtra: feedNegativePromptExtra,
		}),
	});
	const promptId = queued?.prompt_id;
	if (!promptId) throw new Error("comfyui_feed_prompt_id_missing");
	const image = await waitForComfyImage(config, promptId, log, "feed");
	const localPath = await resolveComfyOutputToLocalPath(
		config,
		image,
		tmpDir,
		`${jobId}_feed`,
	);
	if (typeof log === "function") {
		log("thumbnailDesigner2 comfy feed ready", {
			promptId,
			filename: image.filename || "",
			subfolder: image.subfolder || "",
			type: image.type || "output",
			path: path.basename(localPath),
		});
	}
	return {
		path: localPath,
		outputPath: resolveComfyFilePath(config, image),
		method: "comfyui_feed_reference",
	};
}

function renderDesigner2VisualSeed({
	jobId,
	tmpDir,
	presenterLocalPath,
	topicReferencePaths = [],
	accent = ACCENT_PALETTE.default,
	log,
}) {
	const heroSource = topicReferencePaths[0] || presenterLocalPath;
	ensureImageFile(heroSource, 5000);
	ensureImageFile(presenterLocalPath, 5000);
	const outputPath = path.join(tmpDir, `thumb_designer2_seed_${jobId}.jpg`);
	const accentColor = normalizeAccentColor(accent || "0x00C2FF");
	const filters = [
		`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.05:saturation=0.96:brightness=-0.015,gblur=sigma=24,setsar=1[bg]`,
		`[0:v]scale=740:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=740:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.12:saturation=1.10:brightness=0.012,unsharp=5:5:0.62:5:5:0.02,setsar=1[topic]`,
		`[1:v]scale=572:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=540:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.05:saturation=1.03,setsar=1[presenter]`,
		`[bg][topic]overlay=0:0[tmp0]`,
		`[tmp0]drawbox=x=0:y=0:w=740:h=${THUMBNAIL_HEIGHT}:color=black@0.05:t=fill[tmp1]`,
		`[tmp1]drawbox=x=36:y=334:w=664:h=368:color=black@0.74:t=fill[tmp2]`,
		`[tmp2]drawbox=x=36:y=334:w=664:h=3:color=${accentColor}@0.95:t=fill[tmp3]`,
		`[tmp3]drawbox=x=36:y=334:w=7:h=368:color=${accentColor}@0.96:t=fill[tmp4]`,
		`[tmp4]drawbox=x=0:y=0:w=740:h=7:color=${accentColor}@0.18:t=fill[tmp5]`,
		`[tmp5]drawbox=x=0:y=0:w=7:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.18:t=fill[tmp6]`,
		`[tmp6]drawbox=x=682:y=0:w=58:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.14:t=fill[tmp7]`,
		`[tmp7][presenter]overlay=740:0[tmp8]`,
		`[tmp8]drawbox=x=732:y=0:w=8:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.98:t=fill[tmp9]`,
		`[tmp9]drawbox=x=726:y=0:w=20:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.18:t=fill[outv]`,
	];
	runFfmpeg(
		[
			"-i",
			heroSource,
			"-i",
			presenterLocalPath,
			"-filter_complex",
			filters.join(";"),
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-q:v",
			"1",
			"-y",
			outputPath,
		],
		"thumbnail_designer2_seed_compose",
	);
	ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
	if (typeof log === "function") {
		log("thumbnailDesigner2 visual seed ready", {
			path: path.basename(outputPath),
			usesTopicHero: Boolean(topicReferencePaths.length),
		});
	}
	return {
		path: outputPath,
		method: "designer2_visual_seed",
	};
}

function restoreSharpFeedPanel({
	jobId,
	tmpDir,
	basePath,
	topicReferencePath,
	accent = ACCENT_PALETTE.default,
	log,
}) {
	if (!topicReferencePath) return "";
	ensureImageFile(basePath, 5000);
	ensureImageFile(topicReferencePath, 5000);
	const outputPath = path.join(tmpDir, `thumb_feed_sharp_${jobId}.jpg`);
	const accentColor = normalizeAccentColor(accent || "0x00C2FF");
	const filters = [
		`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1[base]`,
		`[1:v]scale=740:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,crop=740:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,eq=contrast=1.08:saturation=1.06:brightness=0.01,unsharp=5:5:0.70:5:5:0.02,setsar=1[feed]`,
		`[base][feed]overlay=0:0[tmp0]`,
		`[tmp0]drawbox=x=0:y=0:w=740:h=${THUMBNAIL_HEIGHT}:color=black@0.03:t=fill[tmp1]`,
		`[tmp1]drawbox=x=0:y=0:w=740:h=7:color=${accentColor}@0.16:t=fill[tmp2]`,
		`[tmp2]drawbox=x=0:y=0:w=7:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.16:t=fill[tmp3]`,
		`[tmp3]drawbox=x=682:y=0:w=58:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.10:t=fill[tmp4]`,
		`[tmp4]drawbox=x=732:y=0:w=8:h=${THUMBNAIL_HEIGHT}:color=${accentColor}@0.98:t=fill[outv]`,
	];
	runFfmpeg(
		[
			"-i",
			basePath,
			"-i",
			topicReferencePath,
			"-filter_complex",
			filters.join(";"),
			"-map",
			"[outv]",
			"-frames:v",
			"1",
			"-q:v",
			"1",
			"-y",
			outputPath,
		],
		"thumbnail_designer2_feed_restore",
	);
	ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
	if (typeof log === "function") {
		log("thumbnailDesigner2 feed image restored sharp", {
			path: path.basename(outputPath),
			source: path.basename(topicReferencePath),
		});
	}
	return outputPath;
}

function applyDesigner2EditorialPolish({
	jobId,
	tmpDir,
	basePath,
	accent = ACCENT_PALETTE.default,
	styleProfile = {},
	log,
}) {
	ensureImageFile(basePath, 5000);
	const outputPath = path.join(tmpDir, `thumb_designer2_polished_${jobId}.jpg`);
	const overlayPath = createDesigner2BroadcastOverlay({
		jobId,
		tmpDir,
		accent,
		styleProfile,
		stage: "foreground",
	});
	try {
		runFfmpeg(
			[
				"-i",
				basePath,
				"-i",
				overlayPath,
				"-filter_complex",
				[
					`[0:v]scale=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,` +
						`crop=${THUMBNAIL_WIDTH}:${THUMBNAIL_HEIGHT}:(iw-ow)/2:(ih-oh)/2,setsar=1,` +
						"eq=contrast=1.028:saturation=1.035:brightness=0.002,unsharp=5:5:0.34:3:3:0.04,format=rgba[base]",
					"[1:v]format=rgba[fx]",
					"[base][fx]overlay=0:0,drawbox=x=0:y=0:w=iw:h=ih:color=white@0.08:t=2,format=yuv420p[outv]",
				].join(";"),
				"-map",
				"[outv]",
				"-frames:v",
				"1",
				"-q:v",
				"1",
				"-y",
				outputPath,
			],
			"thumbnail_designer2_editorial_polish",
		);
		ensureThumbnailFile(outputPath, THUMBNAIL_MIN_BYTES);
		if (typeof log === "function") {
			log("thumbnailDesigner2 editorial polish ready", {
				path: path.basename(outputPath),
			});
		}
		return {
			path: outputPath,
			method: "comfyui_img2img_text_locked",
		};
	} finally {
		safeUnlink(overlayPath);
	}
}

async function generateComfyFirstThumbnailPackage(args = {}) {
	requireInternals();
	const {
		jobId,
		tmpDir,
		presenterLocalPath,
		title,
		shortTitle,
		seoTitle,
		topics = [],
		expression = "neutral",
		log,
		overrideHeadline,
		overrideBadgeText,
		overrideIntent,
	} = args;

	if (!presenterLocalPath) {
		throw new Error("thumbnail_presenter_missing_or_invalid");
	}
	ensureDir(tmpDir);
	ensureImageFile(presenterLocalPath, 5000);

	const contextText = buildContextText({ title, shortTitle, seoTitle, topics });
	const intent =
		normalizeWhitespace(overrideIntent) ||
		inferThumbnailIntent({ title, shortTitle, seoTitle, topics });
	const textPlan = refineDesigner2TextPlan({
		textPlan: buildThumbnailTextPlan({
			title,
			shortTitle,
			seoTitle,
			topics,
			intent,
			overrideHeadline,
			overrideBadgeText,
		}),
		title,
		shortTitle,
		seoTitle,
		topics,
		intent,
	});
	const styleContextText = normalizeWhitespace(
		`${contextText} ${textPlan.primaryHeadline} ${textPlan.badgeText} ${
			textPlan.sublineText || ""
		}`,
	);
	const pose = chooseThumbnailPose({
		expression,
		intent,
		contextText: styleContextText,
	});
	const styleProfile = enhanceDesigner2StyleProfile(
		chooseThumbnailStyleProfile(intent, styleContextText),
		styleContextText,
	);
	const accent =
		styleProfile.accent ||
		chooseAccentColor(intent, styleContextText) ||
		ACCENT_PALETTE.default;

	if (typeof log === "function") {
		log("thumbnail plan", {
			intent,
			pose,
			headline: textPlan.primaryHeadline,
			badgeText: textPlan.badgeText,
			subline: textPlan.sublineText || null,
			style: styleProfile.id,
		});
	}

	let topicReferencePaths = await collectTopicReferenceImages({
		topics,
		tmpDir,
		jobId,
		log,
	});
	let topicReferenceSource = topicReferencePaths.length ? "orchestrator" : "none";
	let generatedFeed = null;
	let generatedFeedAttempted = false;
	const tryGeneratedFeedReference = async (reason) => {
		generatedFeedAttempted = true;
		try {
			generatedFeed = await generateComfyFeedReference({
				jobId,
				tmpDir,
				title,
				shortTitle,
				seoTitle,
				topics,
				contextText,
				styleProfile,
				qualityProfile: reason,
				log,
			});
			if (generatedFeed?.path) {
				const replacedReferences = topicReferencePaths.length;
				topicReferencePaths = [generatedFeed.path];
				topicReferenceSource =
					reason === "abstract_ai_companion"
						? "comfy_generated_ai_companion"
						: reason === "conceptual_free_happiness"
						? "comfy_generated_conceptual"
						: "comfy_generated_fallback";
				if (typeof log === "function") {
					log("thumbnailDesigner2 comfy feed selected", {
						reason,
						replacedReferences,
						path: path.basename(generatedFeed.path),
					});
				}
			}
		} catch (error) {
			if (typeof log === "function") {
				log("thumbnailDesigner2 comfy feed unavailable", {
					error: error?.message || String(error),
				});
			}
		}
	};

	if (
		topicReferencePaths.length &&
		hasDesigner2AiCompanionSignal(styleContextText)
	) {
		await tryGeneratedFeedReference("abstract_ai_companion");
	}
	if (
		!generatedFeedAttempted &&
		hasDesigner2FreeHappinessSignal(styleContextText)
	) {
		await tryGeneratedFeedReference("conceptual_free_happiness");
	}
	if (!topicReferencePaths.length && !generatedFeedAttempted) {
		await tryGeneratedFeedReference("missing_orchestrator_reference");
	}

	if (typeof log === "function") {
		log("thumbnail route selected", {
			primaryRoute: "comfyui_img2img_text_locked",
			fallbackRoutes: getComfyConfig().strict
				? []
				: ["thumbnailDesigner_openai_original"],
			preferTopicLead: Boolean(topicReferencePaths.length),
			intent,
			topicReferenceCount: topicReferencePaths.length,
			topicReferenceSource,
			primaryTopic: primaryTopicLabel(topics),
		});
	}

	const draft = renderDesigner2VisualSeed({
		jobId: `${jobId}_comfy_draft`,
		tmpDir,
		presenterLocalPath,
		topicReferencePaths,
		accent,
		log,
	});
	let comfyPlate = null;
	let feedRestoredPath = "";
	let presenterLockedPath = "";
	let broadcastPath = "";
	let textOverlayPath = "";
	let finalPlate = null;
	try {
		comfyPlate = await generateComfyThumbnailPlate({
			jobId,
			tmpDir,
			draftPath: draft.path,
			title,
			shortTitle,
			seoTitle,
			topics,
			headline: textPlan.primaryHeadline,
			badgeText: textPlan.badgeText,
			sublineText: textPlan.sublineText,
			intent,
			styleProfile,
			hasFeedImage: Boolean(topicReferencePaths.length),
			feedSource: topicReferenceSource,
			log,
		});
		if (!comfyPlate?.path) throw new Error("comfyui_plate_missing");

		feedRestoredPath = restoreSharpFeedPanel({
			jobId,
			tmpDir,
			basePath: comfyPlate.path,
			topicReferencePath: topicReferencePaths[0],
			accent,
			log,
		});
		presenterLockedPath = lockPresenterPanel({
			jobId,
			tmpDir,
			basePath: feedRestoredPath || comfyPlate.path,
			presenterLocalPath,
			label: "comfy_presenter_locked",
			log,
		});
		let textBasePath = presenterLockedPath;
		try {
			broadcastPath = applyDesigner2BroadcastOverlay({
				jobId,
				tmpDir,
				basePath: presenterLockedPath,
				accent,
				styleProfile,
				stage: "background",
				label: "broadcast_backplate",
				log,
			});
			textBasePath = broadcastPath;
		} catch (error) {
			if (typeof log === "function") {
				log("thumbnailDesigner2 broadcast backplate skipped", {
					error: error?.message || String(error),
				});
			}
		}
		const textOverlayPlate = renderLockedThumbnailTextOverlay({
			jobId,
			tmpDir,
			basePath: textBasePath,
			headline: textPlan.primaryHeadline,
			badgeText: textPlan.badgeText,
			sublineText: textPlan.sublineText,
			accent,
			styleProfile,
			log,
		});
		textOverlayPath = textOverlayPlate.path;
		finalPlate = textOverlayPlate;
		try {
			finalPlate = applyDesigner2EditorialPolish({
				jobId,
				tmpDir,
				basePath: textOverlayPlate.path,
				accent,
				styleProfile,
				log,
			});
		} catch (error) {
			if (typeof log === "function") {
				log("thumbnailDesigner2 editorial polish skipped", {
					error: error?.message || String(error),
				});
			}
		}
		finalPlate.method = "comfyui_img2img_text_locked";
		ensureThumbnailFile(finalPlate.path, THUMBNAIL_MIN_BYTES);

		if (typeof log === "function") {
			log("thumbnail route succeeded", {
				strategy: "comfyui_img2img_text_locked",
				method: finalPlate.method,
				path: path.basename(finalPlate.path || ""),
			});
		}
	} finally {
		cleanupComfyFile(
			comfyPlate?.outputPath,
			log,
			"thumbnailDesigner2 comfy plate cleaned",
		);
		if (comfyPlate?.path && comfyPlate.path !== comfyPlate.outputPath) {
			safeUnlink(comfyPlate.path);
		}
		safeUnlink(draft.path);
		if (feedRestoredPath && finalPlate?.path !== feedRestoredPath) {
			safeUnlink(feedRestoredPath);
		}
		if (presenterLockedPath && finalPlate?.path !== presenterLockedPath) {
			safeUnlink(presenterLockedPath);
		}
		if (broadcastPath && finalPlate?.path !== broadcastPath) {
			safeUnlink(broadcastPath);
		}
		if (textOverlayPath && finalPlate?.path !== textOverlayPath) {
			safeUnlink(textOverlayPath);
		}
		if (generatedFeed?.outputPath) {
			cleanupComfyFile(
				generatedFeed.outputPath,
				log,
				"thumbnailDesigner2 comfy feed cleaned",
			);
		}
		if (generatedFeed?.path && generatedFeed.path !== generatedFeed.outputPath) {
			safeUnlink(generatedFeed.path);
		}
	}

	const uploaded = await uploadThumbnailToCloudinary(finalPlate.path, jobId);
	const variant = {
		variant: "comfyui",
		localPath: finalPlate.path,
		url: uploaded.url || "",
		publicId: uploaded.public_id || "",
		width: uploaded.width || THUMBNAIL_WIDTH,
		height: uploaded.height || THUMBNAIL_HEIGHT,
		title: textPlan.primaryHeadline,
		method: finalPlate.method,
	};

	return {
		localPath: finalPlate.path,
		url: uploaded.url || "",
		publicId: uploaded.public_id || "",
		width: uploaded.width || THUMBNAIL_WIDTH,
		height: uploaded.height || THUMBNAIL_HEIGHT,
		title: textPlan.primaryHeadline,
		pose,
		accent,
		style: styleProfile.id,
		method: finalPlate.method,
		variants: [variant],
		comfy: {
			model: comfyPlate.model,
			width: comfyPlate.width,
			height: comfyPlate.height,
			steps: comfyPlate.steps,
			cfg: comfyPlate.cfg,
			denoise: comfyPlate.denoise,
			feedSource: topicReferenceSource,
		},
	};
}

async function generateThumbnailPackage(args = {}) {
	try {
		return await generateComfyFirstThumbnailPackage(args);
	} catch (error) {
		const config = getComfyConfig();
		if (typeof args.log === "function") {
			args.log("thumbnailDesigner2 comfy route failed", {
				error: error?.message || String(error),
				fallback: !config.strict,
			});
		}
		if (config.strict) throw error;
		const result = await baseDesigner.generateThumbnailPackage(args);
		return {
			...result,
			designer: "thumbnailDesigner",
			method: result.method || "thumbnailDesigner_openai_original",
			comfy: null,
		};
	}
}

module.exports = {
	...baseDesigner,
	generateThumbnailPackage,
};
