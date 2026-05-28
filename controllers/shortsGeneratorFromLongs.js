/* controllers/shortsGeneratorFromLongs.js */
const fs = require("fs");
const path = require("path");
const os = require("os");
const child_process = require("child_process");
const axios = require("axios");
const ffmpegStatic = require("ffmpeg-static");

const Video = require("../models/Video");
const ShortVideo = require("../models/ShortVideo");
const User = require("../models/User");
const {
	refreshYouTubeTokensIfNeeded,
	uploadToYouTube,
} = require("./videoController");
const { YT_CATEGORY_MAP } = require("../assets/utils");

const TMP_ROOT = path.join(os.tmpdir(), "agentai_shorts");
const SHORTS_OUTPUT_DIR = path.join(__dirname, "../uploads/shorts");
const SHORTS_SOURCE_DIR = path.join(TMP_ROOT, "sources");
const SHORTS_DEFAULT_CTA_LINE = "Full breakdown on the channel.";

function ensureDir(dir) {
	if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
ensureDir(TMP_ROOT);
ensureDir(SHORTS_OUTPUT_DIR);
ensureDir(SHORTS_SOURCE_DIR);

function toNumber(v, fallback) {
	const n = Number(v);
	return Number.isFinite(n) ? n : fallback;
}

function clampNumber(n, min, max) {
	const x = Number(n);
	if (!Number.isFinite(x)) return min;
	return Math.max(min, Math.min(max, x));
}

const SHORTS_WIDTH = clampNumber(
	toNumber(process.env.SHORTS_WIDTH, 720),
	480,
	1440,
);
const SHORTS_HEIGHT = clampNumber(
	toNumber(process.env.SHORTS_HEIGHT, 1280),
	720,
	2560,
);
const SHORTS_MIN_SEC = clampNumber(
	toNumber(process.env.SHORTS_MIN_SEC, 25),
	25,
	45,
);
const SHORTS_MAX_SEC = clampNumber(
	toNumber(process.env.SHORTS_MAX_SEC, 45),
	25,
	45,
);
const SHORTS_TARGET_SECONDS = [25, 35, 45];
const SHORTS_MAX_CLIPS = clampNumber(
	toNumber(process.env.SHORTS_MAX_CLIPS, 6),
	1,
	10,
);
const SHORTS_MIN_CLIPS = clampNumber(
	toNumber(process.env.SHORTS_MIN_CLIPS, 3),
	1,
	SHORTS_MAX_CLIPS,
);
const SHORTS_CRF = clampNumber(toNumber(process.env.SHORTS_CRF, 20), 16, 28);
const SHORTS_AUDIO_BITRATE = process.env.SHORTS_AUDIO_BITRATE || "160k";
const SHORTS_UPLOAD_DELAY_HOURS = clampNumber(
	toNumber(process.env.SHORTS_UPLOAD_DELAY_HOURS, 24),
	1,
	72,
);
const SHORTS_UPLOAD_GAP_HOURS = clampNumber(
	toNumber(process.env.SHORTS_UPLOAD_GAP_HOURS, 24),
	24,
	168,
);
const SHORTS_UPLOAD_FIRST_IMMEDIATELY = !["0", "false", "no", "off"].includes(
	String(process.env.SHORTS_UPLOAD_FIRST_IMMEDIATELY ?? "true")
		.trim()
		.toLowerCase(),
);
const SHORTS_WATERMARK_TEXT = "https://serenejannat.com";
const SHORTS_WATERMARK_OPACITY = 0.55;
const SHORTS_WATERMARK_MARGIN_PCT = 0.035;
const SHORTS_WATERMARK_FONT_PCT_PORTRAIT = 0.022;
const SHORTS_WATERMARK_FONT_PCT_LANDSCAPE = 0.028;

function nowIso() {
	return new Date().toISOString();
}

function addHours(date, hours) {
	const base = date instanceof Date && !Number.isNaN(date.getTime())
		? date
		: new Date();
	return new Date(base.getTime() + Number(hours || 0) * 60 * 60 * 1000);
}

function safeResolveInside(root, target) {
	const rootPath = path.resolve(root);
	const targetPath = path.resolve(target);
	const rel = path.relative(rootPath, targetPath);
	if (rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel))) {
		return targetPath;
	}
	return "";
}

function safeUnlink(filePath) {
	try {
		if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
	} catch {}
}

function cleanupDownloadedSource(source) {
	if (!source || source.source !== "download" || !source.path) return;
	const safePath = safeResolveInside(SHORTS_SOURCE_DIR, source.path);
	if (safePath) safeUnlink(safePath);
}

function isHttpUrl(u) {
	return typeof u === "string" && /^https?:\/\//i.test(u);
}

function extractYouTubeVideoId(value = "") {
	const raw = String(value || "").trim();
	if (!raw) return "";
	if (/^[A-Za-z0-9_-]{11}$/.test(raw)) return raw;

	try {
		const url = new URL(raw);
		const host = url.hostname.replace(/^www\./i, "").toLowerCase();
		if (host === "youtu.be") {
			const id = url.pathname.split("/").filter(Boolean)[0] || "";
			return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : "";
		}
		if (host === "youtube.com" || host.endsWith(".youtube.com")) {
			const watchId = url.searchParams.get("v") || "";
			if (/^[A-Za-z0-9_-]{11}$/.test(watchId)) return watchId;
			const pathMatch = url.pathname.match(
				/\/(?:embed|shorts|live)\/([A-Za-z0-9_-]{11})/i,
			);
			if (pathMatch) return pathMatch[1];
		}
	} catch {}

	const match = raw.match(
		/(?:v=|youtu\.be\/|\/shorts\/|\/embed\/|\/live\/)([A-Za-z0-9_-]{11})/i,
	);
	return match ? match[1] : "";
}

function normalizeYouTubeWatchUrl(value = "") {
	const id = extractYouTubeVideoId(value);
	return id ? `https://www.youtube.com/watch?v=${id}` : "";
}

function resolveOriginalLongYouTubeUrl(video) {
	return normalizeYouTubeWatchUrl(video?.youtubeLink || "");
}

function buildPublicBaseUrl(req) {
	const envBase = String(process.env.PUBLIC_BASE_URL || "").trim();
	if (envBase) return envBase.replace(/\/+$/, "");
	if (!req) return "";
	return `${req.protocol || "http"}://${req.get("host")}`;
}

function uniqueStrings(list = [], { limit = 0 } = {}) {
	const seen = new Set();
	const out = [];
	for (const raw of Array.isArray(list) ? list : []) {
		const val = String(raw || "").trim();
		if (!val) continue;
		const key = val.toLowerCase();
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(val);
		if (limit && out.length >= limit) break;
	}
	return out;
}

function trimTitleToLimit(text = "", max = 95) {
	const cleaned = String(text || "")
		.replace(/\s+/g, " ")
		.trim();
	if (!cleaned) return "";
	if (cleaned.length <= max) return cleaned;
	const clipped = cleaned.slice(0, max);
	return clipped.replace(/\s+\S*$/, "").trim();
}

function cleanClipTitleBase(text = "") {
	let cleaned = String(text || "")
		.replace(/\s+/g, " ")
		.trim();
	if (!cleaned) return "";
	cleaned = cleaned.replace(/^["']+|["']+$/g, "");
	cleaned = cleaned.replace(
		/^(here's|here is|this is|there's|there is|today|right now)\b[:,-]?\s*/i,
		"",
	);
	cleaned = cleaned.replace(/^[\-\s]+/, "");
	cleaned = cleaned.replace(/[.!?]+$/g, "");
	cleaned = cleaned.replace(/^\s*(and|but|so)\s+/i, "");
	return cleaned.trim();
}

function buildClipTitleCandidates(line = "", fallbackBase = "") {
	const base = trimTitleToLimit(
		cleanClipTitleBase(line) || String(fallbackBase || "").trim(),
		95,
	);
	if (!base) return [];
	const variants = [
		base,
		`The key detail: ${base}`,
		`What changed: ${base}`,
		`Why it matters: ${base}`,
		`The quick update: ${base}`,
		`${base} | The detail people missed`,
	];
	return uniqueStrings(variants, { limit: 8 }).map((t) =>
		trimTitleToLimit(t, 95),
	);
}

function buildClipThumbnailTextCandidates(line = "", fallbackBase = "") {
	const base = cleanClipTitleBase(line) || String(fallbackBase || "").trim();
	const tokens = base.split(/\s+/).slice(0, 3).join(" ");
	const variants = [
		tokens || fallbackBase,
		"Key detail",
		"What changed",
		"Why it matters",
		"The twist",
		"Still unclear",
	];
	return uniqueStrings(variants, { limit: 8 }).map((t) => t.trim());
}

function normalizeTitleKey(text = "") {
	return String(text || "")
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, " ")
		.trim();
}

function buildTitleSuffixFromLine(line = "") {
	const cleaned = cleanClipTitleBase(line);
	if (!cleaned) return "";
	return cleaned.split(/\s+/).slice(0, 3).join(" ");
}

function pickUniqueTitle({
	titleCandidates = [],
	usedTitles,
	line = "",
	clipIndex = 0,
}) {
	const used = usedTitles instanceof Set ? usedTitles : new Set();
	for (const candidate of titleCandidates) {
		const key = normalizeTitleKey(candidate);
		if (!key || used.has(key)) continue;
		used.add(key);
		return candidate;
	}
	const base = trimTitleToLimit(cleanClipTitleBase(line) || line, 95);
	if (base) {
		const baseKey = normalizeTitleKey(base);
		if (baseKey && !used.has(baseKey)) {
			used.add(baseKey);
			return base;
		}
		const suffix = buildTitleSuffixFromLine(line);
		if (suffix) {
			const withSuffix = trimTitleToLimit(`${base} - ${suffix}`, 95);
			const suffixKey = normalizeTitleKey(withSuffix);
			if (suffixKey && !used.has(suffixKey)) {
				used.add(suffixKey);
				return withSuffix;
			}
		}
		const fallback = trimTitleToLimit(`${base} - Clip ${clipIndex + 1}`, 95);
		const fallbackKey = normalizeTitleKey(fallback);
		if (!used.has(fallbackKey)) used.add(fallbackKey);
		return fallback;
	}
	const generic = trimTitleToLimit(`Short update - Clip ${clipIndex + 1}`, 95);
	used.add(normalizeTitleKey(generic));
	return generic;
}

function mapUploadsUrlToPath(url) {
	try {
		const parsed = new URL(url);
		const pathname = decodeURIComponent(parsed.pathname || "");
		if (!pathname.includes("/uploads/")) return "";
		const rel = pathname.replace(/^\/+/, "");
		return path.join(__dirname, "..", rel);
	} catch {
		return "";
	}
}

function makeEven(n) {
	const x = Math.round(Number(n) || 0);
	return x % 2 === 0 ? x : x + 1;
}

function resolveShortsFontFile() {
	const env = String(
		process.env.FFMPEG_FONT_PATH || process.env.FFMPEG_FONT || "",
	).trim();
	if (env && fs.existsSync(env)) return env;
	const candidates = [
		path.join(__dirname, "../assets/fonts/DejaVuSans.ttf"),
		"/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
		"/usr/share/fonts/dejavu/DejaVuSans.ttf",
		"C:/Windows/Fonts/arial.ttf",
		"C:/Windows/Fonts/arialbd.ttf",
	];
	for (const p of candidates) {
		try {
			if (p && fs.existsSync(p)) return p;
		} catch {}
	}
	return null;
}

const SHORTS_WATERMARK_FONT_FILE = resolveShortsFontFile();

function escapeDrawtext(s = "") {
	const placeholder = "__NL__";
	return String(s || "")
		.replace(/\r\n|\r|\n/g, placeholder)
		.replace(/\\/g, "\\\\")
		.replace(/:/g, "\\:")
		.replace(/'/g, "\\'")
		.replace(/%/g, "\\%")
		.replace(/\[/g, "\\[")
		.replace(/\]/g, "\\]")
		.replace(new RegExp(placeholder, "g"), "\\n")
		.trim();
}

function buildShortsWatermarkFilter(width, height) {
	const w = Number(width) || SHORTS_WIDTH;
	const h = Number(height) || SHORTS_HEIGHT;
	const aspect = w / h;
	const fontFactor =
		aspect < 1
			? SHORTS_WATERMARK_FONT_PCT_PORTRAIT
			: SHORTS_WATERMARK_FONT_PCT_LANDSCAPE;
	let fontSize = Math.max(16, Math.round(h * fontFactor));
	const marginX = Math.max(12, Math.round(w * SHORTS_WATERMARK_MARGIN_PCT));
	const marginY = Math.max(12, Math.round(h * SHORTS_WATERMARK_MARGIN_PCT));
	const rawText = SHORTS_WATERMARK_TEXT;
	const text = escapeDrawtext(rawText);
	const maxWidth = Math.max(120, w - marginX * 2);
	const approxCharWidth = fontSize * 0.56;
	const approxTextWidth = rawText.length * approxCharWidth;
	if (approxTextWidth > maxWidth) {
		const scale = maxWidth / approxTextWidth;
		fontSize = Math.max(14, Math.floor(fontSize * scale));
	}
	const fontFile = SHORTS_WATERMARK_FONT_FILE
		? `:fontfile='${escapeDrawtext(SHORTS_WATERMARK_FONT_FILE)}'`
		: "";
	return `drawtext=text='${text}'${fontFile}:fontsize=${fontSize}:fontcolor=white@${SHORTS_WATERMARK_OPACITY.toFixed(
		2,
	)}:shadowcolor=black@0.55:shadowx=2:shadowy=2:x=w-text_w-${marginX}:y=h-text_h-${marginY}`;
}

async function spawnBin(bin, args, label, { timeoutMs = 180000 } = {}) {
	return await new Promise((resolve, reject) => {
		const child = child_process.spawn(bin, args, {
			windowsHide: true,
			stdio: ["ignore", "pipe", "pipe"],
		});
		let stderr = "";
		let stdout = "";
		const timer = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error(`${label} timed out`));
		}, timeoutMs);

		child.stdout.on("data", (d) => (stdout += d.toString()));
		child.stderr.on("data", (d) => (stderr += d.toString()));
		child.on("error", (err) => {
			clearTimeout(timer);
			reject(err);
		});
		child.on("close", (code) => {
			clearTimeout(timer);
			if (code !== 0) {
				const err = new Error(`${label} failed (code ${code})`);
				err.stdout = stdout;
				err.stderr = stderr;
				return reject(err);
			}
			resolve({ stdout, stderr });
		});
	});
}

async function downloadToFile(url, outPath, timeoutMs = 60000) {
	const res = await axios.get(url, {
		responseType: "stream",
		timeout: timeoutMs,
	});
	await new Promise((resolve, reject) => {
		const ws = fs.createWriteStream(outPath);
		res.data.pipe(ws);
		ws.on("finish", resolve);
		ws.on("error", reject);
	});
	return outPath;
}

function normalizeTargetSeconds(raw) {
	const n = Number(raw);
	if (SHORTS_TARGET_SECONDS.includes(n)) return n;
	return SHORTS_TARGET_SECONDS[1];
}

function normalizeClipCandidates(shortsDetails, segments = []) {
	const raw =
		shortsDetails && typeof shortsDetails === "object" ? shortsDetails : {};
	const candidates = Array.isArray(raw.clipCandidates)
		? raw.clipCandidates
		: [];
	const fallbackBase = String(
		raw.angle ||
			(Array.isArray(raw.titleCandidates) ? raw.titleCandidates[0] : "") ||
			"",
	).trim();
	const normalized = candidates
		.map((c, idx) => {
			const segmentIndex = Number(
				c?.segmentIndex ?? c?.segment_index ?? c?.index ?? idx,
			);
			if (!Number.isFinite(segmentIndex) || segmentIndex < 0) return null;
			if (segments.length && segmentIndex >= segments.length) return null;
			const line = String(c?.line || segments[segmentIndex]?.text || "").trim();
			if (!line) return null;
			const rawTitleCandidates = Array.isArray(
				c?.titleCandidates ||
					c?.title_candidates ||
					c?.seoTitleCandidates ||
					c?.seo_title_candidates,
			)
				? c.titleCandidates ||
					c.title_candidates ||
					c.seoTitleCandidates ||
					c.seo_title_candidates
				: [];
			let titleCandidates = uniqueStrings(
				(rawTitleCandidates || [])
					.map((t) => String(t || "").trim())
					.filter(Boolean),
				{ limit: 8 },
			);
			if (titleCandidates.length < 3) {
				const fallbackTitles = buildClipTitleCandidates(line, fallbackBase);
				titleCandidates = uniqueStrings(
					[...titleCandidates, ...fallbackTitles],
					{ limit: 8 },
				);
			}
			const rawThumbCandidates = Array.isArray(
				c?.thumbnailTextCandidates || c?.thumbnail_text_candidates,
			)
				? c.thumbnailTextCandidates || c.thumbnail_text_candidates
				: [];
			let thumbnailTextCandidates = uniqueStrings(
				(rawThumbCandidates || [])
					.map((t) => String(t || "").trim())
					.filter(Boolean),
				{ limit: 8 },
			);
			if (thumbnailTextCandidates.length < 3) {
				const fallbackThumbs = buildClipThumbnailTextCandidates(
					line,
					fallbackBase,
				);
				thumbnailTextCandidates = uniqueStrings(
					[...thumbnailTextCandidates, ...fallbackThumbs],
					{ limit: 8 },
				);
			}
			return {
				id: String(c?.id || `short_${segmentIndex}_${idx}`),
				type: String(c?.type || "context_needed"),
				segmentIndex,
				line,
				openLoop: typeof c?.openLoop === "boolean" ? c.openLoop : false,
				ctaLine: String(c?.ctaLine || c?.cta_line || SHORTS_DEFAULT_CTA_LINE),
				targetSeconds: normalizeTargetSeconds(
					c?.targetSeconds ?? c?.target_seconds,
				),
				titleCandidates,
				thumbnailTextCandidates,
				status: c?.status || "pending",
				uploaded: Boolean(c?.uploaded),
				localPath: c?.localPath || "",
				publicUrl: c?.publicUrl || "",
				youtubeLink: c?.youtubeLink || "",
				fullVideoUrl: normalizeYouTubeWatchUrl(
					c?.fullVideoUrl || c?.full_video_url || "",
				),
				relatedLongVideoUrl: normalizeYouTubeWatchUrl(
					c?.relatedLongVideoUrl || c?.related_long_video_url || "",
				),
				relatedLongYoutubeId: extractYouTubeVideoId(
					c?.relatedLongYoutubeId || c?.related_long_youtube_id || "",
				),
				lastError: c?.lastError || "",
				uploadedAt: c?.uploadedAt || "",
			};
		})
		.filter(Boolean);

	return {
		status: String(raw.status || "").trim() || "pending",
		nextUploadAt: raw.nextUploadAt || "",
		generatedAt: raw.generatedAt || "",
		angle: String(raw.angle || "").trim(),
		titleCandidates: Array.isArray(raw.titleCandidates)
			? raw.titleCandidates.map((t) => String(t || "").trim()).filter(Boolean)
			: [],
		thumbnailTextCandidates: Array.isArray(raw.thumbnailTextCandidates)
			? raw.thumbnailTextCandidates
					.map((t) => String(t || "").trim())
					.filter(Boolean)
			: [],
		clipCandidates: normalized,
	};
}

function stripClipCandidateForPlan(candidate) {
	if (!candidate) return candidate;
	return {
		id: String(candidate.id || "").trim(),
		type: String(candidate.type || "context_needed"),
		segmentIndex: Number.isFinite(Number(candidate.segmentIndex))
			? Number(candidate.segmentIndex)
			: 0,
		line: String(candidate.line || "").trim(),
		openLoop: Boolean(candidate.openLoop),
		ctaLine: String(candidate.ctaLine || SHORTS_DEFAULT_CTA_LINE).trim(),
		targetSeconds: normalizeTargetSeconds(candidate.targetSeconds),
		titleCandidates: Array.isArray(candidate.titleCandidates)
			? candidate.titleCandidates
			: [],
		thumbnailTextCandidates: Array.isArray(candidate.thumbnailTextCandidates)
			? candidate.thumbnailTextCandidates
			: [],
		fullVideoUrl: normalizeYouTubeWatchUrl(candidate.fullVideoUrl || ""),
		relatedLongVideoUrl: normalizeYouTubeWatchUrl(
			candidate.relatedLongVideoUrl || candidate.fullVideoUrl || "",
		),
		relatedLongYoutubeId: extractYouTubeVideoId(
			candidate.relatedLongYoutubeId ||
				candidate.relatedLongVideoUrl ||
				candidate.fullVideoUrl ||
				"",
		),
	};
}

function mergeShortRecordsIntoCandidates(
	clipCandidates = [],
	shortRecords = [],
) {
	if (!Array.isArray(clipCandidates) || !clipCandidates.length) return [];
	const byId = new Map();
	for (const record of Array.isArray(shortRecords) ? shortRecords : []) {
		if (!record || !record.clipId) continue;
		byId.set(String(record.clipId), record);
	}
	return clipCandidates.map((clip) => {
		const record = byId.get(String(clip.id || ""));
		if (!record) return clip;
		return {
			...clip,
			title: record.title || clip.title || "",
			titleCandidates:
				Array.isArray(record.titleCandidates) && record.titleCandidates.length
					? record.titleCandidates
					: clip.titleCandidates,
			thumbnailTextCandidates:
				Array.isArray(record.thumbnailTextCandidates) &&
				record.thumbnailTextCandidates.length
					? record.thumbnailTextCandidates
					: clip.thumbnailTextCandidates,
			status: record.status || clip.status,
			localPath: record.localPath || clip.localPath,
			publicUrl: record.publicUrl || clip.publicUrl,
			youtubeLink: record.youtubeLink || clip.youtubeLink,
			fullVideoUrl:
				normalizeYouTubeWatchUrl(record.fullVideoUrl || "") ||
				clip.fullVideoUrl ||
				"",
			relatedLongVideoUrl:
				normalizeYouTubeWatchUrl(
					record.relatedLongVideoUrl || record.fullVideoUrl || "",
				) ||
				clip.relatedLongVideoUrl ||
				"",
			relatedLongYoutubeId:
				extractYouTubeVideoId(
					record.relatedLongYoutubeId ||
						record.relatedLongVideoUrl ||
						record.fullVideoUrl ||
						"",
				) ||
				clip.relatedLongYoutubeId ||
				"",
			lastError: record.lastError || clip.lastError,
			uploadedAt: record.uploadedAt || clip.uploadedAt || "",
		};
	});
}

function fallbackShortsDetails(video) {
	const segments = Array.isArray(video?.longVideoMeta?.segments)
		? video.longVideoMeta.segments
		: [];
	const fallbackBase = String(video?.seoTitle || "").trim();
	const fallbackCandidates = segments.slice(0, 4).map((seg, idx) => ({
		id: `short_${seg.index ?? idx}_${idx}`,
		type: idx === 0 ? "hook" : "context_needed",
		segmentIndex: Number.isFinite(Number(seg.index)) ? Number(seg.index) : idx,
		line: String(seg.text || "").trim(),
		openLoop: /\?/.test(String(seg.text || "")),
		ctaLine: SHORTS_DEFAULT_CTA_LINE,
		targetSeconds: 25,
		titleCandidates: buildClipTitleCandidates(
			String(seg.text || "").trim(),
			fallbackBase,
		),
		thumbnailTextCandidates: buildClipThumbnailTextCandidates(
			String(seg.text || "").trim(),
			fallbackBase,
		),
		status: "pending",
		uploaded: false,
		localPath: "",
		publicUrl: "",
		youtubeLink: "",
		fullVideoUrl: "",
		relatedLongVideoUrl: "",
		relatedLongYoutubeId: "",
		lastError: "",
		uploadedAt: "",
	}));
	return {
		angle: String(video?.seoTitle || "").trim(),
		titleCandidates: [String(video?.seoTitle || "").trim()].filter(Boolean),
		thumbnailTextCandidates: [],
		clipCandidates: fallbackCandidates,
	};
}

async function resolveSourceVideoPath(video, req) {
	if (video?.localFilePath && fs.existsSync(video.localFilePath)) {
		return { path: video.localFilePath, source: "local" };
	}
	const outputUrl = String(video?.outputUrl || "").trim();
	if (outputUrl) {
		const mapped = mapUploadsUrlToPath(outputUrl);
		if (mapped && fs.existsSync(mapped))
			return { path: mapped, source: "uploads" };
		if (isHttpUrl(outputUrl) && outputUrl.endsWith(".mp4")) {
			const out = path.join(
				SHORTS_SOURCE_DIR,
				`source_${String(video._id)}.mp4`,
			);
			if (!fs.existsSync(out)) await downloadToFile(outputUrl, out, 120000);
			return { path: out, source: "download" };
		}
	}
	const sourceVideoUrl = String(req?.body?.sourceVideoUrl || "").trim();
	if (sourceVideoUrl && isHttpUrl(sourceVideoUrl)) {
		const out = path.join(SHORTS_SOURCE_DIR, `source_${String(video._id)}.mp4`);
		if (!fs.existsSync(out)) await downloadToFile(sourceVideoUrl, out, 120000);
		return { path: out, source: "download" };
	}
	throw new Error(
		"Source video not found. Persist the long video output or provide sourceVideoUrl.",
	);
}

function computeClipWindow(timeline = [], candidate) {
	const segIndex = Number(candidate?.segmentIndex);
	if (!Number.isFinite(segIndex) || !timeline[segIndex]) {
		throw new Error(`Invalid segmentIndex ${candidate?.segmentIndex}`);
	}
	const startSec = Number(timeline[segIndex]?.startSec || 0);
	const targetSeconds = normalizeTargetSeconds(candidate?.targetSeconds);
	let endSec = startSec;
	let idx = segIndex;
	while (idx < timeline.length && endSec - startSec < targetSeconds) {
		const segEnd = Number(timeline[idx]?.endSec || endSec);
		endSec = Math.max(endSec, segEnd);
		idx += 1;
	}
	let durationSec = Math.max(0.1, endSec - startSec);
	if (durationSec < SHORTS_MIN_SEC) {
		while (idx < timeline.length && durationSec < SHORTS_MIN_SEC) {
			const segEnd = Number(timeline[idx]?.endSec || endSec);
			endSec = Math.max(endSec, segEnd);
			durationSec = endSec - startSec;
			idx += 1;
		}
	}
	if (durationSec > SHORTS_MAX_SEC) {
		durationSec = SHORTS_MAX_SEC;
		endSec = startSec + durationSec;
	}
	return { startSec, durationSec, endSec };
}

async function createShortClip({
	inputPath,
	startSec,
	durationSec,
	outputPath,
}) {
	const ffmpegPath = ffmpegStatic || "ffmpeg";
	const w = makeEven(SHORTS_WIDTH);
	const h = makeEven(SHORTS_HEIGHT);
	const watermarkFilter = buildShortsWatermarkFilter(w, h);
	const vf = [
		`scale=${w}:${h}:force_original_aspect_ratio=increase`,
		`crop=${w}:${h}`,
		watermarkFilter,
		"format=yuv420p",
	]
		.filter(Boolean)
		.join(",");
	await spawnBin(
		ffmpegPath,
		[
			"-ss",
			Number(startSec).toFixed(3),
			"-i",
			inputPath,
			"-t",
			Number(durationSec).toFixed(3),
			"-vf",
			vf,
			"-c:v",
			"libx264",
			"-preset",
			"veryfast",
			"-crf",
			String(SHORTS_CRF),
			"-c:a",
			"aac",
			"-b:a",
			SHORTS_AUDIO_BITRATE,
			"-movflags",
			"+faststart",
			"-y",
			outputPath,
		],
		"shorts_clip",
		{ timeoutMs: 240000 },
	);
	return outputPath;
}

function buildShortsTitle({
	candidate,
	clipIndex = 0,
	fallbackTitle = "Short update",
	usedTitles,
}) {
	const explicit = Array.isArray(candidate?.titleCandidates)
		? candidate.titleCandidates
		: [];
	const fallback = buildClipTitleCandidates(
		String(candidate?.line || "").trim(),
		fallbackTitle,
	);
	const pool = uniqueStrings([...explicit, ...fallback], { limit: 8 });
	return pickUniqueTitle({
		titleCandidates: pool,
		usedTitles,
		line: String(candidate?.line || "").trim(),
		clipIndex,
	});
}

function ensureFullVideoLinkInDescription(description = "", fullVideoUrl = "") {
	const url = normalizeYouTubeWatchUrl(fullVideoUrl);
	if (!url) return String(description || "").trim();
	const current = String(description || "").trim();
	const videoId = extractYouTubeVideoId(url);
	if (
		current &&
		(current.includes(url) || (videoId && current.includes(videoId)))
	) {
		return current;
	}
	const header = `Watch the full video: ${url}`;
	if (!current) return header;
	return `${header}\n${current}`;
}

function buildShortsDescription(candidate) {
	const line = String(candidate?.line || "").trim();
	const cta = String(candidate?.ctaLine || SHORTS_DEFAULT_CTA_LINE).trim();
	const fullVideoUrl = normalizeYouTubeWatchUrl(
		candidate?.relatedLongVideoUrl || candidate?.fullVideoUrl || "",
	);
	const fullVideoLine = fullVideoUrl
		? `Watch the full video: ${fullVideoUrl}`
		: "";
	const parts = [fullVideoLine, line, "", cta, "", "#shorts"];
	return parts.filter((p) => p !== "").join("\n");
}

async function uploadShortClip({ video, shortDoc, clipIndex }) {
	const userId = video?.user;
	if (!userId) throw new Error("Short upload missing user");
	const user = await User.findById(userId);
	if (!user) throw new Error("Short upload user not found");
	const relatedLongVideoUrl =
		normalizeYouTubeWatchUrl(shortDoc?.relatedLongVideoUrl || "") ||
		normalizeYouTubeWatchUrl(shortDoc?.fullVideoUrl || "") ||
		resolveOriginalLongYouTubeUrl(video);
	if (!relatedLongVideoUrl) throw new Error("missing_original_long_youtube_link");
	const reqMock = {
		body: {
			youtubeAccessToken: video.youtubeAccessToken || "",
			youtubeRefreshToken: video.youtubeRefreshToken || "",
			youtubeTokenExpiresAt: video.youtubeTokenExpiresAt || "",
		},
	};
	const youtubeTokens = await refreshYouTubeTokensIfNeeded(user, reqMock);
	if (!youtubeTokens?.refresh_token)
		throw new Error("Missing YouTube refresh token");

	const title =
		String(shortDoc?.title || "").trim() ||
		buildShortsTitle({
			candidate: shortDoc,
			clipIndex,
			fallbackTitle: String(video?.seoTitle || "Short update").trim(),
			usedTitles: new Set(),
		});
	const description =
		ensureFullVideoLinkInDescription(
			String(shortDoc?.description || "").trim() ||
				buildShortsDescription({
					...shortDoc.toObject?.(),
					line: shortDoc?.line,
					ctaLine: shortDoc?.ctaLine,
					relatedLongVideoUrl,
					fullVideoUrl: relatedLongVideoUrl,
				}),
			relatedLongVideoUrl,
		);
	const tags = ["shorts", "short", "clip"];
	const category = YT_CATEGORY_MAP[video?.category]
		? video.category
		: "Entertainment";
	console.log("[ShortsFromLong] uploading short", {
		longVideoId: String(video?._id || ""),
		clipId: String(shortDoc?.clipId || ""),
		relatedLongVideoUrl,
	});
	return await uploadToYouTube(youtubeTokens, shortDoc.localPath, {
		title,
		description,
		tags,
		category,
	});
}

async function uploadNextReadyShortForVideo({
	video,
	fullVideoUrl = "",
	markFailed = true,
} = {}) {
	const shortsToUpload = await ShortVideo.find({
		longVideo: video._id,
		status: "ready",
	}).sort({ orderIndex: 1, createdAt: 1 });

	if (!shortsToUpload.length) {
		return { attempted: false, uploaded: false, reason: "no_ready_shorts" };
	}

	const shortDoc = shortsToUpload[0];
	if (!shortDoc.localPath || !fs.existsSync(shortDoc.localPath)) {
		const error = "short_local_file_missing";
		shortDoc.status = markFailed ? "failed" : "ready";
		shortDoc.lastError = error;
		await shortDoc.save();
		return {
			attempted: true,
			uploaded: false,
			clipId: String(shortDoc.clipId || ""),
			orderIndex: Number(shortDoc.orderIndex || 0),
			error,
		};
	}

	const resolvedFullUrl =
		normalizeYouTubeWatchUrl(fullVideoUrl) ||
		normalizeYouTubeWatchUrl(shortDoc.relatedLongVideoUrl || "") ||
		normalizeYouTubeWatchUrl(shortDoc.fullVideoUrl || "") ||
		resolveOriginalLongYouTubeUrl(video);
	if (!resolvedFullUrl) {
		const error = "missing_original_long_youtube_link";
		shortDoc.status = "ready";
		shortDoc.lastError = error;
		await shortDoc.save();
		return {
			attempted: false,
			uploaded: false,
			clipId: String(shortDoc.clipId || ""),
			orderIndex: Number(shortDoc.orderIndex || 0),
			reason: error,
			error,
		};
	}

	shortDoc.fullVideoUrl = resolvedFullUrl;
	shortDoc.relatedLongVideoUrl = resolvedFullUrl;
	shortDoc.relatedLongYoutubeId = extractYouTubeVideoId(resolvedFullUrl);
	if (!shortDoc.description) {
		shortDoc.description = buildShortsDescription({
			line: shortDoc.line,
			ctaLine: shortDoc.ctaLine,
			fullVideoUrl: resolvedFullUrl,
			relatedLongVideoUrl: resolvedFullUrl,
		});
	} else if (resolvedFullUrl) {
		const updatedDescription = ensureFullVideoLinkInDescription(
			shortDoc.description,
			resolvedFullUrl,
		);
		if (updatedDescription !== shortDoc.description) {
			shortDoc.description = updatedDescription;
		}
	}

	try {
		const youtubeLink = await uploadShortClip({
			video,
			shortDoc,
			clipIndex: Number.isFinite(Number(shortDoc.orderIndex))
				? Number(shortDoc.orderIndex)
				: 0,
		});
		const uploadedAt = new Date();
		shortDoc.status = "uploaded";
		shortDoc.youtubeLink = youtubeLink;
		shortDoc.uploadedAt = uploadedAt;
		shortDoc.lastError = "";
		await shortDoc.save();
		return {
			attempted: true,
			uploaded: true,
			clipId: String(shortDoc.clipId || ""),
			orderIndex: Number(shortDoc.orderIndex || 0),
			youtubeLink,
			uploadedAt,
		};
	} catch (e) {
		const error = e.message || "upload_failed";
		shortDoc.status = markFailed ? "failed" : "ready";
		shortDoc.lastError = error;
		await shortDoc.save();
		return {
			attempted: true,
			uploaded: false,
			clipId: String(shortDoc.clipId || ""),
			orderIndex: Number(shortDoc.orderIndex || 0),
			error,
		};
	}
}

exports.createShortsFromLong = async (req, res) => {
	let source = null;
	try {
		const requestStartedAt = new Date();
		const { videoId } = req.params;
		const { maxClips = SHORTS_MAX_CLIPS, forceRegenerate = false } =
			req.body || {};
		const limit = clampNumber(
			Number(maxClips || 0) || 0,
			SHORTS_MIN_CLIPS,
			SHORTS_MAX_CLIPS,
		);

		const video = await Video.findById(videoId);
		if (!video) return res.status(404).json({ error: "Video not found." });
		if (
			String(video.user || "") !== String(req.user?._id || "") &&
			req.user?.role !== "admin"
		) {
			return res.status(403).json({ error: "Not authorized." });
		}
		if (!video.isLongVideo)
			return res.status(400).json({ error: "Not a long video." });
		if (!video.shortsDetails) {
			return res.status(400).json({
				error: "shortsDetails missing on this long video.",
			});
		}

		source = await resolveSourceVideoPath(video, req);
		const timeline = Array.isArray(video?.longVideoMeta?.timeline)
			? video.longVideoMeta.timeline
			: [];
		if (!timeline.length) {
			return res.status(400).json({
				error:
					"Missing timeline metadata. Regenerate this long video with the updated pipeline.",
			});
		}

		const segments = Array.isArray(video?.longVideoMeta?.segments)
			? video.longVideoMeta.segments
			: [];
		let shortsDetails = normalizeClipCandidates(video?.shortsDetails, segments);
		if (!shortsDetails.clipCandidates.length) {
			shortsDetails = fallbackShortsDetails(video);
		}
		if (!shortsDetails.clipCandidates.length) {
			return res.status(400).json({
				error: "Missing shorts clip candidates. Regenerate shorts details.",
			});
		}

		const clipCandidates = shortsDetails.clipCandidates;
		const candidatesToProcess = clipCandidates.slice(0, limit);
		const baseUrl = buildPublicBaseUrl(req);
		const fullVideoUrl = resolveOriginalLongYouTubeUrl(video);
		const relatedLongYoutubeId = extractYouTubeVideoId(fullVideoUrl);

		const existingShorts = await ShortVideo.find({
			longVideo: video._id,
		}).lean();
		const alreadyUploadedCount = forceRegenerate
			? 0
			: (existingShorts || []).filter(
					(s) => s.status === "uploaded" && s.youtubeLink,
				).length;
		const existingNextUploadAt =
			forceRegenerate
				? ""
				: shortsDetails.nextUploadAt || video.shortsDetails?.nextUploadAt || "";
		const existingById = new Map(
			(existingShorts || []).map((s) => [String(s.clipId || ""), s]),
		);
		const usedTitles = new Set(
			(existingShorts || [])
				.map((s) => normalizeTitleKey(s.title || ""))
				.filter(Boolean),
		);

		let generatedCount = 0;
		for (let i = 0; i < candidatesToProcess.length; i++) {
			const candidate = candidatesToProcess[i];
			if (fullVideoUrl) {
				candidate.fullVideoUrl = fullVideoUrl;
				candidate.relatedLongVideoUrl = fullVideoUrl;
				candidate.relatedLongYoutubeId = relatedLongYoutubeId;
			}

			const fallbackBase =
				String(shortsDetails.angle || "").trim() ||
				String(video?.seoTitle || "").trim() ||
				"Short update";
			if (
				!Array.isArray(candidate.titleCandidates) ||
				!candidate.titleCandidates.length
			) {
				candidate.titleCandidates = buildClipTitleCandidates(
					candidate.line,
					fallbackBase,
				);
			}
			if (
				!Array.isArray(candidate.thumbnailTextCandidates) ||
				!candidate.thumbnailTextCandidates.length
			) {
				candidate.thumbnailTextCandidates = buildClipThumbnailTextCandidates(
					candidate.line,
					fallbackBase,
				);
			}

			const existing = existingById.get(String(candidate.id || ""));
			const shouldReuse =
				!forceRegenerate &&
				existing?.localPath &&
				fs.existsSync(existing.localPath);

			const { startSec, durationSec } = computeClipWindow(timeline, candidate);
			const outName = `short_${videoId}_${candidate.id}.mp4`;
			const outPath = path.join(SHORTS_OUTPUT_DIR, outName);

			if (!shouldReuse) {
				await createShortClip({
					inputPath: source.path,
					startSec,
					durationSec,
					outputPath: outPath,
				});
				generatedCount += 1;
			}

			const publicUrl = baseUrl
				? `${baseUrl}/uploads/shorts/${outName}`
				: existing?.publicUrl || "";
			const title =
				!forceRegenerate && existing?.title
					? existing.title
					: buildShortsTitle({
							candidate,
							clipIndex: i,
							fallbackTitle: String(video?.seoTitle || "Short update").trim(),
							usedTitles,
						});
			const rawDescription =
				!forceRegenerate && existing?.description
					? existing.description
					: buildShortsDescription({
							...candidate,
							fullVideoUrl: candidate.fullVideoUrl || fullVideoUrl,
							relatedLongVideoUrl:
								candidate.relatedLongVideoUrl || fullVideoUrl,
						});
			const description = ensureFullVideoLinkInDescription(
				rawDescription,
				fullVideoUrl,
			);
			const status =
				!forceRegenerate && existing?.status === "uploaded"
					? "uploaded"
					: "ready";
			const lastError = forceRegenerate ? "" : existing?.lastError || "";

			await ShortVideo.findOneAndUpdate(
				{ longVideo: video._id, clipId: String(candidate.id || "") },
				{
					user: video.user,
					longVideo: video._id,
					clipId: String(candidate.id || ""),
					orderIndex: i,
					segmentIndex: candidate.segmentIndex,
					type: candidate.type,
					line: candidate.line,
					openLoop: candidate.openLoop,
					ctaLine: candidate.ctaLine,
					targetSeconds: candidate.targetSeconds,
					title,
					titleCandidates: candidate.titleCandidates || [],
					thumbnailTextCandidates: candidate.thumbnailTextCandidates || [],
					description,
					localPath: shouldReuse ? existing?.localPath : outPath,
					publicUrl,
					fullVideoUrl: fullVideoUrl || existing?.fullVideoUrl || "",
					relatedLongVideoUrl:
						fullVideoUrl || existing?.relatedLongVideoUrl || "",
					relatedLongYoutubeId:
						relatedLongYoutubeId || existing?.relatedLongYoutubeId || "",
					status,
					lastError,
					generatedAt: shouldReuse
						? existing?.generatedAt || new Date()
						: new Date(),
					startSec,
					durationSec,
					youtubeLink: forceRegenerate ? "" : existing?.youtubeLink || "",
					uploadedAt: forceRegenerate ? null : existing?.uploadedAt || null,
				},
				{ upsert: true, new: true, setDefaultsOnInsert: true },
			);
		}

		const plannedCandidates = candidatesToProcess.map((c) =>
			stripClipCandidateForPlan(c),
		);
		let nextUploadAt =
			existingNextUploadAt ||
			addHours(requestStartedAt, SHORTS_UPLOAD_DELAY_HOURS).toISOString();
		video.shortsDetails = {
			...shortsDetails,
			clipCandidates: plannedCandidates,
			status: "ready",
			generatedAt: nowIso(),
			nextUploadAt,
			updatedAt: nowIso(),
		};
		await video.save();

		let instantUpload = {
			enabled: SHORTS_UPLOAD_FIRST_IMMEDIATELY,
			attempted: false,
			uploaded: false,
			reason: SHORTS_UPLOAD_FIRST_IMMEDIATELY
				? alreadyUploadedCount > 0
					? "already_has_uploaded_short"
					: ""
				: "disabled",
		};
		if (SHORTS_UPLOAD_FIRST_IMMEDIATELY && alreadyUploadedCount <= 0) {
			instantUpload = {
				enabled: true,
				...(await uploadNextReadyShortForVideo({
					video,
					fullVideoUrl,
					markFailed: false,
				})),
			};
			nextUploadAt = addHours(
				requestStartedAt,
				SHORTS_UPLOAD_GAP_HOURS,
			).toISOString();
			const uploadedCount = await ShortVideo.countDocuments({
				longVideo: video._id,
				status: "uploaded",
			});
			video.shortsDetails = {
				...video.shortsDetails,
				status:
					uploadedCount >= plannedCandidates.length ? "uploaded" : "ready",
				nextUploadAt,
				instantUpload,
				updatedAt: nowIso(),
			};
			await video.save();
		}

		const shortsRecords = await ShortVideo.find({
			longVideo: video._id,
		}).lean();
		const responseDetails = {
			...video.shortsDetails,
			clipCandidates: mergeShortRecordsIntoCandidates(
				video.shortsDetails.clipCandidates || [],
				shortsRecords,
			),
		};

		return res.json({
			success: true,
			generatedCount,
			source: source.source,
			instantUpload,
			shortsDetails: responseDetails,
		});
	} catch (err) {
		console.error("[createShortsFromLong] error:", err.message || err);
		return res.status(500).json({
			error: err.message || "Failed to generate shorts.",
		});
	} finally {
		cleanupDownloadedSource(source);
	}
};

exports.getShortsFromLong = async (req, res) => {
	try {
		const { videoId } = req.params;
		const video = await Video.findById(videoId);
		if (!video) return res.status(404).json({ error: "Video not found." });
		if (
			String(video.user || "") !== String(req.user?._id || "") &&
			req.user?.role !== "admin"
		) {
			return res.status(403).json({ error: "Not authorized." });
		}
		if (!video.isLongVideo)
			return res.status(400).json({ error: "Not a long video." });

		const segments = Array.isArray(video?.longVideoMeta?.segments)
			? video.longVideoMeta.segments
			: [];
		const normalized =
			video.shortsDetails && typeof video.shortsDetails === "object"
				? normalizeClipCandidates(video.shortsDetails, segments)
				: null;
		const shortsRecords = await ShortVideo.find({
			longVideo: video._id,
		}).lean();
		const mergedDetails = normalized
			? {
					...normalized,
					clipCandidates: mergeShortRecordsIntoCandidates(
						normalized.clipCandidates || [],
						shortsRecords,
					),
				}
			: null;

		return res.json({
			success: true,
			shortsDetails: mergedDetails,
			longVideoMeta: video.longVideoMeta || null,
		});
	} catch (err) {
		console.error("[getShortsFromLong] error:", err.message || err);
		return res.status(500).json({
			error: err.message || "Failed to load shorts status.",
		});
	}
};

exports.listShortsEligibleLongVideos = async (req, res) => {
	try {
		const filter = {
			isLongVideo: true,
			shortsDetails: { $ne: null },
			$or: [
				{ "shortsDetails.status": { $exists: false } },
				{ "shortsDetails.status": "planned" },
			],
		};
		if (req.user?.role !== "admin") filter.user = req.user._id;
		const videos = await Video.find(filter)
			.sort({ createdAt: -1 })
			.select("_id seoTitle topic createdAt shortsDetails");

		return res.json({
			success: true,
			count: videos.length,
			data: videos,
		});
	} catch (err) {
		console.error("[listShortsEligibleLongVideos] error:", err.message || err);
		return res.status(500).json({
			error: err.message || "Failed to load eligible long videos.",
		});
	}
};

exports.processPendingShortUploads = async ({ limit = 3 } = {}) => {
	const max = clampNumber(Number(limit) || 0, 1, 10);
	const videos = await Video.find({
		isLongVideo: true,
		shortsDetails: { $ne: null },
	});
	let processed = 0;
	let uploaded = 0;
	const errors = [];
	const nowTs = Date.now();

	for (const video of videos) {
		if (processed >= max) break;
		const segments = Array.isArray(video?.longVideoMeta?.segments)
			? video.longVideoMeta.segments
			: [];
		const details = normalizeClipCandidates(video.shortsDetails, segments);
		const nextUploadAtRaw =
			details.nextUploadAt || video.shortsDetails?.nextUploadAt;
		const nextUploadAtTs = nextUploadAtRaw
			? new Date(nextUploadAtRaw).getTime()
			: 0;
		if (nextUploadAtTs && nowTs < nextUploadAtTs) continue;

		const fullVideoUrl = resolveOriginalLongYouTubeUrl(video);
		const uploadResult = await uploadNextReadyShortForVideo({
			video,
			fullVideoUrl,
			markFailed: true,
		});

		if (!uploadResult.attempted) continue;

		if (uploadResult.uploaded) {
			uploaded += 1;
		} else {
			errors.push({
				videoId: String(video._id),
				clipId: String(uploadResult.clipId || ""),
				error: uploadResult.error || "upload_failed",
			});
		}
		processed += 1;

		video.shortsDetails = {
			...video.shortsDetails,
			nextUploadAt: addHours(
				uploadResult.uploadedAt || new Date(),
				SHORTS_UPLOAD_GAP_HOURS,
			).toISOString(),
			updatedAt: nowIso(),
		};

		const plannedCount = Array.isArray(details.clipCandidates)
			? details.clipCandidates.length
			: 0;
		if (plannedCount) {
			const uploadedCount = await ShortVideo.countDocuments({
				longVideo: video._id,
				status: "uploaded",
			});
			if (uploadedCount >= plannedCount) {
				video.shortsDetails = {
					...video.shortsDetails,
					status: "uploaded",
					updatedAt: nowIso(),
				};
			}
		}

		await video.save();
	}

	return { processed, uploaded, errors };
};
