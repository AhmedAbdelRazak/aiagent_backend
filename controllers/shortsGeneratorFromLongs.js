/* controllers/shortsGeneratorFromLongs.js */
const fs = require("fs");
const path = require("path");
const os = require("os");
const child_process = require("child_process");
const axios = require("axios");
const ffmpegStatic = require("ffmpeg-static");

const Video = require("../models/Video");
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
	1440
);
const SHORTS_HEIGHT = clampNumber(
	toNumber(process.env.SHORTS_HEIGHT, 1280),
	720,
	2560
);
const SHORTS_MIN_SEC = clampNumber(
	toNumber(process.env.SHORTS_MIN_SEC, 25),
	25,
	45
);
const SHORTS_MAX_SEC = clampNumber(
	toNumber(process.env.SHORTS_MAX_SEC, 45),
	25,
	45
);
const SHORTS_TARGET_SECONDS = [25, 35, 45];
const SHORTS_MAX_CLIPS = clampNumber(
	toNumber(process.env.SHORTS_MAX_CLIPS, 6),
	1,
	10
);
const SHORTS_MIN_CLIPS = clampNumber(
	toNumber(process.env.SHORTS_MIN_CLIPS, 3),
	1,
	SHORTS_MAX_CLIPS
);
const SHORTS_CRF = clampNumber(toNumber(process.env.SHORTS_CRF, 20), 16, 28);
const SHORTS_AUDIO_BITRATE = process.env.SHORTS_AUDIO_BITRATE || "160k";
const SHORTS_UPLOAD_DELAY_HOURS = clampNumber(
	toNumber(process.env.SHORTS_UPLOAD_DELAY_HOURS, 24),
	1,
	72
);
const SHORTS_UPLOAD_GAP_HOURS = clampNumber(
	toNumber(process.env.SHORTS_UPLOAD_GAP_HOURS, 2),
	1,
	24
);

function nowIso() {
	return new Date().toISOString();
}

function isHttpUrl(u) {
	return typeof u === "string" && /^https?:\/\//i.test(u);
}

function buildPublicBaseUrl(req) {
	const envBase = String(process.env.PUBLIC_BASE_URL || "").trim();
	if (envBase) return envBase.replace(/\/+$/, "");
	if (!req) return "";
	return `${req.protocol || "http"}://${req.get("host")}`;
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
	const normalized = candidates
		.map((c, idx) => {
			const segmentIndex = Number(
				c?.segmentIndex ?? c?.segment_index ?? c?.index ?? idx
			);
			if (!Number.isFinite(segmentIndex) || segmentIndex < 0) return null;
			if (segments.length && segmentIndex >= segments.length) return null;
			const line = String(c?.line || segments[segmentIndex]?.text || "").trim();
			if (!line) return null;
			return {
				id: String(c?.id || `short_${segmentIndex}_${idx}`),
				type: String(c?.type || "context_needed"),
				segmentIndex,
				line,
				openLoop: typeof c?.openLoop === "boolean" ? c.openLoop : false,
				ctaLine: String(c?.ctaLine || c?.cta_line || SHORTS_DEFAULT_CTA_LINE),
				targetSeconds: normalizeTargetSeconds(
					c?.targetSeconds ?? c?.target_seconds
				),
				status: c?.status || "pending",
				uploaded: Boolean(c?.uploaded),
				localPath: c?.localPath || "",
				publicUrl: c?.publicUrl || "",
				youtubeLink: c?.youtubeLink || "",
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

function fallbackShortsDetails(video) {
	const segments = Array.isArray(video?.longVideoMeta?.segments)
		? video.longVideoMeta.segments
		: [];
	const fallbackCandidates = segments.slice(0, 4).map((seg, idx) => ({
		id: `short_${seg.index ?? idx}_${idx}`,
		type: idx === 0 ? "hook" : "context_needed",
		segmentIndex: Number.isFinite(Number(seg.index)) ? Number(seg.index) : idx,
		line: String(seg.text || "").trim(),
		openLoop: /\?/.test(String(seg.text || "")),
		ctaLine: SHORTS_DEFAULT_CTA_LINE,
		targetSeconds: 25,
		status: "pending",
		uploaded: false,
		localPath: "",
		publicUrl: "",
		youtubeLink: "",
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
				`source_${String(video._id)}.mp4`
			);
			if (!fs.existsSync(out)) await downloadToFile(outputUrl, out, 120000);
			return { path: out, source: "download" };
		}
	}
	const sourceVideoUrl = String(req?.body?.sourceVideoUrl || "").trim();
	if (sourceVideoUrl && isHttpUrl(sourceVideoUrl)) {
		const out = path.join(SHORTS_SOURCE_DIR, `source_${String(video._id)}.mp4`);
		await downloadToFile(sourceVideoUrl, out, 120000);
		return { path: out, source: "download" };
	}
	throw new Error(
		"Source video not found. Persist the long video output or provide sourceVideoUrl."
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
	const vf = `scale=${w}:${h}:force_original_aspect_ratio=increase,crop=${w}:${h},format=yuv420p`;
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
		{ timeoutMs: 240000 }
	);
	return outputPath;
}

function buildShortsTitle(video, clipIndex = 0) {
	const titleCandidates = Array.isArray(video?.shortsDetails?.titleCandidates)
		? video.shortsDetails.titleCandidates
		: [];
	const fallback = String(video?.seoTitle || "Short update").trim();
	const picked =
		titleCandidates.length > 0
			? titleCandidates[clipIndex % titleCandidates.length]
			: fallback;
	return String(picked || fallback)
		.trim()
		.slice(0, 95);
}

function buildShortsDescription(candidate) {
	const line = String(candidate?.line || "").trim();
	const cta = String(candidate?.ctaLine || SHORTS_DEFAULT_CTA_LINE).trim();
	const fullVideoUrl = String(candidate?.fullVideoUrl || "").trim();
	const parts = [
		line,
		fullVideoUrl ? `From the full video: ${fullVideoUrl}` : "",
		"",
		cta,
		"",
		"#shorts",
	];
	return parts.filter((p) => p !== "").join("\n");
}

async function uploadShortClip({ video, candidate, clipIndex }) {
	const userId = video?.user;
	if (!userId) throw new Error("Short upload missing user");
	const user = await User.findById(userId);
	if (!user) throw new Error("Short upload user not found");
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

	const title = buildShortsTitle(video, clipIndex);
	const description = buildShortsDescription(candidate);
	const tags = ["shorts", "short", "clip"];
	const category = YT_CATEGORY_MAP[video?.category]
		? video.category
		: "Entertainment";
	return await uploadToYouTube(youtubeTokens, candidate.localPath, {
		title,
		description,
		tags,
		category,
	});
}

exports.createShortsFromLong = async (req, res) => {
	try {
		const { videoId } = req.params;
		const { maxClips = SHORTS_MAX_CLIPS, forceRegenerate = false } =
			req.body || {};
		const limit = clampNumber(
			Number(maxClips || 0) || 0,
			SHORTS_MIN_CLIPS,
			SHORTS_MAX_CLIPS
		);

		const video = await Video.findById(videoId);
		if (!video) return res.status(404).json({ error: "Video not found." });
		if (!video.isLongVideo)
			return res.status(400).json({ error: "Not a long video." });
		if (!video.shortsDetails) {
			return res.status(400).json({
				error: "shortsDetails missing on this long video.",
			});
		}

		const source = await resolveSourceVideoPath(video, req);
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
		const fullVideoUrl = isHttpUrl(video.youtubeLink)
			? video.youtubeLink
			: isHttpUrl(video.outputUrl)
				? video.outputUrl
				: "";

		let generatedCount = 0;
		for (let i = 0; i < candidatesToProcess.length; i++) {
			const candidate = candidatesToProcess[i];
			if (!candidate.fullVideoUrl && fullVideoUrl)
				candidate.fullVideoUrl = fullVideoUrl;
			if (
				!forceRegenerate &&
				candidate.localPath &&
				fs.existsSync(candidate.localPath)
			) {
				candidate.status = candidate.uploaded ? "uploaded" : "ready";
				continue;
			}

			const { startSec, durationSec } = computeClipWindow(timeline, candidate);
			const outName = `short_${videoId}_${candidate.id}.mp4`;
			const outPath = path.join(SHORTS_OUTPUT_DIR, outName);
			await createShortClip({
				inputPath: source.path,
				startSec,
				durationSec,
				outputPath: outPath,
			});
			const publicUrl = baseUrl ? `${baseUrl}/uploads/shorts/${outName}` : "";
			candidate.localPath = outPath;
			candidate.publicUrl = publicUrl;
			candidate.fullVideoUrl = fullVideoUrl;
			candidate.status = "ready";
			candidate.uploaded = false;
			candidate.lastError = "";
			candidate.generatedAt = nowIso();
			generatedCount += 1;
		}

		const nextUploadAt = new Date(
			Date.now() + SHORTS_UPLOAD_DELAY_HOURS * 60 * 60 * 1000
		).toISOString();
		video.shortsDetails = {
			...shortsDetails,
			clipCandidates,
			status: "ready",
			generatedAt: nowIso(),
			nextUploadAt,
			updatedAt: nowIso(),
		};
		await video.save();

		return res.json({
			success: true,
			generatedCount,
			source: source.source,
			shortsDetails: video.shortsDetails,
		});
	} catch (err) {
		console.error("[createShortsFromLong] error:", err.message || err);
		return res.status(500).json({
			error: err.message || "Failed to generate shorts.",
		});
	}
};

exports.getShortsFromLong = async (req, res) => {
	try {
		const { videoId } = req.params;
		const video = await Video.findById(videoId);
		if (!video) return res.status(404).json({ error: "Video not found." });
		if (!video.isLongVideo)
			return res.status(400).json({ error: "Not a long video." });

		return res.json({
			success: true,
			shortsDetails: video.shortsDetails || null,
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
		const details = normalizeClipCandidates(
			video.shortsDetails,
			video.longVideoMeta?.segments || []
		);
		const nextUploadAtRaw = details.nextUploadAt || video.shortsDetails?.nextUploadAt;
		const nextUploadAtTs = nextUploadAtRaw
			? new Date(nextUploadAtRaw).getTime()
			: 0;
		if (nextUploadAtTs && nowTs < nextUploadAtTs) continue;

		const fullVideoUrl = isHttpUrl(video.youtubeLink)
			? video.youtubeLink
			: isHttpUrl(video.outputUrl)
				? video.outputUrl
				: "";
		let attempted = false;
		for (let i = 0; i < details.clipCandidates.length; i++) {
			if (processed >= max) break;
			const candidate = details.clipCandidates[i];
			if (candidate.uploaded) continue;
			if (candidate.status !== "ready") continue;
			if (!candidate.localPath || !fs.existsSync(candidate.localPath)) continue;
			if (!candidate.fullVideoUrl && fullVideoUrl)
				candidate.fullVideoUrl = fullVideoUrl;

			try {
				const youtubeLink = await uploadShortClip({
					video,
					candidate,
					clipIndex: i,
				});
				candidate.uploaded = true;
				candidate.status = "uploaded";
				candidate.youtubeLink = youtubeLink;
				candidate.uploadedAt = nowIso();
				candidate.lastError = "";
				uploaded += 1;
				attempted = true;
			} catch (e) {
				candidate.status = "failed";
				candidate.lastError = e.message || "upload_failed";
				errors.push({
					videoId: String(video._id),
					clipId: candidate.id,
					error: candidate.lastError,
				});
				attempted = true;
			}

			processed += 1;
		}

		const allUploaded = details.clipCandidates.length
			? details.clipCandidates.every((c) => c.uploaded)
			: false;
		if (attempted) {
			details.nextUploadAt = new Date(
				nowTs + SHORTS_UPLOAD_GAP_HOURS * 60 * 60 * 1000
			).toISOString();
		}
		if (allUploaded) details.status = "uploaded";

		video.shortsDetails = {
			...video.shortsDetails,
			...details,
			updatedAt: nowIso(),
		};
		await video.save();
	}

	return { processed, uploaded, errors };
};
