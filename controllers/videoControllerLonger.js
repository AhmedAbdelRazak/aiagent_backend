/** @format */
/* videoControllerLonger.js - long-form talking-head pipeline (in-memory jobs) *
 * Required env (same access style as videoController.js): CHATGPT_API_TOKEN, ELEVENLABS_API_KEY, SYNC_SO_API_KEY
 * Optional env: ELEVEN_VOICE_ID, GOOGLE_CSE_ID/GOOGLE_CSE_KEY, TMDB_API_KEY, RUNWAY_API_KEY, BASE_URL, FFMPEG_PATH
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const child_process = require("child_process");
const axios = require("axios");
const dayjs = require("dayjs");
const { OpenAI } = require("openai");
const ffmpegStatic = require("ffmpeg-static");
const Schedule = require("../models/Schedule");
const { google } = require("googleapis");

let googleTrends = null;
try {
	googleTrends = require("google-trends-api");
} catch {
	googleTrends = null;
}

const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });
const ELEVEN_API_KEY = process.env.ELEVENLABS_API_KEY;
const ELEVEN_VOICE_ID = process.env.ELEVEN_VOICE_ID;
const SYNC_SO_API_KEY = process.env.SYNC_SO_API_KEY;
const RUNWAY_API_KEY = process.env.RUNWAY_API_KEY;
const TMDB_API_KEY = process.env.TMDB_API_KEY;
const GOOGLE_CSE_ID =
	process.env.GOOGLE_CSE_ID ||
	process.env.GOOGLE_CUSTOM_SEARCH_CX ||
	process.env.GOOGLE_CUSTOM_SEARCH_ID ||
	null;
const GOOGLE_CSE_KEY =
	process.env.GOOGLE_CSE_KEY ||
	process.env.GOOGLE_CUSTOM_SEARCH_KEY ||
	process.env.GOOGLE_API_KEY ||
	null;
const GOOGLE_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1";

const CHAT_MODEL = "gpt-5.1";
const SYNC_SO_BASE = "https://api.sync.so/v2";
const RUNWAY_VERSION = "2024-11-06";
const RUNWAY_MODEL = "gen4_turbo";
const TMP_DIR = path.join(os.tmpdir(), "agentai_long_video");
const OUTPUT_DIR = path.join(__dirname, "../uploads/videos");
const ALLOWED_DURATIONS = new Set([20, 45, 60, 120, 180, 240, 300]);
const DEFAULT_TOPIC = "The biggest movie releases of the year so far";
const PRESENTER_CANDIDATES = [
	path.join(__dirname, "../PhotosForLongerVideos/MyPhotoWithASuit.png"),
	path.join(__dirname, "../PhotosForLongerVideos/MyPhotoWithHalfSleeve.png"),
];

// WARNING: in-memory job store resets on server restart.
const JOBS = new Map();

function ensureDir(dir) {
	if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
ensureDir(TMP_DIR);
ensureDir(OUTPUT_DIR);

const FFMPEG_CANDIDATES = [
	(typeof ffmpegStatic === "string" && ffmpegStatic.trim()) || null,
	process.env.FFMPEG_PATH && process.env.FFMPEG_PATH.trim(),
	process.env.FFMPEG && process.env.FFMPEG.trim(),
	process.env.FFMPEG_BIN && process.env.FFMPEG_BIN.trim(),
	os.platform() === "win32" ? "ffmpeg" : "/usr/bin/ffmpeg",
	"ffmpeg",
];

function resolveFfmpegPath() {
	for (const candidate of FFMPEG_CANDIDATES) {
		if (!candidate) continue;
		try {
			child_process.execSync(`"${candidate}" -version`, { stdio: "ignore" });
			return candidate;
		} catch {
			// try next candidate
		}
	}
	return null;
}

const ffmpegPath = resolveFfmpegPath();
if (!ffmpegPath) {
	console.warn(
		"[LongVideo] WARN - No valid FFmpeg binary found. Set FFMPEG_PATH or ensure ffmpeg is on PATH."
	);
}

function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function nowIso() {
	return new Date().toISOString();
}

function isHttpUrl(u) {
	return typeof u === "string" && /^https?:\/\//i.test(u);
}

function normalizeLanguage(lang = "en") {
	const raw = String(lang || "")
		.trim()
		.toLowerCase();
	if (!raw) return "English";
	if (raw.startsWith("en")) return "English";
	if (raw.startsWith("ar")) return "Arabic";
	if (raw.startsWith("es")) return "Spanish";
	if (raw.startsWith("fr")) return "Francais";
	if (raw.startsWith("de")) return "Deutsch";
	return "English";
}

function clamp(num, min, max) {
	return Math.max(min, Math.min(max, num));
}

function normalizeOverlayAssets(list = [], targetDurationSec) {
	if (!Array.isArray(list)) return [];
	const out = [];
	for (const raw of list) {
		if (!raw || typeof raw !== "object") continue;
		const startSec = Number(raw.startSec);
		const endSec = Number(raw.endSec);
		if (!Number.isFinite(startSec) || !Number.isFinite(endSec)) continue;
		if (endSec <= startSec) continue;
		const clean = {
			type: raw.type === "video" ? "video" : "image",
			url: String(raw.url || "").trim(),
			startSec: clamp(startSec, 0, Math.max(1, targetDurationSec)),
			endSec: clamp(endSec, 0, Math.max(1, targetDurationSec)),
			position: String(raw.position || "topRight"),
			scale: clamp(Number(raw.scale || 0.35), 0.1, 1),
		};
		if (!clean.url) continue;
		out.push(clean);
	}
	return out;
}

function pickPresenterImage(presenterImageUrl) {
	if (presenterImageUrl && isHttpUrl(presenterImageUrl))
		return presenterImageUrl;
	if (presenterImageUrl && fs.existsSync(presenterImageUrl))
		return presenterImageUrl;
	for (const p of PRESENTER_CANDIDATES) {
		if (fs.existsSync(p)) return p;
	}
	return null;
}

function updateJob(jobId, patch = {}) {
	const job = JOBS.get(jobId);
	if (!job) return;
	JOBS.set(jobId, {
		...job,
		...patch,
		updatedAt: nowIso(),
	});
}

async function withRetry(fn, max, label) {
	let lastErr;
	for (let i = 1; i <= max; i++) {
		try {
			return await fn();
		} catch (err) {
			lastErr = err;
			const status = err?.response?.status;
			console.warn(
				`[LongVideo] ${label} attempt ${i}/${max} failed${
					status ? ` (HTTP ${status})` : ""
				} ? ${err?.message || err}`
			);
			if (status && status >= 400 && status < 500 && status !== 429) break;
			await sleep(350 * i);
		}
	}
	throw lastErr;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 20000) {
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), timeoutMs);
	try {
		return await fetch(url, { ...options, signal: controller.signal });
	} finally {
		clearTimeout(timer);
	}
}

function stripCodeFence(s = "") {
	const marker = "```";
	const first = s.indexOf(marker);
	if (first === -1) return s;
	const after = s.slice(first + marker.length);
	const second = after.lastIndexOf(marker);
	if (second === -1) return s;
	return after.slice(0, second);
}

function parseJsonFlexible(raw = "") {
	const cleaned = stripCodeFence(String(raw || "").trim());
	try {
		return JSON.parse(cleaned);
	} catch {
		const m = cleaned.match(/\{[\s\S]*\}/);
		if (!m) return null;
		try {
			return JSON.parse(m[0]);
		} catch {
			return null;
		}
	}
}

async function downloadToFile(url, outPath) {
	await withRetry(
		async () => {
			const res = await axios.get(url, {
				responseType: "stream",
				timeout: 20000,
			});
			await new Promise((resolve, reject) => {
				const ws = fs.createWriteStream(outPath);
				res.data.pipe(ws);
				ws.on("finish", resolve);
				ws.on("error", reject);
			});
		},
		2,
		"download"
	);
	return outPath;
}

function spawnFfmpeg(args, label) {
	return new Promise((resolve, reject) => {
		if (!ffmpegPath) {
			reject(new Error("FFmpeg binary not found"));
			return;
		}
		const proc = child_process.spawn(ffmpegPath, args, {
			stdio: ["ignore", "pipe", "pipe"],
		});
		let stderr = "";
		proc.stderr.on("data", (d) => {
			stderr += d.toString();
		});
		proc.on("error", (err) => reject(err));
		proc.on("close", (code) => {
			if (code === 0) return resolve();
			return reject(
				new Error(`[FFmpeg] ${label} failed (code ${code}): ${stderr}`)
			);
		});
	});
}

async function concatVideos(videoPaths, outPath) {
	const listFile = path.join(TMP_DIR, `concat_${crypto.randomUUID()}.txt`);
	fs.writeFileSync(
		listFile,
		videoPaths.map((p) => `file '${p.replace(/\\/g, "/")}'`).join("\n")
	);
	await spawnFfmpeg(
		["-f", "concat", "-safe", "0", "-i", listFile, "-c", "copy", "-y", outPath],
		"concat"
	);
	try {
		fs.unlinkSync(listFile);
	} catch {}
	return outPath;
}

function positionToExpr(position = "topRight") {
	switch (position) {
		case "topLeft":
			return { x: "30", y: "30" };
		case "bottomLeft":
			return { x: "30", y: "main_h-overlay_h-30" };
		case "bottomRight":
			return { x: "main_w-overlay_w-30", y: "main_h-overlay_h-30" };
		case "center":
			return { x: "(main_w-overlay_w)/2", y: "(main_h-overlay_h)/2" };
		case "topRight":
		default:
			return { x: "main_w-overlay_w-30", y: "30" };
	}
}

async function applyOverlays(baseVideoPath, overlays, outPath) {
	if (!overlays.length) {
		fs.copyFileSync(baseVideoPath, outPath);
		return outPath;
	}

	const inputs = ["-i", baseVideoPath];
	const filterParts = [
		"[0:v]scale=1080:1920:force_original_aspect_ratio=decrease," +
			"pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=black,format=yuv420p[base]",
	];
	let lastLabel = "base";

	overlays.forEach((ov, idx) => {
		inputs.push("-i", ov.localPath);
		const ovLabel = `ov${idx + 1}`;
		const scaledLabel = `ovs${idx + 1}`;
		filterParts.push(
			`[${idx + 1}:v]scale=iw*${ov.scale}:ih*${ov.scale}[${scaledLabel}]`
		);
		const pos = positionToExpr(ov.position);
		const outLabel = `v${idx + 1}`;
		filterParts.push(
			`[${lastLabel}][${scaledLabel}]overlay=${pos.x}:${pos.y}:enable='between(t,${ov.startSec},${ov.endSec})'[${outLabel}]`
		);
		lastLabel = outLabel;
	});

	filterParts.push(`[${lastLabel}]format=yuv420p[vout]`);

	await spawnFfmpeg(
		[
			...inputs,
			"-filter_complex",
			filterParts.join(";"),
			"-map",
			"[vout]",
			"-map",
			"0:a?",
			"-c:v",
			"libx264",
			"-preset",
			"medium",
			"-crf",
			"20",
			"-c:a",
			"aac",
			"-shortest",
			"-y",
			outPath,
		],
		"overlay"
	);
	return outPath;
}

async function mixBackgroundMusic(baseVideoPath, musicPath, outPath) {
	const filter =
		"[1:a]volume=0.16[music];" +
		"[0:a][music]sidechaincompress=threshold=0.1:ratio=6:attack=40:release=200:makeup=1.2[aout]";
	await spawnFfmpeg(
		[
			"-i",
			baseVideoPath,
			"-i",
			musicPath,
			"-filter_complex",
			filter,
			"-map",
			"0:v",
			"-map",
			"[aout]",
			"-c:v",
			"copy",
			"-c:a",
			"aac",
			"-shortest",
			"-y",
			outPath,
		],
		"music"
	);
	return outPath;
}

async function padAudioToDuration(audioPath, targetSec, outPath) {
	await spawnFfmpeg(
		[
			"-i",
			audioPath,
			"-filter:a",
			`apad=pad_dur=${targetSec}`,
			"-t",
			String(targetSec),
			"-c:a",
			"mp3",
			"-y",
			outPath,
		],
		"pad-audio"
	);
	return outPath;
}

// YOUTUBE (disabled until enabled in createLongVideo)
function buildYouTubeOAuth2Client(tokens) {
	if (!tokens) return null;
	const o = new google.auth.OAuth2(
		process.env.YOUTUBE_CLIENT_ID,
		process.env.YOUTUBE_CLIENT_SECRET,
		process.env.YOUTUBE_REDIRECT_URI
	);
	o.setCredentials(tokens);
	return o;
}

async function uploadToYouTube(filePath, { title, description, tags }, tokens) {
	const o = buildYouTubeOAuth2Client(tokens);
	if (!o) throw new Error("Missing YouTube OAuth tokens");
	const youtube = google.youtube({ version: "v3", auth: o });
	const { data } = await youtube.videos.insert({
		part: "snippet,status",
		requestBody: {
			snippet: {
				title: title || "Long Video",
				description: description || "",
				tags: Array.isArray(tags) ? tags : [],
			},
			status: { privacyStatus: "private" },
		},
		media: { body: fs.createReadStream(filePath) },
	});
	return data?.id ? `https://www.youtube.com/watch?v=${data.id}` : null;
}

async function splitAudioBySegments(audioPath, segments) {
	const out = [];
	for (const seg of segments) {
		const dur = Math.max(1, seg.endSec - seg.startSec);
		const outPath = path.join(TMP_DIR, `vo_seg_${seg.index}.mp3`);
		await spawnFfmpeg(
			[
				"-i",
				audioPath,
				"-ss",
				String(seg.startSec),
				"-t",
				String(dur),
				"-c:a",
				"mp3",
				"-y",
				outPath,
			],
			"split-audio"
		);
		out.push({ ...seg, audioPath: outPath });
	}
	return out;
}

async function elevenLabsTTS(text, voiceId, outPath) {
	if (!ELEVEN_API_KEY) throw new Error("ELEVENLABS_API_KEY missing");
	if (!voiceId) throw new Error("ElevenLabs voiceId missing");
	const url = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream?output_format=mp3_44100_128`;
	const payload = {
		text,
		model_id: "eleven_multilingual_v2",
		voice_settings: {
			stability: 0.25,
			similarity_boost: 0.9,
			style: 0.7,
			use_speaker_boost: true,
		},
	};
	const res = await withRetry(
		async () =>
			axios.post(url, payload, {
				headers: {
					"xi-api-key": ELEVEN_API_KEY,
					"Content-Type": "application/json",
					accept: "audio/mpeg",
				},
				responseType: "stream",
				timeout: 20000,
				validateStatus: (s) => s < 500,
			}),
		2,
		"elevenlabs"
	);
	if (res.status >= 300)
		throw new Error(`ElevenLabs TTS failed (${res.status})`);
	await new Promise((resolve, reject) => {
		const ws = fs.createWriteStream(outPath);
		res.data.pipe(ws);
		ws.on("finish", resolve);
		ws.on("error", reject);
	});
	return outPath;
}

async function requestSyncSoJob({ presenterImage, audioPath }) {
	if (!SYNC_SO_API_KEY) throw new Error("SYNC_SO_API_KEY missing");
	const form = new FormData();
	const audioBuf = fs.readFileSync(audioPath);
	form.append("audio", new Blob([audioBuf]), "segment.mp3");

	if (isHttpUrl(presenterImage)) {
		form.append("image_url", presenterImage);
	} else {
		const imgBuf = fs.readFileSync(presenterImage);
		form.append("image", new Blob([imgBuf]), "presenter.png");
	}

	const res = await withRetry(
		async () =>
			fetchWithTimeout(
				`${SYNC_SO_BASE}/lipsync`,
				{
					method: "POST",
					headers: {
						Authorization: `Bearer ${SYNC_SO_API_KEY}`,
					},
					body: form,
				},
				20000
			),
		2,
		"syncso-create"
	);
	const data = await res.json().catch(() => ({}));
	if (!res.ok || !data.id)
		throw new Error(
			`Sync.so lipsync creation failed (${res.status}): ${
				data?.message || "unknown error"
			}`
		);
	return data.id;
}

async function pollSyncSoJob(id, label) {
	const url = `${SYNC_SO_BASE}/lipsync/${id}`;
	for (let i = 0; i < 90; i++) {
		await sleep(2000);
		const res = await fetchWithTimeout(
			url,
			{
				headers: { Authorization: `Bearer ${SYNC_SO_API_KEY}` },
			},
			15000
		);
		const data = await res.json().catch(() => ({}));
		if (data.status === "completed" && data.output_url) return data.output_url;
		if (data.status === "failed")
			throw new Error(`${label} failed: ${data.error || "unknown error"}`);
	}
	throw new Error(`${label} timed out`);
}

async function createLipSyncVideo({ presenterImage, audioPath, segIndex }) {
	const id = await requestSyncSoJob({ presenterImage, audioPath });
	const outputUrl = await pollSyncSoJob(id, `lipsync_seg_${segIndex}`);
	return outputUrl;
}

async function pollRunway(id, label) {
	const url = `https://api.dev.runwayml.com/v1/tasks/${id}`;
	for (let i = 0; i < 80; i++) {
		await sleep(2000);
		const res = await axios.get(url, {
			headers: {
				Authorization: `Bearer ${RUNWAY_API_KEY}`,
				"X-Runway-Version": RUNWAY_VERSION,
			},
		});
		const data = res.data || {};
		if (data.status === "SUCCEEDED" && Array.isArray(data.output)) {
			return data.output[0];
		}
		if (data.status === "FAILED") {
			throw new Error(`${label} failed: ${data.failureCode || "FAILED"}`);
		}
	}
	throw new Error(`${label} timed out`);
}

async function runwayImageToVideo({
	imageUrl,
	promptText,
	durationSec,
	ratio,
}) {
	if (!RUNWAY_API_KEY) return null;
	const payload = {
		model: RUNWAY_MODEL,
		promptImage: imageUrl,
		promptText,
		ratio: ratio || "1280:720",
		duration: durationSec || 5,
		promptStrength: 0.5,
		negativePrompt:
			"text, logo, watermark, distorted, uncanny, jitter, glitch, gore",
	};
	const { data } = await axios.post(
		"https://api.dev.runwayml.com/v1/image_to_video",
		payload,
		{
			headers: {
				Authorization: `Bearer ${RUNWAY_API_KEY}`,
				"X-Runway-Version": RUNWAY_VERSION,
			},
			timeout: 20000,
		}
	);
	if (!data?.id) throw new Error("Runway image_to_video returned no id");
	return await pollRunway(data.id, "runway_broll");
}

async function fetchCseContext(topic) {
	if (!GOOGLE_CSE_ID || !GOOGLE_CSE_KEY || !topic) return [];
	const queries = [
		`${topic} movie latest news`,
		`${topic} box office`,
		`${topic} trailer release date`,
	];
	const results = [];
	for (const q of queries) {
		try {
			const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
				params: {
					key: GOOGLE_CSE_KEY,
					cx: GOOGLE_CSE_ID,
					q,
					num: 3,
					safe: "active",
				},
				timeout: 12000,
			});
			const items = Array.isArray(data?.items) ? data.items : [];
			items.forEach((it) => {
				results.push({
					title: String(it.title || "").slice(0, 160),
					snippet: String(it.snippet || "").slice(0, 240),
					link: it.link || it.formattedUrl || "",
				});
			});
		} catch (e) {
			console.warn("[LongVideo] CSE context failed", e.message);
		}
	}
	return results.slice(0, 6);
}

async function fetchCseChartImages(topic) {
	if (!GOOGLE_CSE_ID || !GOOGLE_CSE_KEY || !topic) return [];
	const queries = [
		`${topic} box office chart`,
		`${topic} movie ranking chart`,
		`${topic} ticket sales chart`,
	];
	const out = [];
	for (const q of queries) {
		try {
			const { data } = await axios.get(GOOGLE_CSE_ENDPOINT, {
				params: {
					key: GOOGLE_CSE_KEY,
					cx: GOOGLE_CSE_ID,
					q,
					searchType: "image",
					imgType: "photo",
					num: 5,
					safe: "active",
				},
				timeout: 12000,
			});
			const items = Array.isArray(data?.items) ? data.items : [];
			for (const it of items) {
				if (it.link && /^https?:\/\//i.test(it.link)) out.push(it.link);
				if (out.length >= 6) break;
			}
		} catch (e) {
			console.warn("[LongVideo] CSE chart search failed", e.message);
		}
	}
	return out.slice(0, 6);
}

async function selectTopic({ preferredTopicHint, dryRun }) {
	if (dryRun) {
		return {
			topic: preferredTopicHint || DEFAULT_TOPIC,
			reason: preferredTopicHint
				? "Dry run: using preferredTopicHint"
				: "Dry run: using default evergreen topic",
		};
	}

	if (TMDB_API_KEY) {
		try {
			const { data } = await axios.get(
				"https://api.themoviedb.org/3/trending/movie/week",
				{
					params: { api_key: TMDB_API_KEY },
					timeout: 12000,
				}
			);
			const top = Array.isArray(data?.results) ? data.results[0] : null;
			if (top?.title) {
				return {
					topic: `Why ${top.title} is blowing up right now`,
					reason: "TMDB trending (weekly) top result",
				};
			}
		} catch (e) {
			console.warn("[LongVideo] TMDB trending failed", e.message);
		}
	}

	if (googleTrends) {
		try {
			const raw = await googleTrends.dailyTrends({ geo: "US" });
			const parsed = JSON.parse(raw || "{}");
			const searches =
				parsed?.default?.trendingSearchesDays?.[0]?.trendingSearches || [];
			const queries = searches
				.map((s) => s?.title?.query)
				.filter(Boolean)
				.slice(0, 20);
			if (queries.length && process.env.CHATGPT_API_TOKEN) {
				const ask = `
Pick ONE movie-related topic from this list of trending queries. If none are movie-related, return "none".
List: ${queries.join(" | ")}
Return JSON: { "topic": "<short movie topic or none>" }
`.trim();
				const { choices } = await openai.chat.completions.create({
					model: CHAT_MODEL,
					messages: [{ role: "user", content: ask }],
				});
				const parsedJson = parseJsonFlexible(choices[0].message.content || "");
				if (parsedJson?.topic && parsedJson.topic !== "none") {
					return {
						topic: String(parsedJson.topic).slice(0, 120),
						reason:
							"Google Trends daily queries + OpenAI pick (not real-time guaranteed)",
					};
				}
			}
		} catch (e) {
			console.warn("[LongVideo] Google Trends failed", e.message);
		}
	}

	if (preferredTopicHint) {
		return {
			topic: preferredTopicHint,
			reason: "Fallback to preferredTopicHint",
		};
	}

	return { topic: DEFAULT_TOPIC, reason: "Fallback to evergreen topic" };
}

function estimateWordTarget(targetDurationSec) {
	const min = Math.round(targetDurationSec * 2.3);
	const max = Math.round(targetDurationSec * 2.9);
	return { min, max };
}

function normalizeSegments(rawSegments, targetDurationSec) {
	if (!Array.isArray(rawSegments) || !rawSegments.length) return null;
	const segments = rawSegments
		.map((s, idx) => {
			const startSec =
				Number.isFinite(Number(s.startSec)) && Number(s.startSec) >= 0
					? Number(s.startSec)
					: idx * 18;
			const endSec =
				Number.isFinite(Number(s.endSec)) && Number(s.endSec) > startSec
					? Number(s.endSec)
					: startSec + 18;
			return {
				index:
					Number.isFinite(Number(s.index)) || Number(s.index) === 0
						? Number(s.index)
						: idx,
				startSec,
				endSec,
				text: String(s.text || "").trim(),
				overlayCues: Array.isArray(s.overlayCues) ? s.overlayCues : [],
			};
		})
		.filter((s) => s.text);

	segments.sort((a, b) => a.startSec - b.startSec);

	let lastEnd = 0;
	for (const seg of segments) {
		if (seg.startSec < lastEnd) seg.startSec = lastEnd;
		if (seg.endSec <= seg.startSec) seg.endSec = seg.startSec + 16;
		lastEnd = seg.endSec;
	}

	const total = segments.length ? segments[segments.length - 1].endSec : 0;
	if (total < targetDurationSec - 5) {
		const tail = targetDurationSec - total;
		const last = segments[segments.length - 1];
		last.endSec += tail;
	}
	return segments;
}

async function generateScript({
	topic,
	language,
	targetDurationSec,
	overlayAssets,
	liveContext,
	chartImages,
	preferredTopicHint,
}) {
	if (!process.env.CHATGPT_API_TOKEN)
		throw new Error("CHATGPT_API_TOKEN missing");

	const { min, max } = estimateWordTarget(targetDurationSec);
	const segmentCount = Math.max(1, Math.ceil(targetDurationSec / 18));
	const overlayAssetNote = overlayAssets.length
		? `Overlay assets already provided (use these timings where relevant): ${JSON.stringify(
				overlayAssets.slice(0, 6)
		  )}`
		: "No overlay assets provided. Suggest a few overlayCues with query hints for charts or facts.";

	const contextLines =
		Array.isArray(liveContext) && liveContext.length
			? liveContext
					.map((c) => `- ${c.title}${c.snippet ? " | " + c.snippet : ""}`)
					.join("\n")
			: "- (no live context available)";

	const chartLine = chartImages.length
		? `Chart images found (use only if relevant): ${chartImages
				.slice(0, 4)
				.join(", ")}`
		: "No chart images found.";

	const prompt = `
You are writing a long-form, energetic movie explainer for a talking-head video.
Topic: "${topic}"
Preferred hint (if any): "${preferredTopicHint || ""}"
Language: ${language}
Target duration: ${targetDurationSec}s
Target word count: ${min}-${max} words total.
Segments: ${segmentCount} segments, each 15-20 seconds.

Live context (use as facts, but do NOT claim real-time certainty):
${contextLines}

${chartLine}
${overlayAssetNote}

Rules:
- Keep it fun, attention-grabbing, and SEO-friendly without hype or false promises.
- Hook in the first 10 seconds, then build momentum with clear facts, comparisons, and a light narrative arc.
- Use short, speakable sentences. Avoid tongue twisters and long lists.
- Use natural spoken English. Expand acronyms on first mention.
- Do NOT invent numbers, dates, or quotes. If unsure, say "reports suggest" or "early reports indicate".
- If you reference charts, mention what the chart shows in plain words.

Output JSON ONLY:
{
  "title": "...",
  "topic": "...",
  "segments": [
    { "index": 0, "startSec": 0, "endSec": 18, "text": "...", "overlayCues":[ { "startSec": 6, "endSec": 12, "text": "Box office trend", "query": "box office chart" } ] }
  ]
}
`.trim();

	const { choices } = await openai.chat.completions.create({
		model: CHAT_MODEL,
		messages: [{ role: "user", content: prompt }],
	});

	const parsed = parseJsonFlexible(choices[0].message.content || "");
	if (!parsed || !Array.isArray(parsed.segments)) {
		throw new Error("OpenAI script JSON parse failed");
	}
	return parsed;
}

async function autoOverlayAssetsFromCues(cues, chartImages, targetDurationSec) {
	if (!Array.isArray(cues) || !cues.length || !chartImages.length) return [];
	const out = [];
	let imgIdx = 0;
	for (const cue of cues) {
		if (imgIdx >= chartImages.length) break;
		const startSec = Number(cue.startSec);
		const endSec = Number(cue.endSec);
		if (!Number.isFinite(startSec) || !Number.isFinite(endSec)) continue;
		out.push({
			type: "image",
			url: chartImages[imgIdx],
			startSec: clamp(startSec, 0, targetDurationSec),
			endSec: clamp(endSec, 0, targetDurationSec),
			position: "topRight",
			scale: 0.35,
		});
		imgIdx += 1;
	}
	return out;
}

async function runLongVideoJob(jobId, payload, baseUrl) {
	const tempPaths = [];
	try {
		updateJob(jobId, { status: "running", progressPct: 1 });

		const {
			presenterImageUrl,
			voiceoverUrl,
			overlayAssets,
			preferredTopicHint,
			language,
			targetDurationSec,
			musicUrl,
			dryRun,
		} = payload;

		// TOPIC
		const topicPick = await selectTopic({ preferredTopicHint, dryRun });
		updateJob(jobId, {
			progressPct: 5,
			topic: topicPick.topic,
			meta: { topicReason: topicPick.reason },
		});

		if (dryRun) {
			await sleep(250);
			updateJob(jobId, { progressPct: 15 });
			await sleep(250);
			updateJob(jobId, { progressPct: 35 });
			await sleep(250);
			updateJob(jobId, { progressPct: 60 });
			await sleep(250);
			updateJob(jobId, { progressPct: 85 });
			const dummyUrl = `${baseUrl}/uploads/videos/long_${jobId}_dryrun.mp4`;
			updateJob(jobId, {
				status: "completed",
				progressPct: 100,
				finalVideoUrl: dummyUrl,
			});
			return;
		}

		const presenterImage = pickPresenterImage(presenterImageUrl);
		if (!presenterImage)
			throw new Error(
				"Presenter image missing. Provide presenterImageUrl or add a default image in PhotosForLongerVideos."
			);

		const liveContext = await fetchCseContext(topicPick.topic);
		const chartImages = await fetchCseChartImages(topicPick.topic);

		// SCRIPT
		const script = await generateScript({
			topic: topicPick.topic,
			language,
			targetDurationSec,
			overlayAssets,
			liveContext,
			chartImages,
			preferredTopicHint,
		});
		const normalized = normalizeSegments(script.segments, targetDurationSec);
		if (!normalized || !normalized.length)
			throw new Error("Script segments invalid");
		const currentMeta = JOBS.get(jobId)?.meta || {};
		updateJob(jobId, {
			progressPct: 15,
			meta: { ...currentMeta, ...script, liveContext },
		});

		const cueList = normalized.flatMap((s) => s.overlayCues || []);
		let brollOverlay = null;
		if (RUNWAY_API_KEY && chartImages.length) {
			try {
				const brollUrl = await runwayImageToVideo({
					imageUrl: chartImages[0],
					promptText:
						"Subtle camera push on the attached chart, keep text unchanged, smooth motion, clean documentary look",
					durationSec: 5,
					ratio: "1280:720",
				});
				const brollPath = path.join(TMP_DIR, `broll_${jobId}.mp4`);
				await downloadToFile(brollUrl, brollPath);
				tempPaths.push(brollPath);
				const cue = cueList[0] || {};
				const startSec = Number.isFinite(Number(cue.startSec))
					? Number(cue.startSec)
					: 12;
				const endSec = Number.isFinite(Number(cue.endSec))
					? Number(cue.endSec)
					: startSec + 5;
				brollOverlay = {
					type: "video",
					localPath: brollPath,
					startSec,
					endSec,
					position: "center",
					scale: 0.6,
				};
			} catch (e) {
				console.warn("[LongVideo] Runway b-roll skipped", e.message);
			}
		}

		// TTS
		let segmentsWithAudio = normalized;
		const voiceId = ELEVEN_VOICE_ID || "21m00Tcm4TlvDq8ikWAM";
		if (voiceoverUrl) {
			const voicePath = path.join(TMP_DIR, `voice_${jobId}.mp3`);
			tempPaths.push(voicePath);
			await downloadToFile(voiceoverUrl, voicePath);
			segmentsWithAudio = await splitAudioBySegments(voicePath, normalized);
			segmentsWithAudio.forEach((s) => {
				if (s.audioPath) tempPaths.push(s.audioPath);
			});
		} else {
			if (!ELEVEN_API_KEY)
				throw new Error(
					"ELEVENLABS_API_KEY missing and no voiceoverUrl provided"
				);
			const audioSegs = [];
			for (const seg of normalized) {
				const audioPath = path.join(TMP_DIR, `tts_${jobId}_${seg.index}.mp3`);
				await elevenLabsTTS(seg.text, voiceId, audioPath);
				const padded = path.join(TMP_DIR, `tts_pad_${jobId}_${seg.index}.mp3`);
				await padAudioToDuration(
					audioPath,
					Math.max(1, seg.endSec - seg.startSec),
					padded
				);
				tempPaths.push(audioPath, padded);
				audioSegs.push({ ...seg, audioPath: padded });
			}
			segmentsWithAudio = audioSegs;
		}
		updateJob(jobId, { progressPct: 35 });

		// LIPSYNC
		if (!SYNC_SO_API_KEY)
			throw new Error(
				"SYNC_SO_API_KEY missing. Set it or use dryRun=true for testing."
			);
		const segmentVideos = [];
		for (const seg of segmentsWithAudio) {
			const url = await createLipSyncVideo({
				presenterImage,
				audioPath: seg.audioPath,
				segIndex: seg.index,
			});
			const localPath = path.join(TMP_DIR, `lip_${jobId}_${seg.index}.mp4`);
			await downloadToFile(url, localPath);
			tempPaths.push(localPath);
			segmentVideos.push(localPath);
		}
		updateJob(jobId, { progressPct: 60 });

		// FFMPEG
		const baseConcat = path.join(TMP_DIR, `base_${jobId}.mp4`);
		await concatVideos(segmentVideos, baseConcat);
		tempPaths.push(baseConcat);

		const normalizedOverlays = normalizeOverlayAssets(
			overlayAssets,
			targetDurationSec
		);
		let autoOverlays = [];
		if (!normalizedOverlays.length) {
			autoOverlays = await autoOverlayAssetsFromCues(
				cueList,
				chartImages,
				targetDurationSec
			);
		}
		let overlaysToUse = normalizedOverlays.length
			? normalizedOverlays
			: autoOverlays;
		if (brollOverlay) overlaysToUse = [...overlaysToUse, brollOverlay];

		const overlayPaths = [];
		for (let i = 0; i < overlaysToUse.length; i++) {
			const ov = overlaysToUse[i];
			if (ov.localPath && fs.existsSync(ov.localPath)) {
				overlayPaths.push({ ...ov, localPath: ov.localPath });
				continue;
			}
			if (!ov.url) continue;
			const localPath = path.join(
				TMP_DIR,
				`ov_${jobId}_${i + 1}.${ov.type === "video" ? "mp4" : "png"}`
			);
			await downloadToFile(ov.url, localPath);
			overlayPaths.push({ ...ov, localPath });
			tempPaths.push(localPath);
		}

		const withOverlays = path.join(TMP_DIR, `overlay_${jobId}.mp4`);
		await applyOverlays(baseConcat, overlayPaths, withOverlays);
		tempPaths.push(withOverlays);

		let finalPath = withOverlays;
		if (musicUrl) {
			const musicPath = path.join(TMP_DIR, `music_${jobId}.mp3`);
			await downloadToFile(musicUrl, musicPath);
			tempPaths.push(musicPath);
			const mixed = path.join(TMP_DIR, `mixed_${jobId}.mp4`);
			await mixBackgroundMusic(withOverlays, musicPath, mixed);
			finalPath = mixed;
			tempPaths.push(mixed);
		}

		const outputName = `long_${jobId}.mp4`;
		const outputPath = path.join(OUTPUT_DIR, outputName);
		fs.copyFileSync(finalPath, outputPath);

		updateJob(jobId, {
			progressPct: 85,
		});

		// NOTE: ensure /uploads/videos is served statically by the API or reverse proxy.
		const finalVideoUrl = `${baseUrl}/uploads/videos/${outputName}`;
		updateJob(jobId, {
			status: "completed",
			progressPct: 100,
			finalVideoUrl,
		});

		// YouTube upload support (disabled until tested)
		/*
		try {
			const youtubeUrl = await uploadToYouTube(outputPath, {
				title: script.title || topicPick.topic,
				description: script.title || topicPick.topic,
				tags: ["movies", "explainer", "short documentary"],
			});
			updateJob(jobId, { meta: { ...script, youtubeUrl } });
		} catch (e) {
			console.warn("[LongVideo] YouTube upload skipped/failed", e.message);
		}
		*/
	} catch (err) {
		updateJob(jobId, {
			status: "failed",
			error: err?.message || "Long video job failed",
		});
	} finally {
		for (const p of tempPaths) {
			try {
				if (fs.existsSync(p)) fs.unlinkSync(p);
			} catch {}
		}
	}
}

// VALIDATION
function validateCreateBody(body = {}) {
	const errors = [];
	const targetDurationSec = Number(body.targetDurationSec || 180);
	const duration = ALLOWED_DURATIONS.has(targetDurationSec)
		? targetDurationSec
		: clamp(targetDurationSec, 20, 300);
	if (!Number.isFinite(duration))
		errors.push("targetDurationSec must be number");

	const overlayAssets = normalizeOverlayAssets(
		body.overlayAssets || [],
		duration
	);
	return {
		errors,
		clean: {
			presenterImageUrl: body.presenterImageUrl || "",
			voiceoverUrl: body.voiceoverUrl || "",
			overlayAssets,
			preferredTopicHint: String(body.preferredTopicHint || "").trim(),
			language: normalizeLanguage(body.language || "en"),
			targetDurationSec: duration,
			musicUrl: body.musicUrl || "",
			dryRun: Boolean(body.dryRun),
		},
	};
}

// STORAGE
function buildBaseUrl(req) {
	return (
		process.env.BASE_URL || `${req.protocol || "http"}://${req.get("host")}`
	);
}

function parseTimeOfDay(timeOfDay) {
	const raw = String(timeOfDay || "").trim();
	const m = raw.match(/^(\d{1,2}):(\d{2})$/);
	if (!m) return null;
	const hh = Number(m[1]);
	const mm = Number(m[2]);
	if (!Number.isInteger(hh) || !Number.isInteger(mm)) return null;
	if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
	return { hh, mm };
}

function computeNextRun({ scheduleType, timeOfDay, startDate }) {
	const t = parseTimeOfDay(timeOfDay);
	if (!t) return null;
	let next = dayjs(startDate).hour(t.hh).minute(t.mm).second(0);
	if (next.isBefore(dayjs())) {
		if (scheduleType === "daily") next = next.add(1, "day");
		else if (scheduleType === "weekly") next = next.add(1, "week");
		else if (scheduleType === "monthly") next = next.add(1, "month");
	}
	return next.toDate();
}

// CONTROLLER: createLongVideo
exports.createLongVideo = async (req, res) => {
	const { errors, clean } = validateCreateBody(req.body || {});
	if (errors.length) return res.status(400).json({ error: errors.join(", ") });

	const jobId = crypto.randomUUID();
	const baseUrl = buildBaseUrl(req);
	const job = {
		jobId,
		status: "queued",
		progressPct: 0,
		topic: null,
		finalVideoUrl: null,
		error: null,
		createdAt: nowIso(),
		updatedAt: nowIso(),
		meta: {},
	};
	JOBS.set(jobId, job);

	res.status(202).json({
		jobId,
		status: "queued",
		statusUrl: `/api/long-video/${jobId}`,
	});

	// Optional scheduling (create schedule record when requested)
	const schedule = req.body?.schedule || null;
	const scheduleJobMeta =
		req.scheduleJobMeta || req.body?.scheduleJobMeta || null;
	const isScheduledJob = Boolean(scheduleJobMeta);

	if (schedule && !isScheduledJob && req.user?._id) {
		const { type, timeOfDay, startDate, endDate } = schedule;
		if (!["daily", "weekly", "monthly"].includes(String(type || ""))) {
			console.warn(
				"[LongVideo] Invalid schedule type; skipping schedule save."
			);
		} else if (!parseTimeOfDay(timeOfDay) || !startDate) {
			console.warn(
				"[LongVideo] Invalid schedule timing; skipping schedule save."
			);
		} else {
			const nextRun = computeNextRun({
				scheduleType: type,
				timeOfDay,
				startDate,
			});
			if (nextRun) {
				try {
					await Schedule.create({
						user: req.user._id,
						category: "LongVideo",
						scheduleType: type,
						timeOfDay,
						startDate: dayjs(startDate).toDate(),
						endDate: endDate ? dayjs(endDate).toDate() : undefined,
						nextRun,
						active: true,
						videoType: "long",
						longVideoConfig: {
							...clean,
						},
					});
				} catch (e) {
					console.warn("[LongVideo] Schedule creation failed", e.message);
				}
			}
		}
	}

	setImmediate(() => runLongVideoJob(jobId, clean, baseUrl));
};

// CONTROLLER: getLongVideoStatus
exports.getLongVideoStatus = async (req, res) => {
	const { jobId } = req.params;
	const job = JOBS.get(jobId);
	if (!job) return res.status(404).json({ error: "Job not found" });
	return res.json({
		jobId: job.jobId,
		status: job.status,
		progressPct: job.progressPct,
		topic: job.topic || null,
		finalVideoUrl: job.finalVideoUrl || null,
		error: job.error || null,
	});
};
