/* server.js — production-hardened, queued-cron, PST-aware
/* eslint-disable no-console */

const path = require("path");
const crypto = require("crypto");

// Always load .env from the same directory as this file (works reliably with PM2)
require("dotenv").config({ path: path.join(__dirname, ".env") });

const express = require("express");
const mongoose = require("mongoose");
const morgan = require("morgan");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const { readdirSync } = require("fs");
const http = require("http");
const net = require("net");
const socketIo = require("socket.io");
const cron = require("node-cron");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const tz = require("dayjs/plugin/timezone");
dayjs.extend(utc);
dayjs.extend(tz);

/* ---------- Models & Controllers ---------- */
const Schedule = require("./models/Schedule");
const Video = require("./models/Video");
const User = require("./models/User");
const Chat = require("./models/Chat");
const jwt = require("jsonwebtoken");
const { createVideo } = require("./controllers/videoController");
const {
	createLongVideo,
	getLongVideoRuntimeProfile,
} = require("./controllers/videoControllerLonger");
const {
	processPendingShortUploads,
} = require("./controllers/shortsGeneratorFromLongs");
const {
	rejectUnsafeMongoKeys,
} = require("./middlewares/securityMiddleware");
const {
	startGeneratedFilesSweeper,
} = require("./utils/generatedFiles");

/* ---------- Middleware ---------- */
const { protect } = require("./middlewares/authMiddleware");
const PST_TZ = "America/Los_Angeles";

/* ---------- Environment normalization ---------- */
if (!process.env.NODE_ENV && process.env.ENVIRONMENT) {
	// map ENVIRONMENT=PRODUCTION -> NODE_ENV=production
	const env = String(process.env.ENVIRONMENT).toLowerCase();
	process.env.NODE_ENV = env === "production" ? "production" : env;
}
const NODE_ENV = process.env.NODE_ENV || "development";
process.env.NODE_ENV = NODE_ENV;
const IS_PRODUCTION = NODE_ENV === "production";
const LOG_STARTUP_DETAILS = ["1", "true", "yes", "on"].includes(
	String(process.env.LOG_STARTUP_DETAILS || "")
		.trim()
		.toLowerCase(),
);

/* ---------- Small helpers ---------- */
function toInt(v, fallback) {
	const n = Number(v);
	return Number.isFinite(n) ? n : fallback;
}

function normalizeOriginList(raw) {
	const v = String(raw || "").trim();
	if (!v) return [];
	if (v === "*") return ["*"];
	// allow comma-separated list
	return v
		.split(",")
		.map((s) => normalizeOrigin(s))
		.filter(Boolean);
}

function normalizeOrigin(raw) {
	const value = String(raw || "").trim().replace(/\/+$/, "");
	if (!value || value === "*") return value;
	try {
		const parsed = new URL(value);
		return `${parsed.protocol}//${parsed.host}`;
	} catch {
		return value;
	}
}

function isEnabled(value, fallback = false) {
	if (value === undefined || value === null || value === "") return fallback;
	return ["1", "true", "yes", "on"].includes(
		String(value).trim().toLowerCase(),
	);
}

function isDisabled(value, fallback = false) {
	if (value === undefined || value === null || value === "") return fallback;
	return ["0", "false", "no", "off"].includes(
		String(value).trim().toLowerCase(),
	);
}

function validateSecurityEnvironment() {
	const jwtSecret = String(process.env.JWT_SECRET || "");
	if (!jwtSecret) {
		if (IS_PRODUCTION) {
			throw new Error("Missing JWT_SECRET.");
		}
		process.env.JWT_SECRET = "agentai-local-development-secret-change-me";
		console.warn("[Startup] Using local development JWT_SECRET fallback.");
	}
	if (IS_PRODUCTION && jwtSecret.length < 32) {
		throw new Error("JWT_SECRET must be at least 32 characters in production.");
	}

	if (IS_PRODUCTION && ALLOW_ALL_ORIGINS) {
		throw new Error(
			"CLIENT_ORIGIN or CLIENT_URL must be set to your exact production origin; wildcard CORS is blocked in production.",
		);
	}
	if (IS_PRODUCTION && ALLOWED_ORIGINS.length === 0) {
		throw new Error("CLIENT_ORIGIN or CLIENT_URL is required in production.");
	}

	const masterPassword = String(process.env.MASTER_PASSWORD || "");
	if (
		IS_PRODUCTION &&
		isEnabled(process.env.ALLOW_MASTER_PASSWORD, false) &&
		masterPassword.length < 20
	) {
		throw new Error(
			"Production MASTER_PASSWORD is enabled but shorter than 20 characters.",
		);
	}
}

const defaultOriginSource = IS_PRODUCTION
	? process.env.CLIENT_ORIGIN || process.env.CLIENT_URL || ""
	: process.env.CLIENT_ORIGIN || "*";
const ALLOWED_ORIGINS = normalizeOriginList(defaultOriginSource);
const ALLOW_ALL_ORIGINS = ALLOWED_ORIGINS.includes("*");
validateSecurityEnvironment();

function isOriginAllowed(origin) {
	if (ALLOW_ALL_ORIGINS) return true;
	return ALLOWED_ORIGINS.includes(normalizeOrigin(origin));
}

/* ---------- Express + HTTP + Socket.IO ---------- */
const app = express();
app.disable("x-powered-by");

// behind Nginx (so req.ip, secure cookies, etc. behave correctly)
app.set("trust proxy", 1);

app.use(
	helmet({
		contentSecurityPolicy: false,
		crossOriginEmbedderPolicy: false,
		crossOriginResourcePolicy: { policy: "cross-origin" },
		hsts: IS_PRODUCTION
			? { maxAge: 15552000, includeSubDomains: true, preload: false }
			: false,
	}),
);

const apiRateLimiter = rateLimit({
	windowMs: toInt(process.env.RATE_LIMIT_WINDOW_MS, 15 * 60 * 1000),
	limit: toInt(process.env.RATE_LIMIT_MAX, IS_PRODUCTION ? 600 : 3000),
	standardHeaders: true,
	legacyHeaders: false,
	message: { error: "Too many requests. Please try again shortly." },
});

const authRateLimiter = rateLimit({
	windowMs: toInt(process.env.AUTH_RATE_LIMIT_WINDOW_MS, 15 * 60 * 1000),
	limit: toInt(process.env.AUTH_RATE_LIMIT_MAX, IS_PRODUCTION ? 20 : 100),
	standardHeaders: true,
	legacyHeaders: false,
	message: { error: "Too many authentication attempts. Please wait and retry." },
});

const server = http.createServer(app);

// Node/HTTP timeouts — safer for long-running API calls (video generation, etc.)
server.keepAliveTimeout = toInt(process.env.KEEP_ALIVE_TIMEOUT_MS, 65000);
server.headersTimeout = toInt(process.env.HEADERS_TIMEOUT_MS, 66000);
// Disable request timeout so long jobs don’t get killed mid-request
server.requestTimeout = 0;

// Socket.IO CORS: if you use credentials, wildcard "*" cannot be used by browsers.
// We reflect/allow based on CLIENT_ORIGIN list.
const io = socketIo(server, {
	cors: {
		origin: (origin, cb) => {
			// origin may be undefined for non-browser clients
			if (!origin) return cb(null, true);
			if (isOriginAllowed(origin)) return cb(null, true);
			return cb(new Error(`Socket.IO CORS blocked origin: ${origin}`), false);
		},
		methods: ["GET", "POST"],
		allowedHeaders: ["Authorization", "Content-Type"],
		credentials: true,
	},
	// Optional: allow websocket upgrades cleanly
	allowEIO3: true,
});
app.set("io", io);

/* ---------- Global middleware ---------- */
const LOG_FORMAT =
	process.env.LOG_FORMAT || (NODE_ENV === "production" ? "combined" : "dev");
app.use(morgan(LOG_FORMAT));

const BODY_LIMIT = process.env.BODY_LIMIT || "50mb";
app.use(express.json({ limit: BODY_LIMIT }));
app.use(express.urlencoded({ extended: true, limit: BODY_LIMIT }));
app.use("/api", apiRateLimiter);
app.use(
	[
		"/api/auth/login",
		"/api/auth/register",
		"/api/auth/forgot-password",
		"/api/auth/reset-password",
	],
	authRateLimiter,
);
app.use(rejectUnsafeMongoKeys);

const UPLOADS_ROOT = path.join(__dirname, "uploads");
const PUBLIC_UPLOAD_EXTENSIONS = new Set([
	".jpg",
	".jpeg",
	".png",
	".gif",
	".webp",
	".mp4",
	".webm",
	".mov",
	".mp3",
	".wav",
	".m4a",
]);

function guardPublicUploads(req, res, next) {
	let pathname = "";
	try {
		pathname = decodeURIComponent(new URL(req.originalUrl, "http://x").pathname);
	} catch {
		return res.status(400).json({ error: "Invalid upload path." });
	}
	const baseName = path.basename(pathname);
	const ext = path.extname(baseName).toLowerCase();
	if (!ext || baseName.startsWith(".") || !PUBLIC_UPLOAD_EXTENSIONS.has(ext)) {
		return res.status(404).end();
	}
	return next();
}

// Serve only non-executable generated media. Job state, markers, scripts, and
// dotfiles are intentionally not public even if they live under uploads/.
app.use(
	"/uploads",
	guardPublicUploads,
	express.static(UPLOADS_ROOT, {
		dotfiles: "deny",
		index: false,
		fallthrough: false,
		setHeaders(res) {
			res.setHeader("X-Content-Type-Options", "nosniff");
			res.setHeader("Cache-Control", "public, max-age=3600");
		},
	}),
);

// Express CORS (same rules as Socket.IO)
app.use(
	cors({
		origin: (origin, cb) => {
			if (!origin) return cb(null, true);
			if (isOriginAllowed(origin)) return cb(null, true);
			return cb(new Error(`CORS blocked origin: ${origin}`));
		},
		methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
		allowedHeaders: ["Content-Type", "Authorization"],
		credentials: true,
	}),
);

/* ---------- Health endpoints ---------- */
// Local-only quick test (direct port)
app.get("/", (req, res) => res.send("Hello from AgentAI API"));

// Public test through Nginx: https://yourdomain.com/api/health
app.get("/api/health", (req, res) => {
	const dbState = mongoose.connection.readyState; // 0=disconnected,1=connected,2=connecting,3=disconnecting
	res.json({
		ok: true,
		env: NODE_ENV,
		uptimeSec: Math.round(process.uptime()),
		dbReadyState: dbState,
		longVideoController:
			typeof getLongVideoRuntimeProfile === "function"
				? getLongVideoRuntimeProfile()
				: null,
		timestamp: new Date().toISOString(),
	});
});

/* ---------- Protected YouTube routes ---------- */
const youTubeAuthRoutes = require("./routes/youtubeAuth");
const youTubeTokensRoutes = require("./routes/youtubeTokens");
const youtubeExchangeRoutes = require("./routes/youtubeExchange");

app.use("/api/youtube", protect, youTubeAuthRoutes);
app.use("/api/youtube", protect, youTubeTokensRoutes);
app.use("/api/youtube", protect, youtubeExchangeRoutes);

/* ---------- All other /api routes ---------- */
const routesDir = path.join(__dirname, "routes");
readdirSync(routesDir)
	.filter((file) => file.endsWith(".js"))
	.forEach((file) => {
		// skip the three explicit files above
		if (/^youtube(A|T|E)/.test(file)) return;
		app.use("/api", require(path.join(routesDir, file)));
	});

/* ---------- Queue + cron (concurrency=1, retry backoff on failure) ---------- */
const jobQueue = [];
const queuedIds = new Set();
let processing = false;

const FAIL_BACKOFF_MINUTES = toInt(
	process.env.SCHEDULE_FAIL_BACKOFF_MINUTES,
	10,
);
const MAX_SCHEDULE_FAILURES = Math.max(
	1,
	toInt(process.env.SCHEDULE_MAX_FAILURES, 3),
);
const SHORTS_UPLOAD_BATCH_MIN = toInt(process.env.SHORTS_UPLOAD_BATCH_MIN, 3);
const SHORTS_UPLOAD_BATCH_MAX = toInt(process.env.SHORTS_UPLOAD_BATCH_MAX, 6);
const SHORTS_CRON_TZ = process.env.SHORTS_CRON_TZ || PST_TZ;
const SHORTS_CRON_SCHEDULE = process.env.SHORTS_CRON_SCHEDULE || "0 */2 * * *";
const SCHEDULE_RUN_HISTORY_MAX = Math.max(
	1,
	toInt(process.env.SCHEDULE_RUN_HISTORY_MAX, 20),
);
const SCHEDULE_QUEUE_STALL_WARN_MINUTES = Math.max(
	1,
	toInt(process.env.SCHEDULE_QUEUE_STALL_WARN_MINUTES, 20),
);
const ENABLE_SCHEDULER = !isDisabled(process.env.ENABLE_SCHEDULER, false);

let activeQueueJob = null;

function clipRunReason(v) {
	const msg = String(v || "").trim();
	return msg ? msg.slice(0, 500) : "";
}

function buildScheduleRunEntry({
	runId,
	status,
	startedAt,
	finishedAt,
	reason,
	nextRun,
	failCount,
}) {
	const started =
		startedAt instanceof Date && !Number.isNaN(startedAt.getTime())
			? startedAt
			: null;
	const finished =
		finishedAt instanceof Date && !Number.isNaN(finishedAt.getTime())
			? finishedAt
			: new Date();
	return {
		runId: String(runId || ""),
		status,
		startedAt: started || finished,
		finishedAt: finished,
		durationMs:
			started && finished
				? Math.max(0, finished.getTime() - started.getTime())
				: 0,
		reason: clipRunReason(reason),
		nextRun:
			nextRun instanceof Date && !Number.isNaN(nextRun.getTime())
				? nextRun
				: null,
		failCount:
			Number.isFinite(Number(failCount)) && Number(failCount) >= 0
				? Math.floor(Number(failCount))
				: 0,
	};
}

function appendScheduleRunEntry(sched, entry) {
	const history = Array.isArray(sched.runHistory) ? [...sched.runHistory] : [];
	history.push(entry);
	if (history.length > SCHEDULE_RUN_HISTORY_MAX) {
		history.splice(0, history.length - SCHEDULE_RUN_HISTORY_MAX);
	}
	sched.lastRun = entry;
	sched.runHistory = history;
}

async function markScheduleRunStarted(sched, runContext) {
	const startedEntry = buildScheduleRunEntry({
		runId: runContext.runId,
		status: "started",
		startedAt: runContext.startedAt,
		finishedAt: runContext.startedAt,
		reason: "started",
		nextRun: sched.nextRun,
		failCount: getScheduleFailCount(sched),
	});
	sched.lastRun = startedEntry;
	await sched.save();
}

function getControllerErrorText(payload) {
	if (!payload) return "";
	if (typeof payload === "string") return payload.slice(0, 500);
	if (typeof payload.error === "string") return payload.error.slice(0, 500);
	if (typeof payload.message === "string") return payload.message.slice(0, 500);
	try {
		return JSON.stringify(payload).slice(0, 500);
	} catch {
		return "";
	}
}

async function processQueue() {
	if (processing) return;
	processing = true;
	console.log(`[Queue] Worker started; queue size ${jobQueue.length}`);

	try {
		while (jobQueue.length) {
			const sched = jobQueue.shift();
			const scheduleId = String(sched?._id || "");
			if (!scheduleId) {
				console.warn("[Queue] Dropping invalid queued job payload");
				continue;
			}
			activeQueueJob = { scheduleId, startedAt: Date.now() };
			try {
				console.log(
					`[Queue] Starting schedule ${scheduleId}; remaining queue ${jobQueue.length}`,
				);
				await handleSchedule(sched);
			} catch (err) {
				console.error(
					`[Queue] job error for schedule ${scheduleId}:`,
					err && err.message ? err.message : err,
				);
			} finally {
				queuedIds.delete(scheduleId);
				activeQueueJob = null;
			}
		}
	} catch (err) {
		console.error("[Queue] fatal loop error:", err?.message || err);
	} finally {
		processing = false;
		console.log(`[Queue] Worker idle; queue size ${jobQueue.length}`);
		if (jobQueue.length) {
			setImmediate(() => {
				void processQueue();
			});
		}
	}
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

function normalizeScheduleType(t) {
	const v = String(t || "")
		.toLowerCase()
		.trim();
	if (v === "daily" || v === "weekly" || v === "monthly") return v;
	return "daily";
}

function getScheduleFailCount(sched) {
	const n = Number(sched?.failCount);
	return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
}

function computeNextRunForSchedule({
	scheduleType,
	timeOfDay,
	baseMoment,
	nowMoment,
}) {
	const t = parseTimeOfDay(timeOfDay);
	if (!t) return null;

	let base = baseMoment && baseMoment.isValid() ? baseMoment : null;
	if (!base) base = nowMoment;

	if (scheduleType === "daily") base = base.add(1, "day");
	else if (scheduleType === "weekly") base = base.add(1, "week");
	else if (scheduleType === "monthly") base = base.add(1, "month");

	let next = base.hour(t.hh).minute(t.mm).second(0).millisecond(0);
	if (next.isBefore(nowMoment)) {
		if (scheduleType === "daily") next = next.add(1, "day");
		else if (scheduleType === "weekly") next = next.add(1, "week");
		else if (scheduleType === "monthly") next = next.add(1, "month");
	}
	return next;
}

async function applyScheduleFailureBackoff(
	sched,
	{ nowPST, scheduleType, reason, err, runContext } = {},
) {
	const currentFails = getScheduleFailCount(sched);
	const nextFails = currentFails + 1;
	const msg = String(reason || err?.message || err || "").trim();

	sched.failCount = nextFails;
	sched.lastFailAt = nowPST.toDate();
	if (msg) sched.lastFailReason = msg.slice(0, 500);

	if (nextFails >= MAX_SCHEDULE_FAILURES) {
		const next = computeNextRunForSchedule({
			scheduleType,
			timeOfDay: sched.timeOfDay,
			baseMoment: nowPST,
			nowMoment: nowPST,
		});
		sched.nextRun = next ? next.toDate() : nowPST.add(1, "day").toDate();
		sched.failCount = 0;
		appendScheduleRunEntry(
			sched,
			buildScheduleRunEntry({
				runId: runContext?.runId,
				status: "failed",
				startedAt: runContext?.startedAt,
				reason: `${msg || "schedule run failed"} (failure limit reached)`,
				nextRun: sched.nextRun,
				failCount: nextFails,
			}),
		);
		await sched.save();
		console.log(
			`[Queue] Failure limit reached (${MAX_SCHEDULE_FAILURES}) for schedule ${
				sched._id
			}; nextRun ${dayjs(sched.nextRun).tz(PST_TZ).format()} PST`,
		);
		return { deferred: true };
	}

	sched.nextRun = nowPST.add(FAIL_BACKOFF_MINUTES, "minute").toDate();
	appendScheduleRunEntry(
		sched,
		buildScheduleRunEntry({
			runId: runContext?.runId,
			status: "failed",
			startedAt: runContext?.startedAt,
			reason: msg || "schedule run failed",
			nextRun: sched.nextRun,
			failCount: nextFails,
		}),
	);
	await sched.save();
	console.log(
		`[Queue] Backoff attempt ${nextFails}/${MAX_SCHEDULE_FAILURES} for schedule ${
			sched._id
		}; retry at ${dayjs(sched.nextRun).tz(PST_TZ).format()} PST`,
	);
	return { deferred: false };
}

async function handleSchedule(sched) {
	const nowPST = dayjs().tz(PST_TZ);
	const runContext = {
		runId: crypto.randomUUID(),
		startedAt: new Date(),
	};
	try {
		await markScheduleRunStarted(sched, runContext);
	} catch (err) {
		console.warn(
			`[Queue] Failed to persist run start for schedule ${sched._id}:`,
			err?.message || err,
		);
	}

	// Basic validation to prevent “bad data” infinite loops
	const t = parseTimeOfDay(sched.timeOfDay);
	if (!t) {
		console.error(
			`[Queue] Invalid timeOfDay for schedule ${sched._id}:`,
			sched.timeOfDay,
		);
		sched.active = false;
		appendScheduleRunEntry(
			sched,
			buildScheduleRunEntry({
				runId: runContext.runId,
				status: "failed",
				startedAt: runContext.startedAt,
				reason: `invalid timeOfDay: ${sched.timeOfDay}`,
				nextRun: sched.nextRun,
				failCount: getScheduleFailCount(sched),
			}),
		);
		await sched.save();
		return;
	}
	const scheduleType = normalizeScheduleType(sched.scheduleType);

	/* 1) stop expired schedules */
	let endPST = null;
	if (sched.endDate) {
		const endDateStr = dayjs(sched.endDate).format("YYYY-MM-DD");
		endPST = dayjs.tz(`${endDateStr} 23:59:59`, "YYYY-MM-DD HH:mm:ss", PST_TZ);
		if (nowPST.isAfter(endPST)) {
			sched.active = false;
			appendScheduleRunEntry(
				sched,
				buildScheduleRunEntry({
					runId: runContext.runId,
					status: "skipped",
					startedAt: runContext.startedAt,
					reason: "schedule expired by endDate",
					nextRun: sched.nextRun,
					failCount: getScheduleFailCount(sched),
				}),
			);
			await sched.save();
			console.log(`[Queue] Schedule ${sched._id} expired & deactivated`);
			return;
		}
	}

	const { user, video } = sched;

	const normalizeScheduleValue = (value = "") => String(value || "").trim();
	const scheduleCategory = normalizeScheduleValue(sched.category);
	if (!scheduleCategory) {
		console.error(
			`[Queue] Schedule ${sched._id} missing category; delaying with backoff`,
		);
		await applyScheduleFailureBackoff(sched, {
			nowPST,
			scheduleType,
			reason: "missing category",
			runContext,
		});
		return;
	}

	const resolveVideoDoc = async (candidate) => {
		if (!candidate) return null;
		if (typeof candidate === "object") {
			const hasCategory = Boolean(candidate.category);
			const hasDuration = Number.isFinite(Number(candidate.duration));
			if (hasCategory && hasDuration) return candidate;
		}
		const id = candidate._id || candidate.id || candidate;
		if (!id) return candidate;
		try {
			return await Video.findById(id).lean();
		} catch {
			return candidate;
		}
	};

	/* 2) resolve base video seed */
	const initialVideo = await resolveVideoDoc(video || null);
	let latestVideo = null;
	if (Array.isArray(sched.videos) && sched.videos.length) {
		latestVideo = await resolveVideoDoc(sched.videos[sched.videos.length - 1]);
	}
	const seedVideo = latestVideo || initialVideo || null;

	let resolvedCategory = scheduleCategory;

	const isLongSchedule =
		String(sched.videoType || "").toLowerCase() === "long" ||
		String(resolvedCategory || "").toLowerCase() === "longvideo";

	const longConfig = isLongSchedule ? sched.longVideoConfig || {} : null;
	if (isLongSchedule && longConfig?.category) {
		resolvedCategory = longConfig.category;
	}

	if (!isLongSchedule) {
		const normBaseCategory = normalizeScheduleValue(initialVideo?.category);
		if (normBaseCategory && normBaseCategory !== resolvedCategory) {
			console.error(
				`[Queue] Schedule ${sched._id} category mismatch (schedule=${resolvedCategory}, seed=${normBaseCategory})`,
			);
			await applyScheduleFailureBackoff(sched, {
				nowPST,
				scheduleType,
				reason: "category mismatch with seed video",
				runContext,
			});
			return;
		}

		const normLatestCategory = normalizeScheduleValue(latestVideo?.category);
		if (normLatestCategory && normLatestCategory !== resolvedCategory) {
			console.error(
				`[Queue] Schedule ${sched._id} category mismatch (schedule=${resolvedCategory}, latest=${normLatestCategory})`,
			);
			await applyScheduleFailureBackoff(sched, {
				nowPST,
				scheduleType,
				reason: "category mismatch with latest schedule video",
				runContext,
			});
			return;
		}

		if (!seedVideo) {
			console.error(
				`[Queue] Schedule ${sched._id} missing seed video; delaying with backoff`,
			);
			await applyScheduleFailureBackoff(sched, {
				nowPST,
				scheduleType,
				reason: "missing seed video",
				runContext,
			});
			return;
		}
	}

	const normalizeDuration = (value) => {
		const n = Number(value);
		return Number.isFinite(n) && n > 0 ? n : null;
	};
	const initialDuration = normalizeDuration(initialVideo?.duration);
	const latestDuration = normalizeDuration(latestVideo?.duration);
	if (
		!isLongSchedule &&
		initialDuration &&
		latestDuration &&
		initialDuration !== latestDuration
	) {
		console.error(
			`[Queue] Schedule ${sched._id} duration mismatch (seed=${initialDuration}, latest=${latestDuration})`,
		);
		await applyScheduleFailureBackoff(sched, {
			nowPST,
			scheduleType,
			reason: "duration mismatch across schedule videos",
			runContext,
		});
		return;
	}
	const resolvedDuration = latestDuration || initialDuration;
	if (!isLongSchedule && !resolvedDuration) {
		console.error(
			`[Queue] Schedule ${sched._id} missing duration from seed videos; delaying with backoff`,
		);
		await applyScheduleFailureBackoff(sched, {
			nowPST,
			scheduleType,
			reason: "missing duration from schedule videos",
			runContext,
		});
		return;
	}

	const body = isLongSchedule
		? {
				...(longConfig || {}),
				category:
					longConfig?.category ||
					(resolvedCategory && !/longvideo/i.test(resolvedCategory)
						? resolvedCategory
						: "Entertainment"),
				presenterAssetUrl:
					longConfig?.presenterAssetUrl || longConfig?.presenterImageUrl || "",
				voiceoverUrl: longConfig?.voiceoverUrl || "",
				overlayAssets: Array.isArray(longConfig?.overlayAssets)
					? longConfig.overlayAssets
					: [],
				preferredTopicHint: longConfig?.preferredTopicHint || "",
				language: longConfig?.language || "en",
				targetDurationSec: longConfig?.targetDurationSec || 180,
				musicUrl: longConfig?.musicUrl || "",
				disableMusic: Boolean(longConfig?.disableMusic),
				dryRun: Boolean(longConfig?.dryRun),
				youtubeAccessToken: longConfig?.youtubeAccessToken || "",
				youtubeRefreshToken: longConfig?.youtubeRefreshToken || "",
				youtubeTokenExpiresAt: longConfig?.youtubeTokenExpiresAt || "",
				youtubeCategory: longConfig?.youtubeCategory || "",
			}
		: {
				category: resolvedCategory,
				ratio: seedVideo?.ratio || "720:1280",
				duration: resolvedDuration,
				language: seedVideo?.language || "English",
				country: seedVideo?.country || "US",
				customPrompt: "",
				videoImage: seedVideo?.videoImage,
				schedule: null,
				useSora: Boolean(seedVideo?.useSora),
				youtubeAccessToken: seedVideo?.youtubeAccessToken,
				youtubeRefreshToken: seedVideo?.youtubeRefreshToken,
				youtubeTokenExpiresAt: seedVideo?.youtubeTokenExpiresAt,
				youtubeEmail: seedVideo?.youtubeEmail,
			};
	const scheduleJobMeta = {
		scheduleId: String(sched._id),
		category: resolvedCategory,
		baseVideoId: seedVideo?._id || seedVideo?.id || undefined,
		useSora: Boolean(seedVideo?.useSora),
		videoType: isLongSchedule ? "long" : "short",
	};

	// Mock req/res so createVideo can run from cron/queue
	const reqMock = { body, user, scheduleJobMeta };
	const controllerResponse = {
		statusCode: 200,
		body: null,
		sawErrorPhase: false,
	};
	const resMock = {
		headersSent: false,
		statusCode: 200,
		setHeader() {},
		setTimeout() {},
		flushHeaders() {},
		flush() {},
		write(chunk) {
			const text = String(chunk || "");
			if (!text || !text.includes("data:")) return;
			const lines = text.split("\n");
			for (const line of lines) {
				if (!line.startsWith("data:")) continue;
				const payloadText = line.slice(5).trim();
				if (!payloadText) continue;
				try {
					const payload = JSON.parse(payloadText);
					if (payload?.phase === "ERROR") {
						controllerResponse.sawErrorPhase = true;
					}
				} catch {
					// ignore non-json SSE chunks
				}
			}
		},
		status(code) {
			const n = Number(code);
			if (Number.isFinite(n) && n > 0) {
				this.statusCode = n;
				controllerResponse.statusCode = n;
			}
			return this;
		},
		json(payload) {
			controllerResponse.body = payload;
			return this;
		},
		end() {},
	};

	console.log(
		`[Queue] Generating ${
			isLongSchedule ? "long video" : "video"
		} for schedule ${sched._id} (${resolvedCategory})`,
	);

	// IMPORTANT: Prevent “infinite retry loop” if createVideo fails:
	// we push nextRun forward by FAIL_BACKOFF_MINUTES on failure.
	try {
		if (isLongSchedule) {
			await createLongVideo(reqMock, resMock);
		} else {
			await createVideo(reqMock, resMock);
		}
		const controllerStatus = Number(controllerResponse.statusCode) || 200;
		if (controllerStatus >= 400) {
			const errText = getControllerErrorText(controllerResponse.body);
			throw new Error(
				`controller returned HTTP ${controllerStatus}${
					errText ? `: ${errText}` : ""
				}`,
			);
		}
		if (!isLongSchedule && controllerResponse.sawErrorPhase) {
			const errText = getControllerErrorText(controllerResponse.body);
			throw new Error(
				`controller emitted ERROR phase${errText ? `: ${errText}` : ""}`,
			);
		}
		sched.failCount = 0;
		sched.lastFailReason = undefined;
		console.log(`[Queue] ✔ Video done for schedule ${sched._id}`);
	} catch (err) {
		console.error(
			`[Queue] ✖ createVideo failed for schedule ${sched._id}:`,
			err?.message || err,
		);
		await applyScheduleFailureBackoff(sched, {
			nowPST,
			scheduleType,
			err,
			runContext,
		});
		return;
	}

	/* 3) compute nextRun (PST wall-clock time) */
	const next = computeNextRunForSchedule({
		scheduleType,
		timeOfDay: sched.timeOfDay,
		baseMoment: dayjs(sched.nextRun).tz(PST_TZ),
		nowMoment: nowPST,
	});
	if (!next) {
		console.error(
			`[Queue] Invalid nextRun calc for schedule ${sched._id}; keeping current`,
		);
		appendScheduleRunEntry(
			sched,
			buildScheduleRunEntry({
				runId: runContext.runId,
				status: "failed",
				startedAt: runContext.startedAt,
				reason: "invalid nextRun calculation",
				nextRun: sched.nextRun,
				failCount: getScheduleFailCount(sched),
			}),
		);
		await sched.save();
		return;
	}

	if (endPST && next.isAfter(endPST)) {
		sched.active = false;
		console.log(
			`[Queue] Schedule ${sched._id} reached endDate, marking inactive`,
		);
	} else {
		sched.nextRun = next.toDate();
	}

	appendScheduleRunEntry(
		sched,
		buildScheduleRunEntry({
			runId: runContext.runId,
			status: "success",
			startedAt: runContext.startedAt,
			reason:
				endPST && next.isAfter(endPST)
					? "run completed; deactivated at endDate boundary"
					: "run completed",
			nextRun: sched.nextRun,
			failCount: getScheduleFailCount(sched),
		}),
	);
	await sched.save();
	console.log(
		`[Queue] Schedule ${
			sched._id
		} → nextRun ${next.format()} PST (stored ${sched.nextRun.toISOString()})`,
	);
}

/* ---------- App scheduler poller (every minute, PST timezone) ---------- */
// Application-level scheduling only. This does not install or modify OS crontab.
const cronTask = ENABLE_SCHEDULER ? cron.schedule(
	"* * * * *",
	async () => {
		try {
			// If DB isn’t connected, don’t enqueue jobs (avoids noisy loops)
			if (mongoose.connection.readyState !== 1) {
				console.warn("[Cron] DB not connected yet; skipping this tick.");
				return;
			}
			if (processing && activeQueueJob?.startedAt) {
				const ageMs = Date.now() - activeQueueJob.startedAt;
				const warnMs = SCHEDULE_QUEUE_STALL_WARN_MINUTES * 60 * 1000;
				if (
					ageMs >= warnMs &&
					(!activeQueueJob.lastWarnAt ||
						Date.now() - activeQueueJob.lastWarnAt >= warnMs)
				) {
					activeQueueJob.lastWarnAt = Date.now();
					console.warn(
						`[Queue] Long-running job detected for schedule ${
							activeQueueJob.scheduleId
						}; age=${Math.round(ageMs / 60000)}m, queued=${jobQueue.length}`,
					);
				}
			}

			const nowPSTDate = dayjs().tz(PST_TZ).toDate();

			const due = await Schedule.find({
				nextRun: { $lte: nowPSTDate },
				active: true,
			})
				.populate("video")
				.populate("videos")
				.populate("user");

			let newlyEnqueued = 0;

			for (const sched of due) {
				const idStr = String(sched._id);
				if (!queuedIds.has(idStr)) {
					jobQueue.push(sched);
					queuedIds.add(idStr);
					newlyEnqueued++;
				}
			}

			if (newlyEnqueued > 0) {
				console.log(
					`[Cron] Enqueued ${newlyEnqueued} job(s); queue size ${jobQueue.length}`,
				);
				void processQueue();
			}
		} catch (err) {
			console.error("[Cron] fatal:", err);
		}
	},
	{ timezone: PST_TZ },
) : { stop() {} };

/* ---------- Shorts uploader cron (every 2 hours) ---------- */
let shortsProcessing = false;
function pickShortsBatchSize() {
	const min = Math.min(SHORTS_UPLOAD_BATCH_MIN, SHORTS_UPLOAD_BATCH_MAX);
	const max = Math.max(SHORTS_UPLOAD_BATCH_MIN, SHORTS_UPLOAD_BATCH_MAX);
	return min + Math.floor(Math.random() * (max - min + 1));
}

const shortsCronTask = ENABLE_SCHEDULER ? cron.schedule(
	SHORTS_CRON_SCHEDULE,
	async () => {
		if (shortsProcessing) return;
		if (mongoose.connection.readyState !== 1) {
			console.warn("[ShortsCron] DB not connected yet; skipping this tick.");
			return;
		}
		shortsProcessing = true;
		const limit = pickShortsBatchSize();
		try {
			const result = await processPendingShortUploads({ limit });
			console.log("[ShortsCron] Upload batch complete", {
				limit,
				processed: result.processed,
				uploaded: result.uploaded,
				errors: result.errors?.length || 0,
			});
		} catch (err) {
			console.error("[ShortsCron] fatal:", err?.message || err);
		} finally {
			shortsProcessing = false;
		}
	},
	{ timezone: SHORTS_CRON_TZ },
) : { stop() {} };

/* ---------- Socket.IO handlers ---------- */
io.use(async (socket, next) => {
	try {
		const token =
			socket.handshake.auth?.token ||
			String(socket.handshake.headers?.authorization || "").replace(
				/^Bearer\s+/i,
				"",
			);
		if (!token) return next(new Error("Authentication required"));
		const decoded = jwt.verify(token, process.env.JWT_SECRET);
		const user = await User.findById(decoded.id).select("_id role name email");
		if (!user) return next(new Error("Authentication required"));
		socket.user = user;
		socket.joinedRooms = new Set();
		return next();
	} catch {
		return next(new Error("Authentication required"));
	}
});

function isValidSocketRoom(chatId) {
	const value = String(chatId || "").trim();
	if (!value || value.length > 100) return false;
	return /^[a-zA-Z0-9:_-]+$/.test(value);
}

async function canJoinSocketRoom(socket, chatId) {
	const value = String(chatId || "").trim();
	if (!isValidSocketRoom(value)) return false;
	if (value === "support_room") return true;
	if (!mongoose.Types.ObjectId.isValid(value)) return false;
	const chat = await Chat.findById(value).select("participants").lean();
	if (!chat) return false;
	if (socket.user?.role === "admin") return true;
	return (chat.participants || []).some(
		(id) => String(id) === String(socket.user?._id),
	);
}

io.on("connection", (socket) => {
	console.log("User connected:", socket.id);

	socket.on("joinRoom", async ({ chatId } = {}) => {
		const room = String(chatId || "").trim();
		if (!(await canJoinSocketRoom(socket, room))) return;
		socket.join(room);
		socket.joinedRooms.add(room);
	});
	socket.on("leaveRoom", ({ chatId } = {}) => {
		const room = String(chatId || "").trim();
		if (!socket.joinedRooms.has(room)) return;
		socket.leave(room);
		socket.joinedRooms.delete(room);
	});

	socket.on("typing", ({ chatId } = {}) => {
		const room = String(chatId || "").trim();
		if (!socket.joinedRooms.has(room)) return;
		socket.to(room).emit("typing", {
			chatId: room,
			userId: String(socket.user?._id || ""),
		});
	});
	socket.on("stopTyping", ({ chatId } = {}) => {
		const room = String(chatId || "").trim();
		if (!socket.joinedRooms.has(room)) return;
		socket.to(room).emit("stopTyping", {
			chatId: room,
			userId: String(socket.user?._id || ""),
		});
	});

	socket.on("sendMessage", (msg = {}) => {
		const room = String(msg.chatId || "").trim();
		if (!socket.joinedRooms.has(room)) return;
		const content = String(msg.content || msg.message?.content || "").slice(
			0,
			2000,
		);
		io.to(room).emit("receiveMessage", {
			chatId: room,
			content,
			userId: String(socket.user?._id || ""),
		});
	});
	socket.on("newChat", (d) => {
		if (socket.user?.role === "admin") io.emit("newChat", d);
	});
	socket.on("deleteMessage", ({ chatId, messageId } = {}) => {
		const room = String(chatId || "").trim();
		if (!socket.joinedRooms.has(room)) return;
		io.to(room).emit("messageDeleted", { chatId: room, messageId });
	});

	socket.on("disconnect", (reason) =>
		console.log(`User disconnected: ${reason}`),
	);
	socket.on("connect_error", (error) =>
		console.error(`Connection error: ${error.message}`),
	);
});

/* ---------- Error handler ---------- */
const { errorHandler } = require("./middlewares/errorHandler");
app.use(errorHandler);

/* ---------- Startup / Shutdown ---------- */
const HOST = process.env.HOST || "127.0.0.1";
const PORT = toInt(process.env.PORT, 8102);

const mongoUri = process.env.MONGODB_URI || process.env.DATABASE;
if (!mongoUri) {
	console.error(
		"Missing MongoDB connection string. Set MONGODB_URI (preferred) or DATABASE in your .env",
	);
	process.exit(1);
}

function redactMongoTarget(uri) {
	try {
		const parsed = new URL(uri);
		const port = parsed.port ? `:${parsed.port}` : "";
		const pathPart = parsed.pathname || "";
		return `${parsed.protocol}//${parsed.hostname}${port}${pathPart}`;
	} catch {
		return uri;
	}
}

let shuttingDown = false;
const IS_DEVELOPMENT = NODE_ENV === "development";
let generatedFilesSweeper = null;
const MONGO_CONNECT_TIMEOUT_MS = Math.max(
	1000,
	toInt(process.env.MONGO_CONNECT_TIMEOUT_MS, 10000),
);
const MONGO_DEV_RETRY_DELAY_MS = Math.max(
	1000,
	toInt(process.env.MONGO_DEV_RETRY_DELAY_MS, 5000),
);
const MONGO_DEV_MAX_RETRIES = Math.max(
	0,
	toInt(process.env.MONGO_DEV_MAX_RETRIES, 0),
);

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function isPortInUseError(err) {
	return err && err.code === "EADDRINUSE";
}

async function connectMongoWithRetry(uri) {
	const display = redactMongoTarget(uri);
	let attempt = 0;

	while (!shuttingDown) {
		attempt += 1;
		try {
			await mongoose.connect(uri, {
				serverSelectionTimeoutMS: MONGO_CONNECT_TIMEOUT_MS,
			});
			console.log(`MongoDB connected (${display})`);
			return;
		} catch (err) {
			if (!IS_DEVELOPMENT) throw err;

			const message = err?.message || String(err);
			const reachedRetryLimit =
				MONGO_DEV_MAX_RETRIES > 0 && attempt >= MONGO_DEV_MAX_RETRIES;

			console.warn(
				`[Startup] MongoDB not ready (${display}). Attempt ${attempt}${
					MONGO_DEV_MAX_RETRIES ? `/${MONGO_DEV_MAX_RETRIES}` : ""
				}: ${message}`,
			);

			if (reachedRetryLimit) throw err;

			console.warn(
				`[Startup] Development mode will retry MongoDB in ${Math.round(
					MONGO_DEV_RETRY_DELAY_MS / 1000,
				)}s. Start MongoDB or press Ctrl+C to stop Nodemon.`,
			);
			await sleep(MONGO_DEV_RETRY_DELAY_MS);
		}
	}

	throw new Error("Startup cancelled while waiting for MongoDB.");
}

async function isPortAvailable(host, port) {
	return await new Promise((resolve, reject) => {
		const tester = net.createServer();
		tester.unref();
		tester.once("error", (err) => {
			if (isPortInUseError(err)) {
				resolve(false);
				return;
			}
			reject(err);
		});
		tester.once("listening", () => {
			tester.close((closeErr) => {
				if (closeErr) {
					reject(closeErr);
					return;
				}
				resolve(true);
			});
		});
		tester.listen(port, host);
	});
}

async function shutdown(code = 0) {
	if (shuttingDown) return;
	shuttingDown = true;

	console.log("[Shutdown] Stopping cron, closing server, disconnecting DB...");

	try {
		cronTask.stop();
	} catch {}
	try {
		shortsCronTask.stop();
	} catch {}
	try {
		generatedFilesSweeper?.stop?.();
	} catch {}

	await new Promise((resolve) => {
		server.close(() => resolve());
	}).catch(() => {});

	try {
		await mongoose.disconnect();
	} catch {}

	console.log("[Shutdown] Done.");
	process.exit(code);
}

process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));

process.on("unhandledRejection", (reason) => {
	console.error("[Process] Unhandled Rejection:", reason);
	shutdown(1);
});

process.on("uncaughtException", (err) => {
	console.error("[Process] Uncaught Exception:", err);
	shutdown(1);
});

async function start() {
	try {
		const portAvailable = await isPortAvailable(HOST, PORT);
		if (!portAvailable) {
			console.warn(
				`[Startup] Port ${HOST}:${PORT} is already in use. Another backend instance is already running, so this process will exit cleanly.`,
			);
			process.exit(0);
			return;
		}

		mongoose.set("strictQuery", false);

		await connectMongoWithRetry(mongoUri);
		generatedFilesSweeper = startGeneratedFilesSweeper();

		server.listen(PORT, HOST, () => {
			console.log(`Server running: http://${HOST}:${PORT} (${NODE_ENV})`);
			if (LOG_STARTUP_DETAILS) {
				console.log(
					`Allowed origins: ${
						ALLOW_ALL_ORIGINS ? "*" : ALLOWED_ORIGINS.join(", ")
					}`,
				);
				console.log(`Cron timezone: ${PST_TZ}`);
				console.log(`Application scheduler enabled: ${ENABLE_SCHEDULER}`);
				try {
					console.log(
						"[Startup] Long video controller",
						getLongVideoRuntimeProfile(),
					);
				} catch {}
			}
		});

		server.on("error", (err) => {
			if (isPortInUseError(err)) {
				console.warn(
					`[Startup] Port ${HOST}:${PORT} became busy before listen completed. Another backend instance is already running, so this process will exit cleanly.`,
				);
				shutdown(0);
				return;
			}
			console.error("[Server] Listen error:", err);
			shutdown(1);
		});
	} catch (err) {
		console.error("Startup failed:", err?.message || err);
		process.exit(1);
	}
}

start();
