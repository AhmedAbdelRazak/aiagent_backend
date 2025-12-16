/* server.js — production-hardened, queued-cron, PST-aware
/* eslint-disable no-console */

const path = require("path");

// Always load .env from the same directory as this file (works reliably with PM2)
require("dotenv").config({ path: path.join(__dirname, ".env") });

const express = require("express");
const mongoose = require("mongoose");
const morgan = require("morgan");
const cors = require("cors");
const { readdirSync } = require("fs");
const http = require("http");
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
const { createVideo } = require("./controllers/videoController");

/* ---------- Middleware ---------- */
const { protect } = require("./middlewares/authMiddleware");
const PST_TZ = "America/Los_Angeles";

/* ---------- Environment normalization ---------- */
if (!process.env.NODE_ENV && process.env.ENVIRONMENT) {
	// map ENVIRONMENT=PRODUCTION -> NODE_ENV=production
	const env = String(process.env.ENVIRONMENT).toLowerCase();
	process.env.NODE_ENV = env === "production" ? "production" : env;
}
const NODE_ENV = process.env.NODE_ENV || "test";

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
		.map((s) => s.trim())
		.filter(Boolean);
}

const ALLOWED_ORIGINS = normalizeOriginList(process.env.CLIENT_ORIGIN || "*");
const ALLOW_ALL_ORIGINS = ALLOWED_ORIGINS.includes("*");

function isOriginAllowed(origin) {
	if (ALLOW_ALL_ORIGINS) return true;
	return ALLOWED_ORIGINS.includes(origin);
}

/* ---------- Express + HTTP + Socket.IO ---------- */
const app = express();
app.disable("x-powered-by");

// behind Nginx (so req.ip, secure cookies, etc. behave correctly)
app.set("trust proxy", 1);

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
	})
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
	10
);

async function processQueue() {
	if (processing) return;
	processing = true;

	while (jobQueue.length) {
		const sched = jobQueue.shift();
		try {
			await handleSchedule(sched);
		} catch (err) {
			console.error(
				"[Queue] job error:",
				err && err.message ? err.message : err
			);
		} finally {
			queuedIds.delete(String(sched._id));
		}
	}

	processing = false;
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

async function handleSchedule(sched) {
	const nowPST = dayjs().tz(PST_TZ);

	// Basic validation to prevent “bad data” infinite loops
	const t = parseTimeOfDay(sched.timeOfDay);
	if (!t) {
		console.error(
			`[Queue] Invalid timeOfDay for schedule ${sched._id}:`,
			sched.timeOfDay
		);
		sched.active = false;
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
			await sched.save();
			console.log(`[Queue] Schedule ${sched._id} expired & deactivated`);
			return;
		}
	}

	const { user, video } = sched;

	/* 2) resolve base video seed */
	let baseVideo = video || null;
	if (!baseVideo && Array.isArray(sched.videos) && sched.videos.length) {
		baseVideo = sched.videos[sched.videos.length - 1];
	}

	const resolvedCategory =
		sched.category || (baseVideo && baseVideo.category) || "Entertainment";

	if (!baseVideo || !baseVideo.category) {
		try {
			baseVideo = await Video.findOne({
				user: (user && user._id) || user,
				category: resolvedCategory,
			})
				.sort({ createdAt: -1 })
				.lean();
		} catch (e) {
			console.warn("[Queue] No base video found; using defaults:", e.message);
		}
	}

	const body = {
		category: resolvedCategory,
		ratio: baseVideo?.ratio || "720:1280",
		duration: baseVideo?.duration || 20,
		language: baseVideo?.language || "English",
		country: baseVideo?.country || "US",
		customPrompt: "",
		videoImage: baseVideo?.videoImage,
		schedule: null,
		youtubeAccessToken: baseVideo?.youtubeAccessToken,
		youtubeRefreshToken: baseVideo?.youtubeRefreshToken,
		youtubeTokenExpiresAt: baseVideo?.youtubeTokenExpiresAt,
		youtubeEmail: baseVideo?.youtubeEmail,
	};

	// Mock req/res so createVideo can run from cron/queue
	const reqMock = { body, user };
	const resMock = {
		headersSent: false,
		setHeader() {},
		setTimeout() {},
		write() {},
		status() {
			return this;
		},
		json() {},
		end() {},
	};

	console.log(`[Queue] ▶ Generating video for schedule ${sched._id}`);

	// IMPORTANT: Prevent “infinite retry loop” if createVideo fails:
	// we push nextRun forward by FAIL_BACKOFF_MINUTES on failure.
	try {
		await createVideo(reqMock, resMock);
		console.log(`[Queue] ✔ Video done for schedule ${sched._id}`);
	} catch (err) {
		console.error(
			`[Queue] ✖ createVideo failed for schedule ${sched._id}:`,
			err?.message || err
		);
		sched.nextRun = nowPST.add(FAIL_BACKOFF_MINUTES, "minute").toDate();
		await sched.save();
		console.log(
			`[Queue] ↻ Backoff: schedule ${sched._id} will retry at ${dayjs(
				sched.nextRun
			)
				.tz(PST_TZ)
				.format()} PST`
		);
		return;
	}

	/* 3) compute nextRun (PST wall-clock time) */
	const { hh, mm } = t;

	let base = dayjs(sched.nextRun).tz(PST_TZ);
	if (!base.isValid()) base = nowPST;

	if (scheduleType === "daily") base = base.add(1, "day");
	else if (scheduleType === "weekly") base = base.add(1, "week");
	else if (scheduleType === "monthly") base = base.add(1, "month");

	let next = base.hour(hh).minute(mm).second(0).millisecond(0);

	if (next.isBefore(nowPST)) {
		if (scheduleType === "daily") next = next.add(1, "day");
		else if (scheduleType === "weekly") next = next.add(1, "week");
		else if (scheduleType === "monthly") next = next.add(1, "month");
	}

	if (endPST && next.isAfter(endPST)) {
		sched.active = false;
		console.log(
			`[Queue] Schedule ${sched._id} reached endDate, marking inactive`
		);
	} else {
		sched.nextRun = next.toDate();
	}

	await sched.save();
	console.log(
		`[Queue] Schedule ${
			sched._id
		} → nextRun ${next.format()} PST (stored ${sched.nextRun.toISOString()})`
	);
}

/* ---------- Cron poller (every minute, PST timezone) ---------- */
const cronTask = cron.schedule(
	"* * * * *",
	async () => {
		try {
			// If DB isn’t connected, don’t enqueue jobs (avoids noisy loops)
			if (mongoose.connection.readyState !== 1) {
				console.warn("[Cron] DB not connected yet; skipping this tick.");
				return;
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
					`[Cron] Enqueued ${newlyEnqueued} job(s); queue size ${jobQueue.length}`
				);
				processQueue();
			}
		} catch (err) {
			console.error("[Cron] fatal:", err);
		}
	},
	{ timezone: PST_TZ }
);

/* ---------- Socket.IO handlers ---------- */
io.on("connection", (socket) => {
	console.log("User connected:", socket.id);

	socket.on("joinRoom", ({ chatId }) => chatId && socket.join(chatId));
	socket.on("leaveRoom", ({ chatId }) => chatId && socket.leave(chatId));

	socket.on("typing", ({ chatId, userId }) =>
		io.to(chatId).emit("typing", { chatId, userId })
	);
	socket.on("stopTyping", ({ chatId, userId }) =>
		io.to(chatId).emit("stopTyping", { chatId, userId })
	);

	socket.on("sendMessage", (msg) =>
		io.to(msg.chatId).emit("receiveMessage", msg)
	);
	socket.on("newChat", (d) => io.emit("newChat", d));
	socket.on("deleteMessage", ({ chatId, messageId }) =>
		io.to(chatId).emit("messageDeleted", { chatId, messageId })
	);

	socket.on("disconnect", (reason) =>
		console.log(`User disconnected: ${reason}`)
	);
	socket.on("connect_error", (error) =>
		console.error(`Connection error: ${error.message}`)
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
		"Missing MongoDB connection string. Set MONGODB_URI (preferred) or DATABASE in your .env"
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

async function shutdown(code = 0) {
	if (shuttingDown) return;
	shuttingDown = true;

	console.log("[Shutdown] Stopping cron, closing server, disconnecting DB...");

	try {
		cronTask.stop();
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
		mongoose.set("strictQuery", false);

		const display = redactMongoTarget(mongoUri);
		await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 10000 });
		console.log(`MongoDB connected (${display})`);

		server.listen(PORT, HOST, () => {
			console.log(`Server running: http://${HOST}:${PORT} (${NODE_ENV})`);
			console.log(
				`Allowed origins: ${
					ALLOW_ALL_ORIGINS ? "*" : ALLOWED_ORIGINS.join(", ")
				}`
			);
			console.log(`Cron timezone: ${PST_TZ}`);
		});

		server.on("error", (err) => {
			console.error("[Server] Listen error:", err);
			shutdown(1);
		});
	} catch (err) {
		console.error("Startup failed:", err?.message || err);
		process.exit(1);
	}
}

start();
