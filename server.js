/* server.js — queued‑cron, PST‑aware 2025‑06‑16 */
/* eslint-disable no-console */
require("dotenv").config();

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

/* ---------- FFmpeg bootstrap (unchanged) ---------- */
const ffmpeg = require("fluent-ffmpeg");
ffmpeg.setFfmpegPath("C:/ffmpeg/bin/ffmpeg.exe");
ffmpeg.setFfprobePath("C:/ffmpeg/bin/ffprobe.exe");

/* ---------- Google APIs ---------- */
const { google } = require("googleapis");

/* ---------- Models & Controllers ---------- */
const Schedule = require("./models/Schedule");
const Video = require("./models/Video");
const {
	createVideo,
	buildYouTubeOAuth2Client,
} = require("./controllers/videoController");

/* ---------- Middleware ---------- */
const { protect } = require("./middlewares/authMiddleware");
const { authorize } = require("./middlewares/roleMiddleware");
const PST_TZ = "America/Los_Angeles";

/* ---------- Express + HTTP + Socket.IO ---------- */
const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
	cors: {
		origin: process.env.CLIENT_ORIGIN || "*",
		methods: ["GET", "POST"],
		allowedHeaders: ["Authorization"],
		credentials: true,
	},
});
app.set("io", io);

/* ───── 1) MongoDB Connection ───── */
mongoose.set("strictQuery", false);
mongoose
	.connect(process.env.DATABASE)
	.then(() => console.log("MongoDB connected"))
	.catch((err) => console.error("DB Connection Error:", err));

/* ───── 2) Global Middleware ───── */
app.use(morgan("dev"));
app.use(
	cors({
		origin: process.env.CLIENT_ORIGIN || "*",
		methods: ["GET", "POST", "PUT", "DELETE"],
		allowedHeaders: ["Content-Type", "Authorization"],
		credentials: true,
	})
);
app.use(express.json({ limit: "50mb" }));

/* ───── 3) Health Check ───── */
app.get("/", (req, res) => res.send("Hello from AgentAI API"));

/* ───── 4) YouTube OAuth Routes (protected) ───── */
const youTubeAuthRoutes = require("./routes/youtubeAuth");
const youTubeTokensRoutes = require("./routes/youtubeTokens");
const youtubeExchangeRoutes = require("./routes/youtubeExchange");

app.use("/api/youtube", protect, youTubeAuthRoutes);
app.use("/api/youtube", protect, youTubeTokensRoutes);
app.use("/api/youtube", protect, youtubeExchangeRoutes);

/* ───── 5) All Other /api Routes ───── */
readdirSync("./routes").forEach((file) => {
	if (/^youtube(A|T|E)/.test(file)) return; // skip the three explicit files
	app.use("/api", require(`./routes/${file}`));
});

/* ───────────────────────────────────────────────────────────── */
/* 6)  SIMPLE IN‑MEMORY JOB QUEUE (concurrency = 1)              */
/* ───────────────────────────────────────────────────────────── */
const jobQueue = [];
const queuedIds = new Set(); // avoid duplicates
let processing = false;

async function processQueue() {
	if (processing) return; // already working
	processing = true;

	while (jobQueue.length) {
		const sched = jobQueue.shift();
		queuedIds.delete(String(sched._id));

		try {
			await handleSchedule(sched);
		} catch (err) {
			console.error("[Queue] job error:", err.message);
		}
	}

	processing = false;
}

/* Core logic for one schedule */
async function handleSchedule(sched) {
	const nowPST = dayjs().tz(PST_TZ);

	/* 1 ▸ stop expired schedules (endDate is a PST calendar date) */
	let endPST = null;
	if (sched.endDate) {
		const endDateStr = dayjs(sched.endDate).format("YYYY-MM-DD");
		endPST = dayjs.tz(`${endDateStr} 23:59`, "YYYY-MM-DD HH:mm", PST_TZ);
		if (nowPST.isAfter(endPST)) {
			sched.active = false;
			await sched.save();
			console.log(`[Queue] Schedule ${sched._id} expired & deactivated`);
			return;
		}
	}

	const { user, video, scheduleType, timeOfDay } = sched;

	/* 2 ▸ build request for createVideo */
	const body = {
		category: video.category,
		ratio: video.ratio,
		duration: video.duration,
		language: video.language,
		country: video.country,
		customPrompt: "",
		videoImage: video.videoImage,
		schedule: null,
		youtubeAccessToken: video.youtubeAccessToken,
		youtubeRefreshToken: video.youtubeRefreshToken,
		youtubeTokenExpiresAt: video.youtubeTokenExpiresAt,
		youtubeEmail: video.youtubeEmail,
	};

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

	console.log(`[Queue] ▶ Generating video for user ${user._id}`);
	await createVideo(reqMock, resMock);
	console.log(`[Queue] ✔ Video done for schedule ${sched._id}`);

	/* 3 ▸ compute nextRun (always PST timeOfDay) */
	const [hh, mm] = timeOfDay.split(":").map(Number);

	// Start from the date of the previous run, in PST
	let base = dayjs(sched.nextRun).tz(PST_TZ);

	if (scheduleType === "daily") base = base.add(1, "day");
	else if (scheduleType === "weekly") base = base.add(1, "week");
	else if (scheduleType === "monthly") base = base.add(1, "month");

	// Force the configured wall‑clock time in PST
	let next = base.hour(hh).minute(mm).second(0).millisecond(0);

	// If for some reason it's already in the past, bump one more period
	if (next.isBefore(nowPST)) {
		if (scheduleType === "daily") next = next.add(1, "day");
		else if (scheduleType === "weekly") next = next.add(1, "week");
		else if (scheduleType === "monthly") next = next.add(1, "month");
	}

	// Re‑evaluate against endDate in PST (end‑of‑day)
	if (endPST && next.isAfter(endPST)) {
		sched.active = false;
	} else {
		sched.nextRun = next.toDate(); // moment in time corresponding to HH:mm PST
	}

	await sched.save();
	console.log(
		`[Queue] Schedule ${
			sched._id
		} → nextRun ${next.format()} PST (stored as ${sched.nextRun.toISOString()})`
	);
}

/* ───────────────────────────────────────────────────────────── */
/* 7)  CRON POLLER — adds due schedules to the queue            */
/* ───────────────────────────────────────────────────────────── */
cron.schedule("* * * * *", async () => {
	try {
		const nowPST = dayjs().tz("America/Los_Angeles").toDate();
		const due = await Schedule.find({ nextRun: { $lte: nowPST }, active: true })
			.populate("video")
			.populate("user");

		for (const sched of due) {
			const idStr = String(sched._id);
			if (!queuedIds.has(idStr)) {
				jobQueue.push(sched);
				queuedIds.add(idStr);
			}
		}

		if (jobQueue.length) {
			console.log(`[Cron] Enqueued ${jobQueue.length} job(s)`);
			processQueue(); // kick the worker if idle
		}
	} catch (err) {
		console.error("[Cron] fatal:", err);
	}
});

/* ───── 8) SOCKET.IO (unchanged) ───── */
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

/* ───── 9) Error handler (unchanged) ───── */
const { errorHandler } = require("./middlewares/errorHandler");
app.use(errorHandler);

/* ───── 10) Start server ───── */
const PORT = process.env.PORT || 8102;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
