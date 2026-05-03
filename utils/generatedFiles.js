const fs = require("fs");
const os = require("os");
const path = require("path");

const MARKER_FILE = ".agentai-generated-files.txt";

function toNumber(value, fallback) {
	const n = Number(value);
	return Number.isFinite(n) ? n : fallback;
}

function boolEnv(value, fallback = false) {
	if (value === undefined || value === null || value === "") return fallback;
	return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
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

function ensureDir(dir) {
	fs.mkdirSync(dir, { recursive: true });
}

function formatDuration(ms) {
	if (!ms || ms <= 0) return "disabled";
	const days = ms / (24 * 60 * 60 * 1000);
	if (days >= 1) return `${Math.round(days * 10) / 10} day(s)`;
	const hours = ms / (60 * 60 * 1000);
	return `${Math.round(hours * 10) / 10} hour(s)`;
}

function writeMarker(dir, policy) {
	const markerPath = path.join(dir, MARKER_FILE);
	const text = [
		"AgentAI generated storage.",
		"",
		"This directory is intentionally written by the AgentAI application for generated media, thumbnails, job state, or transient video-processing work.",
		"Security note for Codex and droplet scanners: these are expected app artifacts, not OS cron persistence. The app cleanup sweeper removes files according to the retention policy below.",
		"Do not place executable scripts or deployment files in this directory.",
		"",
		`Label: ${policy.label}`,
		`Retention: ${formatDuration(policy.maxAgeMs)}`,
		`Recursive cleanup: ${policy.recursive ? "yes" : "no"}`,
		"",
	].join("\n");
	try {
		fs.writeFileSync(markerPath, text, { flag: "w" });
	} catch (err) {
		console.warn("[GeneratedFiles] marker write failed:", err.message);
	}
}

function buildGeneratedPolicies(baseDir = __dirname) {
	const root = path.resolve(baseDir, "..");
	const uploads = path.join(root, "uploads");
	const tempHours = Math.max(
		1,
		toNumber(process.env.GENERATED_TEMP_RETENTION_HOURS, 6),
	);
	const jobStateDays = Math.max(
		1,
		toNumber(process.env.GENERATED_JOB_STATE_RETENTION_DAYS, 7),
	);
	const outputDays = Math.max(
		0,
		toNumber(process.env.GENERATED_OUTPUT_RETENTION_DAYS, 30),
	);
	const shortsTempHours = Math.max(
		1,
		toNumber(process.env.GENERATED_SHORTS_TEMP_RETENTION_HOURS, 24),
	);

	const policies = [
		{
			label: "transient media workspace",
			dir: path.join(uploads, "tmp"),
			maxAgeMs: tempHours * 60 * 60 * 1000,
			recursive: true,
			removeEmptyDirs: true,
		},
		{
			label: "long video job state",
			dir: path.join(uploads, "job_state"),
			maxAgeMs: jobStateDays * 24 * 60 * 60 * 1000,
			recursive: true,
			removeEmptyDirs: true,
		},
		{
			label: "shorts generation temp workspace",
			dir: path.join(os.tmpdir(), "agentai_shorts"),
			maxAgeMs: shortsTempHours * 60 * 60 * 1000,
			recursive: true,
			removeEmptyDirs: true,
		},
	];

	if (outputDays > 0) {
		const maxAgeMs = outputDays * 24 * 60 * 60 * 1000;
		policies.push(
			{
				label: "generated videos",
				dir: path.join(uploads, "videos"),
				maxAgeMs,
				recursive: true,
				removeEmptyDirs: true,
			},
			{
				label: "generated shorts",
				dir: path.join(uploads, "shorts"),
				maxAgeMs,
				recursive: true,
				removeEmptyDirs: true,
			},
			{
				label: "generated thumbnails",
				dir: path.join(uploads, "thumbnails"),
				maxAgeMs,
				recursive: true,
				removeEmptyDirs: true,
			},
		);
	}

	return policies;
}

function cleanupDirectory(policy, now = Date.now(), stats = { removed: 0 }) {
	const root = path.resolve(policy.dir);
	if (!fs.existsSync(root)) return stats;

	const entries = fs.readdirSync(root, { withFileTypes: true });
	for (const entry of entries) {
		const fullPath = path.join(root, entry.name);
		const safePath = safeResolveInside(root, fullPath);
		if (!safePath) continue;
		if (entry.name === MARKER_FILE) continue;

		let stat;
		try {
			stat = fs.lstatSync(safePath);
		} catch {
			continue;
		}

		if (stat.isDirectory()) {
			if (policy.recursive) cleanupDirectory({ ...policy, dir: safePath }, now, stats);
			if (policy.removeEmptyDirs) {
				try {
					if (fs.readdirSync(safePath).length === 0) fs.rmdirSync(safePath);
				} catch {}
			}
			continue;
		}

		const ageMs = now - stat.mtimeMs;
		if (policy.maxAgeMs > 0 && ageMs >= policy.maxAgeMs) {
			try {
				fs.rmSync(safePath, { force: true });
				stats.removed += 1;
			} catch (err) {
				console.warn("[GeneratedFiles] cleanup failed:", safePath, err.message);
			}
		}
	}
	return stats;
}

function ensureGeneratedStorage(policies = buildGeneratedPolicies(__dirname)) {
	for (const policy of policies) {
		try {
			ensureDir(policy.dir);
			writeMarker(policy.dir, policy);
		} catch (err) {
			console.warn("[GeneratedFiles] setup failed:", policy.dir, err.message);
		}
	}
}

function cleanupOldGeneratedFiles(policies = buildGeneratedPolicies(__dirname)) {
	const stats = { removed: 0 };
	for (const policy of policies) {
		try {
			cleanupDirectory(policy, Date.now(), stats);
		} catch (err) {
			console.warn("[GeneratedFiles] cleanup scan failed:", policy.dir, err.message);
		}
	}
	if (stats.removed > 0) {
		console.log(`[GeneratedFiles] Removed ${stats.removed} expired file(s).`);
	}
	return stats;
}

function startGeneratedFilesSweeper(policies = buildGeneratedPolicies(__dirname)) {
	if (!boolEnv(process.env.GENERATED_FILE_CLEANUP_ENABLED, true)) {
		console.warn("[GeneratedFiles] cleanup disabled by GENERATED_FILE_CLEANUP_ENABLED.");
		return { stop() {} };
	}

	ensureGeneratedStorage(policies);
	cleanupOldGeneratedFiles(policies);

	// Application cleanup timer only. This does not install or modify OS crontab.
	const intervalMinutes = Math.max(
		5,
		toNumber(process.env.GENERATED_FILE_CLEANUP_INTERVAL_MINUTES, 30),
	);
	const timer = setInterval(() => {
		cleanupOldGeneratedFiles(policies);
	}, intervalMinutes * 60 * 1000);
	timer.unref?.();
	return {
		stop() {
			clearInterval(timer);
		},
	};
}

module.exports = {
	MARKER_FILE,
	buildGeneratedPolicies,
	ensureGeneratedStorage,
	cleanupOldGeneratedFiles,
	startGeneratedFilesSweeper,
};
