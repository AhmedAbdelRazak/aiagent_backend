const fs = require("fs");
const os = require("os");
const path = require("path");
const childProcess = require("child_process");

function canExecBin(bin, args = ["-version"]) {
	if (!bin) return false;
	try {
		const result = childProcess.spawnSync(bin, args, {
			stdio: "ignore",
			windowsHide: true,
		});
		return result && !result.error && result.status === 0;
	} catch {
		return false;
	}
}

function getStaticFfprobePath() {
	try {
		const ffprobeStatic = require("ffprobe-static");
		const candidate =
			(typeof ffprobeStatic === "string" && ffprobeStatic) ||
			ffprobeStatic?.path ||
			"";
		return String(candidate || "").trim();
	} catch {
		return "";
	}
}

function getSiblingFfprobePath(ffmpegPath) {
	if (!ffmpegPath) return "";
	const probeName = os.platform() === "win32" ? "ffprobe.exe" : "ffprobe";
	const candidate = path.join(path.dirname(ffmpegPath), probeName);
	return fs.existsSync(candidate) ? candidate : "";
}

function resolveFfprobePath({ ffmpegPath, env = process.env } = {}) {
	const candidates = [
		env.FFPROBE_PATH,
		getStaticFfprobePath(),
		getSiblingFfprobePath(ffmpegPath),
		"ffprobe",
		os.platform() === "win32" ? "ffprobe.exe" : "",
	]
		.map((candidate) => String(candidate || "").trim())
		.filter(Boolean);

	for (const candidate of candidates) {
		if (canExecBin(candidate, ["-version"])) return candidate;
	}

	return "";
}

module.exports = {
	canExecBin,
	resolveFfprobePath,
};
