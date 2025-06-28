/** @format */

require("dotenv").config();
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;
const { OpenAI } = require("openai");

/* ------------------------------------------------------------------ */
/* Poly‑fill File for Node < 20                                       */
/* ------------------------------------------------------------------ */
if (typeof globalThis.File === "undefined") {
	const { Blob, File } = require("node:buffer");
	globalThis.File = File || class extends Blob {};
}

/* ------------------------------------------------------------------ */
cloudinary.config({
	cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
	api_key: process.env.CLOUDINARY_API_KEY,
	api_secret: process.env.CLOUDINARY_API_SECRET,
});
const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });

/* ------------------------------------------------------------------ */
function slugify(s = "") {
	return s
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/(^-|-$)+/g, "");
}

/* ---------- 1. safeDescribeSeedImage (unchanged) ------------------ */
async function safeDescribeSeedImage(
	url,
	{
		maxWords = 60,
		model = "gpt-4o",
		retries = 2,
		detailLevel = "concise",
		includeSensitive = false,
	} = {}
) {
	const rules = `You are a professional photo‑captioning assistant.

${
	includeSensitive
		? "If plainly visible, you MAY mention race / ethnicity."
		: "DO NOT mention ethnicity, race, religion, disability, nationality."
}

Hard rules
• DO NOT identify or name any real person.
• DO NOT guess unseen facts or use speculative words (“looks like”, “maybe”).

Allowed visible attributes:
─ age range │ gender impression │ skin tone │ hair colour & style │ attire
─ pose │ facial expression │ environment │ lighting │ colour palette │ camera angle │ mood.`.trim();

	const ask =
		detailLevel === "comprehensive"
			? `Return two sentences not longer than ${maxWords} words in total.`
			: `Return one sentence not longer than ${maxWords} words.`;
	const sys = `${rules}\n\n${ask}`;
	const user = (n) => `Attempt ${n}: Describe the photo.`;
	let err;
	for (let a = 1; a <= retries; a++) {
		try {
			const {
				choices: [
					{
						message: { content },
					},
				],
			} = await openai.chat.completions.create({
				model,
				messages: [
					{ role: "system", content: sys },
					{
						role: "user",
						content: [
							{ type: "text", text: user(a) },
							{ type: "image_url", image_url: { url } },
						],
					},
				],
			});
			const out = content.replace(/\s+/g, " ").trim();
			const words = out.split(/\s+/).length,
				sents = out.split(/[.!?](?:\s|$)/).filter(Boolean).length;
			if (
				words <= maxWords &&
				sents === (detailLevel === "comprehensive" ? 2 : 1)
			)
				return out;
		} catch (e) {
			err = e;
		}
	}
	console.warn("[visionSafe] fallback:", err?.message);
	return "Person looks ahead with neutral expression under even lighting.";
}

/* ---------- 2. describeImageViaCloudinary (unchanged) ------------- */
async function describeImageViaCloudinary(
	src,
	{
		folder = "videomatic",
		publicId,
		useFilename = false,
		uniqueFilename = true,
		openAIFallback = true,
		enhance = true,
		includeSensitive = true,
		openaiOptions = {},
	} = {}
) {
	let derivedId = publicId;
	if (!derivedId) {
		if (typeof publicId === "function") derivedId = publicId(path.parse(src));
		else if (!useFilename) {
			const base =
				path.extname(src) && !src.startsWith("http")
					? path.parse(src).name
					: slugify(new URL(src).pathname.split("/").pop() || "image");
			derivedId = `${base}-${Date.now()}`;
		}
	}
	let uploadRes;
	try {
		uploadRes = await cloudinary.uploader.upload(src, {
			folder,
			public_id: derivedId,
			use_filename: useFilename,
			unique_filename: uniqueFilename,
			detection: "captioning,adv_face",
			overwrite: false,
		});
		const capObj = uploadRes?.info?.detection?.captioning;
		const cldCaption =
			capObj?.data?.caption ||
			(Array.isArray(capObj?.data) && capObj.data[0]?.caption) ||
			"";
		const f = uploadRes?.info?.detection?.adv_face?.data?.[0] || {};
		const faceClause = [
			f.age && `${String(f.age).split(".")[0]}s`,
			f.race?.value,
			f.gender?.value?.toLowerCase(),
		]
			.filter(Boolean)
			.join(" ");
		const gptDesc = enhance
			? await safeDescribeSeedImage(uploadRes.secure_url, {
					detailLevel: "comprehensive",
					maxWords: 120,
					includeSensitive,
					...openaiOptions,
			  })
			: "";
		return {
			secureUrl: uploadRes.secure_url,
			description: [faceClause, cldCaption, gptDesc]
				.filter(Boolean)
				.join(" — ")
				.replace(/\s+—\s*$/, ""),
		};
	} catch (err) {
		console.error("[Cloudinary] captioning failed:", err.message);
		if (!openAIFallback) throw err;
		if (!uploadRes)
			uploadRes = await cloudinary.uploader.upload(src, {
				folder,
				public_id: derivedId,
				use_filename: useFilename,
				unique_filename: uniqueFilename,
				overwrite: false,
			});
		const fallbackDesc = await safeDescribeSeedImage(uploadRes.secure_url, {
			detailLevel: "comprehensive",
			maxWords: 120,
			includeSensitive,
			...openaiOptions,
		});
		return { secureUrl: uploadRes.secure_url, description: fallbackDesc };
	}
}

/* ---------- 3. helper: simple face QA ----------------------------- */
async function faceLooksOk(url) {
	const q =
		"Yes / No — Does the person's face look natural with no obvious distortions?";
	const {
		choices: [
			{
				message: { content },
			},
		],
	} = await openai.chat.completions.create({
		model: "gpt-4o",
		messages: [
			{
				role: "user",
				content: [
					{ type: "text", text: q },
					{ type: "image_url", image_url: { url } },
				],
			},
		],
	});
	return content.trim().toLowerCase().startsWith("yes");
}

/* ---------- 4. uploadWithVariation (fixed) ------------------------ */
async function uploadWithVariation(
	src,
	{ folder = "aivideomatic", size = 1024, maxTries = 3 } = {}
) {
	/* upload original */
	const stamp = Date.now();
	const orig = await cloudinary.uploader.upload(src, {
		folder,
		public_id: `${stamp}_orig`,
		resource_type: "image",
	});

	/* prepare square PNG */
	const squareUrl = cloudinary.url(orig.public_id, {
		secure: true,
		width: size,
		height: size,
		crop: "fill",
		gravity: "auto",
		fetch_format: "png",
	});
	const tmpSquare = path.join(os.tmpdir(), `sq-${stamp}.png`);
	await fs.promises.writeFile(
		tmpSquare,
		(
			await axios.get(squareUrl, { responseType: "arraybuffer" })
		).data
	);

	/* variation loop */
	let acceptedBuf = null;
	for (let t = 1; t <= maxTries; t++) {
		const url = (
			await openai.images.createVariation({
				image: fs.createReadStream(tmpSquare),
				n: 1,
				size: `${size}x${size}`,
			})
		).data[0].url;
		try {
			if (await faceLooksOk(url)) {
				acceptedBuf = (await axios.get(url, { responseType: "arraybuffer" }))
					.data;
				break;
			}
			console.warn(`[Variation QA] try ${t} failed — face looks odd`);
		} catch (e) {
			console.warn("[Variation QA] skipped:", e.message);
			acceptedBuf = (await axios.get(url, { responseType: "arraybuffer" }))
				.data;
			break;
		}
	}
	if (!acceptedBuf)
		acceptedBuf = (await axios.get(src, { responseType: "arraybuffer" })).data; // ultimate fallback

	/* brighten & upload */
	const tmpVar = path.join(os.tmpdir(), `var-${stamp}.png`);
	await fs.promises.writeFile(tmpVar, acceptedBuf);
	const variant = await cloudinary.uploader.upload(tmpVar, {
		folder,
		public_id: `${stamp}_variant`,
		transformation: [{ effect: "brightness:8" }],
		resource_type: "image",
	});

	return {
		original: { public_id: orig.public_id, url: orig.secure_url },
		variant: { public_id: variant.public_id, url: variant.secure_url },
	};
}

/* ---------- 6. uploadRemoteImagePlain ---------------------------------
 * Lightweight wrapper that simply uploads the remote image (no variation,
 * no transformation) and returns the Cloudinary secure URL.               */
async function uploadRemoteImagePlain(
	src,
	{
		folder = "aivideomatic",
		publicId, // optional custom id
		timeout = 12_000,
	} = {}
) {
	const derivedId =
		publicId ||
		slugify(
			(src.startsWith("http") ? new URL(src).pathname : path.parse(src).name) +
				"-" +
				Date.now()
		);

	const { data } = await axios.get(src, {
		responseType: "arraybuffer",
		timeout,
	});
	const tmp = path.join(os.tmpdir(), `${derivedId}.bin`);
	await fs.promises.writeFile(tmp, data);

	const up = await cloudinary.uploader.upload(tmp, {
		folder,
		public_id: derivedId,
		resource_type: "image",
		overwrite: false,
	});
	await fs.promises.unlink(tmp);
	return up.secure_url; // <-- what we finally send to RunwayML
}

/* ---------- 5. injectSeedDescription (unchanged) ------------------ */
function injectSeedDescription(prompt, seed) {
	if (!seed) return prompt;
	return /\b(male|female|person|man|woman|anchor|reporter|human)\b/i.test(
		prompt
	)
		? prompt
		: `${seed}, ${prompt}`;
}

/* ------------------------------------------------------------------ */
module.exports = {
	safeDescribeSeedImage,
	describeImageViaCloudinary,
	injectSeedDescription,
	uploadWithVariation,
	uploadRemoteImagePlain,
};
