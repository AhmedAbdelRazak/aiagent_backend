/** @format */

require("dotenv").config();

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const axios = require("axios");
const cloudinary = require("cloudinary").v2;
const { OpenAI } = require("openai");

/* ------------------------------------------------------------------ */
/* 0️⃣  Poly‑fill `globalThis.File` for Node < 20                      */
/* ------------------------------------------------------------------ */
if (typeof globalThis.File === "undefined") {
	const { Blob, File } = require("node:buffer");
	globalThis.File = File || class extends Blob {};
}

/* ------------------------------------------------------------------ */
/* Cloudinary + OpenAI                                                */
/* ------------------------------------------------------------------ */
cloudinary.config({
	cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
	api_key: process.env.CLOUDINARY_API_KEY,
	api_secret: process.env.CLOUDINARY_API_SECRET,
});
const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });

/* ------------------------------------------------------------------ */
/* Little util                                                        */
/* ------------------------------------------------------------------ */
function slugify(s = "") {
	return s
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/(^-|-$)+/g, "");
}

/* ------------------------------------------------------------------ */
/* 1️⃣  Vision‑GPT caption helper  (unchanged)                        */
/* ------------------------------------------------------------------ */
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
	const rules = `
You are a professional photo‑captioning assistant.

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
─ pose │ facial expression │ environment │ lighting │ colour palette │ camera angle │ mood.
`.trim();

	const ask =
		detailLevel === "comprehensive"
			? `Return two sentences not longer than ${maxWords} words in total.`
			: `Return one sentence not longer than ${maxWords} words.`;

	const sys = `${rules}\n\n${ask}`;
	const user = (n) => `Attempt ${n}: Describe the photo.`;

	let lastErr;
	for (let a = 1; a <= retries; a++) {
		try {
			const rsp = await openai.chat.completions.create({
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
			const out = rsp.choices[0].message.content.replace(/\s+/g, " ").trim();
			const words = out.split(/\s+/).length;
			const sents = out.split(/[.!?](?:\s|$)/).filter(Boolean).length;
			if (
				words <= maxWords &&
				sents === (detailLevel === "comprehensive" ? 2 : 1)
			)
				return out;
		} catch (e) {
			lastErr = e;
		}
	}
	console.warn("[visionSafe] fallback:", lastErr?.message);
	return "A person looks ahead with a neutral expression under even studio lighting.";
}

/* ------------------------------------------------------------------ */
/* 2️⃣  Cloudinary caption helper (unchanged logic)                    */
/* ------------------------------------------------------------------ */
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
	/* build ID ------------------------------------------------------- */
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

		/* read Cloudinary’s own caption & face data -------------------- */
		const capObj = uploadRes?.info?.detection?.captioning;
		const cldCaption =
			capObj?.data?.caption ||
			(Array.isArray(capObj?.data) && capObj.data[0]?.caption) ||
			"";

		const faceData = uploadRes?.info?.detection?.adv_face?.data;
		let faceClause = "";
		if (Array.isArray(faceData) && faceData.length) {
			const f = faceData[0];
			const age = f?.age ? `${f.age}`.replace(/\.\d+$/, "") : "";
			const gender = f?.gender?.value ? f.gender.value.toLowerCase() : "";
			const race = f?.race?.value ? f.race.value : "";
			faceClause = [age && `${age}s`, race, gender].filter(Boolean).join(" ");
		}

		let gptDesc = "";
		if (enhance) {
			gptDesc = await safeDescribeSeedImage(uploadRes.secure_url, {
				detailLevel: "comprehensive",
				maxWords: 120,
				includeSensitive,
				...openaiOptions,
			});
		}

		const description = [faceClause, cldCaption, gptDesc]
			.filter(Boolean)
			.join(" — ")
			.replace(/\s+—\s*$/, "");

		return { secureUrl: uploadRes.secure_url, description };
	} catch (err) {
		console.error("[Cloudinary] captioning failed:", err.message);
		if (!openAIFallback) throw err;

		if (!uploadRes) {
			uploadRes = await cloudinary.uploader.upload(src, {
				folder,
				public_id: derivedId,
				use_filename: useFilename,
				unique_filename: uniqueFilename,
				overwrite: false,
			});
		}
		const fallbackDesc = await safeDescribeSeedImage(uploadRes.secure_url, {
			detailLevel: "comprehensive",
			maxWords: 120,
			includeSensitive,
			...openaiOptions,
		});
		return { secureUrl: uploadRes.secure_url, description: fallbackDesc };
	}
}

/* ------------------------------------------------------------------ */
/* 3️⃣  upload → multi‑variation → QA → brighten → re‑upload           */
/* ------------------------------------------------------------------ */
async function faceLooksOk(url) {
	const ask = `
Yes / No only — Does the person's face in this photo look **natural** with no obvious distortions in the eyes, nose or mouth?`.trim();

	const rsp = await openai.chat.completions.create({
		model: "gpt-4o",
		messages: [
			{
				role: "user",
				content: [
					{ type: "text", text: ask },
					{ type: "image_url", image_url: { url } },
				],
			},
		],
	});
	return rsp.choices[0].message.content.toLowerCase().startsWith("yes");
}

async function uploadWithVariation(
	src,
	{ folder = "aivideomatic", size = 1024, tries = 3 } = {}
) {
	/* 1. upload original ------------------------------------------- */
	const stamp = Date.now();
	const origUpload = await cloudinary.uploader.upload(src, {
		folder,
		public_id: `${stamp}_orig`,
		resource_type: "image",
	});

	/* 2. make square PNG for DALL·E -------------------------------- */
	const squareUrl = cloudinary.url(origUpload.public_id, {
		secure: true,
		width: size,
		height: size,
		crop: "fill",
		gravity: "auto",
		fetch_format: "png",
	});
	const squareBuf = (
		await axios.get(squareUrl, { responseType: "arraybuffer" })
	).data;
	const tmpSquare = path.join(os.tmpdir(), `img-${stamp}.png`);
	await fs.promises.writeFile(tmpSquare, squareBuf);

	/* 3. generate 1‑3 variations until one passes QA --------------- */
	let chosenUrl = null;
	for (let a = 1; a <= tries; a++) {
		const rsp = await openai.images.createVariation({
			image: fs.createReadStream(tmpSquare),
			n: 1,
			size: `${size}x${size}`,
		});
		const url = rsp.data[0].url;

		try {
			if (await faceLooksOk(url)) {
				chosenUrl = url;
				break;
			}
			console.warn(`[Variation QA] try ${a} failed — face looks odd`);
		} catch (e) {
			console.warn("[Variation QA] skipped:", e.message);
			chosenUrl = url; // if vision fails, keep this one
			break;
		}
		chosenUrl = url; // last resort if all tries fail
	}

	/* 4. subtle brightening (~5 %) via Cloudinary ------------------ */
	const brightened = cloudinary.url(chosenUrl, {
		type: "fetch",
		secure: true,
		effect: "brightness:8", // tiny boost; 0‑100
		fetch_format: "png",
	});

	/* 5. upload variant (brightened) ------------------------------- */
	const varUpload = await cloudinary.uploader.upload(brightened, {
		folder,
		public_id: `${stamp}_variant`,
		resource_type: "image",
	});

	return {
		original: { public_id: origUpload.public_id, url: origUpload.secure_url },
		variant: { public_id: varUpload.public_id, url: varUpload.secure_url },
	};
}

/* ------------------------------------------------------------------ */
/* 4️⃣  Prompt helper (unchanged)                                     */
/* ------------------------------------------------------------------ */
function injectSeedDescription(runwayPrompt, seedDesc) {
	if (!seedDesc) return runwayPrompt;
	const hasHuman =
		/\b(male|female|person|man|woman|anchor|reporter|human)\b/i.test(
			runwayPrompt
		);
	return hasHuman ? runwayPrompt : `${seedDesc}, ${runwayPrompt}`;
}

/* ------------------------------------------------------------------ */
module.exports = {
	safeDescribeSeedImage,
	describeImageViaCloudinary,
	injectSeedDescription,
	uploadWithVariation,
};
