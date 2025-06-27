// assets/helper.js  (only the helpers – keep your other imports as‑is)
require("dotenv").config();
const path = require("node:path");
const cloudinary = require("cloudinary").v2;
const { OpenAI } = require("openai");

/* ------------------------------------------------------------------ */
/* Cloudinary initialisation                                          */
/* ------------------------------------------------------------------ */
cloudinary.config({
	cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
	api_key: process.env.CLOUDINARY_API_KEY,
	api_secret: process.env.CLOUDINARY_API_SECRET,
});

/* ------------------------------------------------------------------ */
/* OpenAI initialisation                                              */
/* ------------------------------------------------------------------ */
const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });

/* ------------------------------------------------------------------ */
/* Small util: make a URL‑safe slug from a file/path/URL               */
/* ------------------------------------------------------------------ */
function slugify(str = "") {
	return str
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/(^-|-$)+/g, "");
}

/* ------------------------------------------------------------------ */
/* 1️⃣  Vision‑GPT captioning with two detail levels                   */
/* ------------------------------------------------------------------ */
async function safeDescribeSeedImage(
	imageUrl,
	{
		maxWords = 60,
		model = "gpt-4o",
		retries = 2,
		detailLevel = "concise", // ✨ NEW
	} = {}
) {
	/* ----  build the system prompt dynamically  ---- */
	const baseRules = `
You are a professional photo‑captioning assistant.

**Hard rules**
• DO NOT identify or name any real person.
• DO NOT mention ethnicity, race, religion, disability, nationality.
• DO NOT guess unseen facts or use speculative words (“looks like”, “maybe”).

Allowed visible attributes (use any that apply):
─ age *range* │ gender *impression* │ hair colour & style │ attire
─ pose │ facial expression │ environment │ lighting │ colour palette │ camera angle │ mood.
`.trim();

	const ask =
		detailLevel === "comprehensive"
			? `Return **two sentences** not longer than ${maxWords} words in total.`
			: `Return **one sentence** not longer than ${maxWords} words.`;

	const systemPrompt = `${baseRules}\n\n${ask}`;

	const userPrompt = (n) => `Attempt ${n}: Describe the photo.`;

	let lastErr;
	for (let a = 1; a <= retries; a++) {
		try {
			const rsp = await openai.chat.completions.create({
				model,
				messages: [
					{ role: "system", content: systemPrompt },
					{
						role: "user",
						content: [
							{ type: "text", text: userPrompt(a) },
							{ type: "image_url", image_url: { url: imageUrl } },
						],
					},
				],
			});

			const out = rsp.choices[0].message.content
				.replace(/["“”]/g, "")
				.replace(/\s+/g, " ")
				.trim();

			/* ---- minimal policy validation ---- */
			const wordCnt = out.split(/\s+/).length;
			const sentences = out.split(/[.!?](?:\s|$)/).filter(Boolean).length;
			const allowedSent = detailLevel === "comprehensive" ? 2 : 1;
			const noDisallowed =
				!/\b(Black|White|Asian|Hispanic|Arab|Jewish|Christian|Muslim|disabled|blind)\b/i.test(
					out
				) && !/[A-Z][a-z]+\s+[A-Z][a-z]+/.test(out); // crude proper‑name block

			if (wordCnt <= maxWords && sentences === allowedSent && noDisallowed)
				return out;

			lastErr = new Error("validation failed");
		} catch (e) {
			lastErr = e;
		}
	}

	console.warn(
		"[visionSafe] fallback stub used – last error:",
		lastErr?.message
	);
	return "A person looks ahead with a neutral expression under even studio lighting.";
}

/* ------------------------------------------------------------------ */
/* 2️⃣  Upload + caption + OPTIONAL GPT refinement                      */
/* ------------------------------------------------------------------ */

/**
 * Upload an image to Cloudinary (it is stored *regardless* of captioning),
 * return { secureUrl, description }.
 *
 * @param {string} src  – local path **or** public URL to the image
 * @param {Object} [options]
 * @param {string} [options.folder='videomatic']    – Cloudinary folder
 * @param {string|function} [options.publicId]      – explicit publicId OR a fn that receives `{base, ext}` and returns one
 * @param {boolean} [options.useFilename=false]     – use original filename (normalised)                        :contentReference[oaicite:0]{index=0}
 * @param {boolean} [options.uniqueFilename=true]   – let Cloudinary add a unique hash
 * @param {boolean} [options.openAIFallback=true]   – call GPT‑4o if the add‑on fails
 * @param {boolean} [options.enhance=true]          – run GPT‑4o even when Cloudinary gives a caption
 * @param {Object}  [options.openaiOptions]         – forwarded to safeDescribeSeedImage
 */
async function describeImageViaCloudinary(
	src,
	{
		folder = "videomatic",
		publicId,
		useFilename = false,
		uniqueFilename = true,
		openAIFallback = true,
		enhance = true,
		openaiOptions = {},
	} = {}
) {
	/* ---- 1. work out the public ID (file name) --------------------- */
	let derivedId = publicId;
	if (!derivedId) {
		if (typeof publicId === "function") {
			const parsed = path.parse(src);
			derivedId = publicId(parsed);
		} else if (useFilename) {
			// Cloudinary will take care of normalising + uniqueness
			derivedId = undefined;
		} else {
			// slug(from filename/url) + timestamp for readability AND uniqueness
			const base =
				path.extname(src) && !src.startsWith("http")
					? path.parse(src).name
					: slugify(new URL(src).pathname.split("/").pop() || "image");
			derivedId = `${base}-${Date.now()}`;
		}
	}

	/* ---- 2. initial upload + Cloudinary captioning ----------------- */
	let uploadRes;
	try {
		uploadRes = await cloudinary.uploader.upload(src, {
			folder,
			public_id: derivedId,
			use_filename: useFilename,
			unique_filename: uniqueFilename,
			detection: "captioning", // Cloudinary AI caption add‑on  :contentReference[oaicite:1]{index=1}
			overwrite: false,
		});

		const captionObj = uploadRes?.info?.detection?.captioning;
		const caption =
			captionObj?.data?.caption ||
			(Array.isArray(captionObj?.data) && captionObj.data[0]?.caption);

		/* ---- 3. optionally enrich via GPT ---------------------------- */
		let finalDesc = caption;
		if (enhance) {
			const gptDesc = await safeDescribeSeedImage(uploadRes.secure_url, {
				detailLevel: "comprehensive",
				maxWords: 120,
				...openaiOptions,
			});
			finalDesc = caption ? `${caption} — ${gptDesc}` : gptDesc;
		}

		return { secureUrl: uploadRes.secure_url, description: finalDesc };
	} catch (err) {
		console.error("[Cloudinary] captioning failed:", err.message);

		if (!openAIFallback) throw err;

		/* ---- 4. ensure the asset is STILL uploaded ------------------- */
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
			...openaiOptions,
		});

		return { secureUrl: uploadRes.secure_url, description: fallbackDesc };
	}
}

/* ------------------------------------------------------------------ */
/* 3️⃣  Optional helper (unchanged)                                    */
/* ------------------------------------------------------------------ */
function injectSeedDescription(runwayPrompt, seedDesc) {
	if (!seedDesc) return runwayPrompt;
	const hasHumanWord =
		/\b(male|female|person|man|woman|anchor|reporter|human)\b/i.test(
			runwayPrompt
		);
	return hasHumanWord ? runwayPrompt : `${seedDesc}, ${runwayPrompt}`;
}

/* ------------------------------------------------------------------ */
module.exports = {
	safeDescribeSeedImage,
	describeImageViaCloudinary,
	injectSeedDescription,
};
