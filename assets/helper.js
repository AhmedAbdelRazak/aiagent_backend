// assets/helper.js
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
/*  OPENAI utilities (unchanged: you already had these)               */
/* ------------------------------------------------------------------ */
const openai = new OpenAI({ apiKey: process.env.CHATGPT_API_TOKEN });

async function safeDescribeSeedImage(
	imageUrl,
	{ maxWords = 60, model = "gpt-4o", retries = 2 } = {}
) {
	const systemPrompt = `
You are a professional photo‑captioning assistant.

**Hard rules**
• DO NOT identify or name any real person.
• DO NOT mention ethnicity, race, religion, disability, nationality.
• DO NOT guess unseen facts or use speculative words (“looks like”, “maybe”).

Allowed visible attributes (use any that apply):
─ age *range* │ gender *impression* │ hair colour & style │ attire
─ pose │ facial expression │ environment │ lighting │ colour palette │ camera angle │ mood.

Return **one sentence** not longer than ${maxWords} words.
`;

	const userPrompt = (attempt) => `Attempt ${attempt}: Describe the photo.`;

	let lastErr;
	for (let a = 1; a <= retries; a++) {
		try {
			const rsp = await openai.chat.completions.create({
				model,
				messages: [
					{ role: "system", content: systemPrompt.trim() },
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

			/* ---- minimal validation ---- */
			const wordCnt = out.split(/\s+/).length;
			const singleSentence = !/[.!?].+?[.!?]/.test(out);
			const noDisallowed =
				!/\b(Black|White|Asian|Hispanic|Arab|Jewish|Christian|Muslim|disabled|blind)\b/i.test(
					out
				) && !/[A-Z][a-z]+\s+[A-Z][a-z]+/.test(out); // crude proper‑name / sensitive check

			if (wordCnt <= maxWords && singleSentence && noDisallowed) return out;
			lastErr = new Error("validation failed");
		} catch (e) {
			lastErr = e;
		}
	}

	console.warn(
		"[visionSafe] fallback stub used – last error:",
		lastErr && lastErr.message
	);
	return "A person looks ahead with a neutral expression under even studio lighting.";
}

/* ------------------------------------------------------------------ */
/*  NEW: upload + Cloudinary‑first description                         */
/* ------------------------------------------------------------------ */

/**
 * Upload an image (file path OR public URL) to Cloudinary/videomatic,
 * return { secureUrl, description }.
 *
 * If Cloudinary’s captioning fails, falls back to OpenAI to describe
 * the same image (using the Cloudinary URL if available).
 *
 * @param {string} src – local path or URL.
 * @param {Object}  [options]
 * @param {boolean} [options.openAIFallback=true]
 * @param {Object}  [options.openaiOptions] – passed straight to safeDescribeSeedImage
 * @returns {Promise<{secureUrl:string, description:string}>}
 */
async function describeImageViaCloudinary(
	src,
	{ openAIFallback = true, openaiOptions = {} } = {}
) {
	let uploadRes;
	try {
		uploadRes = await cloudinary.uploader.upload(src, {
			folder: "videomatic", // auto‑creates the folder if it doesn’t exist :contentReference[oaicite:0]{index=0}
			detection: "captioning", // triggers AI Captioning add‑on :contentReference[oaicite:1]{index=1}
		});

		const captionObj = uploadRes?.info?.detection?.captioning;
		const caption =
			captionObj?.data?.caption ||
			(Array.isArray(captionObj?.data) && captionObj.data[0]?.caption);

		if (caption) {
			return { secureUrl: uploadRes.secure_url, description: caption };
		}
		throw new Error("No caption returned in Cloudinary response");
	} catch (err) {
		console.error("[Cloudinary] captioning failed:", err.message);

		if (!openAIFallback) throw err;

		// Ensure we have a public URL to feed GPT‑4o:
		const urlForOpenAI =
			uploadRes?.secure_url ||
			(/^https?:\/\//i.test(src) ? src : null) ||
			(await cloudinary.uploader.upload(src, { folder: "videomatic" }))
				.secure_url;

		const fallbackDesc = await safeDescribeSeedImage(
			urlForOpenAI,
			openaiOptions
		);
		return { secureUrl: urlForOpenAI, description: fallbackDesc };
	}
}

/* ------------------------------------------------------------------ */
/* Optional helper from your original file                            */
/* ------------------------------------------------------------------ */
function injectSeedDescription(runwayPrompt, seedDesc) {
	if (!seedDesc) return runwayPrompt;
	const hasHumanWord =
		/\b(male|female|person|man|woman|anchor|reporter|human)\b/i.test(
			runwayPrompt
		);
	return hasHumanWord ? runwayPrompt : `${seedDesc}, ${runwayPrompt}`;
}

module.exports = {
	safeDescribeSeedImage,
	injectSeedDescription,
	describeImageViaCloudinary,
};
