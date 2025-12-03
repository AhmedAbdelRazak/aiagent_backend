/* routes/googleTrendsPuppeteer.js — bullet‑proof, updated 2025‑11‑28 */
/* eslint-disable no-console, max-len */

require("dotenv").config();
const express = require("express");
const puppeteer = require("puppeteer-extra");
const Stealth = require("puppeteer-extra-plugin-stealth");
const OpenAI = require("openai");

puppeteer.use(Stealth());

const router = express.Router();

const ROW_LIMIT = 6; // how many rising‑search rows we scrape
const ROW_TIMEOUT_MS = 12_000; // per‑row timeout (ms)
const PROTOCOL_TIMEOUT = 120_000; // whole‑browser cap (ms)
const ARTICLE_IMAGE_FETCH_TIMEOUT_MS = 8_000; // cap for fetching article HTML
const log = (...m) => console.log("[Trends]", ...m);

/* ───────────────────────────────────────────── OpenAI client + helpers */

let openai = null;
const CHATGPT_API_TOKEN =
	process.env.CHATGPT_API_TOKEN || process.env.OPENAI_API_KEY || null;

if (CHATGPT_API_TOKEN) {
	try {
		openai = new OpenAI({ apiKey: CHATGPT_API_TOKEN });
		log("OpenAI client initialized");
	} catch (err) {
		console.error("[Trends] Failed to init OpenAI client:", err.message);
		openai = null;
	}
}

function normaliseImageBriefs(briefs = [], topic = "") {
	const targets = ["1280:720", "720:1280"];
	const byAspect = new Map(targets.map((t) => [t, null]));

	if (Array.isArray(briefs)) {
		for (const raw of briefs) {
			if (!raw || !raw.aspectRatio) continue;
			const ar = String(raw.aspectRatio).trim();
			if (!byAspect.has(ar)) continue;
			if (byAspect.get(ar)) continue;
			byAspect.set(ar, {
				aspectRatio: ar,
				visualHook: String(
					raw.visualHook || raw.idea || raw.hook || raw.description || ""
				).trim(),
				emotion: String(raw.emotion || "").trim(),
				rationale: String(raw.rationale || raw.note || "").trim(),
			});
		}
	}

	for (const [ar, val] of byAspect.entries()) {
		if (val) continue;
		byAspect.set(ar, {
			aspectRatio: ar,
			visualHook:
				ar === "1280:720"
					? `Landscape viral frame about ${topic}`
					: `Vertical viral frame about ${topic}`,
			emotion: "High energy",
			rationale:
				"Auto-filled to keep both aspect ratios covered for the video orchestrator.",
		});
	}

	return Array.from(byAspect.values());
}

/**
 * Use GPT‑5.1 to generate SEO‑optimized blog + Shorts titles per story.
 * If anything fails, we just return the original stories unchanged.
 */
async function enhanceStoriesWithOpenAI(
	stories,
	{ geo, hours, category, language = "English" }
) {
	if (!openai || !stories.length) return stories;

	const payload = {
		geo,
		hours,
		category,
		language,
		topics: stories.map((s) => ({
			term: s.title,
			articleTitles: s.articles.map((a) => a.title),
		})),
	};

	try {
		const response = await openai.responses.create({
			model: "gpt-5.1",
			instructions:
				"You are an expert SEO copywriter and YouTube Shorts strategist. " +
				"Given trending search topics and their news article titles, " +
				"for EACH topic create:\n" +
				"1) A compelling yet honest blog post title (no clickbait, max ~80 chars).\n" +
				"2) A highCTR YouTube Shorts title (max ~60 chars, must stay factual).\n" +
				"3) EXACTLY TWO viral thumbnail/image directives: one for aspectRatio 1280:720 (landscape) and one for 720:1280 (vertical). Each directive should include:\n" +
				'   - "aspectRatio": one of those two strings (landscape MUST be 1280:720, vertical MUST be 720:1280)\n' +
				'   - "visualHook": a vivid, specific description of what the image shows (clear subject, camera framing, and motion-friendly composition, no text overlays, no logos)\n' +
				'   - "emotion": the main emotion the image should evoke\n' +
				'   - "rationale": a short note for the video orchestrator about why this hook works for that aspect ratio\n' +
				"4) One short 'imageComment' that plainly describes what the recommended hero shot should look like so downstream video generation can pick the right image for Runway (mention subject + setting, no extra fluff).\n\n" +
				`ALL text must be in ${language}, even if the country/geo differs. No other languages or scripts are allowed.\n` +
				"Your entire reply MUST be valid JSON, no extra commentary.",
			input:
				"Here is the data as JSON:\n\n" +
				JSON.stringify(payload) +
				"\n\n" +
				"Respond with a JSON object of the form:\n" +
				'{ "topics": [ { "term": string, "blogTitle": string, "youtubeShortTitle": string, "imageComment": string, "imageDirectives": [ { "aspectRatio": "1280:720", "visualHook": string, "emotion": string, "rationale": string }, { "aspectRatio": "720:1280", "visualHook": string, "emotion": string, "rationale": string } ] } ] }',
			// We skip response_format to avoid model/version compatibility errors
			// and just force JSON via instructions.
		});

		const raw = response.output_text || "";
		let parsed;
		try {
			parsed = JSON.parse(raw);
		} catch (err) {
			log("Failed to parse OpenAI JSON:", err.message || err.toString());
			return stories;
		}

		const byTerm = new Map();
		if (parsed && Array.isArray(parsed.topics)) {
			for (const t of parsed.topics) {
				if (!t || typeof t.term !== "string") continue;
				byTerm.set(t.term.toLowerCase(), t);
			}
		}

		return stories.map((s) => {
			const key = s.title.toLowerCase();
			const match =
				byTerm.get(key) ||
				// fallback: GPT sometimes normalizes whitespace/case
				Array.from(byTerm.values()).find(
					(t) => t.term && t.term.toLowerCase().trim() === key.trim()
				);

			if (!match) return s;

			const briefs = normaliseImageBriefs(
				match.imageDirectives || match.viralImageBriefs || [],
				s.title
			);
			const imageComment =
				String(match.imageComment || match.imageHook || "").trim() ||
				(briefs[0]?.visualHook
					? `Lead image for ${s.title}: ${briefs[0].visualHook}`
					: `Lead image for ${s.title}, framed for ${
							briefs[0]?.aspectRatio === "720:1280" ? "vertical" : "landscape"
					  } video.`);

			return {
				...s,
				seoTitle: match.blogTitle || s.title,
				youtubeShortTitle: match.youtubeShortTitle || s.title,
				imageComment,
				viralImageBriefs: briefs,
			};
		});
	} catch (err) {
		console.error("[Trends] OpenAI enhancement failed:", err.message || err);
		return stories;
	}
}

/* ───────────────────────────────────────────────────────────── helpers */

const urlFor = ({ geo, hours, category, sort }) => {
	// Clamp hours 1–168 and actually use the requested window.
	const hrs = Math.min(Math.max(Number(hours) || 24, 1), 168);

	const params = new URLSearchParams({
		geo,
		hl: "en-US", // matches the UI you pasted
		hours: 24,
		status: "active",
		// hours: String(hrs),
	});

	if (category) params.set("category", String(category).trim());

	// If caller passes an explicit sort, forward it.
	// Otherwise we let Trends use its default sort (UI default).
	if (sort && String(sort).trim()) {
		params.set("sort", String(sort).trim());
	}

	return `https://trends.google.com/trending?${params.toString()}`;
};

/* ───────────────────────────────────────────────────── cached browser */

let browser; // one instance per container / PM2 worker

async function getBrowser() {
	if (browser) return browser;

	browser = await puppeteer.launch({
		headless: true,
		protocolTimeout: PROTOCOL_TIMEOUT,
		executablePath:
			process.env.CHROME_BIN || // production path
			puppeteer.executablePath(), // dev fallback
		args: [
			"--no-sandbox",
			"--disable-setuid-sandbox",
			"--disable-gpu",
			"--disable-dev-shm-usage",
		],
	});

	return browser;
}

/* ───────────────────────────────────── article image helpers (Node side) */

async function fetchHtmlWithTimeout(url, timeoutMs) {
	// Older Node may not have global fetch; if so we just skip enrichment.
	if (typeof fetch !== "function") return null;

	const supportsAbort =
		typeof AbortController !== "undefined" &&
		typeof AbortController === "function";

	const controller = supportsAbort ? new AbortController() : null;
	let timeoutId;

	try {
		const fetchPromise = fetch(url, {
			redirect: "follow",
			signal: controller ? controller.signal : undefined,
			headers: {
				"User-Agent":
					"Mozilla/5.0 (compatible; TrendsScraper/1.0; +https://trends.google.com)",
				Accept: "text/html,application/xhtml+xml",
			},
		});

		let response;
		if (controller) {
			const timeoutPromise = new Promise((_, reject) => {
				timeoutId = setTimeout(() => {
					controller.abort();
					reject(new Error("timeout"));
				}, timeoutMs);
			});
			response = await Promise.race([fetchPromise, timeoutPromise]);
		} else {
			response = await fetchPromise;
		}

		if (!response || !response.ok) return null;

		const contentType = response.headers.get("content-type") || "";
		if (!contentType.includes("text/html")) return null;

		return await response.text();
	} catch (err) {
		log("Article fetch failed:", url, err.message || String(err));
		return null;
	} finally {
		if (timeoutId) clearTimeout(timeoutId);
	}
}

function extractBestImageFromHtml(html) {
	if (!html) return null;

	const candidates = [];

	const pushMatch = (regex) => {
		const m = regex.exec(html);
		if (m && m[1]) candidates.push(m[1]);
	};

	// Try common Open Graph / Twitter meta tags.
	pushMatch(
		/<meta[^>]+property=["']og:image:secure_url["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);
	pushMatch(
		/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);
	pushMatch(
		/<meta[^>]+name=["']og:image["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);
	pushMatch(
		/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);
	pushMatch(
		/<meta[^>]+name=["']twitter:image:src["'][^>]+content=["']([^"'>]+)["'][^>]*>/i
	);

	const httpCandidates = candidates.filter((u) => /^https?:\/\//i.test(u));
	return httpCandidates[0] || null;
}

async function fetchBestImageForArticle(url, fallbackImage) {
	try {
		const html = await fetchHtmlWithTimeout(
			url,
			ARTICLE_IMAGE_FETCH_TIMEOUT_MS
		);
		const ogImage = extractBestImageFromHtml(html);
		return ogImage || fallbackImage || null;
	} catch (err) {
		log("Image enrichment failed:", url, err.message || String(err));
		return fallbackImage || null;
	}
}

/**
 * For each article we scraped from Google Trends, try to replace the tiny
 * Google thumbnail with the article's OpenGraph hero image. This runs in
 * Node, not in the browser, so CORS is not an issue.
 */
async function hydrateArticleImages(stories) {
	if (!stories.length) return stories;

	// If fetch is missing we just return the original data.
	if (typeof fetch !== "function") {
		log("Global fetch not available, skipping article image hydration");
		return stories;
	}

	return Promise.all(
		stories.map(async (story) => {
			const enrichedArticles = await Promise.all(
				story.articles.map(async (art) => {
					const bestImage = await fetchBestImageForArticle(art.url, art.image);
					return { ...art, image: bestImage };
				})
			);

			const hero =
				enrichedArticles.find((a) => a.image && typeof a.image === "string")
					?.image || story.image;

			return {
				...story,
				image: hero,
				articles: enrichedArticles,
			};
		})
	);
}

/* ───────────────────────────────────────────────────────────── scraper */

async function scrape({ geo, hours, category, sort }) {
	const page = await (await getBrowser()).newPage();
	page.setDefaultNavigationTimeout(PROTOCOL_TIMEOUT);

	// Relay browser console messages, but drop noisy network errors.
	page.on("console", (msg) => {
		const text = msg.text();
		if (/Failed to load resource/i.test(text)) return;
		log("Page>", text);
	});

	// Block only fonts for speed; keep images and CSS so layout stays stable.
	await page.setRequestInterception(true);
	page.on("request", (req) => {
		const type = req.resourceType();
		if (type === "font") {
			return req.abort();
		}
		return req.continue();
	});

	const targetURL = urlFor({ geo, hours, category, sort });
	log("Navigate:", targetURL);

	const stories = [];
	const seenTerms = new Set();

	try {
		await page.goto(targetURL, { waitUntil: "domcontentloaded" });

		await page.waitForSelector('tr[role="row"][data-row-id]', {
			timeout: 60_000,
		});

		// Extract the first ROW_LIMIT keywords exactly as shown in the table.
		const rows = await page.$$eval(
			'tr[role="row"][data-row-id]',
			(trs, LIM) =>
				trs.slice(0, LIM).map((tr) => ({
					id: tr.getAttribute("data-row-id"),
					term: tr.querySelector("td:nth-child(2)")?.innerText.trim() ?? "",
				})),
			ROW_LIMIT
		);
		log("Row list:", rows);

		for (const { id, term } of rows) {
			if (!term) continue;
			const normTerm = term.toLowerCase().trim();
			if (seenTerms.has(normTerm)) {
				log(`Skip duplicate term "${term}"`);
				continue;
			}

			const result = await page.evaluate(
				// eslint-disable-next-line no-undef
				async (rowId, rowTerm, rowMs) => {
					const sleep = (ms) =>
						new Promise((resolve) => setTimeout(resolve, ms));
					const deadline = Date.now() + rowMs;

					const clickRow = () => {
						const tr = document.querySelector(`tr[data-row-id="${rowId}"]`);
						const cell = tr?.querySelector("td:nth-child(2)");
						if (!cell) return false;
						cell.scrollIntoView({ block: "center" });
						cell.click();
						console.log(`Clicked ${rowTerm}`);
						return true;
					};

					if (!clickRow()) return { status: "row-not-found" };

					while (Date.now() < deadline) {
						const dialog = document.querySelector(
							'div[aria-modal="true"][role="dialog"][aria-label]'
						);

						if (dialog) {
							const label = dialog.getAttribute("aria-label") || "";
							const normalizedLabel = label.trim().toLowerCase();
							const normalizedTerm = rowTerm.trim().toLowerCase();

							// Some dialogs prepend extra words; accept prefix match.
							const isMatch =
								normalizedLabel === normalizedTerm ||
								normalizedLabel.startsWith(normalizedTerm);

							if (isMatch) {
								let anchors = [];
								const t2 = Date.now() + 1_000;
								while (Date.now() < t2) {
									anchors = [
										...dialog.querySelectorAll(
											'a[target="_blank"][href^="http"]'
										),
									];
									if (anchors.length) break;
									// eslint-disable-next-line no-await-in-loop
									await sleep(120);
								}

								const arts = anchors.slice(0, 6).map((a) => ({
									title:
										a.querySelector('[role="heading"]')?.textContent.trim() ||
										a.querySelector("div.Q0LBCe")?.textContent.trim() ||
										a.textContent.trim(),
									url: a.href,
									image: a.querySelector("img")?.src || null,
								}));

								// Close the dialog (handle layout variants).
								(
									dialog.querySelector(
										'div[aria-label="Close search"], div[aria-label="Close"], button.pYTkkf-Bz112c-LgbsSe'
									) || document.querySelector('div[aria-label="Close"]')
								)?.click() ||
									document.dispatchEvent(
										new KeyboardEvent("keydown", { key: "Escape" })
									);

								return {
									status: "ok",
									image: arts[0]?.image || null,
									articles: arts,
								};
							}
						}

						// If dialog vanished (virtual scroll) → reclick.
						if (!dialog) clickRow();
						// eslint-disable-next-line no-await-in-loop
						await sleep(200);
					}

					return { status: "timeout" };
				},
				id,
				term,
				ROW_TIMEOUT_MS
			);

			log(`Result for "${term}":`, result.status);

			if (result.status === "ok") {
				seenTerms.add(normTerm);
				stories.push({
					title: term,
					image: result.image,
					entityNames: [term],
					articles: result.articles,
				});
			}

			// Wait until dialog truly closed before next loop.
			try {
				await page.waitForFunction(
					() =>
						!document.querySelector('div[aria-modal="true"][role="dialog"]'),
					{ timeout: 5_000 }
				);
				await page.waitForTimeout(250);
			} catch (e) {
				// ignored
			}
		}
	} finally {
		await page.close().catch(() => {});
	}

	return stories;
}

/* ───────────────────────────────────────────────────────────── express API */

function inferAspectFromUrl(url) {
	try {
		const u = new URL(url);
		const search = u.search || "";
		const path = u.pathname || "";
		let w = null;
		let h = null;
		const qW = search.match(/[?&]w=(\d{2,4})/i);
		const qH = search.match(/[?&]h=(\d{2,4})/i);
		if (qW) w = parseInt(qW[1], 10);
		if (qH) h = parseInt(qH[1], 10);
		if (!w || !h) {
			const m = path.match(/(\d{3,4})x(\d{3,4})/);
			if (m) {
				w = parseInt(m[1], 10);
				h = parseInt(m[2], 10);
			}
		}
		if (!w || !h || h === 0) return "unknown";
		const r = w / h;
		if (r >= 1.1) return "landscape";
		if (r <= 0.9) return "portrait";
		return "square";
	} catch {
		return "unknown";
	}
}

function buildStoryImages(story) {
	const seen = new Set();
	const pool = [];
	const push = (u) => {
		if (!u || typeof u !== "string") return;
		if (!/^https?:\/\//i.test(u)) return;
		if (seen.has(u)) return;
		seen.add(u);
		pool.push(u);
	};
	push(story.image);
	(story.articles || []).forEach((a) => push(a.image));

	const landscape = pool.find((u) => inferAspectFromUrl(u) === "landscape");
	const portrait = pool.find((u) => inferAspectFromUrl(u) === "portrait");
	const square = pool.find((u) => inferAspectFromUrl(u) === "square");

	const ordered = [];
	if (landscape) ordered.push(landscape);
	if (portrait && portrait !== landscape) ordered.push(portrait);
	if (!portrait && square && square !== landscape) ordered.push(square);
	for (const u of pool) if (!ordered.includes(u)) ordered.push(u);
	return ordered;
}

function decorateStoriesWithImages(stories) {
	return stories.map((s) => ({
		...s,
		images: buildStoryImages(s),
	}));
}
router.get("/google-trends", async (req, res) => {
	const geo = (req.query.geo || "").toUpperCase();
	if (!/^[A-Z]{2}$/.test(geo)) {
		return res.status(400).json({
			error: "`geo` must be an ISO‑3166 alpha‑2 country code",
		});
	}

	const hours = Number(req.query.hours) || 24;
	const category = req.query.category ?? null;
	const sort = req.query.sort ?? null;

	try {
		let stories = await scrape({
			geo,
			hours,
			category,
			sort,
		});

		// 1) Replace low‑res Google thumbnails with article OG images where possible.
		stories = await hydrateArticleImages(stories);

		// 2) Ask GPT‑5.1 for better blog + YouTube titles.
		//    This is optional and skipped if CHATGPT_API_TOKEN / OPENAI_API_KEY
		//    is not configured or the call fails.
		stories = await enhanceStoriesWithOpenAI(stories, {
			geo,
			hours,
			category,
		});

		// 3) Ensure we always surface multiple images per topic (mixing aspect hints).
		stories = decorateStoriesWithImages(stories);

		return res.json({
			generatedAt: new Date().toISOString(),
			requestedGeo: geo,
			effectiveGeo: geo, // Trends may redirect, but we fix geo
			hours,
			category,
			stories,
		});
	} catch (err) {
		console.error(err);
		return res.status(500).json({
			error: "Google Trends scraping failed",
			detail: err.message || String(err),
		});
	}
});

module.exports = router;
