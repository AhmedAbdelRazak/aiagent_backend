/* routes/googleTrendsPuppeteer.js — bullet‑proof, tested 2025‑06‑23 */
/* eslint-disable no-console, max-len */

require("dotenv").config();
const express = require("express");
const puppeteer = require("puppeteer-extra");
const Stealth = require("puppeteer-extra-plugin-stealth");
puppeteer.use(Stealth());

const router = express.Router();
const ROW_LIMIT = 5; // how many rising‑search rows we scrape
const ROW_TIMEOUT_MS = 12_000; // per‑row timeout (ms)
const PROTOCOL_TIMEOUT = 120_000; // whole‑browser cap (ms)
const log = (...m) => console.log("[Trends]", ...m);

/* ────────────────────────────────────────────────────────────────── helpers */
const urlFor = ({ geo, hours, category }) => {
	const hrs = Math.min(Math.max(+hours || 168, 1), 168);
	return (
		`https://trends.google.com/trending?geo=${geo}&hl=en&hours=168&sort=recency` +
		(category ? `&category=${category}` : "")
	);
};

/* ─────────────────────────────────────────────────────────── cached browser */
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

/* ──────────────────────────────────────────────────────────────── scraper */
async function scrape({ geo, hours, category }) {
	const page = await (await getBrowser()).newPage();
	page.setDefaultNavigationTimeout(PROTOCOL_TIMEOUT);

	/* relay browser console messages for debugging */
	page.on("console", (msg) => log("Page>", msg.text()));

	/* block heavy resources for speed */
	await page.setRequestInterception(true);
	page.on("request", (r) =>
		["font", "media", "stylesheet"].includes(r.resourceType())
			? r.abort()
			: r.continue()
	);

	const targetURL = urlFor({ geo, hours, category });
	log("Navigate:", targetURL);
	await page.goto(targetURL, { waitUntil: "domcontentloaded" });

	await page.waitForSelector('tr[role="row"][data-row-id]', {
		timeout: 60_000,
	});

	/* extract the first ROW_LIMIT keywords */
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

	const stories = [];

	for (const { id, term } of rows) {
		if (!term) continue;

		const result = await page.evaluate(
			/* eslint-disable no-undef */
			async (rowId, rowTerm, rowMs) => {
				const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
				const deadline = Date.now() + rowMs;

				/* click the keyword cell ------------------------------------------------*/
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

				/* wait for the details dialog to appear --------------------------------*/
				while (Date.now() < deadline) {
					const dialog = document.querySelector(
						'div[aria-modal="true"][role="dialog"][aria-label]'
					);

					if (
						dialog &&
						dialog.getAttribute("aria-label").trim().toLowerCase() ===
							rowTerm.toLowerCase()
					) {
						/* allow ≤1 s for news cards to render */
						let anchors = [];
						const t2 = Date.now() + 1_000;
						while (Date.now() < t2) {
							anchors = [
								...dialog.querySelectorAll('a[target="_blank"][href^="http"]'),
							];
							if (anchors.length) break;
							await sleep(120);
						}

						const arts = anchors.slice(0, 3).map((a) => ({
							title:
								a.querySelector('[role="heading"]')?.textContent.trim() ||
								a.querySelector("div.Q0LBCe")?.textContent.trim() ||
								a.textContent.trim(),
							url: a.href,
							image: a.querySelector("img")?.src || null,
						}));

						/* close the dialog (all layout variants) */
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

					/* if dialog vanished (virtual scroll) → reclick */
					if (!dialog) clickRow();
					await sleep(200);
				}
				return { status: "timeout" };
			},
			id,
			term,
			ROW_TIMEOUT_MS
			/* eslint-enable no-undef */
		);

		log(`Result for "${term}":`, result.status);

		if (result.status === "ok") {
			stories.push({
				title: term,
				image: result.image,
				entityNames: [term],
				articles: result.articles,
			});
		}

		/* wait until dialog truly closed before next loop */
		try {
			await page.waitForFunction(
				() => !document.querySelector('div[aria-modal="true"][role="dialog"]'),
				{ timeout: 5_000 }
			);
			await page.waitForTimeout(250);
		} catch {
			/* ignored */
		}
	}

	await page.close();
	return stories;
}

/* ───────────────────────────────────────────────────────────── express API */
router.get("/google-trends", async (req, res) => {
	const geo = (req.query.geo || "").toUpperCase();
	if (!/^[A-Z]{2}$/.test(geo))
		return res
			.status(400)
			.json({ error: "`geo` must be an ISO‑3166 alpha‑2 country code" });

	try {
		const stories = await scrape({
			geo,
			hours: req.query.hours,
			category: req.query.category,
		});

		res.json({
			generatedAt: new Date().toISOString(),
			requestedGeo: geo,
			effectiveGeo: geo, // Trends may redirect, but we fix geo
			hours: +req.query.hours || 168,
			category: req.query.category ?? null,
			stories,
		});
	} catch (err) {
		console.error(err);
		res.status(500).json({
			error: "Google Trends scraping failed",
			detail: err.message,
		});
	}
});

module.exports = router;
