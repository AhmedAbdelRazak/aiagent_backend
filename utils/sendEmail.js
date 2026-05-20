const sgMail = require("@sendgrid/mail");

exports.sendEmail = async ({ to, subject, html }) => {
	const apiKey = process.env.SENDGRID_API_KEY;
	const from =
		process.env.SENDGRID_FROM_EMAIL ||
		process.env.EMAIL_FROM ||
		process.env.MAIL_FROM;

	if (!apiKey) {
		console.warn("Email skipped: SENDGRID_API_KEY is not configured.");
		return { skipped: true, reason: "missing_api_key" };
	}

	if (!from) {
		console.warn(
			"Email skipped: configure SENDGRID_FROM_EMAIL, EMAIL_FROM, or MAIL_FROM.",
		);
		return { skipped: true, reason: "missing_sender" };
	}

	sgMail.setApiKey(apiKey);

	const msg = {
		to,
		from,
		subject,
		html,
	};

	try {
		await sgMail.send(msg);
		console.log(`Email sent to ${to}: ${subject}`);
		return { sent: true };
	} catch (err) {
		console.error("Error sending email:", err);
		return { sent: false, error: err };
	}
};
