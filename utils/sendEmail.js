// utils/sendEmail.js
const sgMail = require("@sendgrid/mail");
sgMail.setApiKey(process.env.SENDGRID_API_KEY);

exports.sendEmail = async ({ to, subject, html }) => {
	const msg = {
		to,
		from: "no-reply@jannatbooking.com", // Change to a verified sender
		subject,
		html,
	};
	try {
		await sgMail.send(msg);
		console.log(`Email sent to ${to}: ${subject}`);
	} catch (err) {
		console.error("Error sending email:", err);
	}
};
