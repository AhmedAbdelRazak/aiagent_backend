/** @format */

const cloudinary = require("cloudinary");

// config
cloudinary.config({
	cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
	api_key: process.env.CLOUDINARY_API_KEY,
	api_secret: process.env.CLOUDINARY_API_SECRET,
});

exports.uploadImages = async (req, res) => {
	try {
		const result = await cloudinary.uploader.upload(req.body.image, {
			public_id: `ai_agent/${Date.now()}`,
			resource_type: "auto", // let Cloudinary handle the format
		});

		// Return the public_id & secure_url to the client
		return res.json({
			public_id: result.public_id,
			url: result.secure_url,
		});
	} catch (err) {
		console.error("Cloudinary upload error:", err);
		return res.status(400).json({ error: "Upload to Cloudinary failed" });
	}
};

exports.remove = (req, res) => {
	let image_id = req.body.public_id;
	// For debugging:
	console.log("Removing image:", image_id);

	cloudinary.uploader.destroy(image_id, (err, result) => {
		if (err) {
			console.error("Cloudinary remove error:", err);
			return res.json({ success: false, err });
		}
		// Or return any JSON you prefer:
		res.json({ success: true, result });
	});
};
