/** @format */

const cloudinary = require("cloudinary");
const crypto = require("crypto");

// config
cloudinary.config({
	cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
	api_key: process.env.CLOUDINARY_API_KEY,
	api_secret: process.env.CLOUDINARY_API_SECRET,
});

exports.uploadImages = async (req, res) => {
	try {
		const image = String(req.body.image || "");
		if (!/^data:image\/(png|jpe?g|webp|gif);base64,/i.test(image)) {
			return res.status(400).json({ error: "A valid image data URL is required" });
		}

		const publicId = `aivideomatic/${req.user._id}/${Date.now()}-${crypto.randomUUID()}`;
		const result = await cloudinary.uploader.upload(image, {
			public_id: publicId,
			resource_type: "image",
			overwrite: false,
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
	const image_id = String(req.body.public_id || "").trim();
	if (!/^aivideomatic\/[a-zA-Z0-9/_-]+$/.test(image_id)) {
		return res.status(400).json({ error: "Invalid image id" });
	}

	cloudinary.uploader.destroy(image_id, (err, result) => {
		if (err) {
			console.error("Cloudinary remove error:", err);
			return res.status(400).json({ success: false, error: "Image remove failed" });
		}
		// Or return any JSON you prefer:
		res.json({ success: true, result });
	});
};
