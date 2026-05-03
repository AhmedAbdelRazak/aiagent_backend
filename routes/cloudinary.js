/** @format */

const express = require("express");
const router = express.Router();

const { uploadImages, remove } = require("../controllers/cloudinary");
const { protect } = require("../middlewares/authMiddleware");

router.post("/uploadimage", protect, uploadImages);
router.post("/removeimage", protect, remove);

module.exports = router;
