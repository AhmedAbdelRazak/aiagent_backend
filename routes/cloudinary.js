/** @format */

const express = require("express");
const router = express.Router();

const { uploadImages, remove } = require("../controllers/cloudinary");

router.post("/uploadimage", uploadImages);
router.post("/removeimage", remove);

module.exports = router;
