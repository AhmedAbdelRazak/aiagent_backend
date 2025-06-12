// routes/chat.js
const express = require("express");
const router = express.Router();
const {
	getMessages,
	sendMessage,
	markMessageAsRead,
} = require("../controllers/chatController");
const { protect } = require("../middlewares/authMiddleware");

// @route   GET /api/support/chats/:chatId/messages
router.get("/support/chats/:chatId/messages", protect, getMessages);

// @route   POST /api/support/chats/message
router.post("/support/chats/message", protect, sendMessage);

// @route   PUT /api/support/chats/message/:messageId/read
router.put(
	"/support/chats/message/:messageId/read",
	protect,
	markMessageAsRead
);

module.exports = router;
