// controllers/chatController.js
const Chat = require("../models/Chat");
const Message = require("../models/Message");
const SupportCase = require("../models/SupportCase");

// Get messages for a chat (only participants), auto-mark as viewed by current user
exports.getMessages = async (req, res, next) => {
	try {
		const { chatId } = req.params;
		const chat = await Chat.findById(chatId).populate("participants", "name");
		if (!chat) {
			return res.status(404).json({ error: "Chat not found" });
		}

		const userId = req.user._id.toString();
		if (!chat.participants.some((p) => p._id.toString() === userId)) {
			return res.status(403).json({ error: "Not authorized" });
		}

		const messages = await Message.find({ chat: chatId })
			.sort("createdAt")
			.populate("sender", "name email");

		// Mark each message as viewed by this user if not already
		const toUpdate = [];
		for (const msg of messages) {
			if (!msg.viewedBy.map((id) => id.toString()).includes(userId)) {
				msg.viewedBy.push(req.user._id);
				toUpdate.push(msg.save());
			}
		}
		await Promise.all(toUpdate);

		res.json({ success: true, data: messages });
	} catch (err) {
		next(err);
	}
};

// Send a new message (client or assigned admin)
// Admin can only respond if assigned (or if case was open, they auto-assign themselves)
exports.sendMessage = async (req, res, next) => {
	try {
		const { chatId, content } = req.body;
		if (!chatId || !content) {
			return res.status(400).json({ error: "chatId and content are required" });
		}

		const chat = await Chat.findById(chatId).populate("supportCase");
		if (!chat) {
			return res.status(404).json({ error: "Chat not found" });
		}

		const supportCase = await SupportCase.findById(chat.supportCase);
		if (!supportCase) {
			return res
				.status(500)
				.json({ error: "Support case not found for this chat" });
		}

		const userId = req.user._id.toString();
		const isAdmin = req.user.role === "admin";
		const isClient = supportCase.client.toString() === userId;

		if (isAdmin) {
			// If case was open, auto-assign admin
			if (!supportCase.assignedAdmin) {
				supportCase.assignedAdmin = req.user._id;
				supportCase.status = "in_progress";
				await supportCase.save();
				if (!chat.participants.includes(req.user._id)) {
					chat.participants.push(req.user._id);
					await chat.save();
				}
			} else if (supportCase.assignedAdmin.toString() !== userId) {
				return res
					.status(403)
					.json({ error: "Only the assigned admin may respond" });
			}
			if (supportCase.status === "closed") {
				return res
					.status(400)
					.json({ error: "Cannot send messages on a closed case" });
			}
		} else if (isClient) {
			if (supportCase.status === "closed") {
				return res
					.status(400)
					.json({ error: "Cannot send messages on a closed case" });
			}
		} else {
			return res.status(403).json({ error: "Not authorized" });
		}

		// Create the message
		const message = new Message({
			chat: chatId,
			sender: req.user._id,
			content,
			// `viewedBy` gets auto-initialized to [sender] via pre("save")
		});
		await message.save();

		// Emit via Socket.IO
		const populated = await message.populate("sender", "name email");
		req.app.get("io").to(chatId).emit("receiveMessage", {
			chatId,
			message: populated,
		});

		res.status(201).json({ success: true, data: message });
	} catch (err) {
		next(err);
	}
};

// Mark a single message as viewed/read by current user
exports.markMessageAsRead = async (req, res, next) => {
	try {
		const { messageId } = req.params;
		const message = await Message.findById(messageId);
		if (!message) {
			return res.status(404).json({ error: "Message not found" });
		}

		const chat = await Chat.findById(message.chat);
		const userId = req.user._id.toString();
		if (!chat.participants.map((p) => p.toString()).includes(userId)) {
			return res.status(403).json({ error: "Not authorized" });
		}

		if (!message.viewedBy.map((id) => id.toString()).includes(userId)) {
			message.viewedBy.push(req.user._id);
			await message.save();
		}

		res.json({ success: true, data: message });
	} catch (err) {
		next(err);
	}
};
