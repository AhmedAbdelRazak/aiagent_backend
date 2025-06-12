// controllers/supportCaseController.js
const SupportCase = require("../models/SupportCase");
const Chat = require("../models/Chat");
const Message = require("../models/Message");

// Client opens a new support case
exports.createCase = async (req, res, next) => {
	try {
		const { subject, description } = req.body;
		if (!subject || !description) {
			return res
				.status(400)
				.json({ error: "Subject and description are required" });
		}
		const supportCase = new SupportCase({
			client: req.user._id,
			subject,
			description,
			status: "open",
		});
		await supportCase.save();

		// Create a Chat associated with this case
		const chat = new Chat({
			supportCase: supportCase._id,
			participants: [req.user._id], // initially only the client
			isGroup: false,
		});
		await chat.save();

		res.status(201).json({
			success: true,
			data: { supportCase, chatId: chat._id },
		});
	} catch (err) {
		next(err);
	}
};

// Admin-only: get all open cases
exports.getOpenCases = async (req, res, next) => {
	try {
		const cases = await SupportCase.find({ status: "open" }).populate(
			"client",
			"name email"
		);
		res.json({ success: true, data: cases });
	} catch (err) {
		next(err);
	}
};

// Admin assigns themselves to a case (status → in_progress)
exports.assignCase = async (req, res, next) => {
	try {
		const { caseId } = req.params;
		const supportCase = await SupportCase.findById(caseId);
		if (!supportCase) {
			return res.status(404).json({ error: "Support case not found" });
		}
		if (supportCase.status !== "open") {
			return res.status(400).json({ error: "Case is not currently open" });
		}
		supportCase.assignedAdmin = req.user._id;
		supportCase.status = "in_progress";
		await supportCase.save();

		// Add admin to chat participants
		const chat = await Chat.findOne({ supportCase: caseId });
		if (!chat) {
			return res
				.status(500)
				.json({ error: "Chat not found for this support case" });
		}
		if (!chat.participants.includes(req.user._id)) {
			chat.participants.push(req.user._id);
			await chat.save();
		}

		res.json({ success: true, data: supportCase });
	} catch (err) {
		next(err);
	}
};

// Client or assignedAdmin (or any admin) can view a case
exports.getCaseById = async (req, res, next) => {
	try {
		const { caseId } = req.params;
		const supportCase = await SupportCase.findById(caseId).populate(
			"client assignedAdmin",
			"name email"
		);
		if (!supportCase) {
			return res.status(404).json({ error: "Support case not found" });
		}

		const userId = req.user._id.toString();
		const isAdmin = req.user.role === "admin";
		const isClient = supportCase.client._id.toString() === userId;
		const isAssignedAdmin =
			supportCase.assignedAdmin &&
			supportCase.assignedAdmin._id.toString() === userId;

		if (!isClient && !isAssignedAdmin && !isAdmin) {
			return res.status(403).json({ error: "Not authorized" });
		}

		res.json({ success: true, data: supportCase });
	} catch (err) {
		next(err);
	}
};

// Close or resolve a case:
// - Admin calls this to mark "resolved"
// - Client calls this (with rating/feedback) to mark "closed"
exports.closeCase = async (req, res, next) => {
	try {
		const { caseId } = req.params;
		const { rating, feedback } = req.body;

		const supportCase = await SupportCase.findById(caseId);
		if (!supportCase) {
			return res.status(404).json({ error: "Support case not found" });
		}

		const userId = req.user._id.toString();
		const isAdmin = req.user.role === "admin";
		const isClient = supportCase.client.toString() === userId;

		if (isAdmin) {
			// Admin marking "resolved"
			if (supportCase.status !== "in_progress") {
				return res
					.status(400)
					.json({ error: "Case must be in_progress to resolve" });
			}
			supportCase.status = "resolved";
			await supportCase.save();
			return res.json({ success: true, data: supportCase });
		} else if (isClient) {
			// Client submitting rating/feedback (case must be "resolved")
			if (supportCase.status !== "resolved") {
				return res
					.status(400)
					.json({ error: "Case must be resolved before closing" });
			}
			if (!rating || rating < 1 || rating > 5) {
				return res
					.status(400)
					.json({ error: "Rating (1–5) is required to close" });
			}
			supportCase.rating = rating;
			supportCase.feedback = feedback;
			supportCase.status = "closed";
			await supportCase.save();
			return res.json({ success: true, data: supportCase });
		} else {
			return res.status(403).json({ error: "Not authorized" });
		}
	} catch (err) {
		next(err);
	}
};
