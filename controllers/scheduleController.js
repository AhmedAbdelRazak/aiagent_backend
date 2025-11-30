// controllers/scheduleController.js
const mongoose = require("mongoose");
const Schedule = require("../models/Schedule");
const SocialPost = require("../models/SocialPost"); // ← make sure this exists
const dayjs = require("dayjs");

exports.createSchedule = async (req, res, next) => {
	try {
		const { videoId, scheduleType, timeOfDay, startDate, endDate, category } =
			req.body;
		if (!videoId || !scheduleType || !timeOfDay || !startDate)
			return res.status(400).json({ error: "Missing fields" });

		if (!["daily", "weekly", "monthly"].includes(scheduleType))
			return res.status(400).json({ error: "Invalid type" });
		if (!/^\d{2}:\d{2}$/.test(timeOfDay))
			return res.status(400).json({ error: "timeOfDay must be HH:mm" });

		const video = await Video.findById(videoId);
		if (!video) return res.status(404).json({ error: "Video not found" });
		if (
			video.user.toString() !== req.user._id.toString() &&
			req.user.role !== "admin"
		)
			return res.status(403).json({ error: "Not authorized" });

		// compute nextRun
		let next = dayjs(startDate)
			.hour(+timeOfDay.split(":")[0])
			.minute(+timeOfDay.split(":")[1])
			.second(0);
		if (next.isBefore(dayjs())) {
			if (scheduleType === "daily") next = next.add(1, "day");
			else if (scheduleType === "weekly") next = next.add(1, "week");
			else next = next.add(1, "month");
		}

		const effectiveCategory = category || video.category;
		if (!effectiveCategory) {
			return res
				.status(400)
				.json({ error: "Category is required for schedules." });
		}

		const sched = new Schedule({
			user: req.user._id,
			category: effectiveCategory,
			video: video._id,
			scheduleType,
			timeOfDay,
			startDate: dayjs(startDate).toDate(),
			endDate: endDate ? dayjs(endDate).toDate() : undefined,
			nextRun: next.toDate(),
			active: true,
		});
		await sched.save();

		video.scheduled = true;
		await video.save();

		res.status(201).json({ success: true, data: sched });
	} catch (err) {
		next(err);
	}
};

exports.getUserSchedules = async (req, res, next) => {
	try {
		const schedules = await Schedule.find({ user: req.user._id }).populate(
			"video"
		);
		res.json({ success: true, data: schedules });
	} catch (err) {
		next(err);
	}
};

exports.getScheduleById = async (req, res, next) => {
	try {
		const { scheduleId } = req.params;
		const { role, _id: userId } = req.user;

		if (!mongoose.Types.ObjectId.isValid(scheduleId)) {
			return res.status(400).json({ error: "Invalid schedule ID." });
		}

		const schedule = await Schedule.findById(scheduleId)
			.populate("user", "name email role")
			.populate("video", "seoTitle youtubeLink");

		if (!schedule)
			return res.status(404).json({ error: "Schedule not found." });

		if (
			schedule.user._id.toString() !== userId.toString() &&
			role !== "admin"
		) {
			return res.status(403).json({ error: "Not authorized" });
		}

		res.json({ success: true, data: schedule });
	} catch (err) {
		next(err);
	}
};

exports.updateSchedule = async (req, res, next) => {
	try {
		const { scheduleId } = req.params;
		const { scheduleType, timeOfDay, active, category } = req.body;

		const schedule = await Schedule.findById(scheduleId);
		if (!schedule) {
			return res.status(404).json({ error: "Schedule not found" });
		}
		if (
			schedule.user.toString() !== req.user._id.toString() &&
			req.user.role !== "admin"
		) {
			return res.status(403).json({ error: "Not authorized" });
		}

		if (
			scheduleType &&
			!["daily", "weekly", "monthly"].includes(scheduleType)
		) {
			return res.status(400).json({ error: "Invalid schedule type" });
		}
		if (timeOfDay && !/^\d{2}:\d{2}$/.test(timeOfDay)) {
			return res.status(400).json({ error: "timeOfDay must be HH:mm" });
		}

		if (scheduleType) schedule.scheduleType = scheduleType;
		if (timeOfDay) schedule.timeOfDay = timeOfDay;
		if (category) schedule.category = category;
		if (active !== undefined) schedule.active = active;

		// Recompute nextRun if timeOfDay or type changed
		if (timeOfDay || scheduleType) {
			const [hh, mm] = (timeOfDay || schedule.timeOfDay).split(":");
			let nextRun = dayjs().hour(parseInt(hh)).minute(parseInt(mm)).second(0);
			if (nextRun.isBefore(dayjs())) {
				if (schedule.scheduleType === "daily") nextRun = nextRun.add(1, "day");
				else if (schedule.scheduleType === "weekly")
					nextRun = nextRun.add(1, "week");
				else if (schedule.scheduleType === "monthly")
					nextRun = nextRun.add(1, "month");
			}
			schedule.nextRun = nextRun.toDate();
		}

		await schedule.save();
		res.json({ success: true, data: schedule });
	} catch (err) {
		next(err);
	}
};

exports.deleteSchedule = async (req, res, next) => {
	try {
		const { scheduleId } = req.params;
		const schedule = await Schedule.findById(scheduleId);
		if (!schedule) {
			return res.status(404).json({ error: "Schedule not found" });
		}
		if (
			schedule.user.toString() !== req.user._id.toString() &&
			req.user.role !== "admin"
		) {
			return res.status(403).json({ error: "Not authorized" });
		}
		await schedule.remove();

		// Optionally mark post as unscheduled
		await SocialPost.findByIdAndUpdate(schedule.post, { scheduled: false });

		res.json({ success: true, message: "Schedule deleted" });
	} catch (err) {
		next(err);
	}
};

exports.listSchedules = async (req, res, next) => {
	try {
		const { role, _id: userId } = req.user;

		/* pagination query params */
		let page = parseInt(req.query.page, 10) || 1;
		let limit = parseInt(req.query.limit, 10) || 20;
		if (page < 1) page = 1;
		if (limit < 1) limit = 20;
		if (limit > 100) limit = 100;

		const filter = role === "admin" ? {} : { user: userId };

		const total = await Schedule.countDocuments(filter);
		const pages = Math.ceil(total / limit);
		const skip = (page - 1) * limit;

		const schedules = await Schedule.find(filter)
			.sort({ nextRun: 1 }) // soonest first
			.skip(skip)
			.limit(limit)
			.populate("user", "name email role") // who owns it
			.populate("video", "seoTitle youtubeLink"); // basic video data

		return res.status(200).json({
			success: true,
			page,
			pages,
			limit,
			count: schedules.length,
			total,
			data: schedules,
		});
	} catch (err) {
		console.error("[listSchedules] error:", err);
		next(err);
	}
};
