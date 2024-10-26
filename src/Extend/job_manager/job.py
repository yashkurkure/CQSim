from enum import Enum
from dataclasses import dataclass


class JobStatus(Enum):
	NOT_APPLICABLE = -1
	FAILED = 0
	SUBMITTED = 1
	STARTED = 2
	FINISHED = 3
	CANCELLED = 5


@dataclass
class Job:
	index: int
	submit_time: float
	wait_time: float
	run_time: float
	used_processors: int
	used_processor_avg: float
	used_memory: float
	requested_processors: int
	requested_time: float
	requested_memory: float
	status: JobStatus
	user_id: int
	group_id: int
	num_exe: int
	num_queue: int
	num_part: int
	num_pre: int
	think_time: int
	start_time: int = -1
	end_time: float = -1.0
	score: int = 0
	state: JobStatus = JobStatus.NOT_APPLICABLE
	estimated_start: int | None = None

	def __eq__(self, other) -> bool:
		return type(other) is type(self) and self.index == other.index

	def mark_submitted(self, submitted_at: int):
		self.state = JobStatus.SUBMITTED
		self.submit_time = submitted_at

	def mark_started(self, started_at: int):
		self.state = JobStatus.STARTED
		self.start_time = started_at
		self.wait_time = self.submit_time - started_at
		self.end_time = started_at + self.run_time

	def mark_finished(self, finished_at: int | None):
		self.state = JobStatus.FINISHED
		if finished_at:
			self.end_time = finished_at
