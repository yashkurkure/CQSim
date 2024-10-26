from .cluster_state import ClusterState
from .job_manager import Job
from .scheduler import Scheduler


class FCFS(Scheduler):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def backfill(self):
		return []

	def schedule(self, state: ClusterState) -> list[Job]:
		processors_available = state.available_processors
		scheduled_processors = 0
		jobs_to_schedule = []
		for job in state.job_wait_list:
			if scheduled_processors + job.requested_processors < processors_available:
				scheduled_processors += job.requested_processors
				jobs_to_schedule.append(job)

		return jobs_to_schedule + self.backfill()
