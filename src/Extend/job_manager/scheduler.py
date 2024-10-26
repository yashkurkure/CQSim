from Extend.job_manager.job import Job
from .cluster_state import ClusterState


class Scheduler:
	def __init__(
		self, start=-1, num=-1, anchor=-1, density=1.0, read_input_freq=1000, debug=None
	):
		self.myInfo = "Scheduler"

	def schedule(self, cluster_state: ClusterState) -> list[Job]:
		raise NotImplementedError("You need to implement this!")
