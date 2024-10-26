from dataclasses import dataclass
from Extend.job_manager.job import Job


@dataclass
class ClusterState:
	available_processors: int
	total_processors: int
	job_wait_list: list[Job]
