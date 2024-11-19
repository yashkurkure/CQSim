from dataclasses import dataclass
from Extend.job_manager.job import Job

@dataclass
class Node:
	node_id: int
	location: str
	group: int
	state: int
	proc: int
	start: int = -1

@dataclass
class Cluster:
	info: str = "Cluster Nodes"
	nodes: list[Node] = []
	job_list: list[Job] = []

