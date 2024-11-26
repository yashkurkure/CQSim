from dataclasses import dataclass
import re
import json

__metaclass__ = type

@dataclass
class Node:
	id: int
	location: list
	group: int
	state: int
	proc: int
	start: int = -1
	end: int = -1
	extend: list = None

@dataclass
class NodeConfigFile:
	num_nodes: int
	procs_per_node: int


class JobAllocator:
	def __init__(self):
		self.myInfo = "Node Structure"
		self.nodeStruc = []
		self.job_list = []
		self.predict_node = []
		self.predict_job = []
		self.tot = -1
		self.idle = -1
		self.avail = -1

	def reset(self, debug=None):
		self.nodeStruc = []
		self.job_list = []
		self.predict_node = []
		self.tot = -1
		self.idle = -1
		self.avail = -1

	def read_list(self, source_str):
		result_list = []
		regex_str = "[\[,]([^,\[\]]*)"
		result_list = re.findall(regex_str, source_str)
		for item in result_list:
			item = int(item)
		return result_list


	def is_available(self, proc_num:int) -> bool:
		result = self.avail >= proc_num
		return result

	def get_tot(self):
		return self.tot

	def get_idle(self):
		return self.idle

	def get_avail(self):
		return self.avail

	def node_allocate(self, proc_num, job, start, end):
		if not self.is_available(proc_num):
			raise Exception(f'Cannot allocate for job {job.index}')

		self.idle -= proc_num
		self.avail = self.idle
		temp_job_info = {"job": job.index, "end": end, "node": proc_num}
		j = 0
		is_done = False
		temp_num = len(self.job_list)
		while j < temp_num:
			if temp_job_info["end"] < self.job_list[j]["end"]:
				self.job_list.insert(j, temp_job_info)
				is_done = True
				break
			j += 1

		if not is_done:
			self.job_list.append(temp_job_info)

		return 1

	def node_release(self, job, end):
		temp_node = 0
		j = 0
		temp_num = len(self.job_list)
		while j < temp_num:
			if job.index == self.job_list[j]["job"]:
				temp_node = self.job_list[j]["node"]
				break
			j += 1
		self.idle += temp_node
		self.avail = self.idle
		self.job_list.pop(j)

		return 1

	def pre_avail(self, proc_num, start, end=None):
		if not end or end < start:
			end = start

		i = 0
		temp_job_num = len(self.predict_node)
		while i < temp_job_num:
			if (
				self.predict_node[i]["time"] >= start
				and self.predict_node[i]["time"] < end
			):
				if proc_num > self.predict_node[i]["avail"]:
					return 0
			i += 1
		return 1

	def reserve(self, proc_num, job_index, time, start=None, index=-1):
		temp_max = len(self.predict_node)
		if start:
			if self.pre_avail(proc_num, start, start + time) == 0:
				return -1
		else:
			i = 0
			j = 0
			if index >= 0 and index < temp_max:
				i = index
			elif index >= temp_max:
				return -1

			while i < temp_max:
				if proc_num <= self.predict_node[i]["avail"]:
					j = self.find_res_place(proc_num, i, time)
					if j == -1:
						start = self.predict_node[i]["time"]
						break
					else:
						i = j + 1
				else:
					i += 1

		end = start + time
		j = i

		is_done = 0
		start_index = j
		while j < temp_max:
			if self.predict_node[j]["time"] < end:
				self.predict_node[j]["idle"] -= proc_num
				self.predict_node[j]["avail"] = self.predict_node[j]["idle"]
				j += 1
			elif self.predict_node[j]["time"] == end:
				is_done = 1
				break
			else:
				self.predict_node.insert(
					j,
					{
						"time": end,
						"idle": self.predict_node[j - 1]["idle"],
						"avail": self.predict_node[j - 1]["avail"],
					},
				)
				self.predict_node[j]["idle"] += proc_num
				self.predict_node[j]["avail"] = self.predict_node[j]["idle"]
				is_done = 1

				break

		if is_done != 1:
			self.predict_node.append({"time": end, "idle": self.tot, "avail": self.tot})

		self.predict_job.append({"job": job_index, "start": start, "end": end})
		return start_index

	def pre_delete(self, proc_num, job_index):
		return 1

	def pre_modify(self, proc_num, start, end, job_index):
		return 1

	def pre_get_last(self):
		pre_info_last = {"start": -1, "end": -1}
		for temp_job in self.predict_job:
			if temp_job["start"] > pre_info_last["start"]:
				pre_info_last["start"] = temp_job["start"]
			if temp_job["end"] > pre_info_last["end"]:
				pre_info_last["end"] = temp_job["end"]
		return pre_info_last

	def pre_reset(self, time):
		self.predict_node = []
		self.predict_job = []
		self.predict_node.append({"time": time, "idle": self.idle, "avail": self.avail})

		temp_job_num = len(self.job_list)
		i = 0
		j = 0
		while i < temp_job_num:
			if self.predict_node[j]["time"] != self.job_list[i]["end"] or i == 0:
				self.predict_node.append(
					{
						"time": self.job_list[i]["end"],
						"idle": self.predict_node[j]["idle"],
						"avail": self.predict_node[j]["avail"],
					}
				)
				j += 1
			self.predict_node[j]["idle"] += self.job_list[i]["node"]
			self.predict_node[j]["avail"] = self.predict_node[j]["idle"]
			i += 1
		return 1

	def find_res_place(self, proc_num, index, time):
		if index >= len(self.predict_node):
			index = len(self.predict_node) - 1

		i = index
		end = self.predict_node[index]["time"] + time
		temp_node_num = len(self.predict_node)

		while i < temp_node_num:
			if self.predict_node[i]["time"] < end:
				if proc_num > self.predict_node[i]["avail"]:
					return i
			i += 1
		return -1
	
	def read_config(self, file_path):
		with open(file_path, 'r') as f:
			data = json.load(f)
		config = NodeConfigFile(**data)

		for i in range(0, config.num_nodes):
			n = Node(
				id = i,
				location = [],
				group = -1,
				state = -1,
				proc = config.procs_per_node
			)
			self.nodeStruc.append(n)
		
		self.tot = len(self.nodeStruc)
		self.idle = self.tot
		self.avail = self.tot
		return