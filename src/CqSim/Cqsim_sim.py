from enum import Enum
from Extend.job_manager.FCFSWithBackfilling import FCFS
from Extend.job_manager.cluster_state import ClusterState
from Extend.job_manager.job_manager import JobManager
from Extend.job_manager.job_allocator import JobAllocator
from bisect import bisect_left

__metaclass__ = type


class SimulatorEvent(Enum):
	JOB = 1
	MONITOR = 2
	EXTEND = 3


class JobEvents(Enum):
	SUBMIT = 1
	FINISH = 2


class Cqsim_sim:
	def __init__(
		self,
		module,
		scheduler,
		job_manager,
		job_allocator,
		debug=None,
		monitor=None
	):
		self.myInfo = "Cqsim Sim"
		self.module = module
		self.debug = debug
		self.monitor = monitor

		self.event_seq = []
		self.monitor_start = 0
		self.current_event = None
		self.currentTime = 0
		self.read_job_pointer = 0  # next position in job list
		self.previous_read_job_time = -1  # lastest read job submit time
		self.scheduler = FCFS()
		self.job_manager: JobManager = job_manager
		self.job_allocator: JobAllocator = job_allocator

		for module_name in self.module:
			temp_name = self.module[module_name].myInfo

	def reset(self, module=None, debug=None, monitor=None):
		if module:
			self.module = module

		if debug:
			self.debug = debug
		if monitor:
			self.monitor = monitor

		self.event_seq = []
		self.monitor_start = 0
		self.current_event = None
		self.currentTime = 0
		self.read_job_pointer = 0
		self.previous_read_job_time = -1
		self.job_manager.reset()

		for key, val in self.module.items():
			val.reset()

	def cqsim_sim(self):
		self.import_submit_events()
		self.insert_event_extend()
		self.scan_event()
		self.print_result()
		self.debug.debug("------ Simulating Done!", 2)
		self.debug.debug(lvl=1)

	def import_submit_events(self):
		if self.read_job_pointer < 0:
			return None
		temp_return = (
			# self.job_manager.dyn_import_job_file()
			self.job_manager.import_next_job()
		)
		i = 0
		for job in self.job_manager.all_jobs[self.read_job_pointer:]:
			self.insert_event(1, job.submit_time, 2, [1, job.index])
			self.previous_read_job_time = job.submit_time
			i += 1
		if temp_return is None or temp_return < 0:
			self.read_job_pointer = -1
		else:
			self.read_job_pointer += i

	def insert_event_monitor(self, start, end):
		if not self.monitor:
			return -1
		temp_num = start / self.monitor
		temp_num = int(temp_num)
		temp_time = temp_num * self.monitor

		self.monitor_start = 0

		while temp_time < end:
			if temp_time >= start:
				self.insert_event(2, temp_time, 5, None)
			temp_time += self.monitor
		return

	def insert_event_extend(self):
		pass

	def insert_event(self, type, time, priority, para=None):
		temp_index = -1
		new_event = {"type": type, "time": time, "prio": priority, "para": para}
		if type == 1:
			for i, evt in enumerate(self.event_seq):
				if evt["time"] == time:
					if self.event_seq[i]["prio"] > priority:
						temp_index = i
						break
				elif evt["time"] > time:
					temp_index = i
					break

		elif type == 2:
			temp_index = self.get_index_monitor()

		if temp_index >= len(self.event_seq) or temp_index == -1:
			self.event_seq.append(new_event)
		else:
			self.event_seq.insert(temp_index, new_event)

	def get_index_monitor(self):
		self.monitor_start += 1
		return self.monitor_start

	def step(self):
		if len(self.event_seq) > 0:
			temp_current_event = self.event_seq[0]
			temp_currentTime = temp_current_event["time"]
		else:
			temp_current_event = None
			temp_currentTime = -1

		if (
			len(self.event_seq) == 0
			or temp_currentTime >= self.previous_read_job_time
		) and self.read_job_pointer >= 0:
			self.import_submit_events()
			return self.step()

		self.current_event = temp_current_event
		self.currentTime = temp_currentTime

		if self.current_event is None:
			return None

		if self.current_event["type"] == 1:
			self.event_job(self.current_event["para"])
		elif self.current_event["type"] == 2:
			self.event_monitor(self.current_event["para"])
		elif self.current_event["type"] == 3:
			self.event_extend(self.current_event["para"])

		#self.sys_collect()
		self.interface()
		del self.event_seq[0]

		return self.current_event

	def scan_event(self):
		self.current_event = None

		while len(self.event_seq) > 0 or self.read_job_pointer >= 0:
			self.step()

		return

	def get_cluster_state(self):
		n_avail = self.job_allocator.get_avail()
		n_total = self.job_allocator.get_tot()
		job_wait_list = self.job_manager.wait_list()
		time = self.previous_read_job_time

		return ClusterState(n_avail, n_total, job_wait_list, time=self.currentTime)

	def event_job(self, para_in=None):
		if self.current_event["para"][0] == 1:
			self.submit(self.current_event["para"][1])
		elif self.current_event["para"][0] == 2:
			self.finish(self.current_event["para"][1])
		if len(self.event_seq) > 1:
			self.insert_event_monitor(self.currentTime, self.event_seq[1]["time"])
		return

	def event_monitor(self, para_in=None):
		self.print_adapt(None)
		return

	def event_extend(self, para_in=None):
		return

	def submit(self, job_index):
		job = self.job_manager.job_info(job_index)
		# TODO this probably isn't right
		self.job_manager.job_submit(job, self.previous_read_job_time)

	def finish(self, job_index):
		job = self.job_manager.job_info(job_index)

		self.job_allocator.node_release(job, self.currentTime)
		self.job_manager.job_finish(job, self.currentTime)
		self.job_manager.remove_job_from_dict(job)

	def start(self, job):
		self.job_allocator.node_allocate(
			job.requested_processors,
			job,
			self.currentTime,
			self.currentTime + job.requested_time,
		)
		self.job_manager.job_start(job, self.currentTime)
		self.insert_event(
			1,
			self.currentTime + job.run_time,
			1,
			[2, job.index],
		)

	def schedule_jobs(self, jobs=None):
		if jobs is not None:
			for job in jobs:
				self.start(job)

			return

		state = ClusterState(
			available_processors=self.job_allocator.get_avail(),
			total_processors=self.job_allocator.get_tot(),
			job_wait_list=self.job_manager.wait_list(),
		)
		jobs = self.scheduler.schedule(state)

		for job in jobs_to_start:
			self.start(job)

	def sys_collect(self):
		temp_inter = 0
		if len(self.event_seq) > 1:
			temp_inter = self.event_seq[1]["time"] - self.currentTime

		event_code = None
		if self.event_seq[0]["type"] == 1:
			if self.event_seq[0]["para"][0] == 1:
				event_code = "S"
			elif self.event_seq[0]["para"][0] == 2:
				event_code = "E"
		elif self.event_seq[0]["type"] == 2:
			event_code = "Q"
		temp_info = self.module["info"].info_collect(
			time=self.currentTime,
			event=event_code,
			uti=(self.job_allocator.get_tot() - self.job_allocator.get_idle())
			* 1.0
			/ self.job_allocator.get_tot(),
			waitNum=len(self.job_manager.wait_list()),
			waitSize=self.job_manager.wait_size(),
			inter=temp_inter,
		)
		self.print_sys_info(temp_info)
		return

	def interface(self, sys_info=None):
		pass

	def print_sys_info(self, sys_info):
		self.module["output"].print_sys_info(sys_info)

	def print_adapt(self, adapt_info):
		self.module["output"].print_adapt(adapt_info)

	def print_result(self):
		self.module["output"].print_sys_info()
		self.debug.debug(lvl=1)
