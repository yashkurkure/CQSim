from enum import Enum
from Extend.job_manager.FCFSWithBackfilling import FCFS
from Extend.job_manager.cluster_state import ClusterState

__metaclass__ = type


class SimulatorEvent(Enum):
	JOB = 1
	MONITOR = 2
	EXTEND = 3


class JobEvents(Enum):
	SUBMIT = 1
	FINISH = 2


class Cqsim_sim:
	def __init__(self, module, scheduler, job_manager, debug=None, monitor=None):
		self.myInfo = "Cqsim Sim"
		self.module = module
		self.debug = debug
		self.monitor = monitor

		self.debug.line(4, " ")
		self.debug.line(4, "#")
		self.debug.debug("# " + self.myInfo, 1)
		self.debug.line(4, "#")

		self.event_seq = []
		self.monitor_start = 0
		self.current_event = None
		self.currentTime = 0
		self.read_job_pointer = 0  # next position in job list
		self.previous_read_job_time = -1  # lastest read job submit time
		self.scheduler = FCFS()
		self.job_manager = job_manager

		self.debug.line(4)
		for module_name in self.module:
			temp_name = self.module[module_name].myInfo
			self.debug.debug(temp_name + " ................... Load", 4)
			self.debug.line(4)

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
			self.job_manager.dyn_import_job_file()
		)
		i = self.read_job_pointer
		for job in self.job_manager.all_jobs:
			self.insert_event(1, job.submit_time, 2, [1, job.index])
			self.previous_read_job_time = job.submit_time
			self.debug.debug(
				"  " + "Insert job[" + "2" + "] " + str(job.submit_time),
				4,
			)
			i += 1
		if temp_return is None or temp_return < 0:
			self.read_job_pointer = -1
		else:
			self.read_job_pointer = i

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
				self.debug.debug("  " + "Insert mon[" + "5" + "] " + str(temp_time), 4)
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

	def scan_event(self):
		self.debug.line(2, " ")
		self.debug.line(2, "=")
		self.debug.line(2, "=")
		self.current_event = None

		while len(self.event_seq) > 0 or self.read_job_pointer >= 0:
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
				continue

			self.current_event = temp_current_event
			self.currentTime = temp_currentTime
			if self.current_event["type"] == 1:
				print(f'JOB EVENT: {self.current_event["para"]}')
				self.debug.line(2, " ")
				self.debug.line(2, ">>>")
				self.debug.line(2, "--")
				self.debug.debug("  Time: " + str(self.currentTime), 2)
				self.debug.debug("   " + str(self.current_event), 2)
				self.debug.line(2, "--")
				self.debug.debug("  Submit : " + str([j.index for j in self.job_manager.job_submit_list]), 2)
				self.debug.debug("  Wait: " + str(self.job_manager.wait_list()), 2)
				self.debug.debug("  Run : " + str([j.index for j in self.job_manager.run_list()]), 2)
				self.debug.line(2, "--")
				self.debug.debug(
					"  Tot:"
					+ str(self.module["node"].get_tot())
					+ " Idle:"
					+ str(self.module["node"].get_idle())
					+ " Avail:"
					+ str(self.module["node"].get_avail())
					+ " ",
					2,
				)
				self.debug.line(2, "--")
				self.event_job(self.current_event["para"])

			elif self.current_event["type"] == 2:
				self.event_monitor(self.current_event["para"])
			elif self.current_event["type"] == 3:
				self.event_extend(self.current_event["para"])
			self.sys_collect()
			self.interface()
			# self.event_pointer += 1
			del self.event_seq[0]
		self.debug.line(2, "=")
		self.debug.line(2, "=")
		self.debug.line(2, " ")
		return

	def get_cluster_state(self):
		n_avail = self.module["node"].get_avail()
		n_total = self.module["node"].get_total()
		job_wait_list = self.job_manager.wait_list()

		return ClusterState(n_avail, n_total, job_wait_list)

	def event_job(self, para_in=None):
		if self.current_event["para"][0] == 1:
			self.submit(self.current_event["para"][1])
		elif self.current_event["para"][0] == 2:
			self.finish(self.current_event["para"][1])
		self.schedule_jobs()
		if len(self.event_seq) > 1:
			self.insert_event_monitor(self.currentTime, self.event_seq[1]["time"])
		return

	def event_monitor(self, para_in=None):
		# self.debug.debug("# "+self.myInfo+" -- event_monitor",5)
		self.print_adapt(None)
		return

	def event_extend(self, para_in=None):
		# self.debug.debug("# "+self.myInfo+" -- event_extend",5)
		return

	def submit(self, job_index):
		job = self.job_manager.job_info(job_index)
		try:
			self.job_manager.job_submit(job)
			print(f'Submitting job {job.index}')
		except:
			print(f'Failed to submit {job.index}')

	def finish(self, job_index):
		job = self.job_manager.job_info(job_index)

		self.module["node"].node_release(job, self.currentTime)
		self.job_manager.job_finish(job_index)
		self.job_manager.remove_job_from_dict(job_index)

	def start(self, job):
		self.module["node"].node_allocate(
			job.requested_processors,
			job.index,
			self.currentTime,
			self.currentTime + job.requested_time,
		)
		try:
			self.job_manager.job_start(job, self.currentTime)
			print(f'Started job {job.index}')
		except:
			print(f'Failed to start {job.index}')
		self.insert_event(
			1,
			self.currentTime + job.run_time,
			1,
			[2, job.index],
		)

	def schedule_jobs(self):
		state = ClusterState(
			available_processors=self.module["node"].get_avail(),
			total_processors=self.module["node"].get_tot(),
			job_wait_list=self.job_manager.wait_list(),
		)
		jobs_to_start = self.scheduler.schedule(state)

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
			uti=(self.module["node"].get_tot() - self.module["node"].get_idle())
			* 1.0
			/ self.module["node"].get_tot(),
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
