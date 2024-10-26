import re
from Extend.job_manager.job import Job, JobStatus


class JobManager:
	def __init__(
		self, start=-1, num=-1, anchor=-1, density=1.0, read_input_freq=1000, debug=None
	):
		self.myInfo = "Job Manager"
		self.start = start
		self.start_offset_A = 0.0
		self.start_offset_B = 0.0
		self.start_date = ""
		self.anchor = anchor
		self.read_num = num
		self.density = density
		self.job_trace = {}
		self.jobFile = None
		self.read_input_freq = read_input_freq
		self.num_delete_jobs = 0
		self.index_to_job = {}
		self.all_jobs = []

		self.reset_data()

	def reset(
		self,
		start=None,
		num=None,
		anchor=None,
		density=None,
		read_input_freq=None,
		debug=None,
	):
		if start:
			self.anchor = start
			self.start_offset_A = 0.0
			self.start_offset_B = 0.0
		if num:
			self.read_num = num
		if anchor:
			self.anchor = anchor
		if density:
			self.density = density
		if read_input_freq:
			self.read_input_freq = read_input_freq
		self.job_trace = {}
		self.jobFile = None
		self.reset_data()

	def reset_data(self):
		self.job_wait_size = 0
		self.job_submit_list = []
		self.job_wait_list = []
		self.job_run_list = []
		self.num_delete_jobs = 0

	def initial_import_job_file(self, job_file):
		self.temp_start = self.start
		self.jobFile = open(job_file, "r")
		self.min_sub = -1
		self.job_trace = {}
		self.reset_data()
		self.i = 0
		self.j = 0

	def dyn_import_job_file(self):
		if self.jobFile.closed:
			return -1
		temp_n = 0
		regex_str = "([^;\\n]*)[;\\n]"
		while (
			self.i < self.read_num or self.read_num <= 0
		) and temp_n < self.read_input_freq:
			job_str = self.jobFile.readline()
			if self.i == self.read_num - 1 or not job_str:  # break when no more line
				self.jobFile.close()
				return -1
			if self.j >= self.anchor:
				job_data = re.findall(regex_str, job_str)

				if self.min_sub < 0:
					self.min_sub = float(job_data[1])
					if self.temp_start < 0:
						self.temp_start = self.min_sub
					self.start_offset_B = self.min_sub - self.temp_start

				submit = (
					self.density * (float(job_data[1]) - self.min_sub) + self.temp_start
				)
				job = Job(
					index=int(job_data[0]),
					submit_time=submit,
					wait_time=float(job_data[2]),
					run_time=float(job_data[3]),
					used_processors=int(job_data[4]),
					used_processor_avg=float(job_data[5]),
					used_memory=float(job_data[6]),
					requested_processors=int(job_data[7]),
					requested_time=float(job_data[8]),
					requested_memory=float(job_data[9]),
					status=JobStatus(int(job_data[10])),
					user_id=int(job_data[11]),
					group_id=int(job_data[12]),
					num_exe=int(job_data[13]),
					num_queue=int(job_data[14]),
					num_part=int(job_data[15]),
					num_pre=int(job_data[16]),
					think_time=int(job_data[17]),
					start_time=-1,
					end_time=-1,
				)

				# self.job_trace[self.i] = job
				self.job_trace[job.index] = job
				self.job_submit_list.append(job)
				self.all_jobs.append(job)

				self.i += 1
			self.j += 1
			temp_n += 1
			return 0

	def import_job_config(self, config_file):
		regex_str = "([^=\\n]*)[=\\n]"
		jobFile = open(config_file, "r")
		config_data = {}

		while 1:
			tempStr = jobFile.readline()
			if not tempStr:  # break when no more line
				break
			temp_dataList = re.findall(regex_str, tempStr)
			config_data[temp_dataList[0]] = temp_dataList[1]
		jobFile.close()
		self.start_offset_A = config_data["start_offset"]
		self.start_date = config_data["date"]

	def submit_list(self):
		return self.job_submit_list

	def wait_list(self):
		return self.job_wait_list

	def run_list(self):
		return self.job_run_list

	def wait_size(self):
		return self.job_wait_size

	def job_info(self, job_index=-1):
		if job_index == -1:
			return self.job_trace
		return self.job_trace[job_index]

	def job_info_len(self):
		return len(self.job_trace) + self.num_delete_jobs

	def job_submit(self, job:Job, job_est_start=-1):
		job.mark_submitted(job_est_start)
		self.job_submit_list.remove(job)
		self.job_wait_list.append(job)

		self.job_wait_size += job.requested_processors

	def job_start(self, job:Job, time):
		job.mark_started(time)

		self.job_wait_list.remove(job)
		self.job_run_list.append(job)
		self.job_wait_size -= job.requested_processors

	def job_finish(self, job:Job, time=None):
		job.mark_finished()
		self.job_run_list.remove(job)

	def remove_job_from_dict(self, job_index):
		del self.job_trace[job_index]
		self.num_delete_jobs += 1
