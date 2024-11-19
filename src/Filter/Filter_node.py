__metaclass__ = type


class Filter_node:
	def __init__(self, struc=None, config=None, save=None, debug=None):
		self.myInfo = "Filter Node"
		self.struc = str(struc)
		self.save = str(save)
		self.config = str(config)
		self.nodeNum = -1
		self.nodeList = []

		self.reset_config_data()

	def reset(self, struc=None, config=None, save=None, debug=None):
		if struc:
			self.struc = str(struc)
		if save:
			self.save = str(save)
		if config:
			self.config = str(config)
		self.nodeNum = -1
		self.nodeList = []

		self.reset_config_data()

	def reset_config_data(self):
		self.config_start = ";"
		self.config_sep = "\\n"
		self.config_equal = ": "
		self.config_data = []
		# self.config_data.append({'name_config':'date','name':'StartTime','value':''})

	def read_node_struc(self):
		return

	def input_check(self, nodeInfo):
		return

	def get_node_num(self):
		return self.nodeNum

	def get_node_data(self):
		return self.nodeList

	def output_node_data(self):
		if not self.save:
			print("Save file not set!")
			return
		return

	def output_node_config(self):
		if not self.config:
			print("Config file not set!")
			return
		return
