import Queue

class Channels:
	'''Handles communication between nodes'''

	def __init__(self):
		self.queues = {}

	def add_channel(self, channelid):
		self.queues[channelid] = Queue.Queue()

	def remove_channel(self, channelid):
		del self.queues[channelid]

	def send_msg(self, channelid, msg):
		self.queues[channelid].put(msg, True)

	def recv_msg(self, channelid):
		return self.queues[channelid].get()

