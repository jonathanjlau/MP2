class ChordNode
	'''Represents a chord node'''

	def __init__(self, nodeid, channels):
		'''Create a new chord node with the given nodeid and channels for communication'''
		self.nodeid = nodeid
		self.channels = channels
		self.msg_count = 0
		self.finger_table = [0] * 8
		self.prev = 0
		self.key_set = {}

	def get_msg_count(self):
		'''Returns the number of messages this node sent'''
		return self.msg_count

	def reset_msg_count(self):
		'''Sets the number of messages send by this node to 0'''
		self.msg_count = 0

	
