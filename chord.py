import math
import sys
import threading

DEBUG = True
DEBUG = False

# add all keys if node 0

class Node(threading.Thread):
	'''Represents a chord node'''

	def __init__(self, nodeid, chnl):
		'''Create a new chord node with the given nodeid and chnl for communication'''
		self.nodeid = nodeid
		self.chnl = chnl
		self.chnl.add_channel(self.nodeid)

		self.fingers = [0] * 8
		self.pred = 0
		self.key_set = {}

		self.msg_count = 0
		self.msg_handler = {}
		self.setup_msg_handler()

##################################################################
# Utility methods
##################################################################

	def get_nodeid(self):
		return self.nodeid

	def get_msg_count(self):
		'''Returns the number of messages this node sent'''
		return self.msg_count

	def reset_msg_count(self):
		'''Sets the number of messages send by this node to 0'''
		self.msg_count = 0

	def get_key_str(self):
		'''Returns string representing every key stored in this node'''
		string = str(self.nodeid)
		for key in sorted(self.key_set.keys()):
			string += ' ' + str(key)
		return string
	
	def send_msg(self, tgt, msg):
		self.chnl.send_msg(tgt, msg)
		self.msg_count += 1

	def recv_msg(self):
		return self.chnl.recv_msg(self.nodeid)

	def within_right_interval(self, start, end, id):
		'''Check whether the given id is in the interval [start, end)'''
		if (start == end):
			return id != start
		else:
			id_dist = (id - start) % 256
			end_dist = (end - start) % 256
			return id_dist < end_dist

	def within_left_interval(self, start, end, id):
		'''Check whether the given id is in the interval (start, end]'''
		if (start == end):
			return id != start
		else:
			return self.within_right_interval((start + 1) % 256, (end + 1) % 256, id)

	def wait_until_recv(self, ack_str):
		'''Handle messages until the one with the ack message is received'''
		msg = self.recv_msg()
		cmd = msg.split()
		while cmd[0] != ack_str:
			self.handle_message(msg)
			msg = self.recv_msg()
			cmd = msg.split()
		return cmd			

##################################################################
# Message handling methods
##################################################################

	def setup_msg_handler(self):
		'''Sets up a dictionary to execute received messages'''
		self.msg_handler['find-succ'] = self.find_succ
		self.msg_handler['ret-succ'] = self.ret_succ
		self.msg_handler['ret-pred'] = self.ret_pred
		self.msg_handler['update-pred'] = self.update_pred
		self.msg_handler['update-finger'] = self.update_finger
		self.msg_handler['split-key'] = self.split_key
		self.msg_handler['closest-finger'] = self.closest_finger

	def handle_message(self, msg):
		'''Forwards each message to its own command handler'''
		if DEBUG:
			print >> sys.stderr, 'Node', self.nodeid, 'received', msg

		# Check whether the message is supported
		cmd = msg.split()
		if cmd[0] not in self.msg_handler:
			print >> sys.stderr, 'Unrecognized command', cmd[0], 'in node', self.nodeid
			return

		self.msg_handler[cmd[0]](cmd)

	def get_succ(self, node):
		'''Returns the successor of a node'''
		ret_succ_msg = 'ret-succ ' + str(self.nodeid)
		self.send_msg(node, ret_succ_msg)
		cmd = self.wait_until_recv('ack-ret-succ')
		return int(cmd[1])

	def ret_succ(self, cmd):
		'''Sends this node's successor to the requesting node'''
		ret_msg = 'ack-ret-succ ' + str(self.fingers[0])
		self.send_msg(int(cmd[1]), ret_msg)

	def ret_pred(self, cmd):
		'''Sends this node's predecessor to the requesting node'''
		ret_msg = 'ack-ret-pred ' + str(self.pred)
		self.send_msg(int(cmd[1]), ret_msg)

	def update_pred(self, cmd):
		'''Updates this node's predecessor and respond with ack'''
		self.pred = int(cmd[1])
		self.send_msg(int(cmd[1]), 'ack-update-pred')


##################################################################
# Finding successor methods
##################################################################

	def find_succ(self, cmd):
		'''Finds the node that contains the key and send it to the source'''
		key = int(cmd[1])
		src = int(cmd[2])

		# Checks whether the key is stored at this node
		pred = self.find_pred(key)
		succ = self.get_succ(pred)
		ret_msg = 'ack-find-succ ' + str(succ)
		self.send_msg(src, ret_msg)


	def find_pred(self, key):
		'''Returns the predessor of the key'''
		n = self.nodeid
		succ = self.get_succ(n)

		# Keep looking for the closest preceding finger until the key is between the node and successor
		while not (n == succ or self.within_left_interval(n, succ, key)):
			closest_finger_msg = 'closest-finger ' + str(key) + ' ' + str(self.nodeid)
			self.send_msg(n, closest_finger_msg)
			cmd = self.wait_until_recv('ack-closest-finger')
			n = int(cmd[1])
			succ = self.get_succ(n)

		return n


	def closest_finger(self, cmd):
		'''Finds the closest finger preceding the key and sends it to the source'''
		key = int(cmd[1])
		src = int(cmd[2])
		n = self.nodeid
		ret_msg = 'ack-closest-finger '

		# Look for the closest preceding finger in the finger table
		for i in xrange(7, -1, -1):
			finger = self.fingers[i]
			if	self.within_left_interval(n, key, finger) and self.within_right_interval(n, key, finger):
				self.send_msg(src, ret_msg + str(finger))
				return

		self.send_msg(src, ret_msg + str(n))

##################################################################
# Node join methods
##################################################################

	def join(self):
		'''Joins this node to the chord network'''

		# First node
		if self.nodeid == 0:
			self.key_set = {i: True for i in xrange(256)}
		else:
			self.init_finger_table()
			self.update_others()
			self.transfer_keys()

			# Tell the coordinator this node finished joining
			ack_join = 'ack-join'
			self.send_msg(256, ack_join)

	def init_finger_table(self):
		'''Initializes this node's finger table by querying node 0'''

		# Asks node 0 to find this node's successor
		find_succ_msg = 'find-succ ' + str((self.nodeid + 1) % 256) + ' ' + str(self.nodeid)
		self.send_msg(0, find_succ_msg)
		cmd = self.wait_until_recv('ack-find-succ')
		self.fingers[0] = int(cmd[1])

		# Asks successor for this node's predecessor
		get_pred_msg = 'ret-pred ' + str(self.nodeid)
		self.send_msg(self.fingers[0], get_pred_msg)
		ret_pred_msg = self.recv_msg()
		cmd = ret_pred_msg.split()
		if cmd[0] == 'ack-ret-pred':
			self.pred = int(cmd[1])
		else:
			print >> sys.stderr, 'Unexpected message in init finger table'
			sys.exit()

		# Asks the successor to update its predecessor to this node
		update_pred_msg = 'update-pred ' + str(self.nodeid)
		self.send_msg(self.fingers[0], update_pred_msg)
		update_pred_ack = self.recv_msg()
		cmd = update_pred_ack.split()
		if cmd[0] != 'ack-update-pred':
			print >> sys.stderr, 'Unexpected message in init finger table'
			sys.exit()

		# Initialize all fingers
		for i in xrange(1, 8):
			start = (self.nodeid + 2**i) % 256

			# finger[i].start is within (n, finger[i - 1].node]
			if self.within_left_interval(self.nodeid, self.fingers[i - 1], start):
				self.fingers[i] = self.fingers[i - 1]

			# need to ask node 0 for next finger
			else:
				find_succ_msg = 'find-succ ' + str(start) + ' ' + str(self.nodeid)
				self.send_msg(0, find_succ_msg)
				cmd = self.wait_until_recv('ack-find-succ')
				succ = int(cmd[1])

				# Check whether the current node should be the finger
				if self.within_left_interval(succ, self.nodeid, start):
					self.fingers[i] = self.nodeid
				else:
					self.fingers[i] = succ

	def update_others(self):
		'''Ask other nodes to add this node to their finger table'''
		for i in xrange(8):
			candidate = (self.nodeid - 2**i + 1) % 256
			pred = self.find_pred(candidate)
			if pred != self.nodeid:
				update_finger_msg = 'update-finger ' + str(self.nodeid) + ' ' + str(i)
				self.send_msg(pred, update_finger_msg)
				self.wait_until_recv('ack-update-finger')

	def update_finger(self, cmd):
		'''Updates this node's finger table and send an ack to the requesting node'''
		s = int(cmd[1])
		i = int(cmd[2])
		n = self.nodeid
		if s != n and self.within_right_interval(n, self.fingers[i], s):
			self.fingers[i] = s
			update_finger_msg = 'update-finger ' + str(s) + ' ' + str(i)
			self.send_msg(self.pred, update_finger_msg)
		else:
			self.send_msg(s, 'ack-update-finger')

	def transfer_keys(self):
		'''Asks the sucessor to transfer keys to this node'''
		split_key_msg = 'split-key ' + str(self.nodeid)
		self.send_msg(self.fingers[0], split_key_msg)
		ack_split_key = self.recv_msg()
		cmd = ack_split_key.split()
		if cmd[0] == 'ack-split-key':
			for key in cmd[1:]:
				self.key_set[int(key)] = True
		else:
			print >> sys.stderr, 'Unexpected message in transfer keys'
			sys.exit()

	def split_key(self, cmd):
		'''Returns the keys belonging to the requesting node'''
		node = int(cmd[1])
		keys = ''
		for key in self.key_set.keys():
			if self.within_left_interval(self.nodeid, node, key):
				keys += ' ' + str(key)
				del(self.key_set[key])

		self.send_msg(node, 'ack-split-key' + keys)


##################################################################
# Thread function
##################################################################

def run(node):
	'''Thread function for running a node'''
	print >> sys.stderr, 'Node', node.get_nodeid(), 'started'

	# Joins the network
	node.join()

	# Respond to messages
	while 1:
		msg = node.recv_msg()
		node.handle_message(msg)

