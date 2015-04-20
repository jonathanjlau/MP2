#  MP2
from __future__ import division
import channels
import chord
import select
import sys
import threading

# Global variables
input_handler = {}
message_handler = {}
nodes = {}
chnl = channels.Channels()

COORDINATOR = 256
DEBUG = True
DEBUG = False
DEBUG1 = True
DEBUG1 = False


###############################################################
# Functions for the coordinator thread
###############################################################
def within_bounds(num) :
	''' Check whether the input is between 0 and 255'''
	if num < 0 or num > 255:
		return False
	else:
		return True

def setup_input_handler():
	''' Sets up a dictionary to execute input commands'''
	input_handler['join'] = input_join_process
	input_handler['find'] = input_find_key
	input_handler['leave'] = input_leave_process
	input_handler['show'] = input_show_key
	input_handler['show-all'] = input_show_all
	input_handler['show-count'] = input_show_count
	input_handler['reset-count'] = input_reset_count
	input_handler['quit'] = input_quit


def handle_input(input_string):
	'''Executes the command in the given string from stdin'''

	# Ignore empty commands
	command = input_string.split()
	if len(command) == 0:
		return

	# Check whether the command is supported
	if command[0] not in input_handler:
		print >> sys.stderr, 'Unrecognized input command', command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given command
	input_handler[command[0]](command)
	return


# function to handle input command from stdin - 'join'
# 1. start a new thread
# 2. inform the existing processes to add the new process
# 3. update the process_vld array
def input_join_process(command):
	'''Handles the addition of a new process'''

	# check that the command has the right number of parameters
	if len(command) != 2:
		print >> sys.stderr, 'Usage: join p(0-255)'
		return

	p = int(command[1])

	# Check whether the node number is within bounds and does not already exist
	if not within_bounds(p):
		print >> sys.stderr, "Invalid process number"
		return

	if p in nodes:
		print >> sys.stderr, "Process", new_process, "has already joined."
		return

	# Create a new node and starts it
	new_node = chord.Node(p, chnl)
	nodes[p] = new_node
	node_thread = threading.Thread(target = chord.run, args = [new_node])
	node_thread.start()
	
	# Wait for an ack
	ack = chnl.recv_msg(COORDINATOR)
	cmd = ack.split()
	if (cmd[0] != 'ack-join'):
		print >> sys.stderr, 'Error while join a node'

def input_find_key(command):
	'''Handles the find command of a process'''
	if len(command) != 3:
		print >> sys.stderr, 'Usage: find p k'
		return

	# Check whether inputs are integers
	try:
		nodeid = int(command[1])
		key = int(command[2])
	except ValueError:
		print >> sys.stderr, 'Node id or key not integer'
		return

	# Check whether node and keys within bounds
	if not within_bounds(nodeid):
		print >> sys.stderr, "Invalid node id"
		return

	if not within_bounds(key):
		print >> sys.stderr, "Invalid key number"
		return

	# Asks the node to find a key
	if nodeid in nodes:
		message = 'find-succ ' + str(key) + ' ' + str(COORDINATOR)
		chnl.send_msg(nodeid, message)
		ret_msg = chnl.recv_msg(COORDINATOR)
		cmd = ret_msg.split()
		if cmd[0] != 'ack-find-succ':
			print >> sys.stderr, 'Unexpected message in find key'
		else:
			print 'Key', key, 'is in node', cmd[1]
	else:
		print >> sys.stderr, "Node", nodeid, "not found."

# function to handle input command from stdin - 'leave'
# 1. send a message about the departure to all the existing processors
# 2. update process_vld array
def input_leave_process(command):
	'''Handles the removal of a existing process'''
	global chnl
	global nodes

	# Check that the command has the right number of parameters
	if len(command) != 2:
		print >> sys.stderr, 'Usage: leave process_number(0-255)'
		return

	del_process = int(command[1])
	if not within_bounds(del_process) or del_process == 0:
		print >> sys.stderr, "Invalid process number"
		return

	if del_process in nodes:

		message = 'leave_process ' + command[1]

		# let all the existing valid processes know that we are deleting this process
		for i in range(0, COORDINATOR):
			if i in nodes :
				chnl.send_msg(i, message)
				wait_for_ack()

		del nodes[del_process]


	else:
		print >> sys.stderr, "Process", del_process, "not found."

def input_show_key(command):
	'''Outputs the keys stored at a single node'''
	if len(command) != 2:
		print >> sys.stderr, 'Usage: show p'
		return

	try:
		nodeid = int(command[1])
	except ValueError:
		print >> sys.stderr, 'Node id not integer'
		return

	if not within_bounds(nodeid):
		print >> sys.stderr, "Invalid node id"
		return

	if nodeid in nodes:
		print nodes[nodeid].get_key_str()
	else:
		print >> sys.stderr, "Node", nodeid, "not found"

def input_show_all(command):
	'''Outputs the keys stored at each node'''
	for item in sorted(nodes.items()):
		print item[1].get_key_str()

def input_show_count(command):
	'''Displays the the total message count so far'''
	msg_count = 0
	for node in nodes.values():
		msg_count += node.get_msg_count()
	print msg_count

def input_reset_count(command):
	'''Resets the number of messages'''
	for node in nodes.values():
		node.reset_msg_count()

def input_quit(command):
	'''Exits the program'''
	sys.exit()


def msg_leave_process(command, my_id, ft_table, key_vld):
	'''
	# Coordinator's broadcast when a process is deleted.
	# 1. update ft_table, process_vld
	# 2. if deleting my process, transfer key storage to new process if needed
	'''
	global chnl
	global nodes
	del_process = int(command[1])
	if del_process == my_id :
		print >> sys.stderr, "Process thread has exited, id =", my_id

		# send all keys to the successor if any
		key_vld_string = vld_array_string(key_vld)
		if key_vld_string != '' :
			message = 'add_key' + key_vld_string
			chnl.send_msg(ft_table[0].succ, message)
		chnl.send_msg(COORDINATOR, 'ack')
		sys.exit()
	else:
		del nodes[del_process]
		process_vld[del_process] = 0
		ft_table_update(my_id, ft_table)
		chnl.send_msg(COORDINATOR, 'ack')

'''
Main function for the individual servers.

Description:
Each server connects to the sequencer and every other server using separate sockets
It will then spawn two threads: one to send messages that have been queued and one to receive and delay messages
After spawning the threads, the main function handles client input and server response to delivered messages
The send thread continuously checks for messages in the send queue and sends them through the socket
The receive thread continuously reads messages from sockets and delivers them based on receive time, delaying the delivery when necessary

More detail can be found in each thread's description.

To run commands from a file, use cat and pipe
'''
if __name__ == "__main__":

	# Check whether the user enters the output file
	if len(sys.argv) == 3:
		# Redirect stdout to an output file
		sys.stdout = open(sys.argv[2], 'w+')
	elif len(sys.argv) != 1:
		print >> sys.stderr, 'Usage: python mp2.py -g <filename>'

	# Creates the set of channels and add a channel for the coordinator
	chnl.add_channel(COORDINATOR)

	# Create a map to hold nodes and start node 0
	node0 = chord.Node(0, chnl)
	nodes[0] = node0

	node_thread = threading.Thread(target = chord.run, args = [node0])
	node_thread.start()

	# Respond to input
	setup_input_handler()
	while 1:
		string = raw_input()
		handle_input(string)
