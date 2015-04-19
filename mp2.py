#  MP2
from __future__ import division
import select
import sys
import threading

# Global variables
input_handler = {}
message_handler = {}


# ID = 256 for the coordinator, ID = 0 to 255 for the process threads
COORDINATOR = 256
DEBUG = True
DEBUG = False
DEBUG1 = True
DEBUG1 = False
import channels


###############################################################
# Functions for the coordinator thread
###############################################################


def wait_for_ack():
	''' Waits for an ack from a processor before moving to the next command '''
	global chnl
	msg = chnl.recv_msg(256, True)

	if msg != 'ack':
		print >> sys.stderr, 'Error in receivng message - not ACK'
		sys.exit()



def node_ring_between(start, end, check) :
	''' Checks if the given node is found between the given start and end nodes of the ring.'''
	if end == start :
		rtn = True
	if end > start :
		rtn = (check > start and check <= end)
	else :
		rtn = (check > start or check <= end)
	return rtn


def key_ring_between(start, end, check) :
	''' Checks if the given key is found between the given start and end nodes of the ring.'''
	if end > start :
		rtn = (check >= start and check < end)
	else :
		rtn = (check >= start or check < end)
	return rtn

def check_valid_input_process_or_key(check_id) :
	''' Checks if the given input for the process id is between 0 and 255.'''
	if check_id < 0 or check_id > 255  :
		return False
	else :
		return True


def setup_input_handler():
	''' Sets up a dictionary to execute input commands'''
	global input_handler
	input_handler['join'] = input_join_process
	input_handler['leave'] = input_leave_process
	input_handler['find'] = input_find_key
	input_handler['show'] = input_show_key
	input_handler['show-all'] = input_show_all
	input_handler['cnt'] = input_show_count
	input_handler['quit'] = input_quit


def handle_input(input_string, process_vld):
	'''Executes the command in the given string from stdin'''
	command = input_string.split()

	# Ignore empty commands
	if len(command) == 0:
		return

	# Check whether the command is supported
	if command[0] not in input_handler:
		print >> sys.stderr, 'Unrecognized input command ' + command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given command
	input_handler[command[0]](command, process_vld)
	return


# function to handle input command from stdin - 'join'
# 1. start a new thread
# 2. inform the existing processes to add the new process
# 3. update the process_vld array
def input_join_process(command, process_vld):
	'''Handles the addition of a new process'''
	global chnl

	# check that the command has the right number of parameters
	if len(command) != 2:
		print >> sys.stderr, 'Usage: join process_number(0-255)'
		return

	new_process = int(command[1])

	if not check_valid_input_process_or_key(new_process):
		print >> sys.stderr, "Invalid process number"
		return

	if process_vld[new_process]:
		print >> sys.stderr, "Process", new_process, "has already joined."
		return

	# start a new_process
	process_start(new_process)

	# inform the new process about which old processes are valid
	message = 'join_process' + vld_array_string(process_vld)

	chnl.send_msg(new_process, message)
	wait_for_ack()

	# let all other existing process know that a new process is added
	for i in range(256):
		if process_vld[i] :
			chnl.send_msg(i, 'join_process ' + command[1])
			wait_for_ack()

	process_vld[new_process] = 1

# function to handle input command from stdin - 'leave'
# 1. send a message about the departure to all the existing processors
# 2. update process_vld array
def input_leave_process(command, process_vld):
	'''Handles the removal of a existing process'''
	global chnl

	# Check that the command has the right number of parameters
	if len(command) != 2:
		print >> sys.stderr, 'Usage: leave process_number(0-255)'
		return

	del_process = int(command[1])
	if not check_valid_input_process_or_key(del_process) or del_process == 0:
		print >> sys.stderr, "Invalid process number"
		return

	if process_vld[del_process]:

		message = 'leave_process ' + command[1]

		# let all the existing valid processes know that we are deleting this process
		for i in range(0, 256):
			if process_vld[i] :
				chnl.send_msg(i, message)
				wait_for_ack()

		process_vld[del_process] = 0


	else:
		print >> sys.stderr, "Process", del_process, "not found."


# function to handle input command from stdin - 'show'
# 1. send a 'show" message to the destination process(es)
def input_show_key(command, process_vld):
	'''Handles the display of a existing process'''
	global chnl

	if len(command) != 2:
		print >> sys.stderr, 'Usage: show process_id(0-255)'
		return
	process = int(command[1])

	if not check_valid_input_process_or_key(process):
		print >> sys.stderr, "Invalid process number"
		return

	if process_vld[process] :
		chnl.send_msg(process, 'show')
		wait_for_ack()

	else:
		print >> sys.stderr, "Process", process, "not found."

# function to handle input command from stdin - 'show-all'
# 1. send a 'show" message to all processes
def input_show_all(command, process_vld):
	'''Handles the display of a existing process'''
	global chnl
	for i in range (256) :
		if process_vld[i] :
			chnl.send_msg(i, 'show')
			wait_for_ack()

# function to handle input command from stdin - 'find P K'
# 1. send a 'find K " message to  process P
def input_find_key(command, process_vld):
	'''Handles the find command of a process'''
	global chnl
	if len(command) != 3:
		print >> sys.stderr, 'Usage: find processID key'
		return
	message = 'find ' + command[2]
	process_id = int(command[1])

	if not check_valid_input_process_or_key(int(command[2])):
		print >> sys.stderr, "Invalid key number"
		return

	if not check_valid_input_process_or_key(process_id):
		print >> sys.stderr, "Invalid process number"
		return

	if process_vld[process_id] :
		chnl.send_msg(process_id, message)
		wait_for_ack()

	else:
		print >> sys.stderr, "Process", process_id, "not found."

# function to handle input command from stdin - 'cnt'
# 1. display the message count global variable
def input_show_count(command, process_vld):
	'''Displays the the total message count so far'''
#	message_count = send_channel[0].get_msg_count()
#	print >> sys.stderr, ''
#	print >> sys.stderr, 'message count = ', message_count
	print >> sys.stderr, 'message count'

# function to handle input command from stdin - 'quit'
def input_quit(command, process_vld):
	'''Exits the program'''
	global chnl
	for i in range(256):
		if process_vld[i] :
			chnl.send_msg(i, 'quit')
	sys.exit()


# function to start a new process thread and set up a send_channel from coordinator to the new process
def process_start(process_id):
	'''Function to start the process threads'''
	global chnl
	# setup send channel from coordinator to the new process
	if DEBUG:
		print >> sys.stderr, "coordinator SETUP Send_channel to process 0"
	chnl.add_channel(process_id)
	process_thread = threading.Thread(target=process, args=[process_id])
	process_thread.start()











##################################################################
# Functions for the process/node threads
##################################################################


##################################################################
# Simple class for a finger table entry. Each process/node has 8 of them.
##################################################################
class Ft_table:
	def __init__(self, interval, succ):
		self.interval = interval
		self.succ = succ





##################################################################
# Helper fuctions that handle the messages received by the process.
##################################################################


def ft_table_update(process_vld, my_process_id, ft_table):
	'''Updates the ft_table based on array process_vld'''

	for i in range (8):
		j = ft_table[i].interval
		while (process_vld[j] != 1) :
			j = (j + 1) % 256
		ft_table[i].succ = j



def find_predecessor(process_vld, id) :
	'''Finds the valid process that is my predecessor'''
	if id == 0 :
		i = 255
	else:
		i = id - 1

	while (not process_vld[i]) :
		if i == 0 :
			i = 255
		else:
			i -= 1
	return i


def vld_array_string(vld_array):
	'''Turns the vld array (key_vld or process_vld arrays) into string to build the message'''
	message = ''
	for i in range(0,256):
		if vld_array[i] :
			message = message + ' ' + str(i)
	return message

##################################################################
# Functions that handle messages received by the process threads
##################################################################

def setup_message_handler():
	'''Sets up a dictionary to execute received messages'''
	global message_handler
	message_handler['join_process'] = msg_join_process
	message_handler['leave_process'] = msg_leave_process
	message_handler['show'] = msg_show_key
	message_handler['find'] = msg_find_key
	message_handler['add_key'] = msg_add_key

def handle_message(message, my_id, ft_table, key_vld, process_vld):
	global message_handler
	if DEBUG:
		print >> sys.stderr, 'process id', my_id, ' handle_message = ', message
	command = message.split()
	if command[0] =='quit':
		sys.exit()

	# Check whether the message is supported
	if command[0] not in message_handler:
		print >> sys.stderr, 'Unrecognized mesaage command ', command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given message
	message_handler[command[0]](command, my_id, ft_table, key_vld, process_vld)
	return

##################################################################
# Functions for all types of messages received from the socket
##################################################################


def msg_join_process(command, my_id, ft_table, key_vld, process_vld):
	'''
	# Coordinator's broadcast when a new process joins.
	# 1. update ft_table, process_vld
	# 2. setup send channel to new process
	# 3. transfer key storage to new process if needed
	'''
	global chnl
	predecessor = find_predecessor(process_vld, my_id)
	for i in range(1,len(command)) :
		new_process = int(command[i])
		process_vld[new_process] = 1
		# setup new send_channel from my_process to new_process
		if DEBUG:
			print >> sys.stderr, 'SETUP send channel from', my_id, 'to' , new_process

	ft_table_update(process_vld, my_id, ft_table)

	# send add_key message to the new_process to transfer the key storage to new process
	message = ''
	if node_ring_between(predecessor, my_id, new_process) :
		for i in range(256) :
			if key_vld[i] == 1 :
				if node_ring_between(predecessor, new_process, i) :
					key_vld[i] = 0
					message = message + ' ' + str(i)

	# if there is any key need to transfer, send it to the new_process
	if len(message) > 0 :
		message = 'add_key' + message
		new_process = int(command[1])
		chnl.send_msg(new_process, message)
	else:
		chnl.send_msg(COORDINATOR, 'ack')

def msg_leave_process(command, my_id, ft_table, key_vld, process_vld):
	'''
	# Coordinator's broadcast when a process is deleted.
	# 1. update ft_table, process_vld
	# 2. if deleting my process, transfer key storage to new process if needed
	'''
	global chnl
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
		process_vld[del_process] = 0
		ft_table_update(process_vld, my_id, ft_table)
		chnl.send_msg(COORDINATOR, 'ack')


def msg_add_key(command, my_id, ft_table, key_vld, process_vld):
	'''
	# Add message from the coordinator (only at initialization) or other process to transfer key storage
	# 1. update key_valid array
	'''
	global chnl
	for i in range(1, len(command)):
		key_vld[int(command[i])] = 1

	chnl.send_msg(COORDINATOR, 'ack')


def msg_find_key(command, my_id, ft_table, key_vld, process_vld):
	'''
	# Find message from the coordinator or other process
	# 1. return key if stored locally, else send it to the next process according to ft_table
	'''
	global chnl
	key_id = int(command[1])
	if key_vld[key_id] :
		print >> sys.stderr, '\nProcess', my_id, ' has the key:', key_id, "\n"
		chnl.send_msg(COORDINATOR, 'ack')
	else :
		i = 0
		while not key_ring_between(ft_table[i].interval, ft_table[(i+1) % 8].interval, key_id) :
			i = i + 1
		chnl.send_msg(ft_table[i].succ, 'find ' + command[1])


def msg_show_key(command, my_id, ft_table, key_vld, process_vld):
	'''Displays process ID and keys stored'''
	global chnl
	if DEBUG1:
		print >> sys.stderr, ''
	if DEBUG1:
		print >> sys.stderr, 'process_id = ', my_id, 'key_vld = ', vld_array_string(key_vld)
	if DEBUG1:
		print >> sys.stderr, 'process_id = ', my_id, 'procee_vld = ', vld_array_string(process_vld)
	if DEBUG1:
		print >> sys.stderr, 'ft table for process_id = ', my_id
	for i in range (8):
		if DEBUG1:
			print >> sys.stderr, '[', i, ']', 'interval = ', ft_table[i].interval, '	succ = ',  ft_table[i].succ
	if DEBUG1:
		print >> sys.stderr, ''

	print str(my_id) + vld_array_string(key_vld)

	chnl.send_msg(COORDINATOR, 'ack')

##################################################################
# Process thread startup
##################################################################

def process(process_id):
	'''Thread to represent a process/node.'''
	my_process_id = process_id

	# start a receive channel
	print >> sys.stderr, 'Process thread started, id =', my_process_id

	# initailize ft_table, process_vld, key_vld
	ft_table = []
	for i in range (8):
		ft_table.append(0)
		start = (my_process_id + 2 ** i) % 256
		ft_table[i] = Ft_table(start, my_process_id)


	process_vld = []
	key_vld = []

	process_vld = [0]*256
	key_vld = [0]*256

	process_vld[my_process_id] = 1

	# setup send_channel to coordinator

	setup_message_handler()

	# done with the initialization - check for received messages
	while 1:
		msg = chnl.recv_msg(process_id, False)
		if msg != '':
			handle_message(msg, my_process_id, ft_table, key_vld, process_vld)








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

	# initialize the array to all 0s
	process_vld = [0]*256

	# start a receive socket - all processes will send to the coordinator thru this socket
	if DEBUG:
		print >> sys.stderr, "Coordinatior setup rcv channel"

	# Creates the set of channels and add a channel for the coordinator
	chnl = channels.Channels()
	chnl.add_channel(256)

	# start the process thread with id = 0 and rcv_channel = port + 1
	process_start(0)

	setup_input_handler()

	# all keys are initially stored in process 0
	message = 'add_key'
	for i in range(256):
		message = message + ' ' + str(i)

	chnl.send_msg(0, message)
	wait_for_ack()
	process_vld[0] = 1

	# finished with the initialization - now wait for input commands
	while 1:
		rd, wr, err = select.select([sys.stdin], [], [], 0)
		for io in rd:
			handle_input(io.readline(), process_vld)
