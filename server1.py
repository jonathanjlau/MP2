#  server

import collections
import copy
import socket, select, string, sys
import Queue
import threading
import datetime
import time
import random

import util
import channels

# Global variables
key_value_store = {}
input_handler = {}
message_handler = {}
expected_replies = 0
expected_id = 0
message_id = 0
search_result = ''

# Completion time of previous command
prev_time = time.time()

# Time when next command can be started
next_time = time.time()


def handle_input(string):
	'''Executes the command in the given string'''
	command = string.split()
	global input_handler
	global prev_time

	# Ignore empty commands
	if len(command) == 0:
		return

	# Check whether command is supported
	if command[0] not in input_handler:
		print 'Unrecognized command ' + command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given command
	input_handler[command[0]](command)
	prev_time = time.time()
	return


def handle_message(message):
	'''Responds to the given message'''
	command = message['command']
	global message_handler

	# Check whether the message is supported
	if command not in message_handler:
		print 'Unrecognized command ' + command
		return

	# Use a dictionary as a switch case to call the function corresponding to the given message
	message_handler[command](message)
	return


def send_string(command):
	'''Handles the send string command. Sends a message to the dest server with the string to display'''

	# Check that the command has the right number of parameters
	if len(command) != 3:
		print 'Usage: Send string destination'
		return

	# Creates a message telling the target server to display a string
	message = {}
	message['command'] = command[0]
	message['string'] = command[1]
	message['dest'] = command[2]
	message['send-time'] = time.time()
	message['recv-time'] = message['send-time']
	message['sender'] = channels.name
	channels.put_message(message)

	# Output a message that the message is sent
	print 'Send "' + message['string'] + '" to ' + message['dest'] + ', system time is ' + str(message['send-time'])
	return


def recv_string(message):
	'''Displays the string sent by the message'''
	string = message['string']
	sender = message['sender']
	delay = float(message['delay'])
	systime = message['recv-time']
	print 'Received "' + string + '" from ' + sender + ', Max delay is ' + str(delay) + ' s, system time is ' + str(systime)
	return


def send_delete(command):
	'''Sends a delete key message to every other server'''
	if (len(command) != 2):
		print "DELETE usage: delete key"
		return

	# Creates a message telling servers to delete a key
	message = {}
	message['command'] = command[0]
	message['key'] = command[1]
	message['send-time'] = time.time()
	message['recv-time'] = message['send-time']
	message['sender'] = channels.name

	# Sends a copy of the message to every other server
	for server in ['A', 'B', 'C', 'D']:
		if server != channels.name:
			msg = copy.copy(message)
			msg['dest'] = server
			channels.put_message(msg)

	# Executes the delete on self
	recv_delete(message)
	return
	

def recv_delete(message):
	'''Deletes the key specified in the received message'''
	key = message['key']

	# Deletes key if present and print a message
	if key in key_value_store:
		del key_value_store[key]
		print 'Key ' + key + ' deleted'

	# Print error message if the key does not exist
	else:
		print 'Key ' + key + ' does not exist'
	return

def send_get(command):
	'''Handles a get command'''
	if (len(command) != 3):
		print "GET usage: get key model"
		return
	global expected_replies
	global expected_id
	global message_id
	model = command[2]
	key = command[1]
	
	# Prepares a get message to send
	message = {}
	message['command'] = command[0]
	message['key'] = command[1]
	message['model'] = command[2]
	message['send-time'] = time.time()
	message['recv-time'] = message['send-time']
	message['sender'] = channels.name
	message['id'] = message_id
	message_id += 1
	# Linearizability, asks the sequencer to broadcast the get message and wait for the reply
	if model == '1':
		message['dest'] = 'Seq'
		channels.put_message(message)
		expected_replies = 1

	# Sequential consistency, handle message immediately
	elif model == '2':
		if key in key_value_store:
			print 'get(' + key + ') = ' + key_value_store[key][0]
		else:
			print 'key ' + key + ' does not exist'

	# Eventual consistency R=1, handle message immediately
	elif model == '3':
		if key in key_value_store:
			print 'get(' + key + ') = (' + key_value_store[key][0] + ', ' + str(key_value_store[key][1]) + ')'
		else:
			print 'key ' + key + ' does not exist'

	# Eventual consistency R=2, send get message to other servers and wait for one reply
	elif model == '4':
		expected_replies = 1
		expected_id = message['id']
		for server in ['A', 'B', 'C', 'D']:
			if server != channels.name:
				msg = copy.copy(message)
				msg['dest'] = server
				channels.put_message(msg)
	else:
		print 'Model ' + model + ' not recognized'
	return


def recv_get(message):
	'''Responds to the get message'''
	global expected_replies
	global prev_time
	model = message['model']
	key = message['key']

	# Linearizability, print value if is own message
	if model == '1':
		if message['sender'] == channels.name:
			expected_replies = 0
			prev_time = time.time()
			if key in key_value_store:
				print 'get(' + key + ') = ' + key_value_store[key][0]
			else:
				print 'key ' + key + ' does not exist'

	# Eventual consistency R=2, send ack message with values and timestamp in this server
	elif model == '4':
		message['command'] = 'get-ack'
		message['dest'] = message['sender']

		# Check whether key is in key value store, send an error code otherwise
		if key in key_value_store:
			message['value'] = key_value_store[key][0]
			message['timestamp'] = key_value_store[key][1]
		else:
			message['timestamp'] = -1
		channels.put_message(message)
	
	return


def recv_get_ack(message):
	'''Respond to ack of get message for eventual consistency'''
	global expected_replies
	global expected_id
	global prev_time
	recv_id = int(message['id'])
	key = message['key']
	timestamp = float(message['timestamp'])

	# Check whether the server is expecting a reply and the expected original send time of the message is correct
	if expected_replies == 1 and expected_id == recv_id:
		expected_replies = 0

		# Neither this server nor the responding server contains the key
		if key not in key_value_store and timestamp < 0:
			print 'Key ' + key + ' does not exist'

		# Only the this server does not contain the key
		elif key not in key_value_store:
			print 'get(' + key + ') = (' + message['value'] + ', ' + str(timestamp) + ')'

		# Only the other server does not contain the key
		elif timestamp < 0:
			print 'get(' + key + ') = (' + key_value_store[key][0] + ', ' + str(key_value_store[key][1]) + ')'

		# Both servers contain the key, check which one has the later timestamp
		else:
			selftime = float(key_value_store[key][1])
			if selftime > timestamp:
				print 'get(' + key + ') = (' + key_value_store[key][0] + ', ' + str(selftime) + ')'
				print '(' + message['value'] + ', ' + str(timestamp) + ')'
			else:
				print 'get(' + key + ') = (' + message['value'] + ', ' + str(timestamp) + ')'
				print '(' + key_value_store[key][0] + ', ' + str(selftime) + ')'

		# Update completion time of previous command as this requires a reply
		prev_time = time.time()
	
	return


def send_insert(command):
	'''Handles an insert command'''
	if (len(command) != 4):
		print "INSERT usage: insert key value model"
		return
	global expected_replies
	global expected_id
	global message_id
	model = command[3]
	
	# Prepares an insert message to send
	message = {}
	message['command'] = command[0]
	message['key'] = command[1]
	message['value'] = command[2]
	message['model'] = command[3]
	message['send-time'] = time.time()
	message['recv-time'] = message['send-time']
	message['sender'] = channels.name
	message['id'] = message_id
	message_id += 1

	# Linearizability or sequential consistency model, asks the sequencer to broadcast the insert message and wait for the reply
	if model == '1' or model == '2':
		message['dest'] = 'Seq'
		channels.put_message(message)
		expected_replies = 1

	# Eventual consistency, send message to others one by one and insert into own key value store
	elif model == '3' or model == '4':

		# Flag that a reply from another server is expected for W=2
		if model == '4':
			expected_replies = 1
			expected_id = message['id']
		
		# Send insert message to other servers
		for server in ['A', 'B', 'C', 'D']:
			if server != channels.name:
				print server
				msg = copy.copy(message)
				msg['dest'] = server
				channels.put_message(msg)

		# Insert value of own key_value_store
		recv_insert(message)
		
	else:
		print 'Model ' + model + ' not recognized'
	return


def recv_insert(message):
	'''Respond to the insert message'''
	# If the key_value_store already contains the key, handle this like an update
	key = message['key']
	if key in key_value_store:
		recv_update(message)
		return

	global expected_replies
	global prev_time
	model = message['model']
	value = message['value']
	timestamp = float(message['send-time'])

	key_value_store[key] = (value, timestamp)
	print 'Inserted key ' + key

	if (model == '1' or model == '2'):
		# Receiving own message from broadcast
		if message['sender'] == channels.name:
			expected_replies = 0
			prev_time = time.time()

	# Eventual consistency, need to send an ack
	elif message['sender'] != channels.name:
		message['command'] = 'insert-ack'
		message['dest'] = message['sender']
		channels.put_message(message)
	
	return


def recv_insert_ack(message):
	'''Respond to ack of insert message for eventual consistency'''
	global expected_replies
	global expected_id
	global prev_time
	recv_id = int(message['id'])
	key = message['key']

	print 'Inserted key ' + key

	# Check whether the server is expecting a reply to complete a command and the expected original send time of the message is correct
	if expected_replies == 1 and expected_id == recv_id:
		expected_replies = 0
		prev_time = time.time()

	return


def send_update(command):
	'''Handles an update command'''
	if (len(command) != 4):
		print "UPDATE usage: update key value model"
		return
	global expected_replies
	global expected_id
	global message_id
	model = command[3]
	
	# Prepares an update message to send
	message = {}
	message['command'] = command[0]
	message['key'] = command[1]
	message['value'] = command[2]
	message['model'] = command[3]
	message['send-time'] = time.time()
	message['recv-time'] = message['send-time']
	message['sender'] = channels.name
	message['id'] = message_id
	message_id += 1

	# Linearizability or sequential consistency model, asks the sequencer to broadcast the insert message and wait for the reply
	if model == '1' or model == '2':
		message['dest'] = 'Seq'
		channels.put_message(message)
		expected_replies = 1

	# Eventual consistency, send message to others one by one and update own key value store
	elif model == '3' or model == '4':

		# Flag that a reply from another server is expected for W=2
		if model == '4':
			expected_replies = 1
			expected_id = message['id']
		
		# Send update message to other servers
		for server in ['A', 'B', 'C', 'D']:
			if server != channels.name:
				msg = copy.copy(message)
				msg['dest'] = server
				channels.put_message(msg)

		# Insert value of own key_value_store
		recv_update(message)
		
	else:
		print 'Model ' + model + ' not recognized'
	return


def recv_update(message):
	'''Respond to the update message'''

	global expected_replies
	global prev_time
	key = message['key']
	model = message['model']
	value = message['value']
	timestamp = float(message['send-time'])

	if key in key_value_store:
		prev_timestamp = key_value_store[key][1]
		# Received value is newer
		if timestamp > prev_timestamp:
			print 'Key ' + key + ' changed from ' + key_value_store[key][0] + ' to ' + value
			key_value_store[key] = (value, timestamp)
			message['timestamp'] = timestamp
		# Received value is older
		else:
			print 'Key ' + key + ' was not changed'
			message['timestamp'] = prev_timestamp

	# Key does not exist, signal using timestamp field message	
	else:
		print 'Key ' + key + ' does not exist'
		message['timestamp'] = -1

	# Receiving own message from broadcast
	if (model == '1' or model == '2'):
		if message['sender'] == channels.name:
			expected_replies = 0
			prev_time = time.time()

	# Eventual consistency, need to send an ack
	elif message['sender'] != channels.name:
		message['command'] = 'update-ack'
		message['dest'] = message['sender']
		channels.put_message(message)
	
	return


def recv_update_ack(message):
	'''Respond to ack of update message for eventual consistency'''
	global expected_replies
	global expected_id
	global prev_time
	recv_id = int(message['id'])
	sendtime = float(message['send-time'])
	timestamp = float(message['timestamp'])
	key = message['key']
	value = message['value']

	if timestamp < 0:
		print 'Key ' + key + ' does not exist'
	elif timestamp > sendtime:
		print 'Key ' + key + ' was not updated'
	else:
		print 'Key ' + key + ' updated to ' + value

	# Check whether the server is expecting a reply to complete a command and the expected original send time of the message is correct
	if expected_replies == 1 and expected_id == recv_id:
		expected_replies = 0
		prev_time = time.time()

	return

def showall(command):
	'''Displays all key value pairs stored on this server'''
	for key, entry in key_value_store.iteritems():
		print key + ': ' + entry[0]

def send_search(command):
	'''Handles a insert command'''
	if (len(command) != 2):
		print "SEARCH usage: search key"
		return

	global expected_replies
	global expected_id
	global message_id
	global search_result
	
	# Prepares a search message to send
	message = {}
	message['command'] = command[0]
	message['key'] = command[1]
	message['send-time'] = time.time()
	# This makes the message delivered immediately even when delay is added
	message['recv-time'] = 0
	message['sender'] = channels.name
	message['id'] = message_id
	message_id += 1
	
	# Mark that 3 acks are expected
	expected_replies = 3
	expected_id = message['id']
	search_result = ''

	for server in ['A', 'B', 'C', 'D']:
		if server != channels.name:
			msg = copy.copy(message)
			msg['dest'] = server
			channels.put_message(msg)

def recv_search(message):
	'''Responds with whether the server contains the key'''
	key = message['key']
	message['command'] = 'search-ack'
	message['recv-time'] = 0
	message['dest'] = message['sender']

	# Marks the message if the key is found in this server
	if key in key_value_store:
		message['sender'] = channels.name

	channels.put_message(message)

def recv_search_ack(message):
	'''Checks acks to search messages and displays the list of servers with the key after all responses are received'''
	global expected_replies
	global expected_id
	global search_result
	global prev_time

	recv_id = int(message['id'])
	key = message['key']
	
	# Check whether the server is expecting a reply to complete a command and the expected original send time of the message is correct
	if expected_replies > 0 and expected_id == recv_id:
		expected_replies -= 1
		sender = message['sender']

		# Checks flag of whether key was found in responding server
		if sender != channels.name:
			search_result += sender + ','

		# Received all replies
		if expected_replies == 0:
			if key in key_value_store:
				search_result += channels.name

			if len(search_result) > 0:
				print 'Key ' + key + ' is found in servers ' + search_result
			else:
				print 'Key ' + key + ' is not found in any server'
			prev_time = time.time()

	return


def delay(command):
	'''Sets the time when the next command can be input'''
	if (len(command) != 2):
		print "DELAY usage: delay T"
		return

	global next_time
	global prev_time
	try:
		delay = float(command[1])
		# Marks that the next command should be started after delay time passed since the response of the previous operation
		next_time = prev_time + delay
	except:
		print 'delay time is not a number'

	return


def setup_input_handler():
	'''Sets up a dictionary to execute input commands'''
	global input_handler
	input_handler['send'] = send_string
	input_handler['delete'] = send_delete
	input_handler['get'] = send_get
	input_handler['insert'] = send_insert
	input_handler['update'] = send_update
	input_handler['show-all'] = showall
	input_handler['search'] = send_search
	input_handler['delay'] = delay

def setup_message_handler():
	'''Sets up a dictionary to execute received messages'''
	global message_handler
	message_handler['send'] = recv_string
	message_handler['delete'] = recv_delete
	message_handler['get'] = recv_get
	message_handler['get-ack'] = recv_get_ack
	message_handler['insert'] = recv_insert
	message_handler['insert-ack'] = recv_insert_ack
	message_handler['update'] = recv_update
	message_handler['update-ack'] = recv_update_ack
	message_handler['search'] = recv_search
	message_handler['search-ack'] = recv_search_ack

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

	# Make sure the user enters the server name and config file
	if ((len(sys.argv) < 3)):
		print 'Usage: python server.py name config_file'
		sys.exit()

	# Read server name and config from CL arguments and connect to other servers
	name = sys.argv[1]
	config = util.read_key_value_file(sys.argv[2])
	channels = channels.Channels(name)
	channels.make_connections(config)

	# Instatiates the message sender and receiver threads
	send_thread = threading.Thread(target=lambda : channels.send_message())
	send_thread.start()
	recv_thread = threading.Thread(target=lambda : channels.recv_message())
	recv_thread.start()

	setup_input_handler()
	setup_message_handler()
	print 'Server ' + channels.name + ' setup complete'

	while 1:
		# Check for inputs from the commandline when not expecting a reply message and passed delays
		if expected_replies == 0 and time.time() >= next_time:
			rd, wr, err = select.select([sys.stdin], [], [], 0)
			for io in rd:
				handle_input(io.readline())

		# Check for received messages
		msg = channels.get_message()
		if msg is not None:
			handle_message(msg)

