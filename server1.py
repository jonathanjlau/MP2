#  server

import collections
import socket, select, string, sys
import Queue
import threading
import datetime
import time
import random

import util
import channels


# Global variables
server_id = -1
key_value_pairs = {}
search = {}
active_command = None
input_handler = {}
message_handler = {}
display_prompt = True

'''
Class to store message attributes
'''
class Command:
	def __init__(self, message, delay):
		self.message = message
		self.time = time.time()
		self.delay = delay



'''
Simple prompt to remind the server of their id
'''
def prompt():
	global server_id
	sys.stdout.write('< Server ' + server_id + ' / ' + id_to_alpha[server_id] + ' > ')
	sys.stdout.flush()


'''
Deletes the key-value pair
'''
def delete_key(request):
	global key_value_pairs

	key_to_delete = request[1]
	key_deleted = key_value_pairs.pop(key_to_delete, None)

	if key_deleted != None:
		print "\nKey " + key_to_delete + " deleted"
	else:
		print "\nKey-value pair not found."
	prompt()

'''
Gets the key-value pair
'''
def get_key(request, consistency_model = -1):
	global key_value_pairs

	key_to_get = request[1]
	consistency_model = request[2]
	value_gotten = key_value_pairs.get(key_to_get, None)
	if (value_gotten == None):
		print "Key-value pair not found."

	else:
		if (consistency_model == "1" or consistency_model == "2"):
			print "get " + key_to_get + " = " + value_gotten

'''
Inserts the key-value pair
'''
def insert_key(request):
	global key_value_pairs

	key_to_insert = request[1]
	value_to_insert = request[2]
	consistency_model = request[3]
	key_value_pairs[key_to_insert] = value_to_insert

	print "\nInserted key " + key_to_insert
	prompt()


'''
Updates the key-value pair
'''
def update_key(request, local_update = False):
	global key_value_pairs

	key_to_update = request[1]
	value_to_update = request[2]
	consistency_model = request[3]
	old_value = key_value_pairs[key_to_update]
	key_value_pairs[key_to_update] = value_to_update

	if (local_update == True):
		print "\nKey " + key_to_update + " changed from " + old_value + " to " + value_to_update

	else:
		print "\nKey " + key_to_update + " updated to " + value_to_update
	prompt()



'''
The handler for the different messages received from the coordinator server.

Possible coordinator messages:
	server id
	message source_server actual_message
	delete key source_server
	get key model source_server
	insert key value model source_server
	update key value model source_server
	search key source_server
	search-reply key key_available original_source_server original_receiver_server
'''
def message_handl(data):
	global server_id
	global search
	global active_command


	# Extract the message from the coordinator
	request = data.split()

	# If the message begins with "server", set your new id
	if request[0].lower() == "server":

		server_id = request[1]
		print "I am server " + server_id + " / " + id_to_alpha[server_id].upper()
		prompt()

	# If it is a delete message, delete the information associated with that key
	elif (request[0].lower() == "delete"):
		delete_key(request)
		source_server = request[3]
		if source_server == server_id:
			active_command = None
		add_message_to_queue("ACK " + server_id, server_id)
	# If it is a get message, get the information associated with that key according to who's request it was
	elif (request[0].lower() == "get"):
		source_server = request[3]
		consistency_model = request[2]
		if consistency_model == "1":
			if source_server == server_id:
				print ""
				get_key(request, consistency_model)
				active_command = None
				prompt()

			# Send an ACK back to the coordinator server with a delay based on your server_id
			add_message_to_queue("ACK " + server_id, server_id)

		active_command = None


	# If it is an insert message, insert the key-value pair into the dictionary according to who's request it was
	elif (request[0].lower() == "insert"):
		source_server = request[4]
		consistency_model = request[3]

		if consistency_model == "1" or consistency_model == "2":
			insert_key(request)
			if source_server == server_id:
				active_command = None
			# Send an ACK back to the coordinator server with a delay based on your server_id
			add_message_to_queue("ACK " + server_id, server_id)



	# If it is an update message, update the key-value pair into the dictionary according to who's request it was
	elif (request[0].lower() == "update"):
		source_server = request[4]
		consistency_model = request[3]

		if consistency_model == "1" or consistency_model == "2":
			if source_server == server_id:
				update_key(request, True)
				active_command = None
			else:
				update_key(request, False)

			# Send an ACK back to the coordinator server with a delay based on your server_id
			add_message_to_queue("ACK " + server_id, server_id)


	# If it is a search message, try to find the key and report the result back to the source server
	elif (request[0].lower() == "search"):
		source_server = request[2]
		search_key = request[1]
		key_found = key_value_pairs.get(search_key, None)

		if key_found != None:
			key_available = "true"
		else:
			key_available = "false"

		# Send the reply back to the coordinator server to forward to the source server
		add_message_to_queue("search-reply " + search_key + " " + key_available + " " + source_server + " " + server_id, source_server)

	# If it is a search-reply message, get the response from the other servers for whether or not the have the key.
	# search-reply key key_available original_source_server original_receiver_server
	elif (request[0].lower() == "search-reply"):
		search_key = request[1]
		key_available = request[2]
		originating_server = request[3]
		receiver_server = request[4]

		if key_available == "true":
			search[search_key][int(receiver_server)] = True
		else:
			search[search_key][int(receiver_server)] = False


		if len(search[search_key]) == 4 and originating_server == server_id:
			search_response = "Servers with key " + search_key + ": "
			for i in range(1,5):
				if search[search_key][i] == True:
					search_response = search_response + str(i) + " "

			print search_response
			prompt()

			active_command = None

	#If it is an unrecognized message, print it out.
	else:

		sys.stdout.write(data)
		prompt()

def setup_input_handler():
	'''Sets up a dictionary to execute input commands'''
	global input_handler
	input_handler['Send'] = send_string
	return

def setup_message_handler():
	'''Sets up a dictionary to execute received messages'''
	global message_handler
	message_handle['Send'] = recv_string
	return

'''
The handler for the different options that the user can input via the command line or command file

Possible user commands:
	send message receiver(a, b, c, or d)
	delete key
	get key model
	insert key value model
	update key value model
	show-all
	search key
	delay time
	load (loads all the respective commands for this server from the command file)


'''
def handle_input(string):
	global key_value_pairs
	global search
	global active_command

	'''Executes the command in the given string'''
	command = string.split()

	# Ignore empty commands
	if len(command) == 0:
		return

	# Check whether command is supported
	if command[0] not in input_handler:
		print 'Unrecognized command ' + command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given command
	input_handler[command[0]](command)
	return


def handle_message(message):
	command = message['command']

	# Check whether the message is supported
	if command not in message_handler:
		print 'Unrecognized command ' + command
		return

	# Use a dictionary as a switch case to call the function corresponding to the given message
	message_handler[command](message)
	return


def send_string(command):
	'''Handles the send string command. Sends a message to the dest server with the string to display'''

	# Check the command has the right number of parameters
	if len(command) != 3:
		print 'Usage: Send string destination'
		return

	# Creates a message telling the target server to display a string
	message = {}
	message['command'] = command[0]
	message['string'] = command[1]
	message['dest'] = command[2]
	message['sender'] = channels.name
	message['send-time'] = time.time()
	channels.put_message(message)

	# Output a message that the message is sent
	print 'Send "' + message['string'] + '" to ' + message['dest'] + ', system time is ' + str(message['send-time'])
	display_prompt = True
	return


def recv_string(message):
	'''Displays the string sent through the message'''
	string = message['string']
	sender = message['sender']
	delay = float(message['delay'])
	systime = message['recv-time']
	print 'Received "' + string + '" from ' + sender + ', Max delay is ' + str(delay) + ' s, system time is ' + str(systime)
	return


def rem_input(data):
	request = data.split()


	if False:
		pass


	# If it is a delete request, broadcast that request so that all the nodes delete the give key
	elif (request[0].lower() == "delete"):
		if (len(request) < 2):
			print "DELETE usage: delete key"
		else:
			add_message_to_queue(data.rstrip('\n') + " " + server_id, server_id)
			active_command = "delete"

	# If it is a get request, either get the local information immediately or broadcast a read request, depending on the consistency model extracted from the request
	elif (request[0].lower() == "get"):
		if (len(request) < 3):
			print "GET usage: get key model"
		else:
			consistency_model = request[2]
			if(consistency_model == '1'):
				add_message_to_queue(data.rstrip('\n') + " " + server_id, server_id)
				active_command = "get"

			if(consistency_model == '2'):
				get_key(request, consistency_model)


	# If it is an insert request, insert the key-value pair into the dictionary or broadcast the request, depending on the consistency model extracted from the request
	elif (request[0].lower() == "insert"):
		if (len(request) < 4):
			print "INSERT usage: insert key value model"
		else:
			consistency_model = request[3]
			if(consistency_model == '1' or consistency_model == '2'):
				add_message_to_queue(data.rstrip('\n') + " " + server_id, server_id)

			active_command = "insert"

	# If it is an update request, update the key-value pair into the dictionary or broadcast the request, depending on the consistency model extracted from the request
	elif (request[0].lower() == "update"):
		if (len(request) < 4):
			print "UPDATE usage: update key value model"
		else:
			consistency_model = request[3]
			if(consistency_model == '1' or consistency_model == '2'):
				add_message_to_queue(data.rstrip('\n') + " " + server_id, server_id)

			active_command = "update"

	# If it is a show-all request, print out all the entries in the dictionary
	elif (request[0].lower() == "show-all"):
		for key,val in key_value_pairs.items():
			print key + " -> " + val

	# If it is a search request, pull the information about the other servers
	elif (request[0].lower() == "search"):
		key = request[1]

		search[key] = {}

		# Send a broadcast requesting information about the key's availability from the other servers
		add_message_to_queue("search " + key + " " + server_id, server_id)

		active_command = "search"

	# If it is a delay request, delay the invocation by the specified time
	elif (request[0].lower() == "delay"):
		delay_time = float(request[1])
		while active_command != None:
			pass
		time.sleep(delay_time)

	# If it is a load request, execute command from config_file
	elif (request[0].lower() == "load"):
		my_command_format = id_to_alpha[server_id] + ':'
		for command in input_commands:
			command_parse = command.split()
			if command_parse[0].lower() == my_command_format:
				command_input_handler(" ".join(command_parse[1:]))

	# If it is not a valid request, send it to the coordinator to broadcast whatever the message was
	else:
		coordinator.send(data)

	# If an there is an active command, wait until that command has completed. Then process the next command from the input. For Linearizability and Sequential Consistency.
	while active_command != None:
		pass

	
def setup_input_handler():
	'''Sets up a dictionary to execute input commands'''
	global input_handler
	input_handler['Send'] = send_string

def setup_message_handler():
	'''Sets up a dictionary to execute received messages'''
	global message_handler
	message_handler['Send'] = recv_string

'''
Main function for the individual servers.

Description:
Each server connects to the coordinator and the coordinator will give them their unique ids.
It will then spawn three threads: one to handle the user input, one to handle the messages from the coordinator, and another to handle delayed message sending.
The thread handling the user input will have a handler for every option listed in the spec as well as a function "load" that will load the commands from the command file.
The thread handling the coordinator's messages will also have a handler for every message that is sent.
The thread handling the delayed message sending will monitor four outgoing queues, one for each server.
More detail can be found in each thread's description.
'''
if __name__ == "__main__":

	# Make sure the user enters the config file
	if ((len(sys.argv) < 3)):
		print 'Usage: python server.py name config_file'
		sys.exit()

	# Read server name and config from CL arguments
	name = sys.argv[1]
	config = util.read_key_value_file(sys.argv[2])
	channels = channels.Channels(name)
	channels.make_connections(config)

	# Instatiates the message sender thread
	send_thread = threading.Thread(target=lambda : channels.send_message())
	send_thread.start()
	recv_thread = threading.Thread(target=lambda : channels.recv_message())
	recv_thread.start()

	# Initialize the input command storage for the input files
	input_commands = []

	# If a command file was given, add those commands into an array
#	if 'commands' in config:
#		input_commands = collections.deque(util.read_key_value_file(config['commands']))

	setup_input_handler()
	setup_message_handler()
	print 'Setup complete'
	while 1:
		# Execute a command from the commands file if there is one
		if len(input_commands) > 0:
			display_prompt = False
			handle_input(input_commands.popleft())
			if len(input_commands) == 0:
				display_prompt = True

		# Otherwise check for inputs from the commandline
		else:
			if display_prompt:
				print 'Server ' + channels.name + ':'
				display_prompt = False
			rd, wr, err = select.select([sys.stdin], [], [], 0)
			for io in rd:
				handle_input(io.readline())

		# Check for received messages
		msg = channels.get_message()
		if msg is not None:
			handle_message(msg)

