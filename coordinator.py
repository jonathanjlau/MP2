# Coordinator server

import socket, select, sys
import Queue
import threading
import datetime
import time

import mp1util


# Global variables
ack_counter = 0
number_of_client = 0
id_to_socket = {}
cmd_queue = Queue.Queue()

'''
Function to broadcast the given message to all connected servers
'''
def broadcast_data (message):
	# Broadcast to everyone except the coordinator itself
	for socket in CONNECTION_LIST:
		if socket != server_socket:
			try:
				socket.send(message)
			except:
				socket.close()
				CONNECTION_LIST.remove(socket)

'''
Function to find an empty server id for the connecting server
'''
def find_empty_id():
	for i in range(1,5):
		key = str(i)
		if key not in id_to_socket:
			return key
		if id_to_socket[key] == -1:
			return key
	return "-1"


'''
Thread to process the command Queue. The main function will have already read from the socket and pushed to the command queue if it is NOT an ack. If it is Linearizability or Sequentially Consistency, then it will wait until all the acks are received before the next command is popped. Otherwise, it is just a buffer for the commands.

Possible messages (other than a server broadcast):
	send message receiver_server
	search-reply key key_available original_sender original_receiver

'''
def process_command_queue():
	global ack_counter
	global number_of_client
	global id_to_socket
	global cmd_queue

	while 1:
		if ack_counter == 0 and not cmd_queue.empty():
			# get command from queue - instead of from socket
			new_cmd = cmd_queue.get()

			request = new_cmd.split()

			# A direct send request from one server to another, forward the message
			if request[0].lower() == "send":
				sender_id = socket_to_id[sock]
				sender_id_alpha = id_to_alpha[sender_id]
				sender_message = " ".join(request[1:-1])
				receiver_id = alpha_to_id[request[-1].lower()]
				receiver_socket = id_to_socket[receiver_id]
				receiver_socket.sendall("Message " + " " + sender_id_alpha + " " + sender_message)

					# A search-reply request from one server to another, forward the message only to the original source server
			elif request[0].lower() == "search-reply":
				original_sender_id = request[-2]
				original_source_socket = id_to_socket[original_sender_id]
				original_source_socket.sendall(data)


			   # Otherwise broadcast that message to all the clients
			else:
				broadcast_data(new_cmd)
				# If it is a totally ordered command from Linearizability or Sequential Consistency, then start the ACK counter and wait until all ACKS are received.
				if (request[-2] == "1") or (request[-2] == "2"):
					if (request[0].lower() == "insert") or (request[0].lower() == "delete") or (request[0].lower() == "update") or (request[0].lower() == "get"):
						ack_counter = number_of_client




'''
Main function of the coordinator server

Description:
The coordinator server will take up to four other servers and automatically assign them their idenities. (i.e. Server 1/a)
The main function will continually read from its socket and push its messages (other than ACKS) into the shared command queue and have the process_command_queue thread handle them.
For Step 1, it will forward any "send" messages from a server to another. It assumes that those servers have already handled the delays.
For Step 2-1 and 2-2, it will broadcast the data that it receives as long as it is not waiting for ACKS. Its part in handling linearizability and sequential consistency is to see if a server has sent a command requiring totally ordered broadcast. (insert, update, get, delete) After that type of broadcast, it will set an ACK counter and wait to receive the ACKs from each server before sending out another command. All commands that arrive while it is waiting for those ACKs are buffered in the command thread. The ACK counter is decremented in the main function.

'''


if __name__ == "__main__":

	# Makes sure that the user at least tries to enter a port number.
	if(len(sys.argv) < 2):
		print 'Usage: python coordinator.py config_file'
		sys.exit()

	# List to keep track of socket descriptors
	CONNECTION_LIST = []
	RECV_BUFFER = 4096
	PORT = 5000

	print sys.argv[1]
	# Assign the port number from the user input
	config = mp1util.read_config_file(sys.argv[1])
	PORT = config['port']

	# Socket setup
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	server_socket.bind(("0.0.0.0", PORT))
	server_socket.listen(10)


	# Add server socket to the list of readable connections
	CONNECTION_LIST.append(server_socket)

	# Hashmaps from id to respective alphabet letter or socket and vice versa for easier conversion
	socket_to_id = {}
	alpha_to_id = { 'a':'1', 'b':'2', 'c':'3', 'd':'4' }
	id_to_alpha = { '1':'a', '2':'b', '3':'c', '4':'d' }

	# ACK counter for Step 2
	print "Coordinator server started on port " + str(PORT)

   # Instatiates the queue thread that dequeue and process command
	queue_thread = threading.Thread(target=process_command_queue, args=())
	thread_list = []
	thread_list.append(queue_thread)

	# Start all the threads
	for thread in thread_list:
		thread.start()

	while 1:
		# Get the list sockets which are ready to be read through select
		read_sockets,write_sockets,error_sockets = select.select(CONNECTION_LIST,[],[])

		for sock in read_sockets:

			# A new server connection received through the server socket
			if sock == server_socket:

				sockfd, addr = server_socket.accept()


				# Put the accepted socket's data into the two-way hashmap
				empty_id = find_empty_id()
				socket_to_id[sockfd] = empty_id
				id_to_socket[empty_id] = sockfd
				CONNECTION_LIST.append(sockfd)
				number_of_client = len(CONNECTION_LIST) - 1

				print "Server " + socket_to_id[sockfd] +  " (%s, %s) connected" % addr


				# Tell the newly connected client what their id is
				sockfd.sendall("Server " + socket_to_id[sockfd] + "\n")




			# Message from an existing server
			else:

				# Try to process data recieved from client
				try:
					data = sock.recv(RECV_BUFFER)

					#print("DATA RECEIVED: " + data + "			" + str(datetime.datetime.now().time())) #FOR DEBUGGING, CAN DELETE

					# An ACK to tell the coordinator that a server has received the original broadcast message, for sequential consistency and linearizability.
					request = data.split()
					if request[0].upper() == "ACK":
						ack_counter = ack_counter - 1
					# Push all other commands to the shared command queue
					else:
						cmd_queue.put(data)

				except:
					broadcast_data("Client (%s, %s) is offline \n" % addr)
					print "Server (%s, %s) is offline" % addr
					sock_id = socket_to_id[sock]
					id_to_socket[sock_id] = -1
					sock.close()
					CONNECTION_LIST.remove(sock)
					continue

	server_socket.close()


