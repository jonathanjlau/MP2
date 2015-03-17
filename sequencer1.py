# Coordinator server

import sys
import threading

import channels
import util

'''
Main function of the coordinator server

Description:
The coordinator server will take up to four other servers and automatically assign them their idenities. (i.e. Server 1/a)
The main function will continually read from its socket and push its messages (other than ACKS) into the shared command queue and have the process_command_queue thread handle them.
For Step 1, it will forward any "send" messages from a server to another. It assumes that those servers have already handled the delays.
For Step 2-1 and 2-2, it will broadcast the data that it receives as long as it is not waiting for ACKS. Its part in handling linearizability and sequential consistency is to see if a server has sent a command requiring totally ordered broadcast. (insert, update, get, delete) After that type of broadcast, it will set an ACK counter and wait to receive the ACKs from each server before sending out another command. All commands that arrive while it is waiting for those ACKs are buffered in the command thread. The ACK counter is decremented in the main function.

'''
if __name__ == "__main__":

	# Makes sure that the user entered a config file
	if(len(sys.argv) < 2):
		print 'Usage: python coordinator.py config_file'
		sys.exit()

	# Read server name and config from CL arguments
	name = 'Seq'
	config = util.read_key_value_file(sys.argv[1])
	channels = channels.Channels(name)
	channels.make_connections(config)

	# Instatiates the message broacaster and receive threads
	broadcast_thread = threading.Thread(target=lambda : channels.broadcast_message())
	broadcast_thread.start()
	recv_thread = threading.Thread(target=lambda : channels.recv_message())
	recv_thread.start()
