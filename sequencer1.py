# Coordinator server

import sys
import threading

import channels
import util

'''
Main function of the coordinator server

Description:
The sequencer keeps a queue of messages received from each server and broadcasts the messages
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
