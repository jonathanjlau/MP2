#import util
import Queue
import random
import select
import socket
import sys
import threading
import time

sock_queue = []
msg_count = 0
DEBUG = True
DEBUG = False

class Rcv_channel:
	'''Handles channels from a server to other servers'''

	def __init__(self, channel_id, port):
		'''Creates an uninitialized set of channels from a server'''
		self.id = channel_id
		self.port = port 		
		self.sockets = []
		self.close = False
	        self.accepted = False

	def make_connections(self):
               return 
		
		
	def get_message(self):
                global sock_queue
		while sock_queue[self.port].empty():
                   pass
		rcv_data = sock_queue[self.port].get()
                return rcv_data

	def queue_init(self):
            global sock_queue
	    for i in range(0, 257):
		    sock_queue.append(Queue.Queue())

class Send_channel:
	'''Handles channels from a server to other servers'''
	def __init__(self, channel_id, port):
		'''Creates an uninitialized set of channels from a server'''
		self.id = channel_id
		self.port = port 		
		self.sockets = 0
		self.close = False

	def make_connections(self):
            return
		
	def send_message(self, data):
            global sock_queue
            global msg_count

            msg_count = msg_count + 1
            command = data.split()

            if DEBUG: print 'send from process', self.id, 'to process', self.port, ': message = ', data
   	    sock_queue[self.port].put(data)

	def get_msg_count(self):
            global msg_count
	    count = msg_count
	    msg_count = 0
	    return count



	
