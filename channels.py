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
                #print 'ID =', self.id, 'wait for get message on port', self.port
                #print 'wait for sokck,empty, port', sock_queue[self.port].empty(), self.port
		while sock_queue[self.port].empty():
                   pass
		rcv_data = sock_queue[self.port].get()
                print 'ID =', self.id, 'get message on port', self.port, ' msg = ', rcv_data
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
	        #print 'setup Send_channel: id = ', self.id, 'port = ', self.port

	def make_connections(self):
            return
		
	def send_message(self, data):
            global sock_queue
            global msg_count
            msg_count = msg_count + 1
            print 'send_message from ', self.id, 'to', self.port, ': data = ', data
   	    sock_queue[self.port].put(data)

	def get_msg_count(self):
            return msg_count


	
