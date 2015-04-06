#import util
import Queue
import random
import select
import socket
import sys
import threading
import time

class Rcv_channel:
	'''Handles channels from a server to other servers'''

	def __init__(self, channel_id, port):
		'''Creates an uninitialized set of channels from a server'''
		self.channel = channel_id
		self.port = port 		
		self.sockets = 0
		self.close = False

	def make_connections(self):
		'''Create connections to other servers using given configurations'''

		# Create sockets with other servers. Use IP of server making connection and port of server accepting connection

		server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		server_sock.bind(("0.0.0.0", self.port))
		server_sock.listen(1)
		self.sockets =server_sock
		print "rcv socket created - port = ", self.port

		
	def get_message(self):
		conn, addr = self.sockets.accept()
		print 'connected by ', addr

		data = conn.recv(4096)
		if not data:	
		    print '\nDisconnected from process'
		    sys.exit()
		print 'msg get = ', data    
		return data	    
	
class Send_channel:
	'''Handles channels from a server to other servers'''

	def __init__(self, channel_id, port):
		'''Creates an uninitialized set of channels from a server'''
		self.channel = channel_id
		self.port = port 		
		self.sockets = 0
		self.close = False

	def make_connections(self):
		client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		client_sock.settimeout(2)
		try :
			client_sock.connect(("", self.port))
		except :
			print 'Send channel - Unable to connect to ', self.port
			sys.exit()

		print "init Send_channel from id ", self.channel, "to port ", self.port
                self.sockets = client_sock
	
	def send_message(self, data):
		print "send ", data
		self.sockets.send(data)
	
