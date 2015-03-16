import mp1util
import Queue
import select
import socket
import threading
import time

class Channels:
	'''Handles channels from a server to other servers'''

	def __init__(self, server):
		'''Creates an uninitialized set of channels from a server'''
		self.name = server
		self.sockets = {}
		self.delays = {}
		self.last_recv = {}
		self.buffers = {}
		self.name_map = {}
		self.send_queue = Queue.Queue()
		self.delay_queue = Queue.PriorityQueue()
		self.recv_queue = Queue.Queue()

	def make_connections(self, config):
		'''Create connections to other servers using given configurations'''

		# Create sockets with other servers. Use IP of server making connection and port of server accepting connection
		server_sockets = []
		name_map = {}
		for other_name in ['A', 'B', 'C', 'D', 'Seq']:
			# Listen to servers with larger names
			if self.name < other_name:
				server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				ip = config['ip-' + other_name];
				port = int(config['port-' + self.name])
				server_sock.bind((ip, port))
				server_sock.listen(1)
				server_sockets.append(server_sock)
				name_map[server_sock.fileno()] = other_name
				self.delays[other_name] = config['delay-' + self.name + '-' + other_name]

			# Connect to servers with smaller names
			elif self.name > other_name:
				client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				ip = config['ip-' + self.name];
				port = int(config['port-' + other_name])
				try:
					client_sock.connect((ip, port))
				except:
					print 'Cannot connect to ' + other_name
					sys.exit()
				print 'Connected to ' + other_name
				self.sockets[other_name] = client_sock
				self.delays[other_name] = config['delay-' + other_name + '-' + self.name]
				self.last_recv[other_name] = 0.0
				self.buffers[other_name] = []
				self.name_map[client_sock.fileno()] = other_name
				client_sock.close() # Remove

		# Accept connections from servers with smaller names
		accepts_left = len(server_sockets)
		while accepts_left > 0:
			rd, wr, err = select.select(server_sockets, [], [])
			for server_sock in rd:
				client_sock, addr = server_sock.accept()
				client_name = name_map[server_sock.fileno()]
				print 'Connected to ' + client_name
				self.sockets[client_name] = client_sock
				accepts_left -= 1
				self.last_recv[client_name] = 0.0
				self.buffers[client_name] = []
				self.name_map[client_sock.fileno()] = client_name
				client_sock.close()# Remove

		# Close all server sockets
		for sock in server_sockets:
			sock.close()

		self.bufsize = int(config['buffer-size'])

	def put_message(self, message):
		'''Add a message to the send queue'''
		self.send_queue.put(message)

	def send_message(self):
		'''Thread function that sends messages in the queue to their destination'''
		while 1:
			message = self.send_queue.get()
			dest = message['dest']
			message['send-time'] = time.time()
			message['recv-time'] = message['send-time']
			msgstr = mp1util.compress_message(message)
			self.sockets[dest].sendall(msgstr)
	
	def recv_message(self):
		'''Thread function for receiving messages with delay'''
		while 1:
			# Check whether sockets can be read
			rd, wr, err = select.select(self.sockets.values(), [], [])
			for sock in rd:
				sender = self.name_map[sock.fileno()]
				received = sock.recv(self.bufsize)
				self.buffers[sender].extend(list(received))

				# Extract as many messages from the received string as possible
				while 1:
					msg = mp1util.extract_message(self.buffers[sender])
					if msg == None:
						break
					else:
						# Update receive time for this connection, added the smallest number necessary to distinguish previous max time
						msg['recv-time'] = float(msg['recv-time']) + random.uniform(0, self.delays[sender])
						msg['recv-time'] = max(msg['recv-time'], self.last_recv + 0.000001)
						self.last_recv = msg['recv-time']
						self.delay_queue.add((msg['recv-time'], msg))

			# Check whether delayed messages can be delivered
			while 1:
				try:
					(recv_time, msg) = self.delay_queue.get()
					cur_time = time.time()
					if recv_time > cur_time:
						self.delay_queue.add((recv_time, msg))
						break
					else:
						msg['recv-time'] = cur_time
						self.recv_queue.add(msg)
				except:
					break

	def get_message(self):
		'''Returns a message that has been delivered, None if there is no message to deliver'''
		try:
			return self.recv_queue.get()
		except:
			return None
