import select
import socket

class Channels:
	'''Handles channels from a server to other servers'''

	def __init__(self, server):
		'''Creates an uninitialized set of channels from a server'''
		self.name = server
		self.sockets = {}
		self.delays = {}

	def make_connections(self, servers, config):
		'''Create connections to other servers with specific configurations'''

		# Connect to the sequencer if not the sequencer
		if self.name != 'seq':
			self.sequencer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			ip = config['ip-' + self.name];
			port = int(config['port-seq'])
			coord.connect((ip, port))
			print 'Connected to sequencer'
			coord.close() # remove

		# Create sockets with other servers. Use IP of server making connection and port of server accepting connection
		server_sockets = []
		name_map = {}
		for other_name in servers:
			# Listen to servers with larger names
			if self.name < other_name:
				server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				ip = config['ip-' + other_name];
				port = int(config['port-' + self.name])
				server_sock.bind((ip, port))
				server_sock.listen(1)
				server_sockets.append(server_sock)
				name_map[server_sock.fileno()] = other_name

			# Connect to servers with smaller names
			elif self.name > other_name:
				client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				ip = config['ip-' + self.name];
				port = int(config['port-' + other_name])
				client_sock.connect((ip, port))
				print 'Connected to server ' + other_name
				self.sockets[other_name] = client_sock
				client_sock.close() # Remove

		# Accept connections from servers with smaller names
		accepts_left = len(server_sockets)
		while accepts_left > 0:
			rd, wr, err = select.select(server_sockets, [], [])
			for server_sock in rd:
				client_sock, addr = server_sock.accept()
				client_name = name_map[server_sock.fileno()]
				print 'Connected to server ' + client_name
				self.sockets[client_name] = client_sock
				accepts_left -= 1
				client_sock.close()# Remove

		# Close all server sockets
		for sock in server_sockets:
			sock.close()
		pass
