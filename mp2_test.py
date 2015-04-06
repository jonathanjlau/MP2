#  server

import collections
import copy
import socket, select, string, sys
import Queue
import threading
import util
import channels

# Global variables
input_handler = {}
message_handler = {}
base_port = 0

###############################################################
# Functions for the coordinator
###############################################################

def wait_for_ack(rcv_channel):
    msg = rcv_channel.get_message()
    # it should receive ack only
    if msg != 'ack':
       print 'Error in receivng message - not ACK' 
       sys.exit()



def setup_input_handler():    
	'''Sets up a dictionary to execute input commands'''
	global input_handler
	input_handler['join'] = add_process
	#input_handler['leave'] = delete_process
	#input_handler['find'] = find_key
	#input_handler['show'] = show_key
	

def handle_input(input_string, process_vld, send_channel):
	'''Executes the command in the given string from stdin'''
	command = input_string.split()

	# Ignore empty commands
	if len(command) == 0:
		return

	# Check whether command is supported
	if command[0] not in input_handler:
		print 'Unrecognized input command ' + command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given command
	input_handler[command[0]](command, process_vld, send_channel)
	return
        



def add_process(command, process_vld, send_channel):
	'''Handles the addition of a new process'''

	# Check that the command has the right number of parameters
	if len(command) != 2:
		print 'Usage: join process_number(0-255)'
		return
         
        new_process = int(command[1])
        print "adding new process ", new_process
        # start a new_process
        process_start(new_process, base_port+1+new_process, send_channel)

        # start a new write socket connect to the receive port of thew new process
	print 'SETUP send channel from coordinator to ', new_process
        send_channel[new_process] = channels.Send_channel(0, base_port + 1 + new_process)

	message = 'process_valid '
        for i in range(256):
          if ( process_vld[i]):
             message = message + str(i) + ' '

        send_channel[new_process].send_message(message)
        wait_for_ack(recv_channel);         

        # let all other process know that a new process is added

        for i in range(256):
           if process_vld[i] :
               send_channnel[i].send_message(('add_process ' + new_process))
               wait_for_ack

def process_start(process_id, port, send_channel):
    '''Function to start the process threads'''
    process_thread = threading.Thread(target=process, args=(process_id, port)) 
    process_thread.start()
    # setup send channel from coordinator to the new process
    send_channel[process_id] = channels.Send_channel(0, base_port + 1 + process_id)
    send_channel[process_id].make_connections()

























  ##################################################################
  # Functions for the process/node threads
  ##################################################################
    

##################################################################
# Simple class for a finger table entry. Each process/node has 8 of them.
##################################################################
class Ft_table:
   def __init__(self, interval, succ):
        self.interval = interval
        self.succ = succ





##################################################################
# helper fuctions that handle message received by the process
##################################################################
def update_ft_table(process_vld):
    	for i in range(0,7):
            ft[i].interval = (my_process_id + 2**i) / 256
            j = ft[i].interval
            while (process_vld[j] != 1) :
              j += 1
              ft[i].sucess = j
        return ft      

def ring_between(start, end, check) :
    if end > start :
        return (check >= end and check <= start)
    else :
        retrun (check >= start or check <= end)

def find_predecessor(process_vld, id) :
  i = id-1
  while (not process_vld[i]) :
      if i > 0 :
          i -= 1
      else:
          i = 255

  


   ##################################################################
   # Functions that handle messages received by the process
   ##################################################################
   
def setup_message_handler():
	'''Sets up a dictionary to execute received messages'''
	global message_handler
	message_handler['add_process'] = msg_process_add
	#message_handler['deete_process'] = msg_process_delete
	#message_handler['show'] = msg_show_key
	#message_handler['find'] = msg_find_ley
	message_handler['add_key'] = msg_add_key

def handle_message(message, ft_table, key_vld, process_vld):
        print 'message = ', message
	command = message.split()
	'''Responds to the given message'''
        print 'process get command in message', command[0]
	global message_handler

	# Check whether the message is supported
	if command[0] not in message_handler:
		print 'Unrecognized mesaage command ', command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given message
	message_handler[command[0]](command, ft_table, key_vld, process_vld)
	return

   ##################################################################
   # Functions for all types of messages received from the socket
   ##################################################################
       
def msg_process_add(message, ft_table, process_database):

    for i in range(2,len(message)) :
         new_process = int(message[i])
         process_vld[new_process] = 1
         ft_table_update(process_vld)
         print "SETUP Send_channel to the new process"
         send_channel[0] = channels.Send_channel(0, base_port + 1 + new_process)
        

         successor = find_succ(process_vld, new_process)
         if my_process_num == sucessor :

             # send key to new process
             for i in range(sucessor+1, new_prcess) :
                 add_key_message = 'add_key'
                 if key[i] == 1 :
                     add_key_message == add_key_message + ' ' + str(i) 
                     send_channel[new_prcoess].send_message(add_key_message)


def msg_add_key(command, ft_table, key_vld, process_vld):
    
    for i in range(1, len(command)):
         key_vld[int(command[i])] = 1;
         print "set key_vld = ", i
         
         
   ##################################################################
   # Process thread
   ##################################################################

def process(process_id, port_id):
'''Thread to represent a process/node.'''
   my_process_id = process_id
   # start a receive chennels
   print 'start process id = ', my_process_id
   rcv_channel = channels.Rcv_channel(process_id, port_id)
   rcv_channel.make_connections()           
   ft_table = []
   for i in range(0,7):
       ft_table.append(0)
       start = (my_process_id + 2 ** (i - 1)) % 255
       ft_table[i] = Ft_table(start, my_process_id)


   process_vld = []
   # key stored in this process
   key_vld = []   

   process_vld = [0]*256
   key_vld = [0]*256       

   process_vld[my_process_id] = 1

   setup_message_handler()

   # done initialization - wait for command
    
   # Check for received messages
   print "process ", my_process_id, " wait for command"
   msg = rcv_channel.get_message()
   if msg is not None:
       print "process ", my_process_id, " get message ", msg
       handle_message(msg, ft_table, key_vld, process_vld)







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

        if len(sys.argv) != 2:
              print 'Usage: python server.py port'
              sys.exit()

	# Read server name and config from CL arguments and connect to other servers
	base_port = int(sys.argv[1])
        send_channel = [] 
        process_vld = []
         
        # initial the array to all 0 
        for i in range(256):
            process_vld.append(0)
            send_channel.append(0)            

        # start a receive socket - all prcoess send in to coordinator thru this socket
        print "setup rcv channel for coordinator"
        rcv_channel = channels.Rcv_channel(0, base_port)
        rcv_channel.make_connections()        

        # start the process thread with id = 0 and rcv_channel = port + 1 
         
        process_start(0, base_port + 1, send_channel)
    	print "SETUP Send_channel to process 0"

	setup_input_handler()

        # all key store in process 0
        message = 'add_key'
        #for i in range(256):
        for i in range(16):	
                message = message + ' ' + str(i)

        print "coordinator send message to process 0 - ", message
        send_channel[0].send_message(message)                

        # finished intilization - wait for input command



        while 1:
      	    rd, wr, err = select.select([sys.stdin], [], [], 0)
            for io in rd:
	 	handle_input(io.readline(), process_vld, send_channel)










