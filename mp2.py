#  server
from __future__ import division
import collections
import copy
import socket, select, string, sys
import Queue
import time
import threading

# Global variables
input_handler = {}
message_handler = {}
base_port = 0
f1 = 0
find_op_count = 0
message_count_phase1 = 0

# Id for coordinator, ID = 0 to 255 for process thread 
COORDINATOR = 256
DEBUG = True
DEBUG = False
DEBUG1 = False
import channels

###############################################################
# Functions for the coordinator
###############################################################

# waif for ack for processor before move to next command
def wait_for_ack(rcv_channel):
    #if DEBUG: print 'start waitng for ACK'
    msg = rcv_channel.get_message()
    # it should receive ack only
    if msg != 'ack':
       print 'Error in receivng message - not ACK' 
       sys.exit()
    #if DEBUG: print 'get ACK'

# Sets up a dictionary to execute input commands'''
def setup_input_handler():    
	global input_handler
	input_handler['join'] = input_join_process
	input_handler['leave'] = input_leave_process
	input_handler['find'] = input_find_key
	input_handler['show'] = input_show_key
	

def handle_input(input_string, process_vld, rcv_channel, send_channel):
        global  message_count_phase1
        #if DEBUG: print 'handle_input :', input_string
	'''Executes the command in the given string from stdin'''
	command = input_string.split()

	# Ignore empty commands
	if len(command) == 0:
		return

	if command[0] == 'cnt' :
            message_count = send_channel[0].get_msg_count()
            print ''
            print 'message count = ', message_count

            num_process = 0;
            for i in range (256) :
                if process_vld[i] :
                    num_process = num_process + 1

            if find_op_count == 0 :
                message_count_phase1 = message_count 
                print 'averge join/key_transfer message / p = ', message_count_phase1 / num_process
            else :
                 message_count_phase2 =  message_count - message_count_phase1
                 print message_count_phase2, num_process, find_op_count
                 print 'average find message / k * p = ', message_count_phase2/(find_op_count)




            return

	if command[0] == 'quit' :
            for i in range(256):
                if process_vld[i] :
                    send_channel[i].send_message('quit')
            sys.exit()
	# Check whether command is supported
	if command[0] not in input_handler:
		print 'Unrecognized input command ' + command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given command
	input_handler[command[0]](command, process_vld, rcv_channel, send_channel)
	return
        

# function to handle input commonad from stdin - 'join'
# 1. start new thread, info new process what prcoessID is valid
# 2. info existing process adding the new processID
# 3. update process_vld array
def input_join_process(command, process_vld, rcv_channel, send_channel):
	'''Handles the addition of a new process'''

	# Check that the command has the right number of parameters
	if len(command) != 2:
		print 'Usage: join process_number(0-255)'
		return
         
        new_process = int(command[1])
        #if DEBUG: print "adding new process ", new_process
        # start a new_process
        process_start(new_process, base_port+new_process, send_channel)
        # info the new process which processes are valid      
 	message = 'join_process' + vld_array_string(process_vld)

        # start a new write socket (from coordinator) connect to the receive port of the new process
	#if DEBUG: print 'SETUP send channel from coordinator to ', new_process
        send_channel[new_process] = channels.Send_channel(COORDINATOR, base_port + new_process)
        send_channel[new_process].make_connections()
        send_channel[new_process].send_message(message)
        wait_for_ack(rcv_channel);         

        # let all other existing process know that a new process is added
        #if DEBUG: print "send to existing node - adding new process ", command[1]
        for i in range(256):
           if process_vld[i] :
               send_channel[i].send_message(('join_process ' + command[1]))
               wait_for_ack(rcv_channel);

        process_vld[new_process] = 1

# function to handle input commonad from stdin - 'leave'
# 1. send message to all existing processor;
# 2. update process_vld array
def input_leave_process(command, process_vld, rcv_channel, send_channel):
	'''Handles the removal of a existing process'''

	# Check that the command has the right number of parameters
	if len(command) != 2:
		print 'Usage: leave process_number(0-255)'
		return
         
        del_process = int(command[1])
        #if DEBUG: print "deleting process ", del_process

	message = 'leave_process ' + command[1]
        # let all existing valid process know that deleting this process ID 
        for i in range(0, 256):
          if process_vld[i] :
              send_channel[i].send_message(message)
              wait_for_ack(rcv_channel);         
        process_vld[del_process] = 0

# function to handle input commonad from stdin - 'show'
# 1. send 'show" message to destination process(es)
def input_show_key(command, process_vld, rcv_channel, send_channel):

	if len(command) != 2:
		print 'Usage: show process_id(0-255, all)'
		return
        if command[1] == 'all' :
            for i in range (256) :  
                if process_vld[i] :
                    send_channel[i].send_message('show')
                    wait_for_ack(rcv_channel);         
        else:
            process = int(command[1])   
            if process_vld[process] :
                send_channel[process].send_message('show')
                wait_for_ack(rcv_channel);         

# function to handle input commonad from stdin - 'find P K'
# 1. send 'find K " message to  process P  
def input_find_key(command, process_vld, rcv_channel, send_channel):
        global find_op_count

	'''Handles the addition of a new process'''

	if len(command) != 3:
		print 'Usage: find processID key'
		return
	message = 'find ' + command[2]
        process_id = int(command[1])
        if process_vld[process_id] :
            send_channel[process_id].send_message(message)
            wait_for_ack(rcv_channel)         
	find_op_count += 1

# function to start a new process thread setup a send_channel from coordinator to new process
# 1. send 'find K " message to  process P  
def process_start(process_id, port, send_channel):
    '''Function to start the process threads'''
    process_thread = threading.Thread(target=process, args=(process_id, port)) 
    process_thread.start()
    # setup send channel from coordinator to the new process
    send_channel[process_id] = channels.Send_channel(COORDINATOR, base_port + process_id)
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

# update ft_table based on array process_vld
def ft_table_update(process_vld, my_process_id, ft_table):
        #if DEBUG: print 'ft_table_udpate pvld = 0, 11 = ', process_vld[0], process_vld[11]
    	for i in range (8):
            j = ft_table[i].interval
            while (process_vld[j] != 1) :
              j = (j + 1) % 256
            ft_table[i].succ = j

        #if DEBUG: print 'ft_table for process_id = ', my_process_id
    	#for i in range(8):
        #   if DEBUG: print '[', i, ']', '   interval = ', ft_table[i].interval, '    succ = ',  ft_table[i].succ

# return True if the check is between start and end in the ring
def node_ring_between(start, end, check) :

    if end == start :
        rtn = True   
    if end > start :
        rtn = (check > start and check <= end)
    else :
        rtn = (check > start or check <= end)
    #print 'ring_between_test ', start, end, check, rtn
    return rtn

# return True if the check is between start and end in the ring
def key_ring_between(start, end, check) :
    if end > start :
        rtn = (check >= start and check < end)
    else :
        rtn = (check >= start or check < end)
    #print 'ring_between_test ', start, end, check, rtn
    return rtn

# find the valid process that is the predecessor of my_od
def find_predecessor(process_vld, id) :
  if id == 0 :
      i = 255
  else:
      i = id - 1

  while (not process_vld[i]) :
      if i == 0 :
          i = 255
      else:
          i -= 1
  return i

# turn the vld array (key_vld or process_vld) to string
#   for building the send message  
def vld_array_string(vld_array):
    message = ''
    for i in range(0,256): 
          if vld_array[i] :
             message = message + ' ' + str(i)
    return message

   ##################################################################
   # Functions that handle messages received by the process
   ##################################################################
   
def setup_message_handler():
	'''Sets up a dictionary to execute received messages'''
	global message_handler
	message_handler['join_process'] = msg_join_process
	message_handler['leave_process'] = msg_leave_process
	message_handler['show'] = msg_show_key
	message_handler['find'] = msg_find_key
	message_handler['add_key'] = msg_add_key

def handle_message(message, my_id, ft_table, key_vld, process_vld, rcv_channel, send_channel):
	global message_handler
        #if DEBUG: print 'handle_message = ', message
	command = message.split()
        if command[0] =='quit':
            sys.exit()

	# Check whether the message is supported
	if command[0] not in message_handler:
		print 'Unrecognized mesaage command ', command[0]
		return

	# Use a dictionary as a switch case to call the function corresponding to the given message
	message_handler[command[0]](command, my_id, ft_table, key_vld, process_vld, rcv_channel, send_channel)
	return

   ##################################################################
   # Functions for all types of messages received from the socket
   ##################################################################

# coordination broadcast when a new process is join.
# 1. update ft_table, process_vld
# 2. setup send channel to new process
# 3. transfer key storage to new process if needed       
def msg_join_process(command, my_id, ft_table, key_vld, process_vld, rcv_channel, send_channel):
    if DEBUG: print 'execute msg_join_process on process_id = ', my_id
    #print 'len(command) = ', len(command)
    predecessor = find_predecessor(process_vld, my_id) 
    #if DEBUG: print 'predecessor of ', my_id, ' = ', predecessor
    for i in range(1,len(command)) :
         new_process = int(command[i])
         process_vld[new_process] = 1
         # setup new send_channel from my_process to new_process
         send_channel[new_process] = channels.Send_channel(my_id, base_port + new_process)
    ft_table_update(process_vld, my_id, ft_table)

    # send add_key message to new_process - transfer the key storage to new process 
    # for all key beteen my predecessor to new_process
    message = ''
    # if the new process added is between my_id and my predecessor OR only my node is valid
    if node_ring_between(predecessor, my_id, new_process) :
        for i in range(256) :
            if key_vld[i] == 1 :
                if node_ring_between(predecessor, new_process, i) :
                    key_vld[i] = 0
                    message = message + ' ' + str(i)
    # if there is any key need to transfer, send to the new_process
    if len(message) > 0 :
        message = 'add_key' + message
        new_process = int(command[1])
        send_channel[new_process].send_message(message)
    else:
        send_channel[COORDINATOR].send_message('ack')

# coordination broadcast when a process is deleted.
# 1. update ft_table, process_vld
# 2. if deleting my process, transfer key storage to new process if needed       
def msg_leave_process(command, my_id, ft_table, key_vld, process_vld, rcv_channel, send_channel):

         del_process = int(command[1])
         if del_process == my_id :
             # send all key to the successor if any
             key_vld_string = vld_array_string(key_vld)
             if key_vld_string != '' :
                 message = 'add_key' + key_vld_string
                 send_channel[ft_table[0].succ].send_message(message)
             send_channel[COORDINATOR].send_message('ack')          
             sys.exit()
         else:
             process_vld[del_process] = 0
             ft_table_update(process_vld, my_id, ft_table)
             send_channel[COORDINATOR].send_message('ack')          

# from coordinator(only at initialization)/other process - transfer key storage
# 1. update key_valid array
def msg_add_key(command, my_id, ft_table, key_vld, process_vld, rcv_channel, send_channel):
    
    for i in range(1, len(command)):
         key_vld[int(command[i])] = 1;
         #print "set key_vld = ", i
    send_channel[COORDINATOR].send_message('ack')         

# from coordinator/other process 
# 1. return key if stored locally, else send to next process according to ft_table
def msg_find_key(command, my_id, ft_table, key_vld, process_vld, rcv_channel, send_channel):
    
    key_id = int(command[1])
    if key_vld[key_id] :
        if DEBUG: print ' '
        if DEBUG: print 'process_ID = ', my_id, ' has the key_id = ', key_id
        if DEBUG: print ' '
        send_channel[COORDINATOR].send_message('ack')         
    else :
      i = 0
      # while key_id >= ft_table[i].succ and key_id < ft_table[(i+1) % 8].succ : 
     
      while not key_ring_between(ft_table[i].interval, ft_table[(i+1) % 8].interval, key_id) : 
          i = i + 1 
      #print 'find: ft_table lookup idx = ', i, ft_table[i].succ
      send_channel[ft_table[i].succ].send_message('find ' + command[1])         

# display process ID and key stored
def msg_show_key(command, my_id, ft_table, key_vld, process_vld, rcv_channel, send_channel):
       global f1
       if DEBUG1: print ''
       if DEBUG1: print 'process_id = ', my_id, 'key_vld = ', vld_array_string(key_vld)
       if DEBUG1: print 'process_id = ', my_id, 'procee_vld = ', vld_array_string(process_vld)
       if DEBUG1: print 'ft table for process_id = ', my_id
       for i in range (8):
            if DEBUG1: print '[', i, ']', '   interval = ', ft_table[i].interval, '    succ = ',  ft_table[i].succ
       if DEBUG1: print ''

       f1.write(str(my_id))
       f1.write(vld_array_string(key_vld))
       f1.write('\n')

       send_channel[COORDINATOR].send_message('ack')         
         
   ##################################################################
   # Process thread
   ##################################################################

def process(process_id, port_id):
#'''Thread to represent a process/node.'''
   my_process_id = process_id
   # start a receive chennels
   print 'start process thread: id = ', my_process_id
   rcv_channel = channels.Rcv_channel(process_id, port_id)
   rcv_channel.make_connections()  

   # initailize ft_table, process_vld, key_vld         
   ft_table = []
   for i in range (8):
       ft_table.append(0)
       start = (my_process_id + 2 ** i) % 256
       ft_table[i] = Ft_table(start, my_process_id)

   #if DEBUG: print 'INIT ft_table for process_id = ', my_process_id
   #for i in range (8):
   #    if DEBUG: print '[', i, ']', '   interval = ', ft_table[i].interval, '    succ = ',  ft_table[i].succ
        

   process_vld = []
   # key stored in this process
   key_vld = []   

   process_vld = [0]*256
   key_vld = [0]*256       

   process_vld[my_process_id] = 1

   send_channel = [0]*257
   # sentup send_channel to coordinator
   send_channel[COORDINATOR] = channels.Send_channel(my_process_id, base_port + COORDINATOR)
   send_channel[COORDINATOR].make_connections()
   
   setup_message_handler()

   # done initialization - wait for command
    
   # Check for received messages
   #if DEBUG: print "process ", my_process_id, " wait for command"

   while 1:
       msg = rcv_channel.get_message()
       if msg is not None:
	    #if DEBUG: print "process ", my_process_id, " get message ", msg
	    handle_message(msg, my_process_id, ft_table, key_vld, process_vld, rcv_channel, send_channel)








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
        base_port = 0
        out_filename = "mp2.log"
        i = 1
        while i < len(sys.argv) :
           if sys.argv[i] == '-g' :
                i += 1
                out_filename = sys.argv[i]
           elif sys.argv[i] == '-p' :
                 i += 1
                 base_port = int(sys.argv[i])
           else :
              print 'Usage: python server.py -g <filename> -p <port>'
              sys.exit()
           i += 1
       
        f1 = open(out_filename, 'w+')
	# Read server name and config from CL arguments and connect to other servers

        send_channel = [] 
        process_vld = []
       
        # initial the array to all 0 
        for i in range(256):
            process_vld.append(0)
            send_channel.append(0)            

        # start a receive socket - all prcoess send in to coordinator thru this socket
        if DEBUG: print "setup rcv channel for coordinator"
        rcv_channel = channels.Rcv_channel(COORDINATOR, base_port+COORDINATOR)
        rcv_channel.queue_init()        
        rcv_channel.make_connections()        

        # start the process thread with id = 0 and rcv_channel = port + 1 
         
        process_start(0, base_port, send_channel)
    	if DEBUG: print "coordinator SETUP Send_channel to process 0"

	setup_input_handler()

        # all key initially stored in process 0
        message = 'add_key'
        for i in range(256):
        #for i in range(4):	
                message = message + ' ' + str(i)

        #if DEBUG: print "coordinator send message to process 0 - ", message
        send_channel[0].send_message(message)                
        wait_for_ack(rcv_channel);
        process_vld[0] = 1

        # finished intilization - wait for input command
        while 1:
            rd, wr, err = select.select([sys.stdin], [], [], 0)
            for io in rd:
	 	handle_input(io.readline(), process_vld, rcv_channel, send_channel)










