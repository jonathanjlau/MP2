# MP1 server

import socket, select, string, sys
import Queue 
import threading
import datetime
import time
import random

# Global variables
server_id = -1

# Class to store message attributes 
class Command:
    def __init__(self, message, delay):
        self.message = message
        self.time = time.time()
        self.delay = delay 

 

# Simple prompt to remind the client of their id
def prompt() :
    global server_id
    sys.stdout.write('< Server ' + server_id + ' / ' + id_to_alpha[server_id] + ' > ')
    sys.stdout.flush()
    
    
    
    
# A handler for the different options that the user can input via the command line or command file 
def command_input_handler(data):
    
    request = data.split()


    # If it is a send request, add it to the delayed message sending queue
    if (request[0].lower() == "send"):

        receiver_id = alpha_to_id[request[-1].lower()]                  
        max_delay_to_destination =   max_delay[receiver_id]
        delay = random.randrange(0, max_delay_to_destination)   
        command = Command(data,delay)
        queue_list[int(receiver_id)].put(command)
        
        # Output the time that you sent the message according to the spec
        extracted_message = " ".join(request[1:-1])
        message_receiver = request[-1].upper()
        print "Sent \"" + extracted_message + "\" to " + message_receiver + " at system time " + str(datetime.datetime.now())

    
    # If it is not a valid request, send it to the coordinator to broadcast whatever the message was        
    else:    
        coordinator.send(data)


        
        
        
# Thread to handle the messages from the coordinator server and user input.    
def input_receiver() :

    global server_id 

    while 1:
         
        # Get the list sockets which are readable
        read_sockets, write_sockets, error_sockets = select.select(socket_list , [], [])
         
        for sock in read_sockets:
            # Message from Coordinator server
            if sock == coordinator:
                data = sock.recv(4096)
                if not data :
                    print '\nDisconnected from coordinator'
                    sys.exit()
                
                # Extract the message from the coordinator    
                request = data.split()
                
                # If the message begins with "server", set your new id
                if request[0].lower() == "server":

                    server_id = request[1]
                    print "I am server " + server_id + " / " + id_to_alpha[server_id].upper()
                    prompt()
                    
                # If the message begins with "message", then it is a direct message from a server. Print out the message with the source and timestamp
                elif request[0].lower() == "message":
                    source_server = request[1]
                    direct_message = " ".join(request[2:])
                    
                    # Output the time and source that you received the message from according to the spec
                    print "Received \"" + direct_message + "\" from " + source_server.upper() + " at system time " + str(datetime.datetime.now()) + ". Max delay is " + str(max_delay[str(server_id)]) + " s"
                    prompt()
                
                #If it is an unrecognized message, print it out.
                else :

                    sys.stdout.write(data)
                    prompt()
 
             
            # User issued a command from the command line
            else :          
                        
                data = sys.stdin.readline()
               
                # Let the handler handle the user input               
                command_input_handler(data)
                  
                prompt()
                
                
                
                
# Thread to manage sending out messages to the other nodes. There will be four message sending queues, one for every node  
def message_sender():
    
    # Create a cache to hold the next messages to be sent out in each respective queue. This is to overcome the fact that Python's queues do not have a peek function.
    cache = ["placeholder", -1, -1, -1, -1]

    # Go through all four message sending queues and send the messages when it is appropriate
    while 1:
        for i in range(1,5):
            # If the cache is not empty, check if its ready to be sent
            if cache[i] != -1:
                command = cache[i]
                # If the delay is up, send the message and empty the respective cache
                if ((time.time() - command.time) >= command.delay):
                    coordinator.send(command.message) 
                    cache[i] = -1
                    

                    
            # If the cache is empty, add the next message to be sent into the cache         
            elif not queue_list[i].empty():
                cache[i] = queue_list[i].get()
                
                
                
                
                
                
                
                
                
                
 
# Main function
if __name__ == "__main__":
    
    # Initialize the delay hashmap and input command storage for the input files 
    max_delay = { '1': 0, '2': 0, '3': 0, '4': 0 }
    input_commands = []

    # Make sure the user fives at least the port and the config file
    if ((len(sys.argv) < 3) | (len(sys.argv) > 4)) :
        print 'Usage : python server.py port config_file [command_file]'
        sys.exit()
        
    # Update the port
    port = int(sys.argv[1])

    # Add the delay information into the delay hashmap from the config_file
    if(len(sys.argv) >= 3) :
        config_file = sys.argv[2]
        with open(config_file, "r") as ins:
            i = 1
            for line in ins:
                max_delay[str(i)] = int(line)  
                i = i+1

    # If a command file was given, add those commands into an array
    if(len(sys.argv) >= 4) :
        command_file = sys.argv[3]       
        with open(command_file, "r") as ins:
            for line in ins:
                input_commands.append((line))   
                

    #for i in xrange(1,5):
    #    print "idx = " + str(i) + " : " + str(max_delay[str(i)])

    # Setup the socket to the coordinator
    coordinator = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    coordinator.settimeout(2)
     
    # Try to connect to the coordinator
    host = ""
    
    try :
        coordinator.connect((host, port))
    except :
        print 'Unable to connect'
        sys.exit()
     
    print 'Connected to Coordinator server.'
    
     
    # Setup Hashmaps from id to respective alphabet letter and vice versa for easier conversion, similarly to the coordinator
    alpha_to_id = { 'a':'1', 'b':'2', 'c':'3', 'd':'4' }
    id_to_alpha = { '1':'a', '2':'b', '3':'c', '4':'d' }
    
    # Make a list for all the threads and sockets
    thread_list = []
    socket_list = [sys.stdin, coordinator]
    
    # Make a list of four queues, each of which will represent the message queue for node 1/a, 2/b, 3/c, and 4/d
    # The placeholder is used here for entry 0 so the code is more elegant and convenient. i.e. The queue for server 1 will be queue_list[1]
    queue_list = ["placeholder"] 
    for i in xrange(4):
        queue_list.append(Queue.Queue())
    
    # Instatiates the std input and coordinator input thread
    input_thread = threading.Thread(target=input_receiver, args=())
    thread_list.append(input_thread)
    
    # Instatiates the queue thread
    queue_thread = threading.Thread(target=message_sender, args=())
    thread_list.append(queue_thread)   
    
    # Start all the threads
    for thread in thread_list:
        thread.start()
    
    # If the user had given us a command file, process all those commands with the handler
    for command in input_commands:
        command_input_handler(command)
     
    while(1):
        pass
    
