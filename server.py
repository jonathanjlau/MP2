# MP1 server

import socket, select, string, sys
import Queue 
import threading
import datetime
import time
import random


# Global variables
server_id = -1
key_value_pairs = {}                
search = {}                 
active_command = None                

'''
Class to store message attributes
'''
class Command:
    def __init__(self, message, delay):
        self.message = message
        self.time = time.time()
        self.delay = delay 



'''                
Simple prompt to remind the server of their id
'''
def prompt() :
    global server_id
    sys.stdout.write('< Server ' + server_id + ' / ' + id_to_alpha[server_id] + ' > ')
    sys.stdout.flush()


'''
Deletes the key-value pair
'''
def delete_key(request):
    global key_value_pairs

    key_to_delete = request[1]
    key_deleted = key_value_pairs.pop(key_to_delete, None)
    
    if key_deleted != None:
        print "\nKey " + key_to_delete + " deleted"
    else: 
        print "\nKey-value pair not found."
    prompt()
    
'''
Gets the key-value pair     
'''      
def get_key(request, consistency_model = -1):
    global key_value_pairs

    key_to_get = request[1]
    consistency_model = request[2]
    value_gotten = key_value_pairs.get(key_to_get, None)
    if (value_gotten == None):
        print "Key-value pair not found."
        
    else:
        if (consistency_model == "1" or consistency_model == "2"):
            print "get " + key_to_get + " = " + value_gotten  

'''
Inserts the key-value pair 
'''
def insert_key(request):
    global key_value_pairs
        
    key_to_insert = request[1]
    value_to_insert = request[2]
    consistency_model = request[3]
    key_value_pairs[key_to_insert] = value_to_insert
    
    print "\nInserted key " + key_to_insert 
    prompt() 
    
    
'''
Updates the key-value pair     
'''    
def update_key(request, local_update = False):
    global key_value_pairs
    
    key_to_update = request[1]
    value_to_update = request[2]
    consistency_model = request[3]
    old_value = key_value_pairs[key_to_update] 
    key_value_pairs[key_to_update] = value_to_update  
            
    if (local_update == True):   
        print "\nKey " + key_to_update + " changed from " + old_value + " to " + value_to_update
        
    else:
        print "\nKey " + key_to_update + " updated to " + value_to_update
    prompt()   
            
'''    
Adds a message with either a preset delay or randomized delay based on the given receiver id to the appropriate queue
'''
def add_message_to_queue(message, receiver_id, delay = -1):       
    max_delay_to_destination = max_delay[receiver_id]
    if (delay == -1):       
        delay = random.uniform(0, max_delay_to_destination)   
    command = Command(message, delay)
    queue_list[int(receiver_id)].put(command) 
            

'''
The handler for the different messages received from the coordinator server. 

Possible coordinator messages: 
    server id
    message source_server actual_message
    delete key source_server
    get key model source_server
    insert key value model source_server
    update key value model source_server
    search key source_server
    search-reply key key_available original_source_server original_receiver_server
'''
def coordinator_message_handler(data):
    global server_id
    global search
    global active_command
    
    
    # Extract the message from the coordinator    
    request = data.split()
                
    # If the message begins with "server", set your new id
    if request[0].lower() == "server":

        server_id = request[1]
        print "I am server " + server_id + " / " + id_to_alpha[server_id].upper()
        prompt()
                    
    # If the message begins with "message", then it is a direct message from another server. Print out the message with the source and timestamp
    elif request[0].lower() == "message":
        source_server = request[1]
        direct_message = " ".join(request[2:])
                    
            # Output the time and source that you received the message from according to the spec
        print "\nReceived \"" + direct_message + "\" from " + source_server.upper() + " at system time " + str(datetime.datetime.now().time()) + ". Max delay is " + str(max_delay[str(server_id)]) + " s"
        prompt()
                    
    # If it is a delete message, delete the information associated with that key
    elif (request[0].lower() == "delete"):               
        delete_key(request)  
        source_server = request[3]       
        if source_server == server_id:
            active_command = None
        add_message_to_queue("ACK " + server_id, server_id)
    # If it is a get message, get the information associated with that key according to who's request it was
    elif (request[0].lower() == "get"):
        source_server = request[3]
        consistency_model = request[2]
        if consistency_model == "1":
            if source_server == server_id:
                print ""
                get_key(request, consistency_model)
                active_command = None  
                prompt()    
                
            # Send an ACK back to the coordinator server with a delay based on your server_id
            add_message_to_queue("ACK " + server_id, server_id)
            
        active_command = None
            

    # If it is an insert message, insert the key-value pair into the dictionary according to who's request it was
    elif (request[0].lower() == "insert"):
        source_server = request[4]
        consistency_model = request[3]
        
        if consistency_model == "1" or consistency_model == "2":
            insert_key(request)
            if source_server == server_id:
                active_command = None  
            # Send an ACK back to the coordinator server with a delay based on your server_id
            add_message_to_queue("ACK " + server_id, server_id)
        

        
    # If it is an update message, update the key-value pair into the dictionary according to who's request it was
    elif (request[0].lower() == "update"):
        source_server = request[4]
        consistency_model = request[3]

        if consistency_model == "1" or consistency_model == "2":
            if source_server == server_id:
                update_key(request, True)
                active_command = None
            else:
                update_key(request, False)
        
            # Send an ACK back to the coordinator server with a delay based on your server_id
            add_message_to_queue("ACK " + server_id, server_id)
        
        
    # If it is a search message, try to find the key and report the result back to the source server
    elif (request[0].lower() == "search"):
        source_server = request[2]
        search_key = request[1]
        key_found = key_value_pairs.get(search_key, None)

        if key_found != None:
            key_available = "true"
        else:
            key_available = "false"
        
        # Send the reply back to the coordinator server to forward to the source server
        add_message_to_queue("search-reply " + search_key + " " + key_available + " " + source_server + " " + server_id, source_server)        

    # If it is a search-reply message, get the response from the other servers for whether or not the have the key.
    # search-reply key key_available original_source_server original_receiver_server
    elif (request[0].lower() == "search-reply"):
        search_key = request[1]
        key_available = request[2]
        originating_server = request[3]
        receiver_server = request[4]
        
        if key_available == "true":
            search[search_key][int(receiver_server)] = True
        else: 
            search[search_key][int(receiver_server)] = False
 
        
        if len(search[search_key]) == 4 and originating_server == server_id:
            search_response = "Servers with key " + search_key + ": "
            for i in range(1,5):
                if search[search_key][i] == True:
                    search_response = search_response + str(i) + " " 
            
            print search_response
            prompt()
            
            active_command = None
           
    #If it is an unrecognized message, print it out.
    else :

        sys.stdout.write(data)
        prompt()
        



'''    
The handler for the different options that the user can input via the command line or command file 

Possible user commands: 
    send message receiver(a, b, c, or d)
    delete key 
    get key model 
    insert key value model 
    update key value model
    show-all
    search key
    delay time
    load (loads all the respective commands for this server from the command file)
    
    
'''
def command_input_handler(data):
    global key_value_pairs
    global search
    global active_command

    if (len(data) <= 1) :
        return
    
    request = data.split()


    # If it is a send request, add it to the delayed message sending queue
    if (request[0].lower() == "send"):
        
        # Get the id of the receiver and add the message into the message queue. This function will automatically add a random delay
        receiver_id = alpha_to_id[request[-1].lower()]    
        add_message_to_queue(data, receiver_id)              

        
        # Output the time that you "sent" the message according to the spec
        extracted_message = " ".join(request[1:-1])
        message_receiver = request[-1].upper()
        # Yes, this should be the right place to put this.
        print "Sent \"" + extracted_message + "\" to " + message_receiver + " at system time " + str(datetime.datetime.now().time())



    # If it is a delete request, broadcast that request so that all the nodes delete the give key
    elif (request[0].lower() == "delete"):
        if (len(request) < 2):
            print "DELETE usage: delete key"
        else:
            add_message_to_queue(data.rstrip('\n') + " " + server_id, server_id)
            active_command = "delete"
                    
    # If it is a get request, either get the local information immediately or broadcast a read request, depending on the consistency model extracted from the request
    elif (request[0].lower() == "get"):
        if (len(request) < 3):
            print "GET usage: get key model"
        else:
            consistency_model = request[2]  
            if(consistency_model == '1'): 
                add_message_to_queue(data.rstrip('\n') + " " + server_id, server_id)
                active_command = "get"
                
            if(consistency_model == '2'): 
                get_key(request, consistency_model)


    # If it is an insert request, insert the key-value pair into the dictionary or broadcast the request, depending on the consistency model extracted from the request
    elif (request[0].lower() == "insert"):
        if (len(request) < 4):
            print "INSERT usage: insert key value model"
        else:
            consistency_model = request[3]
            if(consistency_model == '1' or consistency_model == '2'):
                add_message_to_queue(data.rstrip('\n') + " " + server_id, server_id)
            
            active_command = "insert"
                
    # If it is an update request, update the key-value pair into the dictionary or broadcast the request, depending on the consistency model extracted from the request
    elif (request[0].lower() == "update"):
        if (len(request) < 4):
            print "UPDATE usage: update key value model"   
        else:
            consistency_model = request[3]
            if(consistency_model == '1' or consistency_model == '2'):
                add_message_to_queue(data.rstrip('\n') + " " + server_id, server_id)       
            
            active_command = "update"
        
    # If it is a show-all request, print out all the entries in the dictionary
    elif (request[0].lower() == "show-all"):
        for key,val in key_value_pairs.items():
            print key + " -> " + val

    # If it is a search request, pull the information about the other servers
    elif (request[0].lower() == "search"):
        key = request[1]
        
        search[key] = {}
        
        # Send a broadcast requesting information about the key's availability from the other servers
        add_message_to_queue("search " + key + " " + server_id, server_id)  
         
        active_command = "search"
            
    # If it is a delay request, delay the invocation by the specified time
    elif (request[0].lower() == "delay"):
        delay_time = float(request[1])
        while active_command != None:
            pass
        time.sleep(delay_time)  
                 
    # If it is a load request, execute command from config_file       
    elif (request[0].lower() == "load"):
        my_command_format = id_to_alpha[server_id] + ':'
        for command in input_commands:
            command_parse = command.split() 
            if command_parse[0].lower() == my_command_format :         
                command_input_handler(" ".join(command_parse[1:])) 
      
    # If it is not a valid request, send it to the coordinator to broadcast whatever the message was        
    else:    
        coordinator.send(data)

    # If an there is an active command, wait until that command has completed. Then process the next command from the input. For Linearizability and Sequential Consistency.
    while active_command != None:
        pass
  







        
        
       
                
                
                
                
                
'''        
Thread to handle the messages from the coordinator server. 

Description:
It will obtain the list of ready sockets and delegate work to the respective coordinator message handler.  
For Step 1, If a server receives a message, it will display the source, message, and the time.
For Step 2-1 and 2-2, it will work just as the slides lecture stated. For Linearizability, after a server has sent one of the four major commands (get, insert, etc.) to the coordinator (with a randomized delay based on its own max delay) for it to be totally broadcasted, all the servers will then reply with an "ACK" and execute the original command if it is a write operation and ignore it if it is someone else's read operation. For Sequential Consistency, it is exactly the same as Linearizability except that "gets" are not broadcasted but immediately executed. 
'''                
def coordinator_message_receiver() :

    global server_id 
    global key_value_pairs

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
                # Let the coordinator input handler handle the coordinator's messages    
                coordinator_message_handler(data)

             
       
                
'''        
Thread to handle the messages from user input. 

Description:
It will obtain the list of ready sockets and delegate work to the respective user input handlers. 
For Step 1, if a server wants to send a message, it will print out the current time and then put that message in the queue with a randomized delay based on the max delay from the configuration file.  
For Step 2-1 and 2-2, it will work just as the slides lecture stated. For Linearizability, if a server wants to execute of the four major commands, (get, insert, etc.) it will send that command to the coordinator (with a randomized delay based on its own max delay) for it to be totally broadcasted. For Sequential Consistency, it is exactly the same as Linearizability except that "gets" are not broadcasted but immediately executed. 
'''
def user_input_receiver() :

    global server_id 
    global key_value_pairs

    while 1:
         
        # Get the list sockets which are readable
        read_sockets, write_sockets, error_sockets = select.select(socket_list , [], [])
         
        for sock in read_sockets:
        
            # User issued a command from the command line
            if sock != coordinator:       
                        
                data = sys.stdin.readline()
               
                # Let the user command handler handle the user input               
                command_input_handler(data)
                  
                prompt()                

                
                
                
'''                
Thread to manage the delayed message sending queues to the other nodes. 

Description:
There will be four message sending queues, one for every node.
It loops through four queues and waits until the first of each queue is ready to send.
The messages in the back of the queue are ignored until the front is sent, even if they have an earlier timestamp. This is to ensure the FIFO property.
'''
def message_sender():
    
    # Create a cache to hold the next messages to be sent out in each respective queue. This is to overcome the fact that Python's queues do not have a peek function.
    cache = ["placeholder", -1, -1, -1, -1]

    # Go through all four message sending queues and send the messages when it is appropriate
    while 1:
        for i in range(1,5):
            # If the cache is not empty, check if its ready to be sent
            if cache[i] != -1:
                command = cache[i]
                # If the delay is up, send the message to the coordinator to handle it and empty the respective cache
                if ((time.time() - command.time) >= command.delay):
                    coordinator.send(command.message) 
                    cache[i] = -1
                    

                    
            # If the cache is empty, add the next message to be sent into the cache         
            elif not queue_list[i].empty():
                cache[i] = queue_list[i].get()
                
                
                
                
                
                
                
                
                
               
    

''' 
Main function for the individual servers.

Description:
Each server connects to the coordinator and the coordinator will give them their unique ids.
It will then spawn three threads: one to handle the user input, one to handle the messages from the coordinator, and another to handle delayed message sending.
The thread handling the user input will have a handler for every option listed in the spec as well as a function "load" that will load the commands from the command file.
The thread handling the coordinator's messages will also have a handler for every message that is sent.
The thread handling the delayed message sending will monitor four outgoing queues, one for each server.
More detail can be found in each thread's description.
'''
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
           
            for line in ins:
                if len(line) > 1 :
                    line_parse = line.split()
                    if line_parse[0].lower() == 'a:' :
                        max_delay['1'] = float(line_parse[1])  
                    elif line_parse[0].lower() == 'b:' :
                        max_delay['2'] = float(line_parse[1])  
                    elif line_parse[0].lower() == 'c:' :
                        max_delay['3'] = float(line_parse[1])  
                    elif line_parse[0].lower() == 'd:' :
                        max_delay['4'] = float(line_parse[1])  
                    else :
                        print 'Unknow format in config file' + line  

    # If a command file was given, add those commands into an array
    if(len(sys.argv) >= 4) :
        command_file = sys.argv[3]       
        with open(command_file, "r") as ins:
            for line in ins:
                input_commands.append((line))   
                

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
    user_input_thread = threading.Thread(target=user_input_receiver, args=())
    thread_list.append(user_input_thread)
    
    # Instatiates the std input and coordinator input thread
    coordinator_message_thread = threading.Thread(target=coordinator_message_receiver, args=())
    thread_list.append(coordinator_message_thread)
    
    # Instatiates the queue thread
    queue_thread = threading.Thread(target=message_sender, args=())
    thread_list.append(queue_thread)   
    
    # Start all the threads
    for thread in thread_list:
        thread.start()
    
     
    while 1:
        pass
