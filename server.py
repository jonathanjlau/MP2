# telnet program example
import socket, select, string, sys
import Queue 
import threading
import time
import random


class Command:
    def __init__(self, message, delay):
        self.message = message
        self.time = time.time()
        self.delay = delay 

 


def prompt() :
    sys.stdout.write('<You> ')
    sys.stdout.flush()
    
def input_receiver() :

    while 1:
         
        # Get the list sockets which are readable
        read_sockets, write_sockets, error_sockets = select.select(socket_list , [], [])
         
        for sock in read_sockets:
            # Message from Coordinator server
            if sock == server:
                data = sock.recv(4096)
                if not data :
                    print '\nDisconnected from coordinator'
                    sys.exit()
                    
                request = data.split()
                if request[0].lower() == "server":

                    server_id = request[1]
                    print "I am server " + server_id + " / " + id_to_alpha[server_id].upper()
                    prompt()
                elif request[0].lower() == "caps":
                    print "CAPS"
                    prompt()
                else :

                    #print data
                    sys.stdout.write(data)
                    prompt()
             
            # User issued a command
            else :          
                        
                data = sys.stdin.readline()
                request = data.split()
                
                # If it is a send request, add it to the delayed message sending queue
                if request[0].lower() == "send":
                    receiver_id = alpha_to_id[request[-1].lower()]
                    delay = 5
                    command = Command(data,delay)
                    queue_list[int(receiver_id)].put(command)
                    
                elif request[0].lower() == "quit":
                    sys.exit()
                    
                else:    
                    server.send(data)

                prompt()
                

def message_sender():

    cache = ["placeholder", -1, -1, -1, -1]

    while 1:
        for i in range(1,5):
            if cache[i] != -1:
                command = cache[i]
                if ((time.time() - command.time) >= command.delay):
                    server.send(command.message) 
                    cache[i] = -1
                    print "MESSAGE SENT: " + command.message
                    prompt()
                    
            elif not queue_list[i].empty():
                cache[i] = queue_list[i].get()
                
                
                
                
                
                
                
                
                
                
 
#main function
if __name__ == "__main__":
     
    if(len(sys.argv) < 3) :
        print 'Usage : python server.py hostname port'
        sys.exit()
     
    host = sys.argv[1]
    port = int(sys.argv[2])
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.settimeout(2)
     
    # connect to remote host
    try :
        server.connect((host, port))
    except :
        print 'Unable to connect'
        sys.exit()
     
    print 'Connected to Coordinator server.'
    
     
    # Hashmaps from id to alphabet and vice versa
    alpha_to_id = { 'a':'1', 'b':'2', 'c':'3', 'd':'4' }
    id_to_alpha = { '1':'a', '2':'b', '3':'c', '4':'d' }
    
    # Init stuff
    server_id = -1
    thread_list = []
    socket_list = [sys.stdin, server]
    
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
     
    while(1):
        pass
    
