# Tcp Chat server
 
import socket, select, sys
 
#Function to broadcast chat messages to all connected clients
def broadcast_data (message):
    #Do not send the message to master socket //and the client who has send us the message
    for socket in CONNECTION_LIST:
        if socket != server_socket:
            try :
                socket.send(message)
            except :
                # broken socket connection may be, chat client pressed ctrl+c for example
                socket.close()
                CONNECTION_LIST.remove(socket)
                
def find_empty_id():
    for i in range(1,5):
        key = str(i)
        if key not in id_to_socket:
            return key
        if id_to_socket[key] == -1:
            return key
    return "-1"
    
    
    
    
    
    

 
if __name__ == "__main__":
     
    # List to keep track of socket descriptors
    CONNECTION_LIST = []
    RECV_BUFFER = 4096 # Advisable to keep it as an exponent of 2
    PORT = 5000
    
    
    if(len(sys.argv) < 2) :
        print 'Usage : python coordinator.py port'
        sys.exit()
    
    PORT = int(sys.argv[1])
     
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # this has no effect, why ?
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", PORT))
    server_socket.listen(10)
    
 
    # Add server socket to the list of readable connections
    CONNECTION_LIST.append(server_socket)
    
    
    # Hashmaps from id to alphabet or socket and vice versa
    socket_to_id = { }
    id_to_socket = {}
    alpha_to_id = { 'a':'1', 'b':'2', 'c':'3', 'd':'4' }
    id_to_alpha = { '1':'a', '2':'b', '3':'c', '4':'d' }
 
    print "Coordinator server started on port " + str(PORT)
 
    while 1:
        # Get the list sockets which are ready to be read through select
        read_sockets,write_sockets,error_sockets = select.select(CONNECTION_LIST,[],[])
 
        for sock in read_sockets:
            # New connection
            if sock == server_socket:
                # Handle the case in which there is a new connection recieved through server_socket
                sockfd, addr = server_socket.accept()
                
                
                
                
                #Put the socket data into the two-way hashmap
                empty_id = find_empty_id()
                socket_to_id[sockfd] = empty_id
                id_to_socket[empty_id] = sockfd
                CONNECTION_LIST.append(sockfd)
                
                
                
                
                
                print "Server " + socket_to_id[sockfd] +  " (%s, %s) connected" % addr
                 
                #broadcast_data("[%s:%s] entered room\n" % addr)
                sockfd.sendall("Server " + socket_to_id[sockfd] + "\n")
                
                #Tell the client what id they have
             
            #Some incoming message from a client
            else:
                # Data recieved from client, process it
                try:  
                    data = sock.recv(RECV_BUFFER)
                    
                    print("DATA RECEIVED: " + data) #DEBUGGING
                    
                    request = data.split()
                    
                    # A direct send request from one server to another, forward only the message
                    if request[0].lower() == "send":
                        sender_id = socket_to_id[sock]
                        sender_message = " ".join(request[1:-1])
                        receiver_id = alpha_to_id[request[-1].lower()]
                        receiver_socket = id_to_socket[receiver_id]
                        receiver_socket.sendall(sender_message + "\n")
                        
                    # Otherwise send a broadcast
                    else:
                        broadcast_data("\r" + '<' + str(sock.getpeername()) + '> ' + data)                
                 
                except:
                    broadcast_data("Client (%s, %s) is offline \n" % addr)
                    print "Server (%s, %s) is offline" % addr
                    sock_id = socket_to_id[sock]
                    id_to_socket[sock_id] = -1
                    sock.close()
                    CONNECTION_LIST.remove(sock)
                    continue
     
    server_socket.close()
