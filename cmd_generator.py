import string, sys
import random

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
def repeat(new_process, process_vld) :
     rtn = False
     for i in range (len(process_vld)) :
         if new_process == process_vld[i] :
            rtn = True
     return rtn

if __name__ == "__main__":

   rand_seed = int(sys.argv[1])
   random.seed(rand_seed)
   out_filename = 'command'
   process_vld = []
   p = [4, 8, 10, 20, 30] 

   # create 5 command file - command_p4, command_p8, ... command_p30
   for i in range (5) :
       out_filename = 'command_p' + str(p[i])
       f1 = open(out_filename, 'w+')

       for j in range (p[i]) : 
       	   new_process = random.randint(0,255)
       	   while repeat(new_process, process_vld) :
           	 new_process = random.randint(0,255)
	   process_vld.append(new_process)
       	   f1.write('join ')
       	   f1.write(str(new_process))
       	   f1.write('\n')
       f1.write('cnt\n')
      
       # find key - F = 64 -400
       for k in range (random.randint(64,400)) :
           f1.write('find ')
           f1.write(str(process_vld[random.randint(0, len(process_vld))-1]))
           f1.write(' ')
           f1.write(str(random.randint(0, 255)))
           f1.write('\n')
       f1.write('cnt\n')
       f1.write('show all\n')
       f1.write('quit\n')
