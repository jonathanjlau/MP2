import string, sys
import random

'''
Main function for the automatic commnand file generator for MP2.

Description:

Generates 5 command files for P = 4, 8, 10, 20, 30. Each command files has 'P' join commands and then 64 find commands.
It randomly picks processes to join and random p and k for the find operations

To run: python cmd_generatior.py [random_seed]
'''

def repeat(new_process, process_vld) :
     '''Checks if there are duplicates for joins. Returns True if the process is already picked'''
     rtn = False
     for i in range (len(process_vld)) :
         if new_process == process_vld[i] :
            rtn = True
     return rtn

if __name__ == "__main__":
   if len(sys.argv) != 2:
  	print 'Usage: python cmd_generator.py random_seed'
	sys.exit()
   rand_seed = int(sys.argv[1])
   random.seed(rand_seed)
   out_filename = 'command'
   process_vld = []
   p = [4, 8, 10, 20, 30] 

   # create 5 command files - command_p4, command_p8, ... command_p30
   for i in range (5) :
       out_filename = 'command_p' + str(p[i])
       print 'create command file -', out_filename
       f1 = open(out_filename, 'w+')
 
       # add join commands
       for j in range (p[i]) : 
       	   new_process = random.randint(1,255)
       	   while repeat(new_process, process_vld) :
           	 new_process = random.randint(1,255)
	   process_vld.append(new_process)
       	   f1.write('join ')
       	   f1.write(str(new_process))
       	   f1.write('\n')
       f1.write('cnt\n')
      

       # add 64 find commands
       for k in range (64) :
           f1.write('find ')
           f1.write(str(process_vld[random.randint(0, len(process_vld))-1]))
           f1.write(' ')
           f1.write(str(random.randint(0, 255)))
           f1.write('\n')
       f1.write('cnt\n')
       f1.write('show all\n')
       f1.write('quit\n')
