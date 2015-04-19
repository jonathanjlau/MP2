import random
import sys

'''
Main function for the automatic command generator for MP2.

Generates commands to perform N experiments of adding P nodes and finding keys F times
'''

def quit(msg):
	'''Prints an error message and tells mp2 to stop running'''
	print >> sys.stderr, msg
	print 'quit'
	sys.exit()		

if __name__ == "__main__":

	# Check number of arguments
	if len(sys.argv) != 5:
		quit('Usage: python test_generator.py N P F')

	# Check whether arguments are integers
	try:
		N = int(sys.argv[2])
		P = int(sys.argv[3])
		F = int(sys.argv[4])
	except TypeError:
		quit(sys.stderr, 'N, P, or F is not an integer')

	# Check argument bounds
	if N < 1 or P < 1 or P > 255 or F < 1:
		quit('N, P, or F is out of bounds')

	all_nodes = xrange(1, 256)
	all_keys = xrange(256)

	for n in xrange(N):

		# Add P random nodes
		print 'reset-count'
		rand_nodes = random.sample(all_nodes, P)
		for node in rand_nodes:
			print 'join ' + node
		print 'show-count'

		# Perform find F times on random p and k
		print 'reset-count'
		cur_nodes = rand_nodes + [0]
		for f in xrange(F):
			p = random.choice(cur_nodes)
			k = random.choice(all_keys)
			print 'find ' + p + ' ' + k
		print 'show-count'

		# Remove all nodes other than node 0 to restart
		for node in rand_nodes:
			print 'leave ' + node
