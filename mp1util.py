# Utility functions

def make_key_value(string):
	'''Convert a string of keys and values separated by newlines into a dictionary of keys and values'''
	# Split the string into a list of strings by removing newlines
	lines = string.splitlines()

	dictionary = {}
	for ln in lines:
		key_value = ln.split(':', 1)
		# Skip any line that does not contain key values
		if len(key_value) > 1:
			key = key_value[0].strip()
			value = key_value[1].strip()
			dictionary[key] = value
	return dictionary

def read_key_value_file(filename):
	'''Read a file containing key value pairs and convert it into a dictionary'''
	f = open(filename, 'r') = f.read()
	return make_key_value(f.read())

def compress_dict(dictionary):
	'''Compresses a dictionary into a string'''
	string = ''
	for key, value in dictionary.iteritems():
		string += key + ':' + value + "\n"
	return string

def compress_message(message):
	'''Compresses a message into a string'''
	return compress_dict(message) + "\r"

def extract_message(char_list):
	'''Extracts a message from character list or return None if the array does not contain a message'''
	try:
		delim_pos = char_array.index("\r")
		message = make_key_value(char_array[0:delim_pos])
		del char_array[0:delim_pos + 1]
		return message
	except ValueError:
		return None
