# Utility functions

'''
Read all lines in a file and return them as a list
'''
def read_file_lines(filename):
	f = open(filename, 'r')
	lines = f.read().splitlines()
	f.close()
	return lines


'''
Read a configuration file and return a dictionary representing the configuration
'''
def read_config_file(filename):
	lines = read_file_lines(filename)

	# Split each line into key value pairs and insert them into a dictionary
	config = {}
	for line in lines:
		key_value = line.split(':', 1)
		if len(key_value) > 1:
			key = key_value[0].strip()
			value = key_value[1].strip()
			config[key] = value

	float_value_keys = ['a', 'b', 'c', 'd']
	int_value_keys = ['port']

	for key in float_value_keys:
		config[key] = float(config[key])

	for key in int_value_keys:
		config[key] = int(config[key])
	
	return config
