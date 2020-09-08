#################################
#			IMPORTS				#
#################################


import os
import json
import importlib
import configparser
import time



#################################
#		GLOBAL VARIABLES		#
#################################


BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = BASE_PATH# + "/configuration"



# Main Function
if __name__ == '__main__':


	# app_configs_list = []
	# config_files = os.listdir( CONFIG_PATH )
	# for each_config in config_files:
	# 	config = configparser.ConfigParser()
	# 	config.read(CONFIG_PATH + '/' + each_config)
	# 	app_configs_list.append(config)

	app_config = configparser.ConfigParser()
	app_config.read(CONFIG_PATH+'/config.ini')

	with open(CONFIG_PATH+'/agg-rules-1Sep.json') as f:
		rule_config = json.load(f)

	# Record the start time for current run
	start_time = time.time()

	# Get aggregator name
	aggregator = str(app_config['INPUT']['File_Aggregator'])

	# Check the aggregator to initialise appropriate module
	if aggregator == 'Amazon':
		module_path_relative = 'Amazon.process_data_amazon'
	elif aggregator == 'Chegg':
		module_path_relative = 'Chegg.process_data_chegg'
	
	# Get the module path and start the process
	module_path = module_path_relative
	module = importlib.import_module(module_path)
	className = getattr(module, module_path_relative.split('.')[-1])
	classObj = className()
	classObj.initialise_processing(app_config, rule_config)

	print("Time taken by the current run is %s seconds ---" % (time.time() - start_time))
		

