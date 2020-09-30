#################################
#			IMPORTS				#
#################################


import os
import json
import importlib
import configparser
import time
import logging



#################################
#		GLOBAL VARIABLES		#
#################################


BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = BASE_PATH# + "/configuration"



# Main Function
if __name__ == '__main__':

	# Enabling logger
	logging.basicConfig(level=logging.INFO)
	logger = logging.getLogger(__name__)

	# Create a file handler
	handler = logging.FileHandler('Output/Output.log')
	handler.setLevel(logging.INFO)

	# Create a logging format
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)

	# Add the handlers to the logger
	logger.addHandler(handler)

	logger.info("######################################")
	logger.info("             INITIALISING             ")
	logger.info("######################################")


	# app_configs_list = []
	# config_files = os.listdir( CONFIG_PATH )
	# for each_config in config_files:
	# 	config = configparser.ConfigParser()
	# 	config.read(CONFIG_PATH + '/' + each_config)
	# 	app_configs_list.append(config)

	app_config = configparser.ConfigParser()
	app_config.read(CONFIG_PATH+'/Config.ini')

	with open(CONFIG_PATH+'/AggRulesVal.json') as f:
		rule_config = json.load(f)

	# Record the start time for current run
	start_time = time.time()

	# Get aggregator name
	aggregator = str(app_config['INPUT']['File_Aggregator'])

	# Check the aggregator to initialise appropriate module
	if aggregator == 'Amazon':
		module_path_relative = 'Amazon.ProcessDataAmazon'
	elif aggregator == 'Ebsco':
		module_path_relative = 'Ebsco.ProcessDataEbsco'
	elif aggregator == 'PQ':
		module_path_relative = 'PQCentral.ProcessDataPqCentral'
	
	# Get the module path and start the process
	module_path = module_path_relative
	module = importlib.import_module(module_path)
	className = getattr(module, module_path_relative.split('.')[-1])
	classObj = className()
	classObj.initialise_processing(logger, app_config, rule_config)

	logger.info('\n+-+-+-+-+-+-+')
	logger.info("#################################")
	logger.info("             EXITING             ")
	logger.info("#################################\n\n")

	# Delete the logger
	logger.removeHandler(handler)
	del logger, handler

	#logger.info("Time taken by the current run is %s seconds ---" % (time.time() - start_time))
		
