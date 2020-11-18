#################################
#			IMPORTS				#
#################################


import os
import sys
import json
import importlib
import configparser
import time
import logging



#################################
#		GLOBAL VARIABLES		#
#################################


BASE_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append('Output/aggregator_data_processing_pipeline-1.0-py3.7.egg')

#Converting Month name to numbers to create output filename
def processMonth(month):
	monthMapper={'JAN':'01','FEB':'02',
				'MAR':'03',
				'APR':'04',
				'MAY':'05',
				'JUN':'06',
				'JUL':'07',
				'AUG':'08',
				'SEP':'09',
				'OCT':'10',
				'NOV':'11',
				'DEC':'12'}
	return monthMapper.get(month,"Invalid month of year")

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

	with open(BASE_PATH+'/Config.json') as f:
		config_json = json.load(f)

	input_bucket_name = config_json['input_bucket_name']
	output_bucket_name = config_json['output_bucket_name']
	aggregator = config_json['aggregator_name']

	aggregator_FileName = config_json['aggregator']
	month = config_json['month']
	year = config_json['year']



	app_config, input_dict = {}, {}
	app_config['input_params'], app_config['output_params'] = [], {}

	fileName = processMonth(month) + str(year) + '.csv'
	print('fileName', fileName)
	input_directory = 'prd/' + aggregator_FileName + '/input/' + str(year) + '/' + month
	print('input_directory', input_directory)
	output_directory = 'staging/revenue/aggregator/' + aggregator_FileName + '/eBook-' + fileName
	print('output_directory', output_directory)


	input_dict['input_base_path'] = 's3://' + input_bucket_name + '/' + input_directory + '/'
	input_dict['input_bucket_name'] = input_bucket_name
	input_dict['input_directory'] = input_directory
	input_dict['input_sheet_name'] = None
	app_config['input_params'].append(input_dict)
	app_config['output_params']['output_bucket_name'] = output_bucket_name
	app_config['output_params']['output_directory'] = output_directory

	print(app_config)

	with open(BASE_PATH+'/AggRulesVal.json') as f:
		rule_config = json.load(f)

	with open(BASE_PATH+'/Default.json') as f:
		default_config = json.load(f)

	# Check the aggregator to initialise appropriate module
	if aggregator == 'Amazon':
		module_path_relative = 'StagingDataGenerators.ProcessDataAmazon'
	elif aggregator == 'Ebsco':
		module_path_relative = 'StagingDataGenerators.ProcessDataEbsco'
	elif aggregator == 'PQ':
		module_path_relative = 'StagingDataGenerators.ProcessDataPqCentral'
	elif aggregator == 'Chegg':
		module_path_relative = 'StagingDataGenerators.ProcessDataChegg'
	elif aggregator == 'Ingram':
		module_path_relative = 'StagingDataGenerators.ProcessDataIngram'
	elif aggregator == 'Gardners':
		module_path_relative = 'StagingDataGenerators.ProcessDataGardners'
	elif aggregator == 'Follett':
		module_path_relative = 'StagingDataGenerators.ProcessDataFollett'
	elif aggregator == 'Barnes':
		module_path_relative = 'StagingDataGenerators.ProcessDataBarnes'
	elif aggregator == 'Overdrive':
		module_path_relative = 'StagingDataGenerators.ProcessDataOverdrive'
	elif aggregator == 'Google':
		module_path_relative = 'StagingDataGenerators.ProcessDataGoogle'
	elif aggregator == 'Redshelf':
		module_path_relative = 'StagingDataGenerators.ProcessDataRedshelf'

	# Get the module path and start the process
	module_path = module_path_relative
	module = importlib.import_module(module_path)
	className = getattr(module, module_path_relative.split('.')[-1])
	classObj = className()
	classObj.initialise_processing(logger, app_config, rule_config, default_config)

	logger.info('\n+-+-+-+-+-+-+')
	logger.info("#################################")
	logger.info("             EXITING             ")
	logger.info("#################################\n\n")

	# Delete the logger
	logger.removeHandler(handler)
	del logger, handler

	