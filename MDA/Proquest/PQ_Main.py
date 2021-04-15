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

# Main Function
if __name__ == '__main__':

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

    with open(BASE_PATH + '/MDAProquestConfig.json') as f:
        config_json = json.load(f)

    input_bucket_name = config_json['input_bucket_name']
    output_bucket_name = config_json['output_bucket_name']
    aggregator = config_json['aggregator_name']
    year = config_json['year']
    aggregator_FileName = config_json['aggregator']
    # month wrapper

    month = config_json['month']
    year = config_json['year']

    app_config, input_dict = {}, {}
    app_config['input_params'], app_config['output_params'] = [], {}

    input_directory = 'prod/' + aggregator_FileName + '/input/' + str(year)
    output_directory = 'mapped_layer/revenue/aggregator/' + aggregator + '/' + str(year)

    input_dict['input_base_path'] = 's3://' + input_bucket_name + '/' + input_directory + '/'
    input_dict['input_bucket_name'] = input_bucket_name
    input_dict['input_directory'] = input_directory
    input_dict['input_sheet_name'] = None
    input_dict['aggregator_file_type'] = config_json['aggregator_file_type']
    app_config['input_params'].append(input_dict)
    app_config['output_params']['output_bucket_name'] = output_bucket_name
    app_config['output_params']['output_directory'] = output_directory
    app_config['output_params']['year'] = year

    module_path_relative = 'MDA.Proquest.MDAMappedProcessDataProquest'

    # Get the module path and start the process
    module_path = module_path_relative
    module = importlib.import_module(module_path)
    className = getattr(module, module_path_relative.split('.')[-1])
    classObj = className()
    classObj.initialise_processing(logger, app_config, BASE_PATH, aggregator)

    logger.info('\n+-+-+-+-+-+-+')
    logger.info("#################################")
    logger.info("             EXITING             ")
    logger.info("#################################\n\n")

    # Delete the logger
    logger.removeHandler(handler)
    del logger, handler
