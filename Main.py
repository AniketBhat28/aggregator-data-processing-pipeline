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
print('Base_path', BASE_PATH)
sys.path.append('Output/aggregator_data_processing_pipeline_pq_historical-1.0-py3.7.egg')

OUTPUT_DIR = os.path.join(BASE_PATH, 'Output')

# Create output directory if not exist
if not os.path.exists(OUTPUT_DIR):
    os.mkdir(OUTPUT_DIR)

# Converting Month name to numbers to create output filename
def process_month(month):
    month_mapper = {'JAN': '01', 'FEB': '02',
                    'MAR': '03',
                    'APR': '04',
                    'MAY': '05',
                    'JUN': '06',
                    'JUL': '07',
                    'AUG': '08',
                    'SEP': '09',
                    'OCT': '10',
                    'NOV': '11',
                    'DEC': '12'}
    month_mapper2 = {'Jan': '01', 'Feb': '02',
                     'Mar': '03',
                     'Apr': '04',
                     'May': '05',
                     'Jun': '06',
                     'July': '07',
                     'Aug': '08',
                     'Sep': '09',
                     'Oct': '10',
                     'Nov': '11',
                     'Dec': '12'}
    return month_mapper.get(month, month_mapper2.get(month, "Invalid Month"))


# Main Function
if __name__ == '__main__':

    # Enabling logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Create a file handler
    handler = logging.FileHandler(os.path.join(OUTPUT_DIR, 'Output.log'))
    handler.setLevel(logging.INFO)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(handler)

    logger.info("######################################")
    logger.info("             INITIALISING             ")
    logger.info("######################################")

    with open(BASE_PATH + '/MDAConfig.json') as f:
        config_json = json.load(f)

    input_bucket_name = config_json['input_bucket_name']
    output_bucket_name = config_json['output_bucket_name']
    aggregator = config_json['aggregator_name']
    input_folder_name = config_json['input_folder_name']

    month = config_json['month']
    year = config_json['year']
    layer = config_json['layer']
    app_config, input_dict = {}, {}
    app_config['input_params'], app_config['output_params'] = [], {}

    mapped_layer_path = 'mapped_layer/revenue/aggregator/'

    if layer != 'staging':
        if month == '':
            input_directory = 'prod/' + input_folder_name + '/input/' + str(year)
            output_directory = mapped_layer_path + aggregator.upper()
        else:
            input_directory = 'prod/' + input_folder_name + '/input/' + str(year) + '/' + month
            output_directory = mapped_layer_path + aggregator.upper()

    else:
        input_directory = mapped_layer_path + aggregator.upper() + '/year=' + str(year)
        output_directory = 'staging_layer/revenue/aggregator/' + aggregator.upper()

    input_dict['input_base_path'] = 's3://' + input_bucket_name + '/' + input_directory + '/'
    input_dict['input_bucket_name'] = input_bucket_name
    input_dict['input_directory'] = input_directory
    input_dict['input_sheet_name'] = None
    app_config['input_params'].append(input_dict)
    app_config['output_params']['output_bucket_name'] = output_bucket_name
    app_config['output_params']['output_directory'] = output_directory
    app_config['output_params']['year'] = str(year)
    app_config['output_params']['layer'] = layer

    with open(BASE_PATH + '/MDA' + aggregator + '/MDAAggRulesVal.json') as f:
        rule_config = json.load(f)

    with open(BASE_PATH + '/MDA' + aggregator + '/MDADefault.json') as f:
        default_config = json.load(f)

    # Check the aggregator to initialise appropriate module
    if aggregator == 'Amazon':
        if layer == 'staging':
            module_path_relative = 'MDAAmazon.MDAStagingProcessDataAmazon'
        else:
            module_path_relative = 'MDAAmazon.MDAMappedProcessDataAmazon'
    elif aggregator == 'Ebsco':
        if layer == 'staging':
            module_path_relative = 'MDAEbsco.MDAStagingProcessDataEbsco'
        else:
            module_path_relative = 'MDAEbsco.MDAMappedProcessDataEbsco'

    elif aggregator == 'PROQUEST':
        if layer == 'staging':
            module_path_relative = 'MDAProquest.MDAStagingProcessDataProquest'
        else:
            module_path_relative = 'MDAProquest.MDAMappedProcessDataProquestLive'

    elif aggregator == 'Chegg':
        if layer == 'staging':
            module_path_relative = 'MDAChegg.MDAStagingProcessDataChegg'
        else:
            module_path_relative = 'MDAChegg.MDAMappedProcessDataChegg'
            
    elif aggregator == 'Ingram':
        module_path_relative = 'StagingDataGenerators.ProcessDataIngram'

    elif aggregator == 'Gardners':
        if layer == 'staging' :
            module_path_relative = 'MDAGardners.MDAStagingProcessDataGardners'
        else :
            module_path_relative = 'MDAGardners.MDAMappedProcessDataGardners'
    elif aggregator == 'Follet':
        if layer == 'staging':
            module_path_relative = 'MDAFollet.MDAStagingProcessDataFollett'
        else:
            module_path_relative = 'MDAFollet.MDAMappedProcessDataFollett'
    elif aggregator == 'Barnes':
        if layer == 'staging' :
            module_path_relative = 'MDABarnes.MDAStagingProcessDataBarnes'
        else :
            module_path_relative = 'MDABarnes.MDAMappedProcessDataBarnes'
    elif aggregator == 'Overdrive':
        module_path_relative = 'StagingDataGenerators.ProcessDataOverdrive'

    elif aggregator == 'Google':
        module_path_relative = 'StagingDataGenerators.ProcessDataGoogle'

    elif aggregator == 'Redshelf':
        if layer == 'staging' :
            module_path_relative = 'MDARedshelf.MDAStagingProcessDataRedshelf'
        else :
            module_path_relative = 'MDARedshelf.MDAMappedProcessDataRedshelf'
    elif aggregator == 'PQ_Subscription':
        module_path_relative = 'StagingDataGenerators.ProcessDataPqSubscription'

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
