
###################
#    Imports      #
###################

import io
import json
import os

import boto3
from io import StringIO
import pandas as pd

##################
# Global Variables
##################

s3 = boto3.resource("s3")
BASE_PATH = os.path.dirname(os.path.realpath(__file__))

class ReadStagingData:

    # Function to connect to S3 Bucket#
    def navigate_staging_bucket(self):
        with open(BASE_PATH + '/configData.json') as f:
            config_json = json.load(f)

        input_bucket_name = config_json['input_bucket_name']
        output_bucket_name = config_json['output_bucket_name']
        aggregator = config_json['aggregator_name']
        input_folder_name = config_json['aggregator_folder_name']
        month = config_json['month']
        year = config_json['year']
        product_type = config_json['product_type']
        trans_type = config_json['trans_type']

        app_config, input_dict = {}, {}
        app_config['input_params'], app_config['output_params'] = [], {}

        fileName = 'data-validation-report-' + aggregator + '-' + str(year) + '.csv'
        # input_directory = 'mapped_layer/revenue/aggregator/' + input_folder_name + '/' + 'year=' + str(year) + '/' + 'product_type=' + product_type + '/' + 'trans_type=' + trans_type
        input_directory = 'mapped_layer/revenue/aggregator/' + input_folder_name + '/' + 'year=' + str(year) + '/'
        output_directory = 'data_validation_scripts/revenue/aggregator/' + aggregator.upper() + '/' + 'year=' + str(year) + '/' + fileName

        input_dict['input_base_path'] = 's3://' + input_bucket_name + '/' + input_directory + '/'
        input_dict['input_bucket_name'] = input_bucket_name
        input_dict['input_directory'] = input_directory
        input_dict['input_sheet_name'] = fileName
        app_config['input_params'].append(input_dict)
        app_config['output_params']['output_bucket_name'] = output_bucket_name
        app_config['output_params']['output_directory'] = output_directory

        return app_config

    # Function to write order validation results data to S3 Bucket
    def store_data_validation_report(self, logger, app_config, final_data):
        logger.info('\n+-+-+-+-+-+-+')
        logger.info("Store Data validation script results at the given S3 location")
        logger.info('\n+-+-+-+-+-+-+')

        output_bucket_name = app_config['output_params']['output_bucket_name']
        output_directory = app_config['output_params']['output_directory']

        logger.info('Writing the output at the given S3 location')
        csv_buffer = StringIO()
        final_data.to_csv(csv_buffer, index=False)
        s3.Object(output_bucket_name, output_directory).put(Body=csv_buffer.getvalue())
        logger.info('Order validation script successfully stored at the given S3 location')