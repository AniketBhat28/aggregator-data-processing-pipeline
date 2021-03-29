
###################
#    Imports      #
###################

import io
import json
import os

import boto3
import pandas as pd

##################
# Global Variables
##################

s3 = boto3.resource("s3")
BASE_PATH = os.path.dirname(os.path.realpath(__file__))

class ReadStagingData:

    # Function to set S3 Bucket#
    def navigate_staging_bucket(self):
        with open(BASE_PATH + '/configData.json') as f:
            config_json = json.load(f)

        input_bucket_name = config_json['input_bucket_name']
        # output_bucket_name = config_json['output_bucket_name']
        # aggregator = config_json['aggregator_name']
        input_folder_name = config_json['aggregator_folder_name']
        month = config_json['month']
        year = config_json['year']

        app_config, input_dict = {}, {}
        app_config['input_params'], app_config['output_params'] = [], {}

        fileName = 'ebook' + '-' + str(month) + str(year) + '.snappy' + '.parquet'
        input_directory = 'raw_layer/revenue/aggregator/' + input_folder_name + '/' + str(year) + '/'
        # output_directory = 'pre_staging/revenue/ebook/' + aggregator.upper() + '/ebook-' + fileName

        input_dict['input_base_path'] = 's3://' + input_bucket_name + '/' + input_directory + '/'
        input_dict['input_bucket_name'] = input_bucket_name
        input_dict['input_directory'] = input_directory
        input_dict['input_sheet_name'] = fileName
        app_config['input_params'].append(input_dict)
        # app_config['output_params']['output_bucket_name'] = output_bucket_name
        # app_config['output_params']['output_directory'] = output_directory

        return app_config