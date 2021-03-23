
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



        # # Connect to s3 bucket
        # s3_bucket = s3.Bucket(input_bucket_name)
        #
        # # Function to read Parquet Aggregator files from S3 Bucket
        # def pd_read_s3_parquet(key, bucket, s3_client=None):
        #     if s3_client is None:
        #         s3_client = boto3.client('s3')
        #     obj = s3_client.get_object(Bucket=input_bucket_name, Key=key)
        #     return pd.read_parquet(io.BytesIO(obj['Body'].read()))
        #
        # s3_keys = [item.key for item in s3_bucket.objects.filter(Prefix=dir_path) if item.key.endswith('.parquet')]
        # if not s3_keys:
        #     print('No parquet file found in S3 bucket path', input_bucket_name, dir_path)
        # for aggFile in s3_keys:
        #     print(aggFile)
        #     extracted_data = pd_read_s3_parquet(aggFile, bucket=input_bucket_name)
        #     print(extracted_data)
        #
        # return pd.concat(extracted_data, ignore_index=True)

        return app_config