
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
BASE_PATH = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/Json/')

#############################
#     Class Functions       #
#############################

class ReadStagingData:

    # Function to connect to S3 Bucket#
    def read_staging_bucket(self, config_json):

        # Read Config Data Json to get input for S3 Bucket Name, Aggregator, fileExtension etc
        input_bucket_name = config_json['input_bucket_name']
        output_bucket_name = config_json['output_bucket_name']
        aggregator = config_json['aggregator_name']
        input_folder_name = config_json['input_folder_name']
        month = config_json['month']
        year = config_json['year']
        product_type = config_json['product_type']
        trans_type = config_json['trans_type']
        file_extension = config_json['file_extension']
        is_aggregator_enabled = config_json['isEnabled']
        input_layer = config_json['input_layer']

        # Defined app_config dictionary
        app_config, input_dict = {}, {}
        app_config['input_params'], app_config['output_params'] = [], {}

        # Creating Data Validation report output file name based on Aggregator name
        fileName = 'data-validation-report-' + aggregator + '-' + str(year) + '.csv'

        # Creating Input Directory path to read parquet file based on channel: Aggregator, Warehouse , Direct Sales
        if input_folder_name in ['AMAZON', 'BARNES', 'EBSCO', 'FOLLETT', 'GARDNERS', 'PROQUEST', 'REDSHELF', 'CHEGG', 'BLACKWELLS', 'INGRAM']:
            input_directory = input_layer + '/revenue/aggregator/' + input_folder_name + '/' + 'year=' + str(year) + '/'
        else:
            if input_folder_name in ['USPT', 'UKBP', 'SGBM', 'AUSTLD']:
                input_directory = input_layer + '/revenue/warehouse/' + input_folder_name + '/' + 'year=' + str(
                    year) + '/'
            else:
                input_directory = input_layer + '/revenue/direct_sales/' + input_folder_name + '/' + 'year=' + str(year) + '/'

        # Creating output directory path to upload the data validation test result report based on aggregator
        output_directory = 'data_validation_scripts/revenue/aggregator/' + aggregator.upper() + '/' + 'year=' + str(year) + '/' + fileName

        # Saving all the input config data like S3 Bucket Name, Aggregator name etc in app_config dictionary
        input_dict['input_base_path'] = 's3://' + input_bucket_name + '/' + input_directory + '/'
        input_dict['input_bucket_name'] = input_bucket_name
        input_dict['input_directory'] = input_directory
        input_dict['input_sheet_name'] = fileName
        input_dict['input_file_extension'] = file_extension
        input_dict['aggregator_name'] = aggregator
        input_dict['is_aggregator_enabled'] = is_aggregator_enabled
        input_dict['input_layer'] = input_layer

        app_config['input_params'].append(input_dict)
        app_config['output_params']['output_bucket_name'] = output_bucket_name
        app_config['output_params']['output_directory'] = output_directory

        return app_config

    def get_parquet_files_list (self, bucket_name, path, file_extension):
        s3_bucket = s3.Bucket(bucket_name)
        file_list = {item.key for item in s3_bucket.objects.filter(Prefix=path) if item.key.endswith(file_extension)}
        if not file_list:
            print('No parquet file found in S3 bucket path', bucket_name, path + '------')
        else:
            print("\n-+-+-+-+-Connected to S3 Bucket:  " + bucket_name + '-+-+-+-+-')
        print("\n-+-+-+-+-Following is the list of parquet files found in s3 bucket-+-+-+-+-")
        return file_list

    def read_parquet_file(self, key, bucket_name, s3_client=None):
        if s3_client is None:
            s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        return pd.read_parquet(io.BytesIO(obj['Body'].read()))

    # Function to save order data validation test results report to S3 Bucket
    def store_data_validation_report(self, logger, app_config, final_data):
        logger.info('\n+-+-+-+-+-+-+')
        logger.info("Store Data validation script results at the given S3 location")
        logger.info('\n+-+-+-+-+-+-+')

        output_bucket_name = app_config['output_params']['output_bucket_name']
        output_directory = app_config['output_params']['output_directory']

        logger.info('Writing the output at the given S3 location')
        csv_buffer = StringIO()
        print(final_data.columns)
        print(final_data)
        final_data.to_csv(csv_buffer, index=False)
        s3.Object(output_bucket_name, output_directory).put(Body=csv_buffer.getvalue())
        logger.info('Order validation script successfully stored at the given S3 location')
        print("\n---------Data Validation Report has been successfully created in S3 bucket-------")