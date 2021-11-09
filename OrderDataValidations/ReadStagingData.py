
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

#############################
#     Class Functions       #
#############################

class ReadStagingData:

    # Function to connect to S3 Bucket#
    def read_staging_bucket(self, config_json):

        # Read Config Data Json to get input for S3 Bucket Name, Aggregator, Year etc
        input_bucket_name = config_json['orders_bucket']
        output_bucket_name = config_json['orders_bucket']
        aggregator = config_json['aggregator_name']
        input_folder_name = config_json['s3_temp_dir']
        input_layer = config_json['input_layer']
        time_frame = config_json['source_start_range'][:4]
        end_time_frame = config_json['source_end_range'][:4]
        source_type = config_json['source_type']
        env = config_json['env']
        job_type = config_json['job_type']

        # Setting Year and file_extension value
        year = time_frame[:4]
        file_extension = '.parquet'

        # Creating Data Validation report output file name based on Aggregator name
        fileName = 'data-validation-report-' + aggregator + '-' + str(year) + '.csv'

        # Creating Input Directory path to read parquet file based on channel: Warehouse , Direct Sales and Aggregator
        if source_type == "warehouse":
            input_directory = input_layer + '/revenue/warehouse/' + input_folder_name + '/' + 'year=' + str(
                    year) + '/'
        else:
            if source_type == "direct_sales":
                input_directory = input_layer + '/revenue/direct_sales/' + input_folder_name + '/' + 'year=' + str(
                    year) + '/'
            else:
                input_directory = input_layer + '/revenue/aggregator/' + input_folder_name + '/' + 'year=' + str(
                    year) + '/'

        # Creating output directory path to upload the data validation test result report based on aggregator
        output_directory = 'data_validation_scripts/revenue/aggregator/' + aggregator.upper() + '/' + 'year=' + str(year) + '/' + fileName

        # Defined app_config dictionary
        app_config, input_dict = {}, {}
        app_config['input_params'], app_config['output_params'] = [], {}

        # Saving all the input config data like S3 Bucket Name, Aggregator name etc in app_config dictionary
        input_dict['input_base_path'] = 's3://' + input_bucket_name + '/' + input_directory + '/'
        input_dict['input_bucket_name'] = input_bucket_name
        input_dict['input_directory'] = input_directory
        input_dict['input_sheet_name'] = fileName
        input_dict['input_file_extension'] = file_extension
        input_dict['aggregator_name'] = aggregator
        input_dict['input_layer'] = input_layer
        input_dict['env'] = env
        input_dict['job_type'] = job_type

        app_config['input_params'].append(input_dict)
        app_config['output_params']['output_bucket_name'] = output_bucket_name
        app_config['output_params']['output_directory'] = output_directory

        return app_config

    def get_parquet_files_list (self, bucket_name, path, file_extension):
        s3_bucket = s3.Bucket(bucket_name)
        file_list = {item.key for item in s3_bucket.objects.filter(Prefix=path) if item.key.endswith(file_extension)}
        if not file_list:
            print('No parquet file found in S3 bucket path', bucket_name, path + '------')
            raise FileNotFoundError
        else:
            print("\n-+-+-+-+-Connected to S3 Bucket:  " + bucket_name + '-+-+-+-+-')
        print("\n-+-+-+-+-Following is the list of parquet files found in s3 bucket-+-+-+-+-\n" + str(file_list))
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