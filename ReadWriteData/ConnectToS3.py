#################################
#			IMPORTS				#
#################################


import pandas as pd
import numpy as np
import boto3
from io import StringIO
import awswrangler as wr

#################################
#		GLOBAL VARIABLES		#
#################################


s3 = boto3.resource("s3")


#################################
#		CLASS FUNCTIONS			#
#################################


class ConnectToS3:

    # Function Description :	This function gets filenames from given S3 location
    # Input Parameters : 		logger - For the logging output file.
    #							input_list - input list params
    # Return Values : 			files_in_s3 - list of filenames
    def get_files(self, logger, input_list):
        logger.info('Getting list of files from given S3 location')
        input_bucket_name = input_list[0]['input_bucket_name']
        dir_path = input_list[0]['input_directory']

        # Connect to s3 bucket
        s3_bucket = s3.Bucket(input_bucket_name)
        # Listing files in s3 bucket
        files_in_s3 = [f.key.split(dir_path + "/")[1] for f in s3_bucket.objects.filter(Prefix=dir_path).all()]
        for file in files_in_s3:
            if file != '' and (file[-1]=='/'):
                files_in_s3.remove(file)

        return files_in_s3

    # Function Description :	This function writes the output to given S3 location
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							final_data - Final staging data
    # Return Values : 			None
    def store_data(self, logger, app_config, final_data):
        logger.info('\n+-+-+-+-+-+-++')
        logger.info("Storing the staging output at the given S3 location")
        logger.info('\n+-+-+-+-+-+-+')

        output_bucket_name = app_config['output_params']['output_bucket_name']
        output_directory = app_config['output_params']['output_directory']

        logger.info('Writing the output at the given S3 location')
        csv_buffer = StringIO()
        final_data.to_csv(csv_buffer, index=False)
        s3.Object(output_bucket_name, output_directory).put(Body=csv_buffer.getvalue())
        logger.info('Staging data successfully stored at the given S3 location')

        # Function Description :	This function writes the parquet output to given S3 location
        # Input Parameters : 		logger - For the logging output file.
        #							app_config - Configuration
        #							final_data - Final master model data
        # Return Values : 			None
    def store_data_as_parquet(self, logger, app_config, final_data) :
        logger.info('\n+-+-+-+-+-+-+*')
        logger.info("Storing the master model data output at the given S3 location")
        logger.info('\n+-+-+-+-+-+-+**')

        output_bucket_name = app_config['output_params']['output_bucket_name']
        output_directory = app_config['output_params']['output_directory']
        year=app_config['output_params']['year']
        final_data['year'] = year



        logger.info('Writing the output at the given S3 location')
        final_data.to_parquet('s3://' + output_bucket_name + '/' + output_directory, compression='snappy',engine='pyarrow'
                             ,partition_cols=['year', 'product_type', 'trans_type'])


        logger.info('master model data successfully stored at the given S3 location')

    def wrangle_data_as_parquet(self, logger, app_config, final_data) :
        logger.info('\n+-+-+-+-+-+-+*')
        logger.info("Storing the master model data output at the given S3 location")
        logger.info('\n+-+-+-+-+-+-+**')

        output_bucket_name = app_config['output_params']['output_bucket_name']
        output_directory = app_config['output_params']['output_directory']
        year=app_config['output_params']['year']
        final_data['year'] = year
        logger.info('Writing the output at the given S3 location')
        wr.s3.to_parquet(  # Storing the data and metadata to Data Lake
            df=final_data,
            path='s3://' + output_bucket_name + '/' + output_directory,
            compression = 'snappy',
            partition_cols=['year', 'product_type', 'trans_type'],
            mode='append',
            dataset=True
        )

        logger.info('master model data successfully stored at the given S3 location')



