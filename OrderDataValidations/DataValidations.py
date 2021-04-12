# imports

import pandas as pd
import boto3
import io

# from OrderDataValidations.AggregatorDataValidations import AmazonAggregatorValidations
from OrderDataValidations.GenericAggregatorValidations import GenericAggregatorValidations
from OrderDataValidations.ReadStagingData import ReadStagingData
from OrderDataValidations.AggregatorDataValidations.Amazon.AmazonAggregatorValidations import AmazonAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Ebsco.EbscoAggregatorValidations import EbscoAggregatorValidations

# Global variables
obj_read_data = ReadStagingData()
obj_amazon_agg_val = AmazonAggregatorValidations()
obj_ebsco_agg_val = EbscoAggregatorValidations()
obj_generic_agg_val = GenericAggregatorValidations()
s3 = boto3.resource("s3")

# Starting Order Data Validations
def OrderDataValidations():
    app_config = obj_read_data.read_staging_bucket()
    input_bucket_name = app_config['input_params'][0]['input_bucket_name']
    input_file_extension = app_config['input_params'][0]['input_file_extension']
    dir_path = app_config['input_params'][0]['input_directory']

    s3_bucket = s3.Bucket(input_bucket_name)

    # Function to read Parquet Aggregator files from S3 Bucket

    def pd_read_s3_parquet(key, bucket, s3_client=None):
        if s3_client is None:
            s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket=input_bucket_name, Key=key)
        return pd.read_parquet(io.BytesIO(obj['Body'].read()))

    # Read Each Parquet File and perform Generic and Aggregator Data validations
    file_list = {item.key for item in s3_bucket.objects.filter(Prefix=dir_path) if item.key.endswith(input_file_extension)}

    if not file_list:
        print('No parquet file found in S3 bucket path', input_bucket_name, dir_path + '------')
    else:
        print("\n-+-+-+-+-Connected to S3 Bucket:  " + input_bucket_name + '-+-+-+-+-')

    print("\n-+-+-+-+-Following is the list of parquet files found in s3 bucket-+-+-+-+-")
    print(file_list)

    for aggFile in file_list:
        extracted_data = pd_read_s3_parquet(aggFile, bucket=input_bucket_name)
        print("\n------FileName: " + aggFile + "------")
        obj_generic_agg_val.generic_data_validations(test_data=extracted_data)
        # obj_ebsco_agg_val.ebsco_agg_validations(test_data=extracted_data)
        # obj_amazon_agg_val.amazon_agg_validations(test_data=extracted_data)