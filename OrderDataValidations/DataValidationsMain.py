# imports

import pandas as pd
import numpy as np
import boto3
import io

# from OrderDataValidations.AggregatorDataValidations import AmazonAggregatorValidations
from OrderDataValidations.GenericAggregatorValidations import GenericAggregatorValidations
from OrderDataValidations.ReadStagingData import ReadStagingData

# Global variables
obj_read_data = ReadStagingData()
# obj_amazon_agg_val = AmazonAggregatorValidations()
obj_generic_agg_val = GenericAggregatorValidations()
s3 = boto3.resource("s3")

# Starting Order Data Validations
def OrderDataValidations():
    app_config = obj_read_data.navigate_staging_bucket()
    input_bucket_name = app_config['input_params'][0]['input_bucket_name']
    dir_path = app_config['input_params'][0]['input_directory']

    print(app_config)
    s3_bucket = s3.Bucket(input_bucket_name)


    # Function to read Parquet Aggregator files from S3 Bucket

    def pd_read_s3_parquet(key, bucket, s3_client=None):
        if s3_client is None:
            s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket=input_bucket_name, Key=key)
        return pd.read_parquet(io.BytesIO(obj['Body'].read()))

    # Read Each Parquet File and perform Generic and Aggregator Data validations
    s3_keys = {item.key for item in s3_bucket.objects.filter(Prefix=dir_path) if item.key.endswith('.parquet')}
    if not s3_keys:
        print('No parquet file found in S3 bucket path', input_bucket_name, dir_path)
    else:
        print("\n-------Connected to S3 Bucket:  " +input_bucket_name + "-------\n")

    for aggFile in s3_keys:
        extracted_data = pd_read_s3_parquet(aggFile, bucket=input_bucket_name)
        print("\n------FileName: " + aggFile + "---------\n")
        obj_generic_agg_val.validate_isbn(test_data=extracted_data)

    return pd.concat(extracted_data, ignore_index=True)

OrderDataValidations()