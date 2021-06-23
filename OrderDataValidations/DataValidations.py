####################
#     Imports      #
####################

import pandas as pd
import boto3
import io
import importlib
import json
import os

from OrderDataValidations.GenericAggregatorValidations import GenericAggregatorValidations
from OrderDataValidations.ReadStagingData import ReadStagingData

#############################
#      Global Variables     #
#############################

obj_read_data = ReadStagingData()
obj_generic_agg_val = GenericAggregatorValidations()
s3 = boto3.resource("s3")
BASE_PATH = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/Json/')

#############################
#     Class Functions       #
#############################

class DataValidations:

    # Starting Order Data Validations script Main function
    def order_data_validation_main(self, app_config, schema_val_rules, generic_val_rules, agg_val_rules):

        # Initializing app_config when running locally
        if __name__ == '__main__':
            with open(BASE_PATH + '/configData.json') as f:
                config_json = json.load(f)
        if not app_config:
            app_config = obj_read_data.read_staging_bucket(config_json)

        # Reading data from app_config and extracting all required details
        input_bucket_name = app_config['input_params'][0]['input_bucket_name']
        aggregator = app_config['input_params'][0]['aggregator_name']
        is_aggregator_enabled = app_config['input_params'][0]['is_aggregator_enabled']
        input_file_extension = app_config['input_params'][0]['input_file_extension']
        dir_path = app_config['input_params'][0]['input_directory']

        s3_bucket = s3.Bucket(input_bucket_name)

        # Function to read all Parquet files from specified S3 Bucket
        def pd_read_s3_parquet(key, bucket, s3_client=None):
            if s3_client is None:
                s3_client = boto3.client('s3')
            obj = s3_client.get_object(Bucket=input_bucket_name, Key=key)
            return pd.read_parquet(io.BytesIO(obj['Body'].read()))

        # Read Each Parquet File from the Parquet files list
        file_list = {item.key for item in s3_bucket.objects.filter(Prefix=dir_path) if item.key.endswith(input_file_extension)}

        if not file_list:
            print('No parquet file found in S3 bucket path', input_bucket_name, dir_path + '------')
        else:
            print("\n-+-+-+-+-Connected to S3 Bucket:  " + input_bucket_name + '-+-+-+-+-')

        print("\n-+-+-+-+-Following is the list of parquet files found in s3 bucket-+-+-+-+-")
        print(file_list)

        # Start Schema and Generic Data validations followed by Aggregator validations on each file
        for aggFile in file_list:
            extracted_data = pd_read_s3_parquet(aggFile, bucket=input_bucket_name)
            print("\n------FileName: " + aggFile + "------")

            # Reading schema rules from json when running locally
            if not schema_val_rules:
                with open(BASE_PATH + '/schema-validation-staging-layer.json') as f:
                    schema_val_rules = json.load(f)

            # Reading generic rules from json when running locally
            if not generic_val_rules:
                with open(BASE_PATH + '/generic-validation-rules.json') as f:
                    generic_val_rules = json.load(f)


            # Run Schema and Generic Data Validations on the extracted file data
            obj_generic_agg_val.generic_data_validations(test_data=extracted_data, schema_val_json=schema_val_rules, generic_val_json=generic_val_rules)

            # Initialize with respective Aggregator specific validation rules based on Aggregator Name
            if aggregator == 'AMAZON':
                module_path_relative = 'AggregatorDataValidations.Amazon.AmazonAggregatorValidations'
            elif aggregator == 'BARNES':
                module_path_relative = 'AggregatorDataValidations.Barnes.BarnesAggregatorValidations'
            elif aggregator == 'CHEGG':
                module_path_relative = 'AggregatorDataValidations.Chegg.CheggAggregatorValidations'
            elif aggregator == 'EBSCO':
                module_path_relative = 'AggregatorDataValidations.Ebsco.EbscoAggregatorValidations'
            elif aggregator == 'FOLLET':
                module_path_relative = 'AggregatorDataValidations.Follett.FollettAggregatorValidations'
            elif aggregator == 'GARDNERS':
                module_path_relative = 'AggregatorDataValidations.Gardners.GardnersAggregatorValidations'
            elif aggregator == 'PROQUEST':
                module_path_relative = 'AggregatorDataValidations.Proquest.ProquestAggregatorValidations'
            elif aggregator == 'REDSHELF':
                module_path_relative = 'AggregatorDataValidations.Redshelf.RedshelfAggregatorValidations'
            elif aggregator == 'BLACKWELLS':
                module_path_relative = 'AggregatorDataValidations.Blackwells.BlackwellsAggregatorValidations'
            elif aggregator == 'INGRAMVS':
                module_path_relative = 'AggregatorDataValidations.Ingramvs.IngramvsAggregatorValidations'
            elif aggregator == 'OMS':
                module_path_relative = 'OwnedsitesDataValidations.OMS.OMSAggregatorValidations'
            elif aggregator == 'UBW':
                module_path_relative = 'OwnedsitesDataValidations.UBW.UBWAggregatorValidations'
            elif aggregator == 'USPT':
                module_path_relative = 'WarehouseDataValidations.USPT.UsptWarehouseValidations'
            elif aggregator == 'UKBP':
                module_path_relative = 'WarehouseDataValidations.UKBP.UkbpWarehouseValidations'
            elif aggregator == 'SGBM':
                module_path_relative = 'WarehouseDataValidations.SGBM.SgbmWarehouseValidations'
            elif aggregator == 'AUSTLD':
                module_path_relative = 'WarehouseDataValidations.AUSTLD.AustldWarehouseValidations'

            # Run the respective Aggregator specific validation rules on the parquet file data
            module_path = module_path_relative
            module = importlib.import_module(module_path)
            className = getattr(module, module_path_relative.split('.')[2])
            classObj = className()
            classObj.aggregator_data_validations(test_data=extracted_data,agg_val_json=agg_val_rules)

        return extracted_data

# Below code is just for executing the class locally
if __name__ == '__main__':
    DataValidations.order_data_validation_main(self=None, app_config=None, schema_val_rules=None, generic_val_rules=None, agg_val_rules=None)