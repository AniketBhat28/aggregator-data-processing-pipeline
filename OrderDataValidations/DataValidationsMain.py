####################
#     Imports      #
####################

import pandas as pd
import boto3
import io
import importlib
import json
import os

from OrderDataValidations.ReadStagingData import ReadStagingData
from OrderDataValidations.SchemaValidations import SchemaValidations
from OrderDataValidations.GenericValidations import GenericValidations
from OrderDataValidations.AggregatorValidations import AggregatorValidations
from OrderDataValidations.DataValidationsReporting import DataValidationsReporting
from OrderDataValidations.EdwGenericDataValidations import EdwGenericDataValidations
from OrderDataValidations.EdwAggregatorSpecificDataValidations import EdwAggregatorSpecificDataValidations

#############################
#      Global Variables     #
#############################
# Creating object for ReadStagingData class
obj_read_data = ReadStagingData()
# Creating object for SchemaValidations class
obj_schema_data_val = SchemaValidations()
# Creating object for GenericValidations class
obj_generic_data_val = GenericValidations()
# Creating object for AggregatorValidations class
obj_aggregator_data_val = AggregatorValidations()
# Creating object for Reporting class
obj_data_val_reporting = DataValidationsReporting()
# Creating object for EDW Generic Data Validation class
obj_edw_generic_data_validations = EdwGenericDataValidations()
# Creating object for EDW Aggregator Specific Data Validation class
obj_edw_agg_data_validations = EdwAggregatorSpecificDataValidations()
# Defining variable to store s3
s3 = boto3.resource("s3")
# Defining variable to store validation rules json local path
val_rules_json_local_path = os.path.dirname(os.path.realpath(__file__))

#############################
#     Class Functions       #
#############################

class DataValidationsMain:
    # Starting Order Data Validations script Main function
    def order_data_validation_main(self, app_config, schema_val_rules, generic_val_rules, agg_val_rules):
        # Reading config (job parameter data) from json when running locally
        if not app_config:
            with open(val_rules_json_local_path + '/Json' + '/configData.json') as f:
                config_json = json.load(f)
            app_config = obj_read_data.read_staging_bucket(config_json)

        # Reading data from app_config and extracting all required information
        input_bucket_name = app_config['input_params'][0]['input_bucket_name']
        aggregator_name = app_config['input_params'][0]['aggregator_name']
        input_file_extension = app_config['input_params'][0]['input_file_extension']
        dir_path = app_config['input_params'][0]['input_directory']
        input_layer = app_config['input_params'][0]['input_layer']
        env = app_config['input_params'][0]['env']
        job_type = app_config['input_params'][0]['job_type']
        # Reading schema rules from json when running locally
        if not schema_val_rules:
            with open(val_rules_json_local_path + '/Json' + '/schema-validation-staging-layer.json') as f:
                schema_val_rules = json.load(f)
        # Reading generic rules from json when running locally
        if not generic_val_rules:
            if 'mapped_layer' in input_layer:
                with open(val_rules_json_local_path + '/Json' + '/generic-validation-rules-mapped-layer.json') as f:
                    generic_val_rules = json.load(f)
            else:
                with open(val_rules_json_local_path + '/Json' + '/generic-validation-rules-staging-layer.json') as f:
                    generic_val_rules = json.load(f)

        # Defining dictionaries to store data validation results
        generic_val_result_dict = {}
        agg_val_result_dict = {}

        # Get list of all parquet files present in the S3 Bucket
        file_list = obj_read_data.get_parquet_files_list(bucket_name=input_bucket_name, path=dir_path, file_extension=input_file_extension)

        # Read each parquet file and start applying data validations on each file
        for aggFile in file_list:
            extracted_data = obj_read_data.read_parquet_file(key=aggFile, bucket_name=input_bucket_name)
            print("\n-+-+-+-+-Invoking Data validations on file-+-+-+-+-" + aggFile)

        # Validation 1: Start Schema validations if input layer is staging layer
            if 'staging_layer' in input_layer:
                obj_schema_data_val.schema_data_validations(test_data=extracted_data,schema_val_json=schema_val_rules)

        # Validation 2: Start Generic data validations on the parquet file
            # Validation 2a: Check if any NA values are present in parquet file
            # obj_generic_data_val.na_check(test_data=extracted_data)
            # Validation 2b: Check generic data validation rules against parquet file using Cerberus
            generic_val_df = obj_generic_data_val.generic_data_validations(test_data=extracted_data, generic_val_json=generic_val_rules)
            generic_val_result_dict = obj_data_val_reporting.capture_data_validation_results(input_data_frame=generic_val_df, input_file=aggFile, input_dict=generic_val_result_dict)

            # Validation 2c: Check if any invalid ISBN's are present in parquet file having trailing zero's
            obj_generic_data_val.check_isbn_format(test_data=extracted_data)

        # Validation 3: Start Aggregator specific data validations on the parquet file
            agg_val_df = obj_aggregator_data_val.aggregator_data_validations(test_data=extracted_data, aggregator=aggregator_name,agg_val_json=agg_val_rules)
            agg_val_result_dict = obj_data_val_reporting.capture_data_validation_results(input_data_frame=agg_val_df, input_file=aggFile, input_dict=agg_val_result_dict)

        # Validation 4: Start EDW data validations on the redshift data
            conn = obj_edw_generic_data_validations.connect_to_redshift_db(input_bucket_name=input_bucket_name, env=env)
            obj_edw_generic_data_validations.edw_generic_data_val(conn=conn, aggregator=aggregator_name, job_type=job_type, env=env, edw_config_json=None)
            obj_edw_agg_data_validations.edw_agg_data_val(conn=conn, aggregator_name=aggregator_name, job_type=job_type, environment=env)

        # final_val_result_dict = {**generic_val_result_dict, **agg_val_result_dict}
        # file_processed = final_val_result_dict['File Processed']
        # passed_records = final_val_result_dict['Total Number of Passed Records']
        # failed_records = final_val_result_dict['Total Number of Failed Records']
        # print("\n Printing the values")
        # print(file_processed)
        # print(passed_records)
        # print(failed_records)
# Below code is just for executing the class locally
if __name__ == '__main__':
    DataValidationsMain.order_data_validation_main(self=None, app_config=None, schema_val_rules=None, generic_val_rules=None, agg_val_rules=None)