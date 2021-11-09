#############################
#      Imports    #
#############################

import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from OrderDataValidations.DataValidationsErrorHandling import DataValidationsErrorHandling
from cerberus import Validator

#############################
#      Global Variables     #
#############################
# Defining variable to store aggregator validation rules json local path
agg_specific_rules_json_local_path = os.path.dirname(os.path.realpath(__file__))
# Creating object for ReadStagingData class
obj_read_data = ReadStagingData()
# Creating object for Data validations Error Handler class
obj_error_handler = DataValidationsErrorHandling()
# Creating object for Cerberus validator class
validator = Validator()
#############################
#       Class Functions     #
#############################

class AmazonAggregatorValidations:
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        # Function to run Amazon specific aggregator validations against input data
        logger.info("\n\t-+-+-+-Starting Amazon aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Amazon aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Initialising with aggregator specific validation rules based on if it is running locally or through glue job
        if not agg_specific_rules:
            with open(agg_specific_rules_json_local_path + '/Amazon-validation-rules.json') as f:
                amazon_val_rule_json = json.load(f)
        else:
            amazon_val_rule_json = agg_specific_rules
        # Initialising with amazon_val_rules with amazon_val_rule_json
        amazon_val_rules = amazon_val_rule_json["schema"]
        amazon_val_rules: dict
        passed_data_val_df = pd.DataFrame()
        failed_data_val_df = pd.DataFrame()

        # Amazon aggregator validations for input_data against amazon_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, amazon_val_rules)
            if (success):
                item = {k: [v] for k, v in item.items()}
                data_record_df = pd.DataFrame(item)
                passed_data_val_df = pd.concat([passed_data_val_df, data_record_df], ignore_index=True)
                passed_data_val_df['Validation Result'] = "PASS"
            else:
                print(validator.errors)
                print(item)
                print("\n")
                item = {k: [v] for k, v in item.items()}
                data_record_df = pd.DataFrame(item)
                failed_data_val_df = pd.concat([failed_data_val_df, data_record_df], ignore_index=True)
                failed_data_val_df['Validation Result'] = "FAIL"
                error = validator.errors
                # Calling Error Handler class with reported to check if it is a forbidden error or not
                obj_error_handler.glue_job_failure(error)

        # # Storing the data validation results in a dataframe for Reporting purpose
        final_data_val_df = pd.concat([passed_data_val_df, failed_data_val_df])

        return final_data_val_df