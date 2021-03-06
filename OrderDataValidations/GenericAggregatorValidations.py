#####################
# Imports
####################

import logging as logger
import os
import json
import pandas as pd
import re
import sys
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator
from collections import Counter

#############################
#      Global Variables     #
#############################

BASE_PATH = os.path.dirname(
    '/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/Json/')
obj_read_data = ReadStagingData()
validator = Validator()


#############################
#     Class Functions       #
#############################

class GenericAggregatorValidations:

    # Function Description : This function runs generic aggregator validation rules
    # Input                : Aggregator Data File
    # Return values        : Dataframe with Generic validation results

    def generic_data_validations(self, test_data):

        logger.info("\n\t------Starting Generic Data Validations------")

        # Initializing the file with Aggregator Dataframe
        load_data = test_data

        # Initialising the file with Aggregator Config Json
        with open(BASE_PATH + '/aggregator-specific-configData.json') as f:
            aggregator_config_json = json.load(f)
        agg_list = aggregator_config_json["aggregator_list"]

        # Initialising the file with Schema Json
        with open(BASE_PATH + '/schema-validation.json') as f:
            schema_val_json = json.load(f)
        schema_val_rules = schema_val_json["schema"]

        # Initialising the file with Generic validation Rules Json
        with open(BASE_PATH + '/generic-validation-rules.json') as f:
            generic_val_rules_json = json.load(f)
        generic_val_rules = generic_val_rules_json["schema"]

        schema_val: dict
        schema_agg_rule: dict


        ############################################################
        #       Starting Data Validations                          #
        ############################################################

        # Validation 1 : Find for any Null or Empty values in the entire Dataframe
        null_Values = load_data[load_data.isnull().any(axis=1)]
        if null_Values.empty:
            print("\n-----There are no NULL values reported in the Data Frame--------------")
        else:
            print(null_Values, "\n-----These are the Null values found in data file. Refer to Data validation Report")
            null_Values['Validation Result'] = "Failed Null check"

        # Validation 2: Running Schema validation on the dataframe
        cerberus_schema_val_df = load_data.to_dict('records')
        schema_val_results = pd.DataFrame()
        for item in cerberus_schema_val_df:
            success = validator.validate(item, schema_val_rules)
            if (success):
                print("Schema validation is successful and no issues are found for this data row")
            else:
                print(validator.errors)
                # Storing the failed values in a dataframe for Reporting purpose
                # failed_schema_val = pd.DataFrame.from_dict(item)
                # schema_val_results = schema_val_results.append(failed_schema_val)
                # schema_val_results['Validation Result'] = str(validator.errors)

        # # Validation 3: Running generic validations on the dataframe
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        generic_val_results = pd.DataFrame()
        for item in cerberus_rule_val_df:
            success = validator.validate(item, generic_val_rules)
            if(success):
                print("Generic validation rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")
                # Storing the failed values in a dataframe for Reporting purpose
                # failed_generic_val = pd.DataFrame.from_dict(item)
                # generic_val_results = generic_val_results.append(failed_generic_val)
                # generic_val_results['Validation Result'] = str(validator.errors)

        # Validation 4: Added a validation to check for Invalid ISBN's having trailing Zero's as part of POF-6917
        for item in load_data['p_product_id']:
            isbn_val = item
            isbn_last_four_digits = [isbn_val[len(isbn_val) - 3:], isbn_val[len(isbn_val) - 4:], isbn_val[len(isbn_val) - 5:]]
            invalid_last_four_digit = ['000', '0000', '00000']
            if set(isbn_last_four_digits) == set(invalid_last_four_digit):
                print("\n\nISBN format error. ISBN value is : " + isbn_val)
                invalid_isbn_format = load_data[load_data['p_product_id'] == item]
                print(invalid_isbn_format[['source_id']])

        # # Creating a final dataframe with failed order validation results
        # final_generic_val_results = pd.concat([schema_val_results, generic_val_results, invalid_isbn_format], ignore_index=True, sort=True)
        #
        # # Storing this final data to S3 location as part of Data validation Results Report
        # app_config = obj_read_data.navigate_staging_bucket()
        # obj_read_data.store_data_validation_report(logger=logger,
        #                                            app_config=app_config,
        #                                            final_data=final_generic_val_results)

        # return final_generic_val_results
