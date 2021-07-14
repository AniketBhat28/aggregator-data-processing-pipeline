#############################
#      Imports    #
#############################

import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator

#############################
#      Global Variables     #
#############################
# Defining variable to store aggregator validation rules json local path
agg_specific_rules_json_local_path = os.path.dirname(os.path.realpath(__file__))
# Creating object for ReadStagingData class
obj_read_data = ReadStagingData()
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

        # Amazon aggregator validations for input_data against amazon_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, amazon_val_rules)
            if (success):
                print("Amazon aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                failed_amazon_val = pd.DataFrame.from_dict(item, orient='index')
                amazon_val_results = pd.DataFrame()
                amazon_val_results = amazon_val_results.append(failed_amazon_val)
                amazon_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed Amazon Validation Rules
        # final_amazon_val_results = pd.concat([amazon_val_results], ignore_index=True, sort=True)
        # return final_amazon_val_results