#############################
#         Imports           #
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

class CheggAggregatorValidations:
    # Function to run Chegg specific aggregator validations against input data
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Chegg aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Chegg aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Initialising with aggregator specific validation rules based on if it is running locally or through glue job
        if not agg_specific_rules:
            with open(agg_specific_rules_json_local_path + '/Chegg-validation-rules.json') as f:
                chegg_val_rule_json = json.load(f)
        else:
            chegg_val_rule_json = agg_specific_rules
        # Initialising with chegg_val_rules with chegg_val_rule_json
        chegg_val_rules = chegg_val_rule_json["schema"]
        chegg_val_rules: dict

        # Chegg aggregator validations for input_data against chegg_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, chegg_val_rules)
            if (success):
                print("Chegg aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                # failed_chegg_val = pd.DataFrame.from_dict(item, orient='index')
                # chegg_val_results = pd.DataFrame()
                # chegg_val_results = chegg_val_results.append(failed_chegg_val)
                # chegg_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed Chegg Validation Rules
        # final_chegg_val_results = pd.concat([chegg_val_results], ignore_index=True, sort=True)
        # return final_chegg_val_results