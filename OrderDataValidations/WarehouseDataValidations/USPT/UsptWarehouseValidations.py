import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator

#############################
#      Global Variables     #
#############################
# Defining variable to store warehouse validation rules json local path
agg_specific_rules_json_local_path = os.path.dirname(os.path.realpath(__file__))
# Creating object for ReadStagingData class
obj_read_data = ReadStagingData()
# Creating object for Cerberus validator class
validator = Validator()
#############################
#       Class Functions     #
#############################
class UsptWarehouseValidations:
    def warehouse_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Uspt warehouse specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Uspt warehouse specific data validations-+-+-+-")
        load_data = input_data

        # Initialising with warehouse specific validation rules based on if it is running locally or through glue job
        if not agg_specific_rules:
            with open(agg_specific_rules_json_local_path + '/USPT-validation-rules.json') as f:
                uspt_val_rule_json = json.load(f)
        else:
            uspt_val_rule_json = agg_specific_rules
        # Initialising with uspt_val_rules with uspt_val_rule_json
        uspt_val_rules = uspt_val_rule_json["schema"]
        uspt_val_rules: dict

        # Austld warehouse validations for input_data against Uspt_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, uspt_val_rules)
            if (success):
                print("USPT aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                # failed_uspt_val = pd.DataFrame.from_dict(item, orient='index')
                # uspt_val_results = pd.DataFrame()
                # uspt_val_results = uspt_val_results.append(failed_uspt_val)
                # uspt_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed validation
        # final_uspt_val_results = pd.concat([uspt_val_results], ignore_index=True, sort=True)
        # return final_uspt_val_results