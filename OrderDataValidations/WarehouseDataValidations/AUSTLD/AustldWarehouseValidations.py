import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator

#############################
#      Global Variables     #
#############################

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/WarehouseDataValidations/AUSTLD/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class AustldWarehouseValidations:
    def warehouse_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Austld warehouse specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Austld warehouse specific data validations-+-+-+-")
        load_data = input_data

        # Reading Austld warehouse specific rules from json when running locally
        if not agg_specific_rules:
            with open(BASE_PATH + '/AUSTLD-validation-rules.json') as f:
                austld_val_rule_json = json.load(f)
        else:
            austld_val_rule_json = agg_specific_rules
        # Initialising with austld_val_rules with austld_val_rule_json
        austld_val_rules = austld_val_rule_json["schema"]
        austld_val_rules: dict

        # Austld warehouse validations for input_data against austld_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, austld_val_rules)
            if (success):
                print("AUSTLD aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                # failed_austld_val = pd.DataFrame.from_dict(item, orient='index')
                # austld_val_results = pd.DataFrame()
                # austld_val_results = austld_val_results.append(failed_austld_val)
                # austld_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed validation
        # final_austld_val_results = pd.concat([austld_val_results], ignore_index=True, sort=True)
        # return final_austld_val_results