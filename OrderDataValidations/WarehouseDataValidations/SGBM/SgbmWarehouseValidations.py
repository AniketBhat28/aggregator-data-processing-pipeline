import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator

#############################
#      Global Variables     #
#############################

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/WarehouseDataValidations/SGBM/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class SgbmWarehouseValidations:
    def warehouse_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Sgbm warehouse specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Sgbm warehouse specific data validations-+-+-+-")
        load_data = input_data

        # Reading Sgbm warehouse specific rules from json when running locally
        if not agg_specific_rules:
            with open(BASE_PATH + '/SGBM-validation-rules.json') as f:
                sgbm_val_rule_json = json.load(f)
        else:
            sgbm_val_rule_json = agg_specific_rules
        # Initialising with Sgbm_val_rules with Sgbm_val_rule_json
        sgbm_val_rules = sgbm_val_rule_json["schema"]
        sgbm_val_rules: dict

        # Sgbm warehouse validations for input_data against Sgbm_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, sgbm_val_rules)
            if (success):
                print("SGBM aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                failed_sgbm_val = pd.DataFrame.from_dict(item, orient='index')
                sgbm_val_results = pd.DataFrame()
                sgbm_val_results = sgbm_val_results.append(failed_sgbm_val)
                sgbm_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed validation
        # final_sgbm_val_results = pd.concat([sgbm_val_results], ignore_index=True, sort=True)
        # return final_sgbm_val_results