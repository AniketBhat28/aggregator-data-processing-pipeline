import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator

#############################
#      Global Variables     #
#############################

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/OwnedsitesDataValidations/UBW/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class UBWOwnedSitesValidations:
    def ownedsites_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting UBW owned sites specific data validations-+-+-+-")
        print("\n-+-+-+-Starting UBW owned sites specific data validations-+-+-+-")
        load_data = input_data

        # Reading UBW owned sites rules from json when running locally
        if not agg_specific_rules:
            with open(BASE_PATH + '/UBW-validation-rules.json') as f:
                ubw_val_rule_json = json.load(f)
        else:
            ubw_val_rule_json = agg_specific_rules
        # Initialising with ubw_val_rules with ubw_val_rule_json
        ubw_val_rules = ubw_val_rule_json["schema"]
        ubw_val_rules: dict

        # UBW owned sites validations for input_data against ubw_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, ubw_val_rules)
            if (success):
                print("UBW aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                # failed_ubw_val = pd.DataFrame.from_dict(item, orient='index')
                # ubw_val_results = pd.DataFrame()
                # ubw_val_results = ubw_val_results.append(failed_ubw_val)
                # ubw_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed UBW validation Rules
        # final_ubw_val_results = pd.concat([ubw_val_results], ignore_index=True, sort=True)
        # return final_ubw_val_results