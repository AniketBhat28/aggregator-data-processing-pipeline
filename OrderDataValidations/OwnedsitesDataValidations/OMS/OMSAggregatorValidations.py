import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator

#############################
#      Global Variables     #
#############################

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/OwnedsitesDataValidations/OMS/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class OMSAOwnedSitesValidations:
    def ownedsites_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting OMS owned sites specific data validations-+-+-+-")
        print("\n-+-+-+-Starting OMS owned sites specific data validations-+-+-+-")
        load_data = input_data

        # Reading OMS owned sites rules from json when running locally
        if not agg_specific_rules:
            with open(BASE_PATH + '/OMS-validation-rules.json') as f:
                oms_val_rule_json = json.load(f)
        else:
            oms_val_rule_json = agg_specific_rules
        # Initialising with oms_val_rules with oms_val_rule_json
        oms_val_rules = oms_val_rule_json["schema"]
        oms_val_rules: dict

        # OMS owned sites validations for input_data against oms_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, oms_val_rules)
            if (success):
                print("OMS aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                # failed_oms_val = pd.DataFrame.from_dict(item, orient='index')
                # oms_val_results = pd.DataFrame()
                # oms_val_results = oms_val_results.append(failed_oms_val)
                # oms_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed OMS validation rules
        # final_oms_val_results = pd.concat([oms_val_results], ignore_index=True, sort=True)
        # return final_oms_val_results