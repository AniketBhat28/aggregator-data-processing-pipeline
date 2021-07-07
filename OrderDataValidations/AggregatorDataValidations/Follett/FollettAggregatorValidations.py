#############################
#        Imports            #
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

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/AggregatorDataValidations/Follett/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class FollettAggregatorValidations:
    # Function to run Follett specific aggregator validations against input data
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Follett aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Follett aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Reading Follett aggregator specific rules from json when running locally
        if not agg_specific_rules:
            with open(BASE_PATH + '/Follett-validation-rules.json') as f:
                follett_val_rule_json = json.load(f)
        else:
            follett_val_rule_json = agg_specific_rules
        # Initialising with Follett_val_rules with Follett_val_rule_json
        follett_val_rules = follett_val_rule_json["schema"]
        follett_val_rules: dict

        # Follett aggregator validations for input_data against Follett_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, follett_val_rules)
            if (success):
                print("Follett aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                # failed_follett_val = pd.DataFrame.from_dict(item, orient='index')
                # follett_val_results = pd.DataFrame()
                # follett_val_results = follett_val_results.append(failed_follett_val)
                # follett_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed Follett validation rules
        # final_follett_val_results = pd.concat([follett_val_results], ignore_index=True, sort=True)
        # return final_follett_val_results