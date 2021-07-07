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

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/AggregatorDataValidations/Barnes/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class BarnesAggregatorValidations:
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Barnes aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Barnes aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Reading Barnes aggregator specific rules from json when running locally
        if not agg_specific_rules:
            with open(BASE_PATH + '/Barnes-validation-rules.json') as f:
                barnes_val_rule_json = json.load(f)
        else:
            barnes_val_rule_json = agg_specific_rules

        # Initialising with barnes_val_rules with barnes_val_rule_json
        barnes_val_rules = barnes_val_rule_json["schema"]
        barnes_val_rules: dict

        # Barnes aggregator validations for input_data against barnes_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, barnes_val_rules)
            if (success):
                print("Barnes aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                # failed_barnes_val = pd.DataFrame.from_dict(item, orient='index')
                # barnes_val_results = pd.DataFrame()
                # barnes_val_results = barnes_val_results.append(failed_barnes_val)
                # barnes_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed validation Barnes validation rules
        # final_barnes_val_results = pd.concat([barnes_val_results], ignore_index=True, sort=True)
        # return final_barnes_val_results