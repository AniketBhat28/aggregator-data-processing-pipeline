import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator

#############################
#      Global Variables     #
#############################

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/AggregatorDataValidations/Gardners/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class GardnersAggregatorValidations:
    # Function to run Gardners specific aggregator validations against input data
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Gardners aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Gardners aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Reading Gardners aggregator specific rules from json when running locally
        if not agg_specific_rules:
            with open(BASE_PATH + '/Gardners-validation-rules.json') as f:
                gardners_val_rule_json = json.load(f)
        else:
            Gardners_val_rule_json = agg_specific_rules
        # Initialising with Gardners_val_rules with Gardners_val_rule_json
        gardners_val_rules = gardners_val_rule_json["schema"]
        gardners_val_rules: dict

        # Gardners aggregator validations for input_data against gardners_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, gardners_val_rules)
            if (success):
                print("Gardners aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")
                # Storing the failed values in a dataframe for Reporting purpose
                # failed_gardners_val = pd.DataFrame.from_dict(item, orient='index')
                # gardners_val_results = pd.DataFrame()
                # gardners_val_results = gardners_val_results.append(failed_gardners_val)
                # gardners_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed Gardners validation rules
        # final_gardners_val_results = pd.concat([gardners_val_results], ignore_index=True, sort=True)
        # return final_gardner_val_results