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

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/AggregatorDataValidations/Ebsco/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class EbscoAggregatorValidations:
    # Function to run Ebsco specific aggregator validations against input data
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Ebsco aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Ebsco aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Reading Ebsco aggregator specific rules from json when running locally
        if not agg_specific_rules:
            with open(BASE_PATH + '/Ebsco-validation-rules.json') as f:
                ebsco_val_rule_json = json.load(f)
        else:
            ebsco_val_rule_json = agg_specific_rules
        # Initialising with ebsco_val_rules with ebsco_val_rule_json
        ebsco_val_rules = ebsco_val_rule_json["schema"]
        ebsco_val_rules: dict

        # Ebsco aggregator validations for input_data against ebsco_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, ebsco_val_rules)
            if (success):
                print("Ebsco aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                failed_ebsco_val = pd.DataFrame.from_dict(item, orient='index')
                ebsco_val_results = pd.DataFrame()
                ebsco_val_results = ebsco_val_results.append(failed_ebsco_val)
                ebsco_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed Ebsco Validation Rules
        # final_ebsco_val_results = pd.concat([ebsco_val_results], ignore_index=True, sort=True)
        # return final_ebsco_val_results