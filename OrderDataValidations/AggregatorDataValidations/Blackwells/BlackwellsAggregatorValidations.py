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

BASE_PATH  = os.path.dirname('/Users/aniketbhatt/Desktop/GitHub Repo/Order Insights/aggregator-data-processing-pipeline/OrderDataValidations/Json/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class BlackwellsAggregatorValidations:
    def aggregator_data_validations(self, test_data, agg_val_json):

        logger.info("\n\t------Starting Blackwells Aggregator Data Validations------")
        print("\n------Starting Blackwells Aggregator Data Validations------")
        load_data = test_data

        # Initialising the file with Aggregator Config Json
        # with open(BASE_PATH + '/aggregator-specific-configData.json') as f:
        #     aggregator_config_json = json.load(f)
        # agg_list = aggregator_config_json["aggregator_list"]

        # Initialising the file with Barnes Validation Rules Json
        if not agg_val_json:
            with open(BASE_PATH1 + '/Blackwells-validation-rules.json') as f:
                agg_val_json = json.load(f)

        Blackwells_val_rules = agg_val_json["schema"]
        Blackwells_val_rules: dict

        ############################################################
        #       Starting Data Validations
        ############################################################

        # Running validation on entire frame based on Blackwells validation rules
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, Blackwells_val_rules)
            if (success):
                print("Blackwells aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                failed_blackwells_val = pd.DataFrame.from_dict(item, orient='index')
                blackwells_val_results = pd.DataFrame()
                blackwells_val_results = blackwells_val_results.append(failed_blackwells_val)
                blackwells_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed validation Blackwells validation rules
        # final_blackwells_val_results = pd.concat([blackwells_val_results], ignore_index=True, sort=True)
        # return final_blackwells_val_results