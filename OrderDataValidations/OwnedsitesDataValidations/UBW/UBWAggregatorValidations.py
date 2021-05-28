import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from cerberus import Validator

#############################
#      Global Variables     #
#############################

BASE_PATH  = os.path.dirname('/OrderDataValidations/Json/')
BASE_PATH1 = os.path.dirname(os.path.realpath(__file__))
obj_read_data = ReadStagingData()
validator = Validator()
#############################
#       Class Functions     #
#############################

class UBWAggregatorValidations:
    def aggregator_data_validations(self,test_data):
        # Initialising the Dataframe with Aggregator Parquet File
        logger.info("\n\t------Starting UBW Aggregator Data Validations------")
        print("\n------Starting UBW Aggregator Data Validations------")
        load_data = test_data

        # Initialising the file with Aggregator Config Json
        with open(BASE_PATH + '/aggregator-specific-configData.json') as f:
            aggregator_config_json = json.load(f)
        agg_list = aggregator_config_json["aggregator_list"]

        # Initialising the file with UBW validation Rules Json
        with open(BASE_PATH1 + '/UBW-validation-rules.json') as f:
            UBW_val_rule_json = json.load(f)
        UBW_val_rules = UBW_val_rule_json["schema"]

        UBW_val_rules: dict

        ############################################################
        #       Starting Data Validations
        ############################################################

        # Running validation on entire frame based on UBW Rules Json
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, UBW_val_rules)
            if (success):
                print("UBW aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                failed_ubw_val = pd.DataFrame.from_dict(item, orient='index')
                ubw_val_results = pd.DataFrame()
                ubw_val_results = ubw_val_results.append(failed_ubw_val)
                ubw_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed UBW validation Rules
        # final_ubw_val_results = pd.concat([ubw_val_results], ignore_index=True, sort=True)
        # return final_ubw_val_results