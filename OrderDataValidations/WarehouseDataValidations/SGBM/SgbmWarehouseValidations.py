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

class SgbmWarehouseValidations:
    def aggregator_data_validations(self,test_data):
        # Initialising the Dataframe with Aggregator Parquet File
        logger.info("\n\t------Starting SGBM Aggregator Data Validations------")
        load_data = test_data

        # Initialising the file with Aggregator Config Json
        with open(BASE_PATH + '/aggregator-specific-configData.json') as f:
            aggregator_config_json = json.load(f)

        # Initialising the file with Aggregator validation Rules Json
        with open(BASE_PATH1 + '/SGBM-validation-rules.json') as f:
            Sgbm_val_rule_json = json.load(f)
        Sgbm_val_rules = Sgbm_val_rule_json["schema"]

        Sgbm_val_rules: dict

        ############################################################
        #       Starting Data Validations
        ############################################################

        # Running validation on entire frame based on Aggregator Rules Json
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, Sgbm_val_rules)
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