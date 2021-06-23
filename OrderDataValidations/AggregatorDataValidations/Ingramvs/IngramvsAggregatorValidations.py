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

class IngramvsAggregatorValidations:
    def aggregator_data_validations(self,test_data):

        logger.info("\n\t------Starting Ingramvs Aggregator Data Validations------")
        print("\n------Starting Ingramvs Aggregator Data Validations------")
        load_data = test_data

        # Initialising the file with Aggregator Config Json
        with open(BASE_PATH + '/aggregator-specific-configData.json') as f:
            aggregator_config_json = json.load(f)
        agg_list = aggregator_config_json["aggregator_list"]

        # Initialising the file with Ingramvs validation Rules Json
        with open(BASE_PATH1 + '/Ingramvs-validation-rules.json') as f:
            Ingramvs_val_rule_json = json.load(f)
        Ingramvs_val_rules = Ingramvs_val_rule_json["schema"]

        Ingramvs_val_rules: dict

        ############################################################
        #       Starting Data Validations
        ############################################################

        # Running validation on entire frame based on Ingramvs Validation Rules Json
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, Ingramvs_val_rules)
            if (success):
                print("Ingramvs aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                Ingramvs_follett_val = pd.DataFrame.from_dict(item, orient='index')
                Ingramvs_val_results = pd.DataFrame()
                Ingramvs_val_results = Ingramvs_val_results.append(Ingramvs_follett_val)
                Ingramvs_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed Ingramvs validation rules
        # final_Ingramvs_val_results = pd.concat([Ingramvs_val_results], ignore_index=True, sort=True)
        # return final_Ingramvs_val_results