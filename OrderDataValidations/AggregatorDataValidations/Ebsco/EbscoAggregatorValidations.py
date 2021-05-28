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

class EbscoAggregatorValidations:
    def aggregator_data_validations(self,test_data):

        logger.info("\n\t------Starting Ebsco Aggregator Data Validations------")
        print("\n------Starting Ebsco Aggregator Data Validations------")
        load_data = test_data

        # Initialising the file with Aggregator Config Json
        with open(BASE_PATH + '/aggregator-specific-configData.json') as f:
            aggregator_config_json = json.load(f)
        agg_list = aggregator_config_json["aggregator_list"]

        # Initialising the file with Ebsco validation Rules Json
        with open(BASE_PATH1 + '/Ebsco-validation-rules.json') as f:
            Ebsco_val_rule_json = json.load(f)
        Ebsco_val_rules = Ebsco_val_rule_json["schema"]

        Ebsco_val_rules: dict

        ############################################################
        #       Starting Data Validations
        ############################################################

        # Running validation on entire frame based on Ebsco Validation Rules Json
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, Ebsco_val_rules)
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