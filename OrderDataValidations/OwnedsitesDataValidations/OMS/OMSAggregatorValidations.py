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

class OMSAggregatorValidations:
    def aggregator_data_validations(self,test_data):

        logger.info("\n\t------Starting OMS Aggregator Data Validations------")
        print("\n------Starting OMS Aggregator Data Validations------")
        load_data = test_data

        # Initialising the file with Aggregator Config Json
        with open(BASE_PATH + '/aggregator-specific-configData.json') as f:
            aggregator_config_json = json.load(f)
        agg_list = aggregator_config_json["aggregator_list"]

        # Initialising the file with OMS validation Rules Json
        with open(BASE_PATH1 + '/OMS-validation-rules.json') as f:
            OMS_val_rule_json = json.load(f)
        OMS_val_rules = OMS_val_rule_json["schema"]

        OMS_val_rules: dict

        ############################################################
        #       Starting Data Validations
        ############################################################

        # Running validation on entire frame based on OMS Rules Json
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True

        for item in cerberus_rule_val_df:
            success = validator.validate(item, OMS_val_rules)
            if (success):
                print("OMS aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                failed_oms_val = pd.DataFrame.from_dict(item, orient='index')
                oms_val_results = pd.DataFrame()
                oms_val_results = oms_val_results.append(failed_oms_val)
                oms_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed OMS validation rules
        # final_oms_val_results = pd.concat([oms_val_results], ignore_index=True, sort=True)
        # return final_oms_val_results