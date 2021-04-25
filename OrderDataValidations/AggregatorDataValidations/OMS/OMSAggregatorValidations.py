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
    def ebsco_agg_validations(self,test_data):
        # Initialising the Dataframe with Aggregator Parquet File
        logger.info("\n\t------Starting Ebsco Aggregator Data Validations------")
        load_data = test_data

        # Initialising the file with Aggregator Config Json
        with open(BASE_PATH + '/aggregator-specific-configData.json') as f:
            aggregator_config_json = json.load(f)
        agg_list = aggregator_config_json["aggregator_list"]

        # Initialising the file with Aggregator validation Rules Json
        with open(BASE_PATH1 + '/Ebsco-validation-rules.json') as f:
            Ebsco_val_rule_json = json.load(f)
        Ebsco_val_rules = Ebsco_val_rule_json["schema"]

        Ebsco_val_rules: dict

        ############################################################
        #       Starting Data Validations
        ############################################################

        # Running validation on entire frame based on Aggregator Rules Json
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, Ebsco_val_rules)
            if not success:
                print(item)
                print(validator.errors)
                print("\n")

        # # Creating Final Data frame which failed validation
        # final_ebsco_val_results = pd.concat([invalid_agg_name, invalid_isbn], ignore_index=True, sort=True)
        # return final_ebsco_val_results