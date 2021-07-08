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
# Defining variable to store aggregator validation rules json local path
agg_specific_rules_json_local_path = os.path.dirname(os.path.realpath(__file__))
# Creating object for ReadStagingData class
obj_read_data = ReadStagingData()
# Creating object for Cerberus validator class
validator = Validator()
#############################
#       Class Functions     #
#############################

class IngramvsAggregatorValidations:
    # Function to run Ingram specific aggregator validations against input data
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Ingram aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Ingram aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Initialising with aggregator specific validation rules based on if it is running locally or through glue job
        if not agg_specific_rules:
            with open(agg_specific_rules_json_local_path + '/Ingramvs-validation-rules.json') as f:
                ingramvs_val_rule_json = json.load(f)
        else:
            ingramvs_val_rule_json = agg_specific_rules
        # Initialising with ingramvs_val_rules with ingramvs_val_rule_json
        ingramvs_val_rules = ingramvs_val_rule_json["schema"]
        ingramvs_val_rules: dict

        # Ingram aggregator validations for input_data against ingramvs_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, ingramvs_val_rules)
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