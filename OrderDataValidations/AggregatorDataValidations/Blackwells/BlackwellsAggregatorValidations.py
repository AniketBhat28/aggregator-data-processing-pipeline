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

class BlackwellsAggregatorValidations:
    # Function to run Blackwells specific aggregator validations against input data
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Blackwells aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Blackwells aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Initialising with aggregator specific validation rules based on if it is running locally or through glue job
        if not agg_specific_rules:
            with open(agg_specific_rules_json_local_path + '/Blackwells-validation-rules.json') as f:
                blackwells_val_rule_json = json.load(f)
        else:
            blackwells_val_rule_json = agg_specific_rules
        # Initialising with blackwells_val_rules with blackwells_val_rule_json
        blackwells_val_rules = blackwells_val_rule_json["schema"]
        blackwells_val_rules: dict

        # Blackwells aggregator validations for input_data against blackwells_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, blackwells_val_rules)
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