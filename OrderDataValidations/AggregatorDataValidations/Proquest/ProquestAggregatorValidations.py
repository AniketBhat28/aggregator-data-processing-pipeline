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

class ProquestAggregatorValidations:
    # Function to run Proquest specific aggregator validations against input data
    def aggregator_specific_validations(self, input_data, agg_specific_rules):
        logger.info("\n\t-+-+-+-Starting Proquest aggregator specific data validations-+-+-+-")
        print("\n-+-+-+-Starting Proquest aggregator specific data validations-+-+-+-")
        load_data = input_data

        # Initialising with aggregator specific validation rules based on if it is running locally or through glue job
        if not agg_specific_rules:
            with open(agg_specific_rules_json_local_path + '/Proquest-validation-rules.json') as f:
                proquest_val_rule_json = json.load(f)
        else:
            proquest_val_rule_json = agg_specific_rules
        # Initialising with proquest_val_rules with proquest_val_rule_json
        proquest_val_rules = proquest_val_rule_json["schema"]
        proquest_val_rules: dict

        # Proquest aggregator validations for input_data against proquest_val_rules using cerberus
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        for item in cerberus_rule_val_df:
            success = validator.validate(item, proquest_val_rules)
            if (success):
                print("Proquest aggregator specific rules are checked and no issues are found for this data row")
            else:
                print(validator.errors)
                print(item)
                print("\n")

                # Storing the failed values in a dataframe for Reporting purpose
                # failed_proquest_val = pd.DataFrame.from_dict(item, orient='index')
                # proquest_val_results = pd.DataFrame()
                # proquest_val_results = proquest_val_results.append(failed_proquest_val)
                # proquest_val_results['Validation Result'] = str(validator.errors)

        # # Creating Final Data frame which failed Proquest validation rules
        # final_proquest_val_results = pd.concat([proquest_val_results], ignore_index=True, sort=True)
        # return final_proquest_val_results