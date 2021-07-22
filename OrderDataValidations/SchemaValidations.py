#####################
# Imports
####################

import logging as logger
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData
from OrderDataValidations.DataValidationsErrorHandling import DataValidationsErrorHandling
from cerberus import Validator

#############################
#      Global Variables     #
#############################
# Creating object for ReadStagingData class
obj_read_data = ReadStagingData()
# Creating object for Cerberus validator class
validator = Validator()
# Creating object for Cerberus Error Handler class
obj_error_handler = DataValidationsErrorHandling()

#############################
#     Class Functions       #
#############################
class SchemaValidations:
    def schema_data_validations(self, test_data, schema_val_json):
        logger.info("\n-+-+-+-+-Starting Generic Data Validations-+-+-+-+-")

        # Initializing the function with parquet file data and schema rules
        load_data = test_data
        schema_val_rules = schema_val_json["schema"]

        # Schema validation
        print("\n-+-+-+-+-Starting Schema validations on Data File-+-+-+-+")
        cerberus_schema_val_df = load_data.to_dict('records')
        schema_val_results = pd.DataFrame()
        for item in cerberus_schema_val_df:
            success = validator.validate(item, schema_val_rules)
            if (success):
                print("Schema validation is successful and no issues are found for this data row")
            else:
                error = validator.errors
                print("\n")
                print(item)
                # Calling Error Handler class with reported to check if it is a forbidden error or not
                obj_error_handler.glue_job_failure(error)

                # # Storing the failed values in a dataframe for Reporting purpose
                # failed_schema_val = pd.DataFrame.from_dict(item)
                # schema_val_results = schema_val_results.append(failed_schema_val)
                # schema_val_results['Validation Result'] = str(validator.errors)