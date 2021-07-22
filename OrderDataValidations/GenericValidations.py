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
# Creating object for Data validations Error Handler class
obj_error_handler = DataValidationsErrorHandling()
#############################
#     Class Functions       #
#############################

class GenericValidations:
    # Function to check if there are any Null values present in the data file
    def null_check(self, test_data):
        logger.info("\n-+-+-+-+-Starting Null check on the data file-+-+-+-+-")
        load_data = test_data
        null_Values = load_data[load_data.isnull().any(axis=1)]
        if null_Values.empty:
            print("\n-+-+-+-+-There are no NULL values found in the data file-+-+-+-+-")
        else:
            print(null_Values,"\n-+-+-+-These are Null values found in data file. Refer to Data validation Report-+-+-+-")
            null_Values['Validation Result'] = "Failed Null check"

    # Function to check data file against generic validation rules json using Cerberus
    def generic_data_validations(self, test_data, generic_val_json):
        logger.info("\n-+-+-+-+-Starting Generic Data Validations-+-+-+-+-")
        # Initializing the function with parquet file data and generic validation rules
        load_data = test_data
        generic_val_rules = generic_val_json["schema"]

        # Generic data validation
        logger.info("\n-+-+-+-+-Starting Generic validations on Data File-+-+-+-+-")
        cerberus_rule_val_df = load_data.to_dict('records')
        validator.allow_unknown = True
        generic_val_results = pd.DataFrame()
        for item in cerberus_rule_val_df:
            success = validator.validate(item, generic_val_rules)
            if(success):
                print("Generic validation rules are checked and no issues are found for this data row")
            else:
                print(item)
                print("\n")
                error = validator.errors
                # Calling Error Handler class with reported to check if it is a forbidden error or not
                obj_error_handler.glue_job_failure(error)

                # # Storing the failed values in a dataframe for Reporting purpose
                # failed_generic_val = pd.DataFrame.from_dict(item)
                # generic_val_results = generic_val_results.append(failed_generic_val)
                # generic_val_results['Validation Result'] = str(validator.errors)

    # Function to check ISBN format for any trailing zero's
    def check_isbn_format(self, test_data):
        logger.info("\n-+-+-+-+-Starting ISBN check for trailing zero's-+-+-+-+-")
        load_data = test_data
        agg_name = load_data['source'][0]
        agg_list = ['AMAZON', 'BARNES', 'CHEGG', 'EBSCO', 'FOLLETT', 'PROQUEST', 'GARDNERS', 'REDSHELF', 'BLACKWELLS', 'INGRAMVS']
        if agg_name in agg_list:
            for item in load_data['e_product_id']:
                isbn_val = item
                isbn_last_four_digits = [isbn_val[len(isbn_val) - 3:], isbn_val[len(isbn_val) - 4:], isbn_val[len(isbn_val) - 5:]]
                invalid_last_four_digit = ['000', '0000', '00000']
                if set(isbn_last_four_digits) == set(invalid_last_four_digit):
                    print("\n\nISBN format error. ISBN value is : " + isbn_val)
                    # invalid_isbn_format = load_data[load_data['e_product_id'] == item]
                    # print(invalid_isbn_format[['e_product_id']])
                else:
                    isbn_val = item
                    # print("ISBN does not have any trailing zero's and ISBN check is successful for this data row")
        else:
            for item in load_data['p_product_id']:
                isbn_val = item
                isbn_last_four_digits = [isbn_val[len(isbn_val) - 3:], isbn_val[len(isbn_val) - 4:], isbn_val[len(isbn_val) - 5:]]
                invalid_last_four_digit = ['000', '0000', '00000']
                if set(isbn_last_four_digits) == set(invalid_last_four_digit):
                    print("\n\nISBN format error. ISBN value is : " + isbn_val)
                    # invalid_isbn_format = load_data[load_data['p_product_id'] == item]
                    # print(invalid_isbn_format)
                    # print(invalid_isbn_format[['p_product_id']])
                    # print(item)
                else:
                    print("ISBN does not have any trailing zero's and ISBN check is successful for this data row")

        ############## Move Reporting to a separate class #######################
        # # Creating a final dataframe with failed order validation results
        # final_generic_val_results = pd.concat([schema_val_results, generic_val_results, invalid_isbn_format], ignore_index=True, sort=True)
        #
        # # Storing this final data to S3 location as part of Data validation Results Report
        # app_config = obj_read_data.navigate_staging_bucket()
        # obj_read_data.store_data_validation_report(logger=logger,
        #                                            app_config=app_config,
        #                                            final_data=final_generic_val_results)

        # return final_generic_val_results
