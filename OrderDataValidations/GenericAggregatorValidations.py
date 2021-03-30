import logging as logger
import os
import json
import pandas as pd
from OrderDataValidations.ReadStagingData import ReadStagingData

#############################
#      Global Variables     #
#############################

BASE_PATH = os.path.dirname(os.path.realpath(__file__))

obj_read_data = ReadStagingData()
#############################
#       Class Functions     #
#############################

class GenericAggregatorValidations:

    # Function Description : This function validates ISBN format in give aggregator file
    # Input                : Final grouped data
    #                      : Aggregator Rules
    # Return values        : Dataframe of Invalid ISBN's

    def generic_data_validations(self, test_data):

        logger.info("\n\t------Starting ISBN Validations on the data file------")
        print("\n------Starting Generic Data Validations------")
        load_data = test_data

        # Initialising with Aggregator Rules
        with open(BASE_PATH + '/AggregatorRules.json') as f:
            aggregator_rules_json = json.load(f)

        agg_list = aggregator_rules_json["aggregator_list"]
        print("\n------Current list of Aggregators configured are------")
        print(agg_list)


        # Validation 1 : Find for any Null values for ISBN's in above dataframe

        null_Values = load_data[load_data.isnull().any(axis=1)]
        if null_Values.empty:
            print("\n-----There are no NULL values reported in the Data Frame--------------")
        else:
            # obj_read_data.store_data_validation_report(logger=logger, app_config=obj_read_data.navigate_staging_bucket(),final_data=null_Values)
            print(null_Values, "\n-----These are the Null values found in data file. Refer to Data validation Report")

        # Validation 2: Check Aggregator Name if correct in Data file

        for i, row in load_data.iterrows():
            agg_name = row['aggregator_name']
            if (agg_name in agg_list):
                valid_agg = load_data[aggregator_rules_json["aggregator_validation_columns"]]
                print("\n-----Aggregator Name is correctly present in the ROW-------\n")
            else:
                invalid_agg = load_data[aggregator_rules_json["aggregator_validation_columns"]]
                invalid_agg['Validation Result'] = "Failed Aggregator validation"
                print("Aggregator Name is not correctly present in the Data file. Refer to Data validation Report")


        # Validation 3: Check if ISBN's are correctly updated or not

        isbn_agg_list = aggregator_rules_json["backup_isbn_agg_list"]
        print("\n------For ISBN validation check if provided aggregator is part of below list------")
        print(isbn_agg_list)

        if (load_data['aggregator_name'][0] in isbn_agg_list):
            extracted_data = load_data[aggregator_rules_json["isbn_columns_with_backup_product_id"]]
            extracted_data = extracted_data.astype(str)
            if ((extracted_data['e_product_id'].str.len() == 13) & (extracted_data['p_product_id'].str.len() == 13)).all():
                print("File has valid ISBN's populated in cols: e_product_id and p_product_id")
            else:
                check_isbn = (extracted_data['e_product_id'].str.len() != 13) & (extracted_data['p_product_id'].str.len() != 13) & (extracted_data['e_backup_product_id'].str.len() != 10)
                print("\n------File has Invalid ISBN's populated in backup cols.Refer to Data Validation Report--------")
        else:
            extracted_data = load_data[aggregator_rules_json["isbn_columns_without_backup_product_id"]]
            extracted_data = extracted_data.astype(str)
            if ((extracted_data['e_product_id'].str.len() == 13) & (extracted_data['p_product_id'].str.len() == 13)).all():
                print("File has valid ISBN's populated in cols: e_product_id and p_product_id")
            else:
                check_isbn = (extracted_data['e_product_id'].str.len() != 13) & (extracted_data['p_product_id'].str.len() != 13) & (extracted_data['p_backup_product_id'].str.len() != 10)
                print("\n------File has Invalid ISBN's populated in cols.Refer to Data Validation Report--------")

        invalid_isbn = extracted_data.loc[check_isbn]
        invalid_isbn['Validation Result'] = "Failed ISBN validation"

        # Validation 3: Check if ISBN's are correctly updated or not

        # Creating Final Data frame which failed validation
        final_generic_val_results = pd.concat([invalid_agg, invalid_isbn], ignore_index=True, sort=True)

        # Store Generic validation Report in s3 bucket
        app_config = obj_read_data.navigate_staging_bucket()
        obj_read_data.store_data_validation_report(logger=logger,
                                                   app_config=app_config,
                                                   final_data=final_generic_val_results)


        return invalid_isbn