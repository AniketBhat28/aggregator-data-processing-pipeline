import logging as logger
import os
import json


#############################
#      Global Variables     #
#############################

BASE_PATH = os.path.dirname(os.path.realpath(__file__))

#############################
#       Class Functions     #
#############################

class GenericAggregatorValidations:

    # Function Description : This function validates ISBN format in give aggregator file
    # Input                : Final grouped data
    #                      : Aggregator Rules
    # Return values        : Dataframe of Invalid ISBN's

    def validate_isbn(self, test_data):

        logger.info("-------\nStarting ISBN Validations on the data file-----")
        print("\n-------Starting Generic Data Validations-----\n")
        load_data = test_data

        # Initialising with Aggregator Rules
        with open(BASE_PATH + '/AggregatorRules.json') as f:
            aggregator_rules_json = json.load(f)

        agg_list = aggregator_rules_json["aggregator_list"]
        print(agg_list)
        # Validation 1 : Find for any Null values for ISBN's in above dataframe

        null_Values = load_data[load_data.isnull().any(axis=1)]
        if null_Values.empty:
            print("\n-----There are no NULL values reported in the Data Frame--------------\n")
        else:
            print(null_Values, "\n-----These are the Null values found in the Data Frame---\n")

        # Validation 4: Check Aggregator Name
        for i, row in load_data.iterrows():
            agg_name = row['aggregator_name']
            if (agg_name in agg_list):
                print("\n-----Aggregator Name is correctly present in the data sheet-------\n")
            else:
                print(f"{row['aggregator_name']}")

        # Validation 3: Find for any Invalid ISBN's in above dataframe
        isbn_agg_list = aggregator_rules_json["backup_isbn_agg_list"]
        print(isbn_agg_list)
        if (load_data['aggregator_name'][0] in isbn_agg_list):
            extracted_data = load_data[aggregator_rules_json["isbn_columns_with_backup_product_id"]]
            extracted_data = extracted_data.astype(str)
            if ((extracted_data['e_product_id'].str.len() == 13) & (extracted_data['p_product_id'].str.len() == 13)).all():
                print("File has valid ISBN's populated in columns: e_product_id and p_product_id")
            else:
                check_isbn = extracted_data['e_backup_product_id'].str.len() != 10
        else:
            extracted_data = load_data[aggregator_rules_json["isbn_columns_without_backup_product_id"]]
            extracted_data = extracted_data.astype(str)
            if ((extracted_data['e_product_id'].str.len() == 13) & (extracted_data['p_product_id'].str.len() == 13)).all():
                print("File has valid ISBN's populated in columns: e_product_id and p_product_id")
            else:
                check_isbn = extracted_data['p_backup_product_id'].str.len() != 10

        invalid_isbn = extracted_data.loc[check_isbn]
        print(invalid_isbn, "\n------Invalid ISBN's found are as above--------")
        return invalid_isbn