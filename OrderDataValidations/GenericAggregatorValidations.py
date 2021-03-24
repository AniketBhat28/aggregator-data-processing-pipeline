import logging as logger


#############################
#      Global Variables     #
#############################

# None

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
        print("\n-------Starting ISBN Validations on the data file-----\n")
        load_data = test_data

        # Validation 1 : Find for any Null values for ISBN's in above dataframe

        null_Values = load_data[load_data.isnull().any(axis=1)]
        if null_Values.empty:
            print("\n-----There are no NULL values reported in the Data Frame--------------\n")
        else:
            print(null_Values, "\n-----These are the Null values found in the Data Frame---\n")

        # Validation 2: Find for any Invalid ISBN's in above dataframe

        agg_list = ['CHEGG', 'FOLLETT', 'EBSCO']

        if (load_data['aggregator_name'][0] in agg_list):
            extracted_data = load_data[['e_product_id', 'p_product_id', 'e_backup_product_id', 'p_backup_product_id']]
            extracted_data = extracted_data.astype(str)
            if ((extracted_data['e_product_id'].str.len() == 13) & (extracted_data['p_product_id'].str.len() == 13)).all():
                print("File has valid ISBN's populated in columns: e_product_id and p_product_id")
            else:
                check_isbn = ((extracted_data['e_backup_product_id'].str.len() != 15) | (extracted_data['p_backup_product_id'].str.len() != 10))
        else:
            extracted_data = load_data[['e_product_id', 'p_product_id', 'p_backup_product_id']]
            extracted_data = extracted_data.astype(str)
            if ((extracted_data['e_product_id'].str.len() == 13) & (extracted_data['p_product_id'].str.len() == 13)).all():
                print("File has valid ISBN's populated in columns: e_product_id and p_product_id")
            else:
                check_isbn = (extracted_data['p_backup_product_id'].str.len() != 10)

        invalid_isbn = extracted_data.loc[check_isbn]
        print(invalid_isbn, "\n------Invalid ISBN's found are as above--------")
        return invalid_isbn





