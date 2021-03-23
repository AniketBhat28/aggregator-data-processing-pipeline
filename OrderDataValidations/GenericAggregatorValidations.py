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

        logger.info("Running ISBN validations on Aggregator Data File")
        print("Running ISBN validations on Aggregator Data File")
        load_data = test_data
        print(load_data, type(load_data))
        print(load_data.columns)
        if load_data['aggregator_name'] in ['AMAZON','FOLLETT','EBSCO']:
            extracted_data = load_data[['e_product_id','p_product_id','e_backup_product_id','p_backup_product_id']]
        else:
            extracted_data = load_data[['e_product_id','p_product_id','p_backup_product_id']]
        # 'e_backup_product_id'
        # Validation 1 : Find for any Null values for ISBN's in above dataframe
        null_Values = extracted_data[extracted_data.isnull().any(axis=1)]

        # Converting all ISBN columns to String format
        extracted_data = extracted_data.astype(str)

        # # Validation 2: Check if ISBN is in 13 digit or 10 digit format
        if ((extracted_data['e_product_id'].str.len() == 13) & (extracted_data['p_product_id'].str.len() == 13)).all():
            print("File has valid ISBN's populated in columns: e_product_id and p_product_id")
        # (extracted_data['e_backup_product_id'].str.len() == 10)
        elif ((extracted_data['e_backup_product_id'].str.len() == 10) & (extracted_data['p_backup_product_id'].str.len() == 10)).all():
            print("File has valid ISBN's populated in Backup columns: e_backup_product_id and p_backup_product_id")
        else:
            check_isbn = (extracted_data['e_product_id'].str.len() != 15) & (extracted_data['p_product_id'].str.len() != 15)

        # check_isbn = ((extracted_data['e_product_id'].str.len() != 15) | (extracted_data['e_backup_product_id'].str.len() != 10)) & ((extracted_data['p_product_id'].str.len() != 15) | (extracted_data['p_backup_product_id'].str.len() != 10))
        invalid_isbn = extracted_data.loc[check_isbn]
        print(invalid_isbn, "Invalid ISBN's found are as follows")

        return invalid_isbn