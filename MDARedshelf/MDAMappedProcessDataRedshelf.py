#################################
#           IMPORTS             #
#################################


import ast
import pandas as pd
from pandas import ExcelFile, concat, DataFrame
import numpy as np

from ReadWriteData.ReadData import ReadData
from Preprocessing.ProcessCore import ProcessCore
from Preprocessing.PreProcess import PreProcess
from ReadWriteData.ConnectToS3 import ConnectToS3
from AttributeGenerators.GenerateStagingAttributes import GenerateStagingAttributes

#################################
#       GLOBAL VARIABLES        #
#################################


obj_read_data = ReadData()
obj_process_core = ProcessCore()
obj_pre_process = PreProcess()
obj_s3_connect = ConnectToS3()
obj_gen_attrs = GenerateStagingAttributes()


#################################
#       CLASS FUNCTIONS         #
#################################


class MDAMappedProcessDataRedshelf :

    # Function Description :    This function processes transaction and sales types
    # Input Parameters :        logger - For the logging output file.
    #                           extracted_data - pr-processed_data
    # Return Values :           extracted_data - extracted staging data
    def process_trans_type(self, logger, extracted_data,amount_column) :

        logger.info("Processing transaction and sales types for Redshelf")
        extracted_data['sale_type'] = extracted_data.apply(
            lambda row : ('Return') if (row[amount_column] < 0) else ('Purchase'), axis=1)

        extracted_data['trans_type'] = extracted_data.apply(
            lambda row : ('Sales') if (row['new_rental_duration'] == 'LIFETIME' or row['new_rental_duration'] == 'LIMITED' or row[
                    'new_rental_duration'] == 'NA') else 'Rental', axis=1)

        logger.info("Transaction and sales types for Redshelf processed")
        return extracted_data

    # Function Description :    This function generates staging data for Reddshelf files
    # Input Parameters :        logger - For the logging output file.
    #                           filename - Name of the file
    #                           agg_rules - Rules json
    #                           default_config - Default json
    #                           extracted_data - pr-processed_data
    # Return Values :           extracted_data - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data,data) :

        extracted_data = extracted_data[extracted_data.e_product_id != 'NA']
        extracted_data = pd.DataFrame(extracted_data)

        extracted_data['aggregator_name'] = agg_rules['name']
        extracted_data['product_type'] = agg_rules['product_type']
        extracted_data['payment_amount_currency'] = 'USD'
        extracted_data['price_currency'] = 'USD'
        extracted_data['price_type'] = 'Retail'

        current_date_format = agg_rules['date_formats']
        extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'reporting_date',
                                                       default_config)


        extracted_data['country'] = extracted_data.apply(
            lambda row : (row['payment_amount_currency']) if (row['country'] == 'NA') else (row['country']), axis=1)
        extracted_data['country'] = extracted_data['country'].map(
            agg_rules['filters']['country_iso_values']).fillna(extracted_data['country'])

        amount_column = agg_rules['filters']['amount_column']
        extracted_data = self.process_trans_type(logger, extracted_data, amount_column)

        extracted_data['units'] = extracted_data.apply(
            lambda row : -1 if (row[amount_column] < 0) else 1, axis=1)
        extracted_data['trans_type_ori'] = extracted_data['new_rental_duration']


        extracted_data['new_rental_duration'] = extracted_data.apply(
            lambda row : (0) if (
                        row['new_rental_duration'] == 'LIFETIME' or row['new_rental_duration'] == 'LIMITED' or row[
                    'new_rental_duration'] == 'NA') else row['new_rental_duration'], axis=1)

        # new attributes addition
        extracted_data['source'] = "REDSHELF"
        # extracted_data['source_id'] = filename.split('.')[0]


        return extracted_data

    # Function Description :    This function processes data for all Ebsco files
    # Input Parameters :        logger - For the logging output file.
    #                           app_config - Configuration
    #                           rule_config - Rules json
    #                           default_config - Default json
    # Return Values :           None
    def initialise_processing(self, logger, app_config, rule_config, default_config) :

        # For the final staging output
        agg_name = 'REDSHELF'
        agg_reference = self
        final_staging_data = DataFrame()
        input_list = list(app_config['input_params'])

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Redshelf files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3 :
            if each_file != '' :
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                input_file_extn = each_file.split('.')[-1]
                if (input_file_extn.lower() == 'xlsx') or (input_file_extn.lower() == 'xls') :

                    # To get the sheet names
                    excel_frame = ExcelFile(input_list[0]['input_base_path'] + each_file)
                    sheets = excel_frame.sheet_names
                    logger.info('\n+-+-+-sheets name+-+-+-+')
                    logger.info(sheets)
                    logger.info('\n+-+-+-+-+-+-+')
                    for each_sheet in sheets :
                        logger.info('Processing sheet: %s', each_sheet)
                        input_list[0]['input_sheet_name'] = each_sheet
                        final_staging_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file,
                                                                                     rule_config,
                                                                                     default_config, final_staging_data,
                                                                                     obj_read_data,
                                                                                     obj_pre_process, agg_name,
                                                                                     agg_reference)



                else :
                    final_staging_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file,
                                                                                 rule_config,
                                                                                 default_config, final_staging_data,
                                                                                 obj_read_data,
                                                                                 obj_pre_process, agg_name,
                                                                                 agg_reference)

        # future date issue resolution
       # final_staging_data = obj_pre_process.process_default_transaction_date(logger, app_config, final_staging_data)

        # Grouping and storing data

        final_mapped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                      default_config[0]['group_staging_data'])
        final_mapped_data = final_mapped_data.applymap(str)

        obj_s3_connect.store_data_as_parquet(logger, app_config, final_mapped_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing Redshelf files\n')


