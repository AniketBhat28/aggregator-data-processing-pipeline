#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import requests

from ReadWriteData.ReadData import ReadData
from Preprocessing.ProcessCore import ProcessCore
from Preprocessing.PreProcess import PreProcess
from ReadWriteData.ConnectToS3 import ConnectToS3
from AttributeGenerators.GenerateStagingAttributes import GenerateStagingAttributes

#################################
#		GLOBAL VARIABLES		#
#################################


obj_read_data = ReadData()
obj_process_core = ProcessCore()
obj_pre_process = PreProcess()
obj_s3_connect = ConnectToS3()
obj_gen_attrs = GenerateStagingAttributes()


#################################
#		CLASS FUNCTIONS			#
#################################


class MDAStagingProcessDataProquest:

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, final_mapped_data):

        logger.info("Processing transaction and sales types")

        final_mapped_data['trans_type'] = final_mapped_data.apply(
            lambda row: 'Subscription' if row['sale_type'] == 'Subscription' else 'Sales', axis=1)

        logger.info("Transaction and sales types processed")
        return final_mapped_data

    # Function Description :	This function generates staging data for Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data

    def generate_edw_staging_data(self, logger, final_mapped_data):
        final_mapped_data = final_mapped_data.replace('nan', 'NA')
        final_mapped_data = final_mapped_data.replace({np.nan: 'NA'})
        final_mapped_data.replace('None', 'NA', inplace=True)
        print("total no of rows before nan : ", final_mapped_data.shape[0])
        final_mapped_data = final_mapped_data[final_mapped_data['External_Purchase_Order'] != 'Total']
        print("total no of rows after nan : ", final_mapped_data.shape[0])

        final_mapped_data = final_mapped_data.replace(r'^\s*$', np.nan, regex=True)
        final_mapped_data['e_product_id'] = final_mapped_data['e_product_id'].apply(
            lambda x: x.replace('.0', ''))
        final_mapped_data['p_product_id'] = final_mapped_data['p_product_id'].apply(
            lambda x: x.replace('.0', ''))
        final_mapped_data['External_Product_ID'] = final_mapped_data['External_Product_ID'].apply(lambda x: x.replace('.0', ''))
        final_mapped_data = self.proquest_price_cal(final_mapped_data)
        final_mapped_data = self.calculate_final_discount_percentage(final_mapped_data, logger)
        # extracted_data = self.process_trans_type(logger, final_mapped_data)
        final_mapped_data['Payment_Amount'].replace('None', 0, inplace=True)
        final_mapped_data['Payment_Amount'] = final_mapped_data['Payment_Amount'].astype('float')
        final_mapped_data['Price'].replace('None', 0, inplace=True)
        final_mapped_data['Price'] = final_mapped_data['Price'].astype('float')

        final_mapped_data['list_price_multiplier'].replace('None', 0, inplace=True)
        final_mapped_data['list_price_multiplier'] = final_mapped_data['list_price_multiplier'].astype('float')

        final_mapped_data['adjusted_publisher_price_ori'].replace('None', 0, inplace=True)
        final_mapped_data['adjusted_publisher_price_ori'] = final_mapped_data['adjusted_publisher_price_ori'].astype(
            'float')

        final_mapped_data['publisher_price_ori'].replace('None', 0, inplace=True)
        final_mapped_data['publisher_price_ori'] = final_mapped_data['publisher_price_ori'].astype('float')

        final_mapped_data['units'] = final_mapped_data['units'].astype('float').astype('int')
        final_mapped_data['sale_type'] = final_mapped_data.apply(
            lambda row: 'REFUNDS' if (row['units'] < 0 or row['Payment_Amount'] < 0) else row['sale_type'], axis=1)
        final_mapped_data['country'] = final_mapped_data.apply(
            lambda row: ('USA') if row['country'] == 'NA' else row['country'], axis=1)
        final_mapped_data['Price_currency'] = final_mapped_data.apply(
            lambda row: ('USD') if row['Price_currency'] == 'NA' else row['Price_currency'], axis=1)
        final_mapped_data['Payment_Amount_Currency'] = final_mapped_data['Price_currency']
        final_mapped_data['reporting_date'] = pd.to_datetime(final_mapped_data['reporting_date'],
                                                             format='%d-%m-%Y', infer_datetime_format=True)
        final_mapped_data['reporting_date'] = final_mapped_data['reporting_date'].dt.date

        return final_mapped_data

    # Function Description :	This function processes data for all Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config):

        # For the final staging output

        final_edw_data = pd.DataFrame()

        input_list = list(app_config['input_params'])
        input_base_path = input_list[0]['input_base_path']
        # Processing for each file in the given folder
        logger.info('\n+-+-+-+-+-+-+Starting Proquest files Processing\n')
        logger.info('Get the corresponding rules object for Proquest')
        # agg_rules = next((item for item in rule_config if (item['name'] == 'PROQUEST')), None)
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)

        for each_file in files_in_s3:
            if each_file != '':
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                final_mapped_data = pd.read_parquet(input_base_path + each_file, engine='pyarrow')
                final_staging_data = self.generate_edw_staging_data(logger, final_mapped_data)
                final_staging_data['year'] = each_file.split('/')[0].split('=')[1]
                final_staging_data['product_type'] = each_file.split('/')[1].split('=')[1]
                final_staging_data['trans_type'] = each_file.split('/')[2].split('=')[1]
                # Append staging data of current file into final staging dataframe
                final_edw_data = pd.concat([final_edw_data, final_staging_data], ignore_index=True, sort=True)

        # Grouping and storing data
        # print("total no of rows before removing duplicate : ", len(final_edw_data))
        # final_edw_data.drop_duplicates(keep=False, inplace=True)
        # print("total no of rows after removing duplicate : ", len(final_edw_data))
        final_edw_data = obj_gen_attrs.group_data(logger, final_edw_data,
                                                  default_config[0]['group_staging_data'])
        obj_s3_connect.store_data_as_parquet(logger, app_config, final_edw_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing Proquest files\n')

    def proquest_price_cal(self, extracted_data):

        extracted_data['Price'] = extracted_data.apply(
            lambda row: 0.0 if row['Price'] == 'NA' else row['Price'], axis=1)

        extracted_data['Payment_Amount'] = extracted_data.apply(
            lambda row: 0.0 if row['Payment_Amount'] == 'NA' else row['Payment_Amount'], axis=1)

        extracted_data['adjusted_publisher_price_ori'].fillna(0, inplace=True)
        extracted_data['publisher_price_ori'].fillna(0, inplace=True)
        extracted_data['adjusted_publisher_price_ori'] = extracted_data.apply(
            lambda row: 0.0 if row['adjusted_publisher_price_ori'] == 'NA' else row['adjusted_publisher_price_ori'],
            axis=1)

        extracted_data['publisher_price_ori'] = extracted_data.apply(
            lambda row: 0.0 if row['publisher_price_ori'] == 'NA' else row['publisher_price_ori'],
            axis=1)

        extracted_data['list_price_multiplier'].fillna(0, inplace=True)
        extracted_data['list_price_multiplier'] = (extracted_data['list_price_multiplier']).replace(
            '%', '', regex=True)
        extracted_data['list_price_multiplier'] = extracted_data['list_price_multiplier'].astype(
            'str')
        extracted_data['list_price_multiplier'] = (extracted_data['list_price_multiplier']).str.rstrip()
        extracted_data['list_price_multiplier'] = extracted_data.apply(
            lambda row: 0.0 if row['list_price_multiplier'] == 'NA' else row['list_price_multiplier'],
            axis=1)

        return extracted_data

    def calculate_final_discount_percentage(self, extracted_data, logger):
        if extracted_data['current_discount_percentage'].all() == 'NA':
            extracted_data['current_discount_percentage'] = 0.0
        extracted_data['current_discount_percentage'] = extracted_data.apply(
            lambda row: 0.0 if row['current_discount_percentage'] == 'NA' else row['current_discount_percentage'],
            axis=1)
        extracted_data['current_discount_percentage'].replace('None', 0, inplace=True)
        extracted_data['current_discount_percentage'] = pd.to_numeric(extracted_data['current_discount_percentage'])
        extracted_data['current_discount_percentage'] = extracted_data.apply(
            lambda row: row['current_discount_percentage'] * 100 if (row['current_discount_percentage'] < 1) else row[
                'current_discount_percentage'],
            axis=1)
        return extracted_data
