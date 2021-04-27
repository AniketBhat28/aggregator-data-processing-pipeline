#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import awswrangler as wr
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


class MDAStagingProcessDataAmazon:

    # Function Description :	This function processes sales
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    #							amount_column - amount_column
    # Return Values : 			extracted_data - extracted staging data
    def process_sales(self, logger, extracted_data, amount_column):

        logger.info('Computing Sales')

        extracted_data['total_sales_count'] = extracted_data.apply(
            lambda row: 1 if (row[amount_column] > 0 and row['units'] == "NA") else 0, axis=1)
        extracted_data['total_sales_count'] = extracted_data.apply(lambda row: 2 if (
                (row[amount_column] > 0) and (row[amount_column] > row['price']) and (
                row['units'] == "NA")) else row['total_sales_count'], axis=1)

        extracted_data['total_sales_count'] = extracted_data.apply(lambda row: 1 if (
                (row[amount_column] == 0) and (row['price'] == 0) and (
                row['sale_type'] in ['Sale', 'Create']) and (row['units'] == "NA")) else row[
            'total_sales_count'], axis=1)
        extracted_data['total_sales_count'] = extracted_data.apply(
            lambda row: row['units'] if ((row[amount_column] > 0) and (row['units'] != "NA")) else row[
                'total_sales_count'], axis=1)

        logger.info('Sales Values and Counts Computed')
        return extracted_data

    # Function Description :	This function processes returns
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    #							amount_column - amount_column
    # Return Values : 			extracted_data - extracted staging data
    def process_returns(self, logger, extracted_data, amount_column):

        logger.info('Computing Returns')

        extracted_data['total_returns_count'] = extracted_data.apply(
            lambda row: 1 if (row[amount_column] < 0 and row['units'] == "NA") else 0, axis=1)
        extracted_data['total_returns_count'] = extracted_data.apply(lambda row: 2 if (
                (row[amount_column] < 0) and (row[amount_column] * -1 > row['price'] * -1) and (
                row['units'] == "NA")) else row['total_returns_count'], axis=1)

        extracted_data['total_returns_count'] = extracted_data.apply(lambda row: 1 if (
                (row[amount_column] == 0) and (row['price'] == 0) and (
                row['sale_type'] in ['Refund', 'Cancel']) and (row['units'] == "NA")) else row[
            'total_returns_count'], axis=1)
        extracted_data['total_returns_count'] = extracted_data.apply(
            lambda row: row['units'] if ((row[amount_column] < 0) and (row['units'] != "NA")) else row[
                'total_returns_count'], axis=1)

        print(extracted_data['total_returns_count'])
        extracted_data['total_returns_count'] = pd.to_numeric(extracted_data['total_returns_count'],
                                                              errors='coerce')
        extracted_data['total_returns_count'] = extracted_data['total_returns_count'].abs()

        logger.info('Return Values and Counts Computed')
        return extracted_data

    # Function Description :	This function generates staging data for Follett files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_edw_staging_data(self, logger, agg_rules, default_config, app_config, extracted_data):

        extracted_data['p_backup_product_id'] = extracted_data.p_backup_product_id.str.split('.', expand=True)
        extracted_data['e_product_id'] = extracted_data.e_product_id.str.split('.', expand=True)
        extracted_data['p_product_id'] = extracted_data.p_product_id.str.split('.', expand=True)
        extracted_data.loc[(extracted_data['old_discount_percentage'] == 'nan'), 'old_discount_percentage'] = 0.0
        extracted_data.loc[(extracted_data['current_discount_percentage'] == 'nan'), 'current_discount_percentage'] = 0.0
        extracted_data['old_discount_percentage'].fillna(0.0, inplace=True)
        extracted_data['current_discount_percentage'].fillna(0.0, inplace=True)

        extracted_data['new_rental_duration'].fillna(0, inplace=True)
        extracted_data['new_rental_duration'] = extracted_data.apply(
            lambda row: 0 if row['new_rental_duration'] == 'nan' else row['new_rental_duration'], axis=1)
        extracted_data['old_rental_duration'].fillna(0, inplace=True)
        extracted_data['old_rental_duration'] = extracted_data.apply(
            lambda row: 0 if row['old_rental_duration'] == 'nan' else row['old_rental_duration'], axis=1)
        extracted_data['rental_duration'].fillna(0, inplace=True)
        extracted_data['rental_duration'] = extracted_data.apply(
            lambda row: 0 if row['rental_duration'] == 'nan' else row['rental_duration'], axis=1)
        extracted_data['units'].fillna(1, inplace=True)
        extracted_data['units'] = extracted_data.apply(
            lambda row: 0 if row['units'] == 'nan' else row['units'], axis=1)
        extracted_data['sales_net_unit'] = extracted_data.apply(
            lambda row: 0 if row['sales_net_unit'] == 'nan' else row['sales_net_unit'], axis=1)

        extracted_data['sales_net_unit'].fillna(0, inplace=True)
        extracted_data['returns_unit'].fillna(0, inplace=True)
        extracted_data['returns_unit'] = extracted_data.apply(
            lambda row: 0 if row['returns_unit'] == 'nan' else row['returns_unit'], axis=1)

        extracted_data['new_rental_duration'] = extracted_data['new_rental_duration'].astype('float').astype('int')
        extracted_data['old_rental_duration'] = extracted_data['old_rental_duration'].astype('float').astype('int')
        extracted_data['rental_duration'] = extracted_data['rental_duration'].astype('float').astype('int')
        extracted_data['units'] = extracted_data['units'].astype('float').astype('int')
        extracted_data['sales_net_unit'] = extracted_data['sales_net_unit'].astype('float').astype('int')
        extracted_data['returns_unit'] = extracted_data['returns_unit'].astype('float').astype('int')

        extracted_data['price'] = extracted_data['price'].astype('str')
        extracted_data['price'] = (extracted_data['price']).str.rstrip()
        extracted_data['price'] = pd.to_numeric(extracted_data['price'], errors='coerce')

        extracted_data['payment_amount_currency'] = extracted_data.apply(
            lambda row: 'USD' if row['payment_amount_currency'] == 'NA' else row['payment_amount_currency'], axis=1)

        extracted_data['payment_amount'] = pd.to_numeric(extracted_data['payment_amount'], errors='coerce')
        extracted_data['units'] = extracted_data['units'].fillna(1)

        extracted_data['units'] = extracted_data.apply(
            lambda row: 1 if row['units'] == 0 else row['units'], axis=1)

        extracted_data['sale_type'] = extracted_data.apply(
            lambda row: 'PURCHASE' if row['sale_type_ori'] == 'NA' else row['sale_type_ori'], axis=1)

        extracted_data['current_discount_percentage'] = extracted_data.apply(
            lambda row: 0 if row['current_discount_percentage'] == 'NA' and
                             row['amount_column'] == 0 or
                             row['price'] == 0 else row['current_discount_percentage'], axis=1)

        extracted_data['current_discount_percentage'] = extracted_data['current_discount_percentage'].replace(np.nan, 0)
        extracted_data['current_discount_percentage'] = extracted_data.apply(
            lambda row: 0.0 if row['current_discount_percentage'] == 'NA' else row['current_discount_percentage'],
            axis=1)
        extracted_data['current_discount_percentage'] = pd.to_numeric(extracted_data['current_discount_percentage'],
                                                                      errors='coerce')
        extracted_data['current_discount_percentage'] = extracted_data.apply(
            lambda row: row['current_discount_percentage'] * 100 if (row['current_discount_percentage'] < 1) else row[
                'current_discount_percentage'],
            axis=1)
        extracted_data['price'] = extracted_data['price'].abs()
        extracted_data['current_discount_percentage'] = round(extracted_data['current_discount_percentage'], 2)
        extracted_data['reporting_date'] = pd.to_datetime(extracted_data['reporting_date'],
                                                          format='%d-%m-%Y', infer_datetime_format=True)

        extracted_data['reporting_date'] = extracted_data['reporting_date'].dt.date
        print('after3', extracted_data['reporting_date'])
        # print(extracted_data.dtypes)
        return extracted_data

    # Function Description :	This function processes data for all Follett files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config):

        # For the final staging output
        agg_name = 'AMAZON'
        final_edw_data = pd.DataFrame()

        input_list = list(app_config['input_params'])
        input_base_path = input_list[0]['input_base_path']

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Follett files Processing\n')
        agg_rules = next((item for item in rule_config if (item['name'] == agg_name)), None)
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3:
            if each_file != '':
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                final_mapped_data = wr.s3.read_parquet(path=input_base_path + each_file)
                final_staging_data = self.generate_edw_staging_data(logger, agg_rules, default_config, app_config,
                                                                    final_mapped_data)
                final_staging_data['product_type'] = each_file.split('/')[0].split('=')[1]
                final_staging_data['trans_type'] = each_file.split('/')[1].split('=')[1]
                final_edw_data = pd.concat([final_edw_data, final_staging_data], ignore_index=True, sort=True)

        final_edw_data = obj_gen_attrs.group_data(logger, final_edw_data,
                                                  default_config[0]['group_staging_data'])
        final_edw_data = final_edw_data[final_edw_data["e_product_id"].str.contains("NA") == False]
        obj_s3_connect.store_data_as_parquet(logger, app_config, final_edw_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing Follett files\n')
