#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np

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


class MDAStagingProcessDataFollett:

    def process_sale_type(self, logger, extracted_data):

        logger.info("Processing  sales types")
        extracted_data['sale_type'] = np.where(
            extracted_data['sale_type'].str.upper().isin(['SALES', 'SALE', 'SALE CORRECTION', 'PURCHASE', 'REFUND REVERSAL', 'CREATE']), 'Purchase', 'Return')

        logger.info("Transaction and sales types processed")
        return extracted_data


    # Function Description :	This function generates staging data for Follett files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_edw_staging_data(self, logger, agg_rules, default_config, app_config, extracted_data):

        logger.info('***********generate staging data started*******************')
        
        # Drop invalid rows
        extracted_data = extracted_data.drop(
            extracted_data[(
                    (
                        (extracted_data.e_product_id == 'NA')
                        & (extracted_data.e_backup_product_id == 'NA')
                    )
                    |
                    (extracted_data.e_product_id.str.contains("Total\w{0,}", case=False))
                    | (extracted_data.e_product_id.str.contains("Vend\w{0,}", case=False))
                )].index
            )

        if extracted_data.dropna(how='all').empty:
            logger.info("All records are invalid entries in this file")
            return extracted_data

        # Hard coded values
        extracted_data['product_type'] = agg_rules['product_type']
        extracted_data['trans_type'] = 'Sales'
        extracted_data['external_invoice_number'] = 'NA'
        extracted_data['internal_invoice_number'] = 'NA'
        extracted_data['internal_order_number'] = 'NA'
        extracted_data['billing_customer_id'] = 'NA'
        extracted_data['e_backup_product_id'] = extracted_data.e_backup_product_id.str.split('.', expand=True)
        extracted_data['e_product_id'] = extracted_data.e_product_id.str.split('.', expand=True)

        extracted_data.loc[(
            (extracted_data['payment_amount_currency'] == 'NA')
            | (extracted_data['payment_amount_currency'] == 'US')
            ), 'payment_amount_currency'] = 'USD'

        extracted_data['price_currency'] = extracted_data['payment_amount_currency']
        extracted_data['payment_amount'] = pd.to_numeric(extracted_data['payment_amount'], errors='coerce')

        currency_suffix = '[\$£,()-]'
        extracted_data['price'] = extracted_data['price'].replace(currency_suffix, '', regex=True)
        extracted_data['price'] = extracted_data['price'].str.rstrip()
        extracted_data['price'] = pd.to_numeric(extracted_data['price'], errors='coerce')
        extracted_data['price'] = extracted_data['price'].abs()

        logger.info('Processing region of sale')
        extracted_data['country'] = extracted_data['payment_amount_currency'].map(
            agg_rules['filters']['country_iso_values']).fillna('NA')
        
        extracted_data['units'] = extracted_data['units'].replace('NA', 1)
        extracted_data['units'] = extracted_data['units'].astype('float').astype('int')
        extracted_data.loc[(extracted_data['units'] == 0), 'units'] = 1

        extracted_data.loc[((extracted_data.sale_type_ori == 'NA') & (extracted_data.payment_amount > 0)), 'sale_type'] = 'PURCHASE'

        amount_column = agg_rules['filters']['amount_column']
        extracted_data['current_discount_percentage'] = extracted_data.apply(
            lambda row: 0 if row['current_discount_percentage'] == 'NA' and
                             row[amount_column] == 0 or
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
        extracted_data['current_discount_percentage'] = round(extracted_data['current_discount_percentage'], 2)

        # extracted_data.to_csv('df_before_date.csv')
        year = app_config['output_params']['year']
        default_date = '01-01-' + str(year)
        extracted_data['reporting_date'] = extracted_data['reporting_date'].replace('NA', default_date)

        current_date_format = agg_rules['date_formats']
        extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'reporting_date',
                                                       default_config)
        extracted_data['reporting_date'] = pd.to_datetime(extracted_data['reporting_date'],
                                                          format='%d-%m-%Y', infer_datetime_format=True)
        extracted_data['reporting_date'] = extracted_data['reporting_date'].dt.date

        self.process_sale_type(logger, extracted_data)
        
        logger.info('****************generate staging data done**************')
        return extracted_data

    # Function Description :	This function processes data for all Follett files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config):

        # For the final staging output
        agg_name = 'FOLLETT'
        final_edw_data = pd.DataFrame()

        input_list = list(app_config['input_params'])
        input_base_path = input_list[0]['input_base_path']

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Follett files Processing\n')
        agg_rules = next((item for item in rule_config if (item['name'] == agg_name)), None)
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3:
            if each_file != '' and each_file.split('.')[-1] != 'txt':
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                final_mapped_data = pd.read_parquet(input_base_path + each_file, engine='pyarrow')

                final_staging_data = self.generate_edw_staging_data(logger, agg_rules, default_config, app_config,
                                                                    final_mapped_data)
                # Append staging data of current file into final staging dataframe
                final_edw_data = pd.concat([final_edw_data, final_staging_data], ignore_index=True, sort=True)

        final_edw_data = obj_gen_attrs.group_data(logger, final_edw_data,
                                                  default_config[0]['group_staging_data'])
        # final_edw_data.dropna(subset=["e_product_id","e_backup_product_id","external_purchase_order","external_transaction_number"], inplace=True)


        obj_s3_connect.wrangle_data_as_parquet(logger, app_config, final_edw_data)

        logger.info('\n+-+-+-+-+-+-+Finished Processing Follett files\n')
