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

import country_converter as coco

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


class MDAStagingProcessDataEbsco :

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, final_mapped_data) :

        logger.info("Processing transaction and sales types")

        final_mapped_data['trans_type'] = final_mapped_data.apply(
            lambda row : 'Subscription' if row['sale_type'] == 'Subscription' else 'Sales',axis=1)

        logger.info("Transaction and sales types processed")
        return final_mapped_data

    # Function Description :	This function generates staging data for Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data

    def generate_edw_staging_data(self,logger, agg_rules, default_config, app_config,final_mapped_data) :


        final_mapped_data = self.ebsco_price_cal(final_mapped_data)

        final_mapped_data = self.process_trans_type(logger, final_mapped_data)

        final_mapped_data = self.calculate_final_discount_percentage(final_mapped_data, logger)

        currency_suffix = '[\$£,()-]'

        final_mapped_data['publisher_price_ori'] = (final_mapped_data['publisher_price_ori']).replace(currency_suffix, '',
                                                                                            regex=True)
        final_mapped_data['publisher_price_ori'] = final_mapped_data['publisher_price_ori'].astype('str')
        final_mapped_data['publisher_price_ori'] = (final_mapped_data['publisher_price_ori']).str.rstrip()
        final_mapped_data['publisher_price_ori'] = pd.to_numeric(final_mapped_data['publisher_price_ori'])

        final_mapped_data['payment_amount'] = (final_mapped_data['payment_amount']).replace(currency_suffix, '',
                                                                                            regex=True)
        final_mapped_data['payment_amount'] = final_mapped_data['payment_amount'].astype('str')
        final_mapped_data['payment_amount'] = (final_mapped_data['payment_amount']).str.rstrip()
        final_mapped_data['payment_amount'] = pd.to_numeric(final_mapped_data['payment_amount'])

        final_mapped_data['price'] = (final_mapped_data['price']).replace(currency_suffix, '', regex=True)
        final_mapped_data['price'] = final_mapped_data['price'].astype('str')
        final_mapped_data['price'] = (final_mapped_data['price']).str.rstrip()
        final_mapped_data['price'] = pd.to_numeric(final_mapped_data['price'])

        final_mapped_data['units'] = final_mapped_data.apply(lambda row : 1 if row['units'] == 'NA' else row['units'],axis=1)


        final_mapped_data['units'] = final_mapped_data['units'].astype('float').astype('int')


        final_mapped_data['list_price_multiplier'] = final_mapped_data['list_price_multiplier'].astype('float')


        final_mapped_data['sale_type'] =  final_mapped_data['sale_type'].replace('NA', 'NA_Test', regex=True)
        #vectorization
        final_mapped_data.loc[(final_mapped_data['sale_type'] == 'NA_Test') &
                                                  ((final_mapped_data['units']<0) | (final_mapped_data['payment_amount']<0)),'sale_type']='Returns'
        final_mapped_data.loc[(final_mapped_data['sale_type'] == 'NA_Test') &
                              ((final_mapped_data['units'] > 0) | (
                                          final_mapped_data['payment_amount'] > 0)), 'sale_type'] = 'Purchase'

        # final_mapped_data['sale_type'] = final_mapped_data.apply(
        #     lambda row: 'Return' if (row['sale_type'] == 'NA_Test' and (row['units'] <0 or row['payment_amount'] <0) ) else 'Purchase', axis=1)

        final_mapped_data['country'] = final_mapped_data.apply(
            lambda row : ('US') if row['country'] == 'NA' else row['country'], axis=1)

        final_mapped_data['payment_amount_currency'] = final_mapped_data.apply(
            lambda row : ('USD') if row['payment_amount_currency'] == 'NA' else row['payment_amount_currency'], axis=1)

        final_mapped_data['price_currency'] = final_mapped_data.apply(
            lambda row : ('USD') if row['price_currency'] == 'NA' else row['price_currency'], axis=1)

        final_mapped_data['current_discount_percentage'] = (final_mapped_data['current_discount_percentage']).replace(
            '%', '', regex=True)
        final_mapped_data['current_discount_percentage'] = final_mapped_data['current_discount_percentage'].astype(
            'str')
        final_mapped_data['current_discount_percentage'] = (final_mapped_data['current_discount_percentage']).str.rstrip()
        final_mapped_data['current_discount_percentage'] = pd.to_numeric(final_mapped_data['current_discount_percentage'])

        final_mapped_data['current_discount_percentage'] = final_mapped_data.apply(
            lambda row : row['current_discount_percentage'] * 100 if (row['current_discount_percentage'] < 1) else row[
                'current_discount_percentage'],
            axis=1)

        final_mapped_data['units'] = final_mapped_data['units'].astype('float').astype('int')
        final_mapped_data['list_price_multiplier'] = final_mapped_data['list_price_multiplier'].astype('float')
        final_mapped_data['p_product_id'] = final_mapped_data.p_product_id.str.split('.', expand=True)
        final_mapped_data['e_product_id'] = final_mapped_data.e_product_id.str.split('.', expand=True)
        final_mapped_data['p_backup_product_id'] = final_mapped_data.p_product_id.str.split('.', expand=True)
        final_mapped_data['e_backup_product_id'] = final_mapped_data.e_product_id.str.split('.', expand=True)
        final_mapped_data['misc_product_ids'] = final_mapped_data.misc_product_ids.str.split('.', expand=True)
        final_mapped_data['price'] = final_mapped_data['price'].astype('float')
        final_mapped_data['publisher_price_ori'] = final_mapped_data['publisher_price_ori'].astype('float')
        final_mapped_data['payment_amount'] = final_mapped_data['payment_amount'].astype('float')
        final_mapped_data['current_discount_percentage'] = final_mapped_data['current_discount_percentage'].astype('float')

        current_date_format = agg_rules['date_formats']

        final_mapped_data = obj_pre_process.process_dates(logger, final_mapped_data, current_date_format, 'reporting_date',
        default_config)

        final_mapped_data['reporting_date'] = pd.to_datetime(final_mapped_data['reporting_date'],
                                                             format='%d-%m-%Y', infer_datetime_format=True)

        final_mapped_data['reporting_date'] = final_mapped_data['reporting_date'].dt.date

        final_mapped_data['product_type'] = agg_rules['product_type']

        final_mapped_data = self.country_converter(final_mapped_data)

        return final_mapped_data




    # Function Description :	This function processes data for all Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config) :

        # For the final staging output

        final_edw_data = pd.DataFrame()

        input_list = list(app_config['input_params'])
        input_base_path = input_list[0]['input_base_path']
        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Ebsco files Processing\n')
        logger.info('Get the corresponding rules object for Ebsco')
        agg_rules = next((item for item in rule_config if (item['name'] == 'EBSCO')), None)
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)

        for each_file in files_in_s3 :
            if each_file != '' :
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')


                final_mapped_data = pd.read_parquet(input_base_path + each_file, engine='pyarrow')

                final_staging_data = self.generate_edw_staging_data(logger, agg_rules, default_config, app_config,
                                                                    final_mapped_data)
                # Append staging data of current file into final staging dataframe
                final_edw_data = pd.concat([final_edw_data, final_staging_data], ignore_index=True, sort=True)


        # Grouping and storing data
        final_edw_data = obj_gen_attrs.group_data(logger, final_edw_data,
                                                     default_config[0]['group_staging_data'])

        obj_s3_connect.store_data_as_parquet(logger, app_config, final_edw_data)

        logger.info('\n+-+-+-+-+-+-+Finished Processing Ebsco files\n')


    def ebsco_price_cal(self, extracted_data) :

        extracted_data['price'] = extracted_data.apply(
            lambda row : 0.0 if row['price'] == 'NA' else row['price'], axis=1)

        extracted_data['publisher_price_ori'] = extracted_data.apply(
            lambda row : 0.0 if row['publisher_price_ori'] == 'NA' else row['publisher_price_ori'], axis=1)

        extracted_data['price_type'] = 'Adjusted Retail Price'
        #extracted_data['price_currency'] = 'NA'

        extracted_data['list_price_multiplier'] = extracted_data.apply(
            lambda row : 1 if row['list_price_multiplier'] == 'NA' else row['list_price_multiplier'], axis=1)


        return extracted_data

    def country_converter(self, final_grouped_data) :

        cc = coco.CountryConverter()
        iso_names = cc.convert(names=final_grouped_data['country'].tolist(), to="ISO2", enforce_list=True)
        final_grouped_data['country'] = iso_names
        iso_new = []
        for i in range(len(final_grouped_data['country'])) :
            iso_new.append(final_grouped_data['country'][i][0])
        final_grouped_data['country'] = iso_new
        return final_grouped_data


    def calculate_final_discount_percentage(self, extracted_data, logger) :
        if (extracted_data['current_discount_percentage'].all() == 'NA') :
            extracted_data['current_discount_percentage'] = 0

        return extracted_data
