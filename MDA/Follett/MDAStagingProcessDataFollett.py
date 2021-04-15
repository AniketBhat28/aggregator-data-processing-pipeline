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


class MDAStagingProcessDataFollett :

    # Function Description :	This function processes sales
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    #							amount_column - amount_column
    # Return Values : 			extracted_data - extracted staging data
    def process_sales(self, logger, extracted_data, amount_column) :

        logger.info('Computing Sales')

        extracted_data['total_sales_count'] = extracted_data.apply(
            lambda row : 1 if (row[amount_column] > 0 and row['units'] == "NA") else 0, axis=1)
        extracted_data['total_sales_count'] = extracted_data.apply(lambda row : 2 if (
                    (row[amount_column] > 0) and (row[amount_column] > row['price']) and (
                        row['units'] == "NA")) else row['total_sales_count'], axis=1)

        extracted_data['total_sales_count'] = extracted_data.apply(lambda row : 1 if (
                    (row[amount_column] == 0) and (row['price'] == 0) and (
                        row['sale_type'] in ['Sale', 'Create']) and (row['units'] == "NA")) else row[
            'total_sales_count'], axis=1)
        extracted_data['total_sales_count'] = extracted_data.apply(
            lambda row : row['units'] if ((row[amount_column] > 0) and (row['units'] != "NA")) else row[
                'total_sales_count'], axis=1)

        logger.info('Sales Values and Counts Computed')
        return extracted_data

    # Function Description :	This function processes returns
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    #							amount_column - amount_column
    # Return Values : 			extracted_data - extracted staging data
    def process_returns(self, logger, extracted_data, amount_column) :

        logger.info('Computing Returns')


        extracted_data['total_returns_count'] = extracted_data.apply(
            lambda row : 1.0 if (row[amount_column] < 0 and row['units'] == "NA") else 0, axis=1)
        extracted_data['total_returns_count'] = extracted_data.apply(lambda row : 2.0 if (
                    (row[amount_column] < 0) and (row[amount_column] * -1 > row['price'] * -1) and (
                        row['units'] == "NA")) else row['total_returns_count'], axis=1)

        extracted_data['total_returns_count'] = extracted_data.apply(lambda row : 1.0 if (
                    (row[amount_column] == 0) and (row['price'] == 0) and (
                        row['sale_type'] in ['Refund', 'Cancel']) and (row['units'] == "NA")) else row[
            'total_returns_count'], axis=1)
        extracted_data['total_returns_count'] = extracted_data.apply(
            lambda row : row['units'] if ((row[amount_column] < 0) and (row['units'] != "NA")) else row[
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
    def generate_edw_staging_data(self, logger, agg_rules, default_config,app_config, extracted_data) :

        year=app_config['output_params']['year']
        default_date = str(year) + '-01-01 00:00:00'
        #print('default_date',default_date)

        currency_suffix = '[\$£,()-]'
        extracted_data['price'] = (extracted_data['price']).replace(currency_suffix, '', regex=True)
        extracted_data['price'] = extracted_data['price'].astype('str')
        extracted_data['price'] = (extracted_data['price']).str.rstrip()
        extracted_data['price'] = pd.to_numeric(extracted_data['price'])

        extracted_data['payment_amount_currency'] = extracted_data.apply(
            lambda row : 'USD'  if row['payment_amount_currency'] == 'NA' else row['payment_amount_currency'],axis=1)
        extracted_data['price_currency'] = extracted_data['payment_amount_currency']
        extracted_data['payment_amount'] = pd.to_numeric(extracted_data['payment_amount'], errors='coerce')
        extracted_data['price'] = pd.to_numeric(extracted_data['price'], errors='coerce')



        current_date_format = agg_rules['date_formats']
        extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'reporting_date',
                                                       default_config)

        logger.info('Processing region of sale')
        extracted_data['country'] = extracted_data['payment_amount_currency'].map(
         agg_rules['filters']['country_iso_values']).fillna('NA')

        extracted_data['trans_type']='Sales'
        extracted_data['product_type'] = agg_rules['product_type']
        amount_column = agg_rules['filters']['amount_column']

        extracted_data = self.process_sales(logger, extracted_data, amount_column)
        extracted_data = self.process_returns(logger, extracted_data, amount_column)

        extracted_data['units'] = extracted_data.apply(
            lambda row : row['total_sales_count'] - row['total_returns_count'] if row['units'] == 'NA' else row['units'],
            axis=1)
        extracted_data['units'] = pd.to_numeric(extracted_data['units'],
                                                                      errors='coerce')
        #print('sale',extracted_data['sales_unit'])

        extracted_data['units'] = extracted_data['units'].astype('float')
        #print('sale 22', extracted_data['sales_unit'])
        extracted_data['units'] = extracted_data.apply(
            lambda row : 1.0 if row['units'] == 0.0 else row['units'],axis=1)

        if extracted_data['sale_type'].all() == 'NA' :
            extracted_data['sale_type'] = extracted_data.apply(
                lambda row : ('REFUNDS') if (row['units'] < 0) else ('PURCHASE'), axis=1)

        extracted_data['current_discount_percentage'] = extracted_data.apply(
            lambda row : 0 if row['current_discount_percentage'] == 'NA' and
                                                            row[amount_column] == 0 or
                                                            row['price'] == 0 else row['current_discount_percentage'],axis=1)

        extracted_data['current_discount_percentage'] = extracted_data.apply(
                lambda row : 1 - (round(((row[amount_column] / row[
                'units']) / abs(row['price'])), 2)) if row['current_discount_percentage'] == 'NA' else row['current_discount_percentage'],
                axis=1)
        extracted_data['current_discount_percentage'] = extracted_data['current_discount_percentage'].replace(np.nan, 0)
        extracted_data['current_discount_percentage'] = pd.to_numeric(extracted_data['current_discount_percentage'],
                                                                      errors='coerce')

        extracted_data['price'] = extracted_data['price'].abs()
        extracted_data['current_discount_percentage'] = extracted_data['current_discount_percentage'] * 100

        extracted_data['reporting_date'] = extracted_data.apply(
            lambda row : default_date if row['reporting_date'] == 'NA' else extracted_data['reporting_date'], axis=1)


        extracted_data['reporting_date'] = pd.to_datetime(extracted_data['reporting_date'],
                                                             format='%d-%m-%Y', infer_datetime_format=True)
        #print('reporting date done')
        extracted_data['reporting_date'] = extracted_data['reporting_date'].dt.date

        #print(extracted_data.dtypes)
        return extracted_data


    # Function Description :	This function processes data for all Follett files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config) :

        # For the final staging output
        agg_name = 'FOLLETT'
        final_edw_data = pd.DataFrame()

        input_list = list(app_config['input_params'])
        input_base_path = input_list[0]['input_base_path']

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Follett files Processing\n')
        agg_rules = next((item for item in rule_config if (item['name'] == agg_name)), None)
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3 :
            if each_file != '' and each_file.split('.')[-1] != 'txt' :
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                final_mapped_data = pd.read_parquet(input_base_path+each_file, engine='pyarrow')

                final_staging_data = self.generate_edw_staging_data(logger, agg_rules, default_config,app_config, final_mapped_data)
                # Append staging data of current file into final staging dataframe
                final_edw_data = pd.concat([final_edw_data, final_staging_data], ignore_index=True, sort=True)


        final_edw_data = obj_gen_attrs.group_data(logger, final_edw_data,
                                                     default_config[0]['group_staging_data'])
        final_edw_data.dropna(subset=["aggregator_name","reporting_date","external_purchase_order","external_transaction_number"], inplace=True)
        final_edw_data.to_csv('staging_Follett_Feb_2020.csv')
        obj_s3_connect.store_data_as_parquet(logger, app_config, final_edw_data)

        logger.info('\n+-+-+-+-+-+-+Finished Processing Follett files\n')