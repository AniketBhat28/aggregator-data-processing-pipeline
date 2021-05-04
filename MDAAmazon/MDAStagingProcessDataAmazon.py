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

    # Class variables
    AGG_NAME = 'AMAZON'
    
    def split_return_sale_data(self, logger, extracted_data):
        """
        Split return and sale data separately and remove the original data 
        :param logger: For the logging output file.
        :param extracted_data: pr-processed_data
        :return: extracted dataframe
        """
        logger.info('*********** Split return and sale data *******************')

        return_sale_list = []
        filtered_data = extracted_data[
            (
                (extracted_data.sales_net_unit > 0) 
                & (extracted_data.returns_unit > 0)
            )]

        for _, row in filtered_data.to_dict('index').items():
            sales_net_unit = row['sales_net_unit']
            returns_unit = row['returns_unit']
            payment_amount = -(row['payment_amount'] / row['sales_net_unit']) * row['returns_unit']

            for puchase_type in ['sales', 'returns']:
                if puchase_type == 'sales':
                    row['units'] = sales_net_unit
                    row['returns_unit'] = 0
                else:
                    row['units'] = 0
                    row['returns_unit'] = -returns_unit
                    row['sales_net_unit'] = row['returns_unit']
                    row['payment_amount'] = payment_amount

                return_sale_list.append(row.copy())
        
        sales_process_data = pd.DataFrame(return_sale_list)
        sales_process_data['payment_amount'] = round(sales_process_data.payment_amount, 2)
        # Drop invalid rows
        extracted_data = extracted_data.drop(filtered_data.index)
        extracted_data = pd.concat([extracted_data, sales_process_data], ignore_index=True, sort=True)

        logger.info('*********** Processes return and sale data*******************')
        return extracted_data


    def generate_edw_staging_data(self, logger, agg_rules, default_config, app_config, extracted_data):
        """
        Generates staging data for Amazon files
        :param logger: For the logging output file.
        :param filename: Name of the file
        :param agg_rules: Rules json
        :param default_config: Default json
        :param extracted_data: pr-processed_data
        :return: extracted dataframe
        """
        logger.info('***********generate staging data started*******************')

        extracted_data = extracted_data.drop(extracted_data[(extracted_data["e_product_id"] == 'NA') &
                    (extracted_data["p_backup_product_id"] == 'NA') &
                    (extracted_data["p_product_id"] == 'NA') &
                    (extracted_data["external_product_id"] == 'NA') ].index)
        extracted_data = pd.DataFrame(extracted_data)

        extracted_data['p_backup_product_id'] = extracted_data.p_backup_product_id.str.split('.', expand=True)
        extracted_data['e_product_id'] = extracted_data.e_product_id.str.split('.', expand=True)
        extracted_data['p_product_id'] = extracted_data.p_product_id.str.split('.', expand=True)

        extracted_data['country'] = extracted_data.country.str.upper()

        extracted_data['old_discount_percentage'].fillna(0.0, inplace=True)
        extracted_data.loc[(extracted_data['old_discount_percentage'] == 'NA'), 'old_discount_percentage'] = '0.0'
        extracted_data['old_discount_percentage'] = round(extracted_data['old_discount_percentage'].astype('float'), 2)

        extracted_data['new_rental_duration'].fillna(0, inplace=True)
        extracted_data.loc[(extracted_data['new_rental_duration'] == 'NA'), 'new_rental_duration'] = '0'
        extracted_data['new_rental_duration'] = extracted_data['new_rental_duration'].astype('float').astype('int')
        
        extracted_data['old_rental_duration'].fillna(0, inplace=True)
        extracted_data.loc[(extracted_data['old_rental_duration'] == 'NA'), 'old_rental_duration'] = '0'
        extracted_data['old_rental_duration'] = extracted_data['old_rental_duration'].astype('float').astype('int')

        extracted_data['rental_duration'].fillna(0, inplace=True)
        extracted_data.loc[(extracted_data['rental_duration'] == 'NA'), 'rental_duration'] = '0'
        extracted_data['rental_duration'] = extracted_data['rental_duration'].astype('float').astype('int')

        extracted_data['units'].fillna(1, inplace=True)
        extracted_data.loc[(extracted_data['units'] == 'NA'), 'units'] = '1'
        extracted_data['units'] = extracted_data['units'].astype('float').astype('int')
        extracted_data.loc[(extracted_data['units'] == 0), 'units'] = 1

        extracted_data['sales_net_unit'].fillna(0, inplace=True)
        extracted_data.loc[(extracted_data['sales_net_unit'] == 'NA'), 'sales_net_unit'] = '0'
        extracted_data['sales_net_unit'] = extracted_data['sales_net_unit'].astype('float').astype('int')
        
        extracted_data['returns_unit'].fillna(0, inplace=True)
        extracted_data.loc[(extracted_data['returns_unit'] == 'NA'), 'returns_unit'] = '0'
        extracted_data['returns_unit'] = extracted_data['returns_unit'].astype('float').astype('int')

        extracted_data['price'] = extracted_data['price'].astype('str').str.rstrip()
        extracted_data['price'] = pd.to_numeric(extracted_data['price'], errors='coerce')
        extracted_data['price'] = round(extracted_data['price'].abs(), 2)

        extracted_data.loc[(extracted_data['ex_rate'] == 'NA'), 'ex_rate'] = '1'
        extracted_data['ex_rate'] = extracted_data['ex_rate'].astype('float')

        extracted_data.loc[(extracted_data['publisher_price_ori'] == 'NA'), 'publisher_price_ori'] = '0.0'
        extracted_data['publisher_price_ori'] = round(extracted_data['publisher_price_ori'].astype('float'), 2)

        extracted_data.loc[(extracted_data['payment_amount_currency'] == 'NA'), 'payment_amount_currency'] = 'USD'

        extracted_data['payment_amount'] = pd.to_numeric(extracted_data['payment_amount'], errors='coerce')
        
        extracted_data.loc[(extracted_data['sale_type_ori'] == 'NA'), 'sale_type'] = 'Purchase'

        extracted_data['current_discount_percentage'].fillna(0.0, inplace=True)
        extracted_data['current_discount_percentage'] = extracted_data['current_discount_percentage'].replace('NA', '0')
        extracted_data.loc[(extracted_data['current_discount_percentage'] == 'NA'), 'current_discount_percentage'] = '0.0'
        extracted_data['current_discount_percentage'] = pd.to_numeric(extracted_data['current_discount_percentage'],
                                                                      errors='coerce')
        extracted_data['current_discount_percentage'] = extracted_data.apply(
            lambda row: row['current_discount_percentage'] * 100 if (row['current_discount_percentage'] < 1) else row[
                'current_discount_percentage'],
            axis=1)
        extracted_data['current_discount_percentage'] = round(extracted_data['current_discount_percentage'], 2)
        
        year = app_config['output_params']['year']
        default_date = '01-01-' + str(year)
        extracted_data['reporting_date'] = extracted_data['reporting_date'].replace('NA', default_date)
        extracted_data['reporting_date'] = pd.to_datetime(extracted_data['reporting_date'],
                                                          format='%d-%m-%Y', infer_datetime_format=True)

        extracted_data['reporting_date'] = extracted_data['reporting_date'].dt.date

        # Split staging data into returns and sales data.
        extracted_data = self.split_return_sale_data(logger, extracted_data)

        logger.info('****************generate staging data done**************')
        return extracted_data


    def initialise_processing(self, logger, app_config, rule_config, default_config):
        """
        To processes data for all Amazon files
        :param logger: For the logging output file.
        :param app_config: Input configuration
        :param rule_config: Rules json
        :param default_config: Default json		
        :return: None
        """
        final_edw_data = pd.DataFrame()

        input_list = list(app_config['input_params'])
        input_base_path = input_list[0]['input_base_path']

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Amazon files Processing\n')
        agg_rules = next((item for item in rule_config if (item['name'] == self.AGG_NAME)), None)
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

        obj_s3_connect.store_data_as_parquet(logger, app_config, final_edw_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing Amazon files\n')
