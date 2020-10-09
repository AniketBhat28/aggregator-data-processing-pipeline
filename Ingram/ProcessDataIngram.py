#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np

from ReadData import ReadData
from ProcessCore import ProcessCore
from PreProcess import PreProcess
from ConnectToS3 import ConnectToS3
from GenerateStagingAttributes import GenerateStagingAttributes

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


class ProcessDataIngram:

    # Function Description :	This function generates staging data for INGRAM(VS) files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, extracted_data):

        extracted_data['aggregator_name'] = agg_rules['name']
        extracted_data['product_type'] = agg_rules['product_type']
        extracted_data['pod'] = 'NA'
        extracted_data['disc_code'] = 'NA'
        extracted_data['e_backup_product_id'] = 'NA'
        extracted_data['vendor_code'] = 'NA'
        extracted_data['misc_product_ids'] = 'NA'

        if 'sale_type' not in extracted_data.columns.to_list():
            extracted_data['sale_type'] = extracted_data.apply(
                lambda row: 'SALES' if (pd.isna(row['duration'])) else 'RENTALS', axis=1)

        if 'rental_duration' not in extracted_data.columns.to_list():
            extracted_data['rental_duration'] = 0

        # Converting negative amounts to positives
        amount_column = agg_rules['filters']['amount_column']
        extracted_data[amount_column] = extracted_data[amount_column].abs()

        if agg_rules['filters']['convert_percentage'] == 'yes':
            logger.info('Converting percentages to decimals')
            extracted_data['disc_percentage'] = extracted_data['disc_percentage'] / 100

        logger.info('Computing net unit price')
        extracted_data['net_unit_price'] = round((extracted_data[amount_column] / extracted_data['net_units']), 2)
        extracted_data['net_unit_price'] = extracted_data['net_unit_price'].abs()
        logger.info('Net units price computed')

        logger.info('Computing Sales and Returns')
        extracted_data['total_sales_value'] = extracted_data.apply(
            lambda row: row[amount_column] if (row['net_units'] >= 0) else 0.0, axis=1)
        extracted_data['total_returns_value'] = extracted_data.apply(
            lambda row: row[amount_column] if (row['net_units'] < 0) else 0.0, axis=1)

        extracted_data['total_sales_count'] = extracted_data.apply(
            lambda row: row['net_units'] if (row['net_units'] >= 0) else 0, axis=1)
        extracted_data['total_returns_count'] = extracted_data.apply(
            lambda row: row['net_units'] if (row['net_units'] < 0) else 0, axis=1)
        extracted_data['total_returns_count'] = extracted_data['total_returns_count'].abs()
        logger.info('Sales and Return Values computed')

        return extracted_data

    # Function Description :	This function processes data for all INGRAM(VS) files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config):

        # For the final staging output
        final_staging_data = pd.DataFrame()
        input_list = list(ast.literal_eval(app_config['INPUT']['File_Data']))

        logger.info('\n+-+-+-+-+-+-+Starting INGRAM(VS) files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3:
            if each_file != '':

                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')
                try:
                    data = obj_read_data.load_data(logger, input_list, each_file)
                    if not data.empty:
                        logger.info('Get the corresponding rules object for INGRAM(VS)')
                        agg_rules = next((item for item in rule_config if (item['name'] == 'INGRAM(VS)')),
                                         None)
                        data = data.dropna(how='all')
                        data.columns = data.columns.str.strip()
                        data[agg_rules['filters']['mandatory_columns']] = data[
                            agg_rules['filters']['mandatory_columns']].fillna(value='NA')

                        extracted_data = obj_pre_process.extract_relevant_attributes(logger, data,
                                                                                     agg_rules['relevant_attributes'])
                        logger.info("Generating final Staging Output")
                        final_staging_data = self.generate_final_staging_output(logger, each_file, agg_rules,
                                                                                extracted_data, final_staging_data)
                except KeyError as err:
                    logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)

        # Grouping and storing data
        final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data, agg_rules['group_staging_data'])
        obj_s3_connect.store_data(logger, app_config, final_grouped_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing INGRAM(VS) files\n')

    def generate_final_staging_output(self, logger, each_file, agg_rules, extracted_data, final_staging_data):
        if extracted_data.dropna(how='all').empty:
            logger.info("This file is empty")
        else:

            logger.info('Processing amount column')
            amount_column = agg_rules['filters']['amount_column']
            extracted_data[amount_column] = (
                extracted_data[amount_column].replace('[,)]', '', regex=True).replace('[(]', '-',
                                                                                      regex=True))

            extracted_data = obj_pre_process.validate_columns(logger, extracted_data,
                                                              agg_rules['filters']['column_validations'])

            extracted_data['region_of_sale'] = extracted_data['region_of_sale'].map(
                agg_rules['filters']['country_iso_values'])

            logger.info("Generating Staging Output")
            extracted_data = self.generate_staging_output(logger, each_file, agg_rules, extracted_data)
            logger.info("Staging output generated for given file")

            # Append staging data of current file into final staging dataframe
            final_staging_data = pd.concat([final_staging_data, extracted_data], ignore_index=True,
                                           sort=True)
            return final_staging_data
