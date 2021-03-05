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


class ProcessDataIngram:

	# Function Description :	This function generates staging data for INGRAM files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#                           default_config - Default json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted - extracted staging data
	def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data):

		extracted_data['aggregator_name'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']
		extracted_data['pod'] = 'N'
		extracted_data['disc_code'] = 'NA'
		extracted_data['e_backup_product_id'] = 'NA'
		extracted_data['vendor_code'] = 'NA'
		extracted_data['misc_product_ids'] = 'NA'

		extracted_data['region_of_sale'] = extracted_data.apply(lambda row: (row['trans_curr']) if(row['region_of_sale'] == 'NA') else (row['region_of_sale']), axis=1)
		extracted_data['region_of_sale'] = extracted_data['region_of_sale'].map(agg_rules['filters']['country_iso_values']).fillna(extracted_data['region_of_sale'])
		
		current_date_format = agg_rules['date_formats']
		extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'transaction_date', default_config)
		
		extracted_data['sale_type'] = extracted_data.apply(lambda row: ('REFUNDS') if(row['net_units'] < 0) else ('PURCHASE'), axis=1)
		extracted_data['trans_type'] = extracted_data.apply(lambda row: ('RETURNS') if(row['net_units'] < 0) else ('SALE'), axis=1)
		if extracted_data['total_rental_duration'].all() != 'NA':
			extracted_data['trans_type'] = extracted_data.apply(lambda row: ('RENTAL') if(pd.notna(row['total_rental_duration'])) else (row['trans_type']), axis=1)
			extracted_data['total_rental_duration'] = extracted_data.apply(lambda row: 0 if (pd.isna(row['total_rental_duration'])) else row['total_rental_duration'], axis=1)
		else:
			extracted_data['total_rental_duration'] = 0

		amount_column = agg_rules['filters']['amount_column']
		
		logger.info('Computing Sales and Returns')
		extracted_data['total_sales_value'] = extracted_data.apply(
			lambda row: row[amount_column] if (row['net_units'] >= 0) else 0.0, axis=1)
		extracted_data['total_returns_value'] = extracted_data.apply(
			lambda row: row[amount_column] if (row['net_units'] < 0) else 0.0, axis=1)
		extracted_data['total_returns_value'] = extracted_data['total_returns_value'].abs()

		extracted_data['total_sales_count'] = extracted_data.apply(
			lambda row: row['net_units'] if (row['net_units'] >= 0) else 0, axis=1)
		extracted_data['total_returns_count'] = extracted_data.apply(
			lambda row: row['net_units'] if (row['net_units'] < 0) else 0, axis=1)
		extracted_data['total_returns_count'] = extracted_data['total_returns_count'].abs()
		logger.info('Sales and Return Values computed')

		if extracted_data['disc_percentage'].dtype == 'object':
			extracted_data['disc_percentage'] = pd.to_numeric(extracted_data['disc_percentage'].str.rstrip('%'), errors='coerce')
		extracted_data['disc_percentage'] = extracted_data['disc_percentage'].abs()
		extracted_data.loc[(extracted_data['disc_percentage'] == -0.0), 'disc_percentage'] = 0.0
		extracted_data['disc_percentage'] = extracted_data.apply(lambda row: (row['disc_percentage']/100) if(row['disc_percentage'] >= 1) else (row['disc_percentage']), axis=1)
		
		extracted_data['publisher_price'] = extracted_data['publisher_price'].abs()
		extracted_data = obj_gen_attrs.process_net_unit_prices(logger, extracted_data, amount_column)
		extracted_data['disc_percentage'] = extracted_data['disc_percentage']*100
		
		# new attributes addition
		extracted_data['source'] = "INGRAM EBook"
		extracted_data['source_id'] = filename.split('.')[0]
		extracted_data['sub_domain'] = 'NA'
		extracted_data['business_model'] = 'B2B'

		return extracted_data

	
	# Function Description :	This function processes data for all INGRAM files
	# Input Parameters : 		logger - For the logging output file.
	#							app_config - Configuration
	#							rule_config - Rules json
	#                           default_config - Default json
	# Return Values : 			None
	def initialise_processing(self, logger, app_config, rule_config, default_config):

		# For the final staging output
		agg_name = 'INGRAM'
		agg_reference = self
		final_staging_data = pd.DataFrame()
		input_list = list(app_config['input_params'])

		logger.info('\n+-+-+-+-+-+-+Starting INGRAM files Processing\n')
		files_in_s3 = obj_s3_connect.get_files(logger, input_list)
		for each_file in files_in_s3:
			if each_file != '':

				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')
				final_staging_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file, rule_config,
																		 default_config, final_staging_data,
																		 obj_read_data,
																		 obj_pre_process,agg_name, agg_reference)
		# future date issue resolution
		final_staging_data = obj_pre_process.process_default_transaction_date(logger, app_config,final_staging_data)

		# Grouping and storing data
		final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data, default_config[0]['group_staging_data'])
		obj_s3_connect.store_data(logger, app_config, final_grouped_data)
		logger.info('\n+-+-+-+-+-+-+Finished Processing INGRAM files\n')
