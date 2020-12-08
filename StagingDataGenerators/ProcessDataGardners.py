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


class ProcessDataGardners:

	# Function Description :	This function processes transaction and sales types
	# Input Parameters : 		logger - For the logging output file.
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted_data - extracted staging data
	def process_trans_type(self, logger, extracted_data):

		logger.info("Processing transaction and sales types")
		extracted_data['sale_type'] = extracted_data.apply(lambda row: ('REFUNDS') if(row['sale_type'] == 'CREDIT') else ('PURCHASE'), axis=1)
		extracted_data['trans_type'] = extracted_data.apply(lambda row: ('RETURNS') if(row['net_units'] < 0) else ('SALE'), axis=1)
		extracted_data['trans_type'] = extracted_data.apply(lambda row: ('RENTAL') if(row['licence_type'] == 'RENTAL') else (row['trans_type']), axis=1)
		
		logger.info("Transaction and sales types processed")
		return extracted_data


	# Function Description :	This function generates staging data for Gardners files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							default_config - Default json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted_data - extracted staging data
	def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data):

		extracted_data['aggregator_name'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']

		extracted_data['e_backup_product_id'] = 'NA'
		extracted_data['p_product_id'] = 'NA'
		extracted_data['p_backup_product_id'] = 'NA'

		extracted_data['pod'] = 'NA'
		extracted_data['imprint'] = 'NA'
		extracted_data['disc_code'] = 'NA'
		extracted_data['misc_product_ids'] = 'NA'
		extracted_data['total_rental_duration'] = 0
		
		current_date_format = agg_rules['date_formats']
		extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'transaction_date', default_config)
		
		if agg_rules['filters']['convert_percentage'] == 'yes':
			logger.info('Converting percentages to decimals')
			extracted_data['disc_percentage'] = extracted_data['disc_percentage']/100
		extracted_data['disc_percentage'] = 1 - (round((extracted_data['discounted_price']/ extracted_data['publisher_price']), 2))
		extracted_data['disc_percentage'] = extracted_data['disc_percentage'].replace(np.nan, 0)
		
		amount_column = agg_rules['filters']['amount_column']
		extracted_data = self.process_trans_type(logger, extracted_data)
		extracted_data = obj_gen_attrs.process_net_unit_prices(logger, extracted_data, amount_column)
		
		logger.info('Computing Sales and Returns')
		extracted_data['total_sales_value'] = extracted_data.apply(lambda row:row[amount_column] if (row['net_units'] >= 0.0) else 0.0, axis=1)
		extracted_data['total_returns_value'] = extracted_data.apply(lambda row:row[amount_column] if (row['net_units'] < 0.0) else 0.0, axis=1)

		extracted_data['total_sales_count'] = extracted_data.apply(lambda row: row['net_units'] if (row['net_units'] >= 0) else 0, axis=1)
		extracted_data['total_returns_count'] = extracted_data.apply(lambda row: row['net_units'] if (row['net_units'] < 0) else 0, axis=1)
		logger.info('Sales and Return Values computed')

		logger.info('Converting negative amounts to positives')
		extracted_data['publisher_price'] = extracted_data['publisher_price'].abs()
		extracted_data['total_returns_value'] = extracted_data['total_returns_value'].abs()
		extracted_data['total_returns_count'] = extracted_data['total_returns_count'].abs()
		extracted_data['disc_percentage'] = extracted_data['disc_percentage']*100
		
		# new attributes addition
		extracted_data['source'] = "GARDNERS EBook"
		extracted_data['source_id'] = filename.split('.')[0]
		extracted_data['sub_domain'] = 'NA'

		return extracted_data

	# Function Description :	This function processes data for all Gardners files
	# Input Parameters : 		logger - For the logging output file.
	#							app_config - Configuration
	#							rule_config - Rules json
	#							default_config - Default json
	# Return Values : 			None
	def initialise_processing(self, logger, app_config, rule_config, default_config):

		# For the final staging output
		agg_name = 'GARDNERS'
		agg_reference = self
		final_staging_data = pd.DataFrame()
		input_list = list(app_config['input_params'])
		
		# Processing for each file in the fiven folder
		logger.info('\n+-+-+-+-+-+-+Starting Gardners files Processing\n')
		files_in_s3 = obj_s3_connect.get_files(logger, input_list)
		for each_file in files_in_s3:
			if each_file != '' and each_file.split('.')[-1] != 'txt':
				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')
				final_staging_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file, rule_config,
																		 default_config, final_staging_data,
																		 obj_read_data,
																		 obj_pre_process,agg_name, agg_reference)

		# Grouping and storing data
		final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data, default_config[0]['group_staging_data'])
		obj_s3_connect.store_data(logger, app_config, final_grouped_data)
		logger.info('\n+-+-+-+-+-+-+Finished Processing Gardners files\n')