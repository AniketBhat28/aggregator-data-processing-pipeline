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


class ProcessDataAmazon:

	# Function Description :	This function cleans data headers and tailers
	# Input Parameters : 		data - input data
	#							agg_rules - rules json
	#							filename - filename
	# Return Values : 			data - cleaned data
	def clean_data(self, data, agg_rules, filename):

		if 'rental' in filename.lower():
			data.columns = data.columns.str.lower()

		if agg_rules['discard_last_rows'] != 0:
			data = data.iloc[:-agg_rules['discard_last_rows']]
		data = data.dropna(how='all')

		if 'attribute_names' in agg_rules.keys():
			if len(data.columns.tolist()) == 21:
				data.columns = agg_rules['attribute_names'][:-1]
			else:
				data.columns = agg_rules['attribute_names']
		data.columns = data.columns.str.strip()

		return data

	
	# Function Description :	This function processes transaction and sales types
	# Input Parameters : 		logger - For the logging output file.
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted_data - extracted staging data
	def process_trans_type(self, logger, extracted_data):

		logger.info("Processing transaction and sales types")
		if 'sale_type' not in extracted_data.columns.to_list():
			extracted_data['sale_type'] = extracted_data.apply(lambda row: ('REFUNDS') if(row['net_units']<0) else ('PURCHASE'), axis=1)
			extracted_data['trans_type'] = extracted_data.apply(lambda row: ('RETURNS') if(row['net_units']<0) else ('SALE'), axis=1)
		else:
			extracted_data.loc[(extracted_data['net_units'] < 0), 'sale_type'] = extracted_data.loc[(extracted_data['net_units'] < 0),'sale_type'].fillna('REFUNDS')
			extracted_data.loc[(extracted_data['net_units'] >= 0), 'sale_type'] = extracted_data.loc[(extracted_data['net_units'] >= 0),'sale_type'].fillna('PURCHASE')
			extracted_data['trans_type'] = 'RENTAL'

		logger.info("Transaction and sales types processed")
		return extracted_data


	# Function Description :	This function generates staging data for Amazon files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							default_config - Default json
	#							extracted_data - pr-processed_data

	# Return Values : 			extracted_data - extracted staging data
	def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data):

		extracted_data['aggregator_name'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']
		extracted_data['pod'] = 'NA'
		extracted_data['e_backup_product_id'] = 'NA'
		extracted_data['disc_code'] = 'NA'
		extracted_data['misc_product_ids'] = 'NA'

		logger.info('Extracting patterns from filename')
		for each_rule in agg_rules['pattern_extractions']:
			extracted_data = obj_gen_attrs.extract_patterns(extracted_data, each_rule, filename)
		logger.info('Patterns extracted')

		if 'country_iso_values' in agg_rules['filters'].keys():
			extracted_data['region_of_sale'] = extracted_data['region_of_sale'].map(agg_rules['filters']['country_iso_values']).fillna(extracted_data['region_of_sale'])

		current_country = extracted_data['region_of_sale'][0]
		current_date_format = next(item for item in agg_rules['date_formats'] if item["country"] == current_country)['format']
		extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'transaction_date', default_config)
		
		extracted_data = self.process_trans_type(logger, extracted_data)
		
		if 'total_rental_duration' not in extracted_data.columns.to_list():
			extracted_data['total_rental_duration'] = 0
		else:
			extracted_data['total_rental_duration'] = extracted_data['total_rental_duration'].apply(lambda row: pd.to_numeric(row, errors='coerce')).fillna(0)

		amount_column = agg_rules['filters']['amount_column']
		
		if agg_rules['filters']['convert_percentage'] == 'yes':
			logger.info('Converting percentages to decimals')
			extracted_data['disc_percentage'] = extracted_data['disc_percentage']/100

		extracted_data = obj_gen_attrs.process_net_unit_prices(logger, extracted_data, amount_column)

		logger.info('Computing Sales and Returns')
		extracted_data['total_sales_value'] = round((extracted_data['total_sales_count'] * (1-extracted_data['disc_percentage']) * extracted_data['publisher_price']), 2)
		extracted_data['total_returns_value'] = round((extracted_data['total_returns_count'] * (1-extracted_data['disc_percentage']) * extracted_data['publisher_price']), 2)
		logger.info('Sales and Return Values computed')

		extracted_data['disc_percentage'] = extracted_data['disc_percentage']*100
		
		# new attributes addition
		if 'rental' in filename.lower():
			extracted_data['source'] = "Amazon EBook Rental"
			extracted_data['sub_domain'] = 'AMAZON RENTAL'
		else:
			extracted_data['source'] = "Amazon EBook"
			extracted_data['sub_domain'] = 'NA'
		extracted_data['source_id'] = filename.split('.')[0]

		return extracted_data


	# Function Description :	This function processes data for all Amazon files
	# Input Parameters : 		logger - For the logging output file.
	#							app_config - Configuration
	#							rule_config - Rules json
	#							default_config - Default json
	# Return Values : 			None
	def initialise_processing(self, logger, app_config, rule_config, default_config):

		# For the final staging output
		final_staging_data = pd.DataFrame()
		input_list = list(app_config['input_params'])
		
		# Processing for each file in the fiven folder
		logger.info('\n+-+-+-+-+-+-+Starting Amazon files Processing\n')
		files_in_s3 = obj_s3_connect.get_files(logger, input_list)
		for each_file in files_in_s3:
			if each_file != '' and 'amazon' in each_file.lower():

				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')
				try:
					data = obj_read_data.load_data(logger, input_list, each_file)
					if not data.empty:
						logger.info('Get the corresponding rules object for Amazon')
						agg_rules = obj_process_core.get_rules_object(rule_config, 'rental', 'AMAZON', each_file, '/Amazon Rental', '/Amazon')

						data = self.clean_data(data, agg_rules, each_file)						
						if 'attribute_mappings' in agg_rules.keys():
							data = obj_gen_attrs.replace_column_names(logger, agg_rules, data)

						data[agg_rules['filters']['mandatory_columns']] = data[agg_rules['filters']['mandatory_columns']].fillna(value='NA')
						
						extracted_data = obj_pre_process.extract_relevant_attributes(logger, data, agg_rules['relevant_attributes'])
						
						agg_reference = self
						final_staging_data = obj_gen_attrs.process_staging_data(logger, each_file, agg_rules,
																					default_config, extracted_data,
																					final_staging_data,
																					agg_reference, obj_pre_process)
				except KeyError as err:
					logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)

		# Grouping and storing data
		final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data, default_config[0]['group_staging_data'])
		obj_s3_connect.store_data(logger, app_config, final_grouped_data)
		logger.info('\n+-+-+-+-+-+-+Finished Processing Amazon files\n')