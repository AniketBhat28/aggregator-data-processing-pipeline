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


class ProcessDataAmazon:

	# Function Description :	This function generates staging data for Amazon files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted - extracted staging data
	def generate_staging_output(self, logger, filename, agg_rules, extracted_data):

		extracted_data['aggregator_name'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']
		extracted_data['pod'] = 'NA'

		logger.info('Extracting patterns from filename')
		for each_rule in agg_rules['pattern_extractions']:
			extracted_data = obj_gen_attrs.extract_patterns(extracted_data, each_rule, filename)
		logger.info('Patterns extracted')

		if 'sale_type' not in extracted_data.columns.to_list():
			extracted_data['sale_type'] = extracted_data.apply(lambda row: ('RETURNS') if(row['net_units']<0) else ('PURCHASE'), axis=1)

		if 'rental_duration' not in extracted_data.columns.to_list():
			extracted_data['rental_duration'] = 0
		else:
			extracted_data['rental_duration'] = extracted_data['rental_duration'].apply(lambda row: pd.to_numeric(row, errors='coerce')).fillna(0)

		# Converting negative amounts to positives
		amount_column = agg_rules['filters']['amount_column']
		extracted_data[amount_column] = extracted_data[amount_column].abs()

		if agg_rules['filters']['convert_percentage'] == 'yes':
			logger.info('Converting percentages to decimals')
			extracted_data['disc_percentage'] = extracted_data['disc_percentage']/100

		logger.info('Computing net unit price')
		extracted_data['net_unit_price'] = round(((1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
		logger.info('Net units price computed')

		logger.info('Computing Sales and Returns')
		extracted_data['total_sales_value'] = round((extracted_data['total_sales_count'] * (1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
		extracted_data['total_returns_value'] = round((extracted_data['total_returns_count'] * (1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
		logger.info('Sales and Return Values computed')

		return extracted_data


	# Function Description :	This function processes data for all Amazon files
	# Input Parameters : 		logger - For the logging output file.
	#							app_config - Configuration
	#							rule_config - Rules json
	# Return Values : 			None
	def initialise_processing(self, logger, app_config, rule_config):

		# For the final staging output
		final_staging_data = pd.DataFrame()
		input_list = list(ast.literal_eval(app_config['INPUT']['File_Data']))
		
		# Processing for each file in the fiven folder
		logger.info('\n+-+-+-+-+-+-+Starting Amazon files Processing\n')
		files_in_s3 = obj_s3_connect.get_files(logger, input_list)
		for each_file in files_in_s3:
			if each_file != '':

				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')

				data = obj_read_data.load_data(logger, input_list, each_file)
				if not data.empty:
					logger.info('Get the corresponding rules object for Amazon')
					if 'rental' in each_file.lower():
						agg_rules = next((item for item in rule_config if
										  (item['name'] == 'AMAZON' and item['filename_pattern'] == '/Amazon Rental')),
										 None)
					else:
						agg_rules = next((item for item in rule_config if
										  (item['name'] == 'AMAZON' and item['filename_pattern'] == '/Amazon')), None)

					if agg_rules['discard_last_rows'] != 0:
						data = data.iloc[:-agg_rules['discard_last_rows']]
					data = data.dropna(how='all')

					if 'attribute_names' in agg_rules.keys():
						if len(data.columns.tolist()) == 21:
							data.columns = agg_rules['attribute_names'][:-1]
						else:
							data.columns = agg_rules['attribute_names']
					data.columns = data.columns.str.strip()
					data[agg_rules['filters']['mandatory_columns']] = data[agg_rules['filters']['mandatory_columns']].fillna(value='NA')

					extracted_data = obj_pre_process.extract_relevant_attributes(logger, data, agg_rules['relevant_attributes'])

					if extracted_data.dropna(how='all').empty:
						logger.info("This file is empty")
					else:

						logger.info('Processing amount column')
						amount_column = agg_rules['filters']['amount_column']
						extracted_data[amount_column] = (
							extracted_data[amount_column].replace('[,)]', '', regex=True).replace('[(]', '-', regex=True))

						extracted_data = obj_pre_process.validate_columns(logger, extracted_data, agg_rules['filters']['column_validations'])

						logger.info("Generating Staging Output")
						extracted_data = self.generate_staging_output(logger, each_file, agg_rules, extracted_data)
						logger.info("Staging output generated for given file")

						# Append staging data of current file into final staging dataframe
						final_staging_data = pd.concat([final_staging_data, extracted_data], ignore_index=True, sort=True)

		# Grouping and storing data
		final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data, agg_rules['group_staging_data'])
		obj_s3_connect.store_data(logger, app_config, final_grouped_data)
		logger.info('\n+-+-+-+-+-+-+Finished Processing Amazon files\n')