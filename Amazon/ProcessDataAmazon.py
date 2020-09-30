#################################
#			IMPORTS				#
#################################


import os
import ast
import pandas as pd
import numpy as np
import time
import boto3
import calendar
import re
from io import StringIO

from ReadData import ReadData
from ProcessCore import ProcessCore
from PreProcess import PreProcess
from Amazon.GenerateStagingDataAmazon import GenerateStagingDataAmazon



#################################
#		GLOBAL VARIABLES		#
#################################


obj_read_data = ReadData()
obj_process_core = ProcessCore()
obj_pre_process = PreProcess()
obj_gen_stage_data = GenerateStagingDataAmazon()


#################################
#		CLASS FUNCTIONS			#
#################################


class ProcessDataAmazon:

	# Function Description :	This function processes data for all Amazon files
	# Input Parameters : 		logger - For the logging output file.
	#							app_config - Configuration
	#							rule_config - Rules json
	# Return Values : 			None
	def initialise_processing(self, logger, app_config, rule_config):

		# Getting configuration file details
		input_list = list(ast.literal_eval(app_config['INPUT']['File_Data']))
		input_base_path = input_list[0]['input_base_path']
		input_bucket_name = input_list[0]['input_bucket_name']
		dir_path = input_list[0]['input_directory']

		# Connect to s3 bucket
		s3 = boto3.resource("s3")
		s3_bucket = s3.Bucket(input_bucket_name)

		# Listing files in s3 bucket
		files_in_s3 = [f.key.split(dir_path + "/")[1] for f in s3_bucket.objects.filter(Prefix=dir_path).all()]
		
		# For the final staging output
		final_data = pd.DataFrame()

		# Processing for each file in the fiven folder
		for each_file in files_in_s3:
			
			if each_file != '':

				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')

				# Load the data
				data = obj_read_data.load_data(logger, input_list, each_file)

				if not data.empty:
					# Get the corresponding rules object
					if 'rental' in each_file.lower():
						agg_rules = next((item for item in rule_config if
										  (item['name'] == 'Amazon' and item['filename_pattern'] == '/Amazon Rental')),
										 None)
					else:
						agg_rules = next((item for item in rule_config if
										  (item['name'] == 'Amazon' and item['filename_pattern'] == '/Amazon')), None)

					# Header details
					header_row = agg_rules['header_row']
					first_row = agg_rules['data_row']
					last_rows = agg_rules['discard_last_rows']

					# Get columns from given header rows
					if header_row != 0:
						data.columns = data.iloc[header_row]

					if 'attribute_names' in agg_rules.keys():
						if len(data.columns.tolist()) == 21:
							data.columns = agg_rules['attribute_names'][:-1]
						else:
							data.columns = agg_rules['attribute_names']

					# Get for first data row
					if first_row != 0:
						data = data.loc[first_row - 1:]

					# Discard given last rows
					if last_rows != 0:
						data = data.iloc[:-last_rows]

					# Discarding leading/trailing spacs from the columns
					data.columns = data.columns.str.strip()

					# Extracting relevant columns
					extracted_data = obj_pre_process.extract_relevant_attributes(logger, data, agg_rules['relevant_attributes'])


					if extracted_data.dropna(how='all').empty:
						logger.info("This file is empty")
					else:

						# Processing amount column
						amount_column = agg_rules['filters']['amount_column']
						extracted_data[amount_column] = (
							extracted_data[amount_column].replace('[,)]', '', regex=True).replace('[(]', '-', regex=True))

						# Column Validations
							extracted_data = obj_pre_process.validate_columns(logger, extracted_data, agg_rules['filters']['column_validations'])

						# Generating staging output from the pre-processed data
						logger.info("Generating Staging Output")
						extracted_data = obj_gen_stage_data.generate_staging_output(logger, each_file, agg_rules, extracted_data)
						logger.info("Staging output generated for given file")

						# Append staging data of current file into final staging dataframe
						final_data = pd.concat([final_data, extracted_data], ignore_index=True, sort=True)

		# Store the results to given S3 location
		logger.info('\n+-+-+-+-+-+-+')
		logger.info("Storing the staging output at the given S3 location")
		logger.info('\n+-+-+-+-+-+-+')
		obj_gen_stage_data.write_staging_output(logger, s3, app_config, final_data)			