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
from Ebsco.GenerateStagingDataEbsco import GenerateStagingDataEbsco



#################################
#		GLOBAL VARIABLES		#
#################################


# None


#################################
#		CLASS FUNCTIONS			#
#################################


class ProcessDataEbsco:

	# Function Description :	This function processes data for all Ebsco files
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

		# Initialising Objects
		obj_read_data = ReadData()
		obj_process_core = ProcessCore()
		obj_gen_stage_data = GenerateStagingDataEbsco()

		# Processing for each file in the fiven folder
		for each_file in files_in_s3:
			if each_file != '':
				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')

				# To get the sheet names
				excel_frame = pd.ExcelFile(input_base_path + each_file)
				sheets = excel_frame.sheet_names
				for each_sheet in sheets:
					logger.info('Processing sheet: %s', each_sheet)
					input_list[0]['input_sheet_name'] = each_sheet

					# Load the data
					data = obj_read_data.load_data(logger, input_list, each_file)

					# Check if dataframe is empty
					if not data.empty:
						# Get the corresponding rules object
						if 'subscription' in each_file.lower():
							agg_rules = next((item for item in rule_config if
											  (item['name'] == 'Ebsco' and item['filename_pattern'] == '/Ebsco Subscription')),
											 None)
							logger.info('This is subscription data, not processed')
							break
						else:
							agg_rules = next((item for item in rule_config if
											  (item['name'] == 'Ebsco' and item['filename_pattern'] == '/Ebsco')), None)
						
						# Take the subset of data, remove header blanks
						logger.info('Removing metadata and blanks')
						raw_data = data
						for i, row in raw_data.iterrows():
							if row.notnull().all():
								data = raw_data.iloc[(i+1):].reset_index(drop=True)
								data.columns = list(raw_data.iloc[i])
								break
						data = data.dropna(subset=[data.columns[1]], how='all')
						logger.info('Actual data extracted')

						# Discarding leading/trailing spacs from the columns
						data.columns = data.columns.str.strip()
						
						# Extracting relevent columns
						logger.info('Getting and mapping relevant attributes')
						extracted_data = pd.DataFrame()
						relevent_cols = agg_rules['relevant_attributes']
						for each_col in relevent_cols:
							extracted_data[each_col['staging_column_name']] = data[each_col['input_column_name']]

						if extracted_data.dropna(how='all').empty:
							logger.info("This file is empty")
						else:

							# Processing amount column
							amount_column = agg_rules['filters']['amount_column']
							extracted_data[amount_column] = (
								extracted_data[amount_column].replace('[,)]', '', regex=True).replace('[(]', '-', regex=True))

							# Column Validations
							for element in agg_rules['filters']['column_validations']:
								if element['dtype'] == 'float':
									extracted_data[element['column_name']] = pd.to_numeric(extracted_data[element['column_name']], errors='coerce')
									#extracted_data[element['column_name']] = extracted_data[element['column_name']].astype(float)
									if 'missing_data' in element.keys():
										extracted_data = obj_process_core.start_process_data(logger, element, extracted_data)

								elif element['dtype'] == 'str':
									if 'missing_data' in element.keys():
										extracted_data = obj_process_core.start_process_data(logger, element, extracted_data)

							# Generating staging output from the pre-processed data
							logger.info("Generating Staging Output")
							extracted_data = obj_gen_stage_data.generate_staging_output(logger, each_file, agg_rules, extracted_data)
							logger.info("Staging output generated for given data")

							# Append staging data of current file into final staging dataframe
							final_data = pd.concat([final_data, extracted_data], ignore_index=True, sort=True)
							final_data.to_csv('output.csv')

		# Store the results to given S3 location
		logger.info('\n+-+-+-+-+-+-+')
		logger.info("Storing the staging output at the given S3 location")
		logger.info('\n+-+-+-+-+-+-+')
		obj_gen_stage_data.write_staging_output(logger, s3, app_config, final_data)			