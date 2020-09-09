#################################
#			IMPORTS				#
#################################


import os
import ast
import pandas as pd
import numpy as np
import time
import boto3

from read_data import read_data



#################################
#		GLOBAL VARIABLES		#
#################################


# None



#################################
#		CLASS FUNCTIONS			#
#################################


class process_data_amazon:

	# Function Description :	This function aggregates revenue for all Amazon files
	# Input Parameters : 		logger - For the logging output file.
	#							app_config - Configuration
	#							rule_config - Rules json
	# Return Values : 			None
	def initialise_processing(self, logger, app_config, rule_config):

		# Getting configuration file details
		Input_List = list(ast.literal_eval(app_config['INPUT']['File_Data']))
		input_base_path = Input_List[0]['input_base_path']
		input_bucket_name = Input_List[0]['input_bucket_name']
		dir_path = Input_List[0]['input_directory']

		# Connect to s3 bucket
		s3 = boto3.resource("s3")
		s3_bucket = s3.Bucket(input_bucket_name)

		# Listing files in s3 bucket
		files_in_s3 = [f.key.split(dir_path + "/")[1] for f in s3_bucket.objects.filter(Prefix=dir_path).all()]
		
		# For the final staging output
		final_data = pd.DataFrame()
		
		# Processing for each file in the fiven folder
		for eachFile in files_in_s3:
			if eachFile != '':
		
				logger.info('\n+-+-+-+-+-+-+')
				logger.info(eachFile)
				logger.info('\n+-+-+-+-+-+-+')

				# Load the data
				objReadData = read_data()
				data = objReadData.load_data(logger, app_config['INPUT'], eachFile)
				
				# Get the corresponding rules object
				if 'rental' in eachFile.lower():
					agg_rules = next((item for item in rule_config if (item['name'] == 'Amazon' and item['filename_pattern'] == '/Amazon Rental')), None)
				else:
					agg_rules = next((item for item in rule_config if (item['name'] == 'Amazon' and item['filename_pattern'] == '/Amazon')), None)
				
				# Header details
				header_row = agg_rules['header_row']
				first_row = agg_rules['data_row']
				last_rows = agg_rules['discard_last_rows']

				# Get columns from given header rows
				if header_row != 0:
					data.columns = data.iloc[header_row]

				# Get for first data row
				if first_row != 0:
					data = data.loc[first_row-1:]

				# Discard given last rows
				if last_rows != 0:
					data = data.iloc[:-last_rows]

				# Discarding leading/trailing spacs from the columns
				data.columns = data.columns.str.strip()

				# Extracting relevent columns
				relevent_cols = agg_rules['relevant_attributes']
				data = data[relevent_cols]

				# Processing amount column
				amount_column = agg_rules['filters']['amount_column']
				data[amount_column] = (data[amount_column].replace( '[,)]','', regex=True ).replace( '[(]','-',regex=True ))

				# Column Validations
				for element in agg_rules['filters']['column_validations']:
					if element['dtype'] == 'float':
						data[element['column_name']] = pd.to_numeric(data[element['column_name']], errors='coerce')
						#data[element['column_name']] = data[element['column_name']].astype(float)
						if 'missing_data' in element.keys():
							if element['missing_data']['process_type'] == 'discard':
								data = data.dropna(subset=[element['column_name']])
							elif element['missing_data']['process_type'] == 'process':
								blank_entries = pd.isnull(data[element['column_name']])
								data.loc[blank_entries, element['column_name']] = element['missing_data']['value']

					elif element['dtype'] == 'str':
						if 'missing_data' in element.keys():
							if element['missing_data']['process_type'] == 'discard':
								data = data.dropna(subset=[element['column_name']])
							elif element['missing_data']['process_type'] == 'process':
								blank_entries = pd.isnull(data[element['column_name']])
								data.loc[blank_entries, element['column_name']] = element['missing_data']['value']

					
				# Processing data for final output
				data['aggregator'] = agg_rules['name']
				data['product_type'] = agg_rules['product_type']

				# Converting negative amounts to positives
				data[amount_column] = data[amount_column].abs()
				data['total_revenue'] = data[amount_column]

				# Extracting date and month from the filename
				filename = eachFile.split('.')[0]
				filename = filename.strip()
				filename = filename.split(' ')
				temp_month = filename[-2]
				temp_year = filename[-1]
				temp_country = filename[-3]

				data['month'] = temp_month #pd.to_datetime(data[date_column]).dt.month
				data['year'] =  temp_year #pd.to_datetime(data[date_column]).dt.year
				#data['region'] = temp_country

				# Process for the transaction type column rules
				column_rules = agg_rules['filters']['column_rules']
				for eachRule in column_rules:
					condition = eachRule['condition']
					value = eachRule['value']
					default_value = eachRule['default_value']
					output_column_name = eachRule['output_column_name']

					df = data.query(condition)
					given_rows = df.index.tolist()
					data.loc[given_rows, output_column_name] = value
					blank_entries = pd.isnull(data[output_column_name])
					data.loc[blank_entries, output_column_name] = default_value

				
				# Group the data to get aggregated revenue
				grouped_data = data.groupby(['aggregator', 'product_type', 'month', 'year', 'transaction_type'])['total_revenue'].sum().reset_index()
				logger.info('\n+-+-+-+-+-+-+')
				logger.info(grouped_data)

				# Append the results to the final staging output
				final_data = pd.concat([final_data,grouped_data])
				

		logger.info('\n+-+-+-+-+-+-+')	
		logger.info(final_data)

		# Aggregate the staging output results
		final_data = final_data.groupby(['aggregator', 'product_type','month', 'year', 'transaction_type'])['total_revenue'].sum().reset_index()
		logger.info('\n+-+-+-+-+-+-+')
		logger.info(final_data)

		# Write the output to a csv
		final_data.to_csv('Output/Amazon_Results.csv', sep=',')



