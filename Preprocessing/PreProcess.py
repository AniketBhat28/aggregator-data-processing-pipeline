#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import time
from Preprocessing.ProcessCore import ProcessCore



#################################
#		GLOBAL VARIABLES		#
#################################


obj_process_core = ProcessCore()



#################################
#		CLASS FUNCTIONS			#
#################################


class PreProcess:

	# Function Description :	This function is to process header templates and trailing metadata
	# Input Parameters : 		logger - For the logging output file.
	#							data - input data
	#							mandatory_columns - list of mandatory columns
	# Return Values : 			data
	def process_header_templates(self, logger, data, mandatory_columns):
		
		logger.info('Removing metadata and blanks')
		raw_data = data
		for i, row in raw_data.iterrows():
			if row.notnull().all():
				data = raw_data.iloc[(i+1):].reset_index(drop=True)
				data.columns = list(raw_data.iloc[i])
				break
		data = data.dropna(subset=mandatory_columns, how='all')
		
		logger.info('Discarding leading/trailing spacs from the columns')
		data.columns = data.columns.str.strip()
		
		logger.info('Actual data extracted')
		return data


	# Function Description :	This function is to extract relevant attributes
	# Input Parameters : 		logger - For the logging output file.
	#							data - input data
	#							relevant_cols - list of relevant columns
	# Return Values : 			data
	def extract_relevant_attributes(self, logger, data, relevant_cols):
		
		logger.info('Getting and mapping relevant attributes')
		extracted_data = pd.DataFrame()
		for each_col in relevant_cols:
			extracted_data[each_col['staging_column_name']] = data[each_col['input_column_name']]

		logger.info('Relevant attributes mapped')		
		return extracted_data


	# Function Description :	This function is to initiate column validations
	# Input Parameters : 		logger - For the logging output file.
	#							data - input data
	#							column_validations - list of column validations
	# Return Values : 			data
	def validate_columns(self, logger, data, column_validations):

		for element in column_validations:
			if element['dtype'] == 'float':
				data[element['column_name']] = pd.to_numeric(data[element['column_name']], errors='coerce')
				if 'missing_data' in element.keys():
					data = obj_process_core.start_process_data(logger, element, data)

			elif element['dtype'] == 'str':
				if 'missing_data' in element.keys():
					data = obj_process_core.start_process_data(logger, element, data)

		return data


	# Function Description :	This function is to process dates and convert them to a common format
	# Input Parameters : 		logger - For the logging output file.
	#							extracted_data - input data
	#							date_formats - list of date formats
	#							date_column_name - Name of the date column
	#							default_config - Default config for output date format
	# Return Values : 			data
	def process_dates(self, logger, extracted_data, date_formats, date_column_name, default_config):

		logger.info("Processing dates and converting to common format")
		output_date_format = default_config[0]['output_date_format']
		if len(date_formats) == 1:
			extracted_data['temp_transaction_date'] = pd.to_datetime(extracted_data[date_column_name], format=date_formats[0])
			extracted_data['temp_transaction_date'] = extracted_data['temp_transaction_date'].dt.strftime(output_date_format)
		else:
			for i in range(len(date_formats)):
				if i == 0:
					date_rows = pd.to_datetime(extracted_data[date_column_name], format=date_formats[i], errors="coerce")
				else:
					date_rows = date_rows.fillna(pd.to_datetime(extracted_data[date_column_name], format=date_formats[i], errors="coerce"))
			extracted_data['temp_transaction_date'] = date_rows
			extracted_data['temp_transaction_date'] = extracted_data['temp_transaction_date'].dt.strftime(output_date_format)

		extracted_data[date_column_name] = extracted_data['temp_transaction_date']

		logger.info("Dates converted to given common format")
		return extracted_data

	# Function Description :	This function is to generate default dates and convert them to a common format
	# Input Parameters : 		logger - For the logging output file.
	#							extracted data: final staging output data
	#							app_config - app config for output directory location
	# Return Values : 			data
	def process_default_transaction_date(self, logger, app_config, extracted_data) :
		logger.info('resolving future date issue')
		output_directory = app_config['output_params']['output_directory']
		year = output_directory[-8:-4]
		month = output_directory[-10:-8]
		default_transaction_date =''
		m1 = ["01", "03", "05", "07","08","10","12"]
		m2 = ["11", "04", "06", "09",]
		m3 = ["02"]
		if any(x in month for x in m1) :
			default_transaction_date = '31-' + month+'-' + year
		elif any(x in month for x in m2) :
			default_transaction_date = '30-' + month+'-' + year
		elif any(x in month for x in m3) :
			default_transaction_date = '28-' + month+'-' + year
		extracted_data['transaction_date'] = extracted_data.apply(
			lambda row : row['transaction_date'].replace(row['transaction_date'], default_transaction_date) if row['transaction_date'][-4 :] > year or
																			row['transaction_date'][-7 :-5] > month else row['transaction_date'], axis=1)
		return extracted_data
