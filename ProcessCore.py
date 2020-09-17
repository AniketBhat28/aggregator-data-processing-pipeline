#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import time



#################################
#		GLOBAL VARIABLES		#
#################################


# None



#################################
#		CLASS FUNCTIONS			#
#################################


class ProcessCore:

	# Function Description :	This function reads input data file and stores its contents to a pandas dataframe.
	# Input Parameters : 		logger - For the logging output file.
	#							config - Configuration
	#							filename - Name of the input file
	# Return Values : 			data - Returns the input dataframe
	def load_data(self, logger, config, filename):

		logger.info('Executing load_data()')
		current_time = time.time()

		# Getting configuration file details
		input_list = list(ast.literal_eval(config['File_Data']))
		input_base_path = input_list[0]['input_base_path']
		input_file_name = input_list[0]['input_file_name']
		input_sheet_name = input_list[0]['input_sheet_name']

		# Contatenate file path
		file_path = input_base_path + filename

		# Read input data file.
		input_file_extn = filename.split('.')[-1]
		if (input_file_extn == 'xlsx') or (input_file_extn == 'xls'):
			excel_frame = pd.ExcelFile(file_path)
			sheets = excel_frame.sheet_names
			if input_sheet_name is None:
				data = excel_frame.parse(sheets[0]) #, dtype=str)
			else:
				data = excel_frame.parse(input_sheet_name) #, dtype=str)
		elif input_file_extn == 'csv':
			data = pd.read_csv(file_path) #, dtype=str)
		# Pre-processing null values
			data.replace('', np.nan, inplace=True)
		logger.info('Input Dataframe size in memory : %s kB', data.memory_usage(deep=True).sum()/1024)
		logger.info('Exiting load_data(), Time taken to load : %s seconds', time.time() - current_time)
		return data

	def start_process_data(self, logger, element, data):
		if element['missing_data']['process_type'] == 'discard':
			data = data.dropna(subset=[element['column_name']])
			logger.info('Input Dataframe size in memory : %s kB', data.memory_usage(deep=True).sum() / 1024)
			logger.info('MISSING DATA has been discarded')
		elif element['missing_data']['process_type'] == 'process':
			blank_entries = pd.isnull(data[element['column_name']])
			data.loc[blank_entries, element['column_name']] = element['missing_data']['value']
			logger.info('Input Dataframe size in memory : %s kB', data.memory_usage(deep=True).sum() / 1024)
			logger.info('MISSING DATA has been processed as per '+ element['dtype']+ ' TYPE')
		return data