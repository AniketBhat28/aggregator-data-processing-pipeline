#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import time
import boto3



#################################
#		GLOBAL VARIABLES		#
#################################


# None



#################################
#		CLASS FUNCTIONS			#
#################################


class read_data:

	# Function Description :	This function reads input data file and stores its contents to a pandas dataframe.
	# Input Parameters : 		logger - For the logging output file.
	#							config - Configuration
	#							filename - Name of the input file
	# Return Values : 			data - Returns the input dataframe
	def load_data(self, logger, config, filename):

		logger.info('Executing load_data()')
		current_time = time.time()

		# Getting configuration file details
		Input_List = list(ast.literal_eval(config['File_Data']))
		input_base_path = Input_List[0]['input_base_path']
		input_file_name = Input_List[0]['input_file_name']
		input_sheet_name = Input_List[0]['input_sheet_name']
		
		# Contatenate file path
		file_path = input_base_path + filename
		
		# Read input data file. 
		inputFileExtension = filename.split('.')[-1]
		if (inputFileExtension == 'xlsx') or (inputFileExtension == 'xls'):
			excelFrame = pd.ExcelFile(file_path)
			sheets = excelFrame.sheet_names
			if input_sheet_name is None:
				data = excelFrame.parse(sheets[0]) #, dtype=str)
			else:
				data = excelFrame.parse(input_sheet_name) #, dtype=str)
		elif inputFileExtension == 'csv':
			data = pd.read_csv(file_path) #, dtype=str)

		
		# Pre-processing null values
		data.replace('', np.nan, inplace=True)
		logger.info('Input Dataframe size in memory : %s kB', data.memory_usage(deep=True).sum()/1024)

		logger.info('Exiting load_data(), Time taken to load : %s seconds', time.time() - current_time)
		return data


		