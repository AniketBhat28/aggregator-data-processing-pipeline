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

	# Function Description :	This function processes data
	# Input Parameters : 		logger - For the logging output file.
	#							element - given sub-process
	#							data - input data
	# Return Values : 			data - data
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
