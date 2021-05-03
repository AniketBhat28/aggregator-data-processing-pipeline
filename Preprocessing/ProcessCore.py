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

	
	# Function Description :	This function to fetch rules object for given aggregator
	# Input Parameters : 		rule_config - rule_config
	#							condition - condition for filename
	#							aggregator_name - aggregator_name
	#							filename - filename
	#							pattern1 - filename pattern1
	#							pattern2 - filename pattern2
	# Return Values : 			data - data
	def get_rules_object(self, rule_config, condition, aggregator_name, filename, pattern1, pattern2):

		if condition in filename.lower():
			agg_rules = next(
				(
					item for item in rule_config if (
						item['name'] == aggregator_name and item['filename_pattern'] == pattern1
						)
				),
				None)
		else:
			agg_rules = next(
				(
					item for item in rule_config if(
						item['name'] == aggregator_name and item['filename_pattern'] == pattern2
						)
					), 
				None)

		return agg_rules

	
	def validate_scientific_notation(self, final_mapped_data, staging_column, max_len=13):
		"""
			Method to validate a field contains valid scientific notation.
			:param final_mapped_data: Final data frame
			:param staging_column: Input column
			:param max_len: Allowed length
			:return: If invalid scientific notation found return False otherwise it will return True
		"""
		regex = "-?\d\.\d+[Ee][+\-]\d\d?"
	
		if final_mapped_data[staging_column].str.contains(regex):
			return True

		return False
		