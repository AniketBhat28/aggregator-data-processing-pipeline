#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import time
import re
import itertools



#################################
#		GLOBAL VARIABLES		#
#################################


#None



#################################
#		CLASS FUNCTIONS			#
#################################


class GenerateStagingAttributes:

	# Function Description :	This function is to process comma seperated isbn
	# Input Parameters : 		logger - For the logging output file.
	#							data - input data
	#							input_column - input column name
	#							staging_column - staging column name
	#							bckp_staging_column - backup staging column name
	#							default_val - Deafult value to be inserted
	# Return Values : 			data
	def process_isbn(self, logger, data, input_column, staging_column, bckp_staging_column, default_val):

		logger.info('Processing ISBN')
		data[input_column] = data[input_column].astype(str)
		
		data[staging_column] = data.apply(lambda row: default_val if (pd.isnull(row[input_column])) else(row[input_column].split(',')[0] if (len(row[input_column].split(',')) >= 1) else default_val) , axis=1)
		
		data[bckp_staging_column] = data.apply(lambda row: default_val if (pd.isnull(row[input_column])) else(row[input_column].split(',')[1] if (len(row[input_column].split(',')) >= 2) else default_val) , axis=1)
		logger.info('ISBN processed')

		return data
		

	# Function Description :	This function is to process comma seperated miscellaneous isbn
	# Input Parameters : 		logger - For the logging output file.
	#							data - input data
	#							e_column - input digital column name
	#							p_column - input physical column name
	#							staging_column - staging column name
	#							default_val - Deafult value to be inserted
	# Return Values : 			data
	def generate_misc_isbn(self, logger, data, e_column, p_column, staging_column, default_val):

		logger.info('Processing Miscellaneous ISBN')
		
		data['misc_e_product_id'] = data.apply(lambda row: [] if (pd.isnull(row[e_column])) else(row[e_column].split(',')[2:] if (len(row[e_column].split(',')) > 2) else []) , axis=1)
		data['misc_p_product_id'] = data.apply(lambda row: [] if (pd.isnull(row[p_column])) else(row[p_column].split(',')[2:] if (len(row[p_column].split(',')) > 2) else []) , axis=1)

		data[staging_column] = data[['misc_e_product_id', 'misc_p_product_id']].values.tolist()
		data[staging_column] = data.apply(lambda row: [] if (row[staging_column] == [[],[]]) else list(itertools.chain.from_iterable(row[staging_column])), axis=1)
		data[staging_column] = [','.join(map(str, l)) for l in data[staging_column]]
		data.replace('', default_val, inplace=True)

		logger.info('Miscellaneous ISBN processed')
		return data


	# Function Description :	This function is to extract patterns using regular expressions
	# Input Parameters : 		data - input data
	#							pattern_dict - containg regex
	#							input_string - input string to search
	# Return Values : 			data
	def extract_patterns(self, data, pattern_dict, input_string):

		pattern = pattern_dict['regex']
		temp_pattern_output = re.findall(fr"(?i)((?:{pattern}))", input_string, re.IGNORECASE)
		if len(temp_pattern_output) != 0:
			data[pattern_dict['staging_column_name']] = temp_pattern_output[0]
			
		return data


	# Function Description :	This function is to group staging data
	# Input Parameters : 		logger - For the logging output file.
	#							data - input data
	#							groupby_object - includes agg_fn and columns to group
	# Return Values : 			final_grouped_data
	def group_data(self, logger, data, groupby_object):

		logger.info('Grouping staging data')
		agg_fn = groupby_object['aggregation_function']
		groupby_columns = groupby_object['groupby_columns']

		final_grouped_data = data.groupby(groupby_columns, as_index=False).agg(agg_fn)

		logger.info('Staging data grouped')
		return final_grouped_data

