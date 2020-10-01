#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import time
from ProcessCore import ProcessCore



#################################
#		GLOBAL VARIABLES		#
#################################


obj_process_core = ProcessCore()



#################################
#		CLASS FUNCTIONS			#
#################################


class PreProcess:

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


	def extract_relevant_attributes(self, logger, data, relevant_cols):
		
		logger.info('Getting and mapping relevant attributes')
		extracted_data = pd.DataFrame()
		for each_col in relevant_cols:
			extracted_data[each_col['staging_column_name']] = data[each_col['input_column_name']]

		logger.info('Relevant attributes mapped')		
		return extracted_data


	def validate_columns(self, logger, data, column_validations):

		for element in column_validations:
			if element['dtype'] == 'float':
				data[element['column_name']] = pd.to_numeric(data[element['column_name']], errors='coerce')
				#data[element['column_name']] = data[element['column_name']].astype(float)
				if 'missing_data' in element.keys():
					data = obj_process_core.start_process_data(logger, element, data)

			elif element['dtype'] == 'str':
				if 'missing_data' in element.keys():
					data = obj_process_core.start_process_data(logger, element, data)

		return data