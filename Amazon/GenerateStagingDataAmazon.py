#################################
#			IMPORTS				#
#################################


import pandas as pd
import numpy as np

from GenerateStagingAttributes import GenerateStagingAttributes



#################################
#		GLOBAL VARIABLES		#
#################################


obj_gen_attrs = GenerateStagingAttributes()



#################################
#		CLASS FUNCTIONS			#
#################################


class GenerateStagingDataAmazon:

	# Function Description :	This function generates staging data for Amazon files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted - extracted staging data
	def generate_staging_output(self, logger, filename, agg_rules, extracted_data):

		extracted_data['aggregator'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']

		logger.info('Extracting patterns from filename')
		for each_rule in agg_rules['pattern_extractions']:
			extracted_data = obj_gen_attrs.extract_patterns(extracted_data, each_rule, filename)
		logger.info('Patterns extracted')

		if 'sale_type' not in extracted_data.columns.to_list():
			extracted_data['sale_type'] = extracted_data.apply(lambda row: ('RETURNS') if(row['net_units']<0) else ('PURCHASE'), axis=1)

		if 'rental_duration' not in extracted_data.columns.to_list():
			extracted_data['rental_duration'] = 0

		# Converting negative amounts to positives
		amount_column = agg_rules['filters']['amount_column']
		extracted_data[amount_column] = extracted_data[amount_column].abs()

		if agg_rules['filters']['convert_percentage'] == 'yes':
			logger.info('Converting percentages to decimals')
			extracted_data['disc_percentage'] = extracted_data['disc_percentage']/100

		logger.info('Computing net unit price')
		extracted_data['net_unit_price'] = round(((1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
		logger.info('Net units price computed')

		logger.info('Computing Sales and Returns')
		extracted_data['total_sales_value'] = round((extracted_data['total_sales_count'] * (1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
		extracted_data['total_returns_value'] = round((extracted_data['total_returns_count'] * (1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
		logger.info('Sales and Return Values computed')

		return extracted_data

