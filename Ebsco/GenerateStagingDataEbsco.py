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


class GenerateStagingDataEbsco:

	# Function Description :	This function generates staging data for Ebsco files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted - extracted staging data
	def generate_staging_output(self, logger, filename, agg_rules, extracted_data):

		extracted_data['aggregator'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']

		extracted_data['vendor_code'] = 'NA'
		extracted_data['product_format'] = 'NA'

		extracted_data = obj_gen_attrs.process_isbn(logger, extracted_data, 'digital_isbn', 'e_product_id', 'e_backup_product_id', 'NA')
		extracted_data = obj_gen_attrs.process_isbn(logger, extracted_data, 'physical_isbn', 'p_product_id', 'p_backup_product_id', 'NA')
		extracted_data = obj_gen_attrs.generate_misc_isbn(logger, extracted_data, 'digital_isbn', 'physical_isbn', 'misc_product_ids', 'NA')
		

		extracted_data['net_units'] = pd.to_numeric(extracted_data['net_units'], errors='coerce')
		if 'sale_type' not in extracted_data.columns.to_list():
			extracted_data['sale_type'] = extracted_data.apply(lambda row: ('RETURNS') if(row['net_units']<0) else ('PURCHASE'), axis=1)
		
		# Converting negative amounts to positives
		amount_column = agg_rules['filters']['amount_column']
		extracted_data[amount_column] = extracted_data[amount_column].abs()
		extracted_data['list_price'] = extracted_data['list_price'].abs()

		if agg_rules['filters']['convert_percentage'] == 'yes':
			logger.info('Converting percentages to decimals')
			extracted_data['disc_percentage'] = extracted_data['disc_percentage']/100

		logger.info('Computing net unit price')
		extracted_data['net_unit_price'] = round((extracted_data[amount_column]/extracted_data['net_units']), 2)
		extracted_data['net_unit_price'] = extracted_data['net_unit_price'].abs()
		logger.info('Net units price computed')

		logger.info('Computing Sales and Returns')
		extracted_data['total_sales_value'] = extracted_data.apply(lambda row:row[amount_column] if (row['net_units'] >= 0) else 0.0, axis=1)
		extracted_data['total_returns_value'] = extracted_data.apply(lambda row:row[amount_column] if (row['net_units'] < 0) else 0.0, axis=1)

		extracted_data['total_sales_count'] = extracted_data.apply(lambda row:row['net_units'] if (row['net_units'] >= 0) else 0, axis=1)
		extracted_data['total_returns_count'] = extracted_data.apply(lambda row:row['net_units'] if (row['net_units'] < 0) else 0, axis=1)
		extracted_data['total_returns_count'] = extracted_data['total_returns_count'].abs()
		logger.info('Sales and Return Values computed')

		return extracted_data