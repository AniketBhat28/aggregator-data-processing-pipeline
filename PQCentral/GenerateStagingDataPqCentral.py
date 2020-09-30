#################################
#			IMPORTS				#
#################################


import pandas as pd
import numpy as np
import itertools
import boto3
import calendar
import re
from io import StringIO



#################################
#		GLOBAL VARIABLES		#
#################################


# None


#################################
#		CLASS FUNCTIONS			#
#################################


class GenerateStagingDataPqCentral:

	# Function Description :	This function generates staging data for PqCentral files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted - extracted staging data
	def generate_staging_output(self, logger, filename, agg_rules, extracted_data):

		# Processing data for final output
		extracted_data['aggregator'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']

		extracted_data['product_format'] = 'NA'
		extracted_data['e_backup_product_id'] = 'NA'
		extracted_data['p_backup_product_id'] = 'NA'
		
		extracted_data['net_units'] = pd.to_numeric(extracted_data['net_units'], errors='coerce')
		if 'sale_type' not in extracted_data.columns.to_list():
			extracted_data['sale_type'] = extracted_data.apply(lambda row: ('RETURNS') if(row['net_units']<0) else ('PURCHASE'), axis=1)
		
		# Converting negative amounts to positives
		logger.info('Converting negative amounts to positives')
		amount_column = agg_rules['filters']['amount_column']
		extracted_data[amount_column] = extracted_data[amount_column].abs()
		extracted_data['list_price'] = extracted_data['list_price'].abs()

		# Converting percentages
		if agg_rules['filters']['convert_percentage'] == 'yes':
			logger.info('Converting percentages to decimals')
			extracted_data['disc_percentage'] = extracted_data['disc_percentage']/100
			extracted_data['promotional_disc_percentage'] = extracted_data['promotional_disc_percentage']/100

		# Processing discounts
		logger.info('Processing percentages')
		extracted_data['disc_percentage'] = 1.0 - extracted_data['disc_percentage']
		extracted_data['disc_percentage'] = (-1*extracted_data['disc_percentage']) - extracted_data['promotional_disc_percentage'] + (extracted_data['disc_percentage']*extracted_data['promotional_disc_percentage'])
		extracted_data['disc_percentage'] = extracted_data['disc_percentage'].abs()

		logger.info('Computing net unit price')
		extracted_data['net_unit_price'] = round((extracted_data[amount_column]/extracted_data['net_units']), 2)
		extracted_data['net_unit_price'] = extracted_data['net_unit_price'].abs()
		logger.info('Net units price computed')

		# Computing sales and returns
		logger.info('Computing Sales and Returns')
		extracted_data['total_sales_value'] = extracted_data.apply(lambda row:row[amount_column] if (row['net_units'] >= 0) else 0.0, axis=1)

		extracted_data['total_returns_value'] = extracted_data.apply(lambda row:row[amount_column] if (row['net_units'] < 0) else 0.0, axis=1)

		extracted_data['total_sales_count'] = extracted_data.apply(lambda row:row['net_units'] if (row['net_units'] >= 0) else 0, axis=1)

		extracted_data['total_returns_count'] = extracted_data.apply(lambda row:row['net_units'] if (row['net_units'] < 0) else 0, axis=1)
		extracted_data['total_returns_count'] = extracted_data['total_returns_count'].abs()
			
		logger.info('Sales and Return Values computed')

		return extracted_data


	# Function Description :	This function writes the output to given S3 location
	# Input Parameters : 		logger - For the logging output file.
	#							s3 - S3 object
	#							app_config - Configuration
	#							final_data - Final staging data
	# Return Values : 			None
	def write_staging_output(self, logger, s3, app_config, final_data):

		# Get the output bucket name and directory
		output_bucket_name = app_config['OUTPUT']['output_bucket_name']
		output_directory = app_config['OUTPUT']['output_directory']

		# Grouping the staging data
		logger.info('Grouping staging data')

		agg_fn = {'list_price': 'sum', 'net_unit_price': 'sum', 'net_units': 'sum', 'revenue_value': 'sum', 'total_sales_count': 'sum', 'total_sales_value':'sum', 'total_returns_count': 'sum', 'total_returns_value':'sum'}
		final_grouped_data = final_data.groupby(['transaction_date', 'e_product_id', 'e_backup_product_id', 'p_product_id', 'p_backup_product_id', 'title', 'vendor_code', 'product_type', 'imprint', 'product_format', 'sale_type', 'trans_currency', 'disc_percentage', 'region_of_sale'], as_index=False).agg(agg_fn)
		logger.info('Staging data grouped')
		#final_grouped_data = final_grouped_data.replace({'NA':np.nan})

		# Write the output
		logger.info('Writing the output at the given S3 location')
		csv_buffer = StringIO()
		final_grouped_data.to_csv(csv_buffer)
		s3.Object(output_bucket_name, output_directory).put(Body=csv_buffer.getvalue())
		logger.info('Staging data successfully stored at the given S3 location')