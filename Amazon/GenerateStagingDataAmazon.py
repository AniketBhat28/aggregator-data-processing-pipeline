#################################
#			IMPORTS				#
#################################


import pandas as pd
import numpy as np
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


class GenerateStagingDataAmazon:

	# Function Description :	This function generates staging data for Amazon files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted - extracted staging data
	def generate_staging_output(self, logger, filename, agg_rules, extracted_data):

		# Processing data for final output
		extracted_data['aggregator'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']

		# Extracting region from the filename
		logger.info('Extracting region from filename')
		regions = "AU|Brazil|Canada|DE|France|GB|India|Italy|Japan|Mexico|NL|Spain|US"
		temp_region = re.findall(fr"(?i)((?:{regions}))", filename, re.IGNORECASE)
		if len(temp_region) != 0:
			extracted_data['region_of_sale'] = temp_region[0]
			logger.info('Regions extracted')

		# Extracting vendor code from the filename
		logger.info('Extracting vendor codes from filename')
		vendor_codes = "ASHEU|INHQQ|TAYQQ|TYFQQ|TYFRQ"
		temp_code = re.findall(fr"(?i)((?:{vendor_codes}))", filename, re.IGNORECASE)
		if len(temp_code) != 0:
			extracted_data['vendor_code'] = temp_code[0]
			logger.info('Vendor codes extracted')

		if 'sale_type' not in extracted_data.columns.to_list():
			extracted_data['sale_type'] = extracted_data.apply(lambda row: ('RETURNS') if(row['net_units']<0) else ('PURCHASE'), axis=1)

		if 'rental_duration' not in extracted_data.columns.to_list():
			extracted_data['rental_duration'] = 'NA'

		# Converting negative amounts to positives
		amount_column = agg_rules['filters']['amount_column']
		extracted_data[amount_column] = extracted_data[amount_column].abs()

		# Converting percentages
		if agg_rules['filters']['convert_percentage'] == 'yes':
			logger.info('Converting percentages to decimals')
			extracted_data['disc_percentage'] = extracted_data['disc_percentage']/100

		logger.info('Computing net unit price')
		#extracted_data['net_unit_price'] = round((extracted_data[amount_column]/extracted_data['net_units']))
		extracted_data['net_unit_price'] = round(((1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
		logger.info('Net units price computed')

		# Computing sales and returns
		logger.info('Computing Sales and Returns')
		extracted_data['total_sales_value'] = round((extracted_data['total_sales_count'] * (1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
		extracted_data['total_returns_value'] = round((extracted_data['total_returns_count'] * (1-extracted_data['disc_percentage']) * extracted_data['list_price']), 2)
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
		final_grouped_data = final_data.groupby(['transaction_date', 'e_product_id', 'p_backup_product_id', 'p_product_id', 'title', 'vendor_code', 'product_type', 'imprint', 'product_format', 'sale_type', 'rental_duration', 'trans_currency', 'disc_percentage', 'region_of_sale'], as_index=False).agg(agg_fn)
		logger.info('Staging data grouped')

		# Write the output
		logger.info('Writing the output at the given S3 location')
		csv_buffer = StringIO()
		final_grouped_data.to_csv(csv_buffer)
		s3.Object(output_bucket_name, output_directory).put(Body=csv_buffer.getvalue())
		logger.info('Staging data successfully stored at the given S3 location')
			

						