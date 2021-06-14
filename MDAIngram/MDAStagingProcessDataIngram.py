#################################
#			IMPORTS				#
#################################


import pandas as pd
import numpy as np
import country_converter as coco

from ReadWriteData.ReadData import ReadData
from Preprocessing.PreProcess import PreProcess
from Preprocessing.ProcessCore import ProcessCore
from ReadWriteData.ConnectToS3 import ConnectToS3
from AttributeGenerators.GenerateStagingAttributes import GenerateStagingAttributes

#################################
#		GLOBAL VARIABLES		#
#################################


obj_read_data = ReadData()
obj_pre_process = PreProcess()
obj_s3_connect = ConnectToS3()
obj_process_core = ProcessCore()
obj_gen_attrs = GenerateStagingAttributes()

#################################
#		CLASS FUNCTIONS			#
#################################


class MDAStagingProcessDataIngram:
	"""
		Class used to convert source data into mapped data
	"""

	# Class variables
	LOGGER = None

	# Function Description :	This function processes transaction and sales types
	# Input Parameters : 		logger - For the logging output file.
	#							final_mapped_data - pr-processed_data
	# Return Values : 			final_mapped_data - extracted staging data
	def process_trans_type(self, final_mapped_data) :
		self.LOGGER.info("Processing transaction type")
		final_mapped_data['trans_type'] = final_mapped_data.apply(
            lambda row : ('Sales') if (row['rental_duration'] == 0) else 'Rental', axis=1)

		self.LOGGER.info("Transaction type is processed")
		return final_mapped_data

	# Function Description :	This function generates staging data for Ingram files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							default_config - Default json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted_data - extracted staging data
	def generate_edw_staging_data(self, logger, agg_rules, app_config, final_mapped_data) :
		logger.info('***********generate staging data started*******************')

		# Drop invalid rows
		final_mapped_data = final_mapped_data.drop(
			final_mapped_data[(
						(final_mapped_data.e_product_id == 'NA')
						&
						(final_mapped_data.e_backup_product_id == 'NA')
						&
						(final_mapped_data.p_product_id == 'NA')
						&
						(final_mapped_data.p_backup_product_id =='NA' )
						&
						(final_mapped_data.external_product_id == 'NA')
			)].index
		)

		# Hard coded values
		final_mapped_data['aggregator_name'] = agg_rules['fullname']
		final_mapped_data['product_type'] = agg_rules['product_type']

		product_columns = ('e_product_id', 'p_product_id', 'e_backup_product_id', 'p_backup_product_id', 'external_product_id')
		for product_column in product_columns:
			final_mapped_data[product_column] = final_mapped_data[product_column].replace(
				'-', '', regex=True
				).replace(
					'', 'NA', regex=True
					)
			final_mapped_data = obj_gen_attrs.remove_str_decimals(logger, final_mapped_data, product_column)
			
		final_mapped_data['external_invoice_number'] = final_mapped_data.external_invoice_number.str.split('.', expand=True)
		final_mapped_data['post_code'] = final_mapped_data.post_code.str.split('.', expand=True)

		final_mapped_data.loc[(final_mapped_data['list_price_multiplier'] == 'NA'), 'list_price_multiplier'] = 1
		final_mapped_data['list_price_multiplier'] = round(final_mapped_data['list_price_multiplier'].astype('float'), 2)

		final_mapped_data.loc[(final_mapped_data['cost_factor'] == 'NA'), 'cost_factor'] = 1
		final_mapped_data['cost_factor'] = round(final_mapped_data['cost_factor'].astype('float'), 2)

		final_mapped_data.loc[(final_mapped_data['rental_duration'] == 'NA'), 'rental_duration'] = 0
		final_mapped_data['rental_duration'] = final_mapped_data['rental_duration'].astype('float').astype('int')

		final_mapped_data.loc[(final_mapped_data['units'] == 'NA'), 'units'] = '1'
		final_mapped_data['units'] = final_mapped_data['units'].astype('float').astype('int')
		final_mapped_data.loc[(final_mapped_data['units'] == 0), 'units'] = 1

		final_mapped_data['payment_amount'] = round(final_mapped_data['payment_amount'].astype('float'), 2)
		final_mapped_data['price'] = round(final_mapped_data['price'].astype('float'), 2)
		final_mapped_data['current_discount_percentage'] = round(final_mapped_data['current_discount_percentage'].astype('float'), 2).abs()

		final_mapped_data['country'] = final_mapped_data.apply(
			lambda row : (row['payment_amount_currency']) if (row['country'] == 'NA') else (row['country']), axis=1)
		final_mapped_data['country'] = final_mapped_data['country'].map(
			agg_rules['filters']['country_iso_values']).fillna(final_mapped_data['country'])

		#converting UK to GB ISO code
		final_mapped_data['country'] = final_mapped_data['country'].replace('UK', 'GB', regex=True)
		#converting country to ISO2 encoding
		final_mapped_data = self.country_converter(logger, final_mapped_data)

		final_mapped_data = self.process_trans_type(final_mapped_data)
		final_mapped_data['reporting_date'] = pd.to_datetime(
			final_mapped_data['reporting_date'], format='%d-%m-%Y', infer_datetime_format=True
		)
		final_mapped_data['reporting_date'] = final_mapped_data['reporting_date'].dt.date

		logger.info('****************generate staging data done**************')
		return final_mapped_data

	# Function Description :	This function processes data for all Gardners files
	# Input Parameters : 		logger - For the logging output file.
	#							app_config - Configuration
	#							rule_config - Rules json
	#							default_config - Default json
	# Return Values : 			None
	def initialise_processing(self, logger, app_config, rule_config, default_config) :
		# For the final staging output
		agg_name = "INGRAM"
		self.LOGGER = logger
		final_edw_data = pd.DataFrame()

		input_list = list(app_config['input_params'])
		input_base_path = input_list[0]['input_base_path']

		# Processing for each file in the fiven folder
		logger.info('\n+-+-+-+-+-+-+Starting Ingram files Processing\n')
		agg_rules = next((item for item in rule_config if (item['name'] == agg_name)), None)

		files_in_s3 = obj_s3_connect.get_files(logger, input_list)
		for each_file in files_in_s3 :
			if each_file != '' and each_file.split('.')[-1] != 'txt' :
				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')

				final_mapped_data = pd.read_parquet(input_base_path + each_file, engine='pyarrow')

				final_staging_data = self.generate_edw_staging_data(logger, agg_rules, app_config, final_mapped_data)

				# Append staging data of current file into final staging dataframe
				final_edw_data = pd.concat([final_edw_data, final_staging_data], ignore_index=True, sort=True)

		final_edw_data = obj_gen_attrs.group_data(logger, final_edw_data, default_config[0]['group_staging_data'])
		obj_s3_connect.store_data_as_parquet(logger, app_config, final_edw_data)

		logger.info('\n+-+-+-+-+-+-+Finished Processing Ingram files\n')

	# Function Description :	This function generates country ISO 2 codes
	# Input Parameters : 		logger - For the logging output file.
	#							final_grouped_data - staging data
	#
	# Return Values : 			final_grouped_data - ISO2 country code converted staging data
	def country_converter(self,logger, final_mapped_data) :
		logger.info("************country conversion starts*****************")
		cc = coco.CountryConverter()
		# Filter non iso2 formats country df
		convertable_df = final_mapped_data[final_mapped_data['country'].str.len() > 2].loc[:, ('country',)]

		if not convertable_df.empty:
			iso_names = cc.convert(names=convertable_df['country'].tolist(), to="ISO2")
			convertable_df['country'] = iso_names
			final_mapped_data.update(convertable_df)

		# Delete temp df to avoid garbage collections
		del convertable_df
		logger.info("************country conversion ends*****************")
		return final_mapped_data
