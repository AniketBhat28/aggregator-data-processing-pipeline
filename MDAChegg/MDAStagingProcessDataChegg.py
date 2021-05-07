#################################
#			IMPORTS				#
#################################


import pandas as pd
import numpy as np

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


#################################
#		CLASS FUNCTIONS			#
#################################


class MDAStagingProcessDataChegg(GenerateStagingAttributes):
	"""
		Class used to convert source data into mapped data
	"""

	# Class variables
	AGG_NAME = 'CHEGG'
	LOGGER = None

	def process_trans_type(self, final_mapped_data):
		"""
		To process the transaction type
		:param extracted_data: pr-processed_data
		:return: extracted dataframe
		"""
		self.LOGGER.info("Processing transaction type")

		final_mapped_data['trans_type'] = final_mapped_data.apply(
			lambda row : (
				'Rental' if(row['trans_type_ori'] in ('rental', 'extension')) else (
					'Sales' if row['trans_type_ori'] == 'sell' else 'Subscription'
					)
				),
			axis=1)
		
		self.LOGGER.info("Transaction type is processed")
		return final_mapped_data


	def generate_edw_staging_data(self, logger, agg_rules, app_config, final_mapped_data):
		"""
		Generates staging data for Chegg files
		:param logger: For the logging output file.
		:param agg_rules: Rules json
		:param app_config: Input configuration
		:param final_mapped_data: Mapped data from the data lake
		:return: final pd dataframe
		"""
		logger.info('***********generate staging data started*******************')

		# Drop invalid rows
		final_mapped_data = final_mapped_data.drop(
			final_mapped_data[(
				(
					(
						(final_mapped_data.reporting_date == 'nan')
						| (final_mapped_data.reporting_date == '')
					) 
					& (
						(final_mapped_data.product_title == 'nan')
						| (final_mapped_data.product_title == '')
					)
					& (
						(final_mapped_data.e_product_id.str.len() > 13)
						| (final_mapped_data.e_product_id.str.isdigit() == False)
					)
				)
				)].index
			)

		if final_mapped_data.dropna(how='all').empty:
			logger.info("All records are invalid entries in this file")
			return final_mapped_data

		# Hard coded values
		final_mapped_data['aggregator_name'] = agg_rules['name']
		final_mapped_data['product_type'] = agg_rules['product_type']

		# Check for the scientific notation
		# Remove decimals if file not contains scientific notation.
		scientific_notation_regex = "-?\d\.\d+[Ee][+\-]\d\d?"
		if not final_mapped_data.e_product_id.str.contains(scientific_notation_regex).all():
			final_mapped_data['e_product_id'] = final_mapped_data.e_product_id.str.split('.', expand=True)
			final_mapped_data['p_product_id'] = final_mapped_data.p_product_id.str.split('.', expand=True)

		final_mapped_data['post_code'] = final_mapped_data.post_code.str.split('.', expand=True)
		
		final_mapped_data = self.process_trans_type(final_mapped_data)
		final_mapped_data.loc[(final_mapped_data['sale_type_ori'] == 'na'), 'sale_type_ori'] = 'NA'

		final_mapped_data.loc[(
			(final_mapped_data['new_rental_duration'] == 'NA') | (final_mapped_data['new_rental_duration'] == 'nan')
			), 'new_rental_duration'] = 0
		final_mapped_data['new_rental_duration'] = final_mapped_data['new_rental_duration'].astype('float').astype('int')

		final_mapped_data.loc[(
			(final_mapped_data['old_rental_duration'] == 'NA') | (final_mapped_data['old_rental_duration'] == 'nan')
			), 'old_rental_duration'] = 0
		final_mapped_data['old_rental_duration'] = final_mapped_data['old_rental_duration'].astype('float').astype('int')

		logger.info('Converting negative amounts to positives')
		# Convert price with currency string into float.
		final_mapped_data['price'] = final_mapped_data.price.replace({'\$': '', ',': ''}, regex=True).astype(float)
		final_mapped_data['payment_amount'] = round(final_mapped_data['payment_amount'].astype('float'), 2)

		final_mapped_data['units'] = final_mapped_data['units'].astype('float').astype('int')
		final_mapped_data['sales_net_unit'] = final_mapped_data['sales_net_unit'].astype('float').astype('int')
		final_mapped_data['returns_unit'] = final_mapped_data['returns_unit'].astype('float').astype('int')

		final_mapped_data.loc[(final_mapped_data.disc_code == 'nan'), 'disc_code'] = 'NA'

		final_mapped_data['reporting_date'] = pd.to_datetime(
			final_mapped_data['reporting_date'], format='%d-%m-%Y', infer_datetime_format=True
			)											 
		final_mapped_data['reporting_date'] = final_mapped_data['reporting_date'].dt.date

		logger.info('****************generate staging data done**************')
		return final_mapped_data


	def initialise_processing(self, logger, app_config, rule_config, default_config):
		"""
		To processes data for all Chegg files
		:param logger: For the logging output file.
		:param app_config: Input configuration
		:param rule_config: Rules json
		:param default_config: Default json		
		:return: None
		"""
		# For the final staging output
		agg_name = self.AGG_NAME
		self.LOGGER = logger
		final_edw_data = pd.DataFrame()

		input_list = list(app_config['input_params'])
		input_base_path = input_list[0]['input_base_path']

		# Processing for each file in the fiven folder
		logger.info('\n+-+-+-+-+-+-+Starting Chegg files Processing\n')
		agg_rules = next((item for item in rule_config if (item['name'] == agg_name)), None)

		files_in_s3 = obj_s3_connect.get_files(logger, input_list)
		for each_file in files_in_s3:
			if each_file != '' and each_file.split('.')[-1] != 'txt':
				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')

				final_mapped_data = pd.read_parquet(input_base_path + each_file, engine='pyarrow')

				final_staging_data = self.generate_edw_staging_data(logger, agg_rules, app_config, final_mapped_data)
				
				# Append staging data of current file into final staging dataframe
				final_edw_data = pd.concat([final_edw_data, final_staging_data], ignore_index=True, sort=True)

		final_edw_data = self.group_data(logger, final_edw_data, default_config[0]['group_staging_data'])

		obj_s3_connect.wrangle_data_as_parquet(logger, app_config, final_edw_data)

		logger.info('\n+-+-+-+-+-+-+Finished Processing Chegg files\n')

