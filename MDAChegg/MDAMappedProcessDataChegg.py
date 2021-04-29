#################################
#			IMPORTS				#
#################################


import pandas as pd

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


class MDAMappedProcessDataChegg(GenerateStagingAttributes):
	"""
		Class used to convert source data into mapped data
	"""

	# Class variables
	AGG_NAME = 'CHEGG'

	def process_trans_type(self, extracted_data):
		"""
		To process the transaction type
		:param extracted_data: pr-processed_data
		:return: extracted dataframe
		"""
		extracted_data['trans_type'] = extracted_data.apply(
			lambda row : (
				'rental' if(row['trans_type_ori'] in ('rental', 'extension')) else (
					'sales' if row['trans_type_ori'] == 'sell' else row['trans_type_ori']
					)
				),
			axis=1)

		return extracted_data


	def process_sale_type(self, filename, extracted_data):
		"""
		To process the sale type
		:param extracted_data: pr-processed_data
		:return: extracted dataframe
		"""
		def process_for_rental(row):
			if row['trans_type'] == 'rental':
				return 'checkout' if (row['trans_type_ori'] == 'rental' and row['sale_type_ori'] == 'na') else (
						'extension' if (row['trans_type_ori'] == 'extension' and row['sale_type_ori'] == 'na') else (
							'cancellation' if (
								row['trans_type_ori'] in ('rental', 'extension') 
								and row['sale_type_ori'] == 'cancellation'
								) else row['sale_type_ori']
						)
					)
			elif row['trans_type'] == 'sales':
				return 'purchase' if row['sale_type_ori'] == 'na' else (
							'return' if row['sale_type_ori']== 'cancellation' else row['sale_type_ori']
						)
			else:
				return row['sale_type_ori']

		extracted_data['sale_type'] = extracted_data.apply(lambda row: process_for_rental(row), axis=1)
		return extracted_data


	def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data, **kwargs):
		"""
		Generates staging data for Chegg files
		:param logger: For the logging output file.
		:param filename: Name of the file
		:param agg_rules: Rules json
		:param default_config: Default json
		:param extracted_data: pr-processed_data
		:return: extracted dataframe
		"""
		extracted_data['aggregator_name'] = agg_rules['name']
		extracted_data['product_type'] = agg_rules['product_type']		
		extracted_data['sale_type_ori'] = extracted_data.sale_type_ori.str.lower()

		if 'rental' in filename.lower() or 'subs' in filename.lower():
			extracted_data['trans_type'] = 'subscription'
			extracted_data['trans_type_ori'] = 'subscription'

			extracted_data['sale_type'] = extracted_data.apply(
				lambda row: (
					'subscription' if not row['sale_type_ori'] else  (
						'cancellation' if row['sale_type_ori'] == 'cancellation' else row['sale_type_ori']
					)
				),
			axis=1)
		else:
			extracted_data['old_rental_duration'] = 0
			extracted_data['new_rental_duration'] = 0

			rental_values = {k.replace(' ', ''): v for k, v in agg_rules['filters']['rental_values'].items()}
			extracted_data['new_rental_duration'] = extracted_data['term_description'].str.replace(" ","").map(rental_values).fillna(0)

			if extracted_data['trans_type_ori'].all() == 'NA':
				for map_key, map_value in agg_rules['filters']['map_rental_duration_trans_type'].items():
					extracted_data.loc[(
						extracted_data.term_description.str.contains(map_key, case=False, regex=False)
						), 'trans_type_ori'] = map_value

			# Process transaction type
			extracted_data['trans_type_ori'] = extracted_data.trans_type_ori.str.lower()
			extracted_data = self.process_trans_type(extracted_data)

			# Process sales type
			extracted_data = self.process_sale_type(filename, extracted_data)

		logger.info('Converting negative amounts to positives')
		# Convert price with currency string into float.
		extracted_data['price'] = extracted_data['price'].replace({'\$': '', ',': ''}, regex=True).astype(float).abs()

		current_date_format = agg_rules['date_formats']
		extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'reporting_date', default_config)

		# new attributes addition
		extracted_data['source'] = "CHEGG EBook"
		extracted_data['sub_domain'] = 'NA'
		extracted_data['business_model'] = 'B2C'
		return extracted_data

	
	def applying_aggregator_rules(self, logger, input_list, each_file, rule_config, default_config,
								  final_staging_data, obj_read_data, obj_pre_process,
								  agg_name, agg_reference):
		"""
		To processes data by applying aggregator rules.
		:param logger: For the logging output file. 
		:param input_list: List of input configuration
		:param each_file: Name of the file
		:param rule_config: Rules json
		:param default_config: Default json
		:param final_staging_data: final extracted dataframe
		:param obj_read_data: Obj of read data class
		:param obj_pre_process: Obj of pre process class
		:param agg_name: Name of the aggregator
		:param agg_reference: Instance of the aggregator class
		:return: None
		"""
		logger.info('\n+-+-+-+-+-+-+Starting Processing %s files\n', agg_name)
		try:
			data = obj_read_data.load_data(logger, input_list, each_file)
			if not data.empty:
				logger.info('Get the corresponding rules object for ' + agg_name)
				agg_rules = obj_process_core.get_rules_object(rule_config, 'rental', 'CHEGG', each_file,
																  '/Chegg Rental', '/Chegg')
				
				data = data.dropna(how='all')
				data.columns = data.columns.str.strip()

				data = self.replace_column_names(logger, agg_rules, data)

				mandatory_columns = agg_rules['filters']['mandatory_columns']
				data[mandatory_columns] = data[mandatory_columns].fillna(value='NA')

				extracted_data = obj_pre_process.extract_relevant_attributes(logger, data,
																			 agg_rules['relevant_attributes'])
				extracted_data = self.replace_column_names(logger, agg_rules, extracted_data)

				final_staging_data = self.process_staging_data(logger, each_file, agg_rules,
															   default_config,
															   extracted_data, final_staging_data,
															   agg_reference, obj_pre_process, 
															   data=data, input_list=input_list)
		except KeyError as err:
			logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)

		logger.info('\n+-+-+-+-+-+-+Finished Processing ' + agg_name + ' files\n')
		return final_staging_data


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
		agg_reference = self
		final_staging_data = pd.DataFrame()
		input_list = list(app_config['input_params'])
		
		# Processing for each file in the fiven folder
		logger.info('\n+-+-+-+-+-+-+Starting Chegg files Processing\n')
		files_in_s3 = obj_s3_connect.get_files(logger, input_list)

		for each_file in files_in_s3:
			logger.info('\n+-+-+-+-+-+-+')
			logger.info(each_file)
			logger.info('\n+-+-+-+-+-+-+')
			
			input_file_extn = each_file.split('.')[-1]

			if (input_file_extn.lower() == 'xlsx') or (input_file_extn.lower() == 'xls'):

				excel_frame = pd.ExcelFile(input_list[0]['input_base_path'] + each_file)
				sheets = excel_frame.sheet_names
				for each_sheet in sheets:
					logger.info('Processing sheet: %s', each_sheet)
					input_list[0]['input_sheet_name'] = each_sheet

					final_staging_data = self.applying_aggregator_rules(logger, input_list, each_file, rule_config,
																		default_config, final_staging_data,
																		obj_read_data, obj_pre_process, 
																		agg_name, agg_reference
																		)
			elif input_file_extn.lower() == 'csv':
				final_staging_data = self.applying_aggregator_rules(logger, input_list, each_file, rule_config,
																		default_config, final_staging_data,
																		obj_read_data, obj_pre_process, 
																		agg_name, agg_reference
																		)
			else:
				logger.info("ignoring processing the " + each_file + " as it is not a csv or excel file")

		# future date issue resolution
		# final_staging_data = obj_pre_process.process_default_transaction_date(logger, app_config, final_staging_data)

		# Grouping and storing data
		final_grouped_data = self.group_data(logger, final_staging_data, default_config[0]['group_staging_data'])
		final_grouped_data = final_grouped_data.astype(str)
		obj_s3_connect.wrangle_data_as_parquet(logger, app_config, final_grouped_data)

		logger.info('\n+-+-+-+-+-+-+Finished Processing Chegg files\n')
