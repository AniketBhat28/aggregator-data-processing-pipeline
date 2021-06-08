#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np

from ReadWriteData.ReadData import ReadData
from Preprocessing.ProcessCore import ProcessCore
from Preprocessing.PreProcess import PreProcess
from ReadWriteData.ConnectToS3 import ConnectToS3
from AttributeGenerators.GenerateStagingAttributes import GenerateStagingAttributes



#################################
#		GLOBAL VARIABLES		#
#################################


obj_read_data = ReadData()
obj_process_core = ProcessCore()
obj_pre_process = PreProcess()
obj_s3_connect = ConnectToS3()
obj_gen_attrs = GenerateStagingAttributes()



#################################
#		CLASS FUNCTIONS			#
#################################


class MDAMappedProcessDataIngramVS:

	# Function Description :	This function processes transaction and sales types
	# Input Parameters : 		logger - For the logging output file.
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted_data - extracted staging data
	def process_trans_type(self, logger, extracted_data):

		logger.info("Processing transaction and sales types")
		extracted_data['sale_type'] = extracted_data.apply(
			lambda row : ('Return') if (row['units'] < 0) else ('Purchase'), axis=1)
		extracted_data['trans_type'] = extracted_data.apply(lambda row: ('Sales') if(row['rental_duration'] == 0) else 'Rental', axis=1)

		logger.info("Transaction and sales types processed")
		return extracted_data


	# Function Description :	This function generates staging data for Gardners files
	# Input Parameters : 		logger - For the logging output file.
	#							filename - Name of the file
	#							agg_rules - Rules json
	#							default_config - Default json
	#							extracted_data - pr-processed_data
	# Return Values : 			extracted_data - extracted staging data
	def generate_staging_output(self, logger, agg_rules, default_config, extracted_data,):

		extracted_data['aggregator_name'] = agg_rules['fullname']
		extracted_data['product_type'] = agg_rules['product_type']
		extracted_data['price_type'] = 'Retail Price'
		current_date_format = agg_rules['date_formats']
		extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'reporting_date', default_config)

		if extracted_data['rental_duration'].all() == 'NA' :
			extracted_data['rental_duration'] = 0

		extracted_data = self.process_trans_type(logger, extracted_data)

		# new attributes addition
		extracted_data['source'] = "Ingram/VitalSource"
		extracted_data = extracted_data.replace(np.nan, 'NA')

		return extracted_data

	# Function Description :	This function processes data for all Gardners files
	# Input Parameters : 		logger - For the logging output file.
	#							app_config - Configuration
	#							rule_config - Rules json
	#							default_config - Default json
	# Return Values : 			None
	def initialise_processing(self, logger, app_config, rule_config, default_config):

		# For the final staging output
		agg_name = 'INGRAM'
		agg_reference = self
		final_mapped_data = pd.DataFrame()
		input_list = list(app_config['input_params'])


		# Processing for each file in the fiven folder
		logger.info('\n+-+-+-+-+-+-+Starting Gardners files Processing\n')
		files_in_s3 = obj_s3_connect.get_files(logger, input_list)
		for each_file in files_in_s3:
			if each_file != '' and each_file.split('.')[-1] != 'txt':
				logger.info('\n+-+-+-+-+-+-+')
				logger.info(each_file)
				logger.info('\n+-+-+-+-+-+-+')
				final_mapped_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file, rule_config,
																		 default_config, final_mapped_data,
																		 obj_read_data,
																		 obj_pre_process,agg_name, agg_reference)

		final_mapped_data = obj_gen_attrs.group_data(logger, final_mapped_data,
								 default_config[0]['group_staging_data'])
		final_mapped_data = final_mapped_data.applymap(str)

		obj_s3_connect.store_data_as_parquet(logger, app_config, final_mapped_data)
		logger.info('\n+-+-+-+-+-+-+Finished Processing Ingram files\n')