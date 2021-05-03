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


class MDAMappedProcessDataBarnes :

    # Function Description :	This function generates staging data for Barnes files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data,data) :


        extracted_data = extracted_data[extracted_data.e_product_id != 'NA']

        extracted_data = pd.DataFrame(extracted_data)
        extracted_data['aggregator_name'] = agg_rules['name']
        extracted_data['product_type'] = agg_rules['product_type']

        extracted_data['e_backup_product_id'] = 'NA'


        current_date_format = agg_rules['date_formats']
        extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'reporting_date',
                                                       default_config)

        logger.info('Processing region of sale')
        extracted_data['country'] = extracted_data['payment_amount_currency'].map(
            agg_rules['filters']['country_iso_values']).fillna('NA')

        if extracted_data['price_currency'].all() == 'NA':
            extracted_data['price_currency'] = 'USD'


        extracted_data['units'] = extracted_data['units'].astype('int')
        if extracted_data['returns_unit'].all() == 'NA':
            extracted_data['returns_unit'] = 0
        else:
            extracted_data['returns_unit'] = extracted_data['returns_unit'].abs()

        extracted_data['units'] = extracted_data['units'] - extracted_data['returns_unit']

        extracted_data['trans_type'] = 'Sales'
        if extracted_data['sale_type_ori'].all() == 'NA':
            extracted_data['sale_type'] = 'Purchase'
        else:
            extracted_data['sale_type'] = extracted_data.apply(
            lambda row : ('Return') if (row['sale_type_ori'] == 'CRE') else ('Purchase'), axis=1)

        if extracted_data['account_name'].all() == 'NA':
            extracted_data['country'] = 'US'


        if extracted_data['price_currency'].all() == 'NA':
            extracted_data['price_currency'] = 'USD'

        # new attributes addition
        extracted_data['source'] = "BARNES"
        extracted_data['price_type'] = 'retail'

        return extracted_data

    # Function Description :	This function processes data for all Barnes files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config) :
    #def initialise_processing(self, logger, app_config, rule_config) :
        # For the final staging output
        agg_name = 'BARNES'
        agg_reference = self
        final_mapped_data = pd.DataFrame()
        input_list = list(app_config['input_params'])

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Barnes files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3 :
            if each_file != '' and each_file.split('.')[-1] != 'txt' :
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')


                final_mapped_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file, rule_config,
                                                                             default_config,final_mapped_data,
                                                                             obj_read_data,
                                                                             obj_pre_process, agg_name, agg_reference)

        # future date issue resolution
        #final_mapped_data = obj_pre_process.process_default_transaction_date(logger, app_config,
        #                                                                      final_mapped_data)
        # Grouping and storing data
        final_mapped_data = obj_gen_attrs.group_data(logger, final_mapped_data,
                                                     default_config[0]['group_staging_data'])

        # obj_s3_connect.store_data(logger, app_config, final_grouped_data)
        final_mapped_data = final_mapped_data.applymap(str)

        #final_mapped_data = final_mapped_data.dropna(subset=['e_product_id'])

        #final_mapped_data = final_mapped_data[final_mapped_data.e_product_id != 'NA']
        final_mapped_data = final_mapped_data.reset_index(drop=True)
        obj_s3_connect.store_data_as_parquet(logger, app_config, final_mapped_data)