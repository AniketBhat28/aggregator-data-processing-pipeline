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


class MDAStagingProcessDataBlackwells:

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, final_mapped_data) :

        logger.info("Processing transaction and sales types")
        final_mapped_data['trans_type'] = 'Sales'

        logger.info("Transaction and sales types processed")

        return final_mapped_data

    # Function Description :	This function generates staging data for Gardners files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_edw_staging_data(self, logger, agg_rules, final_mapped_data) :

        logger.info('***********generate staging data started*******************')
        final_mapped_data = final_mapped_data.replace('nan', 'NA')
        final_mapped_data['aggregator_name'] = agg_rules['name']
        final_mapped_data['product_type'] = agg_rules['product_type']
        final_mapped_data['country'] = 'GB'
        final_mapped_data = self.process_trans_type(logger, final_mapped_data)

        # Drop invalid rows
        final_mapped_data = final_mapped_data.drop(
            final_mapped_data[(final_mapped_data.e_product_id == 'NA')].index)

        final_mapped_data['price'] = final_mapped_data.price.replace({u'\xa0' : '', '£' : ''}, regex=True).astype(float)
        
        final_mapped_data['payment_amount'] = final_mapped_data['payment_amount'].astype('float')
        final_mapped_data['current_discount_percentage'] = final_mapped_data['current_discount_percentage'].astype('float')
        final_mapped_data['tax_percentage'] = final_mapped_data['tax_percentage'].astype(
            'float')
        final_mapped_data['units'] = final_mapped_data['units'].astype('float').astype('int')
        final_mapped_data['e_product_id'] = final_mapped_data.e_product_id.str.split('.', expand=True)
        final_mapped_data['external_purchase_order'] = final_mapped_data.external_purchase_order.str.split('.', expand=True)
        final_mapped_data['reporting_date'] = pd.to_datetime(final_mapped_data['reporting_date'],
                                                             format='%d-%m-%Y', infer_datetime_format=True)
        # print('reporting date done')
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
        agg_name = 'BLACKWELLS'
        final_edw_data = pd.DataFrame()

        input_list = list(app_config['input_params'])
        input_base_path = input_list[0]['input_base_path']

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Blackwells files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        agg_rules = next((item for item in rule_config if (item['name'] == agg_name)), None)
        for each_file in files_in_s3 :
            if each_file != '' and each_file.split('.')[-1] != 'txt' :
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')
                final_mapped_data = pd.read_parquet(input_base_path + each_file, engine='pyarrow')

                final_staging_data = self.generate_edw_staging_data(logger,agg_rules, final_mapped_data)
                # Append staging data of current file into final staging dataframe
                final_edw_data = pd.concat([final_edw_data, final_staging_data], ignore_index=True, sort=True)

        final_edw_data = obj_gen_attrs.group_data(logger, final_edw_data,
                                                      default_config[0]['group_staging_data'])

        obj_s3_connect.store_data_as_parquet(logger, app_config, final_edw_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing Gardners files\n')
