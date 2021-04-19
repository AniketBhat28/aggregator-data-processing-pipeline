#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import decimal
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


class MDAMappedProcessDataFollett :

    # Function Description :	This function generates staging data for Follett files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data


    def float_to_string(self,number, precision=2):
        return '{0:.1f}'.format( number, prec=precision,).rstrip('0').rstrip('.') or '0'

    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data,data) :



        print('dictionary',agg_rules['filters']['country_iso_values'])

        extracted_data['aggregator_name'] = agg_rules['name']
        extracted_data['product_type'] = agg_rules['product_type']

        extracted_data['price_currency'] = 'NA'
        extracted_data['price_type'] = 'Retail Price'

        extracted_data['country'] = 'NA'
        extracted_data['trans_type'] = 'SALES'
        extracted_data['sale_type'] = extracted_data['sale_type_ori']


        # new attributes addition
        extracted_data['source'] = "FOLLET"
        extracted_data['source_id'] = filename.split('.')[0]
        extracted_data['sub_domain'] = 'NA'
        extracted_data['external_purchase_order'] =  extracted_data['external_purchase_order'].replace('NA', 0)
        extracted_data['external_transaction_number'] = extracted_data['external_transaction_number'].replace('NA', 0)

        extracted_data['external_purchase_order'] =  extracted_data['external_purchase_order'].astype("float")
        extracted_data['external_purchase_order'] = extracted_data['external_purchase_order'].apply(self.float_to_string)
        # extracted_data['external_purchase_order'] = extracted_data['external_purchase_order'].replace({np.nan: 'NA'})
        extracted_data['external_purchase_order'] = extracted_data.external_purchase_order.astype(str)
        extracted_data['external_transaction_number'] = extracted_data['external_transaction_number'].astype("float")
        # extracted_data['external_transaction_number'] = extracted_data['external_transaction_number'].replace({np.nan: 0})
        extracted_data['external_transaction_number'] = extracted_data["external_transaction_number"].apply(self.float_to_string)
        extracted_data['external_transaction_number'] = extracted_data.external_transaction_number.astype(str)\
            # .str.split('.', expand=True)
        return extracted_data

    # Function Description :	This function processes data for all Follett files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config) :

        # For the final staging output
        agg_name = 'FOLLET'
        agg_reference = self
        final_staging_data = pd.DataFrame()
        input_list = list(app_config['input_params'])

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Follett files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3 :
            if each_file != '' and each_file.split('.')[-1] != 'txt' :
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                final_staging_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file, rule_config,
                                                                             default_config, final_staging_data,
                                                                             obj_read_data,
                                                                             obj_pre_process, agg_name, agg_reference)


        final_mapped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                     default_config[0]['group_staging_data'])


        final_mapped_data = final_mapped_data.applymap(str)
        obj_s3_connect.wrangle_data_as_parquet(logger, app_config, final_mapped_data)

        logger.info('\n+-+-+-+-+-+-+Finished Processing Follett files\n')