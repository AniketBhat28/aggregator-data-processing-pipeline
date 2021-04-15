#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
from pandas import ExcelFile
import json
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


class MDAMappedProcessDataProquest:

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, extracted_data):

        logger.info("Processing transaction and sales types")
        extracted_data['trans_type'] = np.where(
            extracted_data['sale_type'].str.upper().isin(['ATO_LOAN', 'ATO LOAN', 'STL']), 'Rental', 'Sales')

        logger.info("Transaction and sales types processed")
        return extracted_data

    def process_sale_type(self, logger, extracted_data):

        logger.info("Processing  sales types")
        extracted_data['sale_type'] = extracted_data.apply(
            lambda row: 'REFUNDS' if (row['Payment_Amount'] < 0) else 'PURCHASE', axis=1)

        logger.info("sales types processed")
        return extracted_data

    def process_dates(self, logger, extracted_data, filename):
        logger.info("Processing reporting date")
        if 'MRD' in filename or ('QRD' in filename and ('2020' in filename or '2019' in filename)):
            extracted_data["reporting_date"] = extracted_data['repo_date'].astype(str).str[:10]
        else:
            if '2017' in filename:
                year_no = '2017'
            elif '2018' in filename:
                year_no = '2018'
            elif '2019' in filename:
                year_no = '2019'
            else:
                year_no = '2020'

            if 'Q1' in filename:
                reporting_dt = year_no + '-01-01'
            elif 'Q2' in filename:
                reporting_dt = year_no + '-04-01'
            elif 'Q3' in filename:
                reporting_dt = year_no + '-07-01'
            elif 'Q4' in filename:
                reporting_dt = year_no + '-10-01'
            else:
                reporting_dt = '2017-10-01'
            logger.info("reporting date : " + reporting_dt)
            extracted_data["reporting_date"] = reporting_dt

    # Function Description :	This function generates staging data for PqCentral files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data):

        extracted_data['aggregator_name'] = 'PROQUEST'
        extracted_data['product_type'] = 'Electronic'

        if agg_rules['name'] == 'PROQUEST-EBRARY-STL':
            extracted_data['trans_type'] = 'Rental'
            extracted_data['sale_type'] = 'STL'

        if agg_rules['name'] == 'PROQUEST-EBRARY-PERPETUAL':
            extracted_data['trans_type'] = 'Sales'
            extracted_data['sale_type'] = 'Perpetual'
        if agg_rules['name'] == 'PROQUEST-EBRARY-CORPORATE':
            extracted_data['trans_type'] = 'Subscription'
            extracted_data['sale_type'] = 'Purchase'
        if agg_rules['name'] == 'PROQUEST-EBRARY-SUB':
            extracted_data['External_Product_ID_type'] = 'Book ID'
            extracted_data['trans_type'] = 'Subscription'
            self.process_sale_type(logger, extracted_data)
        extracted_data['Payment_Amount_Currency'] = extracted_data['Price_currency']
        self.process_dates(logger, extracted_data, filename)
        if agg_rules['name'] == 'PROQUEST-EBL':
            extracted_data['price_type'] = 'Adjusted Retail Price'
            self.process_trans_type(logger, extracted_data)
        extracted_data['units'] = pd.to_numeric(extracted_data['units'], errors='coerce')

        return extracted_data

    # Function Description :	This function processes data for all PqCentral files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, base_path, aggregator):

        # For the final staging output
        with open(base_path +  '/MDAProquestRulesVal.json') as f:
            rule_config = json.load(f)

        with open(base_path +  '/MDAProquestDefault.json') as f:
            default_config = json.load(f)

        agg_reference = self
        final_staging_data = pd.DataFrame()
        input_list = list(app_config['input_params'])
        agg_name = input_list[0]['aggregator_file_type']
        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting PQCentral files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3:
            if each_file == 'OCT/SUB/2018-Q3_3856.xlsx' and 'Do not process' not in each_file and 'SUB' in each_file and 'Corporate' not in each_file:
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                input_file_extn = each_file.split('.')[-1]

                if (input_file_extn.lower() == 'xlsx') or (input_file_extn.lower() == 'xls'):
                    app_config['output_params']['output_filename'] = each_file.replace('/', '_').replace('.xlsx',
                                                                                                         '') + '.snappy.parquet'
                    # To get the sheet names
                    excel_frame = ExcelFile(input_list[0]['input_base_path'] + each_file)
                    sheets = excel_frame.sheet_names
                    for each_sheet in sheets:
                        logger.info('Processing sheet: %s', each_sheet)
                        input_list[0]['input_sheet_name'] = each_sheet
                        final_staging_data = obj_gen_attrs.applying_mapped_aggregator_rules(logger, input_list,
                                                                                            each_file,
                                                                                            rule_config,
                                                                                            default_config,
                                                                                            final_staging_data,
                                                                                            obj_read_data,
                                                                                            obj_pre_process, agg_name,
                                                                                            agg_reference)

                elif input_file_extn.lower() == 'csv':
                    final_staging_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file,
                                                                                 rule_config,
                                                                                 default_config, final_staging_data,
                                                                                 obj_read_data,
                                                                                 obj_pre_process, agg_name,
                                                                                 agg_reference)
                else:
                    logger.info("ignoring processing the " + each_file + " as it is not a csv or excel file")

                # # Grouping and storing data
                # # if input_file_extn.lower() != 'txt':
                #     # future date issue resolution
            # final_staging_data = obj_pre_process.process_default_transaction_date(logger, app_config,
            #                                                                       final_staging_data)
            #     if input_file_extn.lower() in ('xlsx','csv','xls') :

        final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                      default_config[0]['group_staging_data'])
        final_grouped_data = final_grouped_data.astype(str)
        obj_s3_connect.store_data_as_parquet(logger, app_config, final_grouped_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing PQCentral files\n')
