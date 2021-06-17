#################################
#			IMPORTS				#
#################################


import ast
import pandas as pd
import numpy as np
import requests

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


class MDAMappedProcessDataEbsco :

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, extracted_data,file_type) :


        logger.info("Processing transaction and sales types")
        extracted_data['trans_type_ori'] = 'NA'
        if(file_type =='subs'):

            extracted_data['trans_type'] = 'Subscription'
            extracted_data['sale_type'] = 'Subscription'
        else:
            extracted_data['trans_type'] = 'Sales'

        logger.info("Transaction and sales types processed")
        return extracted_data


    # Function Description :	This function generates staging data for Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data,data) :


        ext = filename.split('.')[-1]
        file_type='NA'
        sub_file = ["sub", "add","roy"]
        if any(x in filename.lower() for x in sub_file) :
            file_type='subs'
            if (ext != 'csv'):
                extracted_data = self.process_subscription_transaction_date(logger,filename,extracted_data)

        extracted_data['aggregator_name'] = agg_rules['name']
        extracted_data['product_type'] = agg_rules['product_type']

        extracted_data['price_type'] = 'Adjusted Retail Price'

        extracted_data = self.check_csv_transaction_date(ext,extracted_data)



        if extracted_data['price'].all()=='NA':
            extracted_data['price'] = "0"

        if extracted_data['publisher_price_ori'].all()=='NA':
            extracted_data['publisher_price_ori'] = "0"

        if extracted_data['units'].all()=='NA':
            extracted_data['units'] = "1"

        if extracted_data['list_price_multiplier'].all()=='NA':
            extracted_data['list_price_multiplier'] = "1"

        if extracted_data['current_discount_percentage'].all()=='NA':
            extracted_data['current_discount_percentage'] = "0"




        extracted_data = extracted_data.replace(np.nan, 'NA')

        if extracted_data['reporting_date'].all()=='NA':
            extracted_data = self.process_subscription_transaction_date(logger, filename, extracted_data)

        extracted_data = obj_gen_attrs.process_isbn(logger, extracted_data, 'digital_isbn', 'e_product_id',
                                                    'e_backup_product_id', 'NA')
        extracted_data = obj_gen_attrs.process_isbn(logger, extracted_data, 'physical_isbn', 'p_product_id',
                                                    'p_backup_product_id', 'NA')
        extracted_data = obj_gen_attrs.generate_misc_isbn(logger, extracted_data, 'digital_isbn', 'physical_isbn',
                                                          'misc_product_ids', 'NA')

        # print('replacing nans')
        extracted_data = extracted_data.replace('nan', 'NA')

        extracted_data = self.process_trans_type(logger, extracted_data, file_type)

        # new attributes addition
        extracted_data['source'] = "EBSCO"



        return extracted_data

    # Function Description :	This function processes data for all Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config) :

        # For the final staging output
       # agg_reference = self
        final_staging_data = pd.DataFrame()
        input_list = list(app_config['input_params'])

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Ebsco files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)

        for each_file in files_in_s3 :
            if each_file != '' :
                logger.info('\n+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                # To get the sheet names
                input_file_extn = each_file.split('.')[-1]
                if (input_file_extn.lower()  in ['xlsx','xls']) :
                    excel_frame = pd.ExcelFile(input_list[0]['input_base_path'] + each_file)
                    sheets = excel_frame.sheet_names
                    logger.info('\n+-+-+-sheets name+-+-+-+')
                    logger.info(sheets)
                    logger.info('\n+-+-+-+-+-+-+')
                    for each_sheet in sheets :
                        logger.info('Processing sheet: %s', each_sheet)
                        input_list[0]['input_sheet_name'] = each_sheet

                        final_staging_data = self.process_relevant_attributes_ebsco_data(logger, input_list, each_file,
                                                                                         rule_config,
                                                                                         final_staging_data,
                                                                                         default_config, 'excel')

                else:
                    if (self.process_only_quarter_and_subscription_file(each_file, logger)):
                            final_staging_data = self.process_relevant_attributes_ebsco_data(logger, input_list,
                                                                                             each_file, rule_config,
                                                                                             final_staging_data,
                                                                                             default_config, 'csv')

        # Grouping and storing data
        final_mapped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                     default_config[0]['group_staging_data'])


        final_mapped_data = final_mapped_data.applymap(str)
        obj_s3_connect.store_data_as_parquet(logger, app_config, final_mapped_data)

        logger.info('\n+-+-+-+-+-+-+Finished Processing Ebsco files\n')

    def process_relevant_attributes_ebsco_data(self, logger, input_list, each_file, rule_config, final_staging_data,
                                    default_config,file_type) :
        try :
            data = obj_read_data.load_data(logger, input_list, each_file)
            if not data.empty :
                logger.info('Get the corresponding rules object for Ebsco')
                agg_rules = next((item for item in rule_config if (item['name'] == 'EBSCO')), None)

                if(file_type == 'excel'):
                    data = self.process_excel_sheets(data,each_file,agg_rules,logger)
                else:
                    data = data.dropna(how='all')
                    data.columns = data.columns.str.strip()
                    data.columns = data.columns.str.lower()
                    data = obj_gen_attrs.replace_column_names(logger, agg_rules, data)
                    data = data.dropna(subset=['eisbn'], how='all')

                extracted_data = obj_pre_process.extract_relevant_attributes(logger, data,
                                                                             agg_rules['relevant_attributes'])


                agg_reference = self
                final_staging_data = obj_gen_attrs.process_staging_data(logger, each_file, agg_rules,
                                                                        default_config, extracted_data,
                                                                        final_staging_data,
                                                                        agg_reference, obj_pre_process,data,input_list)

        except KeyError as err :
            logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)

        return final_staging_data

    def process_only_quarter_and_subscription_file(self, filename, logger) :
        matches = ["q1", "q2", "q3", "q4", "roy", "sub"]
        if any(x in filename.lower() for x in matches) :
            return True
        else:
            logger.info('This is not a quarter file or subscription data,so not processed')
            return False


    def process_excel_sheets(self,data,each_file,agg_rules,logger):
        sub_file = ["sub", "add","roy"]
        if any(x in each_file.lower() for x in sub_file) :
            mandatory_columns = ['title']
        else:
            mandatory_columns = agg_rules['filters']['mandatory_columns']

        data = obj_pre_process.process_header_templates(logger, data, [mandatory_columns[0].upper()])
        data = data.dropna(how='all')
        data.columns = data.columns.str.strip()
        data.columns = data.columns.str.lower()
        data = obj_gen_attrs.replace_column_names(logger, agg_rules, data)
        return data

    def process_subscription_transaction_date(self,logger,filename,extracted_data):
        logger.info('Processing transaction_dates for subscription')
        filename = filename.split('/')[-1]
        q1 = ["jan", "feb", "mar", "q1"]
        q2 = ["apr", "may", "jun","q2"]
        q3 = ["jul", "aug", "sep","q3"]
        q4 = ["oct", "nov", "dec","q4"]
        if any(x in filename.lower() for x in q1) :
            extracted_data['reporting_date'] = '31-03-'+ filename.split('.')[0][-4:]
        elif any(x in filename.lower() for x in q2) :
            extracted_data['reporting_date'] = '30-06-'+ filename.split('.')[0][-4:]
        elif any(x in filename.lower() for x in q3) :
            extracted_data['reporting_date'] = '30-09-'+ filename.split('.')[0][-4:]
        elif any(x in filename.lower() for x in q4) :
            extracted_data['reporting_date'] = '31-12-'+ filename.split('.')[0][-4:]
        return extracted_data

    def check_csv_transaction_date(self,ext,extracted_data):
        if ext == 'csv' and extracted_data['reporting_date'].all()=='NA' :
            extracted_data['reporting_date'] = extracted_data["alternate_date"]
        return extracted_data

    def calculate_final_discount_percentage(self,extracted_data,logger):
        if (extracted_data['current_discount_percentage'].all() == 'NA') :
            extracted_data['current_discount_percentage'] = 0

        return extracted_data
