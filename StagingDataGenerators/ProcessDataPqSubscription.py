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

import country_converter as coco

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


class ProcessDataPqSubscription :

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, extracted_data) :


        logger.info("Processing transaction and sales types")
        print('sale_type',extracted_data['sale_type'])
        if extracted_data['sale_type'].all() == 'NA' :
            print('inside if')
            extracted_data['sale_type'] = extracted_data.apply(
                lambda row : ('REFUNDS') if (row['net_units'] < 0) else ('PURCHASE'), axis=1)
            extracted_data['trans_type'] = 'SUBSCRIPTION'
        else:
            print('inside else')
            extracted_data.loc[(extracted_data['net_units'] < 0), 'sale_type'] = extracted_data.loc[
                (extracted_data['net_units'] < 0), 'sale_type'].fillna('REFUNDS')
            extracted_data.loc[(extracted_data['net_units'] >= 0), 'sale_type'] = extracted_data.loc[
                (extracted_data['net_units'] >= 0), 'sale_type'].fillna('PURCHASE')
            extracted_data['trans_type'] = 'SUBSCRIPTION'

        logger.info("Transaction and sales types processed")
        return extracted_data

    # Function Description :	This function generates staging data for PqSubscription files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data) :

        extracted_data['aggregator_name'] = "PROQUEST"
        extracted_data['product_type'] = agg_rules['product_type']
        extracted_data['e_backup_product_id'] = 'NA'
        extracted_data['p_backup_product_id'] = 'NA'

        extracted_data['pod'] = 'NA'
        extracted_data['vendor_code'] = 'NA'
        extracted_data['product_format'] = 'NA'
        extracted_data['disc_code'] = 'NA'
        extracted_data['total_rental_duration'] = 0

        if extracted_data['region_of_sale'].all()=='NA':
            extracted_data['region_of_sale'] = 'US'

        current_date_format = agg_rules['date_formats']


        print('trans date',extracted_data['transaction_date'].dtype)
        if extracted_data['transaction_date'].dtype == 'object' and extracted_data['transaction_date'].all() == 'NA':
            extracted_data = self.process_subscription_transaction_date(logger, filename, extracted_data)
            extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format,
                                                           'transaction_date',
                                                           default_config)
        else:
            print("inside else")
            extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'transaction_date',
                                                       default_config)

        if extracted_data['net_units'].all() == 'NA':
            extracted_data['net_units'] = extracted_data.apply(
                lambda row : -1 if (row['revenue_value'] < 0) else 1, axis=1)

        extracted_data['net_units'] = pd.to_numeric(extracted_data['net_units'], errors='coerce')
        extracted_data = self.process_trans_type(logger, extracted_data)
        amount_column = agg_rules['filters']['amount_column']


        extracted_data=self.calculate_final_discount_percentage(extracted_data,logger)
        extracted_data = self.pq_subs_publisher_price_cal(extracted_data)


        extracted_data['tnf_net_price_per_unit'] = round(
            ((1 - extracted_data['disc_percentage']/100) * extracted_data['publisher_price']), 2)

        extracted_data['agg_net_price_per_unit'] = extracted_data.apply(
            lambda row : row[amount_column] / row['net_units'] if (row['net_units'] != 0) else row[
                'tnf_net_price_per_unit'], axis=1)


        extracted_data['agg_net_price_per_unit'] = extracted_data['agg_net_price_per_unit'].abs()
        extracted_data['tnf_net_price_per_unit'] = extracted_data['tnf_net_price_per_unit'].abs()

        logger.info('Computing Sales and Returns')
        extracted_data['total_sales_value'] = extracted_data.apply(
            lambda row : row[amount_column] if (row['net_units'] >= 0) else 0.0, axis=1)
        extracted_data['total_returns_value'] = extracted_data.apply(
            lambda row : row[amount_column] if (row['net_units'] < 0) else 0.0, axis=1)
        extracted_data['total_sales_value'] = extracted_data['total_sales_value'].abs()
        extracted_data['total_returns_value'] = extracted_data['total_returns_value'].abs()

        extracted_data['total_sales_count'] = extracted_data.apply(
            lambda row : row['net_units'] if (row['net_units'] >= 0) else 0, axis=1)
        extracted_data['total_returns_count'] = extracted_data.apply(
            lambda row : row['net_units'] if (row['net_units'] < 0) else 0, axis=1)
        extracted_data['total_returns_count'] = extracted_data['total_returns_count'].abs()
        logger.info('Sales and Return Values computed')

        # new attributes addition
        extracted_data['source'] = "PROQUEST Subscription"
        extracted_data['source_id'] = filename.split('.')[0]
        extracted_data['sub_domain'] = 'NA'
        extracted_data['business_model'] = 'B2B'
        return extracted_data

    # Function Description :	This function processes data for all PQSubscription files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config) :

        # For the final staging output
        final_staging_data = pd.DataFrame()
        input_list = list(app_config['input_params'])

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting PQSubscription files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3 :
            if each_file != '' :
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                # To get the sheet names
                input_file_extn = each_file.split('.')[-1]
                if input_file_extn.lower() in ['xlsx', 'xls']:
                    excel_frame = pd.ExcelFile(input_list[0]['input_base_path'] + each_file)
                    sheets = excel_frame.sheet_names
                    for each_sheet in sheets :
                        logger.info('Processing sheet: %s', each_sheet)
                        input_list[0]['input_sheet_name'] = each_sheet

                        final_staging_data = self.process_relevant_attributes_pq_subscription_data(logger, input_list,
                                                                                                   each_file,
                                                                                                   rule_config,
                                                                                                   final_staging_data,
                                                                                                   default_config)

        # Grouping and storing data
        final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                      default_config[0]['group_staging_data'])

        obj_s3_connect.store_data(logger, app_config, final_grouped_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing pq_subscription_data files\n')

    def process_relevant_attributes_pq_subscription_data(self, logger, input_list, each_file, rule_config, final_staging_data,
                                    default_config) :
        try :
            data = obj_read_data.load_data(logger, input_list, each_file)
            if not data.empty :
                logger.info('Get the corresponding rules object for PQ subscription')
                agg_rules = next((item for item in rule_config if (item['name'] == 'PQ_Subscription')), None)
                data = self.process_excel_sheets(data, agg_rules, logger)

                extracted_data = obj_pre_process.extract_relevant_attributes(logger, data,
                                                                             agg_rules['relevant_attributes'])

                agg_reference = self
                final_staging_data = obj_gen_attrs.process_staging_data(logger, each_file, agg_rules,
                                                                        default_config, extracted_data,
                                                                        final_staging_data,
                                                                        agg_reference, obj_pre_process)
        except KeyError as err :
            logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)

        return final_staging_data


    def pq_subs_publisher_price_cal(self,extracted_data):

        if extracted_data['trans_curr'].all() == 'NA':
            extracted_data['trans_curr']='USD'

        print('publisher price', extracted_data['publisher_price'])
        if extracted_data['publisher_price'].all() == 0.0 :
            extracted_data['publisher_price'] = round((extracted_data["revenue_value"]/extracted_data["net_units"])/(1-extracted_data['disc_percentage']/100),2)
            print('publisher price after processing', extracted_data['publisher_price'])

        extracted_data['publisher_price'] = abs(round((extracted_data['publisher_price']), 2))

        return extracted_data

    def process_excel_sheets(self,data,agg_rules,logger):

        logger.info("pocessing headers and replacing column names start")
        data = self.process_header_templates(logger, data)
        data = data.dropna(how='all')
        data.columns = data.columns.str.strip()
        data.columns = data.columns.str.lower()
        data = obj_gen_attrs.replace_column_names(logger, agg_rules, data)
        logger.info("pocessing headers and replacing column names end")
        return data

    def process_subscription_transaction_date(self,logger,filename,extracted_data):
        logger.info('Processing transaction_dates for subscription')
        q1 = ["q1","jan", "feb", "mar"]
        q2 = ["q2","apr", "may", "jun"]
        q3 = ["q3","jul", "aug", "sep"]
        q4 = ["q4","oct", "nov", "dec"]
        year = self.process_year_from_filename(logger,filename)
        if any(x in filename.lower() for x in q1) :
            extracted_data['transaction_date'] = '31-03-'+ year
        elif any(x in filename.lower() for x in q2) :
            extracted_data['transaction_date'] = '30-06-'+ year
        elif any(x in filename.lower() for x in q3) :
            extracted_data['transaction_date'] = '30-09-'+ year
        elif any(x in filename.lower() for x in q4) :
            extracted_data['transaction_date'] = '31-12-'+ year
        extracted_data['transaction_date'] = pd.to_datetime(extracted_data['transaction_date'])
        return extracted_data

    def process_year_from_filename(self,logger,filename):
        logger.info('Deriving year from filename for subscription')
        q1 = ["2020"]
        q2 = ["2019", "19"]
        q3 = ["2018", "18"]
        q4 = ["2017", "17"]
        if any(x in filename.lower() for x in q1) :
            return '2020'
        elif any(x in filename.lower() for x in q2) :
            return '2019'
        elif any(x in filename.lower() for x in q3) :
            return '2018'
        elif any(x in filename.lower() for x in q4) :
            return '2017'


    def calculate_final_discount_percentage(self,extracted_data,logger):
        logger.info('calculating discount percentage start')
        if (extracted_data['disc_percentage'].all() == 'NA') :
            extracted_data['disc_percentage'] = 0

        else:
            extracted_data['disc_percentage'] = (extracted_data['disc_percentage']).replace('%', '', regex=True)
            extracted_data['disc_percentage'] = extracted_data['disc_percentage'].astype('str')
            extracted_data['disc_percentage'] = (extracted_data['disc_percentage']).str.rstrip()
            extracted_data['disc_percentage'] = pd.to_numeric(extracted_data['disc_percentage'])

            extracted_data['disc_percentage'] = extracted_data.apply(
                lambda row : (1 - row['disc_percentage']) * 100 if (row['disc_percentage'] < 1) else 100 - row[
                    'disc_percentage'], axis=1)

        extracted_data['disc_percentage'] = (extracted_data['disc_percentage']).abs()
        logger.info('calculating discount percentage end')
        return extracted_data

    def process_header_templates(self, logger, data):

        logger.info('Removing metadata and blanks')
        raw_data = data
        for i, row in raw_data.iterrows() :
            if row.notnull().all() :
                data = raw_data.iloc[(i + 1) :].reset_index(drop=True)
                data.columns = list(raw_data.iloc[i])
                break
        mandatory_columns = data.columns[0]
        data = data.dropna(subset=[mandatory_columns], how='all')

        logger.info('Discarding leading/trailing spacs from the columns')
        data.columns = data.columns.str.strip()

        logger.info('Actual data extracted')
        return data