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


class ProcessDataEbsco :

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, extracted_data,file_type) :


        logger.info("Processing transaction and sales types")

        if(file_type =='subs'):
            if extracted_data['sale_type'].all() == 'NA' :
                extracted_data['sale_type'] = extracted_data.apply(
                lambda row : ('REFUNDS') if (row['net_units'] < 0) else ('PURCHASE'), axis=1)
                extracted_data['trans_type'] = 'SUBSCRIPTION'

        else:
            extracted_data.loc[(extracted_data['net_units'] < 0), 'sale_type'] = extracted_data.loc[
                (extracted_data['net_units'] < 0), 'sale_type'].fillna('REFUNDS')
            extracted_data.loc[(extracted_data['net_units'] >= 0), 'sale_type'] = extracted_data.loc[
                (extracted_data['net_units'] >= 0), 'sale_type'].fillna('PURCHASE')
            extracted_data['trans_type'] = extracted_data.apply(
                lambda row : ('RETURNS') if (row['net_units'] < 0) else ('SALE'), axis=1)

        logger.info("Transaction and sales types processed")
        return extracted_data

    # Function Description :	This function generates staging data for Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data) :

        ext = filename.split('.')[-1]
        file_type='NA'
        sub_file = ["sub", "add","roy"]
        if any(x in filename.lower() for x in sub_file) :
            file_type='subs'
            if (ext != 'csv'):
                extracted_data = self.process_subscription_transaction_date(logger,filename,extracted_data)

        extracted_data['aggregator_name'] = agg_rules['name']
        extracted_data['product_type'] = agg_rules['product_type']

        extracted_data['pod'] = 'N'
        extracted_data['vendor_code'] = 'NA'
        extracted_data['product_format'] = 'NA'
        extracted_data['disc_code'] = 'NA'
        extracted_data['total_rental_duration'] = 0

        if extracted_data['region_of_sale'].all()=='NA':
            extracted_data['region_of_sale'] = 'US'

        current_date_format = agg_rules['date_formats']

        extracted_data = self.check_csv_transaction_date(ext,extracted_data)

        extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'transaction_date',
                                                       default_config)

        extracted_data = obj_gen_attrs.process_isbn(logger, extracted_data, 'digital_isbn', 'e_product_id',
                                                    'e_backup_product_id', 'NA')
        extracted_data = obj_gen_attrs.process_isbn(logger, extracted_data, 'physical_isbn', 'p_product_id',
                                                    'p_backup_product_id', 'NA')
        extracted_data = obj_gen_attrs.generate_misc_isbn(logger, extracted_data, 'digital_isbn', 'physical_isbn',
                                                          'misc_product_ids', 'NA')
        extracted_data = extracted_data.replace('nan', 'NA')

        extracted_data['imprint'] = extracted_data['imprint'].str.split('\(').str[0]
        extracted_data['imprint'] = extracted_data['imprint'].str.split('\[').str[0]


        amount_column = agg_rules['filters']['amount_column']
        currency_suffix = '[\$£,()-]'

        extracted_data = self.ebsco_publisher_price_cal(extracted_data,currency_suffix)

        extracted_data[amount_column] = (extracted_data[amount_column]).replace(currency_suffix, '', regex=True)
        extracted_data[amount_column] = extracted_data[amount_column].astype('str')
        extracted_data[amount_column] = (extracted_data[amount_column]).str.rstrip()
        extracted_data[amount_column] = pd.to_numeric(extracted_data[amount_column])

        if extracted_data['net_units'].all() == 'NA':
            extracted_data['net_units'] = extracted_data.apply(
                lambda row : -1 if (row['revenue_value'] < 0) else 1, axis=1)

        extracted_data['net_units'] = pd.to_numeric(extracted_data['net_units'], errors='coerce')
        extracted_data = self.process_trans_type(logger, extracted_data, file_type)

        extracted_data=self.calculate_final_discount_percentage(extracted_data,logger)



        extracted_data['tnf_net_price_per_unit'] = round(
            ((1 - extracted_data['disc_percentage'] / 100) * extracted_data['publisher_price']), 2)

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

        # Fixing amount for refunds
        extracted_data[amount_column] = extracted_data.apply(
            lambda row : row[amount_column]*-1 if (row['trans_type'] == 'RETURNS' and row[amount_column] > 0) else (row[amount_column]), axis=1)

        # new attributes addition
        extracted_data['source'] = "EBSCO EBook"
        extracted_data['source_id'] = filename.split('.')[0]
        extracted_data['sub_domain'] = 'NA'
        extracted_data['business_model'] = 'B2B'
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
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                # To get the sheet names
                input_file_extn = each_file.split('.')[-1]
                if (input_file_extn.lower()  in ['xlsx','xls']) :
                    excel_frame = pd.ExcelFile(input_list[0]['input_base_path'] + each_file)
                    sheets = excel_frame.sheet_names
                    for each_sheet in sheets :
                        logger.info('Processing sheet: %s', each_sheet)
                        input_list[0]['input_sheet_name'] = each_sheet


                        final_staging_data = self.process_relevant_attributes_ebsco_data(logger, input_list, each_file,
                                                                              rule_config, final_staging_data,
                                                                              default_config,'excel')

                else :
                    if(self.process_only_quarter_and_subscription_file(each_file,logger)):
                        final_staging_data = self.process_relevant_attributes_ebsco_data(logger, input_list, each_file, rule_config,
                                                                          final_staging_data, default_config,'csv')


        # Grouping and storing data

        final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                      default_config[0]['group_staging_data'])

        final_grouped_data =self.country_converter(final_grouped_data)


        obj_s3_connect.store_data(logger, app_config, final_grouped_data)
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
                                                                        agg_reference, obj_pre_process)
        except KeyError as err :
            logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)

        return final_staging_data

    def process_only_quarter_and_subscription_file(self, filename,logger) :
        matches = ["q1", "q2", "q3", "q4","roy","sub"]
        if any(x in filename.lower() for x in matches) :
            return True
        else:
            logger.info('This is not a quarter file or subscription data,so not processed')
            return False




    def ebsco_publisher_price_cal(self,extracted_data,currency_suffix):

        if extracted_data['trans_curr'].all() == 'NA':
            extracted_data['trans_curr']='USD'

        if extracted_data['publisher_price'].all() == 'NA':
            extracted_data['publisher_price'] = extracted_data["revenue_value"]

        if extracted_data['lpm'].all() == 'NA':
            extracted_data['lpm'] = 1


        extracted_data['publisher_price'] = (extracted_data['publisher_price']).replace(currency_suffix, '', regex=True)
        extracted_data['publisher_price'] = extracted_data['publisher_price'].astype('str')
        extracted_data['publisher_price'] = (extracted_data['publisher_price']).str.rstrip()
        extracted_data['publisher_price'] = pd.to_numeric(extracted_data['publisher_price'])
        extracted_data['publisher_price'] = round((extracted_data['publisher_price'] * extracted_data['lpm']),2)
        extracted_data['publisher_price'] = extracted_data['publisher_price'].abs()

        return extracted_data

    def country_converter(self,final_grouped_data):

        cc = coco.CountryConverter()
        iso_names =  cc.convert(names=final_grouped_data['region_of_sale'].tolist(),to="ISO2",enforce_list=True)
        final_grouped_data['region_of_sale'] = iso_names
        iso_new =[]
        for i in range(len(final_grouped_data['region_of_sale'])):
            iso_new.append(final_grouped_data['region_of_sale'][i][0])
        final_grouped_data['region_of_sale'] = iso_new
        return final_grouped_data

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
        q1 = ["jan", "feb", "mar"]
        q2 = ["apr", "may", "jun"]
        q3 = ["jul", "aug", "sep"]
        q4 = ["oct", "nov", "dec"]
        if any(x in filename.lower() for x in q1) :
            extracted_data['transaction_date'] = '31-03-'+ filename.split('.')[0][-4:]
        elif any(x in filename.lower() for x in q2) :
            extracted_data['transaction_date'] = '30-06-'+ filename.split('.')[0][-4:]
        elif any(x in filename.lower() for x in q3) :
            extracted_data['transaction_date'] = '30-09-'+ filename.split('.')[0][-4:]
        elif any(x in filename.lower() for x in q4) :
            extracted_data['transaction_date'] = '31-12-'+ filename.split('.')[0][-4:]
        extracted_data['transaction_date'] = pd.to_datetime(extracted_data['transaction_date'])
        return extracted_data

    def check_csv_transaction_date(self,ext,extracted_data):
        if ext == 'csv' and extracted_data['transaction_date'].all()=='NA':
            extracted_data['transaction_date'] = extracted_data["alternate_date"]
        return extracted_data

    def calculate_final_discount_percentage(self,extracted_data,logger):
        if (extracted_data['disc_percentage'].all() == 'NA') :
            extracted_data['disc_percentage'] = 0

        extracted_data['disc_percentage'] = (extracted_data['disc_percentage']).replace('%', '', regex=True)
        extracted_data['disc_percentage'] = extracted_data['disc_percentage'].astype('str')
        extracted_data['disc_percentage'] = (extracted_data['disc_percentage']).str.rstrip()
        extracted_data['disc_percentage'] = pd.to_numeric(extracted_data['disc_percentage'])

        extracted_data['disc_percentage'] = extracted_data.apply(
            lambda row : row['disc_percentage'] * 100 if (row['disc_percentage'] < 1) else row['disc_percentage'],
            axis=1)

        if extracted_data['consortia'].all() == 'NA' :
            extracted_data['consortia'] = 0

        extracted_data['consortia'] = extracted_data.apply(
            lambda row : row['consortia'] * 100 if (row['consortia'] < 1) else row['consortia'], axis=1)

        logger.info('Processing discount percentages')
        extracted_data['disc_percentage'] = -(100 * extracted_data['disc_percentage']) - (
                100 * extracted_data['consortia']) + (
                                                    extracted_data['disc_percentage'] * extracted_data['consortia'])
        extracted_data['disc_percentage'] = (extracted_data['disc_percentage'] / 100).abs()
        return extracted_data





