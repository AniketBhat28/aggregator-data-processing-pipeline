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


class MDAMappedProcessDataAmazon:

    # Function Description :	This function cleans data headers and tailers
    # Input Parameters : 		data - input data
    #							agg_rules - rules json
    #							filename - filename
    # Return Values : 			data - cleaned data
    def clean_data(self, data, agg_rules, filename,d_last_rows):

        if 'rental' in filename.lower():
            data.columns = data.columns.str.lower()

        if agg_rules['discard_last_rows'] != 0:
            if d_last_rows != 0:
                data = data.iloc[:-agg_rules['discard_last_rows']]
        data = data.dropna(how='all')

        if 'attribute_names' in agg_rules.keys():
            if len(data.columns.tolist()) == 21:
                data.columns = agg_rules['attribute_names'][:-1]
            else:
                data.columns = agg_rules['attribute_names']
        data.columns = data.columns.str.strip()

        return data

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, extracted_data, filename):

        logger.info("Processing transaction and sales types")
        if not ('rental' in filename.lower()):
            extracted_data['sale_type'] = 'Purchase'
            extracted_data['trans_type'] = 'Sales'
        else:
            # as sale_type_ori is equal to rental_type
            extracted_data['trans_type'] = extracted_data['sale_type_ori'].astype('str').apply(
                    lambda row: ('Sales') if (row.lower() == 'purchase') else ('Rental'))
            extracted_data['sale_type'] = extracted_data['sale_type_ori']

        logger.info("Transaction and sales types processed")
        return extracted_data

    # Function Description :	This function generates staging data for Amazon files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data

    # Return Values : 			extracted_data - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data, data):

        extracted_data['aggregator_name'] = agg_rules['name']
        extracted_data['product_type'] = 'Electronic'
        extracted_data['External_Product_ID_type'] = 'ASIN'
        logger.info('Extracting patterns from filename')
        for each_rule in agg_rules['pattern_extractions']:
            extracted_data = obj_gen_attrs.extract_patterns(extracted_data, each_rule, filename)
        logger.info('Patterns extracted')

        if 'country_iso_values' in agg_rules['filters'].keys():
            extracted_data['country'] = extracted_data['region_of_sale'].map(
                agg_rules['filters']['country_iso_values']).fillna(extracted_data['region_of_sale'])
        else:
            extracted_data['country'] = extracted_data['region_of_sale']

        current_country = extracted_data['country'][0].upper()
        current_date_format = next(item for item in agg_rules['date_formats'] if item["country"] == current_country)[
            'format']
        extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'reporting_date',
                                                       default_config)

        extracted_data = self.process_trans_type(logger, extracted_data, filename)

        if ('Rental' in filename):
            logger.info('Ignoring subscription file')
            extracted_data['External_Invoice_Number'] = 'NA'
            extracted_data['price_currency'] = 'NA'
            extracted_data['price_type'] = 'NA'
            extracted_data['publisher_price_ori'] = 'NA'
            extracted_data['publisher_price_ori_currency'] = 'NA'
            extracted_data['EX_Currencies'] = 'NA'
            extracted_data['EX_Rate'] = 'NA'
            extracted_data['rental_duration'] = extracted_data['rental_duration'].apply(lambda row: 0 if (row == 'Purchased') else row)
            extracted_data['rental_duration'] = extracted_data['rental_duration'].astype(str)
            extracted_data['old_rental_duration'].fillna(0)

            extracted_data['new_rental_duration'] = extracted_data['rental_duration'].astype(float) - extracted_data[
                'old_rental_duration']
            extracted_data['new_rental_duration'] = extracted_data['new_rental_duration'].apply(
                lambda x: 0 if (x < 0) else x)

            extracted_data['rental_duration_measure'] = 'day'
            extracted_data['old_discount_percentage'] = extracted_data['old_discount_percentage'] * 100
            extracted_data['current_discount_percentage'] = extracted_data['current_discount_percentage'] * 100
            extracted_data['price_type'] = 'NA'


        else:
            
            extracted_data['current_discount_percentage'] = data['Discount Percentage']
            extracted_data['sale_type_ori'] = 'NA'
            extracted_data['old_rental_duration'] = 0
            extracted_data['rental_duration'] = 0
            extracted_data['new_rental_duration'] = 0
            extracted_data['rental_duration_measure'] = 'NA'
            extracted_data['old_discount_percentage'] = 0.0
            extracted_data['price_type'] = 'Retail Price'

        # new attributes addition
        if 'rental' in filename.lower():
            extracted_data['source'] = "Amazon Rental"
            extracted_data['sub_domain'] = 'AMAZON RENTAL'
        else:
            extracted_data['source'] = "Amazon"
            extracted_data['sub_domain'] = 'NA'

        #extracted_data['business_model'] = 'B2C'

        extracted_data=extracted_data.replace(np.nan, 'NA')
        return extracted_data

    # Function Description :	This function processes data for all Amazon files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config):

        # For the final staging output
        final_staging_data = pd.DataFrame()
        input_list = list(app_config['input_params'])

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Amazon files Processing\n')
        files_in_s3 = obj_s3_connect.\
            get_files(logger, input_list)

        for each_file in files_in_s3:
            # and 'rental' in each_file.lower()
            if each_file != '' and 'amazon' in each_file.lower():

                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')
                try:
                    data = obj_read_data.load_data(logger, input_list, each_file)
                    if not data.empty:
                        logger.info('Get the corresponding rules object for Amazon')
                        agg_rules = obj_process_core.get_rules_object(rule_config, 'rental', 'AMAZON', each_file,
                                                                      '/Amazon Rental', '/Amazon')
                        d_last_rows = 0
                        if agg_rules['discard_last_rows'] != 0:
                            trail_data = data.tail(2)
                            trail_data = trail_data.dropna(how='all')
                            if not trail_data.dropna(how='all').empty:
                                trail_data.columns = trail_data.iloc[0]
                                trail_data = trail_data[1:]
                                trail_data = trail_data.iloc[:, : 7]
                                trail_data.columns = agg_rules['trail_attribute_names']
                                if not trail_data.dropna(how='all').empty:
                                    if len(trail_data['Payment Currency Code'].astype(str).item()) <= 4:
                                        d_last_rows = agg_rules['discard_last_rows']


                        c_data = self.clean_data(data, agg_rules, each_file,d_last_rows)
                        if not c_data.empty:
                            if 'attribute_mappings' in agg_rules.keys():
                                c_data = obj_gen_attrs.replace_column_names(logger, agg_rules, c_data)

                            c_data[agg_rules['filters']['mandatory_columns']] = c_data[
                                agg_rules['filters']['mandatory_columns']].fillna(value='NA')

                            extracted_data = obj_pre_process.extract_relevant_attributes(logger, c_data,
                                                                                         agg_rules['relevant_attributes'])
                            extracted_data = obj_gen_attrs.replace_column_names(logger, agg_rules, extracted_data)
                            if not ('Rental' in each_file):
                                extracted_data = self.clean_extract_data(extracted_data, trail_data, each_file)

                            agg_reference = self

                            final_staging_data = obj_gen_attrs.process_staging_data(logger, each_file, agg_rules,
                                                                                    default_config, extracted_data,
                                                                                    final_staging_data, agg_reference,
                                                                                    obj_pre_process, c_data, input_list)
                except KeyError as err:
                    logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)
                    
        final_staging_data.columns = final_staging_data.columns.str.lower()
        final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                      default_config[0]['group_staging_data'])

        final_grouped_data = final_grouped_data.astype(str)

        obj_s3_connect.wrangle_data_as_parquet(logger, app_config, final_grouped_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing Amazon files\n')

    # Function Description :	This function splits rows into sales and return for all Amazon files
    # Input Parameters : 		logger - For the logging output file.
    #							extracted data - dataframe
    # Return Values : 			None

    def clean_extract_data(self, extracted_data, trail_data, each_file):
        if not trail_data.dropna(how='all').empty:
            if len(trail_data['Payment Currency Code'].astype(str).item()) <= 4:

                if trail_data['Payment Currency Code'].isnull().values.any():
                    extracted_data['EX_Rate'] = 0.0
                    extracted_data['EX_Currencies'] = 'NA'
                else:
                    Payment_Curr_Code = trail_data['Payment Currency Code'].item()
                    extracted_data.loc[extracted_data['Payment_Amount_Currency'] != 'USD', 'EX_Rate'] = \
                        trail_data[
                            'Foreign Exchange Rate'].item()
                    extracted_data.loc[extracted_data['Payment_Amount_Currency'] != 'USD', 'EX_Currencies'] = \
                        extracted_data['Payment_Amount_Currency'].astype(str) + '/' + Payment_Curr_Code
                if trail_data['Invoice Number'].isnull().values.any():
                    extracted_data['External_Invoice_Number'] = 'NA'
                else:
                    extracted_data['External_Invoice_Number'] = 'NA'
                    extracted_data.loc[
                        extracted_data['Payment_Amount_Currency'] != 'USD', 'External_Invoice_Number'] = \
                        trail_data['Invoice Number'].item()
            else:
                extracted_data['EX_Rate'] = 0.0
                extracted_data['EX_Currencies'] = 'NA'
                extracted_data['External_Invoice_Number'] = 'NA'
        else:
            extracted_data['EX_Rate'] = 0.0
            extracted_data['EX_Currencies'] = 'NA'
            extracted_data['External_Invoice_Number'] = 'NA'
        return extracted_data
