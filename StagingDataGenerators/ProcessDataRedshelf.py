#################################
#			IMPORTS				#
#################################


import ast
from pandas import ExcelFile, concat, DataFrame

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


class ProcessDataRedshelf:

    # Function Description :	This function processes transaction and sales types
    # Input Parameters : 		logger - For the logging output file.
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def process_trans_type(self, logger, extracted_data):

        logger.info("Processing transaction and sales types for Redshelf")
        extracted_data['sale_type'] = extracted_data.apply(
            lambda row: ('REFUNDS') if (row['sale_type'] == 'REFUNDED') else ('PURCHASE'), axis=1)
        extracted_data['trans_type'] = extracted_data.apply(
            lambda row: ('RENTAL') if (row['total_rental_duration'] != 'LIFETIME') else ('SALE'), axis=1)

        logger.info("Transaction and sales types for Redshelf processed")
        return extracted_data

    # Function Description :	This function generates staging data for Reddshelf files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    # Return Values : 			extracted_data - extracted staging data
    def generate_staging_output(self, logger, filename, agg_rules, default_config, extracted_data):

        extracted_data['aggregator_name'] = agg_rules['name']
        extracted_data['product_type'] = agg_rules['product_type']

        extracted_data['e_backup_product_id'] = 'NA'
        extracted_data['p_backup_product_id'] = 'NA'
        extracted_data['pod'] = 'NA'
        extracted_data['vendor_code'] = 'NA'
        extracted_data['disc_code'] = 'NA'

        extracted_data['trans_currency'] = 'USD'
        extracted_data['misc_product_ids']='NA'
        current_date_format = agg_rules['date_formats']
        extracted_data = obj_pre_process.process_dates(logger, extracted_data, current_date_format, 'transaction_date',
                                                       default_config)

        extracted_data = self.process_trans_type(logger, extracted_data)

        extracted_data['total_rental_duration'] = extracted_data.apply(
            lambda row: (0) if (row['total_rental_duration'] == 'LIFETIME' or row['total_rental_duration'] == 'LIMITED') else row['total_rental_duration'], axis=1)

        extracted_data['revenue_value'] = extracted_data.apply(
            lambda row: (row['agg_net_price_per_unit']) if (row['revenue_value'] == 0) else row['revenue_value'], axis=1)

        extracted_data['publisher_price'] = extracted_data.apply(
            lambda row: (row['tnf_net_price_per_unit']) if (row['publisher_price'] == 0 ) else row['publisher_price'],axis=1)


        amount_column = agg_rules['filters']['amount_column']
        extracted_data['publisher_price'] = extracted_data['publisher_price'].abs()
        extracted_data['agg_net_price_per_unit'] = extracted_data['agg_net_price_per_unit'].abs()
        extracted_data['tnf_net_price_per_unit'] = extracted_data['tnf_net_price_per_unit'].abs()

        logger.info('Calculating disc values in decimals')
        extracted_data['disc_percentage'] = round(((extracted_data['publisher_price'] - extracted_data[
            'tnf_net_price_per_unit']) / extracted_data['publisher_price']),2)

        logger.info('Computing Sales and Returns')
        extracted_data['total_sales_value'] = extracted_data.apply(
            lambda row: row[amount_column] if (row['sale_type'] == 'PURCHASE') else 0.0, axis=1)
        extracted_data['total_returns_value'] = extracted_data.apply(
            lambda row: row[amount_column] if (row['sale_type'] == 'REFUNDS') else 0.0, axis=1)
        extracted_data['total_sales_value'] = extracted_data['total_sales_value'].abs()
        extracted_data['total_returns_value'] = extracted_data['total_returns_value'].abs()

        extracted_data['total_sales_count'] = extracted_data.apply(
            lambda row: 1 if (row['sale_type'] == 'PURCHASE') else 0, axis=1)
        extracted_data['total_returns_count'] = extracted_data.apply(
            lambda row: 1 if (row['sale_type'] == 'REFUNDS') else 0, axis=1)
        extracted_data['total_returns_count'] = extracted_data['total_returns_count'].abs()
        extracted_data['net_units'] = extracted_data['total_sales_count'] - extracted_data['total_returns_count']
        logger.info('Sales and Return Values computed')

        return extracted_data

    # Function Description :	This function processes staging data for Amazon files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    #							final_staging_data - final_staging_data
    # Return Values : 			final_staging_data - final_staging_data
    def process_staging_data(self, logger, filename, agg_rules, default_config, extracted_data, final_staging_data):

        if extracted_data.dropna(how='all').empty:
            logger.info("This file is empty")
        else:
            logger.info('Processing amount column')
            amount_column = agg_rules['filters']['amount_column']
            extracted_data[amount_column] = (
                extracted_data[amount_column].replace('[,)]', '', regex=True).replace('[(]', '-', regex=True))

            extracted_data = obj_pre_process.validate_columns(logger, extracted_data,
                                                              agg_rules['filters']['column_validations'])

            logger.info("Generating Staging Output")
            extracted_data = self.generate_staging_output(logger, filename, agg_rules, default_config, extracted_data)
            logger.info("Staging output generated for given data")

            # Append staging data of current file into final staging dataframe
            final_staging_data = concat([final_staging_data, extracted_data], ignore_index=True, sort=True)

        return final_staging_data

    # Function Description :	This function processes data for all Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config):

        # For the final staging output
        final_staging_data = DataFrame()
        input_list = list(app_config['input_params'])

        # Processing for each file in the fiven folder
        logger.info('\n+-+-+-+-+-+-+Starting Redshelf files Processing\n')
        files_in_s3 = obj_s3_connect.get_files(logger, input_list)
        for each_file in files_in_s3:
            if each_file != '':
                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                input_file_extn = each_file.split('.')[-1]
                if (input_file_extn.lower() == 'xlsx') or (input_file_extn.lower() == 'xls'):

                    # To get the sheet names
                    excel_frame = ExcelFile(input_list[0]['input_base_path'] + each_file)
                    sheets = excel_frame.sheet_names
                    for each_sheet in sheets:
                        logger.info('Processing sheet: %s', each_sheet)
                        input_list[0]['input_sheet_name'] = each_sheet
                        final_staging_data = self.applying_rules_attributes(logger, input_list, each_file, rule_config, default_config,
                                                  final_staging_data)

                else:
                    final_staging_data = self.applying_rules_attributes(logger, input_list, each_file, rule_config,
                                                                        default_config,
                                                                        final_staging_data)

        # Grouping and storing data
        final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                      default_config[0]['group_staging_data'])
        obj_s3_connect.store_data(logger, app_config, final_grouped_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing Redshelf files\n')

    #generating staging data for different files
    def applying_rules_attributes(self,logger, input_list, each_file, rule_config, default_config,
                                  final_staging_data):
        try:
            data = obj_read_data.load_data(logger, input_list, each_file)
            if not data.empty:
                logger.info('Get the corresponding rules object for Redshelf')
                agg_rules = next((item for item in rule_config if (item['name'] == 'REDSHELF')), None)

                data = data.dropna(how='all')
                data.columns = data.columns.str.strip()

                for each_dict in agg_rules['attribute_mappings']:
                    if list(each_dict.values())[0] not in data.columns.to_list():
                        data = data.rename(columns=each_dict)
                for each_col in agg_rules['missing_columns']:
                    if each_col not in data.columns.to_list():
                        data[each_col] = 'NA'

                mandatory_columns = agg_rules['filters']['mandatory_columns']
                data[mandatory_columns] = data[mandatory_columns].fillna(value='NA')

                extracted_data = obj_pre_process.extract_relevant_attributes(logger, data,
                                                                             agg_rules[
                                                                                 'relevant_attributes'])

                final_staging_data = self.process_staging_data(logger, each_file, agg_rules, default_config,
                                                               extracted_data, final_staging_data)

        except KeyError as err:
            logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)

        return final_staging_data

