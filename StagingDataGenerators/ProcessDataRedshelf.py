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

        extracted_data['trans_curr'] = 'USD'
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

        extracted_data['disc_percentage'] = extracted_data['disc_percentage']*100

        # new attributes addition
        extracted_data['source'] = "REDSHELF EBook"
        extracted_data['source_id'] = filename.split('.')[0]
        extracted_data['sub_domain'] = 'NA'
        extracted_data['business_model'] = 'B2C'

        return extracted_data


    # Function Description :	This function processes data for all Ebsco files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    #							default_config - Default json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config, default_config):

        # For the final staging output
        agg_name = 'REDSHELF'
        agg_reference = self
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
                        final_staging_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file, rule_config,
                                                        default_config,final_staging_data, obj_read_data,
                                                        obj_pre_process,agg_name, agg_reference)

                else:
                    final_staging_data = obj_gen_attrs.applying_aggregator_rules(logger, input_list, each_file, rule_config,
                                                        default_config,final_staging_data, obj_read_data,
                                                        obj_pre_process,agg_name, agg_reference)

        # Grouping and storing data
        final_grouped_data = obj_gen_attrs.group_data(logger, final_staging_data,
                                                      default_config[0]['group_staging_data'])
        obj_s3_connect.store_data(logger, app_config, final_grouped_data)
        logger.info('\n+-+-+-+-+-+-+Finished Processing Redshelf files\n')


