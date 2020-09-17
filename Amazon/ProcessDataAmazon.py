#################################
#			IMPORTS				#
#################################


import os
import ast
import pandas as pd
import boto3

from ProcessCore import ProcessCore


#################################
#		GLOBAL VARIABLES		#
#################################


# None


#################################
#		CLASS FUNCTIONS			#
#################################


class ProcessDataAmazon:

    # Function Description :	This function aggregates revenue for all Amazon files
    # Input Parameters : 		logger - For the logging output file.
    #							app_config - Configuration
    #							rule_config - Rules json
    # Return Values : 			None
    def initialise_processing(self, logger, app_config, rule_config):

        # Getting configuration file details
        input_list = list(ast.literal_eval(app_config['INPUT']['File_Data']))
        input_base_path = input_list[0]['input_base_path']
        input_bucket_name = input_list[0]['input_bucket_name']
        dir_path = input_list[0]['input_directory']

        # Connect to s3 bucket
        s3 = boto3.resource("s3")
        s3_bucket = s3.Bucket(input_bucket_name)

        # Listing files in s3 bucket
        files_in_s3 = [f.key.split(dir_path + "/")[1] for f in s3_bucket.objects.filter(Prefix=dir_path).all()]
        # For the final staging output
        final_data = pd.DataFrame()

        # Processing for each file in the fiven folder
        for each_file in files_in_s3:
            # for eachFile in os.listdir(directory):
            # for eachFile in files_in_s3:
            if each_file != '':

                logger.info('\n+-+-+-+-+-+-+')
                logger.info(each_file)
                logger.info('\n+-+-+-+-+-+-+')

                # Load the data
                obj_read_data = ProcessCore()
                data = obj_read_data.load_data(logger, app_config['INPUT'], each_file)

                # Get the corresponding rules object
                if 'rental' in each_file.lower():
                    agg_rules = next((item for item in rule_config if
                                      (item['name'] == 'Amazon' and item['filename_pattern'] == '/Amazon Rental')),
                                     None)
                else:
                    agg_rules = next((item for item in rule_config if
                                      (item['name'] == 'Amazon' and item['filename_pattern'] == '/Amazon')), None)

                # Header details
                header_row = agg_rules['header_row']
                first_row = agg_rules['data_row']
                last_rows = agg_rules['discard_last_rows']

                # Get columns from given header rows
                if header_row != 0:
                    data.columns = data.iloc[header_row]

                # Get for first data row
                if first_row != 0:
                    data = data.loc[first_row - 1:]

                # Discard given last rows
                if last_rows != 0:
                    data = data.iloc[:-last_rows]

                # Discarding leading/trailing spacs from the columns
                data.columns = data.columns.str.strip()

                # Extracting relevent columns
                relevent_cols = agg_rules['relevant_attributes']
                data = data[relevent_cols]

                # Processing amount column
                amount_column = agg_rules['filters']['amount_column']
                data[amount_column] = (
                    data[amount_column].replace('[,)]', '', regex=True).replace('[(]', '-', regex=True))

                # Column Validations
                for element in agg_rules['filters']['column_validations']:
                    if element['dtype'] == 'float':
                        data[element['column_name']] = pd.to_numeric(data[element['column_name']], errors='coerce')
                        # data[element['column_name']] = data[element['column_name']].astype(float)
                        if 'missing_data' in element.keys():
                            final_data = obj_read_data.start_process_data(logger, element, data)

                    elif element['dtype'] == 'str':
                        if 'missing_data' in element.keys():
                            final_data = obj_read_data.start_process_data(logger, element, data)

                logger.info('\n+-+-+-+-+-+-+')
                logger.info(final_data)
                   

                # Write the output to a csv
                # final_data.to_csv(output_directory+'Result '+eachFile, sep=',')




