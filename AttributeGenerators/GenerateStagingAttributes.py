#################################
#			IMPORTS				#
#################################


import pandas as pd
import numpy as np
import re
import itertools
from Preprocessing.ProcessCore import ProcessCore


#################################
#		GLOBAL VARIABLES		#
#################################


# None


#################################
#		CLASS FUNCTIONS			#
#################################


class GenerateStagingAttributes:

    # Function Description :	This function is to process comma seperated isbn
    # Input Parameters : 		logger - For the logging output file.
    #							data - input data
    #							input_column - input column name
    #							staging_column - staging column name
    #							bckp_staging_column - backup staging column name
    #							default_val - Deafult value to be inserted
    # Return Values : 			data
    def process_isbn(self, logger, data, input_column, staging_column, bckp_staging_column, default_val):

        logger.info('Processing ISBN')
        data[input_column] = data[input_column].astype(str)

        data[staging_column] = data.apply(lambda row: (row[input_column].split(',')[0]) if (
                    (pd.notnull(row[input_column])) and (len(row[input_column].split(',')) >= 1)) else (default_val),
                                          axis=1)
        data[bckp_staging_column] = data.apply(lambda row: (row[input_column].split(',')[0]) if (
                    (pd.notnull(row[input_column])) and (len(row[input_column].split(',')) >= 2)) else (default_val),
                                               axis=1)

        logger.info('ISBN processed')

        return data

    # Function Description :	This function is to process comma seperated miscellaneous isbn
    # Input Parameters : 		logger - For the logging output file.
    #							data - input data
    #							e_column - input digital column name
    #							p_column - input physical column name
    #							staging_column - staging column name
    #							default_val - Deafult value to be inserted
    # Return Values : 			data
    def generate_misc_isbn(self, logger, data, e_column, p_column, staging_column, default_val):

        logger.info('Processing Miscellaneous ISBN')

        data['misc_e_product_id'] = data.apply(lambda row: (row[e_column].split(',')[2:]) if (
                    (pd.notnull(row[e_column])) and (len(row[e_column].split(',')) > 2)) else [], axis=1)
        data['misc_p_product_id'] = data.apply(lambda row: (row[p_column].split(',')[2:]) if (
                    (pd.notnull(row[p_column])) and (len(row[p_column].split(',')) > 2)) else [], axis=1)

        data[staging_column] = data[['misc_e_product_id', 'misc_p_product_id']].values.tolist()
        data[staging_column] = data.apply(lambda row: [] if (row[staging_column] == [[], []]) else list(
            itertools.chain.from_iterable(row[staging_column])), axis=1)
        data[staging_column] = [','.join(map(str, l)) for l in data[staging_column]]
        data.replace('', default_val, inplace=True)

        logger.info('Miscellaneous ISBN processed')
        return data

    # Function Description :	This function is to extract patterns using regular expressions
    # Input Parameters : 		data - input data
    #							pattern_dict - containg regex
    #							input_string - input string to search
    # Return Values : 			data
    def extract_patterns(self, data, pattern_dict, input_string):

        pattern = pattern_dict['regex']
        temp_pattern_output = re.findall(fr"(?i)((?:{pattern}))", input_string, re.IGNORECASE)
        if len(temp_pattern_output) != 0:
            data[pattern_dict['staging_column_name']] = temp_pattern_output[0]

        return data

    # Function Description :	This function is to group staging data
    # Input Parameters : 		logger - For the logging output file.
    #							data - input data
    #							groupby_object - includes agg_fn and columns to group
    # Return Values : 			final_grouped_data
    def group_data(self, logger, data, groupby_object):

        logger.info('Grouping staging data')
        final_grouped_data =  data[groupby_object['display_column_sequence']]

        logger.info('Staging data grouped')
        return final_grouped_data

    # Function Description :	This function is to compute net unit prices
    # Input Parameters : 		logger - For the logging output file.
    #							data - input data
    #							amount_column - name of the amount column
    # Return Values : 			data
    def process_net_unit_prices(self, logger, data, amount_column):

        logger.info('Computing aggregator and tnf net unit prices')
        data['tnf_net_price_per_unit'] = round(((1 - data['disc_percentage']) * data['publisher_price']), 2)
        data['agg_net_price_per_unit'] = round((data[amount_column] / data['net_units']), 2)
        data['agg_net_price_per_unit'] = data['agg_net_price_per_unit'].replace(np.nan, data['tnf_net_price_per_unit'])
        data['agg_net_price_per_unit'] = data['agg_net_price_per_unit'].replace(np.inf, data['tnf_net_price_per_unit'])
        data['agg_net_price_per_unit'] = data['agg_net_price_per_unit'].replace(-np.inf, data['tnf_net_price_per_unit'])

        logger.info('Net units prices computed')
        return data

    # Function Description :	This function processes staging data for Amazon files
    # Input Parameters : 		logger - For the logging output file.
    #							filename - Name of the file
    #							agg_rules - Rules json
    #							default_config - Default json
    #							extracted_data - pr-processed_data
    #							final_staging_data - final_staging_data
    # Return Values : 			final_staging_data - final_staging_data
    def process_staging_data(self, logger, filename, agg_rules, default_config, extracted_data, final_staging_data,
                             agg_reference, obj_pre_process):
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
            extracted_data = agg_reference.generate_staging_output(logger, filename, agg_rules, default_config,
                                                                   extracted_data)
            logger.info("Staging output generated for given data")

            # Append staging data of current file into final staging dataframe
            final_staging_data = pd.concat([final_staging_data, extracted_data], ignore_index=True, sort=True)
        return final_staging_data

    def applying_aggregator_rules(self, logger, input_list, each_file, rule_config, default_config,
                                  final_staging_data, obj_read_data, obj_pre_process,
                                  agg_name, agg_reference):

        logger.info('\n+-+-+-+-+-+-+Starting Processing ' + agg_name + ' files\n')
        try:
            data = obj_read_data.load_data(logger, input_list, each_file)
            if not data.empty:
                logger.info('Get the corresponding rules object for ' + agg_name)
                if (agg_name == 'CHEGG'):
                    obj_process_core = ProcessCore()
                    agg_rules = obj_process_core.get_rules_object(rule_config, 'rental', 'CHEGG', each_file,
                                                                  '/Chegg Rental', '/Chegg')
                else:
                    agg_rules = next((item for item in rule_config if (item['name'] == agg_name)), None)

                mandatory_columns = agg_rules['filters']['mandatory_columns']

                data = data.dropna(how='all')
                data.columns = data.columns.str.strip()

                if agg_name in ['REDSHELF','OVERDRIVE','FOLLETT','CHEGG','PROQUEST','INGRAM']:
                    data = self.replace_column_names(logger, agg_rules, data)



                data[mandatory_columns] = data[mandatory_columns].fillna(value='NA')


                extracted_data = obj_pre_process.extract_relevant_attributes(logger, data,
                                                                             agg_rules['relevant_attributes'])

                final_staging_data = self.process_staging_data(logger, each_file, agg_rules,
                                                               default_config,
                                                               extracted_data, final_staging_data,
                                                               agg_reference, obj_pre_process)

        except KeyError as err:
            logger.error(f"KeyError error while processing the file {each_file}. The error message is :  ", err)

        logger.info('\n+-+-+-+-+-+-+Finished Processing ' + agg_name + ' files\n')
        return final_staging_data

    # Function Description :	This function rename column names to make a common schema
    # Input Parameters : 		logger - For the logging output file.
    #							agg_rules - Rules json
    #							data - input_data
    # Return Values : 			data - input_data
    def replace_column_names(self, logger, agg_rules, data):

        logger.info('\n+-+-+-+-+-+-+Renaming Column Start \n')
        for each_dict in agg_rules['attribute_mappings']:
            if list(each_dict.values())[0] not in data.columns.to_list():
                data = data.rename(columns=each_dict)
        for each_col in agg_rules['missing_columns']:
            if each_col not in data.columns.to_list():
                data[each_col] = 'NA'
        logger.info('\n+-+-+-+-+-+-+Renaming Column ends \n')
        return data
