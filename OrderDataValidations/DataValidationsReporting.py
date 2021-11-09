####################
#     Imports      #
####################

import pandas as pd

#############################
#     Class Functions       #
#############################

class DataValidationsReporting:
    def capture_data_validation_results(self, input_data_frame, input_file, input_dict):
        input_data_frame = input_data_frame.apply(lambda x: True if x['Validation Result'] == "PASS" else False, axis=1)
        pass_data_record_count = len(input_data_frame[input_data_frame == True].index)
        fail_data_record_count = len(input_data_frame[input_data_frame == False].index)
        validation_results_dict = input_dict
        validation_results_dict['File Processed'], validation_results_dict['Total Number of Passed Records'], validation_results_dict['Total Number of Failed Records'] = [], [], []
        validation_results_dict['File Processed'].append(input_file)
        validation_results_dict['Total Number of Passed Records'].append(pass_data_record_count)
        validation_results_dict['Total Number of Failed Records'].append(fail_data_record_count)

        return validation_results_dict