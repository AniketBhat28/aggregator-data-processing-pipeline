####################
#     Imports      #
####################

import pandas as pd
import boto3
import io
import importlib
import json
import os
from pyspark.sql.utils import AnalysisException

#############################
#      Global Variables     #
#############################
# Defining variable to store s3
s3 = boto3.resource("s3")

#############################
#     Class Functions       #
#############################

class DataValidationsErrorHandling:
    def glue_job_failure(self, input_error):
        print(input_error)
        failed_data_row = list(dict.keys(input_error))
        error_list = list(dict.values(input_error))

        forbidden_error_list = [['unknown field'], ['required field'], ['document is missing'], ['null value not allowed'], ['empty values not allowed']]
        result = all(elem in forbidden_error_list for elem in error_list)
        if result:
            print("-+-+-+- Failing Glue Job because of Forbidden Error : " + error_list[0][0] + "-+-+-+-+-")
            raise AnalysisException(None, None)
