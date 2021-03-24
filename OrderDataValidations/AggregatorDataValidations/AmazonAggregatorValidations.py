# Imports #

import pandas as pd
import numpy as np
import logging as logger

# Global Variable #

class AmazonAggregatorValidations:
    def validate_reporting_date(self,test_data):
        logger.info("-------\nStarting ISBN Validations on the data file-----")
        print("\n-------Starting ISBN Validations on the data file-----\n")
        load_data = test_data
        extracted_data = load_data['reporting_date']
        print(extracted_data, "\n------This is Reporting date validation--------")