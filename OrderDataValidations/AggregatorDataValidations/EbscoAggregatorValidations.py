# Imports #

import pandas as pd
import numpy as np
import logging as logger

# Global Variable #

class EbscoValidations:
    def validate_reporting_date(self,test_data):
        logger.info("-------\nStarting ISBN Validations on the data file-----")
        print("\n------Starting Aggregator Validations on the data file-----")
        load_data = test_data
        extracted_data = load_data['reporting_date']