#####################
# Imports
####################

import logging as logger
import os
import json
import pandas as pd
from cerberus import Validator

from OrderDataValidations.ReadStagingData import ReadStagingData
from OrderDataValidations.AggregatorDataValidations.Amazon.AmazonAggregatorValidations import AmazonAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Barnes.BarnesAggregatorValidations import BarnesAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Blackwells.BlackwellsAggregatorValidations import BlackwellsAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Chegg.CheggAggregatorValidations import CheggAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Ebsco.EbscoAggregatorValidations import EbscoAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Follett.FollettAggregatorValidations import FollettAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Gardners.GardnersAggregatorValidations import GardnersAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Ingramvs.IngramvsAggregatorValidations import IngramvsAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Proquest.ProquestAggregatorValidations import ProquestAggregatorValidations
from OrderDataValidations.AggregatorDataValidations.Redshelf.RedshelfAggregatorValidations import RedshelfAggregatorValidations

from OrderDataValidations.OwnedsitesDataValidations.OMS.OMSAggregatorValidations import OMSAOwnedSitesValidations
from OrderDataValidations.OwnedsitesDataValidations.UBW.UBWAggregatorValidations import UBWOwnedSitesValidations

from OrderDataValidations.WarehouseDataValidations.AUSTLD.AustldWarehouseValidations import AustldWarehouseValidations
from OrderDataValidations.WarehouseDataValidations.SGBM.SgbmWarehouseValidations import SgbmWarehouseValidations
from OrderDataValidations.WarehouseDataValidations.UKBP.UkbpWarehouseValidations import UkbpWarehouseValidations
from OrderDataValidations.WarehouseDataValidations.USPT.UsptWarehouseValidations import UsptWarehouseValidations


#############################
#      Global Variables     #
#############################
# Creating object for ReadStagingData class
obj_read_data = ReadStagingData()
# Creating object for Cerberus validator class
validator = Validator()
# Creating objects for all aggregator, owned sites and warehouse data validation classes
obj_amazon_aggregator_validations = AmazonAggregatorValidations()
obj_barnes_aggregator_validations = BarnesAggregatorValidations()
obj_blackwells_aggregator_validations = BlackwellsAggregatorValidations()
obj_chegg_aggregator_validations = CheggAggregatorValidations()
obj_ebsco_aggregator_validations = EbscoAggregatorValidations()
obj_follett_aggregator_validations = FollettAggregatorValidations()
obj_gardners_aggregator_validations = GardnersAggregatorValidations()
obj_ingram_aggregator_validations = IngramvsAggregatorValidations()
obj_proquest_aggregator_validations = ProquestAggregatorValidations()
obj_redshelf_aggregator_validations = RedshelfAggregatorValidations()
obj_oms_ownedsites_validations = OMSAOwnedSitesValidations()
obj_ubw_ownedsites_validations = UBWOwnedSitesValidations()
obj_austld_warehouse_validations = AustldWarehouseValidations()
obj_sgbm_warehouse_validations = SgbmWarehouseValidations()
obj_ukbp_warehouse_validations = UkbpWarehouseValidations()
obj_uspt_warehouse_validations = UsptWarehouseValidations()

#############################
#     Class Functions       #
#############################

class AggregatorValidations:

    def aggregator_data_validations(self, test_data, aggregator, agg_val_json):
        logger.info("\n\t-+-+-+-Invoking aggregator data validations based on aggregator_name-+-+-+-")

        # Calling aggregator validations function based on aggregator_name
        if aggregator == 'AMAZON':
            obj_amazon_aggregator_validations.aggregator_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)
        elif aggregator == 'BARNES':
            obj_barnes_aggregator_validations.aggregator_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)
        elif aggregator == 'CHEGG':
            obj_chegg_aggregator_validations.aggregator_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)
        elif aggregator == 'EBSCO':
            obj_ebsco_aggregator_validations.aggregator_specific_validations(input_data=test_data,agg_specific_rules=agg_val_json)
        elif aggregator == 'FOLLETT':
            obj_follett_aggregator_validations.aggregator_specific_validations(input_data=test_data,agg_specific_rules=agg_val_json)
        elif aggregator == 'GARDNERS':
            obj_gardners_aggregator_validations.aggregator_specific_validations(input_data=test_data,agg_specific_rules=agg_val_json)
        elif aggregator == 'PROQUEST':
            obj_proquest_aggregator_validations.aggregator_specific_validations(input_data=test_data,agg_specific_rules=agg_val_json)
        elif aggregator == 'REDSHELF':
            obj_redshelf_aggregator_validations.aggregator_specific_validations(input_data=test_data,agg_specific_rules=agg_val_json)
        elif aggregator == 'BLACKWELLS':
            obj_blackwells_aggregator_validations.aggregator_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)
        elif aggregator == 'INGRAM':
            obj_ingram_aggregator_validations.aggregator_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)

        # Calling owned sites validations function based on aggregator_name
        elif aggregator == 'OMS':
            obj_oms_ownedsites_validations.ownedsites_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)
        elif aggregator == 'UBW':
            obj_ubw_ownedsites_validations.ownedsites_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)

        # Calling Warehouse validations function based on aggregator_name
        elif aggregator == 'AUSTLD':
            obj_austld_warehouse_validations.warehouse_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)
        elif aggregator == 'SGBM':
            obj_sgbm_warehouse_validations.warehouse_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)
        elif aggregator == 'UKBP':
            obj_ukbp_warehouse_validations.warehouse_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)
        elif aggregator == 'USPT':
            obj_uspt_warehouse_validations.warehouse_specific_validations(input_data=test_data, agg_specific_rules=agg_val_json)


