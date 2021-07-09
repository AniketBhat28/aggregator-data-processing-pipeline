import os
import sys
import boto3
from datetime import date, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import lit, input_file_name, regexp_replace, expr
from pyspark.sql.utils import AnalysisException

from mda_utils.utils import gen_time_frame_list


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
required_args = {'job_type', 'time_frame', 'end_time_frame', 's3_base_dir'}

if '--WORKFLOW_RUN_ID' in sys.argv:
    workflow_args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    workflow_name = workflow_args['WORKFLOW_NAME']
    workflow_run_id = workflow_args['WORKFLOW_RUN_ID']
    
    glue_client = boto3.client("glue")
    workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]

    missing_params = required_args - set(workflow_params.keys())
    custom_args = getResolvedOptions(sys.argv, missing_params)
    custom_args.update(workflow_params)
else:
    custom_args = getResolvedOptions(sys.argv, required_args)

job_type = custom_args['job_type'].lower()
s3_base_dir = custom_args['s3_base_dir']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## CONSTANTS ##
input_bucket_name = 's3-euw1-ap-pe-df-order-disseminator-files-prod'
output_bucket_name ='s3-use1-ap-pe-df-orders-insights-storage-d'
input_dir_path = 'AUSTRALIA/INPUT'

coulmns_alias = [("_c0", "PROCESS_NUMBER"),  ("_c9", "ACCOUNT_NUMBER"),  ("_c10", "DOCUMENT_DATE"),  ("_c11", "DOCUMENT_TYPE"),  ("_c12", "DOCUMENT_SUB_TYPE"), ("_c17", "LINE_TYPE"), ("_c24", "TYPE_OF_SALE"),  ("_c26", "CUSTOMER_REFERENCE"),  ("_c29", "ORIGINAL_ORDERED_ITEM"),  ("_c30", "ITEM"),  ("_c32", "ORIGINAL_CLAIM_QUANTITY"),  ("_c33", "ORDER_QUANTITY"),  ("_c34", "DELIVER_QUANTITY"),  ("_c38", "RETURN_QUANTITY"),  ("_c39", "REC_RETAIL_PRICE"),  ("_c40", "PRICE_PER"),  ("_c41", "UNIT_PRICE_NORMAL"),  ("_c42", "UNIT_PRICE_ACTUAL"),  ("_c43", "UNIT_PRICE_TAXABLE"),  ("_c47", "DISC_RATE_ACTUAL"),  ("_c52", "ACTUAL_TAX_RATE"),  ("_c53", "CURRENCY_EXCHANGE_CODE"),  ("_c54", "CURRENCY_EXCHANGE_RATE"),  ("_c55", "TRADE_AMOUNT_EX_TAX"),  ("_c56", "TRADE_AMOUNT_TAX"),  ("_c57", "BASE_AMOUNT_EX_TAX"),  ("_c58", "BASE_AMOUNT_TAX")]

sales_options = "'" + "','".join(['BC', 'BCSP', 'BCSO', 'BCSI', 'BCCM', 'BCSD', 'BCSC', 'BCSS', 'BCRM', 'BCRC', 'BCCN', 'BCHL', 'BCEX', 'BCTL', 'BCEV', 'BCFR', 'BCCS', 'BCSY', 'BCBK', 'BCBC', 'BCTR', 'BNSO', 'BNSI', 'BNSD', 'BNSC', 'BNRM', 'BNRC', 'BNHL', 'BNEX', 'BNEV', 'BNFS', 'BNFR', 'BNFA', 'BNCM', 'BNCN', 'BNTR', 'BNSY', 'BNSP', 'BNSS', 'BSFR', 'BSFS', 'BSIC', 'BSRV', 'BS', 'BSBC', 'BSBK', 'BSCS', 'BSEV', 'BSEX', 'BSHL', 'BSRC', 'BSRM', 'BSSC', 'BSSD', 'BSSI', 'BSSO', 'BSSP', 'AACN', 'AATR', 'AA', 'AABC', 'AABK', 'AACS', 'AAEV', 'AAEX', 'AAHL', 'AARC', 'AARM', 'AARV', 'AASC', 'AASD', 'AASI', 'AASO', 'AASP', 'AASS', 'AASY', 'ABCN', 'AB', 'ABBC', 'ABBK', 'ABCS', 'ABEV', 'ABEX', 'ABHL', 'ABRC', 'ABRM', 'ABSC', 'ABSD', 'ABSI', 'ABSO', 'ABSP', 'ABSS', 'ABSY', 'ABTR', 'AEBK', 'AESP', 'AEBC', 'AECS', 'AE', 'AERM', 'AESS', 'AERC', 'AETR', 'AEHL', 'AESO', 'AEEX', 'AECN', 'AESY', 'AESI', 'AEEV', 'AESD', 'AESC', 'AHBC', 'AH', 'AHCN', 'AHSI', 'AHSD', 'AHSC', 'AHRM', 'AHRC', 'AHHL', 'AHEX', 'AHEV', 'AHCS', 'AHTR', 'AHSY', 'AHSS', 'AHSP', 'AHBK', 'AHSO', 'AISO', 'AISP', 'AISS', 'AISY', 'AITR', 'AIBK', 'AICS', 'AIEV', 'AIEX', 'AIHL', 'AIRC', 'AIRM', 'AISC', 'AISD', 'AISI', 'AICN', 'AI', 'AIBC', 'ALSI', 'ALBC', 'ALSP', 'ALSS', 'ALSY', 'ALTR', 'ALBK', 'ALCS', 'ALEV', 'ALEX', 'ALHL', 'ALRC', 'ALRM', 'ALSC', 'ALSD', 'ALCN', 'AL', 'ALSO', 'ANSI', 'ANBC', 'ANSP', 'ANSS', 'ANSY', 'ANTR', 'ANCN', 'ANBK', 'ANCS', 'ANEV', 'ANEX', 'ANHL', 'ANRC', 'ANRM', 'ANSC', 'ANSD', 'AN', 'ANSO', 'AOSI', 'AOBC', 'AOSP', 'AOSS', 'AOSY', 'AOTR', 'AOCN', 'AOBK', 'AOCS', 'AOEV', 'AOEX', 'AOHL', 'AORC', 'AORM', 'AOSC', 'AOSD', 'AO', 'AOSO', 'AP', 'ASSI', 'ASBC', 'ASSP', 'ASSS', 'ASSY', 'ASTR', 'ASBK', 'ASCS', 'ASEV', 'ASEX', 'ASHL', 'ASRC', 'ASRM', 'ASSC', 'ASSD', 'ASCN', 'AS', 'ASSO']) + "'"

gratis_options = "'" + "','".join(['AAFA', 'AAFR', 'AAFS', 'AAIC', 'AACM', 'ABCM', 'ABFA', 'ABFR', 'ABFS', 'ABIC', 'ABRV', 'AERV', 'AEIC', 'AEFS', 'AEFR', 'AEFA', 'AECM', 'AHRV', 'AHIC', 'AHFS', 'AHFR', 'AHFA', 'AHCM', 'AICM', 'AIFA', 'AIFR', 'AIFS', 'AIIC', 'AIRV', 'ALCM', 'ALFA', 'ALFR', 'ALFS', 'ALIC', 'ALRV', 'ANCM', 'ANFA', 'ANFR', 'ANFS', 'ANIC', 'ANRV', 'AOCM', 'AOFA', 'AOFR', 'AOFS', 'AOIC', 'AORV', 'APCM', 'ASCM', 'ASFA', 'ASFR', 'ASFS', 'ASIC', 'ASRV', 'BCFA', 'BCCM', 'BCRV', 'BCIC', 'BCFS', 'BCFR', 'BNRV', 'BNIC', 'BNFS', 'BNFR', 'BNFA', 'BNCM', 'BSCM', 'BSFA', 'BSFR', 'BSFS', 'BSIC', 'BSRV']) + "'"

return_options = "'" + "','".join(['BC', 'BCSP', 'BCSO', 'BCSI', 'BCCM', 'BCSD', 'BCSC', 'BCSS', 'BCRM', 'BCRC', 'BCCN', 'BCHL', 'BCEX', 'BCTL', 'BCEV', 'BCFR', 'BCCS', 'BCSY', 'BCBK', 'BCBC', 'BCTR', 'BNSO', 'BNSI', 'BNSD', 'BNSC', 'BNRM', 'BNRC', 'BNHL', 'BNEX', 'BNEV', 'BNFS', 'BNFR', 'BNFA', 'BNCM', 'BNCN', 'BNTR', 'BNSY', 'BNSP', 'BNSS', 'BSFR', 'BSFS', 'BSIC', 'BSRV', 'BS', 'BSBC', 'BSBK', 'BSCS', 'BSEV', 'BSEX', 'BSHL', 'BSRC', 'BSRM', 'BSSC', 'BSSD', 'BSSI', 'BSSO', 'BSSP']) + "'"

purchase_options = "'" + "','".join(['AACN', 'AATR', 'AA', 'AABC', 'AABK', 'AACS', 'AAEV', 'AAEX', 'AAHL', 'AARC', 'AARM', 'AARV', 'AASC', 'AASD', 'AASI', 'AASO', 'AASP', 'AASS', 'AASY', 'ABCN', 'AB', 'ABBC', 'ABBK', 'ABCS', 'ABEV', 'ABEX', 'ABHL', 'ABRC', 'ABRM', 'ABSC', 'ABSD', 'ABSI', 'ABSO', 'ABSP', 'ABSS', 'ABSY', 'ABTR', 'AEBK', 'AESP', 'AEBC', 'AECS', 'AE', 'AERM', 'AESS', 'AERC', 'AETR', 'AEHL', 'AESO', 'AEEX', 'AECN', 'AESY', 'AESI', 'AEEV', 'AESD', 'AESC', 'AHBC', 'AH', 'AHCN', 'AHSI', 'AHSD', 'AHSC', 'AHRM', 'AHRC', 'AHHL', 'AHEX', 'AHEV', 'AHCS', 'AHTR', 'AHSY', 'AHSS', 'AHSP', 'AHBK', 'AHSO', 'AISO', 'AISP', 'AISS', 'AISY', 'AITR', 'AIBK', 'AICS', 'AIEV', 'AIEX', 'AIHL', 'AIRC', 'AIRM', 'AISC', 'AISD', 'AISI', 'AICN', 'AI', 'AIBC', 'ALSI', 'ALBC', 'ALSP', 'ALSS', 'ALSY', 'ALTR', 'ALBK', 'ALCS', 'ALEV', 'ALEX', 'ALHL', 'ALRC', 'ALRM', 'ALSC', 'ALSD', 'ALCN', 'AL', 'ALSO', 'ANSI', 'ANBC', 'ANSP', 'ANSS', 'ANSY', 'ANTR', 'ANCN', 'ANBK', 'ANCS', 'ANEV', 'ANEX', 'ANHL', 'ANRC', 'ANRM', 'ANSC', 'ANSD', 'AN', 'ANSO', 'AOSI', 'AOBC', 'AOSP', 'AOSS', 'AOSY', 'AOTR', 'AOCN', 'AOBK', 'AOCS', 'AOEV', 'AOEX', 'AOHL', 'AORC', 'AORM', 'AOSC', 'AOSD', 'AO', 'AOSO', 'AP', 'ASSI', 'ASBC', 'ASSP', 'ASSS', 'ASSY', 'ASTR', 'ASBK', 'ASCS', 'ASEV', 'ASEX', 'ASHL', 'ASRC', 'ASRM', 'ASSC', 'ASSD', 'ASCN', 'AS', 'ASSO', 'AAFA', 'AAFR', 'AAFS', 'AAIC', 'AACM', 'ABCM', 'ABFA', 'ABFR', 'ABFS', 'ABIC', 'ABRV', 'AERV', 'AEIC', 'AEFS', 'AEFR', 'AEFA', 'AECM', 'AHRV', 'AHIC', 'AHFS', 'AHFR', 'AHFA', 'AHCM', 'AICM', 'AIFA', 'AIFR', 'AIFS', 'AIIC', 'AIRV', 'ALCM', 'ALFA', 'ALFR', 'ALFS', 'ALIC', 'ALRV', 'ANCM', 'ANFA', 'ANFR', 'ANFS', 'ANIC', 'ANRV', 'AOCM', 'AOFA', 'AOFR', 'AOFS', 'AOIC', 'AORV', 'APCM', 'ASCM', 'ASFA', 'ASFR', 'ASFS', 'ASIC', 'ASRV', 'BCFA', 'BCCM', 'BCRV', 'BCIC', 'BCFS', 'BCFR', 'BNRV', 'BNIC', 'BNFS', 'BNFR', 'BNFA', 'BNCM', 'BSCM', 'BSFA', 'BSFR', 'BSFS', 'BSIC', 'BSRV']) + "'"

## Connect to s3 bucket ##
s3 = boto3.resource("s3")
s3_bucket = s3.Bucket(input_bucket_name)
s3_op_bucket = s3.Bucket(output_bucket_name)
    

def save_parquet(time_frame, year, output_dir_path):
    '''
    To process source data and store it in parquet files
    :param time_frame: s3 source dir structure
    :param year: reported source year
    :param output_dir_path: s3 path to store parquet file
    :return: None
    '''
    input_s3_uri = os.path.join('s3://' + input_bucket_name, input_dir_path, '')
    input_s3_dir = input_s3_uri + time_frame + '*/*sales*'

    try:
        # Read the input data
        datasource0 = spark.read.option("header", "false").csv(input_s3_dir)
        if datasource0.count() == 0:
            raise AnalysisException
            
    except AnalysisException:
        print("Data source does not exist for the time frame: ", time_frame)
        return False

    print("Data is present at AUSTRALIA location. Started processing the sales files for given period")
    print("> datasource0 count > ", datasource0.count())

    for ori, alias in  coulmns_alias:
        datasource0 = datasource0.withColumnRenamed(ori, alias)
            
    storeSalesDF = datasource0.select( datasource0["*"] )
    storeSalesDF = storeSalesDF.withColumn("FILENAME", input_file_name())
    storeSalesDF = storeSalesDF.withColumn('FILENAME', regexp_replace('FILENAME', input_s3_uri, ''))
    storeSalesDF = storeSalesDF.withColumn("FILENAME",expr("substring(FILENAME, 10, length(FILENAME)-4)"))
    storeSalesDF = storeSalesDF.withColumn('FILENAME', regexp_replace('FILENAME', '.csv', ''))
            
    storeSalesDF.show()
    storeSalesDF.printSchema()
    storeSalesDF.createOrReplaceTempView('salesdata')
                    
    stg_query = f"""
        SELECT
            t.reporting_date, t.internal_invoice_number, t.internal_order_number, t.external_invoice_number, t.external_transaction_number, t.external_purchase_order, t.billing_customer_id, t.p_product_id, t.p_backup_product_id, t.product_type, t.price, t.price_currency, t.publisher_price_ori, t.publisher_price_ori_currency, t.payment_amount, t.payment_amount_currency, t.ex_currencies, t.ex_rate, t.current_discount_percentage, t.tax_percentage, t.demand_units, t.units, t.trans_type_ori, t.sale_type_ori, t.doc_type_attribute, t.trans_type, t.sale_type, t.pod, source, t.source_id
            FROM
            (SELECT
                `DOCUMENT_DATE` as reporting_date, 'NA' as internal_Invoice_number, 'NA' as internal_order_number, 'NA' as external_invoice_number, 'NA' as external_transaction_number, process_number as external_purchase_order, account_number as billing_customer_id, `ITEM` as p_product_id, `ORIGINAL_ORDERED_ITEM` as p_backup_product_id, 'Print' as product_type, `UNIT_PRICE_ACTUAL` as price, 'AUD' as price_currency, `UNIT_PRICE_NORMAL` as publisher_price_ori, 'AUD' as publisher_price_ori_currency, `TRADE_AMOUNT_EX_TAX` as payment_amount, `CURRENCY_EXCHANGE_CODE` as payment_amount_currency, 
                case 
                    when CURRENCY_EXCHANGE_CODE == 'N' then 'AUD/NZD' ELSE 'NA' 
                    end as ex_currencies, 
                currency_exchange_rate as ex_rate, `DISC_RATE_ACTUAL` as current_discount_percentage, ACTUAL_TAX_RATE as tax_percentage, `ORDER_QUANTITY` as demand_units, deliver_quantity as units, document_type as trans_type_ori, document_sub_type as sale_type_ori, type_of_sale as doc_type_attribute, 
                case 
                    when concat(trim(document_type), trim(document_sub_type), trim(type_of_sale)) in ({sales_options}) then 'Sales'
                    when concat(trim(document_type), trim(document_sub_type), trim(type_of_sale)) in ({gratis_options}) then 'Gratis' else 'Sales' 
                end as trans_type, 
                case 
                    when concat(trim(document_type), trim(document_sub_type), trim(type_of_sale)) in ({return_options}) then 'Return'
                    when concat(trim(document_type), trim(document_sub_type), trim(type_of_sale)) in ({purchase_options}) then 'Purchase' else 'Purchase' 
                end as sale_type, 
                case 
                    when concat(trim(document_type), trim(document_sub_type), trim(type_of_sale)) in ('APCM', 'AP') then 'Y' else 'N' 
                end as pod, 
                'AUS_TLD' as source, FILENAME as source_id
                FROM salesdata
            ) t"""    
    staging_df_interim = spark.sql(stg_query)
    staging_df = staging_df_interim.withColumn("year",lit(year))
    print('total count',staging_df.count())
    staging_df.show()
    staging_df.printSchema()
            
    staging_df.coalesce(1).write.option("header", True).partitionBy(
        "year", "product_type", "trans_type"
        ).mode(
            'append'
            ).parquet(
                's3://' + output_bucket_name + '/' + output_dir_path
                )         
    print("> writing to s3  location is successful >")


def initialise():
    '''
        Method to initialise the glue job
        :return : None
    '''
    output_dir_path  = f"mapped_layer/revenue/warehouse/{s3_base_dir}"

    if job_type == 'live':
        # Fetch previous date data
        time_frame = (date.today()-timedelta(days=1)).strftime('%Y%m%d')
        time_frame_list = [time_frame]
    else:
        time_frame = custom_args['time_frame']
        end_time_frame = custom_args['end_time_frame']
        print('generating the historical data for : ', time_frame, ' - ', end_time_frame)

        time_frame_list = gen_time_frame_list(time_frame, end_time_frame)
        print('time_frame_list: ', time_frame_list)

    for time_frame in time_frame_list:
        year = time_frame[:4]
        save_parquet(time_frame, year, output_dir_path)

        print(f"< successfully processed job for the time frame: {time_frame}")

initialise()
job.commit()
