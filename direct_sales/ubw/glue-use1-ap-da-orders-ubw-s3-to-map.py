import os
import re
import sys
import boto3
from datetime import date, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, unix_timestamp, to_date, date_format
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
    
s3_input_currencies_file = 's3://s3-use1-ap-pe-df-orders-insights-storage-d/OWNED_SITES/historical/DB DUMPS/TBL_CURRENCIES.csv'
s3_input_application_file = 's3://s3-use1-ap-pe-df-orders-insights-storage-d/OWNED_SITES/historical/DB DUMPS/TBL_APPLICATIONS.csv'

input_bucket_name = 's3-use1-ap-pe-df-orders-insights-storage-d'
output_bucket_name ='s3-use1-ap-pe-df-orders-insights-storage-d'
input_dir_path = 'OWNED_SITES/input'


def create_view_currencies():
    currencies_df = spark.read.option("header", "true").option("quote", "\"").option("escape","\"").csv(s3_input_currencies_file)
    currencies_df.createOrReplaceTempView("currencies_tab")


def create_view_application():
    application_df = spark.read.option("header", "true").option("quote", "\"").option("escape","\"").csv(s3_input_application_file)
    application_df.createOrReplaceTempView("application_tab")


def create_view_invoices(invoices_file):
    '''
    '''
    invoices_df = spark.read.option("header", "true").option("quote", "\"").option("escape","\"").csv(
        invoices_file
        ).where(
                col("STATUS_ID").isin({"1001", "1002","1008"})
            ).select(
                "ID", "TRANSACTION_DATE", "CURRENCY_ID", "DISCOUNT_CODE", 
                "BILLING_COUNTRY_ID", "INVOICE_NO", "APPLICATION_ID"
                )
    invoices_df.createOrReplaceTempView("invoices_tab")


def create_view_invoices_item(invoices_item_file):
    '''
    '''
    invoices_item_df = spark.read.option("header", "true").option("quote", "\"").option("escape","\"").csv(
        invoices_item_file
        ).select(
            "INVOICE_ID", "ISBN", "SKU", "DESCRIPTION", "Vitalsource_Code", 
            "ORIGINAL_BASE_PRICE", "Price_Type_ID", "LINE_TOTAL", "Quantity", 
            "REVIEWED_BASE_PRICE", "DISCOUNT"
            )
    invoices_item_df.createOrReplaceTempView("invoices_item_tab")


def save_parquet(invoices_file, invoices_item_file, output_dir_path):
    '''
    '''
    create_view_currencies()
    create_view_application()
    
    try:
        create_view_invoices(invoices_file)
        create_view_invoices_item(invoices_item_file)
    except AnalysisException:
        print("Data source does not exist for the file: ", invoices_file)
        return False

    result_df = spark.sql("""
    SELECT
        'TANDF' as aggregator_name,a.TRANSACTION_DATE as reporting_date,INVOICE_NO as internal_Invoice_number,INVOICE_ID as other_order_ref, substr(a.TRANSACTION_DATE,1,4) as year, 
        case 
            when Vitalsource_Code != 'NULL' then cast(cast(ISBN as decimal(15,0)) as string) else 'NA' 
        end as e_product_id, 
        case 
            when Vitalsource_Code != 'NULL' then SKU else 'NA' 
        end as e_backup_product_id, 
        case 
            when Vitalsource_Code = 'NULL' then cast(cast(ISBN as decimal(15,0)) as string) else 'NA' 
        end as p_product_id, 
        case 
            when Vitalsource_Code = 'NULL' then SKU else 'NA' 
        end as p_backup_product_id, 
        b.DESCRIPTION as product_title, 
        case 
            when Vitalsource_Code = 'NULL' then 'Print' else 'Electronic' 
        end as product_type, 
        b.ORIGINAL_BASE_PRICE as price, ISO_NAME as price_currency, LINE_TOTAL as payment_amount, 
        ISO_NAME as payment_amount_currency, DISCOUNT as current_discount_percentage, DISCOUNT_CODE as disc_code,
        case Price_Type_ID 
            when '8' then 12 
            when '12' then 3 
            when '9' then 6 
            else 0 
        end as new_rental_duration,
        'month' as rental_duration_measure, QUANTITY as units, Price_Type_ID as trans_type_ori,
        case 
            when Price_Type_ID = '1' then 'Sales' 
            when Price_Type_ID in ('8','9','12') then 'Rental' 
        end as trans_type, 
        case 
            when Price_Type_ID = '1' then 'Purchase' 
            when Price_Type_ID in ('8','9','12') then 'Checkout'
        end as sale_type, 
        BILLING_COUNTRY_ID as country, 'UBW' as source, 'Routelege.com' as sub_domain, a.ID as source_id, 'NA' as external_invoice_number, 
        'NA' as external_purchase_order, 0 as internal_order_number, 'NA' as external_transaction_number, 'NA' as billing_customer_id 
    FROM
        invoices_tab a 
    inner join
        invoices_item_tab b  on a.ID = b.INVOICE_ID
    left join
        currencies_tab c  on a.CURRENCY_ID = c.ID
    """)
    result_df = result_df.withColumn(
        'reporting_date', 
        to_date(unix_timestamp(col('reporting_date'), 'yyyy-MM-dd HH:mm:ss.SSS').cast("timestamp"))
        )
    result_df = result_df.withColumn('reporting_date', date_format(col('reporting_date'), 'yyyy-MM-dd'))

    print("preresult_df count : ", result_df.count())
    result_df.show(5)
    result_df.printSchema()

    result_df.coalesce(1).write.option("header", True).partitionBy(
        "year", "product_type", "trans_type"
        ).mode(
            'append'
            ).parquet(
                's3://' + output_bucket_name + '/' + output_dir_path
                )   


def initialise():
    '''
        Method to initialise the glue job
        :return : None
    '''
    if job_type == 'historical':
        invoices_file = f"s3://{input_bucket_name}/OWNED_SITES/historical/DB DUMPS/TBL_INVOICE.csv"
        invoices_item_file = f"s3://{input_bucket_name}/OWNED_SITES/historical/DB DUMPS/TBL_INVOICE_ITEMS.csv"

        output_dir_path  = f"mapped_layer/revenue/direct_sales/{s3_base_dir}/"
        save_parquet(invoices_file, invoices_item_file, output_dir_path)
        print(f"< successfully processed job for historical data")
        return True

    if job_type == 'live':
        # Fetch previous date data
        time_frame = (date.today()-timedelta(days=1)).strftime('%Y%m%d')
        time_frame_list = [time_frame]
    else:
        time_frame = custom_args['time_frame']
        end_time_frame = custom_args['end_time_frame']
        print('generating the historical data for : ', time_frame, ' - ', end_time_frame)

        time_frame_list = gen_time_frame_list(time_frame, end_time_frame)

    output_dir_path  = f"mapped_layer/revenue/direct_sales/{s3_base_dir}"

    for time_frame in time_frame_list:
        print('Processing time_frame: ', time_frame)
        input_s3_uri = os.path.join('s3://' + input_bucket_name, input_dir_path, '')
        invoices_file = input_s3_uri + time_frame + '*/' + 'TBL_INVOICES.csv'
        invoices_item_file = input_s3_uri + time_frame + '*/' + 'TBL_INVOICE_ITEMS.csv'

        save_parquet(invoices_file, invoices_item_file, output_dir_path)
        print(f"< successfully processed job for the time frame: {time_frame}")

initialise()
job.commit()
