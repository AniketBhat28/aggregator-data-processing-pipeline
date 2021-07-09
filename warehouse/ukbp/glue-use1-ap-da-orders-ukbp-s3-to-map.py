import os
import sys
import boto3
from datetime import date, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, lit
from pyspark.sql.functions import unix_timestamp, input_file_name, regexp_replace,expr
from pyspark.sql.functions import col, to_date, date_format
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
input_dir_path = 'UK-BOOKPOINT/INPUT'


def read_data(time_frame):
    '''
    To process and read source data
    :param time_frame: s3 source dir structure
    :return: None
    '''
    input_s3_uri = os.path.join('s3://' + input_bucket_name, input_dir_path, '')
    input_s3_dir = input_s3_uri + time_frame + '*/*sales*'

    datasource0 = spark.read.option("header", "true").csv(input_s3_dir)
    if datasource0.count() == 0:
        raise AnalysisException(None, None)
    print("> datasource0 count > ", datasource0.count())

    df1= datasource0.withColumn("input_filename",input_file_name())
    df1 = df1.withColumn('input_filename', regexp_replace('input_filename', input_s3_uri, ''))
    df1 = df1.withColumn("input_filename", expr("substring(input_filename, 10)"))
    df1 = df1.withColumn('input_filename', regexp_replace('input_filename', '.txt', ''))
    df1.createOrReplaceTempView('salesdata')


def save_parquet(time_frame, year, output_dir_path):
    '''
    To process source data and store it in parquet files
    :param time_frame: s3 source dir structure
    :param year: reporting source year
    :param output_dir_path: s3 path to store parquet file
    :return: None
    '''
    # Generate sales data
    try:
        read_data(time_frame)
    except AnalysisException:
        print("Data source does not exist for the time frame: ", time_frame)
        return False

    staging_df_interim = spark.sql("""
    SELECT
        t.reporting_date, t.internal_invoice_number, t.internal_order_number, t.external_purchase_order, t.external_invoice_number, 
        t.external_transaction_number, t.other_order_ref, t.shipping_customer_id, t.billing_customer_id, t.p_product_id, t.p_backup_product_id, 
        t.product_type, t.price, t.price_currency, t.publisher_price_ori, t.publisher_price_ori_currency, t.payment_amount, t.payment_amount_currency, 
        t.ex_currencies, t.ex_rate, t.current_discount_percentage, t.tax, t.pod_ori, t.pod, t.demand_units, t.units, t.trans_type_ori, t.trans_type, 
        t.sale_type_ori, t.sale_type, t.source, t.source_id
    from
        (SELECT
            doc_date as reporting_date, 'NA' as internal_Invoice_number, 'NA' as Internal_order_number, 'NA' as External_Purchase_Order, batch_no as external_invoice_number, 'NA' as External_Transaction_number, cust_ref as other_order_ref, mail_cust as shipping_customer_id, inv_sta_cust as billing_customer_id, sbn13 as p_product_id, sbn as p_backup_product_id, 'Print' as product_type, pub_price as price, 'GBP' as price_currency, pub_price_orig as publisher_price_ori, 'GBP' as publisher_price_ori_currency, equiv_val as payment_amount, currency as payment_amount_currency, 
            case 
                when currency = 'EUR' then 'GBP/EUR' 
                when currency = 'USD' then 'GBP/USD' 
            end as ex_currencies, 
            equiv_roe as ex_rate, disc_pct as current_discount_percentage, vat_val as tax, doc_type as pod_ori, 
            case 
                when doc_type in ('CXNS', 'GXNS', 'IXNS'
                ) then 'Y' 
                else 'N' 
            end as pod, 
            demand_qty as demand_units, del_qty as units, tran_type as trans_type_ori, 
            case 
                when (tran_type = 'S' 
                    or tran_type = 'R'
                ) then 'Sales' 
                when tran_type = 'Z' then 'Gratis' 
                when tran_type = 'T' then 'Transfer' 
            end as trans_type, 
            INV_C_N_PRO as sale_type_ori, 
            case 
                when INV_C_N_PRO = 'I' then 'Purchase' 
                when INV_C_N_PRO = 'C' then 'Return' 
            end as sale_type, 
            'UKBP' as source, input_filename as source_id
        from
            salesdata
        )t""")
    staging_df_date=staging_df_interim.withColumn('reporting_date', 
                    to_date(unix_timestamp(col('reporting_date'), 'yyyyMMdd').cast("timestamp")))
                    
    staging_df = staging_df_date.withColumn('reporting_date', date_format(col('reporting_date'), 'yyyy-MM-dd'))
    staging_df = staging_df.withColumn('year', lit(year))
    staging_df.show()
    staging_df.printSchema()

    staging_df.coalesce(1).write.option("header", True).partitionBy(
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
    output_dir_path = f'mapped_layer/revenue/warehouse/{s3_base_dir}'

    if job_type =='live':
        # Fetch previous date data
        time_frame = (date.today()-timedelta(days=1)).strftime('%Y%m%d')
        time_frame_list = [time_frame]
    else:
        time_frame = custom_args['time_frame']
        end_time_frame = custom_args['end_time_frame']
        print('generating the historical data for : ', time_frame, ' - ', end_time_frame)

        time_frame_list = gen_time_frame_list(time_frame, end_time_frame)

    for time_frame in time_frame_list:
        print('Processing time_frame: ', time_frame)
        year = time_frame[:4]
        save_parquet(time_frame, year, output_dir_path)

        print(f"< successfully processed job for the time frame: {time_frame}")


initialise()
job.commit()
