import sys
import boto3
from datetime import date, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, lit, to_date, unix_timestamp

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
input_bucket_name = 's3-use1-ap-pe-df-orders-insights-storage-d'
output_bucket_name ='s3-use1-ap-pe-df-orders-insights-storage-d'


def read_data(year, input_dir_path):
    '''
    To process and read source data
    :param year: reporting source year
    :param input_dir_path: s3 path to fetch data
    :return: None
    '''
    datasource0 = spark.read.option("header", "true").parquet(
        's3://' + input_bucket_name + '/' + input_dir_path + '/year=' + year + '/*'
        )
    print("> datasource0 count > ", datasource0.count())
    datasource0.createOrReplaceTempView('salesdata')


def save_parquet(year, input_dir_path, output_dir_path):
    '''
    To process source data and store it in parquet files
    :param year: reporting source year
    :param input_dir_path: s3 path to fetch data
    :param output_dir_path: s3 path to store parquet file
    :return: None
    '''
    # Generate sales data
    read_data(year, input_dir_path)

    staging_df_interim = spark.sql("""
    SELECT
        t.reporting_date, t.internal_invoice_number, t.internal_order_number, t.external_purchase_order, t.external_invoice_number, 
        t.external_transaction_number, t.other_order_ref, t.shipping_customer_id, t.billing_customer_id, t.p_product_id, t.p_backup_product_id, 
        t.price, t.price_currency, t.publisher_price_ori, t.publisher_price_ori_currency, t.payment_amount, t.payment_amount_currency, 
        t.ex_currencies, t.ex_rate, t.current_discount_percentage, t.tax, t.pod_ori, t.pod, t.demand_units, t.units, t.trans_type_ori, 
        t.trans_type, t.sale_type_ori, t.sale_type, t.source, t.source_id
    from
        (SELECT
            reporting_date, internal_invoice_number, internal_order_number, external_purchase_order, 
            NVL(external_invoice_number, 'NA') as external_invoice_number, external_transaction_number, 
            NVL(other_order_ref, 'NA') as other_order_ref, 
            NVL(shipping_customer_id, 'NA') as shipping_customer_id, 
            NVL(billing_customer_id, 'NA') as billing_customer_id, 
            p_product_id, p_backup_product_id, abs(cast(price as double)) as price, price_currency, 
            cast(publisher_price_ori as double) as publisher_price_ori, 
            publisher_price_ori_currency, cast(payment_amount as double) as payment_amount, 
            NVL(payment_amount_currency, 'GBP') as payment_amount_currency, NVL(ex_currencies, 'NA') as ex_currencies, 
            cast(ex_rate as double) as ex_rate, cast(current_discount_percentage as double) as current_discount_percentage, 
            cast(tax as double) as tax, pod_ori, pod, cast(demand_units as int) as demand_units, cast(units as int) as units, 
            trans_type_ori, 
            case 
                when (trans_type_ori = 'S' or trans_type_ori = 'R') then 'Sales'
                when trans_type_ori = 'Z' then 'Gratis'  
                when trans_type_ori = 'T' then 'Transfer' 
            end as trans_type, 
            sale_type_ori, NVL(sale_type, 'NA') as sale_type, source, source_id
        from
            salesdata
        )t""")

    staging_df_date = staging_df_interim.withColumn(
        'reporting_date', to_date(unix_timestamp(col('reporting_date'), 'yyyy-MM-dd').cast("timestamp"))
        )
    staging_df = staging_df_date.withColumn('year', lit(year))
    staging_df = staging_df.withColumn('product_type', lit("Print"))

    staging_df.show()
    staging_df.printSchema()

    staging_df.write.option("header", True).partitionBy(
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
    input_dir_path = f'mapped_layer/revenue/warehouse/{s3_base_dir}'
    output_dir_path = f'staging_layer/revenue/warehouse/{s3_base_dir}'

    if job_type =='live':
        # Fetch previous date data
        time_frame = (date.today()-timedelta(days=1)).strftime('%Y%m%d')
        time_frame_list = [time_frame]
    else:
        time_frame = custom_args['time_frame'][:4]
        end_time_frame = custom_args['end_time_frame'][:4]
        print('generating the historical data for : ', time_frame, ' - ', end_time_frame)

        time_frame_list = gen_time_frame_list(time_frame, end_time_frame)

    for time_frame in time_frame_list:
        print('Processing time_frame: ', time_frame)
        year = time_frame[:4]
        save_parquet(year, input_dir_path, output_dir_path)
        
        print(f"< successfully processed job for the time frame: {time_frame}")

initialise()
job.commit()
