import sys
import boto3
from datetime import date, datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, to_date, unix_timestamp, lit

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


def save_parquet(year, input_dir_path, output_dir_path):
    '''
    To process source data and store it in parquet files
    :param year: reported source year
    :param input_dir_path: s3 path to fetch parquet file
    :param output_dir_path: s3 path to store parquet file
    :return: None
    '''
    # Read the input data
    datasource0 = spark.read.option("header", "true").parquet(
        's3://' + input_bucket_name + '/' + input_dir_path + '/year=' + year + '/*'
        )
    if datasource0.count() == 0:
        print("Data does not exist for today")
        return False
    
    print("> datasource0 count > ", datasource0.count())
    datasource0.createOrReplaceTempView('salesdata')
    print(">>>>>df1",datasource0.columns)
    print(datasource0.head(10))
                
    stg_query = """select
        t.reporting_date, t.internal_invoice_number, t.internal_order_number, t.external_purchase_order, t.external_invoice_number, t.external_transaction_number, t.shipping_customer_id, t.billing_customer_id, t.p_product_id, t.p_backup_product_id, t.product_type, t.price, t.price_currency, t.publisher_price_ori, t.publisher_price_ori_currency, t.payment_amount, t.payment_amount_currency, t.ex_currencies, t.ex_rate, t.current_discount_percentage, t.tax, t.pod_ori, t.pod, t.demand_units, t.units, t.trans_type_ori, t.sale_type_ori, t.trans_type, t.sale_type, t.source, t.source_id 
    from
        (select
            reporting_date, internal_invoice_number, internal_order_number, external_purchase_order, 
            NVL(external_invoice_number, 'NA') as external_invoice_number, external_transaction_number, 
            NVL(shipping_customer_id, 'NA') as shipping_customer_id, 
            NVL(billing_customer_id, 'NA) as billing_customer_id, 
            trim(p_product_id) as p_product_id, trim(p_backup_product_id) as p_backup_product_id, 'PRINT' as product_type, 
            cast(price as double) as price, price_currency, 
            cast( publisher_price_ori as double) as publisher_price_ori, publisher_price_ori_currency, 
            cast(payment_amount as double) as payment_amount, payment_amount_currency, ex_currencies, 
            cast(ex_rate as double) as ex_rate, cast(current_discount_percentage as double) as current_discount_percentage, 
            cast(tax as double) as tax, pod_ori, pod, cast(demand_units as int) as demand_units, 
            cast(units as int) as units, trans_type_ori, sale_type_ori, 
            case 
                when trans_type_ori = 'S' then 'Sale' 
                when trans_type_ori = 'Z' then 'Gratis' 
            end as trans_type, 
            sale_type, source, source_id 
        from
            salesdata)t"""
    
    staging_df_interim = spark.sql(stg_query)
    staging_df_date=staging_df_interim.withColumn('reporting_date',to_date(unix_timestamp(col('reporting_date'), 'yyyy-MM-dd').cast("timestamp")))
    print(">>>>>df1",staging_df_interim.columns)
    staging_df = staging_df_date.withColumn('year', lit(year))
    print(">>>>>df1",staging_df_interim.columns)
    
    staging_df.show()
    staging_df.printSchema()
    staging_df.coalesce(1).write.option("header",True).partitionBy(
        "year","product_type","trans_type"
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
        time_frame = custom_args['time_frame']
        end_time_frame = custom_args['end_time_frame']
        print('generating the historical data for : ', time_frame, ' - ', end_time_frame)
        
        # Loop the start and end years to fetch and store all the records at once
        date_strings = ['%Y', '%Y%m', '%Y%m%d']
        for date_string in date_strings:
            try:
                time_frame = datetime.strptime(time_frame, date_string)
                end_time_frame = datetime.strptime(end_time_frame, date_string)
                break
            except ValueError:
                pass

        time_frame_list = gen_time_frame_list(time_frame, end_time_frame)

    for time_frame in time_frame_list:
        year = time_frame[:4]
        save_parquet(year, input_dir_path, output_dir_path)
        
        print(f"< successfully processed job for the time frame: {time_frame}")

initialise()
job.commit()
