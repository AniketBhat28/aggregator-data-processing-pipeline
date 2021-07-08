import os
import sys
import boto3
from datetime import date, datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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


def create_view_input_tab(input_dir_path):
    input_s3_uri = os.path.join('s3://' + input_bucket_name, input_dir_path)
    input_df = spark.read.option("header", "true").parquet(input_s3_uri)
    input_df.createOrReplaceTempView("input_tab")
    spark.sql("select * from input_tab").show()


def save_parquet(input_dir_path, output_dir_path):
    '''
    '''
    create_view_input_tab(input_dir_path)

    result_df = spark.sql("""select
            aggregator_name, cast(reporting_date as date
            ) as reporting_date, cast(internal_Invoice_number as bigint
            ) as internal_Invoice_number, other_order_ref, year, e_product_id, e_backup_product_id, p_product_id, p_backup_product_id, product_title, product_type, cast( price as double
            ) as price,  price_currency, cast(payment_amount as double
            ) as payment_amount, payment_amount_currency, cast(current_discount_percentage as double
            ) as current_discount_percentage, disc_code,new_rental_duration,rental_duration_measure, cast(units as int
            ) as units, trans_type_ori,trans_type, sale_type,country,source, sub_domain,source_id,external_invoice_number , external_purchase_order,internal_order_number, external_transaction_number, billing_customer_id 
        from
            input_tab
        """)
    print("preresult_df count : ",result_df.count())
    result_df.createOrReplaceTempView("result_df_tab")
    result_df.show()

    result_df.show()
    result_df.coalesce(1).write.option("header",True).mode('overwrite').partitionBy(
        'year', 'product_type', 'trans_type'
        ).parquet(
            's3://' + output_bucket_name + '/' + output_dir_path
        )


def initialise():
    '''
        Method to initialise the glue job
        :return : None
    '''
    input_dir_path = f'mapped_layer/revenue/direct_sales/{s3_base_dir}'
    output_dir_path = f'staging_layer/revenue/direct_sales/{s3_base_dir}'

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
        save_parquet(input_dir_path, output_dir_path)
        print(f"< successfully processed job for the time frame: {time_frame}")

initialise()
job.commit()
