import os
import sys
import boto3
from datetime import date, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import unix_timestamp, to_date, date_format
from pyspark.sql.functions import input_file_name, regexp_replace, expr
from pyspark.sql.functions import col, lit
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
input_dir_path = 'SINGAPORE/INPUT'


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
        datasource0 = spark.read.option("header", "true").csv(input_s3_dir)
        if datasource0.count() == 0:
            raise AnalysisException
            
    except AnalysisException:
        print("Data source does not exist for the time frame: ", time_frame)
        return False

    print("> datasource0 count > ", datasource0.count())
    
    storeSalesDF = datasource0.withColumn("FILENAME", input_file_name())
    storeSalesDF = storeSalesDF.withColumn('FILENAME', regexp_replace('FILENAME', input_s3_uri, ''))
    storeSalesDF = storeSalesDF.withColumn("FILENAME",expr("substring(FILENAME, 10, length(FILENAME)-4)"))
    storeSalesDF = storeSalesDF.withColumn('FILENAME', regexp_replace('FILENAME', '.csv', ''))
    storeSalesDF = storeSalesDF.withColumn('FILENAME', regexp_replace('FILENAME', '.CSV', ''))
    storeSalesDF = storeSalesDF.withColumn('FILENAME', regexp_replace('FILENAME', '.TXT', ''))
    
    # storeSalesDF.coalesce(1).write.option("header",True).mode('append').csv('s3://s3-use1-ap-pe-df-orders-insights-storage-d/raw_layer/sgbm/'+time_frame+'/')
    storeSalesDF.printSchema()
    storeSalesDF.show()
    storeSalesDF.createOrReplaceTempView('salesdata')
    
    stg_query = """
    SELECT
        t.reporting_date, t.internal_invoice_number, t.internal_order_number, t.external_purchase_order, t.external_invoice_number, t.external_transaction_number, t.shipping_customer_id, t.billing_customer_id, t.p_product_id, t.p_backup_product_id, t.product_type, t.price, t.price_currency, t.publisher_price_ori, t.publisher_price_ori_currency, t.payment_amount, t.payment_amount_currency, t.ex_currencies, t.ex_rate, t.current_discount_percentage, t.tax, t.pod_ori, t.pod, t.demand_units, t.units, t.trans_type_ori, t.sale_type_ori, t.trans_type, t.sale_type, t.source, t.source_id
    from
        (SELECT
            `DOC-DATE` as reporting_date, 'NA' as internal_invoice_number, 'NA' as internal_order_number, 'NA' as external_purchase_order, `DOC-REF` as external_invoice_number, 'NA' as external_transaction_number, `STA-CUST` as shipping_customer_id, CUST as billing_customer_id, `13 ISBN` as p_product_id, `10 ISBN` as p_backup_product_id, 'PRINT' as product_type, `PUB-VAL` as price, 'SGD' as price_currency, `PUB-PRICE` as publisher_price_ori, 'NA' as publisher_price_ori_currency, `NET-VAL` as payment_amount, 'SGD' payment_amount_currency, 
            case 
                when Currency = 'S$' then 'NA' 
                when Currency = 'GBP' then 'SGD/GBP' 
                when Currency = 'USD' then 'SGD/USD' 
                end as ex_currencies, 
            `equiv-roe` as ex_rate, `DISC-%` as current_discount_percentage, `VAT-VAL` as tax, POD as pod_ori, 
            NVL2(POD, 'Y', 'N') as pod, `demand-qty` as demand_units, `del-qty` as units, `tran-type` as trans_type_ori, 
            `INV/C/N/RPO` as sale_type_ori, 
            case 
                when `tran-type` = 'S' then 'Sale' 
                when `tran-type` = 'Z' then 'Gratis' 
            end as trans_type, 
            case 
                when `INV/C/N/RPO` = 'I' then 'Purchase' 
                when `INV/C/N/RPO` = 'C' then 'Return' 
            end as sale_type, 'SGBM' as source, FILENAME as source_id
        from
            salesdata
        )t"""
        
    staging_df_interim = spark.sql(stg_query)
    staging_df_date = staging_df_interim.withColumn(
        'reporting_date', to_date(unix_timestamp(col('reporting_date'), 'dd-MM-yy').cast("timestamp"))
        )
    staging_df = staging_df_date.withColumn('reporting_date', date_format(col('reporting_date'), 'yyyy-MM-dd'))
    staging_df = staging_df.withColumn("year",lit(year))
    
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
        print('time_frame_list: ', time_frame_list)

    for time_frame in time_frame_list:
        year = time_frame[:4]
        save_parquet(time_frame, year, output_dir_path)

        print(f"< successfully processed job for the time frame: {time_frame}")

initialise()
job.commit()
