import sys
import boto3
from datetime import date, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import input_file_name, regexp_replace, to_date, date_format, unix_timestamp
from pyspark.sql.functions import col, lit, expr
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
input_s3_uri = 's3://s3-euw1-ap-pe-df-order-disseminator-files-prod/US-PT/INPUT/'
output_bucket_name ='s3-use1-ap-pe-df-orders-insights-storage-d'

stg_sql = """select
        source, sys_source, doc_type, doc_date, invno, tran_type, ship_cust, bill_cust, brick_cd, trade, discount, isbn_10, isbn_13, pack_type, pub_price, pub_value, net_value, del_qty, ori_docref, currency, ori_docdt, flex_cat, publisher, imprint, sale_type, action_no, cat_no, order_srce, order_date, price_type, dist_chan, cred_reas, grat_reas, supplysite, desp_value, sale_price, line_no, itemtype, pod, ledger, bulk, trialofrcd, demandqty, authorsale, custpono, pod_supp, refno, country, statprov, shipcode, listcd, release, tax_amount, web_id 
    from"""


def read_default_data(time_frame):
    '''
    To process recent data other than 2017 & 2018 data
    :param time_frame: s3 source dir structure
    :return: None
    '''
    datasource0 = spark.read.option("header", "true").csv(input_s3_uri + time_frame + '*/*SALES*')
    return datasource0


def read_2017_data(time_frame):
    '''
    To process 2017 data separately
    :param time_frame: s3 source dir structure
    :return: None
    '''
    datasource0 = spark.read.option("header", "true").csv(input_s3_uri + time_frame + '*/*SALES*')
    datasource0 = datasource0.drop('_c0')
    return datasource0


def read_2018_data_yearly(time_frame):
    '''
    To process 2018 yearly data separately
    :param time_frame: s3 source dir structure
    :return: None
    '''
    datasource1 = spark.read.option("header", "true").csv(input_s3_uri+'20180[1-7]'+'*/*SALES*')
    datasource4 = spark.read.option("header", "true").csv(input_s3_uri+'2018080[1-3]'+'*/*SALES*')
    
    datasource5 = spark.read.option("header", "true").csv(input_s3_uri+'2018080[4-9]'+'*/*SALES*')
    datasource6 = spark.read.option("header", "true").csv(input_s3_uri+'2018081[0-9]'+'*/*SALES*')
    datasource7 = spark.read.option("header", "true").csv(input_s3_uri+'2018082[0-9]'+'*/*SALES*')
    datasource8 = spark.read.option("header", "true").csv(input_s3_uri+'2018083[0-1]'+'*/*SALES*')
    
    datasource2 = spark.read.option("header", "true").csv(input_s3_uri+'201809'+'*/*SALES*')
    datasource3 = spark.read.option("header", "true").csv(input_s3_uri+'20181[0-2]'+'*/*SALES*')

    datasource9 = datasource1.union(datasource4)
    datasource10 = datasource2.union(datasource3).union(datasource5).union(datasource6).union(datasource7).union(datasource8)
    
    datasource9.createOrReplaceTempView('2018temp')
    datasource11 = spark.sql(stg_sql+' 2018temp')
    
    datasource10.createOrReplaceTempView('2018temp1')
    datasource12 = spark.sql(stg_sql+' 2018temp1')
    
    datasource0 = datasource11.union(datasource12)
    return datasource0


def read_2018_data_monthly(time_frame):
    '''
    To process 2018 monthly data separately
    :param time_frame: s3 source dir structure
    :return: None
    '''
    if time_frame == '201808':
        print('August')
        datasource1 = spark.read.option("header", "true").csv(input_s3_uri+'2018080[1-3]'+'*/*SALES*')
        
        datasource2 = spark.read.option("header", "true").csv(input_s3_uri+'2018080[4-9]'+'*/*SALES*')
        datasource3 = spark.read.option("header", "true").csv(input_s3_uri+'2018081[0-9]'+'*/*SALES*')
        datasource4 = spark.read.option("header", "true").csv(input_s3_uri+'2018082[0-9]'+'*/*SALES*')
        datasource5 = spark.read.option("header", "true").csv(input_s3_uri+'2018083[0-1]'+'*/*SALES*')
        datasource6 = datasource2.union(datasource3).union(datasource4).union(datasource5)

        datasource1.createOrReplaceTempView('2018temp')
        datasource7 = spark.sql(stg_sql+' 2018temp')

        datasource6.createOrReplaceTempView('2018temp1')
        datasource8 = spark.sql(stg_sql+' 2018temp1')

        datasource0 = datasource7.union(datasource8)
    else:
        print('Others')
        datasource0 = read_default_data(time_frame)    
    return datasource0


def read_2018_data(time_frame):
    '''
    To process 2018 data separately
    :param time_frame: s3 source dir structure
    :return: None
    '''
    spans = {
                4:read_2018_data_yearly,
                6:read_2018_data_monthly
            }
    datasource0 = spans.get(len(time_frame), read_default_data)(time_frame)
    return datasource0


def read_year_data(year, time_frame):
    '''
    To process separately for each different year
    :param year: reporting source year
    :param time_frame: s3 source dir structure
    :return: None
    '''
    ops =   {
                '2017':read_2017_data,
                '2018':read_2018_data
            }
    return ops.get(year, read_default_data)(time_frame)


def read_data(year, time_frame):
    '''
    To process and read source data
    :param year: reporting source year
    :param time_frame: s3 source dir structure
    :return: None
    '''
    datasource = read_year_data(year, time_frame)
    if datasource.count() == 0:
        raise AnalysisException(None, None)

    print("datasource count : ", datasource.count())

    datasource = datasource.withColumn("source_id", input_file_name())
    datasource = datasource.withColumn('source_id', regexp_replace('source_id', input_s3_uri, ''))
    datasource = datasource.withColumn("source_id",expr("substring(source_id, 10, length(source_id)-4)"))
    datasource = datasource.withColumn('source_id', regexp_replace('source_id', '.csv', ''))
    datasource.createOrReplaceTempView('salesdata')

    #datasource0.coalesce(1).write.option("header",True).mode('append').csv('s3://s3-use1-ap-pe-df-orders-insights-storage-d/raw_layer/uspt/'+time_frame+'/')


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
        read_data(year, time_frame)
    except AnalysisException:
        print("Data source does not exist for the time frame: ", time_frame)
        return False

    staging_df_interim = spark.sql("""
    SELECT
        t.reporting_date, t.ori_trans_date, t.internal_invoice_number, t.internal_order_number, t.external_purchase_order, t.external_invoice_number, 
        t.external_transaction_number, t.other_order_ref, t.shipping_customer_id, t.billing_customer_id, t.p_product_id, t.p_backup_product_id, 
        t.product_type, t.price, t.price_currency, t.publisher_price_ori, t.publisher_price_ori_currency, t.payment_amount, t.payment_amount_currency, 
        t.current_discount_percentage, t.tax, t.pod, t.demand_units, t.units, t.trans_type_ori, t.trans_type, t.sale_type, t.country, t.state, 
        t.quote, t.source, t.source_id
    from
        (SELECT
            doc_date as reporting_date, ori_docdt as ori_trans_date, 'NA' as internal_Invoice_number, 'NA' as Internal_order_number, 'NA' as External_Purchase_Order, invno as external_invoice_number, 'NA' as External_Transaction_number, custpono as other_order_ref, ship_cust as shipping_customer_id, bill_cust as billing_customer_id, isbn_13 as p_product_id, isbn_10 as p_backup_product_id, 'PRINT' as product_type, sale_price as price, 'USD' as price_currency, pub_price as publisher_price_ori, 'USD' as publisher_price_ori_currency, net_value as payment_amount, currency as payment_amount_currency, discount as current_discount_percentage, tax_amount as tax, pod as pod, demandqty as demand_units, del_qty as units, tran_type as trans_type_ori, 
            case  
                when tran_type = 'S' or tran_type = 'R' then 'Sales'  
                when tran_type = 'Z' then 'Gratis'  
                when tran_type = 'T' then 'Transfer'  
                when tran_type = 'A' then 'Stock Movement'  
            end as trans_type, 
            case  
                when tran_type = 'S' then 'Purchase'  
                when tran_type = 'R' then 'Return'  
                else 'NA'  
            end as sale_type, 
            country as country, statprov as state, cat_no as quote, 'USPT' as source, source_id as source_id
        from
            salesdata
        )t""")
    
    staging_df_date = staging_df_interim.withColumn(
        'reporting_date', 
        to_date(unix_timestamp(col('reporting_date'), 'dd-MMM-yyyy').cast("timestamp"))
    )                    
    staging_df = staging_df_date.withColumn('reporting_date', date_format(col('reporting_date'), 'yyyy-MM-dd'))
    staging_df = staging_df.withColumn(
        'ori_trans_date', to_date(unix_timestamp(col('ori_trans_date'), 'dd-MMM-yyyy').cast("timestamp"))
        )  
    staging_df = staging_df.withColumn('ori_trans_date', date_format(col('ori_trans_date'), 'yyyy-MM-dd'))
    staging_df = staging_df.withColumn("year", lit(year))

    staging_df.show()
    staging_df.printSchema()

    staging_df.coalesce(1).write.option("header",True).partitionBy(
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
