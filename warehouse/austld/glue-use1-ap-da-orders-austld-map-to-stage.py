import sys
import boto3
from datetime import date, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, lit, unix_timestamp, to_date

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

sales_options = "'" + "','".join(['BC', 'BCSP', 'BCSO', 'BCSI', 'BCCM', 'BCSD', 'BCSC', 'BCSS', 'BCRM', 'BCRC', 'BCCN', 'BCHL', 'BCEX', 'BCTL', 'BCEV', 'BCFR', 'BCCS', 'BCSY', 'BCBK', 'BCBC', 'BCTR', 'BNSO', 'BNSI', 'BNSD', 'BNSC', 'BNRM', 'BNRC', 'BNHL', 'BNEX', 'BNEV', 'BNFS', 'BNFR', 'BNFA', 'BNCM', 'BNCN', 'BNTR', 'BNSY', 'BNSP', 'BNSS', 'BSFR', 'BSFS', 'BSIC', 'BSRV', 'BS', 'BSBC', 'BSBK', 'BSCS', 'BSEV', 'BSEX', 'BSHL', 'BSRC', 'BSRM', 'BSSC', 'BSSD', 'BSSI', 'BSSO', 'BSSP', 'AACN', 'AATR', 'AA', 'AABC', 'AABK', 'AACS', 'AAEV', 'AAEX', 'AAHL', 'AARC', 'AARM', 'AARV', 'AASC', 'AASD', 'AASI', 'AASO', 'AASP', 'AASS', 'AASY', 'ABCN', 'AB', 'ABBC', 'ABBK', 'ABCS', 'ABEV', 'ABEX', 'ABHL', 'ABRC', 'ABRM', 'ABSC', 'ABSD', 'ABSI', 'ABSO', 'ABSP', 'ABSS', 'ABSY', 'ABTR', 'AEBK', 'AESP', 'AEBC', 'AECS', 'AE', 'AERM', 'AESS', 'AERC', 'AETR', 'AEHL', 'AESO', 'AEEX', 'AECN', 'AESY', 'AESI', 'AEEV', 'AESD', 'AESC', 'AHBC', 'AH', 'AHCN', 'AHSI', 'AHSD', 'AHSC', 'AHRM', 'AHRC', 'AHHL', 'AHEX', 'AHEV', 'AHCS', 'AHTR', 'AHSY', 'AHSS', 'AHSP', 'AHBK', 'AHSO', 'AISO', 'AISP', 'AISS', 'AISY', 'AITR', 'AIBK', 'AICS', 'AIEV', 'AIEX', 'AIHL', 'AIRC', 'AIRM', 'AISC', 'AISD', 'AISI', 'AICN', 'AI', 'AIBC', 'ALSI', 'ALBC', 'ALSP', 'ALSS', 'ALSY', 'ALTR', 'ALBK', 'ALCS', 'ALEV', 'ALEX', 'ALHL', 'ALRC', 'ALRM', 'ALSC', 'ALSD', 'ALCN', 'AL', 'ALSO', 'ANSI', 'ANBC', 'ANSP', 'ANSS', 'ANSY', 'ANTR', 'ANCN', 'ANBK', 'ANCS', 'ANEV', 'ANEX', 'ANHL', 'ANRC', 'ANRM', 'ANSC', 'ANSD', 'AN', 'ANSO', 'AOSI', 'AOBC', 'AOSP', 'AOSS', 'AOSY', 'AOTR', 'AOCN', 'AOBK', 'AOCS', 'AOEV', 'AOEX', 'AOHL', 'AORC', 'AORM', 'AOSC', 'AOSD', 'AO', 'AOSO', 'AP', 'ASSI', 'ASBC', 'ASSP', 'ASSS', 'ASSY', 'ASTR', 'ASBK', 'ASCS', 'ASEV', 'ASEX', 'ASHL', 'ASRC', 'ASRM', 'ASSC', 'ASSD', 'ASCN', 'AS', 'ASSO']) + "'"

gratis_options = "'" + "','".join(['AAFA', 'AAFR', 'AAFS', 'AAIC', 'AACM', 'ABCM', 'ABFA', 'ABFR', 'ABFS', 'ABIC', 'ABRV', 'AERV', 'AEIC', 'AEFS', 'AEFR', 'AEFA', 'AECM', 'AHRV', 'AHIC', 'AHFS', 'AHFR', 'AHFA', 'AHCM', 'AICM', 'AIFA', 'AIFR', 'AIFS', 'AIIC', 'AIRV', 'ALCM', 'ALFA', 'ALFR', 'ALFS', 'ALIC', 'ALRV', 'ANCM', 'ANFA', 'ANFR', 'ANFS', 'ANIC', 'ANRV', 'AOCM', 'AOFA', 'AOFR', 'AOFS', 'AOIC', 'AORV', 'APCM', 'ASCM', 'ASFA', 'ASFR', 'ASFS', 'ASIC', 'ASRV', 'BCFA', 'BCCM', 'BCRV', 'BCIC', 'BCFS', 'BCFR', 'BNRV', 'BNIC', 'BNFS', 'BNFR', 'BNFA', 'BNCM', 'BSCM', 'BSFA', 'BSFR', 'BSFS', 'BSIC', 'BSRV']) + "'"

## Connect to s3 bucket ##
s3 = boto3.resource("s3")
s3_bucket = s3.Bucket(input_bucket_name)
s3_op_bucket = s3.Bucket(output_bucket_name)


def save_parquet(year, input_dir_path, output_dir_path):
    '''
    To process source data and store it in parquet files
    :param year: reported source year
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
                
    stg_query = f"""
    SELECT
    t.reporting_date, t.internal_invoice_number, t.internal_order_number, t.external_invoice_number, t.external_transaction_number, t.external_purchase_order,  t.billing_customer_id, t.p_product_id, t.p_backup_product_id, t.product_type, t.price, t.price_currency,  t.publisher_price_ori, t.publisher_price_ori_currency, t.payment_amount, t.payment_amount_currency, t.ex_currencies, t.ex_rate, t.current_discount_percentage, t.tax_percentage, t.demand_units, t.units, t.trans_type_ori, t.sale_type_ori, t.doc_type_attribute, t.trans_type, t.sale_type, t.pod, t.source, t.source_id
    FROM
        (SELECT
            reporting_date, internal_Invoice_number, internal_order_number, external_invoice_number, external_transaction_number,  external_purchase_order,  billing_customer_id, trim(p_product_id) as p_product_id, trim(p_backup_product_id) as p_backup_product_id, 'Print' as product_type,  
            round(cast(price as double), 2) as price, price_currency, round(cast(publisher_price_ori as double), 2) as publisher_price_ori, publisher_price_ori_currency, round(cast(payment_amount as double), 2) as payment_amount,  
            case 
                when payment_amount_currency = 'A' then 'AUD' 
                when payment_amount_currency = 'N' then 'NZD' 
            end as payment_amount_currency,  
            ex_currencies, round(cast(ex_rate as double), 2) as ex_rate, 
            round(cast(current_discount_percentage as double), 2) as current_discount_percentage, round(cast(tax_percentage as double), 2) as tax_percentage,  
            cast(cast(demand_units as double) as int) as demand_units, cast(cast(demand_units as double) as int) as units,  trans_type_ori, sale_type_ori, doc_type_attribute,  
            case  
                when concat(trim(trans_type_ori), trim(sale_type_ori), trim(doc_type_attribute)) in ({sales_options}) then 'Sales' 
                when concat(trim(trans_type_ori), trim(sale_type_ori), trim(doc_type_attribute)) in ({gratis_options}) then 'Gratis' 
                else 'Sales'  
            end as trans_type, sale_type, pod, source, source_id
        FROM
            salesdata 
        ) t"""
    
    staging_df_interim = spark.sql(stg_query)
    staging_df_date = staging_df_interim.withColumn(
        'reporting_date', to_date(unix_timestamp(col('reporting_date'), 'yyyy-MM-dd').cast("timestamp"))
        )
    staging_df = staging_df_date.withColumn('year', lit(year))
    
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
