import sys
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
required_args = {'s3_base_dir'}

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

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_base_dir = custom_args['s3_base_dir']
s3_input_dir = f's3://s3-use1-ap-pe-df-orders-insights-storage-d/mapped_layer/revenue/direct_sales/{s3_base_dir}/'
s3_output_dir = f's3://s3-use1-ap-pe-df-orders-insights-storage-d/staging_layer/revenue/direct_sales/{s3_base_dir}/'

input_df = spark.read.option("header", "true").parquet(s3_input_dir)
input_df.createOrReplaceTempView("input_tab")
input_df.coalesce(1).write.mode("overwrite").partitionBy(
    "year", "product_type","trans_type"
    ).parquet(s3_output_dir)

job.commit()
