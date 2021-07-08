import sys
import boto3

from awsglue.transforms import *
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
required_args = {'env', 'source_type', 's3_base_dir', 'master_dir'}

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

env = custom_args['env']
source_type = custom_args['source_type']
s3_base_dir = custom_args['s3_base_dir']
master_dir = custom_args['master_dir']

## Spark connection
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## CONSTANTS ##
input_bucket_name = 's3-use1-ap-pe-df-orders-insights-storage-d'
output_bucket_name ='s3-use1-ap-pe-df-orders-insights-storage-d'

## S3 Client Connection ##
s3_client = boto3.client("s3")


def merge_and_purge_parquet(input_path, output_path, time_frame):
    '''
    To merge live parquet with main parquet
    :param output_dir_path: s3 path to store parquet file
    :return: None
    '''
    s3_master_path = 's3://' + output_bucket_name + '/' + output_path
    master_year_partition = output_path + '/year={}'.format(time_frame)

    s3_input_year_partition = 's3://' + input_bucket_name + '/' + input_path + '/year={}'.format(time_frame)
    s3_master_year_partition = s3_master_path + '/year={}'.format(time_frame)
    
    print(s3_input_year_partition, s3_master_year_partition)
    # Read parquet files
    datasource_1 = spark.read.option("header", "true").parquet(s3_input_year_partition)
    try:
        datasource_2 = spark.read.option("header", "true").parquet(s3_master_year_partition)
    except AnalysisException:
        datasource_2 = spark.createDataFrame([], datasource_1.schema)

    datasource_1_count = datasource_1.count()
    datasource_2_count = datasource_2.count()

    if datasource_1_count == 0:
        print("Data does not exist for today")
        return False

    print('input count: ', datasource_1_count)
    print('source count: ', datasource_2_count)
    datasource_1.show(1)
    datasource_2.show(1)
    
    # Merge two parquets
    datasource_3 = datasource_2.union(datasource_1)
    datasource_3 = datasource_3.withColumn('year', lit(time_frame))

    if datasource_2_count:
        # Fetch original objects
        source_objs = s3_client.list_objects(
                Bucket=output_bucket_name,
                Prefix=master_year_partition,
            )
        print('source_objs', source_objs)

    datasource_3.coalesce(1).write.option("header", True).partitionBy(
        "year", "product_type", "trans_type"
        ).mode('append').parquet(s3_master_path)

    # Delete existing original files
    if datasource_2_count:
        for content in source_objs['Contents']:
            # Uncomment the line below if you wish the delete the original source file
            print('Deleted key:', content['Key'])
            s3_client.delete_object(Bucket=output_bucket_name, Key=content['Key'])

    print('Successfully combined source and input parquet files')


def initialise():
    '''
    Method to initialise the job
    '''
    input_paths = [
        f'mapped_layer/revenue/{source_type}/',
        f'staging_layer/revenue/{source_type}/'
        ]
    for input_path in input_paths:
        output_path = input_path + master_dir
        input_path += s3_base_dir
        
        response = s3_client.list_objects(
            Bucket=input_bucket_name,
            Prefix=input_path,
        )
        
        if 'Contents' not in response:
            print('Given input path is not valid', input_path)
            continue
        
        time_frames = {
            content['Key'].split('year=')[1][0:4]
            for content in response['Contents'] 
            if 'year=' in content['Key'] and '$folder$' not in content['Key']
            }
        print('time_frames: ', time_frames)

        for time_frame in time_frames:
            input_path + '/year=' + time_frame
            merge_and_purge_parquet(input_path, output_path, time_frame)
        
        # Delete input live files
        for content in response['Contents']:
            # Uncomment the line below if you wish the delete the original source file
            print('Deleted key:', content['Key'])
            s3_client.delete_object(Bucket=input_bucket_name, Key=content['Key'])
        

initialise()
job.commit()
