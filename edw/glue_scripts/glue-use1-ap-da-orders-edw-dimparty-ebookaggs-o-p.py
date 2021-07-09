import sys
import json
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
required_args = {'env', 'job_type', 'aggregator_name', 's3_base_dir'}

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
job_type = custom_args['job_type'].lower()
aggregator_name = custom_args['aggregator_name'].upper()
s3_base_dir = custom_args['s3_base_dir']

## DB Connection Params ##
if env in ('prod', 'uat'):
    db_variables = {'db_connection' : f"rd_cluster_{env}_conn" , 'database' : env}
    secret_name = f"sm/appe/use1/ubx/analytics/{env}"    
else:
    db_variables = {'db_connection' : "ind-redshift-dev-v1" , 'database' : "dev"}
    secret_name = "sm/appe/use1/ubx/analytics/dev"

## Spark connection
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def get_secrets():
    '''
    '''
    # Getting DB credentials from Secrets Manager
    client = boto3.client("secretsmanager", region_name="us-east-1")
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secrets = get_secret_value_response['SecretString']
    return json.loads(secrets)


def create_read_connection():
    '''
    '''
    secret = get_secrets()
    db_username = secret.get('username')
    db_password = secret.get('password')
    db_host = secret.get('host')
    db_url = "jdbc:redshift://" + db_host + ":5439/" + env

    # Connection config
    connection_options = {
                "url": db_url,
                "query":"",
                "user": db_username,
                "password": db_password,
                "redshiftTmpDir": args["TempDir"]
                }
    return connection_options


def get_dimparty_party_skey(connection_options):
    '''
    '''
    connection_options['query'] = f"""
        SELECT * from edw.dim_party 
            WHERE party_name='{aggregator_name}' and first_name='{aggregator_name}' and party_type='E-Aggregators'
    """  
    dimparty_isbn_gdf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    dimparty_isbn_df = dimparty_isbn_gdf.toDF()
    dimparty_isbn_df.createOrReplaceTempView('dimparty_tab')
    dimparty_isbn_df.show(5)
    return dimparty_isbn_df.collect()[0]['party_skey']


def update_seller_party_skey(staging_table, connection_options):
    '''
    '''
    dimparty_party_skey = get_dimparty_party_skey(connection_options)

    connection_options['query'] = f"""SELECT * from {staging_table} limit 1"""  
    stg_pbk_edw_gdf = glueContext.create_dynamic_frame_from_options("redshift",connection_options)
    stg_pbk_edw_df = stg_pbk_edw_gdf.toDF()
    stg_pbk_edw_df.createOrReplaceTempView('stg_pbk_tab')

    temp_df = spark.sql(f"""select * from stg_pbk_tab where 1=2""")
    DDFrameForGlue_final_df1 = DynamicFrame.fromDF(temp_df, glueContext,"DDFrameForGlue_final_df1")
    resolvechoice0 = ResolveChoice.apply(frame=DDFrameForGlue_final_df1, choice="make_cols", transformation_ctx="resolvechoice0")

    preactions_queries = f"""update {staging_table} set seller_party_skey={dimparty_party_skey}"""

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice0, catalog_connection=db_variables['db_connection'], 
        connection_options={"preactions":preactions_queries, "dbtable":staging_table, "database":db_variables['database']}, 
        redshift_tmp_dir=args["TempDir"], transformation_ctx="datasink0")

    print(f"< inserting to redshift {staging_table} table is successful >")


def initialise():
    '''
    '''
    table_name =  s3_base_dir.lower()
    staging_table = f"staging.{env}_staging_{table_name}" 
    print("staging_table", staging_table)

    connection_options = create_read_connection()
    update_seller_party_skey(staging_table, connection_options)

initialise()  
job.commit()
