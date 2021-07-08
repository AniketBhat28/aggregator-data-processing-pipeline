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

def fetch_party_skey(connection_options):
    '''
    '''
    connection_options['query'] = f"""SELECT * from edw.dim_party"""
    dimparty_isbn_gdf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    dimparty_isbn_df = dimparty_isbn_gdf.toDF()
    dimparty_isbn_df.createOrReplaceTempView('dimparty_tab')

    print('dimparty_isbn_df data: ', dimparty_isbn_df)
    dimparty_isbn_df.show(5)

    spskey_id = dimparty_isbn_df.filter("party_name = 'Routeledge.com' and party_type = 'Owned Sites'").collect()[0]['party_skey']
    fpskey_id_e = dimparty_isbn_df.filter("party_name = 'VITALSOURCE' and party_type='E-Aggregators'").collect()[0]['party_skey']
    fpskey_id_u = dimparty_isbn_df.filter("party_name = 'USPT' and party_type='Warehouses'").collect()[0]['party_skey']
    fpskey_id_uk = dimparty_isbn_df.filter("party_name = 'UKBP' and party_type='Warehouses'").collect()[0]['party_skey']
    fpskey_id_a = dimparty_isbn_df.filter("party_name = 'AUS_TLD' and party_type='Warehouses'").collect()[0]['party_skey']
    fpskey_id_s = dimparty_isbn_df.filter("party_name = 'SGBM' and party_type='Warehouses'").collect()[0]['party_skey']

    return (spskey_id, fpskey_id_e, fpskey_id_u, fpskey_id_uk, fpskey_id_a, fpskey_id_s)

def update_party_skey(staging_table, connection_options):
    '''
    '''
    spskey_id, fpskey_id_e, fpskey_id_u, fpskey_id_uk, fpskey_id_a, fpskey_id_s = fetch_party_skey(connection_options)

    connection_options['query'] = f"""SELECT * from {staging_table} limit 1"""
    stg_ubw_edw_gdf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    stg_ubw_edw_df = stg_ubw_edw_gdf.toDF()
    stg_ubw_edw_df.createOrReplaceTempView('stg_ubw_edw_tab')

    dummy_df = spark.sql(f"""select * from stg_ubw_edw_tab a where 1=2""")
    DDFrameForGlue_df1 = DynamicFrame.fromDF(dummy_df, glueContext, "DDFrameForGlue_df1")
    resolvechoice0 = ResolveChoice.apply(frame=DDFrameForGlue_df1, choice="make_cols", transformation_ctx="resolvechoice0")

    print(f"< insertion started into {staging_table} >")
    preactions_queries = f"""
        update {staging_table} set seller_party_skey = {spskey_id};
        update {staging_table} set fulfilment_party_skey = case 
                                                when product_type = 'Electronic' then {fpskey_id_e}
                                                when product_type = 'Print' and payment_amount_currency = 'USD' then {fpskey_id_u}
                                                when product_type = 'Print' and payment_amount_currency = 'GBP' then {fpskey_id_uk}
                                                when product_type = 'Print' and payment_amount_currency = 'SGD' then {fpskey_id_s}
                                                when product_type = 'Print' and payment_amount_currency in ('AUD','NZD') then {fpskey_id_a}
                                                else {fpskey_id_uk} 
                                            end """
    print("Post Query: ", preactions_queries)

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame = resolvechoice0, catalog_connection=db_variables['db_connection'], 
        connection_options={"preactions":preactions_queries, "dbtable":staging_table, "database":db_variables['database']},
        redshift_tmp_dir=args["TempDir"], transformation_ctx="datasink0"
        )
    print(f"< inserted into {staging_table} >")

def initialise():
    '''
    '''
    table_name =  s3_base_dir.lower()
    staging_table = f"staging.{env}_staging_{table_name}"

    connection_options = create_read_connection()    
    update_party_skey(staging_table, connection_options)


initialise()
job.commit()
