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
required_args = {'env', 'aggregator_name', 's3_base_dir'}

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

## CONSTANTS ##
etljobName = args['JOB_NAME']

# reading the json file to extract all the queries
with open('glue_edw_sql_queries.json') as f:
    rule_config = json.load(f)

sql_json = next((item for item in rule_config if(item['name'] == aggregator_name)), None)
print('sql_json++++++++++++++++++++++++++++++++')
print(sql_json)


def get_secrets():
    '''
        Method to fetch db credentials from aws secret manager
    '''
    # Getting DB credentials from Secrets Manager
    client = boto3.client("secretsmanager", region_name="us-east-1")
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secrets = get_secret_value_response['SecretString']
    return json.loads(secrets)


def create_read_connection():
    '''
        Connect to redshift db by using secrets
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


def get_dim_audit_key(connection_options):
    '''
        Return audit_key based on the aggregator_name
        :param connection_options: DB connection configs
    '''
    connection_options['query'] = f"""
        SELECT audit_key from (
            SELECT audit_key, start_time, RANK () over(order by start_time desc) as rnk FROM edw.dim_audit where \"source\" = '{aggregator_name}'
            ) where rnk = 1"""
    datasource0 = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    max_df = datasource0.toDF()
    
    if max_df.count() == 0:
        return None
    return max_df.collect()[0]['audit_key']


def get_or_create_dim_audit(connection_options):
    '''
        Method to create audit record if aggregator doesnot exist
        :param connection_options: DB connection configs
        :return: audit key
    '''
    audit_key = get_dim_audit_key(connection_options)

    if not audit_key:
        DimAudit_df = spark.sql(sql_json['DimAudit_sql'].format(aggregator_name, etljobName))
        DimAudit_df.show(truncate = False)
        
        Dyanamic_DataFrameForGlue_DimAudit = DynamicFrame.fromDF(DimAudit_df, glueContext,"Dyanamic_DataFrameForGlue_DimAudit")
        resolvechoice2 = ResolveChoice.apply(frame=Dyanamic_DataFrameForGlue_DimAudit, choice="make_cols", transformation_ctx="resolvechoice2")
        
        print("> insertion started >")
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=resolvechoice2, catalog_connection=db_variables['db_connection'], 
            connection_options={"dbtable": "edw.dim_audit", "database": db_variables['database']}, 
            redshift_tmp_dir=args["TempDir"], transformation_ctx="datasink5"
            )
        print("> inserting to redshift dim_audit table is successful >")
        
        # Fetch audit key from the table
        audit_key = get_dim_audit_key(connection_options)

    print("Audit key: ", audit_key)
    return audit_key


def verify_dim_order(audit_key, connection_options):
    '''
    Method to verify audit record is exist in dim order table
    '''
    connection_options['query'] = f"""SELECT distinct audit_key FROM edw.dim_order where audit_key='{audit_key}'"""
    datasource0 = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    datasource_df = datasource0.toDF()
    if datasource_df.count() == 0:
        return False
    return True
    

def create_view_orders(mda_table):
    '''
    Method to create orders table view
    '''
    spark.sql("use mda_data_lake")
    spark_df = spark.sql(f"select * from mda_data_lake.{mda_table}")
    spark_df.show(5)
    spark_df.createOrReplaceTempView('orders')


def update_dim_order(audit_key, fresh_setup=False):
    '''
    Method to update dim order table
    '''
    connection_options = {"dbtable": "edw.dim_order", "database": db_variables['database']}
    if fresh_setup:
        connection_options["preactions"] = "DELETE from edw.dim_order WHERE audit_key={}".format(audit_key)

    dimOrder_df = spark.sql(sql_json['DimOrder_sql'].format(audit_key))
    dimOrder_df.show(truncate=False)

    Dyanamic_DataFrameForGlue_DimOrder = DynamicFrame.fromDF(dimOrder_df, glueContext, "Dyanamic_DataFrameForGlue_DimOrder")
    resolvechoice3 = ResolveChoice.apply(frame=Dyanamic_DataFrameForGlue_DimOrder, choice="make_cols", transformation_ctx="resolvechoice3")

    print("> insertion started >")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice3, catalog_connection=db_variables['db_connection'], connection_options=connection_options, 
        redshift_tmp_dir=args["TempDir"], transformation_ctx="datasink6"
        )
    print("> inserting to redshift dim_order table is successful >")


def initialise():
    '''
    Method to initialise the job
    '''
    table_name = s3_base_dir.lower()
    mda_table =  f"dev_staging_{table_name}"

    print('env: ', env)
    print('mda_table: ', mda_table)

    connection_options = create_read_connection()
    print("Established redhsift connection")
    
    audit_key = get_or_create_dim_audit(connection_options)
    print("Found audit key: ", audit_key)
    
    create_view_orders(mda_table)
    print("Created orders table")
    
    update_dim_order(audit_key)
    print("Updated dim order")

initialise()
job.commit()
