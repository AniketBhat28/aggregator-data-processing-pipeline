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

# reading the json file to extract all the queries
with open('glue_edw_sql_queries.json') as f:
    rule_config = json.load(f)

sql_json = next((item for item in rule_config if(item['name'] == aggregator_name)), None)
print('sql_json++++++++++++++++++++++++++++++++')
print(sql_json)


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


def create_view_dimlocation(connection_options):
    connection_options['query'] = f"""SELECT * from edw.dim_location"""  
    dimlocation_gdf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    dimlocation_df = dimlocation_gdf.toDF()
    dimlocation_df.createOrReplaceTempView('dimlocation')

    print('Printing dimlocation')
    dimlocation_df.show(5)


def create_view_agg_tab(staging_table, connection_options):
    connection_options['query'] = f"""SELECT * from {staging_table}"""  
    agg_gdf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    agg_df = agg_gdf.toDF()
    agg_df.createOrReplaceTempView('agg_tab')

    print(f"Printing {staging_table} as agg_tab")
    agg_df.show(5)


def create_view_main_table():
    join_df = spark.sql(sql_json['parent_sql'])
    join_df.createOrReplaceTempView('main_tab')

    print('Printing parent_sql as main_tab')
    join_df.show(truncate = False)


def create_view_dimorder(aggregator_name, connection_options):
    connection_options['query'] = f"""
            SELECT ord.* from edw.dim_order as ord
                join (
                    SELECT audit_key from (SELECT audit_key, start_time, RANK () over(PARTITION by \"source\" order by start_time desc) as rnk 
                    FROM edw.dim_audit 
                    where source='{aggregator_name}'
                ) 
                where rnk = 1) as aud
                on ord.audit_key=aud.audit_key
            """
    datasource11 = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    order_df = datasource11.toDF()
    print('Printing dimorder_temp')
    order_df.show()
    order_df.createOrReplaceTempView('dimorder_temp')


def insert_fact_order_line(staging_fol_table):
    '''
    '''
    FactOrderLine_df = spark.sql(sql_json['FactOrderLine_sql'])
    print("FactOrderLine_df", FactOrderLine_df)
    FactOrderLine_df.show(truncate = False)

    Dyanamic_DataFrameForGlue_FactOrderLine = DynamicFrame.fromDF(
        FactOrderLine_df, glueContext,"Dyanamic_DataFrameForGlue_FactOrderLine"
        )
    resolvechoice4 = ResolveChoice.apply(
        frame=Dyanamic_DataFrameForGlue_FactOrderLine, choice="make_cols", transformation_ctx="resolvechoice4"
        )

    print("< insertion started >")
    pre_queries = f"TRUNCATE TABLE {staging_fol_table}"
    post_queries = sql_json['fact_orderline_post_operation'].format(staging_fol_table=staging_fol_table)
    #post_queries += "; REFRESH MATERIALIZED VIEW edw.vw_monthly_Fact_Order_Line;"

    print("pre_queries: ", pre_queries)
    print("post_queries: ", post_queries)

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice4, catalog_connection=db_variables['db_connection'], 
        connection_options={"preactions":pre_queries, "dbtable":staging_fol_table, "database":db_variables['database'], "postactions":post_queries}, 
        redshift_tmp_dir=args["TempDir"], transformation_ctx="datasink7"
        )
    print("< inserting to redshift Fact_Order_Line table is successful >")


def initialise():
    '''
    '''
    table_name =  s3_base_dir.lower()
    staging_table = f"staging.{env}_staging_{table_name}"
    staging_fol_table = f"staging.fact_order_line_{table_name}"

    connection_options = create_read_connection()
    create_view_dimlocation(connection_options)
    create_view_agg_tab(staging_table, connection_options)
    create_view_main_table()
    create_view_dimorder(aggregator_name, connection_options)
    insert_fact_order_line(staging_fol_table)

initialise()
job.commit()
