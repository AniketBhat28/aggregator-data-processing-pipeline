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


def fetch_audit_key(connection_options):
    '''
    '''
    connection_options['query'] = f"""SELECT audit_key from 
            (SELECT audit_key, start_time, RANK () over(order by start_time desc) as rnk FROM edw.dim_audit 
        where \"source\" = '{aggregator_name}') where rnk = 1"""

    datasource0 = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    max_df = datasource0.toDF()
    audit_key = max_df.collect()[0]['audit_key']
    print("Audit key value:", audit_key)
    return audit_key


def create_view_tandf_customer(mda_table):
    '''
    '''    
    db_sql = f"""SELECT billing_customer_id from mda_data_lake.{mda_table} where billing_customer_id != '102059'
                UNION
                SELECT shipping_customer_id from mda_data_lake.{mda_table} where billing_customer_id != shipping_customer_id
         """
    
    spark.sql("use mda_data_lake")
    print("db_sql: ", db_sql)
    tandf_bci_eq_sci_df = spark.sql(db_sql)
    tandf_bci_eq_sci_df.show()
    tandf_bci_eq_sci_df.createOrReplaceTempView('tandf_eq_tab')


def create_view_dimparty(connection_options):
    '''
    '''
    connection_options['query'] = """SELECT * from edw.dim_party"""  
    dimparty_isbn_gdf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    dimparty_isbn_df = dimparty_isbn_gdf.toDF()
    dimparty_isbn_df.createOrReplaceTempView('dimparty_tab')

    fpskey_id = dimparty_isbn_df.filter("party_name = 'UBX' and party_type = 'Owned Sites'").collect()[0]['party_skey']
    return fpskey_id


def fetch_non_match_customer_id():
    '''
    '''
    leftjoin_df = spark.sql("""
        SELECT a.*, b.* from tandf_eq_tab a left join (
            SELECT * from dimparty_tab where party_type='Customer' and external_id != ''
        ) b on a.billing_customer_id = b.external_id"""
        ).cache()

    non_match_cid = leftjoin_df.filter("external_id is null")
    print("printing non_match_cid data")
    non_match_cid.show(5)

    if non_match_cid.count() == 0:
        return False

    non_match_cid.createOrReplaceTempView('non_match_cid_tab')
    return True


def update_dim_party(connection_options):
    '''
    '''
    if not fetch_non_match_customer_id():
        print('Non match customer_id are not found')
        return None 
        
    audit_key = fetch_audit_key(connection_options)
    non_match_cid_df = spark.sql(f"""
        select billing_customer_id as external_id, 'Customer' as party_type, cast({audit_key} as bigint) as audit_key 
        from non_match_cid_tab"""
        ).dropDuplicates()
    print('non_match_cid_df: ', non_match_cid_df)
    non_match_cid_df.show(5)

    DDFrameForGlue_non_match_cid_df = DynamicFrame.fromDF(non_match_cid_df, glueContext,"DDFrameForGlue_non_match_cid_df")
    resolvechoice1 = ResolveChoice.apply(
        frame=DDFrameForGlue_non_match_cid_df, choice="make_cols", transformation_ctx="resolvechoice1"
        )

    print("> insertion started for dim_party >")
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice1, catalog_connection=db_variables['db_connection'], 
        connection_options={"dbtable": "edw.dim_party", "database": db_variables['database']},
        redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink1"
        )
    print("> insertion completed for dim_party >")


def update_party_skey(staging_table, fpskey_id):
    '''
    '''
    dummy_df = spark.sql(f"""select * from {staging_table} where 1=2 limit 1""")
    DDFrameForGlue_df1 = DynamicFrame.fromDF(dummy_df, glueContext, "DDFrameForGlue_df1")
    resolvechoice0 = ResolveChoice.apply(frame=DDFrameForGlue_df1, choice="make_cols", transformation_ctx="resolvechoice0")

    preactions_queries = f"""
        update {staging_table}  set buyer_party_skey = s.party_skey from edw.dim_party s 
            where billing_customer_id = s.external_id;
        update {staging_table} set seller_party_skey = s.party_skey from (
            select * from edw.dim_party where party_name in ('GOBI','OASIS','RIALTO','UBX','EBK_AGENT_SALES','EBK_DIRECT_SALES')
            ) s where sub_domain = s.party_name;
        update {staging_table} set fulfilment_party_skey = {fpskey_id};"""
        
    print("Post Query: ", preactions_queries)
    print(f"< insertion started for {staging_table}>")
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice0, catalog_connection=db_variables['db_connection'], 
        connection_options={"preactions":preactions_queries, "dbtable":staging_table, "database":db_variables['database']},
        redshift_tmp_dir=args["TempDir"], transformation_ctx = "datasink11"
        )
    print(f"< insertion completed for {staging_table} >")
    

def initialise():
    '''
    '''
    table_name =  s3_base_dir.lower()
    mda_table = f"dev_staging_{table_name}"
    staging_table = f"staging.{env}_staging_{table_name}"

    connection_options = create_read_connection()
    create_view_tandf_customer(mda_table)
    fpskey_id = create_view_dimparty(connection_options)
    update_dim_party(connection_options)
    update_party_skey(staging_table, fpskey_id)


initialise()
job.commit()
