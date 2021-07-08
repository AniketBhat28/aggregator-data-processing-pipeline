import sys
import json
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import lit
from datetime import date , datetime


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
required_args = {'env', 'job_type', 'aggregator_name', 'prod_id', 's3_base_dir'}

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
prod_id = custom_args['prod_id']
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


def create_view_source_isbn(mda_table):
    spark.sql("use mda_data_lake")
    union_df = spark.sql(sql_json['isbn_sql'].format(mda_table=mda_table)).dropDuplicates()
    union_df.createOrReplaceTempView('union_tab')


def create_view_ref_isbn(connection_options):
    connection_options['query'] = f"""SELECT * from edw.ref_isbn"""
    dimref_isbn_gdf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    dimref_isbn_df = dimref_isbn_gdf.toDF()
    dimref_isbn_df.createOrReplaceTempView('dimref_isbn')


def fetch_audit_key(connection_options):
    connection_options['query'] = f"""SELECT audit_key from 
            (SELECT audit_key, start_time, RANK () over(order by start_time desc) as rnk FROM edw.dim_audit 
        where \"source\" = '{aggregator_name}') where rnk = 1"""

    datasource0 = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    max_df = datasource0.toDF()
    audit_key = max_df.collect()[0]['audit_key']
    print("Audit key value:", audit_key)
    return audit_key


def sync_ref_isbn_dim_product(mda_table, staging_table, audit_key, connection_options):
    '''
    case 1:
    '''
    dim_prod_queries = f"""select * from (
        select r.master_isbn as isbn, cast('true' as boolean) as is_inferred, cast('true' as boolean) as is_row_active, 
        CURRENT_DATE as Row_Start_Date, to_date('12-31-9999', 'MM-DD-YYYY') as Row_End_Date, cast({audit_key} as bigint) as audit_key 
        from edw.ref_isbn r 
        left join edw.dim_product p on r.master_isbn=p.isbn
        where p.isbn is null and r.master_isbn is not null) a"""
    
    connection_options['query'] = dim_prod_queries
    dimprod_isbn_gdf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
    dimprod_isbn_df = dimprod_isbn_gdf.toDF().dropDuplicates()
    dimprod_isbn_df.createOrReplaceTempView('dimprod_isbn_tab')
    dimprod_isbn_df.show(5)
    dimprod_isbn_df.printSchema()

    DDFrameForGlue_dimproduct_isbn = DynamicFrame.fromDF(dimprod_isbn_df, glueContext, "DDFrameForGlue_dimproduct_isbn")
    resolvechoice101 = ResolveChoice.apply(frame=DDFrameForGlue_dimproduct_isbn, choice="make_cols", transformation_ctx="resolvechoice101")

    pre_queries = f"""drop TABLE if exists {staging_table};
    CREATE TABLE {staging_table} as select * from mda_data_lake.{mda_table} where length({prod_id}) <= 13;
    alter table {staging_table} add column product_skey bigint;
    alter table {staging_table} add column buyer_party_skey bigint;
    alter table {staging_table} add column seller_party_skey bigint;
    alter table {staging_table} add column fulfilment_party_skey bigint""" 

    print("> insertion started for dim_product>")  
    datasink101 = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice101, catalog_connection=db_variables['db_connection'], 
        connection_options={"preactions":pre_queries, "dbtable":"edw.dim_product", "database": db_variables['database']}, 
        redshift_tmp_dir=args["TempDir"],transformation_ctx = "datasink101"
        )    
    print("> insertion completed for dim_product for ref_isbn master isbn which is not present in dim_product table>")


def fetch_non_match_isbn():
    '''
    '''
    leftjoin_df = spark.sql(f"""select a.*, b.* from union_tab a left join dimref_isbn b on a.{prod_id}=b.isbn""").dropDuplicates().cache()
    non_match_isbn = leftjoin_df.filter(f"isbn is null and {prod_id} is not null")
    print("printing non_match_isbn data")
    non_match_isbn.show()

    if non_match_isbn.count() == 0:
        return False

    print('Non matched isbn found')
    non_match_isbn.createOrReplaceTempView('non_match_isbn_tab')
    return True


def update_ref_isbn_dim_product(audit_key):
    '''
    '''
    if not fetch_non_match_isbn():
        print('Non match isbns are not found')
        return None
    
    # Updating source isbn into the ref_isbn table. 
    # Inserting source isbn into isbn column and source isbn prepend with x_ into master_isbn column
    non_match_master_isbn = spark.sql(
        f"""select {prod_id} as isbn, CONCAT('x_', {prod_id}) as master_isbn, 
            CASE WHEN product_type='Electronic' THEN 'e-Book' ELSE null END as render_format, 
            cast('true' as boolean) as is_inferred, cast({audit_key} as bigint) as audit_key 
        from non_match_isbn_tab"""
        ).dropDuplicates()    
    non_match_master_isbn.show(5)
    
    DDFrameForGlue_nonmatch_master_isbn = DynamicFrame.fromDF(
        non_match_master_isbn, glueContext, "DDFrameForGlue_nonmatch_master_isbn"
        )
    resolvechoice0 = ResolveChoice.apply(
        frame=DDFrameForGlue_nonmatch_master_isbn, choice="make_cols", transformation_ctx="resolvechoice0"
        )
    
    print("> insertion started for ref_isbn >")
    datasink0 = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice0, catalog_connection=db_variables['db_connection'], 
        connection_options={"dbtable": "edw.ref_isbn", "database": db_variables['database']}, 
        redshift_tmp_dir=args["TempDir"], transformation_ctx="datasink0"
        )
    print("> insertion completed for ref_isbn >")
    
    # Updating source isbn as isbn into the dim_product table. 
    non_match_product_isbn = spark.sql(
        f"""SELECT 
            CONCAT('x_', {prod_id}) as isbn, cast('true' as boolean) as is_inferred, cast('true' as boolean) as is_row_active, cast({audit_key} as bigint) as audit_key 
        from non_match_isbn_tab"""
        ).dropDuplicates()
    non_match_product_isbn = non_match_product_isbn.withColumn('Row_Start_Date', lit(datetime.now().date()))
    non_match_product_isbn = non_match_product_isbn.withColumn('Row_End_Date', lit(date(2099, 12, 31)))
    non_match_product_isbn = non_match_product_isbn.select("isbn", "is_inferred", "is_row_active", "Row_Start_Date", "Row_End_Date", "audit_key")
    non_match_product_isbn.show(5)
    
    DDFrameForGlue_non_match_product_isbn = DynamicFrame.fromDF(
        non_match_product_isbn, glueContext, "DDFrameForGlue_non_match_product_isbn"
        )
    resolvechoice1 = ResolveChoice.apply(
        frame=DDFrameForGlue_non_match_product_isbn, choice="make_cols", transformation_ctx="resolvechoice1"
        )
        
    print("> insertion started for dim_product>")  
    post_queries = f"""update edw.dim_product set product_bskey = product_skey where product_bskey is null"""
    datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice1, catalog_connection=db_variables['db_connection'], 
        connection_options={"dbtable":"edw.dim_product", "database":db_variables['database'], "postactions":post_queries}, 
        redshift_tmp_dir=args["TempDir"], transformation_ctx="datasink1"
        )    
    print("> insertion completed for dim_product>") 


def update_product_skey(staging_table):
    '''
    '''
    temp_df = spark.sql(f"""select * from dimref_isbn where 1=2""").dropDuplicates()
    print("printing temp_df data")
    temp_df.show()

    DDFrameForGlue_match_master_isbn = DynamicFrame.fromDF(
        temp_df, glueContext,"DDFrameForGlue_match_master_isbn"
        )
    resolvechoice2 = ResolveChoice.apply(
        frame=DDFrameForGlue_match_master_isbn, choice="make_cols", transformation_ctx="resolvechoice2"
        )

    print("> insertion started >")
    preactions_queries = f"""
    update {staging_table} set product_skey=p.product_skey from edw.dim_product p 
    inner join edw.ref_isbn r on p.isbn=r.master_isbn
    where {prod_id}=r.isbn;"""

    print("Post Query: ", preactions_queries)
    datasink2 = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=resolvechoice2, catalog_connection=db_variables['db_connection'], 
        connection_options={"preactions":preactions_queries, "dbtable":"edw.ref_isbn", "database":db_variables['database']}, 
        redshift_tmp_dir=args["TempDir"], transformation_ctx="datasink2"
        )    
    print(f"< updated to redshift {staging_table} is completed >")


def initialise():
    '''
    '''
    table_name = s3_base_dir.lower()
    staging_table = f"staging.{env}_staging_{table_name}" 
    mda_table =  f"dev_staging_{table_name}"
    
    print("staging_table", staging_table)

    connection_options = create_read_connection()
    create_view_source_isbn(mda_table)
    create_view_ref_isbn(connection_options)
    audit_key = fetch_audit_key(connection_options)
    sync_ref_isbn_dim_product(mda_table, staging_table, audit_key, connection_options)
    update_ref_isbn_dim_product(audit_key)
    update_product_skey(staging_table)
    

initialise()
job.commit()
