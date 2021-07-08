import sys
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import to_date, year
from pyspark.sql.functions import col

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
output_loc = f's3://s3-use1-ap-pe-df-orders-insights-storage-d/mapped_layer/revenue/direct_sales/{s3_base_dir}/'

order_gdf = glueContext.create_dynamic_frame.from_catalog(
    database="ordermgmtprod", table_name="ordermgmtprod_public_order", transformation_ctx="order_gdf"
    )
order_item_gdf = glueContext.create_dynamic_frame.from_catalog(
    database="ordermgmtprod", table_name="ordermgmtprod_public_order_item", transformation_ctx="order_item_gdf"
    )
invoice_gdf = glueContext.create_dynamic_frame.from_catalog(
    database="ordermgmtprod", table_name="ordermgmtprod_public_invoice", transformation_ctx="invoice_gdf"
    )
address_gdf = glueContext.create_dynamic_frame.from_catalog(
    database="ordermgmtprod", table_name="ordermgmtprod_public_address", transformation_ctx="address_gdf"
    )
print("TotalCount: First Time from postgres:")

order_item_df = order_item_gdf.toDF()
invoice_df = invoice_gdf.toDF()
address_df = address_gdf.toDF()
order_df = order_gdf.toDF()

print('printing order_df')
print(order_gdf.count())
order_gdf.printSchema()
order_df.show(5)

print('printing order_item_df')
print(order_item_gdf.count())
order_item_gdf.printSchema()
order_item_df.show(5)

print('printing invoice_df')
print(invoice_gdf.count())
invoice_gdf.printSchema()
invoice_df.show(5)

print('printing address_df')
print(address_gdf.count())
address_gdf.printSchema()
address_df.show(5)

order_df.createOrReplaceTempView("order")
invoice_df.createOrReplaceTempView("invoice")
order_item_df.createOrReplaceTempView("order_item")
address_df.createOrReplaceTempView("address")

mapped_data = spark.sql('''
SELECT
    seller_name as aggregator_name, order_date as reporting_date, external_invoice_ref as internal_Invoice_number, order_number as Internal_order_number, purchase_order_number as external_purchase_order, customer_id as shipping_customer_id, buyer_id as billing_customer_id, 
    case 
        when channel in ('ASC', 'TF-SUBMISSION-PLATFORM') then product_id 
    end as DOI, 
    case 
        when channel in ('GOBI', 'OASIS', 'RIALTO', 'EBK_AGENT_SALES', 'UBX', 'EBK_DIRECT_SALES') then product_id  
        when channel in ('SUB_RIGHTS', 'PERMISSIONS') then product_id 
    end as e_product_id, 
    'Electronic' as product_type, price, currency as price_currency, 
    case 
        when channel in ('GOBI', 'OASIS', 'RIALTO', 'EBK_AGENT_SALES', 'UBX', 'EBK_DIRECT_SALES') then 'BYO Library Price' 
    end as price_type, 
    selling_price as payment_amount, currency as payment_amount_currency, discount as current_discount_percentage, total_tax as tax, discount_code as disc_code, ordered_quantity as units, order_type as sale_type_ori, 'Sales' as trans_type, 
    case 
        when order_type in ('SALES', 'REVISED_SALES') then 'Purchase' 
        when order_type in ('RETURN', 'REVISED_SALES_RETURN' ) then 'Return' 
    end as sale_type, 
    country_code as country, address_region as state, address_locality as city, postal_code as postcode, 'OMS' as source, channel as sub_domain, quote_number as quote, business_partner_no as Internal_bp_number, order_number as source_id, 'NA' as external_invoice_number, 'NA' as external_transaction_number 
from
    (SELECT
        a.*, i.external_invoice_ref, a2.business_partner_no, a2.postal_code, a2.address_locality, a2.address_region, a2.country_code, oit.product_id, oit.price, oit.selling_price, oit.currency, oit.discount, oit.ordered_quantity 
    from
        (SELECT
            b.seller_name, b.order_date, cast(b.id as decimal(15, 0)) as id, b.order_number, b.purchase_order_number, b.customer_id, b.buyer_id, cast(cast(b.order_price as decimal(15, 3)) as double) as order_price, cast(cast(b.total_tax as decimal(15, 0)) as double) as total_tax, b.discount_code, b.order_type, b.channel, b.order_status, b.quote_number, b.billing_address_id 
        from
            order b 
        where
            b.channel in ('GOBI', 'OASIS', 'RIALTO', 'EBK_AGENT_SALES', 'UBX', 'EBK_DIRECT_SALES', 'SUB_RIGHTS', 'PERMISSIONS') 
        )a 
        left join invoice i on a.id = cast(i.order_id as decimal(15, 0)) 
        left join address a2 on cast(a.billing_address_id as decimal(15, 0))=a2.id 
        inner join
            (select
                cast(o.order_id as decimal(15, 0)) as order_id, o.product_id, cast(cast(o.price as decimal(15, 3)) as double) as price, cast(cast(o.selling_price as decimal(15, 3)) as double) as selling_price, o.currency, cast(cast(o.discount as decimal(15, 3)) as double) as discount, cast(o.ordered_quantity as long) as ordered_quantity 
            from
                order_item o
            ) oit 
        on a.id = oit.order_id
    )fd
''')
    
mapped_data = mapped_data.withColumn("reporting_date", to_date(col("reporting_date"), "yyyy-MM-dd"))
mapped_data = mapped_data.withColumn(
    "year", year(col("reporting_date"))
    ).filter("year > 2018").withColumn("year",col("year").cast("string"))
mapped_data.show()

mapped_data.coalesce(1).write.mode("overwrite").partitionBy(
    "year", "product_type", "trans_type"
    ).parquet(output_loc)

job.commit()
