# aggregator-data-processing-pipeline
### This application processes different aggregators files and generates staging data for reporting.
# About the application
This application runs for all the available aggregators as per the corresponding 
rules in AggRulesVal.json.The corresponding aggregator files will be read from s3 as the input source and the generated staging out will be written 
 to the s3 location as output.
 The input and output locations along with the file name are controlled by the project Config.ini file. 

# Application Objectives
Using this application we are generating the common staging data as described in https://taylorfrancis.atlassian.net/wiki/spaces/DF/pages/1212579853/Staging+data+entity+-+Post+processing+eBook+sales+files 
for all the available aggregators which helps us on report generation.
using boto3 aws library we are reading the aggregator data from s3 and processing it with the help Pandas
library .
The application is fully config driven and controlled by AggRulesVal.json.Which gives us the privileges to add,modify or delete 
any rules or columns without changing anything in the code.
The input file location and output location are handled by Config.ini which makes the application a fully dynamic one .

# Prerequisites
python3

AWS cli should be configured

# Installing and Running
Install the required dependencies
        
        pip install


Replace your own IAM credentials here : 

        aws_credentials['ACCESS_KEY'] = '' #add aws Access key for IAM user
        aws_credentials['SECRET_KEY'] = '' #Add aws secret key for IAM user

## How to setup
#### step-1:Clone.
        git clone https://github.com/tandfgroup/aggregator-data-processing-pipeline.git <your local path>
for example :

        git clone https://github.com/tandfgroup/aggregator-data-processing-pipeline.git C:/\Users/\Desktop/\codebase/\git_code/\ingram_vital_source

#### step-2:config setup
Change your config.ini file like below as per the aggregator you want to process.YOu can control your input and output location for the application here

        [DEFAULT]
        project = Order Insights Data Pipeline

        [INPUT]
        File_Data = [	
				{	
					'input_base_path':'s3://s3-euw1-ap-pe-orders-worker-agg-storage-etl/prd/INGRAM/input/2020/SEP/',
					'input_bucket_name':'s3-euw1-ap-pe-orders-worker-agg-storage-etl',
					'input_directory':'prd/INGRAM/input/2020/SEP',
					'input_file_name':None,
					'input_sheet_name':None
				}
			]
        File_Aggregator = INGRAM

        [OUTPUT]
        output_bucket_name = s3-use1-ap-pe-df-orders-insights-storage-d
        output_directory = staging/revenue/aggregator/INGRAM/eBook-032020.csv

#### step-3:Rules

Change the AggRulesVal.json as per the business requirements.
It supports,

1.Initial checks.

                "name":  :Aggregator name generation.
		"filename_pattern": "" :File pattern matching for processing(Can be blank if not required)
		"product_type": "EBK"  :Describe the type of the product
		"file_type":"csv"  :Different format of the file
		"header_row": 0,   :To handle corrupt rows in header part
		"data_row": 1      :Start of the data rows 
		"discard_last_rows":0 :To handle corrupt rows in Trailer part
2.  Input column to staging column mapping.

        {"input_column_name": "EISBN", "staging_column_name":"e_product_id"},
		{"input_column_name": "VBID", "staging_column_name":"p_product_id"},
		{"input_column_name": "TITLE", "staging_column_name":"title"},

3.  How to handle null values for different column of different type

        {
			"column_name": "revenue_value",
			"dtype": "float",
			"missing_data":	{
							"process_type": "process",
							"value": 0.0
							}
		},

4.  Column aggregation type and columns to consider while doing aggregation.

        {
			"aggregation_function":{"list_price": "sum", "net_unit_price": "sum", "net_units": "sum", "revenue_value": "sum", "total_sales_count": "sum", "total_sales_value":"sum", "total_returns_count": "sum", "total_returns_value":"sum"},
			"groupby_columns": ["aggregator_name", "transaction_date", "e_product_id", "e_backup_product_id", "p_product_id", "p_backup_product_id", "title", "vendor_code", "product_type", "pod", "imprint", "product_format", "sale_type", "trans_currency", "disc_percentage", "region_of_sale", "misc_product_ids"]
		}

# Developer note
Now you can run the script using 

        python Main.py

