# aggregator-data-processing-pipeline
### This application processes ebook sales files for different aggregators and generates staging data for reporting.
# About the application
This application runs for all the available aggregators as per the corresponding 
rules in AggRulesVal.json.The corresponding aggregator files will be read from s3 as the input source and the generated staging out will be written 
 to the s3 location as output.
 The input and output locations along with the file name are controlled by the project Config.json file. 

# Application Objectives
Using this application we are generating the common staging data as described in https://taylorfrancis.atlassian.net/wiki/spaces/DF/pages/1212579853/Staging+data+entity+-+Post+processing+eBook+sales+files 
for all the available aggregators which helps us on report generation.
We are reading the aggregator data from given s3 location using boto3 aws Python library and processing it with the help of Python Pandas
library .
The application is fully config driven and controlled by AggRulesVal.json which gives us the privileges to add, modify or delete 
any rules thereby processing data with different complex formats without any direct changes in the code.
The input file location and output location are handled by Config.json which makes the application a fully dynamic one .

# Prerequisites
python3

AWS cli should be configured

# Installing and Running
Install the required dependencies using the following command
Required dependencies: boto3, xlrd, s3fs
        
        pip install dependency_name


Replace your own IAM credentials here : 

        aws_credentials['ACCESS_KEY'] = '' #add aws Access key for IAM user
        aws_credentials['SECRET_KEY'] = '' #Add aws secret key for IAM user

## How to setup
#### step-1: Repository Setup - Clone.
        git clone https://github.com/tandfgroup/aggregator-data-processing-pipeline.git <your local path>
for example :

        git clone https://github.com/tandfgroup/aggregator-data-processing-pipeline.git C:/\Users/\Desktop/\codebase/\git_code/\ingram_vital_source

#### step-2: Config Setup
Change your Config.json file like below as per the aggregator you want to process. You can control your input and output location for the application here.

        {
	        "aggregator_name": "Amazon",
	        "input_bucket_name": "s3-euw1-ap-pe-orders-worker-agg-storage-etl",
	        "input_directory" : "prd/AMAZON/input/2020/JUN",
	        "output_bucket_name" : "s3-use1-ap-pe-df-orders-insights-storage-d",
	        "output_directory" : "staging/revenue/aggregator/AMAZON/eBook-062020.csv"
	    }


        

#### step-3: Rules

Change the AggRulesVal.json as per the business requirements.
It supports,

1.  Initial checks.

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

4.  Date formats to be processed.

        [
			"%m/%d/%Y", "%Y-%d-%m %H:%M:%S", "%d.%m.%Y"
		],

# Packaging and Deployment
Create an egg file for the given code base

## How to package
#### step-1: Edit the CreateEgg.py
1.  Name the ouput directory in given repository.

        OUTPUT_DIR = 'Output'

2.  Edit the name of the .egg file to be created and add the folders to be considered while building.

        setup(
        	name="aggregator-data-processing-pipeline",
        	packages=['Preprocessing','ReadWriteData','AttributeGenerators','StagingDataGenerators'],
        	version="1.0",
        	script_args=['--quiet', 'bdist_egg'], # to create egg-file only
             )
    
#### step-2: Run CreateEgg.py to create .egg file at the specified location

        python CreateEgg.py

This egg file can be used as an external code package to any server-based services such as AWS Glue to automate job runs


# Developer note
You can also run the script locally using following command 

        python Main.py

