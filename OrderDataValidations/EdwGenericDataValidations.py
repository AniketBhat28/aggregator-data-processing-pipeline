import json
import boto3
import pg8000
import os
import logging as logger
import ccy

# Defining variable to store validation rules json local path
val_rules_json_local_path = os.path.dirname(os.path.realpath(__file__))

class EdwGenericDataValidations:
    # Function to connect to Redshift database based on env and input bucket passed
    def connect_to_redshift_db(self, input_bucket_name, env):

        if not input_bucket_name:
            # Setting input and output S3 locations
            input_bucket_name = 's3-use1-ap-pe-df-orders-insights-storage-d'
        if not env:
            env = 'uat'
        # Connect to s3 bucket-
        s3 = boto3.resource("s3")
        s3_bucket = s3.Bucket(input_bucket_name)
        s3_c = boto3.client('s3')

        # Fetch the respective db_connection based on the env passed
        if env == 'dev':
            db_variables = {'db_connection': "ind-redshift-dev-v1", 'database': "dev"}
            secret_name = "sm/appe/use1/ubx/analytics/dev"
            print("Using dev config")
        elif env == 'uat':
            db_variables = {'db_connection': "rd_cluster_uat_conn", 'database': "uat"}
            secret_name = "sm/appe/use1/ubx/analytics/uat"
        elif env == 'prod':
            db_variables = {'db_connection': "rd_cluster_prod_conn", 'database': "prod"}
            secret_name = "sm/appe/use1/ubx/analytics/prod"

        # Getting DB credentials from Secrets Manager
        client = boto3.client("secretsmanager", region_name="us-east-1")

        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        secret = json.loads(secret)

        db_username = secret.get('username')
        db_password = secret.get('password')
        db_host = secret.get('host')
        db_url = "jdbc:redshift://" + db_host + ":5439/" + env

        conn = pg8000.connect(user=db_username, database=db_variables['database'], host=db_host, port=5439,
                              password=db_password)
        return conn

    # Function for EDW data validations
    def edw_generic_data_val(self, conn, aggregator, job_type, env, edw_config_json):
        # Initialising the function with default values
        if not env:
            env = 'uat'
        if not job_type:
            env = 'BATCH'
        if not edw_config_json:
            with open(val_rules_json_local_path + '/Json' + '/edw-validation-config.json') as f:
                edw_config_json = json.load(f)
        # Reading the allowed list data from edw-validation-config-json for various validations
        allowed_agg_list = edw_config_json['aggregator_list']
        allowed_tran_type_list = edw_config_json['tran_type_list']
        allowed_sale_type_list = edw_config_json['sale_type_list']
        allowed_prod_frmt_cde_list = edw_config_json['prod_frmt_cde_list']

        logger.info("\n-+-+-+-+-Starting EDW Validations in" + "-+-+-+-+-")
        logger.info("ENVIRONMENT : " + str(env))
        logger.info("JOB_TYPE : " + str(job_type))
        print("\n-+-+-+-+-Starting EDW Validations in" + "-+-+-+-+-")
        print("ENVIRONMENT : " + str(env))
        print("JOB_TYPE : " + str(job_type))
        cursor = conn.cursor()
        ##############################
        # Generic Validation Queries #
        ##############################

        logger.info("\n-+-+-+-+-Starting DIM_AUDIT Table Generic EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting DIM_AUDIT Table Generic EDW validations-+-+-+-+-")
        # EDW Generic Validation 1
        dim_audit_val_tc1 = "TC_1: Verify if 'source' column in dim_audit table is updated correctly with aggregator values"
        dim_audit_val_query_1 = f""" select distinct source from edw.dim_audit where destination='Orders'; """
        print("\n" + dim_audit_val_tc1)
        print("Executing Query : ", dim_audit_val_query_1)
        cursor.execute(dim_audit_val_query_1)
        dim_audit_val_result1 = [val[0] for val in cursor.fetchall()]
        allowed_agg_list.sort()
        dim_audit_val_result1.sort()
        print("Query Output : ", dim_audit_val_result1)
        if dim_audit_val_result1 == allowed_agg_list:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")

        # EDW Generic Validation 2
        dim_audit_val_tc2 = "TC_2: Verify if 'destination' column in DIM_AUDIT table is updated correctly"
        dim_audit_val_query_2 = f""" select distinct destination from edw.dim_audit where SOURCE IN {repr(tuple(map(str, allowed_agg_list)))}"""
        print("\n" + dim_audit_val_tc2)
        print("Executing Query : ", dim_audit_val_query_2)
        cursor.execute(dim_audit_val_query_2)
        dim_audit_val_result2 = cursor.fetchmany()
        print("Query Output : ", dim_audit_val_result2)
        if [element for li in dim_audit_val_result2 for element in li] == ['Orders']:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"Destination column is incorrectly updated as {dim_audit_val_result2}")

        # EDW Generic Validation 3
        dim_audit_val_tc3 = "TC_3: Verify if 'audit_key' column in DIM_AUDIT table is NOT NULL"
        dim_audit_val_query_3 = f""" select count(audit_key) from edw.dim_audit where audit_key is null; """
        print("\n" + dim_audit_val_tc3)
        print("Executing Query : ", dim_audit_val_query_3)
        cursor.execute(dim_audit_val_query_3)
        dim_audit_val_result3 = cursor.fetchmany()
        print("Query Output : ", dim_audit_val_result3)
        if [element for li in dim_audit_val_result3 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_AUDIT table has few entries with NULL audit_key's ")

        logger.info("\n-+-+-+-+-Starting DIM_ORDER Table Generic EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting DIM_ORDER Table Generic EDW validations-+-+-+-+-")

        # EDW Generic Validation 4
        dim_order_val_tc1 = "TC_4: Verify if 'audit_key' column in DIM_ORDER table is NOT NULL"
        dim_order_val_query_1 = f""" select count(audit_key) from edw.dim_order where audit_key is null; """
        print("\n" + dim_order_val_tc1)
        print("Executing Query : ", dim_order_val_query_1)
        cursor.execute(dim_order_val_query_1)
        dim_order_val_result1 = cursor.fetchmany()
        print("Query Output : ", dim_order_val_result1)
        if [element for li in dim_order_val_result1 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_ORDER table has few entries with NULL audit_key's ")

        # EDW Generic Validation 5
        dim_order_val_tc2 = "TC_5: Verify if 'order_skey' column in DIM_ORDER table is NOT NULL"
        dim_order_val_query_2 = f""" select count(order_skey) from edw.dim_order where order_skey is null; """
        print("\n" + dim_order_val_tc2)
        print("Executing Query : ", dim_order_val_query_2)
        cursor.execute(dim_order_val_query_2)
        dim_order_val_result2 = cursor.fetchmany()
        print("Query Output : ", dim_order_val_result2)
        if [element for li in dim_order_val_result2 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_ORDER table has few entries with NULL order_skey's ")

        logger.info("\n-+-+-+-+-Starting FACT_ORDER_LINE Table Generic EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting FACT_ORDER_LINE Table Generic EDW validations-+-+-+-+-")

        # EDW Generic Validation 6
        fact_ord_line_val_tc1 = "TC_6: Verify if 'audit_key' column in FACT_ORDER_LINE table is NOT NULL"
        fact_ord_line_val_query_1 = f""" select count(audit_key) from edw.fact_order_line where audit_key is null; """
        print("\n" + fact_ord_line_val_tc1)
        print("Executing Query : ", fact_ord_line_val_query_1)
        cursor.execute(fact_ord_line_val_query_1)
        fact_ord_line_val_result_1 = cursor.fetchmany()
        print("Query Output : ", fact_ord_line_val_result_1)
        if [element for li in fact_ord_line_val_result_1 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"FACT_ORDER_LINE table has few entries with NULL audit_key's ")

        # EDW Generic Validation 7
        fact_ord_line_val_tc2 = "TC_7: Verify if 'order_line_skey' column in FACT_ORDER_LINE table is NOT NULL"
        fact_ord_line_val_query_2 = f""" select count(order_line_skey) from edw.fact_order_line where order_line_skey is null; """
        print("\n" + fact_ord_line_val_tc2)
        print("Executing Query : ", fact_ord_line_val_query_2)
        cursor.execute(fact_ord_line_val_query_2)
        fact_ord_line_val_result_2 = cursor.fetchmany()
        print("Query Output : ", fact_ord_line_val_result_2)
        if [element for li in fact_ord_line_val_result_2 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"FACT_ORDER_LINE table has few entries with NULL order_line_skey's ")

        # EDW Generic Validation 8
        fact_ord_line_val_tc3 = "TC_8: Verify if following columns in FACT_ORDER_LINE table are NOT NULL"
        fact_ord_line_val_query_3 = f"""
                                    SELECT
                                    COUNT(*)
                                    FROM edw.fact_order_line
                                    WHERE (buyer_location_skey IS NULL
                                    OR fulfilment_party_skey IS NULL
                                    OR reporting_date_skey IS NULL
                                    OR tran_type IS NULL
                                    OR product_format_code IS NULL
                                    OR list_price_currency IS NULL
                                    OR payment_currency IS NULL
                                    OR units IS NULL);
                                """
        print("\n" + fact_ord_line_val_tc3)
        print("Executing Query : ", fact_ord_line_val_query_3)
        cursor.execute(fact_ord_line_val_query_3)
        fact_ord_line_val_result_3 = cursor.fetchmany()
        print("Query Output : ", fact_ord_line_val_result_3)
        print([element for li in fact_ord_line_val_result_3 for element in li])
        if [element for li in fact_ord_line_val_result_3 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            # raise ValueError(f"FACT_ORDER_LINE table has few entries with NULL values present in above columns")

        # EDW Generic Validation 9
        fact_ord_line_val_tc4 = "TC_9: Verify if 'tran_type' column is correctly updated in FACT_ORDER_LINE Table with below values"
        fact_ord_line_val_query_4 = f""" select distinct tran_type from edw.fact_order_line; """
        print("\n" + fact_ord_line_val_tc4)
        print("Executing Query : ", fact_ord_line_val_query_4)
        cursor.execute(fact_ord_line_val_query_4)
        fact_ord_line_val_result_4 = [val[0] for val in cursor.fetchall()]
        allowed_tran_type_list.sort()
        fact_ord_line_val_result_4.sort()
        print("Query Output : ", fact_ord_line_val_result_4)
        if fact_ord_line_val_result_4 == allowed_tran_type_list:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            # raise ValueError(f"FACT_ORDER_LINE table has few wrong values in 'tran_type' column ")

        # EDW Generic Validation 10
        fact_ord_line_val_tc5 = "TC_10: Verify if 'sale_type' column is correctly updated in FACT_ORDER_LINE Table with below values"
        fact_ord_line_val_query_5 = f""" select distinct sale_type from edw.fact_order_line; """
        print("\n" + fact_ord_line_val_tc5)
        print("Executing Query : ", fact_ord_line_val_query_5)
        cursor.execute(fact_ord_line_val_query_5)
        fact_ord_line_val_result_5 = [val[0] for val in cursor.fetchall()]
        allowed_sale_type_list = sorted(allowed_sale_type_list, key=lambda x: (x is None, x))
        fact_ord_line_val_result_5 = sorted(fact_ord_line_val_result_5, key=lambda x: (x is None, x))
        print("Query Output : ", fact_ord_line_val_result_5)
        print("Expected Result : ", allowed_sale_type_list)
        if fact_ord_line_val_result_5 == allowed_sale_type_list:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            # raise ValueError(f"FACT_ORDER_LINE table has few wrong values in 'sale_type' column ")

        # EDW Generic Validation 11
        fact_ord_line_val_tc6 = "TC_11: Verify if 'product_format_code' column is correctly updated in FACT_ORDER_LINE Table with below values"
        fact_ord_line_val_query_6 = f""" select distinct product_format_code from edw.fact_order_line; """
        print("\n" + fact_ord_line_val_tc6)
        print("Executing Query : ", fact_ord_line_val_query_6)
        cursor.execute(fact_ord_line_val_query_6)
        fact_ord_line_val_result_6 = [val[0] for val in cursor.fetchall()]
        allowed_prod_frmt_cde_list.sort()
        fact_ord_line_val_result_6.sort()
        print("Query Output : ", fact_ord_line_val_result_6)
        print("Expected Result : ", allowed_prod_frmt_cde_list)
        if fact_ord_line_val_result_6 == allowed_prod_frmt_cde_list:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            # raise ValueError(f"FACT_ORDER_LINE table has few wrong values in 'product_format_code' column ")

        # EDW Generic Validation 12
        fact_ord_line_val_tc7 = "TC_12: Verify if 'list_price_currency' column is correctly updated in FACT_ORDER_LINE Table with below values"
        fact_ord_line_val_query_7 = f""" select distinct list_price_currency from edw.fact_order_line; """
        print("\n" + fact_ord_line_val_tc7)
        print("Executing Query : ", fact_ord_line_val_query_7)
        cursor.execute(fact_ord_line_val_query_7)
        fact_ord_line_val_result_7 = [val[0] for val in cursor.fetchall()]
        fact_ord_line_val_result_7 = sorted(fact_ord_line_val_result_7, key=lambda x: (x is None, x))
        print("Query Output : ", fact_ord_line_val_result_7)
        for element in fact_ord_line_val_result_7:
            result = ccy.currency(element)
            if result is None:
                print("list_price_currency code : " + str(element) + " is INVALID")
                # raise ValueError(f"Invalid list_price_currency code is present in FACT_ORDER_LINE table")
            else:
                print("list_price_currency code : " + str(element) + " is VALID")

        # EDW Generic Validation 13
        fact_ord_line_val_tc8 = "TC_13: Verify if 'payment_currency' column is correctly updated in FACT_ORDER_LINE Table with below values"
        fact_ord_line_val_query_8 = f""" select distinct payment_currency from edw.fact_order_line; """
        # Need to check if 'SAR', 'ZAR' are valid currencies
        print("\n" + fact_ord_line_val_tc8)
        print("Executing Query : ", fact_ord_line_val_query_8)
        cursor.execute(fact_ord_line_val_query_8)
        fact_ord_line_val_result_8 = [val[0] for val in cursor.fetchall()]
        fact_ord_line_val_result_8.sort()
        print("Query Output : ", fact_ord_line_val_result_8)
        for element in fact_ord_line_val_result_8:
            result = ccy.currency(element)
            if result is None:
                print("payment_currency code : " + str(element) + " is INVALID")
                raise ValueError(f"Invalid payment_currency code is present in FACT_ORDER_LINE table")
            else:
                print("payment_currency code : " + str(element) + " is VALID")

        # EDW Generic Validation 14
        fact_ord_line_val_tc9 = "TC_14: Verify if 'is_print_on_demand' column is correctly updated in FACT_ORDER_LINE Table with below values"
        fact_ord_line_val_query_9 = f""" select distinct is_print_on_demand from edw.fact_order_line; """
        allowed_prnt_on_dmnd_list = [False, True]
        print("\n" + fact_ord_line_val_tc9)
        print("Executing Query : ", fact_ord_line_val_query_9)
        cursor.execute(fact_ord_line_val_query_9)
        fact_ord_line_val_result_9 = [val[0] for val in cursor.fetchall()]
        allowed_prnt_on_dmnd_list.sort()
        fact_ord_line_val_result_9.sort()
        print("Query Output : ", fact_ord_line_val_result_9)
        if fact_ord_line_val_result_9 == allowed_prnt_on_dmnd_list:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"FACT_ORDER_LINE table has few wrong values in 'is_print_on_demand' column ")

        logger.info("\n-+-+-+-+-Starting REF_ISBN Table Generic EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting REF_ISBN Table Generic EDW validations-+-+-+-+-")

        # EDW Generic Validation 15
        ref_isbn_val_tc1 = "TC_15: Verify if 'audit_key' column in REF_ISBN table is NOT NULL"
        ref_isbn_val_query_1 = f""" select count(audit_key) from edw.ref_isbn where audit_key is null; """
        print("\n" + ref_isbn_val_tc1)
        print("Executing Query : ", ref_isbn_val_query_1)
        cursor.execute(ref_isbn_val_query_1)
        ref_isbn_val_result_1 = cursor.fetchmany()
        print("Query Output : ", ref_isbn_val_result_1)
        if [element for li in ref_isbn_val_result_1 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"REF_ISBN table has entries with NULL audit_key's ")

        # EDW Generic Validation 16
        ref_isbn_val_tc2 = "TC_16: Verify if 'isbn' column in REF_ISBN table is NOT NULL"
        ref_isbn_val_query_2 = f""" select count(isbn) from edw.ref_isbn where isbn is null; """
        print("\n" + ref_isbn_val_tc2)
        print("Executing Query : ", ref_isbn_val_query_2)
        cursor.execute(ref_isbn_val_query_2)
        ref_isbn_val_result_2 = cursor.fetchmany()
        print("Query Output : ", ref_isbn_val_result_2)
        if [element for li in ref_isbn_val_result_2 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"REF_ISBN table has few entries with NULL 'isbn' values ")

        # EDW Generic Validation 17
        ref_isbn_val_tc3 = "TC_17: Verify if 'master_isbn' column in REF_ISBN table is NOT NULL"
        ref_isbn_val_query_3 = f""" select count(master_isbn) from edw.ref_isbn where isbn is null; """
        print("\n" + ref_isbn_val_tc3)
        print("Executing Query : ", ref_isbn_val_query_3)
        cursor.execute(ref_isbn_val_query_3)
        ref_isbn_val_result_3 = cursor.fetchmany()
        print("Query Output : ", ref_isbn_val_result_3)
        if [element for li in ref_isbn_val_result_3 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"REF_ISBN table has few entries with NULL 'master_isbn' values ")

        # EDW Generic Validation 18
        ref_isbn_val_tc4 = "TC_18: Verify if 'is_inferred' column is correctly updated in REF_ISBN Table"
        ref_isbn_val_query_4 = f""" select distinct is_inferred from edw.REF_ISBN; """
        allowed_is_inferred_list = [False, True]
        print("\n" + ref_isbn_val_tc4)
        print("Executing Query : ", ref_isbn_val_query_4)
        cursor.execute(ref_isbn_val_query_4)
        ref_isbn_val_result_4 = [val[0] for val in cursor.fetchall()]
        allowed_is_inferred_list.sort()
        ref_isbn_val_result_4.sort()
        print("Query Output : ", ref_isbn_val_result_4)
        if ref_isbn_val_result_4 == allowed_is_inferred_list:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"REF_ISBN table has few wrong values in 'is_inferred' column ")

        logger.info("\n-+-+-+-+-Starting DIM_PRODUCT Table Generic EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting DIM_PRODUCT Table Generic EDW validations-+-+-+-+-")

        # EDW Generic Validation 19
        dim_product_val_tc1 = "TC_19: Verify if 'audit_key' column in DIM_PRODUCT table is NOT NULL"
        dim_product_val_query_1 = f""" select count(audit_key) from edw.dim_product where audit_key is null; """
        print("\n" + dim_product_val_tc1)
        print("Executing Query : ", dim_product_val_query_1)
        cursor.execute(dim_product_val_query_1)
        dim_product_val_result_1 = cursor.fetchmany()
        print("Query Output : ", dim_product_val_result_1)
        if [element for li in dim_product_val_result_1 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_PRODUCT table has entries with NULL audit_key's ")

        # EDW Generic Validation 20
        dim_product_val_tc2 = "TC_20: Verify if 'product_skey' column in DIM_PRODUCT table is NOT NULL"
        dim_product_val_query_2 = f""" select count(product_skey) from edw.dim_product where product_skey is null; """
        print("\n" + dim_product_val_tc2)
        print("Executing Query : ", dim_product_val_query_2)
        cursor.execute(dim_product_val_query_2)
        dim_product_val_result_2 = cursor.fetchmany()
        print("Query Output : ", dim_product_val_result_2)
        if [element for li in dim_product_val_result_2 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_PRODUCT table has few entries with NULL 'product_skey' values ")

        # EDW Generic Validation 21
        dim_product_val_tc3 = "TC_21: Verify if 'product_bskey' column in REF_ISBN table is NOT NULL"
        dim_product_val_query_3 = f""" select count(product_bskey) from edw.dim_product where product_bskey is null; """
        print("\n" + dim_product_val_tc3)
        print("Executing Query : ", dim_product_val_query_3)
        cursor.execute(dim_product_val_query_3)
        dim_product_val_result_3 = cursor.fetchmany()
        print("Query Output : ", dim_product_val_result_3)
        if [element for li in dim_product_val_result_3 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_PRODUCT table has few entries with NULL 'product_bskey' values ")

        # EDW Generic Validation 22
        dim_product_val_tc4 = "TC_22: Verify if 'is_inferred' column is correctly updated in DIM_PRODUCT Table"
        dim_product_val_query_4 = f""" select distinct is_inferred from edw.dim_product; """
        allowed_is_inferred_list = [False, True]
        print("\n" + dim_product_val_tc4)
        print("Executing Query : ", dim_product_val_query_4)
        cursor.execute(dim_product_val_query_4)
        dim_product_val_result_4 = [val[0] for val in cursor.fetchall()]
        allowed_is_inferred_list.sort()
        dim_product_val_result_4.sort()
        print("Query Output : ", dim_product_val_result_4)
        if dim_product_val_result_4 == allowed_is_inferred_list:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_PRODUCT table has few wrong values in 'is_inferred' column ")

        logger.info("\n-+-+-+-+-Starting DIM_PARTY Table Generic EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting DIM_PARTY Table Generic EDW validations-+-+-+-+-")

        # EDW Generic Validation 23
        dim_party_val_tc1 = "TC_23: Verify if 'audit_key' column in DIM_PARTY table is NOT NULL"
        dim_party_val_query_1 = f""" select count(audit_key) from edw.dim_party where audit_key is null; """
        print("\n" + dim_party_val_tc1)
        print("Executing Query : ", dim_party_val_query_1)
        cursor.execute(dim_party_val_query_1)
        dim_party_val_result_1 = cursor.fetchmany()
        print("Query Output : ", dim_party_val_result_1)
        if [element for li in dim_party_val_result_1 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_PARTY table has entries with NULL audit_key's ")

        # EDW Generic Validation 24
        dim_party_val_tc2 = "TC_24: Verify if 'party_skey' column in DIM_PARTY table is NOT NULL"
        dim_party_val_query_2 = f""" select count(party_skey) from edw.dim_party where party_skey is null; """
        print("\n" + dim_party_val_tc2)
        print("Executing Query : ", dim_party_val_query_2)
        cursor.execute(dim_party_val_query_2)
        dim_party_val_result_2 = cursor.fetchmany()
        print("Query Output : ", dim_party_val_result_2)
        if [element for li in dim_party_val_result_2 for element in li] == [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_PARTY table has few entries with NULL 'party_skey' values ")

        # EDW Generic Validation 25
        dim_party_val_tc3 = "TC_25: Verify if DIM_PARTY has NO duplicate entries present with same Billing Customer ID"
        dim_party_val_query_3 = f"""select count(*) from edw.dim_party group by external_id having count(external_id)>1"""
        print("\n" + dim_party_val_tc3)
        print("Executing Query : ", dim_party_val_query_3)
        cursor.execute(dim_party_val_query_3)
        dim_party_val_result_3 = cursor.fetchall()
        print("Query Output : ", dim_party_val_result_3)
        for li in dim_party_val_result_3:
            for element in li:
                if element > 1:
                    print("\nCount of Duplicate records :" + str(element))
                    print("Test Result : FAIL")
                    # raise ValueError(f"DIM_PARTY table has Duplicate entries with same Billing Customer ID ")
                else:
                    print("\n" + "Test Result : PASS")
        cursor.close()