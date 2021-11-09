import json
import os
import logging as logger

# Defining variable to store validation rules json local path
val_rules_json_local_path = os.path.dirname(os.path.realpath(__file__))

class EdwAggregatorSpecificDataValidations:
    # Function for EDW data validations
    def edw_agg_data_val(self, conn, aggregator_name, job_type, environment):
        # Initialising the function with default values
        if not environment:
            environment = 'uat'
        if not job_type:
            job_type = 'BATCH'

        logger.info("\n-+-+-+-+-Starting Aggregator Specific EDW Data Validations-+-+-+-+-")
        logger.info("\n-+-+-+-+-Starting DIM_AUDIT Table Aggregator Specific EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting Aggregator Specific EDW Data Validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting DIM_AUDIT Table Aggregator Specific EDW validations-+-+-+-+-")
        cursor = conn.cursor()

        # Aggregator Specific EDW Validation 1
        dim_audit_val_tc1 = f"AGG_TC_1: Verify if 'audit_key' is present in DIM_AUDIT table for {aggregator_name}"
        if job_type == 'LIVE' or 'BATCH':
            dim_audit_val_query_1 = f""" select audit_key from edw.dim_audit where destination='Orders' and SOURCE = '{aggregator_name}' order by start_time desc limit 1; """
        else:
            dim_audit_val_query_1 = f""" select audit_key from edw.dim_audit where destination ='Orders' and source='{aggregator_name}' and job_status='success'; """
        print("\n" + dim_audit_val_tc1)
        print("Executing Query : ", dim_audit_val_query_1)
        cursor.execute(dim_audit_val_query_1)
        dim_audit_val_result1 = cursor.fetchmany()
        audit_key_val = dim_audit_val_result1[-1][0]
        print("Query Output : ", dim_audit_val_result1)
        if [element for li in dim_audit_val_result1 for element in li] is None:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_AUDIT table has No audit_key present for {aggregator_name}")
        else:
            print("\n" + "Test Result : PASS")

        # Aggregator Specific EDW Validation 2
        dim_audit_val_tc2 = "AGG_TC_2: Verify if all older audit_key's are updated with job_status as 'order reloaded' "
        dim_audit_val_query_2 = f""" select audit_key,job_status from edw.dim_audit where destination='Orders' and source='{aggregator_name}'; """
        print("\n" + dim_audit_val_tc2)
        print("Executing Query : ", dim_audit_val_query_2)
        cursor.execute(dim_audit_val_query_2)
        dim_audit_val_result2 = cursor.fetchall()
        print("Query Output : ", dim_audit_val_result2)
        # Filtering out older audit_key's having status not as 'success'
        dim_audit_val_result_temp = (list(filter(lambda x: x[1] != 'success', dim_audit_val_result2)))
        print("Older audit_keys are: " + str(dim_audit_val_result_temp))
        if ['order reloaded' in element for element in dim_audit_val_result_temp]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_AUDIT table is correctly updated with job_status as ‘order reloaded' for older audit keys")

        logger.info("\n-+-+-+-+-Starting DIM_ORDER Table Aggregator Specific EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting DIM_ORDER Table Aggregator Specific EDW validations-+-+-+-+-")

        # Aggregator Specific EDW Validation 3
        dim_order_val_tc1 = f"AGG_TC_3: Verify if entries are created in DIM_ORDER table with {aggregator_name} audit_key "
        dim_order_val_query_1 = f""" select count(audit_key) from edw.dim_order where audit_key='{audit_key_val}'; """
        print("\n" + dim_order_val_tc1)
        print("Executing Query : ", dim_order_val_query_1)
        cursor.execute(dim_order_val_query_1)
        dim_order_val_result1 = cursor.fetchmany()
        print("Query Output : ", dim_order_val_result1)
        if [element for li in dim_order_val_result1 for element in li] != [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"DIM_ORDER table has no entries with {aggregator_name} audit_key")


        logger.info("\n-+-+-+-+-Starting FACT_ORDER_LINE Table Aggregator Specific EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting FACT_ORDER_LINE Table Aggregator Specific EDW validations-+-+-+-+-")

        # Aggregator Specific EDW Validation 4
        fact_ord_line_val_tc1 = f"AGG_TC_4: Verify if entries are created in FACT_ORDER_LINE table with {aggregator_name} audit_key "
        fact_ord_line_val_query_1 = f""" select count(audit_key) from edw.fact_order_line where audit_key='{audit_key_val}'; """
        print("\n" + fact_ord_line_val_tc1)
        print("Executing Query : ", fact_ord_line_val_query_1)
        cursor.execute(fact_ord_line_val_query_1)
        fact_ord_line_val_result1 = cursor.fetchmany()
        print("Query Output : ", fact_ord_line_val_result1)
        if [element for li in fact_ord_line_val_result1 for element in li] != [0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"FACT_ORDER_LINE table has no entries with {aggregator_name} audit_key")

        # Aggregator Specific EDW Validation 5
        fact_ord_line_val_tc2 = "AGG_TC_5: Validate if number of USPT records in staging layer and redshift layer are matching correctly or not "
        print("\n" + fact_ord_line_val_tc2)
        # Getting all distinct audit_key's for USPT
        fact_ord_line_val_query_2a = f""" select distinct audit_key from edw.dim_audit where destination='Orders' and SOURCE = '{aggregator_name}'; """
        print("Executing Query : ", fact_ord_line_val_query_2a)
        cursor.execute(fact_ord_line_val_query_2a)
        fact_ord_line_val_result2a = cursor.fetchall()
        print("Query Output : ", fact_ord_line_val_result2a)
        # Getting count of Aggregator records from EDW Redshift layer
        audit_key_list = [element for li in fact_ord_line_val_result2a for element in li]
        print(audit_key_list)
        fact_ord_line_val_query_2b = f""" select count(*) from edw.fact_order_line where audit_key in (%s);""" % ",".join(map(str, audit_key_list))
        print("Executing Query : ", fact_ord_line_val_query_2b)
        cursor.execute(fact_ord_line_val_query_2b)
        fact_ord_line_val_result2b = cursor.fetchall()
        print("Query Output : ", fact_ord_line_val_result2b)
        # Getting count of Aggregator records from Staging Redshift layer
        fact_ord_line_val_query_2c = f""" select COUNT(*) from mda_data_lake.{environment}_staging_{aggregator_name}; """
        print("Executing Query : ", fact_ord_line_val_query_2c)
        cursor.execute(fact_ord_line_val_query_2c)
        fact_ord_line_val_result2c = cursor.fetchmany()
        print("Query Output : ", fact_ord_line_val_result2c)
        if fact_ord_line_val_result2b == fact_ord_line_val_result2c:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")

        logger.info("\n-+-+-+-+-Starting REF_ISBN Table Aggregator Specific EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting REF_ISBN Table Aggregator Specific EDW validations-+-+-+-+-")

        # Aggregator Specific EDW Validation 6
        ref_isbn_val_tc1 = f"AGG_TC_6: Verify if onlt 10 Digit or 13 Digit entries are created in REF_ISBN table through {aggregator_name} orders pipeline "
        ref_isbn_val_step1_query = f""" select audit_key from edw.dim_audit where destination='Orders' and SOURCE = '{aggregator_name}' order by start_time desc limit 1; """
        print("\n" + ref_isbn_val_tc1)
        print("Executing Query : ", ref_isbn_val_step1_query)
        cursor.execute(ref_isbn_val_step1_query)
        ref_isbn_val_step1_result = cursor.fetchall()
        print("Query Output : ", ref_isbn_val_step1_result)
        # Filtering out older audit_key's having status as 'success' based on job_type
        if job_type == 'LIVE' or 'BATCH':
            dim_audit_keys_list = ref_isbn_val_step1_result[0]
            print(f"List of NEW {aggregator_name} audit_keys are : " + str(dim_audit_keys_list))
        else:
            dim_audit_keys_list = [item for li in ref_isbn_val_step1_result for item in li]
            print("List of ALL USPT audit_keys are : " + str(dim_audit_keys_list))

        ref_isbn_val_step2_query = f""" select count(*) from edw.ref_isbn where audit_key in (%s);""" % ",".join(map(str, dim_audit_keys_list))
        print("Executing Query : ", ref_isbn_val_step2_query)
        cursor.execute(ref_isbn_val_step2_query)
        ref_isbn_val_step2_result = cursor.fetchall()
        print("Number of records inserted in REF_ISBN table through Orders pipeline are : ", ref_isbn_val_step2_result[0][0])

        ref_isbn_val_step3_query = f""" select distinct length(isbn) from edw.ref_isbn where audit_key in (%s);""" % ",".join(map(str, dim_audit_keys_list))
        print("Executing Query : ", ref_isbn_val_step3_query)
        cursor.execute(ref_isbn_val_step3_query)
        ref_isbn_val_step3_result = cursor.fetchall()
        print("Query Output : ", list(ref_isbn_val_step3_result))
        for li in ref_isbn_val_step3_result:
            for element in li:
                if element == 10 or element == 13:
                    print("\n" + "ISBN length from the above query output is : " + str(element))
                    print("Test Result : PASS")
                else:
                    if element > 13:
                        ref_isbn_val_step4_query = f"""select count(isbn) from edw.ref_isbn where length(isbn) > 13;"""
                        print("\n" + "Executing Query : ", ref_isbn_val_step4_query)
                        cursor.execute(ref_isbn_val_step4_query)
                        ref_isbn_val_step4_result = cursor.fetchall()
                        print("Query Output : ", ref_isbn_val_step4_result)
                        print("Test Result : FAIL")
                        raise ValueError(f"ISBN records with length greater than 13 digits are present in REF_ISBN table")
                    else:
                        ref_isbn_val_step5_query = f"""select count(isbn) from edw.ref_isbn where length(isbn)=({element});"""
                        print("\n" + "Executing Query : ", ref_isbn_val_step5_query)
                        cursor.execute(ref_isbn_val_step5_query)
                        ref_isbn_val_step5_result = cursor.fetchall()
                        print("Query Output : ", ref_isbn_val_step5_result)
                        print("Test Result : FAIL")

        logger.info("\n-+-+-+-+-Starting DIM_PRODUCT Table Aggregator Specific EDW validations-+-+-+-+-")
        print("\n-+-+-+-+-Starting DIM_PRODUCT Table Aggregator Specific EDW validations-+-+-+-+-")

        # Aggregator Specific EDW Validation 7
        dim_product_val_tc1 = "AGG_TC_7: Verify count of entries created in DIM_PRODUCT table through Orders pipeline are matching with REF_ISBN table"
        print("\n" + dim_product_val_tc1)
        # Fetching count of records inserted into DIM_PRODUCT using uspt audit keys
        dim_product_val_step2_query = f""" select count(*) from edw.dim_product where audit_key in (%s);""" % ",".join(map(str, dim_audit_keys_list))
        print("Executing Query : ", dim_product_val_step2_query)
        cursor.execute(dim_product_val_step2_query)
        dim_product_val_step2_result = cursor.fetchall()
        print(f"\nNumber of records inserted in DIM_PRODUCT table through {aggregator_name} orders pipeline are : ", dim_product_val_step2_result[0][0])
        print(f"Number of records inserted in REF_ISBN table through {aggregator_name} orders pipeline are : ",ref_isbn_val_step2_result[0][0])
        if dim_product_val_step2_result[0] == ref_isbn_val_step2_result[0]:
            print("\n" + "Test Result : PASS")
        else:
            print("\n" + "Test Result : FAIL")
            raise ValueError(f"Count of Records inserted into DIM_PRODUCT table is NOT matching with REF_ISBN inserted records count")

        # Aggregator Specific Advance Source File Checks
        if job_type == 'LIVE':
            dim_order_source_file_query_1 = f""" select distinct source from edw.dim_order where audit_key='{audit_key_val}'; """
            print("Executing Query : ", dim_order_source_file_query_1)
            cursor.execute(dim_order_source_file_query_1)
            dim_order_source_file_result_1 = cursor.fetchall()
            print("Query Output : ", dim_order_source_file_result_1)
            for index, element in enumerate(dim_order_source_file_result_1):
                print("\n\n+-+-+-+-+-+--+-+-+-+-+-+-+-+-+-Starting Advance Checks on Source File : " + str(element[0]) + "+-+-+-+-+-+--+-+-+-+-+-+-+-+-+-")
                dim_order_source_file_query_2 = f""" select distinct order_skey from edw.dim_order where SOURCE='{element[0]}'; """
                print("\nExecuting Query : ", dim_order_source_file_query_2)
                cursor.execute(dim_order_source_file_query_2)
                order_skey_list = cursor.fetchall()
                order_skey_list = [element for li in order_skey_list for element in li]
                print("Query Output : ", order_skey_list)
                # Source File Advance Check 1
                # Fetching count of records in fact_order_line table using the list of order_skey_list
                src_fle_chk_tc_1 = "AGG_TC_8: Source File Advance Check 1: Validate if COUNT of Records in EDW Layer and Staging Layer are matching for a Given Source File"
                print("\n" + src_fle_chk_tc_1)
                src_fle_chk_query_1a = f""" select count(*) from edw.fact_order_line where order_skey in (%s);""" % ",".join(map(str, order_skey_list))
                print("Executing Query : ", src_fle_chk_query_1a)
                cursor.execute(src_fle_chk_query_1a)
                src_fle_chk_rslt_1a = cursor.fetchall()
                print("Query Output : " + str(src_fle_chk_rslt_1a[0][0]))
                # Fetching count of records in STAGING table for the give source file
                src_fle_chk_query_1b = f""" select count(*) from mda_data_lake.{environment}_staging_{aggregator_name} where SOURCE_ID = '{element[0]}'; """
                print("Executing Query : ", src_fle_chk_query_1b)
                cursor.execute(src_fle_chk_query_1b)
                src_fle_chk_rslt_1b = cursor.fetchall()
                print("Query Output : " + str(src_fle_chk_rslt_1b[0][0]))
                # Comparing count of records in FACT_ORDER_LINE table Vs STAGING table
                if src_fle_chk_rslt_1a[0] == src_fle_chk_rslt_1b[0]:
                    print("\n" + "Test Result : PASS")
                else:
                    print("\n" + "Test Result : FAIL")

                # Source File Advance Check 2
                src_fle_chk_tc_2 = "AGG_TC_9: Source File Advance Check 2: Validate if SUM of UNITS in EDW Layer and Staging Layer are matching for a Given Source File"
                print("\n" + src_fle_chk_tc_2)
                src_fle_chk_query_2a = f""" select sum(units) from edw.fact_order_line where order_skey in (%s);""" % ",".join(map(str, order_skey_list))
                print("Executing Query : ", src_fle_chk_query_2a)
                cursor.execute(src_fle_chk_query_2a)
                src_fle_chk_rslt_2a = cursor.fetchall()
                print("Query Output : " + str(src_fle_chk_rslt_2a[0][0]))
                # Fetching count of records in STAGING table for the give source file
                src_fle_chk_query_2b = f""" select sum(units) from mda_data_lake.{environment}_staging_{aggregator_name} where SOURCE_ID = '{element[0]}'; """
                print("Executing Query : ", src_fle_chk_query_2b)
                cursor.execute(src_fle_chk_query_2b)
                src_fle_chk_rslt_2b = cursor.fetchall()
                print("Query Output : " + str(src_fle_chk_rslt_2b[0][0]))
                # print("Sum of UNITS in UAT_STAGING_USPT table for source file-" + str(element[0]) + " is: " + str(src_fle_chk_rslt_2b[0][0]))
                # Comparing count of records in FACT_ORDER_LINE table Vs STAGING table
                if src_fle_chk_rslt_2a[0] == src_fle_chk_rslt_2b[0]:
                    print("\n" + "Test Result : PASS")
                else:
                    print("\n" + "Test Result : FAIL")

                # Source File Advance Check 3
                src_fle_chk_tc_3 = "AGG_TC_10: Source File Advance Check 3: Validate if SUM of PAYMENT_AMOUNT in EDW Layer and Staging Layer are matching for a Given Source File"
                print("\n" + src_fle_chk_tc_3)
                src_fle_chk_query_3a = f""" select sum(payment_amount) from edw.fact_order_line where order_skey in (%s);""" % ",".join(map(str, order_skey_list))
                cursor.execute(src_fle_chk_query_3a)
                print("Executing Query : ", src_fle_chk_query_3a)
                src_fle_chk_rslt_3a = cursor.fetchall()
                print("Query Output : " + str(src_fle_chk_rslt_3a[0][0]))
                # print("Sum of UNITS in FACT_ORDER_LINE table for source file-" + str(element[0]) + " is: " + str(src_fle_chk_rslt_3a[0][0]))
                # Fetching count of records in STAGING table for the give source file
                src_fle_chk_query_3b = f""" select sum(payment_amount) from mda_data_lake.{environment}_staging_{aggregator_name} where SOURCE_ID = '{element[0]}'; """
                print("Executing Query : ", src_fle_chk_query_3b)
                cursor.execute(src_fle_chk_query_3b)
                src_fle_chk_rslt_3b = cursor.fetchall()
                print("Query Output : " + str(src_fle_chk_rslt_3b[0][0]))
                # print("Sum of UNITS in UAT_STAGING_USPT table for source file-" + str(element[0]) + " is: " + str(src_fle_chk_rslt_3b[0][0]))
                # Comparing count of records in FACT_ORDER_LINE table Vs STAGING table
                if src_fle_chk_rslt_3a[0] == src_fle_chk_rslt_3b[0]:
                    print("\n" + "Test Result : PASS")
                else:
                    print("\n" + "Test Result : FAIL")
        cursor.close()
        conn.close()