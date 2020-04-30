__author__ = "dvbbf"
# Filename: all_trn_to_1_collection.py
#
# Source/Target details:
# Input : ALL LS DATASETS(JSON format)
# Output: ALL_TRN_TO_1_COLLECTION_FULL (EMBEDDED_COLLECTION)
#
# Description:
# The script processes all LS_data in two steps keeping intermediate data at MongoDB to avoid memory crash.
# Known issues:
# Script at times failes at VSL_TBL while processing Step 2 of 4 @IDV_Relationship. Solution: It just needs re-run.
#########################################################################################################################
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, lit, col, concat
from pyspark.sql.types import *

import sys
import os
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser

from datetime import datetime
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.window import Window
# import xml.etree.ElementTree as ET
# import requests

#Arguments
path_kafka_hdfs = sys.argv[1]
mongo_write_collection_full = sys.argv[2]
mongo_read_collection=sys.argv[3]

## LS_MAP_Details (Source) - Begin
LS_MAP_IDV_ASSO = sys.argv[4]
LS_MAP_GURN = sys.argv[5]
LS_MAP_IDV_CTZN = sys.argv[6]
LS_MAP_IDV_FLAG = sys.argv[7]
LS_MAP_STREET_ADDR = sys.argv[8]
LS_MAP_IDV_TAX = sys.argv[9]
LS_MAP_IND_RLSHP = sys.argv[10]
LS_MAP_EMAIL_ADDR = sys.argv[11]
LS_MAP_IDV_PARTY_AGMT = sys.argv[12]
columndata = sys.argv[13]

## Global variables
mongo_write_collection_partial = 'all_ls_to_trn_part'
mongo_temp_collection = "all_ls_to_trn_temp"
ALL_TRN_TEMP_UPDATE_COLL = "all_trn_unique_temp_coll"

# Testing parameters
# path_kafka_hdfs = "F:\Project\ODS_Mongo\LS_Data\LS_DATA_2020_01\LS_Layer_Output.json"
# mongo_write_collection_full = 'ALL_TRN_TO_1_COLLECTION_FULL'
# ## VSL Collection Name
# mongo_read_collection="TRN_VALUE_SET_LOOKUP"
#
# ## LS_MAP_Details (Source) - Begin
# LS_MAP_IDV_ASSO="STREAMING.CDB.INDIVIDUAL-ASSO.LDM"
# LS_MAP_GURN="STREAMING.CDB.GURN.LDM"
# LS_MAP_IDV_CTZN = "STREAMING.CDB.IND-ADDNL-CTZN.LDM"
# LS_MAP_IDV_FLAG = "STREAMING.CDB.IND-INDICATORS.LDM"
# LS_MAP_STREET_ADDR="STREAMING.CDB.STREET-ADDRESS.LDM"
# LS_MAP_IDV_TAX="STREAMING.CDB.PARTY-TAX-RESIDENCE.LDM"
# LS_MAP_IND_RLSHP = "STREAMING.CDB.IND-RELATIONSHIP.LDM"
# LS_MAP_EMAIL_ADDR="STREAMING.CDB.ELECTRONIC-ADDRESS.LDM"
# LS_MAP_IDV_PARTY_AGMT="STREAMING.CDB.PARTY-AGREEMENT.LDM"
# ## LS_MAP_Details - End
# # addr_dr_api_url="http://devecpvm008344.server.rbsgrp.net:6301/DataIntegrationService/WebService/IAV_UK_Interactive"
# columndata = "F:\Project\ODS_Mongo\LS_Data\LS_DATA_2020_01\lsColumnsData.json"
#
# ## Embedded TRN_Layer's Intermediate & Final Collection Name
# mongo_write_collection_partial = 'ALL_TRN_TO_1_COLLECTION_PART'
# mongo_dr_address_collection='IDV_SRC_DR_ADDR_REC'


#######################################################################################################################
def main():
    global log, db_url, db_database, debug
    global df_idv_asso, df_street_addr, df_gurn, vsl_tbl_df, df_idv_relship, df_party_agrmnt
    global df_idv_email, df_idv_flag, df_idv_tax, df_idv_ctzn, df_individual_asso_trn, df_individual_asso_trn_1;
    try:
        spark = SparkSession.builder.config("spark.sql.warehouse.dir", "/tmp/hive-site.xml").getOrCreate()

        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + 'Started creating Embedded collection from all TRNs!!!! ');
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'INITIATED', 'Started creating Embedded collection from all TRNs')
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + 'SPARK SESSION INITIALIZED. STARTING PIPELINE!!!!')

        log = set_app_logger(spark)

        ## Prints: 0 - None; 1 - HDFS records fetched; 2 - Schema & count; 3 - sub_doc;
        ## 4 - intermediate data; 5 - main dataset
        # debug = [0]
        debug = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        # Parsing config file
        db_url, db_database = getMongoDBConfiguration()


        ###################################### Fetching LS Data  as Sources #####################################
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Accessing the input from Kafka_HDFS")
        record_from_kafka_hdfs = spark.read.json(path_kafka_hdfs)
        # print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Validating for invalid records")

        rec_cnt = record_from_kafka_hdfs.count();
        if (rec_cnt <= 0):
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to access input/no-records from Kafka_HDFS")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ABORT', 'No records to process, hence aborting')
            exit(1)
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Total number of record found: " + str(rec_cnt));

        ##################### Checking for previous run pending/failure data #####################
        all_records_df = storeDataTemporarily(spark, record_from_kafka_hdfs)
        all_records_df = all_records_df.drop("_id")
        if 1 in debug: all_records_df.show()

        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed reading valid data")
        handleStatus(spark, all_records_df)
        all_records_df = read_from_mongo(spark, ALL_TRN_TEMP_UPDATE_COLL)
        if 1 in debug: all_records_df.show()

        ##################### Fetching keys as a precaution from failure due to data unavailability  #####################
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching LS_layers keys");
        try:
            columndata_df = spark.read.json(columndata)
            if 1 in debug: columndata_df.show()
        except Py4JJavaError as ex:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to fetch LS_layers keys");
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'GeneralError',ex.java_exception.toString())
            exit(0)

        ##################################### Fetching VSL data #####################################
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching VSL Data from "+mongo_read_collection)
        try:
            vsl_tbl_df = read_from_mongo(spark, mongo_read_collection)
            vsl_cnt = vsl_tbl_df.count()
        except:
            vsl_tbl_df = getEmptyDataFrame(columndata, mongo_read_collection)
            vsl_cnt = vsl_tbl_df.count()

        if vsl_cnt == 0:
            print(str(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to fetch VSL Data from " + mongo_read_collection)
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Aborting the process")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ABORT', 'Unable to fetch VSL records, hence aborting')
        ##################################### End of fetching VSL data #####################################


        # ## Checking if there is any pending data/records from previous run
        # if all_trn_cnt > 1:
        #     print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Collection has previous run documents")
        #     print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing previous set of TRN's data")
        #     function_process_second_trn(spark, mongo_read_collection)
        #     write_to_mongo(df_individual_asso_trn_1, mongo_write_collection_full)
        #     print(str(datetime.now().strftime(
        #         '%Y-%m-%d %H:%M:%S')) + " :: Completed processing pending TRN's data : " + mongo_write_collection_full);
        # else:
        #     print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: There are no previous/pending data")

        ###################################### Fetching records from input ######################################

        ##################################### Fetching Kafka-HDFS data data #####################################
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating fetching records from Kafka_HDFS")
        read_ls_layer_data(spark, all_records_df, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching records from Kafka_HDFS")

        try:  ## Used across the function
            df_idv_asso = df_idv_asso.distinct()
            idv_asso_cnt = df_idv_asso.count()
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) +" :: "+ LS_MAP_IDV_ASSO + " - count : " + str(idv_asso_cnt))
        except: idv_asso_cnt = 0

        if idv_asso_cnt <= 0:
            print("\n" + str(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_ASSO : There are no records available, aborting the process")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ABORT', 'TRN - INDIVIDUAL_ASS0 - no records available')
            exit(0)

        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Process INDIVIDUAL_ASSO")

        df_idv_asso.show()

        ## Begin - Processing logic to generate TRN - INDIVIDUAL_ASSO
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating first set of TRN collection generation")
        function_process_idv_trn(spark)
        ## Writing generated TRN's to MongoDB (INDIVIDUAL_ASSO, EMAIL, PHONE, PASSPORT, CLASS, IDVNATID, IDVIDENT - 7:Nos)
        print("Writing generated TRN's data (IDV_ASSO, EML, PHN, PP, CLS, NATID, IDENT) to MongoDB: " + mongo_write_collection_partial)
        df_individual_asso_trn = df_individual_asso_trn.distinct()
        if 3 in debug: df_individual_asso_trn.show(2, truncate=False)
        overwrite_to_mongo(df_individual_asso_trn, mongo_write_collection_partial)

        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing next set of TRN's")
        function_process_second_trn(spark, mongo_read_collection)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed processing second set of TRN's")
        if 3 in debug :
            df_individual_asso_trn_1.show(2, truncate=False)
            df_individual_asso_trn_1.printSchema()

        write_to_mongo(df_individual_asso_trn_1, mongo_write_collection_full)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed processing all TRN's: "+ mongo_write_collection_full);

        ## Performing clean-up of temporary collection(s)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Performing Clean-up ...")
        dummy_data = spark.sql("""SELECT '' AS dummy""");
        overwrite_to_mongo(dummy_data, mongo_write_collection_partial);
        overwrite_to_mongo(dummy_data, mongo_temp_collection);
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Successfully completed the process")

    except Py4JJavaError as ex:
        writeApplicationStatusToFile(spark,path_kafka_hdfs, path_kafka_hdfs, 'ERROR')
        print("An error occurred: " + ex.java_exception.toString())

###################################################### UTILITIES ######################################################

def handleStatus(spark, df):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IN & UP/DL")
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: ########################################")
        df_in = df.filter(df['SRC_EVT_TYP_FLG'] == "IN").distinct()
        df_in.show()
        overwrite_to_mongo(df_in, ALL_TRN_TEMP_UPDATE_COLL)

        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing UP/DL records")
        df_up_dl = df.filter(df['SRC_EVT_TYP_FLG'] != "IN")
        df_up_dl.createOrReplaceTempView("ALL_RECORDS_UP_DL_VW")
        df_idv_asso_grp = spark.sql("""SELECT count(1) AS CNT, CUSTOMER_ID, SRC_SYS_ID, SRC_SYS_INST_ID, SRC_LS_MAP  
            FROM ALL_RECORDS_UP_DL_VW 
            GROUP BY CUSTOMER_ID, SRC_SYS_ID, SRC_SYS_INST_ID, SRC_LS_MAP""")
        df_idv_asso_grp.show()

        df_single_updl_asso = df_idv_asso_grp.filter(df_idv_asso_grp['CNT'] == 1)
        df_single_updl_asso.show()

        ##Extracting new UP/DL records with no IN records
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Extracting new UP/DL records with no IN records match")
        try:
            df_new_updl = df_single_updl_asso.join(df_in, on=['CUSTOMER_ID', 'SRC_SYS_ID', 'SRC_SYS_INST_ID', 'SRC_LS_MAP'], how='left_anti')
            cnt = df_new_updl.count()
            df_new_updl.show()
            df_new_updl_full = df_up_dl.join(df_new_updl, on=['CUSTOMER_ID', 'SRC_SYS_ID', 'SRC_SYS_INST_ID', 'SRC_LS_MAP'], how='left_semi').drop('CNT')
            df_new_updl_full.show()
            write_to_mongo(df_new_updl_full, ALL_TRN_TEMP_UPDATE_COLL)
        except: print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: No Single UP/DL records")

        df_up_dl_rec = df_up_dl.join(df_new_updl, on=['CUSTOMER_ID', 'SRC_SYS_ID', 'SRC_SYS_INST_ID', 'SRC_LS_MAP'], how='left_anti')
        df_up_dl_rec.show()
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Identifying multiple records of UP/DL")
        df_up_dl_rec.createOrReplaceTempView("IDV_ASSO_MULTI_VW")
        df_multiple_ts = spark.sql("""SELECT A.*, ROW_NUMBER() OVER (ORDER BY A.SRC_LS_MAP,A.CUSTOMER_ID,A.SRC_SYS_ID,
            A.SRC_SYS_INST_ID,A.SRC_EXTRACT_TS DESC) as RK FROM IDV_ASSO_MULTI_VW A""")
        if 1 in debug: df_multiple_ts.show()

        df_up_dl_rec_1 = df_multiple_ts.filter(df_multiple_ts['RK'] == 1)
        df_up_dl_rec_1.show()

        df_up_dl_rec_1.createOrReplaceTempView("DF_UP_DL_1")
        cnt_1 = df_up_dl_rec_1.count()

        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing pipeline")
        ## ASSO_PIPELINE
        # pipeline = "[{ $project: {_id: 1, CUSTOMER_ID: 1, SRC_SYS_ID: 1, SRC_SYS_INST_ID: 1, SRC_EVT_TYP_FLG: 1, " \
        #    "SRC_EXTRACT_TS: 1, CUSTOMER_TITLE: 1, CUSTOMER_SURNAME: 1, CUSTOMER_FIRST_NAM: 1, CUSTOMER_FORENAM_1: 1, " \
        #    "CUSTOMER_FORENAM_2: 1, CUSTOMER_FORENAM_3: 1, CUSTOMER_NAME_SFX: 1, CUSTOMER_GENDER: 1, CUSTOMER_DOB: 1, " \
        #    "DEPEND_CHILD_CNT: 1, BANK_ID: 1, CUSTOMER_OWNED_BY: 1, CTRY_OF_RESIDENCE: 1, CTRY_OF_BIRTH: 1, " \
        #    "PLACE_OF_BIRTH: 1, TAX_NUMBER: 1, GRADUATION_DATE: 1, DECEASED_DATE: 1, FIRST_CONTACT_DATE: 1, " \
        #    "BUSINESS_PHONE_NO: 1, HOME_PHONE_NO: 1, MOBILE_PHONE_NO: 1, LAST_CHNG_DATE: 1, PASSPORT_CTRY: 1, " \
        #    "PASSPORT_EXP_DATE: 1, PASSPORT_NO: 1, HOME_OWNER_IND: 1, DISABILITY_CODE: 1, CUSTOMER_STATUS: 1, " \
        #    "SEGMENT_CODE: 1, OCCUPATION_CODE: 1, NATIONAL_INS_NO: 1, CTRY_OF_NATIONALTY:1}}," \
        #    "{'$match': {'CUSTOMER_ID':{$in: " + str([int(i.CUSTOMER_ID) for i in df_up_dl_rec_1.select('CUSTOMER_ID').
        #                                             collect()]) + "}}}]"

        pipeline = "[{ $project: {_id: 1, ACCESS_HOLDER_IND: 1, ACCES_GOLD_HLD_IND: 1, ALL_PROD_MAIL_IND: 1, " \
           "AMEX_GOLD_HOLD_IND: 1, AMEX_GREEN_HLD_IND: 1, BANK_ID: 1, BUSINESS_PHONE_NO: 1, B_SOC_CC_ACC_IND: 1, " \
           "B_SOC_MORTGAGE_IND: 1, B_SOC_SAVINGS_IND: 1, CAR_OWNER_IND: 1, CHANNEL_ATM_IND: 1, CHANNEL_BRANCH_IND: 1, " \
           "CHANNEL_INET_IND: 1, CHANNEL_PHONE_IND: 1, CONTACT_PERIOD_CD: 1, CREDIT_GRADE: 1, CREDIT_RESP_IND: 1, " \
           "CRED_CARD_MAIL_IND: 1, CRT_ISSUING_CTRY: 1, CTRY_OF_BIRTH: 1, CTRY_OF_NATIONALTY: 1, CTRY_OF_RESIDENCE: 1, " \
           "CUSTOMER_ADDRESSEE: 1, CUSTOMER_ADDR_NO: 1, CUSTOMER_DOB: 1, CUSTOMER_FIRST_NAM: 1, CUSTOMER_FORENAM_1: 1, " \
           "CUSTOMER_FORENAM_2: 1, CUSTOMER_FORENAM_3: 1, CUSTOMER_GENDER: 1, CUSTOMER_ID: 1, CUSTOMER_NAME_KEY: 1, " \
           "CUSTOMER_NAME_SFX: 1, CUSTOMER_OWNED_BY: 1, CUSTOMER_SALUTATN: 1, CUSTOMER_SRC_TYPE: 1, CUSTOMER_STATUS: 1, " \
           "CUSTOMER_SURNAME: 1, CUSTOMER_TITLE: 1, CUSTOMER_TYPE: 1, CUST_PROGRESS_IND: 1, DECEASED_DATE: 1, " \
           "DEPEND_CHILD_CNT: 1, DINERS_HOLDER_IND: 1, DISABILITY_CODE: 1, DISABILITY_DESC: 1, DPA_CONSENT_MARKER: 1, " \
           "EMAIL_ADDRESS: 1, EMAIL_IND: 1, FINPIN_CLASS_10: 1, FINPIN_CLASS_4: 1, FINPIN_CLASS_40: 1, " \
           "FIRST_CONTACT_DATE: 1, GRADUATION_DATE: 1, HIGH_NET_WORTH_IND: 1, HIGH_VAL_PROD_MAIL: 1, HOME_OWNER_IND: 1, " \
           "HOME_PHONE_NO: 1, INSURANCE_PROD_IND: 1, JOB_DESCPTN_CODE: 1, LAST_CHNG_DATE: 1, LAST_CONT_SAT_RATE: 1, " \
           "MOBILE_PHONE_NO: 1, MORT_EQU_REL_MAIL: 1, NATIONAL_INS_NO: 1, NON_RBS_ACCESS_IND: 1, NON_RBS_AX_GLD_IND: 1, " \
           "NON_RBS_MASTER_IND: 1, NON_RBS_VISA_IND: 1, NO_MAIL_IND: 1, OCCUPATION_CODE: 1, OCCUPATION_DESC: 1, " \
           "OTHER_BANK_SAV_IND: 1, OTHER_BNK_MORT_IND: 1, OTHER_CARD_HLD_IND: 1, OTHER_GOLD_IND: 1, O_BANK_CC_ACC_IND: 1, " \
           "PASSPORT_CTRY: 1, PASSPORT_EXP_DATE: 1, PASSPORT_NO: 1, PERSONAL_UPT_COUNT: 1, PER_CRED_PROD_IND: 1, " \
           "PHONE_IND: 1, PLACE_OF_BIRTH: 1, POTENTIAL_PROVISN: 1, PPS_TAX_CHARITY_NO: 1, PREF_PHONE_NO_IND: 1, " \
           "PREV_BUS_PHONE_NO: 1, PREV_HOME_PHONE_NO: 1, PREV_MOB_PHONE_NO: 1, PRIVATE_BANK_IND: 1, PROMPT_IND: 1, " \
           "RBS_ACCESS_IND: 1, RBS_AMEX_GOLD_IND: 1, RBS_MASTERCARD_IND: 1, RBS_MAST_GOLD_IND: 1, RBS_VISA_GOLD_IND: 1, " \
           "RBS_VISA_IND: 1, RISK_RATING: 1, RM_FORENAME: 1, RM_SURNAME: 1, RT_IND: 1, SAVS_PROD_MAIL_IND: 1, " \
           "SECURITY_ID: 1, SECURITY_ID_PROMPT: 1, SEGMENT_CODE: 1, SHADOW_LIMIT: 1, SHADOW_LIMIT_IND: 1, " \
           "SHAREHOLDER_IND: 1, SMOKING_INDICATOR: 1, SMS_IND: 1, SPECIAL_CREDT_IND: 1, SRC_EVT_TYP_FLG: 1, " \
           "SRC_EXTRACT_TS: 1, SRC_LS_MAP: 1, SRC_OF_INCOME: 1, SRC_SYS_ID: 1, SRC_SYS_INST_ID: 1, STYLE_HOLDER_IND: 1, " \
           "TAX_NUMBER: 1, TRAVEL_PROD_MAIL: 1, UNIT_TRUST_HLD_IND: 1, VERIFICATN_STATUS: 1, VISA_HOLDER_IND: 1, " \
           "WAIVE_EXPIRY_DATE: 1, WAIVE_REASON_CODE: 1}}," \
           "{'$match': {'CUSTOMER_ID':{$in: " + str([int(i.CUSTOMER_ID) for i in df_up_dl_rec_1.select('CUSTOMER_ID').
                                                    collect()]) + "}}}]"

        if 1 in debug: print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Pipeline - "+pipeline)
        try:
            match_ref_df = read_from_mongo_pipeline(spark, ALL_TRN_TEMP_UPDATE_COLL, pipeline)
            match_cnt = match_ref_df.count()
            if 1 in debug :  match_ref_df.show()
        except:
            match_cnt = 0
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process pipeline")

        if match_cnt > 0:
            match_ref_df.createOrReplaceTempView("EXISTING_DATA_VW")

            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Final latest data")
            df_latest_data = spark.sql("""SELECT A.* FROM DF_UP_DL_1 A LEFT JOIN EXISTING_DATA_VW B 
            ON A.SRC_LS_MAP == B.SRC_LS_MAP AND A.CUSTOMER_ID == B.CUSTOMER_ID
            AND A.SRC_SYS_ID == B.SRC_SYS_ID AND A.SRC_SYS_INST_ID == B.SRC_SYS_INST_ID """)
            if 1 in debug: df_latest_data.show()
            try:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Writing latest updated data to "+ALL_TRN_TEMP_UPDATE_COLL)
                latest_cnt = df_latest_data.count()
                write_to_mongo(df_latest_data, ALL_TRN_TEMP_UPDATE_COLL)
            except:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: There is no latest updated data")
    except: print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Failed to generate unique data")

def storeDataTemporarily(spark, record_from_kafka_hdfs):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Checking for previous pending data")
        read_data_df = read_from_mongo(spark, mongo_temp_collection)
        if 1 in debug: read_data_df.show(2)
        read_data_cnt = read_data_df.count()
        if read_data_cnt > 1:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: There is previous run data...")
            if 3 in debug : record_from_kafka_hdfs.show(2, truncate=False)
            try:
                record_from_kafka_hdfs = record_from_kafka_hdfs.where(col("SRC_LS_MAP").isNotNull())
                write_to_mongo(record_from_kafka_hdfs, mongo_temp_collection)
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Written latest data to MongoDB")
            except: print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: There isn't new valid data, checking for previous run data")
            try:
                full_data_df = read_from_mongo(spark, mongo_temp_collection)
                count = full_data_df.count()
                full_data_df = full_data_df.drop("_id")
                if 3 in debug : full_data_df.show(2, truncate=False)
                return full_data_df
            except:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: There is no previous data, Aborting process")
                exit(1)
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: There is no previous data pending, proceeding...")
            write_to_mongo(record_from_kafka_hdfs, mongo_temp_collection)
            return record_from_kafka_hdfs
    except:
        print(
            str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: This is first run and there isn't pending data.")
        write_to_mongo(record_from_kafka_hdfs, mongo_temp_collection)
        return record_from_kafka_hdfs

## Read LS records from input
def read_ls_layer_data(spark, record_from_kafka_hdfs, columndata_df):
    # try:
        global df_idv_asso, df_street_addr, df_gurn, vsl_tbl_df, df_idv_relship
        global df_idv_email, df_idv_flag, df_idv_tax, df_idv_ctzn, df_party_agrmnt

        ############ LS_CDB_INDIVIDUAL ############
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching IDV_ASSO records - "+LS_MAP_IDV_ASSO)
        df_idv_asso = read_individual_asso_records(spark, record_from_kafka_hdfs, LS_MAP_IDV_ASSO, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching INDIVIDUAL_ASSO records")

        ## Read LS_CDB_STREET_ADDRESS records ##
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing LS_CDB_STREET_ADDRESS")
        df_street_addr = read_street_addr_records(spark, record_from_kafka_hdfs, LS_MAP_STREET_ADDR, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching LS_CDB_STREET_ADDRESS records")

        ## Read LS_CDB_GURN records ##
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing LS_CDB_GURN")
        df_gurn = read_gurn_records(spark, record_from_kafka_hdfs, LS_MAP_GURN, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching LS_CDB_GURN records")

        ## Read RELATIONSHIP records ##
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_RELATIONSHIP")
        df_idv_relship = read_ind_relationship(spark, record_from_kafka_hdfs, LS_MAP_IND_RLSHP, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching RELATIONSHIP records")

        ## Read EMAIL (Electronic Address) records ##
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDVEMAIL")
        df_idv_email = read_idv_email(spark, record_from_kafka_hdfs, LS_MAP_EMAIL_ADDR, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching IDVEMAIL records")

        ## Read IDV_FLAG (Indicators) records ##
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_FLAG")
        df_idv_flag = read_idvFlag(spark, record_from_kafka_hdfs, LS_MAP_IDV_FLAG, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching IDV_FLAG records")

        ## Read IDV_TAX (Party_Tax_Residence) records ##
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_TAX")
        df_idv_tax = read_idvTax(spark, record_from_kafka_hdfs, LS_MAP_IDV_TAX, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching IDV_TAX records")

        ## Read IDV_CTZN (Additional_Citizionship) records ##
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CTZN")
        df_idv_ctzn = read_idvCtzn(spark, record_from_kafka_hdfs, LS_MAP_IDV_CTZN, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching IDV_CTZN records")

        ## Read Party_Agreement (Addres & Citizenship) records ##
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching IDV_Party_Agreement records")
        df_party_agrmnt = read_idvpartyAgrmnt(spark, record_from_kafka_hdfs, LS_MAP_IDV_PARTY_AGMT, columndata_df)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed fetching IDV_SRC records")
    # except: print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Reading LS_Layer data failed")

# #Idv_asso, Email, Phone Passport, Class, IdvNatId, Idvident
def function_process_idv_trn(spark):
    # try:
    global df_idv_asso, df_street_addr, df_gurn, vsl_tbl_df, df_idv_relship
    global df_idv_email, df_individual_asso_trn, df_individual_asso_trn_1;

    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating TRN process - IDV_ASSO, Email, Phone, Passport & Class")
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Created spark session")

    ## Begin of IDV_ASSO
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiated generating INDIVIDUAL_ASSO (trn_C_LD_BBBB_CCC_IDV) records")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'INITIATED', 'TRN - INDIVIDUAL_ASS0')

    df_idv_asso.createOrReplaceTempView("LS_CDB_INDIVIDUAL_TBL")  # LS_CDB_INDIVIDUAL_TBL
    vsl_tbl_df.createOrReplaceTempView("VSL_TBL")

    df_street_addr.createOrReplaceTempView("LS_CDB_STREET_ADDR")
    df_gurn.createOrReplaceTempView("LS_CDB_GURN")
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Creating LS_CDB_PARTY_RELATIONSHIP table")
    df_idv_relship_cnt = df_idv_relship.count()
    df_idv_relship.createOrReplaceTempView("LS_CDB_PARTY_RELATIONSHIP")

    idv_asso_cnt = df_idv_asso.count()
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: IDV_ASSO count: " + str(idv_asso_cnt));
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: STREET_ADDR count: " + str(df_street_addr.count()));
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: GURN count: " + str(df_gurn.count()));
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: LS_CDB_PARTY_RELATIONSHIP count - " + str(df_idv_relship_cnt))

    print("\n" + str(
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_ASSO :trn_c_ld_individual_df - Step 1 of 4")
    try:
        trn_c_ld_individual_df = spark.sql("""SELECT DISTINCT 
           LTRIM(RTRIM(TAX_NUMBER)) AS TAX_NUMBER,             LTRIM(RTRIM(PLACE_OF_BIRTH)) AS PLACE_OF_BIRTH,
           LTRIM(RTRIM(CTRY_OF_BIRTH)) AS CTRY_OF_BIRTH,     CTRY_OF_RESIDENCE,    GRADUATION_DATE, CUSTOMER_GENDER,
           LS_CDB_INDIVIDUAL_TBL.SRC_EXTRACT_TS AS CRT_DT,       DECEASED_DATE,        FIRST_CONTACT_DATE,
           DEPEND_CHILD_CNT,       CUSTOMER_DOB,             LTRIM(RTRIM(CUSTOMER_NAME_SFX)) AS CUSTOMER_NAME_SFX,
           LTRIM(RTRIM(CUSTOMER_FORENAM_3)) AS MIDDLE_NM_3,  LTRIM(RTRIM(CUSTOMER_FORENAM_2)) AS MIDDLE_NM_2,
           LTRIM(RTRIM(CUSTOMER_FORENAM_1)) AS MIDDLE_NM_1,  LTRIM(RTRIM(CUSTOMER_FIRST_NAM)) AS CUSTOMER_FIRST_NAM,
           LTRIM(RTRIM(CUSTOMER_SURNAME)) AS CUSTOMER_SURNAME, LTRIM(RTRIM(CUSTOMER_TITLE)) AS SALUTATION_NM,
           LS_CDB_INDIVIDUAL_TBL.CUSTOMER_ID,                    LS_CDB_INDIVIDUAL_TBL.CUSTOMER_OWNED_BY, 
           LTRIM(RTRIM(concat(BANK_ID,CUSTOMER_OWNED_BY))) AS BANK_ID, LTRIM(RTRIM(LS_CDB_GURN.GURN)) AS GURN,
           LTRIM(RTRIM(concat_ws(' ', IF(ISNULL(CUSTOMER_TITLE),'',LTRIM(RTRIM(CUSTOMER_TITLE))),
           IF(ISNULL(CUSTOMER_FIRST_NAM),'', LTRIM(RTRIM(CUSTOMER_FIRST_NAM))),
           IF(ISNULL(CUSTOMER_FORENAM_1),'', LTRIM(RTRIM(CUSTOMER_FORENAM_1))),
           IF(ISNULL(CUSTOMER_FORENAM_2),'', LTRIM(RTRIM(CUSTOMER_FORENAM_2))),
           IF(ISNULL(CUSTOMER_FORENAM_3),'', LTRIM(RTRIM(CUSTOMER_FORENAM_3))),
           IF(ISNULL(CUSTOMER_SURNAME),'', LTRIM(RTRIM(CUSTOMER_SURNAME))),
           IF(ISNULL(CUSTOMER_NAME_SFX),'', LTRIM(RTRIM(CUSTOMER_NAME_SFX)))))) AS FULL_NM,
           -1 AS CNTRY_OF_RISK_ID,         -1 AS LEGAL_CAPACITY_TP_ID,         NULL AS ALIAS_NM,
           NULL AS FORMER_SURNM_NM,        NULL AS SRC_ROWID,                  NULL AS LAST_UPDATE_DATE,
           NULL AS RBS_EMP_ID,             NULL AS NI_NUM_VAL,                 NULL AS CITY_OF_RES_ID,
           NULL AS RES_ADDR_ST_DT,         NULL AS RES_ADDR_END_DT,            NULL AS MAIDEN_NM,
           NULL AS ROW_ACTION,             NULL AS DATA_SRC_ID,                LS_CDB_INDIVIDUAL_TBL.SRC_EVT_TYP_FLG,
           LS_CDB_INDIVIDUAL_TBL.SRC_EXTRACT_TS,   LS_CDB_INDIVIDUAL_TBL.SRC_SYS_ID,   LS_CDB_INDIVIDUAL_TBL.SRC_SYS_INST_ID
           FROM LS_CDB_INDIVIDUAL_TBL 
           LEFT OUTER JOIN LS_CDB_STREET_ADDR SA on LS_CDB_INDIVIDUAL_TBL.CUSTOMER_ID=SA.CUSTOMER_ID
           LEFT OUTER JOIN LS_CDB_GURN on LS_CDB_INDIVIDUAL_TBL.CUSTOMER_ID=LS_CDB_GURN.CUSTOMER_ID""")
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated - trn_c_ld_individual_df")
        if 1 in debug: trn_c_ld_individual_df.show(5, truncate=False)
        trn_c_ld_individual_df.createOrReplaceTempView("trn_c_ld_individual")
        trn_c_ld_individual_cnt = trn_c_ld_individual_df.count()

        if trn_c_ld_individual_cnt > 0:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_ASSO : df_idv_clean - Step 2 of 4")
            # df_idv_clean: #SA.POSTAL_ADDR_ID, SA.RES_ADDR_ID
            df_idv_clean = spark.sql("""SELECT TAX_NUMBER, PLACE_OF_BIRTH, CTRY_OF_BIRTH, CTRY_OF_RESIDENCE, GRADUATION_DATE, CRT_DT,
                    DECEASED_DATE, FIRST_CONTACT_DATE, DEPEND_CHILD_CNT, CUSTOMER_DOB, CUSTOMER_GENDER, CUSTOMER_NAME_SFX, ALIAS_NM,
                    MIDDLE_NM_3, MIDDLE_NM_2, MIDDLE_NM_1, CUSTOMER_FIRST_NAM, CUSTOMER_SURNAME, FORMER_SURNM_NM, SALUTATION_NM,
                    CUSTOMER_ID, FULL_NM, GURN, SRC_SYS_INST_ID, SRC_SYS_ID, BANK_ID, SRC_ROWID, LAST_UPDATE_DATE, RBS_EMP_ID,
                    NI_NUM_VAL, CNTRY_OF_RISK_ID, LEGAL_CAPACITY_TP_ID, CITY_OF_RES_ID, RES_ADDR_ST_DT, RES_ADDR_END_DT, MAIDEN_NM,
                    ROW_ACTION, DATA_SRC_ID, IF(CTRY_OF_BIRTH IS NULL, PLACE_OF_BIRTH, CTRY_OF_BIRTH) AS VSL_CNTRY_OF_BIRTH_INP,
                    IF(PLACE_OF_BIRTH IS NULL, CTRY_OF_BIRTH, PLACE_OF_BIRTH) AS VSL_PLACE_OF_BIRTH_INP, SRC_EVT_TYP_FLG, CUSTOMER_OWNED_BY
                    FROM trn_c_ld_individual""")
            if 1 in debug: df_idv_clean.show(2, truncate=False)
            df_idv_clean.createOrReplaceTempView("df_idv_clean")

            # Not being used in the logic, hence commented.
            # print("Processing - df_idv_clean_vsl")
            # df_idv_clean_vsl = spark.sql("""SELECT TAX_NUMBER, PLACE_OF_BIRTH, CTRY_OF_BIRTH, CTRY_OF_RESIDENCE, GRADUATION_DATE, CRT_DT,
            #                 DECEASED_DATE, FIRST_CONTACT_DATE, DEPEND_CHILD_CNT, CUSTOMER_DOB, CUSTOMER_GENDER, CUSTOMER_NAME_SFX, ALIAS_NM,
            #                 MIDDLE_NM_3, MIDDLE_NM_2, MIDDLE_NM_1, CUSTOMER_FIRST_NAM, CUSTOMER_SURNAME, FORMER_SURNM_NM, SALUTATION_NM,
            #                 CUSTOMER_ID, FULL_NM, GURN, SRC_SYS_INST_ID, SRC_SYS_ID, BANK_ID, SRC_ROWID, LAST_UPDATE_DATE, RBS_EMP_ID,
            #                 NI_NUM_VAL, CNTRY_OF_RISK_ID, LEGAL_CAPACITY_TP_ID, CITY_OF_RES_ID, RES_ADDR_ST_DT, RES_ADDR_END_DT, MAIDEN_NM,
            #                 ROW_ACTION, DATA_SRC_ID,VSL_CNTRY_OF_BIRTH_INP, VSL_PLACE_OF_BIRTH_INP
            #                 FROM df_idv_clean idv
            #                 LEFT OUTER JOIN VSL_TBL vsl1
            #                 ON vsl1.VALUE_SET_NAME='RD COUNTRY CODES' AND vsl1.COLUMN_1_STRING_VALUE=idv.VSL_CNTRY_OF_BIRTH_INP
            #                 AND vsl1.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')
            #                 LEFT OUTER JOIN VSL_TBL vsl2 ON vsl1.VALUE_SET_NAME='RD GENDER'
            #                 AND vsl2.COLUMN_1_STRING_VALUE=idv.CUSTOMER_GENDER AND vsl2.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')
            #                 LEFT OUTER JOIN VSL_TBL vsl3 ON vsl1.VALUE_SET_NAME='RD SRC SYSTEMS'
            #                 AND vsl3.COLUMN_1_STRING_VALUE=idv.SRC_SYS_ID AND vsl3.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')
            #                 LEFT OUTER JOIN VSL_TBL vsl4 ON vsl1.VALUE_SET_NAME='RD BRAND NAMES'
            #                 AND vsl4.COLUMN_1_STRING_VALUE=idv.BANK_ID AND vsl4.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')
            #                 LEFT OUTER JOIN VSL_TBL vsl5 ON vsl1.VALUE_SET_NAME='RD COUNTRY CODES'
            #                 AND vsl5.COLUMN_1_STRING_VALUE=idv.CTRY_OF_RESIDENCE AND vsl5.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')
            #                 LEFT OUTER JOIN VSL_TBL vsl6 ON vsl1.VALUE_SET_NAME='RD COUNTRY CODES'
            #                 AND vsl6.COLUMN_1_STRING_VALUE=idv.VSL_PLACE_OF_BIRTH_INP
            #                 AND vsl6.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')""")
            # if 1 in debug: df_idv_clean_vsl.show(2, truncate=False)

            # df_relation:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_ASSO : df_relation - Step 3 of 4")
            df_relation = spark.sql("""SELECT DISTINCT A.REL_TYPE FROM 
                (
                    SELECT PR.CUSTOMER_ID_1, PR.REL_TYPE, PR.CUSTOMER_ID_2, PR.DATE_CREATED AS CRT_DT
                    FROM LS_CDB_PARTY_RELATIONSHIP PR INNER JOIN LS_CDB_INDIVIDUAL_TBL I ON PR.CUSTOMER_ID_2 = I.CUSTOMER_ID
                    WHERE PR.REL_TYPE IN ('SB','ZB','YB','WB','FB','TB','PB','XB','BB','DB','CB')
                    UNION
                    SELECT Pr.CUSTOMER_ID_2 AS CUSTOMER_ID_1, PR.REL_TYPE, PR.CUSTOMER_ID_1 AS CUSTOMER_ID_2, 
                    PR.DATE_CREATED AS CRT_DT FROM LS_CDB_PARTY_RELATIONSHIP PR INNER JOIN LS_CDB_INDIVIDUAL_TBL I 
                    ON PR.CUSTOMER_ID_2 = I.CUSTOMER_ID WHERE PR.REL_TYPE IN ('OO')
                ) A LEFT OUTER JOIN VSL_TBL vsl on vsl.VALUE_SET_NAME='RD CUST_REL_TYPES' 
                AND vsl.COLUMN_1_STRING_VALUE=A.REL_TYPE""")
            if 1 in debug: df_relation.show(5, truncate=False)
            df_relation.createOrReplaceTempView("DF_RELATION")

            # df_individual_asso_trn:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_ASSO : df_idv_final - Step 4 of 4")
            df_individual_asso_trn = spark.sql("""SELECT DISTINCT TAX_NUMBER, PLACE_OF_BIRTH, CTRY_OF_BIRTH, CTRY_OF_RESIDENCE,
                GRADUATION_DATE, CRT_DT, DECEASED_DATE, FIRST_CONTACT_DATE, DEPEND_CHILD_CNT, CUSTOMER_DOB, CUSTOMER_GENDER,
                CUSTOMER_NAME_SFX, ALIAS_NM, MIDDLE_NM_3, MIDDLE_NM_2, MIDDLE_NM_1, CUSTOMER_FIRST_NAM, CUSTOMER_SURNAME,
                FORMER_SURNM_NM, SALUTATION_NM, CUSTOMER_ID, GURN, SRC_SYS_INST_ID, SRC_SYS_ID, BANK_ID, SRC_ROWID,
                LAST_UPDATE_DATE, RBS_EMP_ID, NI_NUM_VAL, CNTRY_OF_RISK_ID, LEGAL_CAPACITY_TP_ID, CITY_OF_RES_ID, FULL_NM,
                RES_ADDR_ST_DT, RES_ADDR_END_DT, MAIDEN_NM, ROW_ACTION, DATA_SRC_ID, SRC_EVT_TYP_FLG, CUSTOMER_OWNED_BY,
                (SELECT REL_TYPE FROM DF_RELATION) as RL_TP_ID        
                FROM df_idv_clean ORDER BY CRT_DT DESC""")
            cnt = df_individual_asso_trn.count()
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: IDV_ASSO record count: "+ str(cnt))
            if 5 in debug: df_individual_asso_trn.show(5, truncate=False);
            df_individual_asso_trn.createOrReplaceTempView("INDIVIDUAL_ASSO_TRN_VW");

            if df_individual_asso_trn.count() <= 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records from TRN- INDIVIDUAL_ASSO")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NoRecords', 'Generated 0 TRN - IDV_ASSO records')
                exit(1)
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Found 0 records to process TRN - INDIVIDUAL_ASSO, aborting process")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'FAILURE','Found 0 records to process TRN - INDIVIDUAL_ASSO, aborting process')
            df_individual_asso_trn=df_idv_asso
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'INDIVIDUAL_ASSO - SUCCESS', 'Generated TRN records for INDIVIDUAL_ASSO');
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable to generate TRN - INDIVIDUAL_ASSO")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'No_Data', 'Record unavailable INDIVIDUAL_ASSO')
        df_individual_asso_trn = df_idv_asso
    ##END of IDV_ASSO
    ## ------------------------------------------------------------------------------------------------------- ##


    ##################### Begin of Email #####################
    # Creating table on top of email records
    # df_idvemail_start:
    print("\n Initiated generating ELECTRONIC_ADDRESS (C_LD_BBBB_CCC_IDVEMAIL) records")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating IDV_EMAIL');
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing ELECTRONIC_ADDRESS : df_idvemail_start - step 1 of 4")
    try:
        idv_email_cnt = df_idv_email.count()
        if idv_email_cnt > 0:
            df_idv_email.createOrReplaceTempView("LS_CDB_ELECTRONIC_ADDRESS")
            df_idvemail_start = spark.sql("""SELECT EA.CUSTOMER_ID, E_ADDR_TYPE, UPDATE_DATE_TIME, TRIM(E_ADDR) AS EMAIL_ID_VAL,
                    EA.SRC_SYS_ID, EA.SRC_SYS_INST_ID, TRIM(concat(ID.BANK_ID, ID.CUSTOMER_OWNED_BY)) as BANK_ID, 'CDB' AS CDB,
                    'IND EMAIL' AS IND_EMAIL, EA.SRC_EXTRACT_TS, EA.SRC_EVT_TYP_FLG FROM LS_CDB_ELECTRONIC_ADDRESS EA 
                    INNER JOIN LS_CDB_INDIVIDUAL_TBL ID
                    ON EA.CUSTOMER_ID=ID.CUSTOMER_ID AND EA.SRC_SYS_INST_ID=ID.SRC_SYS_INST_ID""").distinct()
            # log.info(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing - df_idvemail_start")
            if 1 in debug: df_idvemail_start.show(2, truncate=False)
            df_idvemail_start.createOrReplaceTempView("df_idvemail_start")
            df_idvemail_cnt = df_idvemail_start.count()

            if df_idvemail_cnt > 0:
                # try:
                # df_indemail_final
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing ELECTRONIC_ADDRESS : df_indemail_final - step 2 of 4")
                df_indemail_final = spark.sql("""
                    SELECT DISTINCT IDV.CUSTOMER_ID, IDV.E_ADDR_TYPE, IDV.UPDATE_DATE_TIME, IDV.EMAIL_ID_VAL, IDV.SRC_SYS_ID,
                    IDV.SRC_SYS_INST_ID, IDV.BANK_ID, vsl2.VSL_LKP_VALUE as DATA_SRC_ID, vsl2.VSL_LKP_VALUE as SRC_BRND_ID,
                    NULL as SRC_ROWID, NULL as LAST_UPDATE_DATE, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG 
                    FROM df_idvemail_start IDV 
                    LEFT OUTER JOIN VSL_TBL vsl2 ON vsl2.COLUMN_1_STRING_VALUE=IDV.BANK_ID AND vsl2.VALUE_SET_NAME='RD BRAND NAMES' AND vsl2.OUT_ACTION_CODE NOT IN ('STOP','BLOCK') 
                    LEFT OUTER JOIN VSL_TBL vsl1 ON vsl1.VALUE_SET_NAME='RD SRC SYSTEMS' AND vsl1.COLUMN_1_STRING_VALUE=IDV.CDB AND vsl1.OUT_ACTION_CODE NOT IN ('STOP','BLOCK') 
                    LEFT OUTER JOIN VSL_TBL vsl3 ON vsl3.VALUE_SET_NAME='RD EMAIL TYPES' AND vsl3.COLUMN_1_STRING_VALUE=IDV.IND_EMAIL AND vsl3.OUT_ACTION_CODE NOT IN ('STOP','BLOCK') 
                    """)
                log.info(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing ELECTRONIC_ADDRESS - df_indemail_final");
                if 4 in debug: df_indemail_final.show(2, truncate=False);

                if df_indemail_final.count() > 0:
                    df_indemail_final.createOrReplaceTempView("df_indemail_final")
                    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing ELECTRONIC_ADDRESS : sub_document - step 3 of 4")
                    df_indemail_sub_doc_1 = spark.sql("""SELECT DISTINCT EML.CUSTOMER_ID, EML.SRC_SYS_INST_ID, SRC_SYS_ID, 
                        STRUCT(EML.E_ADDR_TYPE AS EMAIL_TP_ID, EML.DATA_SRC_ID, EML.SRC_BRND_ID, EML.EMAIL_ID_VAL, 
                        EML.SRC_EXTRACT_TS AS SRC_SYS_CHG_DT, EML.SRC_EVT_TYP_FLG) EMAIL FROM df_indemail_final EML 
                        ORDER BY SRC_EXTRACT_TS DESC""")
                    if 4 in debug: df_indemail_sub_doc_1.show(2, truncate=False)
                    df_indemail_sub_doc_1.createOrReplaceTempView("df_indemail_sub_doc_1")

                    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing ELECTRONIC_ADDRESS : sub_document with array - step 4 of 4")
                    df_indemail_sub_doc = spark.sql("SELECT DISTINCT CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, COLLECT_SET(EMAIL) AS EMAIL "
                        "FROM df_indemail_sub_doc_1 WHERE CUSTOMER_ID IS NOT NULL "
                        "GROUP BY CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID").coalesce(1).distinct()
                    if 3 in debug: df_indemail_sub_doc.show(2, truncate=False)

                    # Joining Electronic_Address into Individual_ASSO
                    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Joining Electronic_Address with INDIVIDUAL_ASSO")
                    df_individual_asso_trn = df_individual_asso_trn.join(df_indemail_sub_doc,
                               on=['CUSTOMER_ID', 'SRC_SYS_INST_ID', 'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())

                    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Joined IDV_ASSO & Email data successfully")
                    if 5 in debug: df_individual_asso_trn.show(2, truncate=False)
                else:
                    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records for TRN - Email")
                    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS', 'Generated 0 (zero) TRN - IDV_EMAIL records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process - TRN: Email ")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_EMAIL has no/zero valid records');
    except:
        print(
            str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process - TRN:IDV_Email ")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_EMAIL has no/zero valid records');
    ##End of Email -------------------------------------------------------------------------------------------- ##


    ##################### Begin of IDV_Phone #####################
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating IDV_Phone")
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Phone - idvphone_start_df - Step 1 of 4")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating IDV_Phone');
    if idv_asso_cnt > 0:
        try:
            idvphone_start_df = spark.sql("""
                SELECT 'IND BUSINESS PHONE' AS PHONE_TYP_ID, BUSINESS_PHONE_NO AS PHONE_NUM, 
                CUSTOMER_ID AS SRC_IDV_ID_VAL, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID, to_timestamp(SRC_EXTRACT_TS, 
                "yyyy-MM-dd'T'HH:mm:ss") AS SRC_SYS_CHG_DT, concat(BANK_ID, CUSTOMER_OWNED_BY) AS BANK_ID FROM 
                LS_CDB_INDIVIDUAL_TBL
                UNION ALL
                SELECT 'IND DAYTIME PHONE' AS PHONE_TYP_ID, BUSINESS_PHONE_NO AS PHONE_NUM,
                CUSTOMER_ID AS SRC_IDV_ID_VAL, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID, to_timestamp(SRC_EXTRACT_TS,
                "yyyy-MM-dd'T'HH:mm:ss") AS SRC_SYS_CHG_DT, concat(BANK_ID, CUSTOMER_OWNED_BY) AS BANK_ID FROM
                LS_CDB_INDIVIDUAL_TBL
                UNION ALL
                SELECT 'IND EVENING PHONE' AS PHONE_TYP_ID, HOME_PHONE_NO AS PHONE_NUM,
                CUSTOMER_ID AS SRC_IDV_ID_VAL, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID, to_timestamp(SRC_EXTRACT_TS,
                "yyyy-MM-dd'T'HH:mm:ss") AS SRC_SYS_CHG_DT, concat(BANK_ID, CUSTOMER_OWNED_BY) AS BANK_ID FROM
                LS_CDB_INDIVIDUAL_TBL
                UNION ALL
                SELECT 'IND LANDLINE PHONE' AS PHONE_TYP_ID, HOME_PHONE_NO AS PHONE_NUM,
                CUSTOMER_ID AS SRC_IDV_ID_VAL, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID, to_timestamp(SRC_EXTRACT_TS,
                "yyyy-MM-dd'T'HH:mm:ss") AS SRC_SYS_CHG_DT, concat(BANK_ID, CUSTOMER_OWNED_BY) AS BANK_ID FROM
                LS_CDB_INDIVIDUAL_TBL
                UNION ALL
                SELECT 'IND MOBILE PHONE' AS PHONE_TYP_ID, MOBILE_PHONE_NO AS PHONE_NUM,
                CUSTOMER_ID AS SRC_IDV_ID_VAL, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID, to_timestamp(SRC_EXTRACT_TS,
                "yyyy-MM-dd'T'HH:mm:ss") AS SRC_SYS_CHG_DT, concat(BANK_ID, CUSTOMER_OWNED_BY) AS BANK_ID FROM
                LS_CDB_INDIVIDUAL_TBL""").distinct()
            if 1 in debug: idvphone_start_df.show(2, truncate=False)
            idvphone_start_df.createOrReplaceTempView("IDVPHONE_START_TBL")
            idvphone_start_cnt = idvphone_start_df.count()

            if idvphone_start_cnt > 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Phone - Combining UNIONs - Step 2 of 4")
                c_ld_bbbb_ccc_idvphone_df = spark.sql("""SELECT DISTINCT PHONE_TYP_ID, LTRIM(RTRIM(PHONE_NUM)) AS PHONE_NUM,
                    SRC_IDV_ID_VAL, CASE WHEN LTRIM(RTRIM(SRC_SYS_ID)) = '' OR ISNULL(SRC_SYS_ID) THEN 'Not Available' ELSE
                    SRC_SYS_ID END AS t_SRC_SYS_ID,  CASE WHEN LTRIM(RTRIM(PHONE_TYP_ID)) = '' OR ISNULL(PHONE_TYP_ID) THEN 'Not Available' ELSE
                    PHONE_TYP_ID END AS t_PHONE_TYP_ID, CASE WHEN LTRIM(RTRIM(BANK_ID)) = '' OR ISNULL(BANK_ID) THEN 'Not Available' ELSE
                    BANK_ID END AS t_BANK_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_SYS_CHG_DT, BANK_ID 
                    FROM IDVPHONE_START_TBL WHERE PHONE_NUM IS NOT NULL OR LTRIM(RTRIM(PHONE_NUM)) != ''""")
                if 1 in debug: c_ld_bbbb_ccc_idvphone_df.show(2, truncate=False)
                c_ld_bbbb_ccc_idvphone_df.createOrReplaceTempView('IDVPHONE_COMBINED_TBL')

                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Phone - c_ld_bbbb_ccc_idvphone_trn_df - Step 3 of 4")
                c_ld_bbbb_ccc_idvphone_trn_df = spark.sql("""SELECT DISTINCT idvphone.PHONE_NUM, idvphone.PHONE_TYP_ID,
                        idvphone.SRC_IDV_ID_VAL, idvphone.SRC_EVT_TYP_FLG, idvphone.SRC_SYS_INST_ID, idvphone.SRC_SYS_ID,
                        idvphone.SRC_SYS_CHG_DT, idvphone.BANK_ID, NULL as SRC_ROWID, NULL as LAST_UPDATE_DATE, 
                        vsl1.VSL_LKP_VALUE AS DATA_SRC_ID, idvphone.t_PHONE_TYP_ID, vsl2.VSL_LKP_VALUE AS PHONE_TP_ID,
                        idvphone.t_BANK_ID, vsl3.VSL_LKP_VALUE AS SRC_BRND_ID
                        FROM IDVPHONE_COMBINED_TBL idvphone
                        LEFT OUTER JOIN VSL_TBL vsl1 ON vsl1.VALUE_SET_NAME='RD SRC SYSTEMS' 
                        AND vsl1.COLUMN_1_STRING_VALUE=idvphone.t_SRC_SYS_ID 
                        AND vsl1.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')
                        LEFT OUTER JOIN VSL_TBL vsl2 ON vsl2.VALUE_SET_NAME='RD PHONE NUMBER TYPES' 
                        AND vsl2.COLUMN_1_STRING_VALUE=idvphone.t_PHONE_TYP_ID 
                        AND vsl2.OUT_ACTION_CODE NOT IN ('STOP', 'BLOCK')
                        LEFT OUTER JOIN VSL_TBL vsl3 ON vsl3.VALUE_SET_NAME='RD BRAND NAMES' 
                        AND vsl3.COLUMN_1_STRING_VALUE=idvphone.t_BANK_ID 
                        AND vsl3.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')""").distinct()
                if 1 in debug: c_ld_bbbb_ccc_idvphone_trn_df.show(2, truncate=False)
                c_ld_bbbb_ccc_idvphone_trn_df.createOrReplaceTempView("IDVPHONE_FINAL_VW")

                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Phone - df_idvPhone_sub_doc - Step 4 of 4")
                df_idvPhone_sub_doc = spark.sql("""SELECT DISTINCT SRC_IDV_ID_VAL AS CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, COLLECT_SET(IDV_PHONE) AS PHONE FROM 
                        (SELECT SRC_IDV_ID_VAL, SRC_SYS_INST_ID, SRC_SYS_ID, STRUCT(PHONE_TYP_ID, PHONE_NUM, DATA_SRC_ID, 
                        SRC_BRND_ID, SRC_SYS_CHG_DT, SRC_EVT_TYP_FLG) AS IDV_PHONE FROM IDVPHONE_FINAL_VW WHERE SRC_IDV_ID_VAL IS NOT NULL) A 
                        GROUP BY SRC_IDV_ID_VAL, SRC_SYS_INST_ID, SRC_SYS_ID ORDER BY SRC_SYS_CHG_DT DESC""").distinct()
                if 3 in debug:
                    df_idvPhone_sub_doc.printSchema()
                    df_idvPhone_sub_doc.show(2, truncate=False)

                if df_idvPhone_sub_doc.count() > 0:
                    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Joining the Phone with IDV_ASSO");
                    df_individual_asso_trn = df_individual_asso_trn.join(df_idvPhone_sub_doc,
                                            on=['CUSTOMER_ID','SRC_SYS_INST_ID','SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())
                    if 5 in debug: df_individual_asso_trn.show(2, truncate=False)
                else:
                    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records from TRN: IDV_PHONE")
                    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS', 'Generated 0 (zero) TRN - IDV_Phone records');
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process TRN: IDV_PHONE")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_Phone has no/zero valid records');
        except:
            print(str(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')) + " :: Unable to process - IDV_PHONE")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process - IDV_Phone');
    else:
        print(str(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process TRN: IDV_PHONE")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_Phone has no/zero valid records');
    ## End of IDV_Phone---------------------------------------------------------------------------------------- ##


    ##################### Begin of IDV_PASSPORT #####################
    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating IDV_PASSPORT")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating IDV_PASSPORT');
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Passport - step 1 of 3")
    try:
        if idv_asso_cnt > 0:
            c_ld_bbbb_ccc_passport_df = spark.sql("""SELECT DISTINCT cdb.LAST_CHNG_DATE, cdb.CUSTOMER_ID, CASE WHEN LTRIM(RTRIM(
                    cdb.PASSPORT_CTRY)) = '' OR ISNULL(cdb.PASSPORT_CTRY) THEN 'Not Available' ELSE cdb.PASSPORT_CTRY END AS 
                    t_PASSPORT_CTRY, cdb.PASSPORT_CTRY, cdb.PASSPORT_EXP_DATE, TRIM(cdb.PASSPORT_NO) AS PASSPORT_NO, cdb.SRC_SYS_INST_ID, 
                    cdb.SRC_SYS_ID, to_timestamp(cdb.SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS, cdb.SRC_EVT_TYP_FLG FROM 
                    LS_CDB_INDIVIDUAL_TBL cdb WHERE cdb.PASSPORT_NO IS NOT NULL or TRIM(cdb.PASSPORT_NO) !=''""")
            # passport_map_df.show(truncate=False)
            c_ld_bbbb_ccc_passport_df.createOrReplaceTempView("PASSPORT_MAP_TBL")
            c_ld_bbbb_ccc_passport_cnt = c_ld_bbbb_ccc_passport_df.count()
        else: c_ld_bbbb_ccc_passport_cnt=0

        if c_ld_bbbb_ccc_passport_cnt > 0:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Passport - step 2 of 3")
            c_ld_bbbb_ccc_passport_trn_df = spark.sql("""SELECT DISTINCT passport_map.LAST_CHNG_DATE as SRC_SYS_CHG_DT, 
                    passport_map.CUSTOMER_ID, passport_map.SRC_EVT_TYP_FLG, to_timestamp(passport_map.SRC_EXTRACT_TS, 
                    "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS, passport_map.PASSPORT_CTRY, passport_map.PASSPORT_EXP_DATE, 
                    passport_map.PASSPORT_NO, passport_map.SRC_SYS_INST_ID, passport_map.SRC_SYS_ID, vsl.VSL_LKP_VALUE as 
                    PPT_ISSUING_CNTRY_ID, NULL as LAST_UPDATE_DATE, passport_map.t_PASSPORT_CTRY, vsl.OUT_ACTION_CODE 
                    FROM PASSPORT_MAP_TBL passport_map 
                    LEFT OUTER JOIN VSL_TBL vsl ON vsl.VALUE_SET_NAME='RD COUNTRY CODES' 
                    AND vsl.COLUMN_1_STRING_VALUE=passport_map.t_PASSPORT_CTRY 
                    AND vsl.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')""");
            # c_ld_bbbb_ccc_passport_trn_df.show(2, truncate=False)
            c_ld_bbbb_ccc_passport_trn_df.createOrReplaceTempView("TRN_PASSPORT_VW");

            if c_ld_bbbb_ccc_passport_trn_df.count() > 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Passport - step 3 of 3");
                passport_sub_doc=spark.sql("""SELECT DISTINCT CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, 
                STRUCT(PASSPORT_NO AS PPT_NUM, PPT_ISSUING_CNTRY_ID, SRC_SYS_CHG_DT, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG) AS PASSPORT 
                FROM TRN_PASSPORT_VW WHERE CUSTOMER_ID IS NULL ORDER BY SRC_EXTRACT_TS DESC""").distinct()
                if 3 in debug: passport_sub_doc.show(2, truncate=False)
                # passport_sub_doc.printSchema()

                df_individual_asso_trn = df_individual_asso_trn.join(passport_sub_doc, on=['CUSTOMER_ID', 'SRC_SYS_INST_ID',
                                           'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Joined IDV_ASSO with Passport")
                if 5 in debug: df_individual_asso_trn.show(2, truncate=False)
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records for TRN: IDV_PASSPORT")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS','Generated 0 (zero) TRN - IDV_PASSPORT records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process TRN: IDV_PASSPORT")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_PASSPORT has no/zero valid records');
    except:
        print(str(datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S')) + " :: Unable to process - IDV_PASSPORT")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process - IDV_PASSPORT');
    ## End of IDV_PASSPORT--------------------------------------------------------------------------------- ##

    ##################### Begin of IDV_CLASS #####################
    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating - IDV_CLASS")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating IDV_CLASS');
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CLASS - step 1 of 3")
    if idv_asso_cnt > 0:
        try:
            idvclass_start_df = spark.sql("""SELECT 'RD HOME OWNERSHIP STATUS' AS CLASS_SET_NAME, HOME_OWNER_IND AS 
                    CLASS_VAL_ID, LAST_CHNG_DATE, CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID, to_timestamp(
                    SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS 
                    FROM LS_CDB_INDIVIDUAL_TBL
                    UNION ALL
                    SELECT 'RD DISABILITY' AS CLASS_SET_NAME, DISABILITY_CODE AS CLASS_VAL_ID, LAST_CHNG_DATE,
                    CUSTOMER_ID, SRC_SYS_INST_ID, SRC_EVT_TYP_FLG, SRC_SYS_ID,
                    to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS
                    FROM LS_CDB_INDIVIDUAL_TBL
                    UNION ALL
                    SELECT 'RD MARITAL STATUS' AS CLASS_SET_NAME, CUSTOMER_STATUS AS CLASS_VAL_ID, LAST_CHNG_DATE,
                    CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID,
                    to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS
                    FROM LS_CDB_INDIVIDUAL_TBL 
                    UNION ALL
                    SELECT 'RD INDIVIDUAL MARKET SEGMENT' AS CLASS_SET_NAME, SEGMENT_CODE AS CLASS_VAL_ID, LAST_CHNG_DATE,
                    CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID,
                    to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS 
                    FROM LS_CDB_INDIVIDUAL_TBL 
                    UNION ALL
                    SELECT 'RD INDIVIDUAL EMPLOYMENT STATUS' AS CLASS_SET_NAME, OCCUPATION_CODE AS CLASS_VAL_ID,
                    LAST_CHNG_DATE, CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID,
                    to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS
                    FROM LS_CDB_INDIVIDUAL_TBL 
                    UNION ALL
                    SELECT 'RD RBS EMPLOYEE' AS CLASS_SET_NAME , OCCUPATION_CODE AS CLASS_VAL_ID, LAST_CHNG_DATE,
                    CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID,
                    to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS 
                    FROM LS_CDB_INDIVIDUAL_TBL 
                    UNION ALL 
                    SELECT 'RD INDIVIDUAL JOB TITLE CODE' AS CLASS_SET_NAME, OCCUPATION_CODE AS CLASS_VAL_ID,
                    LAST_CHNG_DATE, CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID,
                    to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS 
                    FROM LS_CDB_INDIVIDUAL_TBL
                    UNION ALL 
                    SELECT 'RD INDIVIDUAL JOB TITLE' AS CLASS_SET_NAME, OCCUPATION_CODE AS CLASS_VAL_ID, LAST_CHNG_DATE,
                    CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID, SRC_SYS_ID,
                    to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS
                    FROM LS_CDB_INDIVIDUAL_TBL
                    UNION ALL
                    SELECT 'RD BANK ID' AS CLASS_SET_NAME, (BANK_ID) AS CLASS_VAL_ID, LAST_CHNG_DATE, CUSTOMER_ID, SRC_EVT_TYP_FLG,
                    SRC_SYS_INST_ID, SRC_SYS_ID, to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS
                    FROM LS_CDB_INDIVIDUAL_TBL
                    UNION ALL
                    SELECT 'RD CUSTOMER OWNED BY' AS CLASS_SET_NAME, LTRIM(RTRIM( CUSTOMER_OWNED_BY)) AS CLASS_VAL_ID,
                    LAST_CHNG_DATE, CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_SYS_INST_ID,  SRC_SYS_ID,
                    to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS
                    FROM LS_CDB_INDIVIDUAL_TBL""")
            if 1 in debug: idvclass_start_df.show(2, truncate=False)
            idvclass_start_cnt = idvclass_start_df.count()
        except: idvclass_start_cnt = 0
    else: idvclass_start_cnt=0

    try:
        if idvclass_start_cnt > 0:
            idvclass_start_df.createOrReplaceTempView("IDVCLASS_START_TBL")

            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CLASS - step 2 of 3")
            c_ld_bbbb_ccc_idvclass_df = spark.sql("""Select CLASS_SET_NAME, CLASS_VAL_ID, CASE WHEN LTRIM(RTRIM(CLASS_VAL_ID)) =
                    '' OR ISNULL(CLASS_VAL_ID) THEN 'Not Available' ELSE CLASS_VAL_ID END AS t_CLASS_VAL_ID, LAST_CHNG_DATE,
                    CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG, SRC_EXTRACT_TS
                    FROM IDVCLASS_START_TBL WHERE CLASS_VAL_ID IS NOT NULL or TRIM(CLASS_VAL_ID) !=''""").distinct()
            # c_ld_bbbb_ccc_idvclass_df.show(3, truncate=False)
            c_ld_bbbb_ccc_idvclass_df.createOrReplaceTempView("IDVCLASS_FILTER_TBL")

            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CLASS - step 3 of 3")
            c_ld_bbbb_ccc_idvclass_trn_df = spark.sql("""SELECT DISTINCT idvclass.CLASS_SET_NAME, idvclass.LAST_CHNG_DATE,
                    idvclass.CUSTOMER_ID, idvclass.SRC_SYS_INST_ID, idvclass.SRC_SYS_ID, idvclass.SRC_EVT_TYP_FLG,
                    idvclass.SRC_EXTRACT_TS, idvclass.CLASS_VAL_ID AS ORIG_CLASS_VAL_ID, idvclass.t_CLASS_VAL_ID,
                    vsl.VSL_LKP_VALUE AS CLASS_VAL_ID, vsl.OUT_ACTION_CODE
                    FROM IDVCLASS_FILTER_TBL idvclass
                    LEFT OUTER JOIN VSL_TBL vsl
                    ON vsl.VALUE_SET_NAME = idvclass.CLASS_SET_NAME AND
                    vsl.COLUMN_1_STRING_VALUE=idvclass.t_CLASS_VAL_ID 
                    AND vsl.OUT_ACTION_CODE NOT IN ('STOP', 'BLOCK')""").distinct()
            if 1 in debug: c_ld_bbbb_ccc_idvclass_trn_df.show(2, truncate=False)
            c_ld_bbbb_ccc_idvclass_trn_df.createOrReplaceTempView("TRN_IDV_CLASS_VW")

            if c_ld_bbbb_ccc_idvclass_trn_df.count() > 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Creating Sub-document")
                idv_class_sub_doc = spark.sql("""SELECT CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, STRUCT(SRC_EVT_TYP_FLG, 
                            ORIG_CLASS_VAL_ID, LAST_CHNG_DATE, SRC_EXTRACT_TS) AS CLASS FROM (SELECT DISTINCT CUSTOMER_ID, 
                            SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG, ORIG_CLASS_VAL_ID, LAST_CHNG_DATE, 
                            SRC_EXTRACT_TS FROM TRN_IDV_CLASS_VW) A WHERE CUSTOMER_ID IS NOT NULL ORDER BY SRC_EXTRACT_TS DESC""")

                if 3 in debug: idv_class_sub_doc.show(2, truncate=False)

                df_individual_asso_trn=df_individual_asso_trn.join(idv_class_sub_doc, on=['CUSTOMER_ID', 'SRC_SYS_INST_ID',
                         'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Joined IDV_ASSO with CLASS")
                # df_individual_asso_trn.printSchema()
                if 5 in debug: df_individual_asso_trn.show(2, truncate=False)
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records for IDV_CLASS")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS','Generated 0 (zero) TRN - IDV_CLASS records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process TRN: IDV_CLASS")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_CLASS has no/zero valid records')
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process - IDV_CLASS")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process - IDV_CLASS')
    ## End of IDV_CLASS------------------------------------------------------------------------------------ ##

    ##################### Begin of NATID #####################
    #C_LD_BBBB_CCC_IDVNATID
    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating - IDV_NATID")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating NATID');
    try:
        if idv_asso_cnt > 0:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_NATID - Step 1 of 3")
            c_ld_bbbb_ccc_idvnatid_df = spark.sql("""SELECT NATIONAL_INS_NO, 
                to_timestamp(SRC_EXTRACT_TS, "yyyy-MM-dd'T'HH:mm:ss") AS SRC_EXTRACT_TS, LAST_CHNG_DATE, 
                CASE WHEN LTRIM(RTRIM('Individual National 
                Insurance Number')) = '' OR ISNULL('Individual National Insurance Number') THEN 'Not Available' ELSE 
                'Individual National Insurance Number' END AS t_NATID,  CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, 
                CASE WHEN LTRIM(RTRIM('GBR')) = '' OR ISNULL('GBR') THEN 'Not Available' ELSE 'GBR' END AS t_COUNTRY_CODE, 
                SRC_EVT_TYP_FLG FROM LS_CDB_INDIVIDUAL_TBL 
                WHERE NATIONAL_INS_NO IS NOT NULL AND TRIM(NATIONAL_INS_NO) !=''""")
            if 4 in debug: c_ld_bbbb_ccc_idvnatid_df.show(2, truncate=False)
            c_ld_bbbb_ccc_idvnatid_df.createOrReplaceTempView("IDV_NATID_TBL")
            c_ld_bbbb_ccc_idvnatid_cnt = c_ld_bbbb_ccc_idvnatid_df.count()
        else: c_ld_bbbb_ccc_idvnatid_cnt=0
    except: c_ld_bbbb_ccc_idvnatid_cnt = 0

    try:
        if c_ld_bbbb_ccc_idvnatid_cnt > 0:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_NATID - Step 2 of 3")
            c_ld_bbbb_ccc_idvnatid_trn_df = spark.sql("""SELECT DISTINCT idvnatid.NATIONAL_INS_NO as NAT_IDENT_VAL, 
               idvnatid.LAST_CHNG_DATE as SRC_SYS_CHG_DT, idvnatid.CUSTOMER_ID as SRC_IDV_ID_VAL, idvnatid.SRC_SYS_INST_ID, 
               idvnatid.SRC_SYS_ID, idvnatid.SRC_EXTRACT_TS, idvnatid.SRC_EVT_TYP_FLG, idvnatid.t_NATID, vsl1.VSL_LKP_VALUE 
               AS NAT_IDENT_TP, idvnatid.t_COUNTRY_CODE, vsl2.VSL_LKP_VALUE AS ISSUING_CNTRY_ID 
               FROM IDV_NATID_TBL idvnatid 
               LEFT OUTER JOIN VSL_TBL vsl1 ON vsl1.VALUE_SET_NAME='RD NATIONAL IDENTITY TYPE' AND 
               vsl1.COLUMN_1_STRING_VALUE=idvnatid.t_NATID AND vsl1.OUT_ACTION_CODE NOT IN ('STOP', 'BLOCK') LEFT OUTER JOIN 
               VSL_TBL vsl2 ON vsl2.VALUE_SET_NAME='RD COUNTRY CODES' AND vsl2.COLUMN_1_STRING_VALUE=idvnatid.t_COUNTRY_CODE 
               AND vsl2.OUT_ACTION_CODE NOT IN ('STOP', 'BLOCK')""").distinct()
            if 4 in debug: c_ld_bbbb_ccc_idvnatid_trn_df.show(2, truncate=False)
            c_ld_bbbb_ccc_idvnatid_trn_df.createOrReplaceTempView("LS_IDV_NATID_VW")

            if c_ld_bbbb_ccc_idvnatid_trn_df.count() > 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_NATID - Step 3 of 3")
                df_idv_natid_sub_doc=spark.sql("""SELECT DISTINCT SRC_IDV_ID_VAL AS CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID,
                    STRUCT(NAT_IDENT_TP, ISSUING_CNTRY_ID, NAT_IDENT_VAL, SRC_SYS_CHG_DT, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG) AS IDVNATID 
                    FROM LS_IDV_NATID_VW WHERE SRC_IDV_ID_VAL IS NOT NULL ORDER BY SRC_EXTRACT_TS DESC""").distinct()
                if 3 in debug:
                    df_idv_natid_sub_doc.show(2, truncate=False);
                    df_idv_natid_sub_doc.printSchema()

                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Joining IDV_ASSO_TRN and IDV_NATID")
                df_individual_asso_trn=df_individual_asso_trn.join(df_idv_natid_sub_doc,
                                           on=['CUSTOMER_ID', 'SRC_SYS_INST_ID', 'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())
                # df_individual_asso_trn.printSchema()
                if 5 in debug: df_individual_asso_trn.show(2, truncate=False)
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records for TRN: NATID")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS', 'Generated 0 (zero) TRN - NATID records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process - TRN: NATID")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'NATID has no/zero valid records');
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process - NATID")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process - NATID');
    ## End of NATID
    ## ------------------------------------------------------------------------------------------------------- ##


    ##################### Begin of IDVIDENT #####################
    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating IDVIDENT")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating IDVIDENT');
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDVIDENT - Step 1 of 3")
    try:
        if idv_asso_cnt > 0:
            df_idvident_start = spark.sql(""" 
                WITH GURN AS 
                (SELECT CUSTOMER_ID, GURN, CASE WHEN LTRIM(RTRIM(SRC_SYS_ID)) = '' OR ISNULL(SRC_SYS_ID) THEN 'Not Available' ELSE 
                SRC_SYS_ID END AS SRC_SYS_ID FROM LS_CDB_GURN )
                SELECT IDV.LAST_CHNG_DATE,IDV.CUSTOMER_ID,IDV.SRC_SYS_INST_ID,
                CASE WHEN LTRIM(RTRIM(IDV.BANK_ID)) = '' OR ISNULL(IDV.BANK_ID) THEN 'Not Available' ELSE IDV.BANK_ID END AS BANK_ID,
                CASE WHEN LTRIM(RTRIM(IDV.SRC_SYS_ID)) = '' OR ISNULL(IDV.SRC_SYS_ID) THEN 'Not Available' ELSE IDV.SRC_SYS_ID END AS SRC_SYS_ID,
                IDV.CUSTOMER_OWNED_BY,Null AS GURN, CASE WHEN LTRIM(RTRIM('CIN')) = '' OR ISNULL('CIN') THEN 'Not Available' ELSE 'CIN' END AS IDENT_TP_ID,
                IDV.CUSTOMER_ID AS IDV_ID_VAL, IDV.CUSTOMER_ID AS SRC_IDV_ID_VAL, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG
                FROM  LS_CDB_INDIVIDUAL_TBL IDV  
                UNION ALL
                SELECT IDV.LAST_CHNG_DATE, IDV.CUSTOMER_ID  ,IDV.SRC_SYS_INST_ID ,CASE WHEN LTRIM(RTRIM(IDV.BANK_ID)) = '' OR ISNULL(IDV.BANK_ID) THEN 'Not Available' ELSE 
                IDV.BANK_ID END AS BANK_ID,CASE WHEN LTRIM(RTRIM(IDV.SRC_SYS_ID)) = '' OR ISNULL(IDV.SRC_SYS_ID) THEN 'Not Available' ELSE 
                IDV.SRC_SYS_ID END AS SRC_SYS_ID,IDV.CUSTOMER_OWNED_BY ,GURN.GURN ,CASE WHEN LTRIM(RTRIM('GURN')) = '' OR ISNULL('GURN') THEN 'Not Available' 
                ELSE 'GURN' END AS IDENT_TP_ID,GURN.GURN AS IDV_ID_VAL ,IDV.CUSTOMER_ID AS SRC_IDV_ID_VAL, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG
                FROM LS_CDB_INDIVIDUAL_TBL IDV INNER JOIN  GURN ON IDV.CUSTOMER_ID=GURN.CUSTOMER_ID """);
            if 1 in debug: df_idvident_start.show(2, truncate=False)
            df_idvident_start.createOrReplaceTempView("df_idvident_start")
            df_idvident_start_cnt = df_idvident_start.count()
        else: df_idvident_start_cnt = 0
    except: df_idvident_start_cnt = 0

    try:
        if df_idvident_start_cnt > 0:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_NATID - df_idvident_final - Step 2 of 3")
            df_idvident_final = spark.sql("""  
                SELECT DISTINCT idvident.LAST_CHNG_DATE as SRC_SYS_CHG_DT, idvident.CUSTOMER_ID, idvident.SRC_SYS_INST_ID,
                idvident.SRC_SYS_ID, IDV_ID_VAL, idvident.SRC_IDV_ID_VAL, 'CIN' AS IDENTIFIER_TYPE, idvident.BANK_ID,
                vsl1.VSL_LKP_VALUE AS IDENT_TP_ID, idvident.SRC_EXTRACT_TS, idvident.SRC_EVT_TYP_FLG, vsl2.VSL_LKP_VALUE AS SRC_BRND_ID, vsl3.VSL_LKP_VALUE AS DATA_SRC_ID,
                NULL AS SRC_ROWID, vsl1.OUT_ACTION_CODE as OUT_ACTION_CODE 
                FROM df_idvident_start idvident 
                LEFT OUTER JOIN VSL_TBL vsl1 ON vsl1.VALUE_SET_NAME='RD IDENTIFIER TYPES' AND 
                vsl1.COLUMN_1_STRING_VALUE=idvident.IDENT_TP_ID AND vsl1.OUT_ACTION_CODE NOT IN ('STOP','BLOCK') 
                LEFT OUTER JOIN VSL_TBL vsl2 ON vsl2.VALUE_SET_NAME='RD BRAND NAMES' AND 
                vsl2.COLUMN_1_STRING_VALUE=idvident.BANK_ID AND vsl2.OUT_ACTION_CODE NOT IN ('STOP', 'BLOCK')
                LEFT OUTER JOIN VSL_TBL vsl3 ON vsl3.VALUE_SET_NAME='RD SRC SYSTEMS' AND 
                vsl3.COLUMN_1_STRING_VALUE=idvident.SRC_SYS_ID AND vsl3.OUT_ACTION_CODE NOT IN ('STOP', 'BLOCK')""")
            if 1 in debug: df_idvident_final.show(2, truncate=False)
            df_idvident_final.createOrReplaceTempView("IDV_IDENT_VW")

            if df_idvident_final.count() > 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generating IDVIDENT - Sub_document - Step 3 of 3")
                df_idvident_sub_doc = spark.sql("""SELECT DISTINCT SRC_IDV_ID_VAL AS CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, 
                   STRUCT(IDENT_TP_ID, DATA_SRC_ID, SRC_BRND_ID, IDV_ID_VAL, SRC_SYS_CHG_DT, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG) AS 
                   IDVIDENT FROM IDV_IDENT_VW WHERE SRC_IDV_ID_VAL IS NOT NULL ORDER BY SRC_EXTRACT_TS DESC""").distinct()
                if 3 in debug: df_idvident_sub_doc.show(2, truncate=False)

                df_individual_asso_trn = df_individual_asso_trn.join(df_idvident_sub_doc, on=['CUSTOMER_ID', 'SRC_SYS_INST_ID',
                                              'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull()).distinct()
                if 5 in debug: df_individual_asso_trn.show(5, truncate=False)
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records for TRN: IDVIDENT")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS','Generated 0 (zero) TRN - IDVIDENT records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process TRN: IDVIDENT")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDVIDENT has no/zero valid records');
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process - IDVIDENT")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process - IDVIDENT');
    ## End of IDVIDENT
    ## --------------------------------------------------------------------------------------------------------- ##


## IDV_RL, IDV_RLORG, IDV_FLAG, IDV_CNTRY, IDV_SRC
def function_process_second_trn(spark, mongo_read_collection):
    # try:
    global df_idv_asso, df_street_addr, df_gurn, df_idv_relship, df_party_agrmnt
    global df_idv_email, df_idv_flag, df_idv_tax, df_idv_ctzn, df_individual_asso_trn, df_individual_asso_trn_1;

    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating processing IDV_RL, RL_IDVORG, IDV_FLAG, IDVCNTRY & IDV_SRC")
    print(
        str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching processed TRN - INDIVIDIAL_ASSO data from MongoDB-ODS")
    df_individual_asso_trn_1 = read_from_mongo(spark, mongo_write_collection_partial)
    df_individual_asso_trn_1 = df_individual_asso_trn_1.drop("_id")
    df_individual_asso_trn_1_drv = df_individual_asso_trn_1.select("CUSTOMER_ID","SRC_SYS_INST_ID","SRC_SYS_ID").distinct()
    # # createOrReplaceTempView("IDV_ASSO_TRN_1_VW")
    # df_individual_asso_trn_1_drv = spark.sql("""SELECT CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID FROM IDV_ASSO_TRN_1_VW""").distinct()
    if 3 in debug:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: df_individual_asso_trn_1")
        df_individual_asso_trn_1.show(2, truncate=False)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: df_individual_asso_trn_1_drv")
        df_individual_asso_trn_1_drv.show(2, truncate=False)

    ## Checking VSL Table record count
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching VSL Data from " + mongo_read_collection)
        vsl_tbl_df = read_from_mongo(spark, mongo_read_collection)
        vsl_cnt = vsl_tbl_df.count()
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: VSL record count is: " + str(vsl_cnt))
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to load VLS data")
        vsl_tbl_df=getEmptyDataFrame(columndata, mongo_read_collection)
        vsl_tbl_df.createOrReplaceTempView("VSL_TBL");

        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching VSL Data from " + mongo_read_collection)
        df_idv_asso.createOrReplaceTempView("LS_CDB_INDIVIDUAL_TBL");
        df_idv_tax.createOrReplaceTempView("LS_CDB_PARTY_TAX_RESIDENCE");
        df_idv_ctzn.createOrReplaceTempView("LS_CDB_IND_ADDNL_CTZN");
        df_street_addr.createOrReplaceTempView("LS_CDB_STREET_ADDR");
        df_party_agrmnt.createOrReplaceTempView("LS_PARTY_AGREEMENT_VW");

    ##################### Begin of RL_IDV (C_LD_BBBB_CCC_RL_IDV) #####################
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiated TRN - IDV_RL generation")
    if 3 in debug: df_idv_relship.show(2, truncate=False)
    df_idv_relship.createOrReplaceTempView("LS_CDB_PARTY_RELATIONSHIP")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating - RL_IDV generation');

    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Relationship - Step 1 of 4")
    try:
        relship_cnt = df_idv_relship.count()
        if relship_cnt > 0:
            df_rlnshp_start = spark.sql("""SELECT DISTINCT r.CUSTOMER_ID_1, r.REL_TYPE, r.CUSTOMER_ID_2, r.DATE_CREATED, 
            r.SRC_SYS_ID, r.SRC_SYS_INST_ID, r.SRC_EVT_TYP_FLG, r.SRC_EXTRACT_TS 
            FROM LS_CDB_PARTY_RELATIONSHIP r 
            INNER JOIN LS_CDB_INDIVIDUAL_TBL a ON (r.CUSTOMER_ID_1 = a.CUSTOMER_ID) 
            INNER JOIN LS_CDB_INDIVIDUAL_TBL b ON (r.CUSTOMER_ID_2 = b.CUSTOMER_ID) 
            WHERE r.REL_TYPE = 'SB'""");
            if 1 in debug: df_rlnshp_start.show(2, truncate=False)
            df_rlnshp_start.createOrReplaceTempView("df_rlnshp_start")
            df_rlnshp_cnt = df_rlnshp_start.count()
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Relationship - count : "+str(df_rlnshp_cnt))
        else: df_rlnshp_cnt = 0
    except:
        df_rlnshp_cnt = 0
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Relationship - Records not available");

    try:
        if df_rlnshp_cnt > 0:
            # df_rlnshp_final
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Relationship - Step 2 of 4");
            try:
                df_rlnshp_final = spark.sql("""SELECT DISTINCT CUSTOMER_ID_1, CUSTOMER_ID_2, REL_TYPE, DATE_CREATED, 
                VSL_LKP_VALUE as RL_TP_ID, NULL as SRC_ROWID, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG, SRC_EXTRACT_TS 
                FROM df_rlnshp_start 
                LEFT OUTER JOIN VSL_TBL vsl ON vsl.VALUE_SET_NAME='RD CUST_REL_TYPES' 
                AND vsl.COLUMN_1_STRING_VALUE=REL_TYPE 
                AND vsl.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')""")

                if 1 in debug: df_rlnshp_final.show(2, truncate=False)
                df_rlnshp_final.createOrReplaceTempView("IDV_RELATIONSHIP_TRN_VW");

                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_Relationship - Step 3 of 4");
                df_rl_idv_2Sets = spark.sql("""SELECT CUSTOMER_ID_1 AS CUSTOMER_ID, REL_TYPE, DATE_CREATED, RL_TP_ID, 
                    SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG, SRC_EXTRACT_TS FROM IDV_RELATIONSHIP_TRN_VW 
                    UNION 
                    SELECT CUSTOMER_ID_2 AS CUSTOMER_ID, REL_TYPE, DATE_CREATED, RL_TP_ID, SRC_SYS_INST_ID, SRC_SYS_ID, 
                    SRC_EVT_TYP_FLG, SRC_EXTRACT_TS FROM IDV_RELATIONSHIP_TRN_VW""");
                if 4 in debug:
                    df_rl_idv_2Sets.show(2, truncate=False)
                df_rl_idv_2Sets.createOrReplaceTempView("IDV_REL_TRN_VW_2")
                df_rl_idv_2Sets_cnt = df_rl_idv_2Sets.count()
            except: df_rl_idv_2Sets_cnt = 0

            if df_rl_idv_2Sets_cnt > 0:
                print("\n"+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generating RL_IDV - Sub_document - Step 4 of 4")
                df_rl_idv_sub_doc = spark.sql("""SELECT DISTINCT CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, STRUCT(REL_TYPE, RL_TP_ID, 
                    DATE_CREATED, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG) AS RL_IDV FROM IDV_REL_TRN_VW_2 
                    WHERE CUSTOMER_ID IS NULL ORDER BY SRC_EXTRACT_TS DESC""");
                if 4 in debug: df_rl_idv_sub_doc.show(2, truncate=False)

                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Joining TRN-INDIVIDUAL_ASSO & RL_IDV")
                df_individual_asso_trn_1_drv=df_individual_asso_trn_1_drv.join(df_rl_idv_sub_doc,
                                           on=['CUSTOMER_ID', 'SRC_SYS_INST_ID', 'SRC_SYS_ID'], how='left').\
                                           where(col("CUSTOMER_ID").isNotNull())
                if 5 in debug:
                    df_individual_asso_trn_1_drv.printSchema()
                    df_individual_asso_trn_1_drv.show(2, truncate=False)
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: RL_IDV processing completed")
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records for TRN: RL_IDV")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS', 'Generated 0 (zero) TRN - RL_IDV records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available to process TRN: RL_IDV")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'RL_IDV has no/zero valid records');
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process: RL_IDV")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process: RL_IDV');
    # End of RL_IDV
    ## -------------------------------------------------------------------------------- ##

    ##################### BEGIN OF C_LD_BBBB_CCC_RL_IDVORG #####################
    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating TRN - RL_IDVORG generation")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating RL_IDVORG');

    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing RL_IDVORG - Step 1 of 2")
    try:
        relship_cnt = df_idv_relship.count()
        if relship_cnt > 0:
            df_relation = spark.sql("""SELECT PR.CUSTOMER_ID_1, 
            CASE WHEN LTRIM(RTRIM(PR.REL_TYPE)) = '' OR ISNULL(PR.REL_TYPE) THEN 
            'Not Available' ELSE PR.REL_TYPE END AS REL_TYPE,PR.CUSTOMER_ID_2,PR.DATE_CREATED AS CRT_DT, 
            PR.SRC_SYS_ID, PR.SRC_SYS_INST_ID, PR.SRC_EVT_TYP_FLG, PR.SRC_EXTRACT_TS 
            FROM LS_CDB_PARTY_RELATIONSHIP PR 
            INNER JOIN LS_CDB_INDIVIDUAL_TBL I  ON PR.CUSTOMER_ID_2 = I.CUSTOMER_ID 
            WHERE PR.REL_TYPE IN ('SB','ZB','YB','WB','FB','TB','PB','XB','BB','DB','CB') 
            UNION ALL 
            SELECT PR.CUSTOMER_ID_2 AS CUSTOMER_ID_1,CASE WHEN LTRIM(RTRIM(PR.REL_TYPE)) = '' OR ISNULL(PR.REL_TYPE) THEN 
            'Not Available' ELSE PR.REL_TYPE END AS REL_TYPE, PR.CUSTOMER_ID_1 AS CUSTOMER_ID_2, 
            PR.DATE_CREATED AS CRT_DT, PR.SRC_SYS_ID, PR.SRC_SYS_INST_ID, PR.SRC_EVT_TYP_FLG, PR.SRC_EXTRACT_TS 
            FROM LS_CDB_PARTY_RELATIONSHIP PR 
            INNER JOIN  LS_CDB_INDIVIDUAL_TBL I ON PR.CUSTOMER_ID_2 = I.CUSTOMER_ID 
            WHERE  PR.REL_TYPE IN ('OO')""")
            df_relation.createOrReplaceTempView("df_relation")
            df_relation_cnt = df_relation.count()
        else: df_relation_cnt = 0
    except: df_relation_cnt = 0

    try:
        if df_relation_cnt > 0:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing RL_IDVORG - Step 2 of 2")
            try:
                df_relation_final = spark.sql("""SELECT DISTINCT idvorg.CUSTOMER_ID_1,idvorg.REL_TYPE,CUSTOMER_ID_2, idvorg.CRT_DT, 
                    vsl1.VSL_LKP_VALUE AS RL_TP_ID, NULL as SRC_ROWID, NULL as LAST_UPDATE_DATE, 
                    vsl1.OUT_ACTION_CODE as OUT_ACTION_CODE, SRC_SYS_ID, SRC_SYS_INST_ID, SRC_EVT_TYP_FLG, SRC_EXTRACT_TS 
                    FROM df_relation idvorg 
                    LEFT OUTER JOIN VSL_TBL vsl1 
                    ON vsl1.VALUE_SET_NAME='RD CUST_REL_TYPES' 
                    AND vsl1.COLUMN_1_STRING_VALUE = idvorg.REL_TYPE AND vsl1.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')""")
                df_relation_final.createOrReplaceTempView("RL_IDVORG_VW")
                if 4 in debug: df_relation_final.show(2, truncate=False)
                df_relation_cnt = df_relation_final.count()
            except: df_relation_cnt=0

            if df_relation_cnt > 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Creating  Sub-document - RL_IDVORG")
                df_rl_idvorg_sub_doc=spark.sql("""SELECT DISTINCT CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, STRUCT(REL_TYPE, RL_TP_ID, CRT_DT, 
                    SRC_EXTRACT_TS, SRC_EVT_TYP_FLG) AS IDVORG FROM ( 
                        SELECT CUSTOMER_ID_1 AS CUSTOMER_ID, REL_TYPE, CRT_DT, RL_TP_ID, SRC_SYS_ID, SRC_SYS_INST_ID, SRC_EXTRACT_TS,  
                        SRC_EVT_TYP_FLG FROM RL_IDVORG_VW 
                        UNION ALL 
                        SELECT CUSTOMER_ID_2 AS CUSTOMER_ID, REL_TYPE, CRT_DT, RL_TP_ID, SRC_SYS_ID, SRC_SYS_INST_ID, SRC_EXTRACT_TS, 
                        SRC_EVT_TYP_FLG FROM RL_IDVORG_VW
                    ) A WHERE CUSTOMER_ID IS NOT NULL ORDER BY SRC_EXTRACT_TS DESC""").distinct()
                if 3 in debug: df_rl_idvorg_sub_doc.show(2, truncate=False)

                df_individual_asso_trn_1_drv = df_individual_asso_trn_1_drv.join(df_rl_idvorg_sub_doc, on=['CUSTOMER_ID',
                                         'SRC_SYS_INST_ID', 'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())
                if 5 in debug:
                    df_individual_asso_trn_1_drv.printSchema()
                    df_individual_asso_trn_1_drv.show(2, truncate=False)
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records to process: RL_IDVORG")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS',
                                             'Generated 0 (zero) TRN - RL_IDVORG records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available for RL_IDVORG")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'RL_IDVORG has no/zero valid records');
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process - RL_IDVORG")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process - RL_IDVORG');
    ## END OF C_LD_BBBB_CCC_RL_IDVORG
    ## ------------------------------------------------------------------------------------------------------ ##

    ##################### Begin of IDVFLAG (C_LD_BBBB_CCC_IDVFLAG) #####################
    # #LS_CDB_INDIVIDUAL, LS_CDB_IND_INDICATORS
    df_idv_flag.createOrReplaceTempView("LS_CDB_IND_INDICATORS")
    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating IDV_FLAG")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating IDV_FLAG');

    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_FLAG - Step 1 of 3");
    try:
        idv_flag_cnt = df_idv_flag.count()
        if idv_flag_cnt > 0:
            df_idvflag_start = spark.sql("""SELECT CASE WHEN LTRIM(RTRIM('YES')) = '' OR ISNULL('YES') THEN 'Not Available' 
            ELSE 'Yes' END AS INDICATOR_TYPE, CASE WHEN LTRIM(RTRIM('RD SENSITIVITY FLAG')) = '' OR 
            ISNULL('RD SENSITIVITY FLAG') THEN 'Not Available' ELSE 'RD SENSITIVITY FLAG' END AS INDICATOR_NAME, 
            CUSTOMER_ID, LAST_CHNG_DATE AS UPDATE_DATE_TIME, SRC_SYS_INST_ID,  SRC_SYS_ID, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG 
            FROM LS_CDB_INDIVIDUAL_TBL WHERE DISABILITY_CODE IS NOT NULL 
            UNION 
            SELECT CASE WHEN LTRIM(RTRIM(concat(LTRIM(RTRIM(BANK_ID)), LTRIM(RTRIM(CUSTOMER_OWNED_BY))))) = '' OR 
            ISNULL(concat(LTRIM(RTRIM(BANK_ID)), LTRIM(RTRIM(CUSTOMER_OWNED_BY)))) THEN 'Not Available' ELSE 
            concat(LTRIM(RTRIM(BANK_ID)), LTRIM(RTRIM(CUSTOMER_OWNED_BY))) END AS INDICATOR_TYPE, 
            CASE WHEN LTRIM(RTRIM('RD RING FENCE INDICATOR')) = '' OR ISNULL('RD RING FENCE INDICATOR') THEN 
            'Not Available' ELSE 'RD RING FENCE INDICATOR' END AS INDICATOR_NAME, CUSTOMER_ID, 
            LAST_CHNG_DATE AS UPDATE_DATE_TIME, SRC_SYS_INST_ID,  SRC_SYS_ID, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG 
            FROM LS_CDB_INDIVIDUAL_TBL 
            UNION 
            SELECT CASE WHEN LTRIM(RTRIM('YES')) = '' OR ISNULL('YES') THEN 'Not Available' ELSE 'Yes' END AS INDICATOR_TYPE, 
            CASE WHEN LTRIM(RTRIM('RD POLITICALLY EXPOSED INDICATOR')) = '' OR ISNULL('RD POLITICALLY EXPOSED INDICATOR') THEN 'Not Available' ELSE 
            'RD POLITICALLY EXPOSED INDICATOR' END AS INDICATOR_NAME, CUSTOMER_ID, UPDATE_DATE_TIME AS UPDATE_DATE_TIME, 
            SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG 
            FROM LS_CDB_IND_INDICATORS 
            WHERE INDICATOR_TYPE IN ('PEP') AND TRIM(INDICATOR_VALUE) IN ('Y')""");
            if 4 in debug: df_idvflag_start.show(2, truncate=False)
            df_idvflag_start.createOrReplaceTempView("df_idvflag_start_vw");
            df_idvflag_cnt = df_idvflag_start.count()
        else: df_idvflag_cnt = 0
    except: df_idvflag_cnt = 0

    try:
        if df_idvflag_cnt > 0:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Starting IDV_FLAG - Step 2 of 3")
            df_idvflag_final = spark.sql("""SELECT DISTINCT idvflag.INDICATOR_TYPE, idvflag.INDICATOR_NAME, idvflag.CUSTOMER_ID, 
                idvflag.UPDATE_DATE_TIME AS SRC_SYS_CHG_DT, NULL as SRC_ROWID, NULL as LAST_UPDATE_DATE, 
                idvflag.SRC_SYS_INST_ID, idvflag.SRC_SYS_ID, idvflag.SRC_EXTRACT_TS, idvflag.SRC_EVT_TYP_FLG, 
                vsl1.VSL_LKP_VALUE AS FLAG_VAL_ID, vsl1.OUT_ACTION_CODE AS OUT_ACTION_CODE 
                FROM df_idvflag_start_vw idvflag 
                LEFT OUTER JOIN VSL_TBL vsl1 ON vsl1.VALUE_SET_NAME=idvflag.INDICATOR_NAME AND 
                vsl1.COLUMN_1_STRING_VALUE=idvflag.INDICATOR_TYPE AND vsl1.OUT_ACTION_CODE NOT IN ('STOP','BLOCK') 
                """).distinct()
            if 4 in debug: df_idvflag_final.show(2, truncate=False)
            df_idvflag_final.createOrReplaceTempView("TRN_IDV_FLAG_VW")

            if df_idvflag_final.count() > 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Creating sub_document for IDV_FLAG - Step 3 of 3")
                df_idvFlag_sub_doc = spark.sql("""SELECT DISTINCT CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, STRUCT(INDICATOR_TYPE, 
                    INDICATOR_NAME, SRC_SYS_CHG_DT, SRC_EXTRACT_TS, SRC_EVT_TYP_FLG) AS IDV_FLAG FROM TRN_IDV_FLAG_VW 
                    WHERE CUSTOMER_ID IS NOT NULL """)
                if 3 in debug: df_idvFlag_sub_doc.show(2, truncate=False)

                df_individual_asso_trn_1_drv = df_individual_asso_trn_1_drv.join(df_idvFlag_sub_doc, on=['CUSTOMER_ID',
                                        'SRC_SYS_INST_ID', 'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())
                if 5 in debug:
                    df_individual_asso_trn_1_drv.printSchema()
                    df_individual_asso_trn_1_drv.show(2, truncate=False)
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records to process TRN: IDVFLAG");
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS',
                                             'Generated 0 (zero) TRN - IDV_FLAG records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available for TRN: IDVFLAG")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_FLAG has no/zero valid records');
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process - IDVFLAG")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process - IDVFLAG');

    ## End of IDVFLAG
    ## ------------------------------------------------------------------------------------------------------ ##

    ##################### Begin of IDV_CNTRY (C_LD_BBBB_CCC_IDVCNTRY) #####################
    # df_idvcntry_first
    print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating IDV_CNTRY")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating IDV_CNTRY');

    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CNTRY - df_idvcntry_first - Step 1 of 10")
    try:
        idv_tax_cnt = df_idv_tax.count()
        if idv_tax_cnt > 0:
            df_idvcntry_first = spark.sql("""SELECT TAX.CUSTOMER_ID, TAX.CTRY_OF_TAXATION, TAX.SRC_EXTRACT_TS AS CRT_DT, 
            ROW_NUMBER() OVER (PARTITION BY TAX.CUSTOMER_ID, TAX.CTRY_OF_TAXATION ORDER BY TAX.TAX_CRE_TIMESTAMP DESC) AS RNK, 
            TAX.SRC_SYS_INST_ID, TAX.SRC_SYS_ID, TAX.SRC_EVT_TYP_FLG 
            FROM LS_CDB_PARTY_TAX_RESIDENCE TAX 
            INNER JOIN LS_CDB_INDIVIDUAL_TBL IDV ON TAX.CUSTOMER_ID=IDV.CUSTOMER_ID""")
            if 4 in debug: df_idvcntry_first.show(2, truncate=False)
            df_idvcntry_first.createOrReplaceTempView("df_idvcntry_first");
            df_idvcntry_cnt = df_idvcntry_first.count()
        else: df_idvcntry_cnt=0
    except: df_idvcntry_cnt=0

    try:
        if df_idvcntry_cnt > 0:
            # df_idvcntry_second
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CNTRY - df_idvcntry_second - Step 2 of 10")
            df_idvcntry_second = spark.sql("""SELECT CRT_DT AS LAST_CHNG_DATE, CUSTOMER_ID, CTRY_OF_TAXATION AS CTRY_CODE, 
                SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_first WHERE RNK=1""")
            df_idvcntry_second.createOrReplaceTempView("df_idvcntry_second");
            if 4 in debug: df_idvcntry_second.show(2, truncate=False)

            # df_idvcntry_third
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CNTRY - df_idvcntry_third - Step 3 of 10")
            df_idvcntry_third = spark.sql("""SELECT SRC_EXTRACT_TS AS LAST_CHNG_DATE, CUSTOMER_ID, CTRY_OF_NATIONALTY, 
                ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID, CTRY_OF_NATIONALTY ORDER BY LAST_CHNG_DATE DESC) AS RNK, 
                SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM LS_CDB_INDIVIDUAL_TBL""")
            df_idvcntry_third.createOrReplaceTempView("df_idvcntry_third");
            if 4 in debug: df_idvcntry_third.show(2, truncate=False)

            # df_idvcntry_fourth:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + "Processing IDV_CNTRY - df_idvcntry_fourth - Step 4 of 10")
            df_idvcntry_fourth = spark.sql("""SELECT LAST_CHNG_DATE, CUSTOMER_ID, CTRY_OF_NATIONALTY AS CTRY_CODE, 
                SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_third WHERE RNK=1""")
            df_idvcntry_fourth.createOrReplaceTempView("df_idvcntry_fourth");
            if 4 in debug: df_idvcntry_fourth.show(2, truncate=False)

            # df_idvcntry_fifth
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + "Processing IDV_CNTRY - df_idvcntry_fifth - Step 5 of 10")
            df_idvcntry_fifth = spark.sql("""SELECT CTZN.CUSTOMER_ID, CTZN.ADDNL_CITIZENSHIP, CTZN.SRC_EXTRACT_TS AS LAST_CHNG_DATE, 
                ROW_NUMBER() OVER (PARTITION BY CTZN.CUSTOMER_ID, CTZN.ADDNL_CITIZENSHIP ORDER BY CTZN.SRC_EXTRACT_TS DESC) AS RNK, 
                CTZN.SRC_SYS_INST_ID, CTZN.SRC_SYS_ID, CTZN.SRC_EVT_TYP_FLG  FROM LS_CDB_IND_ADDNL_CTZN CTZN 
                INNER JOIN LS_CDB_INDIVIDUAL_TBL IDV ON IDV.CUSTOMER_ID=CTZN.CUSTOMER_ID""")
            df_idvcntry_fifth.createOrReplaceTempView("df_idvcntry_fifth");
            if 4 in debug: df_idvcntry_fifth.show(2, truncate=False)

            # df_idvcntry_sixth
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + "Processing IDV_CNTRY - df_idvcntry_sixth - Step 6 of 10")
            df_idvcntry_sixth = spark.sql("""SELECT LAST_CHNG_DATE , CUSTOMER_ID, ADDNL_CITIZENSHIP AS CTRY_CODE, 
                SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_fifth WHERE RNK = 1""")
            df_idvcntry_sixth.createOrReplaceTempView("df_idvcntry_sixth");
            if 4 in debug: df_idvcntry_sixth.show(2, truncate=False)

            # df_idvcntry_seventh
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + "Processing IDV_CNTRY - df_idvcntry_seventh - Step 7 of 10")
            df_idvcntry_seventh = spark.sql("""
                SELECT LAST_CHNG_DATE, CUSTOMER_ID, CTRY_CODE, 'COUNTRY OF CITIZENSHIP' AS CTRY_TYPE_NAME, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_fourth 
                UNION SELECT LAST_CHNG_DATE, CUSTOMER_ID, CTRY_CODE, 'COUNTRY OF NATIONALITY' AS CTRY_TYPE_NAME, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_fourth 
                UNION SELECT LAST_CHNG_DATE, CUSTOMER_ID, CTRY_CODE, 'COUNTRY OF CITIZENSHIP' AS CTRY_TYPE_NAME, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_sixth 
                UNION SELECT LAST_CHNG_DATE, CUSTOMER_ID, CTRY_CODE, 'COUNTRY OF NATIONALITY' AS CTRY_TYPE_NAME, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_sixth 
                UNION SELECT LAST_CHNG_DATE, CUSTOMER_ID, CTRY_CODE, 'TAX RESIDENCY COUNTRY' AS CTRY_TYPE_NAME, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_second""")
            df_idvcntry_seventh.createOrReplaceTempView("df_idvcntry_seventh");
            if 4 in debug: df_idvcntry_seventh.show(2, truncate=False)

            # df_idvcntry_eighth
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + "Processing IDV_CNTRY - df_idvcntry_eighth - Step 8 of 10")
            df_idvcntry_eighth = spark.sql("""SELECT LAST_CHNG_DATE , CUSTOMER_ID, CTRY_CODE, CTRY_TYPE_NAME, 
                ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID, CTRY_CODE, CTRY_TYPE_NAME ORDER BY LAST_CHNG_DATE DESC) AS RNK, 
                SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG FROM df_idvcntry_seventh""")
            df_idvcntry_eighth.createOrReplaceTempView("df_idvcntry_eighth");
            if 4 in debug: df_idvcntry_eighth.show(2, truncate=False)

            # df_idvcntry_ninth
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + "Processing IDV_CNTRY - df_idvcntry_ninth - Step 9 of 10")
            df_idvcntry_ninth = spark.sql("""SELECT LAST_CHNG_DATE, CUSTOMER_ID, LTRIM(RTRIM(CTRY_CODE)) AS CTRY_OF_NATIONALTY, 
                LTRIM(RTRIM(CTRY_TYPE_NAME)) AS CTRY_TYPE_NAME, SRC_SYS_INST_ID, SRC_SYS_ID, SRC_EVT_TYP_FLG 
                FROM df_idvcntry_eighth WHERE RNK=1 AND (CTRY_CODE IS NOT NULL OR LTRIM(RTRIM(CTRY_CODE)) != '')""")
            df_idvcntry_ninth.createOrReplaceTempView("df_idvcntry_ninth");
            if 4 in debug: df_idvcntry_ninth.show(2, truncate=False)

            # df_combined_val
            vsl_tbl_df.createOrReplaceTempView("VSL_TBL");
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CNTRY - combined_val_vw - Step 10 of 10")
            df_combined_val = spark.sql(
                """SELECT LAST_CHNG_DATE, CUSTOMER_ID AS SRC_IDV_ID_VAL, CTRY_OF_NATIONALTY, CTRY_TYPE_NAME, 
                IF(ISNULL(LAST_CHNG_DATE), current_date(), LAST_CHNG_DATE) AS SRC_SYS_CHG_DT, 
                SRC_EVT_TYP_FLG, vsl1.VSL_LKP_VALUE as CNTRY_TYPE_ID, vsl1.VSL_LKP_VALUE as CNTRY_ID, 
                NULL as SRC_ROWID, NULL as LAST_UPDATE_DATE, SRC_SYS_INST_ID, SRC_SYS_ID 
                FROM df_idvcntry_ninth 
                LEFT OUTER JOIN VSL_TBL vsl1 on vsl1.VALUE_SET_NAME='RD COUNTRY TYPES' 
                AND vsl1.COLUMN_1_STRING_VALUE=CTRY_TYPE_NAME 
                AND vsl1.OUT_ACTION_CODE NOT IN ('STOP','BLOCK') 
                LEFT OUTER JOIN VSL_TBL vsl2 on vsl2.VALUE_SET_NAME='RD COUNTRY CODES' 
                AND vsl2.COLUMN_1_STRING_VALUE=CTRY_OF_NATIONALTY 
                AND vsl2.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')""");
            if 4 in debug: df_combined_val.show(2, truncate=False)
            df_combined_val.createOrReplaceTempView("combined_val_vw");

            if df_combined_val.count() > 0:
                print("\n"+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_CNTRY - generating Sub_document");
                idv_cntry_sub_doc = spark.sql("""SELECT DISTINCT SRC_IDV_ID_VAL AS CUSTOMER_ID, SRC_SYS_INST_ID, SRC_SYS_ID, 
                    STRUCT(CNTRY_TYPE_ID, CNTRY_ID, SRC_SYS_CHG_DT, SRC_EVT_TYP_FLG) AS IDV_CNTRY FROM combined_val_vw 
                    WHERE SRC_IDV_ID_VAL IS NOT NULL ORDER BY SRC_SYS_CHG_DT DESC""").distinct()
                if 3 in debug: idv_cntry_sub_doc.show(2, truncate=False)

                df_individual_asso_trn_1_drv = df_individual_asso_trn_1_drv.join(idv_cntry_sub_doc,
                                                 on=['CUSTOMER_ID', 'SRC_SYS_INST_ID', 'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())
                if 5 in debug:
                    df_individual_asso_trn_1_drv.printSchema()
                    df_individual_asso_trn_1_drv.show(2, truncate=False)
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records to process TRN: IDV_CNTRY")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS',
                                             'Generated 0 (zero) TRN - IDV_CNTRY records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: 0 records are available for TRN: IDV_CNTRY")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_CNTRY has no/zero valid records');
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process - IDV_CNTRY")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process - IDV_CNTRY');
    ## End of IDV_CNTRY
    ## ------------------------------------------------------------------------------------------------------ ##


    ##################### Begin of IDV_SRC (C_LD_BBBB_CCC_IDV_SRC) #####################
    print("\n"+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating IDV_SRC")
    writeApplicationStatusToFile(spark,path_kafka_hdfs, 'STARTED', 'Initiating IDV_SRC');
    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - custadr_idv_df - Step 1 of 3")
    try:
        str_addr_cnt = df_street_addr.count()
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - Records found :"+str(str_addr_cnt))
        if str_addr_cnt > 0:
            custadr_idv_df = spark.sql("""SELECT SA.CUSTOMER_ID, 
                CASE WHEN LTRIM(RTRIM('CDB')) = '' OR ISNULL('CDB') THEN 'Not Available' ELSE 'CDB' END AS t_DATA_SRC_ID, 
                CASE WHEN PA.CUST_ADDR_STATUS IN ('C', 'A') THEN TRIM(SA.ADDRESS_LINE_1) 
                     WHEN PA.CUST_ADDR_STATUS='R' THEN NULL END AS POSTAL_ADDR_LN_1_TX, 
                CASE WHEN PA.CUST_ADDR_STATUS IN ('C','A') THEN TRIM(SA.ADDRESS_LINE_2) 
                     WHEN PA.CUST_ADDR_STATUS='R' THEN NULL END AS POSTAL_ADDR_LN_2_TX, 
                CASE WHEN PA.CUST_ADDR_STATUS IN ('C', 'A') THEN TRIM(SA.ADDRESS_LINE_3) 
                     WHEN PA.CUST_ADDR_STATUS='R' THEN NULL END AS POSTAL_ADDR_LN_3_TX, 
                CASE WHEN PA.CUST_ADDR_STATUS IN ('C', 'A') THEN TRIM(SA.ADDRESS_LINE_4) 
                     WHEN PA.CUST_ADDR_STATUS='R' THEN NULL END AS POSTAL_ADDR_LN_4_TX, 
                CASE WHEN PA.CUST_ADDR_STATUS IN ('C','A') THEN TRIM(SA.POSTCODE) 
                     WHEN PA.CUST_ADDR_STATUS='R' THEN NULL END AS POSTAL_POSTCODE_TX,  
                TRIM(SA.ADDRESS_LINE_1) AS RES_ADDR_LN_1_TX,  TRIM(SA.ADDRESS_LINE_2) AS RES_ADDR_LN_2_TX,  
                TRIM(SA.ADDRESS_LINE_3) AS RES_ADDR_LN_3_TX,  TRIM(SA.ADDRESS_LINE_4) AS RES_ADDR_LN_4_TX, 
                TRIM(SA.POSTCODE) AS RES_POSTCODE_TX, IND.CTRY_OF_RESIDENCE AS RES_CNTRY_CD, SA.SRC_SYS_ID, 
                CASE WHEN LTRIM(RTRIM(IND.CTRY_OF_RESIDENCE)) = '' OR ISNULL(IND.CTRY_OF_RESIDENCE) THEN 'Not Available' 
                ELSE IND.CTRY_OF_RESIDENCE END AS t_RES_CNTRY_CD, SA.SRC_SYS_INST_ID, CONCAT(IND.BANK_ID,LTRIM(TRIM(IND.CUSTOMER_OWNED_BY))) AS SRC_BRND_ID, 
                CASE WHEN LTRIM(RTRIM(CONCAT(IND.BANK_ID,LTRIM(RTRIM(IND.CUSTOMER_OWNED_BY))))) = '' OR ISNULL(CONCAT(IND.BANK_ID,LTRIM(RTRIM(IND.CUSTOMER_OWNED_BY)))) THEN 'Not Available' 
                ELSE CONCAT(IND.BANK_ID,LTRIM(RTRIM(IND.CUSTOMER_OWNED_BY))) END AS t_SRC_BRND_ID, 
                CASE WHEN PA.CUST_ADDR_STATUS IN ('C','A') THEN TRIM( SA.ADDRESS_LINE_5) 
                     WHEN PA.CUST_ADDR_STATUS='R' THEN NULL END AS POSTAL_ADDR_LN_5_TX, 
                TRIM(SA.ADDRESS_LINE_5) AS RES_ADDR_LN_5_TX, PA.SRC_EXTRACT_TS, PA.SRC_EVT_TYP_FLG, 
                ROW_NUMBER() OVER (PARTITION BY SA.CUSTOMER_ID ORDER BY CASE 
                    WHEN PA.CUST_ADDR_STATUS ='C' THEN 1 
                    WHEN PA.CUST_ADDR_STATUS ='A' THEN 2 
                    WHEN PA.CUST_ADDR_STATUS ='R' THEN 3 
                    ELSE 4 END ASC) AS rnk 
                FROM LS_CDB_STREET_ADDR SA 
                INNER JOIN LS_CDB_INDIVIDUAL_TBL IND ON SA.CUSTOMER_ID = IND.CUSTOMER_ID 
                LEFT JOIN LS_PARTY_AGREEMENT_VW PA ON PA.CUSTOMER_ID = SA.CUSTOMER_ID""");
            if 4 in debug: custadr_idv_df.show(2, truncate=False)
            custadr_idv_df.createOrReplaceTempView("CUSTADR_IDV_TBL")
            custadr_idv_cnt = custadr_idv_df.count()
        else: custadr_idv_cnt = 0
    except: custadr_idv_cnt = 0

    try:
        if custadr_idv_cnt > 0:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - Step 2 of 3")
            try:
                idv_src_tmp_df = spark.sql("""SELECT idvsrc.CUSTOMER_ID, idvsrc.POSTAL_ADDR_LN_1_TX AS addressLine1, idvsrc.POSTAL_ADDR_LN_2_TX
                        AS addressLine2, idvsrc.POSTAL_ADDR_LN_3_TX AS addressLine3, idvsrc.POSTAL_ADDR_LN_4_TX AS addressLine4,
                        idvsrc.POSTAL_ADDR_LN_5_TX AS addressLine5, idvsrc.POSTAL_POSTCODE_TX AS addressLine6,
                        NULL AS POSTAL_CITY_CD, NULL AS POSTAL_CNTRY_CD, idvsrc.RES_ADDR_LN_1_TX, idvsrc.RES_ADDR_LN_2_TX,
                        idvsrc.RES_ADDR_LN_3_TX, idvsrc.RES_ADDR_LN_4_TX, idvsrc.RES_ADDR_LN_5_TX,  
                        idvsrc.SRC_EXTRACT_TS, idvsrc.SRC_EVT_TYP_FLG, vsl1.VSL_LKP_VALUE as VSL_DATA_SRC_ID, RES_POSTCODE_TX, 
                        NULL AS RES_CITY_CD, SRC_SYS_ID, SRC_SYS_INST_ID, vsl2.VSL_LKP_VALUE as VSL_SRC_BRAND_ID, 
                        vsl3.VSL_LKP_VALUE as VSL_RES_CNTRY_CD 
                        FROM (SELECT * FROM CUSTADR_IDV_TBL WHERE rnk=1) idvsrc
                        LEFT OUTER JOIN VSL_TBL vsl1 ON vsl1.VALUE_SET_NAME='RD SRC SYSTEMS'
                             AND vsl1.COLUMN_1_STRING_VALUE=idvsrc.t_DATA_SRC_ID
                             AND vsl1.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')
                        LEFT OUTER JOIN VSL_TBL vsl2 ON vsl2.VALUE_SET_NAME='RD BRAND NAMES'
                             AND vsl2.COLUMN_1_STRING_VALUE=idvsrc.t_SRC_BRND_ID
                            AND vsl2.OUT_ACTION_CODE NOT IN ('STOP', 'BLOCK')
                        LEFT OUTER JOIN VSL_TBL vsl3 ON vsl3.VALUE_SET_NAME='RD COUNTRY CODES'
                             AND vsl3.COLUMN_1_STRING_VALUE=idvsrc.t_RES_CNTRY_CD
                             AND vsl3.OUT_ACTION_CODE NOT IN ('STOP','BLOCK')""")
                if 4 in debug: idv_src_tmp_df.show(2, truncate=False)
                idv_src_tmp_cnt = idv_src_tmp_df.count()

                # ######################### DR_ADDRESS_CODE ##########################################
                # print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - Step 3 of 6 - Creating index column")
                # idv_src_df_with_col = create_join_column(idv_src_tmp_df)
                # print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Created index column")
                # if 4 in debug:
                #     idv_src_df_with_col.show(truncate=False)
                #     idv_src_df_with_col.printSchema() ## -To remove
                #
                # print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - Step 4 of 6 - Creating payload for Address Doctor API")
                # idv_src_df_pyl = create_addr_dr_payload(idv_src_df_with_col)
                # if 4 in debug: idv_src_df_pyl.show(truncate=False)
                # print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Payload created")
                #
                # # idv_src_df.printSchema()
                # dummy_df = spark.sql("SELECT '' as DUMMY")
                # overwrite_to_mongo(dummy_df, mongo_dr_address_collection)
                #
                # ##Performing SOAP_API URL POST Activity
                # print(str(datetime.now().strftime(
                #     '%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - Step 5 of 6 - Calling Address Doctor API")
                # idv_src_dr_addr_verfy_df = idv_src_df_pyl.foreachPartition(call_addr_dr_api)
                #
                # ## Read data from MongoDB Collection and join it with the derieved data
                # print(str(datetime.now().strftime(
                #     '%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - Step 6 of 7 - Fetching DR_Address data from MongodB");
                # addr_dr_verified_df = read_from_mongo(spark, mongo_dr_address_collection)
                # addr_dr_verified_df.show()
                # idv_src_dr_addr_df = idv_src_df_with_col. \
                #     drop("addressLine1", "addressLine2", "addressLine3", "addressLine4", "addressLine5", "addressLine6"). \
                #     join(addr_dr_verified_df, "ColumnIndex"). \
                #     drop("ColumnIndex"). \
                #     replace('?', 'null')
                #
                # print(str(datetime.now().strftime(
                #     '%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - Step - test - printing the results after Dr_address. \nCount:" + str(idv_src_dr_addr_df.count()));
                #
                # idv_src_dr_addr_df.show()
                # idv_src_dr_addr_df.createOrReplaceTempView("IDV_SRC_TMP_DF_VW")
                # ######################### END_OF_DR_ADDRESS ##########################################

                idv_src_tmp_df.createOrReplaceTempView("IDV_SRC_TMP_DF_VW")
                # , VSL_SRC_BRAND_ID, VSL_RES_CNTRY_CD
            except: idv_src_tmp_cnt = 0

            if idv_src_tmp_cnt > 0:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing IDV_SRC - idv_src_sub_doc - Step 3 of 3")
                # #--VSL_SRC_BRAND_ID
                idv_src_sub_doc = spark.sql("""SELECT DISTINCT CUSTOMER_ID, VSL_DATA_SRC_ID, SRC_SYS_INST_ID, SRC_SYS_ID, 
                addressLine1, addressLine2, addressLine3, addressLine4, addressLine5, 
                struct(RES_ADDR_LN_1_TX, RES_ADDR_LN_2_TX, RES_ADDR_LN_3_TX, RES_ADDR_LN_4_TX, RES_ADDR_LN_5_TX,
                POSTAL_CITY_CD, POSTAL_CNTRY_CD, RES_POSTCODE_TX, RES_CITY_CD, VSL_SRC_BRAND_ID, SRC_EXTRACT_TS, 
                SRC_EVT_TYP_FLG) AS IDV_SRC FROM IDV_SRC_TMP_DF_VW WHERE CUSTOMER_ID IS NOT NULL ORDER BY SRC_EXTRACT_TS DESC""")
                if 3 in debug: idv_src_sub_doc.show(2, truncate=False)

                # # RES_ADDR_LN_1_TX, RES_ADDR_LN_2_TX, RES_ADDR_LN_3_TX, RES_ADDR_LN_4_TX, RES_POSTCODE_TX, RES_CITY_CD,
                # # POSTAL_ADDR_LN_1_TX, POSTAL_ADDR_LN_2_TX, POSTAL_ADDR_LN_3_TX, POSTAL_ADDR_LN_4_TX, POSTAL_POSTCODE_TX,
                # # POSTAL_CNTRY_CD, POSTAL_CITY_CD, DATA_SRC_ID AS VSL_DATA_SRC_ID, SRC_BRND_ID AS VSL_SRC_BRAND_ID,
                # # SRC_SYS_CHG_DT, GENDER_VAL, MARITAL_STAT_VAL, HOME_OWN_STAT_VAL,
                # # DISABILITY_STAT_VAL, EMP_STAT_VAL, MKT_SEG_VAL, PREF_CONT_CHNL_VAL, SALUTATION_NM, SUFFIX_NM,
                # # JOB_TITLE_VAL, RES_CNTRY_CD, CNTRY_OF_RES_VAL, CNTRY_OF_RISK_VAL, CNTRY_OF_BIRTH_VAL,
                # # CNTRY_OF_TAX_DOM_VAL, CNTRY_OF_CITIZEN_VAL, CNTRY_OF_NATIONALITY_VAL, PPT_ISSUING_CNTRY_VAL

                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Joining IDV_SRC with Individual_ASSO")
                df_individual_asso_trn_1_drv=df_individual_asso_trn_1_drv.join(idv_src_sub_doc,
                                       on=['CUSTOMER_ID', 'SRC_SYS_INST_ID', 'SRC_SYS_ID'], how='left').where(col("CUSTOMER_ID").isNotNull())
                if 5 in debug:
                    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: IDV_SRC Schema & sample data")
                    df_individual_asso_trn_1_drv.printSchema()
                    df_individual_asso_trn_1_drv.show(2, truncate=False)
            else:
                print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generated 0 records to process TRN: IDV_SRC")
                writeApplicationStatusToFile(spark,path_kafka_hdfs, 'ZERO_RECORDS', 'Generated 0 (zero) TRN - IDV_SRC records');
        else:
            print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process TRN: IDV_SRC")
            writeApplicationStatusToFile(spark,path_kafka_hdfs, 'NO_RECORDS', 'IDV_SRC - Unable to process');
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Unable to process: IDV_SRC")
        writeApplicationStatusToFile(spark,path_kafka_hdfs, 'Data_Issue', 'Unable to process: IDV_SRC');
    ## End of IDV_SRC

    print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Generating final collection - df_individual_asso_trn_1")
    if 5 in debug:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: printing df_individual_asso_trn_1_drv dataframe")
        df_individual_asso_trn_1_drv.printSchema()
        df_individual_asso_trn_1_drv.show(2, truncate=False)

    df_individual_asso_trn_1 = df_individual_asso_trn_1.join(df_individual_asso_trn_1_drv,
                                 on=['CUSTOMER_ID', 'SRC_SYS_INST_ID', 'SRC_SYS_ID'], how='left')\
                                .where(col("CUSTOMER_ID").isNotNull()).distinct()
    if 5 in debug:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: printing df_individual_asso_trn_1 dataframe")
        df_individual_asso_trn_1.printSchema()
        df_individual_asso_trn_1.show(2, truncate=False)
## ---------------------------- End of Second function------------------------------------------------------##

def create_join_column(df):
    new_schema = StructType(df.schema.fields + [StructField("ColumnIndex", LongType(), False), ])
    return df.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema)

def create_addr_dr_payload(idv_src_df):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Creating Address_Docter payload");
        idv_src_df = idv_src_df.withColumn("addr_dr_payload_xml", concat(lit(
            '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" '
            'xmlns:iav="http://www.informatica.com/dis/ws/IAV_UK_Interactive">'
            '<soap:Header/><soap:Body><iav:Operation><iav:AddressLine1>'),
            "addressLine1", lit('</iav:AddressLine1><iav:AddressLine2>'),
            "addressLine2", lit('</iav:AddressLine2><iav:AddressLine3>'),
            "addressLine3", lit('</iav:AddressLine3><iav:AddressLine4>'),
            "addressLine4", lit('</iav:AddressLine4><iav:AddressLine5>'),
            "addressLine5", lit('</iav:AddressLine5><iav:AddressLine6>'),
            "addressLine6", lit('</iav:AddressLine6><iav:SEQ_NUM>'),
            "ColumnIndex",  lit('</iav:SEQ_NUM></iav:Operation></soap:Body></soap:Envelope>')))
        # idv_src_df.show(truncate=False)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + "Returning created payload")
        return idv_src_df
    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())

def call_addr_dr_api(rows):
    # for row in rows.collect():
    results=[];
    for row in rows:
        try:
            spark = SparkSession.builder.config("spark.sql.warehouse.dir", "/tmp/hive-site.xml").getOrCreate()
            print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + 'Calling Address doctor API at: ' + addr_dr_api_url)
            addr_dr_soap_api_url = addr_dr_api_url
            headers = {'content-type': 'application/xml'}
            addr_dr_payload = str(row.addr_dr_payload_xml)

            ## TESTING - 5 - BEGIN
            print("\n addr_dr_soap_api_url: "+ addr_dr_soap_api_url + "addr_dr_payload: " + addr_dr_payload)
            ## TESTING - 5 - END

            addr_dr_api_resp = requests.post(addr_dr_soap_api_url, data=addr_dr_payload, headers=headers)

            print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + 'Response received from address doctor API. Extracting tags and creating dataframe for update ')
            response_namespace = {"iav": "http://www.informatica.com/dis/ws/IAV_UK_Interactive"}
            addr_dr_verified_fields = ET.fromstring(addr_dr_api_resp.content)
            uprn = addr_dr_verified_fields.find('.//iav:UPRN', response_namespace).text
            mail_score = addr_dr_verified_fields.find('.//iav:Mail_Score', response_namespace).text
            match_code = addr_dr_verified_fields.find('.//iav:Match_Code', response_namespace).text
            result_percent = addr_dr_verified_fields.find('.//iav:Result_Percent', response_namespace).text
            address_line1 = addr_dr_verified_fields.find('.//iav:AddressLine1', response_namespace).text
            address_line2 = addr_dr_verified_fields.find('.//iav:AddressLine2', response_namespace).text
            address_line3 = addr_dr_verified_fields.find('.//iav:AddressLine3', response_namespace).text
            address_line4 = addr_dr_verified_fields.find('.//iav:AddressLine4', response_namespace).text
            address_line5 = addr_dr_verified_fields.find('.//iav:AddressLine5', response_namespace).text
            post_code = addr_dr_verified_fields.find('.//iav:PostCode', response_namespace).text
            column_index = addr_dr_verified_fields.find('.//iav:SEQ_NUM', response_namespace).text

            addr_dr_verified_fields_list = [
                (uprn, mail_score, match_code, result_percent, address_line1, address_line2, address_line3,
                 address_line4, address_line5, post_code, column_index)]

            schema_addr_dr_df = StructType(
                [StructField("uprn", StringType(), True),         StructField("mail_score", StringType(), True),
                 StructField("match_code", StringType(), True),   StructField("result_percent", StringType(), True),
                 StructField("addressLine1", StringType(), True), StructField("addressLine2", StringType(), True),
                 StructField("addressLine3", StringType(), True), StructField("addressLine4", StringType(), True),
                 StructField("addressLine5", StringType(), True), StructField("post_code", StringType(), True),
                 StructField("ColumnIndex", StringType(), True)])

            addr_dr_verified_df = spark.createDataFrame(addr_dr_verified_fields_list, schema=schema_addr_dr_df)
            print("\n" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: " + "Address verified Records retrieved. Dataframe created")
            addr_dr_verified_df.show(5, truncate=False)
            addr_dr_verified_df = addr_dr_verified_df.na.fill('null')

            print("Capturing in arrays")
            results.append(addr_dr_verified_df);
            print("\n Results: str(results) - " + str(results))
            write_to_mongo(addr_dr_verified_df, mongo_dr_address_collection)

        except Py4JJavaError as ex:
            print("An error occurred: " + ex.java_exception.toString())
    # print(str(results))
    # print("Results: " + str(results.toDF()))
    # write_to_mongo(results.toDF(), mongo_dr_address_collection)

def read_individual_asso_records(spark, record_from_kafka_hdfs, LS_MAP_IDV_ASSO, refDataFrame):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching LS_CDB_INDIVIDUAL records from "+ LS_MAP_IDV_ASSO);
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Received records");

        ls_cdb_individual = record_from_kafka_hdfs.filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_IDV_ASSO)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records-2");
        ls_cdb_individual.show()

        ls_cdb_individual.createOrReplaceTempView("IND_ASSO_VW")
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records-3");

        idv_asso_df = spark.sql("""SELECT CUSTOMER_ID, SRC_EVT_TYP_FLG, SRC_EXTRACT_TS, SRC_SYS_ID, SRC_SYS_INST_ID,
            COALESCE(CUSTOMER_TITLE, '') AS CUSTOMER_TITLE, COALESCE(CUSTOMER_SURNAME, '') AS CUSTOMER_SURNAME, 
            COALESCE(CUSTOMER_FIRST_NAM, '') AS CUSTOMER_FIRST_NAM,
            COALESCE(CUSTOMER_FORENAM_1, '') AS CUSTOMER_FORENAM_1, COALESCE(CUSTOMER_FORENAM_2, '') AS CUSTOMER_FORENAM_2,
            COALESCE(CUSTOMER_FORENAM_3, '') AS CUSTOMER_FORENAM_3, COALESCE(CUSTOMER_NAME_SFX, '') AS CUSTOMER_NAME_SFX,
            COALESCE(CUSTOMER_GENDER, '') AS CUSTOMER_GENDER, COALESCE(CUSTOMER_DOB, '') AS CUSTOMER_DOB,
            COALESCE(DEPEND_CHILD_CNT, '') AS DEPEND_CHILD_CNT, COALESCE(BANK_ID, '') AS BANK_ID,
            COALESCE(CUSTOMER_OWNED_BY, '') AS CUSTOMER_OWNED_BY, 
            COALESCE(CTRY_OF_RESIDENCE, '') AS CTRY_OF_RESIDENCE,
            COALESCE(CTRY_OF_BIRTH, '') AS CTRY_OF_BIRTH, 
            COALESCE(PLACE_OF_BIRTH, '') AS PLACE_OF_BIRTH,
            COALESCE(TAX_NUMBER, '') AS TAX_NUMBER, 
            COALESCE(GRADUATION_DATE, '') AS GRADUATION_DATE,
            COALESCE(DECEASED_DATE, '') AS DECEASED_DATE, 
            COALESCE(FIRST_CONTACT_DATE, '') AS FIRST_CONTACT_DATE,
            COALESCE(BUSINESS_PHONE_NO, '') AS BUSINESS_PHONE_NO, 
            COALESCE(HOME_PHONE_NO, '') AS HOME_PHONE_NO,
            COALESCE(MOBILE_PHONE_NO, '') AS MOBILE_PHONE_NO, 
            COALESCE(LAST_CHNG_DATE, '') AS LAST_CHNG_DATE,
            COALESCE(PASSPORT_CTRY, '') AS PASSPORT_CTRY, COALESCE(PASSPORT_EXP_DATE, '') AS PASSPORT_EXP_DATE,
            COALESCE(PASSPORT_NO, '') AS PASSPORT_NO, COALESCE(HOME_OWNER_IND, '') AS HOME_OWNER_IND,
            COALESCE(DISABILITY_CODE, '') AS DISABILITY_CODE, COALESCE(CUSTOMER_STATUS, '') AS CUSTOMER_STATUS,
            COALESCE(SEGMENT_CODE, '') AS SEGMENT_CODE, COALESCE(OCCUPATION_CODE, '') AS OCCUPATION_CODE,
            COALESCE(NATIONAL_INS_NO, '') AS NATIONAL_INS_NO, COALESCE(CTRY_OF_NATIONALTY, '') AS CTRY_OF_NATIONALTY 
            FROM IND_ASSO_VW 
            WHERE CUSTOMER_ID IS NOT NULL""")

        idv_asso_df.show()
        ls_cdb_individual =idv_asso_df

        # GROUP BY CUSTOMER_ID, SRC_SYS_ID, SRC_SYS_INST_ID
        #     ORDER BY SRC_EXTRACT_TS DESC SRC_EVT_TYP_FLG ASC
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Processing data for IN/UP/DL records")

        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Printing - LS_CDB_INDIVIDUAL dataframe");
        idv_asso_cnt = ls_cdb_individual.count()
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+" :: LS_CDB_INDIVIDUAL count: "+str(idv_asso_cnt))
        if 1 in debug: ls_cdb_individual.show(2, truncate=False)
        return ls_cdb_individual
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable at "+ LS_MAP_IDV_ASSO +", aborting the process")
        exit(1)
        # ls_cdb_individual = getEmptyDataFrame(refDataFrame, LS_MAP_IDV_ASSO)
        # return ls_cdb_individual


def read_street_addr_records(spark, record_from_kafka_hdfs, LS_MAP_STREET_ADDR, refDataFrame):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching STREET_ADDRESS records")
        ls_cdb_street_addr = record_from_kafka_hdfs.select("CUSTOMER_ID", "CUSTOMER_ADDR_NO", "ADDRESS_LINE_1",
               "ADDRESS_LINE_2", "ADDRESS_LINE_3", "ADDRESS_LINE_4", "ADDRESS_LINE_5", "POSTCODE",
               "POSTCODE_EXEMPT", "ADDRESS_UPDT_COUNT", "SRC_SYS_INST_ID", "SRC_SYS_ID"). \
            filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_STREET_ADDR)
        # print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Sampling street address data")
        ls_cdb_street_addr_cnt = ls_cdb_street_addr.count()
        if 1 in debug: ls_cdb_street_addr.show(2, truncate=False)
        return ls_cdb_street_addr
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable Street_addr_records")
        ls_cdb_street_addr = getEmptyDataFrame(refDataFrame, LS_MAP_STREET_ADDR)
        if 1 in debug: ls_cdb_street_addr.show()
        return ls_cdb_street_addr

def read_gurn_records(spark, record_from_kafka_hdfs, LS_MAP_GURN, refDataFrame):
    try:
        # Topic: LS_CDB_GURN: LS_CDB_GURN
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching GURN records - "+LS_MAP_GURN)
        ls_cdb_gurn = record_from_kafka_hdfs.select("GURN", "CUSTOMER_ID", "SRC_SYS_INST_ID", "SRC_SYS_ID").\
            filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_GURN)
        ls_cdb_gurn_cnt = ls_cdb_gurn.count()
        if 1 in debug: ls_cdb_gurn.show(2, truncate=False)
        return ls_cdb_gurn
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable GURN_records")
        ls_cdb_gurn = getEmptyDataFrame(refDataFrame, LS_MAP_GURN)
        if 1 in debug: ls_cdb_gurn.show()
        return ls_cdb_gurn

def read_idv_email(spark, record_from_kafka_hdfs, LS_MAP_EMAIL_ADDR, refDataFrame):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Initiating reading Electronic Address records")
        df_ls_cdb_elec_addr = record_from_kafka_hdfs.select("CUSTOMER_ID", "E_ADDR_TYPE", "UPDATE_DATE_TIME", "E_ADDR",
                "SRC_SYS_ID","SRC_SYS_INST_ID", "SRC_EXTRACT_TS","SRC_EVT_TYP_FLG","SRC_LS_MAP").\
            filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_EMAIL_ADDR)
        # log.info(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching - LS_CDB_ELECTRONIC_ADDRESS dataframe");
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Completed reading Electronic Address records")
        df_ls_cdb_elec_addr_cnt = df_ls_cdb_elec_addr.count()
        if 1 in debug: df_ls_cdb_elec_addr.show(2, truncate=False);
        return df_ls_cdb_elec_addr
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable for ELECTRONIC ADDRESS")
        df_ls_cdb_elec_addr = getEmptyDataFrame(refDataFrame, LS_MAP_EMAIL_ADDR)
        if 1 in debug: df_ls_cdb_elec_addr.show()
        return df_ls_cdb_elec_addr

def read_ind_relationship(spark, record_from_kafka_hdfs, LS_MAP_IND_RLSHP, refDataFrame):
    try:
        # df_relation: (LS_CDB_PARTY_RELATIONSHIP+LS_CDB_INDIVIDUAL)
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching - ls_cdb_ind_relationship records")
        ls_cdb_ind_relationship = record_from_kafka_hdfs.select("SRC_SYS_ID", "SRC_SYS_INST_ID", "SRC_EVT_TYP_FLG",
            "SRC_EXTRACT_TS", "CUSTOMER_ID_1", "REL_TYPE", "CUSTOMER_ID_2", "DATE_CREATED",
            "SRC_LS_MAP").filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_IND_RLSHP)
        ls_cdb_ind_relationship_cnt = ls_cdb_ind_relationship.count()
        if 1 in debug: ls_cdb_ind_relationship.show(2, truncate=False)
        return ls_cdb_ind_relationship
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable for LS_CDB_PARTY_RELATIONSHIP")
        ls_cdb_ind_relationship = getEmptyDataFrame(refDataFrame, LS_MAP_IND_RLSHP)
        if 1 in debug: ls_cdb_ind_relationship.show()
        return ls_cdb_ind_relationship

def read_idvFlag(spark, record_from_kafka_hdfs, LS_MAP_IDV_FLAG, refDataFrame):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching - IDV_FLAG records")
        ls_cdb_idv_flag = record_from_kafka_hdfs.select("CUSTOMER_ID", "UPDATE_DATE_TIME", "INDICATOR_TYPE",
            "INDICATOR_VALUE", "SRC_SYS_INST_ID", "SRC_SYS_ID", "SRC_EXTRACT_TS", "SRC_EVT_TYP_FLG"). \
            filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_IDV_FLAG)
        ls_cdb_idv_flag_cnt = ls_cdb_idv_flag.count()
        if 1 in debug: ls_cdb_idv_flag.show(2, truncate=False)
        return ls_cdb_idv_flag
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable for LS_CDB_PARTY_RELATIONSHIP")
        ls_cdb_idv_flag = getEmptyDataFrame(refDataFrame, LS_MAP_IDV_FLAG)
        if 1 in debug: ls_cdb_idv_flag.show()
        return ls_cdb_idv_flag

def read_idvTax(spark, record_from_kafka_hdfs, LS_MAP_IDV_TAX, refDataFrame):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching - IDV_TAX records")
        ls_cdb_idv_tax = record_from_kafka_hdfs.select("CUSTOMER_ID", "CTRY_OF_TAXATION", "SRC_EXTRACT_TS", "TAX_CRE_TIMESTAMP",
                "SRC_SYS_INST_ID", "SRC_SYS_ID", "SRC_EVT_TYP_FLG").\
            filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_IDV_TAX)
        ls_cdb_idv_tax_cnt = ls_cdb_idv_tax.count()
        if 1 in debug: ls_cdb_idv_tax.show(2, truncate=False)
        return ls_cdb_idv_tax
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable for "+LS_MAP_IDV_TAX)
        ls_cdb_idv_tax = getEmptyDataFrame(refDataFrame, LS_MAP_IDV_TAX)
        if 1 in debug: ls_cdb_idv_tax.show()
        return ls_cdb_idv_tax

def read_idvCtzn(spark, record_from_kafka_hdfs, LS_MAP_IDV_CTZN, refDataFrame):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching - IDV_CTZN records")
        ls_cdb_idv_ctzn = record_from_kafka_hdfs.select("CUSTOMER_ID", "ADDNL_CITIZENSHIP", "SRC_SYS_INST_ID", "SRC_SYS_ID",
            "SRC_EXTRACT_TS", "SRC_EVT_TYP_FLG").filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_IDV_CTZN)
        ls_cdb_idv_ctzn_cnt = ls_cdb_idv_ctzn.count()
        if 1 in debug: ls_cdb_idv_ctzn.show(2, truncate=False)
        return ls_cdb_idv_ctzn
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable for "+LS_MAP_IDV_CTZN)
        ls_cdb_idv_ctzn = getEmptyDataFrame(refDataFrame, LS_MAP_IDV_CTZN)
        if 1 in debug: ls_cdb_idv_ctzn.show()
        return ls_cdb_idv_ctzn

def read_idvpartyAgrmnt(spark, record_from_kafka_hdfs, LS_MAP_IDV_PARTY_AGMT, refDataFrame):
    try:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Fetching - IDV_PARTY_AGREEMENT records")
        df_party_agrmnt = record_from_kafka_hdfs.select("CUSTOMER_ID", "CUST_ADDR_STATUS", "BRANCH_NO", "ACCOUNT_NO",
            "CUSTOMER_NAME_KEY", "CUSTOMER_TYPE", "BANK_ID", "ACCOUNT_TYPE_CODE", "CUST_ACC_ADDR_ID", "RELATION_TYPE",
            "RELATION_SOURCE", "CONSENT_TO_CTCT", "SRC_SYS_ID", "SRC_SYS_INST_ID", "SRC_EXTRACT_TS", "SRC_EVT_TYP_FLG").\
            filter(record_from_kafka_hdfs['SRC_LS_MAP'] == LS_MAP_IDV_PARTY_AGMT)
        df_party_agrmnt_cnt = df_party_agrmnt.count()
        if 1 in debug: df_party_agrmnt.show(2, truncate=False)
        return df_party_agrmnt
    except:
        print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " :: Records unavailable for "+LS_MAP_IDV_PARTY_AGMT);
        df_party_agrmnt = getEmptyDataFrame(refDataFrame, LS_MAP_IDV_PARTY_AGMT)
        if 1 in debug: df_party_agrmnt.show()
        return df_party_agrmnt

def getEmptyDataFrame(refDataFrame, filtertable):
    tempDF = refDataFrame.filter(refDataFrame['SRC_LS_MAP'] == filtertable)
    return tempDF

def write_to_mongo(df, collection):
    try:
        global db_url, db_database;
        df.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append")\
            .option('uri', db_url) \
            .option("database", db_database) \
            .option("collection", collection) \
            .save()
        print(collection + " collection written by SPARK on MongoDB! with " + str(df.count()) + " records")
    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())

def overwrite_to_mongo(df, collection):
    try:
        global db_url, db_database;
        df.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("overwrite") \
            .option('uri', db_url) \
            .option("database", db_database) \
            .option("collection", collection) \
            .save()
        print(collection + " collection written by SPARK on MongoDB! with " + str(df.count()) + " records")
    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())

def read_from_mongo(spark, collection):
    try:
        global db_url, db_database;
        df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option('uri', db_url) \
            .option('database', db_database) \
            .option("collection", collection) \
            .load()
        print(collection + " from MongoDB loaded to SPARK!")
        return df
    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())

def getMongoDBConfiguration():
    try:
        global db_url, db_database;
        config = configparser.ConfigParser()
        config.read(os.getcwd() + '/dbConfiguration.ini')
        db_url = config.get('mongodb-configuration', 'db_url')
        db_database = config.get('mongodb-configuration', 'db_database')
        return db_url, db_database
    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())

def produce_on_kafka(df, topic_name):
    try:
        df.selectExpr("CAST(value as STRING)"). \
            write. \
            format("kafka"). \
            option("kafka.bootstrap.servers", kafka_bootstrap_server). \
            option("topic", topic_name). \
            save()
    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())

def set_spark_logger(spark):
    try:
        log4j_logger = spark._jvm.org.apache.log4j
        spark_log = log4j_logger.LogManager.getLogger(__name__)
        return spark_log

    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())

def set_app_logger(spark):
    try:
        log4j_logger = spark._jvm.org.apache.log4j
        app_logger = log4j_logger.LogManager.getLogger("appLogger")
        app_logger.info("Setting log level as INFO at application level")
        return app_logger

    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())


def read_from_mongo_pipeline(spark, collection, pipeline):
    try:
        global db_url, db_database;
        df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option('uri', db_url) \
            .option('database', db_database) \
            .option("collection", collection) \
            .option("pipeline", pipeline) \
            .load()
        print(str(datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S')) + " :: Loaded Source Collection: " + collection + " from MongoDB ")
        return df

    except Py4JJavaError as ex:
        print("An error occurred: " + ex.java_exception.toString())

def writeApplicationStatusToFile(spark,data_file, status, message):
    try:

        cSchema = StructType([StructField("filename", StringType()) \
                                 , StructField("status", StringType()) \
                                 , StructField("date", DateType())
                                 , StructField("lslayer", StringType()) \
                                 , StructField("messare", StringType())                              \
                              ])

        dateTimeObj = datetime.now()
        objlist=[]
        objlist.append({"filename":data_file,"status":status,"date":dateTimeObj,"lslayer":"ls_cdb_driver_customer","message":message})
        df = spark.createDataFrame(objlist, schema=cSchema)
        write_to_mongo(df,"ods_entdb_cdb_audit_details")
    except Py4JJavaError as ex:
        raise ex


if __name__ == '__main__':
    main()
