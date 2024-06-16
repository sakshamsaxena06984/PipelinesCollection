import os
import sys
# below import use of calculating program execution time
from time import perf_counter

import get_env_variables as gav
from create_spark import get_spark_obj
from validate import get_current_date, print_schema, check_for_nulls
import logging
import logging.config
from data_processing import *
from data_processing import data_clean
from ingest import load_files, display_show, df_count
from data_transfromation import data_report1, data_report2
from extraction import extract_files
from persist import *

logging.config.fileConfig('Properties/configuration/logging.config')

start_time = perf_counter()


def main():
    try:
        logging.info('I am the driver applications.........')
        print("this is the main function defination")
        # print(gav.appName)
        # print(gav.header)
        logging.info('Calling the spark object.........  ')
        spark = get_spark_obj(gav.envn, gav.appName)

        # print("object create..... ",spark)
        logging.info('object created ......   ' + str(spark))
        logging.info('validating the spark object')
        get_current_date(spark)
        print("=================================================")
        # print(os.listdir(gav.src_olap))
        for file in os.listdir(gav.src_olap):
            print("file is " + file)
            file_dir = gav.src_olap + '/' + file
            print(file_dir)
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        logging.info("reading the file format is {} ".format(file_format))

        df_city = load_files(spark=spark,
                             file_dir=file_dir,
                             file_format=file_format,
                             header=header,
                             inferSchema=inferSchema)
        logging.info("displaying dataframe {} ".format(df_city))
        display_show(df_city, 'df_city')

        logging.info("validating the dataframe......")
        logging.info("total number of records in the dataframe " + str(df_count(df_city, 'df_city')))

        # ------------------ working for oltp section files
        # it is for the oltp section
        for file2 in os.listdir(gav.src_oltp):
            print("file is " + file2)
            file_dir = gav.src_oltp + '/' + file2
            print(file_dir)
            if file2.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file2.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        df_fact = load_files(spark=spark,
                             file_dir=file_dir,
                             file_format=file_format,
                             header=header,
                             inferSchema=inferSchema)

        logging.info("displaying dataframe {} ".format(df_fact))
        display_show(df_fact, 'df_fact')

        logging.info("validating the dataframe......")
        logging.info("total number of records in the dataframe " + str(df_count(df_fact, 'df_fact')))

        #  data processing ------------
        logging.info("data processing..........")

        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)
        print("type of new data frames are : ", type(df_city_sel), type(df_presc_sel))

        display_show(df_city_sel, 'df_city')
        display_show(df_presc_sel, 'df_fact')

        logging.info("validating schema for dataframes.....")
        print_schema(df_city_sel, 'df_city_sel')
        print_schema(df_presc_sel, 'df_presc_sel')

        logging.info("checking for null values in dataframes... after processing ")
        # check_df = check_for_nulls(df_presc_sel,'df_fact')
        # display_show(check_df,'df_fact')

        logging.info("data_transformation executing....")

        df_report_1 = data_report1(df_city_sel, df_presc_sel)
        logging.info("displaying the df_report_1")
        # display_show(df_report_1,'data_report_1')

        logging.info("displaying data_report2 method.... ")

        df_report_2 = data_report2(df_presc_sel)
        display_show(df_report_2, 'data_report_2')

        logging.info("extracting files to Output .....")
        city_path = gav.city_path
        extract_files(df_report_1, 'orc', city_path, 1, False, 'snappy')

        presc_path = gav.presc_path
        extract_files(df_report_2, 'parquet', presc_path, 2, False, 'snappy')

        logging.info("extraction files to output completed..... ")

        # loggers.info("writing into hive table")
        # data_hive_persist(spark=spark,df=df_report_1,dfname='df_city',partitionBy='state_name',mode='append')
        # data_hive_persist_presc(spark=spark,df=df_report_2,dfname='df_presc',partitionBy='presc_state',mode='append')

        logging.info("successfully written into hive")
        logging.info("Now write {} into MYSQL ".format(df_report_1))

        persist_data_mysql(spark=spark,df=df_report_1,dfName='df_city', url='jdbc:mysql://localhost:3306/datapipeline2',
                           dbtable='city_df',mode='append',user=gav.user,password=gav.password)

        logging.info("successfully data inserted into table......")










    except Exception as exp:
        print("An error occur when calling main function : ", str(exp))
        sys.exit(1)


if __name__ == "__main__":
    main()
    end_time = perf_counter()
    total_time = end_time - start_time
    logging.info(f"total amount of time taken via process : {total_time:.2f} seconds")
    logging.info('Applications name .......   ')
