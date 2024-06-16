import logging.config
import sys
from pyspark.sql.functions import *

logging.config.fileConfig('Properties/configuration/logging.config')
loggers=logging.getLogger('validate')


def get_current_date(spark):
    try:
        loggers.warning("start the get current date !")
        output=spark.sql("""select current_date""")

        # print("validating spark object on current -- "+str(output.first()))
        logging.warning("validating spark object on current -- "+str(output.first()))

    except Exception as exp:
        logging.error("error in getting the exception in get_current_date! "+str(exp))
        # print(str(exp))
        raise
    else:
        logging.warning("validation done ! go ahead......")
        # sys.exit(1)


def print_schema(df, dfName):
    try:
        loggers.warning("print schema method executing.....{} ".format(dfName))
        sch = df.schema.fields

        for i in sch:
            loggers.info(f"/{i}")

    except Exception as exp:
        loggers.error("An error occured at printSchema :: ",str(exp))

        raise
    else:
        loggers.info("print_schema done .......")


def check_for_nulls(df, dfName):
    try:
        loggers.info("check for nulls method executing ...... for {} ".format(dfName))
        check_null_df= df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.colums])

    except Exception as e:
        loggers.error("An error occur while working on check_for_nulls ",str(e))

    return check_null_df
