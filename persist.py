import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime as date

logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger('Persist')

def data_hive_persist(spark, df, dfname, partitionBy, mode):
    try:
        loggers.warning('persisting the data into Hive Table for {} '.format(dfname))
        loggers.warning('lets create a database...')
        spark.sql("""create database if not exists cities""")
        spark.sql("""use cities""")
        loggers.warning("No writing {} into hive_table by {} ".format(df,partitionBy))
        df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)

    except Exception as exp:
        loggers.error("An error occurred while processing data_hive_persist :: ",str(exp))
        raise
    else:
        loggers.warning("Data successfully persisted into hive table")

def data_hive_persist_presc(spark, df, dfname, partitionBy, mode):
    try:
        loggers.warning('persisting the data into Hive Table for {} '.format(dfname))
        loggers.warning('lets create a database...')
        spark.sql("""create database if not exists presc""")
        spark.sql("""use presc""")
        loggers.warning("No writing {} into hive_table by {} ".format(df,partitionBy))
        df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)

    except Exception as exp:
        loggers.error("An error occurred while processing data_hive_persist :: ",str(exp))
        raise
    else:
        loggers.warning("Data successfully persisted into hive table")

def persist_data_mysql(spark,df,dfName,url,dbtable,mode,user,password):
    try:
        loggers.warning("executing the data_persist_mysql method... {} ".format(dfName))
        df.write.format("jdbc").option("url",url).option("dbtable",dbtable)\
        .mode(mode).option("user",user).option("password",password).save()

    except Exception as exp:
        loggers.error("An error occured @ persist_data_mysql method :: ",str(exp))
        raise
    else:
        loggers.warning("Persist_data_mysql method executing successfully .. into {} ".format(dbtable))
