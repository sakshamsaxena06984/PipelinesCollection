from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger("SparKSession")


def get_spark_obj(envn, appName):
    try:
        loggers.info("get_spark_object started!")
        if envn == 'DEV':
            master = 'local'
        else:
            master = 'Yarn'

        loggers.info("master is {} ".format(master))

        # spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
        spark = SparkSession.builder\
            .master(master)\
            .appName(appName)\
            .enableHiveSupport()\
            .config('spark.driver'
                                                                                                '.extraClassPath',
                                                                                                'mysql-connector-j'
                                                                                                '-8.3.0.jar').getOrCreate()
        return spark
    except Exception as exp:
        loggers.error("error occur in get_spark_obj : ",str(exp))
        raise
        print(str(exp))
    else:
        loggers.info("spark object created")
