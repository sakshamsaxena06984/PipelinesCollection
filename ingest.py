import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Ingest')


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('load_file method started!...')
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).load(file_dir, header=True, inferSchema=True)

    except Exception as exp:
        logger.error("An error occur in file loading {} ", str(exp))
        raise
    else:
        logger.warning("dataframe created successfully which is {} -> ".format(file_format))

    return df


def display_show(df, dfName):
    df_show = df.show()

    return df_show


def df_count(df, dfName):
    try:
        logger.warning("count the records {} ".format(dfName))
        df_c = df.count()
    except Exception as exp:
        raise
    else:
        logger.info("count of records in df {} ".format(df_c))
    return df_c
