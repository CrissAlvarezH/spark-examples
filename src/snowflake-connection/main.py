import os

from pyspark.sql import SparkSession


def init_spark():
    # build spark session
    spark = SparkSession.builder.appName("MyApp").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_logger(spark, name="MY-LOGGER"):
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(name)
    return logger


def get_snowflake_opts():
    credentials = {
        "format": "net.snowflake.spark.snowflake",
        "sfURL": os.getenv("SNOWFLAKE_URL"),
        "sfRegion": os.getenv("SNOWFLAKE_REGION"),
        "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "useRegion": bool(os.getenv("SNOWFLAKE_USE_REGION", "true")),
        "query_tag": os.getenv("SNOWFLAKE_QUERY_TAG", "testing"),
    }
    return credentials


if __name__ == "__main__":
    spark = init_spark()
    logger = get_logger(spark)

    # Load data from snowflake
    options = get_snowflake_opts()

    logger.warn("Ser variable")
    query = "SELECT *, getvariable('var_test') AS VAR FROM test_spark LIMIT 10;"

    logger.warn("Load data from snowflake")
    df = (
        spark.read
            .format(options['format'])
            .options(**options)
            .option("preactions", 'SET "var_test" = \'valor de prueba\'')
            .option("query", query)
            .load()
    )

    df.show()
