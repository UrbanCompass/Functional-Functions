from .logger import logger

def get_data_as_pandas(spark, query, prompt, lowercase_columns=False):
    try:
        logger.info(f"start query {prompt} ...")
        df = spark.sql(query)
        pandas_df = df.toPandas()
        if lowercase_columns:
            pandas_df.columns = map(str.lower, pandas_df.columns)
        return pandas_df
    finally:
        logger.info("finished!")
