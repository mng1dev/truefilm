from pyspark.sql import SparkSession
import os
from utils.constants.common import *


def get_active_spark_session() -> SparkSession:
    """
    Utility method that returns the currently active Spark Session, or creates a new one with Spark defaults.
    :return: a usable Spark session.
    """
    return SparkSession.builder.getOrCreate()


def write_dataframe_to_db(df_output: str, table_name: str, mode='overwrite'):
    """
    Function to write a Spark DataFrame to a target postgresql database
    :param df_output: the DataFrame to write
    :param table_name: the table name on the destination db
    :param mode: the table write mode (e.g. 'overwrite', 'append')
    """
    db_user = os.getenv(POSTGRES_USER)
    db_pass = os.getenv(POSTGRES_PASSWORD)
    db_host = os.getenv(POSTGRES_HOST)
    df_output.write.mode(mode).jdbc(
        url=f"jdbc:postgresql://{db_host}:5432/",
        table=table_name,
        properties={'user': db_user, 'password': db_pass, 'driver': 'org.postgresql.Driver'}
    )
