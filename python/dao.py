from pyspark.sql import SparkSession, DataFrame
from utils.spark import get_active_spark_session
from utils.constants.wikipedia_abstracts import WIKIPEDIA_ABSTRACTS_FILE_NAME
from utils.constants.movies_metadata import MOVIES_METADATA_FILE_NAME


class BaseDAO(object):
    def __init__(self):
        self.spark: SparkSession = get_active_spark_session()


class WikiAbstractsDAO(BaseDAO):
    def get_data(self) -> DataFrame:
        return self.spark.read.format('xml') \
            .option('rowtag', 'doc') \
            .option('inferschema', 'true') \
            .option('samplingratio', 0.01) \
            .load(f'/input/{WIKIPEDIA_ABSTRACTS_FILE_NAME}')


class MoviesMetadataDAO(BaseDAO):
    def get_data(self) -> DataFrame:
        return self.spark.read.csv(f'/input/{MOVIES_METADATA_FILE_NAME}',
                                   header=True, quote='"', escape='"')
