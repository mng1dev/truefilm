from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from utils.constants.movies_metadata import *


def generate_top_performing_genres_report(df_input: DataFrame) -> DataFrame:
    """
    Method that generates a report containing total budget and revenue, plus their ratio, for movie genres.
    :param df_input: the input DataFrame
    :return: The DataFrame containing the report
    """
    return generate_report(df_input, GENRES, 'genre')


def generate_top_performing_production_companies_report(df_input: DataFrame) -> DataFrame:
    """
    Method that generates a report containing total budget and revenue, plus their ratio, for production companies.
    :param df_input: the input DataFrame
    :return: The DataFrame containing the report
    """
    return generate_report(df_input, PRODUCTION_COMPANIES, 'production_company')


def generate_report(df_input: DataFrame, column_to_explode: str, exploded_column_name: str) -> DataFrame:
    """
    Method that generates a report containing total budget and revenue, plus their ratio, for a column
    which is obtained by exploding another one containing string arrays.
    :param df_input: the input DataFrame
    :param column_to_explode: the name of the column to explode
    :param exploded_column_name: the alias for the column once it is exploded
    :return: The DataFrame containing the report
    """
    df_input_nonempty_lists = df_input.filter(
        (f.col(column_to_explode).isNotNull()) &
        (f.size(f.col(column_to_explode)) > 0)
    )
    df_exploded = df_input_nonempty_lists.select(column_to_explode, BUDGET, REVENUE).withColumn(
        exploded_column_name, f.explode(column_to_explode))
    df_total_revenues_and_budgets = df_exploded\
        .groupBy(exploded_column_name).agg(f.sum(BUDGET).alias('sum_budget'), f.sum(REVENUE).alias('sum_revenue'))
    return df_total_revenues_and_budgets.withColumn(
        RATIO, f.col('sum_budget')/f.col('sum_revenue')
    )
