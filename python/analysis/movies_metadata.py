from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from utils.constants.movies_metadata import *
from utils.data_preparation import clean_title


def prepare_movies_data(df_movies: DataFrame) -> DataFrame:
    """
    This function prepares the movies_metadata dataset.
    :param df_movies: the DataFrame containing movies_metadata dataset
    :return: a prepared dataset version
    """
    df_movies_slim = df_movies.select(
        TITLE,
        GENRES,
        PRODUCTION_COMPANIES,
        RELEASE_DATE,
        BUDGET,
        REVENUE,
        VOTE_AVERAGE
    )

    df_movies_cleaned = clean_movies_metadata(df_movies_slim)

    df_movies_parsed_genres = parse_json_column(df_movies_cleaned, GENRES)
    df_movies_parsed_production_companies = parse_json_column(df_movies_parsed_genres, PRODUCTION_COMPANIES)

    df_movies_year = df_movies_parsed_production_companies.withColumn(
        YEAR, f.year(f.col(RELEASE_DATE))
    )
    df_movies_ratio = generate_ratio_column(df_movies_year)

    return df_movies_ratio


def clean_movies_metadata(df_input: DataFrame) -> DataFrame:
    """
    Method that cleans the movies_metadata dataset by:
    - Cleaning all titles
    - Removing all records with anomalies (null titles, invalid revenue/budgets, etc.)
    - Casting budget and Revenue columns to double
    :param df_input: movies_metadata DataFrame
    :return: the cleaned DataFrame
    """
    string_columns = [TITLE]
    for string_column in string_columns:
        df_input = df_input.withColumn(string_column, f.trim(f.col(string_column)))
    df_input_cleaned = df_input \
        .filter(f.col(TITLE).isNotNull() & (f.col(TITLE) != '')) \
        .filter(f.col(GENRES).isNotNull()) \
        .filter(f.col(PRODUCTION_COMPANIES).isNotNull()) \
        .filter(f.col(REVENUE).isNotNull() & (f.col(REVENUE) >= 0)) \
        .filter(f.col(BUDGET).isNotNull() & (f.col(BUDGET) >= 0)) \
        .filter((f.col(BUDGET) != 0) | (f.col(REVENUE) != 0)) \
        .withColumnRenamed(VOTE_AVERAGE, RATING) \
        .withColumn(BUDGET, f.col(BUDGET).cast('double')) \
        .withColumn(REVENUE, f.col(REVENUE).cast('double'))
    return clean_title(df_input_cleaned, TITLE)


def parse_json_column(df_input: DataFrame, column: str) -> DataFrame:
    """
    Method that converts genres and proudction companies to string arrays
    :param df_input: movies_metadata DataFrame
    :param column: array column to transform
    :return: the input DataFrame with the column converted to a string array
    """
    df_json_column = df_input.withColumn(
        column, f.from_json(f.col(column), JSON_SCHEMA)
    )
    return df_json_column.withColumn(
        column, f.col(f'{column}.name')
    )


def generate_ratio_column(df_input: DataFrame) -> DataFrame:
    """
    Method that calculates the budget to revenue ratio if budget and revenue are both different than zero,
    will set it to null otherwise.
    :param df_input: movies_metadata DataFrame
    :return: the input DataFrame with the new ratio column
    """
    return df_input.withColumn(
        RATIO,
        f.when(
            ((f.col(REVENUE) == 0) | (f.col(BUDGET) == 0)),
            f.lit(None)
        ).otherwise(f.col(BUDGET)/f.col(REVENUE))
    )
