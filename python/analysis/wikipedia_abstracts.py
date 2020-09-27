from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from utils.constants.wikipedia_abstracts import *
from utils.data_preparation import clean_title


def prepare_wikipedia_abstracts(df_abstracts: DataFrame) -> DataFrame:
    """
    This function prepares the wikipedia_abstracts dataset.
    :param df_abstracts: the DataFrame containing wikipedia_abstracts dataset
    :return: a prepared dataset version
    """
    df_abstracts_slim = df_abstracts.select(
        TITLE,
        URL,
        ABSTRACT
    )

    df_abstracts_cleaned = clean_wikipedia_abstracts(df_abstracts_slim)

    return df_abstracts_cleaned


def clean_wikipedia_abstracts(df_input: DataFrame) -> DataFrame:
    """
    Method that cleans the movies_metadata dataset by:
    - Cleaning all titles
    - Adding a URL type column containing any valuable information within braces in the Wikipedia Article URL,
    used to better identify its content (e.g. film, music_album, etc.)
    :param df_input: wikipedia_abstracts DataFrame
    :return: the cleaned DataFrame
    """
    for column in [TITLE, ABSTRACT, URL]:
        df_input = df_input.withColumn(column, f.trim(f.col(column)))

    df_input_no_title_prefix = df_input.withColumn(
        f'{TITLE}',
        f.regexp_replace(f.col(TITLE), rf'^{WIKIPEDIA_PREFIX}\s+', '')
    )

    df_input_clean_title = clean_title(df_input_no_title_prefix, TITLE)

    df_input_link_type = df_input_clean_title.withColumn(
        f'{URL}_type',
        f.regexp_extract(f.col(URL), r'\((.*?)\)', 1)
    )

    return df_input_link_type
