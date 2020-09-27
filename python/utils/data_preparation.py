from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def clean_title(df_input: DataFrame, title_column: str) -> DataFrame:
    """
    Given a DataFrame and a the name of a String type column, this function will clean all its records by:
    - removing any braces and their contents, leaving only the "main" string
    - setting the strings to lowercase
    :param df_input: the DataFrame containing the column to clean
    :param title_column: the name of the column
    :return: the DataFrame with a new '<title_column>_clean' column, containing the cleaned values.
    """

    df_input_remove_braces = df_input.withColumn(
        f'{title_column}_clean',
        f.trim(f.regexp_replace(f.col(f'{title_column}'), r'\([^)]*\)', ''))
    )

    df_input_title_lower = df_input_remove_braces.withColumn(
        f'{title_column}_clean',
        f.trim(f.lower(f.col(f'{title_column}_clean')))
    )

    return df_input_title_lower
