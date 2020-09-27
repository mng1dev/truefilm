from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from utils.constants import wikipedia_abstracts as w
from utils.constants import movies_metadata as m
from pyspark.sql import Window


def match_movies_with_wikipedia_abstracts(
        df_movies_metadata: DataFrame,
        df_wikipedia_abstracts: DataFrame
) -> DataFrame:
    """
    Method that attempts to pair movies with their related Wikipedia article.
    Pairing is performed by checking equality of the 'title_clean' column:
    - if movies have only one matching article, this article is considered a good match
    - if movies have more than one matching article, they are processed to find the best match among them
    :param df_movies_metadata: movies_metadata preprocessed dataset
    :param df_wikipedia_abstracts: wikipedia_abstracts preprocessed dataset
    :return: movies with their paired Wikipedia article.
    """
    df_movies_with_wikipedia_abstracts = df_movies_metadata.join(
        df_wikipedia_abstracts,
        df_movies_metadata[f'{w.TITLE}_clean'] == df_wikipedia_abstracts[f'{w.TITLE}_clean'],
        how='left'
    ).drop(df_wikipedia_abstracts[w.TITLE]).drop(df_wikipedia_abstracts[f'{w.TITLE}_clean'])

    matches_by_movie = df_movies_with_wikipedia_abstracts \
        .groupBy(df_movies_metadata.columns) \
        .agg(f.count(w.URL).alias('count_urls'))

    df_movies_single_match = df_movies_with_wikipedia_abstracts.join(
        matches_by_movie.filter('count_urls = 1').drop('count_urls'),
        df_movies_metadata.columns,
    )

    df_movies_multiple_matches = df_movies_with_wikipedia_abstracts.join(
        matches_by_movie.filter('count_urls > 1').drop('count_urls'),
        df_movies_metadata.columns
    )

    df_movies_multiple_matches_best_match = find_best_link_for_movie(df_movies_multiple_matches)

    return df_movies_single_match.unionByName(df_movies_multiple_matches_best_match).select(
        m.TITLE, m.BUDGET, m.YEAR, m.REVENUE, m.RATING, m.RATIO, m.GENRES, m.PRODUCTION_COMPANIES, w.URL, w.ABSTRACT
    ).dropDuplicates(subset=[m.TITLE, m.BUDGET, m.YEAR, m.REVENUE, m.RATING, m.RATIO, m.GENRES, m.PRODUCTION_COMPANIES])


def find_best_link_for_movie(df_movies_with_abstracts: DataFrame) -> DataFrame:
    """
    When movies have more than one corresponding article, the match relies on the 'url_type' column.
    If the record has one link where url_type is equal to '<release_year>_film',
    that record is considered the best possible match.
    If the record has one link where url_type is equal to 'film', that record is considered a good match.
    All the records that have no articles where 'url_type' follows this pattern are discarded, since it's not possible
    to pair them reliably.
    :param df_movies_with_abstracts: The dataset containing movies paired with more than one Wikipedia article
    :return: the best article match for each of the movies
    """
    df_movies_only_film_matches = df_movies_with_abstracts.filter(
        (df_movies_with_abstracts[f'{w.URL}_type'] == 'film') |
        (df_movies_with_abstracts[f'{w.URL}_type'] == f.concat(f.col(m.YEAR), f.lit('_film')))
    ).withColumn(
        'match',
        f.when(df_movies_with_abstracts[f'{w.URL}_type'] == f.concat(f.col(m.YEAR), f.lit('_film')), 2)
         .when(df_movies_with_abstracts[f'{w.URL}_type'] == 'film', 1)
         .otherwise(0)
    )

    match_window = Window.partitionBy(m.TITLE, m.YEAR).orderBy(f.col('match').desc())
    df_movies_best_film_matches = df_movies_only_film_matches.withColumn(
        'match_rank',
        f.rank().over(match_window)
    ).filter('match > 0 and match_rank = 1')

    return df_movies_best_film_matches.drop('match', 'match_rank')
