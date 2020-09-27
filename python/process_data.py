from dao import MoviesMetadataDAO, WikiAbstractsDAO
from analysis.movies_metadata import prepare_movies_data
from analysis.wikipedia_abstracts import prepare_wikipedia_abstracts
from analysis.match_datasets import match_movies_with_wikipedia_abstracts
from analysis.reports import *
from utils.spark import write_dataframe_to_db, get_active_spark_session
from utils.constants import wikipedia_abstracts as w
from utils.constants import movies_metadata as m
from pyspark.sql import functions as f


def process_data():
    df_movies_metadata = MoviesMetadataDAO().get_data()
    df_movies_metadata_prepared = prepare_movies_data(df_movies_metadata).repartition(f'{m.TITLE}_clean').cache()
    print(f"Movies Metadata Records: Before Data Preparation {df_movies_metadata.count()} - After {df_movies_metadata_prepared.count()}")

    df_wikipedia_abstracts = WikiAbstractsDAO().get_data()
    df_wikipedia_abstracts_prepared = prepare_wikipedia_abstracts(df_wikipedia_abstracts).repartition(f'{w.TITLE}').cache()
    print(f"Wikipedia Abstracts Records: {df_wikipedia_abstracts_prepared.count()}")

    df_movies_with_abstracts = match_movies_with_wikipedia_abstracts(
        df_movies_metadata=df_movies_metadata_prepared,
        df_wikipedia_abstracts=df_wikipedia_abstracts_prepared
    ).repartition(10).cache()
    df_movies_with_abstracts.count()

    print("Write top movies table - Start")
    df_top_movies = df_movies_with_abstracts.drop(m.GENRES).orderBy(f.col(m.RATIO).desc()).limit(1000)
    write_dataframe_to_db(df_top_movies, 'movies')
    print("Write top movies table - OK")

    print("Write top movies table - Start")
    df_top_movies = df_movies_with_abstracts.drop(m.GENRES).orderBy(f.col(m.RATIO).desc())
    write_dataframe_to_db(df_top_movies, 'movies_all')
    print("Write top movies table - OK")

    print("Write top genres table - Start")
    df_top_genres = generate_top_performing_genres_report(df_movies_with_abstracts).orderBy(f.col(m.RATIO))
    write_dataframe_to_db(df_top_genres, 'top_genres')
    print("Write top genres table - OK")

    print("Write top production companies table - Start")
    df_top_companies = generate_top_performing_production_companies_report(df_movies_with_abstracts).orderBy(f.col(m.RATIO))
    write_dataframe_to_db(df_top_companies, 'top_production_companies')
    print("Write top production companies table - OK")


if __name__ == '__main__':
    process_data()
