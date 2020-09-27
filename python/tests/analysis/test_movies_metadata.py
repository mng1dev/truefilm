from utils.testing import ReusedPySparkTestCase
from analysis.movies_metadata import prepare_movies_data
from utils.constants.movies_metadata import *
import json


class TestMoviesMetadata(ReusedPySparkTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.input = self.spark.read.csv(
            'tests/fixtures/sample_movies_metadata.csv'
            , header=True
            , escape='"'
            , quote='"'
        )

    def test_process_movies_data_output_columns(self):
        df_result = prepare_movies_data(self.input)

        output_columns = [
            TITLE,
            GENRES,
            PRODUCTION_COMPANIES,
            RELEASE_DATE,
            BUDGET,
            REVENUE,
            RATING,
            YEAR,
            RATIO
        ]

        for column in output_columns:
            self.assertTrue(column in df_result.columns, f"{column} is not here")

    def test_process_metadata_cleaning_titles(self):
        df_titles = prepare_movies_data(self.input).select(TITLE, f'{TITLE}_clean').collect()

        expected_results = {
            'Toy Story': 'toy story',
            'Jumanji': 'jumanji',
            'Grumpier Old Men': 'grumpier old men',
            'L.I.E.': 'l.i.e.'
        }

        for row in df_titles:
            self.assertEqual(expected_results[row[TITLE]], row[f'{TITLE}_clean'])

    def test_process_metadata_no_budget_and_revenue_anomalies(self):
        df_result = prepare_movies_data(self.input)

        records_with_zero_budget_and_revenue = df_result.filter(f'{BUDGET} = 0 and {REVENUE} = 0').count()

        self.assertEqual(records_with_zero_budget_and_revenue, 0)

    def test_process_metadata_no_ratio_anomalies(self):
        df_result = prepare_movies_data(self.input)

        records_with_zero_budget_or_revenue_and_nonnull_ratio = df_result.filter(
            f'({BUDGET} = 0 OR {REVENUE} = 0) AND {RATIO} is not null'
        ).count()

        self.assertEqual(records_with_zero_budget_or_revenue_and_nonnull_ratio, 0)

    def test_process_metadata_json_string_to_string_array_transformation(self):
        df_result = prepare_movies_data(self.input)

        first_raw_record = self.input.orderBy(TITLE).first()
        first_processed_record = df_result.orderBy(TITLE).first()

        movie_genres_structs = json.loads(first_raw_record[GENRES].replace("'", "\""))
        movie_genres = [
            item['name'] for item in movie_genres_structs
        ]

        self.assertEqual(
            sorted(first_processed_record[GENRES]),
            sorted(movie_genres)
        )
