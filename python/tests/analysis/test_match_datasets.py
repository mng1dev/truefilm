from utils.testing import ReusedPySparkTestCase
from analysis.match_datasets import match_movies_with_wikipedia_abstracts
from utils.constants import movies_metadata as m
from utils.constants import wikipedia_abstracts as w


class TestMatchDatasets(ReusedPySparkTestCase):
    def test_match_movies_with_wikipedia_abstracts_single_match_on_title(self):
        df_movies_metadata = self.spark.createDataFrame(
            [
                ['A Title', 3000.0, 1995, 1500.0, 2.5, 2.0, ['Comedy'], ['Paramount'], 'a title']
            ],
            [
                m.TITLE, m.BUDGET, m.YEAR, m.REVENUE, m.RATING, m.RATIO, m.GENRES, m.PRODUCTION_COMPANIES,
                f'{m.TITLE}_clean'
            ]
        )

        df_wikipedia_abstracts = self.spark.createDataFrame(
            [
                ['A Title', 'http://somelink', 'SomeAbstract', 'a title', '']
            ],
            [w.TITLE, w.URL, w.ABSTRACT, f'{w.TITLE}_clean', f'{w.URL}_type']
        )

        df_result = match_movies_with_wikipedia_abstracts(
            df_movies_metadata=df_movies_metadata,
            df_wikipedia_abstracts=df_wikipedia_abstracts
        ).collect()

        self.assertEqual(len(df_result), 1)
        self.assertEqual(df_result[0][w.URL], 'http://somelink')
        self.assertEqual(df_result[0][w.ABSTRACT], 'SomeAbstract')

    def test_match_movies_with_wikipedia_abstracts_multiple_matches_keep_year_film_only(self):
        df_movies_metadata = self.spark.createDataFrame(
            [
                ['A Title', 3000.0, 1995, 1500.0, 2.5, 2.0, ['Comedy'], ['Paramount'], 'a title']
            ],
            [
                m.TITLE, m.BUDGET, m.YEAR, m.REVENUE, m.RATING, m.RATIO, m.GENRES, m.PRODUCTION_COMPANIES,
                f'{m.TITLE}_clean'
            ]
        )

        df_wikipedia_abstracts = self.spark.createDataFrame(
            [
                ['A Title', 'http://somelink(1995_film)', 'SomeAbstract', 'a title', '1995_film'],
                ['A Title', 'http://somelink(film)', 'SomeAbstract', 'a title', 'film']
            ],
            [w.TITLE, w.URL, w.ABSTRACT, f'{w.TITLE}_clean', f'{w.URL}_type']
        )

        df_result = match_movies_with_wikipedia_abstracts(
            df_movies_metadata=df_movies_metadata,
            df_wikipedia_abstracts=df_wikipedia_abstracts
        ).collect()

        self.assertEqual(len(df_result), 1)
        self.assertEqual(df_result[0][w.URL], 'http://somelink(1995_film)')

    def test_match_movies_with_wikipedia_abstracts_multiple_matches_keep_film_only(self):
        df_movies_metadata = self.spark.createDataFrame(
            [
                ['A Title', 3000.0, 1995, 1500.0, 2.5, 2.0, ['Comedy'], ['Paramount'], 'a title']
            ],
            [
                m.TITLE, m.BUDGET, m.YEAR, m.REVENUE, m.RATING, m.RATIO, m.GENRES, m.PRODUCTION_COMPANIES,
                f'{m.TITLE}_clean'
            ]
        )

        df_wikipedia_abstracts = self.spark.createDataFrame(
            [
                ['A Title', 'http://somelink', 'SomeAbstract', 'a title', ''],
                ['A Title', 'http://somelink(film)', 'SomeAbstract', 'a title', 'film']
            ],
            [w.TITLE, w.URL, w.ABSTRACT, f'{w.TITLE}_clean', f'{w.URL}_type']
        )

        df_result = match_movies_with_wikipedia_abstracts(
            df_movies_metadata=df_movies_metadata,
            df_wikipedia_abstracts=df_wikipedia_abstracts
        ).collect()

        self.assertEqual(len(df_result), 1)
        self.assertEqual(df_result[0][w.URL], 'http://somelink(film)')

    def test_match_movies_with_wikipedia_abstracts_multiple_matches_drop_records_with_no_film_url_type(self):
        df_movies_metadata = self.spark.createDataFrame(
            [
                ['A Title', 3000.0, 1995, 1500.0, 2.5, 2.0, ['Comedy'], ['Paramount'], 'a title']
            ],
            [
                m.TITLE, m.BUDGET, m.YEAR, m.REVENUE, m.RATING, m.RATIO, m.GENRES, m.PRODUCTION_COMPANIES,
                f'{m.TITLE}_clean'
            ]
        )

        df_wikipedia_abstracts = self.spark.createDataFrame(
            [
                ['A Title', 'http://somelink', 'SomeAbstract', 'a title', ''],
                ['A Title 2', 'http://somelink2', 'SomeAbstract', 'a title', ''],
            ],
            [w.TITLE, w.URL, w.ABSTRACT, f'{w.TITLE}_clean', f'{w.URL}_type']
        )

        num_results = match_movies_with_wikipedia_abstracts(
            df_movies_metadata=df_movies_metadata,
            df_wikipedia_abstracts=df_wikipedia_abstracts
        ).count()

        self.assertEqual(num_results, 0)

    def test_match_movies_with_wikipedia_abstracts_multiple_matches_keep_one_best_match_only(self):
        df_movies_metadata = self.spark.createDataFrame(
            [
                ['A Title', 3000.0, 1995, 1500.0, 2.5, 2.0, ['Comedy'], ['Paramount'], 'a title']
            ],
            [
                m.TITLE, m.BUDGET, m.YEAR, m.REVENUE, m.RATING, m.RATIO, m.GENRES, m.PRODUCTION_COMPANIES,
                f'{m.TITLE}_clean'
            ]
        )

        df_wikipedia_abstracts = self.spark.createDataFrame(
            [
                ['A Title', 'http://somelink(1995_film)', 'SomeAbstract', 'a title', '1995_film'],
                ['A Title', 'http://somelink(1995_film)', 'SomeAbstract', 'a title', '1995_film']
            ],
            [w.TITLE, w.URL, w.ABSTRACT, f'{w.TITLE}_clean', f'{w.URL}_type']
        )

        df_result = match_movies_with_wikipedia_abstracts(
            df_movies_metadata=df_movies_metadata,
            df_wikipedia_abstracts=df_wikipedia_abstracts
        ).collect()

        self.assertEqual(len(df_result), 1)