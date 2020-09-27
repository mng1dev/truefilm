from utils.testing import ReusedPySparkTestCase
from utils.constants import movies_metadata as m
from analysis.reports import *


class TestReports(ReusedPySparkTestCase):
    def test_generate_top_performing_genres_report(self):
        df_input = self.spark.createDataFrame(
            [
                [['Comedy', 'Drama'], 5.0, 10.0],
                [['Drama'], 1.0, 5.0]
            ],
            [m.GENRES, m.BUDGET, m.REVENUE]
        )

        df_expected = self.spark.createDataFrame(
            [
                ['Drama', 6.0, 15.0, 0.4],
                ['Comedy', 5.0, 10.0, 0.5]
            ],
            ['genre', f'sum_{m.BUDGET}', f'sum_{m.REVENUE}', m.RATIO]
        )

        df_result = generate_top_performing_genres_report(df_input)

        self.assertDataFrameEqual(df_result, df_expected)

    def test_generate_top_performing_genres_report_ignore_empty_array_rows(self):
        df_input = self.spark.createDataFrame(
            [
                [['Comedy', 'Drama'], 5.0, 10.0],
                [['Drama'], 1.0, 5.0],
                [[], 1000.0, 5000.0],
                [None, 1000.0, 5000.0],
            ],
            [m.GENRES, m.BUDGET, m.REVENUE]
        )

        df_expected = self.spark.createDataFrame(
            [
                ['Drama', 6.0, 15.0, 0.4],
                ['Comedy', 5.0, 10.0, 0.5]
            ],
            ['genre', f'sum_{m.BUDGET}', f'sum_{m.REVENUE}', m.RATIO]
        )

        df_result = generate_top_performing_genres_report(df_input)

        self.assertDataFrameEqual(df_result, df_expected)

    def test_generate_top_performing_production_companies_report(self):
        df_input = self.spark.createDataFrame(
            [
                [['Tristar', 'Columbia'], 5.0, 10.0],
                [['Columbia'], 1.0, 5.0]
            ],
            [m.PRODUCTION_COMPANIES, m.BUDGET, m.REVENUE]
        )

        df_expected = self.spark.createDataFrame(
            [
                ['Columbia', 6.0, 15.0, 0.4],
                ['Tristar', 5.0, 10.0, 0.5]
            ],
            ['production_company', f'sum_{m.BUDGET}', f'sum_{m.REVENUE}', m.RATIO]
        )

        df_result = generate_top_performing_production_companies_report(df_input)

        self.assertDataFrameEqual(df_result, df_expected)