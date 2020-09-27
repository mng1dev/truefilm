from utils.testing import ReusedPySparkTestCase
from analysis.wikipedia_abstracts import prepare_wikipedia_abstracts
from utils.constants.wikipedia_abstracts import *


class TestWikipediaAbstracts(ReusedPySparkTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.input = self.spark.read.csv(
            'tests/fixtures/sample_wikipedia_abstracts.csv'
            , header=True
            , quote='"'
            , escape='"'
        )

    def test_prepare_wikipedia_abstracts_no_wikipedia_prefix_in_titles(self):
        df_result = prepare_wikipedia_abstracts(self.input).collect()

        for row in df_result:
            self.assertTrue(not row[TITLE].startswith(WIKIPEDIA_PREFIX))

    def test_prepare_wikipedia_abstracts_no_braces_in_clean_title(self):
        df_result_with_braces = prepare_wikipedia_abstracts(self.input)\
            .filter(f'{TITLE}_clean like "%anarchism%"').collect()

        self.assertEqual(df_result_with_braces[0][f'{TITLE}_clean'], 'anarchism')

    def test_prepare_wikipedia_abstracts_test_url_type_extraction(self):
        df_result_urltype = prepare_wikipedia_abstracts(self.input)\
            .filter(f'{TITLE}_clean like "%anarchism%"').collect()

        self.assertEqual(df_result_urltype[0][f'{URL}_type'], 'type_test')
