import unittest
from pyspark.sql import SparkSession, DataFrame


class BasePySparkTestCase(unittest.TestCase):
    """
    The BasePySparkTestCase class only contains support methods that need to be used by both PySparkTestCase and
    ReusedPySparkTestCase classes. This class is not meant to directly extend your TestCase classes.
    """

    def assertDataFrameEqual(self, df1: DataFrame, df2: DataFrame) -> None:
        """
        This function compares two DataFrames in order to establish if they contain the same data.
        First, the function checks whether the two DataFrames have the same columns, and if so also checks that they
        contain exactly the same data by subtracting each DataFrame to the other.
        :param df1: The first DataFrame
        :param df2: The second DataFrame
        """

        df1_columns = sorted(df1.dtypes, key=lambda x: x[0])
        df2_columns = sorted(df2.dtypes, key=lambda x: x[0])

        self.assertEqual(
            len(df1_columns),
            len(df2_columns),
            msg='The number of columns of these DataFrames is different.'
        )

        for df1_column, df2_column in zip(df1_columns, df2_columns):
            self.assertEqual(
                df1_column[0],
                df2_column[0],
                msg=f'Different columns in the same position: df1[\'{df1_column[0]}\'], df2[\'{df2_column[0]}\']'
            )
            self.assertEqual(
                df1_column[1],
                df2_column[1],
                msg=f'Different column types for column {df1_column[0]}: df1[\'{df1_column[1]}\'],'
                    f' df2[\'{df2_column[1]}\']'
            )

        self.assertEqual(
            df1.count(),
            df2.count(),
            msg='The two DataFrames have different row counts!'
        )

        df1_subtract_df2_count = df1.subtract(df2).count()
        df2_subtract_df1_count = df2.subtract(df1).count()
        self.assertEqual(df1_subtract_df2_count, 0, msg='The second DataFrames has more rows than the first one.')
        self.assertEqual(df2_subtract_df1_count, 0, msg='The first DataFrames has more rows than the second one.')


class PySparkTestCase(BasePySparkTestCase):
    """
    The PySparkTestCase class is meant to be used if and only if
    there is the need to have a dedicated SparkSession for each single test.
    The SparkSession can be accessed from each test function using 'self.spark'.
    """

    def setUp(self) -> None:
        super().setUp()
        self.spark: SparkSession = SparkSession.builder.master('local[*]').getOrCreate()

    def tearDown(self) -> None:
        super().tearDown()
        self.spark.stop()


class ReusedPySparkTestCase(BasePySparkTestCase):
    """
    The ReusedPySparkTestCase class is meant to be used when
    there is no need to have a dedicated SparkSession for each single test.
    The SparkSession can be accessed from each test function using 'self.spark'.
    """

    spark: SparkSession = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.spark: SparkSession = SparkSession.builder.master('local[*]').getOrCreate()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        if cls.spark is not None:
            cls.spark.stop()
