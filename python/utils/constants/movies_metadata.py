from pyspark.sql import types as t

MOVIES_METADATA_FILE_NAME = 'movies_metadata.csv'

# CSV Columns
BUDGET = 'budget'
GENRES = 'genres'
PRODUCTION_COMPANIES = 'production_companies'
REVENUE = 'revenue'
RELEASE_DATE = 'release_date'
TITLE = 'title'
VOTE_AVERAGE = 'vote_average'

# Added Columns
RATIO = 'ratio'
YEAR = 'year'
RATING = 'rating'

JSON_SCHEMA = t.ArrayType(t.StructType([
    t.StructField("id", t.IntegerType()), t.StructField("name", t.StringType())
]))

