import pyspark.sql
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("Unittest") \
      .getOrCreate()


path_1 = '../csv_files/dataset_one.csv'
path_2 = '../csv_files/dataset_two.csv'
output_file_path = '../client_data/my_file.csv'
country_to_drop = ['United States', 'France']


@pytest.fixture
def output_df(spark):
    return spark.read.csv(output_file_path, header=True, inferSchema=True)


def test_filter_country(output_df: pyspark.sql.DataFrame):
    ind1 = bool(output_df.filter(output_df.country.contains(country_to_drop[0])).collect())
    ind2 = bool(output_df.filter(output_df.country.contains(country_to_drop[1])).collect())
    assert ind1 is False
    assert ind2 is False

