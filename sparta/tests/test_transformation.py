import pytest
from chispa.dataframe_comparer import *
from sparta.transformation import drop_duplicates, aggregation, format_timestamp, create_col_list
from pyspark.sql import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.master("local[*]").getOrCreate()

def test_drop_duplicates() -> None:
    source_data = [
            ('jose', '1', 'BA'),
            ('jose', '2', 'BA'),
            ('maria', '3', 'PA'),
            ('joao', '4', 'RJ')
    ]

    source_df = spark.createDataFrame(source_data, ['name', 'index', 'state'])

    actual_df = drop_duplicates(source_df, 'index', ['name', 'state'])

    expected_data = [
            ('jose', '2', 'BA'),
            ('maria', '3', 'PA'),
            ('joao', '4', 'RJ')
    ]

    expected_df = spark.createDataFrame(expected_data, ['name', 'index', 'state'])

    assert_df_equality(actual_df, expected_df)
    
def test_aggregation() -> None:
    source_data = [
            ('jose', 21, 'BA'),
            ('enzo', 11, 'BA'),
            ('maria', 32, 'PA'),
            ('joao', 44, 'RJ')
    ]

    source_df = spark.createDataFrame(source_data, ['name', 'age', 'state'])

    actual_df = aggregation(source_df, 'name', ['state'], {F.avg:'age'})

    expected_data = [
            ('jose', 21.0, 'BA'),
            ('maria', 32.0, 'PA'),
            ('joao', 44.0, 'RJ')
    ]

    expected_df = spark.createDataFrame(expected_data, ['name', 'age', 'state'])

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
    
def test_format_timestamp() -> None:
    source_data = [
            ('1', '2022-06-16', 2022),
            ('2', '2022-07-16', 2022),
            ('3', '2022-08-16', 2022),
            ('4', '2022-09-16', 2022)
    ]

    source_df = spark.createDataFrame(source_data, ['index', 'date', 'year'])

    actual_df = format_timestamp(source_df, ['date'])

    expected_data = [
           ('1', datetime(2022, 6, 16, 0, 0), 2022),
            ('2', datetime(2022, 7, 16, 0, 0), 2022),
            ('3', datetime(2022, 8, 16, 0, 0), 2022),
            ('4', datetime(2022, 9, 16, 0, 0), 2022)
    ]

    expected_df = spark.createDataFrame(expected_data, ['index', 'date', 'year'])

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
    
def test_create_col_list() -> None:
    source_data = [
            ('jose', 21, 'BA'),
            ('jose', 11, 'BA'),
            ('maria', 32, 'PA'),
            ('joao', 44, 'RJ')
    ]

    source_df = spark.createDataFrame(source_data, ['name', 'age', 'state'])

    actual_list = create_col_list(source_df, 'name')

    expected_list = ['jose', 'joao','maria']
    assert actual_list == expected_list