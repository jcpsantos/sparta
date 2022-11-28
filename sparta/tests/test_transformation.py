import pytest
from chispa.dataframe_comparer import assert_df_equality
from sparta.transformation import drop_duplicates, aggregation, format_timestamp, create_col_list, typed_columns
from pyspark.sql import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.master("local[*]").getOrCreate()

def test_drop_duplicates() -> None:
    """Test to check if the duplicates are dropped."""    
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
    """Test to check if the aggregation is done correctly."""    
    source_data = [
            ('enzo', 11, 'BA', 5),
            ('jose', 21, 'BA', 10),
            ('maria', 32, 'PA', 23),
            ('joao', 44, 'RJ', 16),
            ('joao', 45, 'RJ', 52),
            ('zezinho', 32, 'RJ', 28),
            ('jojo', 8, 'PA', 16),
    ]

    source_df = spark.createDataFrame(source_data, ['name', 'age', 'state', 'value'])

    actual_df = aggregation(source_df, 'name', ['state'], {F.sum: ['age', 'value'], F.avg:'age'})

    expected_data = [
            ('BA', 32, 15, 16.0),
            ('PA', 40, 39, 20.0),
            ('RJ', 121, 96, 40.333333333333336),
            ]

    expected_df = spark.createDataFrame(expected_data, ['state', 'age', 'value', 'age_1'])

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
    
def test_format_timestamp() -> None:
    """Test to check if the timestamp is formatted correctly.""" 
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
    """Test to check if the column list is created correctly."""    
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
    
def test_typed_columns() -> None:
    """Test to see if columns have been capitalized."""    
    source_data = [
            ('jose', '1', 'BA'),
            ('jose', '2', 'BA'),
            ('maria', '3', 'PA'),
            ('joao', '4', 'RJ')
    ]

    source_df = spark.createDataFrame(source_data, ['name', 'index', 'state'])

    actual_df = typed_columns(source_df, 'upper')

    expected_data = [
            ('jose', '1', 'BA'),
            ('jose', '2', 'BA'),
            ('maria', '3', 'PA'),
            ('joao', '4', 'RJ')
    ]

    expected_df = spark.createDataFrame(expected_data, ['NAME', 'INDEX', 'STATE'])

    assert_df_equality(actual_df, expected_df)