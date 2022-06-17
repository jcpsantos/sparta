# Sparta

Library to help ETL using Pyspark.

Sparta is a simple library to help you work on ETL builds using PySpark.

## Important Sources

- <a href="https://spark.apache.org/">Apache Spark</a>
- <a href="https://pypi.org/project/smart-open/">Smart Open</a>
- <a href="https://github.com/MrPowers/chispa">Chispa</a>

## Installation

Install the latest version with ```pip install sparta```

## Modules

### Extract

This is a module with functions for extracting and reading data.

**Example**

```python
schema = 'epidemiological_week LONG, date DATE, order_for_place INT, state STRING, city STRING, city_ibge_code LONG, place_type STRING, last_available_confirmed INT'
path = '/content/sample_data/covid19-e0534be4ad17411e81305aba2d9194d9.csv'
df = read_with_schema(path, schema, {'header': 'true'}, 'csv')
```

### Transformation

This is a module with data transformation functions

**Example**

```python
cols = ['longitude','latitude']
df = drop_duplicates(df, 'population', cols)
```

### Load

This is a module with load and write functions.

**Example**

```python
create_hive_table(df, "table_name", 5, "col1", "col2", "col3")
```

### Others

This is a module with several functions that can help in ETL work.

**Example**

```python
get_secret_aws('Nome_Secret', 'sa-east-1')
```

## Supported PySpark / Python versions

Sparta currently supports PySpark 3.0+ and Python 3.7+.
