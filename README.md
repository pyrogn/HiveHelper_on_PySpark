# HiveHelper_on_PySpark
 Very specific, but time-saving library


## Features
1. Getting info about a table based on primary key and NULL values in columns
2. Comparing a table with a reference table by PK
3. Getting info about schemas and tables in a schema. Cleaning old tables.
4. Validating and getting stats on a table.
5. Simplifying operations like reading, writing Hive tables.

## Demonstration of features

### [Demo script](https://github.com/pyrogn/HiveHelper_on_PySpark/blob/main/demo.ipynb)

### [Code](https://github.com/pyrogn/HiveHelper_on_PySpark/tree/main/hhop)

## Classes

### DFExtender

This class helps you

1. Getting info about PK of one DF
2. Getting info about NULL columns of one DF
3. Comparing two tables based on PK

```python
# 1 check PK and non PK columns
df_check = DFExtender(df, pk=['pk1', 'pk2'], verbose=True)
df_check.get_info()

# You can access DF with PK duplicates in an attribute `.df_duplicates_pk`

# Count all:                9
# Unique PK count:          8
# PK with duplicates:       1
# PK column 'pk1' contains empty values, be careful!
# PK column 'pk2' contains empty values, be careful!

# Null values in columns - {'column': [count NULL, share NULL]}:
# {'pk1': [2, 0.2222], 'pk2': [2, 0.2222], 'var1': [1, 0.1111], 'var2': [1, 0.1111]}
# Use method `.get_df_with_null(List[str])` to get a df with specified NULL columns


# 2
# this method returns a DF sorted by count of nulls in selected columns in descending order
df_check_null = df_check.get_df_with_null(['var1', 'var2'])
df_check_null.show()

# +-----+----+---+------+--------+----------+----------+---------+
# |index| pk1|pk2|  var1|    var2|   dt_part|group_part|cnt_nulls|
# +-----+----+---+------+--------+----------+----------+---------+
# |    4|key2|  2|  null|value2_1|2022-12-17|    group1|        1|
# |    6|key2|  4|value1|    null|2022-12-19|    group3|        1|
# +-----+----+---+------+--------+----------+----------+---------+

# 3
# Comparing tables
df_main = DFExtender(df, pk=['pk1', 'pk2'], verbose=True)
df_main.compare_tables(df_ref)

# Main DF
# Count all:                6
# Unique PK count:          6
# PK with duplicates:       0

# Reference DF
# Count all:                6
# Unique PK count:          6
# PK with duplicates:       0

# Errors in columns - {'column': [count is_error, share is_error]}
# {'var1': [2, 0.4], 'group_part': [2, 0.4], 'var2': [1, 0.2]}

# Count stats of matching main and reference tables:
# not in main table:        1
# not in reference table:   1
# correct matching:         5

# Use DF in attribute `.df_with_errors` for further analysis

# 4 
# analyzing results
# filter for finding an exact difference in column
df_matching_errors.filter(col('var1_is_diff') == 1).select('var1_is_diff', 'var1_main', 'var1_ref').show()

# +------------+---------+--------------+
# |var1_is_diff|var1_main|      var1_ref|
# +------------+---------+--------------+
# |           1|   value1|       value19|
# |           1|     null|value_not_null|
# +------------+---------+--------------+

# columns only present in Reference DF
(
    df_matching_errors
    .filter(col('is_joined_main').isNull())
    .select('pk1', 'pk2', 'is_joined_main', 'is_joined_ref', 'var1_main', 'var1_ref')
).show()

# +----+---+--------------+-------------+---------+--------+
# | pk1|pk2|is_joined_main|is_joined_ref|var1_main|var1_ref|
# +----+---+--------------+-------------+---------+--------+
# |key2|  5|          null|            1|     null|  value1|
# +----+---+--------------+-------------+---------+--------+
```

### SchemaManager

This class helps cleaning a database that has a lot of empty tables.
Empty tables for example might be created because of bulk dropping of huge files in HDFS.
```python
# 1
popular_schema = SchemaManager('popular_schema')
# 3 tables in popular_schema
# run find_empty_tables() on instance to find empty tables in popular_schema

# 2
popular_schema.find_empty_tables()
# 2 tables going to be dropped out of 3 (66.67%)
# Data about tables is stored in an attribute '.dict_of_tables':
# 1 - has data, 0 - doesn't and going to be deleted

# run drop_empty_tables() on instance to drop empty tables in popular_schema

# 3
popular_schema.drop_empty_tables()
# After dropping tables there are 1 tables in popular_schema
```

## Useful functions

### union_all

Makes union of more than 2 tables

```python 
df_unioned = union_all(df1, df2, df3, df4)
# or 
list_df = [df1, df2, df3, df4]
df_unioned = union_all(*list_df)
```
### read_table

Helps reading a Hive table with additional information

```python
df = read_table('default.part_table_test1', verbose=True, cnt_files=True)

# root
#  |-- index: string (nullable = true)
#  |-- pk1: string (nullable = true)
#  |-- pk2: string (nullable = true)
#  |-- var1: string (nullable = true)
#  |-- var2: string (nullable = true)
#  |-- dt_part: string (nullable = true)
#  |-- group_part: string (nullable = true)

# partition columns: ['dt_part', 'group_part']
                                                                                
# Running command: hdfs dfs -ls -R file:/Users/pyro/github/HiveHelper_on_PySpark/spark-warehouse/part_table_test1 | grep '.parquet' | wc -l
# 9 parquet files in the specified above location
```

### write_table

Writes DataFrame to Hive.
This function uses PySpark `.write` method, but with common defaults.

```python
# Mandatory parameters are DF and name of the table
write_table(df.coalesce(1), 'test_writing_2', schema='default', partition_cols=['index', 'var1'], mode='overwrite', format_files='parquet')
# DF saved as default.test_writing_2

# it is same as
df.coalesce(1).write.partitionBy(['index', 'var1']).mode('overwrite').saveAsTable('default.test_writing_1')
```


### TODO
- [x] Setup local PySpark with Hive [1](https://codewitharjun.medium.com/install-hadoop-on-macos-efe7c860c3ed) [2](https://medium.com/@datacouch/how-to-set-up-spark-environment-on-mac-c1553005e1f4)
- [x] Synth table with partitions
- [x] Basic testing script (testing inside .ipynb)
- [x] How to extend Pyspark
- [x] Finish DFExtender
- [x] Finish SchemaManager
- [x] Finish comparing tables
- [x] Finish Extra: reading from Hive, writing to Hive
- [x] Validate on a Production cluster
- [x] Breaking down big methods and reducing duplication
- [x] Add rounding float numbers in comparing tables
- [ ] Good documentation
- [ ] Add tests at the bottom of demo.ipynb
