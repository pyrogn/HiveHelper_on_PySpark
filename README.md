# HiveHelper_on_PySpark
 A very specific, but time-saving library for analyzing Hive tables using PySpark.

The library is tested on PySpark 3.1, 3.2

## How to install
1. Clone repository
2. `conda create -n my_env python=3.10`
3. `conda activate my_env`
4. `pip install -r requirements.txt`
5. `pip install -e .`

### How to rename all txt to py (and back) in Unix
1. `unzip some_zip.zip`
2. `find ./ -depth -name "*.txt" -exec sh -c 'mv "$1" "${1%.txt}.py"' _ {} \;`
[credits to](https://stackoverflow.com/a/65957034)

## Features
1. Getting info about a table based on Primary Key (PK) and count NULL values in columns
2. Comparing a table with a reference table by PK
3. Getting info about tables in a schema. Dropping empty tables.
4. Validating and getting stats on a table.
5. Simplifying operations like reading, writing Hive tables.
6. Working with SCD2 tables (creating, merging, joining)

## Quick links

### [Full demo script](https://github.com/pyrogn/HiveHelper_on_PySpark/blob/main/jupyter/demo.ipynb)

### [Demo SCD2 script](https://github.com/pyrogn/HiveHelper_on_PySpark/blob/main/jupyter/scd2_demo.ipynb)

### [Code](https://github.com/pyrogn/HiveHelper_on_PySpark/tree/main/hhop)

[Code in txt for bypassing email firewall](https://github.com/pyrogn/HiveHelper_on_PySpark/tree/main/hhop/hhop/txt)

Email for any suggestions - 2point71828182846@gmail.com

## Classes

### DFExtender

This class helps you

1. Getting info about Primary Key of one DF
2. Getting info about NULL columns of one DF
3. Comparing two tables based on Primary Key

```python
# 1 checks for PK and non PK columns
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

# get results in attr like:
df_check.pk_stats
# PK_stats(cnt_rows=9, unique_pk_cnt=8, pk_with_duplicates_pk=1)

# 2
# finding rows with duplicates by PK sorted by a number of duplicates in descending order
df_check_pk = df_check.df_duplicates_pk.cache()
df_check_pk.show()

# +-----+----+---+------+--------+----------+----------+------+
# |index| pk1|pk2|  var1|    var2|   dt_part|group_part|cnt_pk|
# +-----+----+---+------+--------+----------+----------+------+
# |    1|key1|  1|value1|value2_1|2022-12-15|    group1|     2|
# |    2|key1|  1|value1|value2_1|2022-12-16|    group2|     2|
# +-----+----+---+------+--------+----------+----------+------+


# 3
# this method returns a DF sorted by count of nulls in selected columns in descending order
df_check_null = df_check.get_df_with_null(['var1', 'var2'])
df_check_null.show()

# +-----+----+---+------+--------+----------+----------+---------+
# |index| pk1|pk2|  var1|    var2|   dt_part|group_part|cnt_nulls|
# +-----+----+---+------+--------+----------+----------+---------+
# |    4|key2|  2|  null|value2_1|2022-12-17|    group1|        1|
# |    6|key2|  4|value1|    null|2022-12-19|    group3|        1|
# +-----+----+---+------+--------+----------+----------+---------+

# 4
# Comparing 2 DataFrames by PK
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

# you can get result from attrs like:
df_main.dict_cols_with_errors
# {'group_part': [2, 0.4], 'var1': [2, 0.4], 'var2': [1, 0.2]}
df_main.matching_results
# Compare_tables_pk_stats(not_in_main_table_cnt=1, not_in_ref_table=1, correct_matching_cnt=5)

# 5
# analyzing results of tables comparison
# filter for finding an exact difference in column
df_matching_errors = df_main.df_with_errors.cache()

df_matching_errors.filter(col('var1_is_diff') == 1)\
    .select('var1_is_diff', 'var1_main', 'var1_ref').show()

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

# 6
# There is an option to alternative order of columns in .df_with_errors attribute
# Use it this way. It might be easier to read
alt_order_cols = df_main.columns_diff_reordered_all
(
    df_matching_errors
    .select(*alt_order_cols)
    .filter(col('var1_is_diff') == 1)
).show()

# +----+---+--------------+-------------+------------+-----------+---------------+----------+---------+-------------+---------+--------+------------+---------------+--------------+------------------+---------+--------------+------------+
# | pk1|pk2|is_joined_main|is_joined_ref|dt_part_main|dt_part_ref|dt_part_is_diff|index_main|index_ref|index_is_diff|var2_main|var2_ref|var2_is_diff|group_part_main|group_part_ref|group_part_is_diff|var1_main|      var1_ref|var1_is_diff|
# +----+---+--------------+-------------+------------+-----------+---------------+----------+---------+-------------+---------+--------+------------+---------------+--------------+------------------+---------+--------------+------------+
# |key1|  1|             1|            1|  2022-12-15| 2022-12-15|              0|         1|        1|            0| value2_1|value2_1|           0|         group2|        group7|                 1|   value1|       value19|           1|
# |key2|  1|             1|            1|  2022-12-17| 2022-12-17|              0|         4|        4|            0| value2_1|value2_1|           0|         group1|       group10|                 1|     null|value_not_null|           1|
# +----+---+--------------+-------------+------------+-----------+---------------+----------+---------+-------------+---------+--------+------------+---------------+--------------+------------------+---------+--------------+------------+


```

### TablePartitionDescriber
The class helps to get partitions of partitioned Hive table in a readable and ready-to-use format

```python
# 1
table_partitions = TablePartitionDescriber('default.part_table_test1')
table_partitions_got = table_partitions.get_partitions_parsed()
table_partitions_got.show(100, False)

# +----------+----------+
# |dt_part   |group_part|
# +----------+----------+
# |2022-12-15|group1    |
# |2022-12-16|group2    |
# |2022-12-16|group3    |
# |2022-12-17|group1    |
# |2022-12-18|group2    |
# |2022-12-19|group3    |
# |2022-12-19|group4    |
# |2022-12-20|group3    |
# |2022-12-20|group7    |
# +----------+----------+

# 2
max_dt = table_partitions.get_max_value_from_partitions('dt_part')
print(max_dt)
# '2022-12-20'

# 3
prefilter = col('group_part') == 'group1'
max_dt_group = table_partitions.get_max_value_from_partitions('dt_part', prefilter=prefilter)
print(max_dt_group)
# '2022-12-17'

# extra
# You may want to change types of column in a table 'table_partitions_got' like in the example but still use a method .get_max_value_from_partitions()
# For this action use in-place method .cast_col_types() like:
table_partitions_got.cast_col_types({'dt_part': 'date'})
```


### SCD2Helper (WIP)

SCD2Helper helps to create, validate, update and join SCD2 tables.

```python
# 1. Create SCD2 table
df1_transactions_s = SCD2Helper(
    df1_transactions, 
    pk=['pk1', 'pk2'], 
    non_pk=['nonpk1', 'nonpk2', 'nonpk3'],
    time_col='ts',
)
df1_scd2 = df1_transactions_s.df_to_scd2()
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+
# |pk1|pk2 |nonpk1|nonpk2|nonpk3|nonpk_extra|ts                 |row_hash                        |row_actual_from|row_actual_to|
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+
# |v1 |null|null  |null  |c2    |r3         |2023-05-07 15:00:00|56e6807f4b745e20dffeb1b731e5a6d4|2023-05-07     |2023-05-09   |
# |v1 |null|null  |null  |null  |null       |2023-05-10 15:00:00|6654c734ccab8f440ff0825eb443dc7f|2023-05-10     |2023-05-10   |
# |v1 |null|null  |fds   |null  |null       |2023-05-11 15:00:00|2d2722576095dd7996570b307d777539|2023-05-11     |2023-05-11   |
# |v1 |null|null  |fds   |asdf  |null       |2023-05-12 15:00:00|b08363345cd7c1cb14e6f4747ce1563d|2023-05-12     |9999-12-31   |
# |v1 |c1  |a1    |b1    |c1    |r1         |2023-05-01 10:00:00|93e6cc4b8b0445cf261e9417106ae6f0|2023-05-01     |2023-05-02   |
# |v1 |c1  |a1    |b2    |c2    |null       |2023-05-03 15:00:00|a6244d3c7c2aed33c4d9525fbef29c1d|2023-05-03     |2023-05-04   |
# |v1 |c1  |null  |b2    |c2    |r3         |2023-05-05 15:00:00|17f599be9e07976c2036361c9ad8f633|2023-05-05     |2023-05-06   |
# |v1 |c1  |null  |null  |c2    |r3         |2023-05-07 15:00:00|a363a9dd6d5b30865ab5813581941516|2023-05-07     |2023-05-09   |
# |v1 |c1  |null  |null  |null  |null       |2023-05-10 15:00:00|da58ea33b20d82042d9969c46c16c3b8|2023-05-10     |2023-05-12   |
# |v1 |c1  |null  |null  |c2    |r3         |2023-05-13 15:00:00|a363a9dd6d5b30865ab5813581941516|2023-05-13     |9999-12-31   |
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+



# 2. Validate SCD2 table
df1_scd2_wrong_copy = SCD2Helper(
    df1_scd2.withColumn('row_actual_to', F.when(col('row_actual_to') == '9999-12-31', F.lit('1000-01-01'))), 
    pk=['pk1', 'pk2'], 
    non_pk=['nonpk1', 'nonpk2', 'nonpk3'],
    time_col='ts',
)
res = df1_scd2_wrong_copy.validate_scd2()
# There are 2 PK duplicates by ['pk1', 'pk2', 'row_actual_to'] Look at `.basic_pk_check.df_duplicates_pk`
# 10 rows with invalid dates, look at `.df_invalid_dates`
# Number of records: 10
# Errors_In_SCD2_table(duplicates_by_pk=2, invalid_dates=10, broken_history=0, duplicates_by_version=0)


# 3. Fill history with versions with null values
df1_scd2_add_more_holes = SCD2Helper(
    df1_holes_in_history,
    pk=['pk1', 'pk2'], 
    non_pk=['nonpk1', 'nonpk2', 'nonpk3'],
)
df1_filled_history = df1_scd2_add_more_holes.fill_scd2_history()
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+
# |pk1|pk2 |nonpk1|nonpk2|nonpk3|nonpk_extra|ts                 |row_hash                        |row_actual_from|row_actual_to|
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+
# |v1 |null|null  |null  |null  |null       |null               |56e6807f4b745e20dffeb1b731e5a6d4|1000-01-01     |2023-05-06   |
# |v1 |null|null  |null  |c2    |r3         |2023-05-07 15:00:00|56e6807f4b745e20dffeb1b731e5a6d4|2023-05-07     |2023-05-09   |
# |v1 |null|null  |null  |null  |null       |2023-05-10 15:00:00|6654c734ccab8f440ff0825eb443dc7f|2023-05-10     |2023-05-10   |
# |v1 |null|null  |fds   |null  |null       |2023-05-11 15:00:00|2d2722576095dd7996570b307d777539|2023-05-11     |2023-05-11   |
# |v1 |null|null  |fds   |asdf  |null       |2023-05-12 15:00:00|b08363345cd7c1cb14e6f4747ce1563d|2023-05-12     |9999-12-31   |
# |v1 |c1  |null  |null  |null  |null       |null               |93e6cc4b8b0445cf261e9417106ae6f0|1000-01-01     |2023-04-30   |
# |v1 |c1  |a1    |b1    |c1    |r1         |2023-05-01 10:00:00|93e6cc4b8b0445cf261e9417106ae6f0|2023-05-01     |2023-05-02   |
# |v1 |c1  |null  |null  |null  |null       |null               |17f599be9e07976c2036361c9ad8f633|2023-05-03     |2023-05-04   |
# |v1 |c1  |null  |b2    |c2    |r3         |2023-05-05 15:00:00|17f599be9e07976c2036361c9ad8f633|2023-05-05     |2023-05-06   |
# |v1 |c1  |null  |null  |null  |null       |null               |da58ea33b20d82042d9969c46c16c3b8|2023-05-07     |2023-05-09   |
# |v1 |c1  |null  |null  |null  |null       |2023-05-10 15:00:00|da58ea33b20d82042d9969c46c16c3b8|2023-05-10     |2023-05-12   |
# |v1 |c1  |null  |null  |null  |null       |null               |da58ea33b20d82042d9969c46c16c3b8|2023-05-13     |9999-12-31   |
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+

# 4. Merge versions with same hash so only TRUE versions are left
df1_scd2_fewer_non_pk = SCD2Helper(
    df1_scd2,
    pk=['pk1', 'pk2'], 
    non_pk=['nonpk2'],
)
df1_merged_history = df1_scd2_fewer_non_pk.merge_scd2_history().cache()
df1_scd2.orderBy('pk1', 'pk2', 'row_actual_from').show(10, False)
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+
# |pk1|pk2 |nonpk1|nonpk2|nonpk3|nonpk_extra|ts                 |row_hash                        |row_actual_from|row_actual_to|
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+
# |v1 |null|null  |null  |c2    |r3         |2023-05-07 15:00:00|56e6807f4b745e20dffeb1b731e5a6d4|2023-05-07     |2023-05-09   |
# |v1 |null|null  |null  |null  |null       |2023-05-10 15:00:00|6654c734ccab8f440ff0825eb443dc7f|2023-05-10     |2023-05-10   |
# |v1 |null|null  |fds   |null  |null       |2023-05-11 15:00:00|2d2722576095dd7996570b307d777539|2023-05-11     |2023-05-11   |
# |v1 |null|null  |fds   |asdf  |null       |2023-05-12 15:00:00|b08363345cd7c1cb14e6f4747ce1563d|2023-05-12     |9999-12-31   |
# |v1 |c1  |a1    |b1    |c1    |r1         |2023-05-01 10:00:00|93e6cc4b8b0445cf261e9417106ae6f0|2023-05-01     |2023-05-02   |
# |v1 |c1  |a1    |b2    |c2    |null       |2023-05-03 15:00:00|a6244d3c7c2aed33c4d9525fbef29c1d|2023-05-03     |2023-05-04   |
# |v1 |c1  |null  |b2    |c2    |r3         |2023-05-05 15:00:00|17f599be9e07976c2036361c9ad8f633|2023-05-05     |2023-05-06   |
# |v1 |c1  |null  |null  |c2    |r3         |2023-05-07 15:00:00|a363a9dd6d5b30865ab5813581941516|2023-05-07     |2023-05-09   |
# |v1 |c1  |null  |null  |null  |null       |2023-05-10 15:00:00|da58ea33b20d82042d9969c46c16c3b8|2023-05-10     |2023-05-12   |
# |v1 |c1  |null  |null  |c2    |r3         |2023-05-13 15:00:00|a363a9dd6d5b30865ab5813581941516|2023-05-13     |9999-12-31   |
# +---+----+------+------+------+-----------+-------------------+--------------------------------+---------------+-------------+

# 5. Join scd2 tables

df1_scd2_j, df2_scd2_j = [SCD2Helper(df, ['pk1', 'pk2'], [non_pk_col], 'ts').df_to_scd2().cache() for df, non_pk_col in zip((df1, df2), ('email_id', 'phone_id'))]
df1_scd2_j, df2_scd2_j = [SCD2Helper(df.drop('ts'), ['pk1', 'pk2'], [non_pk_col], 'ts') for df, non_pk_col in zip((df1_scd2_j, df2_scd2_j),('email_id', 'phone_id'))]
df1_scd2_j.join_scd2(df2_scd2_j).orderBy('pk1', 'pk2', 'row_actual_from').show(100, False)
# +---+---+--------+--------+---------------+-------------+
# |pk1|pk2|email_id|phone_id|row_actual_from|row_actual_to|
# +---+---+--------+--------+---------------+-------------+
# |v1 |c1 |e1      |e1      |2023-05-01     |2023-05-03   |
# |v1 |c1 |e2      |e1      |2023-05-04     |2023-05-05   |
# |v1 |c1 |e2      |e2      |2023-05-06     |2023-05-09   |
# |v1 |c1 |e3      |e2      |2023-05-10     |2023-05-11   |
# |v1 |c1 |e1      |e3      |2023-05-12     |2023-05-12   |
# |v1 |c1 |e1      |e1      |2023-05-13     |9999-12-31   |
# |v1 |c2 |e1      |e1      |2023-05-01     |2023-05-05   |
# |v1 |c2 |e1      |e2      |2023-05-06     |2023-05-11   |
# |v1 |c2 |e1      |e3      |2023-05-12     |2023-05-12   |
# |v1 |c2 |e1      |e1      |2023-05-13     |9999-12-31   |
# |v1 |c3 |e2      |null    |2023-05-04     |2023-05-09   |
# |v1 |c3 |e3      |null    |2023-05-10     |2023-05-11   |
# |v1 |c3 |e1      |null    |2023-05-12     |9999-12-31   |
# +---+---+--------+--------+---------------+-------------+

# 6. Merge SCD2 update
df_merged_upd = SCD2Helper(dfv1, pk=['pk1', 'pk2'], non_pk=['nonpk1', 'nonpk2', 'nonpk3']).merge_scd2_update(dfv2_ded).cache()
df_merged_upd.show(10, False)
# +---+---+------+------+------+-----------+---------------+-------------+--------------------------------+
# |pk1|pk2|nonpk1|nonpk2|nonpk3|nonpk_extra|row_actual_from|row_actual_to|row_hash                        |
# +---+---+------+------+------+-----------+---------------+-------------+--------------------------------+
# |v1 |c0 |a1    |b1    |c1    |r1         |2023-01-01     |2023-05-01   |c3ded78319e4a4af660f608d7ce273f7|
# |v1 |c1 |a1    |b1    |c1    |r1         |2023-01-01     |2023-05-01   |93e6cc4b8b0445cf261e9417106ae6f0|
# |v1 |c1 |a1    |b1    |c1    |r2         |2023-05-02     |9999-12-31   |93e6cc4b8b0445cf261e9417106ae6f0|
# |v1 |c3 |a1    |b1    |c1    |r2         |2023-05-01     |9999-12-31   |86c2c40a3d76eb1ea4de2d093525e14b|
# |v1 |c2 |a1    |b1    |c1    |r2         |2023-05-01     |2023-07-23   |afc91c3bf13930551a1658788e1b5ba5|
# |v1 |c2 |a3    |b1    |c1    |r2         |2023-07-24     |9999-12-31   |185d9be103f655c0e42a7ed94e81c2e1|
# |v1 |c0 |a2    |b1    |c1    |r2         |2023-07-24     |9999-12-31   |ebd84206aad35e3e55fb273eaa6eb288|
# |v1 |c4 |a2    |b1    |c1    |r2         |2023-07-24     |9999-12-31   |f987461f2b6fefed0b4a0b6f740f4a50|
# +---+---+------+------+------+-----------+---------------+-------------+--------------------------------+

```



### SchemaManager

This class helps cleaning a database that has a lot of empty tables.
Empty tables for example might be created because of bulk dropping huge files in HDFS.
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
# dict of tables
popular_schema.dict_of_tables
# {'table2': 0, 'table1': 1, 'table3': 0}

# 4
popular_schema.drop_empty_tables()
# After dropping tables there are 1 tables in popular_schema
```

## Useful functions

### union_all

Makes union of more than 2 tables. Meanwhile native Spark function `.unionByName()` or `.union()` allows unioning of only two tables at once.

```python 
df_unioned = union_all(df1, df2, df3, df4)
# or 
list_df = [df1, df2, df3, df4]
df_unioned = union_all(*list_df)
```
### read_table

This function helps reading a Hive table with additional information

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
# Mandatory parameters are DF and a name of the table. Other are optional

write_table(df.repartition(1), 'test_writing_2', schema='default', partition_cols=['index', 'var1'], mode='overwrite', format_files='parquet')
# DF saved as default.test_writing_2

# it is same as
df.repartition(1).write.format('parquet').partitionBy(['index', 'var1']).mode('overwrite').saveAsTable('default.test_writing_1')
```

### write_read_table

Makes a checkpoint of DF by writing to HDFS and reading this table.
It saves an extra line of doing it in two actions: write_table() and read_table()
```python
# Mandatory parameters are DF and table_name
df_dedup_cp = write_read_table(df_dedup, 'table_name12', schema='test_checkpoint', verbose=1)
# DF saved as test_checkpoint.table_name12
```


### deduplicate_df

Function makes a new DF based on pk and order of non-PK columns with deduplication using `row_number()`

```python
df.show()
# +-----+----+---+------+--------+----------+----------+
# |index| pk1|pk2|  var1|    var2|   dt_part|group_part|
# +-----+----+---+------+--------+----------+----------+
# |    1|key1|  1|value1|value2_1|2022-12-15|    group2|
# |    2|key1|  2|value1|value2_1|2022-12-16|    group2|
# |    3|key1|  3|value1|value2_1|2022-12-16|    group3|
# |    5|key2|  2|value1|value2_1|2022-12-18|    group2|
# |    4|key2|  1|  null|value2_1|2022-12-17|    group1|
# |    6|key2|  3|value1|    null|2022-12-20|    group3|
# +-----+----+---+------+--------+----------+----------+

df_dedup = deduplicate_df(df, pk=['pk1'], order_by_cols=[col('dt_part').desc(), col('group_part')])

df_dedup.show()
# +-----+----+---+------+--------+----------+----------+
# |index| pk1|pk2|  var1|    var2|   dt_part|group_part|
# +-----+----+---+------+--------+----------+----------+
# |    2|key1|  2|value1|value2_1|2022-12-16|    group2|
# |    6|key2|  3|value1|    null|2022-12-20|    group3|
# +-----+----+---+------+--------+----------+----------+
``` 


### TODO
- [x] Setup local PySpark with Hive (although it runs fine without Hive) [1](https://codewitharjun.medium.com/install-hadoop-on-macos-efe7c860c3ed) [2](https://medium.com/@datacouch/how-to-set-up-spark-environment-on-mac-c1553005e1f4)
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
- [x] Good documentation
- [x] Add tests at the bottom of demo.ipynb
- [x] Include custom values in NULLs check
- [x] Add function write_read_table to make checkpoints
- [x] Clean up attributes and methods in DFExtender and refactor something
- [x] Create a new class SCD2Helper to create, validate, update and join SCD2 tables
- [x] Add automatic tests outside of .ipynb (coverage is very low, so is the available time)
- [x] Enable linter and reformat code even more
- [x] Create class to help to compare pk of different tables and simplify routine operations inside functions
- [ ] Make another code refactoring in the future