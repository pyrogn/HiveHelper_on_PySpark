from functools import reduce
from spark_init import pyspark, spark, col, F

def read_table(schema, table):
    df = spark.sql(f"select * from {schema}.{table}")
    return df


def unionAll(*dfs): pass


def write_table_through_view(sqlContext, df, schema, table):
    '''
    Possibly, a worse and redundant solution than:
    df.write.mode('overwrite').partitionBy('dt_part', 'group_part').saveAsTable('default.part_table_test1')

    Candidate for a change
    '''
    df.createOrReplaceTempView(table)
    sqlContext.sql(f'drop table if exists {schema}.{table}')
    sqlContext.sql(f'create table {schema}.{table} stored as parquet as select * from {table}')