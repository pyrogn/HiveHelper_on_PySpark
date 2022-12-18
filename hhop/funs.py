from functools import reduce
from spark_init import pyspark, spark, col, F
from pyspark.sql import DataFrame

def read_table(schema_table, columns='all', verbose=False, alias=None):
    df = spark.sql(f"select * from {schema_table}")

    if columns != 'all':
        df = df.select(columns)
    
    if alias:
        df = df.alias(alias)

    if verbose:
        v_print = lambda *args, **kwargs: print(*args, **kwargs) if verbose else None

        describe_table = spark.sql(f"describe formatted {schema_table}")
        df.printSchema()
        schema_name, table_name = schema_table.split('.')
        cols = spark.catalog.listColumns(tableName=table_name, dbName=schema_name)
        _part_cols = [col.name for col in cols if col.isPartition == True]

        _location = ( # may be rewrite with .rdd.flatMap(lambda x: x).collect()[0]
            describe_table
            .filter(col('col_name')=='Location')
            .select('data_type')
            .collect()[0]
            .asDict()['data_type']
        )
        print(f'location: {_location}')

        if _part_cols:
            print(f'partition columns: {_part_cols}')
        else:
            print('there are no partition columns')
        
    return df


def union_all(*dfs):
    return reduce(DataFrame.unionByName, dfs)


def write_table(df, schema, table):
    '''
    Possibly, a worse and redundant solution than:
    df.write.mode('overwrite').partitionBy('dt_part', 'group_part').saveAsTable('default.part_table_test1')

    Candidate for a change

    Does it support partitions?????? I don't think so
    '''
    df.createOrReplaceTempView(table)
    spark.sql(f'drop table if exists {schema}.{table}')
    spark.sql(f'create table {schema}.{table} stored as parquet as select * from {table}')