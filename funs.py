from functools import reduce
from spark_init import pyspark, spark, col, F

def read_table(schema_table, columns='all', verbose=False):
    df = spark.sql(f"select * from {schema_table}")

    if columns != 'all':
        df = df.select(columns)

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
            print(f'there are no partition columns')
        
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