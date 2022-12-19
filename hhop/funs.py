from functools import reduce
from spark_init import pyspark, spark, sc, col, F
from pyspark.sql import DataFrame
import os


def read_table(schema_table, columns="all", verbose=False, alias=None, cnt_files=False):
    df = spark.sql(f"select * from {schema_table}")

    if columns != "all":
        df = df.select(columns)

    if alias:
        df = df.alias(alias)

    if verbose:
        # all columns of the table
        df.printSchema()

        # partition columns
        schema_name, table_name = schema_table.split(".")
        cols = spark.catalog.listColumns(tableName=table_name, dbName=schema_name)
        _part_cols = [col.name for col in cols if col.isPartition == True]
        if _part_cols:
            print(f"partition columns: {_part_cols}")
        else:
            print("there are no partition columns")

        # table_location
    if cnt_files:
        __analyze_table_location(
            schema_table=schema_table, is_partitioned=len(_part_cols) > 0
        )
    return df


# for looking at files in HDFS
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()


def __analyze_table_location(schema_table, is_partitioned):
    describe_table = spark.sql(f"describe formatted {schema_table}")
    table_location = (
        describe_table.filter(col("col_name") == "Location")
        .select("data_type")
        .rdd.flatMap(lambda x: x)
        .collect()[0]
    )

    list_folders = [""]
    if is_partitioned:
        list_folders = (
            spark.sql(f"show partitions {schema_table}")
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    cnt_files = 0
    for folder in list_folders:
        path = hadoop.fs.Path(os.path.join(table_location, folder))
        cnt_files += len(
            [
                str(f.getPath())
                for f in fs.get(conf).listStatus(path)
                if str(f.getPath()).endswith(".parquet")  # .endswith(".orc")
            ]
        )  # only works with parquet. How to change for other formats?

    print(f"location: {table_location}")
    print(f"{cnt_files} parquet files at the location")


def union_all(*dfs):
    return reduce(DataFrame.unionByName, dfs)


def write_table(
    df,
    table,
    schema="default",
    mode="overwrite",
    format_files="parquet",
    partition_cols=[],
):
    """
    This function saves a DF to Hive using common default values
    """
    df_save = df.write.mode(mode).format(format_files)

    if set(partition_cols) - set(df.columns):
        raise Exception(
            f"{set(partition_cols) - set(df.columns)} are not in columns of provided DF"
        )

    if partition_cols:
        df_save = df_save.partitionBy(partition_cols)
    df_save.saveAsTable(f"{schema}.{table}")
    print(f"DF saved as {schema}.{table}")
