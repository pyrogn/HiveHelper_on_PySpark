from functools import reduce
import subprocess
from spark_init import pyspark, spark, sc, col
from pyspark.sql import DataFrame


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

    cnt_files_raw = subprocess.getoutput(
        f"hdfs dfs -ls -R {table_location} | grep '.parquet' | wc -l"
    )
    try:
        cnt_files = int(cnt_files_raw.split("\n")[-1].strip())
    except Exception as e:
        print("Error in count files. Check command output:")
        print(cnt_files_raw)
        print(e)

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
