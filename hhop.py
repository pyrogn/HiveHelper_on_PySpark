from spark_init import pyspark, spark, col, F

class DFExtender(pyspark.sql.dataframe.DataFrame):
    def __init__(self, schema, table, pk=None, default_schema_write='default'):
        self.schema_name = schema
        self.table_name = table
        self.schema_table_name = schema+'.'+table
        self.df = self._read_table(self.schema_name, self.table_name)
        
        super().__init__(self.df._jdf, self.df.sql_ctx)
        print(f"table info {self.schema_table_name}\n{'-'*40}")
        
        self._introduction_info()
        
        print()
        self._get_location()
        print(f'location: {self._location}')
        
        
    def _introduction_info(self):
        self.describe_table = spark.sql(f"describe formatted {self.schema_table_name}")
        cols = spark.catalog.listColumns(tableName=self.table_name, dbName=self.schema_name)
        self._part_cols = [col.name for col in cols if col.isPartition == True]
        
        self.df.printSchema()
        print(f'cols: {self.df.columns}')
        print(f'partition columns: {self._part_cols}')
        print(f'see attribute like this for full info: DataFrame.describe_table.show(100, False)')
        
    def getInfo(self):
        ...
        
    def _read_table(self, schema, table):
        self._df = spark.sql(f"select * from {self.schema_table_name}")
        return self._df
    
    def _get_location(self):
        self._location = (
            self.describe_table
            .filter(col('col_name')=='Location')
            .select('data_type')
            .collect()[0]
            .asDict()['data_type']
        )
        return self._location

    def getInfo(self):
        return self.count()
    
    
class SchemaManager
    
    
def write_table_through_view(sqlContext, df, schema, table):
    '''
    Возможно, это аналогично или даже хуше чем:
    df.write.mode('overwrite').partitionBy('dt_part', 'group_part').saveAsTable('default.part_table_test1')
    Надо тестировать
    '''
    df.createOrReplaceTempView(table)
    sqlContext.sql(f'drop table if exists {schema}.{table}')
    sqlContext.sql(f'create table {schema}.{table} stored as parquet as select * from {table}')