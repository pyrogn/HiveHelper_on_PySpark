from spark_init import pyspark, spark, col, F
from funs import read_table

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
        self._location = ( # may be rewrite with .rdd.flatMap(lambda x: x).collect()[0]
            self.describe_table
            .filter(col('col_name')=='Location')
            .select('data_type')
            .collect()[0]
            .asDict()['data_type']
        )
        return self._location

    def getInfo(self):
        return self.count()
    

class SchemaManager:
    '''
    Class drops empty tables where there are 0 records or table folder doesn't exist
    '''
    def __init__(self, schema='default'):
        self.schema=schema
        self._cnt_list_tables()
        print(f'{self._cnt_tables} tables in {schema}')
        print(f'run drop_empty_tables() to drop empty tables in {schema}')
        
    def _cnt_list_tables(self):
        self._list_of_tables = (
            spark.sql(f"show tables in {self.schema}")
            .select('tableName')
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        self._cnt_tables = len(self._list_of_tables)
        self._dict_of_tables = dict.fromkeys(self._list_of_tables, 1)
    
    def _find_empty_tables(self):
        for table in self._dict_of_tables:
            try:
                slice = read_table(self.schema, table).take(2)
                if len(slice) == 0:
                    self._dict_of_tables[table] = 0
            except:
                self._dict_of_tables[table] = 0

    def drop_empty_tables(self):

        self._find_empty_tables()
        self._cnt_empty_tables = len([table for table, val in self._dict_of_tables.items() if val == 0])
        perc_empty = round(self._cnt_empty_tables / self._cnt_tables * 100, 2)

        print(f'{self._cnt_empty_tables} going to be dropped out of {self._cnt_tables} ({perc_empty}%). Schema: {self.schema}')

        for table, val in self._dict_of_tables.items():
            if val == 0:
                spark.sql(f"drop table if exists {self.schema}.{table}")

        self._cnt_list_tables()

        print(f'After dropping tables there are {self._cnt_tables} tables in {self.schema}')





    
