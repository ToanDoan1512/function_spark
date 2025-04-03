import logging
import subprocess
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException

LOGGER = logging.getLogger(__name__)

class ETLDataPostgresToPostgresBackfill():
    def __init__(self,
                 url_postgres: str,
                 properties_postgres: str,
                 url_postgres_mart: str,
                 properties_postgres_mart: str,
                 source: str,
                 table: str,
                 data_source: str,
                 *args, **kwargs):
        self.url_postgres = url_postgres
        self.properties_postgres = properties_postgres
        self.url_postgres_mart = url_postgres_mart
        self.properties_postgres_mart = properties_postgres_mart
        self.table = table
        self.source = source

    
    def pre_execute(self):

        self.spark = SparkSession.builder \
            .appName(f"{self.source}_to_{self.table}_data_mart") \
            .getOrCreate()
        
        print("A Spark session has been created.")
        LOGGER.info("A Spark session has been created.")
    
    def get_data_postgres(self):        

        query = f"(SELECT * FROM public.{self.table}) AS temp_table"
        print(f"Execute SQL: {query}")
        LOGGER.info(f"Execute SQL: {query}")
        # Read data from PostgreSQL into Spark DataFrame
        df = self.spark.read.jdbc(url=self.url_postgres, table=query, properties=self.properties_postgres)
        print("Successfully retrieved data from postgres")
        return df
    
    def write_temp_table(self, df):
        df = df.repartition(10)
        df.write.jdbc(url=self.url_postgres_mart, 
                        table=f"temp.{self.table}", 
                        mode="overwrite", 
                        properties=self.properties_postgres_mart,
                        batchsize=10000)
        print("Successfully wrote data to the data mart")
        LOGGER.info("Successfully wrote data to the data mart")
        self.spark.stop()
    
    def main(self):
        self.pre_execute()
        df = self.get_data_postgres()
        self.write_temp_table(df)