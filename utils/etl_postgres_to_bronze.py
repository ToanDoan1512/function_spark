import logging
import subprocess
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException

LOGGER = logging.getLogger(__name__)

class ETLDataPostgresToDeltaTable():
    def __init__(self,
                 url_postgres: str,
                 properties_postgres: str,
                 endpoint_url: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 bucket_name: str,
                 source: str,
                 table: str,
                 column_checkpoint: str,
                 data_source: str,
                 *args, **kwargs):
        self.url_postgres = url_postgres
        self.properties_postgres = properties_postgres
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name = bucket_name
        self.table = table
        self.source = source
        self.column_checkpoint = column_checkpoint
        self.target_path = f"s3a://{bucket_name}/{data_source}/{source}/{table}"

    
    def pre_execute(self):

        self.spark = SparkSession.builder \
            .appName(f"{self.source}_to_{self.table}_{self.bucket_name}") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.access.key", self.aws_access_key_id) \
            .config("spark.hadoop.fs.s3a.secret.key", self.aws_secret_access_key) \
            .config("spark.hadoop.fs.s3a.endpoint", self.endpoint_url) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .getOrCreate()
        
        print("A Spark session has been created.")
        LOGGER.info("A Spark session has been created.")
    
    def get_checkpoint(self):
        try:
            df_target = self.spark.read.format("delta").load(self.target_path)
            check_point = df_target.select(f"{self.column_checkpoint}") \
                .orderBy(df_target.write_date.desc()) \
                .limit(1) \
                .collect()[0][0]
            print(f"Checkpoint value: {check_point}")
            LOGGER.info(f"Checkpoint value: {check_point}")
            return check_point
            
        except AnalysisException as e:
            LOGGER.error(f"Path not found: {self.target_path} - Error: {e}")
            print("Checkpoint value: None")
            LOGGER.info("Checkpoint value: None")
            return None

    def get_data_postgres(self, check_point):        
        if check_point:
            query = f"""
                (SELECT * FROM public.{self.table} 
                WHERE {self.column_checkpoint} >= '{check_point}'
                ORDER BY {self.column_checkpoint} ASC
                LIMIT 100000) AS temp_table
            """
        else:
            query = f"""
                (SELECT * FROM public.{self.table}
                ORDER BY {self.column_checkpoint} ASC
                LIMIT 100000) AS temp_table
            """
        print(f"Execute SQL: {query}")
        LOGGER.info(f"Execute SQL: {query}")
        # Read data from PostgreSQL into Spark DataFrame
        df = self.spark.read.jdbc(url=self.url_postgres, table=query, properties=self.properties_postgres)
        print("Successfully retrieved data from postgres")
        return df
    
    def write_delta_table(self, df, check_point):
        if check_point:
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.id = source.id"  # Điều kiện so khớp
            ).whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        else:
            df.write.format("delta") \
                .option("mergeSchema", "true") \
                .mode("append") \
                .save(self.target_path)
        print("Successfully wrote data to the data lakehouse")
        LOGGER.info("Successfully wrote data to the data lakehouse")
        self.spark.stop()
    
    def main(self):
        self.pre_execute()
        check_point = self.get_checkpoint()
        df = self.get_data_postgres(check_point)
        self.write_delta_table(df, check_point)