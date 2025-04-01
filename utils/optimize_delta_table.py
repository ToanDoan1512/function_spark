import logging
from delta import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, date_format, max as spark_max
from pyspark.errors.exceptions.captured import AnalysisException

LOGGER = logging.getLogger(__name__)

class OptimizeAndVacuumDeltTable():
    def __init__(self,
                 endpoint_url: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 bucket_name_source: str,
                 data_source: str,
                 source: str,
                 table: str,
                 vacuum_time: int,
                 *args, **kwargs):
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name_source = bucket_name_source
        self.table = table
        self.source = source
        self.vacuum_time = vacuum_time
        self.source_path = f"s3a://{bucket_name_source}/{data_source}/{source}/{table}"
    
    def pre_execute(self):

        self.spark = SparkSession.builder \
            .appName(f"delta_table_{self.table}_to_datamart") \
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
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .getOrCreate()
                
        print("A Spark session has been created.")
        LOGGER.info("A Spark session has been created.")
    
    def optimize(self):
        delta_table = DeltaTable.forPath(self.spark, self.source_path)
        delta_table.optimize().executeCompaction()
        print("Optimize Delta Table completed.")
        LOGGER.info("Optimize Delta Table completed.")
    
    def vacuum_delta_table(self):
        delta_table = DeltaTable.forPath(self.spark, self.source_path)
        delta_table.vacuum(self.vacuum_time)
        print("Vacuum Delta Table completed.")
        LOGGER.info("Vacuum Delta Table completed.")

    def main(self):
        self.pre_execute()
        self.optimize()
        self.vacuum_delta_table()
        self.spark.stop()
        print("Spark session stopped.")
        LOGGER.info("Spark session stopped.")

        
    
