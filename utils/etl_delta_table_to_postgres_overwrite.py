import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, date_format, max as spark_max
from pyspark.errors.exceptions.captured import AnalysisException

LOGGER = logging.getLogger(__name__)

class ETLDeltaTableToPostgres():
    def __init__(self,
                 url_postgres: str,
                 properties_postgres: dict,
                 endpoint_url: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 bucket_name_source: str,
                 data_source: str,
                 source: str,
                 table: str,
                 *args, **kwargs):
        self.url_postgres = url_postgres
        self.properties_postgres = properties_postgres
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name_source = bucket_name_source
        self.table = table
        self.source = source
        self.source_path = f"s3a://{bucket_name_source}/{data_source}/{source}/{table}"


    def pre_execute(self):

        self.spark = SparkSession.builder \
            .appName(f"delta_table_to_datamart") \
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
    
    def get_data_delta_table(self):
        df = self.spark.read.format("delta").load(self.source_path)
        print(f"Successfully retrieved data from {self.source_path}")
        LOGGER.info(f"Successfully retrieved data from {self.source_path}")
        return df
    
    def write_table(self, df):
        df.write.jdbc(url=self.url_postgres, table=f"{self.table}", mode="overwrite", properties=self.properties_postgres)
        print("Successfully wrote data to the data mart")
        LOGGER.info("Successfully wrote data to the data mart")
        self.spark.stop()

    def main(self):
        self.pre_execute()
        df = self.get_data_delta_table()
        self.write_table(df)