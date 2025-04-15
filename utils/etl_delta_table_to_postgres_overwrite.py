import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, date_format, max as spark_max
from pyspark.errors.exceptions.captured import AnalysisException
import psycopg2

LOGGER = logging.getLogger(__name__)

class ETLDeltaTableToPostgresOverwrite():
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
                 target_host: str,
                 target_port: str,
                 target_database: str,
                 target_user: str,
                 target_password: str,
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
        self.target_host = target_host
        self.target_port = target_port
        self.target_database = target_database
        self.target_user = target_user
        self.target_password = target_password


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
        # Establish psycopg2 connection to truncate table
        try:
            conn = psycopg2.connect(
                host=self.target_host,
                port=self.target_port,
                database=self.target_database,
                user=self.target_user,
                password=self.target_password
            )
            cursor = conn.cursor()
            cursor.execute(f"TRUNCATE TABLE public.{self.table}")
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Successfully truncated table public.{self.table}")
            LOGGER.info(f"Successfully truncated table public.{self.table}")
        except Exception as e:
            LOGGER.error(f"Failed to truncate table public.{self.table}: {str(e)}")
            raise
        
        # Write DataFrame to PostgreSQL with append mode
        df.write.jdbc(
            url=self.url_postgres,
            table=f"{self.table}",
            mode="append",  # Changed from overwrite to append
            properties=self.properties_postgres
        )
        print("Successfully appended data to the data mart")
        LOGGER.info("Successfully appended data to the data mart")
        self.spark.stop()

    def main(self):
        self.pre_execute()
        df = self.get_data_delta_table()
        self.write_table(df)