import logging
import subprocess
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException
from datetime import datetime  # Thêm import datetime để lấy ngày hiện tại

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
        self.data_source = data_source
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.target_path = f"s3a://{bucket_name}/{data_source}/{source}_daily/{table}_{self.current_date}"
        # Tên bảng tạm trong PostgreSQL
        LOGGER.info(f"Target path for date {self.current_date}: {self.target_path}")
        LOGGER.info(f"Temporary PostgreSQL table name: {self.temp_postgres_table}")

    def pre_execute(self):
        self.spark = SparkSession.builder \
            .appName(f"{self.source}_to_{self.table}_{self.bucket_name}_{self.current_date}") \
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
                .orderBy(col(self.column_checkpoint).desc()) \
                .limit(1) \
                .collect()[0][0]
            print(f"Checkpoint value for date {self.current_date}: {check_point}")
            LOGGER.info(f"Checkpoint value for date {self.current_date}: {check_point}")
            return check_point
        except AnalysisException as e:
            LOGGER.error(f"Path not found: {self.target_path} - Error: {e}")
            print(f"Checkpoint value for date {self.current_date}: None")
            LOGGER.info(f"Checkpoint value for date {self.current_date}: None")
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
        print(f"Execute SQL for date {self.current_date}: {query}")
        LOGGER.info(f"Execute SQL for date {self.current_date}: {query}")
        # Read data from PostgreSQL into Spark DataFrame
        df = self.spark.read.jdbc(url=self.url_postgres, table=query, properties=self.properties_postgres)
        print(f"Successfully retrieved data from PostgreSQL for date {self.current_date}")
        LOGGER.info(f"Successfully retrieved data from PostgreSQL for date {self.current_date}")
        return df
    
    def write_to_temp_postgres(self, df):
        try:
            # Ghi dữ liệu vào bảng tạm trong PostgreSQL
            df.write.jdbc(
                url=self.url_postgres,
                table=f"public.{self.temp_postgres_table}",
                mode="overwrite",  # Ghi đè bảng tạm nếu đã tồn tại
                properties=self.properties_postgres
            )
            print(f"Successfully wrote data to temporary PostgreSQL table {self.temp_postgres_table}")
            LOGGER.info(f"Successfully wrote data to temporary PostgreSQL table {self.temp_postgres_table}")
        except Exception as e:
            LOGGER.error(f"Failed to write to temporary PostgreSQL table {self.temp_postgres_table} - Error: {e}")
            print(f"Failed to write to temporary PostgreSQL table {self.temp_postgres_table} - Error: {e}")

    def write_delta_table(self, df, check_point):
        # Ghi vào bảng Delta Table chính
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
        print(f"Successfully wrote data to the main Delta Table for date {self.current_date} at {self.target_path}")
        LOGGER.info(f"Successfully wrote data to the main Delta Table for date {self.current_date} at {self.target_path}")

        # Đọc dữ liệu từ bảng Delta Table chính và ghi vào bảng tạm trong PostgreSQL
        try:
            df_main = self.spark.read.format("delta").load(self.target_path)
            self.write_to_temp_postgres(df_main)
        except Exception as e:
            LOGGER.error(f"Failed to read from main Delta Table or write to PostgreSQL - Error: {e}")
            print(f"Failed to read from main Delta Table or write to PostgreSQL - Error: {e}")

        self.spark.stop()
    
    def main(self):
        self.pre_execute()
        check_point = self.get_checkpoint()
        df = self.get_data_postgres(check_point)
        self.write_delta_table(df, check_point)