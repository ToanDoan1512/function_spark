import logging
import subprocess
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException

LOGGER = logging.getLogger(__name__)

class ETLDataPostgresToPostgres():
    def __init__(self,
                 url_postgres: str,
                 properties_postgres: str,
                 endpoint_url: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 bucket_name: str,
                 source: str,
                 table: str,
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
    
    def get_data_postgres(self):        

        query = f"""(WITH changed_leads AS (
                        SELECT id
                        FROM crm_lead
                        WHERE write_date >= CURRENT_DATE - INTERVAL '1 day' 
                        AND write_date < CURRENT_DATE
                    ),
                    changed_tags AS (
                        SELECT id
                        FROM crm_tag
                        WHERE write_date >= CURRENT_DATE - INTERVAL '1 day' 
                        AND write_date < CURRENT_DATE
                    )
                    SELECT DISTINCT
                        rel.lead_id,
                        rel.tag_id
                    FROM crm_tag_rel rel
                    WHERE rel.lead_id IN (SELECT id FROM changed_leads)
                    OR rel.tag_id IN (SELECT id FROM changed_tags)) AS temp_table"""
        print(f"Execute SQL: {query}")
        LOGGER.info(f"Execute SQL: {query}")
        # Read data from PostgreSQL into Spark DataFrame
        df = self.spark.read.jdbc(url=self.url_postgres, table=query, properties=self.properties_postgres)
        print("Successfully retrieved data from postgres")
        return df
    
    def write_temp_table(self, df):
        df.write.jdbc(url=self.url_postgres, table=f"temp.{self.table}", mode="overwrite", properties=self.properties_postgres)
        print("Successfully wrote data to the data mart")
        LOGGER.info("Successfully wrote data to the data mart")
        self.spark.stop()
    
    def main(self):
        self.pre_execute()
        df = self.get_data_postgres()
        self.write_temp_table(df)
    

    
