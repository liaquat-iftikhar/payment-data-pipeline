import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class AthenaTableRegistrar:
    def __init__(self, spark: SparkSession, curated_path: str, enriched_path: str, curated_db: str, enriched_db: str):
        self.spark = spark
        self.curated_path = curated_path
        self.enriched_path = enriched_path
        self.curated_db = curated_db
        self.enriched_db = enriched_db

    def register(self):
        try:
            logger.info("Registering external tables in Athena/Glue Catalog...")

            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.curated_db}")
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.enriched_db}")

            self.spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {self.curated_db}.payments (
                    transaction_id STRING,
                    game STRING,
                    price DOUBLE,
                    currency STRING,
                    status STRING
                )
                PARTITIONED BY (payment_date STRING)
                STORED AS PARQUET
                LOCATION '{self.curated_path}'
            """)

            self.spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {self.enriched_db}.daily_game_payments (
                    game STRING,
                    total_revenue DOUBLE,
                    successful_transactions BIGINT
                )
                PARTITIONED BY (payment_date STRING)
                STORED AS PARQUET
                LOCATION '{self.enriched_path}'
            """)

            self.spark.sql(f"MSCK REPAIR TABLE {self.curated_db}.payments")
            self.spark.sql(f"MSCK REPAIR TABLE {self.enriched_db}.daily_game_payments")

            logger.info("Tables registered successfully.")

        except Exception as e:
            logger.error(f"Failed to register Athena tables: {e}")
            raise