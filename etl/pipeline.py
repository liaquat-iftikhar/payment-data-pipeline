import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count

from etl.loaders.manifest_reader import ManifestReader
from etl.loaders.raw_data_loader import RawDataLoader
from etl.writers.athena_table_registrar import AthenaTableRegistrar
from etl.writers.data_writer import DataWriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class GamePaymentETLPipeline:
    def __init__(self, start_date, end_date, bucket_name, glue_curated_database, glue_enriched_database, app_name):
        self.start_date = start_date
        self.end_date = end_date
        self.bucket = bucket_name
        self.curated_db = glue_curated_database
        self.enriched_db = glue_enriched_database
        self.app_name = app_name

        self.input_prefix = f"s3a://{self.bucket}/raw/payments_{self.start_date}_{self.end_date}"
        self.curated_output = f"s3a://{self.bucket}/curated/payments/"
        self.enriched_output = f"s3a://{self.bucket}/enriched/payments/"

    def run(self, ):

        spark = (SparkSession.builder
                 .appName(self.app_name)
                 .enableHiveSupport()
                 .getOrCreate())
        try:

            manifest_reader = ManifestReader(
                spark,
                f"{self.input_prefix}/manifest.json", self.bucket
            )
            raw_loader = RawDataLoader(spark)
            raw_writer = DataWriter(self.curated_output)
            agg_writer = DataWriter(self.enriched_output)
            registrar = AthenaTableRegistrar(
                spark,
                self.curated_output,
                self.enriched_output,
                self.curated_db,
                self.enriched_db
            )

            file_paths = manifest_reader.get_file_paths()
            df = raw_loader.load(file_paths)

            raw_writer.write_partitioned(df)

            df_success = df.filter(col("status") == "success")
            df_agg = df_success.groupBy("game", "payment_date").agg(
                _sum("price").alias("total_revenue"),
                count("*").alias("successful_transactions")
            )

            agg_writer.write_partitioned(df_agg)
            registrar.register()

            logger.info("ETL pipeline completed successfully.")
        finally:
            spark.stop()