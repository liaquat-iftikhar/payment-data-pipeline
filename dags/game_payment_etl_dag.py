import logging
import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from dynaconf import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="payment_data_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["payments", "etl"],
    doc_md=__doc__,  # Use module docstring as DAG documentation if desired
)
def payment_data_pipeline():
    """
    DAG to orchestrate payment data ingestion pipeline with the following steps:
    1. Fetch payment data from external API and upload to S3.
    2. Run curated data pipeline to convert raw data into partitioned parquet dataset.
    3. Run enrichment pipeline to transform curated data into enriched datasets.

    Uses dynaconf for config management.
    """

    @task()
    def fetch_data(start_date: str, end_date: str) -> str:
        """
        Fetch payment data for the given execution date using PaymentDataIngestionService.

        Args:
            start_date (str): Date string (YYYY-MM-DD) to fetch data from.
            end_date (str): Date string (YYYY-MM-DD) to fetch data till.

        Returns:
            str: S3 manifest path of the uploaded raw payment data.

        Raises:
            Exception if fetching or uploading fails.
        """
        logger.info(f"Starting data fetch from {start_date} till {end_date}")

        from pipeline.fetcher.payment_data_ingestion_service import PaymentDataIngestionService

        ingestion_service = PaymentDataIngestionService(
            s3_bucket=settings.s3_bucket,
            api_base_url=settings.api_base_url,
        )
        result = ingestion_service.run(start_date=start_date, end_date=end_date)

        if result["status"] != "success":
            logger.error("Fetcher failed or returned no data")
            raise Exception("Fetcher failed or returned no data")

        manifest_path = result["manifest_path"]
        logger.info(f"Fetch completed, manifest path: {manifest_path}")
        return manifest_path

    @task()
    def curate_data(manifest_path: str) -> list:
        """
        Run the curated pipeline to convert raw JSON files to partitioned Parquet dataset.

        Args:
            manifest_path (str): S3 manifest path containing list of raw JSON files.

        Returns:
            list: List of loaded partition paths.

        Raises:
            Exception if curated pipeline fails or produces no partitions.
        """
        logger.info("Starting curated data pipeline")

        from pipeline.curated.game_payment_curated_pipeline import GamePaymentCuratedPipeline

        manifest_file_info = {"manifest_path": manifest_path}
        pipeline = GamePaymentCuratedPipeline(
            manifest_file_info=manifest_file_info,
            output_path=f"s3://{settings.s3_bucket}/curated/game_payments",
            database=settings.database,
            table=settings.curated_table,
            partition_col=settings.partition_column,
        )

        partitions = pipeline.run()

        if not partitions:
            logger.error("Curated pipeline returned no partitions")
            raise Exception("Curated pipeline returned no partitions")

        logger.info(f"Curated pipeline completed, loaded partitions: {partitions}")
        return partitions

    @task()
    def enrich_data(partitions: list):
        """
        Run the enrichment pipeline on the curated partitions.

        Args:
            partitions (list): List of S3 partition paths from the curated pipeline.
        """
        logger.info("Starting enrichment pipeline")

        from pipeline.enriched.aggregated_game_payment_enriched_pipeline import EnrichedGamePaymentPipeline

        enriched_pipeline = EnrichedGamePaymentPipeline(
            input_partitions=partitions,
            enriched_output=settings.enriched_output_path,
            database=settings.database,
            table=settings.enriched_table,
            partition_col=settings.partition_column,
        )
        enriched_pipeline.run()
        logger.info("Enrichment pipeline completed successfully")

    # DAG flow
    exec_date = "{{ ds }}"
    manifest = fetch_data(exec_date, exec_date)
    partitions = curate_data(manifest)
    enrich_data(partitions)


dag = payment_data_pipeline()
