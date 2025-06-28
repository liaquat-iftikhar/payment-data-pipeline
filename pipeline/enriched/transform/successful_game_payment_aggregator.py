import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import count

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SuccessfulGamePaymentAggregator:
    """
    Transforms a payment DataFrame to aggregate successful transactions.

    - Filters for successful transactions.
    - Groups by game and payment_date.
    - Aggregates total revenue and count of successful transactions.

    Attributes:
        df (DataFrame): Input Spark DataFrame containing payment data.
    """

    def __init__(self, df: DataFrame):
        """
        Initialize the aggregator with a DataFrame.

        Args:
            df (DataFrame): Raw input payment data.
        """
        self.df = df

    def transform(self) -> DataFrame:
        """
        Filters and aggregates successful payments per game and day.

        Returns:
            DataFrame: Aggregated metrics with columns:
                - game
                - payment_date
                - total_revenue
                - successful_transactions

        Raises:
            ValueError: If the input DataFrame is empty or lacks required columns.
        """
        required_columns = {"game", "payment_date", "price", "status"}
        missing = required_columns - set(self.df.columns)

        if missing:
            logger.error(f"Input DataFrame is missing required columns: {missing}")
            raise ValueError(f"Missing required columns: {missing}")

        logger.info("Filtering and aggregating successful game payments...")
        try:
            result_df = self.df.filter(
                F.col("status") == "success"
            ).groupBy(
                "game", "payment_date"
            ).agg(
                F.sum("price").alias("total_revenue"),
                count("*").alias("successful_transactions")
            )
            logger.info("Aggregation completed successfully.")
            return result_df

        except Exception as e:
            logger.exception("Error during aggregation of successful game payments.")
            raise RuntimeError("Aggregation failed.") from e
