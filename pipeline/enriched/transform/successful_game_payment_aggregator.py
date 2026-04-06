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

    # Hardcoded currency conversion rates to USD.
    # NOTE: These rates are static and used only for assignment/demo purposes.
    # In a real-world scenario, rates should come from a trusted external source,
    # such as a currency rates API gor a historical exchange rate table in the data lake.
    CONVERSION_RATES = {
        "USD": 1.0,
        "EUR": 1.1,
    }

    REQUIRED_COLUMNS = {
        "transaction_id", "game", "payment_date", "price", "currency", "status"
    }

    def __init__(self, df: DataFrame):
        """
        Initialize the aggregator with a DataFrame.

        Args:
            df (DataFrame): Raw input payment data.
        """
        self.df = df

    def _validate_data(self) -> None:
        """
        Validates input data and fails the pipeline if any data quality check fails.

        Checks:
        - Required columns present
        - Non-null transaction_id, game, payment_date, status
        - price is non-negative
        - currency is in allowed list
        - transaction_id is unique

        Returns:
            None

        Raises:
            ValueError: If required columns are missing or any record fails validation
        """
        missing = self.REQUIRED_COLUMNS - set(self.df.columns)
        if missing:
            logger.error(f"Missing required columns: {missing}")
            raise ValueError(f"DataFrame is missing required columns: {missing}")

        logger.info("Performing data quality validation...")

        valid_currency = list(self.CONVERSION_RATES.keys())

        # Step 1: Check invalid rows based on rules
        invalid_df = self.df.filter(
            F.col("transaction_id").isNull() |
            F.col("game").isNull() |
            F.col("payment_date").isNull() |
            F.col("status").isNull() |
            (F.col("price") < 0) |
            (~F.col("currency").isin(*valid_currency))
        )

        invalid_count = invalid_df.count()
        if invalid_count > 0:
            logger.error(f"Data quality check failed: {invalid_count} invalid rows found.")
            invalid_df.show(truncate=False, n=20)
            raise ValueError(f"Data quality validation failed with {invalid_count} invalid records.")

        # Step 2: Check for duplicate transaction_ids
        duplicates_df = self.df.groupBy("transaction_id").count().filter("count > 1")
        dup_count = duplicates_df.count()

        if dup_count > 0:
            logger.error(f"Found {dup_count} duplicate transaction_id(s).")
            duplicates_df.show(truncate=False, n=20)
            raise ValueError(f"Duplicate transaction_ids detected: {dup_count} duplicate keys.")

        logger.info("All records passed data quality checks.")

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

        logger.info("Filtering and aggregating successful game payments...")
        try:

            # Create a mapping expression for conversion
            conversion_expr = F.create_map(*[F.lit(k), F.lit(v)] for k, v in self.CONVERSION_RATES.items())

            df_with_usd_price = self.df.withColumn(
                "usd_price",
                F.col("price") * conversion_expr.getItem(F.col("currency"))
            )

            result_df = df_with_usd_price.filter(
                F.col("status") == "success"
            ).groupBy(
                "game", "payment_date"
            ).agg(
                F.sum("usd_price").alias("total_revenue_in_usd"),
                count("*").alias("successful_transactions")
            )
            logger.info("Aggregation completed successfully.")
            return result_df

        except Exception as e:
            logger.exception("Error during aggregation of successful game payments.")
            raise RuntimeError("Aggregation failed.") from e
