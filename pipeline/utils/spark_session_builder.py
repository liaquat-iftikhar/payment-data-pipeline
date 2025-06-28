import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SparkSessionBuilder:
    """
    Utility class to encapsulate the construction and cleanup of a configured SparkSession,
    especially for AWS S3 environments and Hive-compatible catalogs.

    Can be used as a context manager to ensure SparkSession is properly stopped.
    """

    def __init__(self, app_name: str = "DefaultSparkApp", enable_hive: bool = True):
        """
        Initializes the Spark session builder with application-specific configurations.

        Args:
            app_name (str): Name for the Spark application.
            enable_hive (bool): Whether to enable Hive support (default: True).
        """
        self.app_name = app_name
        self.enable_hive = enable_hive
        self._spark = None

    def build(self) -> SparkSession:
        """
        Builds and returns a configured SparkSession.

        Returns:
            SparkSession: Active Spark session.

        Raises:
            Exception: If SparkSession creation fails.
        """
        if self._spark:
            logger.warning("SparkSession is already created. Returning existing session.")
            return self._spark

        try:
            logger.info(f"Creating Spark session with app name: '{self.app_name}'")

            builder = (
                SparkSession.builder
                .appName(self.app_name)
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .config("spark.hadoop.fs.s3a.impl",
                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
            )

            if self.enable_hive:
                builder = builder.enableHiveSupport()

            self._spark = builder.getOrCreate()
            logger.info("Spark session created successfully.")
            return self._spark

        except Exception as e:
            logger.error("Failed to create Spark session", exc_info=True)
            raise

    def close(self) -> None:
        """
        Stops the active SparkSession, if any.
        """
        if self._spark:
            logger.info("Stopping Spark session...")
            try:
                self._spark.stop()
                logger.info("Spark session stopped.")
            except Exception as e:
                logger.error("Failed to stop Spark session", exc_info=True)
            finally:
                self._spark = None
        else:
            logger.debug("No active Spark session to stop.")

    def __enter__(self) -> SparkSession:
        """
        Enables use of the builder as a context manager.

        Returns:
            SparkSession: The built Spark session.
        """
        return self.build()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Ensures SparkSession is stopped when exiting the context manager.
        """
        self.close()
