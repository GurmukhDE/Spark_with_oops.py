#---------------------------Production-Ready Spark ETL Pipeline (OOPS + Logging)--------------------------------

"""
Outcome of this code:-

1- How real pipelines fail safely
2- How to debug in Databricks / EMR
3- How to explain operational excellence in interviews

"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# ---------------------------------
# LOGGING CONFIGURATION
# ---------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


class SalesSparkPipeline:
    """
    Production-grade Spark ETL Pipeline
    """

    def __init__(self, spark, file_path, batch_date):
        self.spark = spark
        self.file_path = file_path
        self.batch_date = batch_date
        self.df = None

    def read_data(self):
        try:
            logger.info("Reading data from source")

            self.df = (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(self.file_path)
            )

        except Exception as e:
            logger.error("Failed to read data", exc_info=True)
            raise e

    def validate_data(self):
        try:
            logger.info("Validating data")

            if self.df is None or self.df.count() == 0:
                raise ValueError("Source data is empty")

            null_count = self.df.filter(col("amount").isNull()).count()
            if null_count > 0:
                raise ValueError("Amount column contains NULL values")

        except Exception as e:
            logger.error("Data validation failed", exc_info=True)
            raise e

    def transform_data(self):
        try:
            logger.info("Transforming data")

            self.df = (
                self.df
                .withColumn("amount_with_tax", col("amount") * lit(1.18))
                .withColumn("batch_date", lit(self.batch_date))
            )

        except Exception as e:
            logger.error("Transformation failed", exc_info=True)
            raise e

    def load_data(self):
        try:
            logger.info("Loading data to target system")

            # Real life:
            # .write.format("delta").mode("append").save("/delta/sales")

            self.df.show()

        except Exception as e:
            logger.error("Load step failed", exc_info=True)
            raise e

    def run_pipeline(self):
        logger.info("===== PIPELINE STARTED =====")

        try:
            self.read_data()
            self.validate_data()
            self.transform_data()
            self.load_data()

            logger.info("===== PIPELINE COMPLETED SUCCESSFULLY =====")

        except Exception as e:
            logger.critical("PIPELINE FAILED", exc_info=True)
            raise e

#----------------------------------------------------------object-Creation-----------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SalesSparkETL_Prod")
    .getOrCreate()
)

pipeline = SalesSparkPipeline(
    spark=spark,
    file_path="/mnt/data/daily_sales.csv",  # s3:// or abfss:// in real use
    batch_date="2026-02-01"
)

pipeline.run_pipeline()


#-------------------------------------------------------Code-  Summary------------------------------------------------------------------

"""
I build Spark pipelines using OOPS with structured logging and fail-fast exception handling.
Each ETL stage is wrapped in try–except with meaningful logs, which helps in monitoring and debugging production failures.
This design aligns well with Databricks Jobs, Airflow, and enterprise data platforms.

"""
