

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

class SalesSparkPipeline:
    """
    Spark-based ETL pipeline using OOPS
    """

    def __init__(self, spark, file_path, batch_date):
        self.spark = spark
        self.file_path = file_path
        self.batch_date = batch_date
        self.df = None

    def read_data(self):
        print("[READ] Reading CSV using Spark")

        self.df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(self.file_path)
        )

    def validate_data(self):
        print("[VALIDATE] Validating data")

        if self.df.count() == 0:
            raise Exception("No data available")

        null_count = self.df.filter(col("amount").isNull()).count()
        if null_count > 0:
            raise Exception("Amount column has NULL values")

    def transform_data(self):
        print("[TRANSFORM] Applying transformations")

        self.df = (
            self.df
            .withColumn("amount_with_tax", col("amount") * lit(1.18))
            .withColumn("batch_date", lit(self.batch_date))
        )

    def load_data(self):
        print("[LOAD] Writing data to Data Warehouse (simulated)")

        # In real life: Redshift / Delta / Snowflake
        self.df.show()

    def run_pipeline(self):
        print("----- SPARK PIPELINE STARTED -----")

        self.read_data()
        self.validate_data()
        self.transform_data()
        self.load_data()

        print("----- SPARK PIPELINE COMPLETED -----")


#-------------------------------------Spark - Object - Creation- ----------------------------------------------

spark = (
    SparkSession.builder
    .appName("SalesSparkETL")
    .getOrCreate()
)

pipeline = SalesSparkPipeline(
    spark=spark,
    file_path="/mnt/data/daily_sales.csv",  # s3:// / abfss:// in real life
    batch_date="2026-02-01"
)

pipeline.run_pipeline()

#---------------------------------------------------Code - Summary-- -------------------------------------------------


"""I designed a Spark-based ETL pipeline using OOPS.
The class represents the pipeline template, while each object represents a job execution.
SparkSession is injected into the class, making it reusable and testable.
Each ETL step is modular, which aligns well with Databricks job orchestration."""
