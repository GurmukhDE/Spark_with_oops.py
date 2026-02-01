
#----------------What we are testing-------
"""
1- We will test:
2- Spark session creation
3- Read step
4- Validation logic
5- Transformation correctness
6- Failure scenarios 
"""
"""
#---------------------------Project-Structure--------------------------
project/
│
├── pipeline/
│   └── sales_pipeline.py
│
├── tests/
│   └── test_sales_pipeline.py
│
└── requirements.txt

"""


#-------------------------Test File-------test_sales_pipeline.py-------------------------

import pytest
from pyspark.sql import SparkSession
from pipeline.sales_pipeline import SalesSparkPipeline
from pyspark.sql.functions import col


@pytest.fixture(scope="session") #Spark session is created once per test session for performance.
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .getOrCreate()
    )
    yield spark
    spark.stop()
#----------------------------------Sample Data----------------
@pytest.fixture
def sample_df(spark):
    data = [
        (1, 100),
        (2, 200),
        (3, 300)
    ]
    columns = ["order_id", "amount"]
    return spark.createDataFrame(data, columns)

#-----------------------------------Test---Read Data----------------------------

def test_read_data(spark, tmp_path):
    file_path = tmp_path / "sales.csv"
    file_path.write_text("order_id,amount\n1,100\n2,200")

    pipeline = SalesSparkPipeline(
        spark=spark,
        file_path=str(file_path),
        batch_date="2026-02-01"
    )

    pipeline.read_data()

    assert pipeline.df is not None
    assert pipeline.df.count() == 2

#---------------------------------------Read Data---------------------------

def test_read_data(spark, tmp_path):
    file_path = tmp_path / "sales.csv"
    file_path.write_text("order_id,amount\n1,100\n2,200")

    pipeline = SalesSparkPipeline(
        spark=spark,
        file_path=str(file_path),
        batch_date="2026-02-01"
    )

    pipeline.read_data()

    assert pipeline.df is not None
    assert pipeline.df.count() == 2

#----------------------------------------------Test - Validation Success-----------------

def test_validate_data_success(spark, sample_df):
    pipeline = SalesSparkPipeline(
        spark=spark,
        file_path="dummy",
        batch_date="2026-02-01"
    )

    pipeline.df = sample_df
    pipeline.validate_data()  # should NOT fail


#-----------------------------------------Test - Validation Failure (NULL values)-----------------

def test_validate_data_failure_null_amount(spark):
    data = [(1, None)]
    df = spark.createDataFrame(data, ["order_id", "amount"])

    pipeline = SalesSparkPipeline(
        spark=spark,
        file_path="dummy",
        batch_date="2026-02-01"
    )

    pipeline.df = df

    with pytest.raises(ValueError):
        pipeline.validate_data()


#-----------------------------------------------------Test-Transformation Logic----------------------

def test_transform_data(spark, sample_df):
    pipeline = SalesSparkPipeline(
        spark=spark,
        file_path="dummy",
        batch_date="2026-02-01"
    )

    pipeline.df = sample_df
    pipeline.transform_data()

    result = pipeline.df.collect()

    assert result[0]["amount_with_tax"] == 118.0
    assert result[0]["batch_date"] == "2026-02-01"


#------------------------------------------------------Test - Full Pipeline Execution-------------------------

def test_run_pipeline(spark, tmp_path):
    file_path = tmp_path / "sales.csv"
    file_path.write_text("order_id,amount\n1,100\n2,200")

    pipeline = SalesSparkPipeline(
        spark=spark,
        file_path=str(file_path),
        batch_date="2026-02-01"
    )

    pipeline.run_pipeline()

    assert pipeline.df.count() == 2

#---------------------------------------------------Code Summary----------------------

"""
I unit test Spark pipelines using pytest with local Spark sessions.
Each ETL step is tested independently, including failure scenarios.
This ensures data quality, faster debugging, and safer production deployments.
"""
  
