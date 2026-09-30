import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

@pytest.fixture(scope="module")
def spark_local():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-pyspark-job")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_clean_data_logic(spark_local):
    data = [
        ("Alice", 100.0),
        ("Bob", -50.0),
        ("Sara", 0.0),
        (None, 200.0),
        ("Ziad", 50.0)
    ]
    df = spark_local.createDataFrame(data, ["name", "amount"])
    
    transformed_df = clean_data(df)
    results = transformed_df.collect()
    
    assert len(results) == 2
    
    assert results[0]["name"] == "Alice"
    assert results[0]["amount"] == 100.0
    assert results[0]["amount_with_tax"] == 120.0
    
    assert results[1]["name"] == "Ziad"
    assert results[1]["amount"] == 50.0
    assert results[1]["amount_with_tax"] == 60.0