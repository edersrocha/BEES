import pytest
from src.utils.spark_session import create_spark_session


def test_create_spark_session_returns_spark_session():
    spark = create_spark_session("TestSparkSession")

    assert spark is not None
    assert spark.sparkContext.appName == "TestSparkSession"

    conf = spark.sparkContext.getConf()
    assert conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"
    assert conf.get("spark.sql.catalog.spark_catalog") == "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])

    import tempfile
    import shutil
    temp_dir = tempfile.mkdtemp()

    try:
        df.write.format("delta").mode("overwrite").save(temp_dir)
        df_read = spark.read.format("delta").load(temp_dir)
        assert df_read.count() == 2
        assert set(df_read.columns) == {"id", "value"}
    finally:
        shutil.rmtree(temp_dir)

    spark.stop()
