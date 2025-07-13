import os
import shutil
import tempfile
import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


@pytest.fixture
def temp_paths(monkeypatch):
    silver_temp = tempfile.mkdtemp()
    gold_temp = tempfile.mkdtemp()

    monkeypatch.setattr("src.config.SILVER_PATH", silver_temp)
    monkeypatch.setattr("src.config.GOLD_PATH", gold_temp)

    yield silver_temp, gold_temp

    shutil.rmtree(silver_temp)
    shutil.rmtree(gold_temp)


def test_run_gold_aggregation_with_full_silver_schema(spark, temp_paths):
    silver_path, gold_path = temp_paths

    from src.gold.brewery_per_type_and_location import run_gold_aggregation

    data = [
        Row(
            ID="123",
            NAME="(405) BREWING CO",
            BREWERY_TYPE="MICRO",
            ADDRESS_1="1716 TOPEKA ST",
            ADDRESS_2=None,
            ADDRESS_3=None,
            CITY="NORMAN",
            STATE_PROVINCE="OKLAHOMA",
            POSTAL_CODE="73069-8224",
            COUNTRY="UNITED STATES",
            LONGITUDE=-97.46818222,
            LATITUDE=35.25738891,
            PHONE="4058160490",
            WEBSITE_URL="HTTP://WWW.405BREWING.COM",
            STATE="OKLAHOMA",
            STREET="1716 TOPEKA ST"
        ),
        Row(
            ID="456",
            NAME="(512) BREWING CO",
            BREWERY_TYPE="MICRO",
            ADDRESS_1="407 RADAM LN STE F200",
            ADDRESS_2=None,
            ADDRESS_3=None,
            CITY="AUSTIN",
            STATE_PROVINCE="TEXAS",
            POSTAL_CODE="78745-1197",
            COUNTRY="UNITED STATES",
            LONGITUDE=None,
            LATITUDE=None,
            PHONE="5129211545",
            WEBSITE_URL="HTTP://WWW.512BREWING.COM",
            STATE="TEXAS",
            STREET="407 RADAM LN STE F200"
        ),
        Row(
            ID="789",
            NAME="CANADIAN BREW CO",
            BREWERY_TYPE="MICRO",
            ADDRESS_1="123 BREW ST",
            ADDRESS_2=None,
            ADDRESS_3=None,
            CITY="TORONTO",
            STATE_PROVINCE="ONTARIO",
            POSTAL_CODE="M5V 2T6",
            COUNTRY="CANADA",
            LONGITUDE=-79.3832,
            LATITUDE=43.6532,
            PHONE="1112223333",
            WEBSITE_URL="HTTP://WWW.CANADIANBREW.CA",
            STATE="ONTARIO",
            STREET="123 BREW ST"
        ),
        Row(
            ID="101",
            NAME="REGIONAL HOUSE",
            BREWERY_TYPE="REGIONAL",
            ADDRESS_1="999 WEST ST",
            ADDRESS_2=None,
            ADDRESS_3=None,
            CITY="DENVER",
            STATE_PROVINCE="COLORADO",
            POSTAL_CODE="80014",
            COUNTRY="UNITED STATES",
            LONGITUDE=-104.9903,
            LATITUDE=39.7392,
            PHONE="2223334444",
            WEBSITE_URL="HTTP://WWW.REGIONALHOUSE.COM",
            STATE="COLORADO",
            STREET="999 WEST ST"
        )
    ]

    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("BREWERY_TYPE", StringType(), True),
        StructField("ADDRESS_1", StringType(), True),
        StructField("ADDRESS_2", StringType(), True),
        StructField("ADDRESS_3", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE_PROVINCE", StringType(), True),
        StructField("POSTAL_CODE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
        StructField("LONGITUDE", DoubleType(), True),
        StructField("LATITUDE", DoubleType(), True),
        StructField("PHONE", StringType(), True),
        StructField("WEBSITE_URL", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("STREET", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema=schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{silver_path}/mtd/breweries")

    run_gold_aggregation()

    output_path = f"{gold_path}/brewery_per_type_and_location"
    assert os.path.exists(output_path)

    result_df = spark.read.format("delta").load(output_path)

    result = {(row["BREWERY_TYPE"], row["COUNTRY"]): row["TOTAL_BREWERIES"] for row in result_df.collect()}

    expected = {
        ("MICRO", "UNITED STATES"): 2,
        ("MICRO", "CANADA"): 1,
        ("REGIONAL", "UNITED STATES"): 1
    }

    assert result == expected