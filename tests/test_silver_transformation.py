# tests/test_silver_transformation.py

import os
import json
import tempfile
import shutil
import pytest

@pytest.fixture
def temp_dirs(monkeypatch):
    bronze_temp = tempfile.mkdtemp()
    silver_temp = tempfile.mkdtemp()

    monkeypatch.setattr("src.config.BRONZE_PATH", bronze_temp)
    monkeypatch.setattr("src.config.SILVER_PATH", silver_temp)

    yield bronze_temp, silver_temp

    shutil.rmtree(bronze_temp)
    shutil.rmtree(silver_temp)

def test_transform_to_silver_validates_full_schema(spark, temp_dirs):
    bronze_path, silver_path = temp_dirs

    from src.silver.mtd.breweries import transform_to_silver

    mock_data = [
        {
            "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
            "name": "(405) Brewing Co",
            "brewery_type": "micro",
            "address_1": "1716 Topeka St",
            "address_2": None,
            "address_3": None,
            "city": "Norman",
            "state_province": "Oklahoma",
            "postal_code": "73069-8224",
            "country": "United States",
            "longitude": -97.46818222,
            "latitude": 35.25738891,
            "phone": "4058160490",
            "website_url": "http://www.405brewing.com",
            "state": "Oklahoma",
            "street": "1716 Topeka St"
        },
        {
            "id": "9c5a66c8-cc13-416f-a5d9-0a769c87d318",
            "name": "(512) Brewing Co",
            "brewery_type": "micro",
            "address_1": "407 Radam Ln Ste F200",
            "address_2": None,
            "address_3": None,
            "city": "Austin",
            "state_province": "Texas",
            "postal_code": "78745-1197",
            "country": "United States",
            "longitude": None,
            "latitude": None,
            "phone": "5129211545",
            "website_url": "http://www.512brewing.com",
            "state": "Texas",
            "street": "407 Radam Ln Ste F200"
        }
    ]

    mock_file_path = os.path.join(bronze_path, "mock_breweries.json")
    with open(mock_file_path, "w") as f:
        json.dump(mock_data, f, indent=2)

    transform_to_silver()

    output_path = os.path.join(silver_path, "mtd/breweries")
    assert os.path.exists(output_path)

    df = spark.read.format("delta").load(output_path)

    expected_columns = {
        "ID", "NAME", "BREWERY_TYPE", "ADDRESS_1", "ADDRESS_2", "ADDRESS_3",
        "CITY", "STATE_PROVINCE", "POSTAL_CODE", "COUNTRY",
        "LONGITUDE", "LATITUDE", "PHONE", "WEBSITE_URL", "STATE", "STREET"
    }

    assert set(df.columns) == expected_columns
    assert df.count() == 2
    assert df.filter(df["COUNTRY"] == "UNITED STATES").count() == 2
