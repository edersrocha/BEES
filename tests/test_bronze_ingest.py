import os
import json
import tempfile
from unittest import mock
from src.bronze.openbrewerydb import save_to_bronze

mock_data = [
    {
        "id": "e2e78bd8-80ff-4a61-a65c-3bfb9d76ce2",
        "name": "10 Barrel Brewing Co",
        "brewery_type": "large",
        "address_1": "1135 NW Galveston Ave Ste B",
        "address_2": None,
        "address_3": None,
        "city": "Bend",
        "state_province": "Oregon",
        "postal_code": "97703-2465",
        "country": "United States",
        "longitude": -121.3288021,
        "latitude": 44.0575649,
        "phone": "5415851007",
        "website_url": None,
        "state": "Oregon",
        "street": "1135 NW Galveston Ave Ste B"
    }
]

def test_save_to_bronze_creates_correct_json_structure():
    with tempfile.TemporaryDirectory() as tmp_dir:
        with mock.patch("src.bronze.openbrewerydb.BRONZE_PATH", tmp_dir):
            save_to_bronze(mock_data)

            bronze_root = os.path.join(tmp_dir, "openbrewerydb", "breweries")
            found_file = None

            for root, _, files in os.walk(bronze_root):
                if "breweries.json" in files:
                    found_file = os.path.join(root, "breweries.json")
                    break

            assert found_file, "Arquivo breweries.json não encontrado na estrutura esperada."

            with open(found_file, "r", encoding="utf-8") as f:
                content = json.load(f)
                assert isinstance(content, list)
                assert len(content) == 1
                assert content[0]["name"] == "10 Barrel Brewing Co"
                assert content[0]["postal_code"] == "97703-2465"
                assert content[0]["latitude"] == 44.0575649
