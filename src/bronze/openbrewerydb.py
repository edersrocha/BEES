import os
import json
from datetime import datetime
from src.api_client.openbrewerydb import breweries
from src.config import BRONZE_PATH, LOG_LEVEL
import logging

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

def save_to_bronze(data):
    now = datetime.now()
    sub_path = os.path.join(
        "openbrewerydb",
        "breweries",
        now.strftime("%Y"),
        now.strftime("%m"),
        now.strftime("%d"),
        now.strftime("%H"),
        now.strftime("%M"),
        now.strftime("%S")
    )

    full_path = os.path.join(BRONZE_PATH, sub_path)
    os.makedirs(full_path, exist_ok=True)

    filepath = os.path.join(full_path, "breweries.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Dados salvos em {filepath}")

def run_bronze_ingestion():
    logger.info("Iniciando ingestão da camada bronze...")
    data = breweries()
    save_to_bronze(data)
    logger.info("Ingestão da camada bronze concluída.")

if __name__ == "__main__":
    run_bronze_ingestion()