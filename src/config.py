import os
from dotenv import load_dotenv

load_dotenv()

# API
API_OPEN_BREWERY_DB_BASE_URL = os.getenv("API_OPEN_BREWERY_DB_BASE_URL")
API_OPEN_BREWERY_DB_TIMEOUT = int(os.getenv("API_OPEN_BREWERY_DB_TIMEOUT", 30))

# Ambiente
ENV = os.getenv("ENV", "development")

# Data Lake Paths
BRONZE_PATH = os.getenv("BRONZE_PATH")
SILVER_PATH = os.getenv("SILVER_PATH")
GOLD_PATH = os.getenv("GOLD_PATH")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "./logs/app.log")

# Orquestração
RETRIES = int(os.getenv("RETRIES", 3))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 300))

# Outros
DATE_FORMAT = os.getenv("DATE_FORMAT", "%Y-%m-%d")
PARTITION_COLUMN = os.getenv("PARTITION_COLUMN", "STATE")
