import logging
from pyspark.sql.functions import col, count
from pyspark.sql.utils import AnalysisException
from src.utils.spark_session import create_spark_session
from src.config import SILVER_PATH, GOLD_PATH, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


def run_gold_aggregation():
    logger.info("Iniciando agregação da camada Gold")
    try:
        spark = create_spark_session("TGold Aggregation - OpenBreweryDB")
    except Exception as e:
        logger.error("Falha ao criar SparkSession", exc_info=True)
        return

    logger.info("Lendo dados da camada Silver")
    try:
        silver_df = spark.read.format("delta").load(f"{SILVER_PATH}/mtd/breweries")
    except AnalysisException as ae:
        logger.error("Erro ao ler dados da camada Silver", exc_info=True)
        return
    except Exception as e:
        logger.error("Erro inesperado ao ler a camada Silver", exc_info=True)
        return

    logger.info("Agregando dados por tipo e localização")
    try:
        gold_df = silver_df.groupBy(
            col("BREWERY_TYPE"),
            col("COUNTRY")
        ).agg(
            count("*").alias("TOTAL_BREWERIES")
        )
    except Exception as e:
        logger.error("Erro ao realizar agregação", exc_info=True)
        return

    output_path = f"{GOLD_PATH}/brewery_per_type_and_location"

    logger.info("Salvando dados na camada Gold")
    try:
        gold_df.write.format("delta") \
            .mode("overwrite") \
            .save(output_path)
        logger.info(f"Dados agregados salvos em {output_path}")
    except Exception as e:
        logger.error("Erro ao salvar dados na camada Gold", exc_info=True)


if __name__ == "__main__":
    run_gold_aggregation()
