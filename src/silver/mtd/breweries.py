import os
from src.utils.spark_session import create_spark_session
from src.config import BRONZE_PATH, SILVER_PATH
import logging
from pyspark.sql.functions import trim, col, upper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_to_silver():
    try:
        logger.info("Iniciando transformação para camada Silver")

        spark = create_spark_session("Transformação Silver - OpenBreweryDB")

        arquivos = []
        for root, _, files in os.walk(BRONZE_PATH):
            for file in files:
                if file.endswith(".json"):
                    arquivos.append(os.path.join(root, file))

        if not arquivos:
            logger.error("Nenhum arquivo encontrado na camada Bronze")
            return

        arquivo_mais_recente = max(arquivos, key=os.path.getctime)
        logger.info(f"Arquivo mais recente encontrado: {arquivo_mais_recente}")

        df = spark.read.json(arquivo_mais_recente, multiLine=True)

        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.upper())

        for field in df.schema.fields:
            if field.dataType.simpleString() == "string":
                df = df.withColumn(field.name, upper(trim(col(field.name))))

        if "COUNTRY" not in df.columns:
            logger.warning("Coluna country não encontrada para particionamento")

        output_path = f'{SILVER_PATH}/mtd/breweries'
        df.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("COUNTRY") \
            .save(output_path)

        logger.info(f"Dados salvos em formato Delta na pasta {output_path}")

    except Exception as e:
        logger.exception(f"Erro durante a transformação da camada Silver: {e}")

if __name__ == "__main__":
    transform_to_silver()
