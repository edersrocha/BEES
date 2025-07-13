import pytest
from src.utils.spark_session import create_spark_session

@pytest.fixture(scope="session")
def spark():
    return create_spark_session("Test Silver Transformation")
