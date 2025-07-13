import requests
from src.config import API_OPEN_BREWERY_DB_BASE_URL, API_OPEN_BREWERY_DB_TIMEOUT


def breweries(per_page:int = 200):
    all_breweries = []
    page = 1

    while True:
        params = {"page": page, "per_page": per_page}
        try:
            response = requests.get(f'{API_OPEN_BREWERY_DB_BASE_URL}/breweries', params=params, timeout=API_OPEN_BREWERY_DB_TIMEOUT)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Erro ao buscar dados da API: {e}")
        if not data:
            break
        all_breweries.extend(data)
        page += 1

    return all_breweries
