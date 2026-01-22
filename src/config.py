import os
from pathlib import Path

# --- ПУТИ К ФАЙЛАМ ---
# Определяем корень проекта
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Файлы данных
INPUT_CATALOG_FILE = DATA_DIR / "output.csv"
RAW_ADS_FILE = DATA_DIR / "ads_raw.json"
ENRICHED_ADS_FILE = DATA_DIR / "ads_enriched.json"
MISSING_COVERAGE_FILE = DATA_DIR / "missing_coverage.csv"
LOG_FILE = LOGS_DIR / "app.log"

# --- НАСТРОЙКИ AVITO ---
AVITO_BASE_URL = "https://www.avito.ru"
# Поисковый запрос, как договорились (узкая ниша)
AVITO_SEARCH_QUERY = "Натяжитель гусеницы"
AVITO_REGION = "all"  # Ищем по всей России
TARGET_ADS_COUNT = 15  # Нужно 10-20, берем золотую середину

# --- НАСТРОЙКИ API ОБОГАЩЕНИЯ ---
API_BASE_URL = "https://top505.ru"
API_ENDPOINT = "/api/item_batch"
API_KEY = "PXonxrdz8g45#rd61d5e732Ap4uhf/Sc="
API_BATCH_SIZE = 5  # Не жадничаем, шлем маленькими порциями