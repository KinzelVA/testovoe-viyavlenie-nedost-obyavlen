import time
import json
import requests
from loguru import logger
from src.config import (
    API_BASE_URL, API_ENDPOINT, API_KEY, API_BATCH_SIZE,
    RAW_ADS_FILE, ENRICHED_ADS_FILE
)


def enrich_ads_data():
    """
    Читает сырые объявления, отправляет их в API пачками
    и сохраняет обогащенный результат.
    """
    logger.info("🚀 Начинаем процесс обогащения данных через API...")

    # 1. Загружаем сырые данные
    try:
        with open(RAW_ADS_FILE, 'r', encoding='utf-8') as f:
            raw_ads = json.load(f)
    except FileNotFoundError:
        logger.error(f"Файл {RAW_ADS_FILE} не найден! Сначала запустите парсер.")
        return

    if not raw_ads:
        logger.warning("Список объявлений пуст. Нечего обогащать.")
        return

    enriched_results = []
    total = len(raw_ads)

    # 2. Разбиваем на батчи
    for i in range(0, total, API_BATCH_SIZE):
        batch = raw_ads[i: i + API_BATCH_SIZE]
        logger.info(f"Обработка батча {i // API_BATCH_SIZE + 1} (объявления {i + 1}-{min(i + API_BATCH_SIZE, total)})")

        # Формируем тело запроса по спецификации API
        # API ждет: { "source": "1c", "data": [ { "title": "..." }, ... ] }
        payload_data = []
        for item in batch:
            payload_data.append({
                "title": item["title"],
                "day": time.strftime("%Y-%m-%d")  # Опционально, но полезно
            })

        payload = {
            "source": "1c",
            "data": payload_data
        }

        # 3. Отправляем запрос с ретраями
        response_data = send_api_request(payload)

        if response_data and "processed_data" in response_data:
            processed_list = response_data["processed_data"]

            # 4. Объединяем ответ API с исходными данными
            # API возвращает список в том же порядке, но надежнее сопоставить по title (или индексу, если порядок гарантирован)
            # В ТЗ порядок не гарантирован явно, но обычно сохраняется.
            # Для надежности мы просто пройдемся по обоим спискам.

            # Создаем словарь для быстрого поиска ответа по title
            processed_map = {p.get("raw_item"): p for p in processed_list}

            for original_item in batch:
                title_key = original_item["title"]
                enrichment = processed_map.get(title_key, {})

                # Сливаем два словаря (исходный + обогащение)
                merged_item = {**original_item, **enrichment}
                enriched_results.append(merged_item)
        else:
            logger.warning("Батч не удалось обработать или пустой ответ, сохраняем как есть.")
            enriched_results.extend(batch)

        # Пауза между запросами (Rate Limit)
        time.sleep(1.5)

    # 5. Сохраняем итог
    save_enriched_json(enriched_results)

    # Выводим статистику
    success_count = sum(1 for x in enriched_results if x.get("group0"))
    logger.success(
        f"🏁 Обогащение завершено. Всего: {total}, Успешно распознано: {success_count} ({success_count / total * 100:.1f}%)")


def send_api_request(payload):
    """Отправляет POST запрос с обработкой ошибок и повторами"""
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                logger.warning(f"Превышен лимит запросов (429). Ждем 5 сек...")
                time.sleep(5)
                continue

            elif response.status_code >= 500:
                logger.warning(f"Ошибка сервера ({response.status_code}). Попытка {attempt + 1}/{max_retries}...")
                time.sleep(2)
                continue

            else:
                logger.error(f"API ошибка {response.status_code}: {response.text}")
                return None

        except requests.RequestException as e:
            logger.error(f"Сетевая ошибка: {e}")
            time.sleep(2)

    logger.error("Не удалось получить ответ от API после всех попыток.")
    return None


def save_enriched_json(data):
    with open(ENRICHED_ADS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info(f"Файл сохранен: {ENRICHED_ADS_FILE}")


if __name__ == "__main__":
    enrich_ads_data()