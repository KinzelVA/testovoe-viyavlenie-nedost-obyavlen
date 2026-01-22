from loguru import logger
import sys

# Импортируем наши модули
from src.avito_parser import get_avito_ads
from src.api_enricher import enrich_ads_data
from src.analyzer import analyze_coverage

def main():
    logger.add("logs/app.log", rotation="1 MB", level="DEBUG")
    logger.info("🤖 ЗАПУСК ПАЙПЛАЙНА: Avito Gap Analyzer")

    try:
        # Шаг 1: Парсинг
        logger.info("--- ШАГ 1: ПАРСИНГ AVITO ---")
        ads = get_avito_ads()
        if not ads:
            logger.error("Парсинг вернул пустой список. Остановка.")
            return

        # Шаг 2: Обогащение
        logger.info("--- ШАГ 2: ОБОГАЩЕНИЕ ЧЕРЕЗ API ---")
        enrich_ads_data()

        # Шаг 3: Аналитика
        logger.info("--- ШАГ 3: АНАЛИЗ ПОКРЫТИЯ ---")
        analyze_coverage()

        logger.success("✅ Все этапы выполнены успешно! Проверьте папку data/")

    except KeyboardInterrupt:
        logger.warning("Программа остановлена пользователем.")
    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()