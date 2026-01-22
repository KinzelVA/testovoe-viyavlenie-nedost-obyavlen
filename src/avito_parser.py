import time
import random
import json
from playwright.sync_api import sync_playwright
from loguru import logger
from src.config import (
    AVITO_BASE_URL, AVITO_SEARCH_QUERY, RAW_ADS_FILE,
    TARGET_ADS_COUNT, AVITO_REGION
)


def get_avito_ads():
    """
    Запускает браузер, ищет объявления и собирает данные.
    Возвращает список словарей.
    """
    logger.info(f"🚀 Запускаем парсер Avito (v2 fix) по запросу: '{AVITO_SEARCH_QUERY}'")

    ads_data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        search_url = f"{AVITO_BASE_URL}/{AVITO_REGION}?q={AVITO_SEARCH_QUERY}"
        logger.info(f"Переходим на страницу: {search_url}")
        page.goto(search_url, timeout=60000)

        try:
            page.wait_for_selector('[data-marker="item"]', timeout=15000)
        except Exception:
            logger.error("Не удалось найти объявления. Возможно, бан или капча.")
            browser.close()
            return []

        page.mouse.wheel(0, 1000)  # Скроллим чуть больше
        time.sleep(random.uniform(2, 4))

        ad_elements = page.query_selector_all('[data-marker="item"]')
        logger.info(f"Найдено элементов: {len(ad_elements)}")

        for index, ad in enumerate(ad_elements):
            if len(ads_data) >= TARGET_ADS_COUNT:
                break

            try:
                # --- УЛУЧШЕННЫЙ ПОИСК ЗАГОЛОВКА ---
                title = None

                # Попытка 1: По атрибуту itemprop="name" (часто внутри h3)
                title_el = ad.query_selector('[itemprop="name"]')
                if title_el:
                    title = title_el.inner_text().strip()

                # Попытка 2: Если не нашли, ищем любой h3
                if not title:
                    h3_el = ad.query_selector('h3')
                    if h3_el:
                        title = h3_el.inner_text().strip()

                # Попытка 3: Ищем ссылку с title атрибутом, если текст пустой
                link_el = ad.query_selector('[itemprop="url"]')
                if not link_el:
                    continue  # Без ссылки нам объявление не нужно

                full_url = f"{AVITO_BASE_URL}{link_el.get_attribute('href')}"

                if not title:
                    # Последний шанс: берем title у ссылки
                    title = link_el.get_attribute('title')

                # Если все равно пусто — пропускаем
                if not title:
                    logger.warning(f"Пропуск объявления {index}: не найден заголовок")
                    continue

                # ID и Регион
                ad_id = ad.get_attribute('data-item-id')
                geo_el = ad.query_selector('div[class*="geo-"]')
                region = geo_el.inner_text().strip() if geo_el else "Не определен"

                item = {
                    "avito_ad_id": ad_id,
                    "title": title,
                    "url": full_url,
                    "region": region,
                    "parsed_at": time.strftime("%Y-%m-%d %H:%M:%S")
                }

                ads_data.append(item)
                logger.debug(f"Спарсили: {title[:40]}...")

            except Exception as e:
                logger.warning(f"Ошибка парсинга элемента: {e}")
                continue

        browser.close()

    logger.success(f"🎉 Парсинг v2 завершен. Собрано: {len(ads_data)}")
    save_to_json(ads_data)
    return ads_data


def save_to_json(data):
    with open(RAW_ADS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info(f"Файл сохранен: {RAW_ADS_FILE}")


if __name__ == "__main__":
    get_avito_ads()