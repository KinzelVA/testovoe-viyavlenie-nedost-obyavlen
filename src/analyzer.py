import pandas as pd
from loguru import logger
from src.config import (
    INPUT_CATALOG_FILE, ENRICHED_ADS_FILE, MISSING_COVERAGE_FILE
)


def analyze_coverage():
    """
    Сравнивает целевой каталог (output.csv) с найденными объявлениями (ads_enriched.json).
    Генерирует отчет о недостающих позициях.
    """
    logger.info("📊 Начинаем анализ покрытия (Plan vs Fact)...")

    # 1. Загрузка данных
    try:
        df_plan = pd.read_csv(INPUT_CATALOG_FILE)
        df_fact = pd.read_json(ENRICHED_ADS_FILE)
    except ValueError as e:
        logger.error(f"Ошибка чтения файлов: {e}")
        return

    logger.info(f"Загружено плана: {len(df_plan)} строк")
    logger.info(f"Загружено факта: {len(df_fact)} объявлений")

    # 2. Подготовка ключей для сравнения
    # Нам нужно сравнить комбинацию: group0 + group1 + group2 + marka + model
    # Важно: приводим все к нижнему регистру и строкам, чтобы "CAT" == "cat"

    cols_to_compare = ['group0', 'group1', 'group2', 'marka', 'model']

    # Функция нормализации строки
    def normalize(val):
        if pd.isna(val) or val is None or str(val).strip() == "":
            return "unknown"
        return str(val).lower().strip()

    # Создаем столбец 'signature' (подпись товара) в обоих датафреймах
    def create_signature(df):
        # Проверяем, есть ли нужные колонки, если нет - создаем пустые
        for col in cols_to_compare:
            if col not in df.columns:
                df[col] = "unknown"

        # Склеиваем колонки через разделитель "|"
        return df[cols_to_compare].apply(
            lambda row: "|".join([normalize(row[c]) for c in cols_to_compare]), axis=1
        )

    df_plan['signature'] = create_signature(df_plan)
    df_fact['signature'] = create_signature(df_fact)

    # 3. Поиск расхождений
    # Берем те строки из ПЛАНА, чья подпись не встречается в ФАКТЕ
    found_signatures = set(df_fact['signature'].unique())

    missing_mask = ~df_plan['signature'].isin(found_signatures)
    df_missing = df_plan[missing_mask].copy()

    # 4. Сортировка и сохранение
    # Добавляем колонку с причиной (для красоты отчета)
    df_missing['reason'] = 'Not found on Avito'

    # Оставляем только важные колонки для отчета
    final_cols = cols_to_compare + ['catalog_number', 'reason']
    # Если каких-то колонок нет в исходном csv (например catalog_number), игнорируем ошибки
    existing_final_cols = [c for c in final_cols if c in df_missing.columns]

    df_missing_export = df_missing[existing_final_cols]

    # Сохраняем
    df_missing_export.to_csv(MISSING_COVERAGE_FILE, index=False, encoding='utf-8-sig')

    missing_count = len(df_missing)
    coverage_percent = ((len(df_plan) - missing_count) / len(df_plan)) * 100

    logger.success(f"📉 Анализ завершен.")
    logger.info(f"Всего позиций в каталоге: {len(df_plan)}")
    logger.info(f"Найдено на Авито (совпадений): {len(df_plan) - missing_count}")
    logger.warning(f"Не хватает объявлений (Дыры): {missing_count}")
    logger.info(f"Текущее покрытие: {coverage_percent:.2f}%")
    logger.info(f"Файл с недостающими позициями сохранен: {MISSING_COVERAGE_FILE}")


if __name__ == "__main__":
    analyze_coverage()