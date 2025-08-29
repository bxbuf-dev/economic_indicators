# collectors/fred_data_collector.py (refactored)
import os
import sys
import pandas as pd
from dotenv import load_dotenv

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()

# --- ДОБАВЛЕНИЕ КОРНЕВОЙ ПАПКИ В ПУТЬ ПОИСКА МОДУЛЕЙ ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from dao import IndicatorDAO
from collectors.fred_parser import get_fred_calculated_series

# --- КОНФИГУРАЦИЯ ---
INDICATOR_CONFIG = {
    'name': 'real_m2_usd',
    'full_name': 'Real M2 Money Stock (in Billions of 1982-84 Dollars)',
    'source': 'FRED (Federal Reserve Bank of St. Louis)',
    'description': 'Calculated as M2SL / CPIAUCSL * 100. Seasonally Adjusted.'
}

SERIES_CONFIGS = [
    {'id': 'M2SL', 'name': 'M2SL'},
    {'id': 'CPIAUCSL', 'name': 'CPIAUCSL'}
]

def calculate_real_m2(df):
    """Функция расчета Real M2"""
    return (df['M2SL'] / df['CPIAUCSL']) * 100

def main():
    """
    Рефакторированная версия с использованием общего FRED парсера.
    """
    dao = None
    try:
        print("--- Запуск сборщика данных Real M2 (рефакторированная версия) ---")
        dao = IndicatorDAO()

        indicator_id = dao.add_indicator(**INDICATOR_CONFIG)
        if not indicator_id:
            print("Не удалось получить ID индикатора.")
            return
        print(f"Индикатор '{INDICATOR_CONFIG['name']}' зарегистрирован с ID: {indicator_id}")

        start_date = dao.get_latest_indicator_date(indicator_id)
        if start_date:
            print(f"Последняя дата в БД: {start_date}. Загружаем данные с этой даты.")
        else:
            print("Данных в БД нет. Загружаем всю историю.")
        
        # ИСПОЛЬЗУЕМ НОВЫЙ ОБЩИЙ ПАРСЕР
        real_m2_df = get_fred_calculated_series(
            series_configs=SERIES_CONFIGS,
            calculation_func=calculate_real_m2,
            start_date=start_date
        )

        if real_m2_df.empty:
            print("Нет новых данных для сохранения.")
            return
        
        print(f"Найдено {len(real_m2_df)} записей для обработки. Сохраняем в БД (дубликаты будут проигнорированы)...")
        for _, row in real_m2_df.iterrows():
            dao.add_indicator_value(
                indicator_id=indicator_id,
                date=row['date'].strftime('%Y-%m-%d'),
                value=row['value']
            )
        
        print("Обработка завершена.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        if dao:
            dao.close()
            print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()