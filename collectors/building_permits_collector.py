# collectors/building_permits_collector.py
import os
import sys

# --- ДОБАВЛЕНИЕ КОРНЕВОЙ ПАПКИ В ПУТЬ ПОИСКА МОДУЛЕЙ ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from dao import IndicatorDAO
from collectors.fred_parser import get_fred_series_history

# --- КОНФИГУРАЦИЯ ---
INDICATOR_CONFIG = {
    'name': 'building_permits_us',
    'full_name': 'New Private Housing Units Authorized by Building Permits',
    'source': 'FRED (Federal Reserve Bank of St. Louis)',
    'description': 'Monthly data on new private housing units authorized by building permits. Seasonally Adjusted Annual Rate, in thousands of units.'
}

FRED_SERIES_ID = 'PERMIT'

def main():
    """
    Коллектор данных Building Permits из FRED API.
    """
    dao = None
    try:
        print("--- Запуск сборщика данных Building Permits ---")
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
        
        # ИСПОЛЬЗУЕМ ОБЩИЙ FRED ПАРСЕР
        permits_df = get_fred_series_history(
            series_id=FRED_SERIES_ID,
            start_date=start_date
        )

        if permits_df.empty:
            print("Нет новых данных для сохранения.")
            return
        
        print(f"Найдено {len(permits_df)} записей для обработки. Сохраняем в БД (дубликаты будут проигнорированы)...")
        for _, row in permits_df.iterrows():
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