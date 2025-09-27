# collectors/building_permits_collector.py
import os
import sys
from dotenv import load_dotenv

load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from dao import IndicatorDAO
from collectors.fred_parser import get_fred_series_history

INDICATOR_CONFIG = {
    'name': 'building_permits_us',
    'full_name': 'New Private Housing Units Authorized by Building Permits',
    'source': 'FRED (Federal Reserve Bank of St. Louis)',
    'description': 'Monthly data on new private housing units authorized by building permits by structure type. Seasonally Adjusted Annual Rate, in thousands of units.'
}

PERMIT_SERIES = [
    {'fred_id': 'PERMIT1', 'category': '1 unit'},
    {'fred_id': 'PERMIT24', 'category': '2-4 units'},
    {'fred_id': 'PERMIT5', 'category': '5+ units'},
    {'fred_id': 'PERMIT', 'category': 'total'}
]

def main():
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

        total_records = 0
        for series_config in PERMIT_SERIES:
            print(f"\nЗагрузка серии {series_config['fred_id']} ({series_config['category']})...")
            series_df = get_fred_series_history(series_id=series_config['fred_id'], start_date=start_date)

            if series_df.empty:
                print(f"Нет новых данных для {series_config['category']}")
                continue

            for _, row in series_df.iterrows():
                dao.add_indicator_value(
                    indicator_id=indicator_id,
                    date=row['date'].strftime('%Y-%m-%d'),
                    value=row['value'],
                    category=series_config['category']
                )

            total_records += len(series_df)
            print(f"Сохранено {len(series_df)} записей для {series_config['category']}")

        print(f"\nВсего обработано {total_records} записей.")
        print("Сбор данных Building Permits завершен.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        if dao:
            dao.close()
            print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()
