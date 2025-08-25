# collectors/fred_data_collector.py
import os
import sys
import pandas as pd
from fredapi import Fred

# --- ДОБАВЛЕНИЕ КОРНЕВОЙ ПАПКИ В ПУТЬ ПОИСКА МОДУЛЕЙ ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from dao import IndicatorDAO

# --- КОНФИГУРАЦИЯ ---
FRED_API_KEY = '789b6994e97bd3107e584fda90796e5f'
INDICATOR_CONFIG = {
    'name': 'real_m2_usd',
    'full_name': 'Real M2 Money Stock (in Billions of 1982-84 Dollars)',
    'source': 'FRED (Federal Reserve Bank of St. Louis)',
    'description': 'Calculated as M2SL / CPIAUCSL * 100. Seasonally Adjusted.'
}

def collect_real_m2():
    """
    Финальная, эффективная версия. Загружает данные с последней известной даты
    и полагается на DAO и БД для отсеивания дубликатов.
    """
    dao = None
    try:
        print("Инициализация клиентов FRED API и DAO...")
        fred = Fred(api_key=FRED_API_KEY)
        dao = IndicatorDAO()

        indicator_id = dao.add_indicator(
            name=INDICATOR_CONFIG['name'],
            full_name=INDICATOR_CONFIG['full_name'],
            source=INDICATOR_CONFIG['source'],
            description=INDICATOR_CONFIG['description']
        )
        if not indicator_id:
            print("Не удалось получить ID индикатора.")
            return
        print(f"Индикатор '{INDICATOR_CONFIG['name']}' зарегистрирован с ID: {indicator_id}")

        start_date = dao.get_latest_indicator_date(indicator_id)
        if start_date:
            print(f"Последняя дата в БД: {start_date}. Загружаем данные с этой даты.")
        else:
            print("Данных в БД нет. Загружаем всю историю.")
        
        print("Загрузка данных M2SL и CPIAUCSL из FRED...")
        m2_series = fred.get_series('M2SL', observation_start=start_date)
        cpi_series = fred.get_series('CPIAUCSL', observation_start=start_date)

        df = pd.concat([m2_series.rename('M2SL'), cpi_series.rename('CPIAUCSL')], axis=1, join='inner')

        if df.empty:
            print("Нет новых данных для сохранения.")
            return

        df['value'] = (df['M2SL'] / df['CPIAUCSL']) * 100
        real_m2_df = df[['value']].reset_index().rename(columns={'index': 'date'})
        
        print(f"Найдено {len(real_m2_df)} записей для обработки. Сохраняем в БД (дубликаты будут проигнорированы)...")
        for index, row in real_m2_df.iterrows():
            dao.add_indicator_value(
                indicator_id=indicator_id,
                date=row['date'].strftime('%Y-%m-%d'),
                value=row['value']
            )
        
        print(f"Обработка завершена.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        if dao:
            dao.close()
            print("Соединение с базой данных закрыто.")


if __name__ == "__main__":
    print("--- Запуск сборщика данных Real M2 (финальная, эффективная версия) ---")
    collect_real_m2()
    print("--- Работа сборщика завершена ---")