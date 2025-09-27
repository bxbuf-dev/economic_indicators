# collectors/real_yield_curve_collector.py
import os
import sys
from datetime import datetime
import pandas as pd

# Добавляем корневую папку в путь поиска модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from dao import IndicatorDAO
from collectors.treasury_parser import get_treasury_history # <-- ИМПОРТИРУЕМ ТУ ЖЕ ФУНКЦИЮ

# --- КОНФИГУРАЦИЯ ---
INDICATOR_CONFIG = {
    'name': 'us_treasury_real_yield_curve',
    'full_name': 'Daily Treasury Par Real Yield Curve Rates',
    'source': 'U.S. Department of the Treasury',
    'description': 'Real yield curve rates (TIPS) for various maturities, Fridays only.'
}
RATE_TYPE = 'daily_treasury_real_yield_curve'
DEFAULT_START_DATE = datetime(2004, 1, 1).date()

def main():
    dao = None
    try:
        print("--- Запуск сборщика РЕАЛЬНОЙ кривой доходности ---")
        dao = IndicatorDAO()
        indicator_id = dao.add_indicator(**INDICATOR_CONFIG)

        if not indicator_id:
            print("Не удалось получить ID индикатора.")
            return
            
        latest_date_str = dao.get_latest_indicator_date(indicator_id)
        start_date = pd.to_datetime(latest_date_str).date() if latest_date_str else DEFAULT_START_DATE
        
        # ОДИН ВЫЗОВ для получения всей истории
        history_df = get_treasury_history(RATE_TYPE, start_date)

        if history_df.empty:
            print("Нет новых данных для сохранения.")
            return

        print(f"Получено {len(history_df)} новых записей. Сохранение в БД (дубликаты будут проигнорированы)...")
        for _, row in history_df.iterrows():
            dao.add_indicator_value(
                indicator_id=indicator_id,
                date=row['date'].strftime('%Y-%m-%d'),
                value=row['value'],
                category=row['category']
            )
        
        print("\nСбор данных завершен.")

    except Exception as e:
        print(f"Произошла ошибка в коллекторе реальных ставок: {e}")
    finally:
        if dao:
            dao.close()
            print("Соединение с базой данных закрыто.")

if __name__ == '__main__':
    main()