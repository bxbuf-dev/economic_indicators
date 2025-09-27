# collectors/gdp_historical_loader.py
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
from collectors.fred_parser import get_fred_series_history

# --- КОНФИГУРАЦИЯ ---
INDICATOR_CONFIG = {
    'name': 'us_real_gdp',
    'full_name': 'Real Gross Domestic Product (Billions of Chained 2017 Dollars)',
    'source': 'FRED (Federal Reserve Bank of St. Louis)',
    'description': 'Real gross domestic product is the inflation adjusted value of the goods and services produced by labor and property located in the United States. Seasonally Adjusted Annual Rate.'
}

FRED_SERIES_ID = 'GDPC1'

def main():
    """
    Исторический загрузчик данных по реальному ВВП США (GDPC1).
    Загружает полную историю без учета существующих данных.
    """
    dao = None
    try:
        print("=== ИСТОРИЧЕСКИЙ ЗАГРУЗЧИК ДАННЫХ REAL GDP USA ===")
        dao = IndicatorDAO()

        # Регистрируем индикатор
        indicator_id = dao.add_indicator(**INDICATOR_CONFIG)
        if not indicator_id:
            print("Не удалось получить ID индикатора.")
            return
        print(f"Индикатор '{INDICATOR_CONFIG['name']}' зарегистрирован с ID: {indicator_id}")

        # Проверяем текущее состояние данных
        existing_data = dao.get_indicator_values(indicator_id)
        existing_count = len(existing_data) if not existing_data.empty else 0
        print(f"Существующих записей в БД: {existing_count}")

        # Загружаем ВСЮ историю (без start_date)
        print("Загружаем полную историю данных по ВВП...")
        gdp_df = get_fred_series_history(
            series_id=FRED_SERIES_ID,
            start_date=None  # Загружаем всю доступную историю
        )

        if gdp_df.empty:
            print("Не удалось загрузить данные.")
            return
        
        print(f"Загружено {len(gdp_df)} записей. Диапазон данных: {gdp_df['date'].min()} - {gdp_df['date'].max()}")
        
        # Показываем статистику по данным
        print(f"Минимальное значение ВВП: ${gdp_df['value'].min():.1f} млрд")
        print(f"Максимальное значение ВВП: ${gdp_df['value'].max():.1f} млрд")
        print(f"Последнее значение ВВП: ${gdp_df['value'].iloc[-1]:.1f} млрд")
        
        # Сохраняем данные в БД
        print("Сохраняем данные в БД (дубликаты будут проигнорированы)...")
        saved_count = 0
        for _, row in gdp_df.iterrows():
            success = dao.add_indicator_value(
                indicator_id=indicator_id,
                date=row['date'].strftime('%Y-%m-%d'),
                value=row['value']
            )
            if success:
                saved_count += 1
        
        print(f"Сохранено {saved_count} новых записей из {len(gdp_df)} обработанных.")
        
        # Итоговая статистика
        final_data = dao.get_indicator_values(indicator_id)
        final_count = len(final_data) if not final_data.empty else 0
        print(f"Итого записей в БД для индикатора '{INDICATOR_CONFIG['name']}': {final_count}")
        
        # Показываем первые и последние записи
        if not final_data.empty:
            print("\n=== ПЕРВЫЕ 5 ЗАПИСЕЙ ===")
            print(final_data.head(5)[['date', 'value']].to_string(index=False))
            print("\n=== ПОСЛЕДНИЕ 5 ЗАПИСЕЙ ===")
            print(final_data.tail(5)[['date', 'value']].to_string(index=False))

        print("\n=== ЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ ЗАВЕРШЕНА ===")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if dao:
            dao.close()
            print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()