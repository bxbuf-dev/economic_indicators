# collectors/gdp_collector.py
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
    Коллектор данных по реальному ВВП США (GDPC1) из FRED API.
    """
    dao = None
    try:
        print("--- Запуск сборщика данных Real GDP USA ---")
        dao = IndicatorDAO()

        # Регистрируем индикатор
        indicator_id = dao.add_indicator(**INDICATOR_CONFIG)
        if not indicator_id:
            print("Не удалось получить ID индикатора.")
            return
        print(f"Индикатор '{INDICATOR_CONFIG['name']}' зарегистрирован с ID: {indicator_id}")

        # Определяем дату начала загрузки с учетом ревизий ВВП
        # ВВП пересматривается несколько раз, поэтому загружаем данные за последние 2 года
        # для захвата всех возможных ревизий
        from datetime import datetime, timedelta
        
        latest_date = dao.get_latest_indicator_date(indicator_id)
        if latest_date:
            # Берем дату на 2 года назад для захвата ревизий
            revision_start = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
            start_date = min(latest_date, revision_start)
            print(f"Последняя дата в БД: {latest_date}")
            print(f"Загружаем данные с {start_date} для учета возможных ревизий ВВП")
        else:
            start_date = None
            print("Данных в БД нет. Загружаем всю историю.")
        
        # Загружаем данные через парсер FRED
        gdp_df = get_fred_series_history(
            series_id=FRED_SERIES_ID,
            start_date=start_date
        )

        if gdp_df.empty:
            print("Нет новых данных для сохранения.")
            return
        
        print(f"Найдено {len(gdp_df)} записей для обработки. Сохраняем в БД (дубликаты будут проигнорированы)...")
        
        # Сохраняем данные в БД с учетом возможных обновлений значений
        saved_count = 0
        updated_count = 0
        
        for _, row in gdp_df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            
            # Проверяем, существует ли уже запись для этой даты
            existing_data = dao.get_indicator_values(indicator_id)
            if not existing_data.empty:
                existing_record = existing_data[existing_data['date'] == date_str]
                if not existing_record.empty:
                    old_value = existing_record.iloc[0]['value']
                    new_value = row['value']
                    
                    # Если значение изменилось (ревизия), обновляем
                    if abs(old_value - new_value) > 0.01:  # Учитываем погрешность округления
                        print(f"Ревизия для {date_str}: {old_value:.1f} → {new_value:.1f} млрд")
                        updated_count += 1
            
            success = dao.add_indicator_value(
                indicator_id=indicator_id,
                date=date_str,
                value=row['value']
            )
            if success:
                saved_count += 1
        
        if updated_count > 0:
            print(f"Обнаружено {updated_count} ревизий данных ВВП")
        print(f"Обработано {saved_count} записей (новые + обновления).")
        print("Обработка завершена.")

        # Показываем последние записи для проверки
        latest_data = dao.get_indicator_values(indicator_id)
        if not latest_data.empty:
            print("\nПоследние 5 записей:")
            print(latest_data.tail(5)[['date', 'value']].to_string(index=False))

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