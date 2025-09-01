# collectors/gdp_revision_monitor.py
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()

# --- ДОБАВЛЕНИЕ КОРНЕВОЙ ПАПКИ В ПУТЬ ПОИСКА МОДУЛЕЙ ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from dao import IndicatorDAO
from collectors.fred_parser import get_fred_series_history

def main():
    """
    Специальный монитор ревизий данных ВВП.
    Сравнивает текущие данные FRED с сохраненными в БД и выявляет изменения.
    """
    dao = None
    try:
        print("=== МОНИТОР РЕВИЗИЙ ДАННЫХ ВВП США ===")
        dao = IndicatorDAO()

        # Находим ID индикатора ВВП
        # Поскольку метода get_all_indicators() нет, попробуем найти индикатор через add_indicator
        # который вернет существующий ID, если индикатор уже есть
        indicator_config = {
            'name': 'us_real_gdp',
            'full_name': 'Real Gross Domestic Product (Billions of Chained 2017 Dollars)',
            'source': 'FRED (Federal Reserve Bank of St. Louis)',
            'description': 'Real gross domestic product is the inflation adjusted value of the goods and services produced by labor and property located in the United States. Seasonally Adjusted Annual Rate.'
        }
        
        indicator_id = dao.add_indicator(**indicator_config)
        if not indicator_id:
            print("Не удалось найти/создать индикатор 'us_real_gdp'.")
            return
            
        print(f"Найден индикатор ВВП с ID: {indicator_id}")

        # Получаем ВСЕ данные из БД (не только за 2 года) для корректного сравнения
        all_db_data = dao.get_indicator_values(indicator_id)
        if all_db_data.empty:
            print("Нет данных в БД для сравнения.")
            return

        # Фильтруем данные за последние 2 года только для статистики
        cutoff_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        recent_db_data = all_db_data[all_db_data['date'] >= cutoff_date].copy()
        recent_db_data['date'] = pd.to_datetime(recent_db_data['date'])
        
        print(f"Всего данных в БД: {len(all_db_data)}")
        print(f"Данных в БД за последние 2 года: {len(recent_db_data)}")

        # Получаем свежие данные из FRED только за последние 2 года для анализа ревизий
        print("Загружаем свежие данные из FRED за последние 2 года...")
        fresh_data = get_fred_series_history('GDPC1', start_date=cutoff_date)
        
        if fresh_data.empty:
            print("Не удалось получить свежие данные из FRED.")
            return
            
        print(f"Получено из FRED: {len(fresh_data)} записей")

        # Сравниваем данные
        print("\n=== АНАЛИЗ РЕВИЗИЙ ===")
        revisions_found = False
        new_records_found = False
        
        # Создаем словарь для быстрого поиска ВСЕХ данных из БД (не только за 2 года)
        all_db_dict = {}
        if not all_db_data.empty:
            for _, row in all_db_data.iterrows():
                # Приводим к единому формату YYYY-MM-DD
                if isinstance(row['date'], str):
                    date_key = row['date']
                else:
                    date_key = row['date'].strftime('%Y-%m-%d')
                all_db_dict[date_key] = row['value']
        
        for _, fresh_row in fresh_data.iterrows():
            fresh_date = fresh_row['date'].strftime('%Y-%m-%d')
            fresh_value = fresh_row['value']
            
            # Проверяем, есть ли запись в БД вообще
            if fresh_date in all_db_dict:
                db_value = all_db_dict[fresh_date]
                difference = fresh_value - db_value
                
                # Если разница больше 0.1 млрд долларов, это ревизия
                if abs(difference) > 0.1:
                    revisions_found = True
                    percentage_change = (difference / db_value) * 100 if db_value != 0 else 0
                    print(f"📊 {fresh_date}: {db_value:.1f} → {fresh_value:.1f} млрд "
                          f"(ревизия: {difference:+.1f} млрд, {percentage_change:+.2f}%)")
            else:
                # Действительно новая запись (не существует в БД)
                print(f"🆕 {fresh_date}: новая запись {fresh_value:.1f} млрд")
                new_records_found = True

        if not revisions_found and not new_records_found:
            print("✅ Ревизий и новых записей не обнаружено. Данные актуальны.")
        else:
            if revisions_found:
                print(f"\n⚠️ Обнаружены ревизии существующих данных ВВП!")
                print("Рекомендуется запустить gdp_collector.py для обновления БД.")
            if new_records_found:
                print(f"\n🆕 Обнаружены новые записи данных ВВП!")
                print("Рекомендуется запустить gdp_collector.py для добавления новых данных.")

        # Показываем последние значения
        print(f"\n=== ПОСЛЕДНИЕ ДАННЫЕ ===")
        
        print("Последние 3 записи в БД:")
        if not recent_db_data.empty:
            for _, row in recent_db_data.tail(3).iterrows():
                print(f"  {row['date'].strftime('%Y-%m-%d')}: {row['value']:.1f} млрд")
        else:
            print("  Нет данных в БД")
            
        print("Последние 3 записи из FRED:")
        for _, row in fresh_data.tail(3).iterrows():
            print(f"  {row['date'].strftime('%Y-%m-%d')}: {row['value']:.1f} млрд")

        print("\n=== МОНИТОРИНГ ЗАВЕРШЕН ===")

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