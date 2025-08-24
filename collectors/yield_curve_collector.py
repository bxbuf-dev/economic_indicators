# economic_indicators/collectors/yield_curve_collector.py

import pandas as pd
import requests
import sys
import os
from datetime import date
import time

# Добавляем корневую папку проекта в путь
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from dao import IndicatorDAO

# --- Конфигурация сборщика ---
# НОВЫЙ, ПРОСТОЙ URL НА ОСНОВЕ ВАШЕГО ОТКРЫТИЯ
URL_TEMPLATE = 'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value={YYYYMM}'
INDICATOR_NAME = 'us_treasury_yield_curve'
INDICATOR_FULL_NAME = 'Daily Treasury Par Yield Curve Rates'
INDICATOR_SOURCE = 'U.S. Department of the Treasury'
INDICATOR_DESC = 'Nominal yield curve for U.S. Treasury securities.'


def fetch_yield_curve_data(year_month: str) -> pd.DataFrame | None:
    """Загружает данные за указанный месяц (формат YYYYMM), "притворяясь" браузером."""
    url = URL_TEMPLATE.format(YYYYMM=year_month)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        print(f"Загрузка данных за месяц: {year_month}...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        tables = pd.read_html(response.text)
        
        if not tables:
            print(f"  -> На странице не найдено таблиц за {year_month}.")
            return None
            
        print(f"  -> ✅ Данные за {year_month} успешно извлечены.")
        return tables[0]

    except requests.exceptions.RequestException as e:
        print(f"  -> ❌ Ошибка при загрузке страницы за {year_month}: {e}")
        return None


def process_and_filter_data(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает данные, выбирает нужные столбцы, преобразует типы и фильтрует по пятницам."""
    df = df.loc[:, ~df.columns.duplicated(keep='first')]
    df = df.rename(columns={df.columns[0]: 'Date'})
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
    df.dropna(subset=['Date'], inplace=True)
    
    DESIRED_COLUMNS = ['Date', '1 Mo', '2 Mo', '3 Mo', '4 Mo', '6 Mo', '1 Yr', '2 Yr', '3 Yr', '5 Yr', '7 Yr', '10 Yr', '20 Yr', '30 Yr']
    columns_to_keep = [col for col in DESIRED_COLUMNS if col in df.columns]
    processed_df = df[columns_to_keep].copy()

    for col in processed_df.columns:
        if col != 'Date':
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
            
    friday_df = processed_df[processed_df['Date'].dt.dayofweek == 4].copy()
    return friday_df


def main():
    """Главный скрипт-оркестратор, загружает историю по месяцам."""
    print(f"\n--- Запуск сборщика для индикатора: {INDICATOR_FULL_NAME} ---")
    dao = IndicatorDAO()

    indicator_id = dao.add_indicator(
        name=INDICATOR_NAME, full_name=INDICATOR_FULL_NAME,
        source=INDICATOR_SOURCE, description=INDICATOR_DESC
    )

    latest_date_str = dao.get_latest_indicator_date(indicator_id)
    if latest_date_str:
        start_date = pd.to_datetime(latest_date_str)
    else:
        start_date = pd.to_datetime("2000-01-01")
        print("База данных пуста. Начинаем загрузку с 01.01.2000.")
    
    end_date = pd.to_datetime(date.today())

    # Создаем список всех месяцев, которые нужно загрузить
    months_to_fetch = pd.date_range(start=start_date, end=end_date, freq='MS').strftime('%Y%m').unique()

    if len(months_to_fetch) == 0:
        print("✅ База данных находится в актуальном состоянии. Новых данных нет.")
        dao.close()
        return

    total_saved_points = 0
    # Итерируем по каждому месяцу и загружаем данные
    for year_month in months_to_fetch:
        raw_data = fetch_yield_curve_data(year_month)
        
        if raw_data is not None:
            processed_fridays = process_and_filter_data(raw_data)
            
            if not processed_fridays.empty:
                melted_df = processed_fridays.melt(
                    id_vars=['Date'], var_name='category', value_name='value'
                ).dropna(subset=['value'])
                
                points_to_save = len(melted_df)
                if points_to_save > 0:
                    print(f"  -> Найдено {points_to_save} точек данных для сохранения.")
                    for index, row in melted_df.iterrows():
                        dao.add_indicator_value(
                            indicator_id=indicator_id, date=row['Date'].strftime('%Y-%m-%d'),
                            category=row['category'], value=row['value']
                        )
                    total_saved_points += points_to_save
        
        # Делаем небольшую паузу, чтобы не перегружать сервер
        time.sleep(1) 

    print(f"\n✅ Загрузка завершена. Всего сохранено в БД: {total_saved_points} точек данных.")
    dao.close()
    print("--- Работа сборщика завершена ---")


if __name__ == '__main__':
    main()