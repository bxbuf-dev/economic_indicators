# collectors/fred_parser.py
import pandas as pd
from fredapi import Fred
from datetime import datetime

# --- КОНФИГУРАЦИЯ ---
FRED_API_KEY = ''

def get_fred_series_history(series_id: str, start_date: str = None) -> pd.DataFrame:
    """
    Универсальная функция для загрузки любого ряда из FRED API.
    
    Args:
        series_id: ID серии в FRED (например, 'PERMIT', 'UMCSENT')
        start_date: Дата начала в формате 'YYYY-MM-DD' или None для всей истории
        
    Returns:
        DataFrame с колонками ['date', 'value'] или пустой DataFrame при ошибке
    """
    try:
        print(f"Загрузка серии {series_id} из FRED API...")
        fred = Fred(api_key=FRED_API_KEY)
        
        series_data = fred.get_series(series_id, observation_start=start_date)
        
        if series_data.empty:
            print(f"Нет данных для серии {series_id}")
            return pd.DataFrame()
        
        # Преобразуем в стандартный формат
        df = series_data.reset_index()
        df.columns = ['date', 'value']
        df = df.dropna(subset=['value'])
        
        print(f"Загружено {len(df)} записей для {series_id}")
        return df
        
    except Exception as e:
        print(f"Ошибка при загрузке серии {series_id}: {e}")
        return pd.DataFrame()

def get_fred_calculated_series(series_configs: list, calculation_func, start_date: str = None) -> pd.DataFrame:
    """
    Функция для загрузки и расчета составных индикаторов (как Real M2).
    
    Args:
        series_configs: Список с конфигурацией серий [{'id': 'M2SL', 'name': 'M2SL'}, ...]
        calculation_func: Функция для расчета результата из DataFrame
        start_date: Дата начала загрузки
        
    Returns:
        DataFrame с колонками ['date', 'value']
    """
    try:
        print("Загрузка составных серий из FRED API...")
        fred = Fred(api_key=FRED_API_KEY)
        
        series_data = {}
        for config in series_configs:
            series_data[config['name']] = fred.get_series(config['id'], observation_start=start_date)
        
        # Объединяем все серии по датам
        df = pd.concat(series_data, axis=1, join='inner')
        
        if df.empty:
            print("Нет данных для расчета составного индикатора")
            return pd.DataFrame()
        
        # Применяем функцию расчета
        df['value'] = calculation_func(df)
        
        # Возвращаем в стандартном формате
        result_df = df[['value']].reset_index()
        result_df.columns = ['date', 'value']
        result_df = result_df.dropna(subset=['value'])
        
        print(f"Рассчитано {len(result_df)} записей для составного индикатора")
        return result_df
        
    except Exception as e:
        print(f"Ошибка при расчете составного индикатора: {e}")
        return pd.DataFrame()