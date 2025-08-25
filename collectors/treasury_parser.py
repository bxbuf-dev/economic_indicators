# collectors/treasury_parser.py
import pandas as pd
import requests
from io import StringIO
from datetime import datetime

# --- КОНФИГУРАЦИЯ ---
URL_TEMPLATE = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type={rate_type}&field_tdr_date_value={year_month}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

def _fetch_and_parse_month(rate_type: str, year: int, month: int) -> pd.DataFrame | None:
    """
    Внутренняя функция для загрузки и парсинга данных за ОДИН месяц.
    """
    year_month_str = f"{year}{month:02d}"
    url = URL_TEMPLATE.format(rate_type=rate_type, year_month=year_month_str)
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        tables = pd.read_html(StringIO(response.text))
        if not tables:
            return None
            
        df = tables[0]
        
        df.rename(columns={'Date': 'date'}, inplace=True)
        df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
        
        id_vars = ['date']
        value_vars = [col for col in df.columns if col not in id_vars]
        
        long_df = pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name='category', value_name='value')
        long_df.dropna(subset=['value'], inplace=True)
        
        fridays_only_df = long_df[long_df['date'].dt.dayofweek == 4].copy()
        
        return fridays_only_df

    except requests.RequestException:
        # Игнорируем ошибки для будущих месяцев, где данных еще нет
        return None
    except Exception as e:
        print(f"Неожиданная ошибка при обработке {year}-{month:02d}: {e}")
        return None

def get_treasury_history(rate_type: str, start_date: datetime.date) -> pd.DataFrame:
    """
    Главная функция. Получает всю историю данных с сайта Treasury с указанной даты.
    Содержит цикл перебора месяцев.
    """
    all_data = []
    current_date = start_date
    end_date = datetime.now().date()
    
    print(f"Начинаем загрузку данных с {start_date} по {end_date}")
    
    while current_date <= end_date:
        print(f"Обработка {current_date.year}-{current_date.month:02d}...")
        monthly_df = _fetch_and_parse_month(rate_type, current_date.year, current_date.month)
        
        if monthly_df is not None and not monthly_df.empty:
            all_data.append(monthly_df)

        # Переход к следующему месяцу
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
            
    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)