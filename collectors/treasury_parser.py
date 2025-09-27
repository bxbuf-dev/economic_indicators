# collectors/treasury_parser.py
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime, date

URL_TEMPLATE = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "TextView?type={rate_type}&field_tdr_date_value={year_month}"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def _read_table_html(response_text: str) -> pd.DataFrame | None:
    """
    Находим первую таблицу на странице и превращаем её в DataFrame.
    BeautifulSoup повышает устойчивость, когда read_html по всей странице мимо.
    """
    soup = BeautifulSoup(response_text, "lxml")
    table = soup.find("table")
    if table is None:
        return None
    tables = pd.read_html(StringIO(str(table)))
    if not tables:
        return None
    return tables[0]

def _normalize_date_column(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame | None:
    """
    Находим колонку даты без учёта регистра и приводим к datetime (mm/dd/YYYY).
    """
    date_col = next((c for c in df.columns if str(c).strip().lower() == "date"), None)
    if not date_col:
        print(f"❌ Не найдена колонка даты за {year}-{month:02d}. Колонки: {list(df.columns)}")
        return None
    df = df.rename(columns={date_col: "date"}).copy()
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y", errors="coerce")
    df = df.dropna(subset=["date"])
    return df

def _reduce_month_rows(long_df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """
    Сужаем ежедневные данные месяца до одной даты:
    1) Если есть пятницы — берём все ряды за пятницы (как было).
    2) Если пятниц нет — берём последнюю доступную дату месяца (fallback).
    """
    fridays = long_df[long_df["date"].dt.dayofweek == 4]
    if not fridays.empty:
        return fridays.copy()

    last_date = long_df["date"].max()
    fallback = long_df[long_df["date"] == last_date].copy()
    print(f"⚠️ За {year}-{month:02d} пятниц нет, взята последняя дата: {last_date.date()}")
    return fallback

def _fetch_and_parse_month(rate_type: str, year: int, month: int) -> pd.DataFrame | None:
    """
    Загрузка и парсинг данных за один месяц.
    rate_type:
      - 'daily_treasury_yield_curve' (номинальная кривая)
      - 'daily_treasury_real_yield_curve' (реальная кривая)
    """
    year_month_str = f"{year}{month:02d}"
    url = URL_TEMPLATE.format(rate_type=rate_type, year_month=year_month_str)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        df = _read_table_html(resp.text)
        if df is None or df.empty:
            print(f"❌ Нет таблицы в ответе Treasury за {year}-{month:02d}")
            return None

        df = _normalize_date_column(df, year, month)
        if df is None or df.empty:
            return None

        # В long-формат: категория — любой столбец, кроме date.
        id_vars = ["date"]
        value_vars = [c for c in df.columns if c not in id_vars]
        long_df = pd.melt(df, id_vars=id_vars, value_vars=value_vars,
                          var_name="category", value_name="value")
        long_df.dropna(subset=["value"], inplace=True)

        # Числовые значения (бывает '—' или текст)
        long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
        long_df.dropna(subset=["value"], inplace=True)

        # Сужаем месяц до пятниц или последней доступной даты
        return _reduce_month_rows(long_df, year, month)

    except requests.RequestException as e:
        print(f"⚠️ Ошибка запроса для {year}-{month:02d}: {e}")
        return None
    except Exception as e:
        print(f"❌ Неожиданная ошибка при обработке {year}-{month:02d}: {e}")
        return None

def get_treasury_history(rate_type: str, start_date: date) -> pd.DataFrame:
    """
    Возвращает конкатенацию по месяцам с сузившимися датами (см. _reduce_month_rows).
    """
    all_parts: list[pd.DataFrame] = []
    cur = start_date
    end = datetime.now().date()

    print(f"Начинаем загрузку данных с {start_date} по {end}")

    while cur <= end:
        print(f"Обработка {cur.year}-{cur.month:02d}...")
        part = _fetch_and_parse_month(rate_type, cur.year, cur.month)
        if part is not None and not part.empty:
            all_parts.append(part)

        # следующий месяц
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1, day=1)
        else:
            cur = cur.replace(month=cur.month + 1, day=1)

    if not all_parts:
        return pd.DataFrame()

    out = pd.concat(all_parts, ignore_index=True)
    # страховка: сортировка
    out.sort_values(["date", "category"], inplace=True, ignore_index=True)
    return out
