#!/usr/bin/env python3
# history_importers/import_building_permits_history.py

import argparse
import pandas as pd
import sqlite3
import os
from pathlib import Path
from dotenv import load_dotenv

# --- Настройки окружения ---
load_dotenv()
home_dir = Path.home()
DB_SUBDIR = os.getenv("DB_SUBDIR", "Documents")
DB_FILE = os.getenv("DB_FILE", "economic_indicators.db")
DB_PATH = home_dir / DB_SUBDIR / DB_FILE

INDICATOR_META = {
    "name": "building_permits_us",
    "full_name": "New Private Housing Units Authorized by Building Permits",
    "source": "FRED (Federal Reserve Bank of St. Louis)",
    "description": "Monthly data on new private housing units authorized by building permits by structure type. SAAR, thousands."
}

CATEGORY_MAP = {
    "total": "total",
    "1 unit": "1 unit",
    "2 to 4 units": "2-4 units",
    "5 units": "5+ units",
    "more than 5 units": "5+ units",
    "5 units or more": "5+ units",
}

def ensure_indicator(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM indicators WHERE name = ?", (INDICATOR_META["name"],))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO indicators (name, full_name, source, description) VALUES (?, ?, ?, ?)",
        (
            INDICATOR_META["name"],
            INDICATOR_META["full_name"],
            INDICATOR_META["source"],
            INDICATOR_META["description"],
        ),
    )
    conn.commit()
    return cur.lastrowid

def import_csv(csv_path: Path, dry_run: bool = False):
    df = pd.read_csv(csv_path, sep=None, engine="python")

    if "Date" not in df.columns:
        raise ValueError("CSV должен содержать колонку 'Date'")

    # Приводим даты к YYYY-MM-01
    df["Date"] = pd.to_datetime(df["Date"], format="%b %Y").dt.strftime("%Y-%m-01")

    conn = sqlite3.connect(DB_PATH)
    try:
        indicator_id = ensure_indicator(conn)
        cur = conn.cursor()

        if not dry_run:
            cur.execute("DELETE FROM indicator_values WHERE indicator_id = ?", (indicator_id,))
            print(f"🗑 Удалено {cur.rowcount} старых записей для building_permits_us")

        inserted = 0
        for _, row in df.iterrows():
            date_str = row["Date"]
            for col in df.columns:
                if col == "Date":
                    continue
                col_norm = col.strip().lower()
                if col_norm in CATEGORY_MAP:
                    category = CATEGORY_MAP[col_norm]
                else:
                    continue
                value = row[col]
                if pd.isna(value):
                    continue

                if not dry_run:
                    cur.execute(
                        """
                        INSERT INTO indicator_values (indicator_id, date, category, value, created_at)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (indicator_id, date_str, category, float(value)),
                    )
                inserted += 1

        if not dry_run:
            conn.commit()

        print(f"✅ Импорт завершён. Вставлено {inserted} записей")

    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Import Building Permits history (overwrite mode).")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report without DB writes")
    args = parser.parse_args()
    import_csv(Path(args.csv), dry_run=args.dry_run)

if __name__ == "__main__":
    main()
