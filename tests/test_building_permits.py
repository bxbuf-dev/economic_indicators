#!/usr/bin/env python3
# tests/test_building_permits.py

import sqlite3
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
home_dir = Path.home()
DB_SUBDIR = os.getenv("DB_SUBDIR", "Documents")
DB_FILE = os.getenv("DB_FILE", "economic_indicators.db")
DB_PATH = home_dir / DB_SUBDIR / DB_FILE

conn = sqlite3.connect(DB_PATH)

row = conn.execute("SELECT id FROM indicators WHERE name = ?", ("building_permits_us",)).fetchone()
if not row:
    print("❌ Indicator building_permits_us not found")
    exit(1)
iid = row[0]
print(f"▶ Indicator ID: {iid}")

cats = pd.read_sql_query(
    "SELECT DISTINCT category FROM indicator_values WHERE indicator_id = ? ORDER BY category",
    conn, params=(iid,)
)
print("Categories:\n", cats)

df = pd.read_sql_query(
    """
    SELECT date, category, value
    FROM indicator_values
    WHERE indicator_id = ?
    ORDER BY date DESC, category
    LIMIT 24
    """,
    conn, params=(iid,)
)
print("\nLast records:\n", df)

empty = conn.execute(
    "SELECT COUNT(*) FROM indicator_values WHERE indicator_id = ? AND (category IS NULL OR TRIM(category) = '')",
    (iid,)
).fetchone()[0]
print(f"\nEmpty-category rows: {empty}")

conn.close()
