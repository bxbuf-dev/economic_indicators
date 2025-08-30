# dao.py
import sqlite3
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# --- Определение пути к файлу Базы Данных ---
home_dir = Path.home()
DB_PATH = home_dir / 'Documents' / 'economic_indicators.db'

class IndicatorDAO:
    def __init__(self):
        try:
            self.conn = sqlite3.connect(DB_PATH)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Ошибка подключения к БД: {e}")
            self.conn = None

    def add_indicator(self, name, full_name, source, description):
        sql = "INSERT INTO indicators (name, full_name, source, description) VALUES (?, ?, ?, ?)"
        try:
            self.cursor.execute(sql, (name, full_name, source, description))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:
            id_sql = "SELECT id FROM indicators WHERE name = ?"
            self.cursor.execute(id_sql, (name,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении индикатора: {e}")
            return None

    def add_indicator_value(self, indicator_id, date, value, category=''):
        created_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT OR IGNORE INTO indicator_values (indicator_id, date, category, value, created_at) VALUES (?, ?, ?, ?, ?)"
        try:
            self.cursor.execute(sql, (indicator_id, date, category, value, created_time))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении значения в БД: {e}")

    def get_latest_indicator_date(self, indicator_id: int) -> str | None:
        query = "SELECT MAX(date) FROM indicator_values WHERE indicator_id = ?"
        self.cursor.execute(query, (indicator_id,))
        result = self.cursor.fetchone()
        return result[0] if result and result[0] else None

    def get_indicator_values_by_category(self, indicator_id, category=''):
        """
        Get indicator values filtered by category
        Returns pandas DataFrame
        """
        query = """
        SELECT date, value, category 
        FROM indicator_values 
        WHERE indicator_id = ? AND category = ?
        ORDER BY date DESC
        """
        
        df = pd.read_sql_query(query, self.conn, params=(indicator_id, category))
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        
        return df

    def close(self):
        if self.conn:
            self.conn.close()
    
    # --- остальные get/add методы ---
    def add_indicator_release(self, indicator_id, date, release_data, source_url, category=None):
        created_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        release_data_json = json.dumps(release_data, indent=4)
        sql = "INSERT INTO indicator_releases (indicator_id, date, category, release_data, source_url, created_at) VALUES (?, ?, ?, ?, ?, ?)"
        self.cursor.execute(sql, (indicator_id, date, category, release_data_json, source_url, created_time))
        self.conn.commit()

    def add_comment(self, indicator_id, date, comment_text):
        created_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO comments (indicator_id, date, comment_text, created_at) VALUES (?, ?, ?, ?)"
        self.cursor.execute(sql, (indicator_id, date, comment_text, created_time))
        self.conn.commit()

    def get_indicator_values(self, indicator_id):
        sql = "SELECT date, value FROM indicator_values WHERE indicator_id = ? ORDER BY date"
        df = pd.read_sql_query(sql, self.conn, params=(indicator_id,))
        df['date'] = pd.to_datetime(df['date'])
        return df