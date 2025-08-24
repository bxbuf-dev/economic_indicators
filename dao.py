# dao.py
# Финальная версия. Содержит только класс IndicatorDAO для импорта в другие модули.

import sqlite3
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# --- Определение пути к файлу Базы Данных ---
home_dir = Path.home()
DB_PATH = home_dir / 'Documents' / 'economic_indicators.db'


class IndicatorDAO:
    """
    Класс для инкапсуляции всех операций с базой данных экономических индикаторов.
    Является единственным посредником между приложением и БД.
    """
    def __init__(self):
        """Конструктор класса. При создании объекта сразу подключается к БД."""
        try:
            self.conn = sqlite3.connect(DB_PATH)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Ошибка подключения к БД: {e}")
            self.conn = None

    def _dangerously_clear_all_tables(self):
        """Служебный метод для полной очистки таблиц. Использовать только для тестов!"""
        print("\n--- ОЧИСТКА ДАННЫХ В ТАБЛИЦАХ ---")
        try:
            self.cursor.execute("PRAGMA foreign_keys = OFF;")
            self.cursor.execute("DELETE FROM comments;")
            self.cursor.execute("DELETE FROM indicator_values;")
            self.cursor.execute("DELETE FROM indicator_releases;")
            self.cursor.execute("DELETE FROM indicators;")
            self.conn.commit()
            print("Все таблицы очищены.")
        finally:
            self.cursor.execute("PRAGMA foreign_keys = ON;")

    def add_indicator(self, name, full_name, source, description):
        """Добавляет новый индикатор в справочник 'indicators'."""
        sql = "INSERT INTO indicators (name, full_name, source, description) VALUES (?, ?, ?, ?)"
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, (name, full_name, source, description))
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            cursor = self.conn.cursor()
            id_sql = "SELECT id FROM indicators WHERE name = ?"
            cursor.execute(id_sql, (name,))
            result = cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении индикатора: {e}")
            return None

    def add_indicator_value(self, indicator_id, date, value, category=None):
        """Добавляет одно числовое значение для индикатора в 'indicator_values'."""
        created_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO indicator_values (indicator_id, date, category, value, created_at) VALUES (?, ?, ?, ?, ?)"
        try:
            self.cursor.execute(sql, (indicator_id, date, category, value, created_time))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении значения в БД: {e}")

    def add_indicator_release(self, indicator_id, date, release_data, source_url, category=None):
        """Добавляет структурированный текстовый релиз в 'indicator_releases'."""
        created_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        release_data_json = json.dumps(release_data, indent=4)
        sql = "INSERT INTO indicator_releases (indicator_id, date, category, release_data, source_url, created_at) VALUES (?, ?, ?, ?, ?, ?)"
        try:
            self.cursor.execute(sql, (indicator_id, date, category, release_data_json, source_url, created_time))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении релиза в БД: {e}")

    # --- НОВЫЙ МЕТОД ---
    def add_comment(self, indicator_id, date, comment_text):
        """Добавляет пользовательский комментарий в таблицу 'comments'."""
        created_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO comments (indicator_id, date, comment_text, created_at) VALUES (?, ?, ?, ?)"
        try:
            self.cursor.execute(sql, (indicator_id, date, comment_text, created_time))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении комментария: {e}")

    def get_indicator_values(self, indicator_id):
        """Извлекает все числовые значения для индикатора в виде pandas.DataFrame."""
        sql = "SELECT date, value FROM indicator_values WHERE indicator_id = ? ORDER BY date"
        try:
            df = pd.read_sql_query(sql, self.conn, params=(indicator_id,))
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            return pd.DataFrame()

    def get_indicator_release(self, indicator_id, date):
        """Извлекает и декодирует JSON-релиз для индикатора на конкретную дату."""
        sql = "SELECT release_data, source_url FROM indicator_releases WHERE indicator_id = ? AND date = ?"
        cursor = self.conn.cursor()
        cursor.execute(sql, (indicator_id, date))
        result = cursor.fetchone()
        if result:
            release_dict = json.loads(result['release_data'])
            source_url = result['source_url']
            return release_dict, source_url
        else:
            return None, None

    # --- НОВЫЙ МЕТОД ---
    def get_comments(self, indicator_id):
        """Извлекает все комментарии для индикатора в виде pandas.DataFrame."""
        sql = "SELECT date, comment_text FROM comments WHERE indicator_id = ? ORDER BY date"
        try:
            df = pd.read_sql_query(sql, self.conn, params=(indicator_id,))
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception as e:
            print(f"Ошибка при получении комментариев: {e}")
            return pd.DataFrame()

    def get_latest_indicator_date(self, indicator_id: int) -> str | None:
        """
        Находит самую последнюю дату для указанного индикатора.

        Параметры:
            indicator_id (int): ID индикатора.

        Возвращает:
            str: Дата в формате 'YYYY-MM-DD' или None, если данных нет.
        """
        query = "SELECT MAX(date) FROM indicator_values WHERE indicator_id = ?"
        self.cursor.execute(query, (indicator_id,))
        result = self.cursor.fetchone()
        if result and result[0]:
            return result[0]
        return None

    def close(self):
        """Закрывает соединение с базой данных."""
        if self.conn:
            self.conn.close()