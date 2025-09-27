# database_setup.py
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
import os

# --- Загрузка переменных окружения ---
load_dotenv()
home_dir = Path.home()
DB_SUBDIR = os.getenv("DB_SUBDIR", "Documents")
DB_FILE = os.getenv("DB_FILE", "economic_indicators.db")
DB_PATH = home_dir / DB_SUBDIR / DB_FILE
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def setup_database():
    """
    Инициализирует базу данных, удаляя старые таблицы и создавая новые.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print(f"Подключено к базе данных SQLite: {DB_PATH}")

        # --- Удаление старых таблиц для чистой установки ---
        cursor.execute("DROP TABLE IF EXISTS comments;")
        cursor.execute("DROP TABLE IF EXISTS indicator_releases;")
        cursor.execute("DROP TABLE IF EXISTS indicator_values;")
        cursor.execute("DROP TABLE IF EXISTS indicators;")
        print("Старые таблицы удалены.")

        # --- Создание таблицы indicators (Справочник индикаторов) ---
        cursor.execute("""
        CREATE TABLE indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            full_name TEXT,
            source TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        print("Таблица 'indicators' создана.")

        # --- Создание таблицы indicator_values (Числовые значения) ---
        cursor.execute("""
        CREATE TABLE indicator_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT '',
            value REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (indicator_id) REFERENCES indicators (id),
            UNIQUE(indicator_id, date, category)
        );
        """)
        print("Таблица 'indicator_values' создана с UNIQUE ограничением.")

        # --- Создание остальных таблиц ---
        cursor.execute("""
        CREATE TABLE indicator_releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            category TEXT,
            release_data TEXT,
            source_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (indicator_id) REFERENCES indicators (id)
        );
        """)
        cursor.execute("""
        CREATE TABLE comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            comment_text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (indicator_id) REFERENCES indicators (id)
        );
        """)
        print("Таблицы 'indicator_releases' и 'comments' созданы.")

        conn.commit()
        print("Структура базы данных успешно создана/обновлена.")

    except sqlite3.Error as e:
        print(f"Ошибка при работе с SQLite: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    setup_database()
