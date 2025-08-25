# _db_inspector.py
import sqlite3
from pathlib import Path

# --- КОНФИГУРАЦИЯ ---
DB_PATH = Path.home() / 'Documents' / 'economic_indicators.db'
INDICATOR_NAME_TO_INSPECT = 'us_treasury_yield_curve'

def inspect_db():
    """
    Выводит диагностическую информацию по индикатору:
    1. Список уникальных категорий.
    2. Последние 10 записей.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        print(f"Подключено к базе данных: {DB_PATH}")

        # 1. Находим ID индикатора
        cursor.execute("SELECT id FROM indicators WHERE name = ?", (INDICATOR_NAME_TO_INSPECT,))
        result = cursor.fetchone()
        if not result:
            print(f"Индикатор '{INDICATOR_NAME_TO_INSPECT}' не найден.")
            return
        indicator_id = result[0]

        # 2. Получаем список уникальных категорий
        print("\n" + "="*20 + " УНИКАЛЬНЫЕ КАТЕГОРИИ " + "="*20)
        cursor.execute("SELECT DISTINCT category FROM indicator_values WHERE indicator_id = ? ORDER BY category", (indicator_id,))
        categories = cursor.fetchall()
        if categories:
            for cat in categories:
                # Выводим категорию в кавычках, чтобы были видны пробелы
                print(f"'{cat['category']}'")
        else:
            print("Категории не найдены.")

        # 3. Получаем последние 10 записей, чтобы проверить даты
        print("\n" + "="*20 + " ПОСЛЕДНИЕ 10 ЗАПИСЕЙ " + "="*23)
        cursor.execute("SELECT date, category, value FROM indicator_values WHERE indicator_id = ? ORDER BY date DESC, category LIMIT 10", (indicator_id,))
        last_entries = cursor.fetchall()
        if last_entries:
            print(f"{'Date':<12} | {'Category':<10} | {'Value'}")
            print("-"*35)
            for entry in last_entries:
                print(f"{entry['date']:<12} | {entry['category']:<10} | {entry['value']}")
        else:
            print("Записи не найдены.")

    except sqlite3.Error as e:
        print(f"Произошла ошибка SQLite: {e}")
    finally:
        if conn:
            conn.close()
            print("\nСоединение с базой данных закрыто.")

if __name__ == '__main__':
    inspect_db()