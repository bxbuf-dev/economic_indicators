# _db_maintenance_tool.py
import sqlite3
from pathlib import Path

# --- КОНФИГУРАЦИЯ ---
DB_PATH = Path.home() / 'Documents' / 'economic_indicators.db'
INDICATOR_NAME_TO_CLEAN = 'us_treasury_yield_curve' # <-- ПРАВИЛЬНОЕ ИМЯ
CLEANUP_START_DATE = '2025-08-01'
CATEGORIES_TO_DELETE = ['3 Mo', '6 Mo', '10 Yr', '20 Yr'] 

def cleanup_indicator_data():
    """ Удаляет записи для указанного индикатора и списка категорий после определенной даты. """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print(f"Подключено к базе данных: {DB_PATH}")

        # 1. Находим ID индикатора
        cursor.execute("SELECT id FROM indicators WHERE name = ?", (INDICATOR_NAME_TO_CLEAN,))
        result = cursor.fetchone()
        if not result:
            print(f"Индикатор '{INDICATOR_NAME_TO_CLEAN}' не найден.")
            return

        indicator_id = result[0]
        print(f"Найден индикатор '{INDICATOR_NAME_TO_CLEAN}' с ID: {indicator_id}")

        # 2. Формируем и выполняем SQL-запрос на удаление
        placeholders = ','.join('?' for _ in CATEGORIES_TO_DELETE)
        sql_delete = f"DELETE FROM indicator_values WHERE indicator_id = ? AND date >= ? AND category IN ({placeholders})"
        params = [indicator_id, CLEANUP_START_DATE] + CATEGORIES_TO_DELETE
        cursor.execute(sql_delete, params)
        deleted_count = cursor.rowcount
        conn.commit()
        
        if deleted_count > 0:
            print(f"Успешно удалено {deleted_count} записей для категорий {CATEGORIES_TO_DELETE}.")
        else:
            print(f"Записи для указанных категорий и даты не найдены.")

    except sqlite3.Error as e:
        print(f"Произошла ошибка SQLite: {e}")
    finally:
        if conn:
            conn.close()
            print("Соединение с базой данных закрыто.")

if __name__ == '__main__':
    print("--- Запуск инструмента частичной очистки данных ---")
    cleanup_indicator_data()
    print("--- Очистка завершена ---")