# database_setup.py
# Финальная версия скрипта для создания базы данных.

import sqlite3
from pathlib import Path # Импортируем модуль для удобной работы с путями к файлам

# --- 1. Определение пути к файлу Базы Данных ---
# Находим домашнюю директорию текущего пользователя (работает на любой ОС).
home_dir = Path.home()
# Формируем полный путь к файлу БД, который будет лежать в папке "Documents".
# Если папки "Documents" не существует, скрипт может выдать ошибку.
# Убедитесь, что такая папка у вас есть.
DB_PATH = home_dir / 'Documents' / 'economic_indicators.db'


# --- 2. SQL-команды для создания таблиц ---
# Каждая команда основана на вашем Техническом задании.
# "CREATE TABLE IF NOT EXISTS" - безопасная команда, которая не вызовет ошибку,
# если таблицы уже были созданы ранее.

# Таблица 1: Справочник индикаторов
create_table_indicators = """
CREATE TABLE IF NOT EXISTS indicators (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    source TEXT,
    description TEXT
);
"""

# Таблица 2: Числовые значения индикаторов
create_table_indicator_values = """
CREATE TABLE IF NOT EXISTS indicator_values (
    id INTEGER PRIMARY KEY,
    indicator_id INTEGER,
    date TEXT NOT NULL,
    category TEXT,
    value REAL NOT NULL,
    created_at TEXT,
    FOREIGN KEY (indicator_id) REFERENCES indicators (id)
);
"""

# Таблица 3: Текстовые релизы отчётов
create_table_indicator_releases = """
CREATE TABLE IF NOT EXISTS indicator_releases (
    id INTEGER PRIMARY KEY,
    indicator_id INTEGER,
    date TEXT NOT NULL,
    category TEXT,
    release_data TEXT,
    source_url TEXT,
    created_at TEXT,
    FOREIGN KEY (indicator_id) REFERENCES indicators (id)
);
"""

# Таблица 4: Пользовательские комментарии
create_table_comments = """
CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY,
    indicator_id INTEGER,
    date TEXT NOT NULL,
    comment_text TEXT NOT NULL,
    created_at TEXT,
    FOREIGN KEY (indicator_id) REFERENCES indicators (id)
);
"""

def setup_database():
    """
    Основная функция для создания и настройки базы данных.
    """
    # Устанавливаем соединение с файлом БД по полному пути, который мы определили выше.
    # Если файла нет, SQLite его автоматически создаст.
    conn = sqlite3.connect(DB_PATH)
    # Курсор - это объект, который позволяет выполнять SQL-запросы.
    cursor = conn.cursor()
    
    print(f"База данных будет создана по пути: {DB_PATH}")
    print("Создание таблиц...")
    
    # Последовательно выполняем команды создания для каждой таблицы
    cursor.execute(create_table_indicators)
    cursor.execute(create_table_indicator_values)
    cursor.execute(create_table_indicator_releases)
    cursor.execute(create_table_comments)
    
    print("Таблицы успешно созданы.")
    
    # Сохраняем все сделанные изменения
    conn.commit()
    # Закрываем соединение с базой данных
    conn.close()

# Этот блок выполнится только в том случае,
# если мы запускаем этот файл напрямую (а не импортируем его).
if __name__ == '__main__':
    setup_database()