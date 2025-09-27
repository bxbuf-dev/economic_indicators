# universal_record_deleter.py - Универсальный скрипт для удаления данных из всех таблиц
import sqlite3
from dotenv import load_dotenv
import os
from pathlib import Path
import pandas as pd

# --- Путь к БД ---2
load_dotenv() # Загружаем переменные окружения из .env файла
home_dir = Path.home()
DB_SUBDIR = os.getenv("DB_SUBDIR", "Documents")
DB_FILE = os.getenv("DB_FILE", "economic_indicators.db")
DB_PATH = home_dir / DB_SUBDIR / DB_FILE

def show_indicators():
    """Показать все доступные индикаторы с количеством записей во всех таблицах"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT i.id, i.name, i.full_name,
                   (SELECT COUNT(*) FROM indicator_values WHERE indicator_id = i.id) as values_count,
                   (SELECT COUNT(*) FROM indicator_releases WHERE indicator_id = i.id) as releases_count,
                   (SELECT COUNT(*) FROM comments WHERE indicator_id = i.id) as comments_count
            FROM indicators i
            ORDER BY i.id
        """)
        
        indicators = cursor.fetchall()
        
        if not indicators:
            print("❌ В БД нет индикаторов")
            return []
        
        print("📊 ДОСТУПНЫЕ ИНДИКАТОРЫ:")
        print("="*90)
        
        for row in indicators:
            id_val, name, full_name, values_count, releases_count, comments_count = row
            print(f"ID: {id_val} | {name}")
            print(f"    📈 Values: {values_count} | 📰 Releases: {releases_count} | 💬 Comments: {comments_count}")
            print(f"    {full_name}")
            print("-" * 50)
        
        conn.close()
        return indicators
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при получении списка индикаторов: {e}")
        return []

def show_recent_data(indicator_id, limit=2):
    """Показать последние записи индикатора (по датам) из всех таблиц"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Проверяем существование индикатора
        cursor.execute("SELECT name, full_name FROM indicators WHERE id = ?", (indicator_id,))
        indicator = cursor.fetchone()
        
        if not indicator:
            print(f"❌ Индикатор с ID {indicator_id} не найден")
            conn.close()
            return {}
        
        name, full_name = indicator
        print(f"\n📈 ПОСЛЕДНИЕ {limit} ЗАПИСЕЙ ДЛЯ: {name}")
        print(f"    {full_name}")
        print("="*90)
        
        # Получаем последние уникальные даты из всех таблиц
        cursor.execute("""
            SELECT DISTINCT date FROM (
                SELECT date FROM indicator_values WHERE indicator_id = ?
                UNION
                SELECT date FROM indicator_releases WHERE indicator_id = ?
                UNION  
                SELECT date FROM comments WHERE indicator_id = ?
            )
            ORDER BY date DESC
            LIMIT ?
        """, (indicator_id, indicator_id, indicator_id, limit))
        
        recent_dates = [row[0] for row in cursor.fetchall()]
        
        if not recent_dates:
            print("❌ Нет данных для этого индикатора")
            conn.close()
            return {}
        
        print(f"Найдены данные за даты: {', '.join(recent_dates)}")
        
        data = {'dates': recent_dates, 'values': [], 'releases': [], 'comments': []}
        
        # Для каждой даты показываем ВСЕ связанные данные
        for date_str in recent_dates:
            print(f"\n🗓️  ДАТА: {date_str}")
            print("="*50)
            
            # VALUES для этой даты
            cursor.execute("""
                SELECT id, date, category, value, created_at
                FROM indicator_values 
                WHERE indicator_id = ? AND date = ?
                ORDER BY category
            """, (indicator_id, date_str))
            
            date_values = cursor.fetchall()
            data['values'].extend(date_values)
            
            if date_values:
                print("📈 VALUES:")
                print(f"{'ID':<6} | {'Категория':<15} | {'Значение':<10} | {'Создано'}")
                print("-" * 50)
                for record in date_values:
                    record_id, date, category, value, created_at = record
                    category_str = category if category else "(без категории)"
                    created_short = created_at[:16] if created_at else ""
                    print(f"{record_id:<6} | {category_str:<15} | {value:<10} | {created_short}")
            
            # RELEASES для этой даты
            cursor.execute("""
                SELECT id, date, category, source_url, created_at
                FROM indicator_releases 
                WHERE indicator_id = ? AND date = ?
                ORDER BY category
            """, (indicator_id, date_str))
            
            date_releases = cursor.fetchall()
            data['releases'].extend(date_releases)
            
            if date_releases:
                print("\n📰 RELEASES:")
                print(f"{'ID':<6} | {'Категория':<15} | {'URL':<30} | {'Создано'}")
                print("-" * 65)
                for record in date_releases:
                    record_id, date, category, source_url, created_at = record
                    category_str = category if category else "(без категории)"
                    url_short = (source_url[:27] + "...") if source_url and len(source_url) > 30 else (source_url or "")
                    created_short = created_at[:16] if created_at else ""
                    print(f"{record_id:<6} | {category_str:<15} | {url_short:<30} | {created_short}")
            
            # COMMENTS для этой даты
            cursor.execute("""
                SELECT id, date, comment_text, created_at
                FROM comments 
                WHERE indicator_id = ? AND date = ?
                ORDER BY created_at
            """, (indicator_id, date_str))
            
            date_comments = cursor.fetchall()
            data['comments'].extend(date_comments)
            
            if date_comments:
                print("\n💬 COMMENTS:")
                print(f"{'ID':<6} | {'Комментарий':<40} | {'Создано'}")
                print("-" * 55)
                for record in date_comments:
                    record_id, date, comment_text, created_at = record
                    comment_short = (comment_text[:37] + "...") if len(comment_text) > 40 else comment_text
                    created_short = created_at[:16] if created_at else ""
                    print(f"{record_id:<6} | {comment_short:<40} | {created_short}")
            
            if not date_values and not date_releases and not date_comments:
                print("Нет данных за эту дату")
        
        # Итоговая статистика
        total_records = len(data['values']) + len(data['releases']) + len(data['comments'])
        print(f"\n📊 ИТОГО ЗА {len(recent_dates)} ДАТ:")
        print(f"    📈 Values: {len(data['values'])}")
        print(f"    📰 Releases: {len(data['releases'])}")
        print(f"    💬 Comments: {len(data['comments'])}")
        print(f"    📋 Всего записей: {total_records}")
        
        conn.close()
        return data
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при получении записей: {e}")
        return {}

# --- Новая функция ---
def show_categories(indicator_id):
    """Показать все уникальные категории для выбранного индикатора"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Проверяем существование индикатора
        cursor.execute("SELECT name, full_name FROM indicators WHERE id = ?", (indicator_id,))
        indicator = cursor.fetchone()
        if not indicator:
            print(f"❌ Индикатор с ID {indicator_id} не найден")
            conn.close()
            return []

        name, full_name = indicator
        print(f"\n📂 КАТЕГОРИИ ДЛЯ: {name}")
        print(f"    {full_name}")
        print("="*50)

        cursor.execute("""
            SELECT DISTINCT category 
            FROM indicator_values 
            WHERE indicator_id = ?
            ORDER BY category
        """, (indicator_id,))
        categories = [row[0] for row in cursor.fetchall()]

        if categories:
            for cat in categories:
                cat_str = cat if cat else "(без категории)"
                print(f" - {cat_str}")
        else:
            print("❌ Категории не найдены")

        conn.close()
        return categories

    except sqlite3.Error as e:
        print(f"❌ Ошибка при получении категорий: {e}")
        return []

def delete_by_date_and_indicator(indicator_id, date_str):
    ...  # остальной код без изменений

def delete_record_by_table_and_id(table_name, record_id):
    ...  # остальной код без изменений

def main():
    print("🗑️  УНИВЕРСАЛЬНЫЙ УДАЛЯТОР ДАННЫХ")
    print("="*50)
    
    if not DB_PATH.exists():
        print(f"❌ База данных не найдена: {DB_PATH}")
        return
    
    while True:
        print("\n📋 МЕНЮ:")
        print("0. Выход")
        print("1. Показать все индикаторы")
        print("2. Показать последние данные индикатора")
        print("3. Удалить ВСЕ данные по индикатору и дате")
        print("4. Удалить конкретную запись по таблице и ID")
        print("5. Показать категории индикатора")
        
        choice = input("\nВыберите действие (1-6): ").strip()
        
        if choice == '1':
            show_indicators()
            
        elif choice == '2':
            try:
                indicator_id = int(input("Введите ID индикатора: "))
                limit = input("Количество последних ЗАПИСЕЙ (по датам) для показа (по умолчанию 2): ").strip()
                limit = int(limit) if limit else 2
                show_recent_data(indicator_id, limit)
            except ValueError:
                print("❌ Некорректный ID или число")
                
        elif choice == '3':
            ...  # остальной код без изменений
                
        elif choice == '4':
            ...  # остальной код без изменений
                
        elif choice == '0':
            print("👋 Выход")
            break

        elif choice == '5':
            try:
                indicator_id = int(input("Введите ID индикатора: "))
                show_categories(indicator_id)
            except ValueError:
                print("❌ Некорректный ID")
                
        else:
            print("❌ Некорректный выбор")

if __name__ == "__main__":
    main()
