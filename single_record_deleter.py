# single_record_deleter.py - Скрипт для удаления конкретных записей из indicator_values
import sqlite3
from pathlib import Path
import pandas as pd

# --- Путь к БД ---
home_dir = Path.home()
DB_PATH = home_dir / 'Documents' / 'economic_indicators.db'

def show_indicators():
    """Показать все доступные индикаторы"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, name, full_name,
                   (SELECT COUNT(*) FROM indicator_values WHERE indicator_id = i.id) as values_count
            FROM indicators i
            ORDER BY id
        """)
        
        indicators = cursor.fetchall()
        
        if not indicators:
            print("❌ В БД нет индикаторов")
            return []
        
        print("📊 ДОСТУПНЫЕ ИНДИКАТОРЫ:")
        print("="*80)
        
        for row in indicators:
            id_val, name, full_name, values_count = row
            print(f"ID: {id_val} | {name} | Записей: {values_count}")
            print(f"    {full_name}")
            print("-" * 40)
        
        conn.close()
        return indicators
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при получении списка индикаторов: {e}")
        return []

def show_recent_records(indicator_id, limit=20):
    """Показать последние записи для выбранного индикатора"""
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
        print(f"\n📈 ПОСЛЕДНИЕ {limit} ЗАПИСЕЙ ДЛЯ: {name}")
        print(f"    {full_name}")
        print("="*80)
        
        cursor.execute("""
            SELECT id, date, category, value, created_at
            FROM indicator_values 
            WHERE indicator_id = ?
            ORDER BY date DESC, category
            LIMIT ?
        """, (indicator_id, limit))
        
        records = cursor.fetchall()
        
        if not records:
            print("❌ Записей не найдено")
            conn.close()
            return []
        
        print(f"{'ID':<6} | {'Дата':<12} | {'Категория':<15} | {'Значение':<10} | {'Создано'}")
        print("-" * 80)
        
        for record in records:
            record_id, date, category, value, created_at = record
            category_str = category if category else "(без категории)"
            created_short = created_at[:16] if created_at else ""
            print(f"{record_id:<6} | {date:<12} | {category_str:<15} | {value:<10} | {created_short}")
        
        conn.close()
        return records
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при получении записей: {e}")
        return []

def delete_record_by_id(record_id):
    """Удалить запись по ID"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Сначала получаем информацию о записи
        cursor.execute("""
            SELECT iv.id, iv.date, iv.category, iv.value, i.name
            FROM indicator_values iv
            JOIN indicators i ON iv.indicator_id = i.id
            WHERE iv.id = ?
        """, (record_id,))
        
        record = cursor.fetchone()
        
        if not record:
            print(f"❌ Запись с ID {record_id} не найдена")
            conn.close()
            return False
        
        rec_id, date, category, value, indicator_name = record
        category_str = category if category else "(без категории)"
        
        print(f"🗑️  УДАЛЕНИЕ ЗАПИСИ:")
        print(f"    ID: {rec_id}")
        print(f"    Индикатор: {indicator_name}")
        print(f"    Дата: {date}")
        print(f"    Категория: {category_str}")
        print(f"    Значение: {value}")
        
        # Подтверждение
        confirm = input("\n❓ Подтвердите удаление (y/N): ").strip().lower()
        if confirm not in ['y', 'yes', 'д', 'да']:
            print("✋ Удаление отменено")
            conn.close()
            return False
        
        # Удаляем запись
        cursor.execute("DELETE FROM indicator_values WHERE id = ?", (record_id,))
        
        if cursor.rowcount == 0:
            print("❌ Запись не была удалена")
            conn.close()
            return False
        
        conn.commit()
        conn.close()
        
        print(f"🎉 Запись {record_id} успешно удалена!")
        return True
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при удалении записи: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def delete_latest_records(indicator_id, count=1):
    """Удалить последние N записей для индикатора"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Получаем последние записи
        cursor.execute("""
            SELECT id, date, category, value
            FROM indicator_values
            WHERE indicator_id = ?
            ORDER BY date DESC, id DESC
            LIMIT ?
        """, (indicator_id, count))
        
        records = cursor.fetchall()
        
        if not records:
            print(f"❌ Нет записей для удаления")
            conn.close()
            return False
        
        print(f"🗑️  УДАЛЕНИЕ ПОСЛЕДНИХ {len(records)} ЗАПИСЕЙ:")
        for record in records:
            rec_id, date, category, value = record
            category_str = category if category else "(без категории)"
            print(f"    ID: {rec_id} | {date} | {category_str} | {value}")
        
        # Подтверждение
        confirm = input(f"\n❓ Подтвердите удаление {len(records)} записей (y/N): ").strip().lower()
        if confirm not in ['y', 'yes', 'д', 'да']:
            print("✋ Удаление отменено")
            conn.close()
            return False
        
        # Удаляем записи
        record_ids = [record[0] for record in records]
        placeholders = ','.join('?' for _ in record_ids)
        cursor.execute(f"DELETE FROM indicator_values WHERE id IN ({placeholders})", record_ids)
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"🎉 Удалено {deleted_count} записей!")
        return True
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при удалении записей: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def main():
    print("🗑️  СКРИПТ УДАЛЕНИЯ ОТДЕЛЬНЫХ ЗАПИСЕЙ")
    print("="*50)
    
    if not DB_PATH.exists():
        print(f"❌ База данных не найдена: {DB_PATH}")
        return
    
    while True:
        print("\n📋 МЕНЮ:")
        print("1. Показать все индикаторы")
        print("2. Показать последние записи индикатора")
        print("3. Удалить запись по ID")
        print("4. Удалить последние N записей индикатора")
        print("5. Выход")
        
        choice = input("\nВыберите действие (1-5): ").strip()
        
        if choice == '1':
            show_indicators()
            
        elif choice == '2':
            try:
                indicator_id = int(input("Введите ID индикатора: "))
                limit = input("Количество записей для показа (по умолчанию 20): ").strip()
                limit = int(limit) if limit else 20
                show_recent_records(indicator_id, limit)
            except ValueError:
                print("❌ Некорректный ID или число")
                
        elif choice == '3':
            try:
                record_id = int(input("Введите ID записи для удаления: "))
                delete_record_by_id(record_id)
            except ValueError:
                print("❌ Некорректный ID записи")
                
        elif choice == '4':
            try:
                indicator_id = int(input("Введите ID индикатора: "))
                count = input("Количество последних записей для удаления (по умолчанию 1): ").strip()
                count = int(count) if count else 1
                delete_latest_records(indicator_id, count)
            except ValueError:
                print("❌ Некорректный ID или число")
                
        elif choice == '5':
            print("👋 Выход")
            break
            
        else:
            print("❌ Некорректный выбор")

if __name__ == "__main__":
    main()