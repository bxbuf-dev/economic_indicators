# delete_indicator.py - Скрипт для удаления конкретного индикатора из БД
import sqlite3
from pathlib import Path

# --- Путь к БД ---
home_dir = Path.home()
DB_PATH = home_dir / 'Documents' / 'economic_indicators.db'

def list_all_indicators():
    """Показать все индикаторы в БД"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, name, full_name, source, 
                   (SELECT COUNT(*) FROM indicator_values WHERE indicator_id = i.id) as values_count,
                   created_at
            FROM indicators i
            ORDER BY id
        """)
        
        indicators = cursor.fetchall()
        
        if not indicators:
            print("❌ В БД нет индикаторов")
            return []
        
        print("📊 ИНДИКАТОРЫ В БАЗЕ ДАННЫХ:")
        print("="*80)
        
        for row in indicators:
            id_val, name, full_name, source, values_count, created_at = row
            print(f"ID: {id_val}")
            print(f"Название: {name}")
            print(f"Полное название: {full_name}")
            print(f"Источник: {source}")
            print(f"Количество значений: {values_count}")
            print(f"Создан: {created_at}")
            print("-" * 40)
        
        conn.close()
        return indicators
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при получении списка индикаторов: {e}")
        return []

def delete_indicator_by_id(indicator_id):
    """Удалить индикатор по ID"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Сначала проверяем, существует ли индикатор
        cursor.execute("SELECT name, full_name FROM indicators WHERE id = ?", (indicator_id,))
        indicator = cursor.fetchone()
        
        if not indicator:
            print(f"❌ Индикатор с ID {indicator_id} не найден")
            conn.close()
            return False
        
        name, full_name = indicator
        print(f"🗑️  Удаление индикатора:")
        print(f"   ID: {indicator_id}")
        print(f"   Название: {name}")
        print(f"   Полное название: {full_name}")
        
        # Подсчитываем связанные записи
        cursor.execute("SELECT COUNT(*) FROM indicator_values WHERE indicator_id = ?", (indicator_id,))
        values_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM indicator_releases WHERE indicator_id = ?", (indicator_id,))
        releases_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM comments WHERE indicator_id = ?", (indicator_id,))
        comments_count = cursor.fetchone()[0]
        
        print(f"   📈 Значений для удаления: {values_count}")
        print(f"   📄 Релизов для удаления: {releases_count}")
        print(f"   💬 Комментариев для удаления: {comments_count}")
        
        # Подтверждение
        confirm = input("\n❓ Подтвердите удаление (y/N): ").strip().lower()
        if confirm not in ['y', 'yes', 'д', 'да']:
            print("✋ Удаление отменено")
            conn.close()
            return False
        
        # Удаляем в правильном порядке (сначала зависимые таблицы)
        print("\n🗑️  Удаление данных...")
        
        cursor.execute("DELETE FROM comments WHERE indicator_id = ?", (indicator_id,))
        print(f"   ✅ Удалено комментариев: {cursor.rowcount}")
        
        cursor.execute("DELETE FROM indicator_releases WHERE indicator_id = ?", (indicator_id,))
        print(f"   ✅ Удалено релизов: {cursor.rowcount}")
        
        cursor.execute("DELETE FROM indicator_values WHERE indicator_id = ?", (indicator_id,))
        print(f"   ✅ Удалено значений: {cursor.rowcount}")
        
        cursor.execute("DELETE FROM indicators WHERE id = ?", (indicator_id,))
        print(f"   ✅ Удален индикатор: {cursor.rowcount}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 Индикатор {indicator_id} ({name}) успешно удален!")
        return True
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при удалении индикатора: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def delete_indicator_by_name(indicator_name):
    """Удалить индикатор по названию"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM indicators WHERE name = ?", (indicator_name,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return delete_indicator_by_id(result[0])
        else:
            print(f"❌ Индикатор с названием '{indicator_name}' не найден")
            return False
            
    except sqlite3.Error as e:
        print(f"❌ Ошибка при поиске индикатора: {e}")
        return False

def main():
    print("🗑️  СКРИПТ УДАЛЕНИЯ ИНДИКАТОРОВ")
    print("="*50)
    
    if not DB_PATH.exists():
        print(f"❌ База данных не найдена: {DB_PATH}")
        return
    
    while True:
        print("\n📋 МЕНЮ:")
        print("1. Показать все индикаторы")
        print("2. Удалить по ID")
        print("3. Удалить по названию")
        print("4. Выход")
        
        choice = input("\nВыберите действие (1-4): ").strip()
        
        if choice == '1':
            list_all_indicators()
            
        elif choice == '2':
            try:
                indicator_id = int(input("Введите ID индикатора для удаления: "))
                delete_indicator_by_id(indicator_id)
            except ValueError:
                print("❌ Некорректный ID")
                
        elif choice == '3':
            indicator_name = input("Введите название индикатора для удаления: ").strip()
            if indicator_name:
                delete_indicator_by_name(indicator_name)
            else:
                print("❌ Название не может быть пустым")
                
        elif choice == '4':
            print("👋 Выход")
            break
            
        else:
            print("❌ Некорректный выбор")

if __name__ == "__main__":
    main()