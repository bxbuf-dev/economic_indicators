# collectors/michigan_historical_loader.py
import os
import sys
import pandas as pd
from pathlib import Path

# --- ДОБАВЛЕНИЕ КОРНЕВОЙ ПАПКИ В ПУТЬ ПОИСКА МОДУЛЕЙ ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from dao import IndicatorDAO

# --- КОНФИГУРАЦИЯ ---
INDICATOR_CONFIG = {
    'name': 'umcsi_michigan_full',
    'full_name': 'University of Michigan Consumer Sentiment Survey (Full Data)',
    'source': 'University of Michigan Survey Research Center',
    'description': 'Complete UMCSI data: composite, current conditions, expectations (preliminary & final)'
}

# Путь к CSV файлу (относительно корня проекта)
CSV_FILE_PATH = 'data/michigan_historical.csv'  # Настройте под ваш путь

def load_historical_data(csv_path):
    """
    Загружает исторические данные Michigan из CSV
    
    Ожидаемый формат CSV:
    DATE,COMPOSITE UMCSI,CURRENT,EXPECTATIONS
    01/01/14,81.2,96.8,71.2
    """
    try:
        # Проверяем существование файла
        if not Path(csv_path).exists():
            print(f"❌ Файл не найден: {csv_path}")
            print("   Укажите корректный путь к CSV файлу с историческими данными Michigan")
            return pd.DataFrame()
        
        # Загружаем CSV
        df = pd.read_csv(csv_path)
        print(f"📄 Загружен CSV файл: {len(df)} строк")
        
        # Проверяем ожидаемые колонки
        expected_columns = ['DATE', 'COMPOSITE UMCSI', 'CURRENT', 'EXPECTATIONS']
        if not all(col in df.columns for col in expected_columns):
            print(f"❌ Неожиданные колонки в CSV: {list(df.columns)}")
            print(f"   Ожидаемые: {expected_columns}")
            return pd.DataFrame()
        
        # Парсим даты MM/DD/YY -> YYYY-MM-DD
        df['DATE'] = pd.to_datetime(df['DATE'], format='%m/%d/%y')
        
        # Переименовываем колонки для удобства
        df.rename(columns={
            'DATE': 'date',
            'COMPOSITE UMCSI': 'composite',
            'CURRENT': 'current', 
            'EXPECTATIONS': 'expectations'
        }, inplace=True)
        
        # Преобразуем в long format для удобной загрузки в БД
        long_data = []
        
        for _, row in df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            
            # Добавляем каждую категорию как отдельную запись
            for category in ['composite', 'current', 'expectations']:
                if pd.notna(row[category]):  # Пропускаем NaN значения
                    long_data.append({
                        'date': date_str,
                        'category': category,  # Без суффикса _p для исторических данных
                        'value': float(row[category])
                    })
        
        result_df = pd.DataFrame(long_data)
        print(f"✅ Обработано {len(result_df)} записей в long format")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке CSV: {e}")
        return pd.DataFrame()

def main():
    """
    Одноразовый загрузчик исторических данных Michigan UMCSI
    """
    dao = None
    try:
        print("--- ЗАГРУЗЧИК ИСТОРИЧЕСКИХ ДАННЫХ MICHIGAN UMCSI ---")
        
        # Путь к CSV файлу
        project_root = Path(__file__).parent.parent
        csv_path = project_root / CSV_FILE_PATH
        
        print(f"🔍 Поиск CSV файла: {csv_path}")
        
        # Если файл не найден, просим указать путь
        if not csv_path.exists():
            print("\n📝 Файл не найден. Введите путь к CSV файлу с данными Michigan:")
            user_path = input("Путь к файлу: ").strip()
            if user_path:
                csv_path = Path(user_path)
            else:
                print("❌ Путь не указан. Выход.")
                return
        
        # Загружаем данные
        historical_df = load_historical_data(csv_path)
        
        if historical_df.empty:
            print("❌ Нет данных для загрузки")
            return
        
        # Инициализируем DAO и добавляем индикатор
        dao = IndicatorDAO()
        indicator_id = dao.add_indicator(**INDICATOR_CONFIG)
        
        if not indicator_id:
            print("❌ Не удалось создать индикатор")
            return
        
        print(f"✅ Индикатор создан с ID: {indicator_id}")
        
        # Проверяем, есть ли уже данные в БД
        existing_date = dao.get_latest_indicator_date(indicator_id)
        if existing_date:
            print(f"⚠️  В БД уже есть данные до {existing_date}")
            confirm = input("Продолжить загрузку? (y/N): ").strip().lower()
            if confirm not in ['y', 'yes', 'д', 'да']:
                print("❌ Загрузка отменена")
                return
        
        # Загружаем данные в БД
        print(f"💾 Загрузка {len(historical_df)} записей в БД...")
        
        success_count = 0
        for _, row in historical_df.iterrows():
            dao.add_indicator_value(
                indicator_id=indicator_id,
                date=row['date'],
                value=row['value'],
                category=row['category']
            )
            success_count += 1
            
            # Показываем прогресс каждые 100 записей
            if success_count % 100 == 0:
                print(f"   Загружено: {success_count}/{len(historical_df)}")
        
        print(f"🎉 Исторические данные успешно загружены!")
        print(f"   Всего записей: {success_count}")
        
        # Показываем статистику по периоду
        min_date = historical_df['date'].min()
        max_date = historical_df['date'].max()
        print(f"   Период: {min_date} - {max_date}")
        
        categories = historical_df['category'].unique()
        print(f"   Категории: {', '.join(categories)}")

    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")
    finally:
        if dao:
            dao.close()
            print("🔌 Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()