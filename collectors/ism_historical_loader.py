#!/usr/bin/env python3
# ism_manufacturing_historical_loader.py

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Добавляем корневую директорию проекта в sys.path (из collectors/)
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from dao import IndicatorDAO

def load_ism_manufacturing_historical_data(csv_file_path):
    """
    Загрузчик исторических данных ISM Manufacturing PMI из CSV файла
    """
    
    dao = IndicatorDAO()
    
    try:
        # 1. Регистрируем индикатор
        indicator_id = dao.add_indicator(
            name="us_ism_manufacturing_pmi",
            full_name="ISM Manufacturing Purchasing Managers Index",
            source="ISM",
            description="ISM Manufacturing PMI and sub-indices including New Orders, Production, Employment, etc."
        )
        
        if not indicator_id:
            print("❌ Ошибка при регистрации индикатора")
            return False
            
        print(f"✅ Индикатор зарегистрирован с ID: {indicator_id}")
        
        # 2. Читаем CSV файл
        print(f"📂 Загружаем данные из: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        # 3. Обрабатываем даты
        df['Month'] = pd.to_datetime(df['Month'], format='%m/%d/%y')
        df['date'] = df['Month'].dt.strftime('%Y-%m-%d')
        
        print(f"📊 Загружено {len(df)} записей с {df['Month'].min().strftime('%Y-%m-%d')} по {df['Month'].max().strftime('%Y-%m-%d')}")
        
        # 4. Определяем маппинг колонок к категориям
        column_mapping = {
            'PMI': 'headline',
            'New Orders': 'new_orders', 
            'Production': 'production',
            'Employment': 'employment',
            'Deliveries': 'supplier_deliveries',
            'Inventories': 'inventories',
            'Custom Inv': 'customers_inventories',
            'Prices': 'prices_paid',
            'Ord Backlog': 'order_backlog',
            'Exports': 'exports',
            'Imports': 'imports'
        }
        
        # 5. Загружаем данные в БД
        total_records = 0
        
        for _, row in df.iterrows():
            date_str = row['date']
            
            for csv_column, category in column_mapping.items():
                if csv_column in df.columns and pd.notna(row[csv_column]):
                    value = float(row[csv_column])
                    
                    dao.add_indicator_value(
                        indicator_id=indicator_id,
                        date=date_str,
                        value=value,
                        category=category
                    )
                    total_records += 1
        
        print(f"✅ Успешно загружено {total_records} записей в БД")
        print(f"📈 Категории данных: {list(column_mapping.values())}")
        
        # 6. Выводим сводную статистику
        print("\n📊 СВОДКА ПО КАТЕГОРИЯМ:")
        for csv_col, category in column_mapping.items():
            if csv_col in df.columns:
                valid_count = df[csv_col].notna().sum()
                if valid_count > 0:
                    avg_val = df[csv_col].mean()
                    min_val = df[csv_col].min() 
                    max_val = df[csv_col].max()
                    print(f"  {category:20} | {valid_count:3d} записей | Среднее: {avg_val:5.1f} | Диапазон: {min_val:5.1f}-{max_val:5.1f}")
        
        return True
        
    except FileNotFoundError:
        print(f"❌ Файл не найден: {csv_file_path}")
        return False
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        return False
    finally:
        dao.close()

def main():
    # Определяем путь к CSV файлу относительно расположения скрипта
    script_dir = Path(__file__).parent
    csv_path = script_dir.parent / "data" / "ism_manufacturing_historical.csv"
    
    print("🏭 ISM Manufacturing PMI - Загрузчик исторических данных")
    print("=" * 60)
    print(f"🔍 Ищем файл: {csv_path}")
    print(f"📂 Текущая директория: {Path.cwd()}")
    
    if not csv_path.exists():
        print(f"❌ Файл {csv_path} не найден!")
        print(f"   Проверьте, что файл существует по пути: {csv_path}")
        # Покажем содержимое data/ директории для диагностики
        data_dir = script_dir.parent / "data"
        if data_dir.exists():
            print(f"   Содержимое {data_dir}:")
            for file in data_dir.iterdir():
                print(f"     - {file.name}")
        else:
            print(f"   Директория {data_dir} не существует!")
        return
    
    success = load_ism_manufacturing_historical_data(str(csv_path))
    
    if success:
        print("\n🎉 Загрузка исторических данных завершена успешно!")
        print("   Теперь можно приступать к парсеру текущих данных с сайта ISM")
    else:
        print("\n❌ Загрузка завершена с ошибками")

if __name__ == "__main__":
    main()