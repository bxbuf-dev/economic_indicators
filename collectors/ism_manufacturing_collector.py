#!/usr/bin/env python3
# collectors/ism_manufacturing_collector.py

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в sys.path (из collectors/)
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from dao import IndicatorDAO
from collectors.ism_manufacturing_parser import get_ism_manufacturing_data, check_if_data_exists_in_db

def collect_ism_manufacturing_pmi():
    """
    Коллектор данных ISM Manufacturing PMI
    Проверяет наличие данных в БД и загружает новые данные с сайта ISM
    """
    
    dao = IndicatorDAO()
    
    try:
        # 1. Получаем/регистрируем индикатор
        indicator_id = dao.add_indicator(
            name="us_ism_manufacturing_pmi",
            full_name="ISM Manufacturing Purchasing Managers Index",
            source="ISM", 
            description="ISM Manufacturing PMI and sub-indices including New Orders, Production, Employment, etc."
        )
        
        if not indicator_id:
            print("Ошибка при регистрации индикатора")
            return False
            
        print(f"Работаем с индикатором: us_ism_manufacturing_pmi (ID: {indicator_id})")
        
        # 2. Парсим данные с сайта ISM
        print("\nЗапускаем парсер ISM Manufacturing PMI...")
        ism_data = get_ism_manufacturing_data()
        
        if not ism_data:
            print("Не удалось получить данные с сайта ISM")
            return False
            
        expected_date = ism_data['date']
        values_data = ism_data['values']
        source_url = ism_data['source_url']
        
        print(f"Получены данные за: {expected_date}")
        print(f"Количество показателей: {len(values_data)}")
        
        # 3. Проверяем, есть ли уже данные в БД за этот месяц
        if check_if_data_exists_in_db(dao, indicator_id, expected_date):
            print("Данные за этот период уже существуют в БД")
            return True
        
        # 4. Записываем данные в БД
        print(f"\nЗаписываем данные в БД...")
        
        records_added = 0
        for category, value in values_data.items():
            try:
                dao.add_indicator_value(
                    indicator_id=indicator_id,
                    date=expected_date,
                    value=value,
                    category=category
                )
                print(f"  {category}: {value}")
                records_added += 1
            except Exception as e:
                print(f"Ошибка при записи {category}: {e}")
        
        print(f"\nУспешно добавлено {records_added} записей в таблицу indicator_values")
        
        # 5. Сохраняем метаданные о релизе (опционально)
        try:
            release_metadata = {
                "data_points": len(values_data),
                "categories": list(values_data.keys()),
                "parsing_timestamp": ism_data.get('parsing_timestamp', 'unknown')
            }
            
            dao.add_indicator_release(
                indicator_id=indicator_id,
                date=expected_date,
                release_data=release_metadata,
                source_url=source_url,
                category="metadata"
            )
            print("Метаданные релиза сохранены")
            
        except Exception as e:
            print(f"Предупреждение: не удалось сохранить метаданные релиза: {e}")
        
        print(f"\nКоллекция данных ISM Manufacturing PMI завершена успешно!")
        print(f"Дата: {expected_date}")
        print(f"Записей: {records_added}")
        print(f"URL: {source_url}")
        
        return True
        
    except Exception as e:
        print(f"Ошибка в коллекторе ISM Manufacturing PMI: {e}")
        return False
    finally:
        dao.close()

def main():
    print("ISM Manufacturing PMI Data Collector")
    print("=" * 50)
    
    success = collect_ism_manufacturing_pmi()
    
    if success:
        print("\nДанные успешно собраны и сохранены!")
        print("Используйте _db_inspector.py для проверки данных")
    else:
        print("\nОшибка при сборе данных")

if __name__ == "__main__":
    main()