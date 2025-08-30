# test_dao.py
# Комплексный тест для проверки всех основных методов IndicatorDAO.

from dao_No_Debug import IndicatorDAO

def run_dao_test():
    """
    Выполняет полный цикл тестирования DAO:
    1. Очищает таблицы.
    2. Записывает данные во все таблицы.
    3. Читает записанные данные для проверки.
    """
    print("--- ЗАПУСК КОМПЛЕКСНОГО ТЕСТА DAO ---")
    dao = IndicatorDAO()
    if not dao.conn:
        print("Тест прерван: не удалось подключиться к БД.")
        return

    # --- ЭТАП 1: ОЧИСТКА ---
    dao._dangerously_clear_all_tables()

    # --- ЭТАП 2: ЗАПИСЬ ДАННЫХ (WRITE) ---
    print("\n--- ТЕСТИРОВАНИЕ ЗАПИСИ ДАННЫХ ---")
    
    # 2.1. Индикатор
    test_indicator_id = dao.add_indicator(
        name='test_ism_services', full_name='Test ISM Services PMI',
        source='Test Source Inc.', description='A comprehensive test indicator.'
    )
    print(f"Создан тестовый индикатор с ID: {test_indicator_id}")

    # 2.2. Числовые значения
    test_date_1 = '2025-07-25'
    test_date_2 = '2025-08-23'
    dao.add_indicator_value(indicator_id=test_indicator_id, date=test_date_1, value=52.5)
    dao.add_indicator_value(indicator_id=test_indicator_id, date=test_date_2, value=53.8)

    # 2.3. JSON-релиз
    complex_release_data = {"headline_index": {"name": "Services PMI", "value": 53.8}}
    dao.add_indicator_release(
        indicator_id=test_indicator_id, date=test_date_2,
        release_data=complex_release_data, source_url='http://test.com/report123'
    )
    
    # 2.4. Добавляем комментарии
    dao.add_comment(
        indicator_id=test_indicator_id, date=test_date_1,
        comment_text="Рынок отреагировал позитивно на эти данные."
    )
    dao.add_comment(
        indicator_id=test_indicator_id, date=test_date_2,
        comment_text="Наметилась тенденция к росту."
    )
    print("Тестовые данные успешно записаны во все таблицы.")

    # --- ЭТАП 3: ЧТЕНИЕ ДАННЫХ (READ) ---
    print("\n--- ТЕСТИРОВАНИЕ ЧТЕНИЯ ДАННЫХ ---")

    # 3.1. Читаем числовые значения
    print("\nЧтение indicator_values:")
    values_df = dao.get_indicator_values(test_indicator_id)
    print(values_df)

    # 3.2. Читаем JSON-релиз
    print("\nЧтение indicator_releases:")
    release_dict, url = dao.get_indicator_release(test_indicator_id, test_date_2)
    if release_dict:
        print("JSON-релиз успешно прочитан.")
        print(f"  - Заголовок: {release_dict.get('headline_index', {}).get('name')}")
    else:
        print("Ошибка: не удалось прочитать JSON-релиз.")

    # 3.3. Читаем комментарии
    print("\nЧтение comments:")
    comments_df = dao.get_comments(test_indicator_id)
    print(comments_df)

    # --- ЗАВЕРШЕНИЕ ---
    dao.close()
    print("\n--- ТЕСТ ЗАВЕРШЕН ---")


if __name__ == '__main__':
    run_dao_test()