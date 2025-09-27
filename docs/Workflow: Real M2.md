# ⚙️ Workflow: Real M2 Collector

1. **Запуск скрипта**
   ```
   python collectors/real_m2_collector.py
   ```

2. **Регистрация индикатора**
   * DAO добавляет запись в таблицу `indicators` с параметрами:
     * `name = real_m2_usd`
     * `full_name = Real M2 Money Stock (in Billions of 1982-84 Dollars)`
     * `source = FRED`
     * `description = Calculated as M2SL / CPIAUCSL * 100`

3. **Определение стартовой даты**
   * DAO проверяет таблицу `indicator_values`
   * Если записи есть → берётся `MAX(date)`
   * Если пусто → грузится вся история

4. **Загрузка данных** (через `fred_parser.get_fred_calculated_series`)
   * Серии:
     * `M2SL` (денежная масса M2)
     * `CPIAUCSL` (индекс потребительских цен)
   * Период: начиная со `start_date`

5. **Расчёт**
   ```
   Real M2 = (M2SL / CPIAUCSL) * 100
   ```

6. **Сохранение**
   * DAO пишет в `indicator_values` строки:
     * `date`
     * `value` (real M2)
     * `category = ''` (по умолчанию пусто)
   * `INSERT OR IGNORE` защищает от дублей

7. **Выход**
   * Сообщение «Обработка завершена»
   * Закрытие соединения с БД
