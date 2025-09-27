# Workflow: Building Permits

### 1. Импорт истории (восстановление базы)

```bash
python history_importers/import_building_permits_history.py --csv ./data/bps_history.csv
```

* Полностью удаляет старые значения.
* Загружает историю из CSV (4 категории: `total`, `1 unit`, `2-4 units`, `5+ units`).
* CSV лежит в папке /data. Если понадобится загрузка свежей истории сохранить формат файла. Но проще загрузить, что есть, а острально стащить с FRED

---

### 2. Проверка после импорта

```bash
python tests/test_building_permits.py
```

* Должны быть все 4 категории.
* `Empty-category rows: 0`.

---

### 3. Обновление свежих данных

```bash
python collectors/building_permits_collector.py
```

* Подтягивает новые значения из FRED.
* Повторный запуск не пишет дубли.

