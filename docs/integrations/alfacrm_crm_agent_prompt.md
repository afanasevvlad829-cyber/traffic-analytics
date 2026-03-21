# Prompt для ветки Traffic Analytics (AlfaCRM ingest)

Ты работаешь в репозитории `traffic-analytics` (production utility, PostgreSQL + ETL + AI layer).

## Цель

Интегрировать данные из `alfacrm-exporter` (xlsx) в текущий ETL/DB контур так, чтобы:
- данные о пользователях (customers/leads/clients) и коммуникациях попадали в PostgreSQL;
- загрузка была повторяемой и безопасной;
- существующие ETL процессы Direct/Metrica/Webmaster не ломались;
- можно было запускать CRM ingestion как отдельно, так и в составе `run_etl.py`.

## Уже реализовано в ветке

- `sql/045_alfacrm_crm_ingest.sql`
- `src/load_alfacrm_crm_xlsx.py`
- обновление `run_etl.py` через env `ALFACRM_XLSX_FILE`
- `openpyxl` в `requirements.txt`

## Технические требования

1. Не удалять существующие таблицы и ETL-потоки.
2. Работать через staging-слой `stg_alfacrm_*`.
3. Использовать upsert-логику (idempotent loads).
4. Любой шаг должен быть откатываемым.
5. Не добавлять новые сервисы/фреймворки.

## Проверка готовности (обязательно)

1. Применить SQL `045_alfacrm_crm_ingest.sql`.
2. Запустить:
   - `python3 src/load_alfacrm_crm_xlsx.py --xlsx <path> --report-date <date>`
3. Проверить row counts в:
   - `stg_alfacrm_customers_daily`
   - `stg_alfacrm_communications_daily`
   - `etl_alfacrm_file_loads`
4. Убедиться, что `run_etl.py` без `ALFACRM_XLSX_FILE` работает как раньше.
5. Убедиться, что `run_etl.py` с `ALFACRM_XLSX_FILE` включает CRM шаг.

## Что сделать следующим шагом

После базовой загрузки построить отдельный mart/view слой для аналитики связи CRM-сигналов с текущим SERM/Direct AI контуром, не меняя существующие боевые таблицы.
