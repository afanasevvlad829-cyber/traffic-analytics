# AlfaCRM -> Traffic Analytics (CRM ingest handoff)

## Цель

Подключить выгрузку из `alfacrm-exporter` (xlsx) в текущее PostgreSQL-хранилище `traffic-analytics` без ломки существующего ETL.

## Что уже добавлено в ветке

- SQL схема для CRM staging: `sql/045_alfacrm_crm_ingest.sql`
- Скрипт загрузки xlsx: `src/load_alfacrm_crm_xlsx.py`
- Опциональный вызов из ETL: `run_etl.py` (через env `ALFACRM_XLSX_FILE`)
- Зависимость для чтения xlsx: `openpyxl==3.1.5` в `requirements.txt`

## Структура staging-таблиц

### `stg_alfacrm_customers_daily`

Содержит строки из листов:
- `customers_all`
- `leads_active`
- `leads_archived`
- `clients_active`
- `clients_archived`

Ключ: `(report_date, segment, customer_id)`

### `stg_alfacrm_communications_daily`

Содержит строки из листа `communications`.

Ключ: `(report_date, row_key)`
- если есть `id` в строке, то `row_key = id:<id>`
- иначе `row_key = fp:<md5(json_row)>`

### `etl_alfacrm_file_loads`

Лог загрузок файлов (дедуп по `file_hash`).

## Запуск загрузки вручную

```bash
cd /home/kv145/traffic-analytics
source .venv/bin/activate
pip install -r requirements.txt

python3 src/load_alfacrm_crm_xlsx.py \
  --xlsx /home/kv145/traffic-analytics/exports/full_with_comm.xlsx \
  --report-date 2026-03-21
```

Без communications:

```bash
python3 src/load_alfacrm_crm_xlsx.py \
  --xlsx /home/kv145/traffic-analytics/exports/smoke_no_comm.xlsx \
  --report-date 2026-03-21 \
  --skip-communications
```

## Запуск через общий ETL

Если нужно подтянуть CRM в том же прогоне `run_etl.py`, задайте env:

```bash
export ALFACRM_XLSX_FILE=/home/kv145/traffic-analytics/exports/full_with_comm.xlsx
python3 run_etl.py
```

Если `ALFACRM_XLSX_FILE` пустой, CRM-loader не запускается.

## Быстрая проверка после загрузки

```sql
select report_date, segment, count(*)
from stg_alfacrm_customers_daily
group by 1,2
order by 1 desc, 2;

select report_date, count(*)
from stg_alfacrm_communications_daily
group by 1
order by 1 desc;

select *
from etl_alfacrm_file_loads
order by loaded_at desc
limit 10;
```

## Дальше для SERM/Direct AI

1. Использовать `vw_alfacrm_customers_latest` и `vw_alfacrm_communications_latest` как стабильный source layer.
2. Добавить mart-слой сопоставления CRM-сигналов с текущими AI/Direct таблицами (отдельным SQL шагом), не трогая существующий контур `mart_competitor_serp_alerts`.
3. Сначала внедрить read-only витрины, затем уже action logic.

## Ограничения

- Скрипт ориентирован на xlsx-формат из `alfacrm_export_v5.py`.
- Если в xlsx отсутствуют нужные листы, они просто пропускаются.
- Данные из xlsx считаются источником истины на дату `report_date`.
