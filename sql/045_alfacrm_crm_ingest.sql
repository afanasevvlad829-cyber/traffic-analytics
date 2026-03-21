create table if not exists stg_alfacrm_customers_daily (
    report_date date not null,
    segment text not null,
    customer_id bigint not null,
    customer_name text,
    phone_normalized text,
    email_normalized text,
    telegram_username text,
    is_study smallint,
    removed smallint,
    source_file text,
    payload_json jsonb,
    loaded_at timestamp default now(),
    primary key (report_date, segment, customer_id)
);

create index if not exists idx_stg_alfacrm_customers_daily_report_date
    on stg_alfacrm_customers_daily (report_date desc);

create index if not exists idx_stg_alfacrm_customers_daily_customer_id
    on stg_alfacrm_customers_daily (customer_id);

create index if not exists idx_stg_alfacrm_customers_daily_telegram
    on stg_alfacrm_customers_daily (telegram_username);

create table if not exists stg_alfacrm_communications_daily (
    report_date date not null,
    row_key text not null,
    communication_id bigint,
    customer_id bigint,
    communication_type text,
    created_at text,
    source_file text,
    payload_json jsonb,
    loaded_at timestamp default now(),
    primary key (report_date, row_key)
);

create index if not exists idx_stg_alfacrm_communications_daily_report_date
    on stg_alfacrm_communications_daily (report_date desc);

create index if not exists idx_stg_alfacrm_communications_daily_customer_id
    on stg_alfacrm_communications_daily (customer_id);

create table if not exists etl_alfacrm_file_loads (
    load_id bigserial primary key,
    loaded_at timestamp default now(),
    report_date date not null,
    source_file text not null,
    file_hash text not null,
    customers_rows integer not null default 0,
    communications_rows integer not null default 0,
    note text
);

create unique index if not exists uq_etl_alfacrm_file_loads_file_hash
    on etl_alfacrm_file_loads (file_hash);

create or replace view vw_alfacrm_customers_latest as
select distinct on (segment, customer_id)
    report_date,
    segment,
    customer_id,
    customer_name,
    phone_normalized,
    email_normalized,
    telegram_username,
    is_study,
    removed,
    source_file,
    loaded_at
from stg_alfacrm_customers_daily
order by segment, customer_id, report_date desc, loaded_at desc;

create or replace view vw_alfacrm_communications_latest as
select distinct on (row_key)
    report_date,
    row_key,
    communication_id,
    customer_id,
    communication_type,
    created_at,
    source_file,
    loaded_at,
    payload_json
from stg_alfacrm_communications_daily
order by row_key, report_date desc, loaded_at desc;
