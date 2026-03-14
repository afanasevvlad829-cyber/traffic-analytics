create table if not exists mart_leads_daily (
    date date not null,
    channel_group text not null,
    leads bigint not null default 0,
    revenue numeric(14,2),
    loaded_at timestamp default now(),
    primary key (date, channel_group)
);
