create table if not exists mart_channel_daily(
date date,
channel_group text,
sessions bigint,
users bigint,
cost numeric
);
