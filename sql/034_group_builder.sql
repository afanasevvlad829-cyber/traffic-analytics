drop table if exists mart_group_builder cascade;

create table mart_group_builder (

created_at timestamp default now(),

campaign_name text,
ad_group_id bigint,

queries text,

cluster_name text,
cluster_queries text,

recommendation text
);

create index idx_group_builder_group
on mart_group_builder(ad_group_id);
