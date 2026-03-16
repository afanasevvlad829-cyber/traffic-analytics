drop table if exists mart_structure_audit cascade;

create table mart_structure_audit (
created_at timestamp default now(),

campaign_name text,
ad_group_id bigint,

queries text,
keywords text,

avg_ctr numeric,
avg_cpc numeric,

issue text,
recommendation text
);

create index idx_structure_audit_group
on mart_structure_audit(ad_group_id);
