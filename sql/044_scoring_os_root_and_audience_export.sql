alter table if exists stg_metrica_visitors_features
    add column if not exists os_root text;

alter table if exists mart_visitor_scoring
    add column if not exists os_root text;

create index if not exists idx_stg_metrica_visitors_features_os_root
    on stg_metrica_visitors_features(os_root);

create index if not exists idx_mart_visitor_scoring_os_root
    on mart_visitor_scoring(os_root);
