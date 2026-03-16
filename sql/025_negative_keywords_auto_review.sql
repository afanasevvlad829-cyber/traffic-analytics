drop view if exists vw_campaign_negative_keywords_auto_copy_paste;
drop view if exists vw_campaign_negative_keywords_review_copy_paste;
drop view if exists vw_campaign_negative_keywords_auto;
drop view if exists vw_campaign_negative_keywords_review;
drop view if exists vw_campaign_negative_keywords;
drop materialized view if exists mv_campaign_negative_keywords_source;

create materialized view mv_campaign_negative_keywords_source as
with base as (
    select
        campaign_name,
        search_query,
        coalesce(minus_words, '') as minus_words
    from mart_direct_ai_creatives
    where decision = 'EXCLUDE'
),
split_words as (
    select
        campaign_name,
        trim(lower(regexp_replace(word, '[^a-zа-я0-9 -]+', '', 'g'))) as word
    from base,
    regexp_split_to_table(minus_words, '\s*,\s*') as word
),
query_words as (
    select
        campaign_name,
        trim(lower(regexp_replace(word, '[^a-zа-я0-9 -]+', '', 'g'))) as word
    from (
        select
            campaign_name,
            regexp_split_to_table(search_query, '\s+') as word
        from mart_direct_action_queue_v3
        where decision = 'EXCLUDE'
    ) q
),
unioned as (
    select campaign_name, word from split_words
    union
    select campaign_name, word from query_words
)
select distinct
    campaign_name,
    word as negative_keyword
from unioned
where word is not null
  and word <> ''
  and length(word) >= 3
  and word not in (
    'для','под','это','что','как','или','без','лето','детей',
    'лагерь','детский','летний','подмосковье'
  );

create view vw_campaign_negative_keywords as
select
    campaign_name,
    negative_keyword
from mv_campaign_negative_keywords_source
order by campaign_name, negative_keyword;

create view vw_campaign_negative_keywords_auto as
select
    campaign_name,
    negative_keyword,
    'AUTO_NEGATIVE'::text as negative_type
from vw_campaign_negative_keywords
where negative_keyword similar to '%(страна|оз|дубки|маяк|полушкино|спутник|заброшенн|инженерной|медицинский|энергетик|practice|incamp|derel)%';

create view vw_campaign_negative_keywords_review as
select
    campaign_name,
    negative_keyword,
    'REVIEW_NEGATIVE'::text as negative_type
from vw_campaign_negative_keywords
where (campaign_name, negative_keyword) not in (
    select campaign_name, negative_keyword
    from vw_campaign_negative_keywords_auto
);

create view vw_campaign_negative_keywords_auto_copy_paste as
select
    campaign_name,
    string_agg(negative_keyword, ', ' order by negative_keyword) as auto_negative_keywords_copy_paste,
    count(*) as keywords_count
from vw_campaign_negative_keywords_auto
group by campaign_name
order by campaign_name;

create view vw_campaign_negative_keywords_review_copy_paste as
select
    campaign_name,
    string_agg(negative_keyword, ', ' order by negative_keyword) as review_negative_keywords_copy_paste,
    count(*) as keywords_count
from vw_campaign_negative_keywords_review
group by campaign_name
order by campaign_name;
