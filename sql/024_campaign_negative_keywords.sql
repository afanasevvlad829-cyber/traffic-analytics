drop view if exists vw_campaign_negative_keywords;
drop view if exists vw_campaign_negative_keywords_copy_paste;

create view vw_campaign_negative_keywords as
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
),
filtered as (
    select distinct
        campaign_name,
        word
    from unioned
    where word is not null
      and word <> ''
      and length(word) >= 3
      and word not in (
        'для','под','это','что','как','или','без','лето','детей',
        'лагерь','детский','летний','подмосковье'
      )
)
select
    campaign_name,
    word as negative_keyword
from filtered
order by campaign_name, negative_keyword;

create view vw_campaign_negative_keywords_copy_paste as
select
    campaign_name,
    string_agg(negative_keyword, ', ' order by negative_keyword) as negative_keywords_copy_paste,
    count(*) as keywords_count
from vw_campaign_negative_keywords
group by campaign_name
order by campaign_name;
