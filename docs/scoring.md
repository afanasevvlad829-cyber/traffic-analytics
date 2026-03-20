# Scoring v1 (Lead / Visitor Scoring)

## Что это

`Scoring v1` — rule-based модуль для оценки вероятности покупки лагеря по visitor-level поведению.

Результат по каждому посетителю:

- `visitor_id`
- `raw_score`
- `normalized_score` (0..1)
- `segment` (`hot` / `warm` / `cold`)
- `explanation_json` (вклад факторов)
- `human_explanation`
- `short_reason`
- `recommended_action`

## Архитектура

Новый модуль:

- `src/scoring/feature_builder.py`
- `src/scoring/rules.py`
- `src/scoring/scorer.py`
- `src/scoring/service.py`
- `src/scoring/feature_sync.py`
- `src/run_build_scoring_features.py`
- `src/run_scoring_v1.py`

### Поток данных

1. Builder читает visitor features.
2. Rule engine считает explainable score.
3. Service сохраняет результат в `mart_visitor_scoring`.
4. Web API отдаёт summary, список и карточку visitor.
5. WebApp `/admin` показывает раздел `Scoring`.
6. Rebuild flow сначала синхронизирует visitor-level признаки из Метрики.

## SQL объекты

Миграция: `sql/040_scoring_v1.sql`

Создаёт:

1. `stg_metrica_visitors_features`
- staging-вход для visitor-level признаков из Метрики/ETL.
- одна строка = один visitor (MVP).

2. `mart_visitor_scoring`
- результат скоринга для UI/API.

## Источники признаков

`FeatureBuilder` использует приоритет:

1. `stg_metrica_visitors_features` (основной production путь)
2. fallback из `stg_metrica_source_daily` (если visitor staging пока пуст)

### Откуда берутся реальные visitor данные

Реальные признаки загружаются шагом `build_scoring_features` из Yandex Metrica Stat API:

- dimensions: `clientID`, `lastTrafficSource`, `lastAdvEngine`, `lastUTMSource`, `lastUTMMedium`, `deviceCategory`, `lastUTMCampaign`
- metrics: `visits`, `pageDepth`, `avgVisitDurationSeconds`, `bounceRate`

Затем выполняется upsert в `stg_metrica_visitors_features`.

Дополнительно делается попытка получить `clientID + startURL`, чтобы точнее вычислить:

- `visited_price_page`
- `visited_program_page`
- `visited_booking_page`
- `clicked_booking_button`

Если срез `startURL` недоступен, эти признаки заполняются безопасным fallback-эвристическим способом (по UTM/source/campaign), чтобы rebuild не падал.

### Важно про fallback

Fallback нужен, чтобы модуль работал даже до появления полной visitor-level выгрузки.

- создаётся стабильный псевдо-id вида `fallback_<hash>`
- hash строится из `traffic_source + source_engine + source_medium + campaign_name`

Это временный адаптерный режим для MVP и демонстрации end-to-end контура.

## Правила Scoring v1

Используются правила:

- `visited_price_page` = +3
- `visited_program_page` = +2
- `visited_booking_page` = +5
- `clicked_booking_button` = +7
- `sessions_count > 1` = +2
- `sessions_count > 2` = +1
- `total_time_sec > 120` = +2 (`total_time_gt_120`)
- `total_time_sec > 300` = +1 (`total_time_gt_300`)
- `pageviews >= 4` = +1
- `pageviews >= 8` = +1
- `scroll_70` = +1
- `return_visitor` = +2
- high-intent source (`direct/brand/search/cpc/...`) = +1
- `bounce_session` = -2

Нормализация:

- диапазон `raw_score` приводится к `0..1`
- версия правил: `v1_rules_2026_03`

Сегменты:

- `hot`: `normalized_score >= 0.70`
- `warm`: `0.40 <= normalized_score < 0.70`
- `cold`: `< 0.40`

## API

Добавлены endpoints:

1. `GET /api/scoring/summary`
- total scored visitors
- hot/warm/cold count
- avg score
- latest scored timestamp

2. `GET /api/scoring/visitors?limit=100&segment=hot&source=yandex`
- таблица скоринга
- фильтры: `limit`, `segment`, `source`
- ключевые поля в ответе:
  - `visitor_id`
  - `score` / `normalized_score`
  - `segment`
  - `short_reason`
  - `human_explanation`
  - `recommended_action`
  - `explanation_json`

3. `POST /api/scoring/rebuild`
- синхронный rebuild scoring
- body:
  - `limit` (optional)
  - `use_fallback` (default: true)
  - `send_report` (default: false)
  - `sync_features` (default: true) — перед rebuild синхронизировать real visitor features из Метрики
  - `features_days` (default: 30) — окно данных Метрики
  - `features_limit` (default: 50000) — лимит строк синка

4. `GET /api/scoring/visitor/{visitor_id}`
- полная карточка visitor:
  - feature values
  - `explanation_json`
  - `human_explanation`
  - `short_reason`
  - `recommended_action`
  - `data_source` / `source_mode`
  - `score_metadata`

5. `GET /api/scoring/timeseries?days=30`
- таймсерия сегментов для графиков (Chart.js):
  - `dates`: массив дат
  - `hot`: массив значений HOT
  - `warm`: массив значений WARM
  - `cold`: массив значений COLD

6. `GET /api/scoring/activation/plan?days=90&min_audience_size=100&export_limit=5000`
- строит activation plan по cohort-ам для Direct:
  - cohort name / segment / os / окно
  - размер экспортируемой аудитории
  - флаг `eligible` (достаточен ли размер)
  - рекомендованный креатив и `direct_tag`

7. `POST /api/scoring/activation/direct-sync`
- синхронизирует eligible cohort-ы в Direct AudienceTargets.
- body:
  - `days`
  - `min_audience_size`
  - `export_limit`
  - `dry_run` (default: `true`)
- важно:
  - фактическая запись в Direct выполняется только при `dry_run=false` и `SCORING_DIRECT_SYNC_ENABLED=1`.
  - нужен mapping через env `SCORING_DIRECT_RETARGET_MAP_JSON`.

8. `GET /api/scoring/activation/reaction?days=30&limit=50`
- статистика реакции в Direct по тегам `scoring_*` из `stg_direct_campaign_daily`:
  - impressions, clicks, ctr_pct, avg_cpc, cost.

9. `GET /api/scoring/ad-templates?days=90&min_audience_size=1&include_small=true&variants=3`
- шаблоны объявлений по cohort с explainable логикой:
  - `variants[]` (угол, headline, body, CTA, why_this),
  - `kpi_hypothesis`:
    - baseline (impressions/clicks/CTR/CPC/cost),
    - expected targets (CTR(STR), CR клик->заявка, CPC, CPL, CAC),
    - sample gate (минимум показов/кликов),
    - success_rule (критерий сравнения).

10. `POST /api/scoring/ad-templates/generate-banners`
- генерация баннеров через OpenAI Image API по выбранному cohort.
- body:
  - `cohort_name` (required),
  - `variant_key` (optional),
  - `days`, `min_audience_size`, `include_small`, `variants`,
  - `images_per_variant` (1..3),
  - `size` (например `1536x1024`),
  - `quality` (`low|medium|high`),
  - `output_format` (`png|jpeg|webp`).
- ответ:
  - `generated[]` со ссылками `/static/generated/scoring_banners/...`
  - `failed[]` по ошибкам генерации.

### Бизнес-параметры KPI (текущая версия)

- Средний чек: `75 000 ₽`
- Маржа: `25%`
- CR заявка->оплата: `30%`
- Целевой CAC оплаты: `5 000 ₽`
- Максимальный CAC оплаты: `10 000 ₽`

Из этого автоматически считаются пороги:

- `target CPL = 1 500 ₽`, `max CPL = 3 000 ₽`
- `target/max CPC` — через фактический CR клик->заявка (если доступен), иначе fallback на модельный.

### Goal IDs для расчёта фактического CR клик->заявка

Поддерживаются env:

- `SCORING_LEAD_PRIMARY_GOAL_IDS` (по умолчанию: `437747318` — СПАСИБО `/submitted`)
- `SCORING_LEAD_ASSIST_GOAL_IDS` (по умолчанию: `519273838,519273814,327368948,327368949`)

Логика:

- берутся клики из `stg_direct_campaign_daily` за окно (`30/90` дней),
- берутся достижения целей из Метрики (`goal<id>reaches`),
- если есть primary goals — используются они, иначе assist goals,
- считается фактический `CR клик->заявка` и автоматически подставляется в KPI-гипотезы.

### ENV для генерации баннеров

- `SCORING_IMAGE_PROVIDER` — `auto` (по умолчанию), `openai`, `openrouter`.
- `OPENAI_IMAGE_API_KEY` — ключ OpenAI Image API.
- `SCORING_IMAGE_MODEL` — модель OpenAI (`gpt-image-1.5` по умолчанию).
- `OPENAI_IMAGE_BASE_URL` — опциональный base URL для OpenAI Images.

OpenRouter fallback/режим:

- `OPENROUTER_API_KEY` — ключ OpenRouter (или fallback на `OPENAI_API_KEY`/`OPENAI_KEY`).
- `OPENROUTER_IMAGE_MODEL` — по умолчанию `google/gemini-2.5-flash-image-preview`.
- `OPENROUTER_BASE_URL` — по умолчанию `https://openrouter.ai/api/v1`.
- `OPENROUTER_HTTP_REFERER` и `OPENROUTER_APP_TITLE` — опционально для атрибуции OpenRouter.

## WebApp

В `/admin` добавлен раздел `Scoring`:

- summary cards: hot/warm/cold/avg
- line chart по сегментам за 30 дней
- distribution chart (hot/warm/cold)
- data table visitors (Tabulator)
- фильтры segment/source
- кнопка rebuild
- модалка visitor details + рекомендации

Рекомендации:

- `hot` → сильный CTA, follow-up, срочный оффер
- `warm` → дожим контентом (программа/преимущества/кейсы)
- `cold` → прогрев, доверие, объяснение концепции лагеря

## Как запустить

1. Применить SQL:

```bash
psql -h localhost -U traffic_admin -d traffic_analytics -f sql/040_scoring_v1.sql
psql -h localhost -U traffic_admin -d traffic_analytics -f sql/042_scoring_v1_explainable_upgrade.sql
```

2. Построить visitor-level признаки из реальной Метрики:

```bash
python3 -m src.run_build_scoring_features --days 30 --max-rows 50000
```

3. Запустить rebuild:

```bash
python3 -m src.run_scoring_v1
```

Опции:

```bash
python3 -m src.run_scoring_v1 --limit 500
python3 -m src.run_scoring_v1 --no-fallback
python3 -m src.run_scoring_v1 --features-days 30 --features-limit 50000
python3 -m src.run_scoring_v1 --skip-build-features  # только если нужен rebuild по уже загруженному staging
```

4. Проверить API:

- `GET /api/scoring/summary`
- `GET /api/scoring/visitors?limit=50`
- `GET /api/scoring/visitor/<id>`

5. Открыть `/admin` → раздел `Scoring`

## Scoring -> Direct activation (практика)

Dry-run план без записи в Direct:

```bash
python3 -m src.run_scoring_activation_sync --days 90 --min-audience-size 100
```

Выполнить sync в Direct:

```bash
SCORING_DIRECT_SYNC_ENABLED=1 \
python3 -m src.run_scoring_activation_sync --days 90 --min-audience-size 100 --execute
```

Добавить реакцию по Direct тегам:

```bash
python3 -m src.run_scoring_activation_sync --days 90 --with-reaction
```

### Auto-bootstrap Direct сущностей (adgroups + retargeting lists + audience targets)

Теперь можно автоматически создать сущности в Direct и сразу записать `SCORING_DIRECT_RETARGET_MAP_JSON` в env.

Dry-run:

```bash
python3 -m src.run_scoring_direct_bootstrap --days 90 --min-audience-size 100
```

Реальное создание:

```bash
python3 -m src.run_scoring_direct_bootstrap --days 90 --min-audience-size 100 --apply
```

Опционально можно явно задать кампанию:

```bash
python3 -m src.run_scoring_direct_bootstrap --campaign-id 123456789 --apply
```

Если `--campaign-id` не указан, скрипт берёт кампанию с наибольшим расходом за 30 дней из `stg_direct_campaign_daily`.

Что делает bootstrap:

1. Берёт scoring cohort-ы из activation plan.
2. Для каждого cohort создаёт:
   - `AdGroup` в выбранной кампании,
   - `RetargetingList` (по goal из Метрики),
   - `AudienceTarget` (привязка списка к группе).
3. Обновляет env-переменные:
   - `SCORING_DIRECT_RETARGET_MAP_JSON`
   - `SCORING_DIRECT_SYNC_ENABLED=1`

По умолчанию запись идет в `/home/kv145/traffic-analytics/.env` (чтобы webapp API сразу подхватил mapping после рестарта сервиса).

### Нужный env mapping для Direct

`SCORING_DIRECT_RETARGET_MAP_JSON` (пример):

```json
{
  "hot_all_7d": {"ad_group_id": 123456789, "retargeting_list_id": 987654321, "strategy_priority": "HIGH"},
  "warm_all_14d": {"ad_group_id": 123456780, "retargeting_list_id": 987654320, "strategy_priority": "NORMAL"}
}
```

## Local smoke test

Одна команда для end-to-end smoke-проверки:

```bash
python3 -m src.run_scoring_v1_smoke
```

Smoke script делает:

1. применяет seed SQL;
2. запускает `POST /api/scoring/rebuild` с `use_fallback=false`;
3. проверяет `GET /api/scoring/summary`;
4. проверяет `GET /api/scoring/visitors`;
5. проверяет `GET /api/scoring/visitor/{id}`;
6. печатает итог `PASS` или `FAIL`.

## Final attribution audit (one command)

Для финальной post-deploy проверки attribution по реальным visitor данным:

```bash
python3 -m src.run_scoring_attribution_audit \
  --base-url https://ai.aidaplus.ru \
  --features-days 30 \
  --features-limit 50000 \
  --insecure
```

Что делает команда:

1. снимает baseline по `unknown` (API sample);
2. запускает `POST /api/scoring/rebuild` с:
   - `sync_features=true`
   - `use_fallback=false`
   - `features_days=30`
   - `features_limit=50000`
3. снимает post-rebuild метрики;
4. печатает единый JSON-отчёт:
   - rebuild result
   - summary
   - before/after unknown
   - top traffic sources
   - 5 sample visitor rows

Примечание:
- если DB доступна (обычно на сервере), скрипт дополнительно считает точные метрики по таблицам `stg_metrica_visitors_features` и `mart_visitor_scoring`;
- если DB недоступна из текущей среды, скрипт честно показывает API-based выборку (до 1000 строк).

## How to seed

Ручной запуск seed:

```bash
psql -h localhost -U traffic_admin -d traffic_analytics -f sql/041_scoring_v1_seed.sql
```

Seed содержит 5 visitor:

- `hot` (booking-intent + returning),
- `warm`,
- `warm` (returning),
- `cold`,
- `cold`.

Seed безопасен для повторного запуска: используется `ON CONFLICT (visitor_id) DO UPDATE`.

## How to rebuild

Ручной rebuild:

```bash
python3 -m src.run_scoring_v1
```

Rebuild + Telegram отчет:

```bash
python3 -m src.run_scoring_v1 --report
```

Через API:

```bash
curl -s -X POST http://127.0.0.1:8088/api/scoring/rebuild \
  -H "Content-Type: application/json" \
  -d '{"use_fallback": false, "sync_features": true, "features_days": 30, "features_limit": 50000}'
```

Через API с Telegram-отчетом:

```bash
curl -s -X POST http://127.0.0.1:8088/api/scoring/rebuild \
  -H "Content-Type: application/json" \
  -d '{"use_fallback": false, "sync_features": true, "send_report": true}'
```

## Pipeline шаги

Рекомендуемая последовательность для production:

1. `run_etl.py` (включает extract Metrica + `build_scoring_features`)
2. `python3 -m src.run_scoring_v1 --no-fallback`
3. Проверка `GET /api/scoring/summary`

Если API Метрики временно недоступен, rebuild не блокируется при уже заполненном staging; если staging пустой и sync неуспешен — rebuild вернёт ошибку.

## Как проверить, что это реальные визиты, а не 5 seed

1. Проверить staging:

```bash
psql -h localhost -U traffic_admin -d traffic_analytics -c "select count(*) from stg_metrica_visitors_features;"
```

2. Проверить источники в mart:

```bash
psql -h localhost -U traffic_admin -d traffic_analytics -c "select data_source, count(*) from mart_visitor_scoring group by 1 order by 2 desc;"
```

Ожидаемо для реального контура:

- доминирует `data_source = stg_metrica_visitors_features`
- количество записей существенно больше 5

3. Проверить API/UI:

```bash
curl -s "http://127.0.0.1:8088/api/scoring/summary"
curl -s "http://127.0.0.1:8088/api/scoring/visitors?limit=100"
```

В `/admin/scoring` summary должен показывать реальное число visitor scoring записей, а не только seed.

## Telegram report

Новый модуль: `src/scoring/report.py`

Функция: `send_scoring_report(summary, top_visitors)`

Переменные окружения:

- `TELEGRAM_BOT_TOKEN`
- `CHAT_ID`

Совместимость: при отсутствии новых переменных используется fallback на `TG_TOKEN` и `TG_CHAT`.

## Как интерпретировать score по посетителю

`normalized_score` показывает степень вероятности покупки в диапазоне `0..1`.

- `hot` (>= 0.70): есть явные сигналы покупки.
- `warm` (0.40..0.69): посетитель вовлечен, но без финального действия.
- `cold` (< 0.40): пока ознакомительное поведение.

Как читать `explanation_json`:

- это вклад только сработавших факторов;
- ключ = фактор, значение = вклад в `raw_score`;
- нулевые факторы не сохраняются.

Что такое `short_reason`:

- короткая метка главной причины сегмента/оценки;
- используется для таблицы и быстрой фильтрации;
- примеры: `booking_intent`, `price_interest`, `returning_engaged`, `content_engaged`, `exploratory_low_intent`, `bounce_like_session`.

Как использовать `recommended_action`:

- это готовая маркетинговая инструкция на следующий шаг;
- для `hot`: приоритетный дожим в бронь;
- для `warm`: контентный дожим и усиление доверия;
- для `cold`: прогрев и объяснение ценности лагеря.

## How to validate expected segment output

Минимальная проверка после seed + rebuild:

```bash
curl -s "http://127.0.0.1:8088/api/scoring/visitors?limit=200&source=seed_"
```

Ожидаемое поведение:

- в выборке есть seeded visitor с префиксом `scoring_v1_seed_`;
- присутствуют сегменты `hot`, `warm`, `cold`;
- detail endpoint по seeded visitor возвращает непустой `explanation_json`.

## Готовность к v2 (ML)

Для перехода на ML достаточно заменить/расширить слой `scorer.py`:

- оставить контракт `ScoreResult`
- добавить `MLScorer` рядом с `RuleBasedScorer`
- выбирать engine через конфиг/флаг
- сохранить текущий API/таблицу без breaking changes
