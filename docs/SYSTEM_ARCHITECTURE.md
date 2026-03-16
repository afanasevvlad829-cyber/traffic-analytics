# SYSTEM_ARCHITECTURE.md

# Системная архитектура Direct AI Control Center

## Назначение

Direct AI Control Center — это система управления рекламными кампаниями Яндекс Директ, которая объединяет:

- сбор и хранение рекламной аналитики
- AI-анализ рекламных кампаний
- генерацию креативов
- прогнозирование CTR / CPC
- анализ структуры групп
- исполнение управленческих решений
- интерфейсы управления через Web и Telegram

Проект строится как операционная система AI-директолога, а не просто как набор скриптов или дашборд.

---

# 1. Концептуальная схема

Система устроена как несколько уровней:

Яндекс Директ API
        ↓
ETL / Extractors
        ↓
PostgreSQL / marts / views
        ↓
AI engines
        ↓
Decision layer
        ↓
Executor layer
        ↓
WebApp / Full Web / Telegram

---

# 2. Ключевые уровни системы

## 2.1 Data Source Layer

Источник данных:

- Яндекс Директ API
- данные по объявлениям
- кампаниям
- группам
- ключевым словам
- метрикам CTR / CPC / spend / clicks / impressions

Назначение слоя:
- получить фактические рекламные данные
- поддерживать регулярное обновление аналитического хранилища

---

## 2.2 ETL Layer

ETL-слой отвечает за загрузку и преобразование данных.

Типичные задачи:
- загрузка статистики кампаний
- загрузка метаданных объявлений
- обновление аналитических таблиц
- построение marts и views

Примеры сущностей:
- объявления
- кампании
- группы
- поисковые запросы
- SERP / competitor data

ETL работает по cron и периодически обновляет базу.

---

## 2.3 Storage Layer

Хранилище — PostgreSQL.

Используется как:
- аналитическое хранилище
- очередь действий
- журнал решений
- база для AI-контекста

### Основные типы таблиц

#### 1. Сырые / промежуточные таблицы
Хранят исходные или полуобработанные данные из API.

#### 2. Mart-таблицы
Хранят готовые аналитические сущности:
- mart_ai_creative_candidates
- mart_group_builder
- mart_forecast_review
- mart_negative_actions
- mart_ai_ab_test_actions
- ui_decision_log
- ai_context_registry

#### 3. View-слой
Используется для:
- safe negatives
- blocked negatives
- scoring views
- отчётных выборок

---

# 3. AI Layer

Это ядро системы.

AI-слой состоит из нескольких специализированных движков.

## 3.1 Creative Engine

Назначение:
- анализ текущих объявлений
- генерация альтернативных заголовков и описаний
- подготовка вариантов A/B тестов

Вход:
- текущее объявление
- исторические метрики
- ключевые запросы
- контекст кампании

Выход:
- варианты A / B / C
- прогнозный CTR
- прогнозный CPC
- confidence
- explanation

Основная таблица:
- mart_ai_creative_candidates

---

## 3.2 Forecast Engine

Назначение:
- предсказание эффективности объявления
- сравнение прогноза с фактом

Вход:
- метрики объявлений
- тексты
- история CTR / CPC
- аккаунтные средние значения

Выход:
- predicted CTR
- predicted CPC
- predicted relevance
- forecast review

Назначение для бизнеса:
- понимать, какие AI-решения реально работают
- отсеивать слабые рекомендации
- строить feedback loop

---

## 3.3 Group Builder

Назначение:
- анализ структуры рекламных групп
- поиск разнородных запросов
- рекомендации по split / rebuild

Вход:
- ключевые фразы
- поисковые запросы
- текущее распределение по ad groups

Выход:
- текстовая рекомендация
- suggested grouping
- split priority
- expected effect
- risks

Таблица:
- mart_group_builder

---

## 3.4 Negative Keyword AI

Назначение:
- анализ кандидатных минус-слов
- разделение на safe / blocked
- защита ядра оффера от самоубийственного минусования

Вход:
- поисковые запросы
- ключевые слова
- фразы с мусорным интентом

Выход:
- safe negatives
- blocked negatives
- copy-paste block
- очередь на применение

---

## 3.5 AI Context Layer

Назначение:
- подготовка структурированного контекста для AI-анализа
- формирование единых кодов объектов

Коды:
- CR-... — creative objects
- ST-... — structure issues
- NG-... — negative blocks
- FC-... — forecast review

Таблица:
- ai_context_registry

Это позволяет:
- копировать контекст в чат
- анализировать объект по коду
- передавать данные другой нейросети

---

# 4. Decision Layer

Этот слой превращает аналитику в управленческие решения.

## Примеры решений:
- approve A/B test
- ignore
- snooze
- apply safe negatives
- apply split
- manual review

Слой решений живёт одновременно:
- в WebApp
- в Full Web interface
- в Telegram logic
- в decision log

Основная идея:
система не просто показывает данные, а формирует управляемые действия.

---

# 5. Executor Layer

Executor — это мост между аналитикой и боевыми изменениями.

## Основные режимы

### 5.1 Safe Manual Mode
Действия:
- пишутся в очередь
- логируются
- переводятся по статусам
- не всегда сразу мутируют Direct API

Этот режим безопасен для ранних этапов.

### 5.2 Direct API Mode
Executor:
- создаёт новые объявления
- применяет safe negatives
- меняет статусы сущностей
- фиксирует результат

Типовой цикл:
1. читаем PENDING
2. запускаем действие
3. пишем DONE / FAILED
4. добавляем запись в журнал решений

Таблицы:
- mart_ai_ab_test_actions
- mart_negative_actions
- mart_structure_actions
- ui_decision_log

---

# 6. Interface Layer

Система имеет два основных интерфейса и один вспомогательный.

## 6.1 Telegram Bot

Функции:
- алерты
- уведомления
- вход в WebApp
- AI analysis mode
- отправка кратких рекомендаций

Telegram — это оперативный интерфейс.

---

## 6.2 Telegram WebApp

Функции:
- просмотр карточек AI-решений
- approve / ignore / snooze
- apply safe negatives
- structure actions
- журнал решений
- диагностика

Telegram WebApp — это быстрый интерфейс принятия решений.

---

## 6.3 Full Web Interface

Назначение:
- большие таблицы
- длинные explain-блоки
- работа с большим количеством данных
- постраничная аналитика
- copy-to-AI context

Full Web нужен потому, что Telegram WebApp ограничен по плотности интерфейса.

---

# 7. Diagnostic Layer

Диагностический слой позволяет быстро собирать состояние системы.

Компоненты:
- backend health
- config API
- nginx status
- systemd status
- certbot status
- портовая диагностика
- domain checks
- Telegram menu button check
- diagnostic report

Файлы:
- src/ai_diagnostic.sh
- /api/diagnostic

Цель:
- давать одну кнопку “Диагностика”
- быстро копировать технический отчёт в чат

---

# 8. Deployment Layer

Серверный стек:

- Ubuntu
- Python
- FastAPI / Uvicorn
- PostgreSQL
- Nginx
- Systemd
- Certbot
- Cron

Поток запроса:

Browser / Telegram WebApp
        ↓
https://ai.aidaplus.ru
        ↓
nginx
        ↓
127.0.0.1:8088
        ↓
FastAPI app
        ↓
PostgreSQL

---

# 9. Информационные потоки

## 9.1 Аналитический поток

Директ API
→ ETL
→ Postgres
→ AI engines
→ dashboards / alerts

## 9.2 Поток решений

WebApp / Telegram / Full Web
→ decision API
→ action queue
→ executor
→ Direct API
→ status / logs

## 9.3 Поток AI-контекста

mart tables
→ ai_context_registry
→ copy for AI
→ Telegram / external LLM analysis

---

# 10. Логика работы системы по шагам

## Сценарий 1: слабое объявление
1. ETL загружает статистику
2. Creative engine обнаруживает слабое объявление
3. Генерируются 3 варианта
4. Forecast engine рассчитывает прогноз
5. В WebApp появляется карточка
6. Пользователь нажимает Approve A/B
7. Решение попадает в queue
8. Executor применяет действие
9. Результат пишется в action log
10. Позже forecast review сравнивает прогноз с фактом

## Сценарий 2: мусорный трафик
1. AI negative layer находит плохие запросы
2. Формируются safe negatives и blocked negatives
3. В интерфейсе появляется блок
4. Пользователь нажимает Apply safe negatives
5. Действие уходит в queue
6. Executor применяет минус-слова
7. Результат фиксируется

## Сценарий 3: плохая структура групп
1. Group builder анализирует запросы
2. Выявляет разнородные группы
3. Формирует split recommendation
4. В интерфейсе доступна карточка
5. Пользователь выбирает Apply split или Snooze
6. Действие фиксируется в журнале

---

# 11. Принципы проектирования

## 11.1 Отделение аналитики от исполнения
Сначала система анализирует и предлагает, потом исполняет.

## 11.2 Управляемость
Все действия проходят через:
- очередь
- статусы
- журнал

## 11.3 Наблюдаемость
У системы есть:
- health
- диагностика
- action log
- forecast review

## 11.4 Расширяемость
Новые модули могут быть добавлены как отдельные AI engines.

## 11.5 Безопасность
Секреты и ключи находятся только в .env, не в репозитории.

---

# 12. Текущее состояние зрелости

На текущий момент система находится между:
- V1 — операционный AI-директолог
- V1.1 — частичный исполнитель
- V1.2 — контекст, диагностика, WebApp control

Это уже не просто аналитика, а управляемая рекламная AI-система.

---

# 13. Следующие уровни развития

## V2
- полноценный Full Web Admin
- AI context copy everywhere
- Telegram analysis by code
- executor hardening
- feedback loop

## V3
- self-learning headline lift
- auto experiment prioritization
- structure scoring
- negative keyword confidence model

## V4
- автоматическое расширение семантики
- budget allocator
- bid optimization
- automated campaign builder

---

# 14. Роль AI в системе

AI здесь выполняет 4 роли:

## 1. Analyst
Понимает, что плохо работает.

## 2. Strategist
Предлагает, что нужно изменить.

## 3. Executor
Применяет решение.

## 4. Copilot
Помогает человеку принимать более сильные решения.

---

# 15. Итоговое определение системы

Direct AI Control Center — это AI-платформа управления контекстной рекламой, в которой аналитика, прогнозирование, рекомендации, исполнение и интерфейсы принятия решений объединены в единую операционную систему AI-директолога.
