
# Direct AI Control Center

AI‑система управления рекламными кампаниями (Яндекс Директ) с аналитикой, прогнозированием и автоматическим принятием решений.

## Назначение системы

Direct AI Control Center предназначен для:

- анализа рекламных кампаний
- прогнозирования CTR / CPC
- генерации рекламных креативов
- оптимизации структуры рекламных групп
- автоматического исполнения решений
- управления через WebApp и Telegram

---

# Общая архитектура

Система состоит из нескольких слоев:

Яндекс Директ API
        │
        ▼
ETL сбор данных
        │
        ▼
PostgreSQL аналитическое хранилище
        │
        ▼
AI аналитика и алгоритмы
        │
        ▼
WebApp + Telegram интерфейс

---

# Структура проекта

traffic-analytics
│
├─ src/
│   бизнес‑логика и исполнители
│
├─ webapp/
│   веб интерфейс FastAPI
│
├─ sql/
│   аналитические таблицы
│
├─ logs/
│   журналы работы системы
│
└─ docs/
    документация архитектуры

---

# Основные модули

## AI Creative Engine
Генерация альтернативных объявлений на основе текущих метрик.

## Group Builder
Оптимизация структуры рекламных групп.

## Forecast Engine
Прогноз CTR и CPC.

## Executor
Модуль исполнения решений.

## Diagnostic System
Система диагностики сервера и сервисов.

## Scoring v1
Rule-based visitor/lead scoring по данным Метрики:

- расчет вероятности покупки (`raw_score`, `normalized_score`)
- сегментация `hot / warm / cold`
- explainable `explanation_json`
- API + раздел `Scoring` в `/admin`

Документация: `docs/scoring.md`

---

# Web интерфейс

Адрес:

https://ai.aidaplus.ru

Функции:

- просмотр аналитики
- запуск диагностики
- управление решениями AI

---

# Telegram

Telegram используется как:

- интерфейс уведомлений
- точка входа в WebApp

---

# База данных

Используется PostgreSQL.

Основные таблицы:

mart_ai_creative_candidates
mart_group_builder
mart_forecast_review
mart_campaign_analytics
mart_visitor_scoring

---

# Cron задачи

Executor запускается регулярно:

*/3 * * * * direct_v11_executor.py

---

# Сервер

Ubuntu
Python
FastAPI
PostgreSQL
Nginx
Systemd

Порты:

80 nginx  
443 nginx  
8088 backend

---

# Диагностика

В WebApp доступна кнопка:

Диагностика

которая запускает:

/api/diagnostic

---

# Безопасность

В репозитории не должны храниться:

.env  
ключи API  
лог‑файлы

Все секреты находятся в `.env`.
