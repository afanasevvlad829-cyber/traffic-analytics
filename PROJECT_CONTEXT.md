# Direct AI — Project Context

## Project

Traffic Analytics / Direct AI

Система анализа и автоматизации рекламы Яндекс Директ.

Проект собирает данные из рекламных кампаний, анализирует эффективность,
генерирует рекомендации и формирует очередь действий для исполнения.

Основная идея:

AI → анализ → рекомендация → подтверждение человеком → исполнение.

---

# Основные компоненты

## 1. Web Control Center

Папка:

webapp/

Функция:
панель управления системой.

Основной файл:

webapp/app.py

Используемые технологии:

- FastAPI
- HTML templates
- Vanilla JS dashboard
- REST API

Основные страницы:

/webapp
/admin

API endpoints:

/api/full-dashboard
/api/diagnostic
/api/config

Dashboard показывает:

- creative tasks
- structure issues
- forecast review
- approved actions
- pending actions

---

# 2. AI Engine

Папка:

src/

Основные модули:

direct_v11_executor.py
creative generation
structure analysis
forecast engine

Функции:

- анализ CTR
- анализ CPC
- анализ конверсий
- прогноз эффективности
- рекомендации по структуре
- генерация минус-слов
- генерация новых креативов

---

# 3. Database

Используется:

PostgreSQL

Основные таблицы:

creative_tasks
structure_items
forecast_items
approved_actions
action_log

Назначение:

creative_tasks  
→ задачи на создание креативов

structure_items  
→ проблемы структуры кампаний

forecast_items  
→ прогнозы эффективности

approved_actions  
→ действия, подтверждённые пользователем

action_log  
→ история выполнения действий

---

# 4. Executor

Файл:

src/direct_v11_executor.py

Назначение:

исполняет действия из таблицы

approved_actions

Логика:

AI recommendation
↓
user approval
↓
executor
↓
Yandex Direct API

Executor запускается через cron.

---

# 5. Diagnostics System

Endpoint:

/api/diagnostic

Запускает:

src/ai_diagnostic.sh

Проверяет:

- backend
- nginx
- ssl
- database
- executor
- API доступность

---

# 6. Infrastructure

Production server:

ai.aidaplus.ru

Stack:

nginx
uvicorn
systemd
certbot
postgresql

Backend порт:

127.0.0.1:8088

Reverse proxy:

nginx → uvicorn

---

# Development workflow

Разработка:

Mac
↓
Cursor IDE
↓
GitHub
↓
Server deploy

Этапы:

1. локальная разработка
2. commit
3. push на GitHub
4. deploy на сервер

---

# Development rules

Backend:

FastAPI

Frontend:

Vanilla JS dashboard

Database:

PostgreSQL

AI-логика должна быть:

- объяснимой
- проверяемой
- не выполнять действия автоматически

Все действия проходят этап:

AI recommendation
↓
Human approval
↓
Executor

---

# Project Goal

Создать AI-директора Яндекс Директа.

Система должна:

1. анализировать рекламу
2. находить проблемы
3. предлагать решения
4. показывать прогноз
5. выполнять действия после подтверждения
