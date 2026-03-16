
# Architecture

Direct AI построен по модульной архитектуре.

## Основные компоненты

1. Data Layer  
PostgreSQL хранит аналитические данные.

2. Processing Layer  
Python скрипты выполняют анализ и прогноз.

3. AI Layer  
Алгоритмы генерации креативов и оптимизации.

4. Execution Layer  
Executor применяет решения.

5. Interface Layer  
WebApp + Telegram.

---

# Поток данных

Данные рекламы  
↓  
ETL  
↓  
PostgreSQL  
↓  
AI анализ  
↓  
Рекомендации  
↓  
Executor  
↓  
Изменения кампаний
