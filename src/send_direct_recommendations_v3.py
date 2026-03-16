import os
import math
import psycopg2
import requests

ENV_PATH = "/home/kv145/traffic-analytics/.env"

def load_env(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def pct_change(old, new):
    old = safe_float(old, 0.0)
    new = safe_float(new, 0.0)
    if old <= 0:
        return None
    return round(((new - old) / old) * 100, 1)

def money_change(old, new):
    old = safe_float(old, 0.0)
    new = safe_float(new, 0.0)
    return round(new - old, 2)

def interpret_metrics(decision, ctr_pct, account_ctr_pct, avg_cpc, account_avg_cpc, relevance_score, traffic_quality_score, cost):
    ctr_pct = safe_float(ctr_pct)
    account_ctr_pct = safe_float(account_ctr_pct)
    avg_cpc = safe_float(avg_cpc)
    account_avg_cpc = safe_float(account_avg_cpc)
    relevance_score = safe_float(relevance_score)
    traffic_quality_score = safe_float(traffic_quality_score)
    cost = safe_float(cost)

    lines = []

    if decision == "EXCLUDE":
        lines.append("Запросы в этом кластере выглядят нецелевыми или конкурентными.")
        if cost > 0:
            lines.append("На них уже был расход, поэтому их лучше вырезать, а не наблюдать.")
        else:
            lines.append("Даже если расход пока небольшой, лучше убрать этот мусор заранее.")
        return " ".join(lines)

    if ctr_pct >= max(account_ctr_pct * 1.2, account_ctr_pct + 1):
        lines.append("Объявление кликают хорошо.")
    elif ctr_pct < max(account_ctr_pct * 0.8, account_ctr_pct - 1):
        lines.append("CTR ниже среднего по аккаунту — заголовок или оффер цепляют слабо.")
    else:
        lines.append("CTR около среднего по аккаунту.")

    if avg_cpc > account_avg_cpc * 1.5 and account_avg_cpc > 0:
        lines.append("Клик заметно дороже среднего — Яндекс видит слабую релевантность или конкурентный интент.")
    elif avg_cpc > account_avg_cpc * 1.15 and account_avg_cpc > 0:
        lines.append("Клик немного дороже среднего — есть запас для улучшения релевантности.")
    elif account_avg_cpc > 0 and avg_cpc <= account_avg_cpc:
        lines.append("Стоимость клика нормальная или ниже средней.")

    if relevance_score < 55:
        lines.append("Главная проблема — слабое совпадение заголовка/оффера с запросом.")
    elif relevance_score < 70:
        lines.append("Релевантность средняя: объявление можно сделать точнее под интент.")
    else:
        lines.append("Релевантность уже неплохая.")

    if traffic_quality_score < 70:
        lines.append("Есть риск нецелевого трафика — часть запросов стоит почистить.")
    elif traffic_quality_score >= 90:
        lines.append("Качество трафика хорошее, значит проблема скорее в креативе, а не в мусорных запросах.")

    if decision == "REWRITE_AD":
        lines.append("Практический вывод: нужно тестировать более релевантные заголовки и описание.")
    elif decision == "LOWER_BID_OR_SPLIT":
        lines.append("Практический вывод: этот интент дорогой, его лучше упаковать в более точное объявление.")
    elif decision == "LANDING_CHECK":
        lines.append("Практический вывод: объявление уже цепляет, но стоит усилить соответствие оффера и первого экрана.")
    elif decision == "SCALE":
        lines.append("Практический вывод: это сильный шаблон объявления, его стоит развивать дальше.")

    return " ".join(lines)

def build_expected_effect(decision, ctr_pct, account_ctr_pct, avg_cpc, account_avg_cpc, relevance_score):
    ctr_pct = safe_float(ctr_pct)
    account_ctr_pct = safe_float(account_ctr_pct)
    avg_cpc = safe_float(avg_cpc)
    account_avg_cpc = safe_float(account_avg_cpc)
    relevance_score = safe_float(relevance_score)

    # Базовые консервативные прогнозы
    target_ctr = ctr_pct
    target_cpc = avg_cpc
    target_relevance = relevance_score

    if decision == "EXCLUDE":
        return {
            "target_ctr": None,
            "target_cpc": None,
            "target_relevance": None,
            "summary": "Ожидаемый эффект: убрать нецелевой расход и очистить поисковый трафик.",
        }

    if decision in ("REWRITE_AD", "LOWER_BID_OR_SPLIT", "LANDING_CHECK"):
        # CTR тянем к среднему аккаунта или чуть выше
        target_ctr = max(ctr_pct, min(max(account_ctr_pct, 0), ctr_pct * 1.25 if ctr_pct > 0 else account_ctr_pct))
        if account_ctr_pct > 0:
            target_ctr = max(target_ctr, round(account_ctr_pct * 1.05, 2))

        # CPC пытаемся опустить ближе к среднему
        if avg_cpc > 0 and account_avg_cpc > 0:
            target_cpc = round(max(account_avg_cpc * 1.05, avg_cpc * 0.7), 2)
            target_cpc = min(target_cpc, avg_cpc)
        elif avg_cpc > 0:
            target_cpc = round(avg_cpc * 0.85, 2)

        # Relevance тянем вверх
        target_relevance = min(80.0, max(relevance_score + 15, 65.0))

    elif decision == "SCALE":
        target_ctr = max(ctr_pct, round(account_ctr_pct * 1.1, 2)) if account_ctr_pct > 0 else ctr_pct
        target_cpc = avg_cpc
        target_relevance = min(85.0, max(relevance_score, 70.0))

    ctr_delta = pct_change(ctr_pct, target_ctr) if target_ctr is not None else None
    cpc_delta = pct_change(avg_cpc, target_cpc) if target_cpc is not None else None
    rel_delta = pct_change(relevance_score, target_relevance) if target_relevance is not None and relevance_score > 0 else None

    parts = []
    if target_ctr is not None:
        if ctr_delta is not None:
            parts.append(f"CTR: {round(ctr_pct,2)}% → {round(target_ctr,2)}% ({'+' if ctr_delta >= 0 else ''}{ctr_delta}%)")
        else:
            parts.append(f"CTR: {round(target_ctr,2)}%")
    if target_cpc is not None:
        if cpc_delta is not None:
            parts.append(f"CPC: {round(avg_cpc,2)} ₽ → {round(target_cpc,2)} ₽ ({cpc_delta}%)")
        else:
            parts.append(f"CPC: {round(target_cpc,2)} ₽")
    if target_relevance is not None:
        if rel_delta is not None:
            parts.append(f"Relevance: {round(relevance_score,2)} → {round(target_relevance,2)} ({'+' if rel_delta >= 0 else ''}{rel_delta}%)")
        else:
            parts.append(f"Relevance: {round(target_relevance,2)}")

    return {
        "target_ctr": target_ctr,
        "target_cpc": target_cpc,
        "target_relevance": target_relevance,
        "summary": "Ожидаемый эффект: " + " | ".join(parts) if parts else "Ожидаемый эффект: уточнить после теста.",
    }

def action_text(decision):
    mapping = {
        "REWRITE_AD": "Что делать: запустить A/B тест новых заголовков и описания.",
        "LOWER_BID_OR_SPLIT": "Что делать: запустить более точные варианты объявления под этот интент.",
        "LANDING_CHECK": "Что делать: проверить связку запрос → объявление → первый экран и протестировать более точный оффер.",
        "SCALE": "Что делать: оставить шаблон как опорный и использовать его как основу для похожих интентов.",
        "EXCLUDE": "Что делать: добавить минус-слова на уровень кампании.",
    }
    return mapping.get(decision, "Что делать: посмотреть вручную и принять решение по тесту.")

load_env(ENV_PATH)

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD"),
)
cur = conn.cursor()

cur.execute("""
select
    task_id,
    campaign_name,
    search_query,
    decision,
    impressions,
    clicks,
    ctr_pct,
    avg_cpc,
    cost,
    priority_score,
    exact_action,
    forecast_text,
    competitor_domains,
    ai_title_1,
    ai_title_2,
    ai_text,
    ai_title_1_b,
    ai_title_2_b,
    ai_text_b,
    ai_title_1_c,
    ai_title_2_c,
    ai_text_c,
    minus_words,
    account_avg_ctr_pct,
    account_avg_cpc,
    relevance_score,
    traffic_quality_score,
    ad_group_id,
    ad_title
from vw_direct_ops_report
where status = 'OPEN'
order by priority_score desc, cost desc
limit 8
""")
rows = cur.fetchall()

cur.execute("""
select
    campaign_name,
    auto_negative_keywords_copy_paste,
    keywords_count
from vw_campaign_negative_keywords_auto_copy_paste
order by campaign_name
""")
auto_neg_rows = cur.fetchall()

cur.execute("""
select
    campaign_name,
    review_negative_keywords_copy_paste,
    keywords_count
from vw_campaign_negative_keywords_review_copy_paste
order by campaign_name
""")
review_neg_rows = cur.fetchall()

cur.close()
conn.close()

text = "📊 DIRECTOLOGIST v3\n\n"

if not rows:
    text += "Сегодня открытых задач нет.\n\n"
else:
    for r in rows:
        (
            task_id, campaign, query, decision, impressions, clicks, ctr_pct, avg_cpc, cost,
            priority_score, exact_action, forecast_text, competitor_domains,
            ai_title_1, ai_title_2, ai_text,
            ai_title_1_b, ai_title_2_b, ai_text_b,
            ai_title_1_c, ai_title_2_c, ai_text_c,
            minus_words,
            account_avg_ctr_pct, account_avg_cpc, relevance_score, traffic_quality_score,
            ad_group_id, ad_title
        ) = r

        interpretation = interpret_metrics(
            decision=decision,
            ctr_pct=ctr_pct,
            account_ctr_pct=account_avg_ctr_pct,
            avg_cpc=avg_cpc,
            account_avg_cpc=account_avg_cpc,
            relevance_score=relevance_score,
            traffic_quality_score=traffic_quality_score,
            cost=cost,
        )

        expected = build_expected_effect(
            decision=decision,
            ctr_pct=ctr_pct,
            account_ctr_pct=account_avg_ctr_pct,
            avg_cpc=avg_cpc,
            account_avg_cpc=account_avg_cpc,
            relevance_score=relevance_score,
        )

        text += (
            f"#{task_id} | {decision}\n"
            f"{campaign}\n"
            f"ID группы: {ad_group_id}\n"
            f"Текущее объявление: {ad_title}\n"
            f"Запрос: {query}\n\n"
            f"📊 Метрики\n"
            f"Показы: {impressions} | Клики: {clicks}\n"
            f"CTR: {ctr_pct}% vs аккаунт {round(safe_float(account_avg_ctr_pct),2)}%\n"
            f"CPC: {avg_cpc} ₽ vs аккаунт {round(safe_float(account_avg_cpc),2)} ₽\n"
            f"Relevance: {round(safe_float(relevance_score),2)} | Traffic Quality: {round(safe_float(traffic_quality_score),2)}\n"
            f"Cost: {round(safe_float(cost),2)} ₽\n\n"
            f"💡 Интерпретация\n"
            f"{interpretation}\n\n"
            f"📈 Ожидаемый эффект\n"
            f"{expected['summary']}\n\n"
            f"🛠 Рекомендация системы\n"
            f"{action_text(decision)}\n"
            f"{exact_action}\n"
        )

        if competitor_domains:
            text += f"Конкуренты в SERP: {competitor_domains}\n"

        if decision in ("REWRITE_AD", "LOWER_BID_OR_SPLIT", "LANDING_CHECK"):
            if ai_title_1 or ai_text:
                text += (
                    f"\nНовые варианты объявления\n"
                    f"1) {ai_title_1}\n   {ai_title_2}\n   {ai_text}\n\n"
                    f"2) {ai_title_1_b}\n   {ai_title_2_b}\n   {ai_text_b}\n\n"
                    f"3) {ai_title_1_c}\n   {ai_title_2_c}\n   {ai_text_c}\n"
                )

        if decision == "EXCLUDE" and minus_words:
            text += f"\nМинус-слова: {minus_words}\n"

        text += f"\nЗакрыть: /done {task_id}\nОтложить: /snooze {task_id}\n\n---\n\n"

if auto_neg_rows:
    text += "🚫 AUTO_NEGATIVE ДЛЯ КАМПАНИЙ\n\n"
    for campaign_name, negative_keywords_copy_paste, keywords_count in auto_neg_rows:
        text += (
            f"{campaign_name}\n"
            f"Кол-во: {keywords_count}\n"
            f"{negative_keywords_copy_paste}\n\n---\n\n"
        )

if review_neg_rows:
    text += "👀 REVIEW_NEGATIVE ДЛЯ РУЧНОЙ ПРОВЕРКИ\n\n"
    for campaign_name, negative_keywords_copy_paste, keywords_count in review_neg_rows:
        text += (
            f"{campaign_name}\n"
            f"Кол-во: {keywords_count}\n"
            f"{negative_keywords_copy_paste}\n\n---\n\n"
        )

url = f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": os.getenv("TG_CHAT"), "text": text[:4000]},
    timeout=30
)
resp.raise_for_status()
print("Direct v3 report sent")
