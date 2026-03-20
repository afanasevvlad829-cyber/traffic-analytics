from __future__ import annotations

from typing import Any


def _segment_ru(segment: str) -> str:
    s = (segment or "").lower()
    if s == "hot":
        return "горячие"
    if s == "warm":
        return "тёплые"
    return "холодные"


def _source_ru(source: str) -> str:
    s = (source or "").lower()
    mapping = {
        "direct": "прямой трафик",
        "organic": "органический поиск",
        "referral": "реферальный трафик",
        "yandex_direct": "Яндекс Директ",
        "messenger": "мессенджеры",
        "social": "соцсети",
        "email": "email",
    }
    return mapping.get(s, source or "не определено")


def build_kpi_hypothesis(
    segment: str,
    short_reason: str,
    traffic_source: str,
    baseline: dict[str, Any] | None = None,
    click_to_lead_actual_pct: float | None = None,
    reference_window_days: int | None = None,
) -> dict[str, Any]:
    s = (segment or "").lower().strip()
    src = _source_ru(traffic_source)
    reason = (short_reason or "").lower().strip()
    baseline = baseline or {}

    # Бизнес-входы (из текущих договоренностей):
    # средний чек 75 000 ₽, маржа 25%, CR заявка->оплата 30%,
    # допустимый CAC оплаты до 10 000 ₽, целевой CAC оплаты 5 000 ₽.
    avg_check_rub = 75_000.0
    margin_pct = 25.0
    lead_to_payment_cvr_pct = 30.0
    max_cac_pay_rub = 10_000.0
    target_cac_pay_rub = 5_000.0
    margin_rub = round(avg_check_rub * margin_pct / 100.0, 2)
    max_marketing_share_of_margin_pct = round((max_cac_pay_rub / margin_rub) * 100.0, 2) if margin_rub > 0 else 0.0

    actual_click_to_lead = None
    if click_to_lead_actual_pct is not None:
        try:
            parsed = float(click_to_lead_actual_pct)
            if parsed > 0:
                actual_click_to_lead = parsed
        except Exception:  # noqa: BLE001
            actual_click_to_lead = None

    if actual_click_to_lead is not None:
        if s == "hot":
            click_to_lead_target_pct = max(0.8, actual_click_to_lead * 1.25)
            click_to_lead_min_pct = max(0.6, actual_click_to_lead * 1.05)
        elif s == "warm":
            click_to_lead_target_pct = max(0.7, actual_click_to_lead * 1.00)
            click_to_lead_min_pct = max(0.5, actual_click_to_lead * 0.85)
        else:
            click_to_lead_target_pct = max(0.5, actual_click_to_lead * 0.75)
            click_to_lead_min_pct = max(0.4, actual_click_to_lead * 0.60)
    else:
        if s == "hot":
            click_to_lead_min_pct = 8.0
            click_to_lead_target_pct = 10.0
        elif s == "warm":
            click_to_lead_min_pct = 4.0
            click_to_lead_target_pct = 5.5
        else:
            click_to_lead_min_pct = 1.5
            click_to_lead_target_pct = 2.5

    if s == "hot":
        objective = "Дожим до заявки/бронирования"
        ctr_str_min_pct = 3.2
        ctr_str_target_pct = 4.0
        sample_gate = {"min_impressions": 3000, "min_clicks": 80}
    elif s == "warm":
        objective = "Перевод в заявку через контент/доверие"
        ctr_str_min_pct = 2.4
        ctr_str_target_pct = 3.2
        sample_gate = {"min_impressions": 4000, "min_clicks": 100}
    else:
        objective = "Прогрев и квалификация аудитории"
        ctr_str_min_pct = 1.6
        ctr_str_target_pct = 2.2
        sample_gate = {"min_impressions": 5000, "min_clicks": 120}

    # Формулы:
    # CPL = CAC_оплаты * CR(заявка->оплата)
    # CPC = CPL * CR(клик->заявка)
    target_cpl_rub = round(target_cac_pay_rub * lead_to_payment_cvr_pct / 100.0, 2)
    max_cpl_rub = round(max_cac_pay_rub * lead_to_payment_cvr_pct / 100.0, 2)
    target_cpc_rub = round(target_cpl_rub * click_to_lead_target_pct / 100.0, 2)
    max_cpc_rub = round(max_cpl_rub * click_to_lead_min_pct / 100.0, 2)

    expected = {
        "ctr_str_min_pct": ctr_str_min_pct,
        "ctr_str_target_pct": ctr_str_target_pct,
        "click_to_lead_min_pct": click_to_lead_min_pct,
        "click_to_lead_target_pct": click_to_lead_target_pct,
        "click_to_lead_actual_pct": round(actual_click_to_lead, 2) if actual_click_to_lead is not None else None,
        "click_to_lead_basis": "real" if actual_click_to_lead is not None else "model",
        "reference_window_days": int(reference_window_days or 0) if reference_window_days else None,
        # backward compatibility for older UI blocks
        "cvr_to_lead_min_pct": click_to_lead_min_pct,
        "cvr_to_lead_target_pct": click_to_lead_target_pct,
        "lead_to_payment_cvr_pct": lead_to_payment_cvr_pct,
        "target_cpl_rub": target_cpl_rub,
        "max_cpl_rub": max_cpl_rub,
        "target_cpc_rub": target_cpc_rub,
        "max_cpc_rub": max_cpc_rub,
        "avg_cpc_max_rub": max_cpc_rub,
        "target_cac_pay_rub": target_cac_pay_rub,
        "max_cac_pay_rub": max_cac_pay_rub,
        # backward compatibility name
        "target_cpa_rub": target_cac_pay_rub,
    }

    if s == "hot":
        expected = {
            **expected,
            "click_to_lead_target_pct": 10.0,
            "cvr_to_lead_target_pct": 10.0,
        }
        if actual_click_to_lead is not None:
            expected["click_to_lead_target_pct"] = click_to_lead_target_pct
            expected["cvr_to_lead_target_pct"] = click_to_lead_target_pct
        success_rule = (
            f"Успех: CPL <= {target_cpl_rub:.0f} ₽ и CAC оплаты <= {target_cac_pay_rub:.0f} ₽, "
            f"при CR клик->заявка >= {expected['click_to_lead_target_pct']:.1f}% и CTR(STR) >= {ctr_str_target_pct:.1f}%."
        )
    elif s == "warm":
        expected = {**expected}
        success_rule = (
            f"Успех: CPL <= {target_cpl_rub:.0f} ₽ и CAC оплаты <= {target_cac_pay_rub:.0f} ₽, "
            f"при CR клик->заявка >= {expected['click_to_lead_target_pct']:.1f}% и CTR(STR) >= {ctr_str_target_pct:.1f}%."
        )
    else:
        expected = {**expected}
        success_rule = (
            f"Успех: CPL <= {max_cpl_rub:.0f} ₽ и CAC оплаты <= {max_cac_pay_rub:.0f} ₽, "
            f"при CR клик->заявка >= {expected['click_to_lead_target_pct']:.1f}% и CTR(STR) >= {ctr_str_target_pct:.1f}%."
        )

    baseline_payload = {
        "impressions": int(baseline.get("impressions") or 0),
        "clicks": int(baseline.get("clicks") or 0),
        "ctr_pct": float(baseline.get("ctr_pct") or 0.0),
        "avg_cpc_rub": float(baseline.get("avg_cpc") or 0.0),
        "cost_rub": float(baseline.get("cost") or 0.0),
    }

    primary_metrics = [
        {"key": "cpl_rub", "label": "Цена заявки (CPL)", "why": "Главная метрика стоимости лида."},
        {"key": "cac_pay_rub", "label": "Цена оплаты (CAC)", "why": "Контроль экономики продаж."},
        {"key": "click_to_lead_pct", "label": "CR клик->заявка", "why": "Качество трафика и посадочной."},
    ]
    secondary_metrics = [
        {"key": "ctr_str_pct", "label": "CTR (STR)", "why": "Показывает отклик на объявление."},
        {"key": "avg_cpc_rub", "label": "Цена клика (CPC)", "why": "Контроль цены трафика."},
        {"key": "impressions", "label": "Показы", "why": "Проверка статистической значимости теста."},
        {"key": "clicks", "label": "Клики", "why": "База для расчёта CTR/CVR."},
    ]

    nuance = "цена" if "price" in reason else "контент/оффер"
    return {
        "objective": objective,
        "comparison_window_days": 14 if s in {"hot", "warm"} else 21,
        "source_context": src,
        "angle_context": nuance,
        "sample_gate": sample_gate,
        "economics": {
            "avg_check_rub": avg_check_rub,
            "margin_pct": margin_pct,
            "margin_rub": margin_rub,
            "max_marketing_share_of_margin_pct": max_marketing_share_of_margin_pct,
        },
        "baseline": baseline_payload,
        "expected": expected,
        "primary_metrics": primary_metrics,
        "secondary_metrics": secondary_metrics,
        "success_rule": success_rule,
        "data_gap_note": (
            f"CR клик->заявка взят из факта за {int(reference_window_days)} дней."
            if actual_click_to_lead is not None and reference_window_days
            else "CR клик->заявка пока модельный (по сегментам). После расчета факта из Метрики/CRM обновить target/max автоматически."
        ),
    }


def build_creative_plan_row(segment: str, short_reason: str, traffic_source: str) -> dict[str, Any]:
    s = (segment or "").lower()
    reason = (short_reason or "").lower()
    src = _source_ru(traffic_source)

    if s == "hot":
        return {
            "creative_angle": "дожим на бронирование",
            "headline": "Места в AidaCamp ограничены",
            "body": "Вы уже изучали условия и бронирование. Зафиксируйте место ребёнка в ближайший заезд.",
            "cta": "Забронировать место",
            "offer": "приоритетная консультация + подбор смены",
            "hypothesis": f"Для сегмента { _segment_ru(s) } из канала '{src}' сработает срочный CTA и дефицит.",
            "direct_tag": "scoring_hot_retarget_7d",
        }

    if s == "warm":
        if "price" in reason:
            return {
                "creative_angle": "ценность за цену",
                "headline": "Что входит в программу AidaCamp",
                "body": "Покажите родителю, за что он платит: программа, наставники, безопасность, результаты.",
                "cta": "Посмотреть программу",
                "offer": "разбор формата лагеря под ваш запрос",
                "hypothesis": f"Для тёплых лидов из '{src}' с интересом к цене лучше работает контент с ценностью.",
                "direct_tag": "scoring_warm_price_value_14d",
            }
        return {
            "creative_angle": "прогрев кейсами и доверием",
            "headline": "Как AidaCamp помогает подросткам",
            "body": "Кейсы родителей, формат работы, результаты по дисциплине и мотивации.",
            "cta": "Смотреть кейсы",
            "offer": "бесплатная консультация по подбору смены",
            "hypothesis": f"Для тёплых лидов из '{src}' лучше работает социальное доказательство.",
            "direct_tag": "scoring_warm_content_21d",
        }

    return {
        "creative_angle": "мягкий прогрев",
        "headline": "Почему AidaCamp подходит подросткам",
        "body": "Понятно и без давления объяснить формат лагеря, пользу и безопасность для семьи.",
        "cta": "Узнать подробнее",
        "offer": "чек-лист выбора лагеря для подростка",
        "hypothesis": f"Для холодных лидов из '{src}' лучше работает обучающий контент без жесткого CTA.",
        "direct_tag": "scoring_cold_education_30d",
    }


def build_creative_variants(
    segment: str,
    short_reason: str,
    traffic_source: str,
    max_variants: int = 3,
) -> list[dict[str, Any]]:
    s = (segment or "").lower()
    reason = (short_reason or "").lower()
    src = _source_ru(traffic_source)
    limit = max(1, min(int(max_variants or 3), 5))

    if s == "hot":
        variants = [
            {
                "variant_key": "hot_urgency",
                "creative_angle": "срочность и дефицит",
                "headline": "Осталось мало мест в AidaCamp",
                "body": "Вы уже интересовались бронированием. Закрепите место на ближайшую смену, пока есть слот.",
                "cta": "Забронировать место",
                "why_this": f"Сегмент HOT из канала '{src}': уже есть сигналы покупки, нужен быстрый закрывающий оффер.",
            },
            {
                "variant_key": "hot_consult",
                "creative_angle": "личный разбор",
                "headline": "Подберём смену под вашего подростка",
                "body": "Короткая консультация по программе и условиям, чтобы быстро принять решение о записи.",
                "cta": "Получить консультацию",
                "why_this": f"HOT лучше реагирует на короткий путь к решению без лишнего контента.",
            },
            {
                "variant_key": "hot_value",
                "creative_angle": "сильная ценность",
                "headline": "Что получает подросток в AidaCamp",
                "body": "Фокус на дисциплине, мотивации и поддержке наставников. Переходите к бронированию сразу.",
                "cta": "Перейти к брони",
                "why_this": f"HOT-аудитория близка к покупке: показываем ценность + прямой CTA.",
            },
        ]
        return variants[:limit]

    if s == "warm":
        if "price" in reason:
            variants = [
                {
                    "variant_key": "warm_price_breakdown",
                    "creative_angle": "цена через ценность",
                    "headline": "За что вы платите в AidaCamp",
                    "body": "Покажем программу, наставников и результаты подростков, чтобы цена была понятна и обоснована.",
                    "cta": "Посмотреть программу",
                    "why_this": f"WARM + интерес к цене из '{src}': нужен контент с разбивкой ценности.",
                },
                {
                    "variant_key": "warm_cases",
                    "creative_angle": "доверие и кейсы",
                    "headline": "Истории родителей после AidaCamp",
                    "body": "Реальные кейсы о том, как подростки меняют отношение к учебе и ответственности.",
                    "cta": "Смотреть кейсы",
                    "why_this": "Для WARM сегмента социальное доказательство снижает барьер до заявки.",
                },
                {
                    "variant_key": "warm_soft_offer",
                    "creative_angle": "мягкий оффер",
                    "headline": "Подберём формат лагеря под вашу задачу",
                    "body": "Без давления: коротко разберем запрос семьи и предложим оптимальную смену.",
                    "cta": "Оставить заявку",
                    "why_this": "WARM-сегменту полезен мягкий переход от изучения к контакту.",
                },
            ]
        else:
            variants = [
                {
                    "variant_key": "warm_program",
                    "creative_angle": "программа и преимущества",
                    "headline": "Как устроена программа AidaCamp",
                    "body": "Показываем ключевые активности, формат дня и практическую пользу для подростка.",
                    "cta": "Узнать программу",
                    "why_this": f"WARM из '{src}': логика дожима через полезный контент.",
                },
                {
                    "variant_key": "warm_trust",
                    "creative_angle": "доверие",
                    "headline": "Безопасность и сопровождение в AidaCamp",
                    "body": "Разбираем подход команды и поддержку подростков, чтобы снять главные сомнения родителей.",
                    "cta": "Смотреть детали",
                    "why_this": "Для WARM лидов важно снять риск и укрепить доверие.",
                },
                {
                    "variant_key": "warm_consult",
                    "creative_angle": "конверсия в диалог",
                    "headline": "Получите консультацию перед выбором смены",
                    "body": "Поможем определить подходящий формат лагеря и маршрут развития подростка.",
                    "cta": "Получить консультацию",
                    "why_this": "Следующий шаг для WARM — перевод в контакт с менеджером.",
                },
            ]
        return variants[:limit]

    variants = [
        {
            "variant_key": "cold_education",
            "creative_angle": "обучающий прогрев",
            "headline": "Как выбрать лагерь для подростка",
            "body": "Полезный гайд для родителей: на что смотреть в программе, среде и наставниках.",
            "cta": "Получить гайд",
            "why_this": f"COLD из '{src}': сначала образование и доверие, затем продажа.",
        },
        {
            "variant_key": "cold_problem_solution",
            "creative_angle": "боль → решение",
            "headline": "Подросток теряет мотивацию? Есть решение",
            "body": "Покажем подход AidaCamp к дисциплине, ответственности и развитию самостоятельности.",
            "cta": "Узнать как работает",
            "why_this": "COLD-сегмент лучше реагирует на ясную проблему и понятный путь решения.",
        },
        {
            "variant_key": "cold_social_proof",
            "creative_angle": "социальное доказательство",
            "headline": "Почему родители выбирают AidaCamp",
            "body": "Отзывы, результаты и формат сопровождения подростков без давления и стресса.",
            "cta": "Смотреть отзывы",
            "why_this": "Для холодного трафика сначала строим доверие через доказательства.",
        },
    ]
    return variants[:limit]
