from __future__ import annotations

import base64
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

STATIC_ROOT = Path("/home/kv145/traffic-analytics/webapp/static")
OUTPUT_DIR = STATIC_ROOT / "generated" / "scoring_banners"


def _safe_slug(value: str, default: str = "item") -> str:
    raw = (value or "").strip().lower()
    slug = re.sub(r"[^a-z0-9_\\-]+", "-", raw)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or default


def _openai_base_url() -> str:
    # Preferred override for image-only calls.
    explicit = str(os.getenv("OPENAI_IMAGE_BASE_URL", "")).strip()
    if explicit:
        return explicit.rstrip("/")

    return "https://api.openai.com/v1"


def _openrouter_base_url() -> str:
    explicit = str(os.getenv("OPENROUTER_BASE_URL", "")).strip()
    if explicit:
        return explicit.rstrip("/")
    return "https://openrouter.ai/api/v1"


def _image_provider() -> str:
    raw = str(os.getenv("SCORING_IMAGE_PROVIDER", "auto")).strip().lower()
    if raw in {"openai", "openrouter", "auto"}:
        return raw
    return "auto"


def _openrouter_model() -> str:
    return str(
        os.getenv("OPENROUTER_IMAGE_MODEL", "google/gemini-2.5-flash-image")
    ).strip()


def _openrouter_headers(token: str) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    referer = str(
        os.getenv("OPENROUTER_HTTP_REFERER", "") or os.getenv("APP_BASE_URL", "")
    ).strip()
    if referer:
        headers["HTTP-Referer"] = referer
    title = str(os.getenv("OPENROUTER_APP_TITLE", "AidaCamp Scoring")).strip()
    if title:
        headers["X-Title"] = title
    return headers


def _aspect_ratio_from_size(size: str) -> str:
    raw = str(size or "").strip().lower()
    mapping = {
        "1024x1024": "1:1",
        "1536x1024": "3:2",
        "1024x1536": "2:3",
        "1344x768": "16:9",
        "768x1344": "9:16",
    }
    return mapping.get(raw, "16:9")


def _openrouter_image_bytes(
    *,
    prompt: str,
    model: str,
    n: int,
    size: str,
    token: str,
    timeout: int,
) -> tuple[list[bytes], str]:
    endpoint = f"{_openrouter_base_url()}/chat/completions"
    headers = _openrouter_headers(token)
    aspect_ratio = _aspect_ratio_from_size(size)
    images: list[bytes] = []
    last_error = ""

    for _ in range(max(1, n)):
        payload: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "modalities": ["image", "text"],
            "stream": False,
            "image_config": {"aspect_ratio": aspect_ratio},
        }
        try:
            resp = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
            if resp.status_code >= 400:
                last_error = f"model={model} | {_extract_http_error(resp)}"
                continue
            body = resp.json()
            choices = body.get("choices") or []
            message = (choices[0] or {}).get("message") if choices else {}
            message = message or {}
            payload_images = message.get("images") or []
            if not payload_images:
                last_error = f"model={model} | no images in response"
                continue

            image_picked = False
            for image in payload_images:
                image_url_obj = image.get("image_url") or image.get("imageUrl") or {}
                image_url = str(image_url_obj.get("url") or "").strip()
                if not image_url:
                    continue
                if image_url.startswith("data:image"):
                    _, _, b64_part = image_url.partition(",")
                    if not b64_part:
                        continue
                    images.append(base64.b64decode(b64_part))
                    image_picked = True
                    break
                images.append(_download_binary(image_url, timeout=timeout))
                image_picked = True
                break

            if not image_picked:
                last_error = f"model={model} | no usable image payload"
        except Exception as exc:  # noqa: BLE001
            last_error = f"model={model} | {exc}"

    return images, last_error


def _is_region_blocked_error(error: str) -> bool:
    text = str(error or "").strip().lower()
    return (
        "unsupported_country_region_territory" in text
        or "country, region, or territory not supported" in text
        or "status=403" in text
    )


def _banner_prompt(
    *,
    cohort_name: str,
    segment: str,
    os_root: str,
    variant: dict[str, Any],
    short_reason: str,
    source_hint: str,
) -> str:
    headline = str(variant.get("headline") or "").strip()
    body = str(variant.get("body") or "").strip()
    cta = str(variant.get("cta") or "").strip()
    angle = str(variant.get("creative_angle") or "").strip()
    why_this = str(variant.get("why_this") or "").strip()

    return (
        "Сгенерируй рекламный баннер для лагеря AidaCamp.\n"
        "Требования:\n"
        "- современный, чистый, эмоционально тёплый стиль;\n"
        "- акцент на семье, развитии подростка, доверии;\n"
        "- без узнаваемых реальных лиц, без логотипов чужих брендов, без водяных знаков;\n"
        "- композиция пригодна для performance-рекламы;\n"
        "- в изображении оставить свободную зону под текстовые оверлеи.\n\n"
        f"Сегмент аудитории: {segment}\n"
        f"Cohort: {cohort_name}\n"
        f"ОС аудитории: {os_root}\n"
        f"Источник трафика: {source_hint}\n"
        f"Сигнал сегмента: {short_reason}\n"
        f"Креативный угол: {angle}\n"
        f"Заголовок: {headline}\n"
        f"Текст: {body}\n"
        f"CTA: {cta}\n"
        f"Гипотеза: {why_this}\n"
    )


def _download_binary(url: str, timeout: int = 120) -> bytes:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def _extract_http_error(resp: requests.Response) -> str:
    status = resp.status_code
    message = ""
    code = ""
    err_type = ""
    try:
        payload = resp.json()
        err = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(err, dict):
            message = str(err.get("message") or "").strip()
            code = str(err.get("code") or "").strip()
            err_type = str(err.get("type") or "").strip()
    except Exception:  # noqa: BLE001
        message = ""

    parts = [f"status={status}"]
    if err_type:
        parts.append(f"type={err_type}")
    if code:
        parts.append(f"code={code}")
    if message:
        parts.append(f"message={message}")
    return " | ".join(parts)


def _write_image_bytes(
    *,
    image_bytes: bytes,
    cohort_name: str,
    variant_key: str,
    idx: int,
    extension: str,
) -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    file_name = (
        f"{ts}_{_safe_slug(cohort_name)}_{_safe_slug(variant_key, 'variant')}"
        f"_{idx}_{uid}.{extension}"
    )
    path = OUTPUT_DIR / file_name
    path.write_bytes(image_bytes)
    return {
        "file_name": file_name,
        "file_path": str(path),
        "static_url": f"/static/generated/scoring_banners/{file_name}",
    }


def generate_template_banners(
    *,
    template_item: dict[str, Any],
    variant_key: str | None = None,
    images_per_variant: int = 1,
    model: str | None = None,
    size: str = "1536x1024",
    quality: str = "medium",
    output_format: str = "png",
    timeout: int = 180,
) -> dict[str, Any]:
    provider_requested = _image_provider()

    openai_key_source = "missing"
    openai_token = str(os.getenv("OPENAI_IMAGE_API_KEY", "")).strip()
    if openai_token:
        openai_key_source = "OPENAI_IMAGE_API_KEY"
    else:
        openai_token = str(os.getenv("OPENAI_API_KEY", "")).strip()
        if openai_token:
            openai_key_source = "OPENAI_API_KEY"
        else:
            openai_token = str(os.getenv("OPENAI_KEY", "")).strip()
            if openai_token:
                openai_key_source = "OPENAI_KEY"

    openrouter_key_source = "missing"
    openrouter_token = str(os.getenv("OPENROUTER_API_KEY", "")).strip()
    if openrouter_token:
        openrouter_key_source = "OPENROUTER_API_KEY"
    else:
        openrouter_token = str(os.getenv("OPENAI_API_KEY", "")).strip()
        if openrouter_token:
            openrouter_key_source = "OPENAI_API_KEY"
        else:
            openrouter_token = str(os.getenv("OPENAI_KEY", "")).strip()
            if openrouter_token:
                openrouter_key_source = "OPENAI_KEY"

    if provider_requested == "openai" and not openai_token:
        return {"ok": False, "error": "OPENAI_IMAGE_API_KEY is missing", "generated": [], "failed": []}
    if provider_requested == "openrouter" and not openrouter_token:
        return {"ok": False, "error": "OPENROUTER_API_KEY is missing", "generated": [], "failed": []}
    if provider_requested == "auto" and not openai_token and not openrouter_token:
        return {
            "ok": False,
            "error": "both OPENAI_IMAGE_API_KEY and OPENROUTER_API_KEY are missing",
            "generated": [],
            "failed": [],
        }

    safe_n = max(1, min(int(images_per_variant or 1), 3))
    safe_model = str(model or os.getenv("SCORING_IMAGE_MODEL", "gpt-image-1.5")).strip()
    safe_size = str(size or "1536x1024").strip()
    safe_quality = str(quality or "medium").strip()
    safe_output_format = str(output_format or "png").strip().lower()
    if safe_output_format not in {"png", "jpeg", "webp"}:
        safe_output_format = "png"
    model_candidates = [safe_model]
    if safe_model != "gpt-image-1":
        model_candidates.append("gpt-image-1")

    base_url = _openai_base_url()
    endpoint = f"{base_url}/images/generations"
    openai_headers = {
        "Authorization": f"Bearer {openai_token}",
        "Content-Type": "application/json",
    }

    cohort_name = str(template_item.get("cohort_name") or "cohort")
    segment = str(template_item.get("segment") or "unknown")
    os_root = str(template_item.get("os_root") or "all")
    short_reason = str(template_item.get("short_reason_hint") or "")
    source_hint = str(template_item.get("source_hint") or "unknown")
    variants = template_item.get("variants") or []
    if variant_key:
        variants = [v for v in variants if str(v.get("variant_key") or "") == str(variant_key)]
        if not variants:
            return {
                "ok": False,
                "error": f"variant_key not found: {variant_key}",
                "generated": [],
                "failed": [],
            }

    generated: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    model_used: str | None = None
    provider_used: str | None = None
    key_source_used = "missing"

    for variant in variants:
        v_key = str(variant.get("variant_key") or "variant")
        prompt = _banner_prompt(
            cohort_name=cohort_name,
            segment=segment,
            os_root=os_root,
            variant=variant,
            short_reason=short_reason,
            source_hint=source_hint,
        )
        last_error = ""
        rows: list[dict[str, Any]] = []

        can_try_openai = provider_requested in {"openai", "auto"} and bool(openai_token)
        if can_try_openai:
            for model_name in model_candidates:
                payload = {
                    "model": model_name,
                    "prompt": prompt,
                    "n": safe_n,
                    "size": safe_size,
                    "quality": safe_quality,
                    "output_format": safe_output_format,
                }
                try:
                    resp = requests.post(endpoint, headers=openai_headers, json=payload, timeout=timeout)
                    if resp.status_code >= 400:
                        last_error = f"provider=openai | model={model_name} | {_extract_http_error(resp)}"
                        if model_name != model_candidates[-1]:
                            continue
                        raise requests.HTTPError(last_error)

                    data = resp.json()
                    rows = data.get("data") or []
                    model_used = model_name
                    provider_used = "openai"
                    key_source_used = openai_key_source
                    break
                except Exception as exc:  # noqa: BLE001
                    if not last_error:
                        last_error = f"provider=openai | model={model_name} | {exc}"

        if rows:
            for idx, row in enumerate(rows, start=1):
                img_bytes: bytes | None = None
                if row.get("b64_json"):
                    img_bytes = base64.b64decode(row["b64_json"])
                elif row.get("url"):
                    img_bytes = _download_binary(str(row["url"]), timeout=timeout)

                if not img_bytes:
                    failed.append({"variant_key": v_key, "idx": idx, "error": "no image payload"})
                    continue

                file_meta = _write_image_bytes(
                    image_bytes=img_bytes,
                    cohort_name=cohort_name,
                    variant_key=v_key,
                    idx=idx,
                    extension="jpg" if safe_output_format == "jpeg" else safe_output_format,
                )
                generated.append(
                    {
                        "cohort_name": cohort_name,
                        "segment": segment,
                        "variant_key": v_key,
                        "headline": variant.get("headline"),
                        "cta": variant.get("cta"),
                        "prompt_excerpt": prompt[:280],
                        **file_meta,
                    }
                )
            continue

        should_try_openrouter = provider_requested in {"openrouter", "auto"} and bool(openrouter_token)
        if provider_requested == "auto" and last_error and not _is_region_blocked_error(last_error):
            should_try_openrouter = False

        if should_try_openrouter:
            or_model = _openrouter_model()
            or_images, or_error = _openrouter_image_bytes(
                prompt=prompt,
                model=or_model,
                n=safe_n,
                size=safe_size,
                token=openrouter_token,
                timeout=timeout,
            )
            if or_images:
                provider_used = "openrouter"
                model_used = or_model
                key_source_used = openrouter_key_source
                for idx, image_bytes in enumerate(or_images, start=1):
                    file_meta = _write_image_bytes(
                        image_bytes=image_bytes,
                        cohort_name=cohort_name,
                        variant_key=v_key,
                        idx=idx,
                        extension="jpg" if safe_output_format == "jpeg" else safe_output_format,
                    )
                    generated.append(
                        {
                            "cohort_name": cohort_name,
                            "segment": segment,
                            "variant_key": v_key,
                            "headline": variant.get("headline"),
                            "cta": variant.get("cta"),
                            "prompt_excerpt": prompt[:280],
                            **file_meta,
                        }
                    )
                continue
            if or_error:
                last_error = or_error

        failed.append(
            {
                "variant_key": v_key,
                "error": (
                    f"{last_error or 'image generation failed'} | "
                    "hint=configure OPENROUTER_API_KEY or OPENAI_IMAGE_API_KEY"
                ),
            }
        )

    return {
        "ok": len(generated) > 0,
        "provider_requested": provider_requested,
        "provider_used": provider_used,
        "base_url": base_url,
        "effective_base_url": (_openrouter_base_url() if provider_used == "openrouter" else base_url),
        "auth_key_source": key_source_used,
        "model_requested": safe_model,
        "model_used": model_used,
        "model": safe_model,
        "size": safe_size,
        "quality": safe_quality,
        "output_format": safe_output_format,
        "images_per_variant": safe_n,
        "generated_count": len(generated),
        "failed_count": len(failed),
        "generated": generated,
        "failed": failed,
        "error": (failed[0].get("error") if failed and not generated else None),
    }
