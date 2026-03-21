from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _openrouter_base_url() -> str:
    base = str(os.getenv("OPENROUTER_BASE_URL", "")).strip()
    if base:
        return base.rstrip("/")
    fallback = str(os.getenv("OPENAI_BASE_URL", "")).strip()
    if fallback:
        return fallback.rstrip("/")
    return "https://openrouter.ai/api/v1"


def _openrouter_token() -> str:
    token = str(os.getenv("OPENROUTER_API_KEY", "")).strip()
    if token:
        return token
    token = str(os.getenv("OPENAI_API_KEY", "")).strip()
    if token:
        return token
    token = str(os.getenv("OPENAI_KEY", "")).strip()
    if token:
        return token
    return ""


def _openrouter_audit_model(model: str | None = None) -> str:
    if model and str(model).strip():
        return str(model).strip()
    configured = str(os.getenv("OPENROUTER_AUDIT_MODEL", "")).strip()
    if configured:
        return configured
    fallback = str(os.getenv("OPENAI_MODEL", "")).strip()
    if fallback:
        return fallback
    return "openrouter/auto"


def _headers(token: str) -> dict[str, str]:
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    referer = str(os.getenv("OPENROUTER_HTTP_REFERER", "") or os.getenv("APP_BASE_URL", "")).strip()
    if referer:
        headers["HTTP-Referer"] = referer
    title = str(os.getenv("OPENROUTER_APP_TITLE", "Traffic Analytics Audits")).strip()
    if title:
        headers["X-Title"] = title
    return headers


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
    except Exception:
        message = ""

    parts = [f"status={status}"]
    if err_type:
        parts.append(f"type={err_type}")
    if code:
        parts.append(f"code={code}")
    if message:
        parts.append(f"message={message}")
    return " | ".join(parts)


@dataclass
class CallOutcome:
    ok: bool
    data: dict[str, Any] | None
    raw_response: dict[str, Any] | None
    attempt_count: int
    latency_ms: int | None
    last_attempt_at: str
    last_error: str | None
    error_class: str | None
    retryable: bool
    transport_status_code: int | None
    external_status: str
    attempt_log: list[dict[str, Any]]


def _classify_error(*, status_code: int | None, error: Exception | None, parse_error: bool = False, schema_error: bool = False) -> tuple[str, bool]:
    if schema_error:
        return "schema", False
    if parse_error:
        return "parsing", False
    if status_code in (401, 403):
        return "auth", False
    if status_code == 429:
        return "rate_limit", True
    if status_code is not None and 500 <= status_code <= 599:
        return "network", True
    if status_code is not None and status_code >= 400:
        return "request", False
    if isinstance(error, requests.Timeout):
        return "timeout", True
    if isinstance(error, requests.ConnectionError):
        return "network", True
    if error is not None:
        return "network", True
    return "unknown", False


def _extract_json_from_content(content: Any) -> dict[str, Any]:
    if isinstance(content, dict):
        return content
    if isinstance(content, list):
        chunks: list[str] = []
        for part in content:
            if isinstance(part, str):
                chunks.append(part)
                continue
            if isinstance(part, dict):
                text = part.get("text")
                if text is not None:
                    chunks.append(str(text))
        raw = "\n".join(chunks).strip()
        if not raw:
            raise ValueError("response content list does not contain text")
    else:
        raw = str(content or "").strip()

    if not raw:
        raise ValueError("empty response content")

    # Support fenced JSON.
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw, flags=re.IGNORECASE).strip()
        raw = re.sub(r"```$", "", raw).strip()

    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise ValueError(f"json parse failed: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError("parsed response must be object")
    return payload


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def normalize_and_validate_audit_response(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        raise ValueError("audit response is empty")

    scores = payload.get("scores")
    if not isinstance(scores, dict):
        scores = {}

    overall = _to_float(payload.get("overall_score"))
    if overall is None:
        overall = _to_float(scores.get("overall"))

    architecture = _to_float(payload.get("architecture_score"))
    if architecture is None:
        architecture = _to_float(scores.get("architecture"))

    code_hygiene = _to_float(payload.get("code_hygiene_score"))
    if code_hygiene is None:
        code_hygiene = _to_float(scores.get("code_hygiene"))

    scalability = _to_float(payload.get("scalability_score"))
    if scalability is None:
        scalability = _to_float(scores.get("scalability"))

    production = _to_float(payload.get("production_readiness_score"))
    if production is None:
        production = _to_float(scores.get("production_readiness"))

    verdict = str(payload.get("verdict") or "").strip()
    if not verdict:
        raise ValueError("verdict is required")

    can_proceed_raw = payload.get("can_proceed")
    if isinstance(can_proceed_raw, bool):
        can_proceed = can_proceed_raw
    elif isinstance(can_proceed_raw, str):
        can_proceed = can_proceed_raw.strip().lower() in {"1", "true", "yes", "y"}
    elif can_proceed_raw is None:
        can_proceed = False
    else:
        raise ValueError("can_proceed must be bool")

    top_risks = payload.get("top_risks") or payload.get("top_risks_json") or []
    required_fixes = payload.get("required_fixes") or payload.get("required_fixes_json") or []
    strengths = payload.get("strengths") or payload.get("strengths_json") or []
    changed_modules = payload.get("changed_modules") or payload.get("changed_modules_json") or []

    if not isinstance(top_risks, list):
        raise ValueError("top_risks must be list")
    if not isinstance(required_fixes, list):
        raise ValueError("required_fixes must be list")
    if not isinstance(strengths, list):
        raise ValueError("strengths must be list")
    if not isinstance(changed_modules, list):
        raise ValueError("changed_modules must be list")

    if overall is None and not (architecture or code_hygiene or scalability or production):
        raise ValueError("scores are missing")

    return {
        "overall_score": overall,
        "architecture_score": architecture,
        "code_hygiene_score": code_hygiene,
        "scalability_score": scalability,
        "production_readiness_score": production,
        "verdict": verdict,
        "can_proceed": bool(can_proceed),
        "top_risks_json": top_risks,
        "required_fixes_json": required_fixes,
        "strengths_json": strengths,
        "changed_modules_json": changed_modules,
        "report_markdown": str(payload.get("report_markdown") or payload.get("summary_markdown") or "").strip(),
        "response_json": payload,
    }


def call_openrouter_audit(
    *,
    prompt: str,
    model: str | None = None,
    timeout_sec: int = 45,
    max_retries: int = 3,
    backoff_sec: float = 1.5,
) -> CallOutcome:
    token = _openrouter_token()
    if not token:
        now = _utc_now_iso()
        return CallOutcome(
            ok=False,
            data=None,
            raw_response=None,
            attempt_count=0,
            latency_ms=0,
            last_attempt_at=now,
            last_error="OPENROUTER_API_KEY/OPENAI_API_KEY is missing",
            error_class="auth",
            retryable=False,
            transport_status_code=None,
            external_status="error",
            attempt_log=[
                {
                    "attempt": 0,
                    "at": now,
                    "status_code": None,
                    "latency_ms": 0,
                    "error_class": "auth",
                    "retryable": False,
                    "error": "OPENROUTER_API_KEY/OPENAI_API_KEY is missing",
                }
            ],
        )

    endpoint = f"{_openrouter_base_url()}/chat/completions"
    chosen_model = _openrouter_audit_model(model=model)

    attempt = 0
    last_error = ""
    last_class = "unknown"
    last_retryable = False
    latency_ms: int | None = None
    transport_status_code: int | None = None
    last_attempt_at = _utc_now_iso()
    attempt_log: list[dict[str, Any]] = []

    while attempt < max(1, max_retries):
        attempt += 1
        started = time.perf_counter()
        last_attempt_at = _utc_now_iso()
        try:
            payload = {
                "model": chosen_model,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Return STRICT JSON object only with keys: "
                            "scores, overall_score, architecture_score, code_hygiene_score, scalability_score, "
                            "production_readiness_score, verdict, can_proceed, top_risks, required_fixes, strengths, changed_modules, report_markdown."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0,
                "response_format": {"type": "json_object"},
            }
            response = requests.post(
                endpoint,
                headers=_headers(token),
                json=payload,
                timeout=max(5, int(timeout_sec)),
            )
            latency_ms = int((time.perf_counter() - started) * 1000)
            transport_status_code = int(response.status_code)
            if response.status_code >= 400:
                last_error = _extract_http_error(response)
                last_class, last_retryable = _classify_error(status_code=response.status_code, error=None)
                attempt_log.append(
                    {
                        "attempt": attempt,
                        "at": last_attempt_at,
                        "status_code": transport_status_code,
                        "latency_ms": latency_ms,
                        "error_class": last_class,
                        "retryable": bool(last_retryable),
                        "error": last_error,
                    }
                )
                if last_retryable and attempt < max_retries:
                    time.sleep(backoff_sec * (2 ** (attempt - 1)))
                    continue
                return CallOutcome(
                    ok=False,
                    data=None,
                    raw_response=None,
                    attempt_count=attempt,
                    latency_ms=latency_ms,
                    last_attempt_at=last_attempt_at,
                    last_error=last_error,
                    error_class=last_class,
                    retryable=last_retryable,
                    transport_status_code=transport_status_code,
                    external_status="error",
                    attempt_log=attempt_log,
                )

            body = response.json() if response.content else {}
            choices = body.get("choices") if isinstance(body, dict) else None
            message = (choices[0] or {}).get("message") if isinstance(choices, list) and choices else {}
            content = (message or {}).get("content")
            parsed = _extract_json_from_content(content)
            normalized = normalize_and_validate_audit_response(parsed)
            attempt_log.append(
                {
                    "attempt": attempt,
                    "at": last_attempt_at,
                    "status_code": transport_status_code,
                    "latency_ms": latency_ms,
                    "error_class": None,
                    "retryable": False,
                    "error": None,
                }
            )
            return CallOutcome(
                ok=True,
                data=normalized,
                raw_response=body if isinstance(body, dict) else {"raw": body},
                attempt_count=attempt,
                latency_ms=latency_ms,
                last_attempt_at=last_attempt_at,
                last_error=None,
                error_class=None,
                retryable=False,
                transport_status_code=transport_status_code,
                external_status="ok",
                attempt_log=attempt_log,
            )
        except requests.Timeout as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            last_error = str(exc)
            last_class, last_retryable = _classify_error(status_code=None, error=exc)
            attempt_log.append(
                {
                    "attempt": attempt,
                    "at": last_attempt_at,
                    "status_code": None,
                    "latency_ms": latency_ms,
                    "error_class": last_class,
                    "retryable": bool(last_retryable),
                    "error": last_error,
                }
            )
            if attempt < max_retries:
                time.sleep(backoff_sec * (2 ** (attempt - 1)))
                continue
            return CallOutcome(
                ok=False,
                data=None,
                raw_response=None,
                attempt_count=attempt,
                latency_ms=latency_ms,
                last_attempt_at=last_attempt_at,
                last_error=last_error,
                error_class=last_class,
                retryable=last_retryable,
                transport_status_code=transport_status_code,
                external_status="error",
                attempt_log=attempt_log,
            )
        except requests.RequestException as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            last_error = str(exc)
            last_class, last_retryable = _classify_error(status_code=None, error=exc)
            attempt_log.append(
                {
                    "attempt": attempt,
                    "at": last_attempt_at,
                    "status_code": None,
                    "latency_ms": latency_ms,
                    "error_class": last_class,
                    "retryable": bool(last_retryable),
                    "error": last_error,
                }
            )
            if last_retryable and attempt < max_retries:
                time.sleep(backoff_sec * (2 ** (attempt - 1)))
                continue
            return CallOutcome(
                ok=False,
                data=None,
                raw_response=None,
                attempt_count=attempt,
                latency_ms=latency_ms,
                last_attempt_at=last_attempt_at,
                last_error=last_error,
                error_class=last_class,
                retryable=last_retryable,
                transport_status_code=transport_status_code,
                external_status="error",
                attempt_log=attempt_log,
            )
        except ValueError as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            last_error = str(exc)
            parse_or_schema = "schema" if "required" in last_error.lower() or "must be" in last_error.lower() else "parsing"
            last_class, last_retryable = _classify_error(
                status_code=None,
                error=None,
                parse_error=parse_or_schema == "parsing",
                schema_error=parse_or_schema == "schema",
            )
            attempt_log.append(
                {
                    "attempt": attempt,
                    "at": last_attempt_at,
                    "status_code": transport_status_code,
                    "latency_ms": latency_ms,
                    "error_class": last_class,
                    "retryable": bool(last_retryable),
                    "error": last_error,
                }
            )
            return CallOutcome(
                ok=False,
                data=None,
                raw_response=None,
                attempt_count=attempt,
                latency_ms=latency_ms,
                last_attempt_at=last_attempt_at,
                last_error=last_error,
                error_class=last_class,
                retryable=last_retryable,
                transport_status_code=transport_status_code,
                external_status="error",
                attempt_log=attempt_log,
            )

    return CallOutcome(
        ok=False,
        data=None,
        raw_response=None,
        attempt_count=attempt,
        latency_ms=latency_ms,
        last_attempt_at=last_attempt_at,
        last_error=last_error or "openrouter call failed",
        error_class=last_class,
        retryable=last_retryable,
        transport_status_code=transport_status_code,
        external_status="error",
        attempt_log=attempt_log,
    )


def openrouter_health(timeout_sec: int = 15) -> dict[str, Any]:
    token = _openrouter_token()
    started = time.perf_counter()
    checked_at = _utc_now_iso()

    if not token:
        return {
            "ok": False,
            "checked_at": checked_at,
            "latency_ms": 0,
            "error_class": "auth",
            "error": "OPENROUTER_API_KEY/OPENAI_API_KEY is missing",
            "retryable": False,
        }

    endpoint = f"{_openrouter_base_url()}/models"
    try:
        resp = requests.get(endpoint, headers=_headers(token), timeout=max(3, int(timeout_sec)))
        latency_ms = int((time.perf_counter() - started) * 1000)
        if resp.status_code >= 400:
            cls, retryable = _classify_error(status_code=resp.status_code, error=None)
            return {
                "ok": False,
                "checked_at": checked_at,
                "latency_ms": latency_ms,
                "error_class": cls,
                "error": _extract_http_error(resp),
                "retryable": retryable,
            }
        return {
            "ok": True,
            "checked_at": checked_at,
            "latency_ms": latency_ms,
            "error_class": None,
            "error": None,
            "retryable": False,
        }
    except requests.Timeout as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return {
            "ok": False,
            "checked_at": checked_at,
            "latency_ms": latency_ms,
            "error_class": "timeout",
            "error": str(exc),
            "retryable": True,
        }
    except requests.RequestException as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return {
            "ok": False,
            "checked_at": checked_at,
            "latency_ms": latency_ms,
            "error_class": "network",
            "error": str(exc),
            "retryable": True,
        }
