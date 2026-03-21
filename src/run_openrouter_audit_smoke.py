from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable

import requests

import webapp.audit_openrouter as client


@dataclass
class FakeResponse:
    status_code: int
    payload: dict[str, Any] | None = None

    @property
    def content(self) -> bytes:
        return b"{}"

    def json(self) -> dict[str, Any]:
        return self.payload or {}


@contextmanager
def patched_post(handler: Callable[..., Any]):
    original = client.requests.post
    client.requests.post = handler  # type: ignore[assignment]
    try:
        yield
    finally:
        client.requests.post = original  # type: ignore[assignment]


@contextmanager
def patched_env(key: str, value: str | None):
    prev = os.getenv(key)
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = prev


def _success_post(*args: Any, **kwargs: Any) -> FakeResponse:
    payload = {
        "choices": [
            {
                "message": {
                    "content": json.dumps(
                        {
                            "scores": {
                                "overall": 0.81,
                                "architecture": 0.8,
                                "code_hygiene": 0.79,
                                "scalability": 0.76,
                                "production_readiness": 0.77,
                            },
                            "verdict": "approved",
                            "can_proceed": True,
                            "top_risks": [{"risk": "none"}],
                            "required_fixes": [],
                            "strengths": [{"item": "good"}],
                            "changed_modules": ["webapp/app.py"],
                            "report_markdown": "ok",
                        }
                    )
                }
            }
        ]
    }
    return FakeResponse(status_code=200, payload=payload)


def _timeout_post(*args: Any, **kwargs: Any) -> FakeResponse:
    raise requests.Timeout("simulated timeout")


def _invalid_key_post(*args: Any, **kwargs: Any) -> FakeResponse:
    return FakeResponse(
        status_code=401,
        payload={"error": {"type": "authentication_error", "code": "invalid_api_key", "message": "bad key"}},
    )


def _malformed_post(*args: Any, **kwargs: Any) -> FakeResponse:
    payload = {"choices": [{"message": {"content": "not-json"}}]}
    return FakeResponse(status_code=200, payload=payload)


def run_smoke() -> dict[str, Any]:
    report: dict[str, Any] = {"ok": True, "cases": []}

    with patched_env("OPENROUTER_API_KEY", "smoke-test-key"):
        with patched_post(_success_post):
            outcome = client.call_openrouter_audit(prompt="smoke", max_retries=2, timeout_sec=5)
            passed = outcome.ok and outcome.attempt_count == 1 and outcome.data is not None
            report["cases"].append(
                {
                    "name": "success_case",
                    "passed": passed,
                    "ok": outcome.ok,
                    "attempt_count": outcome.attempt_count,
                    "error_class": outcome.error_class,
                }
            )

        with patched_post(_timeout_post):
            outcome = client.call_openrouter_audit(prompt="smoke", max_retries=2, timeout_sec=1, backoff_sec=0.01)
            passed = (not outcome.ok) and outcome.error_class == "timeout" and outcome.attempt_count == 2
            report["cases"].append(
                {
                    "name": "timeout_case",
                    "passed": passed,
                    "ok": outcome.ok,
                    "attempt_count": outcome.attempt_count,
                    "error_class": outcome.error_class,
                    "retryable": outcome.retryable,
                }
            )

        with patched_post(_invalid_key_post):
            outcome = client.call_openrouter_audit(prompt="smoke", max_retries=2, timeout_sec=5)
            passed = (not outcome.ok) and outcome.error_class == "auth" and outcome.attempt_count == 1
            report["cases"].append(
                {
                    "name": "invalid_api_key_case",
                    "passed": passed,
                    "ok": outcome.ok,
                    "attempt_count": outcome.attempt_count,
                    "error_class": outcome.error_class,
                    "retryable": outcome.retryable,
                }
            )

        with patched_post(_malformed_post):
            outcome = client.call_openrouter_audit(prompt="smoke", max_retries=2, timeout_sec=5)
            passed = (not outcome.ok) and outcome.error_class in {"parsing", "schema"}
            report["cases"].append(
                {
                    "name": "malformed_response_case",
                    "passed": passed,
                    "ok": outcome.ok,
                    "attempt_count": outcome.attempt_count,
                    "error_class": outcome.error_class,
                }
            )

    report["ok"] = all(bool(x.get("passed")) for x in report["cases"])
    return report


def main() -> None:
    print(json.dumps(run_smoke(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
