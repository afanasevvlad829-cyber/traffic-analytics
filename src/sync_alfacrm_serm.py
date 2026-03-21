import json
import os
import shlex
import subprocess
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

from src.load_alfacrm_crm_xlsx import run as load_alfacrm_crm_xlsx


def _tail_lines(text: str, max_lines: int = 20) -> str:
    lines = [ln for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


def _extract_xlsx_path_from_stdout(stdout: str) -> str:
    lines = [ln.strip() for ln in (stdout or "").splitlines() if ln.strip()]
    for line in reversed(lines):
        if line.startswith("{") and line.endswith("}"):
            try:
                payload = json.loads(line)
            except Exception:
                continue
            for key in ("xlsx_path", "output_xlsx", "file", "path"):
                val = payload.get(key)
                if val:
                    return str(val).strip()
    return ""


def sync_alfacrm_from_serm(
    report_date: str | None = None,
    updates_only: bool = True,
    include_communications: bool = True,
    include_lessons: bool = False,
    include_extra: bool = False,
    timeout_sec: int = 1800,
) -> dict[str, Any]:
    cmd_text = (os.getenv("ALFACRM_DIRECT_SYNC_CMD", "") or "").strip()
    if not cmd_text:
        raise RuntimeError(
            "ALFACRM_DIRECT_SYNC_CMD is not configured; set export/sync command for direct SERM ingestion"
        )

    effective_date = (report_date or date.today().isoformat()).strip()
    notes: list[str] = []
    if include_lessons:
        notes.append("include_lessons=true requested; current DB ingest stores clients+communications only")
    if include_extra:
        notes.append("include_extra=true requested; current DB ingest stores clients+communications only")

    with tempfile.TemporaryDirectory(prefix="alfacrm_serm_sync_") as tmp_dir:
        suggested_xlsx = Path(tmp_dir) / f"alfacrm_serm_{effective_date}.xlsx"
        env = os.environ.copy()
        env["ALFACRM_SYNC_REPORT_DATE"] = effective_date
        env["ALFACRM_SYNC_OUTPUT_XLSX"] = str(suggested_xlsx)
        env["ALFACRM_SYNC_UPDATES_ONLY"] = "1" if updates_only else "0"
        env["ALFACRM_SYNC_INCLUDE_COMMUNICATIONS"] = "1" if include_communications else "0"
        env["ALFACRM_SYNC_INCLUDE_LESSONS"] = "1" if include_lessons else "0"
        env["ALFACRM_SYNC_INCLUDE_EXTRA"] = "1" if include_extra else "0"

        proc = subprocess.run(
            shlex.split(cmd_text),
            capture_output=True,
            text=True,
            timeout=max(60, int(timeout_sec)),
            check=False,
            env=env,
        )
        stdout_tail = _tail_lines(proc.stdout, 20)
        stderr_tail = _tail_lines(proc.stderr, 20)

        if proc.returncode != 0:
            raise RuntimeError(
                f"direct SERM sync command failed (exit={proc.returncode})"
                f"\nstdout_tail:\n{stdout_tail}\nstderr_tail:\n{stderr_tail}"
            )

        xlsx_path = suggested_xlsx
        if not xlsx_path.exists():
            extracted = _extract_xlsx_path_from_stdout(proc.stdout)
            if extracted:
                xlsx_path = Path(extracted).expanduser().resolve()
        if not xlsx_path.exists():
            raise RuntimeError(
                "direct SERM sync finished but XLSX output not found; "
                "set ALFACRM_SYNC_OUTPUT_XLSX support in exporter command or print xlsx_path in JSON"
            )

        load_result = load_alfacrm_crm_xlsx(
            xlsx_path=str(xlsx_path),
            report_date=effective_date,
            source_file=f"serm_sync:{xlsx_path.name}",
            skip_communications=not include_communications,
            schema_sql_path="",
            load_note="sync_alfacrm_serm_direct",
        )

        return {
            "ok": True,
            "source": "serm_direct",
            "report_date": effective_date,
            "updates_only": bool(updates_only),
            "include_communications": bool(include_communications),
            "include_lessons": bool(include_lessons),
            "include_extra": bool(include_extra),
            "command": cmd_text,
            "command_exit_code": proc.returncode,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "xlsx_path": str(xlsx_path),
            "load_result": load_result,
            "notes": notes,
        }

