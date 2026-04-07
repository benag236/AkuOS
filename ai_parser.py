from __future__ import annotations

import json
import os
from urllib import error, request

from transaction_normalizer import normalize_row_payload, parse_date_any, safe_float


DEFAULT_AI_MODEL = os.getenv("AI_IMPORT_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
DEFAULT_AI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")


def ai_parser_enabled():
    return bool((os.getenv("OPENAI_API_KEY") or "").strip())


def chunk_text_for_ai(raw_text, max_chars=12000):
    text = (raw_text or "").strip()
    if not text:
        return []
    lines = text.splitlines()
    chunks = []
    current = []
    current_len = 0
    for line in lines:
        line_len = len(line) + 1
        if current and current_len + line_len > max_chars:
            chunks.append("\n".join(current))
            current = [line]
            current_len = line_len
        else:
            current.append(line)
            current_len += line_len
    if current:
        chunks.append("\n".join(current))
    return chunks


def build_ai_prompt(cleaned_text, source_document, bank_hint=""):
    return f"""
You are extracting bank or credit card transactions from a statement.

Return strict JSON only with this shape:
{{
  "transactions": [
    {{
      "date": "YYYY-MM-DD",
      "post_date": "",
      "raw_description": "",
      "display_name": "",
      "amount": -12.34,
      "source_category": "",
      "transaction_type": "Income|Expense|Bills/Payments|Cash Withdrawal"
    }}
  ],
  "warnings": []
}}

Rules:
- Only extract real transaction rows from statement tables.
- Ignore summaries, balances, rewards, legal text, help text, addresses, payment instructions, and footers.
- If a row is a payment/credit card payment/transfer, mark transaction_type as Bills/Payments and source_category as Transfer.
- If unsure, exclude the row instead of guessing.
- Preserve raw_description from the statement text.
- display_name should be a short readable merchant/source name.
- amounts must be numbers, expenses negative, income positive.

Statement source: {source_document}
Bank hint: {bank_hint or "unknown"}

Statement text:
{cleaned_text}
""".strip()


def call_openai_json(prompt, model=None):
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured.")

    endpoint = f"{DEFAULT_AI_BASE_URL}/chat/completions"
    payload = {
        "model": model or DEFAULT_AI_MODEL,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
    }
    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=60) as response:
        data = json.loads(response.read().decode("utf-8"))
    content = data["choices"][0]["message"]["content"]
    return json.loads(content)


def parse_statement_with_ai(raw_text, source_document, bank_hint=""):
    if not ai_parser_enabled():
        return {
            "rows": [],
            "warnings": ["AI parser unavailable because OPENAI_API_KEY is not configured."],
            "error": "ai_unavailable",
            "model": None,
        }

    chunks = chunk_text_for_ai(raw_text)
    if not chunks:
        return {
            "rows": [],
            "warnings": ["AI parser did not receive any text to parse."],
            "error": "empty_text",
            "model": DEFAULT_AI_MODEL,
        }

    rows = []
    warnings = []
    for chunk_index, chunk in enumerate(chunks, start=1):
        try:
            response = call_openai_json(build_ai_prompt(chunk, source_document, bank_hint=bank_hint))
        except (RuntimeError, error.URLError, error.HTTPError, json.JSONDecodeError, KeyError, ValueError) as exc:
            return {
                "rows": [],
                "warnings": [f"AI parser failed: {exc}"],
                "error": "ai_request_failed",
                "model": DEFAULT_AI_MODEL,
            }

        for item in response.get("transactions", []):
            parsed_date = parse_date_any(item.get("date"))
            post_date = parse_date_any(item.get("post_date"))
            amount = safe_float(item.get("amount"))
            raw_description = (item.get("raw_description") or "").strip()
            if parsed_date is None or amount is None or not raw_description:
                continue
            rows.append(
                normalize_row_payload(
                    source_document=source_document,
                    raw_source=raw_description,
                    parsed_date=parsed_date,
                    post_date=post_date,
                    raw_description=raw_description,
                    amount=amount,
                    source_category=(item.get("source_category") or "").strip(),
                    raw_category=(item.get("source_category") or "").strip(),
                    transaction_type=(item.get("transaction_type") or "").strip(),
                    parser_label=f"AI parser · chunk {chunk_index}",
                    parser_source="ai",
                    parser_confidence=0.62,
                    parser_warnings=response.get("warnings", []),
                    display_name=(item.get("display_name") or "").strip(),
                )
            )
        warnings.extend(response.get("warnings", []))

    return {
        "rows": rows,
        "warnings": warnings[:10],
        "error": "",
        "model": DEFAULT_AI_MODEL,
    }
