from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()
GEMINI_MODEL = (os.getenv("GEMINI_MODEL") or "gemini-1.5-flash").strip() or "gemini-1.5-flash"
GEMINI_DASHBOARD_CACHE_PATH = os.getenv("GEMINI_DASHBOARD_CACHE_PATH", "uploads/gemini_dashboard_insights_cache.json")
GEMINI_DASHBOARD_CACHE_TTL_SECONDS = int(os.getenv("GEMINI_INSIGHTS_CACHE_TTL_SECONDS", "86400"))
GEMINI_TIMEOUT_SECONDS = float(os.getenv("GEMINI_TIMEOUT_SECONDS", "3.5"))

_CACHE_LOCK = threading.Lock()


def gemini_dashboard_enabled():
    return bool(GEMINI_API_KEY)


def _ensure_cache_path():
    directory = os.path.dirname(os.path.abspath(GEMINI_DASHBOARD_CACHE_PATH))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def _load_cache():
    _ensure_cache_path()
    try:
        with open(GEMINI_DASHBOARD_CACHE_PATH, "r", encoding="utf-8") as cache_file:
            data = json.load(cache_file)
            return {key: value for key, value in (data or {}).items() if isinstance(value, dict)}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_cache(cache):
    _ensure_cache_path()
    try:
        with open(GEMINI_DASHBOARD_CACHE_PATH, "w", encoding="utf-8") as cache_file:
            json.dump(cache, cache_file, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _cache_entry_key(user_id, month, year):
    return f"{user_id}:{month}:{year}"


def get_cached_dashboard_insights(user_id, month, year):
    cache_key = _cache_entry_key(user_id, month, year)
    with _CACHE_LOCK:
        cache = _load_cache()
        entry = cache.get(cache_key)
        if not entry:
            return None
        if entry.get("expires_at", 0) < time.time():
            return None
        return entry


def set_cached_dashboard_insights(user_id, month, year, insights):
    cache_key = _cache_entry_key(user_id, month, year)
    with _CACHE_LOCK:
        cache = _load_cache()
        cache[cache_key] = {
            "insights": insights,
            "generated_at": datetime_isoformat(),
            "expires_at": time.time() + GEMINI_DASHBOARD_CACHE_TTL_SECONDS,
        }
        _save_cache(cache)


def datetime_isoformat():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _normalize_json_text(text):
    cleaned = (text or "").strip()
    if not cleaned:
        return None
    json_match = re.search(r"(\{.*\})", cleaned, re.S)
    if json_match:
        cleaned = json_match.group(1)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def _extract_output_text(response_payload):
    candidates = (response_payload or {}).get("candidates") or []
    for candidate in candidates:
        content = candidate.get("content") or {}
        parts = content.get("parts") or []
        text = "\n".join((part.get("text") or "").strip() for part in parts if part.get("text"))
        if text:
            return text.strip()
    return ""


def _call_gemini(prompt_text):
    if not gemini_dashboard_enabled():
        raise RuntimeError("GEMINI_API_KEY is not configured.")

    endpoint = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{quote(GEMINI_MODEL, safe='')}:generateContent?key={quote(GEMINI_API_KEY, safe='')}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt_text}]}],
        "generationConfig": {
            "temperature": 0.25,
            "maxOutputTokens": 220,
            "responseMimeType": "application/json",
        },
    }
    request = Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=GEMINI_TIMEOUT_SECONDS) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code in {429, 500, 502, 503, 504}:
            raise RuntimeError("Gemini is temporarily unavailable or rate-limited.") from exc
        raise RuntimeError("Gemini request failed.") from exc
    except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("Gemini is temporarily unavailable.") from exc
    return _extract_output_text(response_payload)


def _build_prompt(summary_data):
    lines = [
        "You are a concise financial insights assistant for a personal budgeting dashboard.",
        "Use only the provided summary facts. Do not invent or expose account credentials, tokens, or raw account details.",
        "Return only valid JSON with a single key named \"insights\" and a list of 3 to 5 items.",
        "Each item must contain \"title\" and \"detail\".",
        "Do not wrap the output in markdown or additional text.",
        "",
        f"Month: {summary_data['selected_month']}/{summary_data['selected_year']}",
        f"Monthly income: ${summary_data['monthly_income']:,.2f}",
        f"Monthly expenses: ${summary_data['monthly_expenses']:,.2f}",
        f"Previous month income: ${summary_data['previous_income']:,.2f}",
        f"Previous month expenses: ${summary_data['previous_expenses']:,.2f}",
        f"Expense change: ${summary_data['expense_delta']:,.2f}",
        f"Income change: ${summary_data['income_delta']:,.2f}",
        f"Transfer spending total: ${summary_data['transfer_total']:,.2f}",
        f"Subscription spending total: ${summary_data['subscription_total']:,.2f}",
        f"Needs attention count: {summary_data['needs_attention_count']}",
        "",
        "Top categories:",
    ]
    for category_line in summary_data.get("top_categories", []):
        lines.append(f"- {category_line['category']}: ${category_line['amount']:,.2f}")
    lines.append("")
    lines.append("Top merchants:")
    for merchant_line in summary_data.get("top_merchants", []):
        lines.append(f"- {merchant_line['merchant']}: ${merchant_line['amount']:,.2f}")
    lines.append("")
    lines.append("Return JSON only.")
    return "\n".join(lines)


def generate_gemini_dashboard_insights(summary_data):
    prompt_text = _build_prompt(summary_data)
    raw_output = _call_gemini(prompt_text)
    parsed = _normalize_json_text(raw_output)
    if not parsed or not isinstance(parsed.get("insights"), list):
        raise RuntimeError("AI response did not contain valid insights JSON.")

    insights = []
    for item in parsed.get("insights", [])[:5]:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or "").strip()
        detail = (item.get("detail") or "").strip()
        if title and detail:
            insights.append({"title": title, "detail": detail})
    if not insights:
        raise RuntimeError("AI returned no valid insights.")
    return insights
