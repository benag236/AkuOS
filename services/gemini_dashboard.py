from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time

try:
    from google.generativeai import Client
    GOOGLE_GENAI_AVAILABLE = True
except ImportError:
    Client = None
    GOOGLE_GENAI_AVAILABLE = False

GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()
GEMINI_MODEL = (os.getenv("GEMINI_MODEL") or "gemini-1.0-mini").strip() or "gemini-1.0-mini"
GEMINI_DASHBOARD_CACHE_PATH = os.getenv("GEMINI_DASHBOARD_CACHE_PATH", "uploads/gemini_dashboard_insights_cache.json")
GEMINI_DASHBOARD_CACHE_TTL_SECONDS = int(os.getenv("GEMINI_INSIGHTS_CACHE_TTL_SECONDS", "86400"))

_CACHE_LOCK = threading.Lock()


def gemini_dashboard_enabled():
    return bool(GEMINI_API_KEY and GOOGLE_GENAI_AVAILABLE)


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


def _extract_output_text(response):
    if hasattr(response, "output_text") and isinstance(response.output_text, str):
        return response.output_text.strip()
    if hasattr(response, "candidates") and response.candidates:
        candidate = response.candidates[0]
        if hasattr(candidate, "content") and isinstance(candidate.content, str):
            return candidate.content.strip()
        if hasattr(candidate, "output") and isinstance(candidate.output, str):
            return candidate.output.strip()
    if hasattr(response, "message") and isinstance(response.message, dict):
        return (response.message.get("content") or "").strip()
    return ""


def _call_gemini(prompt_text):
    if not gemini_dashboard_enabled():
        raise RuntimeError("GEMINI_API_KEY is not configured or google-genai is unavailable.")

    client = Client(api_key=GEMINI_API_KEY)
    response = client.responses.create(
        model=GEMINI_MODEL,
        input=prompt_text,
        temperature=0.25,
        max_output_tokens=220,
    )
    return _extract_output_text(response)


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
