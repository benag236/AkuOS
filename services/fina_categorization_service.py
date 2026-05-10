"""Optional Fina transaction categorization fallback.

This module is intentionally isolated from Flask and the database. AkuOS should
keep categorizing locally when Fina credentials are missing, rate-limited, or
temporarily unavailable.
"""

from __future__ import annotations

import json
import os
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


FINA_API_URL = os.getenv("FINA_API_URL", "https://app.fina.money/api/resource/categorize")
FINA_API_KEY = (os.getenv("FINA_API_KEY") or "").strip()
FINA_PARTNER_ID = (os.getenv("FINA_PARTNER_ID") or "").strip()
FINA_API_MODEL = (os.getenv("FINA_API_MODEL") or "v2").strip() or "v2"
FINA_TIMEOUT_SECONDS = float(os.getenv("FINA_TIMEOUT_SECONDS", "2.5"))
FINA_MAX_RETRIES = int(os.getenv("FINA_MAX_RETRIES", "1"))
FINA_FAILURE_COOLDOWN_SECONDS = int(os.getenv("FINA_FAILURE_COOLDOWN_SECONDS", "120"))
_UNAVAILABLE_UNTIL = 0.0


NOISY_MERCHANT_PREFIX_RE = re.compile(
    r"^(?:"
    r"visa|dda|pos|debit|purchase|pur(?:\s+ap)?|ach|card|checkcard|sq|tst\*?|paypal|"
    r"online|payment|auth(?:orization)?|ppd|ccd|withdraw(?:al)?|wthdrl|ap"
    r")\b[\s\*\-:#]*",
    re.I,
)
NUMERIC_REFERENCE_RE = re.compile(r"\b(?:\d{4,}|[a-z]*\d+[a-z0-9]*)\b", re.I)
PHONE_RE = re.compile(r"\b1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")

MERCHANT_ALIASES = {
    "tesla supercharger": "Tesla Supercharger",
    "mcdonalds": "McDonald's",
    "mc donalds": "McDonald's",
    "paypal uber": "Uber",
    "uber": "Uber",
    "wal mart": "Walmart",
    "walmart": "Walmart",
    "7eleven": "7-Eleven",
    "7 eleven": "7-Eleven",
    "amazon": "Amazon",
    "amzn": "Amazon",
    "cash app": "Cash App",
    "cashapp": "Cash App",
    "venmo": "Venmo",
    "zelle": "Zelle",
    "wealthfront": "Wealthfront",
}

FINA_CATEGORY_MAP = {
    "transportation.gas_and_charging": ("Transportation", "Gas & Charging"),
    "transportation.gas": ("Car Related", "Gas"),
    "transportation.ev_charging": ("Car Related", "Charging"),
    "loan_payments.credit_card_payment": ("Transfers", "Credit Card Payment"),
    "food_and_drink.restaurant": ("Food", "Restaurants"),
    "food_and_drink.restaurants": ("Food", "Restaurants"),
    "food_and_drink.coffee": ("Food", "Coffee"),
    "food_and_drink.groceries": ("Groceries", ""),
    "shopping.general": ("Shopping", ""),
    "shopping.online": ("Shopping", ""),
    "transfer.money_transfer": ("Transfers", "Money Transfer"),
    "transfers.money_transfer": ("Transfers", "Money Transfer"),
    "transfer.internal_transfer": ("Transfers", "Internal Transfer"),
    "transfers.internal_transfer": ("Transfers", "Internal Transfer"),
    "income.paycheck": ("Income", "Salary"),
    "income.salary": ("Income", "Salary"),
    "cash.atm": ("Cash", "ATM Withdrawal"),
    "cash.atm_withdrawal": ("Cash", "ATM Withdrawal"),
}


def fina_configured():
    return bool(FINA_API_KEY and FINA_PARTNER_ID) and time.time() >= _UNAVAILABLE_UNTIL


def normalize_fina_merchant(description, merchant_guess=""):
    value = f"{merchant_guess or ''} {description or ''}".strip()
    value = PHONE_RE.sub(" ", value)
    value = re.sub(r"[^a-zA-Z0-9&'*\s-]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    for _ in range(8):
        cleaned = NOISY_MERCHANT_PREFIX_RE.sub("", value).strip()
        if cleaned == value:
            break
        value = cleaned
    value = NUMERIC_REFERENCE_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value).strip(" -*")
    lowered = value.lower()
    for alias, label in MERCHANT_ALIASES.items():
        if alias in lowered:
            return label
    if not value:
        return ""
    return " ".join(part.upper() if len(part) <= 3 and part.isalpha() else part.capitalize() for part in value.split()[:4])


def fina_category_to_akuos(category_value, subcategory_value=""):
    category_key = re.sub(r"[^a-z0-9]+", "_", (category_value or "").strip().lower()).strip("_")
    subcategory_key = re.sub(r"[^a-z0-9]+", "_", (subcategory_value or "").strip().lower()).strip("_")
    combined_key = f"{category_key}.{subcategory_key}".strip(".")
    for key in (combined_key, category_key):
        if key in FINA_CATEGORY_MAP:
            return FINA_CATEGORY_MAP[key]
    return ("Needs Review", "")


def _extract_response_rows(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("data", "transactions", "results", "items", "categorized"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return [payload]


def _confidence_from_row(row):
    for key in ("confidence", "confidence_score", "score", "probability"):
        value = row.get(key)
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        return number / 100 if number > 1 else number
    label = str(row.get("confidence_label") or row.get("confidenceLevel") or "").strip().lower()
    if label in {"high", "very_high"}:
        return 0.9
    if label in {"medium", "moderate"}:
        return 0.76
    if label:
        return 0.6
    return 0.0


def _normalize_fina_row(row, original):
    if not isinstance(row, dict):
        return None
    category_value = (
        row.get("category")
        or row.get("category_slug")
        or row.get("categoryId")
        or row.get("primary_category")
        or ""
    )
    subcategory_value = (
        row.get("subcategory")
        or row.get("sub_category")
        or row.get("subcategory_slug")
        or row.get("secondary_category")
        or ""
    )
    category, subcategory = fina_category_to_akuos(category_value, subcategory_value)
    merchant = (
        row.get("merchant")
        or row.get("merchant_name")
        or row.get("normalized_merchant")
        or row.get("cleaned_merchant")
        or ""
    )
    normalized_merchant = normalize_fina_merchant(original.get("name", ""), merchant or original.get("merchant", ""))
    confidence = _confidence_from_row(row)
    return {
        "category": category,
        "subcategory": subcategory,
        "confidence": round(confidence, 3),
        "raw_category": category_value,
        "raw_subcategory": subcategory_value,
        "merchant": normalized_merchant,
        "source_payload": {
            key: row.get(key)
            for key in ("category", "subcategory", "merchant", "confidence", "score")
            if key in row
        },
    }


def categorize_batch(transactions, logger=None):
    global _UNAVAILABLE_UNTIL
    if not fina_configured() or not transactions:
        return []
    payload = [
        {
            "name": (tx.get("name") or tx.get("raw_description") or "").strip(),
            "merchant": normalize_fina_merchant(tx.get("name") or tx.get("raw_description"), tx.get("merchant") or ""),
            "amount": float(tx.get("amount") or 0),
        }
        for tx in transactions
    ]
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "x-api-key": FINA_API_KEY,
        "x-partner-id": FINA_PARTNER_ID,
        "x-api-model": FINA_API_MODEL or "v2",
    }
    last_error = None
    for attempt in range(FINA_MAX_RETRIES + 1):
        try:
            request = Request(FINA_API_URL, data=body, headers=headers, method="POST")
            with urlopen(request, timeout=FINA_TIMEOUT_SECONDS) as response:
                response_body = response.read().decode("utf-8")
            parsed = json.loads(response_body) if response_body else []
            rows = _extract_response_rows(parsed)
            return [
                _normalize_fina_row(row, original)
                for row, original in zip(rows, payload)
            ]
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_error = exc
            if logger:
                logger.warning("Fina categorization attempt failed attempt=%s error=%s", attempt + 1, exc)
            if attempt < FINA_MAX_RETRIES:
                time.sleep(0.15 * (attempt + 1))
    if logger and last_error:
        logger.warning("Fina categorization unavailable after retries: %s", last_error)
    _UNAVAILABLE_UNTIL = time.time() + FINA_FAILURE_COOLDOWN_SECONDS
    return []


def categorize_one(description, merchant="", amount=0, logger=None):
    rows = categorize_batch([{"name": description, "merchant": merchant, "amount": amount}], logger=logger)
    return rows[0] if rows else None
