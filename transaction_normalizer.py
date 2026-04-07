from __future__ import annotations

import re
from datetime import date, datetime, timedelta

from finance_engine import clean_transaction_description, normalize_text


DATE_PATTERN = re.compile(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b")
AMOUNT_PATTERN = re.compile(r"(?<!\d)(?:\(?-?\$?\d[\d,]*\.\d{2}\)?(?:\s*(?:CR|DR))?)")

NON_TRANSACTION_PHRASES = (
    "years previous adjusted",
    "payment information",
    "minimum payment due",
    "previous balance",
    "new balance",
    "credit line",
    "available credit",
    "cash advance line",
    "rewards summary",
    "rewards earned",
    "earnings summary",
    "interest charge",
    "interest charged",
    "interest calculation",
    "legal notice",
    "billing rights",
    "payment mailing address",
    "customer service",
    "statement period",
    "account summary",
    "daily balance summary",
    "account number",
    "important information",
    "member fdic",
    "year to date",
)

DESCRIPTION_PREFIX_PATTERNS = [
    re.compile(r"^\s*dbcrd\s+pur(?:chase)?(?:\s+ap)?\s+", re.I),
    re.compile(r"^\s*purchase(?:\s+authorized\s+on)?\s+", re.I),
    re.compile(r"^\s*ach\s+deposit\s+", re.I),
    re.compile(r"^\s*direct\s+deposit\s+", re.I),
    re.compile(r"^\s*atm(?:\s+withdrawal|\s+wd)?\s+", re.I),
    re.compile(r"^\s*electronic\s+pmt\s+", re.I),
    re.compile(r"^\s*electronic\s+payment\s+", re.I),
    re.compile(r"^\s*capital\s+one(?:\s+online)?\s+payment\s+", re.I),
    re.compile(r"^\s*visa\s+checkcard\s+", re.I),
    re.compile(r"^\s*checkcard\s+", re.I),
    re.compile(r"^\s*dd\s*\*\s*", re.I),
    re.compile(r"^\s*uber\s*\*", re.I),
    re.compile(r"^\s*amazon\s+mktpl\*?\s*", re.I),
]


def normalize_whitespace(value):
    return " ".join(str(value or "").split()).strip()


def safe_float(value):
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except Exception:
        return None


def parse_date_any(value):
    text = normalize_whitespace(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_statement_date_with_fallback(value, reference_year=None):
    parsed = parse_date_any(value)
    if parsed:
        return parsed

    cleaned = normalize_whitespace(value)
    match = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})", cleaned)
    if not match:
        return None

    month = int(match.group(1))
    day = int(match.group(2))
    year = int(reference_year or date.today().year)
    try:
        parsed = date(year, month, day)
    except ValueError:
        return None

    if parsed > (date.today() + timedelta(days=31)):
        try:
            parsed = date(year - 1, month, day)
        except ValueError:
            return None
    return parsed


def parse_statement_amount(value, force_sign=None):
    text_value = normalize_whitespace(value)
    if not text_value:
        return None
    negative = "(" in text_value or text_value.startswith("-") or " DR" in text_value.upper()
    amount = safe_float(text_value.replace("(", "").replace(")", "").replace("CR", "").replace("DR", ""))
    if amount is None:
        return None
    if force_sign == "negative":
        return -abs(amount)
    if force_sign == "positive":
        return abs(amount)
    return -abs(amount) if negative else abs(amount)


def extract_date_matches(text):
    cleaned = normalize_whitespace(text)
    matches = []
    for match in DATE_PATTERN.finditer(cleaned):
        parsed = parse_statement_date_with_fallback(match.group(0))
        if parsed:
            matches.append({"match": match, "parsed": parsed})
    return matches


def choose_amount_match(text, date_matches):
    cleaned = normalize_whitespace(text)
    amount_matches = list(AMOUNT_PATTERN.finditer(cleaned))
    if not amount_matches:
        return None
    if date_matches:
        last_date_end = date_matches[-1]["match"].end()
        trailing = [match for match in amount_matches if match.start() > last_date_end]
        if trailing:
            return trailing[-1]
    return amount_matches[-1]


def description_between_dates_and_amount(text, date_matches, amount_match):
    cleaned = normalize_whitespace(text)
    if not amount_match:
        return ""
    start_index = date_matches[-1]["match"].end() if date_matches else 0
    description = cleaned[start_index:amount_match.start()].strip(" -|")
    if description:
        return description
    if date_matches:
        return cleaned[date_matches[0]["match"].end():amount_match.start()].strip(" -|")
    return ""


def strip_transaction_prefix(description):
    cleaned = normalize_whitespace(description)
    for pattern in DESCRIPTION_PREFIX_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    return cleaned.strip(" -")


def is_obviously_non_transaction_text(text, extra_stop_phrases=None):
    cleaned = normalize_whitespace(text).lower()
    if not cleaned:
        return True
    stop_phrases = list(NON_TRANSACTION_PHRASES) + list(extra_stop_phrases or [])
    if any(phrase in cleaned for phrase in stop_phrases):
        return True
    if len(cleaned) > 180:
        return True
    if len(cleaned.split()) > 20:
        return True
    return False


def cleaned_display_name(raw_description, transaction_type="", fallback=""):
    stripped = strip_transaction_prefix(raw_description or fallback)
    cleaned = clean_transaction_description(stripped or raw_description or fallback)
    if cleaned and not is_obviously_non_transaction_text(cleaned):
        return cleaned

    fallback_map = {
        "Income": "Deposit",
        "Cash Withdrawal": "ATM Withdrawal",
        "Bills/Payments": "Payment",
        "Expense": "Purchase",
    }
    return fallback_map.get(transaction_type or "", cleaned or fallback or "")


def infer_sign_from_text(raw_text, amount_token, positive_hints=None, negative_hints=None):
    upper_raw = (raw_text or "").lower()
    token = (amount_token or "").upper()
    if "(" in (amount_token or "") or "-" in (amount_token or "") or "DR" in token:
        return "negative"
    if "CR" in token:
        return "positive"
    if any(hint in upper_raw for hint in (positive_hints or ())):
        return "positive"
    if any(hint in upper_raw for hint in (negative_hints or ())):
        return "negative"
    return "negative"


def build_transaction_fingerprint(tx_date, description, amount):
    if hasattr(tx_date, "isoformat"):
        date_key = tx_date.isoformat()
    else:
        date_key = str(tx_date)
    amount_key = round(float(amount or 0), 2)
    merchant_key = normalize_text(description)
    return f"{date_key}|{amount_key:.2f}|{merchant_key}"


def normalize_row_payload(
    *,
    source_document,
    raw_source,
    parsed_date,
    post_date=None,
    raw_description="",
    amount=None,
    source_category="",
    raw_category="",
    transaction_type="",
    parser_label="",
    parser_source="rule_based",
    parser_confidence=0.0,
    parser_warnings=None,
    display_name=None,
):
    cleaned_name = cleaned_display_name(raw_description, transaction_type=transaction_type, fallback=display_name or raw_source)
    return {
        "source_document": source_document,
        "raw_source": normalize_whitespace(raw_source),
        "date": parsed_date.isoformat() if parsed_date else "",
        "post_date": post_date.isoformat() if post_date else "",
        "description": cleaned_name,
        "raw_description": normalize_whitespace(raw_description or raw_source),
        "amount": round(float(amount or 0), 2) if amount is not None else "",
        "source_category": source_category or "",
        "raw_category": raw_category or "",
        "category": "",
        "category_source": "",
        "fingerprint": build_transaction_fingerprint(parsed_date, normalize_whitespace(raw_description or cleaned_name), amount)
        if parsed_date and amount is not None
        else "",
        "requires_manual_fields": False,
        "manual_reason": "",
        "transaction_type": transaction_type or "",
        "parser_label": parser_label or "Statement parser",
        "parser_source": parser_source,
        "parser_confidence": round(float(parser_confidence or 0), 3),
        "parser_warnings": list(parser_warnings or []),
    }
