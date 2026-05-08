"""Rule-based categorization engine with hierarchy-aware suggestions."""

from collections import Counter, defaultdict
import re

from finance_engine import merchant_match_strength, merchant_similarity
from services.category_rules import canonical_category_pair
from services.merchant_normalizer import merchant_guess, merchant_key, normalized_description


DEFAULT_RULE_CONFIDENCE = {
    "exact": 0.95,
    "startswith": 0.9,
    "contains": 0.85,
    "regex": 0.82,
    "amount_sign": 0.72,
    "recurring": 0.75,
}

CONFIDENCE_BUCKETS = (
    (0.9, "high"),
    (0.75, "medium"),
    (0.6, "low"),
)

PAYMENT_KEYWORDS = (
    "payment thank you",
    "online payment",
    "mobile payment",
    "autopay payment",
    "credit card payment",
    "capital one payment",
)
RIDESHARE_KEYWORDS = (
    "uber trip",
    "ubertrip",
    "uber ride",
    "lyft",
    "rideshare",
)
TAKEOUT_KEYWORDS = (
    "uber eats",
    "doordash",
    "grubhub",
)
AIRLINE_KEYWORDS = (
    "united airlines",
    "united air",
    "ua airlines",
    "delta air lines",
    "delta",
    "american airlines",
)
GAS_STATION_KEYWORDS = (
    "shell",
    "exxon",
    "bp",
    "chevron",
    "sunoco",
    "wawa",
)
TRANSFER_KEYWORDS = (
    "transfer",
    "ach transfer",
    "zelle",
    "venmo cashout",
    "cash app",
    "paypal transfer",
)
INCOME_KEYWORDS = (
    "payroll",
    "direct dep",
    "direct deposit",
    "adp",
    "paychex",
    "salary",
    "bonus",
    "refund",
    "interest paid",
    "dividend",
)
FEE_KEYWORDS = ("fee", "service charge", "overdraft", "late fee", "annual fee")
ATM_KEYWORDS = ("atm", "cash withdrawal")

BANKING_STOP_WORDS = {
    "ach", "credit", "debit", "dda", "withdraw", "withdrawal", "wthdrl", "ap",
    "atm", "fcti", "online", "internal", "transfer", "payment", "deposit",
    "pos", "purchase", "checking", "savings", "account", "bank",
}

BANKING_BRAND_ALIASES = {
    "7eleven": "7-Eleven",
    "7 eleven": "7-Eleven",
    "wealthfront": "Wealthfront",
    "td bank": "TD Bank",
    "td": "TD Bank",
    "fidelity": "Fidelity",
    "vanguard": "Vanguard",
    "schwab": "Schwab",
    "robinhood": "Robinhood",
    "venmo": "Venmo",
    "zelle": "Zelle",
    "paypal": "PayPal",
    "cash app": "Cash App",
}


def _get_field(item, field, default=None):
    if isinstance(item, dict):
        return item.get(field, default)
    return getattr(item, field, default)


def confidence_bucket(score):
    score = float(score or 0)
    if score <= 0:
        return "uncategorized"
    for threshold, label in CONFIDENCE_BUCKETS:
        if score >= threshold:
            return label
    return "low"


def normalize_rule_type(rule_type):
    cleaned = (_get_field({"x": rule_type}, "x", "") or "").strip().lower()
    return cleaned if cleaned in {"exact", "startswith", "contains", "regex", "amount_sign", "recurring"} else "contains"


def rule_sort_key(rule):
    rule_type = normalize_rule_type(_get_field(rule, "rule_type") or _get_field(rule, "match_type"))
    pattern = (_get_field(rule, "pattern") or _get_field(rule, "keyword") or "").strip()
    return (
        int(_get_field(rule, "priority", 100) or 100),
        {"exact": 6, "startswith": 5, "contains": 4, "regex": 3, "amount_sign": 2, "recurring": 1}.get(rule_type, 0),
        len(pattern),
    )


def sorted_rules(rules):
    active_rules = [
        rule for rule in (rules or [])
        if _get_field(rule, "is_active", True) not in (False, 0, "0")
    ]
    return sorted(active_rules, key=rule_sort_key, reverse=True)


def resolve_rule_category(rule, category_lookup=None):
    category_lookup = category_lookup or {}
    category_name = (_get_field(rule, "category_name") or _get_field(rule, "category") or "").strip()
    subcategory_name = (_get_field(rule, "subcategory_name") or _get_field(rule, "subcategory") or "").strip()
    category_id = _get_field(rule, "category_id")
    subcategory_id = _get_field(rule, "subcategory_id")
    if category_id and category_id in category_lookup:
        category_name = category_lookup[category_id]["name"]
    if subcategory_id and subcategory_id in category_lookup:
        subcategory_name = category_lookup[subcategory_id]["name"]
    return canonical_category_pair(category_name or "Needs Review", subcategory_name)


def matches_rule(description, amount, rule):
    rule_type = normalize_rule_type(_get_field(rule, "rule_type") or _get_field(rule, "match_type"))
    pattern = (_get_field(rule, "pattern") or _get_field(rule, "keyword") or "").strip()
    if not pattern:
        return False

    amount_direction = (_get_field(rule, "amount_direction") or "any").strip().lower()
    if amount_direction == "credit" and float(amount or 0) <= 0:
        return False
    if amount_direction == "debit" and float(amount or 0) >= 0:
        return False

    normalized_desc = normalized_description(description)
    merchant_key_value = merchant_key(description)
    normalized_pattern = normalized_description(pattern)
    if rule_type == "exact":
        return normalized_desc == normalized_pattern or merchant_key_value == normalized_pattern
    if rule_type == "startswith":
        return normalized_desc.startswith(normalized_pattern) or merchant_key_value.startswith(normalized_pattern)
    if rule_type == "regex":
        try:
            return bool(re.search(pattern, description or "", re.IGNORECASE))
        except re.error:
            return False
    if rule_type == "amount_sign":
        sign = normalized_pattern or pattern.strip().lower()
        if sign == "positive":
            return float(amount or 0) > 0
        if sign == "negative":
            return float(amount or 0) < 0
        return False
    if rule_type == "recurring":
        return False
    return (
        normalized_pattern in normalized_desc
        or normalized_pattern in merchant_key_value
    )


def build_recurring_index(transactions):
    grouped = defaultdict(list)
    for tx in transactions or []:
        category = (_get_field(tx, "category") or "").strip()
        if not category or category.lower() in {"needs review", "other"}:
            continue
        key = merchant_key(_get_field(tx, "raw_description") or _get_field(tx, "description"))
        if not key:
            continue
        grouped[key].append(tx)

    recurring_index = {}
    for key, items in grouped.items():
        if len(items) < 3:
            continue
        amounts = [round(abs(float(_get_field(tx, "amount") or 0)), 2) for tx in items if _get_field(tx, "amount") is not None]
        if not amounts:
            continue
        avg_amount = sum(amounts) / len(amounts)
        if avg_amount <= 0:
            continue
        variance = max(amounts) - min(amounts)
        if variance > max(10.0, avg_amount * 0.2):
            continue
        category_counter = Counter(
            (
                (_get_field(tx, "category") or "").strip(),
                (_get_field(tx, "subcategory") or "").strip(),
                (_get_field(tx, "transaction_subtype") or "").strip(),
            )
            for tx in items
        )
        (category_name, subcategory_name, subtype), _ = category_counter.most_common(1)[0]
        recurring_index[key] = {
            "category": category_name,
            "subcategory": subcategory_name,
            "subtype": subtype,
            "confidence": 0.75,
        }
    return recurring_index


def heuristic_category(description, amount):
    normalized_desc = normalized_description(description)
    lowered = normalized_desc.lower()
    if any(keyword in lowered for keyword in TAKEOUT_KEYWORDS) and float(amount or 0) < 0:
        return ("Food", "Takeout", 0.9, "Heuristic (merchant)", "expense")
    if any(keyword in lowered for keyword in RIDESHARE_KEYWORDS) and float(amount or 0) < 0:
        return ("Transportation", "Uber / Rideshare", 0.9, "Heuristic (merchant)", "expense")
    if any(keyword in lowered for keyword in AIRLINE_KEYWORDS) and float(amount or 0) < 0:
        return ("Travel", "Flights", 0.9, "Heuristic (merchant)", "expense")
    if any(keyword in lowered for keyword in GAS_STATION_KEYWORDS) and float(amount or 0) < 0:
        return ("Car Related", "Gas", 0.9, "Heuristic (merchant)", "expense")
    if any(keyword in lowered for keyword in PAYMENT_KEYWORDS) and float(amount or 0) < 0:
        return ("Subscriptions / Bills", "Credit Card Payment", 0.9, "Heuristic (payment)", "payment")
    if any(keyword in lowered for keyword in TRANSFER_KEYWORDS):
        return ("Other", "", 0.85, "Heuristic (transfer)", "transfer")
    if any(keyword in lowered for keyword in ATM_KEYWORDS) and float(amount or 0) < 0:
        return ("Other", "", 0.85, "Heuristic (ATM)", "expense")
    if any(keyword in lowered for keyword in FEE_KEYWORDS) and float(amount or 0) < 0:
        return ("Other", "", 0.82, "Heuristic (fees)", "expense")
    if float(amount or 0) > 0 and any(keyword in lowered for keyword in INCOME_KEYWORDS):
        subcategory = "Refund" if "refund" in lowered else "Investment Income" if "dividend" in lowered or "interest" in lowered else "Salary"
        return ("Income", subcategory, 0.88, "Heuristic (income)", "income")
    if float(amount or 0) > 0:
        return ("Income", "", 0.68, "Heuristic (amount)", "income")
    return None


def titleize_merchant(value):
    cleaned = re.sub(r"[^a-zA-Z0-9&' ]+", " ", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    alias = BANKING_BRAND_ALIASES.get(cleaned.lower())
    if alias:
        return alias
    return " ".join(part.upper() if len(part) <= 3 and part.isalpha() else part.capitalize() for part in cleaned.split())


def banking_merchant_hint(description):
    raw_lowered = re.sub(r"[^a-z0-9&' ]+", " ", (description or "").lower())
    raw_lowered = re.sub(r"\s+", " ", raw_lowered).strip()
    lowered = normalized_description(description).lower()
    for key, label in BANKING_BRAND_ALIASES.items():
        if key in lowered or key in raw_lowered:
            return label
    tokens = [token for token in re.findall(r"[a-zA-Z][a-zA-Z0-9&']+", raw_lowered or lowered) if token not in BANKING_STOP_WORDS]
    tokens = [token for token in tokens if not re.fullmatch(r"[a-z]*\d+[a-z0-9]*", token)]
    if not tokens:
        return ""
    return titleize_merchant(" ".join(tokens[:3]))


def banking_transaction_intent(description, amount):
    raw_lowered = re.sub(r"[^a-z0-9&' ]+", " ", (description or "").lower())
    raw_lowered = re.sub(r"\s+", " ", raw_lowered).strip()
    lowered = normalized_description(description).lower()
    searchable = f"{lowered} {raw_lowered}".strip()
    if not searchable:
        return None

    merchant = banking_merchant_hint(description)
    is_debit = float(amount or 0) < 0
    is_credit = float(amount or 0) > 0
    has_dda = "dda" in searchable
    has_atm = "atm" in searchable or "fcti" in searchable
    has_withdrawal = any(keyword in searchable for keyword in ("withdraw", "withdrawal", "wthdrl"))
    has_pos = re.search(r"\bpos\b", searchable) or "point of sale" in searchable
    has_ach = "ach" in searchable
    has_transfer = "transfer" in searchable or any(keyword in searchable for keyword in ("zelle", "venmo", "cash app", "paypal transfer"))
    has_payment = "payment" in searchable
    has_deposit = "deposit" in searchable
    has_savings = "savings" in searchable
    has_brokerage = "brokerage" in searchable

    if is_debit and (has_atm or has_withdrawal) and not has_transfer:
        merchant_label = merchant or "ATM"
        place_copy = f" at/near {merchant_label}" if merchant and merchant != "ATM" else ""
        return {
            "category": "Cash",
            "subcategory": "ATM Withdrawal",
            "confidence": 0.92 if merchant else 0.86,
            "source": "Banking Parser (ATM)",
            "subtype": "expense",
            "display_name": f"{merchant_label} ATM Withdrawal" if merchant and "ATM" not in merchant else "ATM Withdrawal",
            "detected_context": f"Possible ATM withdrawal from checking account{place_copy}.",
        }

    if has_transfer or has_ach:
        if "internal transfer" in searchable:
            subcategory = "Internal Transfer"
            confidence = 0.92
        elif has_savings:
            subcategory = "Savings Transfer"
            confidence = 0.9
        elif has_brokerage:
            subcategory = "Brokerage Transfer"
            confidence = 0.9
        else:
            subcategory = "Money Transfer"
            confidence = 0.88
        merchant_label = merchant or "Bank"
        return {
            "category": "Transfers",
            "subcategory": subcategory,
            "confidence": confidence,
            "source": "Banking Parser (transfer)",
            "subtype": "transfer",
            "display_name": f"{merchant_label} Transfer",
            "detected_context": "Possible account-to-account money movement.",
        }

    if is_debit and has_pos:
        merchant_label = merchant or "Card Purchase"
        return {
            "category": "Shopping",
            "subcategory": "",
            "confidence": 0.72,
            "source": "Banking Parser (POS)",
            "subtype": "expense",
            "display_name": f"{merchant_label} Purchase" if merchant else "Card Purchase",
            "detected_context": "Possible card purchase.",
        }

    if is_credit and has_deposit and "direct deposit" not in lowered:
        return {
            "category": "Cash",
            "subcategory": "Cash Deposit",
            "confidence": 0.82,
            "source": "Banking Parser (deposit)",
            "subtype": "income",
            "display_name": "Cash Deposit",
            "detected_context": "Possible cash deposit.",
        }

    if is_debit and has_payment:
        return {
            "category": "Subscriptions / Bills",
            "subcategory": "Credit Card Payment",
            "confidence": 0.84,
            "source": "Banking Parser (payment)",
            "subtype": "payment",
            "display_name": f"{merchant} Payment" if merchant else "Card or Loan Payment",
            "detected_context": "Possible card or loan payment.",
        }

    if has_dda and is_debit and has_withdrawal:
        return {
            "category": "Cash",
            "subcategory": "ATM Withdrawal",
            "confidence": 0.82,
            "source": "Banking Parser (checking withdrawal)",
            "subtype": "expense",
            "display_name": "Checking Withdrawal",
            "detected_context": "Possible withdrawal from a checking account.",
        }
    return None


def categorize_transaction_record(description, amount, tx_date=None, user_rules=None, merchant_memories=None, category_lookup=None, recurring_index=None):
    normalized_desc = normalized_description(description)
    guessed_merchant = merchant_guess(description)
    merchant_memories = merchant_memories or []
    recurring_index = recurring_index or {}

    result = {
        "normalized_description": normalized_desc,
        "merchant_guess": guessed_merchant,
        "category": "Needs Review",
        "subcategory": "",
        "confidence_score": 0.0,
        "confidence_bucket": "uncategorized",
        "category_source": "Needs Review",
        "matched_rule_id": None,
        "matched_rule_type": "",
        "matched_rule_pattern": "",
        "needs_review": True,
        "transaction_subtype": "expense" if float(amount or 0) < 0 else "income" if float(amount or 0) > 0 else "neutral",
    }

    merchant_key_value = merchant_key(description)
    banking_intent = banking_transaction_intent(description, amount)
    best_memory = None
    best_memory_score = 0.0
    for memory in merchant_memories:
        memory_key = (_get_field(memory, "merchant") or "").strip()
        if not memory_key:
            continue
        if merchant_key_value and memory_key == merchant_key_value:
            category_name, subcategory_name = canonical_category_pair(
                _get_field(memory, "category", "Needs Review"),
                _get_field(memory, "subcategory", ""),
            )
            result.update({
                "category": category_name,
                "subcategory": subcategory_name,
                "confidence_score": 0.95,
                "confidence_bucket": "high",
                "category_source": "Merchant Memory",
                "transaction_subtype": (_get_field(memory, "subtype") or result["transaction_subtype"]).strip().lower() or result["transaction_subtype"],
                "needs_review": False,
            })
            return result
        if merchant_key_value:
            similarity = merchant_match_strength(memory_key, merchant_key_value)
            if similarity > best_memory_score:
                best_memory_score = similarity
                best_memory = memory

    if best_memory and best_memory_score >= 0.72:
        category_name, subcategory_name = canonical_category_pair(
            _get_field(best_memory, "category", "Needs Review"),
            _get_field(best_memory, "subcategory", ""),
        )
        confidence = 0.95 if best_memory_score >= 0.92 else 0.91 if best_memory_score >= 0.84 else 0.84
        result.update({
            "category": category_name,
            "subcategory": subcategory_name,
            "confidence_score": confidence,
            "confidence_bucket": confidence_bucket(confidence),
            "category_source": "Merchant Memory",
            "transaction_subtype": (_get_field(best_memory, "subtype") or result["transaction_subtype"]).strip().lower() or result["transaction_subtype"],
            "needs_review": confidence < 0.9,
        })
        return result

    for rule in sorted_rules(user_rules):
        if matches_rule(description, amount, rule):
            category_name, subcategory_name = resolve_rule_category(rule, category_lookup)
            confidence = float(_get_field(rule, "confidence") or DEFAULT_RULE_CONFIDENCE.get(normalize_rule_type(_get_field(rule, "rule_type") or _get_field(rule, "match_type")), 0.8))
            result.update({
                "category": category_name,
                "subcategory": subcategory_name,
                "confidence_score": confidence,
                "confidence_bucket": confidence_bucket(confidence),
                "category_source": "System Rule" if _get_field(rule, "is_system_rule", False) else f"Rule ({normalize_rule_type(_get_field(rule, 'rule_type') or _get_field(rule, 'match_type'))})",
                "matched_rule_id": _get_field(rule, "id"),
                "matched_rule_type": normalize_rule_type(_get_field(rule, "rule_type") or _get_field(rule, "match_type")),
                "matched_rule_pattern": (_get_field(rule, "pattern") or _get_field(rule, "keyword") or "").strip(),
                "transaction_subtype": (_get_field(rule, "subtype") or result["transaction_subtype"]).strip().lower() or result["transaction_subtype"],
                "needs_review": confidence < 0.9,
            })
            return result

    if banking_intent:
        category_name, subcategory_name = canonical_category_pair(
            banking_intent["category"],
            banking_intent.get("subcategory", ""),
        )
        confidence = float(banking_intent.get("confidence") or 0)
        result.update({
            "category": category_name,
            "subcategory": subcategory_name,
            "confidence_score": confidence,
            "confidence_bucket": confidence_bucket(confidence),
            "category_source": banking_intent.get("source") or "Banking Parser",
            "transaction_subtype": banking_intent.get("subtype") or result["transaction_subtype"],
            "needs_review": confidence < 0.82,
            "suggested_display_name": banking_intent.get("display_name", ""),
            "detected_context": banking_intent.get("detected_context", ""),
        })
        return result

    heuristic = heuristic_category(description, amount)
    if heuristic:
        category_name, subcategory_name, confidence, source, subtype = heuristic
        category_name, subcategory_name = canonical_category_pair(category_name, subcategory_name)
        result.update({
            "category": category_name,
            "subcategory": subcategory_name,
            "confidence_score": confidence,
            "confidence_bucket": confidence_bucket(confidence),
            "category_source": source,
            "transaction_subtype": subtype,
            "needs_review": confidence < 0.85,
        })
        if not result["needs_review"]:
            return result

    if merchant_key_value and merchant_key_value in recurring_index:
        recurring = recurring_index[merchant_key_value]
        category_name, subcategory_name = canonical_category_pair(recurring["category"], recurring.get("subcategory"))
        confidence = float(recurring.get("confidence") or 0.75)
        result.update({
            "category": category_name,
            "subcategory": subcategory_name,
            "confidence_score": max(result["confidence_score"], confidence),
            "confidence_bucket": confidence_bucket(max(result["confidence_score"], confidence)),
            "category_source": "Recurring Pattern",
            "transaction_subtype": (recurring.get("subtype") or result["transaction_subtype"]).strip().lower() or result["transaction_subtype"],
            "needs_review": max(result["confidence_score"], confidence) < 0.85,
        })
        return result

    return result
