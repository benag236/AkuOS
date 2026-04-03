import re

NON_SPENDING_CATEGORIES = {"Transfer", "Transfer / Payment", "Internal Transfer", "Credit Card Payment"}
GENERIC_CATEGORIES = {"other", "uncategorized", "needs review"}
MERCHANT_STOPWORDS = {
    "ach", "debit", "credit", "card", "purchase", "pos", "dbt", "check", "checkcard",
    "online", "payment", "withdrawal", "deposit", "transfer", "visa", "mc", "fee",
    "transaction", "trans", "posted", "pending", "recurring", "bill", "sepa", "sq",
    "inc", "co", "corp", "llc", "usa", "us", "ny", "ca"
}
MATCH_PRIORITY = {"exact": 3, "startswith": 2, "contains": 1}
DISPLAY_ALIAS_RULES = [
    ("capital one payment", "Capital One Payment"),
    ("capital one mobile payment", "Capital One Payment"),
    ("capital one autopay", "Capital One Payment"),
    ("mobile payment", "Mobile Payment"),
    ("autopay payment", "Autopay Payment"),
    ("payment thank you", "Capital One Payment"),
    ("doordash", "DoorDash"),
    ("dd doordash", "DoorDash"),
    ("shake shack", "Shake Shack"),
    ("starbucks", "Starbucks"),
    ("uber eats", "Uber Eats"),
    ("uber trip", "Uber"),
    ("ubertrip", "Uber"),
    ("uber", "Uber"),
    ("amazon mktplace", "Amazon"),
    ("amazon marketplace", "Amazon"),
    ("amazon mktpl", "Amazon"),
    ("amazon mk", "Amazon"),
    ("amzn mktplace", "Amazon"),
    ("amzn marketplace", "Amazon"),
    ("amzn", "Amazon"),
    ("amazon prime", "Amazon Prime"),
    ("amazon", "Amazon"),
    ("apple com bill", "Apple"),
    ("apple com", "Apple"),
    ("apple cash", "Apple Cash"),
    ("netflix", "Netflix"),
    ("spotify", "Spotify"),
    ("whole foods", "Whole Foods"),
    ("trader joe", "Trader Joe's"),
    ("chipotle", "Chipotle"),
    ("mcdonald", "McDonald's"),
    ("shell", "Shell"),
    ("exxon", "Exxon"),
    ("chevron", "Chevron"),
    ("walmart", "Walmart"),
    ("target", "Target"),
]
DISPLAY_NOISE_PHRASES = (
    "pos withdrawal",
    "pos purchase",
    "dbt purchase",
    "debit purchase",
    "card purchase",
    "visa purchase",
    "checkcard purchase",
    "purchase authorized on",
    "payment to",
    "direct debit",
    "dbcrd pur ap",
    "dbcrd purchase",
    "dbcrd pur",
    "electronic pmt",
    "ach deposit",
    "atm withdrawal",
    "atm wd",
    "dd *",
)
DISPLAY_LOCATION_TOKENS = {
    "new", "york", "brooklyn", "queens", "manhattan", "atlanta", "miami", "orlando",
    "austin", "dallas", "houston", "seattle", "boston", "chicago", "denver", "phoenix",
    "ca", "ny", "tx", "wa", "fl", "ga", "ma", "il", "co", "az"
}

BUILTIN_RULES = [
    {"keyword": "uber eats", "category": "Eating Out", "priority": 990, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "ubertrip", "category": "Transport", "priority": 985, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "uber trip", "category": "Transport", "priority": 985, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "uber", "category": "Transport", "priority": 980, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "amazon prime", "category": "Subscription", "priority": 980, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "prime video", "category": "Subscription", "priority": 978, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "amazon music", "category": "Subscription", "priority": 976, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "amazon", "category": "Shopping", "priority": 920, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "apple.com bill", "category": "Subscription", "priority": 980, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "apple com bill", "category": "Subscription", "priority": 979, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "apple cash", "category": "Transfer", "priority": 970, "match_type": "contains", "amount_direction": "any"},
    {"keyword": "netflix", "category": "Subscription", "priority": 975, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "spotify", "category": "Subscription", "priority": 975, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "shell", "category": "Transport", "priority": 940, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "exxon", "category": "Transport", "priority": 940, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "whole foods", "category": "Groceries", "priority": 950, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "trader joe", "category": "Groceries", "priority": 950, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "aldi", "category": "Groceries", "priority": 945, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "costco", "category": "Groceries", "priority": 944, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "kroger", "category": "Groceries", "priority": 944, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "instacart", "category": "Groceries", "priority": 943, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "chipotle", "category": "Eating Out", "priority": 950, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "mcdonald", "category": "Eating Out", "priority": 950, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "starbucks", "category": "Eating Out", "priority": 948, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "doordash", "category": "Eating Out", "priority": 948, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "walmart", "category": "Groceries", "priority": 900, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "target", "category": "Shopping", "priority": 900, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "lyft", "category": "Transport", "priority": 939, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "bp", "category": "Transport", "priority": 938, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "chevron", "category": "Transport", "priority": 938, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "rent", "category": "Housing", "priority": 970, "match_type": "startswith", "amount_direction": "debit"},
    {"keyword": "property management", "category": "Housing", "priority": 968, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "capital one mobile payment", "category": "Credit Card Payment", "priority": 1000, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "capital one payment", "category": "Credit Card Payment", "priority": 999, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "payment thank you", "category": "Credit Card Payment", "priority": 999, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "autopay payment", "category": "Credit Card Payment", "priority": 998, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "mobile payment", "category": "Credit Card Payment", "priority": 997, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "online transfer", "category": "Transfer", "priority": 997, "match_type": "contains", "amount_direction": "any"},
    {"keyword": "internal transfer", "category": "Transfer", "priority": 996, "match_type": "contains", "amount_direction": "any"},
    {"keyword": "external transfer", "category": "Transfer", "priority": 995, "match_type": "contains", "amount_direction": "any"},
    {"keyword": "credit card payment", "category": "Credit Card Payment", "priority": 994, "match_type": "contains", "amount_direction": "debit"},
    {"keyword": "payment received", "category": "Income", "priority": 994, "match_type": "contains", "amount_direction": "credit"},
    {"keyword": "direct deposit", "category": "Income", "priority": 993, "match_type": "contains", "amount_direction": "credit"},
    {"keyword": "ach deposit", "category": "Income", "priority": 992, "match_type": "contains", "amount_direction": "credit"},
    {"keyword": "zelle", "category": "Transfer", "priority": 991, "match_type": "contains", "amount_direction": "any"},
    {"keyword": "venmo", "category": "Transfer", "priority": 990, "match_type": "contains", "amount_direction": "any"},
    {"keyword": "cash app", "category": "Transfer", "priority": 989, "match_type": "contains", "amount_direction": "any"},
    {"keyword": "paypal", "category": "Transfer", "priority": 988, "match_type": "contains", "amount_direction": "any"},
    {"keyword": "atm withdrawal", "category": "Cash Withdrawal", "priority": 991, "match_type": "contains", "amount_direction": "debit"},
]


def _get_field(item, field, default=None):
    if isinstance(item, dict):
        return item.get(field, default)
    return getattr(item, field, default)


def normalize_merchant(description):
    description = (description or "").lower()
    description = re.sub(r"\b\d{2,}\b", " ", description)
    description = re.sub(r"[*#_/\\-]+", " ", description)
    description = re.sub(r"[^a-z\s]", " ", description)
    tokens = [token for token in description.split() if len(token) > 2 and token not in MERCHANT_STOPWORDS]
    if not tokens:
        return ""
    return " ".join(tokens[:4])


def title_case_merchant(text):
    words = []
    for token in (text or "").split():
        if token.lower() in {"and", "of", "the"}:
            words.append(token.lower())
        elif "'" in token:
            parts = token.split("'")
            words.append("'".join(part.capitalize() for part in parts))
        else:
            words.append(token.capitalize())
    return " ".join(words)


def clean_transaction_description(description):
    raw = " ".join(str(description or "").replace("|", " ").split()).strip()
    if not raw:
        return ""

    lowered = raw.lower()
    if re.search(r"\bcapital\s+one(?:\s+n\.?\s*a\.?)?\s+(?:online\s+)?payment\b", lowered):
        return "Capital One Payment"
    if re.search(r"\bcapital\s+one\s+mobile\s+payment\b", lowered):
        return "Capital One Payment"
    if re.search(r"\bpayment thank you\b", lowered):
        return "Capital One Payment"
    if re.search(r"\bautopay payment\b", lowered):
        return "Autopay Payment"
    if re.search(r"\bmobile payment\b", lowered):
        return "Mobile Payment"
    for noise in DISPLAY_NOISE_PHRASES:
        lowered = lowered.replace(noise, " ")

    lowered = re.sub(r"^\s*dd\s+\*?\s*", " ", lowered)
    lowered = re.sub(r"\bamazon\s+mktpl\*?\b", " amazon marketplace ", lowered)
    lowered = re.sub(r"\bamzn\s+mktpl\*?\b", " amazon marketplace ", lowered)
    lowered = re.sub(r"\buber\s+\*?\s*eats\b", " uber eats ", lowered)
    lowered = re.sub(r"\bcapital\s+one\s+mobile\s+payment\b", " capital one payment ", lowered)
    lowered = re.sub(r"\bcapital\s+one(?:\s+n\.?\s*a\.?)?\s+(?:online\s+)?payment\b", " capital one payment ", lowered)
    lowered = re.sub(r"\bforeign currency\b.*$", " ", lowered)
    lowered = re.sub(r"\bexchange rate\b.*$", " ", lowered)
    lowered = re.sub(r"\bcurrency conversion\b.*$", " ", lowered)
    lowered = re.sub(r"https?://\S+", " ", lowered)
    lowered = re.sub(r"\b(?:store|st#|store#|location|loc|ref|trace|auth|approval|terminal|term|id|txn|trans|seq|order|ord|ticket|invoice)\s*[:#-]?\s*[a-z0-9-]+\b", " ", lowered)
    lowered = re.sub(r"\b[a-z]{0,3}\d{3,}[a-z0-9-]*\b", " ", lowered)
    lowered = re.sub(r"\b\d{3,}\b", " ", lowered)
    lowered = re.sub(r"[*#_/\\-]+", " ", lowered)
    lowered = re.sub(r"[^a-z\s&']", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()

    normalized = normalize_text(lowered)
    if normalized:
        for alias_key, alias_label in DISPLAY_ALIAS_RULES:
            if alias_key in normalized:
                return alias_label

    tokens = [token for token in lowered.split() if token not in MERCHANT_STOPWORDS and token not in DISPLAY_LOCATION_TOKENS]
    if not tokens:
        tokens = [token for token in lowered.split() if token not in DISPLAY_LOCATION_TOKENS]
    if not tokens:
        tokens = lowered.split()
    if not tokens:
        return title_case_merchant(raw[:40].strip())

    cleaned = " ".join(tokens[:3]).strip()
    if not cleaned:
        return title_case_merchant(raw[:40].strip())
    return title_case_merchant(cleaned)


def normalize_text(text):
    normalized_merchant = normalize_merchant(text)
    if normalized_merchant:
        return normalized_merchant
    text = (text or "").lower()
    text = re.sub(r"\d+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def merchant_similarity(left, right):
    left_tokens = set((left or "").split())
    right_tokens = set((right or "").split())
    if not left_tokens or not right_tokens:
        return 0
    overlap = left_tokens & right_tokens
    return len(overlap) / max(len(left_tokens), len(right_tokens))


def matches_rule(description, keyword, match_type):
    desc = normalize_text(description)
    key = normalize_text(keyword)
    if not desc or not key:
        return False
    if match_type == "exact":
        return desc == key
    if match_type == "startswith":
        return desc.startswith(key)
    return key in desc


def matches_amount_direction(amount, amount_direction):
    if amount_direction == "debit":
        return amount < 0
    if amount_direction == "credit":
        return amount > 0
    return True


def sort_rules(rules):
    return sorted(
        rules,
        key=lambda rule: (
            _get_field(rule, "priority", 100),
            MATCH_PRIORITY.get(_get_field(rule, "match_type", "contains"), 1),
            len(normalize_text(_get_field(rule, "keyword", "")))
        ),
        reverse=True
    )


def is_spending_category(category):
    return (category or "").strip() not in NON_SPENDING_CATEGORIES


def is_spending_transaction(tx):
    subtype = (getattr(tx, "transaction_subtype", "") or "").strip().lower()
    if subtype:
        return subtype == "expense"
    return getattr(tx, "amount", 0) < 0 and is_spending_category(getattr(tx, "category", ""))


def categorize_from_sources(description, amount, user_rules, merchant_memories, builtin_rules=None):
    desc = normalize_text(description)

    for rule in sort_rules(user_rules):
        match_type = _get_field(rule, "match_type", "contains")
        amount_direction = _get_field(rule, "amount_direction", "any")
        if matches_amount_direction(amount, amount_direction) and matches_rule(desc, _get_field(rule, "keyword", ""), match_type):
            return _get_field(rule, "category", "Other"), f"Rule ({match_type})"

    best_memory = None
    best_score = 0
    for memory in merchant_memories:
        merchant = _get_field(memory, "merchant", "")
        if not merchant:
            continue
        if merchant == desc or merchant in desc or desc in merchant:
            return _get_field(memory, "category", "Other"), "Merchant Memory"
        score = merchant_similarity(merchant, desc)
        if score > best_score:
            best_score = score
            best_memory = memory

    if best_memory and best_score >= 0.6:
        return _get_field(best_memory, "category", "Other"), "Merchant Memory"

    ruleset = builtin_rules or BUILTIN_RULES
    for rule in sort_rules(ruleset):
        match_type = _get_field(rule, "match_type", "contains")
        amount_direction = _get_field(rule, "amount_direction", "any")
        if matches_amount_direction(amount, amount_direction) and matches_rule(desc, _get_field(rule, "keyword", ""), match_type):
            return _get_field(rule, "category", "Other"), f"Built-in ({match_type})"

    if amount > 0:
        return "Income", "Income Fallback"
    return "Other", "Fallback"


def detect_csv_column(row_keys, candidates):
    lowered = {k.lower().strip(): k for k in row_keys}
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    return None


def detect_amount_from_row(row, safe_float):
    amount_candidates = ["amount", "amt", "transaction amount", "amount ($)", "value"]
    debit_candidates = ["debit", "withdrawal", "money out", "outflow", "charge"]
    credit_candidates = ["credit", "deposit", "money in", "inflow", "payment"]

    amount_key = detect_csv_column(row.keys(), amount_candidates)
    if amount_key:
        amount = safe_float(row.get(amount_key))
        if amount is not None:
            return amount, {"amount": amount_key}

    debit_key = detect_csv_column(row.keys(), debit_candidates)
    credit_key = detect_csv_column(row.keys(), credit_candidates)
    debit = safe_float(row.get(debit_key)) if debit_key else None
    credit = safe_float(row.get(credit_key)) if credit_key else None

    if credit is not None or debit is not None:
        return (credit or 0) - (debit or 0), {"debit": debit_key, "credit": credit_key}

    return None, {}


def _clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def _factor_result(name, earned, weight, detail, available=True):
    earned = _clamp(float(earned), 0.0, float(weight)) if available else 0.0
    ratio = (earned / weight) if weight else 0.0
    if not available:
        tone = "neutral"
    elif ratio >= 0.7:
        tone = "positive"
    elif ratio >= 0.4:
        tone = "neutral"
    else:
        tone = "warning"
    return {
        "name": name,
        "earned": round(earned, 2),
        "weight": weight,
        "detail": detail,
        "available": available,
        "tone": tone,
        "ratio": round(ratio, 4)
    }


def compute_financial_health(snapshot):
    monthly_income = float(snapshot.get("monthly_income") or 0)
    monthly_expenses = float(snapshot.get("monthly_expenses") or 0)
    savings_rate = float(snapshot.get("savings_rate") or 0)
    total_assets = float(snapshot.get("total_assets") or 0)
    total_liabilities = float(snapshot.get("total_liabilities") or 0)
    subscription_total = float(snapshot.get("subscription_total") or 0)
    recurring_monthly_obligations = float(snapshot.get("recurring_monthly_obligations") or 0)
    current_cash = float(snapshot.get("current_cash") or 0)
    prev_monthly_expenses = float(snapshot.get("prev_monthly_expenses") or 0)
    current_day = int(snapshot.get("current_day") or 0)
    days_in_month = int(snapshot.get("days_in_month") or 0)
    budget_rows = snapshot.get("budget_rows") or []

    factors = []

    if monthly_income > 0:
        if savings_rate >= 25:
            score = 20
        elif savings_rate >= 15:
            score = 16
        elif savings_rate >= 10:
            score = 12
        elif savings_rate >= 5:
            score = 8
        elif savings_rate >= 0:
            score = 4
        else:
            score = 0
        factors.append(_factor_result(
            "Savings rate",
            score,
            20,
            f"Savings rate is {savings_rate:.1f}% this month."
        ))
    else:
        factors.append(_factor_result(
            "Savings rate",
            0,
            20,
            "Savings rate could not be scored because income data is limited.",
            available=False
        ))

    if monthly_income > 0:
        expense_ratio = monthly_expenses / monthly_income
        if expense_ratio <= 0.60:
            score = 20
        elif expense_ratio <= 0.80:
            score = 16
        elif expense_ratio <= 1.00:
            score = 10
        elif expense_ratio <= 1.10:
            score = 4
        else:
            score = 0
        factors.append(_factor_result(
            "Expenses vs income",
            score,
            20,
            f"Monthly expenses are {expense_ratio * 100:.0f}% of income."
        ))
    else:
        factors.append(_factor_result(
            "Expenses vs income",
            0,
            20,
            "Expenses versus income could not be scored because income data is missing.",
            available=False
        ))

    if total_assets > 0 or total_liabilities > 0:
        if total_assets <= 0:
            score = 0 if total_liabilities > 0 else 8
            ratio_text = "Liabilities exist without tracked assets."
        else:
            liability_ratio = total_liabilities / total_assets
            if liability_ratio <= 0.30:
                score = 15
            elif liability_ratio <= 0.60:
                score = 12
            elif liability_ratio <= 1.00:
                score = 8
            elif liability_ratio <= 1.50:
                score = 3
            else:
                score = 0
            ratio_text = f"Liabilities are {liability_ratio * 100:.0f}% of assets."
        factors.append(_factor_result("Balance sheet", score, 15, ratio_text))
    else:
        factors.append(_factor_result(
            "Balance sheet",
            0,
            15,
            "Balance-sheet scoring is limited until assets or liabilities are added.",
            available=False
        ))

    sub_base = monthly_income if monthly_income > 0 else monthly_expenses
    if sub_base > 0:
        subscription_ratio = subscription_total / sub_base
        if subscription_ratio <= 0.05:
            score = 10
        elif subscription_ratio <= 0.10:
            score = 8
        elif subscription_ratio <= 0.15:
            score = 5
        elif subscription_ratio <= 0.20:
            score = 2
        else:
            score = 0
        factors.append(_factor_result(
            "Subscription burden",
            score,
            10,
            f"Subscriptions are about {subscription_ratio * 100:.0f}% of monthly {'income' if monthly_income > 0 else 'expenses'}."
        ))
    else:
        factors.append(_factor_result(
            "Subscription burden",
            0,
            10,
            "Subscription burden could not be scored because monthly cash flow data is thin.",
            available=False
        ))

    if budget_rows:
        over_budget = sum(1 for row in budget_rows if float(row.get("pct") or 0) > 100)
        near_budget = sum(1 for row in budget_rows if 80 <= float(row.get("pct") or 0) <= 100)
        avg_pct = sum(float(row.get("pct") or 0) for row in budget_rows) / len(budget_rows)
        if over_budget == 0 and avg_pct <= 70:
            score = 10
        elif over_budget == 0 and avg_pct <= 90:
            score = 8
        elif over_budget <= max(1, len(budget_rows) // 4):
            score = 5
        elif over_budget < len(budget_rows):
            score = 2
        else:
            score = 0
        factors.append(_factor_result(
            "Budget adherence",
            score,
            10,
            f"{len(budget_rows) - over_budget} of {len(budget_rows)} budget categories are still under plan; {near_budget} are close to the limit."
        ))
    else:
        factors.append(_factor_result(
            "Budget adherence",
            0,
            10,
            "Budget adherence is not scored until monthly budgets are added.",
            available=False
        ))

    obligation_base = monthly_income if monthly_income > 0 else monthly_expenses
    if obligation_base > 0:
        obligation_ratio = recurring_monthly_obligations / obligation_base
        if obligation_ratio <= 0.30:
            score = 10
        elif obligation_ratio <= 0.50:
            score = 7
        elif obligation_ratio <= 0.70:
            score = 4
        elif obligation_ratio <= 0.85:
            score = 2
        else:
            score = 0
        factors.append(_factor_result(
            "Recurring obligations",
            score,
            10,
            f"Recurring monthly obligations are about {obligation_ratio * 100:.0f}% of monthly {'income' if monthly_income > 0 else 'expenses'}."
        ))
    else:
        factors.append(_factor_result(
            "Recurring obligations",
            0,
            10,
            "Recurring obligations could not be scored because monthly cash flow data is limited.",
            available=False
        ))

    if monthly_expenses > 0:
        buffer_months = current_cash / monthly_expenses if monthly_expenses > 0 else 0
        if buffer_months >= 6:
            score = 10
        elif buffer_months >= 3:
            score = 8
        elif buffer_months >= 1:
            score = 5
        elif buffer_months >= 0.5:
            score = 3
        else:
            score = 1 if current_cash > 0 else 0
        factors.append(_factor_result(
            "Cash buffer",
            score,
            10,
            f"Cash reserves cover about {buffer_months:.1f} month{'s' if round(buffer_months, 1) != 1 else ''} of current spending."
        ))
    else:
        factors.append(_factor_result(
            "Cash buffer",
            0,
            10,
            "Cash buffer strength could not be scored because expense history is limited.",
            available=False
        ))

    if prev_monthly_expenses > 0 and monthly_expenses > 0 and current_day > 0 and days_in_month > 0:
        projected_current_spend = monthly_expenses if current_day >= days_in_month else (monthly_expenses / current_day) * days_in_month
        spend_change = (projected_current_spend - prev_monthly_expenses) / prev_monthly_expenses
        if spend_change <= -0.10:
            score = 5
        elif spend_change <= 0.05:
            score = 4
        elif spend_change <= 0.15:
            score = 3
        elif spend_change <= 0.30:
            score = 1
        else:
            score = 0
        factors.append(_factor_result(
            "Spending trend",
            score,
            5,
            f"Current spending pace is {spend_change * 100:+.0f}% versus last month."
        ))
    else:
        factors.append(_factor_result(
            "Spending trend",
            0,
            5,
            "Spending trend is not scored until at least two comparable months of expense data exist.",
            available=False
        ))

    available_factors = [factor for factor in factors if factor["available"]]
    available_points = sum(factor["weight"] for factor in available_factors)
    earned_points = sum(factor["earned"] for factor in available_factors)
    score = round((earned_points / available_points) * 100) if available_points > 0 else 0

    negative_factors = sorted(
        [factor for factor in available_factors if factor["tone"] == "warning"],
        key=lambda factor: (factor["ratio"], factor["weight"])
    )
    positive_factors = sorted(
        [factor for factor in available_factors if factor["tone"] == "positive"],
        key=lambda factor: (factor["ratio"], factor["weight"]),
        reverse=True
    )
    neutral_factors = sorted(
        [factor for factor in available_factors if factor["tone"] == "neutral"],
        key=lambda factor: (factor["ratio"], factor["weight"])
    )

    explanations = []
    for factor in negative_factors[:2]:
        explanations.append(f"{factor['name']} hurt the score: {factor['detail']}")
    for factor in positive_factors[:2]:
        explanations.append(f"{factor['name']} helped the score: {factor['detail']}")
    for factor in neutral_factors:
        if len(explanations) >= 5:
            break
        explanations.append(f"{factor['name']} is mixed right now: {factor['detail']}")

    if len(explanations) < 3:
        for factor in factors:
            if factor["available"]:
                continue
            explanations.append(factor["detail"])
            if len(explanations) >= 3:
                break

    tone = "positive" if score >= 75 else "neutral" if score >= 50 else "warning"
    return {
        "score": int(_clamp(score, 0, 100)),
        "tone": tone,
        "explanations": explanations[:5],
        "factors": factors,
        "available_points": round(available_points, 2),
        "earned_points": round(earned_points, 2)
    }


def compute_wealth_score(snapshot):
    monthly_income = float(snapshot.get("monthly_income") or 0)
    monthly_expenses = float(snapshot.get("monthly_expenses") or 0)
    savings_rate = float(snapshot.get("savings_rate") or 0)
    current_savings = float(snapshot.get("current_savings") or 0)
    recommended_savings_amount = snapshot.get("recommended_savings_amount")
    emergency_fund_months = snapshot.get("emergency_fund_months")
    net_worth = float(snapshot.get("net_worth") or 0)
    net_worth_trend_delta = snapshot.get("net_worth_trend_delta")
    total_liabilities = float(snapshot.get("total_liabilities") or 0)
    investment_total = float(snapshot.get("investment_total") or 0)
    goal_progress_ratio = snapshot.get("goal_progress_ratio")

    factors = []

    if monthly_income > 0:
        if savings_rate >= 20:
            score = 20
        elif savings_rate >= 15:
            score = 17
        elif savings_rate >= 10:
            score = 13
        elif savings_rate >= 5:
            score = 8
        elif savings_rate >= 0:
            score = 4
        else:
            score = 0
        factors.append(_factor_result("Savings rate", score, 20, f"Current savings rate is {savings_rate:.1f}% of income."))
    else:
        factors.append(_factor_result("Savings rate", 0, 20, "Savings-rate scoring needs monthly income data.", available=False))

    if emergency_fund_months is not None:
        emergency_fund_months = float(emergency_fund_months)
        if emergency_fund_months >= 6:
            score = 20
        elif emergency_fund_months >= 3:
            score = 16
        elif emergency_fund_months >= 1.5:
            score = 10
        elif emergency_fund_months > 0:
            score = 5
        else:
            score = 0
        factors.append(_factor_result("Emergency fund", score, 20, f"Emergency savings cover about {emergency_fund_months:.1f} months of essential expenses."))
    else:
        factors.append(_factor_result("Emergency fund", 0, 20, "Emergency-fund scoring needs enough expense data to estimate essential costs.", available=False))

    if net_worth_trend_delta is not None:
        trend_delta = float(net_worth_trend_delta)
        if trend_delta >= 2000:
            score = 15
        elif trend_delta >= 500:
            score = 12
        elif trend_delta >= 0:
            score = 9
        elif trend_delta >= -1000:
            score = 5
        else:
            score = 1
        direction = "up" if trend_delta > 0 else "down" if trend_delta < 0 else "flat"
        factors.append(_factor_result("Net worth trend", score, 15, f"Tracked net worth is {direction} by ${abs(trend_delta):,.0f} over the available history."))
    else:
        factors.append(_factor_result("Net worth trend", 0, 15, "Net-worth trend needs more historical account activity.", available=False))

    balance_sheet_base = max(current_savings + investment_total, net_worth if net_worth > 0 else 0)
    if total_liabilities > 0 or balance_sheet_base > 0:
        if total_liabilities <= 0:
            score = 15
            detail = "No tracked liabilities are reducing wealth right now."
        else:
            burden_ratio = total_liabilities / max(balance_sheet_base, total_liabilities, 1)
            if burden_ratio <= 0.30:
                score = 15
            elif burden_ratio <= 0.60:
                score = 11
            elif burden_ratio <= 1.0:
                score = 7
            elif burden_ratio <= 1.5:
                score = 3
            else:
                score = 0
            detail = f"Liabilities are about {burden_ratio * 100:.0f}% of tracked wealth-building balances."
        factors.append(_factor_result("Debt burden", score, 15, detail))
    else:
        factors.append(_factor_result("Debt burden", 0, 15, "Debt-burden scoring needs tracked liabilities or wealth balances.", available=False))

    if monthly_income > 0 or current_savings > 0 or investment_total > 0:
        wealth_balance_ratio = (current_savings + investment_total) / monthly_income if monthly_income > 0 else 0
        if wealth_balance_ratio >= 6:
            score = 15
        elif wealth_balance_ratio >= 3:
            score = 12
        elif wealth_balance_ratio >= 1:
            score = 8
        elif (current_savings + investment_total) > 0:
            score = 4
        else:
            score = 0
        detail = f"Savings and investment balances total ${current_savings + investment_total:,.0f}."
        if monthly_income > 0:
            detail += f" That is about {wealth_balance_ratio:.1f} month{'s' if round(wealth_balance_ratio, 1) != 1 else ''} of income."
        factors.append(_factor_result("Savings and investments", score, 15, detail))
    else:
        factors.append(_factor_result("Savings and investments", 0, 15, "Savings-and-investment scoring needs income or wealth balances.", available=False))

    if goal_progress_ratio is not None:
        goal_progress_ratio = float(goal_progress_ratio)
        if goal_progress_ratio >= 0.9:
            score = 15
        elif goal_progress_ratio >= 0.6:
            score = 11
        elif goal_progress_ratio >= 0.3:
            score = 7
        elif goal_progress_ratio > 0:
            score = 3
        else:
            score = 0
        factors.append(_factor_result("Goal progress", score, 15, f"Average tracked goal progress is {goal_progress_ratio * 100:.0f}%." ))
    else:
        factors.append(_factor_result("Goal progress", 0, 15, "Goal-progress scoring starts once at least one financial goal is added.", available=False))

    available_factors = [factor for factor in factors if factor["available"]]
    available_points = sum(factor["weight"] for factor in available_factors)
    earned_points = sum(factor["earned"] for factor in available_factors)
    score = round((earned_points / available_points) * 100) if available_points > 0 else 0

    label = "Excellent" if score >= 85 else "Strong" if score >= 70 else "Solid" if score >= 50 else "Building"
    tone = "positive" if score >= 70 else "neutral" if score >= 50 else "warning"

    negative_factors = sorted(
        [factor for factor in available_factors if factor["tone"] == "warning"],
        key=lambda factor: (factor["ratio"], factor["weight"])
    )
    positive_factors = sorted(
        [factor for factor in available_factors if factor["tone"] == "positive"],
        key=lambda factor: (factor["ratio"], factor["weight"]),
        reverse=True
    )
    neutral_factors = sorted(
        [factor for factor in available_factors if factor["tone"] == "neutral"],
        key=lambda factor: (factor["ratio"], factor["weight"])
    )

    explanations = []
    for factor in positive_factors[:2]:
        explanations.append(f"{factor['name']} helps: {factor['detail']}")
    for factor in negative_factors[:2]:
        explanations.append(f"{factor['name']} needs work: {factor['detail']}")
    for factor in neutral_factors:
        if len(explanations) >= 5:
            break
        explanations.append(f"{factor['name']} is mixed: {factor['detail']}")

    if len(explanations) < 3:
        for factor in factors:
            if factor["available"]:
                continue
            explanations.append(factor["detail"])
            if len(explanations) >= 3:
                break

    target_gap = None
    if recommended_savings_amount is not None and monthly_income > 0:
        target_gap = float(recommended_savings_amount) - ((savings_rate / 100.0) * monthly_income)

    return {
        "score": int(_clamp(score, 0, 100)),
        "label": label,
        "tone": tone,
        "explanations": explanations[:5],
        "factors": factors,
        "available_points": round(available_points, 2),
        "earned_points": round(earned_points, 2),
        "target_gap": round(target_gap, 2) if target_gap is not None else None,
    }
