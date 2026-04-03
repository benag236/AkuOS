from flask import Flask, render_template, request, redirect, session, Response, url_for
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import inspect, text, func
import os
import math
import json
import uuid
import re
import calendar
from datetime import datetime, date, timedelta
from collections import defaultdict
import csv
from io import StringIO, BytesIO
try:
    import pdfplumber
except ImportError:
    pdfplumber = None
from finance_engine import (
    compute_financial_health,
    compute_wealth_score,
    GENERIC_CATEGORIES,
    categorize_from_sources,
    detect_amount_from_row as detect_amount_from_row_helper,
    detect_csv_column,
    is_spending_category,
    is_spending_transaction,
    normalize_text,
    sort_rules,
)

app = Flask(__name__)
app.config["_SCHEMA_READY"] = False

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


def resolve_database_uri():
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        if database_url.startswith("postgres://"):
            database_url = "postgresql://" + database_url[len("postgres://"):]
        return database_url

    render_disk_path = os.getenv("RENDER_DISK_PATH", "").strip()
    db_dir = render_disk_path or BASE_DIR
    db_path = os.path.join(db_dir, "finance.db")
    return f"sqlite:///{db_path}"


app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-this")
app.config["SQLALCHEMY_DATABASE_URI"] = resolve_database_uri()
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_pre_ping": True}

if os.getenv("RENDER"):
    app.config["PREFERRED_URL_SCHEME"] = "https"
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

db = SQLAlchemy(app)

# ---------------------
# MODELS
# ---------------------

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    is_admin = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    last_login_at = db.Column(db.DateTime, nullable=True)
    reset_token = db.Column(db.String(120), nullable=True)
    reset_token_expires_at = db.Column(db.DateTime, nullable=True)


class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.String(20), nullable=False)  # asset or liability
    balance = db.Column(db.Float, default=0)
    savings_preference = db.Column(db.String(20), nullable=False, default="auto")
    subtype = db.Column(db.String(40), nullable=False, default="")


class Budget(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    category = db.Column(db.String(100), nullable=False)
    monthly_limit = db.Column(db.Float, nullable=False)


class Debt(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    name = db.Column(db.String(100))
    balance = db.Column(db.Float)
    rate = db.Column(db.Float)


class CategoryRule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    keyword = db.Column(db.String(100), nullable=False)
    category = db.Column(db.String(100), nullable=False)
    priority = db.Column(db.Integer, nullable=False, default=100)
    match_type = db.Column(db.String(20), nullable=False, default="contains")
    amount_direction = db.Column(db.String(20), nullable=False, default="any")


class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.Integer, nullable=False)
    date = db.Column(db.Date, nullable=False)
    description = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    category = db.Column(db.String(100), nullable=False)


class MerchantMemory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    merchant = db.Column(db.String(200), nullable=False)
    category = db.Column(db.String(100), nullable=False)


class FinancialGoal(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(120), nullable=False)
    goal_type = db.Column(db.String(40), nullable=False, default="custom")
    target_amount = db.Column(db.Float, nullable=False)
    current_amount = db.Column(db.Float, nullable=False, default=0)
    target_date = db.Column(db.Date, nullable=True)
    linked_metric = db.Column(db.String(40), nullable=False, default="manual")


class ActivityLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    kind = db.Column(db.String(40), nullable=False, default="general")
    title = db.Column(db.String(140), nullable=False)
    detail = db.Column(db.String(255), nullable=True)
    icon = db.Column(db.String(40), nullable=False, default="bi-stars")
    target_url = db.Column(db.String(160), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


# ---------------------
# HELPERS
# ---------------------

def require_login():
    return "user_id" in session

def get_user_id():
    return session.get("user_id")


def current_user():
    user_id = get_user_id()
    return User.query.get(user_id) if user_id else None


def push_ui_feedback(message, tone="success"):
    session["_ui_feedback"] = {
        "message": message,
        "tone": tone,
    }


@app.context_processor
def inject_shared_ui_state():
    return {
        "ui_feedback": session.pop("_ui_feedback", None)
    }


def require_admin():
    user = current_user()
    return bool(user and user.is_admin)


def normalize_username(username):
    return (username or "").strip()


def find_user_by_username(username):
    normalized = normalize_username(username)
    if not normalized:
        return None
    return User.query.filter(func.lower(User.username) == normalized.lower()).order_by(User.id.asc()).first()


def delete_account_and_transactions(account):
    if not account:
        return
    Transaction.query.filter_by(account_id=account.id).delete()
    db.session.delete(account)


def delete_user_and_related_data(user):
    if not user:
        return
    ActivityLog.query.filter_by(user_id=user.id).delete()
    Transaction.query.filter_by(user_id=user.id).delete()
    Account.query.filter_by(user_id=user.id).delete()
    Budget.query.filter_by(user_id=user.id).delete()
    Debt.query.filter_by(user_id=user.id).delete()
    CategoryRule.query.filter_by(user_id=user.id).delete()
    MerchantMemory.query.filter_by(user_id=user.id).delete()
    FinancialGoal.query.filter_by(user_id=user.id).delete()
    db.session.delete(user)


def log_activity(user_id, title, detail="", kind="general", icon="bi-stars", target_url=None):
    if not user_id or not title:
        return
    db.session.add(ActivityLog(
        user_id=user_id,
        kind=kind,
        title=title[:140],
        detail=(detail or "")[:255],
        icon=icon,
        target_url=target_url,
    ))


def recent_activity_for_user(user_id, limit=8):
    if not user_id:
        return []
    rows = ActivityLog.query.filter_by(user_id=user_id).order_by(ActivityLog.created_at.desc(), ActivityLog.id.desc()).limit(limit).all()
    return [
        {
            "title": row.title,
            "detail": row.detail,
            "icon": row.icon or "bi-stars",
            "target_url": row.target_url,
            "created_at": row.created_at,
        }
        for row in rows
    ]


def build_onboarding_state(accounts, transactions, budgets, goals):
    account_count = len(accounts or [])
    transaction_count = len(transactions or [])
    budget_count = len(budgets or [])
    goal_count = len(goals or [])
    needs_review_count = sum(1 for tx in (transactions or []) if (tx.category or "").strip().lower() == "needs review")

    steps = [
        {
            "label": "Create your first account",
            "detail": "Add a checking, savings, credit card, or loan so AkuOS has a structure to work with.",
            "done": account_count > 0,
            "href": "/accounts",
            "cta": "Open Accounts",
        },
        {
            "label": "Import your first statement",
            "detail": "Bring in transaction history so the dashboard, subscriptions, and budgets become useful.",
            "done": transaction_count > 0,
            "href": "/imports",
            "cta": "Open Import Center",
        },
        {
            "label": "Review categories",
            "detail": "Fix any rows that still need review so merchant memory keeps learning from your choices.",
            "done": transaction_count > 0 and needs_review_count == 0,
            "href": "/review",
            "cta": "Review Categories",
        },
        {
            "label": "Check your dashboard",
            "detail": "Use the dashboard as your day-to-day overview for cash flow, safety, and next steps.",
            "done": transaction_count > 0,
            "href": "/",
            "cta": "View Dashboard",
        },
        {
            "label": "Create your first goal",
            "detail": "Optional, but useful if you want to track an emergency fund, vacation, or debt-free target.",
            "done": goal_count > 0,
            "href": "/goals-wealth",
            "cta": "Add Goal",
        },
    ]

    completed_count = sum(1 for step in steps if step["done"])
    next_step = next((step for step in steps if not step["done"]), None)
    is_first_use = transaction_count == 0 and (account_count <= 1) and budget_count == 0 and goal_count == 0

    return {
        "show": completed_count < len(steps),
        "is_first_use": is_first_use,
        "completed_count": completed_count,
        "total_count": len(steps),
        "steps": steps,
        "next_step": next_step,
        "needs_review_count": needs_review_count,
    }


def suggested_budget_categories(transactions, budgets, limit=5):
    existing_budget_categories = {(budget.category or "").strip().lower() for budget in (budgets or [])}
    category_totals = defaultdict(float)

    for tx in transactions or []:
        category = (tx.category or "").strip()
        if tx.amount >= 0 or not category or category.lower() in GENERIC_CATEGORIES:
            continue
        if not is_spending_category(category):
            continue
        if category.lower() in existing_budget_categories:
            continue
        category_totals[category] += abs(tx.amount)

    return [
        {"category": category, "amount": round(amount, 2)}
        for category, amount in sorted(category_totals.items(), key=lambda item: item[1], reverse=True)[:limit]
    ]


def ensure_db_schema():
    if app.config.get("_SCHEMA_READY"):
        return
    db.create_all()
    inspector = inspect(db.engine)
    if "account" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("account")}
        with db.engine.begin() as conn:
            if "savings_preference" not in columns:
                conn.execute(text("ALTER TABLE account ADD COLUMN savings_preference VARCHAR(20) NOT NULL DEFAULT 'auto'"))
            if "subtype" not in columns:
                conn.execute(text("ALTER TABLE account ADD COLUMN subtype VARCHAR(40) NOT NULL DEFAULT ''"))
    if "user" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("user")}
        with db.engine.begin() as conn:
            if "is_admin" not in columns:
                conn.execute(text("ALTER TABLE user ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT 0"))
            if "created_at" not in columns:
                conn.execute(text("ALTER TABLE user ADD COLUMN created_at DATETIME"))
            if "last_login_at" not in columns:
                conn.execute(text("ALTER TABLE user ADD COLUMN last_login_at DATETIME"))
            if "reset_token" not in columns:
                conn.execute(text("ALTER TABLE user ADD COLUMN reset_token VARCHAR(120)"))
            if "reset_token_expires_at" not in columns:
                conn.execute(text("ALTER TABLE user ADD COLUMN reset_token_expires_at DATETIME"))
    if "category_rule" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("category_rule")}
        with db.engine.begin() as conn:
            if "priority" not in columns:
                conn.execute(text("ALTER TABLE category_rule ADD COLUMN priority INTEGER NOT NULL DEFAULT 100"))
            if "match_type" not in columns:
                conn.execute(text("ALTER TABLE category_rule ADD COLUMN match_type VARCHAR(20) NOT NULL DEFAULT 'contains'"))
            if "amount_direction" not in columns:
                conn.execute(text("ALTER TABLE category_rule ADD COLUMN amount_direction VARCHAR(20) NOT NULL DEFAULT 'any'"))
    app.config["_SCHEMA_READY"] = True

    with db.engine.begin() as conn:
        conn.execute(text("UPDATE user SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"))
        conn.execute(text("UPDATE user SET is_admin = 1 WHERE id = (SELECT id FROM user ORDER BY id ASC LIMIT 1) AND NOT EXISTS (SELECT 1 FROM user WHERE is_admin = 1)"))


@app.before_request
def prepare_schema():
    ensure_db_schema()

def safe_float(val):
    try:
        return float(str(val).replace("$", "").replace(",", "").strip())
    except:
        return None

def parse_date_any(s):
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    try:
        parts = s.replace("-", "/").split("/")
        if len(parts) == 3:
            m, d, y = parts
            y = int(y)
            if y < 100:
                y += 2000
            return date(int(y), int(m), int(d))
    except:
        return None
    return None

def auto_category_for_user(user_id, description, amount):
    return auto_categorize(user_id, description, amount)


def sorted_user_rules(user_id):
    rules = CategoryRule.query.filter_by(user_id=user_id).all()
    return sort_rules(rules)


def bootstrap_merchant_memory(user_id):
    transactions = Transaction.query.filter_by(user_id=user_id).all()
    learned = {}
    for tx in transactions:
        category = (tx.category or "").strip()
        if not category or category.lower() in GENERIC_CATEGORIES:
            continue
        merchant = normalize_text(tx.description)
        if merchant:
            learned[merchant] = category

    for merchant, category in learned.items():
        remember_merchant_category(user_id, merchant, category)


def remember_merchant_category(user_id, description, category):
    normalized = normalize_text(description)
    cleaned_category = (category or "").strip()
    if not normalized or not cleaned_category or cleaned_category.lower() in GENERIC_CATEGORIES:
        return

    memory = MerchantMemory.query.filter_by(user_id=user_id, merchant=normalized).first()
    if memory:
        memory.category = cleaned_category
    else:
        db.session.add(MerchantMemory(user_id=user_id, merchant=normalized, category=cleaned_category))


def categorize_transaction(user_id, description, amount):
    user_rules = sorted_user_rules(user_id)
    memories = MerchantMemory.query.filter_by(user_id=user_id).all()
    return categorize_from_sources(
        description,
        amount,
        user_rules=user_rules,
        merchant_memories=memories
    )


def auto_categorize(user_id, description, amount):
    category, _ = categorize_transaction(user_id, description, amount)
    return category


def get_import_preview_dir():
    path = os.path.join(BASE_DIR, "uploads", "import_previews")
    os.makedirs(path, exist_ok=True)
    return path


def detect_amount_from_row(row):
    return detect_amount_from_row_helper(row, safe_float)


def import_category_choices(user_id):
    categories = set()
    categories.update(r.category for r in CategoryRule.query.filter_by(user_id=user_id).all() if r.category)
    categories.update(b.category for b in Budget.query.filter_by(user_id=user_id).all() if b.category)
    categories.update(m.category for m in MerchantMemory.query.filter_by(user_id=user_id).all() if m.category)
    categories.update(tx.category for tx in Transaction.query.filter_by(user_id=user_id).all() if tx.category)
    categories.update([
        "Income", "Groceries", "Eating Out", "Transport", "Shopping", "Housing",
        "Subscription", "Health", "Entertainment", "Travel", "Utilities", "Other", "Needs Review"
    ])
    return sorted(categories)


def save_import_preview(user_id, payload):
    preview_id = f"{user_id}_{uuid.uuid4().hex}"
    preview_path = os.path.join(get_import_preview_dir(), f"{preview_id}.json")
    with open(preview_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    session["import_preview_id"] = preview_id
    return preview_id


def load_import_preview():
    preview_id = session.get("import_preview_id")
    if not preview_id:
        return None
    preview_path = os.path.join(get_import_preview_dir(), f"{preview_id}.json")
    if not os.path.exists(preview_path):
        session.pop("import_preview_id", None)
        return None
    with open(preview_path, "r", encoding="utf-8") as f:
        return json.load(f)


def clear_import_preview():
    preview_id = session.pop("import_preview_id", None)
    if not preview_id:
        return
    preview_path = os.path.join(get_import_preview_dir(), f"{preview_id}.json")
    if os.path.exists(preview_path):
        os.remove(preview_path)


def transaction_fingerprint(tx_date, description, amount):
    if hasattr(tx_date, "isoformat"):
        date_key = tx_date.isoformat()
    else:
        date_key = str(tx_date)
    amount_key = round(float(amount or 0), 2)
    merchant_key = normalize_text(description)
    return f"{date_key}|{amount_key:.2f}|{merchant_key}"


def existing_transaction_fingerprints(user_id, account_id):
    fingerprints = set()
    account_transactions = Transaction.query.filter_by(user_id=user_id, account_id=account_id).all()
    for tx in account_transactions:
        fingerprints.add(transaction_fingerprint(tx.date, tx.description, tx.amount))
    return fingerprints


PDF_DATE_PATTERN = re.compile(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b")
PDF_AMOUNT_PATTERN = re.compile(r"(?<!\d)(?:\(?-?\$?\d[\d,]*\.\d{2}\)?(?:\s*(?:CR|DR))?)")
PDF_POSITIVE_HINTS = (
    "deposit", "refund", "interest", "credit", "payment received",
    "payroll", "salary", "direct dep", "reversal", "cashback"
)
PDF_NEGATIVE_HINTS = (
    "purchase", "withdrawal", "debit", "pos", "check", "fee",
    "autopay", "card", "payment thank you", "zelle", "venmo",
    "online transfer", "transfer", "rent"
)
PDF_SKIP_LINE_HINTS = (
    "page ", "beginning balance", "ending balance", "available balance",
    "daily balance", "total fees", "customer service", "account number",
    "statement period", "transactions", "description", "balance forward",
)


def normalize_pdf_cell(value):
    return " ".join(str(value or "").split()).strip()


def parse_statement_amount(value, force_sign=None):
    text_value = normalize_pdf_cell(value)
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


def infer_statement_sign(raw_text, amount_token):
    upper_raw = (raw_text or "").lower()
    token = (amount_token or "").upper()
    if "(" in (amount_token or "") or "-" in (amount_token or "") or "DR" in token:
        return "negative"
    if "CR" in token:
        return "positive"
    if any(hint in upper_raw for hint in PDF_POSITIVE_HINTS):
        return "positive"
    if any(hint in upper_raw for hint in PDF_NEGATIVE_HINTS):
        return "negative"
    return "negative"


def is_pdf_noise_line(line):
    cleaned = normalize_pdf_cell(line).lower()
    if not cleaned or len(cleaned) < 6:
        return True
    if any(hint in cleaned for hint in PDF_SKIP_LINE_HINTS):
        return True
    return False


def parse_pdf_line_record(line, source_document, row_index):
    cleaned = normalize_pdf_cell(line)
    if is_pdf_noise_line(cleaned):
        return None

    date_match = PDF_DATE_PATTERN.search(cleaned)
    amount_matches = list(PDF_AMOUNT_PATTERN.finditer(cleaned))
    if not date_match and not amount_matches:
        return None

    parsed_date = parse_date_any(date_match.group(0)) if date_match else None
    chosen_amount_match = amount_matches[0] if amount_matches else None
    amount = None
    if chosen_amount_match:
        sign_hint = infer_statement_sign(cleaned, chosen_amount_match.group(0))
        amount = parse_statement_amount(chosen_amount_match.group(0), force_sign=sign_hint)

    description = ""
    if date_match and chosen_amount_match:
        description = cleaned[date_match.end():chosen_amount_match.start()].strip(" -")
    elif date_match:
        description = cleaned[date_match.end():].strip(" -")
    elif chosen_amount_match:
        description = cleaned[:chosen_amount_match.start()].strip(" -")

    requires_manual_fields = parsed_date is None or not description or amount is None
    if not description and not requires_manual_fields:
        return None

    return {
        "source_document": source_document,
        "raw_source": cleaned,
        "date": parsed_date.isoformat() if parsed_date else "",
        "description": description or cleaned,
        "amount": round(amount, 2) if amount is not None else "",
        "source_category": "",
        "raw_category": "",
        "category": "",
        "category_source": "",
        "fingerprint": f"pdfline|{source_document}|{row_index}|{normalize_text(cleaned)}",
        "requires_manual_fields": requires_manual_fields,
        "manual_reason": "Complete missing date, description, or amount." if requires_manual_fields else "",
        "parser_label": "PDF line parser",
    }


def parse_pdf_table_row_record(cells, source_document, row_index):
    normalized_cells = [normalize_pdf_cell(cell) for cell in cells if normalize_pdf_cell(cell)]
    if len(normalized_cells) < 2:
        return None

    raw_line = " | ".join(normalized_cells)
    if is_pdf_noise_line(raw_line):
        return None

    date_idx = None
    parsed_date = None
    for idx, cell in enumerate(normalized_cells):
        parsed = parse_date_any(cell)
        if parsed:
            date_idx = idx
            parsed_date = parsed
            break

    amount_idx = None
    amount = None
    for idx in range(len(normalized_cells) - 1, -1, -1):
        cell = normalized_cells[idx]
        amount_tokens = PDF_AMOUNT_PATTERN.findall(cell)
        if amount_tokens:
            amount_idx = idx
            token = amount_tokens[0]
            amount = parse_statement_amount(token, force_sign=infer_statement_sign(raw_line, token))
            break
        fallback_amount = safe_float(cell)
        if fallback_amount is not None:
            amount_idx = idx
            amount = -abs(fallback_amount)
            break

    description_parts = []
    for idx, cell in enumerate(normalized_cells):
        if idx == date_idx or idx == amount_idx:
            continue
        description_parts.append(cell)
    description = " ".join(description_parts).strip()
    if not parsed_date and not amount and not description:
        return None

    requires_manual_fields = parsed_date is None or not description or amount is None
    return {
        "source_document": source_document,
        "raw_source": raw_line,
        "date": parsed_date.isoformat() if parsed_date else "",
        "description": description or raw_line,
        "amount": round(amount, 2) if amount is not None else "",
        "source_category": "",
        "raw_category": "",
        "category": "",
        "category_source": "",
        "fingerprint": f"pdftable|{source_document}|{row_index}|{normalize_text(raw_line)}",
        "requires_manual_fields": requires_manual_fields,
        "manual_reason": "Complete missing date, description, or amount." if requires_manual_fields else "",
        "parser_label": "PDF table parser",
    }


def extract_csv_statement_data(file_storage):
    content = file_storage.read().decode("utf-8-sig", errors="ignore")
    reader = csv.DictReader(StringIO(content))
    rows = list(reader)

    if not rows:
        return None, "CSV is empty."

    date_candidates = ["date", "transaction date", "posted date", "posting date", "trans date"]
    desc_candidates = ["description", "name", "merchant", "details", "transaction details", "memo"]
    cat_candidates = ["category", "type"]

    date_key = detect_csv_column(rows[0].keys(), date_candidates)
    desc_key = detect_csv_column(rows[0].keys(), desc_candidates)
    source_category_key = detect_csv_column(rows[0].keys(), cat_candidates)
    _, amount_columns = detect_amount_from_row(rows[0])

    if not date_key or not desc_key or not amount_columns:
        return None, "Could not detect the required Date, Description, and Amount columns."

    extracted_rows = []
    skipped_rows = 0
    for idx, row in enumerate(rows):
        parsed_date = parse_date_any(row.get(date_key))
        description = (row.get(desc_key) or "").strip()
        amount, _ = detect_amount_from_row(row)

        if parsed_date is None or not description or amount is None:
            skipped_rows += 1
            continue

        source_category = (row.get(source_category_key) or "").strip() if source_category_key else ""
        extracted_rows.append({
            "source_document": file_storage.filename or "statement.csv",
            "raw_source": json.dumps(row, ensure_ascii=True),
            "date": parsed_date.isoformat(),
            "description": description,
            "amount": round(amount, 2),
            "source_category": source_category,
            "raw_category": source_category,
            "category": "",
            "category_source": "",
            "fingerprint": transaction_fingerprint(parsed_date, description, amount),
            "requires_manual_fields": False,
            "manual_reason": "",
            "parser_label": "CSV detector",
        })

    if not extracted_rows:
        return None, "No valid transactions were detected in the uploaded CSV."

    return {
        "rows": extracted_rows,
        "skipped_rows": skipped_rows,
        "detected_columns": {
            "date": date_key,
            "description": desc_key,
            "amount": ", ".join([v for v in amount_columns.values() if v]),
            "source_category": source_category_key or "Not provided",
        }
    }, None


def extract_pdf_statement_data(file_storage):
    if pdfplumber is None:
        return None, "PDF import support requires `pdfplumber`. Add it to your environment and try again."

    pdf_bytes = file_storage.read()
    extracted_rows = []
    skipped_rows = 0
    detected_methods = set()
    seen_raw_keys = set()

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                page_tables = page.extract_tables() or []
                for table_index, table in enumerate(page_tables, start=1):
                    for row_index, row in enumerate(table or [], start=1):
                        record = parse_pdf_table_row_record(row or [], file_storage.filename or "statement.pdf", f"{page_index}_{table_index}_{row_index}")
                        if not record:
                            continue
                        raw_key = normalize_text(record["raw_source"])
                        if raw_key in seen_raw_keys:
                            continue
                        seen_raw_keys.add(raw_key)
                        detected_methods.add("Table extraction")
                        extracted_rows.append(record)

                page_text = page.extract_text() or ""
                for line_index, line in enumerate(page_text.splitlines(), start=1):
                    record = parse_pdf_line_record(line, file_storage.filename or "statement.pdf", f"{page_index}_{line_index}")
                    if not record:
                        skipped_rows += 1
                        continue
                    raw_key = normalize_text(record["raw_source"])
                    if raw_key in seen_raw_keys:
                        continue
                    seen_raw_keys.add(raw_key)
                    detected_methods.add("Line extraction")
                    extracted_rows.append(record)
    except Exception:
        return None, f"Could not read {file_storage.filename or 'the PDF'}. Try another statement or convert it to CSV."

    if not extracted_rows:
        return None, f"No transaction-like rows were detected in {file_storage.filename or 'the PDF'}."

    return {
        "rows": extracted_rows,
        "skipped_rows": skipped_rows,
        "detected_columns": {
            "date": "PDF statement detection",
            "description": "PDF statement detection",
            "amount": "PDF statement detection",
            "source_category": "Not provided",
            "parser": ", ".join(sorted(detected_methods)) or "Heuristic parser",
        }
    }, None


def detect_statement_file_type(file_storage):
    filename = (file_storage.filename or "").lower()
    if filename.endswith(".pdf"):
        return "pdf"
    return "csv"


def build_import_preview(user_id, file_storages, account_id):
    if not isinstance(file_storages, list):
        file_storages = [file_storages]

    account_id = int(account_id)
    existing_fingerprints = existing_transaction_fingerprints(user_id, account_id)
    preview_fingerprints = set()
    preview_rows = []
    skipped_rows = 0
    duplicate_existing_count = 0
    duplicate_file_count = 0
    needs_review_count = 0
    ready_count = 0
    manual_fix_count = 0
    source_breakdown = defaultdict(int)
    file_summaries = []
    detected_columns = []
    row_counter = 0

    for file_storage in file_storages:
        file_type = detect_statement_file_type(file_storage)
        extracted, error = (
            extract_pdf_statement_data(file_storage)
            if file_type == "pdf"
            else extract_csv_statement_data(file_storage)
        )
        if error:
            return None, error

        file_rows = extracted["rows"]
        skipped_rows += extracted.get("skipped_rows", 0)
        detected_columns.append({
            "document": file_storage.filename or ("statement.pdf" if file_type == "pdf" else "statement.csv"),
            **extracted.get("detected_columns", {})
        })
        file_summaries.append({
            "name": file_storage.filename or ("statement.pdf" if file_type == "pdf" else "statement.csv"),
            "file_type": file_type.upper(),
            "row_count": len(file_rows),
        })

        for row in file_rows:
            date_value = row.get("date", "")
            description = (row.get("description") or "").strip()
            amount_value = row.get("amount")
            parsed_date = parse_date_any(date_value)
            amount = safe_float(amount_value) if amount_value != "" else None

            fingerprint = row.get("fingerprint") or (
                transaction_fingerprint(parsed_date, description, amount)
                if parsed_date and description and amount is not None
                else f"manual|{row_counter}|{normalize_text(row.get('raw_source'))}"
            )
            is_existing_duplicate = bool(parsed_date and description and amount is not None and fingerprint in existing_fingerprints)
            is_file_duplicate = fingerprint in preview_fingerprints
            preview_fingerprints.add(fingerprint)

            source_category = (row.get("source_category") or "").strip()
            if parsed_date and description and amount is not None and not source_category:
                detected_category, category_source = categorize_transaction(user_id, description, amount)
            else:
                detected_category, category_source = ("Needs Review", "Needs Review")

            if source_category:
                detected_category = source_category
                category_source = "CSV"
            elif category_source == "Fallback":
                detected_category = "Needs Review"
                category_source = "Needs Review"

            requires_manual_fields = row.get("requires_manual_fields", False) or parsed_date is None or not description or amount is None
            review_required = requires_manual_fields or (detected_category or "").strip().lower() in GENERIC_CATEGORIES or category_source == "Needs Review"
            if is_existing_duplicate:
                duplicate_existing_count += 1
                row_status = "Duplicate in account"
                status_tone = "warning"
            elif is_file_duplicate:
                duplicate_file_count += 1
                row_status = "Duplicate in file"
                status_tone = "warning"
            elif requires_manual_fields:
                manual_fix_count += 1
                row_status = "Needs manual correction"
                status_tone = "warning"
            elif review_required:
                needs_review_count += 1
                row_status = "Needs review"
                status_tone = "warning"
            else:
                ready_count += 1
                row_status = "Ready to import"
                status_tone = "positive"

            source_breakdown[category_source] += 1
            preview_rows.append({
                "row_id": row_counter,
                "source_document": row.get("source_document") or (file_storage.filename or ""),
                "parser_label": row.get("parser_label") or file_type.upper(),
                "raw_source": row.get("raw_source") or "",
                "date": date_value,
                "description": description,
                "amount": amount_value,
                "category": detected_category,
                "source_category": source_category,
                "category_source": category_source,
                "row_status": row_status,
                "status_tone": status_tone,
                "is_duplicate": is_existing_duplicate or is_file_duplicate,
                "duplicate_reason": row_status if (is_existing_duplicate or is_file_duplicate) else "",
                "review_required": review_required,
                "requires_manual_fields": requires_manual_fields,
                "manual_reason": row.get("manual_reason", ""),
                "fingerprint": fingerprint,
            })
            row_counter += 1

    if not preview_rows:
        return None, "No valid transactions were detected in the uploaded documents."

    payload = {
        "account_id": account_id,
        "filenames": [summary["name"] for summary in file_summaries],
        "rows": preview_rows,
        "detected_columns": detected_columns,
        "file_summaries": file_summaries,
        "skipped_rows": skipped_rows,
        "summary": {
            "ready_count": ready_count,
            "needs_review_count": needs_review_count,
            "manual_fix_count": manual_fix_count,
            "duplicate_existing_count": duplicate_existing_count,
            "duplicate_file_count": duplicate_file_count,
            "source_breakdown": dict(source_breakdown)
        }
    }
    return payload, None


def build_dashboard_insights(
    transactions,
    selected_month,
    selected_year,
    monthly_income,
    monthly_expenses,
    category_totals,
    subscriptions
):
    insights = []
    now = datetime.now()
    days_in_month = calendar.monthrange(selected_year, selected_month)[1]
    current_day = now.day if now.month == selected_month and now.year == selected_year else days_in_month
    current_day = max(1, min(current_day, days_in_month))

    previous_month = 12 if selected_month == 1 else selected_month - 1
    previous_year = selected_year - 1 if selected_month == 1 else selected_year

    prev_category_totals = defaultdict(float)
    current_daily_by_category = defaultdict(lambda: defaultdict(float))
    prev_daily_by_category = defaultdict(lambda: defaultdict(float))
    current_subscription_total = sum(float(sub.get("average_amount") or 0) for sub in subscriptions)
    recurring_cost = current_subscription_total

    for tx in transactions:
        if not is_spending_transaction(tx):
            continue
        amount = abs(tx.amount)

        if tx.date.month == selected_month and tx.date.year == selected_year:
            current_daily_by_category[tx.category][tx.date.day] += amount
        elif tx.date.month == previous_month and tx.date.year == previous_year:
            prev_category_totals[tx.category] += amount
            prev_daily_by_category[tx.category][tx.date.day] += amount

    current_categories = {k: v for k, v in category_totals.items() if v > 0}
    prev_categories = {k: v for k, v in prev_category_totals.items() if v > 0}

    growth_candidates = []
    for category, current_value in current_categories.items():
        prev_value = prev_categories.get(category, 0)
        if prev_value > 0:
            change_pct = ((current_value - prev_value) / prev_value) * 100
            growth_candidates.append((change_pct, category, current_value, prev_value))

    if growth_candidates:
        growth_candidates.sort(reverse=True)
        top_growth = growth_candidates[0]
        if top_growth[0] >= 8:
            insights.append({
                "title": f"You spent {round(top_growth[0])}% more on {top_growth[1]} this month",
                "detail": f"${top_growth[2]:,.2f} vs ${top_growth[3]:,.2f} last month.",
                "tone": "warning"
            })

    if current_subscription_total > 0:
        insights.append({
            "title": f"Subscriptions total ${current_subscription_total:,.0f}/month" if current_subscription_total >= 100 else f"Subscriptions total ${current_subscription_total:,.2f}/month",
            "detail": "Recurring charges are consuming predictable monthly cash flow.",
            "tone": "neutral"
        })

    pace_savings = 0.0
    if current_day > 0:
        income_run_rate = monthly_income / current_day
        expense_run_rate = monthly_expenses / current_day
        pace_savings = (income_run_rate - expense_run_rate) * days_in_month
        insights.append({
            "title": f"You are on pace to save ${pace_savings:,.2f} this month",
            "detail": f"Based on {current_day} day{'s' if current_day != 1 else ''} of activity this month.",
            "tone": "positive" if pace_savings >= 0 else "warning"
        })

    trend_candidates = []
    for category, current_days in current_daily_by_category.items():
        current_avg = sum(current_days.values()) / max(len(current_days), 1)
        prev_days = prev_daily_by_category.get(category, {})
        prev_avg = (sum(prev_days.values()) / len(prev_days)) if prev_days else 0
        if current_avg > 0:
            trend_candidates.append(((current_avg - prev_avg), category, current_avg, prev_avg))

    trend_candidates.sort(reverse=True)
    if trend_candidates and trend_candidates[0][0] > 0:
        _, trend_category, current_avg, prev_avg = trend_candidates[0]
        insights.append({
            "title": f"{trend_category} is your fastest-growing category",
            "detail": f"Average spend per active day is ${current_avg:,.2f}" + (f", up from ${prev_avg:,.2f} last month." if prev_avg > 0 else "."),
            "tone": "neutral"
        })

    if current_categories:
        top_category = max(current_categories.items(), key=lambda item: item[1])
        share = (top_category[1] / monthly_expenses * 100) if monthly_expenses > 0 else 0
        insights.append({
            "title": f"{top_category[0]} is your largest expense category",
            "detail": f"It represents {share:.0f}% of this month's spending.",
            "tone": "neutral"
        })

    if monthly_income > 0 and monthly_expenses > monthly_income:
        insights.append({
            "title": "You are currently spending more than you earn this month",
            "detail": f"Expenses are ahead by ${(monthly_expenses - monthly_income):,.2f}.",
            "tone": "warning"
        })

    if recurring_cost > 0:
        insights.append({
            "title": f"${recurring_cost:,.2f} of monthly spend looks recurring",
            "detail": "This is a good place to trim fixed costs if you want faster savings growth.",
            "tone": "positive"
        })

    seen_titles = set()
    unique_insights = []
    for insight in insights:
        if insight["title"] in seen_titles:
            continue
        seen_titles.add(insight["title"])
        unique_insights.append(insight)

    return unique_insights[:6]


def build_dashboard_assistant(
    monthly_income,
    monthly_expenses,
    savings_rate,
    budget_rows,
    recurring_transactions,
    dashboard_insights
):
    why_lines = []
    action_items = []

    monthly_net = monthly_income - monthly_expenses
    recurring_cost = sum(abs(r["avg_amount"]) for r in recurring_transactions if r["avg_amount"] < 0)
    over_budget = [b for b in budget_rows if b["pct"] >= 100]
    near_budget = [b for b in budget_rows if 80 <= b["pct"] < 100]

    if monthly_net >= 0:
        why_lines.append(
            f"You are currently cash-flow positive by ${monthly_net:,.2f}, which gives you room to redirect money toward savings, debt payoff, or fixed-cost reductions."
        )
    else:
        why_lines.append(
            f"You are currently cash-flow negative by ${abs(monthly_net):,.2f}, so tightening fast-growing categories will likely have the biggest short-term impact."
        )

    if recurring_cost > 0:
        why_lines.append(
            f"About ${recurring_cost:,.2f} of your monthly spending looks recurring, which means a few subscription or fixed-cost changes could improve results every single month."
        )

    if savings_rate > 0:
        why_lines.append(
            f"Your current savings rate is {savings_rate:.2f}%, which is a useful benchmark for judging whether new spending decisions are strengthening or weakening your plan."
        )

    if over_budget:
        worst = max(over_budget, key=lambda item: item["pct"])
        action_items.append(
            f"Review {worst['category']} first because it is already at {worst['pct']}% of budget."
        )
    elif near_budget:
        watch = max(near_budget, key=lambda item: item["pct"])
        action_items.append(
            f"Slow down {watch['category']} spending because it is already at {watch['pct']}% of budget."
        )

    if recurring_cost > 0:
        action_items.append(
            f"Audit subscriptions and recurring merchants for at least ${recurring_cost:,.2f}/month in fixed-cost pressure."
        )

    if monthly_net > 0:
        action_items.append(
            f"Move part of your current monthly surplus of ${monthly_net:,.2f} into savings or debt payoff before it gets absorbed by variable spending."
        )
    else:
        action_items.append(
            "Pause new discretionary purchases until monthly net turns positive again."
        )

    warning_insight = next((insight for insight in dashboard_insights if insight.get("tone") == "warning"), None)
    if warning_insight:
        action_items.append(
            f"Address this alert next: {warning_insight['title']}."
        )

    if not action_items:
        action_items.append("Keep building transaction history so the dashboard can generate stronger recommendations.")

    return {
        "why_this_matters": why_lines[:3],
        "next_actions": action_items[:4]
    }


def calculate_safe_to_spend(accounts, subscriptions, budget_rows, monthly_expenses, selected_month, selected_year):
    current_cash = sum(max(float(a.balance or 0), 0) for a in accounts if a.type == "asset")

    today = datetime.now()
    days_in_month = calendar.monthrange(selected_year, selected_month)[1]
    current_day = today.day if today.month == selected_month and today.year == selected_year else days_in_month
    current_day = max(1, min(current_day, days_in_month))
    days_remaining = max(days_in_month - current_day, 0)

    remaining_recurring_bills = sum(
        sub["average_amount"]
        for sub in subscriptions
        if sub.get("next_expected_charge")
        and sub["next_expected_charge"].month == selected_month
        and sub["next_expected_charge"].year == selected_year
        and sub["next_expected_charge"].day >= current_day
    )

    remaining_budget_commitments = sum(
        max(float(b["limit"]) - float(b["spent"]), 0)
        for b in budget_rows
    )

    daily_expense_run_rate = monthly_expenses / current_day
    expected_remaining_spending = max((daily_expense_run_rate * days_remaining) - remaining_recurring_bills, 0)

    safe_to_spend = current_cash - remaining_recurring_bills - remaining_budget_commitments - expected_remaining_spending

    return {
        "current_cash": round(current_cash, 2),
        "remaining_recurring_bills": round(remaining_recurring_bills, 2),
        "remaining_budget_commitments": round(remaining_budget_commitments, 2),
        "expected_remaining_spending": round(expected_remaining_spending, 2),
        "safe_to_spend": round(safe_to_spend, 2)
    }


ACCOUNT_SUBTYPE_LABELS = {
    "checking": "Checking / Cash Flow",
    "cash": "Cash",
    "savings": "Savings",
    "investment": "Investment",
    "credit_card": "Credit Card",
    "loan": "Loan",
    "other_asset": "Other Asset",
    "other_liability": "Other Liability",
    "": "Auto detect",
}

ASSET_SUBTYPE_OPTIONS = {"", "checking", "cash", "savings", "investment", "other_asset"}
LIABILITY_SUBTYPE_OPTIONS = {"", "credit_card", "loan", "other_liability"}
ESSENTIAL_CATEGORY_KEYWORDS = {
    "housing",
    "rent",
    "mortgage",
    "grocer",
    "utility",
    "insurance",
    "health",
    "medical",
    "transport",
    "gas",
    "fuel",
    "childcare",
    "debt",
}

GOAL_TYPE_CHOICES = [
    ("emergency_fund", "Emergency Fund"),
    ("car_down_payment", "Car Down Payment"),
    ("investing_milestone", "Investing Milestone"),
    ("debt_free", "Debt-Free Goal"),
    ("vacation_fund", "Vacation Fund"),
    ("custom", "Custom Goal"),
]

GOAL_LINK_CHOICES = [
    ("manual", "Manual progress"),
    ("total_savings", "Link to total savings"),
    ("net_worth", "Link to net worth"),
    ("investments", "Link to investment balances"),
    ("debt_paydown", "Link to debt paid down"),
]


def normalize_account_subtype(subtype, account_type):
    subtype = (subtype or "").strip().lower()
    valid = ASSET_SUBTYPE_OPTIONS if account_type == "asset" else LIABILITY_SUBTYPE_OPTIONS
    return subtype if subtype in valid else ""


def infer_account_subtype(account):
    if not account:
        return ""
    explicit = normalize_account_subtype(getattr(account, "subtype", ""), account.type)
    if explicit:
        return explicit

    name = (account.name or "").lower()
    if account.type == "asset":
        if any(keyword in name for keyword in ("saving", "reserve", "emergency", "hysa", "money market")):
            return "savings"
        if any(keyword in name for keyword in ("brokerage", "invest", "401k", "ira", "roth", "portfolio")):
            return "investment"
        if any(keyword in name for keyword in ("cash", "wallet", "petty cash")):
            return "cash"
        if any(keyword in name for keyword in ("checking", "everyday", "spend", "debit")):
            return "checking"
        return "other_asset"

    if any(keyword in name for keyword in ("card", "visa", "mastercard", "amex", "discover")):
        return "credit_card"
    if any(keyword in name for keyword in ("loan", "mortgage", "student", "auto", "car", "heloc")):
        return "loan"
    return "other_liability"


def subtype_label(account):
    return ACCOUNT_SUBTYPE_LABELS.get(infer_account_subtype(account), "Auto detect")


def savings_target_tiers(monthly_income):
    if monthly_income <= 0:
        return {
            "minimum_amount": None,
            "solid_amount": None,
            "strong_amount": None,
            "recommended_rate": None,
            "recommended_amount": None,
            "recommended_label": "No income data",
        }

    minimum_amount = monthly_income * 0.10
    solid_amount = monthly_income * 0.15
    strong_amount = monthly_income * 0.20
    if monthly_income < 4000:
        recommended_rate = 10
        recommended_amount = minimum_amount
        recommended_label = "Minimum target"
    elif monthly_income < 8000:
        recommended_rate = 15
        recommended_amount = solid_amount
        recommended_label = "Solid target"
    else:
        recommended_rate = 20
        recommended_amount = strong_amount
        recommended_label = "Strong target"

    return {
        "minimum_amount": round(minimum_amount, 2),
        "solid_amount": round(solid_amount, 2),
        "strong_amount": round(strong_amount, 2),
        "recommended_rate": recommended_rate,
        "recommended_amount": round(recommended_amount, 2),
        "recommended_label": recommended_label,
    }


SAVINGS_ACCOUNT_KEYWORDS = {
    "savings",
    "saving",
    "hysa",
    "high yield",
    "money market",
    "reserve",
    "emergency",
    "rainy day"
}


SAVINGS_BEHAVIOR_KEYWORDS = {
    "transfer",
    "payment",
    "autopay",
    "thank you",
    "interest",
    "deposit",
    "reserve",
    "save",
    "zelle",
    "venmo",
    "paypal",
    "cash app"
}

SAVINGS_PREFERENCES = {"auto", "include", "exclude"}


def normalize_savings_preference(value):
    value = (value or "auto").strip().lower()
    return value if value in SAVINGS_PREFERENCES else "auto"


def savings_account_profile(account, account_transactions):
    if not account or account.type != "asset":
        return {
            "is_savings": False,
            "score": 0,
            "confidence": "Low confidence",
            "reasons": [],
            "account": account
        }

    score = 0
    reasons = []
    name = (account.name or "").lower()
    inferred_subtype = infer_account_subtype(account)
    if inferred_subtype == "savings":
        score += 5
        reasons.append("account is classified as savings")
    elif inferred_subtype == "investment":
        score += 1
        reasons.append("account is classified as investment")
    elif inferred_subtype in {"checking", "cash"}:
        score -= 2

    name_match = any(keyword in name for keyword in SAVINGS_ACCOUNT_KEYWORDS)
    if name_match:
        score += 4
        reasons.append("name suggests savings")

    total_transactions = len(account_transactions)
    if total_transactions:
        active_months = max(1, len({(tx.date.year, tx.date.month) for tx in account_transactions}))
        avg_transactions_per_month = total_transactions / active_months
        spending_transactions = [tx for tx in account_transactions if is_spending_transaction(tx)]
        spending_ratio = len(spending_transactions) / total_transactions

        transfer_like_transactions = 0
        for tx in account_transactions:
            normalized_desc = normalize_text(tx.description)
            if not is_spending_category(tx.category) or any(keyword in normalized_desc for keyword in SAVINGS_BEHAVIOR_KEYWORDS):
                transfer_like_transactions += 1
        transfer_ratio = transfer_like_transactions / total_transactions

        if spending_ratio == 0:
            score += 3
            reasons.append("no spending transactions detected")
        elif spending_ratio <= 0.15:
            score += 2
            reasons.append("very little spending activity")
        elif spending_ratio >= 0.45:
            score -= 3

        if transfer_ratio >= 0.7:
            score += 3
            reasons.append("mostly transfer/deposit activity")
        elif transfer_ratio >= 0.5:
            score += 2
            reasons.append("transfer-heavy activity")

        if avg_transactions_per_month <= 8:
            score += 1
            reasons.append("low transaction volume")
        elif avg_transactions_per_month >= 18:
            score -= 1

    if float(account.balance or 0) > 0:
        score += 1
        reasons.append("positive stored balance")

    is_savings = score >= 5 or (name_match and score >= 3)
    if score >= 7:
        confidence = "High confidence"
    elif score >= 5:
        confidence = "Moderate confidence"
    else:
        confidence = "Low confidence"

    return {
        "is_savings": is_savings,
        "score": score,
        "confidence": confidence,
        "reasons": reasons[:3],
        "account": account,
        "subtype": inferred_subtype,
    }


def resolve_savings_account_profile(account, account_transactions):
    profile = savings_account_profile(account, account_transactions)
    preference = normalize_savings_preference(getattr(account, "savings_preference", "auto"))

    if account.type != "asset":
        profile.update({
            "is_savings": False,
            "confidence": "Not applicable",
            "reasons": ["Liability accounts are excluded from savings tracking."],
            "preference": preference,
            "detection_mode": "not_applicable",
        })
        return profile

    if preference == "include":
        reasons = ["Marked as savings by you."]
        reasons.extend(profile["reasons"][:2])
        profile.update({
            "is_savings": True,
            "score": max(profile["score"], 9),
            "confidence": "User confirmed",
            "reasons": reasons[:3],
            "preference": preference,
            "detection_mode": "manual_include",
        })
        return profile

    if preference == "exclude":
        profile.update({
            "is_savings": False,
            "confidence": "Excluded",
            "reasons": ["Excluded from savings tracking by you."],
            "preference": preference,
            "detection_mode": "manual_exclude",
        })
        return profile

    profile.update({
        "preference": preference,
        "detection_mode": "auto",
    })
    return profile


def build_savings_profiles(accounts, transactions):
    tx_by_account = defaultdict(list)
    for tx in transactions:
        tx_by_account[tx.account_id].append(tx)

    profiles = []
    for account in accounts:
        profile = resolve_savings_account_profile(account, tx_by_account.get(account.id, []))
        profiles.append(profile)

    profiles.sort(key=lambda profile: (not profile["is_savings"], -profile["score"], profile["account"].name.lower()))
    return profiles


def detect_savings_accounts(accounts, transactions):
    return [profile for profile in build_savings_profiles(accounts, transactions) if profile["is_savings"]]


def recommended_savings_rate(monthly_income):
    return savings_target_tiers(monthly_income)["recommended_rate"]


def calculate_savings_snapshot(accounts, transactions, selected_month, selected_year, monthly_income, monthly_expenses):
    savings_profiles = build_savings_profiles(accounts, transactions)
    detected_savings_accounts = [profile for profile in savings_profiles if profile["is_savings"]]
    savings_accounts = [profile["account"] for profile in detected_savings_accounts]
    savings_account_ids = {account.id for account in savings_accounts}
    current_savings = sum(max(float(account.balance or 0), 0) for account in savings_accounts)

    monthly_inflows = 0.0
    monthly_outflows = 0.0
    for tx in transactions:
        if tx.account_id not in savings_account_ids:
            continue
        if tx.date.month != selected_month or tx.date.year != selected_year:
            continue
        if tx.amount > 0:
            monthly_inflows += tx.amount
        elif tx.amount < 0:
            monthly_outflows += abs(tx.amount)

    net_monthly_contribution = monthly_inflows - monthly_outflows
    target_tiers = savings_target_tiers(monthly_income)
    target_rate = target_tiers["recommended_rate"]
    recommended_amount = target_tiers["recommended_amount"]
    savings_coverage_months = (current_savings / monthly_expenses) if monthly_expenses > 0 else None

    if not savings_accounts and recommended_amount is None:
        message = "Savings tracking will improve as the app learns which asset accounts behave like savings and as income history grows."
        status = "neutral"
    elif not savings_accounts:
        message = f"No likely savings account was detected yet. The app is looking for asset accounts with transfer-heavy, low-spending behavior compared with a ${recommended_amount:,.2f} monthly target."
        status = "warning"
    elif recommended_amount is None:
        message = f"You currently have ${current_savings:,.2f} in savings, but there is not enough income data yet to calculate a recommended monthly target."
        status = "neutral"
    elif net_monthly_contribution >= recommended_amount * 1.05:
        message = f"You are exceeding your recommended monthly savings target by ${net_monthly_contribution - recommended_amount:,.2f} this month."
        status = "positive"
    elif net_monthly_contribution >= recommended_amount * 0.9:
        message = f"You are meeting your recommended monthly savings target for {calendar.month_name[selected_month]}."
        status = "positive"
    else:
        message = f"You are currently ${max(recommended_amount - net_monthly_contribution, 0):,.2f} below your recommended monthly savings target."
        status = "warning"

    return {
        "account_count": len(savings_accounts),
        "account_names": [account.name for account in savings_accounts],
        "manual_included_count": sum(1 for profile in detected_savings_accounts if profile["detection_mode"] == "manual_include"),
        "manual_excluded_count": sum(1 for profile in savings_profiles if profile["detection_mode"] == "manual_exclude"),
        "auto_detected_count": sum(1 for profile in detected_savings_accounts if profile["detection_mode"] == "auto"),
        "account_detections": [
            {
                "name": profile["account"].name,
                "confidence": profile["confidence"],
                "reasons": profile["reasons"],
                "mode": profile["detection_mode"]
            }
            for profile in detected_savings_accounts
        ],
        "current_savings": round(current_savings, 2),
        "monthly_inflows": round(monthly_inflows, 2),
        "monthly_outflows": round(monthly_outflows, 2),
        "monthly_contribution": round(net_monthly_contribution, 2),
        "minimum_target": target_tiers["minimum_amount"],
        "solid_target": target_tiers["solid_amount"],
        "strong_target": target_tiers["strong_amount"],
        "recommended_label": target_tiers["recommended_label"],
        "recommended_rate": target_rate,
        "recommended_amount": round(recommended_amount, 2) if recommended_amount is not None else None,
        "coverage_months": round(savings_coverage_months, 1) if savings_coverage_months is not None else None,
        "message": message,
        "status": status
    }


def build_net_worth_breakdown(accounts):
    asset_groups = {
        "cash_checking": 0.0,
        "savings": 0.0,
        "investments": 0.0,
        "other_assets": 0.0,
    }
    liability_groups = {
        "credit_cards": 0.0,
        "loans": 0.0,
        "other_liabilities": 0.0,
    }

    for account in accounts:
        balance = float(account.balance or 0)
        subtype = infer_account_subtype(account)
        if account.type == "asset":
            if subtype in {"checking", "cash"}:
                asset_groups["cash_checking"] += balance
            elif subtype == "savings":
                asset_groups["savings"] += balance
            elif subtype == "investment":
                asset_groups["investments"] += balance
            else:
                asset_groups["other_assets"] += balance
        else:
            if subtype == "credit_card":
                liability_groups["credit_cards"] += balance
            elif subtype == "loan":
                liability_groups["loans"] += balance
            else:
                liability_groups["other_liabilities"] += balance

    total_assets = sum(asset_groups.values())
    total_liabilities = sum(liability_groups.values())
    return {
        "asset_groups": {key: round(value, 2) for key, value in asset_groups.items()},
        "liability_groups": {key: round(value, 2) for key, value in liability_groups.items()},
        "total_assets": round(total_assets, 2),
        "total_liabilities": round(total_liabilities, 2),
        "net_worth": round(total_assets - total_liabilities, 2),
        "investment_total": round(asset_groups["investments"], 2),
    }


def estimate_essential_monthly_expenses(category_totals, monthly_expenses):
    essential_total = 0.0
    matched_categories = []
    for category, amount in (category_totals or {}).items():
        normalized = (category or "").lower()
        if any(keyword in normalized for keyword in ESSENTIAL_CATEGORY_KEYWORDS):
            essential_total += float(amount or 0)
            matched_categories.append(category)

    if essential_total > 0:
        return round(essential_total, 2), matched_categories, "category_estimate"
    if monthly_expenses > 0:
        return round(monthly_expenses * 0.6, 2), [], "fallback_estimate"
    return None, [], "insufficient_data"


def net_worth_trend_summary(nw_values):
    if not nw_values or len(nw_values) < 2:
        return {"delta": None, "percent": None, "direction": "flat"}
    first_value = float(nw_values[0] or 0)
    last_value = float(nw_values[-1] or 0)
    delta = last_value - first_value
    percent = ((delta / abs(first_value)) * 100) if first_value else None
    direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
    return {
        "delta": round(delta, 2),
        "percent": round(percent, 1) if percent is not None else None,
        "direction": direction,
    }


def resolve_goal_current_amount(goal, wealth_context):
    linked_metric = (goal.linked_metric or "manual").strip().lower()
    if linked_metric == "total_savings":
        return float(wealth_context["savings_snapshot"]["current_savings"] or 0), "Linked to total savings"
    if linked_metric == "net_worth":
        return float(wealth_context["net_worth_breakdown"]["net_worth"] or 0), "Linked to net worth"
    if linked_metric == "investments":
        return float(wealth_context["net_worth_breakdown"]["investment_total"] or 0), "Linked to investment balances"
    if linked_metric == "debt_paydown":
        target_amount = float(goal.target_amount or 0)
        liabilities = float(wealth_context["net_worth_breakdown"]["total_liabilities"] or 0)
        return max(target_amount - liabilities, 0), "Linked to debt paid down"
    return float(goal.current_amount or 0), "Manual progress"


def build_goal_progress(goals, wealth_context):
    goal_rows = []
    total_progress_ratio = 0.0
    for goal in goals:
        current_amount, source_label = resolve_goal_current_amount(goal, wealth_context)
        target_amount = max(float(goal.target_amount or 0), 0)
        progress_pct = min(100.0, (current_amount / target_amount) * 100) if target_amount > 0 else 0
        gap_remaining = max(target_amount - current_amount, 0)
        goal_rows.append({
            "id": goal.id,
            "name": goal.name,
            "goal_type": goal.goal_type,
            "target_amount": round(target_amount, 2),
            "current_amount": round(current_amount, 2),
            "target_date": goal.target_date,
            "linked_metric": goal.linked_metric,
            "source_label": source_label,
            "progress_pct": round(progress_pct, 1),
            "gap_remaining": round(gap_remaining, 2),
        })
        if target_amount > 0:
            total_progress_ratio += min(current_amount / target_amount, 1.0)

    average_progress = (total_progress_ratio / len(goal_rows)) if goal_rows else None
    return goal_rows, average_progress


def build_wealth_snapshot(accounts, transactions, goals, selected_month, selected_year, monthly_income, monthly_expenses, category_totals, savings_snapshot, nw_values):
    net_worth_breakdown = build_net_worth_breakdown(accounts)
    essential_expenses, essential_categories, emergency_source = estimate_essential_monthly_expenses(category_totals, monthly_expenses)
    current_savings = float(savings_snapshot["current_savings"] or 0)
    emergency_fund_months = (current_savings / essential_expenses) if essential_expenses and essential_expenses > 0 else None
    target_3_month = (essential_expenses * 3) if essential_expenses else None
    target_6_month = (essential_expenses * 6) if essential_expenses else None
    emergency_progress_pct = min(100.0, (current_savings / target_6_month) * 100) if target_6_month else None
    wealth_context = {
        "savings_snapshot": savings_snapshot,
        "net_worth_breakdown": net_worth_breakdown,
    }
    goal_rows, average_goal_progress = build_goal_progress(goals, wealth_context)
    trend = net_worth_trend_summary(nw_values)

    wealth_score_summary = compute_wealth_score({
        "monthly_income": monthly_income,
        "monthly_expenses": monthly_expenses,
        "savings_rate": ((monthly_income - monthly_expenses) / monthly_income * 100) if monthly_income > 0 else 0,
        "current_savings": current_savings,
        "recommended_savings_amount": savings_snapshot["recommended_amount"],
        "emergency_fund_months": emergency_fund_months,
        "net_worth": net_worth_breakdown["net_worth"],
        "net_worth_trend_delta": trend["delta"],
        "total_liabilities": net_worth_breakdown["total_liabilities"],
        "investment_total": net_worth_breakdown["investment_total"],
        "goal_progress_ratio": average_goal_progress,
    })

    if savings_snapshot["recommended_amount"] is None:
        recommendation = "Add or import income data to unlock a more specific monthly savings target."
    elif savings_snapshot["monthly_contribution"] < savings_snapshot["recommended_amount"]:
        recommendation = f"Increasing monthly savings by ${max(savings_snapshot['recommended_amount'] - savings_snapshot['monthly_contribution'], 0):,.0f} would improve your wealth score."
    elif emergency_fund_months is not None and emergency_fund_months < 3:
        recommendation = f"Your emergency fund covers {emergency_fund_months:.1f} months. Building toward 3 months would strengthen your wealth buffer."
    elif goal_rows:
        next_goal = min((goal for goal in goal_rows if goal["gap_remaining"] > 0), key=lambda goal: goal["gap_remaining"], default=None)
        recommendation = f"Your next easiest win is closing the remaining ${next_goal['gap_remaining']:,.0f} on {next_goal['name']}." if next_goal else "Your tracked goals are on pace right now."
    else:
        recommendation = "Set at least one financial goal so the app can turn your savings and net-worth progress into clearer milestones."

    guidance = []
    if savings_snapshot["recommended_amount"] is not None:
        if savings_snapshot["monthly_contribution"] >= savings_snapshot["recommended_amount"]:
            guidance.append("You are meeting or exceeding your recommended savings target this month.")
        else:
            guidance.append("You are below your recommended savings target this month.")
    if emergency_fund_months is not None:
        guidance.append(f"Your emergency fund covers {emergency_fund_months:.1f} months of essential expenses.")
    if trend["delta"] is not None:
        direction_text = "up" if trend["delta"] > 0 else "down" if trend["delta"] < 0 else "flat"
        guidance.append(f"Your net worth trend is {direction_text} by ${abs(trend['delta']):,.0f} across the available history.")
    if goal_rows:
        leading_goal = max(goal_rows, key=lambda goal: goal["progress_pct"])
        guidance.append(f"Your strongest goal progress is {leading_goal['name']} at {leading_goal['progress_pct']:.0f}% complete.")

    return {
        "net_worth_breakdown": net_worth_breakdown,
        "essential_expenses": essential_expenses,
        "essential_categories": essential_categories,
        "emergency_source": emergency_source,
        "emergency_fund_months": round(emergency_fund_months, 1) if emergency_fund_months is not None else None,
        "target_3_month": round(target_3_month, 2) if target_3_month is not None else None,
        "target_6_month": round(target_6_month, 2) if target_6_month is not None else None,
        "emergency_progress_pct": round(emergency_progress_pct, 1) if emergency_progress_pct is not None else None,
        "goal_rows": goal_rows,
        "goal_count": len(goal_rows),
        "wealth_score": wealth_score_summary,
        "wealth_recommendation": recommendation,
        "guidance": guidance[:4],
        "net_worth_trend": trend,
    }


def compute_net_worth_history(accounts, transactions):
    networth_by_date = {}
    running_balances = {a.id: 0 for a in accounts}

    for tx in transactions:
        running_balances[tx.account_id] += tx.amount
        total_assets_running = sum(running_balances[a.id] for a in accounts if a.type == "asset")
        total_liabilities_running = sum(running_balances[a.id] for a in accounts if a.type == "liability")
        networth_by_date[tx.date.isoformat()] = total_assets_running - total_liabilities_running

    labels = list(networth_by_date.keys())
    values = list(networth_by_date.values())
    if len(labels) > 60:
        step = math.ceil(len(labels) / 60)
        sampled_points = list(zip(labels, values))[::step]
        if sampled_points and sampled_points[-1][0] != labels[-1]:
            sampled_points.append((labels[-1], values[-1]))
        labels = [label for label, _ in sampled_points]
        values = [value for _, value in sampled_points]
    return labels, values


def summarize_monthly_finances(transactions, selected_month, selected_year):
    category_totals = defaultdict(float)
    prev_category_totals = defaultdict(float)
    monthly_income = 0.0
    monthly_expenses = 0.0
    prev_monthly_income = 0.0
    prev_monthly_expenses = 0.0

    previous_month = 12 if selected_month == 1 else selected_month - 1
    previous_year = selected_year - 1 if selected_month == 1 else selected_year

    for tx in transactions:
        if tx.date.month == selected_month and tx.date.year == selected_year:
            if tx.amount > 0:
                monthly_income += tx.amount
            elif is_spending_category(tx.category):
                monthly_expenses += abs(tx.amount)
                category_totals[tx.category] += abs(tx.amount)
        elif tx.date.month == previous_month and tx.date.year == previous_year:
            if tx.amount > 0:
                prev_monthly_income += tx.amount
            elif is_spending_category(tx.category):
                prev_monthly_expenses += abs(tx.amount)
                prev_category_totals[tx.category] += abs(tx.amount)

    return {
        "monthly_income": round(monthly_income, 2),
        "monthly_expenses": round(monthly_expenses, 2),
        "category_totals": category_totals,
        "prev_category_totals": prev_category_totals,
        "prev_monthly_income": round(prev_monthly_income, 2),
        "prev_monthly_expenses": round(prev_monthly_expenses, 2),
    }


def median_value(values):
    ordered = sorted(values)
    if not ordered:
        return 0
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def subscription_interval_metrics(intervals):
    if not intervals:
        return {
            "avg_interval": 0,
            "median_interval": 0,
            "monthly_hit_ratio": 0,
            "timing_stability_days": 0,
            "interval_score": 0
        }

    avg_interval = sum(intervals) / len(intervals)
    median_interval = median_value(intervals)
    monthly_hits = [gap for gap in intervals if 21 <= gap <= 40]
    monthly_hit_ratio = len(monthly_hits) / len(intervals)
    timing_stability_days = sum(abs(gap - median_interval) for gap in intervals) / len(intervals)

    closeness_penalty = min(abs(median_interval - 30) / 20, 1)
    variability_penalty = min(timing_stability_days / 12, 1)
    interval_score = max(
        0,
        (monthly_hit_ratio * 0.65) + ((1 - closeness_penalty) * 0.2) + ((1 - variability_penalty) * 0.15)
    )

    return {
        "avg_interval": avg_interval,
        "median_interval": median_interval,
        "monthly_hit_ratio": monthly_hit_ratio,
        "timing_stability_days": timing_stability_days,
        "interval_score": interval_score
    }


def subscription_amount_metrics(amounts):
    if not amounts:
        return {
            "average_amount": 0,
            "median_amount": 0,
            "amount_tolerance_pct": 0,
            "stable_amount_ratio": 0,
            "amount_score": 0
        }

    avg_amount = sum(amounts) / len(amounts)
    median_amount = median_value(amounts)
    if median_amount <= 0:
        return {
            "average_amount": avg_amount,
            "median_amount": median_amount,
            "amount_tolerance_pct": 0,
            "stable_amount_ratio": 0,
            "amount_score": 0
        }

    pct_diffs = [abs(amount - median_amount) / median_amount for amount in amounts]
    amount_tolerance_pct = (sum(pct_diffs) / len(pct_diffs)) * 100
    stable_amount_ratio = sum(1 for diff in pct_diffs if diff <= 0.18) / len(pct_diffs)
    tolerance_penalty = min(amount_tolerance_pct / 35, 1)
    amount_score = max(0, (stable_amount_ratio * 0.7) + ((1 - tolerance_penalty) * 0.3))

    return {
        "average_amount": avg_amount,
        "median_amount": median_amount,
        "amount_tolerance_pct": amount_tolerance_pct,
        "stable_amount_ratio": stable_amount_ratio,
        "amount_score": amount_score
    }


def analyze_subscriptions(transactions):
    merchant_groups = defaultdict(list)

    for tx in transactions:
        if not is_spending_transaction(tx):
            continue
        key = normalize_text(tx.description)
        merchant_groups[key].append(tx)

    subscriptions = []
    today = date.today()

    for merchant, tx_list in merchant_groups.items():
        if len(tx_list) < 2:
            continue

        tx_list.sort(key=lambda x: x.date)
        intervals = [(tx_list[i].date - tx_list[i - 1].date).days for i in range(1, len(tx_list))]
        amounts = [abs(t.amount) for t in tx_list]
        interval_metrics = subscription_interval_metrics(intervals)
        amount_metrics = subscription_amount_metrics(amounts)
        count_score = min(len(tx_list) / 4, 1)
        confidence_score = (
            interval_metrics["interval_score"] * 0.5
            + amount_metrics["amount_score"] * 0.35
            + count_score * 0.15
        )

        if interval_metrics["monthly_hit_ratio"] < 0.6 or confidence_score < 0.55:
            continue

        avg_amount = amount_metrics["average_amount"]
        avg_interval = interval_metrics["avg_interval"]
        median_interval = interval_metrics["median_interval"] or avg_interval
        last_charge = tx_list[-1].date
        next_charge = last_charge + timedelta(days=max(1, round(median_interval or avg_interval or 30)))
        latest_amount = amounts[-1]
        baseline_amounts = amounts[:-1]
        baseline_average = (sum(baseline_amounts) / len(baseline_amounts)) if baseline_amounts else avg_amount
        price_increase_pct = ((latest_amount - baseline_average) / baseline_average * 100) if baseline_average > 0 else 0
        has_price_increase = len(tx_list) >= 3 and price_increase_pct >= 8
        overdue_days = (today - next_charge).days
        cancel_candidate = overdue_days > max(10, round((median_interval or 30) * 0.5))

        flags = []
        if has_price_increase:
            flags.append("Price increase")
        if cancel_candidate:
            flags.append("Cancel candidate")

        if confidence_score >= 0.82:
            confidence_label = "High confidence"
        elif confidence_score >= 0.67:
            confidence_label = "Moderate confidence"
        else:
            confidence_label = "Emerging pattern"

        subscriptions.append({
            "name": merchant.title(),
            "average_amount": round(avg_amount, 2),
            "occurrences": len(tx_list),
            "estimated_yearly_cost": round(avg_amount * 12, 2),
            "next_expected_charge": next_charge,
            "last_charge": last_charge,
            "avg_interval_days": round(avg_interval, 1),
            "median_interval_days": round(median_interval, 1),
            "monthly_hit_ratio": round(interval_metrics["monthly_hit_ratio"] * 100, 1),
            "timing_stability_days": round(interval_metrics["timing_stability_days"], 1),
            "latest_amount": round(latest_amount, 2),
            "baseline_amount": round(baseline_average, 2),
            "price_increase_pct": round(max(price_increase_pct, 0), 1),
            "has_price_increase": has_price_increase,
            "cancel_candidate": cancel_candidate,
            "overdue_days": max(overdue_days, 0),
            "amount_tolerance_pct": round(amount_metrics["amount_tolerance_pct"], 1),
            "stable_amount_ratio": round(amount_metrics["stable_amount_ratio"] * 100, 1),
            "confidence_score": round(confidence_score * 100, 1),
            "confidence_label": confidence_label,
            "flags": flags
        })

    subscriptions.sort(key=lambda sub: (sub["confidence_score"], sub["average_amount"]), reverse=True)
    return subscriptions


def build_finance_ai_response(question, snapshot):
    q = normalize_text(question)
    if not q:
        return None

    monthly_income = snapshot["monthly_income"]
    monthly_expenses = snapshot["monthly_expenses"]
    monthly_net = monthly_income - monthly_expenses
    savings_rate = snapshot["savings_rate"]
    category_totals = snapshot["category_totals"]
    prev_category_totals = snapshot["prev_category_totals"]
    subscriptions = snapshot["subscriptions"]
    debts = snapshot["debts"]
    net_worth = snapshot["net_worth"]
    total_assets = snapshot["total_assets"]
    total_liabilities = snapshot["total_liabilities"]
    budgets = snapshot["budget_rows"]
    pace_savings = snapshot["pace_savings"]
    days_in_month = snapshot["days_in_month"]
    current_day = snapshot["current_day"]
    safe_to_spend = snapshot["safe_to_spend"]

    def top_category_pair():
        if not category_totals:
            return None
        return max(category_totals.items(), key=lambda item: item[1])

    def affordability_answer():
        top_subs = subscriptions[:3]
        bullets = [
            f"Monthly income is ${monthly_income:,.2f} and monthly expenses are ${monthly_expenses:,.2f}, leaving a current monthly net of ${monthly_net:,.2f}.",
            f"Projected savings pace for this month is ${pace_savings:,.2f} based on {current_day} of {days_in_month} days logged.",
            f"Estimated safe-to-spend right now is ${safe_to_spend['safe_to_spend']:,.2f} after cash, recurring bills, budgets, and expected remaining spending are considered."
        ]
        if total_liabilities > 0:
            bullets.append(f"Liabilities total ${total_liabilities:,.2f}, so new fixed payments would stack on top of existing debt pressure.")
        if top_subs:
            bullets.append(f"Recurring subscriptions already consume about ${sum(s['average_amount'] for s in top_subs):,.2f}/month across your top tracked merchants.")

        if monthly_net > 0:
            recommendation = "You can afford new spending only if the new monthly payment stays comfortably below your current monthly surplus and does not push savings backward."
        else:
            recommendation = "A new monthly payment looks risky right now because your current monthly surplus is not strong enough to absorb it."

        return {
            "title": "Can you afford it right now?",
            "summary": recommendation,
            "bullets": bullets,
            "follow_up": "Use Financial Planning to test a specific payment amount against your current surplus."
        }

    def cutback_answer():
        sorted_categories = sorted(category_totals.items(), key=lambda item: item[1], reverse=True)
        bullets = []
        for category, amount in sorted_categories[:3]:
            prev_amount = prev_category_totals.get(category, 0)
            if prev_amount > 0:
                change_pct = ((amount - prev_amount) / prev_amount) * 100
                bullets.append(f"{category}: ${amount:,.2f} this month ({change_pct:+.0f}% vs last month).")
            else:
                bullets.append(f"{category}: ${amount:,.2f} this month.")
        recurring_cost = sum(s["average_amount"] for s in subscriptions)
        if recurring_cost > 0:
            bullets.append(f"Subscriptions and recurring charges look like about ${recurring_cost:,.2f}/month of fixed outflow.")
        summary = "The best cutback targets are the categories taking the most dollars right now, especially if they are also rising month over month."
        follow = "Start with the top category above, then review subscriptions because fixed monthly costs compound fastest."
        return {"title": "Where can you cut back?", "summary": summary, "bullets": bullets or ["Add more transactions to identify the clearest cutback targets."], "follow_up": follow}

    def hurting_answer():
        top_pair = top_category_pair()
        if top_pair:
            category, amount = top_pair
            share = (amount / monthly_expenses * 100) if monthly_expenses > 0 else 0
            summary = f"{category} is hurting you most right now because it is your largest spending category at ${amount:,.2f}, about {share:.0f}% of total monthly expenses."
        else:
            summary = "There is not enough spending data yet to identify a damaging category."
        bullets = []
        growth = sorted(
            [((amount - prev_category_totals.get(cat, 0)), cat, amount, prev_category_totals.get(cat, 0)) for cat, amount in category_totals.items()],
            reverse=True
        )
        for delta, cat, amount, prev in growth[:3]:
            if amount <= 0:
                continue
            if prev > 0:
                bullets.append(f"{cat}: ${amount:,.2f} this month vs ${prev:,.2f} last month.")
            else:
                bullets.append(f"{cat}: ${amount:,.2f} this month.")
        return {"title": "What category is hurting you most?", "summary": summary, "bullets": bullets, "follow_up": "If you want the fastest improvement, reduce the largest category before optimizing smaller ones."}

    def subscription_answer():
        total_monthly = sum(s["average_amount"] for s in subscriptions)
        bullets = []
        for sub in subscriptions[:4]:
            detail = f"{sub['name']}: ${sub['average_amount']:,.2f}/month, next around {sub['next_expected_charge'].strftime('%b %d')}."
            if sub["has_price_increase"]:
                detail += f" Price appears up {sub['price_increase_pct']:.1f}%."
            if sub["cancel_candidate"]:
                detail += f" Expected charge overdue by {sub['overdue_days']} days."
            bullets.append(detail)
        summary = f"Tracked subscriptions and recurring merchants total about ${total_monthly:,.2f} per month." if total_monthly > 0 else "No recurring subscriptions were confidently detected yet."
        return {"title": "How much are subscriptions costing you?", "summary": summary, "bullets": bullets or ["Upload or categorize more recurring charges to strengthen subscription detection."], "follow_up": "Review any price increases first, then decide which recurring costs still justify their monthly impact."}

    def safe_to_spend_answer():
        bullets = [
            f"Current cash is estimated at ${safe_to_spend['current_cash']:,.2f}.",
            f"Remaining recurring bills this month: ${safe_to_spend['remaining_recurring_bills']:,.2f}.",
            f"Remaining budget commitments: ${safe_to_spend['remaining_budget_commitments']:,.2f}.",
            f"Expected remaining variable spending: ${safe_to_spend['expected_remaining_spending']:,.2f}."
        ]
        summary = f"Safe-to-spend is about ${safe_to_spend['safe_to_spend']:,.2f} right now." if safe_to_spend["safe_to_spend"] >= 0 else f"You are about ${abs(safe_to_spend['safe_to_spend']):,.2f} past a comfortable safe-to-spend buffer right now."
        return {"title": "How much is safe to spend?", "summary": summary, "bullets": bullets, "follow_up": "Use this number as a ceiling for discretionary spending unless new income arrives or expected bills change."}

    def debt_answer():
        if not debts:
            return {
                "title": "How does debt affect the picture?",
                "summary": "No debts are stored yet, so the app cannot weigh debt drag accurately.",
                "bullets": ["Add your debts in Financial Planning to compare snowball vs avalanche and see payoff impact."],
                "follow_up": "Once debts are added, I can compare repayment pressure against your monthly surplus."
            }
        total_debt = sum(float(d.balance or 0) for d in debts)
        avg_rate = sum(float(d.rate or 0) for d in debts) / len(debts)
        bullets = [f"Tracked debt totals ${total_debt:,.2f} across {len(debts)} account(s).", f"Average stored rate is about {avg_rate:.2f}%."]
        if monthly_net > 0:
            bullets.append(f"Your current monthly surplus of ${monthly_net:,.2f} gives you room to accelerate payoff if you prioritize it.")
        else:
            bullets.append("Your current cash flow is tight, so stabilizing monthly spending should come before aggressive extra debt payments.")
        return {"title": "How is debt shaping your finances?", "summary": "Debt is reducing flexibility, and high-rate balances are likely the best place to send extra cash once monthly spending is under control.", "bullets": bullets, "follow_up": "Use Financial Planning to compare payoff strategies with your real monthly debt budget."}

    def net_worth_answer():
        bullets = [
            f"Assets total ${total_assets:,.2f}.",
            f"Liabilities total ${total_liabilities:,.2f}.",
            f"Current net worth is ${net_worth:,.2f}."
        ]
        summary = "Net worth improves fastest when monthly surplus stays positive and high-interest liabilities stop growing."
        return {"title": "What does your balance sheet say?", "summary": summary, "bullets": bullets, "follow_up": "Grow assets with savings and reduce liabilities at the same time for the strongest long-term gain."}

    prompt_map = [
        (["cut back", "save more", "reduce spending"], cutback_answer),
        (["afford", "can i buy", "new car", "purchase"], affordability_answer),
        (["hurting", "worst category", "biggest problem", "category"], hurting_answer),
        (["subscription", "recurring"], subscription_answer),
        (["safe to spend", "safe-to-spend", "how much can i spend", "spend safely"], safe_to_spend_answer),
        (["debt", "loan payoff"], debt_answer),
        (["net worth", "balance sheet", "assets", "liabilities"], net_worth_answer),
    ]

    for triggers, fn in prompt_map:
        if any(trigger in q for trigger in triggers):
            return fn()

    generic_bullets = [
        f"Monthly income: ${monthly_income:,.2f}; monthly expenses: ${monthly_expenses:,.2f}; monthly net: ${monthly_net:,.2f}.",
        f"Savings rate: {savings_rate:.2f}%; net worth: ${net_worth:,.2f}.",
        f"Tracked recurring subscriptions: {len(subscriptions)} merchant(s)."
    ]
    return {
        "title": "Finance AI overview",
        "summary": "I can dig into your spending, savings pace, subscriptions, debt pressure, and affordability using the financial data already in the app.",
        "bullets": generic_bullets,
        "follow_up": "Try asking: Where can I cut back this month? Can I afford a new car? What category is hurting me most? How much are subscriptions costing me?"
    }

def month_year_from_request():
    now = datetime.now()
    m = request.args.get("month", str(now.month))
    y = request.args.get("year", str(now.year))
    try:
        m = int(m)
        y = int(y)
    except:
        m = now.month
        y = now.year
    return m, y


# ---------------------
# AUTH ROUTES
# ---------------------

@app.route("/register", methods=["GET", "POST"])
def register():
    register_error = None
    username_value = ""
    if request.method == "POST":
        username = normalize_username(request.form["username"])
        password = request.form["password"].strip()
        username_value = username

        if not username or not password:
            register_error = "Username and password are required."
            return render_template("register.html", register_error=register_error, username_value=username_value)
        if len(password) < 8:
            register_error = "Password must be at least 8 characters."
            return render_template("register.html", register_error=register_error, username_value=username_value)

        existing = find_user_by_username(username)
        if existing:
            register_error = "Username already exists."
            return render_template("register.html", register_error=register_error, username_value=username_value)

        hashed_pw = generate_password_hash(password)
        new_user = User(
            username=username,
            password_hash=hashed_pw,
            is_admin=(User.query.count() == 0),
            created_at=datetime.utcnow(),
        )
        db.session.add(new_user)
        db.session.commit()
        return redirect("/login")

    return render_template("register.html", register_error=register_error, username_value=username_value)


@app.route("/review")
def review():

    if "user_id" not in session:
        return redirect("/login")

    txs = Transaction.query.filter_by(
        user_id=session["user_id"],
        category="Needs Review"
    ).all()

    return render_template("review.html", transactions=txs)


@app.route("/subscriptions")
def subscriptions():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    bootstrap_merchant_memory(user_id)
    transactions = Transaction.query.filter_by(user_id=user_id).order_by(Transaction.date.asc()).all()
    subscriptions = analyze_subscriptions(transactions)
    total_monthly = sum(s["average_amount"] for s in subscriptions)
    total_yearly = sum(s["estimated_yearly_cost"] for s in subscriptions)
    price_increase_count = sum(1 for s in subscriptions if s["has_price_increase"])
    cancel_candidate_count = sum(1 for s in subscriptions if s["cancel_candidate"])

    return render_template(
        "subscriptions.html",
        subs=subscriptions,
        total_monthly=round(total_monthly, 2),
        total_yearly=round(total_yearly, 2),
        price_increase_count=price_increase_count,
        cancel_candidate_count=cancel_candidate_count
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    login_error = None
    login_notice = None
    username_value = ""
    if request.method == "GET" and request.args.get("reset_status") == "updated":
        login_notice = "Password updated successfully. Sign in with your new password."
        username_value = normalize_username(request.args.get("username", ""))
    if request.method == "POST":
        username = normalize_username(request.form["username"])
        password = request.form["password"].strip()
        username_value = username

        user = find_user_by_username(username)
        if user and check_password_hash(user.password_hash, password):
            session["user_id"] = user.id
            user.last_login_at = datetime.utcnow()
            db.session.commit()
            return redirect("/")

        login_error = "The username or password is incorrect. Please try again."

    return render_template(
        "login.html",
        login_error=login_error,
        login_notice=login_notice,
        username_value=username_value,
    )


@app.route("/reset-password", methods=["GET", "POST"])
def reset_password():
    reset_error = None
    reset_success = None
    request_error = None
    request_success = None
    generated_reset_link = None
    username_value = normalize_username(request.values.get("username", ""))
    generated_mode = request.args.get("generated") == "1"
    token = request.values.get("token", "").strip()

    user = None
    if token:
        user = User.query.filter_by(reset_token=token).first()
        if not user or not user.reset_token_expires_at or user.reset_token_expires_at < datetime.utcnow():
            user = None
            reset_error = "This reset link is invalid or has expired."

    if request.method == "POST":
        if token and user:
            new_password = request.form.get("new_password", "").strip()
            confirm_password = request.form.get("confirm_password", "").strip()
            if len(new_password) < 8:
                reset_error = "New password must be at least 8 characters."
            elif new_password != confirm_password:
                reset_error = "New password and confirmation do not match."
            else:
                user.password_hash = generate_password_hash(new_password)
                user.reset_token = None
                user.reset_token_expires_at = None
                db.session.commit()
                return redirect(url_for("login", reset_status="updated", username=user.username))
        elif not token:
            username = normalize_username(request.form.get("username", ""))
            username_value = username
            if not username:
                request_error = "Enter your username to generate a reset link."
            else:
                requested_user = find_user_by_username(username)
                if not requested_user:
                    request_error = "No profile was found with that username."
                elif User.query.count() > 1:
                    request_error = "This app has multiple profiles. Ask an admin to generate your reset link from Settings > Admin."
                else:
                    requested_user.reset_token = uuid.uuid4().hex
                    requested_user.reset_token_expires_at = datetime.utcnow() + timedelta(hours=1)
                    db.session.commit()
                    return redirect(url_for("reset_password", token=requested_user.reset_token, username=requested_user.username, generated=1))

    if token and user and generated_mode:
        request_success = "Reset request accepted. Set your new password below."
        generated_reset_link = url_for("reset_password", token=token, _external=True)

    return render_template(
        "reset_password.html",
        token=token,
        reset_error=reset_error,
        reset_success=reset_success,
        token_valid=bool(user),
        request_error=request_error,
        request_success=request_success,
        generated_reset_link=generated_reset_link,
        username_value=username_value,
        generated_mode=generated_mode,
    )


@app.route("/settings", methods=["GET", "POST"])
def settings():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    user = User.query.get(user_id)
    password_error = None
    password_success = None
    reset_link = None
    admin_error = None
    admin_success = None
    active_tab = "overview"

    if request.method == "POST":
        form_name = request.form.get("form_name")
        if form_name == "change_password":
            active_tab = "security"
            current_password = request.form.get("current_password", "").strip()
            new_password = request.form.get("new_password", "").strip()
            confirm_password = request.form.get("confirm_password", "").strip()

            if not user or not check_password_hash(user.password_hash, current_password):
                password_error = "Current password is incorrect."
            elif len(new_password) < 8:
                password_error = "New password must be at least 8 characters."
            elif new_password != confirm_password:
                password_error = "New password and confirmation do not match."
            elif check_password_hash(user.password_hash, new_password):
                password_error = "New password must be different from your current password."
            else:
                user.password_hash = generate_password_hash(new_password)
                db.session.commit()
                password_success = "Password updated successfully."
        elif form_name == "generate_reset_link" and user and user.is_admin:
            active_tab = "admin"
            target_user = User.query.get(int(request.form.get("target_user_id") or 0))
            if not target_user:
                admin_error = "User not found for password reset."
            else:
                target_user.reset_token = uuid.uuid4().hex
                target_user.reset_token_expires_at = datetime.utcnow() + timedelta(hours=1)
                db.session.commit()
                reset_link = url_for("reset_password", token=target_user.reset_token, _external=True)
                admin_success = f"Reset link created for {target_user.username}."
        elif form_name == "revoke_reset_link" and user and user.is_admin:
            active_tab = "admin"
            target_user = User.query.get(int(request.form.get("target_user_id") or 0))
            if not target_user:
                admin_error = "User not found for reset-link revocation."
            elif not target_user.reset_token:
                admin_error = f"{target_user.username} does not have an active reset link."
            else:
                target_user.reset_token = None
                target_user.reset_token_expires_at = None
                db.session.commit()
                admin_success = f"Reset link revoked for {target_user.username}."
        elif form_name == "toggle_admin_access" and user and user.is_admin:
            active_tab = "admin"
            target_user = User.query.get(int(request.form.get("target_user_id") or 0))
            if not target_user:
                admin_error = "User not found for access update."
            elif target_user.is_admin and User.query.filter_by(is_admin=True).count() <= 1:
                admin_error = "You cannot remove admin access from the last admin."
            else:
                target_user.is_admin = not target_user.is_admin
                db.session.commit()
                admin_success = (
                    f"{target_user.username} is now an admin."
                    if target_user.is_admin
                    else f"{target_user.username} is now a member."
                )
        elif form_name == "delete_managed_account" and user and user.is_admin:
            active_tab = "admin"
            target_account = Account.query.get(int(request.form.get("target_account_id") or 0))
            if not target_account:
                admin_error = "Account not found for deletion."
            else:
                owner = User.query.get(target_account.user_id)
                account_name = target_account.name
                owner_name = owner.username if owner else "Unknown user"
                delete_account_and_transactions(target_account)
                db.session.commit()
                admin_success = f"Deleted account '{account_name}' for {owner_name}."
        elif form_name == "delete_profile" and user and user.is_admin:
            active_tab = "admin"
            target_user = User.query.get(int(request.form.get("target_user_id") or 0))
            confirm_username = normalize_username(request.form.get("confirm_username"))
            if not target_user:
                admin_error = "User not found for profile deletion."
            elif target_user.id == user.id:
                admin_error = "You cannot delete the account you are currently using."
            elif User.query.count() <= 1:
                admin_error = "You cannot delete the last remaining profile."
            elif target_user.is_admin and User.query.filter_by(is_admin=True).count() <= 1:
                admin_error = "You cannot delete the last admin profile."
            elif confirm_username.lower() != normalize_username(target_user.username).lower():
                admin_error = f"Type {target_user.username} exactly to confirm profile deletion."
            else:
                deleted_username = target_user.username
                delete_user_and_related_data(target_user)
                db.session.commit()
                admin_success = f"Deleted profile {deleted_username} and all associated data."

    account_count = Account.query.filter_by(user_id=user_id).count()
    budget_count = Budget.query.filter_by(user_id=user_id).count()
    rule_count = CategoryRule.query.filter_by(user_id=user_id).count()
    transaction_count = Transaction.query.filter_by(user_id=user_id).count()
    user_rows = []
    managed_account_rows = []
    admin_summary = {
        "profile_count": 0,
        "admin_count": 0,
        "account_count": 0,
        "transaction_count": 0,
        "budget_count": 0,
        "goal_count": 0,
        "active_reset_count": 0,
    }
    if user and user.is_admin:
        users = User.query.order_by(User.created_at.asc(), User.id.asc()).all()
        admin_summary["profile_count"] = len(users)
        admin_summary["admin_count"] = sum(1 for managed_user in users if managed_user.is_admin)
        admin_summary["account_count"] = Account.query.count()
        admin_summary["transaction_count"] = Transaction.query.count()
        admin_summary["budget_count"] = Budget.query.count()
        admin_summary["goal_count"] = FinancialGoal.query.count()
        admin_summary["active_reset_count"] = User.query.filter(User.reset_token.isnot(None)).count()
        for managed_user in users:
            managed_accounts = Account.query.filter_by(user_id=managed_user.id).order_by(Account.name.asc()).all()
            can_delete_profile = True
            delete_guardrail_text = ""
            if managed_user.id == user.id:
                can_delete_profile = False
                delete_guardrail_text = "Current session"
            elif len(users) <= 1:
                can_delete_profile = False
                delete_guardrail_text = "Last profile"
            elif managed_user.is_admin and admin_summary["admin_count"] <= 1:
                can_delete_profile = False
                delete_guardrail_text = "Last admin"
            user_rows.append({
                "id": managed_user.id,
                "username": managed_user.username,
                "is_admin": managed_user.is_admin,
                "created_at": managed_user.created_at,
                "last_login_at": managed_user.last_login_at,
                "account_count": len(managed_accounts),
                "account_names": [account.name for account in managed_accounts[:6]],
                "transaction_count": Transaction.query.filter_by(user_id=managed_user.id).count(),
                "budget_count": Budget.query.filter_by(user_id=managed_user.id).count(),
                "goal_count": FinancialGoal.query.filter_by(user_id=managed_user.id).count(),
                "has_reset_link": bool(managed_user.reset_token and (managed_user.reset_token_expires_at is None or managed_user.reset_token_expires_at > datetime.utcnow())),
                "can_delete_profile": can_delete_profile,
                "delete_guardrail_text": delete_guardrail_text,
            })
            for managed_account in managed_accounts:
                managed_account_rows.append({
                    "id": managed_account.id,
                    "name": managed_account.name,
                    "owner_username": managed_user.username,
                    "type": managed_account.type.title(),
                    "subtype_label": subtype_label(managed_account),
                    "balance": float(managed_account.balance or 0),
                    "transaction_count": Transaction.query.filter_by(account_id=managed_account.id).count(),
                })
        managed_account_rows.sort(key=lambda row: (row["owner_username"].lower(), row["name"].lower()))

    return render_template(
        "settings.html",
        user=user,
        account_count=account_count,
        budget_count=budget_count,
        rule_count=rule_count,
        transaction_count=transaction_count,
        password_error=password_error,
        password_success=password_success,
        active_tab=active_tab,
        user_rows=user_rows,
        managed_account_rows=managed_account_rows,
        admin_summary=admin_summary,
        reset_link=reset_link,
        admin_error=admin_error,
        admin_success=admin_success,
    )


def estimated_minimum_payment(balance, annual_rate):
    interest_component = (balance * (annual_rate / 100.0)) / 12.0 if annual_rate else 0
    return min(balance, max(25.0, round(balance * 0.02 + interest_component, 2)))


def simulate_debt_payoff(debts, strategy, monthly_budget):
    debt_states = []
    for debt in debts:
        minimum_payment = estimated_minimum_payment(debt.balance, debt.rate)
        debt_states.append({
            "name": debt.name,
            "balance": float(debt.balance or 0),
            "rate": float(debt.rate or 0),
            "minimum_payment": minimum_payment
        })

    debt_states = [d for d in debt_states if d["balance"] > 0]
    if not debt_states:
        return {
            "order": [],
            "months": 0,
            "interest_paid": 0.0,
            "total_paid": 0.0
        }

    total_minimum = sum(d["minimum_payment"] for d in debt_states)
    budget = max(float(monthly_budget or 0), total_minimum)

    order_key = (lambda d: (d["balance"], -d["rate"])) if strategy == "snowball" else (lambda d: (-d["rate"], d["balance"]))
    ordered_names = [d["name"] for d in sorted(debt_states, key=order_key)]

    months = 0
    total_interest = 0.0
    total_paid = 0.0

    while any(d["balance"] > 0.01 for d in debt_states) and months < 600:
        months += 1
        active_debts = [d for d in debt_states if d["balance"] > 0.01]

        for debt in active_debts:
            monthly_rate = debt["rate"] / 100.0 / 12.0
            interest = debt["balance"] * monthly_rate
            debt["balance"] += interest
            total_interest += interest

        active_debts = sorted(
            [d for d in debt_states if d["balance"] > 0.01],
            key=order_key
        )

        remaining_budget = budget
        for debt in active_debts:
            payment = min(debt["minimum_payment"], debt["balance"], remaining_budget)
            debt["balance"] -= payment
            total_paid += payment
            remaining_budget -= payment

        for debt in active_debts:
            if remaining_budget <= 0:
                break
            if debt["balance"] <= 0.01:
                continue
            extra_payment = min(debt["balance"], remaining_budget)
            debt["balance"] -= extra_payment
            total_paid += extra_payment
            remaining_budget -= extra_payment

    return {
        "order": ordered_names,
        "months": months,
        "interest_paid": round(total_interest, 2),
        "total_paid": round(total_paid, 2)
    }


@app.route("/planning", methods=["GET", "POST"])
def planning():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    current_month = date.today().month
    current_year = date.today().year

    accounts = Account.query.filter_by(user_id=user_id).all()
    transactions = Transaction.query.filter_by(user_id=user_id).all()
    debts = Debt.query.filter_by(user_id=user_id).all()

    monthly_income = 0.0
    monthly_expenses = 0.0
    for tx in transactions:
        if tx.date.month == current_month and tx.date.year == current_year:
            if tx.amount > 0:
                monthly_income += tx.amount
            else:
                monthly_expenses += abs(tx.amount)

    planning_result = None
    purchase_result = None
    total_debt_balance = sum(float(d.balance or 0) for d in debts)

    monthly_debt_budget = sum(estimated_minimum_payment(float(d.balance or 0), float(d.rate or 0)) for d in debts)
    if debts:
        monthly_debt_budget += 250.0

    if request.method == "POST":
        form_name = request.form.get("form_name")

        if form_name == "add_debt":
            name = request.form.get("name", "").strip()
            balance = safe_float(request.form.get("balance"))
            rate = safe_float(request.form.get("rate"))
            if name and balance is not None and rate is not None:
                db.session.add(Debt(user_id=user_id, name=name, balance=balance, rate=rate))
                db.session.commit()
            return redirect("/planning")

        if form_name == "debt_plan":
            submitted_budget = safe_float(request.form.get("monthly_budget"))
            if submitted_budget is not None:
                monthly_debt_budget = submitted_budget

        if form_name == "purchase_plan":
            purchase_name = request.form.get("name", "").strip()
            price = safe_float(request.form.get("price"))
            down = safe_float(request.form.get("down"))
            rate = safe_float(request.form.get("rate"))
            years = safe_float(request.form.get("years"))

            if purchase_name and price is not None and down is not None and rate is not None and years:
                loan_amount = max(price - down, 0)
                months = max(int(years * 12), 1)
                monthly_rate = (rate / 100) / 12

                if monthly_rate > 0:
                    monthly_payment = loan_amount * (monthly_rate * (1 + monthly_rate) ** months) / ((1 + monthly_rate) ** months - 1)
                else:
                    monthly_payment = loan_amount / months

                total_paid = monthly_payment * months + down
                total_interest = total_paid - price
                current_monthly_net = monthly_income - monthly_expenses
                net_after_purchase = current_monthly_net - monthly_payment

                purchase_result = {
                    "name": purchase_name,
                    "loan_amount": round(loan_amount, 2),
                    "monthly_payment": round(monthly_payment, 2),
                    "total_paid": round(total_paid, 2),
                    "total_interest": round(total_interest, 2),
                    "current_monthly_net": round(current_monthly_net, 2),
                    "net_after_purchase": round(net_after_purchase, 2),
                    "budget_pressure": round((monthly_payment / monthly_income) * 100, 2) if monthly_income > 0 else None
                }

    snowball = simulate_debt_payoff(debts, "snowball", monthly_debt_budget)
    avalanche = simulate_debt_payoff(debts, "avalanche", monthly_debt_budget)
    interest_saved = round(max(snowball["interest_paid"] - avalanche["interest_paid"], 0), 2)

    planning_result = {
        "monthly_budget": round(monthly_debt_budget, 2),
        "total_balance": round(total_debt_balance, 2),
        "snowball": snowball,
        "avalanche": avalanche,
        "interest_saved": interest_saved
    }

    return render_template(
        "planning.html",
        debts=debts,
        planning_result=planning_result,
        purchase_result=purchase_result,
        monthly_income=round(monthly_income, 2),
        monthly_expenses=round(monthly_expenses, 2)
    )


@app.route("/debt", methods=["GET", "POST"])
def debt():
    # Compatibility alias for the older Debt Optimizer URL.
    if request.method == "POST":
        if not require_login():
            return redirect("/login")
        user_id = get_user_id()
        name = request.form.get("name", "").strip()
        balance = safe_float(request.form.get("balance"))
        rate = safe_float(request.form.get("rate"))
        if name and balance is not None and rate is not None:
            db.session.add(Debt(user_id=user_id, name=name, balance=balance, rate=rate))
            db.session.commit()
    return redirect("/planning")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


# ---------------------
# ACCOUNTS
# ---------------------

@app.route("/accounts")
def accounts():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    accounts = Account.query.filter_by(user_id=user_id).all()
    transactions = Transaction.query.filter_by(user_id=user_id).all()
    savings_profiles = build_savings_profiles(accounts, transactions)
    savings_profile_map = {profile["account"].id: profile for profile in savings_profiles}
    savings_summary = {
        "confirmed": sum(1 for profile in savings_profiles if profile["detection_mode"] == "manual_include"),
        "auto_detected": sum(1 for profile in savings_profiles if profile["detection_mode"] == "auto" and profile["is_savings"]),
        "excluded": sum(1 for profile in savings_profiles if profile["detection_mode"] == "manual_exclude"),
    }
    return render_template(
        "accounts.html",
        accounts=accounts,
        has_accounts=bool(accounts),
        savings_profile_map=savings_profile_map,
        savings_summary=savings_summary,
        asset_subtype_choices=[(value, ACCOUNT_SUBTYPE_LABELS[value]) for value in ["", "checking", "cash", "savings", "investment", "other_asset"]],
        liability_subtype_choices=[(value, ACCOUNT_SUBTYPE_LABELS[value]) for value in ["", "credit_card", "loan", "other_liability"]],
        subtype_label=subtype_label,
    )


@app.route("/add_account", methods=["POST"])
def add_account():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    name = request.form["name"].strip()
    type_ = request.form["type"].strip()
    balance = safe_float(request.form["balance"])
    savings_preference = normalize_savings_preference(request.form.get("savings_preference", "auto"))
    subtype = normalize_account_subtype(request.form.get("subtype", ""), type_)

    if not name or type_ not in ("asset", "liability") or balance is None:
        push_ui_feedback("Enter an account name, choose asset or liability, and provide a valid balance.", "danger")
        return redirect("/accounts")

    if type_ == "liability":
        savings_preference = "exclude"

    new_account = Account(
        user_id=user_id,
        name=name,
        type=type_,
        balance=balance,
        savings_preference=savings_preference,
        subtype=subtype,
    )
    db.session.add(new_account)
    log_activity(
        user_id,
        f"Created account {new_account.name}",
        f"{new_account.type.title()} account added with a starting balance of ${new_account.balance:,.2f}.",
        kind="account_created",
        icon="bi-wallet2",
        target_url="/accounts",
    )
    db.session.commit()
    push_ui_feedback(f"{new_account.name} was added successfully.", "success")
    return redirect("/accounts")


@app.route("/edit_account/<int:account_id>", methods=["GET", "POST"])
def edit_account(account_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    acct = Account.query.get(account_id)
    if not acct or acct.user_id != user_id:
        return "Account not found"

    if request.method == "POST":
        name = request.form["name"].strip()
        type_ = request.form["type"].strip()
        savings_preference = normalize_savings_preference(request.form.get("savings_preference", "auto"))
        subtype = normalize_account_subtype(request.form.get("subtype", ""), type_)
        if not name or type_ not in ("asset", "liability"):
            push_ui_feedback("Update the account with a valid name and account type.", "danger")
            return redirect(f"/edit_account/{account_id}")
        acct.name = name
        acct.type = type_
        acct.savings_preference = savings_preference if type_ == "asset" else "exclude"
        acct.subtype = subtype
        db.session.commit()
        push_ui_feedback(f"{acct.name} was updated.", "success")
        return redirect("/accounts")

    return render_template(
        "edit_account.html",
        account=acct,
        asset_subtype_choices=[(value, ACCOUNT_SUBTYPE_LABELS[value]) for value in ["", "checking", "cash", "savings", "investment", "other_asset"]],
        liability_subtype_choices=[(value, ACCOUNT_SUBTYPE_LABELS[value]) for value in ["", "credit_card", "loan", "other_liability"]],
    )


@app.route("/accounts/<int:account_id>/savings-preference", methods=["POST"])
def update_account_savings_preference(account_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    acct = Account.query.get(account_id)
    if not acct or acct.user_id != user_id:
        return "Account not found"

    savings_preference = normalize_savings_preference(request.form.get("savings_preference", "auto"))
    acct.savings_preference = savings_preference if acct.type == "asset" else "exclude"
    log_activity(
        user_id,
        f"Updated savings tracking for {acct.name}",
        f"Savings preference is now set to {acct.savings_preference}.",
        kind="account_updated",
        icon="bi-piggy-bank",
        target_url="/accounts",
    )
    db.session.commit()
    push_ui_feedback(f"Savings tracking updated for {acct.name}.", "success")
    return redirect("/accounts")


@app.route("/delete_account/<int:account_id>", methods=["POST"])
def delete_account(account_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    acct = Account.query.get(account_id)

    if acct and acct.user_id == user_id:
        account_name = acct.name
        delete_account_and_transactions(acct)
        log_activity(
            user_id,
            f"Deleted account {account_name}",
            "The account and its transactions were removed from your workspace.",
            kind="account_deleted",
            icon="bi-trash3",
            target_url="/accounts",
        )
        db.session.commit()
        push_ui_feedback(f"{account_name} was deleted.", "success")

    return redirect("/accounts")


@app.route("/goals-wealth")
def goals_wealth():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    selected_month, selected_year = month_year_from_request()
    accounts = Account.query.filter_by(user_id=user_id).all()
    transactions = Transaction.query.filter_by(user_id=user_id).order_by(Transaction.date.asc()).all()
    goals = FinancialGoal.query.filter_by(user_id=user_id).all()
    goals.sort(key=lambda goal: (goal.target_date is None, goal.target_date or date.max, -goal.id))

    monthly_summary = summarize_monthly_finances(transactions, selected_month, selected_year)
    monthly_income = monthly_summary["monthly_income"]
    monthly_expenses = monthly_summary["monthly_expenses"]
    category_totals = monthly_summary["category_totals"]
    nw_labels, nw_values = compute_net_worth_history(accounts, transactions)
    savings_snapshot = calculate_savings_snapshot(
        accounts=accounts,
        transactions=transactions,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses,
    )
    wealth_snapshot = build_wealth_snapshot(
        accounts=accounts,
        transactions=transactions,
        goals=goals,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses,
        category_totals=category_totals,
        savings_snapshot=savings_snapshot,
        nw_values=nw_values,
    )
    transaction_years = sorted({tx.date.year for tx in transactions} | {selected_year, datetime.now().year}, reverse=True)
    month_labels = {month: calendar.month_name[month] for month in range(1, 13)}
    starter_goal_suggestions = [
        {
            "name": "Emergency fund",
            "goal_type": "emergency_fund",
            "target_amount": round(wealth_snapshot.get("target_3_month") or 1000, 2),
            "current_amount": round(savings_snapshot.get("current_savings") or 0, 2),
            "linked_metric": "savings_balance",
            "description": "Build a 3-month cash cushion using your current emergency fund estimate.",
        },
        {
            "name": "Car down payment",
            "goal_type": "car_down_payment",
            "target_amount": 5000.0,
            "current_amount": 0.0,
            "linked_metric": "manual",
            "description": "Track a dedicated vehicle down payment without mixing it into general savings.",
        },
        {
            "name": "Debt-free",
            "goal_type": "debt_free",
            "target_amount": round(wealth_snapshot["net_worth_breakdown"].get("total_liabilities") or 1000, 2),
            "current_amount": 0.0,
            "linked_metric": "debt_paydown",
            "description": "Use your liabilities as the target so the goal measures progress toward zero debt.",
        },
        {
            "name": "Vacation fund",
            "goal_type": "vacation_fund",
            "target_amount": 2500.0,
            "current_amount": 0.0,
            "linked_metric": "manual",
            "description": "Save for travel in a separate goal so it does not compete with core bills.",
        },
    ]

    return render_template(
        "goals_wealth.html",
        selected_month=selected_month,
        selected_year=selected_year,
        transaction_years=transaction_years,
        month_labels=month_labels,
        nw_labels=nw_labels,
        nw_values=nw_values,
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses,
        savings_snapshot=savings_snapshot,
        wealth_snapshot=wealth_snapshot,
        has_goals=bool(goals),
        starter_goal_suggestions=starter_goal_suggestions,
        goal_type_choices=GOAL_TYPE_CHOICES,
        goal_link_choices=GOAL_LINK_CHOICES,
    )


@app.route("/goals-wealth/add-goal", methods=["POST"])
def add_financial_goal():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    name = request.form.get("name", "").strip()
    goal_type = request.form.get("goal_type", "custom").strip()
    target_amount = safe_float(request.form.get("target_amount"))
    current_amount = safe_float(request.form.get("current_amount"))
    linked_metric = request.form.get("linked_metric", "manual").strip()
    target_date = parse_date_any(request.form.get("target_date"))

    valid_goal_types = {value for value, _ in GOAL_TYPE_CHOICES}
    valid_linked_metrics = {value for value, _ in GOAL_LINK_CHOICES}
    if not name or target_amount is None or target_amount <= 0:
        push_ui_feedback("Add a goal name and a target amount greater than zero.", "danger")
        return redirect("/goals-wealth")

    new_goal = FinancialGoal(
        user_id=user_id,
        name=name,
        goal_type=goal_type if goal_type in valid_goal_types else "custom",
        target_amount=target_amount,
        current_amount=current_amount or 0,
        target_date=target_date,
        linked_metric=linked_metric if linked_metric in valid_linked_metrics else "manual",
    )
    db.session.add(new_goal)
    log_activity(
        user_id,
        f"Created goal {new_goal.name}",
        f"Target set to ${new_goal.target_amount:,.2f}.",
        kind="goal_created",
        icon="bi-bullseye",
        target_url="/goals-wealth",
    )
    db.session.commit()
    push_ui_feedback(f"Goal created for {new_goal.name}.", "success")
    return redirect("/goals-wealth")


@app.route("/goals-wealth/update-goal/<int:goal_id>", methods=["POST"])
def update_financial_goal(goal_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    goal = FinancialGoal.query.get(goal_id)
    if not goal or goal.user_id != user_id:
        return redirect("/goals-wealth")

    valid_goal_types = {value for value, _ in GOAL_TYPE_CHOICES}
    valid_linked_metrics = {value for value, _ in GOAL_LINK_CHOICES}
    name = request.form.get("name", "").strip()
    target_amount = safe_float(request.form.get("target_amount"))
    current_amount = safe_float(request.form.get("current_amount"))

    if name:
        goal.name = name
    if target_amount is not None and target_amount > 0:
        goal.target_amount = target_amount
    if current_amount is not None:
        goal.current_amount = current_amount
    goal.goal_type = request.form.get("goal_type", goal.goal_type).strip()
    if goal.goal_type not in valid_goal_types:
        goal.goal_type = "custom"
    goal.linked_metric = request.form.get("linked_metric", goal.linked_metric).strip()
    if goal.linked_metric not in valid_linked_metrics:
        goal.linked_metric = "manual"
    goal.target_date = parse_date_any(request.form.get("target_date"))
    log_activity(
        user_id,
        f"Updated goal {goal.name}",
        f"Progress now tracks toward ${goal.target_amount:,.2f}.",
        kind="goal_updated",
        icon="bi-pencil-square",
        target_url="/goals-wealth",
    )
    db.session.commit()
    push_ui_feedback(f"Goal updated for {goal.name}.", "success")
    return redirect("/goals-wealth")


@app.route("/goals-wealth/delete-goal/<int:goal_id>", methods=["POST"])
def delete_financial_goal(goal_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    goal = FinancialGoal.query.get(goal_id)
    if goal and goal.user_id == user_id:
        goal_name = goal.name
        db.session.delete(goal)
        log_activity(
            user_id,
            f"Deleted goal {goal_name}",
            "The goal was removed from your wealth tracker.",
            kind="goal_deleted",
            icon="bi-trash3",
            target_url="/goals-wealth",
        )
        db.session.commit()
        push_ui_feedback(f"Goal deleted for {goal_name}.", "success")
    return redirect("/goals-wealth")


# ---------------------
# RULES
# ---------------------

@app.route("/rules", methods=["GET", "POST"])
def rules():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    rules = sorted_user_rules(user_id)
    rule_test_result = None

    if request.method == "POST" and request.form.get("form_name") == "test_rule":
        description = request.form.get("description", "").strip()
        amount = safe_float(request.form.get("amount"))
        if description and amount is not None:
            category, source = categorize_transaction(user_id, description, amount)
            rule_test_result = {
                "description": description,
                "amount": round(amount, 2),
                "normalized_merchant": normalize_text(description),
                "category": category,
                "source": source
            }
        else:
            rule_test_result = {
                "error": "Enter both a description and an amount to test categorization."
            }

    return render_template("rules.html", rules=rules, rule_test_result=rule_test_result)


@app.route("/add_rule", methods=["POST"])
def add_rule():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    keyword = request.form["keyword"].strip()
    category = request.form["category"].strip()
    priority = request.form.get("priority", "100").strip()
    match_type = request.form.get("match_type", "contains").strip()
    amount_direction = request.form.get("amount_direction", "any").strip()
    try:
        priority = int(priority)
    except:
        priority = 100
    if match_type not in ("exact", "contains", "startswith"):
        match_type = "contains"
    if amount_direction not in ("debit", "credit", "any"):
        amount_direction = "any"
    if not keyword or not category:
        return "Keyword and category required"
    r = CategoryRule(
        user_id=user_id,
        keyword=keyword,
        category=category,
        priority=priority,
        match_type=match_type,
        amount_direction=amount_direction
    )
    db.session.add(r)
    db.session.commit()
    return redirect("/rules")


@app.route("/delete_rule/<int:rule_id>", methods=["POST"])
def delete_rule(rule_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    r = CategoryRule.query.get(rule_id)
    if r and r.user_id == user_id:
        db.session.delete(r)
        db.session.commit()
    return redirect("/rules")


@app.route("/merchant-memory")
def merchant_memory():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    memories = MerchantMemory.query.filter_by(user_id=user_id).order_by(MerchantMemory.merchant.asc()).all()
    categories = sorted({memory.category for memory in memories if memory.category})
    return render_template(
        "merchant_memory.html",
        memories=memories,
        memory_count=len(memories),
        category_count=len(categories),
        categories=categories,
        category_choices=import_category_choices(user_id)
    )


@app.route("/merchant-memory/add", methods=["POST"])
def add_merchant_memory():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    merchant = request.form.get("merchant", "").strip()
    category = request.form.get("category", "").strip()
    if merchant and category:
        remember_merchant_category(user_id, merchant, category)
        db.session.commit()
    return redirect("/merchant-memory")


@app.route("/merchant-memory/update/<int:memory_id>", methods=["POST"])
def update_merchant_memory(memory_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    memory = MerchantMemory.query.get(memory_id)
    if not memory or memory.user_id != user_id:
        return redirect("/merchant-memory")

    merchant = request.form.get("merchant", "").strip()
    category = request.form.get("category", "").strip()
    normalized = normalize_text(merchant)
    if normalized and category and category.lower() not in GENERIC_CATEGORIES:
        memory.merchant = normalized
        memory.category = category
        db.session.commit()
    return redirect("/merchant-memory")


@app.route("/merchant-memory/delete/<int:memory_id>", methods=["POST"])
def delete_merchant_memory(memory_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    memory = MerchantMemory.query.get(memory_id)
    if memory and memory.user_id == user_id:
        db.session.delete(memory)
        db.session.commit()
    return redirect("/merchant-memory")


# ---------------------
# BUDGETS
# ---------------------

@app.route("/budgets")
def budgets():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    budgets = Budget.query.filter_by(user_id=user_id).all()
    transactions = Transaction.query.filter_by(user_id=user_id).all()
    return render_template(
        "budgets.html",
        budgets=budgets,
        budget_suggestions=suggested_budget_categories(transactions, budgets),
        has_transactions=bool(transactions),
    )


@app.route("/add_budget", methods=["POST"])
def add_budget():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    category = request.form["category"].strip()
    limit_ = safe_float(request.form["monthly_limit"])
    if not category or limit_ is None or limit_ <= 0:
        push_ui_feedback("Enter a category and a monthly budget limit greater than zero.", "danger")
        return redirect("/budgets")
    b = Budget(user_id=user_id, category=category, monthly_limit=limit_)
    db.session.add(b)
    log_activity(
        user_id,
        f"Saved budget for {category}",
        f"Monthly limit set to ${limit_:,.2f}.",
        kind="budget_saved",
        icon="bi-pie-chart-fill",
        target_url="/budgets",
    )
    db.session.commit()
    push_ui_feedback(f"Budget saved for {category}.", "success")
    return redirect("/budgets")


@app.route("/imports", methods=["GET", "POST"])
def imports():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    bootstrap_merchant_memory(user_id)
    accounts = Account.query.filter_by(user_id=user_id).all()
    transaction_count = Transaction.query.filter_by(user_id=user_id).count()
    preview = load_import_preview()
    import_error = None
    import_success = None
    import_summary = None
    category_choices = import_category_choices(user_id)
    selected_account_id = preview["account_id"] if preview else None
    import_new_account_open = False
    pending_import_account = {
        "name": "",
        "type": "asset",
        "balance": "0",
        "subtype": "",
        "savings_preference": "auto",
    }

    if request.method == "POST":
        form_name = request.form.get("form_name")

        if form_name == "preview_import":
            account_id = request.form.get("account_id")
            files = [file for file in request.files.getlist("files") if file and file.filename]
            if not files:
                single_file = request.files.get("file")
                if single_file and single_file.filename:
                    files = [single_file]

            if not accounts:
                import_error = "Add an account before importing transactions."
                import_new_account_open = True
            elif account_id == "__new__":
                import_error = "Create the new account first, then preview the import."
                import_new_account_open = True
            elif not account_id:
                import_error = "Choose an account before previewing the import."
            elif not files:
                import_error = "Choose one or more CSV or PDF statements to preview."
                selected_account_id = int(account_id)
            else:
                payload, error = build_import_preview(user_id, files, account_id)
                if error:
                    import_error = error
                    selected_account_id = int(account_id)
                else:
                    save_import_preview(user_id, payload)
                    preview = payload
                    selected_account_id = payload["account_id"]

        elif form_name == "create_import_account":
            import_new_account_open = True
            name = request.form.get("name", "").strip()
            type_ = request.form.get("type", "").strip()
            balance = safe_float(request.form.get("balance"))
            savings_preference = normalize_savings_preference(request.form.get("savings_preference", "auto"))
            subtype = normalize_account_subtype(request.form.get("subtype", ""), type_)
            pending_import_account = {
                "name": name,
                "type": type_ or "asset",
                "balance": request.form.get("balance", "0"),
                "subtype": subtype,
                "savings_preference": savings_preference,
            }

            if not name or type_ not in ("asset", "liability"):
                import_error = "Enter an account name and choose whether it is money you have or money you owe."
            elif balance is None:
                import_error = "Enter a valid opening balance for the new account."
            else:
                if type_ == "liability":
                    savings_preference = "exclude"
                new_account = Account(
                    user_id=user_id,
                    name=name,
                    type=type_,
                    balance=balance,
                    savings_preference=savings_preference,
                    subtype=subtype,
                )
                db.session.add(new_account)
                log_activity(
                    user_id,
                    f"Created account {new_account.name} from Import Center",
                    "The new account is ready to receive imported transactions.",
                    kind="account_created",
                    icon="bi-cloud-arrow-up-fill",
                    target_url="/imports",
                )
                db.session.commit()
                accounts = Account.query.filter_by(user_id=user_id).all()
                selected_account_id = new_account.id
                import_new_account_open = False
                pending_import_account = {
                    "name": "",
                    "type": "asset",
                    "balance": "0",
                    "subtype": "",
                    "savings_preference": "auto",
                }
                if preview:
                    clear_import_preview()
                    preview = None
                    import_success = f"Added {new_account.name}. Previous import preview was cleared so you can re-run it against the new account."
                else:
                    import_success = f"Added {new_account.name}. It is now selected for your next import."

        elif form_name == "commit_import":
            preview = load_import_preview()
            if not preview:
                import_error = "Import preview expired. Upload the file again."
            else:
                account_id = int(preview["account_id"])
                acct = Account.query.get(account_id)
                if not acct or acct.user_id != user_id:
                    import_error = "Selected account is no longer available."
                else:
                    imported_count = 0
                    duplicate_count = 0
                    needs_review_count = 0
                    corrected_count = 0
                    auto_detected_count = 0
                    pending_manual_count = 0
                    existing_fingerprints = existing_transaction_fingerprints(user_id, account_id)
                    commit_fingerprints = set()
                    prepared_transactions = []
                    for row in preview["rows"]:
                        row_fingerprint = row.get("fingerprint") or transaction_fingerprint(row["date"], row["description"], row["amount"])
                        if row.get("is_duplicate") or row_fingerprint in existing_fingerprints or row_fingerprint in commit_fingerprints:
                            duplicate_count += 1
                            continue

                        chosen_date_raw = request.form.get(f"date_{row['row_id']}", row.get("date", "")).strip()
                        chosen_description = request.form.get(f"description_{row['row_id']}", row.get("description", "")).strip()
                        chosen_amount_raw = request.form.get(f"amount_{row['row_id']}", str(row.get("amount", ""))).strip()
                        chosen_category = request.form.get(f"category_{row['row_id']}", "").strip() or row["category"]
                        original_category = (row.get("category") or "").strip()
                        if not chosen_category or chosen_category.lower() in GENERIC_CATEGORIES:
                            chosen_category = "Needs Review"
                        parsed_date = parse_date_any(chosen_date_raw)
                        amount = safe_float(chosen_amount_raw)
                        if parsed_date is None or not chosen_description or amount is None:
                            pending_manual_count += 1
                            continue

                        final_fingerprint = transaction_fingerprint(parsed_date, chosen_description, amount)
                        prepared_transactions.append({
                            "date": parsed_date,
                            "description": chosen_description,
                            "amount": amount,
                            "category": chosen_category,
                        })
                        commit_fingerprints.add(final_fingerprint)
                        existing_fingerprints.add(final_fingerprint)
                        if (
                            original_category and chosen_category != original_category
                        ) or (
                            row.get("date", "") != chosen_date_raw
                        ) or (
                            (row.get("description") or "").strip() != chosen_description
                        ) or (
                            str(row.get("amount", "")).strip() != chosen_amount_raw
                        ):
                            corrected_count += 1
                        if original_category and row.get("category_source") not in ("Fallback", "", None):
                            auto_detected_count += 1
                        if chosen_category == "Needs Review":
                            needs_review_count += 1

                        imported_count += 1

                    if pending_manual_count:
                        import_error = f"{pending_manual_count} row{'s' if pending_manual_count != 1 else ''} still need a valid date, description, and amount before import can finish."
                    else:
                        for prepared_row in prepared_transactions:
                            tx = Transaction(
                                user_id=user_id,
                                account_id=account_id,
                                date=prepared_row["date"],
                                description=prepared_row["description"],
                                amount=prepared_row["amount"],
                                category=prepared_row["category"],
                            )
                            db.session.add(tx)
                            acct.balance += prepared_row["amount"]
                            remember_merchant_category(user_id, prepared_row["description"], prepared_row["category"])
                        log_activity(
                            user_id,
                            f"Imported {imported_count} transaction{'s' if imported_count != 1 else ''}",
                            f"{auto_detected_count} categories prefilled, {corrected_count} corrections, {duplicate_count} duplicates skipped.",
                            kind="import_completed",
                            icon="bi-database-check",
                            target_url="/imports",
                        )
                        db.session.commit()
                        clear_import_preview()
                        preview = None
                        import_success = f"Imported {imported_count} transaction{'s' if imported_count != 1 else ''}."
                        if duplicate_count:
                            import_success += f" Skipped {duplicate_count} duplicate row{'s' if duplicate_count != 1 else ''}."
                        if needs_review_count:
                            import_success += f" {needs_review_count} row{'s' if needs_review_count != 1 else ''} still need review."
                        import_success += " Merchant memory was updated for confirmed categories."
                        import_summary = {
                            "imported_count": imported_count,
                            "auto_detected_count": auto_detected_count,
                            "corrected_count": corrected_count,
                            "duplicate_count": duplicate_count,
                            "needs_review_count": needs_review_count,
                        }

        elif form_name == "clear_preview":
            clear_import_preview()
            preview = None

    if preview and not selected_account_id:
        selected_account_id = preview["account_id"]
    return render_template(
        "imports.html",
        accounts=accounts,
        preview=preview,
        import_error=import_error,
        import_success=import_success,
        import_summary=import_summary,
        category_choices=category_choices,
        selected_account_id=selected_account_id,
        import_new_account_open=import_new_account_open,
        pending_import_account=pending_import_account,
        has_import_history=transaction_count > 0,
        asset_subtype_choices=[(value, ACCOUNT_SUBTYPE_LABELS[value]) for value in ["", "checking", "cash", "savings", "investment", "other_asset"]],
        liability_subtype_choices=[(value, ACCOUNT_SUBTYPE_LABELS[value]) for value in ["", "credit_card", "loan", "other_liability"]],
    )


@app.route("/delete_budget/<int:budget_id>", methods=["POST"])
def delete_budget(budget_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    b = Budget.query.get(budget_id)
    if b and b.user_id == user_id:
        category_name = b.category
        db.session.delete(b)
        log_activity(
            user_id,
            f"Deleted budget for {category_name}",
            "The spending limit was removed.",
            kind="budget_deleted",
            icon="bi-trash3",
            target_url="/budgets",
        )
        db.session.commit()
        push_ui_feedback(f"Budget deleted for {category_name}.", "success")
    return redirect("/budgets")


# ---------------------
# TRANSACTIONS
# ---------------------

@app.route("/add", methods=["POST"])
def add_transaction():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()

    account_id = request.form.get("account_id")
    if not account_id:
        push_ui_feedback("Select an account before saving a transaction.", "danger")
        return redirect("/")

    dt = parse_date_any(request.form.get("date"))
    description = request.form.get("description", "").strip()
    amount = safe_float(request.form.get("amount"))
    category = request.form.get("category", "").strip()

    if dt is None or not description or amount is None:
        push_ui_feedback("Enter a date, description, and valid amount to save the transaction.", "danger")
        return redirect("/")

    if not category:
        category = auto_categorize(user_id, description, amount)
    else:
        remember_merchant_category(user_id, description, category)

    account_id = int(account_id)

    tx = Transaction(
        user_id=user_id,
        account_id=account_id,
        date=dt,
        description=description,
        amount=amount,
        category=category
    )

    db.session.add(tx)

    acct = Account.query.get(account_id)
    if acct and acct.user_id == user_id:
        acct.balance += amount

    log_activity(
        user_id,
        f"Added transaction {description}",
        f"{category} · ${amount:,.2f} saved to {acct.name if acct else 'your account'}.",
        kind="transaction_added",
        icon="bi-receipt",
        target_url="/",
    )
    db.session.commit()
    push_ui_feedback("Transaction saved successfully.", "success")
    return redirect("/")


@app.route("/update_transaction", methods=["POST"])
def update_transaction():

    if not require_login():
        return redirect("/login")

    user_id = get_user_id()

    tx_id = request.form.get("tx_id")
    new_category = request.form.get("category")
    redirect_to = request.form.get("redirect_to", "/").strip()
    if not redirect_to.startswith("/"):
        redirect_to = "/"

    if not tx_id or not new_category:
        return redirect(redirect_to)

    transaction = Transaction.query.get(int(tx_id))

    if not transaction or transaction.user_id != user_id:
        return redirect(redirect_to)

    # update transaction category
    transaction.category = new_category

    remember_merchant_category(user_id, transaction.description, new_category)
    log_activity(
        user_id,
        f"Updated category for {transaction.description}",
        f"Saved as {new_category}.",
        kind="category_updated",
        icon="bi-tags",
        target_url=redirect_to,
    )

    db.session.commit()
    push_ui_feedback("Category correction saved.", "success")

    return redirect(redirect_to)


@app.route("/edit_tx/<int:tx_id>", methods=["GET", "POST"])
def edit_tx(tx_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    redirect_to = request.values.get("redirect_to", "/").strip()
    if not redirect_to.startswith("/"):
        redirect_to = "/"
    tx = Transaction.query.get(tx_id)
    if not tx or tx.user_id != user_id:
        return "Transaction not found"

    accounts = Account.query.filter_by(user_id=user_id).all()

    if request.method == "POST":
        new_date = parse_date_any(request.form.get("date"))
        new_desc = request.form.get("description", "").strip()
        new_amount = safe_float(request.form.get("amount"))
        new_category = request.form.get("category", "").strip()
        new_account_id = int(request.form.get("account_id"))

        if new_date is None or not new_desc or new_amount is None:
            return "Invalid input"

        # reverse old impact
        old_acct = Account.query.get(tx.account_id)
        if old_acct and old_acct.user_id == user_id:
            old_acct.balance -= tx.amount

        # apply new data
        tx.date = new_date
        tx.description = new_desc
        tx.amount = new_amount
        tx.category = new_category or auto_categorize(user_id, new_desc, new_amount)
        tx.account_id = new_account_id

        if new_category:
            remember_merchant_category(user_id, new_desc, new_category)

        new_acct = Account.query.get(new_account_id)
        if new_acct and new_acct.user_id == user_id:
            new_acct.balance += new_amount

        db.session.commit()
        return redirect(redirect_to)

    return render_template("edit_transaction.html", tx=tx, accounts=accounts, redirect_to=redirect_to)


@app.route("/delete_tx/<int:tx_id>", methods=["POST"])
def delete_tx(tx_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    tx = Transaction.query.get(tx_id)
    if tx and tx.user_id == user_id:
        acct = Account.query.get(tx.account_id)
        if acct and acct.user_id == user_id:
            acct.balance -= tx.amount
        db.session.delete(tx)
        db.session.commit()
    redirect_to = request.form.get("redirect_to", "/").strip()
    if not redirect_to.startswith("/"):
        redirect_to = "/"
    return redirect(redirect_to)


@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    # Compatibility alias for the older direct-upload POST endpoint.
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    account_id = request.form.get("account_id")
    files = [file for file in request.files.getlist("files") if file and file.filename]
    if not files:
        single_file = request.files.get("file")
        if single_file and single_file.filename:
            files = [single_file]

    if not account_id or not files:
        return redirect("/imports")

    payload, error = build_import_preview(user_id, files, account_id)
    if error:
        return redirect("/imports")

    save_import_preview(user_id, payload)
    return redirect("/imports")


@app.route("/export_csv")
def export_csv():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()

    txs = Transaction.query.filter_by(user_id=user_id).order_by(Transaction.date.asc()).all()
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Description", "Amount", "Category", "Account"])

    for tx in txs:
        acct = Account.query.get(tx.account_id)
        acct_name = acct.name if acct and acct.user_id == user_id else ""
        writer.writerow([tx.date.isoformat(), tx.description, tx.amount, tx.category, acct_name])

    csv_data = output.getvalue()
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=transactions.csv"}
    )


# ---------------------
# DASHBOARD
# ---------------------

@app.route("/", methods=["GET", "POST"])
def home():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    bootstrap_merchant_memory(user_id)

    selected_month, selected_year = month_year_from_request()

    transactions = Transaction.query.filter_by(user_id=user_id).order_by(Transaction.date.asc()).all()
    accounts = Account.query.filter_by(user_id=user_id).all()
    budgets = Budget.query.filter_by(user_id=user_id).all()
    debts = Debt.query.filter_by(user_id=user_id).all()
    goals = FinancialGoal.query.filter_by(user_id=user_id).all()
    account_name_map = {a.id: a.name for a in accounts}
    onboarding_state = build_onboarding_state(accounts, transactions, budgets, goals)
    recent_activity = recent_activity_for_user(user_id)

    # -------------------------
    # NET WORTH
    # -------------------------
    total_assets = sum(a.balance for a in accounts if a.type == "asset")
    total_liabilities = sum(a.balance for a in accounts if a.type == "liability")
    net_worth = total_assets - total_liabilities
    liquid_balance = sum(
        a.balance for a in accounts
        if a.type == "asset" and infer_account_subtype(a) in ("checking", "cash", "savings")
    )

    # -------------------------
    # NET WORTH OVER TIME
    # -------------------------
    networth_by_date = {}
    running_balances = {a.id: 0 for a in accounts}

    for tx in transactions:
        running_balances[tx.account_id] += tx.amount
        total_assets_running = sum(running_balances[a.id] for a in accounts if a.type == "asset")
        total_liabilities_running = sum(running_balances[a.id] for a in accounts if a.type == "liability")
        networth_by_date[tx.date.isoformat()] = total_assets_running - total_liabilities_running

    nw_labels = list(networth_by_date.keys())
    nw_values = list(networth_by_date.values())

    if len(nw_labels) > 60:
        step = math.ceil(len(nw_labels) / 60)
        sampled_points = list(zip(nw_labels, nw_values))[::step]
        if sampled_points and sampled_points[-1][0] != nw_labels[-1]:
            sampled_points.append((nw_labels[-1], nw_values[-1]))
        nw_labels = [label for label, _ in sampled_points]
        nw_values = [value for _, value in sampled_points]

    # -------------------------
    # MONTHLY STATS
    # -------------------------
    category_totals = defaultdict(float)
    prev_category_totals = defaultdict(float)
    monthly_income = 0
    monthly_expenses = 0
    daily_spend = defaultdict(float)
    prev_monthly_income = 0
    prev_monthly_expenses = 0

    previous_month = 12 if selected_month == 1 else selected_month - 1
    previous_year = selected_year - 1 if selected_month == 1 else selected_year

    for tx in transactions:
        if tx.date.month == selected_month and tx.date.year == selected_year:
            if tx.amount > 0:
                monthly_income += tx.amount
            elif is_spending_category(tx.category):
                monthly_expenses += abs(tx.amount)
                category_totals[tx.category] += abs(tx.amount)
                daily_spend[tx.date.day] += abs(tx.amount)
        elif tx.date.month == previous_month and tx.date.year == previous_year:
            if tx.amount > 0:
                prev_monthly_income += tx.amount
            elif is_spending_category(tx.category):
                prev_monthly_expenses += abs(tx.amount)
                prev_category_totals[tx.category] += abs(tx.amount)

    savings_rate = 0
    if monthly_income > 0:
        savings_rate = round(((monthly_income - monthly_expenses) / monthly_income) * 100, 2)

    # -------------------------
    # PROJECTION (6 months)
    # -------------------------
    avg_monthly_savings = monthly_income - monthly_expenses
    projection_labels = []
    projection_values = []

    projected_value = net_worth
    for i in range(1, 7):
        projected_value += avg_monthly_savings
        projection_labels.append(f"Month +{i}")
        projection_values.append(round(projected_value, 2))

    # -------------------------
    # ACCOUNT DISTRIBUTION
    # -------------------------
    account_labels = [a.name for a in accounts]
    account_values = [a.balance for a in accounts]

    # -------------------------
    # BUDGET PROGRESS
    # -------------------------
    budget_rows = []
    for b in budgets:
        spent = category_totals.get(b.category, 0)
        pct = 0 if b.monthly_limit == 0 else min(100, round((spent / b.monthly_limit) * 100, 2))
        budget_rows.append({
            "category": b.category,
            "limit": b.monthly_limit,
            "spent": spent,
            "pct": pct
        })
    subscriptions = analyze_subscriptions(transactions)
    recurring_transactions = [
        {
            "description": sub["name"],
            "count": sub["occurrences"],
            "avg_amount": round(-abs(sub["average_amount"]), 2)
        }
        for sub in subscriptions
    ]
    subscription_total = sum(float(sub["average_amount"] or 0) for sub in subscriptions)
    recurring_debt_payments = sum(
        estimated_minimum_payment(float(debt.balance or 0), float(debt.rate or 0))
        for debt in debts
    )
    recurring_monthly_obligations = subscription_total + recurring_debt_payments
    budget_on_track_count = sum(1 for row in budget_rows if row["pct"] < 100)
    over_budget_count = sum(1 for row in budget_rows if row["pct"] >= 100)
    savings_snapshot = calculate_savings_snapshot(
        accounts=accounts,
        transactions=transactions,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses
    )
    wealth_snapshot = build_wealth_snapshot(
        accounts=accounts,
        transactions=transactions,
        goals=goals,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses,
        category_totals=category_totals,
        savings_snapshot=savings_snapshot,
        nw_values=nw_values,
    )
    safe_to_spend = calculate_safe_to_spend(
        accounts=accounts,
        subscriptions=subscriptions,
        budget_rows=budget_rows,
        monthly_expenses=monthly_expenses,
        selected_month=selected_month,
        selected_year=selected_year
    )
    days_in_month = calendar.monthrange(selected_year, selected_month)[1]
    now = datetime.now()
    current_day = now.day if now.month == selected_month and now.year == selected_year else days_in_month
    current_day = max(1, min(current_day, days_in_month))
    income_run_rate = monthly_income / current_day
    expense_run_rate = monthly_expenses / current_day
    pace_savings = (income_run_rate - expense_run_rate) * days_in_month
    cash_runway_months = (safe_to_spend["current_cash"] / monthly_expenses) if monthly_expenses > 0 else None
    income_change_pct = (((monthly_income - prev_monthly_income) / prev_monthly_income) * 100) if prev_monthly_income > 0 else None
    expense_change_pct = (((monthly_expenses - prev_monthly_expenses) / prev_monthly_expenses) * 100) if prev_monthly_expenses > 0 else None
    transaction_years = sorted({tx.date.year for tx in transactions} | {selected_year, datetime.now().year}, reverse=True)
    month_labels = {month: calendar.month_name[month] for month in range(1, 13)}
    health_summary = compute_financial_health({
        "monthly_income": monthly_income,
        "monthly_expenses": monthly_expenses,
        "savings_rate": savings_rate,
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
        "subscription_total": subscription_total,
        "budget_rows": budget_rows,
        "recurring_monthly_obligations": recurring_monthly_obligations,
        "current_cash": safe_to_spend["current_cash"],
        "prev_monthly_expenses": prev_monthly_expenses,
        "days_in_month": days_in_month,
        "current_day": current_day
    })
    health_score = health_summary["score"]

    dashboard_insights = build_dashboard_insights(
        transactions=transactions,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses,
        category_totals=category_totals,
        subscriptions=subscriptions
    )

    dashboard_assistant = build_dashboard_assistant(
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses,
        savings_rate=savings_rate,
        budget_rows=budget_rows,
        recurring_transactions=recurring_transactions,
        dashboard_insights=dashboard_insights
    )
    primary_recommendation = dashboard_assistant["next_actions"][0] if dashboard_assistant["next_actions"] else "Import or add transactions so AkuOS can tell you what matters next."

    finance_ai_question = ""
    finance_ai_response = None
    prompt_text = request.form.get("prompt_text", "").strip() if request.method == "POST" else ""
    question_text = request.form.get("finance_ai_question", "").strip() if request.method == "POST" else ""
    finance_ai_question = prompt_text or question_text
    if finance_ai_question:
        finance_ai_response = build_finance_ai_response(finance_ai_question, {
            "monthly_income": monthly_income,
            "monthly_expenses": monthly_expenses,
            "prev_monthly_income": prev_monthly_income,
            "prev_monthly_expenses": prev_monthly_expenses,
            "savings_rate": savings_rate,
            "category_totals": dict(category_totals),
            "prev_category_totals": dict(prev_category_totals),
            "subscriptions": subscriptions,
            "debts": debts,
            "net_worth": net_worth,
            "total_assets": total_assets,
            "total_liabilities": total_liabilities,
            "budget_rows": budget_rows,
            "pace_savings": pace_savings,
            "days_in_month": days_in_month,
            "current_day": current_day,
            "safe_to_spend": safe_to_spend
        })

    recent_transactions = list(reversed(transactions[-75:]))

    return render_template(
        "home.html",
        accounts=accounts,
        today_iso=date.today().isoformat(),
        transactions=recent_transactions,
        transaction_count=len(transactions),
        displayed_transaction_count=len(recent_transactions),
        account_name_map=account_name_map,
        onboarding_state=onboarding_state,
        recent_activity=recent_activity,
        subscriptions=subscriptions,
        recurring_transactions=recurring_transactions,
        selected_month=selected_month,
        selected_year=selected_year,
        month_labels=month_labels,
        transaction_years=transaction_years,
        budget_on_track_count=budget_on_track_count,
        over_budget_count=over_budget_count,
        cash_runway_months=cash_runway_months,
        income_change_pct=income_change_pct,
        expense_change_pct=expense_change_pct,
        subscription_total=subscription_total,
        dashboard_insights=dashboard_insights,
        dashboard_assistant=dashboard_assistant,
        finance_ai_question=finance_ai_question,
        finance_ai_response=finance_ai_response,
        safe_to_spend=safe_to_spend,
        savings_snapshot=savings_snapshot,
        wealth_snapshot=wealth_snapshot,
        health_summary=health_summary,
        primary_recommendation=primary_recommendation,
        liquid_balance=liquid_balance,
        net_worth=net_worth,
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses,
        savings_rate=savings_rate,
        health_score=health_score,
        nw_labels=nw_labels,
        nw_values=nw_values,
        projection_labels=projection_labels,
        projection_values=projection_values,
        account_labels=account_labels,
        account_values=account_values,
        category_labels=list(category_totals.keys()),
        category_values=list(category_totals.values()),
        budget_rows=budget_rows
    )

@app.route("/init_db")
def init_db():
    with app.app_context():
        db.create_all()
        ensure_db_schema()
    return "DB initialized"

@app.route("/simulator", methods=["GET", "POST"])
def simulator():
    # Compatibility alias for the older Purchase Simulator URL.
    return redirect("/planning")

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        ensure_db_schema()
    app.run(debug=True)
