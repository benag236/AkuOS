from flask import Flask, render_template, request, redirect, session, Response, url_for
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
from sqlalchemy import inspect, text, func, or_, String
from sqlalchemy.engine.url import make_url
import os
import math
import json
import uuid
import re
import calendar
import shutil
import threading
from datetime import datetime, date, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo
import csv
from io import StringIO, BytesIO
try:
    import pdfplumber
except ImportError:
    pdfplumber = None
from finance_engine import (
    clean_transaction_description,
    compute_financial_health,
    compute_wealth_score,
    GENERIC_CATEGORIES,
    categorize_from_sources,
    detect_amount_from_row as detect_amount_from_row_helper,
    detect_csv_column,
    is_spending_category,
    is_spending_transaction,
    merchant_similarity,
    normalize_merchant,
    normalize_text,
    sort_rules,
)

app = Flask(__name__)
app.config["_SCHEMA_READY"] = False

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
IS_RENDER = bool(os.getenv("RENDER") or os.getenv("RENDER_EXTERNAL_URL"))
IS_PRODUCTION = IS_RENDER or (os.getenv("FLASK_ENV", "").strip().lower() == "production")
APP_TIMEZONE = (os.getenv("APP_TIMEZONE") or os.getenv("TZ") or "America/New_York").strip() or "America/New_York"


def local_secret_fallback():
    return os.getenv("AKUOS_LOCAL_SECRET_KEY", "").strip() or "akuos-local-dev-secret-change-this"


def resolve_secret_key():
    configured_secret = (os.getenv("SECRET_KEY") or "").strip()
    if configured_secret:
        return configured_secret
    return local_secret_fallback()


def normalize_database_url(database_url):
    database_url = (database_url or "").strip()
    if not database_url:
        return ""
    if database_url.startswith("postgres://"):
        database_url = "postgresql://" + database_url[len("postgres://"):]
    try:
        parsed = make_url(database_url)
        if parsed.drivername == "postgresql":
            return parsed.set(drivername="postgresql+psycopg2").render_as_string(hide_password=False)
    except Exception:
        pass
    return database_url


def resolve_database_uri():
    database_url = normalize_database_url(os.getenv("DATABASE_URL", ""))
    if database_url:
        if database_url.startswith("postgres://"):
            database_url = "postgresql://" + database_url[len("postgres://"):]
        return database_url

    if IS_PRODUCTION:
        raise RuntimeError("DATABASE_URL must be set in production.")

    render_disk_path = os.getenv("RENDER_DISK_PATH", "").strip()
    db_dir = render_disk_path or BASE_DIR
    db_path = os.path.join(db_dir, "finance.db")
    return f"sqlite:///{db_path}"


DATABASE_URI = resolve_database_uri()
app.config["SECRET_KEY"] = resolve_secret_key()
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URI
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = (
    {"pool_pre_ping": True, "pool_recycle": 300}
    if DATABASE_URI.startswith("postgresql")
    else {"pool_pre_ping": True}
)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = IS_PRODUCTION
app.config["SESSION_COOKIE_NAME"] = "akuos_session"
app.config["SESSION_COOKIE_PATH"] = "/"
app.config["SESSION_REFRESH_EACH_REQUEST"] = True
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)
app.config["PRESERVE_CONTEXT_ON_EXCEPTION"] = False
app.config["PROPAGATE_EXCEPTIONS"] = not IS_PRODUCTION
app.config["DEBUG"] = bool(os.getenv("FLASK_DEBUG", "").strip() == "1" and not IS_PRODUCTION)

if IS_PRODUCTION:
    app.config["PREFERRED_URL_SCHEME"] = "https"

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

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
    raw_description = db.Column(db.String(255), nullable=False, default="")
    display_name = db.Column(db.String(255), nullable=False, default="")
    amount = db.Column(db.Float, nullable=False)
    category = db.Column(db.String(100), nullable=False)
    category_source = db.Column(db.String(80), nullable=False, default="")
    category_confidence = db.Column(db.String(20), nullable=False, default="")
    transaction_subtype = db.Column(db.String(20), nullable=False, default="")
    tags = db.Column(db.String(255), nullable=False, default="")
    import_batch_id = db.Column(db.String(32), nullable=True)


class ImportBatch(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.Integer, nullable=False)
    imported_count = db.Column(db.Integer, nullable=False, default=0)
    net_change = db.Column(db.Float, nullable=False, default=0)
    starting_balance = db.Column(db.Float, nullable=False, default=0)
    ending_balance = db.Column(db.Float, nullable=False, default=0)
    balance_mode = db.Column(db.String(20), nullable=False, default="add")
    auto_detected_count = db.Column(db.Integer, nullable=False, default=0)
    corrected_count = db.Column(db.Integer, nullable=False, default=0)
    duplicate_count = db.Column(db.Integer, nullable=False, default=0)
    skipped_count = db.Column(db.Integer, nullable=False, default=0)
    not_transaction_count = db.Column(db.Integer, nullable=False, default=0)
    needs_review_count = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class ImportJob(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.Integer, nullable=False)
    status = db.Column(db.String(20), nullable=False, default="queued")
    current_stage = db.Column(db.String(40), nullable=False, default="uploaded")
    progress_percent = db.Column(db.Integer, nullable=False, default=5)
    balance_mode = db.Column(db.String(20), nullable=False, default="add")
    source_files = db.Column(db.Text, nullable=False, default="[]")
    file_count = db.Column(db.Integer, nullable=False, default=0)
    preview_id = db.Column(db.String(64), nullable=True)
    summary_json = db.Column(db.Text, nullable=False, default="{}")
    error_message = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)


class MerchantMemory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    merchant = db.Column(db.String(200), nullable=False)
    category = db.Column(db.String(100), nullable=False)
    display_name = db.Column(db.String(255), nullable=False, default="")
    subtype = db.Column(db.String(20), nullable=False, default="")
    is_disabled = db.Column(db.Boolean, nullable=False, default=False)


class FinancialGoal(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(120), nullable=False)
    goal_type = db.Column(db.String(40), nullable=False, default="custom")
    target_amount = db.Column(db.Float, nullable=False)
    current_amount = db.Column(db.Float, nullable=False, default=0)
    target_date = db.Column(db.Date, nullable=True)
    linked_metric = db.Column(db.String(40), nullable=False, default="manual")
    linked_account_id = db.Column(db.Integer, nullable=True)
    allocated_amount = db.Column(db.Float, nullable=False, default=0)


class GoalAllocation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    goal_id = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.Integer, nullable=False)
    allocated_amount = db.Column(db.Float, nullable=False, default=0)


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


def transaction_raw_description(tx):
    raw_description = getattr(tx, "raw_description", "") or ""
    return raw_description.strip() or (getattr(tx, "description", "") or "").strip()


def transaction_display_name(tx):
    display_name = getattr(tx, "display_name", "") or ""
    if display_name.strip():
        return display_name.strip()
    description = (getattr(tx, "description", "") or "").strip()
    if description:
        return description
    return clean_transaction_description(transaction_raw_description(tx))


def transaction_reference_description(tx):
    return transaction_raw_description(tx)


def app_timezone():
    try:
        return ZoneInfo(APP_TIMEZONE)
    except Exception:
        return ZoneInfo("UTC")


def to_local_datetime(value):
    if not value:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo("UTC"))
    return value.astimezone(app_timezone())


def format_local_datetime(value, fmt="%b %d, %Y %I:%M %p"):
    localized = to_local_datetime(value)
    return localized.strftime(fmt) if localized else ""


def push_ui_feedback(message, tone="success", action_label=None, action_url=None, action_method="GET"):
    session["_ui_feedback"] = {
        "message": message,
        "tone": tone,
        "action_label": (action_label or "").strip(),
        "action_url": (action_url or "").strip(),
        "action_method": (action_method or "GET").strip().upper(),
    }


VALID_TRANSACTION_SUBTYPES = {"income", "expense", "transfer", "payment", "neutral"}
VALID_CONFIDENCE_BUCKETS = {"error", "uncategorized", "low", "medium", "high"}


def normalize_confidence_bucket(value):
    value = (value or "").strip().lower()
    if value in VALID_CONFIDENCE_BUCKETS:
        return value
    if "high" in value:
        return "high"
    if "medium" in value or "moderate" in value:
        return "medium"
    if "low" in value:
        return "low"
    if "uncategor" in value:
        return "uncategorized"
    if "error" in value:
        return "error"
    return ""


def transaction_subtype_for(amount, category, source="", row_kind=""):
    explicit_kind = (row_kind or "").strip().lower()
    if explicit_kind in VALID_TRANSACTION_SUBTYPES:
        return explicit_kind

    normalized_category = normalize_text(category)
    normalized_source = normalize_text(source)
    number = float(amount or 0)

    if normalized_category in {"credit card payment"}:
        return "payment"
    if normalized_category in {"transfer", "transfer payment"} or "transfer" in normalized_source or "payment" in normalized_source:
        return "transfer"
    if normalized_category == "income" or number > 0:
        return "income"
    if number < 0 and is_spending_category(category):
        return "expense"
    if number < 0:
        return "transfer"
    return "neutral"


def transaction_type_label(tx):
    subtype = (getattr(tx, "transaction_subtype", "") or "").strip().lower()
    if subtype == "payment":
        return "Payment"
    if subtype == "transfer":
        return "Transfer"
    if subtype == "income":
        return "Income"
    if subtype == "expense":
        return "Expense"
    if float(getattr(tx, "amount", 0) or 0) > 0:
        return "Income"
    if float(getattr(tx, "amount", 0) or 0) < 0 and is_spending_category(getattr(tx, "category", "")):
        return "Expense"
    if float(getattr(tx, "amount", 0) or 0) < 0:
        return "Transfer"
    return "Neutral"


def store_allocation_undo(action_label, changes, redirect_url):
    cleaned_changes = []
    for change in changes or []:
        goal_id = safe_int(change.get("goal_id"))
        account_id = safe_int(change.get("account_id"))
        if not goal_id or not account_id:
            continue
        cleaned_changes.append({
            "goal_id": goal_id,
            "account_id": account_id,
            "previous_amount": round(float(change.get("previous_amount") or 0), 2),
            "new_amount": round(float(change.get("new_amount") or 0), 2),
        })
    if cleaned_changes:
        session["_allocation_undo"] = {
            "label": action_label,
            "changes": cleaned_changes,
            "redirect_url": redirect_url if (redirect_url or "").startswith("/") else "/goals-wealth#allocations",
        }


def clear_allocation_undo():
    session.pop("_allocation_undo", None)


@app.context_processor
def inject_shared_ui_state():
    user_id = session.get("user_id")
    import_jobs = recent_import_jobs_for_user(user_id, limit=3) if user_id else []
    pending_import_jobs = sum(1 for job in import_jobs if job["status"] in {"queued", "processing"})
    active_import_job = next((job for job in import_jobs if job["status"] in {"queued", "processing"}), None)
    recent_failed_import_job = next(
        (
            job for job in import_jobs
            if job["status"] == "failed"
            and (
                (job.get("completed_at") and (datetime.utcnow() - job["completed_at"]).total_seconds() <= 1800)
                or (job.get("created_at") and (datetime.utcnow() - job["created_at"]).total_seconds() <= 1800)
            )
        ),
        None,
    )
    shared_import_status_job = active_import_job or recent_failed_import_job
    return {
        "ui_feedback": session.pop("_ui_feedback", None),
        "shared_import_jobs": import_jobs,
        "pending_import_jobs": pending_import_jobs,
        "shared_import_status_job": shared_import_status_job,
        "tx_display_name": transaction_display_name,
        "tx_raw_description": transaction_raw_description,
        "tx_type_label": transaction_type_label,
        "display_tag": display_tag,
        "format_local_datetime": format_local_datetime,
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
    FinancialGoal.query.filter_by(user_id=account.user_id, linked_account_id=account.id).update({
        FinancialGoal.linked_account_id: None,
    }, synchronize_session=False)
    GoalAllocation.query.filter_by(account_id=account.id).delete()
    ImportBatch.query.filter_by(user_id=account.user_id, account_id=account.id).delete()
    Transaction.query.filter_by(account_id=account.id).delete()
    db.session.delete(account)


def delete_user_and_related_data(user):
    if not user:
        return
    ActivityLog.query.filter_by(user_id=user.id).delete()
    ImportBatch.query.filter_by(user_id=user.id).delete()
    Transaction.query.filter_by(user_id=user.id).delete()
    Account.query.filter_by(user_id=user.id).delete()
    Budget.query.filter_by(user_id=user.id).delete()
    Debt.query.filter_by(user_id=user.id).delete()
    CategoryRule.query.filter_by(user_id=user.id).delete()
    MerchantMemory.query.filter_by(user_id=user.id).delete()
    goal_ids = [goal.id for goal in FinancialGoal.query.filter_by(user_id=user.id).all()]
    if goal_ids:
        GoalAllocation.query.filter(GoalAllocation.goal_id.in_(goal_ids)).delete(synchronize_session=False)
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


REVIEW_FILTER_OPTIONS = {
    "all": "All review items",
    "uncategorized": "Only uncategorized",
    "low-confidence": "Only low confidence",
}


def build_review_transaction_rows(user_id, transactions):
    rows = []
    for tx in transactions or []:
        current_category = (tx.category or "").strip() or "Needs Review"
        normalized_current = current_category.lower()
        persisted_source = (getattr(tx, "category_source", "") or "").strip()
        persisted_confidence = normalize_confidence_bucket(getattr(tx, "category_confidence", ""))
        suggested_category, suggested_source = categorize_transaction(user_id, transaction_reference_description(tx), float(tx.amount or 0))
        suggested_category = (suggested_category or "").strip() or "Needs Review"

        if persisted_confidence == "error":
            confidence_label = "Error"
            confidence_tone = "danger"
            confidence_detail = "This transaction still has broken or incomplete metadata."
            is_low_confidence = True
            is_uncategorized = False
        elif not normalized_current or normalized_current in GENERIC_CATEGORIES or persisted_confidence == "uncategorized":
            confidence_label = "Uncategorized"
            confidence_tone = "danger"
            confidence_detail = "No strong category is saved yet."
            is_low_confidence = True
            is_uncategorized = True
        elif persisted_confidence == "low" or suggested_source in ("Fallback", "Needs Review"):
            confidence_label = "Low confidence"
            confidence_tone = "warning"
            confidence_detail = "AkuOS does not have a strong rule or memory match for this merchant yet."
            is_low_confidence = True
            is_uncategorized = False
        elif persisted_confidence == "medium" or suggested_source.startswith("Built-in") or suggested_source == "Income Fallback":
            confidence_label = "Moderate confidence"
            confidence_tone = "info"
            source_label = (persisted_source or suggested_source).lower()
            confidence_detail = f"Current category is supported by {source_label}."
            is_low_confidence = False
            is_uncategorized = False
        else:
            confidence_label = "High confidence"
            confidence_tone = "positive"
            source_label = (persisted_source or suggested_source).lower()
            confidence_detail = f"Current category is backed by {source_label}."
            is_low_confidence = False
            is_uncategorized = False

        needs_review = is_uncategorized or is_low_confidence
        if not needs_review:
            continue

        amount_value = round(float(tx.amount or 0), 2)
        rows.append({
            "tx": tx,
            "current_category": current_category,
            "suggested_category": suggested_category,
            "suggested_source": persisted_source or suggested_source,
            "show_suggestion": suggested_category != current_category,
            "is_uncategorized": is_uncategorized,
            "is_low_confidence": is_low_confidence,
            "confidence_label": confidence_label,
            "confidence_tone": confidence_tone,
            "confidence_detail": confidence_detail,
            "amount_display": f"${abs(amount_value):,.2f}",
        })

    return rows


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
                conn.execute(text('ALTER TABLE "user" ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT 0'))
            if "created_at" not in columns:
                conn.execute(text('ALTER TABLE "user" ADD COLUMN created_at TIMESTAMP'))
            if "last_login_at" not in columns:
                conn.execute(text('ALTER TABLE "user" ADD COLUMN last_login_at TIMESTAMP'))
            if "reset_token" not in columns:
                conn.execute(text('ALTER TABLE "user" ADD COLUMN reset_token VARCHAR(120)'))
            if "reset_token_expires_at" not in columns:
                conn.execute(text('ALTER TABLE "user" ADD COLUMN reset_token_expires_at TIMESTAMP'))
    if "category_rule" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("category_rule")}
        with db.engine.begin() as conn:
            if "priority" not in columns:
                conn.execute(text("ALTER TABLE category_rule ADD COLUMN priority INTEGER NOT NULL DEFAULT 100"))
            if "match_type" not in columns:
                conn.execute(text("ALTER TABLE category_rule ADD COLUMN match_type VARCHAR(20) NOT NULL DEFAULT 'contains'"))
            if "amount_direction" not in columns:
                conn.execute(text("ALTER TABLE category_rule ADD COLUMN amount_direction VARCHAR(20) NOT NULL DEFAULT 'any'"))
    if "transaction" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("transaction")}
        with db.engine.begin() as conn:
            if "tags" not in columns:
                conn.execute(text('ALTER TABLE "transaction" ADD COLUMN tags VARCHAR(255) NOT NULL DEFAULT \'\''))
            if "import_batch_id" not in columns:
                conn.execute(text('ALTER TABLE "transaction" ADD COLUMN import_batch_id VARCHAR(32)'))
            if "raw_description" not in columns:
                conn.execute(text('ALTER TABLE "transaction" ADD COLUMN raw_description VARCHAR(255) NOT NULL DEFAULT \'\''))
            if "display_name" not in columns:
                conn.execute(text('ALTER TABLE "transaction" ADD COLUMN display_name VARCHAR(255) NOT NULL DEFAULT \'\''))
            if "category_source" not in columns:
                conn.execute(text('ALTER TABLE "transaction" ADD COLUMN category_source VARCHAR(80) NOT NULL DEFAULT \'\''))
            if "category_confidence" not in columns:
                conn.execute(text('ALTER TABLE "transaction" ADD COLUMN category_confidence VARCHAR(20) NOT NULL DEFAULT \'\''))
            if "transaction_subtype" not in columns:
                conn.execute(text('ALTER TABLE "transaction" ADD COLUMN transaction_subtype VARCHAR(20) NOT NULL DEFAULT \'\''))
    if "merchant_memory" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("merchant_memory")}
        with db.engine.begin() as conn:
            if "display_name" not in columns:
                conn.execute(text("ALTER TABLE merchant_memory ADD COLUMN display_name VARCHAR(255) NOT NULL DEFAULT ''"))
            if "subtype" not in columns:
                conn.execute(text("ALTER TABLE merchant_memory ADD COLUMN subtype VARCHAR(20) NOT NULL DEFAULT ''"))
            if "is_disabled" not in columns:
                conn.execute(text("ALTER TABLE merchant_memory ADD COLUMN is_disabled BOOLEAN NOT NULL DEFAULT 0"))
    if "import_job" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("import_job")}
        with db.engine.begin() as conn:
            if "current_stage" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN current_stage VARCHAR(40) NOT NULL DEFAULT 'uploaded'"))
            if "progress_percent" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN progress_percent INTEGER NOT NULL DEFAULT 5"))
            if "balance_mode" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN balance_mode VARCHAR(20) NOT NULL DEFAULT 'add'"))
            if "source_files" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN source_files TEXT NOT NULL DEFAULT '[]'"))
            if "file_count" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN file_count INTEGER NOT NULL DEFAULT 0"))
            if "preview_id" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN preview_id VARCHAR(64)"))
            if "summary_json" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN summary_json TEXT NOT NULL DEFAULT '{}'"))
            if "error_message" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN error_message VARCHAR(255)"))
            if "started_at" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN started_at DATETIME"))
            if "completed_at" not in columns:
                conn.execute(text("ALTER TABLE import_job ADD COLUMN completed_at DATETIME"))
    if "financial_goal" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("financial_goal")}
        with db.engine.begin() as conn:
            if "linked_account_id" not in columns:
                conn.execute(text("ALTER TABLE financial_goal ADD COLUMN linked_account_id INTEGER"))
            if "allocated_amount" not in columns:
                conn.execute(text("ALTER TABLE financial_goal ADD COLUMN allocated_amount FLOAT NOT NULL DEFAULT 0"))
    if "goal_allocation" in inspector.get_table_names():
        columns = {col["name"] for col in inspector.get_columns("goal_allocation")}
        with db.engine.begin() as conn:
            if "allocated_amount" not in columns:
                conn.execute(text("ALTER TABLE goal_allocation ADD COLUMN allocated_amount FLOAT NOT NULL DEFAULT 0"))
    app.config["_SCHEMA_READY"] = True

    with db.engine.begin() as conn:
        conn.execute(text('UPDATE "user" SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL'))
        conn.execute(text('UPDATE "user" SET is_admin = 1 WHERE id = (SELECT id FROM "user" ORDER BY id ASC LIMIT 1) AND NOT EXISTS (SELECT 1 FROM "user" WHERE is_admin = 1)'))
        conn.execute(text('UPDATE "transaction" SET raw_description = description WHERE COALESCE(raw_description, \'\') = \'\''))
        conn.execute(text('UPDATE "transaction" SET display_name = description WHERE COALESCE(display_name, \'\') = \'\''))
        conn.execute(text('UPDATE "transaction" SET category_source = COALESCE(category_source, \'\')'))
        conn.execute(text('UPDATE "transaction" SET category_confidence = COALESCE(category_confidence, \'\')'))
        conn.execute(text('UPDATE "transaction" SET transaction_subtype = CASE WHEN COALESCE(transaction_subtype, \'\') <> \'\' THEN transaction_subtype WHEN amount > 0 THEN \'income\' WHEN LOWER(COALESCE(category, \'\')) IN (\'transfer\', \'transfer / payment\') THEN \'transfer\' WHEN LOWER(COALESCE(category, \'\')) = \'credit card payment\' THEN \'payment\' WHEN amount < 0 THEN \'expense\' ELSE \'neutral\' END'))
        conn.execute(text("UPDATE merchant_memory SET display_name = '' WHERE display_name IS NULL"))
        conn.execute(text("UPDATE merchant_memory SET subtype = '' WHERE subtype IS NULL"))
        conn.execute(text("UPDATE merchant_memory SET is_disabled = 0 WHERE is_disabled IS NULL"))
        conn.execute(text("UPDATE financial_goal SET allocated_amount = COALESCE(allocated_amount, 0)"))
        conn.execute(text("""
            INSERT INTO goal_allocation (goal_id, account_id, allocated_amount)
            SELECT fg.id, fg.linked_account_id, COALESCE(fg.allocated_amount, 0)
            FROM financial_goal fg
            WHERE fg.linked_account_id IS NOT NULL
              AND COALESCE(fg.allocated_amount, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM goal_allocation ga
                WHERE ga.goal_id = fg.id AND ga.account_id = fg.linked_account_id
              )
        """))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_date ON "transaction" (user_id, date)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_account ON "transaction" (user_id, account_id)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_category ON "transaction" (user_id, category)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_subtype ON "transaction" (user_id, transaction_subtype)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_confidence ON "transaction" (user_id, category_confidence)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_import_batch ON "transaction" (import_batch_id)'))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_activity_log_user_created_at ON activity_log (user_id, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_import_batch_user_created_at ON import_batch (user_id, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_import_job_user_created_at ON import_job (user_id, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_import_job_user_status ON import_job (user_id, status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_merchant_memory_user_merchant ON merchant_memory (user_id, merchant)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_financial_goal_user_account ON financial_goal (user_id, linked_account_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_goal_allocation_goal ON goal_allocation (goal_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_goal_allocation_account ON goal_allocation (account_id)"))


@app.before_request
def prepare_schema():
    ensure_db_schema()
    if "user_id" in session:
        if not User.query.get(session.get("user_id")):
            session.clear()
        else:
            session.permanent = True

def safe_float(val):
    try:
        return float(str(val).replace("$", "").replace(",", "").strip())
    except:
        return None


def safe_int(val):
    try:
        return int(str(val).strip())
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
    if MerchantMemory.query.filter_by(user_id=user_id).first():
        return
    transactions = Transaction.query.filter_by(user_id=user_id).all()
    learned = {}
    for tx in transactions:
        category = (tx.category or "").strip()
        if not category or category.lower() in GENERIC_CATEGORIES:
            continue
        merchant = normalize_text(transaction_reference_description(tx))
        if merchant:
            learned[merchant] = {
                "category": category,
                "display_name": transaction_display_name(tx),
            }

    for merchant, payload in learned.items():
        remember_merchant_category(user_id, merchant, payload["category"], display_name=payload.get("display_name"))


def active_merchant_memories_for_user(user_id):
    return MerchantMemory.query.filter_by(user_id=user_id, is_disabled=False).all()


def find_best_merchant_memory(user_id, description, memories=None):
    normalized = normalize_text(description)
    if not normalized:
        return None

    memories = memories if memories is not None else active_merchant_memories_for_user(user_id)
    best_memory = None
    best_score = 0
    for memory in memories:
        merchant = (memory.merchant or "").strip()
        if not merchant:
            continue
        if merchant == normalized or merchant in normalized or normalized in merchant:
            return memory
        score = merchant_similarity(merchant, normalized)
        if score > best_score:
            best_score = score
            best_memory = memory
    if best_memory and best_score >= 0.6:
        return best_memory
    return None


def remember_merchant_category(user_id, description, category, display_name=None, subtype=None):
    normalized = normalize_text(description)
    cleaned_category = canonical_transaction_category(category)
    cleaned_display_name = (display_name or "").strip()
    cleaned_subtype = (subtype or "").strip().lower()
    if cleaned_subtype not in VALID_TRANSACTION_SUBTYPES:
        cleaned_subtype = ""
    if not normalized or not cleaned_category or cleaned_category.lower() in GENERIC_CATEGORIES:
        return

    memory = MerchantMemory.query.filter_by(user_id=user_id, merchant=normalized).first()
    if memory:
        memory.category = cleaned_category
        if cleaned_display_name:
            memory.display_name = cleaned_display_name
        if cleaned_subtype:
            memory.subtype = cleaned_subtype
        memory.is_disabled = False
    else:
        db.session.add(MerchantMemory(
            user_id=user_id,
            merchant=normalized,
            category=cleaned_category,
            display_name=cleaned_display_name,
            subtype=cleaned_subtype,
            is_disabled=False,
        ))


def preferred_display_name_for_user(user_id, description, fallback=None):
    normalized = normalize_text(description)
    if not normalized:
        return (fallback or "").strip()
    memory = MerchantMemory.query.filter_by(user_id=user_id, merchant=normalized, is_disabled=False).first()
    if memory and (memory.display_name or "").strip():
        return memory.display_name.strip()
    return (fallback or "").strip()


def categorize_transaction(user_id, description, amount):
    user_rules = sorted_user_rules(user_id)
    memories = active_merchant_memories_for_user(user_id)
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


def get_import_job_dir():
    path = os.path.join(BASE_DIR, "uploads", "import_jobs")
    os.makedirs(path, exist_ok=True)
    return path


def get_import_job_file_dir(job_id):
    path = os.path.join(get_import_job_dir(), str(job_id))
    os.makedirs(path, exist_ok=True)
    return path


def import_job_file_path(job_id, filename):
    safe_name = secure_filename(filename or "statement")
    if not safe_name:
        safe_name = "statement"
    return os.path.join(get_import_job_file_dir(job_id), safe_name)


def remove_import_job_files(job_id):
    job_dir = os.path.join(get_import_job_dir(), str(job_id))
    if os.path.isdir(job_dir):
        shutil.rmtree(job_dir, ignore_errors=True)


def detect_amount_from_row(row):
    return detect_amount_from_row_helper(row, safe_float)


IMPORT_REVIEW_BASE_CATEGORIES = [
    "Income",
    "Transfer",
    "Credit Card Payment",
    "Food & Drink",
    "Groceries",
    "Transport",
    "Gas",
    "Shopping",
    "Housing",
    "Utilities",
    "Health",
    "Subscriptions",
    "Entertainment",
    "Savings",
    "Other",
    "Needs Review",
]


def import_category_choices(user_id):
    categories = set()
    categories.update(r.category for r in CategoryRule.query.filter_by(user_id=user_id).all() if r.category)
    categories.update(b.category for b in Budget.query.filter_by(user_id=user_id).all() if b.category)
    categories.update(m.category for m in MerchantMemory.query.filter_by(user_id=user_id).all() if m.category)
    categories.update(tx.category for tx in Transaction.query.filter_by(user_id=user_id).all() if tx.category)
    categories.update(IMPORT_REVIEW_BASE_CATEGORIES)
    categories.update([
        "Eating Out", "Subscription", "Travel", "Transfer / Payment",
        "Internal Transfer", "Cash Withdrawal"
    ])
    ordered = []
    seen = set()
    for category in IMPORT_REVIEW_BASE_CATEGORIES:
        if category in categories and category not in seen:
            ordered.append(category)
            seen.add(category)
    for category in sorted(categories):
        if category not in seen:
            ordered.append(category)
            seen.add(category)
    return ordered


TRANSACTION_UI_CATEGORY_ALIASES = {
    "Eating Out": "Food & Drink",
    "Subscription": "Subscriptions",
    "Transfer / Payment": "Transfer",
    "Internal Transfer": "Transfer",
}

TRANSACTION_UI_CATEGORY_ORDER = [
    "Income",
    "Transfer",
    "Credit Card Payment",
    "Food & Drink",
    "Groceries",
    "Transport",
    "Gas",
    "Shopping",
    "Housing",
    "Utilities",
    "Health",
    "Subscriptions",
    "Entertainment",
    "Savings",
    "Other",
    "Needs Review",
]


def transaction_ui_category(category):
    cleaned = (category or "").strip()
    if not cleaned:
        return ""
    return TRANSACTION_UI_CATEGORY_ALIASES.get(cleaned, cleaned)


def transaction_ui_category_choices(user_id):
    categories = {
        transaction_ui_category(category)
        for category in import_category_choices(user_id)
        if transaction_ui_category(category)
    }
    ordered = []
    seen = set()
    for category in TRANSACTION_UI_CATEGORY_ORDER:
        if category in categories and category not in seen:
            ordered.append(category)
            seen.add(category)
    for category in sorted(categories):
        if category not in seen:
            ordered.append(category)
            seen.add(category)
    return ordered


TRANSACTION_STATUS_OPTIONS = [
    ("needs_attention", "Needs attention"),
    ("reviewed", "Reviewed"),
    ("errors", "Errors"),
]


def canonical_transaction_category(category):
    cleaned = (category or "").strip()
    if not cleaned:
        return "Needs Review"
    normalized = transaction_ui_category(cleaned)
    return normalized or "Needs Review"


def save_import_preview(user_id, payload, preview_id=None, store_in_session=True):
    preview_id = preview_id or f"{user_id}_{uuid.uuid4().hex}"
    preview_path = os.path.join(get_import_preview_dir(), f"{preview_id}.json")
    with open(preview_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    if store_in_session:
        session["import_preview_id"] = preview_id
    return preview_id


def load_import_preview_by_id(preview_id):
    if not preview_id:
        return None
    preview_path = os.path.join(get_import_preview_dir(), f"{preview_id}.json")
    if not os.path.exists(preview_path):
        return None
    with open(preview_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_import_preview():
    preview_id = session.get("import_preview_id")
    preview = load_import_preview_by_id(preview_id)
    if preview is None:
        session.pop("import_preview_id", None)
    return preview


def activate_import_preview(preview_id):
    if preview_id and load_import_preview_by_id(preview_id):
        session["import_preview_id"] = preview_id
        return True
    return False


def clear_import_preview():
    preview_id = session.pop("import_preview_id", None)
    if not preview_id:
        return
    preview_path = os.path.join(get_import_preview_dir(), f"{preview_id}.json")
    if os.path.exists(preview_path):
        os.remove(preview_path)


def set_last_import_account(account_id):
    try:
        session["last_import_account_id"] = int(account_id)
    except (TypeError, ValueError):
        session.pop("last_import_account_id", None)


def get_last_import_account_id(accounts):
    stored_id = session.get("last_import_account_id")
    if stored_id and any(account.id == stored_id for account in accounts or []):
        return stored_id
    if len(accounts or []) == 1:
        return accounts[0].id
    return None


def normalize_tag_label(raw_tag):
    cleaned = re.sub(r"[^a-z0-9&+/\- ]", " ", (raw_tag or "").strip().lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,#")
    if not cleaned:
        return ""
    return cleaned[:32]


def parse_tags(raw_value):
    if not raw_value:
        return []
    if isinstance(raw_value, (list, tuple, set)):
        raw_parts = raw_value
    else:
        raw_parts = re.split(r"[,|\n]+", str(raw_value))
    tags = []
    seen = set()
    for part in raw_parts:
        tag = normalize_tag_label(part)
        if tag and tag not in seen:
            seen.add(tag)
            tags.append(tag)
    return tags[:8]


def serialize_tags(tags):
    return ",".join(parse_tags(tags))


def display_tag(tag):
    words = []
    for token in (tag or "").split():
        words.append(token.upper() if len(token) <= 3 and token.isalpha() else token.title())
    return " ".join(words)


def tag_filter_clauses(tag):
    tag = normalize_tag_label(tag)
    if not tag:
        return []
    return [
        Transaction.tags == tag,
        Transaction.tags.like(f"{tag},%"),
        Transaction.tags.like(f"%,{tag},%"),
        Transaction.tags.like(f"%,{tag}"),
    ]


def latest_import_batch_for_user(user_id):
    if not user_id:
        return None
    batch = ImportBatch.query.filter_by(user_id=user_id).order_by(ImportBatch.created_at.desc(), ImportBatch.id.desc()).first()
    if not batch:
        return None
    account = Account.query.get(batch.account_id)
    return {
        "id": batch.id,
        "account_id": batch.account_id,
        "account_name": account.name if account and account.user_id == user_id else "Unknown account",
        "imported_count": batch.imported_count,
        "net_change": round(float(batch.net_change or 0), 2),
        "starting_balance": round(float(batch.starting_balance or 0), 2),
        "ending_balance": round(float(batch.ending_balance or 0), 2),
        "balance_mode": batch.balance_mode,
        "created_at": batch.created_at,
    }


def parse_import_job_summary(raw_summary):
    if not raw_summary:
        return {}
    if isinstance(raw_summary, dict):
        return raw_summary
    try:
        return json.loads(raw_summary)
    except (TypeError, ValueError):
        return {}


def import_job_status_label(status):
    labels = {
        "queued": "Uploaded",
        "processing": "Processing",
        "completed": "Ready for review",
        "imported": "Imported",
        "failed": "Failed",
    }
    return labels.get((status or "").lower(), "Queued")


IMPORT_JOB_STAGE_LABELS = {
    "uploaded": "Uploaded",
    "extracting": "Extracting transactions",
    "filtering": "Filtering non-transactions",
    "cleaning": "Cleaning descriptions",
    "categorizing": "Categorizing",
    "saving": "Saving",
    "complete": "Complete",
    "failed": "Failed",
}


def import_job_stage_label(stage):
    return IMPORT_JOB_STAGE_LABELS.get((stage or "").lower(), "Uploaded")


def import_job_status_tone(status):
    tones = {
        "queued": "info",
        "processing": "info",
        "completed": "success",
        "imported": "success",
        "failed": "danger",
    }
    return tones.get((status or "").lower(), "info")


def update_import_job_progress(job_id, stage=None, progress=None, status=None, summary=None, error_message=None):
    job = ImportJob.query.get(job_id)
    if not job:
        return None
    if stage:
        job.current_stage = stage
    if progress is not None:
        job.progress_percent = max(0, min(100, int(progress)))
    if status:
        job.status = status
    if summary is not None:
        job.summary_json = json.dumps(summary)
    if error_message is not None:
        job.error_message = (error_message or "")[:255] or None
    db.session.commit()
    return job


def recent_import_jobs_for_user(user_id, limit=5):
    if not user_id:
        return []
    jobs = (
        ImportJob.query
        .filter_by(user_id=user_id)
        .order_by(ImportJob.created_at.desc(), ImportJob.id.desc())
        .limit(limit)
        .all()
    )
    account_map = {account.id: account.name for account in Account.query.filter_by(user_id=user_id).all()}
    rows = []
    for job in jobs:
        summary = parse_import_job_summary(job.summary_json)
        raw_status = (job.status or "queued").lower()
        display_status = raw_status
        if raw_status == "completed" and job.preview_id:
            display_status = "completed"
        rows.append({
            "id": job.id,
            "account_id": job.account_id,
            "account_name": account_map.get(job.account_id, "Unknown account"),
            "status": display_status,
            "status_label": import_job_status_label(display_status),
            "status_tone": import_job_status_tone(display_status),
            "current_stage": (job.current_stage or "uploaded").lower(),
            "stage_label": import_job_stage_label(job.current_stage),
            "progress_percent": max(0, min(100, int(job.progress_percent or 0))),
            "file_count": job.file_count or 0,
            "source_files": parse_import_job_summary(job.source_files) if job.source_files else [],
            "summary": summary,
            "preview_id": job.preview_id,
            "is_ready_for_review": (job.status or "").lower() == "completed" and bool(job.preview_id),
            "error_message": job.error_message or "",
            "created_at": job.created_at,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
        })
    return rows


def queue_import_job(user_id, account_id, file_storages):
    job_id = uuid.uuid4().hex[:32]
    stored_files = []
    file_names = []
    for index, file_storage in enumerate(file_storages, start=1):
        original_name = file_storage.filename or f"statement-{index}.csv"
        destination_name = f"{index:02d}_{secure_filename(original_name) or f'statement-{index}.dat'}"
        destination_path = import_job_file_path(job_id, destination_name)
        file_storage.save(destination_path)
        stored_files.append({
            "path": destination_path,
            "filename": original_name,
        })
        file_names.append(original_name)

    job = ImportJob(
        id=job_id,
        user_id=user_id,
        account_id=int(account_id),
        status="queued",
        current_stage="uploaded",
        progress_percent=8,
        source_files=json.dumps(stored_files),
        file_count=len(stored_files),
        summary_json=json.dumps({}),
    )
    db.session.add(job)
    log_activity(
        user_id,
        "Queued statement import",
        f"{len(stored_files)} file{'s' if len(stored_files) != 1 else ''} queued for background processing.",
        kind="import_queued",
        icon="bi-cloud-arrow-up-fill",
        target_url="/imports",
    )
    db.session.commit()

    worker = threading.Thread(target=process_import_job, args=(job_id,), daemon=True)
    worker.start()
    return job


def process_import_job(job_id):
    with app.app_context():
        db.session.remove()
        job = ImportJob.query.get(job_id)
        if not job:
            return

        try:
            update_import_job_progress(job_id, stage="extracting", progress=16, status="processing")
            job = ImportJob.query.get(job_id)
            job.started_at = datetime.utcnow()
            db.session.commit()

            account = Account.query.get(job.account_id)
            if not account or account.user_id != job.user_id:
                raise ValueError("The selected account is no longer available for this import job.")

            saved_files = parse_import_job_summary(job.source_files) if job.source_files else []
            if not saved_files:
                raise ValueError("No uploaded files were found for this import job.")

            file_storages = []
            open_streams = []
            try:
                for file_meta in saved_files:
                    path = file_meta.get("path")
                    filename = file_meta.get("filename") or os.path.basename(path or "")
                    if not path or not os.path.exists(path):
                        raise ValueError(f"{filename or 'A statement file'} is no longer available for processing.")
                    stream = open(path, "rb")
                    open_streams.append(stream)
                    from werkzeug.datastructures import FileStorage
                    file_storages.append(FileStorage(stream=stream, filename=filename))

                payload, error = build_import_preview(
                    job.user_id,
                    file_storages,
                    job.account_id,
                    progress_callback=lambda stage, progress: update_import_job_progress(job_id, stage=stage, progress=progress, status="processing"),
                )
                if error or not payload:
                    raise ValueError(error or "AkuOS could not prepare a transaction review for this import.")
            finally:
                for stream in open_streams:
                    try:
                        stream.close()
                    except Exception:
                        pass

            payload["import_job_id"] = job.id
            preview_id = save_import_preview(job.user_id, payload, preview_id=f"job_{job.id}", store_in_session=False)

            summary = payload.get("summary", {})
            summary_payload = {
                "transaction_count": summary.get("transaction_count", len(payload.get("rows", []))),
                "ignored_row_count": summary.get("ignored_row_count", 0),
                "needs_review_count": summary.get("needs_review_count", 0),
                "ready_count": summary.get("ready_count", 0),
                "auto_approved_count": summary.get("auto_approved_count", 0),
                "duplicate_count": summary.get("duplicate_existing_count", 0) + summary.get("duplicate_file_count", 0),
                "net_impact": summary.get("net_impact", 0),
                "file_count": len(saved_files),
            }
            job.preview_id = preview_id
            job.summary_json = json.dumps(summary_payload)
            job.current_stage = "complete"
            job.progress_percent = 100
            job.status = "completed"
            job.error_message = None
            job.completed_at = datetime.utcnow()
            log_activity(
                job.user_id,
                "Statement review ready",
                f"{summary.get('transaction_count', len(payload.get('rows', [])))} transactions prepared with {summary.get('needs_review_count', 0)} needing attention.",
                kind="import_processed",
                icon="bi-hourglass-split",
                target_url="/imports",
            )
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            job = ImportJob.query.get(job_id)
            if job:
                job.status = "failed"
                job.current_stage = "failed"
                job.progress_percent = 100
                job.error_message = str(exc)[:255]
                job.completed_at = datetime.utcnow()
                db.session.commit()
        finally:
            remove_import_job_files(job_id)
            db.session.remove()


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
        fingerprints.add(transaction_fingerprint(tx.date, transaction_reference_description(tx), tx.amount))
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
    "account summary", "daily balance summary", "subtotals", "subtotal",
    "page ", "beginning balance", "ending balance", "available balance",
    "daily balance", "total fees", "customer service", "account number",
    "statement period", "transactions", "description", "balance forward",
    "average ledger balance", "checks paid", "other withdrawals", "deposits and additions",
    "service charges", "account activity", "important information", "member fdic",
    "page total", "continued on next page", "fees charged", "interest charged",
    "interest charge", "interest summary", "foreign currency", "exchange rate",
    "currency conversion", "merchant amount", "cash advances",
)
PDF_TRANSACTION_TYPE_PATTERNS = [
    (re.compile(r"\bach\s+deposit\b|\bdirect\s+deposit\b", re.I), "Income", "Income"),
    (re.compile(r"\batm\b", re.I), "Cash Withdrawal", "Cash Withdrawal"),
    (re.compile(r"\belectronic\s+pmt\b|\bpayment thank you\b|\bautopay payment\b|\bcredit card payment\b|\bcapital one(?:\s+online)? payment\b|\bpayment received\b", re.I), "Bills/Payments", "Transfer"),
    (re.compile(r"\bdbcrd\b|\bpurchase\b|\bpur\b", re.I), "Expense", None),
]
PDF_ACTIVE_SECTION_PATTERNS = [
    (re.compile(r"^\s*transactions(?:\s*\(continued\))?\s*$", re.I), "transactions"),
    (re.compile(r"^\s*payments,\s*credits?\s+and\s+adjustments\s*$", re.I), "payments_credits_adjustments"),
    (re.compile(r"^\s*deposits?(?:\s+and\s+credits?)?\s*$", re.I), "transactions"),
    (re.compile(r"^\s*deposits?\s+and\s+additions\s*$", re.I), "transactions"),
    (re.compile(r"^\s*electronic\s+payments?\s*$", re.I), "payments_credits_adjustments"),
    (re.compile(r"^\s*payments?\s*$", re.I), "payments_credits_adjustments"),
    (re.compile(r"^\s*other\s+withdrawals?\s*$", re.I), "transactions"),
    (re.compile(r"^\s*checks?\s+paid\s*$", re.I), "transactions"),
    (re.compile(r"^\s*debit\s+card\s+purchases?\s*$", re.I), "transactions"),
]
PDF_BLOCKED_SECTION_PATTERNS = [
    re.compile(r"^\s*account summary\s*$", re.I),
    re.compile(r"^\s*daily balance summary\s*$", re.I),
    re.compile(r"^\s*totals?\s*$", re.I),
    re.compile(r"^\s*fees?\s*(?:charged|summary)?\s*$", re.I),
    re.compile(r"^\s*interest\s*(?:charged|summary|details?)?\s*$", re.I),
    re.compile(r"^\s*rewards?\s+summary\s*$", re.I),
    re.compile(r"^\s*cash advances?\s*$", re.I),
]
PDF_FOREIGN_CURRENCY_PATTERNS = [
    re.compile(r"\bforeign currency\b", re.I),
    re.compile(r"\bexchange rate\b", re.I),
    re.compile(r"\bcurrency conversion\b", re.I),
    re.compile(r"\bmerchant amount\b", re.I),
    re.compile(r"\bconverted from\b", re.I),
    re.compile(r"\busd\b.*\bexchange\b", re.I),
    re.compile(r"\bexchange\b.*\brate\b", re.I),
    re.compile(r"\b(?:usd|cad|eur|gbp|jpy|mxn|aud|chf)\b.*\b(?:usd|cad|eur|gbp|jpy|mxn|aud|chf)\b", re.I),
    re.compile(r"\b(?:usd|cad|eur|gbp|jpy|mxn|aud|chf)\b", re.I),
]
PDF_DESCRIPTION_PREFIX_PATTERNS = [
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
]


def normalize_pdf_cell(value):
    return " ".join(str(value or "").split()).strip()


def parse_statement_date_with_fallback(value, reference_year=None):
    parsed = parse_date_any(value)
    if parsed:
        return parsed

    cleaned = normalize_pdf_cell(value)
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


def pdf_section_for_line(line):
    cleaned = normalize_pdf_cell(line)
    if not cleaned:
        return None
    for pattern, section_name in PDF_ACTIVE_SECTION_PATTERNS:
        if pattern.match(cleaned):
            return section_name
    for pattern in PDF_BLOCKED_SECTION_PATTERNS:
        if pattern.match(cleaned):
            return "__blocked__"
    return None


def is_foreign_currency_followup(line):
    cleaned = normalize_pdf_cell(line)
    if not cleaned:
        return False
    if any(pattern.search(cleaned) for pattern in PDF_FOREIGN_CURRENCY_PATTERNS):
        return True
    amount_tokens = PDF_AMOUNT_PATTERN.findall(cleaned)
    return len(amount_tokens) >= 2 and bool(re.search(r"\b(?:rate|currency|converted)\b", cleaned, re.I))


def pdf_date_matches_with_values(line):
    cleaned = normalize_pdf_cell(line)
    matches = []
    for match in PDF_DATE_PATTERN.finditer(cleaned):
        parsed = parse_statement_date_with_fallback(match.group(0))
        if parsed:
            matches.append({"match": match, "parsed": parsed})
    return matches


def pdf_description_between_dates_and_amount(cleaned, date_matches, amount_match):
    if not amount_match:
        return ""
    if date_matches:
        start_index = date_matches[-1]["match"].end()
    else:
        start_index = 0
    description = cleaned[start_index:amount_match.start()].strip(" -|")
    if description:
        return description
    if date_matches:
        description = cleaned[date_matches[0]["match"].end():amount_match.start()].strip(" -|")
    return description


def choose_pdf_amount_match(cleaned, date_matches):
    amount_matches = list(PDF_AMOUNT_PATTERN.finditer(cleaned))
    if not amount_matches:
        return None
    if date_matches:
        last_date_end = date_matches[-1]["match"].end()
        trailing_matches = [match for match in amount_matches if match.start() > last_date_end]
        if trailing_matches:
            return trailing_matches[-1]
    return amount_matches[-1]


def looks_like_pdf_transaction_candidate(line):
    cleaned = normalize_pdf_cell(line)
    if not cleaned or is_pdf_noise_line(cleaned) or is_foreign_currency_followup(cleaned):
        return False
    date_matches = pdf_date_matches_with_values(cleaned)
    amount_match = choose_pdf_amount_match(cleaned, date_matches)
    if not date_matches or not amount_match:
        return False
    description = pdf_description_between_dates_and_amount(cleaned, date_matches, amount_match)
    return bool(re.search(r"[A-Za-z]", description or cleaned))


def is_pdf_continuation_line(line):
    cleaned = normalize_pdf_cell(line)
    if not cleaned or is_pdf_noise_line(cleaned) or is_foreign_currency_followup(cleaned):
        return False
    if pdf_date_matches_with_values(cleaned):
        return False
    amount_matches = list(PDF_AMOUNT_PATTERN.finditer(cleaned))
    if amount_matches and not re.search(r"[A-Za-z]", cleaned):
        return False
    return bool(re.search(r"[A-Za-z]", cleaned))


def append_pdf_continuation(record, line):
    continuation = normalize_pdf_cell(line)
    if not record or not continuation:
        return
    current_raw = normalize_pdf_cell(record.get("raw_description") or record.get("description") or "")
    combined = f"{current_raw} {continuation}".strip()
    record["raw_description"] = combined
    cleaned_description = clean_transaction_description(combined)
    if cleaned_description:
        record["description"] = cleaned_description


def classify_pdf_transaction_type(raw_text, section_name=None):
    text = normalize_pdf_cell(raw_text)
    if section_name == "payments_credits_adjustments":
        return "Bills/Payments", "Transfer"
    for pattern, label, default_category in PDF_TRANSACTION_TYPE_PATTERNS:
        if pattern.search(text):
            return label, default_category
    return None, None


def strip_pdf_transaction_prefix(description):
    cleaned = normalize_pdf_cell(description)
    for pattern in PDF_DESCRIPTION_PREFIX_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    return cleaned.strip(" -")


def build_pdf_transaction_description(description, raw_text, transaction_type):
    stripped = strip_pdf_transaction_prefix(description or raw_text)
    cleaned = clean_transaction_description(stripped or raw_text)
    generic_by_type = {
        "Income": {"Deposit", "Credit"},
        "Cash Withdrawal": {"Withdrawal", "Atm"},
        "Bills/Payments": {"Payment", "Credit Card"},
        "Expense": {"Purchase"},
    }
    if cleaned:
        if transaction_type in generic_by_type and cleaned in generic_by_type[transaction_type]:
            cleaned = ""
        else:
            return cleaned
    fallback_map = {
        "Income": "Deposit",
        "Cash Withdrawal": "ATM Withdrawal",
        "Bills/Payments": "Electronic Payment",
        "Expense": "Card Purchase",
    }
    return fallback_map.get(transaction_type, "")


def parse_pdf_line_record(line, source_document, row_index, section_name=None):
    if section_name not in {"transactions", "payments_credits_adjustments"}:
        return None
    cleaned = normalize_pdf_cell(line)
    if is_pdf_noise_line(cleaned) or is_foreign_currency_followup(cleaned):
        return None

    date_matches = pdf_date_matches_with_values(cleaned)
    chosen_amount_match = choose_pdf_amount_match(cleaned, date_matches)
    if not date_matches or not chosen_amount_match:
        return None

    parsed_date = date_matches[0]["parsed"]
    sign_hint = infer_statement_sign(cleaned, chosen_amount_match.group(0))
    amount = parse_statement_amount(chosen_amount_match.group(0), force_sign=sign_hint)
    description = pdf_description_between_dates_and_amount(cleaned, date_matches, chosen_amount_match)

    transaction_type, default_category = classify_pdf_transaction_type(description or cleaned, section_name=section_name)
    cleaned_description = build_pdf_transaction_description(description, cleaned, transaction_type)
    requires_manual_fields = parsed_date is None or not cleaned_description or amount is None or not re.search(r"[A-Za-z]", cleaned_description or "")
    if requires_manual_fields:
        return None

    return {
        "source_document": source_document,
        "raw_source": cleaned,
        "date": parsed_date.isoformat() if parsed_date else "",
        "description": cleaned_description,
        "raw_description": description or cleaned,
        "amount": round(amount, 2) if amount is not None else "",
        "source_category": default_category or "",
        "raw_category": "",
        "category": "",
        "category_source": "",
        "fingerprint": f"pdfline|{source_document}|{row_index}|{normalize_text(cleaned)}",
        "requires_manual_fields": False,
        "manual_reason": "",
        "transaction_type": transaction_type or "",
        "parser_label": f"PDF line parser · {section_name.replace('_', ' ')}",
    }


def parse_pdf_table_row_record(cells, source_document, row_index, section_name=None):
    if section_name not in {"transactions", "payments_credits_adjustments"}:
        return None
    normalized_cells = [normalize_pdf_cell(cell) for cell in cells if normalize_pdf_cell(cell)]
    if len(normalized_cells) < 2:
        return None

    raw_line = " | ".join(normalized_cells)
    if is_pdf_noise_line(raw_line) or is_foreign_currency_followup(raw_line):
        return None

    date_indexes = []
    parsed_date = None
    for idx, cell in enumerate(normalized_cells):
        parsed = parse_statement_date_with_fallback(cell)
        if parsed:
            date_indexes.append(idx)
            if parsed_date is None:
                parsed_date = parsed

    amount_idx = None
    amount = None
    search_start = (date_indexes[-1] + 1) if date_indexes else 0
    for idx in range(search_start, len(normalized_cells)):
        cell = normalized_cells[idx]
        amount_tokens = PDF_AMOUNT_PATTERN.findall(cell)
        if amount_tokens:
            amount_idx = idx
            token = amount_tokens[0]
            amount = parse_statement_amount(token, force_sign=infer_statement_sign(raw_line, token))
            break
        fallback_amount = safe_float(cell) if idx > search_start else None
        if fallback_amount is not None:
            amount_idx = idx
            amount = -abs(fallback_amount)
            break
    if amount_idx is None:
        for idx in range(len(normalized_cells) - 1, -1, -1):
            cell = normalized_cells[idx]
            amount_tokens = PDF_AMOUNT_PATTERN.findall(cell)
            if amount_tokens:
                amount_idx = idx
                token = amount_tokens[0]
                amount = parse_statement_amount(token, force_sign=infer_statement_sign(raw_line, token))
                break

    description_parts = []
    for idx, cell in enumerate(normalized_cells):
        if idx in date_indexes or idx == amount_idx:
            continue
        description_parts.append(cell)
    description = " ".join(description_parts).strip()
    if not parsed_date and not amount and not description:
        return None

    transaction_type, default_category = classify_pdf_transaction_type(description or raw_line, section_name=section_name)
    cleaned_description = build_pdf_transaction_description(description, raw_line, transaction_type)
    requires_manual_fields = parsed_date is None or not cleaned_description or amount is None or not re.search(r"[A-Za-z]", cleaned_description or "")
    if requires_manual_fields:
        return None
    return {
        "source_document": source_document,
        "raw_source": raw_line,
        "date": parsed_date.isoformat() if parsed_date else "",
        "description": cleaned_description,
        "raw_description": description or raw_line,
        "amount": round(amount, 2) if amount is not None else "",
        "source_category": default_category or "",
        "raw_category": "",
        "category": "",
        "category_source": "",
        "fingerprint": f"pdftable|{source_document}|{row_index}|{normalize_text(raw_line)}",
        "requires_manual_fields": False,
        "manual_reason": "",
        "transaction_type": transaction_type or "",
        "parser_label": f"PDF table parser · {section_name.replace('_', ' ')}",
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
        raw_description = (row.get(desc_key) or "").strip()
        description = clean_transaction_description(raw_description)
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
            "raw_description": raw_description,
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
    readable_page_count = 0
    sections_found = set()
    candidate_row_count = 0
    filtered_candidate_count = 0
    continuation_count = 0
    section_row_count = 0

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text() or ""
                if page_text.strip():
                    readable_page_count += 1
                current_section = None
                page_active_sections = []
                seen_sections = set()
                last_record_for_section = {}
                for line_index, line in enumerate(page_text.splitlines(), start=1):
                    section_marker = pdf_section_for_line(line)
                    if section_marker == "__blocked__":
                        current_section = None
                        continue
                    if section_marker:
                        current_section = section_marker
                        sections_found.add(section_marker)
                        if section_marker not in seen_sections:
                            seen_sections.add(section_marker)
                            page_active_sections.append(section_marker)
                        continue
                    if current_section in {"transactions", "payments_credits_adjustments"} and looks_like_pdf_transaction_candidate(line):
                        candidate_row_count += 1
                    if current_section in {"transactions", "payments_credits_adjustments"}:
                        section_row_count += 1
                    record = parse_pdf_line_record(line, file_storage.filename or "statement.pdf", f"{page_index}_{line_index}", current_section)
                    if not record:
                        if current_section in {"transactions", "payments_credits_adjustments"} and is_pdf_continuation_line(line):
                            previous = last_record_for_section.get(current_section)
                            if previous:
                                append_pdf_continuation(previous, line)
                                continuation_count += 1
                                continue
                        if current_section in {"transactions", "payments_credits_adjustments"} and looks_like_pdf_transaction_candidate(line):
                            filtered_candidate_count += 1
                        skipped_rows += 1
                        continue
                    raw_key = normalize_text(record["raw_source"])
                    if raw_key in seen_raw_keys:
                        continue
                    seen_raw_keys.add(raw_key)
                    detected_methods.add("Line extraction")
                    extracted_rows.append(record)
                    last_record_for_section[current_section] = record

                page_tables = page.extract_tables() or []
                if page_active_sections:
                    for table_index, table in enumerate(page_tables, start=1):
                        current_table_section = page_active_sections[0]
                        for row_index, row in enumerate(table or [], start=1):
                            row_cells = row or []
                            raw_row = " | ".join(normalize_pdf_cell(cell) for cell in row_cells if normalize_pdf_cell(cell))
                            if len(page_active_sections) > 1:
                                if classify_pdf_transaction_type(raw_row, section_name="payments_credits_adjustments")[1] == "Transfer":
                                    current_table_section = "payments_credits_adjustments"
                                else:
                                    current_table_section = "transactions"
                            if current_table_section in {"transactions", "payments_credits_adjustments"} and looks_like_pdf_transaction_candidate(raw_row):
                                candidate_row_count += 1
                            if current_table_section in {"transactions", "payments_credits_adjustments"}:
                                section_row_count += 1
                            record = parse_pdf_table_row_record(
                                row_cells,
                                file_storage.filename or "statement.pdf",
                                f"{page_index}_{table_index}_{row_index}",
                                current_table_section,
                            )
                            if not record:
                                if current_table_section in {"transactions", "payments_credits_adjustments"} and is_pdf_continuation_line(raw_row):
                                    previous = last_record_for_section.get(current_table_section)
                                    if previous:
                                        append_pdf_continuation(previous, raw_row)
                                        continuation_count += 1
                                        continue
                                if current_table_section in {"transactions", "payments_credits_adjustments"} and looks_like_pdf_transaction_candidate(raw_row):
                                    filtered_candidate_count += 1
                                skipped_rows += 1
                                continue
                            raw_key = normalize_text(record["raw_source"])
                            if raw_key in seen_raw_keys:
                                continue
                            seen_raw_keys.add(raw_key)
                            detected_methods.add("Table extraction")
                            extracted_rows.append(record)
                            last_record_for_section[current_table_section] = record
    except Exception:
        return None, f"Could not read {file_storage.filename or 'the PDF'}. Try another statement or convert it to CSV."

    if not extracted_rows:
        filename = file_storage.filename or "the PDF"
        if readable_page_count == 0:
            return None, f"No readable text was found in {filename}. The PDF may be image-only or protected."
        if not sections_found:
            return None, f"AkuOS could not find a Transactions section in {filename}. Try a full statement export instead of a summary PDF."
        if candidate_row_count == 0:
            if section_row_count > 0:
                return None, f"A transactions section was found in {filename}, but the rows did not match the expected date, description, and amount layout."
            return None, f"A transactions section was found in {filename}, but no transaction rows could be detected."
        if filtered_candidate_count >= candidate_row_count:
            return None, f"AkuOS found transaction-like rows in {filename}, but all of them were filtered out during parsing. This usually means the statement layout needs a parser adjustment."
        return None, f"No valid transactions were detected in {filename}."

    return {
        "rows": extracted_rows,
        "skipped_rows": skipped_rows,
        "detected_columns": {
            "date": "PDF statement detection",
            "description": "PDF statement detection",
            "amount": "PDF statement detection",
            "source_category": "Not provided",
            "parser": ", ".join(sorted(detected_methods)) or "Heuristic parser",
            "sections": ", ".join(sorted(section.replace("_", " ") for section in sections_found)) or "Not detected",
            "continuations": continuation_count,
        }
    }, None


def detect_statement_file_type(file_storage):
    filename = (file_storage.filename or "").lower()
    if filename.endswith(".pdf"):
        return "pdf"
    return "csv"


def import_review_priority(row):
    if row.get("requires_manual_fields"):
        return 0
    bucket = row.get("confidence_bucket")
    if bucket == "low":
        return 1
    if bucket == "medium":
        return 2
    if bucket == "high":
        return 3
    if row.get("is_duplicate") or row.get("default_row_action") == "skip":
        return 4
    return 5


def build_import_preview(user_id, file_storages, account_id, progress_callback=None):
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
    low_confidence_count = 0
    medium_confidence_count = 0
    high_confidence_count = 0
    uncategorized_count = 0
    auto_approved_count = 0
    source_breakdown = defaultdict(int)
    file_summaries = []
    detected_columns = []
    row_counter = 0
    net_impact = 0.0
    transfer_count = 0
    expense_impact = 0.0
    payment_impact = 0.0
    importable_count = 0
    extracted_file_rows = []
    active_memories = active_merchant_memories_for_user(user_id)

    if progress_callback:
        progress_callback("extracting", 18)

    total_files = max(1, len(file_storages))
    for file_index, file_storage in enumerate(file_storages, start=1):
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
        extracted_file_rows.append((file_type, file_storage, file_rows))
        if progress_callback:
            progress_callback("extracting", 18 + int((file_index / total_files) * 14))

    if progress_callback:
        progress_callback("filtering", 38)
        progress_callback("cleaning", 54)

    total_rows = max(1, sum(len(file_rows) for _, _, file_rows in extracted_file_rows))
    processed_rows = 0
    if progress_callback:
        progress_callback("categorizing", 68)

    for file_type, file_storage, file_rows in extracted_file_rows:
        for row in file_rows:
            processed_rows += 1
            date_value = row.get("date", "")
            raw_description = (row.get("raw_description") or row.get("description") or "").strip()
            description = preferred_display_name_for_user(
                user_id,
                raw_description or row.get("description") or "",
                fallback=clean_transaction_description(row.get("description") or raw_description),
            )
            matched_memory = find_best_merchant_memory(user_id, raw_description or description, memories=active_memories)
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
            detected_category = canonical_transaction_category(source_category or detected_category)

            if source_category:
                detected_category = canonical_transaction_category(source_category)
                category_source = "PDF Type" if file_type == "pdf" else "CSV"
            elif category_source == "Fallback":
                detected_category = "Needs Review"
                category_source = "Needs Review"

            requires_manual_fields = row.get("requires_manual_fields", False) or parsed_date is None or not description or amount is None
            review_required = requires_manual_fields or (detected_category or "").strip().lower() in GENERIC_CATEGORIES or category_source == "Needs Review"
            normalized_detected_category = (detected_category or "").strip().lower()
            auto_approved = False

            if source_category and file_type == "pdf":
                confidence_label = "High confidence"
                confidence_tone = "positive"
                confidence_detail = "Inferred from the PDF transaction pattern."
                confidence_bucket = "high"
                auto_approved = True
            elif source_category:
                confidence_label = "High confidence"
                confidence_tone = "positive"
                confidence_detail = "Category came directly from the imported file."
                confidence_bucket = "high"
                auto_approved = True
            elif requires_manual_fields:
                confidence_label = "Error"
                confidence_tone = "warning"
                confidence_detail = "This row still needs field fixes before it can import cleanly."
                confidence_bucket = "error"
            elif normalized_detected_category in GENERIC_CATEGORIES or category_source == "Needs Review":
                confidence_label = "Uncategorized"
                confidence_tone = "warning"
                confidence_detail = "AkuOS could not find a strong category match yet."
                confidence_bucket = "low"
            elif category_source == "Merchant Memory" or category_source.startswith("Rule"):
                if category_source in {"Merchant Memory", "Rule (exact)", "Rule (startswith)"}:
                    confidence_label = "High confidence"
                    confidence_tone = "positive"
                    confidence_detail = f"Matched using {category_source.lower()}."
                    confidence_bucket = "high"
                    auto_approved = True
                else:
                    confidence_label = "Moderate confidence"
                    confidence_tone = "info"
                    confidence_detail = f"Suggested using {category_source.lower()}."
                    confidence_bucket = "medium"
            elif category_source.startswith("Built-in") or category_source == "Income Fallback":
                confidence_label = "Moderate confidence"
                confidence_tone = "info"
                confidence_detail = f"Suggested using {category_source.lower()}."
                confidence_bucket = "medium"
            else:
                confidence_label = "Low confidence"
                confidence_tone = "warning"
                confidence_detail = "This transaction should be reviewed before import."
                confidence_bucket = "low"

            if confidence_bucket in {"error", "low", "medium"}:
                review_required = True

            if is_existing_duplicate:
                duplicate_existing_count += 1
                row_status = "Skipped"
                status_tone = "warning"
                default_row_action = "skip"
            elif is_file_duplicate:
                duplicate_file_count += 1
                row_status = "Skipped"
                status_tone = "warning"
                default_row_action = "skip"
            elif requires_manual_fields:
                manual_fix_count += 1
                row_status = "Error"
                status_tone = "warning"
                default_row_action = "import"
            elif review_required:
                needs_review_count += 1
                row_status = "Needs attention"
                status_tone = "warning"
                default_row_action = "import"
            else:
                ready_count += 1
                row_status = "Ready"
                status_tone = "positive"
                default_row_action = "import"

            if normalized_detected_category in GENERIC_CATEGORIES:
                uncategorized_count += 1
            if confidence_bucket == "low":
                low_confidence_count += 1
            elif confidence_bucket == "medium":
                medium_confidence_count += 1
            elif confidence_bucket == "high":
                high_confidence_count += 1
            if auto_approved and not (is_existing_duplicate or is_file_duplicate):
                auto_approved_count += 1

            row_kind = "income" if (amount or 0) > 0 else "expense"
            if matched_memory and (matched_memory.subtype or "").strip().lower() in VALID_TRANSACTION_SUBTYPES:
                row_kind = matched_memory.subtype.strip().lower()
            elif detected_category in {"Transfer", "Credit Card Payment"}:
                row_kind = "payment" if detected_category == "Credit Card Payment" else "transfer"
            source_breakdown[category_source] += 1
            if row_kind == "payment":
                transfer_count += 1
            if not (is_existing_duplicate or is_file_duplicate):
                importable_count += 1
                net_impact += float(amount_value or 0)
                if (amount or 0) < 0:
                    if row_kind == "payment":
                        payment_impact += abs(float(amount_value or 0))
                    elif row_kind == "expense":
                        expense_impact += abs(float(amount_value or 0))
            preview_rows.append({
                "row_id": row_counter,
                "source_document": row.get("source_document") or (file_storage.filename or ""),
                "parser_label": row.get("parser_label") or file_type.upper(),
                "raw_source": row.get("raw_source") or "",
                "raw_description": raw_description,
                "date": date_value,
                "description": description,
                "display_name": description,
                "amount": amount_value,
                "category": detected_category,
                "source_category": source_category,
                "category_source": category_source,
                "row_status": row_status,
                "status_tone": status_tone,
                "status_label": row_status,
                "is_duplicate": is_existing_duplicate or is_file_duplicate,
                "duplicate_reason": row_status if (is_existing_duplicate or is_file_duplicate) else "",
                "review_required": review_required,
                "requires_manual_fields": requires_manual_fields,
                "manual_reason": row.get("manual_reason", ""),
                "confidence_label": confidence_label,
                "confidence_tone": confidence_tone,
                "confidence_detail": confidence_detail,
                "confidence_bucket": confidence_bucket,
                "auto_approved": auto_approved and not review_required and not (is_existing_duplicate or is_file_duplicate),
                "is_uncategorized": normalized_detected_category in GENERIC_CATEGORIES,
                "is_low_confidence": confidence_bucket == "low",
                "default_row_action": default_row_action,
                "fingerprint": fingerprint,
                "row_kind": row_kind,
            })
            row_counter += 1
            if progress_callback and processed_rows == total_rows:
                progress_callback("saving", 86)

    if not preview_rows:
        return None, "No valid transactions were detected in the uploaded files."

    preview_rows = sorted(
        preview_rows,
        key=lambda row: (
            import_review_priority(row),
            row.get("is_duplicate", False),
            row.get("date", ""),
            str(row.get("display_name") or row.get("description") or "").lower(),
        )
    )

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
            "low_confidence_count": low_confidence_count,
            "medium_confidence_count": medium_confidence_count,
            "high_confidence_count": high_confidence_count,
            "uncategorized_count": uncategorized_count,
            "ignored_row_count": skipped_rows,
            "duplicate_existing_count": duplicate_existing_count,
            "duplicate_file_count": duplicate_file_count,
            "auto_approved_count": auto_approved_count,
            "skipped_error_count": skipped_rows + duplicate_existing_count + duplicate_file_count,
            "source_breakdown": dict(source_breakdown),
            "net_impact": round(net_impact, 2),
            "expense_impact": round(expense_impact, 2),
            "payment_impact": round(payment_impact, 2),
            "transaction_count": len(preview_rows),
            "transfer_count": transfer_count,
            "importable_count": importable_count,
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


def calculate_safe_to_spend(
    accounts,
    subscriptions,
    budget_rows,
    monthly_income,
    monthly_expenses,
    recurring_monthly_obligations,
    savings_target_amount,
    selected_month,
    selected_year,
    actual_monthly_income=None,
    goal_set_aside_amount=None,
):
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

    recurring_expenses = max(float(recurring_monthly_obligations or remaining_recurring_bills or 0), 0)
    savings_target_amount = max(float(savings_target_amount or 0), 0)
    goal_set_aside_amount = max(float(goal_set_aside_amount or 0), 0)
    base_safe_to_spend = float(monthly_income or 0) - recurring_expenses - savings_target_amount - goal_set_aside_amount
    discretionary_spending_used = max(float(monthly_expenses or 0) - remaining_recurring_bills, 0)
    safe_to_spend_remaining = base_safe_to_spend - discretionary_spending_used
    usage_ratio = (
        min(max(discretionary_spending_used / base_safe_to_spend, 0), 1.5)
        if base_safe_to_spend > 0 else
        (1 if discretionary_spending_used > 0 else 0)
    )
    income_basis_note = ""
    if actual_monthly_income is not None and float(actual_monthly_income or 0) != float(monthly_income or 0):
        income_basis_note = f" This includes a recurring income estimate of ${monthly_income:,.2f} based on confirmed deposits."

    explanation = (
        f"Expected monthly income ${monthly_income:,.2f} minus fixed obligations ${recurring_expenses:,.2f}, "
        f"subscriptions and recurring bills, suggested savings of ${savings_target_amount:,.2f}, "
        f"and goal set-asides of ${goal_set_aside_amount:,.2f} leaves ${base_safe_to_spend:,.2f} available for flexible spending."
        f"{income_basis_note}"
        if monthly_income > 0 else
        "Safe-to-spend will improve once enough income history is available to compare recurring obligations against your monthly inflow."
    )

    return {
        "current_cash": round(current_cash, 2),
        "remaining_recurring_bills": round(remaining_recurring_bills, 2),
        "remaining_budget_commitments": round(remaining_budget_commitments, 2),
        "expected_remaining_spending": round(expected_remaining_spending, 2),
        "safe_to_spend": round(safe_to_spend_remaining, 2),
        "base_safe_to_spend": round(base_safe_to_spend, 2),
        "used_amount": round(discretionary_spending_used, 2),
        "remaining_amount": round(safe_to_spend_remaining, 2),
        "usage_ratio": round(usage_ratio, 4),
        "recurring_expenses": round(recurring_expenses, 2),
        "savings_target_amount": round(savings_target_amount, 2),
        "goal_set_aside_amount": round(goal_set_aside_amount, 2),
        "income_basis": round(float(monthly_income or 0), 2),
        "explanation": explanation,
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


ACCOUNT_KIND_CHOICES = [
    ("checking", "Checking"),
    ("savings", "Savings"),
    ("credit_card", "Credit Card"),
    ("loan", "Loan"),
    ("investment", "Investment"),
    ("cash", "Cash"),
    ("other", "Other"),
]


def resolve_account_kind(account):
    subtype = infer_account_subtype(account)
    if subtype in {"checking", "savings", "credit_card", "investment", "cash"}:
        return subtype
    return "other"


def map_account_kind(kind):
    normalized = (kind or "").strip().lower()
    if normalized == "checking":
        return "asset", "checking"
    if normalized == "savings":
        return "asset", "savings"
    if normalized == "investment":
        return "asset", "investment"
    if normalized == "cash":
        return "asset", "cash"
    if normalized == "credit_card":
        return "liability", "credit_card"
    if normalized == "loan":
        return "liability", "loan"
    return "asset", "other_asset"


def group_import_jobs(import_jobs):
    grouped_rows = []
    grouped_failed = {}
    for job in import_jobs or []:
        if job.get("status") == "failed":
            error_key = ((job.get("error_message") or "").strip().lower(), job.get("account_id"))
            if error_key in grouped_failed:
                grouped_row = grouped_failed[error_key]
                grouped_row["repeat_count"] += 1
                grouped_row["grouped_job_ids"].append(job["id"])
                grouped_row["grouped_file_count"] += int(job.get("file_count") or 0)
                continue
            grouped_row = dict(job)
            grouped_row["repeat_count"] = 1
            grouped_row["grouped_job_ids"] = [job["id"]]
            grouped_row["grouped_file_count"] = int(job.get("file_count") or 0)
            grouped_failed[error_key] = grouped_row
            grouped_rows.append(grouped_row)
        else:
            grouped_row = dict(job)
            grouped_row["repeat_count"] = 1
            grouped_row["grouped_job_ids"] = [job["id"]]
            grouped_row["grouped_file_count"] = int(job.get("file_count") or 0)
            grouped_rows.append(grouped_row)
    return grouped_rows


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
            normalized_desc = normalize_text(transaction_reference_description(tx))
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


def linked_goalable_accounts(accounts):
    return [account for account in (accounts or []) if account.type == "asset"]


def goal_allocations_for_goals(goals):
    goal_ids = [goal.id for goal in (goals or [])]
    if not goal_ids:
        return {}

    allocation_rows = GoalAllocation.query.filter(GoalAllocation.goal_id.in_(goal_ids)).all()
    allocation_map = defaultdict(list)
    for row in allocation_rows:
        allocation_map[row.goal_id].append(row)
    return allocation_map


def validate_account_allocation(user_id, account_id, allocated_amount, exclude_allocation_id=None):
    if not account_id:
        return None, None

    account = Account.query.get(account_id)
    if not account or account.user_id != user_id or account.type != "asset":
        return None, "Choose a valid asset account for this goal allocation."

    allocation_value = max(float(allocated_amount or 0), 0)
    existing_rows = GoalAllocation.query.filter_by(account_id=account_id).all()
    existing_total = 0.0
    for row in existing_rows:
        if exclude_allocation_id and row.id == exclude_allocation_id:
            continue
        existing_total += float(row.allocated_amount or 0)
    projected_total = existing_total + allocation_value

    if projected_total > float(account.balance or 0) + 0.005:
        available = max(float(account.balance or 0) - existing_total, 0)
        return account, f"That allocation would exceed {account.name}'s balance. Available to allocate: ${available:,.2f}."
    return account, None


def goal_allocation_rows(goal, wealth_context):
    allocation_map = wealth_context.get("goal_allocation_map", {})
    accounts_by_id = wealth_context.get("accounts_by_id", {})
    rows = []

    for allocation in allocation_map.get(goal.id, []):
        account = accounts_by_id.get(allocation.account_id)
        rows.append({
            "id": allocation.id,
            "account_id": allocation.account_id,
            "account_name": account.name if account else "Linked Account",
            "allocated_amount": round(float(allocation.allocated_amount or 0), 2),
        })

    if not rows and getattr(goal, "linked_account_id", None) and float(getattr(goal, "allocated_amount", 0) or 0) > 0:
        account = accounts_by_id.get(getattr(goal, "linked_account_id", None))
        rows.append({
            "id": None,
            "account_id": getattr(goal, "linked_account_id", None),
            "account_name": account.name if account else "Linked Account",
            "allocated_amount": round(float(getattr(goal, "allocated_amount", 0) or 0), 2),
        })
    return rows


def resolve_goal_current_amount(goal, wealth_context):
    allocation_rows = goal_allocation_rows(goal, wealth_context)
    if allocation_rows:
        account_count = len(allocation_rows)
        total_allocated = sum(float(row["allocated_amount"] or 0) for row in allocation_rows)
        if account_count == 1:
            return total_allocated, f"Allocated from {allocation_rows[0]['account_name']}"
        return total_allocated, f"Allocated across {account_count} accounts"

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
        allocation_rows = goal_allocation_rows(goal, wealth_context)
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
            "linked_account_id": allocation_rows[0]["account_id"] if allocation_rows else getattr(goal, "linked_account_id", None),
            "linked_account_name": allocation_rows[0]["account_name"] if allocation_rows else (
                wealth_context.get("accounts_by_id", {}).get(getattr(goal, "linked_account_id", None)).name
                if getattr(goal, "linked_account_id", None) and wealth_context.get("accounts_by_id", {}).get(getattr(goal, "linked_account_id", None))
                else None
            ),
            "linked_account_names": [row["account_name"] for row in allocation_rows],
            "allocation_rows": allocation_rows,
            "allocated_amount": round(sum(float(row["allocated_amount"] or 0) for row in allocation_rows), 2) if allocation_rows else round(float(getattr(goal, "allocated_amount", 0) or 0), 2),
            "is_account_linked": bool(allocation_rows or getattr(goal, "linked_account_id", None)),
            "source_label": source_label,
            "progress_pct": round(progress_pct, 1),
            "gap_remaining": round(gap_remaining, 2),
        })
        if target_amount > 0:
            total_progress_ratio += min(current_amount / target_amount, 1.0)

    average_progress = (total_progress_ratio / len(goal_rows)) if goal_rows else None
    return goal_rows, average_progress


def build_goal_dashboard_state(goal_rows):
    if not goal_rows:
        return None, []

    def sort_key(goal):
        goal_type = (goal.get("goal_type") or "").lower()
        goal_name = (goal.get("name") or "").lower()
        is_emergency = goal_type == "emergency_fund" or "emergency" in goal_name
        target_date = goal.get("target_date") or date.max
        incomplete_rank = 0 if float(goal.get("progress_pct") or 0) < 100 else 1
        return (
            0 if is_emergency else 1,
            incomplete_rank,
            target_date,
            float(goal.get("gap_remaining") or 0),
            -float(goal.get("progress_pct") or 0),
        )

    ordered_goals = sorted(goal_rows, key=sort_key)
    return ordered_goals[0], ordered_goals[1:]


def account_type_breakdown_series(accounts):
    grouped_balances = defaultdict(float)

    for account in accounts or []:
        subtype = infer_account_subtype(account)
        amount = abs(float(account.balance or 0))
        if amount <= 0:
            continue

        if account.type == "liability":
            if subtype == "credit_card":
                label = "Credit Cards"
            elif subtype == "loan":
                label = "Loans"
            else:
                label = "Other Liabilities"
        else:
            if subtype == "checking":
                label = "Checking"
            elif subtype == "cash":
                label = "Cash"
            elif subtype == "savings":
                label = "Savings"
            elif subtype == "investment":
                label = "Investments"
            else:
                label = "Other Assets"

        grouped_balances[label] += amount

    labels = list(grouped_balances.keys())
    values = [round(grouped_balances[label], 2) for label in labels]
    return labels, values


def account_goal_allocation_summary(user_id, account):
    if not account:
        return {
            "goal_rows": [],
            "allocated_total": 0.0,
            "unallocated_balance": 0.0,
            "overallocated": False,
        }

    rows_query = (
        db.session.query(GoalAllocation, FinancialGoal)
        .join(FinancialGoal, FinancialGoal.id == GoalAllocation.goal_id)
        .filter(FinancialGoal.user_id == user_id, GoalAllocation.account_id == account.id)
        .order_by(FinancialGoal.id.asc(), GoalAllocation.id.asc())
        .all()
    )
    rows = []
    allocated_total = 0.0
    for allocation, goal in rows_query:
        allocated_amount = float(allocation.allocated_amount or 0)
        target_amount = float(goal.target_amount or 0)
        progress_pct = min(100.0, (allocated_amount / target_amount) * 100) if target_amount > 0 else 0
        rows.append({
            "id": goal.id,
            "name": goal.name,
            "allocation_id": allocation.id,
            "allocated_amount": round(allocated_amount, 2),
            "target_amount": round(target_amount, 2),
            "progress_pct": round(progress_pct, 1),
            "amount_remaining": round(max(target_amount - allocated_amount, 0), 2),
        })
        allocated_total += allocated_amount

    balance = float(account.balance or 0)
    unallocated_balance = round(balance - allocated_total, 2)
    return {
        "goal_rows": rows,
        "allocated_total": round(allocated_total, 2),
        "unallocated_balance": unallocated_balance,
        "overallocated": allocated_total > balance + 0.005,
    }


def goals_account_allocation_summary(user_id, accounts, goal_rows=None):
    asset_accounts = [account for account in (accounts or []) if account.type == "asset"]
    if not asset_accounts:
        return []

    account_ids = [account.id for account in asset_accounts]
    rows_query = (
        db.session.query(GoalAllocation, FinancialGoal)
        .join(FinancialGoal, FinancialGoal.id == GoalAllocation.goal_id)
        .filter(FinancialGoal.user_id == user_id, GoalAllocation.account_id.in_(account_ids))
        .order_by(GoalAllocation.account_id.asc(), FinancialGoal.name.asc())
        .all()
    )

    allocations_by_account = defaultdict(list)
    for allocation, goal in rows_query:
        allocations_by_account[allocation.account_id].append({
            "goal_id": goal.id,
            "goal_name": goal.name,
            "allocated_amount": round(float(allocation.allocated_amount or 0), 2),
        })

    summary_rows = []
    for account in asset_accounts:
        allocation_rows = allocations_by_account.get(account.id, [])
        allocated_total = round(sum(float(row["allocated_amount"] or 0) for row in allocation_rows), 2)
        unallocated_amount = round(float(account.balance or 0) - allocated_total, 2)
        summary_rows.append({
            "account_id": account.id,
            "account_name": account.name,
            "balance": round(float(account.balance or 0), 2),
            "allocated_amount": allocated_total,
            "unallocated_amount": unallocated_amount,
            "goal_allocations": allocation_rows,
            "suggestions": suggested_allocations_for_account({
                "unallocated_amount": unallocated_amount,
            }, goal_rows or []),
            "overallocated": allocated_total > float(account.balance or 0) + 0.005,
        })
    return summary_rows


def goal_priority_key(goal):
    goal_type = (goal.get("goal_type") or "").lower()
    goal_name = (goal.get("name") or "").lower()
    is_emergency = goal_type == "emergency_fund" or "emergency" in goal_name
    target_date = goal.get("target_date") or date.max
    gap_remaining = float(goal.get("gap_remaining") or 0)
    progress_pct = float(goal.get("progress_pct") or 0)
    return (
        0 if is_emergency else 1,
        0 if gap_remaining > 0 else 1,
        0 if gap_remaining and gap_remaining <= 500 else 1,
        gap_remaining,
        -progress_pct,
        target_date,
        (goal.get("name") or "").lower(),
    )


def suggested_allocations_for_account(account_row, goal_rows):
    if not account_row or float(account_row.get("unallocated_amount") or 0) <= 0:
        return []

    remaining_pool = float(account_row.get("unallocated_amount") or 0)
    suggestions = []
    ranked_goals = sorted(
        [goal for goal in (goal_rows or []) if float(goal.get("gap_remaining") or 0) > 0],
        key=goal_priority_key,
    )

    if not ranked_goals:
        return []

    for goal in ranked_goals:
        if remaining_pool <= 0.01:
            break
        gap_remaining = float(goal.get("gap_remaining") or 0)
        if gap_remaining <= 0:
            continue

        is_emergency = (goal.get("goal_type") or "").lower() == "emergency_fund" or "emergency" in (goal.get("name") or "").lower()
        if is_emergency or gap_remaining <= remaining_pool * 0.45:
            suggested_amount = min(gap_remaining, remaining_pool)
        else:
            remaining_goal_count = max(len(ranked_goals) - len(suggestions), 1)
            suggested_amount = min(gap_remaining, remaining_pool / remaining_goal_count)

        suggested_amount = round(suggested_amount, 2)
        if suggested_amount <= 0:
            continue

        suggestions.append({
            "goal_id": goal["id"],
            "goal_name": goal["name"],
            "suggested_amount": suggested_amount,
        })
        remaining_pool = round(remaining_pool - suggested_amount, 2)

    return suggestions


def upsert_goal_allocation(goal_id, account_id, amount):
    allocation = GoalAllocation.query.filter_by(goal_id=goal_id, account_id=account_id).first()
    normalized_amount = max(float(amount or 0), 0)
    if allocation:
        if normalized_amount <= 0:
            db.session.delete(allocation)
            return "removed"
        allocation.allocated_amount = normalized_amount
        return "updated"
    if normalized_amount > 0:
        db.session.add(GoalAllocation(goal_id=goal_id, account_id=account_id, allocated_amount=normalized_amount))
        return "created"
    return "skipped"


def auto_allocate_account_to_goals(user_id, account, goal_rows):
    account_summary = account_goal_allocation_summary(user_id, account)
    suggestions = suggested_allocations_for_account({
        "unallocated_amount": account_summary["unallocated_balance"],
    }, goal_rows)

    applied = []
    for suggestion in suggestions:
        existing_amount = sum(
            float(row.allocated_amount or 0)
            for row in GoalAllocation.query.filter_by(goal_id=suggestion["goal_id"], account_id=account.id).all()
        )
        new_amount = existing_amount + suggestion["suggested_amount"]
        upsert_goal_allocation(suggestion["goal_id"], account.id, new_amount)
        applied.append({
            **suggestion,
            "account_id": account.id,
            "previous_amount": round(existing_amount, 2),
            "new_amount": round(new_amount, 2),
        })
    return applied


def quick_allocate_goal(user_id, goal, mode):
    account_rows = goals_account_allocation_summary(user_id, Account.query.filter_by(user_id=user_id).all())
    goal_rows, _ = build_goal_progress([goal], {
        "savings_snapshot": {"current_savings": 0},
        "net_worth_breakdown": build_net_worth_breakdown(Account.query.filter_by(user_id=user_id).all()),
        "accounts_by_id": {account.id: account for account in Account.query.filter_by(user_id=user_id).all()},
        "goal_allocation_map": goal_allocations_for_goals([goal]),
    })
    goal_row = goal_rows[0] if goal_rows else None
    if not goal_row:
        return 0.0

    remaining_gap = float(goal_row.get("gap_remaining") or 0)
    if remaining_gap <= 0:
        return 0.0

    ordered_accounts = sorted(account_rows, key=lambda row: (0 if infer_account_subtype(Account.query.get(row["account_id"])) == "savings" else 1, -float(row["unallocated_amount"] or 0), row["account_name"].lower()))
    if mode == "remaining":
        ordered_accounts = ordered_accounts[:1]

    added_total = 0.0
    changes = []
    for account_row in ordered_accounts:
        available = max(float(account_row.get("unallocated_amount") or 0), 0)
        if available <= 0 or remaining_gap <= 0:
            continue
        amount = min(available, remaining_gap)
        previous_amount = sum(
            float(alloc.allocated_amount or 0)
            for alloc in GoalAllocation.query.filter_by(goal_id=goal.id, account_id=account_row["account_id"]).all()
        )
        new_amount = amount + previous_amount
        upsert_goal_allocation(goal.id, account_row["account_id"], new_amount)
        changes.append({
            "goal_id": goal.id,
            "account_id": account_row["account_id"],
            "previous_amount": round(previous_amount, 2),
            "new_amount": round(new_amount, 2),
        })
        added_total += amount
        remaining_gap -= amount
        if mode == "remaining":
            break
    return {
        "added_total": round(added_total, 2),
        "changes": changes,
    }


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
        "accounts_by_id": {account.id: account for account in (accounts or [])},
        "goal_allocation_map": goal_allocations_for_goals(goals),
    }
    goal_rows, average_goal_progress = build_goal_progress(goals, wealth_context)
    primary_goal, secondary_goals = build_goal_dashboard_state(goal_rows)
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
        "primary_goal": primary_goal,
        "secondary_goals": secondary_goals,
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


def monthly_overview_series(transactions, limit=6):
    bucket_map = defaultdict(lambda: {"income": 0.0, "expenses": 0.0})
    for tx in transactions or []:
        key = (tx.date.year, tx.date.month)
        if tx.amount > 0:
            bucket_map[key]["income"] += float(tx.amount or 0)
        elif is_spending_category(tx.category):
            bucket_map[key]["expenses"] += abs(float(tx.amount or 0))

    if not bucket_map:
        today = date.today()
        bucket_map[(today.year, today.month)] = {"income": 0.0, "expenses": 0.0}

    ordered_keys = sorted(bucket_map.keys())[-limit:]
    labels = [f"{calendar.month_abbr[month]} {str(year)[-2:]}" for year, month in ordered_keys]
    income_values = [round(bucket_map[key]["income"], 2) for key in ordered_keys]
    expense_values = [round(bucket_map[key]["expenses"], 2) for key in ordered_keys]
    return labels, income_values, expense_values


def savings_progress_series(accounts, transactions, limit=6):
    account_map = {account.id: account for account in (accounts or [])}
    month_buckets = defaultdict(float)
    for tx in transactions or []:
        account = account_map.get(tx.account_id)
        if not account or account.type != "asset":
            continue
        subtype = infer_account_subtype(account)
        savings_like = subtype in {"savings", "investment"} or normalize_savings_preference(getattr(account, "savings_preference", "auto")) == "include"
        if not savings_like:
            continue
        month_buckets[(tx.date.year, tx.date.month)] += float(tx.amount or 0)

    ordered_keys = sorted(month_buckets.keys())[-limit:]
    labels = [f"{calendar.month_abbr[month]} {str(year)[-2:]}" for year, month in ordered_keys]
    values = [round(month_buckets[key], 2) for key in ordered_keys]
    return labels, values


def goal_allocation_chart_series(goal_rows):
    labels = []
    values = []
    for goal in goal_rows or []:
        allocated = round(float(goal.get("allocated_amount") or 0), 2)
        if allocated <= 0:
            continue
        labels.append(goal.get("name") or "Goal")
        values.append(allocated)
    return labels, values


def compute_previous_net_worth(accounts, transactions, selected_month, selected_year):
    account_month_deltas = defaultdict(float)
    for tx in transactions or []:
        if tx.date.month == selected_month and tx.date.year == selected_year:
            account_month_deltas[tx.account_id] += float(tx.amount or 0)

    previous_asset_total = 0.0
    previous_liability_total = 0.0
    for account in accounts or []:
        prior_balance = float(account.balance or 0) - account_month_deltas.get(account.id, 0.0)
        if account.type == "asset":
            previous_asset_total += prior_balance
        elif account.type == "liability":
            previous_liability_total += prior_balance
    return round(previous_asset_total - previous_liability_total, 2)


def build_metric_change(current_value, previous_value, favorable_direction="up"):
    if previous_value is None:
        return None

    previous_number = float(previous_value or 0)
    current_number = float(current_value or 0)
    if abs(previous_number) < 0.005:
        return None

    delta = current_number - previous_number
    pct_change = (delta / abs(previous_number)) * 100
    if abs(pct_change) < 0.05:
        tone = "neutral"
        icon = "bi-arrow-right"
    else:
        improved = delta > 0 if favorable_direction == "up" else delta < 0
        tone = "positive" if improved else "negative"
        icon = "bi-arrow-up-right" if delta > 0 else "bi-arrow-down-right"

    return {
        "delta": round(delta, 2),
        "percent": round(pct_change, 1),
        "tone": tone,
        "icon": icon,
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
        key = normalize_text(transaction_reference_description(tx))
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


RECURRING_INCOME_KEYWORDS = (
    "direct deposit",
    "payroll",
    "salary",
    "paycheck",
    "ach deposit",
    "deposit",
    "income",
)

INTERNAL_TRANSFER_EXCLUDE_KEYWORDS = (
    "payment thank you",
    "autopay payment",
    "capital one payment",
    "mobile payment",
    "online transfer",
    "zelle",
    "venmo",
    "cash app",
    "paypal",
    "transfer from savings",
    "transfer from checking",
    "transfer to savings",
    "transfer to checking",
)


def recurring_income_frequency(avg_interval_days):
    if avg_interval_days <= 9:
        return "Weekly", 52 / 12
    if avg_interval_days <= 18:
        return "Biweekly", 26 / 12
    if avg_interval_days <= 24:
        return "Semimonthly", 2
    if avg_interval_days <= 36:
        return "Monthly", 1
    if avg_interval_days <= 50:
        return "Every 6 weeks", 52 / 12 / 1.5
    return "Irregular", 0


def is_candidate_recurring_income(tx):
    if float(getattr(tx, "amount", 0) or 0) <= 0:
        return False

    subtype = (getattr(tx, "transaction_subtype", "") or "").strip().lower()
    if subtype and subtype != "income":
        return False

    raw_description = normalize_text(transaction_reference_description(tx))
    category = normalize_text(getattr(tx, "category", ""))
    cleaned = normalize_text(clean_transaction_description(transaction_reference_description(tx)))

    if any(keyword in raw_description for keyword in INTERNAL_TRANSFER_EXCLUDE_KEYWORDS):
        return False
    if any(keyword in cleaned for keyword in INTERNAL_TRANSFER_EXCLUDE_KEYWORDS):
        return False

    if category == "income":
        return True
    if any(keyword in raw_description for keyword in RECURRING_INCOME_KEYWORDS):
        return True
    return False


def analyze_recurring_income(transactions):
    source_groups = defaultdict(list)

    for tx in transactions or []:
        if not is_candidate_recurring_income(tx):
            continue
        source_key = normalize_merchant(transaction_reference_description(tx)) or normalize_text(clean_transaction_description(transaction_reference_description(tx)))
        if not source_key:
            continue
        source_groups[source_key].append(tx)

    recurring_sources = []
    today = date.today()

    for source_key, tx_list in source_groups.items():
        if len(tx_list) < 2:
            continue

        tx_list.sort(key=lambda row: row.date)
        intervals = [(tx_list[i].date - tx_list[i - 1].date).days for i in range(1, len(tx_list))]
        amounts = [float(tx.amount or 0) for tx in tx_list]
        interval_metrics = subscription_interval_metrics(intervals)
        amount_metrics = subscription_amount_metrics(amounts)
        count_score = min(len(tx_list) / 4, 1)
        confidence_score = (
            interval_metrics["interval_score"] * 0.5
            + amount_metrics["amount_score"] * 0.3
            + count_score * 0.2
        )

        if interval_metrics["monthly_hit_ratio"] < 0.45 or confidence_score < 0.58:
            continue

        avg_amount = round(amount_metrics["average_amount"], 2)
        median_interval = interval_metrics["median_interval"] or interval_metrics["avg_interval"] or 30
        frequency_label, monthly_factor = recurring_income_frequency(median_interval)
        if monthly_factor <= 0:
            continue

        last_received = tx_list[-1].date
        next_expected = last_received + timedelta(days=max(1, round(median_interval)))
        source_name = clean_transaction_description(tx_list[-1].display_name or tx_list[-1].description or tx_list[-1].raw_description or source_key).title()
        if not source_name:
            source_name = source_key.title()

        status_label = "Confirmed recurring income" if confidence_score >= 0.76 else "Suspected recurring income"
        recurring_sources.append({
            "source_name": source_name,
            "average_amount": avg_amount,
            "monthly_equivalent": round(avg_amount * monthly_factor, 2),
            "frequency": frequency_label,
            "last_received_date": last_received,
            "latest_received_amount": round(float(tx_list[-1].amount or 0), 2),
            "latest_account_id": tx_list[-1].account_id,
            "next_expected_date": next_expected,
            "confidence_score": round(confidence_score * 100, 1),
            "status_label": status_label,
            "is_confirmed": confidence_score >= 0.76,
            "occurrences": len(tx_list),
        })

    recurring_sources.sort(key=lambda item: (item["is_confirmed"], item["monthly_equivalent"], item["average_amount"]), reverse=True)
    return recurring_sources


def recurring_income_monthly_estimate(recurring_sources):
    confirmed_sources = [item for item in (recurring_sources or []) if item.get("is_confirmed")]
    return round(sum(float(item.get("monthly_equivalent") or 0) for item in confirmed_sources), 2)


def build_income_allocation_alerts(recurring_income_sources, goal_rows, account_allocation_rows, selected_month, selected_year):
    if not recurring_income_sources or not goal_rows:
        return []

    account_summary_map = {row["account_id"]: row for row in (account_allocation_rows or [])}
    alerts = []

    for source in recurring_income_sources:
        last_received_date = source.get("last_received_date")
        account_id = source.get("latest_account_id")
        latest_amount = float(source.get("latest_received_amount") or 0)
        if not last_received_date or last_received_date.month != selected_month or last_received_date.year != selected_year:
            continue
        if latest_amount <= 0 or not account_id:
            continue

        account_row = account_summary_map.get(account_id)
        if not account_row:
            continue

        suggested_pool = min(float(account_row.get("unallocated_amount") or 0), round(latest_amount * 0.3, 2))
        if suggested_pool <= 0:
            continue

        suggestions = suggested_allocations_for_account({"unallocated_amount": suggested_pool}, goal_rows)
        if not suggestions:
            continue

        alerts.append({
            "source_name": source["source_name"],
            "account_id": account_id,
            "account_name": account_row["account_name"],
            "amount_received": round(latest_amount, 2),
            "suggested_pool": round(suggested_pool, 2),
            "status_label": source.get("status_label"),
            "last_received_date": last_received_date,
            "suggestions": suggestions,
        })

    alerts.sort(key=lambda item: item["last_received_date"], reverse=True)
    return alerts[:3]


def suggested_goal_allocation_budget(goal_rows):
    if not goal_rows:
        return {"suggested_goal_set_aside": 0.0, "priority_goals": []}

    open_goals = [goal for goal in goal_rows if float(goal.get("gap_remaining") or 0) > 0]
    if not open_goals:
        return {"suggested_goal_set_aside": 0.0, "priority_goals": []}

    ranked_goals = sorted(open_goals, key=goal_priority_key)
    priority_goals = []
    total_set_aside = 0.0

    for goal in ranked_goals[:3]:
        gap_remaining = float(goal.get("gap_remaining") or 0)
        target_amount = float(goal.get("target_amount") or 0)
        is_emergency = (goal.get("goal_type") or "").lower() == "emergency_fund" or "emergency" in (goal.get("name") or "").lower()
        suggested_amount = min(
            gap_remaining,
            max(target_amount * (0.12 if is_emergency else 0.06), 75 if is_emergency else 40),
        )
        suggested_amount = round(suggested_amount, 2)
        if suggested_amount <= 0:
            continue
        priority_goals.append({
            "goal_name": goal["name"],
            "suggested_amount": suggested_amount,
        })
        total_set_aside += suggested_amount

    return {
        "suggested_goal_set_aside": round(total_set_aside, 2),
        "priority_goals": priority_goals,
    }


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
            f"Safe-to-spend remaining is ${safe_to_spend['safe_to_spend']:,.2f} after recurring obligations, your savings target, and the flexible spending already used this month."
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
            f"Monthly income this period: ${monthly_income:,.2f}.",
            f"Recurring obligations counted: ${safe_to_spend['recurring_expenses']:,.2f}.",
            f"Savings target reserved: ${safe_to_spend['savings_target_amount']:,.2f}.",
            f"Flexible spending already used: ${safe_to_spend['used_amount']:,.2f}."
        ]
        summary = (
            f"Safe-to-spend remaining is about ${safe_to_spend['safe_to_spend']:,.2f} right now."
            if safe_to_spend["safe_to_spend"] >= 0 else
            f"You are about ${abs(safe_to_spend['safe_to_spend']):,.2f} past this month's safer spending buffer right now."
        )
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


@app.route("/review", methods=["GET", "POST"])
def review():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    selected_filter = (request.values.get("filter") or "all").strip().lower()
    if selected_filter not in REVIEW_FILTER_OPTIONS:
        selected_filter = "all"

    if request.method == "POST":
        action = (request.form.get("review_action") or "").strip()
        redirect_filter = (request.form.get("filter") or "all").strip().lower()
        if redirect_filter not in REVIEW_FILTER_OPTIONS:
            redirect_filter = "all"

        if action == "bulk_update":
            selected_ids = []
            for raw_id in request.form.getlist("selected_tx_ids"):
                try:
                    selected_ids.append(int(raw_id))
                except (TypeError, ValueError):
                    continue
            bulk_category = (request.form.get("bulk_category") or "").strip()
            if not selected_ids:
                push_ui_feedback("Select at least one transaction before applying a bulk category change.", "danger")
            elif not bulk_category:
                push_ui_feedback("Choose a category before applying the bulk update.", "danger")
            else:
                transactions = Transaction.query.filter(
                    Transaction.user_id == user_id,
                    Transaction.id.in_(selected_ids),
                ).all()
                updated_count = 0
                for tx in transactions:
                    tx.category = bulk_category
                    remember_merchant_category(
                        user_id,
                        transaction_reference_description(tx),
                        bulk_category,
                        display_name=transaction_display_name(tx),
                    )
                    updated_count += 1
                if updated_count:
                    log_activity(
                        user_id,
                        f"Bulk updated {updated_count} transaction{'s' if updated_count != 1 else ''}",
                        f"Transactions were recategorized as {bulk_category}.",
                        kind="category_updated",
                        icon="bi-tags-fill",
                        target_url="/review",
                    )
                    db.session.commit()
                    push_ui_feedback(
                        f"Updated {updated_count} transaction{'s' if updated_count != 1 else ''} to {bulk_category}.",
                        "success",
                    )
                else:
                    push_ui_feedback("No matching transactions were available for that bulk update.", "danger")
            return redirect(f"/review?filter={redirect_filter}")

        if action.startswith("single_update:"):
            tx_id_raw = action.split(":", 1)[1]
            try:
                tx_id = int(tx_id_raw)
            except (TypeError, ValueError):
                tx_id = None
            if not tx_id:
                push_ui_feedback("Choose a transaction to update.", "danger")
                return redirect(f"/review?filter={redirect_filter}")
            tx = Transaction.query.get(tx_id)
            chosen_category = (request.form.get(f"category_{tx_id}") or "").strip()
            if not tx or tx.user_id != user_id:
                push_ui_feedback("That transaction is no longer available.", "danger")
            elif not chosen_category:
                push_ui_feedback("Choose a category before saving the change.", "danger")
            else:
                tx.category = chosen_category
                remember_merchant_category(
                    user_id,
                    transaction_reference_description(tx),
                    chosen_category,
                    display_name=transaction_display_name(tx),
                )
                log_activity(
                    user_id,
                    f"Updated category for {transaction_display_name(tx)}",
                    f"Saved as {chosen_category}.",
                    kind="category_updated",
                    icon="bi-tags",
                    target_url="/review",
                )
                db.session.commit()
                push_ui_feedback("Category updated.", "success")
            return redirect(f"/review?filter={redirect_filter}")

    txs = Transaction.query.filter_by(user_id=user_id).order_by(Transaction.date.desc(), Transaction.id.desc()).all()
    review_rows = build_review_transaction_rows(user_id, txs)

    if selected_filter == "uncategorized":
        filtered_rows = [row for row in review_rows if row["is_uncategorized"]]
    elif selected_filter == "low-confidence":
        filtered_rows = [row for row in review_rows if row["is_low_confidence"]]
    else:
        filtered_rows = review_rows

    summary = {
        "all_count": len(review_rows),
        "uncategorized_count": sum(1 for row in review_rows if row["is_uncategorized"]),
        "low_confidence_count": sum(1 for row in review_rows if row["is_low_confidence"]),
        "filtered_count": len(filtered_rows),
    }

    return render_template(
        "review.html",
        review_rows=filtered_rows,
        review_summary=summary,
        selected_filter=selected_filter,
        filter_options=REVIEW_FILTER_OPTIONS,
        category_choices=import_category_choices(user_id),
    )


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
            session.clear()
            session.permanent = True
            session["user_id"] = user.id
            session["login_at"] = datetime.utcnow().isoformat()
            session.modified = True
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
    generated_reset_path = None
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
        generated_reset_path = url_for("reset_password", token=token)
        generated_reset_link = url_for("reset_password", token=token, _external=True)

    return render_template(
        "reset_password.html",
        token=token,
        reset_error=reset_error,
        reset_success=reset_success,
        token_valid=bool(user),
        request_error=request_error,
        request_success=request_success,
        generated_reset_path=generated_reset_path,
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
    reset_path = None
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
                reset_path = url_for("reset_password", token=target_user.reset_token)
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
        reset_path=reset_path,
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
    goals = FinancialGoal.query.filter_by(user_id=user_id).all()

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

    recurring_income_sources = analyze_recurring_income(transactions)
    recurring_income_estimate = recurring_income_monthly_estimate(recurring_income_sources)
    effective_monthly_income = max(monthly_income, recurring_income_estimate)
    subscriptions = analyze_subscriptions(transactions)
    category_totals = defaultdict(float)
    for tx in transactions:
        if tx.date.month == current_month and tx.date.year == current_year and is_spending_transaction(tx):
            category_totals[tx.category] += abs(float(tx.amount or 0))
    budget_rows = []
    savings_snapshot = calculate_savings_snapshot(
        accounts,
        transactions,
        current_month,
        current_year,
        effective_monthly_income,
        monthly_expenses,
    )
    wealth_snapshot = build_wealth_snapshot(
        accounts,
        transactions,
        goals,
        current_month,
        current_year,
        effective_monthly_income,
        monthly_expenses,
        category_totals,
        savings_snapshot,
        [],
    )
    goal_budget = suggested_goal_allocation_budget(wealth_snapshot.get("goal_rows", []))
    recurring_obligations = sum(float(sub.get("average_amount") or 0) for sub in subscriptions)
    safe_to_spend = calculate_safe_to_spend(
        accounts,
        subscriptions,
        budget_rows,
        effective_monthly_income,
        monthly_expenses,
        recurring_obligations,
        savings_snapshot.get("recommended_amount"),
        current_month,
        current_year,
        actual_monthly_income=monthly_income,
        goal_set_aside_amount=goal_budget.get("suggested_goal_set_aside"),
    )
    plan_labels, plan_income_values, plan_expense_values = monthly_overview_series(transactions, limit=6)
    plan_net_worth_labels, plan_net_worth_values = compute_net_worth_history(accounts, transactions)
    savings_labels, savings_values = savings_progress_series(accounts, transactions, limit=6)
    goal_allocation_labels, goal_allocation_values = goal_allocation_chart_series(wealth_snapshot.get("goal_rows", []))

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
        monthly_expenses=round(monthly_expenses, 2),
        recurring_income_sources=recurring_income_sources[:4],
        recurring_income_estimate=round(recurring_income_estimate, 2),
        effective_monthly_income=round(effective_monthly_income, 2),
        recurring_obligations=round(recurring_obligations, 2),
        safe_to_spend=safe_to_spend,
        savings_snapshot=savings_snapshot,
        goal_budget=goal_budget,
        wealth_goal_rows=wealth_snapshot.get("goal_rows", []),
        plan_labels=plan_labels,
        plan_income_values=plan_income_values,
        plan_expense_values=plan_expense_values,
        plan_net_worth_labels=plan_net_worth_labels,
        plan_net_worth_values=plan_net_worth_values,
        savings_labels=savings_labels,
        savings_values=savings_values,
        goal_allocation_labels=goal_allocation_labels,
        goal_allocation_values=goal_allocation_values,
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
    response = redirect("/login")
    response.delete_cookie(
        app.config.get("SESSION_COOKIE_NAME", "session"),
        path=app.config.get("SESSION_COOKIE_PATH", "/"),
        secure=app.config.get("SESSION_COOKIE_SECURE", False),
        httponly=app.config.get("SESSION_COOKIE_HTTPONLY", True),
        samesite=app.config.get("SESSION_COOKIE_SAMESITE", "Lax"),
    )
    return response


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
    total_assets = round(sum(float(account.balance or 0) for account in accounts if account.type == "asset"), 2)
    total_liabilities = round(sum(float(account.balance or 0) for account in accounts if account.type == "liability"), 2)
    liability_only_nudge = ""
    if total_liabilities > 0 and total_assets <= 0:
        liability_only_nudge = "Add a checking or savings account to see your full net worth."
    return render_template(
        "accounts.html",
        accounts=accounts,
        has_accounts=bool(accounts),
        savings_profile_map=savings_profile_map,
        savings_summary=savings_summary,
        total_assets=total_assets,
        total_liabilities=total_liabilities,
        net_worth=round(total_assets - total_liabilities, 2),
        liability_only_nudge=liability_only_nudge,
        account_kind_choices=ACCOUNT_KIND_CHOICES,
        account_kind_for=resolve_account_kind,
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
    account_kind = request.form.get("account_kind", "").strip()
    if account_kind:
        type_, subtype = map_account_kind(account_kind)
    else:
        type_ = request.form["type"].strip()
        subtype = normalize_account_subtype(request.form.get("subtype", ""), type_)
    balance = safe_float(request.form["balance"])
    savings_preference = normalize_savings_preference(request.form.get("savings_preference", "auto"))

    if not name or type_ not in ("asset", "liability") or balance is None:
        push_ui_feedback("Enter an account name, choose asset or liability, and provide a valid balance.", "danger")
        return redirect("/accounts")

    if type_ == "liability":
        savings_preference = "exclude"
    elif account_kind == "savings" and savings_preference == "auto":
        savings_preference = "include"

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


@app.route("/accounts/<int:account_id>")
def account_detail(account_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    account = Account.query.get(account_id)
    if not account or account.user_id != user_id:
        return "Account not found"

    transactions = (
        Transaction.query
        .filter_by(user_id=user_id, account_id=account_id)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .limit(100)
        .all()
    )
    goal_allocations = account_goal_allocation_summary(user_id, account)
    return render_template(
        "account_detail.html",
        account=account,
        transactions=transactions,
        goal_allocations=goal_allocations,
        subtype_label=subtype_label,
        transaction_count=len(transactions),
    )


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
    recurring_income_sources = analyze_recurring_income(transactions)
    recurring_income_estimate = recurring_income_monthly_estimate(recurring_income_sources)
    effective_monthly_income = max(float(monthly_income or 0), float(recurring_income_estimate or 0))
    nw_labels, nw_values = compute_net_worth_history(accounts, transactions)
    savings_snapshot = calculate_savings_snapshot(
        accounts=accounts,
        transactions=transactions,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=effective_monthly_income,
        monthly_expenses=monthly_expenses,
    )
    wealth_snapshot = build_wealth_snapshot(
        accounts=accounts,
        transactions=transactions,
        goals=goals,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=effective_monthly_income,
        monthly_expenses=monthly_expenses,
        category_totals=category_totals,
        savings_snapshot=savings_snapshot,
        nw_values=nw_values,
    )
    goal_allocation_budget = suggested_goal_allocation_budget(wealth_snapshot["goal_rows"])
    account_allocation_summary = goals_account_allocation_summary(user_id, accounts, wealth_snapshot["goal_rows"])
    transaction_years = sorted({tx.date.year for tx in transactions} | {selected_year, datetime.now().year}, reverse=True)
    month_labels = {month: calendar.month_name[month] for month in range(1, 13)}
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
        account_allocation_summary=account_allocation_summary,
        has_goals=bool(goals),
        goal_linkable_accounts=linked_goalable_accounts(accounts),
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
    linked_account_id = safe_int(request.form.get("linked_account_id"))
    allocated_amount = safe_float(request.form.get("allocated_amount"))
    target_date = parse_date_any(request.form.get("target_date"))

    valid_goal_types = {value for value, _ in GOAL_TYPE_CHOICES}
    valid_linked_metrics = {value for value, _ in GOAL_LINK_CHOICES}
    if not name or target_amount is None or target_amount <= 0:
        push_ui_feedback("Add a goal name and a target amount greater than zero.", "danger")
        return redirect("/goals-wealth")

    allocation_value = allocated_amount
    if linked_account_id:
        if allocation_value is None:
            allocation_value = current_amount if current_amount is not None else 0
        if allocation_value is None or allocation_value < 0:
            push_ui_feedback("Add an allocated amount of zero or more for the linked account.", "danger")
            return redirect("/goals-wealth")
        linked_account, allocation_error = validate_account_allocation(user_id, linked_account_id, allocation_value)
        if allocation_error:
            push_ui_feedback(allocation_error, "danger")
            return redirect("/goals-wealth")
    else:
        linked_account = None
        allocation_value = 0

    new_goal = FinancialGoal(
        user_id=user_id,
        name=name,
        goal_type=goal_type if goal_type in valid_goal_types else "custom",
        target_amount=target_amount,
        current_amount=(allocation_value if linked_account else (current_amount or 0)),
        target_date=target_date,
        linked_metric=linked_metric if linked_metric in valid_linked_metrics else "manual",
        linked_account_id=linked_account.id if linked_account else None,
        allocated_amount=allocation_value or 0,
    )
    db.session.add(new_goal)
    db.session.flush()
    if linked_account and (allocation_value or 0) > 0:
        db.session.add(GoalAllocation(
            goal_id=new_goal.id,
            account_id=linked_account.id,
            allocated_amount=allocation_value or 0,
        ))
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
    linked_account_id = safe_int(request.form.get("linked_account_id"))
    allocated_amount = safe_float(request.form.get("allocated_amount"))

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
    if linked_account_id:
        allocation_value = allocated_amount if allocated_amount is not None else current_amount if current_amount is not None else float(goal.allocated_amount or 0)
        existing_allocation = GoalAllocation.query.filter_by(goal_id=goal.id, account_id=linked_account_id).first()
        linked_account, allocation_error = validate_account_allocation(
            user_id,
            linked_account_id,
            allocation_value,
            exclude_allocation_id=existing_allocation.id if existing_allocation else None,
        )
        if allocation_error:
            push_ui_feedback(allocation_error, "danger")
            return redirect("/goals-wealth")
        goal.linked_account_id = linked_account.id if linked_account else None
        goal.allocated_amount = allocation_value or 0
        goal.current_amount = allocation_value or 0
        if existing_allocation:
            existing_allocation.allocated_amount = allocation_value or 0
        elif linked_account and (allocation_value or 0) > 0:
            db.session.add(GoalAllocation(goal_id=goal.id, account_id=linked_account.id, allocated_amount=allocation_value or 0))
    else:
        goal.linked_account_id = None
        goal.allocated_amount = 0
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


@app.route("/goals-wealth/allocate", methods=["POST"])
def allocate_goal_from_account():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    goal_id = safe_int(request.form.get("goal_id"))
    account_id = safe_int(request.form.get("account_id"))
    allocated_amount = safe_float(request.form.get("allocated_amount"))

    goal = FinancialGoal.query.get(goal_id) if goal_id else None
    if not goal or goal.user_id != user_id:
        push_ui_feedback("Choose a valid goal to allocate funds.", "danger")
        return redirect("/goals-wealth#allocations")

    if allocated_amount is None or allocated_amount < 0:
        push_ui_feedback("Enter an allocation amount of zero or more.", "danger")
        return redirect("/goals-wealth#allocations")

    existing_allocation = GoalAllocation.query.filter_by(goal_id=goal.id, account_id=account_id).first() if account_id else None
    linked_account, allocation_error = validate_account_allocation(
        user_id,
        account_id,
        allocated_amount,
        exclude_allocation_id=existing_allocation.id if existing_allocation else None,
    )
    if allocation_error:
        push_ui_feedback(allocation_error, "danger")
        return redirect("/goals-wealth#allocations")

    if existing_allocation and allocated_amount <= 0:
        db.session.delete(existing_allocation)
        action_text = "removed"
    elif existing_allocation:
        existing_allocation.allocated_amount = allocated_amount
        action_text = "updated"
    elif linked_account and allocated_amount > 0:
        db.session.add(GoalAllocation(goal_id=goal.id, account_id=linked_account.id, allocated_amount=allocated_amount))
        action_text = "added"
    else:
        push_ui_feedback("Choose an account and allocation amount to continue.", "danger")
        return redirect("/goals-wealth#allocations")

    log_activity(
        user_id,
        f"Allocation {action_text} for {goal.name}",
        f"{linked_account.name if linked_account else 'Account'} allocation is now ${allocated_amount:,.2f}.",
        kind="goal_updated",
        icon="bi-diagram-3",
        target_url="/goals-wealth",
    )
    db.session.commit()
    push_ui_feedback(f"Allocation {action_text} for {goal.name}.", "success")
    return redirect("/goals-wealth#allocations")


@app.route("/goals-wealth/auto-allocate/<int:account_id>", methods=["POST"])
def auto_allocate_account(account_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    account = Account.query.get(account_id)
    if not account or account.user_id != user_id or account.type != "asset":
        push_ui_feedback("Choose a valid asset account to auto-allocate.", "danger")
        return redirect("/goals-wealth#allocations")

    accounts = Account.query.filter_by(user_id=user_id).all()
    goals = FinancialGoal.query.filter_by(user_id=user_id).all()
    wealth_context = {
        "savings_snapshot": {"current_savings": 0},
        "net_worth_breakdown": build_net_worth_breakdown(accounts),
        "accounts_by_id": {acct.id: acct for acct in accounts},
        "goal_allocation_map": goal_allocations_for_goals(goals),
    }
    goal_rows, _ = build_goal_progress(goals, wealth_context)
    applied = auto_allocate_account_to_goals(user_id, account, goal_rows)

    if not applied:
        push_ui_feedback(f"No suggested allocations were available for {account.name}.", "warning")
        return redirect("/goals-wealth#allocations")

    store_allocation_undo(
        f"Auto-allocate from {account.name}",
        [
            {
                "goal_id": item["goal_id"],
                "account_id": item["account_id"],
                "previous_amount": item["previous_amount"],
                "new_amount": item["new_amount"],
            }
            for item in applied
        ],
        "/goals-wealth#allocations",
    )

    log_activity(
        user_id,
        f"Auto-allocated {account.name}",
        f"Applied {len(applied)} suggested goal allocation{'s' if len(applied) != 1 else ''}.",
        kind="goal_updated",
        icon="bi-magic",
        target_url="/goals-wealth",
    )
    db.session.commit()
    push_ui_feedback(
        f"Auto-allocated ${sum(item['suggested_amount'] for item in applied):,.2f} from {account.name}.",
        "success",
        action_label="Undo",
        action_url="/allocations/undo",
        action_method="POST",
    )
    return redirect("/goals-wealth#allocations")


@app.route("/goals-wealth/goal-action/<int:goal_id>", methods=["POST"])
def goal_quick_action(goal_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    goal = FinancialGoal.query.get(goal_id)
    if not goal or goal.user_id != user_id:
        push_ui_feedback("Choose a valid goal first.", "danger")
        return redirect("/goals-wealth")

    action = (request.form.get("action") or "").strip().lower()
    if action not in {"fully_fund", "add_remaining"}:
        push_ui_feedback("Choose a valid goal action.", "danger")
        return redirect("/goals-wealth")

    result = quick_allocate_goal(user_id, goal, "full" if action == "fully_fund" else "remaining")
    added_total = float(result.get("added_total") or 0)
    if added_total <= 0:
        push_ui_feedback(f"No unallocated funds were available to update {goal.name}.", "warning")
        return redirect("/goals-wealth")

    store_allocation_undo(
        f"Goal quick action for {goal.name}",
        result.get("changes"),
        "/goals-wealth",
    )

    action_label = "Fully funded" if action == "fully_fund" else "Added remaining amount to"
    log_activity(
        user_id,
        f"{action_label} {goal.name}",
        f"${added_total:,.2f} was allocated automatically.",
        kind="goal_updated",
        icon="bi-bullseye",
        target_url="/goals-wealth",
    )
    db.session.commit()
    push_ui_feedback(
        f"${added_total:,.2f} was allocated to {goal.name}.",
        "success",
        action_label="Undo",
        action_url="/allocations/undo",
        action_method="POST",
    )
    return redirect("/goals-wealth")


@app.route("/income-allocation/apply", methods=["POST"])
def apply_income_allocation_suggestion():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    account_id = safe_int(request.form.get("account_id"))
    source_name = (request.form.get("source_name") or "Income source").strip()
    account = Account.query.get(account_id) if account_id else None
    if not account or account.user_id != user_id or account.type != "asset":
        push_ui_feedback("Choose a valid account before applying income suggestions.", "danger")
        return redirect("/")

    goal_ids = request.form.getlist("goal_id")
    amounts = request.form.getlist("suggested_amount")
    if not goal_ids or not amounts or len(goal_ids) != len(amounts):
        push_ui_feedback("No income allocation suggestions were submitted.", "danger")
        return redirect("/")

    proposed_rows = []
    total_new_amount = 0.0
    for raw_goal_id, raw_amount in zip(goal_ids, amounts):
        goal_id = safe_int(raw_goal_id)
        amount = safe_float(raw_amount)
        if not goal_id or amount is None or amount <= 0:
            continue
        goal = FinancialGoal.query.get(goal_id)
        if not goal or goal.user_id != user_id:
            continue
        existing_allocation = GoalAllocation.query.filter_by(goal_id=goal.id, account_id=account.id).first()
        existing_amount = float(existing_allocation.allocated_amount or 0) if existing_allocation else 0
        proposed_rows.append((goal, existing_amount + amount))
        total_new_amount += amount

    if not proposed_rows:
        push_ui_feedback("Add at least one positive allocation amount to apply the suggestion.", "danger")
        return redirect("/")

    existing_other_total = sum(
        float(row.allocated_amount or 0)
        for row in GoalAllocation.query.filter_by(account_id=account.id).all()
        if row.goal_id not in {goal.id for goal, _ in proposed_rows}
    )
    total_after_apply = existing_other_total + sum(amount for _, amount in proposed_rows)
    if total_after_apply > float(account.balance or 0) + 0.005:
        available = max(float(account.balance or 0) - existing_other_total, 0)
        push_ui_feedback(f"Those edited allocations exceed {account.name}'s available balance. Available to allocate: ${available:,.2f}.", "danger")
        return redirect("/")

    updated_count = 0
    undo_changes = []
    for goal, new_total in proposed_rows:
        previous_allocation = GoalAllocation.query.filter_by(goal_id=goal.id, account_id=account.id).first()
        previous_amount = float(previous_allocation.allocated_amount or 0) if previous_allocation else 0
        upsert_goal_allocation(goal.id, account.id, new_total)
        updated_count += 1
        undo_changes.append({
            "goal_id": goal.id,
            "account_id": account.id,
            "previous_amount": round(previous_amount, 2),
            "new_amount": round(new_total, 2),
        })

    store_allocation_undo(
        f"Income suggestion from {source_name}",
        undo_changes,
        "/",
    )

    log_activity(
        user_id,
        f"Applied income suggestion from {source_name}",
        f"${total_new_amount:,.2f} was allocated across {updated_count} goal{'s' if updated_count != 1 else ''}.",
        kind="goal_updated",
        icon="bi-cash-coin",
        target_url="/",
    )
    db.session.commit()
    push_ui_feedback(
        f"Applied ${total_new_amount:,.2f} from {source_name} to your goals.",
        "success",
        action_label="Undo",
        action_url="/allocations/undo",
        action_method="POST",
    )
    return redirect("/")


@app.route("/allocations/undo", methods=["POST"])
def undo_allocation_changes():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    payload = session.get("_allocation_undo")
    if not payload or not payload.get("changes"):
        push_ui_feedback("There is no recent allocation action to undo.", "info")
        return redirect("/goals-wealth#allocations")

    valid_goal_ids = {goal.id for goal in FinancialGoal.query.filter_by(user_id=user_id).all()}
    valid_account_ids = {account.id for account in Account.query.filter_by(user_id=user_id).all()}
    restored = 0
    for change in payload["changes"]:
        goal_id = safe_int(change.get("goal_id"))
        account_id = safe_int(change.get("account_id"))
        previous_amount = float(change.get("previous_amount") or 0)
        if goal_id not in valid_goal_ids or account_id not in valid_account_ids:
            continue
        upsert_goal_allocation(goal_id, account_id, previous_amount)
        restored += 1

    redirect_url = payload.get("redirect_url") or "/goals-wealth#allocations"
    clear_allocation_undo()
    log_activity(
        user_id,
        "Reverted allocation change",
        f"Restored {restored} allocation row{'s' if restored != 1 else ''} to the previous amounts.",
        kind="goal_updated",
        icon="bi-arrow-counterclockwise",
        target_url=redirect_url,
    )
    db.session.commit()
    push_ui_feedback("Reverted the last allocation action.", "success")
    return redirect(redirect_url)


@app.route("/goals-wealth/delete-goal/<int:goal_id>", methods=["POST"])
def delete_financial_goal(goal_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    goal = FinancialGoal.query.get(goal_id)
    if goal and goal.user_id == user_id:
        goal_name = goal.name
        GoalAllocation.query.filter_by(goal_id=goal.id).delete()
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
        category_choices=transaction_ui_category_choices(user_id),
        subtype_choices=[("income", "Income"), ("expense", "Expense"), ("transfer", "Transfer"), ("payment", "Payment")],
    )


@app.route("/merchant-memory/add", methods=["POST"])
def add_merchant_memory():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    merchant = request.form.get("merchant", "").strip()
    category = canonical_transaction_category(request.form.get("category", "").strip())
    display_name = clean_transaction_description(request.form.get("display_name", "").strip() or merchant)
    subtype = (request.form.get("subtype", "") or "").strip().lower()
    is_disabled = (request.form.get("is_disabled") or "").strip() == "1"
    if merchant and category:
        remember_merchant_category(user_id, merchant, category, display_name=display_name, subtype=subtype)
        memory = MerchantMemory.query.filter_by(user_id=user_id, merchant=normalize_text(merchant)).first()
        if memory:
            memory.is_disabled = is_disabled
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
    category = canonical_transaction_category(request.form.get("category", "").strip())
    display_name = clean_transaction_description(request.form.get("display_name", "").strip() or merchant)
    subtype = (request.form.get("subtype", "") or "").strip().lower()
    is_disabled = (request.form.get("is_disabled") or "").strip() == "1"
    normalized = normalize_text(merchant)
    if normalized and category and category.lower() not in GENERIC_CATEGORIES:
        memory.merchant = normalized
        memory.category = category
        memory.display_name = display_name
        memory.subtype = subtype if subtype in VALID_TRANSACTION_SUBTYPES else ""
        memory.is_disabled = is_disabled
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
    last_import_batch = latest_import_batch_for_user(user_id)
    import_error = None
    import_success = None
    import_summary = None
    category_choices = transaction_ui_category_choices(user_id)
    selected_account_id = preview["account_id"] if preview else get_last_import_account_id(accounts)
    import_new_account_open = False
    pending_import_account = {
        "name": "",
        "account_kind": "checking",
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
                set_last_import_account(account_id)
                queued_job = queue_import_job(user_id, account_id, files)
                push_ui_feedback(
                    f"Import queued for background processing. AkuOS is preparing your transaction review for {len(files)} file{'s' if len(files) != 1 else ''}.",
                    "info",
                )
                return redirect(url_for("imports"))

        elif form_name == "create_import_account":
            import_new_account_open = True
            name = request.form.get("name", "").strip()
            account_kind = request.form.get("account_kind", "").strip() or "checking"
            type_, subtype = map_account_kind(account_kind)
            balance = safe_float(request.form.get("balance"))
            savings_preference = normalize_savings_preference(request.form.get("savings_preference", "auto"))
            pending_import_account = {
                "name": name,
                "account_kind": account_kind,
                "balance": request.form.get("balance", "0"),
                "subtype": subtype,
                "savings_preference": savings_preference,
            }

            if not name:
                import_error = "Enter an account name."
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
                set_last_import_account(new_account.id)
                import_new_account_open = False
                pending_import_account = {
                    "name": "",
                    "account_kind": "checking",
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
                import_job_id = (preview.get("import_job_id") or "").strip()
                acct = Account.query.get(account_id)
                if not acct or acct.user_id != user_id:
                    import_error = "Selected account is no longer available."
                else:
                    set_last_import_account(account_id)
                    imported_count = 0
                    duplicate_count = 0
                    skipped_count = 0
                    not_transaction_count = 0
                    needs_review_count = 0
                    corrected_count = 0
                    auto_detected_count = 0
                    merchant_memory_updated_count = 0
                    pending_manual_count = 0
                    existing_fingerprints = existing_transaction_fingerprints(user_id, account_id)
                    commit_fingerprints = set()
                    prepared_transactions = []
                    for row in preview["rows"]:
                        row_fingerprint = row.get("fingerprint") or transaction_fingerprint(row["date"], row["description"], row["amount"])
                        if row.get("is_duplicate") or row_fingerprint in existing_fingerprints or row_fingerprint in commit_fingerprints:
                            duplicate_count += 1
                            continue

                        row_action = (request.form.get(f"row_action_{row['row_id']}") or row.get("default_row_action") or "import").strip().lower()
                        if row_action == "skip":
                            skipped_count += 1
                            continue
                        if row_action == "not_transaction":
                            skipped_count += 1
                            not_transaction_count += 1
                            continue

                        chosen_date_raw = request.form.get(f"date_{row['row_id']}", row.get("date", "")).strip()
                        chosen_display_name_input = request.form.get(f"display_name_{row['row_id']}", row.get("display_name") or row.get("description", "")).strip()
                        chosen_display_name = clean_transaction_description(chosen_display_name_input)
                        raw_description_value = (row.get("raw_description") or row.get("description") or "").strip()
                        chosen_amount_raw = request.form.get(f"amount_{row['row_id']}", str(row.get("amount", ""))).strip()
                        chosen_category = canonical_transaction_category(request.form.get(f"category_{row['row_id']}", "").strip() or row["category"])
                        original_category = canonical_transaction_category((row.get("category") or "").strip())
                        if not chosen_category or chosen_category.lower() in GENERIC_CATEGORIES:
                            chosen_category = "Needs Review"
                        parsed_date = parse_date_any(chosen_date_raw)
                        amount = safe_float(chosen_amount_raw)
                        if parsed_date is None or not chosen_display_name or amount is None:
                            pending_manual_count += 1
                            continue

                        final_fingerprint = transaction_fingerprint(parsed_date, raw_description_value or chosen_display_name, amount)
                        category_source = row.get("category_source") or "Manual Review"
                        category_confidence = normalize_confidence_bucket(row.get("confidence_bucket") or row.get("confidence_label"))
                        if chosen_category != original_category and chosen_category.lower() not in GENERIC_CATEGORIES:
                            category_source = "Manual Review"
                            category_confidence = "high"
                        final_subtype = transaction_subtype_for(
                            amount,
                            chosen_category,
                            category_source,
                            row.get("row_kind"),
                        )

                        prepared_transactions.append({
                            "date": parsed_date,
                            "description": chosen_display_name,
                            "display_name": chosen_display_name,
                            "raw_description": raw_description_value,
                            "amount": amount,
                            "category": chosen_category,
                            "category_source": category_source,
                            "category_confidence": category_confidence,
                            "transaction_subtype": final_subtype,
                        })
                        commit_fingerprints.add(final_fingerprint)
                        existing_fingerprints.add(final_fingerprint)
                        if (
                            original_category and chosen_category != original_category
                        ) or (
                            row.get("date", "") != chosen_date_raw
                        ) or (
                            (row.get("display_name") or row.get("description") or "").strip() != chosen_display_name
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
                        starting_balance = round(float(acct.balance or 0), 2)
                        net_change = round(sum(row["amount"] for row in prepared_transactions), 2)
                        import_batch_id = uuid.uuid4().hex[:32] if prepared_transactions else None
                        for prepared_row in prepared_transactions:
                            tx = Transaction(
                                user_id=user_id,
                                account_id=account_id,
                                date=prepared_row["date"],
                                description=prepared_row["display_name"],
                                raw_description=prepared_row["raw_description"] or prepared_row["display_name"],
                                display_name=prepared_row["display_name"],
                                amount=prepared_row["amount"],
                                category=prepared_row["category"],
                                category_source=prepared_row["category_source"],
                                category_confidence=prepared_row["category_confidence"] or "high",
                                transaction_subtype=prepared_row["transaction_subtype"],
                                tags="",
                                import_batch_id=import_batch_id,
                            )
                            db.session.add(tx)
                            remember_merchant_category(
                                user_id,
                                prepared_row["raw_description"] or prepared_row["display_name"],
                                prepared_row["category"],
                                display_name=prepared_row["display_name"],
                                subtype=prepared_row["transaction_subtype"],
                            )
                            merchant_memory_updated_count += 1
                        if not prepared_transactions:
                            ending_balance = starting_balance
                        else:
                            acct.balance = round(starting_balance + net_change, 2)
                            ending_balance = round(float(acct.balance or 0), 2)
                        if import_batch_id:
                            db.session.add(ImportBatch(
                                id=import_batch_id,
                                user_id=user_id,
                                account_id=account_id,
                                imported_count=imported_count,
                                net_change=net_change,
                                starting_balance=starting_balance,
                                ending_balance=ending_balance,
                                auto_detected_count=auto_detected_count,
                                corrected_count=corrected_count,
                                duplicate_count=duplicate_count,
                                skipped_count=skipped_count,
                                not_transaction_count=not_transaction_count,
                                needs_review_count=needs_review_count,
                            ))
                        if import_job_id:
                            import_job = ImportJob.query.get(import_job_id)
                            if import_job and import_job.user_id == user_id:
                                import_job.status = "imported"
                                import_job.current_stage = "complete"
                                import_job.progress_percent = 100
                                import_job.completed_at = datetime.utcnow()
                                import_job.summary_json = json.dumps({
                                    "transaction_count": imported_count,
                                    "imported_count": imported_count,
                                    "auto_approved_count": preview.get("summary", {}).get("auto_approved_count", 0),
                                    "needs_review_count": needs_review_count,
                                    "ignored_row_count": skipped_count,
                                    "duplicate_count": duplicate_count,
                                    "skipped_count": skipped_count,
                                    "not_transaction_count": not_transaction_count,
                                    "merchant_memory_updated_count": merchant_memory_updated_count,
                                    "net_impact": net_change,
                                })
                        log_activity(
                            user_id,
                            f"Imported {imported_count} transaction{'s' if imported_count != 1 else ''}",
                            f"{auto_detected_count} categories prefilled, {corrected_count} corrections, {duplicate_count} duplicates skipped, {skipped_count} manually skipped, net change {'+' if net_change >= 0 else '-'}${abs(net_change):,.2f}.",
                            kind="import_completed",
                            icon="bi-database-check",
                            target_url="/imports",
                        )
                        db.session.commit()
                        last_import_batch = latest_import_batch_for_user(user_id)
                        clear_import_preview()
                        preview = None
                        if imported_count:
                            import_success = (
                                f"Imported {imported_count} transaction{'s' if imported_count != 1 else ''}. "
                                f"Net change {'+' if net_change >= 0 else '-'}${abs(net_change):,.2f}. "
                                f"Account balance is now ${ending_balance:,.2f}."
                            )
                        else:
                            import_success = "No new transactions were imported. Every previewed row was already in the account or duplicated within this import."
                        if duplicate_count:
                            import_success += f" Skipped {duplicate_count} duplicate row{'s' if duplicate_count != 1 else ''}."
                        if skipped_count:
                            import_success += f" {skipped_count} row{'s' if skipped_count != 1 else ''} were marked to skip."
                        if not_transaction_count:
                            import_success += f" {not_transaction_count} row{'s' if not_transaction_count != 1 else ''} were marked as not real transactions."
                        if needs_review_count:
                            import_success += f" {needs_review_count} row{'s' if needs_review_count != 1 else ''} still need review."
                        if imported_count:
                            import_success += " Merchant memory was updated for confirmed categories."
                        import_summary = {
                            "imported_count": imported_count,
                            "auto_approved_count": preview.get("summary", {}).get("auto_approved_count", 0),
                            "auto_detected_count": auto_detected_count,
                            "corrected_count": corrected_count,
                            "duplicate_count": duplicate_count,
                            "skipped_count": skipped_count,
                            "not_transaction_count": not_transaction_count,
                            "needs_review_count": needs_review_count,
                            "merchant_memory_updated_count": merchant_memory_updated_count,
                            "net_change": net_change,
                            "starting_balance": starting_balance,
                            "ending_balance": ending_balance,
                            "import_batch_id": import_batch_id,
                        }

        elif form_name == "clear_preview":
            clear_import_preview()
            preview = None

    import_jobs = recent_import_jobs_for_user(user_id, limit=8)
    grouped_import_jobs = group_import_jobs(import_jobs)
    latest_active_job = next((job for job in import_jobs if job["status"] in {"queued", "processing"}), None)
    latest_ready_job = next((job for job in import_jobs if job["is_ready_for_review"]), None)
    if not preview and latest_ready_job:
        last_seen_completed_job = session.get("last_seen_completed_import_job_id")
        if last_seen_completed_job != latest_ready_job["id"]:
            ready_summary = latest_ready_job.get("summary", {})
            import_success = (
                f"Import review is ready. "
                f"{ready_summary.get('transaction_count', 0)} transactions found, "
                f"{ready_summary.get('ignored_row_count', 0)} ignored, "
                f"and {ready_summary.get('needs_review_count', 0)} need attention."
            )
            session["last_seen_completed_import_job_id"] = latest_ready_job["id"]

    if preview and not selected_account_id:
        selected_account_id = preview["account_id"]
    selected_account_name = next((account.name for account in accounts if account.id == selected_account_id), None)
    return render_template(
        "imports.html",
        accounts=accounts,
        preview=preview,
        import_error=import_error,
        import_success=import_success,
        import_summary=import_summary,
        category_choices=category_choices,
        selected_account_id=selected_account_id,
        selected_account_name=selected_account_name,
        import_new_account_open=import_new_account_open,
        pending_import_account=pending_import_account,
        has_import_history=transaction_count > 0 or bool(import_jobs) or bool(last_import_batch),
        last_import_batch=last_import_batch,
        import_jobs=grouped_import_jobs,
        latest_active_job=latest_active_job,
        latest_ready_job=latest_ready_job,
        import_account_kind_choices=[
            ("checking", "Checking"),
            ("savings", "Savings"),
            ("investment", "Investment"),
            ("cash", "Cash"),
            ("credit_card", "Credit Card"),
            ("loan", "Loan"),
            ("other", "Other"),
        ],
        asset_subtype_choices=[(value, ACCOUNT_SUBTYPE_LABELS[value]) for value in ["", "checking", "cash", "savings", "investment", "other_asset"]],
        liability_subtype_choices=[(value, ACCOUNT_SUBTYPE_LABELS[value]) for value in ["", "credit_card", "loan", "other_liability"]],
    )


@app.route("/imports/undo-last", methods=["POST"])
def undo_last_import():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    latest_batch = ImportBatch.query.filter_by(user_id=user_id).order_by(ImportBatch.created_at.desc(), ImportBatch.id.desc()).first()
    if not latest_batch:
        push_ui_feedback("There is no recent import batch to undo.", "danger")
        return redirect("/imports")

    account = Account.query.get(latest_batch.account_id)
    if account and account.user_id == user_id:
        account.balance = round(float(latest_batch.starting_balance or 0), 2)

    removed_count = Transaction.query.filter_by(user_id=user_id, import_batch_id=latest_batch.id).delete()
    db.session.delete(latest_batch)
    log_activity(
        user_id,
        "Undid last import batch",
        f"Removed {removed_count} imported transaction{'s' if removed_count != 1 else ''} and restored the linked account balance.",
        kind="import_undone",
        icon="bi-arrow-counterclockwise",
        target_url="/imports",
    )
    db.session.commit()
    push_ui_feedback(
        f"Removed the last import batch and restored the account balance for {account.name if account else 'the linked account'}.",
        "success",
    )
    return redirect("/imports")


@app.route("/imports/jobs/<job_id>/review")
def review_import_job(job_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    job = ImportJob.query.get(job_id)
    if not job or job.user_id != user_id:
        push_ui_feedback("That import job is no longer available.", "danger")
        return redirect("/imports")

    if (job.status or "").lower() != "completed" or not job.preview_id:
        push_ui_feedback("That import job is still processing or did not finish successfully yet.", "info")
        return redirect("/imports")

    if not activate_import_preview(job.preview_id):
        push_ui_feedback("AkuOS could not reopen that import review. Please upload the files again.", "danger")
        return redirect("/imports")

    session["last_seen_completed_import_job_id"] = job.id
    return redirect("/imports")


@app.route("/imports/jobs/clear", methods=["POST"])
def clear_import_jobs():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    raw_job_ids = request.form.getlist("job_ids")
    job_ids = [job_id.strip() for job_id in raw_job_ids if (job_id or "").strip()]
    if not job_ids:
        push_ui_feedback("No import jobs were selected to clear.", "danger")
        return redirect("/imports")

    deleted_count = ImportJob.query.filter(
        ImportJob.user_id == user_id,
        ImportJob.id.in_(job_ids),
    ).delete(synchronize_session=False)
    db.session.commit()
    push_ui_feedback(
        f"Cleared {deleted_count} import job{'s' if deleted_count != 1 else ''}.",
        "success",
    )
    return redirect("/imports")


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
    raw_description = request.form.get("raw_description", "").strip() or description
    display_name = clean_transaction_description(request.form.get("display_name", "").strip() or description)
    amount = safe_float(request.form.get("amount"))
    category = request.form.get("category", "").strip()
    tags = serialize_tags(request.form.get("tags", ""))

    if dt is None or not display_name or amount is None:
        push_ui_feedback("Enter a date, description, and valid amount to save the transaction.", "danger")
        return redirect("/")

    category_source = "Manual"
    category_confidence = "high"
    if not category:
        category, source = categorize_transaction(user_id, raw_description, amount)
        category = (category or "Needs Review").strip()
        category_source = (source or "Auto").strip()
        category_confidence = "uncategorized" if category.lower() in GENERIC_CATEGORIES else "high"
    else:
        remember_merchant_category(user_id, raw_description, category, display_name=display_name)

    account_id = int(account_id)

    tx = Transaction(
        user_id=user_id,
        account_id=account_id,
        date=dt,
        description=display_name,
        raw_description=raw_description,
        display_name=display_name,
        amount=amount,
        category=category,
        category_source=category_source,
        category_confidence=category_confidence,
        transaction_subtype=transaction_subtype_for(amount, category, category_source),
        tags=tags,
    )

    db.session.add(tx)

    acct = Account.query.get(account_id)
    if acct and acct.user_id == user_id:
        acct.balance += amount

    log_activity(
        user_id,
        f"Added transaction {display_name}",
        f"{category} · ${amount:,.2f} saved to {acct.name if acct else 'your account'}{f' · tags: {', '.join(display_tag(tag) for tag in parse_tags(tags))}' if tags else ''}.",
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
    transaction.category_source = "Manual Review"
    transaction.category_confidence = "high"
    transaction.transaction_subtype = transaction_subtype_for(transaction.amount, new_category, "Manual Review")

    remember_merchant_category(
        user_id,
        transaction_reference_description(transaction),
        new_category,
        display_name=transaction_display_name(transaction),
    )
    log_activity(
        user_id,
        f"Updated category for {transaction_display_name(transaction)}",
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
    category_choices = import_category_choices(user_id)

    if request.method == "POST":
        new_date = parse_date_any(request.form.get("date"))
        new_raw_desc = request.form.get("raw_description", "").strip() or transaction_raw_description(tx)
        new_display_name = clean_transaction_description(request.form.get("display_name", "").strip() or request.form.get("description", "").strip())
        new_amount = safe_float(request.form.get("amount"))
        new_category = request.form.get("category", "").strip()
        new_tags = serialize_tags(request.form.get("tags", ""))
        new_account_id = int(request.form.get("account_id"))
        requested_subtype = (request.form.get("transaction_subtype") or "").strip().lower()

        if new_date is None or not new_display_name or new_amount is None:
            return "Invalid input"

        # reverse old impact
        old_acct = Account.query.get(tx.account_id)
        if old_acct and old_acct.user_id == user_id:
            old_acct.balance -= tx.amount

        # apply new data
        tx.date = new_date
        tx.raw_description = new_raw_desc
        tx.display_name = new_display_name
        tx.description = new_display_name
        tx.amount = new_amount
        if new_category:
            resolved_category = new_category
            resolved_source = "Manual Edit"
            resolved_confidence = "high"
        else:
            resolved_category, resolved_source = categorize_transaction(user_id, new_raw_desc, new_amount)
            resolved_category = (resolved_category or "Needs Review").strip()
            resolved_confidence = "uncategorized" if resolved_category.lower() in GENERIC_CATEGORIES else "medium"
        tx.category = resolved_category
        tx.category_source = resolved_source
        tx.category_confidence = resolved_confidence
        tx.transaction_subtype = requested_subtype if requested_subtype in VALID_TRANSACTION_SUBTYPES else transaction_subtype_for(new_amount, resolved_category, resolved_source)
        tx.account_id = new_account_id
        tx.tags = new_tags

        if new_category:
            remember_merchant_category(user_id, new_raw_desc, new_category, display_name=new_display_name)

        new_acct = Account.query.get(new_account_id)
        if new_acct and new_acct.user_id == user_id:
            new_acct.balance += new_amount

        log_activity(
            user_id,
            f"Edited transaction {transaction_display_name(tx)}",
            f"Updated amount to ${new_amount:,.2f} and category to {tx.category}.",
            kind="transaction_edited",
            icon="bi-pencil-square",
            target_url=redirect_to,
        )
        db.session.commit()
        return redirect(redirect_to)

    return render_template(
        "edit_transaction.html",
        tx=tx,
        accounts=accounts,
        redirect_to=redirect_to,
        category_choices=category_choices,
        tx_tags=", ".join(display_tag(tag) for tag in parse_tags(getattr(tx, "tags", ""))),
    )


@app.route("/delete_tx/<int:tx_id>", methods=["POST"])
def delete_tx(tx_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    tx = Transaction.query.get(tx_id)
    if tx and tx.user_id == user_id:
        description = transaction_display_name(tx)
        acct = Account.query.get(tx.account_id)
        if acct and acct.user_id == user_id:
            acct.balance -= tx.amount
        db.session.delete(tx)
        log_activity(
            user_id,
            f"Deleted transaction {description}",
            "The transaction was removed and the account balance was adjusted.",
            kind="transaction_deleted",
            icon="bi-trash3",
            target_url="/",
        )
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


@app.route("/transactions", methods=["GET", "POST"])
def transactions_page():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    if request.method == "POST":
        selected_ids = [safe_int(value) for value in request.form.getlist("selected_tx")]
        selected_ids = [value for value in selected_ids if value]
        action = (request.form.get("bulk_action") or "").strip().lower()
        bulk_category = (request.form.get("bulk_category") or "").strip()
        bulk_tags = serialize_tags(request.form.get("bulk_tags", ""))
        bulk_subtype = (request.form.get("bulk_subtype") or "").strip().lower()

        if not selected_ids:
            push_ui_feedback("Select at least one transaction first.", "danger")
            return redirect("/transactions")

        transactions_to_update = (
            Transaction.query
            .filter(Transaction.user_id == user_id, Transaction.id.in_(selected_ids))
            .all()
        )
        if not transactions_to_update:
            push_ui_feedback("Those transactions are no longer available.", "danger")
            return redirect("/transactions")

        updated_count = 0
        if action == "set_category" and bulk_category:
            for tx in transactions_to_update:
                tx.category = bulk_category
                tx.category_source = "Bulk Edit"
                tx.category_confidence = "high"
                tx.transaction_subtype = transaction_subtype_for(tx.amount, bulk_category, "Bulk Edit", getattr(tx, "transaction_subtype", ""))
                remember_merchant_category(
                    user_id,
                    transaction_reference_description(tx),
                    bulk_category,
                    display_name=transaction_display_name(tx),
                )
                updated_count += 1
            push_ui_feedback(f"Updated categories on {updated_count} transaction{'s' if updated_count != 1 else ''}.", "success")
        elif action == "add_tags" and bulk_tags:
            tag_set = parse_tags(bulk_tags)
            for tx in transactions_to_update:
                merged_tags = sorted(set(parse_tags(getattr(tx, "tags", ""))) | set(tag_set))
                tx.tags = serialize_tags(merged_tags)
                updated_count += 1
            push_ui_feedback(f"Updated tags on {updated_count} transaction{'s' if updated_count != 1 else ''}.", "success")
        elif action == "set_subtype" and bulk_subtype in VALID_TRANSACTION_SUBTYPES:
            for tx in transactions_to_update:
                tx.transaction_subtype = bulk_subtype
                tx.category_source = "Bulk Edit"
                tx.category_confidence = "high"
                updated_count += 1
            push_ui_feedback(f"Updated transaction type on {updated_count} transaction{'s' if updated_count != 1 else ''}.", "success")
        else:
            push_ui_feedback("Choose a valid bulk action and value to continue.", "danger")
            return redirect("/transactions")

        log_activity(
            user_id,
            "Bulk updated transactions",
            f"{updated_count} transactions were updated from the transactions command center.",
            kind="transaction_edited",
            icon="bi-sliders",
            target_url="/transactions",
        )
        db.session.commit()
        return redirect("/transactions")

    query_text = request.args.get("q", "").strip()
    category_filter = request.args.get("category", "").strip()
    type_filter = request.args.get("type", "").strip().lower()
    tag_filter = normalize_tag_label(request.args.get("tag", ""))
    source_filter = request.args.get("source", "").strip()
    status_filter = (request.args.get("status", "") or "").strip().lower()

    transactions = (
        Transaction.query
        .filter_by(user_id=user_id)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .all()
    )

    if query_text:
        lowered = query_text.lower()
        parsed_date = parse_date_any(query_text)
        transactions = [
            tx for tx in transactions
            if lowered in (transaction_display_name(tx) or "").lower()
            or lowered in (transaction_raw_description(tx) or "").lower()
            or lowered in (tx.category or "").lower()
            or lowered in str(tx.date)
            or (parsed_date is not None and tx.date == parsed_date)
        ]

    if category_filter:
        transactions = [tx for tx in transactions if transaction_ui_category(tx.category) == category_filter]

    if tag_filter:
        transactions = [tx for tx in transactions if tag_filter in parse_tags(getattr(tx, "tags", ""))]

    if type_filter:
        transactions = [tx for tx in transactions if (getattr(tx, "transaction_subtype", "") or transaction_subtype_for(tx.amount, tx.category, getattr(tx, "category_source", ""))).lower() == type_filter]

    if source_filter:
        transactions = [tx for tx in transactions if (getattr(tx, "category_source", "") or "") == source_filter]

    if status_filter == "needs_attention":
        transactions = [
            tx for tx in transactions
            if normalize_confidence_bucket(getattr(tx, "category_confidence", "")) in {"low", "uncategorized"}
        ]
    elif status_filter == "errors":
        transactions = [
            tx for tx in transactions
            if normalize_confidence_bucket(getattr(tx, "category_confidence", "")) == "error"
        ]
    elif status_filter == "reviewed":
        transactions = [
            tx for tx in transactions
            if normalize_confidence_bucket(getattr(tx, "category_confidence", "")) not in {"low", "uncategorized", "error"}
        ]

    all_user_transactions = Transaction.query.filter_by(user_id=user_id).all()
    categories = transaction_ui_category_choices(user_id)
    account_name_map = {account.id: account.name for account in Account.query.filter_by(user_id=user_id).all()}
    source_choices = sorted({(getattr(tx, "category_source", "") or "").strip() for tx in all_user_transactions if (getattr(tx, "category_source", "") or "").strip()})
    known_tags = sorted({tag for tx in all_user_transactions for tag in parse_tags(getattr(tx, "tags", ""))})
    has_transactions = bool(all_user_transactions)
    has_active_filters = any([query_text, category_filter, type_filter, tag_filter, source_filter, status_filter])

    return render_template(
        "transactions.html",
        transactions=transactions[:200],
        total_results=len(transactions),
        has_transactions=has_transactions,
        categories=categories,
        account_name_map=account_name_map,
        query_text=query_text,
        category_filter=category_filter,
        type_filter=type_filter,
        tag_filter=tag_filter,
        source_filter=source_filter,
        status_filter=status_filter,
        source_choices=source_choices,
        status_choices=TRANSACTION_STATUS_OPTIONS,
        bulk_subtype_choices=[("income", "Income"), ("expense", "Expense"), ("transfer", "Transfer"), ("payment", "Payment")],
        known_tags=known_tags,
        category_choices=transaction_ui_category_choices(user_id),
        has_active_filters=has_active_filters,
    )


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
        writer.writerow([tx.date.isoformat(), transaction_display_name(tx), tx.amount, tx.category, acct_name])

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
    transaction_q = request.args.get("transaction_q", "").strip()
    tag_filter = normalize_tag_label(request.args.get("tag", ""))
    try:
        transaction_page = max(int(request.args.get("page", "1")), 1)
    except ValueError:
        transaction_page = 1
    transaction_page_size = 25

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
    dashboard_empty_state = len(transactions) == 0
    net_worth_explainer = ""
    if dashboard_empty_state and total_liabilities > 0 and total_assets <= 0:
        net_worth_explainer = (
            f"Net worth is negative because liabilities total ${total_liabilities:,.2f} and no asset accounts are connected yet."
        )
    elif dashboard_empty_state and total_liabilities > 0 and total_assets > 0:
        net_worth_explainer = (
            f"Net worth reflects ${total_assets:,.2f} in assets and ${total_liabilities:,.2f} in liabilities."
        )
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
    account_labels, account_values = account_type_breakdown_series(accounts)

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
    recurring_income_sources = analyze_recurring_income(transactions)
    recurring_income_estimate = recurring_income_monthly_estimate(recurring_income_sources)
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
    effective_monthly_income = max(float(monthly_income or 0), float(recurring_income_estimate or 0))
    savings_snapshot = calculate_savings_snapshot(
        accounts=accounts,
        transactions=transactions,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=effective_monthly_income,
        monthly_expenses=monthly_expenses
    )
    previous_savings_snapshot = calculate_savings_snapshot(
        accounts=accounts,
        transactions=transactions,
        selected_month=previous_month,
        selected_year=previous_year,
        monthly_income=max(float(prev_monthly_income or 0), float(recurring_income_estimate or 0)),
        monthly_expenses=prev_monthly_expenses
    )
    wealth_snapshot = build_wealth_snapshot(
        accounts=accounts,
        transactions=transactions,
        goals=goals,
        selected_month=selected_month,
        selected_year=selected_year,
        monthly_income=effective_monthly_income,
        monthly_expenses=monthly_expenses,
        category_totals=category_totals,
        savings_snapshot=savings_snapshot,
        nw_values=nw_values,
    )
    goal_allocation_budget = suggested_goal_allocation_budget(wealth_snapshot["goal_rows"])
    account_allocation_summary = goals_account_allocation_summary(user_id, accounts, wealth_snapshot["goal_rows"])
    income_allocation_alerts = build_income_allocation_alerts(
        recurring_income_sources=recurring_income_sources,
        goal_rows=wealth_snapshot["goal_rows"],
        account_allocation_rows=account_allocation_summary,
        selected_month=selected_month,
        selected_year=selected_year,
    )
    safe_to_spend = calculate_safe_to_spend(
        accounts=accounts,
        subscriptions=subscriptions,
        budget_rows=budget_rows,
        monthly_income=effective_monthly_income,
        monthly_expenses=monthly_expenses,
        recurring_monthly_obligations=recurring_monthly_obligations,
        savings_target_amount=savings_snapshot["recommended_amount"],
        goal_set_aside_amount=goal_allocation_budget["suggested_goal_set_aside"],
        selected_month=selected_month,
        selected_year=selected_year,
        actual_monthly_income=monthly_income,
    )
    previous_safe_to_spend = calculate_safe_to_spend(
        accounts=accounts,
        subscriptions=subscriptions,
        budget_rows=budget_rows,
        monthly_income=max(float(prev_monthly_income or 0), float(recurring_income_estimate or 0)),
        monthly_expenses=prev_monthly_expenses,
        recurring_monthly_obligations=recurring_monthly_obligations,
        savings_target_amount=previous_savings_snapshot["recommended_amount"],
        goal_set_aside_amount=goal_allocation_budget["suggested_goal_set_aside"],
        selected_month=previous_month,
        selected_year=previous_year,
        actual_monthly_income=prev_monthly_income,
    )
    previous_net_worth = compute_previous_net_worth(accounts, transactions, selected_month, selected_year)
    dashboard_metric_changes = {
        "net_worth": build_metric_change(net_worth, previous_net_worth, "up"),
        "income": build_metric_change(monthly_income, prev_monthly_income, "up"),
        "expenses": build_metric_change(monthly_expenses, prev_monthly_expenses, "down"),
        "savings": build_metric_change(savings_snapshot["current_savings"], previous_savings_snapshot["current_savings"], "up"),
        "safe_to_spend": build_metric_change(safe_to_spend["safe_to_spend"], previous_safe_to_spend["safe_to_spend"], "up"),
    }
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
    monthly_overview_labels, monthly_overview_income, monthly_overview_expenses = monthly_overview_series(transactions)
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

    recent_transactions_query = Transaction.query.filter_by(user_id=user_id)
    if transaction_q:
        lowered_query = transaction_q.lower()
        search_like = f"%{lowered_query}%"
        parsed_search_date = parse_date_any(transaction_q)
        search_clauses = [
            func.lower(Transaction.description).like(search_like),
            func.lower(Transaction.category).like(search_like),
            Transaction.date.cast(String).like(f"%{transaction_q}%"),
        ]
        if parsed_search_date:
            search_clauses.append(Transaction.date == parsed_search_date)
        recent_transactions_query = recent_transactions_query.filter(or_(*search_clauses))
    if tag_filter:
        recent_transactions_query = recent_transactions_query.filter(or_(*tag_filter_clauses(tag_filter)))

    filtered_transaction_count = recent_transactions_query.count()
    recent_transactions = (
        recent_transactions_query
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .offset((transaction_page - 1) * transaction_page_size)
        .limit(transaction_page_size)
        .all()
    )
    for tx in recent_transactions:
        tx.tag_list = parse_tags(getattr(tx, "tags", ""))
        tx.tag_display_list = [display_tag(tag) for tag in tx.tag_list]

    displayed_transaction_count = len(recent_transactions)
    has_prev_page = transaction_page > 1
    has_next_page = (transaction_page * transaction_page_size) < filtered_transaction_count

    return render_template(
        "home.html",
        accounts=accounts,
        today_iso=date.today().isoformat(),
        transactions=recent_transactions,
        transaction_q=transaction_q,
        tag_filter=tag_filter,
        transaction_page=transaction_page,
        transaction_page_size=transaction_page_size,
        has_prev_page=has_prev_page,
        has_next_page=has_next_page,
        transaction_count=len(transactions),
        filtered_transaction_count=filtered_transaction_count,
        displayed_transaction_count=displayed_transaction_count,
        account_name_map=account_name_map,
        onboarding_state=onboarding_state,
        dashboard_empty_state=dashboard_empty_state,
        net_worth_explainer=net_worth_explainer,
        recent_activity=recent_activity,
        subscriptions=subscriptions,
        recurring_income_sources=recurring_income_sources,
        recurring_income_estimate=recurring_income_estimate,
        income_allocation_alerts=income_allocation_alerts,
        effective_monthly_income=effective_monthly_income,
        goal_allocation_budget=goal_allocation_budget,
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
        dashboard_metric_changes=dashboard_metric_changes,
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
        monthly_overview_labels=monthly_overview_labels,
        monthly_overview_income=monthly_overview_income,
        monthly_overview_expenses=monthly_overview_expenses,
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
    app.run(
        host=os.getenv("FLASK_RUN_HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", os.getenv("FLASK_RUN_PORT", 5000))),
        debug=app.config["DEBUG"],
    )
