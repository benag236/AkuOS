from flask import Flask, render_template, request, redirect, session, Response, url_for, has_request_context, jsonify, g
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
from sqlalchemy import inspect, text, func, or_, and_, extract, String
from sqlalchemy.orm import load_only
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import OperationalError, ProgrammingError
from contextlib import contextmanager
import os
import math
import json
import uuid
import re
import calendar
import shutil
import threading
import time
import base64
import hashlib
import secrets
from datetime import datetime, date, timedelta
from collections import Counter, defaultdict, deque
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
import csv
from io import StringIO, BytesIO
try:
    import pdfplumber
except ImportError:
    pdfplumber = None
try:
    import plaid
    from plaid.api import plaid_api
except ImportError:
    plaid = None
    plaid_api = None
try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:
    Fernet = None
    InvalidToken = Exception
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
    merchant_match_strength,
    merchant_similarity,
    normalize_merchant,
    normalize_text,
    sort_rules,
)
from parser_service import (
    detect_statement_file_type as parser_detect_statement_file_type,
    parse_statement_input,
)
from seed.default_category_rules import DEFAULT_CATEGORY_TAXONOMY, DEFAULT_SYSTEM_RULES
from services.category_rules import (
    TOP_LEVEL_CATEGORY_ORDER,
    canonical_category_name,
    canonical_category_pair,
    canonical_subcategory_name,
    category_label,
    taxonomy_index,
)
from services.categorization_service import (
    build_recurring_index,
    categorize_transaction_record,
    confidence_bucket as categorization_confidence_bucket,
    matches_rule as categorization_rule_matches,
    sorted_rules as sorted_categorization_rules,
)
from services.merchant_normalizer import (
    merchant_guess as derive_merchant_guess,
    merchant_key as derive_merchant_key,
    normalized_description as derive_normalized_description,
)

app = Flask(__name__)
app.config["_SCHEMA_READY"] = False
app.config["_SCHEMA_INIT_ATTEMPTED"] = False

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
IS_RENDER = bool(os.getenv("RENDER") or os.getenv("RENDER_EXTERNAL_URL"))
IS_PRODUCTION = IS_RENDER or (os.getenv("FLASK_ENV", "").strip().lower() == "production")
APP_TIMEZONE = (os.getenv("APP_TIMEZONE") or os.getenv("TZ") or "America/New_York").strip() or "America/New_York"
DB_INIT_LOCK = threading.Lock()
PLAID_ENV = (os.getenv("PLAID_ENV") or "sandbox").strip().lower() or "sandbox"
PLAID_CLIENT_ID = (os.getenv("PLAID_CLIENT_ID") or "").strip()
PLAID_SECRET = (os.getenv("PLAID_SECRET") or "").strip()
PLAID_REDIRECT_URI = (os.getenv("PLAID_REDIRECT_URI") or "").strip()
PLAID_WEBHOOK = (os.getenv("PLAID_WEBHOOK") or "").strip()
PLAID_TOKEN_ENCRYPTION_KEY = (os.getenv("PLAID_TOKEN_ENCRYPTION_KEY") or "").strip()
CSRF_EXEMPT_ENDPOINTS = {"plaid_webhook"}
RATE_LIMIT_LOCK = threading.Lock()
RATE_LIMIT_BUCKETS = defaultdict(deque)
SLOW_REQUEST_WARNING_MS = int(os.getenv("SLOW_REQUEST_WARNING_MS", "1200"))
SLOW_SECTION_WARNING_MS = int(os.getenv("SLOW_SECTION_WARNING_MS", "250"))
DASHBOARD_HISTORY_DAYS = int(os.getenv("DASHBOARD_HISTORY_DAYS", "365"))
DASHBOARD_TRANSACTION_LIMIT = int(os.getenv("DASHBOARD_TRANSACTION_LIMIT", "1500"))


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
        if parsed.drivername in ("postgresql", "postgresql+psycopg2"):
            return parsed.set(drivername="postgresql+psycopg").render_as_string(hide_password=False)
    except Exception:
        pass
    return database_url


def configured_database_url():
    for env_name in ("DATABASE_URL", "RENDER_DATABASE_URL", "RENDER_POSTGRES_URL", "POSTGRES_URL"):
        value = normalize_database_url(os.getenv(env_name, ""))
        if value:
            return value
    return ""


def resolve_database_uri():
    database_url = configured_database_url()
    if database_url:
        return database_url

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
    else {
        "pool_pre_ping": True,
        "connect_args": {
            "check_same_thread": False,
            "timeout": 15,
        },
    }
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
IMPORT_WORKER_LOCK = threading.Lock()
IMPORT_WORKER_THREAD = None


def resolve_token_cipher_key():
    key_material = PLAID_TOKEN_ENCRYPTION_KEY or app.config.get("SECRET_KEY") or local_secret_fallback()
    if Fernet is None:
        return None
    key_bytes = key_material.encode("utf-8")
    try:
        Fernet(key_bytes)
        return key_bytes
    except Exception:
        digest = hashlib.sha256(key_bytes).digest()
        return base64.urlsafe_b64encode(digest)


def sensitive_value_cipher():
    if Fernet is None:
        raise RuntimeError("cryptography is required for Plaid token protection.")
    return Fernet(resolve_token_cipher_key())


def encrypt_sensitive_value(value):
    value = (value or "").strip()
    if not value:
        return ""
    if value.startswith("enc::"):
        return value
    encrypted = sensitive_value_cipher().encrypt(value.encode("utf-8")).decode("utf-8")
    return f"enc::{encrypted}"


def decrypt_sensitive_value(value):
    value = (value or "").strip()
    if not value:
        return ""
    if not value.startswith("enc::"):
        return value
    encrypted_payload = value[len("enc::"):]
    try:
        return sensitive_value_cipher().decrypt(encrypted_payload.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError("Stored Plaid credentials could not be decrypted.") from exc


def redact_sensitive_text(message):
    text_value = str(message or "").strip()
    if not text_value:
        return ""
    redaction_patterns = [
        r"(?i)(access[_-]?token|public[_-]?token|secret|client[_-]?id|database[_-]?url|session|cookie)\s*[:=]\s*[^,\s]+",
        r"(?i)(item[_-]?id|institution[_-]?id|account[_-]?id)\s*[:=]\s*[^,\s]+",
    ]
    for pattern in redaction_patterns:
        text_value = re.sub(pattern, "[redacted]", text_value)
    return text_value[:255]


def plaid_user_error_message(message, fallback):
    cleaned = redact_sensitive_text(message)
    return cleaned or fallback


def log_safe_exception(message, exc=None):
    error_type = exc.__class__.__name__ if exc else "Error"
    app.logger.error("%s [%s]", message, error_type)


def get_or_create_csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def request_wants_json():
    accept_header = request.headers.get("Accept", "")
    return (
        request.is_json
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or "application/json" in accept_header.lower()
        or request.path.startswith("/plaid/")
    )


def request_origin_is_trusted():
    expected = urlparse(request.host_url)
    for header_name in ("Origin", "Referer"):
        header_value = (request.headers.get(header_name) or "").strip()
        if not header_value:
            continue
        parsed = urlparse(header_value)
        return parsed.scheme == expected.scheme and parsed.netloc == expected.netloc
    return True


def submitted_csrf_token():
    header_token = (request.headers.get("X-CSRF-Token") or "").strip()
    if header_token:
        return header_token
    form_token = (request.form.get("csrf_token") or "").strip()
    if form_token:
        return form_token
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        return (payload.get("csrf_token") or "").strip()
    return ""


def csrf_failure_response():
    message = "Security check failed. Refresh the page and try again."
    if request_wants_json():
        return jsonify({"error": message}), 400
    push_ui_feedback(message, "danger")
    fallback = safe_local_redirect(request.referrer, request.path if request.path.startswith("/") else "/")
    return redirect(fallback)


def validate_csrf_request():
    if request.method in {"GET", "HEAD", "OPTIONS", "TRACE"}:
        return None
    if request.endpoint in CSRF_EXEMPT_ENDPOINTS:
        return None
    session_token = session.get("_csrf_token") or get_or_create_csrf_token()
    request_token = submitted_csrf_token()
    if not session_token or not request_token or not secrets.compare_digest(session_token, request_token):
        return csrf_failure_response()
    if not request_origin_is_trusted():
        return csrf_failure_response()
    return None


def rate_limit_identity():
    user_id = get_user_id() if has_request_context() else None
    if user_id:
        return f"user:{user_id}"
    forwarded_for = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    remote_addr = forwarded_for or (request.remote_addr or "anonymous")
    return f"ip:{remote_addr}"


def hit_rate_limit(limit_key, limit, window_seconds):
    now = time.time()
    bucket_key = f"{limit_key}:{rate_limit_identity()}"
    with RATE_LIMIT_LOCK:
        bucket = RATE_LIMIT_BUCKETS[bucket_key]
        while bucket and bucket[0] <= now - window_seconds:
            bucket.popleft()
        if len(bucket) >= limit:
            retry_after = max(1, int(window_seconds - (now - bucket[0])))
            return True, retry_after
        bucket.append(now)
    return False, 0


def rate_limit_response(limit_key, limit, window_seconds, html_fallback="/", message="Too many requests. Please wait and try again."):
    limited, retry_after = hit_rate_limit(limit_key, limit, window_seconds)
    if not limited:
        return None
    if request_wants_json():
        response = jsonify({"error": message})
        response.status_code = 429
        response.headers["Retry-After"] = str(retry_after)
        return response
    push_ui_feedback(message, "danger")
    return redirect(safe_local_redirect(html_fallback, "/"))


def sql_boolean_literal(value):
    return "TRUE" if bool(value) else "FALSE"


def safe_schema_alter(conn, statement):
    try:
        conn.execute(text(statement))
    except Exception as exc:
        message = str(exc).lower()
        if "duplicate column name" in message or "already exists" in message:
            return
        raise


def table_exists(table_name):
    try:
        return table_name in inspect(db.engine).get_table_names()
    except Exception:
        return False


def model_table_name(model):
    table = getattr(model, "__table__", None)
    if table is not None and getattr(table, "name", ""):
        return table.name
    explicit_name = getattr(model, "__tablename__", "")
    if explicit_name:
        return explicit_name
    return model.__name__.lower()


def model_table_exists(model):
    return table_exists(model_table_name(model))


def safe_user_delete_step(user_id, label, model, query_factory, delete_counts, optional=True):
    table_name = model_table_name(model)
    if not model_table_exists(model):
        log_method = app.logger.warning if optional else app.logger.error
        log_method(
            "Skipping delete-all step '%s' for user_id=%s because table '%s' is unavailable.",
            label,
            user_id,
            table_name,
        )
        delete_counts[label] = 0
        return 0

    try:
        deleted = query_factory().delete(synchronize_session=False)
        delete_counts[label] = deleted
        app.logger.info(
            "Delete-all step '%s' removed %s row(s) for user_id=%s from table '%s'.",
            label,
            deleted,
            user_id,
            table_name,
        )
        return deleted
    except Exception as exc:
        app.logger.exception(
            "Delete-all step '%s' failed for user_id=%s on table '%s': %s",
            label,
            user_id,
            table_name,
            exc,
        )
        raise

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
    plaid_account_id = db.Column(db.String(120), nullable=True)


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


class Category(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    slug = db.Column(db.String(120), nullable=False, unique=True)
    parent_id = db.Column(db.Integer, nullable=True)
    sort_order = db.Column(db.Integer, nullable=False, default=0)
    is_system = db.Column(db.Boolean, nullable=False, default=True)


class CategoryRule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    keyword = db.Column(db.String(100), nullable=False)
    category = db.Column(db.String(100), nullable=False)
    priority = db.Column(db.Integer, nullable=False, default=100)
    match_type = db.Column(db.String(20), nullable=False, default="contains")
    amount_direction = db.Column(db.String(20), nullable=False, default="any")
    rule_type = db.Column(db.String(20), nullable=False, default="contains")
    pattern = db.Column(db.String(255), nullable=False, default="")
    category_id = db.Column(db.Integer, nullable=True)
    subcategory_id = db.Column(db.Integer, nullable=True)
    confidence = db.Column(db.Float, nullable=False, default=0.8)
    is_system_rule = db.Column(db.Boolean, nullable=False, default=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    subtype = db.Column(db.String(20), nullable=False, default="")
    display_name_override = db.Column(db.String(255), nullable=False, default="")
    tag_rules = db.Column(db.String(255), nullable=False, default="")
    skip_transaction = db.Column(db.Boolean, nullable=False, default=False)


class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.Integer, nullable=False)
    date = db.Column(db.Date, nullable=False)
    description = db.Column(db.String(255), nullable=False)
    raw_description = db.Column(db.String(255), nullable=False, default="")
    display_name = db.Column(db.String(255), nullable=False, default="")
    normalized_description = db.Column(db.String(255), nullable=False, default="")
    merchant_guess = db.Column(db.String(255), nullable=False, default="")
    amount = db.Column(db.Float, nullable=False)
    category = db.Column(db.String(100), nullable=False)
    subcategory = db.Column(db.String(100), nullable=False, default="")
    suggested_category_id = db.Column(db.Integer, nullable=True)
    suggested_subcategory_id = db.Column(db.Integer, nullable=True)
    category_source = db.Column(db.String(80), nullable=False, default="")
    category_confidence = db.Column(db.String(20), nullable=False, default="")
    matched_rule_id = db.Column(db.Integer, nullable=True)
    needs_review = db.Column(db.Boolean, nullable=False, default=False)
    transaction_subtype = db.Column(db.String(20), nullable=False, default="")
    import_source = db.Column(db.String(20), nullable=False, default="")
    fingerprint = db.Column(db.String(255), nullable=False, default="")
    plaid_transaction_id = db.Column(db.String(120), nullable=True)
    plaid_pending_transaction_id = db.Column(db.String(120), nullable=True)
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
    duplicate_candidate_count = db.Column(db.Integer, nullable=False, default=0)
    skipped_count = db.Column(db.Integer, nullable=False, default=0)
    not_transaction_count = db.Column(db.Integer, nullable=False, default=0)
    needs_review_count = db.Column(db.Integer, nullable=False, default=0)
    start_date = db.Column(db.Date, nullable=True)
    end_date = db.Column(db.Date, nullable=True)
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
    review_payload_json = db.Column(db.Text, nullable=False, default="{}")
    summary_json = db.Column(db.Text, nullable=False, default="{}")
    error_message = db.Column(db.String(255), nullable=True)
    start_date = db.Column(db.Date, nullable=True)
    end_date = db.Column(db.Date, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)


class PlaidItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    item_id = db.Column(db.String(120), nullable=False, unique=True)
    access_token = db.Column(db.Text, nullable=False)
    institution_id = db.Column(db.String(120), nullable=False, default="")
    institution_name = db.Column(db.String(255), nullable=False, default="")
    sync_cursor = db.Column(db.Text, nullable=False, default="")
    status = db.Column(db.String(20), nullable=False, default="active")
    last_sync_error = db.Column(db.String(255), nullable=True)
    last_synced_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class PlaidAccountLink(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    plaid_item_id = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.Integer, nullable=False)
    plaid_account_id = db.Column(db.String(120), nullable=False)
    name = db.Column(db.String(255), nullable=False, default="")
    official_name = db.Column(db.String(255), nullable=False, default="")
    mask = db.Column(db.String(20), nullable=False, default="")
    plaid_type = db.Column(db.String(80), nullable=False, default="")
    plaid_subtype = db.Column(db.String(80), nullable=False, default="")
    current_balance = db.Column(db.Float, nullable=True)
    available_balance = db.Column(db.Float, nullable=True)
    currency_code = db.Column(db.String(12), nullable=False, default="USD")
    status = db.Column(db.String(20), nullable=False, default="active")
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class MerchantMemory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    merchant = db.Column(db.String(200), nullable=False)
    category = db.Column(db.String(100), nullable=False)
    subcategory = db.Column(db.String(100), nullable=False, default="")
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


class UpcomingPayment(db.Model):
    __tablename__ = "upcoming_payment"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(140), nullable=False)
    amount = db.Column(db.Float, nullable=False, default=0)
    due_date = db.Column(db.Date, nullable=False)
    account_id = db.Column(db.Integer, nullable=True)
    category = db.Column(db.String(100), nullable=False, default="")
    is_recurring = db.Column(db.Boolean, nullable=False, default=False)
    frequency = db.Column(db.String(40), nullable=False, default="Monthly")
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


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


@contextmanager
def timed_route_section(route_name, section_name, warning_ms=SLOW_SECTION_WARNING_MS):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
        if elapsed_ms >= warning_ms:
            app.logger.warning(
                "Slow route section route=%s section=%s elapsed_ms=%.1f path=%s",
                route_name,
                section_name,
                elapsed_ms,
                request.path if has_request_context() else "",
            )


def transaction_minimal_load_options():
    return (
        load_only(
            Transaction.id,
            Transaction.user_id,
            Transaction.account_id,
            Transaction.date,
            Transaction.description,
            Transaction.raw_description,
            Transaction.display_name,
            Transaction.normalized_description,
            Transaction.merchant_guess,
            Transaction.amount,
            Transaction.category,
            Transaction.subcategory,
            Transaction.suggested_category_id,
            Transaction.suggested_subcategory_id,
            Transaction.category_source,
            Transaction.category_confidence,
            Transaction.matched_rule_id,
            Transaction.needs_review,
            Transaction.transaction_subtype,
            Transaction.import_source,
            Transaction.tags,
        ),
    )


def load_dashboard_transactions(user_id, newest_first=False):
    cutoff_date = date.today() - timedelta(days=max(DASHBOARD_HISTORY_DAYS, 30))
    base_query = (
        Transaction.query
        .filter(Transaction.user_id == user_id, Transaction.date >= cutoff_date)
        .options(*transaction_minimal_load_options())
    )
    order_columns = (
        (Transaction.date.desc(), Transaction.id.desc())
        if newest_first else
        (Transaction.date.asc(), Transaction.id.asc())
    )
    rows = base_query.order_by(*order_columns).limit(DASHBOARD_TRANSACTION_LIMIT).all()
    if rows:
        return rows

    fallback_rows = (
        Transaction.query
        .filter(Transaction.user_id == user_id)
        .options(*transaction_minimal_load_options())
        .order_by(*order_columns)
        .limit(DASHBOARD_TRANSACTION_LIMIT)
        .all()
    )
    if newest_first:
        return fallback_rows
    return sorted(fallback_rows, key=lambda tx: (tx.date or date.min, tx.id or 0))


def dashboard_month_bounds(month, year):
    start_date = date(year, month, 1)
    end_date = date(year, month, calendar.monthrange(year, month)[1])
    return start_date, end_date


def dashboard_income_clause():
    subtype = func.lower(func.coalesce(Transaction.transaction_subtype, ""))
    return and_(Transaction.amount > 0, or_(subtype == "income", subtype == ""))


def dashboard_expense_clause():
    subtype = func.lower(func.coalesce(Transaction.transaction_subtype, ""))
    category_name = func.lower(func.coalesce(Transaction.category, ""))
    return and_(
        Transaction.amount < 0,
        subtype.notin_(["transfer", "payment", "income"]),
        category_name.notin_(["transfer", "credit card payment", "transfer payment"]),
    )


def dashboard_month_totals_aggregate(user_id, month, year):
    start_date, end_date = dashboard_month_bounds(month, year)
    current_income = (
        db.session.query(func.coalesce(func.sum(Transaction.amount), 0.0))
        .filter(
            Transaction.user_id == user_id,
            Transaction.date >= start_date,
            Transaction.date <= end_date,
            dashboard_income_clause(),
        )
        .scalar()
        or 0.0
    )
    current_expenses = (
        db.session.query(func.coalesce(func.sum(-Transaction.amount), 0.0))
        .filter(
            Transaction.user_id == user_id,
            Transaction.date >= start_date,
            Transaction.date <= end_date,
            dashboard_expense_clause(),
        )
        .scalar()
        or 0.0
    )
    return round(float(current_income or 0), 2), round(float(current_expenses or 0), 2)


def dashboard_spending_chart_state_aggregate(user_id, selected_month, selected_year, requested_month=None, requested_year=None, limit=12):
    month_rows = (
        db.session.query(
            extract("year", Transaction.date).label("year_value"),
            extract("month", Transaction.date).label("month_value"),
        )
        .filter(Transaction.user_id == user_id, dashboard_expense_clause())
        .group_by("year_value", "month_value")
        .order_by(text("year_value DESC"), text("month_value DESC"))
        .limit(limit)
        .all()
    )
    month_options = []
    month_pairs = []
    seen_pairs = set()
    for year_value, month_value in month_rows:
        pair = (int(year_value), int(month_value))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        month_pairs.append(pair)
        month_options.append({
            "value_month": pair[1],
            "value_year": pair[0],
            "label": f"{calendar.month_name[pair[1]]} {pair[0]}",
            "has_data": True,
        })

    requested_key = (
        int(requested_year or selected_year),
        int(requested_month or selected_month),
    )
    chart_key = requested_key if requested_key in seen_pairs or not month_pairs else requested_key
    if chart_key not in seen_pairs and month_pairs:
        chart_key = requested_key

    start_date, end_date = dashboard_month_bounds(chart_key[1], chart_key[0])
    category_rows = (
        db.session.query(
            Transaction.category,
            func.coalesce(func.sum(-Transaction.amount), 0.0).label("total_amount"),
            func.count(Transaction.id).label("tx_count"),
        )
        .filter(
            Transaction.user_id == user_id,
            Transaction.date >= start_date,
            Transaction.date <= end_date,
            dashboard_expense_clause(),
        )
        .group_by(Transaction.category)
        .all()
    )

    normalized_totals = defaultdict(float)
    uncategorized_count = 0
    uncategorized_total = 0.0
    expense_count = 0
    for raw_category, total_amount, tx_count in category_rows:
        count_value = int(tx_count or 0)
        expense_count += count_value
        normalized_category = transaction_ui_category(raw_category) or "Other"
        if normalized_category == "Needs Review":
            normalized_category = "Other"
            uncategorized_count += count_value
            uncategorized_total += float(total_amount or 0)
        normalized_totals[normalized_category] += float(total_amount or 0)

    chart_labels = list(normalized_totals.keys())
    chart_values = [round(normalized_totals[label], 2) for label in chart_labels]
    chart_month_label = f"{calendar.month_name[chart_key[1]]} {chart_key[0]}"

    if expense_count <= 0:
        empty_message = "No categorized spending for this month"
        notice = "Choose another month to see recent spending history."
    elif uncategorized_count > 0 and uncategorized_count == expense_count:
        empty_message = ""
        notice = f"Showing {chart_month_label} spending grouped under Other until categories are cleaned up."
    elif uncategorized_count > 0:
        empty_message = ""
        notice = f"{uncategorized_count} expense transaction{'s' if uncategorized_count != 1 else ''} are grouped under Other in {chart_month_label}."
    else:
        empty_message = ""
        notice = f"Showing spending for {chart_month_label}."

    return {
        "labels": chart_labels,
        "values": chart_values,
        "month_label": chart_month_label,
        "month": chart_key[1],
        "year": chart_key[0],
        "expense_count": expense_count,
        "uncategorized_count": uncategorized_count,
        "uncategorized_total": round(uncategorized_total, 2),
        "empty_message": empty_message or "No categorized spending yet",
        "notice": notice,
        "month_options": month_options,
        "used_fallback": False,
    }


def dashboard_monthly_overview_series_aggregate(user_id, limit=6):
    income_rows = (
        db.session.query(
            extract("year", Transaction.date).label("year_value"),
            extract("month", Transaction.date).label("month_value"),
            func.coalesce(func.sum(Transaction.amount), 0.0).label("total_amount"),
        )
        .filter(Transaction.user_id == user_id, dashboard_income_clause())
        .group_by("year_value", "month_value")
        .all()
    )
    expense_rows = (
        db.session.query(
            extract("year", Transaction.date).label("year_value"),
            extract("month", Transaction.date).label("month_value"),
            func.coalesce(func.sum(-Transaction.amount), 0.0).label("total_amount"),
        )
        .filter(Transaction.user_id == user_id, dashboard_expense_clause())
        .group_by("year_value", "month_value")
        .all()
    )
    bucket_map = defaultdict(lambda: {"income": 0.0, "expenses": 0.0})
    for year_value, month_value, total_amount in income_rows:
        bucket_map[(int(year_value), int(month_value))]["income"] = round(float(total_amount or 0), 2)
    for year_value, month_value, total_amount in expense_rows:
        bucket_map[(int(year_value), int(month_value))]["expenses"] = round(float(total_amount or 0), 2)
    if not bucket_map:
        today = date.today()
        bucket_map[(today.year, today.month)] = {"income": 0.0, "expenses": 0.0}
    ordered_keys = sorted(bucket_map.keys())[-limit:]
    labels = [f"{calendar.month_abbr[month]} {str(year)[-2:]}" for year, month in ordered_keys]
    income_values = [bucket_map[key]["income"] for key in ordered_keys]
    expense_values = [bucket_map[key]["expenses"] for key in ordered_keys]
    return labels, income_values, expense_values


def dashboard_savings_snapshot_light(accounts, monthly_income):
    savings_like_accounts = []
    for account in accounts or []:
        if account.type != "asset":
            continue
        subtype = infer_account_subtype(account)
        preference = normalize_savings_preference(getattr(account, "savings_preference", "auto"))
        if preference == "include" or subtype in {"savings", "investment"}:
            savings_like_accounts.append(account)
    current_savings = round(sum(max(float(account.balance or 0), 0) for account in savings_like_accounts), 2)
    tiers = savings_target_tiers(monthly_income)
    return {
        "current_savings": current_savings,
        "recommended_amount": tiers.get("recommended_amount") or 0,
        "recommended_rate": tiers.get("recommended_rate"),
    }


def build_dashboard_goal_snapshot_light(goals):
    goal_ids = [goal.id for goal in (goals or []) if getattr(goal, "id", None)]
    allocation_map = defaultdict(list)
    allocation_totals = defaultdict(float)
    if goal_ids:
        allocations = GoalAllocation.query.filter(GoalAllocation.goal_id.in_(goal_ids)).all()
        for allocation in allocations:
            amount = round(float(allocation.allocated_amount or 0), 2)
            allocation_totals[allocation.goal_id] += amount
            allocation_map[allocation.goal_id].append({
                "account_id": allocation.account_id,
                "allocated_amount": amount,
            })

    goal_rows = []
    for goal in goals or []:
        allocated_amount = round(allocation_totals.get(goal.id, 0.0), 2)
        current_amount = round(max(float(goal.current_amount or 0), allocated_amount), 2)
        target_amount = round(float(goal.target_amount or 0), 2)
        progress_pct = round(min((current_amount / target_amount) * 100, 100), 1) if target_amount > 0 else 0
        goal_rows.append({
            "id": goal.id,
            "name": goal.name,
            "goal_type": goal.goal_type,
            "target_amount": target_amount,
            "current_amount": current_amount,
            "allocated_amount": allocated_amount,
            "gap_remaining": round(max(target_amount - current_amount, 0), 2),
            "progress_pct": progress_pct,
            "allocation_rows": allocation_map.get(goal.id, []),
            "target_date": getattr(goal, "target_date", None),
        })

    goal_rows.sort(key=lambda row: (row.get("goal_type") != "emergency_fund", row.get("gap_remaining", 0), row.get("name", "").lower()))
    primary_goal = goal_rows[0] if goal_rows else None
    secondary_goals = goal_rows[1:] if len(goal_rows) > 1 else []
    return {
        "goal_rows": goal_rows,
        "primary_goal": primary_goal,
        "secondary_goals": secondary_goals,
    }


def dashboard_safe_to_spend_light(monthly_income, monthly_expenses, savings_target_amount):
    base_safe_to_spend = round(float(monthly_income or 0) - float(monthly_expenses or 0) - float(savings_target_amount or 0), 2)
    return {
        "safe_to_spend": base_safe_to_spend,
        "base_safe_to_spend": base_safe_to_spend,
        "used_amount": round(float(monthly_expenses or 0), 2),
        "remaining_amount": base_safe_to_spend,
        "recurring_expenses": round(float(monthly_expenses or 0), 2),
        "savings_target_amount": round(float(savings_target_amount or 0), 2),
        "goal_set_aside_amount": 0.0,
        "income_basis": round(float(monthly_income or 0), 2),
        "current_cash": 0.0,
        "explanation": (
            f"Monthly income of ${float(monthly_income or 0):,.2f} minus this month's expenses of ${float(monthly_expenses or 0):,.2f} "
            f"and a suggested savings set-aside of ${float(savings_target_amount or 0):,.2f}."
        ),
    }


def scoped_record(model, record_id, user_id=None):
    target_user_id = user_id or get_user_id()
    if record_id in (None, "") or not target_user_id:
        return None
    id_column = getattr(model, "__table__", None).columns.get("id") if getattr(model, "__table__", None) is not None else None
    if id_column is not None:
        try:
            if id_column.type.python_type is int:
                record_id = safe_int(record_id)
        except Exception:
            pass
    if record_id in (None, ""):
        return None
    return model.query.filter_by(id=record_id, user_id=target_user_id).first()


def all_categories():
    return Category.query.order_by(Category.sort_order.asc(), Category.name.asc()).all()


def category_lookup_by_id():
    return {
        category.id: {
            "id": category.id,
            "name": category.name,
            "slug": category.slug,
            "parent_id": category.parent_id,
        }
        for category in all_categories()
    }


def category_tree():
    allowed_slugs = {
        node["slug"]
        for node in DEFAULT_CATEGORY_TAXONOMY
    }
    allowed_slugs.update(
        child["slug"]
        for node in DEFAULT_CATEGORY_TAXONOMY
        for child in node.get("children", [])
    )
    categories = [category for category in all_categories() if category.slug in allowed_slugs]
    top_level = [category for category in categories if not category.parent_id]
    children = defaultdict(list)
    for category in categories:
        if category.parent_id:
            children[category.parent_id].append(category)
    return top_level, children


def resolve_category_ids(category_name="", subcategory_name=""):
    category_name, subcategory_name = canonical_category_pair(category_name, subcategory_name)
    categories = all_categories()
    by_name = {(category.name, category.parent_id): category for category in categories}
    top_level = by_name.get((category_name, None))
    subcategory = by_name.get((subcategory_name, top_level.id if top_level else None)) if subcategory_name and top_level else None
    return (
        top_level.id if top_level else None,
        subcategory.id if subcategory else None,
    )


def category_grouped_choices(user_id, include_most_used=True):
    top_level, children = category_tree()
    most_used_counter = Counter()
    if user_id:
        for category_name, count in (
            db.session.query(Transaction.category, func.count(Transaction.id))
            .filter(Transaction.user_id == user_id, Transaction.category.isnot(None), Transaction.category != "")
            .group_by(Transaction.category)
            .all()
        ):
            normalized_category = canonical_transaction_category(category_name)
            if normalized_category:
                most_used_counter[normalized_category] += int(count or 0)
    groups = []
    if include_most_used and most_used_counter:
        most_used = [name for name, _ in most_used_counter.most_common(6) if name]
        if most_used:
            groups.append({"label": "Most Used", "options": [{"value": name, "label": name} for name in most_used]})
    for category in top_level:
        option_group = [{"value": category.name, "label": category.name}]
        for child in children.get(category.id, []):
            option_group.append({
                "value": category.name,
                "label": child.name,
                "subcategory": child.name,
            })
        groups.append({"label": category.name, "options": option_group})
    return groups


def category_subcategory_map():
    top_level, children = category_tree()
    return {
        category.name: [child.name for child in children.get(category.id, [])]
        for category in top_level
    }


def transaction_category_label(tx_or_category, subcategory=None):
    if hasattr(tx_or_category, "category"):
        category_name = getattr(tx_or_category, "category", "")
        subcategory_name = getattr(tx_or_category, "subcategory", "")
    else:
        category_name = tx_or_category
        subcategory_name = subcategory
    return category_label(category_name or "Needs Review", subcategory_name or "")


def seed_default_categories():
    existing = {category.slug: category for category in Category.query.all()}
    sort_order = 0
    for node in DEFAULT_CATEGORY_TAXONOMY:
        sort_order += 10
        top_level = existing.get(node["slug"])
        if not top_level:
            top_level = Category(name=node["name"], slug=node["slug"], parent_id=None, sort_order=sort_order, is_system=True)
            db.session.add(top_level)
            db.session.flush()
            existing[node["slug"]] = top_level
        else:
            top_level.name = node["name"]
            top_level.parent_id = None
            top_level.sort_order = sort_order
            top_level.is_system = True
        child_sort_order = sort_order
        for child in node.get("children", []):
            child_sort_order += 1
            child_node = existing.get(child["slug"])
            if not child_node:
                child_node = Category(
                    name=child["name"],
                    slug=child["slug"],
                    parent_id=top_level.id,
                    sort_order=child_sort_order,
                    is_system=True,
                )
                db.session.add(child_node)
                db.session.flush()
                existing[child["slug"]] = child_node
            else:
                child_node.name = child["name"]
                child_node.parent_id = top_level.id
                child_node.sort_order = child_sort_order
                child_node.is_system = True


def seed_default_category_rules():
    categories = Category.query.all()
    categories_by_id = {category.id: category for category in categories}
    desired_rule_keys = set()

    existing_rules = {
        (
            (rule.pattern or rule.keyword or "").strip().lower(),
            (rule.rule_type or rule.match_type or "contains").strip().lower(),
            (rule.category or "").strip(),
            (
                categories_by_id[rule.subcategory_id].name
                if getattr(rule, "subcategory_id", None) in categories_by_id
                else ""
            ).strip(),
        ): rule
        for rule in CategoryRule.query.filter_by(is_system_rule=True).all()
    }

    for seed_rule in DEFAULT_SYSTEM_RULES:
        category_name, subcategory_name = canonical_category_pair(seed_rule["category"], seed_rule.get("subcategory"))
        lookup_key = (
            seed_rule["pattern"].strip().lower(),
            seed_rule["rule_type"].strip().lower(),
            category_name,
            subcategory_name,
        )
        desired_rule_keys.add(lookup_key)
        category_id, subcategory_id = resolve_category_ids(category_name, subcategory_name)
        rule = existing_rules.get(lookup_key)
        if not rule:
            rule = CategoryRule(
                user_id=0,
                keyword=seed_rule["pattern"],
                category=category_name,
                priority=seed_rule["priority"],
                match_type=seed_rule["rule_type"],
                amount_direction="credit" if seed_rule.get("subtype") == "income" else "debit" if seed_rule.get("subtype") in {"expense", "payment"} else "any",
                rule_type=seed_rule["rule_type"],
                pattern=seed_rule["pattern"],
                category_id=category_id,
                subcategory_id=subcategory_id,
                confidence=seed_rule["confidence"],
                is_system_rule=True,
                is_active=True,
                subtype=seed_rule.get("subtype", ""),
            )
            db.session.add(rule)
        else:
            rule.user_id = 0
            rule.keyword = seed_rule["pattern"]
            rule.category = category_name
            rule.priority = seed_rule["priority"]
            rule.match_type = seed_rule["rule_type"]
            rule.amount_direction = "credit" if seed_rule.get("subtype") == "income" else "debit" if seed_rule.get("subtype") in {"expense", "payment"} else "any"
            rule.rule_type = seed_rule["rule_type"]
            rule.pattern = seed_rule["pattern"]
            rule.category_id = category_id
            rule.subcategory_id = subcategory_id
            rule.confidence = seed_rule["confidence"]
            rule.is_system_rule = True
            rule.is_active = True
            rule.subtype = seed_rule.get("subtype", "")

    for lookup_key, rule in existing_rules.items():
        if lookup_key not in desired_rule_keys:
            rule.is_active = False


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


def plaid_is_configured():
    return bool(plaid and plaid_api and PLAID_CLIENT_ID and PLAID_SECRET)


def plaid_environment_host():
    if plaid is None:
        return None
    env_value = PLAID_ENV.lower()
    if env_value == "production":
        return plaid.Environment.Production
    if env_value == "development":
        return plaid.Environment.Development
    return plaid.Environment.Sandbox


def plaid_to_dict(value):
    if value is None:
        return {}
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return value
    return {}


def plaid_client():
    if not plaid_is_configured():
        raise RuntimeError("Plaid is not configured.")
    configuration = plaid.Configuration(
        host=plaid_environment_host(),
        api_key={
            "clientId": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET,
        },
    )
    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


def plaid_account_kind(plaid_type, plaid_subtype):
    plaid_type = (plaid_type or "").strip().lower()
    plaid_subtype = (plaid_subtype or "").strip().lower()
    if plaid_type == "credit":
        return "liability", "credit_card"
    if plaid_type == "loan":
        return "liability", "loan"
    if plaid_type == "investment":
        return "asset", "investment"
    if plaid_type in {"depository", "cash"}:
        if plaid_subtype in {"checking"}:
            return "asset", "checking"
        if plaid_subtype in {"savings", "money market"}:
            return "asset", "savings"
        return "asset", "cash" if plaid_type == "cash" else "checking"
    return "asset", "other_asset"


def plaid_account_display_name(plaid_account):
    account_name = (plaid_account.get("official_name") or "").strip()
    if account_name:
        return account_name[:100]
    account_name = (plaid_account.get("name") or "").strip()
    if account_name:
        return account_name[:100]
    mask = (plaid_account.get("mask") or "").strip()
    return (f"Connected Account {mask}" if mask else "Connected Account")[:100]


def plaid_account_balance_value(plaid_account):
    balances = plaid_account.get("balances") or {}
    current = balances.get("current")
    available = balances.get("available")
    chosen = current if current is not None else available
    return round(float(chosen or 0), 2)


def plaid_amount_to_akuos(plaid_amount):
    return round(0 - float(plaid_amount or 0), 2)


def find_existing_account_for_plaid(user_id, plaid_account):
    normalized_candidates = [
        normalize_text(plaid_account.get("official_name")),
        normalize_text(plaid_account.get("name")),
    ]
    normalized_candidates = [value for value in normalized_candidates if value]
    target_type, target_subtype = plaid_account_kind(plaid_account.get("type"), plaid_account.get("subtype"))
    for account in Account.query.filter_by(user_id=user_id).all():
        if account.type != target_type:
            continue
        if target_subtype and infer_account_subtype(account) not in {target_subtype, ""}:
            continue
        if normalize_text(account.name) in normalized_candidates:
            return account
    return None


def plaid_account_record(user_id, plaid_account_id):
    plaid_account_id = (plaid_account_id or "").strip()
    if not plaid_account_id:
        return None
    return Account.query.filter_by(user_id=user_id, plaid_account_id=plaid_account_id).order_by(Account.id.asc()).first()


def existing_plaid_item_for_institution(user_id, institution_id="", institution_name="", exclude_item_id=None):
    institution_id = (institution_id or "").strip()
    institution_name_normalized = normalize_text(institution_name)
    items = PlaidItem.query.filter_by(user_id=user_id).order_by(PlaidItem.updated_at.desc(), PlaidItem.id.desc()).all()
    for item in items:
        if exclude_item_id and item.id == exclude_item_id:
            continue
        if institution_id and (item.institution_id or "").strip().lower() == institution_id.lower():
            return item
        if institution_name_normalized and normalize_text(item.institution_name) == institution_name_normalized:
            return item
    return None


def upsert_plaid_account_link(user_id, plaid_item, plaid_account):
    plaid_account_id = (plaid_account.get("account_id") or "").strip()
    if not plaid_account_id:
        return None, False, None
    link = PlaidAccountLink.query.filter_by(user_id=user_id, plaid_account_id=plaid_account_id).first()
    created = False
    target_type, target_subtype = plaid_account_kind(plaid_account.get("type"), plaid_account.get("subtype"))
    account_name = plaid_account_display_name(plaid_account)
    balance_value = plaid_account_balance_value(plaid_account)
    account = plaid_account_record(user_id, plaid_account_id)
    if not account and link:
        account = Account.query.get(link.account_id)
    if not account:
        account = find_existing_account_for_plaid(user_id, plaid_account)
        if not account:
            account = Account(
                user_id=user_id,
                name=account_name,
                type=target_type,
                balance=balance_value,
                savings_preference="include" if target_subtype == "savings" else "exclude" if target_type == "liability" else "auto",
                subtype=target_subtype,
                plaid_account_id=plaid_account_id,
            )
            db.session.add(account)
            db.session.flush()
            created = True
        else:
            account.plaid_account_id = plaid_account_id
    if not link:
        link = PlaidAccountLink(
            user_id=user_id,
            plaid_item_id=plaid_item.id,
            account_id=account.id,
            plaid_account_id=plaid_account_id,
        )
        db.session.add(link)

    account.balance = balance_value
    account.plaid_account_id = plaid_account_id
    if not account.subtype:
        account.subtype = target_subtype
    if account.type != target_type:
        account.type = target_type
    if created and not account.name:
        account.name = account_name

    balances = plaid_account.get("balances") or {}
    link.plaid_item_id = plaid_item.id
    link.account_id = account.id
    link.name = (plaid_account.get("name") or "")[:255]
    link.official_name = (plaid_account.get("official_name") or "")[:255]
    link.mask = (plaid_account.get("mask") or "")[:20]
    link.plaid_type = (plaid_account.get("type") or "")[:80]
    link.plaid_subtype = (plaid_account.get("subtype") or "")[:80]
    link.current_balance = safe_float(balances.get("current"))
    link.available_balance = safe_float(balances.get("available"))
    link.currency_code = ((balances.get("iso_currency_code") or "USD") or "USD")[:12]
    link.status = "active"
    link.updated_at = datetime.utcnow()
    return link, created, account


def plaid_connected_summary(user_id):
    items = PlaidItem.query.filter_by(user_id=user_id).order_by(PlaidItem.created_at.desc()).all()
    links = PlaidAccountLink.query.filter_by(user_id=user_id).all()
    account_map = {account.id: account for account in Account.query.filter_by(user_id=user_id).all()}
    item_map = {item.id: item for item in items}
    last_synced = max((item.last_synced_at for item in items if item.last_synced_at), default=None)
    institutions = [item.institution_name for item in items if (item.institution_name or "").strip()]
    linked_accounts = []
    for link in links:
        account = account_map.get(link.account_id)
        if not account:
            continue
        plaid_item = item_map.get(link.plaid_item_id)
        linked_accounts.append({
            "account_id": account.id,
            "account_name": account.name,
            "institution_name": (plaid_item.institution_name if plaid_item else "") or (link.official_name or link.name or "Connected bank"),
            "mask": (link.mask or "").strip(),
            "plaid_type": (link.plaid_type or "").strip(),
            "plaid_subtype": (link.plaid_subtype or "").strip(),
            "balance": round(float(account.balance or 0), 2),
            "status": (link.status or "active").strip().lower(),
        })
    linked_accounts.sort(key=lambda row: ((row.get("institution_name") or "").lower(), (row.get("account_name") or "").lower()))
    attention_items = []
    for item in items:
        status = (item.status or "active").strip().lower()
        needs_attention = status in {"error", "reconnect_required", "needs_update"}
        if not needs_attention:
            continue
        linked_count = sum(1 for link in links if link.plaid_item_id == item.id)
        if status == "needs_update":
            message = "New account access is available. Reconnect to review and add it."
            status_label = "Update Available"
        elif status == "reconnect_required":
            message = (item.last_sync_error or "").strip() or "This bank connection needs to be refreshed."
            status_label = "Reconnect Needed"
        else:
            message = (item.last_sync_error or "").strip() or "AkuOS needs you to reconnect this bank."
            status_label = "Needs Attention"
        attention_items.append({
            "id": item.id,
            "institution_name": (item.institution_name or "Connected bank").strip(),
            "status": status,
            "status_label": status_label,
            "message": message,
            "linked_account_count": linked_count,
            "last_synced_at": item.last_synced_at,
        })
    return {
        "enabled": plaid_is_configured(),
        "item_count": len(items),
        "account_count": len(links),
        "institutions": institutions[:4],
        "last_synced_at": last_synced,
        "linked_accounts": linked_accounts,
        "attention_items": attention_items,
        "needs_attention_count": len(attention_items),
    }


def plaid_access_token_value(plaid_item):
    return decrypt_sensitive_value(getattr(plaid_item, "access_token", ""))


def encrypt_existing_plaid_tokens_if_needed():
    if Fernet is None:
        return
    for plaid_item in PlaidItem.query.all():
        token_value = (plaid_item.access_token or "").strip()
        if token_value and not token_value.startswith("enc::"):
            plaid_item.access_token = encrypt_sensitive_value(token_value)
            plaid_item.updated_at = datetime.utcnow()


def plaid_link_token(user, plaid_item=None):
    from plaid.model.country_code import CountryCode
    from plaid.model.link_token_create_request import LinkTokenCreateRequest
    from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
    from plaid.model.products import Products

    request_kwargs = {
        "user": LinkTokenCreateRequestUser(client_user_id=str(user.id)),
        "client_name": "AkuOS",
        "products": [Products("transactions")],
        "country_codes": [CountryCode("US")],
        "language": "en",
    }
    if plaid_item:
        request_kwargs["access_token"] = plaid_access_token_value(plaid_item)
    if PLAID_REDIRECT_URI:
        request_kwargs["redirect_uri"] = PLAID_REDIRECT_URI
    if PLAID_WEBHOOK:
        request_kwargs["webhook"] = PLAID_WEBHOOK
    request_obj = LinkTokenCreateRequest(**request_kwargs)
    response = plaid_client().link_token_create(request_obj)
    return plaid_to_dict(response).get("link_token")


def plaid_exchange_public_token(public_token):
    from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest

    response = plaid_client().item_public_token_exchange(
        ItemPublicTokenExchangeRequest(public_token=public_token)
    )
    return plaid_to_dict(response)


def plaid_fetch_accounts(access_token):
    from plaid.model.accounts_get_request import AccountsGetRequest

    response = plaid_client().accounts_get(AccountsGetRequest(access_token=access_token))
    payload = plaid_to_dict(response)
    return payload.get("accounts") or []


def sync_plaid_item_transactions(plaid_item, user_id=None):
    from plaid.model.transactions_sync_request import TransactionsSyncRequest

    user_id = user_id or plaid_item.user_id
    access_token = plaid_access_token_value(plaid_item)
    synced_accounts = plaid_fetch_accounts(access_token)
    created_accounts = 0
    added_account_names = []
    skipped_account_names = []
    for plaid_account in synced_accounts:
        link, created, account = upsert_plaid_account_link(user_id, plaid_item, plaid_account)
        if not link or not account:
            continue
        if created:
            created_accounts += 1
            added_account_names.append((account.name or plaid_account_display_name(plaid_account) or "Linked account").strip())
        else:
            skipped_account_names.append((account.name or plaid_account_display_name(plaid_account) or "Linked account").strip())
    deduplicate_plaid_accounts_for_user(user_id)
    account_links_by_plaid_id = {
        link.plaid_account_id: link
        for link in PlaidAccountLink.query.filter_by(user_id=user_id).all()
        if (link.plaid_account_id or "").strip()
    }

    cursor = (plaid_item.sync_cursor or "").strip() or None
    added_count = 0
    modified_count = 0
    removed_count = 0
    has_more = True
    latest_cursor = cursor or ""
    recurring_index = build_recurring_index(
        Transaction.query
        .filter_by(user_id=user_id)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .limit(500)
        .all()
    )

    while has_more:
        request_kwargs = {"access_token": access_token}
        if latest_cursor:
            request_kwargs["cursor"] = latest_cursor
        response = plaid_client().transactions_sync(TransactionsSyncRequest(**request_kwargs))
        payload = plaid_to_dict(response)
        added = payload.get("added") or []
        modified = payload.get("modified") or []
        removed = payload.get("removed") or []
        latest_cursor = (payload.get("next_cursor") or latest_cursor or "").strip()
        has_more = bool(payload.get("has_more"))

        for removed_row in removed:
            plaid_transaction_id = (removed_row.get("transaction_id") or "").strip()
            if not plaid_transaction_id:
                continue
            removed_count += Transaction.query.filter_by(
                user_id=user_id,
                plaid_transaction_id=plaid_transaction_id,
                import_source="plaid",
            ).delete(synchronize_session=False)

        for source_rows, is_modified in ((added, False), (modified, True)):
            for plaid_tx in source_rows:
                if plaid_tx.get("pending"):
                    continue
                plaid_account_id = (plaid_tx.get("account_id") or "").strip()
                link = account_links_by_plaid_id.get(plaid_account_id) or PlaidAccountLink.query.filter_by(
                    user_id=user_id,
                    plaid_account_id=plaid_account_id,
                ).first()
                if not link:
                    continue
                account = Account.query.get(link.account_id)
                if not account or account.user_id != user_id:
                    continue
                tx_date = parse_date_any(plaid_tx.get("authorized_date")) or parse_date_any(plaid_tx.get("date"))
                if not tx_date:
                    continue
                plaid_transaction_id = (plaid_tx.get("transaction_id") or "").strip()
                if not plaid_transaction_id:
                    continue
                raw_description = (plaid_tx.get("merchant_name") or plaid_tx.get("name") or "").strip()
                display_name = preferred_display_name_for_user(
                    user_id,
                    raw_description,
                    fallback=clean_transaction_description(raw_description or (plaid_tx.get("name") or "")),
                )
                amount = plaid_amount_to_akuos(plaid_tx.get("amount"))
                categorization = categorize_transaction_detailed(
                    user_id,
                    raw_description or display_name,
                    amount,
                    tx_date=tx_date,
                    recurring_index=recurring_index,
                )
                category = (categorization.get("category") or "Needs Review").strip()
                subcategory = (categorization.get("subcategory") or "").strip()
                category_source = (categorization.get("category_source") or "Plaid Sync").strip()
                category_confidence = categorization.get("category_confidence") or "uncategorized"
                if normalize_rule_display_name((categorization or {}).get("rule_display_name", "")):
                    display_name = normalize_rule_display_name(categorization.get("rule_display_name"))
                applied_tags = normalize_rule_tags_value((categorization or {}).get("rule_tags", ""))
                tx_subtype = (categorization.get("transaction_subtype") or transaction_subtype_for(amount, category, category_source))
                fingerprint = transaction_fingerprint(
                    tx_date,
                    raw_description or display_name,
                    amount,
                    merchant_guess=derive_merchant_guess(raw_description or display_name),
                )
                transaction = Transaction.query.filter_by(
                    user_id=user_id,
                    plaid_transaction_id=plaid_transaction_id,
                ).first()
                if categorization.get("skip_transaction"):
                    if transaction:
                        db.session.delete(transaction)
                        removed_count += 1
                    continue
                if not transaction:
                    transaction = Transaction(
                        user_id=user_id,
                        account_id=account.id,
                        date=tx_date,
                        description=display_name,
                        raw_description=raw_description or display_name,
                        display_name=display_name,
                        normalized_description=derive_normalized_description(raw_description or display_name),
                        merchant_guess=derive_merchant_guess(raw_description or display_name),
                        amount=amount,
                        category=category,
                        subcategory=subcategory,
                        suggested_category_id=categorization.get("suggested_category_id"),
                        suggested_subcategory_id=categorization.get("suggested_subcategory_id"),
                        category_source=category_source,
                        category_confidence=category_confidence,
                        matched_rule_id=categorization.get("matched_rule_id"),
                        needs_review=category.lower() in GENERIC_CATEGORIES or category_confidence in {"error", "uncategorized", "low"},
                        transaction_subtype=tx_subtype,
                        import_source="plaid",
                        fingerprint=fingerprint,
                        plaid_transaction_id=plaid_transaction_id,
                        plaid_pending_transaction_id=(plaid_tx.get("pending_transaction_id") or "").strip() or None,
                        tags=applied_tags,
                    )
                    db.session.add(transaction)
                    added_count += 1
                else:
                    transaction.account_id = account.id
                    transaction.date = tx_date
                    transaction.description = display_name
                    transaction.raw_description = raw_description or display_name
                    transaction.display_name = display_name
                    transaction.normalized_description = derive_normalized_description(raw_description or display_name)
                    transaction.merchant_guess = derive_merchant_guess(raw_description or display_name)
                    transaction.amount = amount
                    transaction.category = category
                    transaction.subcategory = subcategory
                    transaction.suggested_category_id = categorization.get("suggested_category_id")
                    transaction.suggested_subcategory_id = categorization.get("suggested_subcategory_id")
                    transaction.category_source = category_source
                    transaction.category_confidence = category_confidence
                    transaction.matched_rule_id = categorization.get("matched_rule_id")
                    transaction.needs_review = category.lower() in GENERIC_CATEGORIES or category_confidence in {"error", "uncategorized", "low"}
                    transaction.transaction_subtype = tx_subtype
                    transaction.import_source = "plaid"
                    transaction.fingerprint = fingerprint
                    transaction.plaid_pending_transaction_id = (plaid_tx.get("pending_transaction_id") or "").strip() or None
                    transaction.tags = applied_tags
                    modified_count += 1 if is_modified else 0

    plaid_item.sync_cursor = latest_cursor or ""
    plaid_item.status = "active"
    plaid_item.last_sync_error = None
    plaid_item.last_synced_at = datetime.utcnow()
    plaid_item.updated_at = datetime.utcnow()

    return {
        "accounts_found": len(synced_accounts),
        "accounts_created": created_accounts,
        "accounts_skipped": max(len(skipped_account_names), 0),
        "accounts_added_names": sorted({name for name in added_account_names if name}, key=str.lower),
        "accounts_skipped_names": sorted({name for name in skipped_account_names if name}, key=str.lower),
        "accounts_linked": len(account_links_by_plaid_id),
        "transactions_added": added_count,
        "transactions_modified": modified_count,
        "transactions_removed": removed_count,
        "next_cursor": plaid_item.sync_cursor,
    }


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


def safe_local_redirect(target, fallback="/"):
    target = (target or fallback or "/").strip()
    if not target.startswith("/"):
        return fallback or "/"
    return target


def transaction_needs_attention(tx):
    if not tx:
        return False
    confidence_bucket = normalize_confidence_bucket(getattr(tx, "category_confidence", ""))
    if confidence_bucket in {"error", "uncategorized", "low"}:
        return True
    if getattr(tx, "needs_review", False):
        return True
    return canonical_transaction_category(getattr(tx, "category", "")) == "Needs Review"


def transaction_review_status_label(tx):
    confidence_bucket = normalize_confidence_bucket(getattr(tx, "category_confidence", ""))
    if confidence_bucket == "error":
        return "Error"
    if transaction_needs_attention(tx):
        return "Needs Attention"
    return "Reviewed"


def transaction_can_be_approved(tx, category_name=None, subcategory_name=""):
    if not tx:
        return False
    resolved_category, _resolved_subcategory = canonical_category_pair(
        category_name if category_name is not None else getattr(tx, "category", ""),
        subcategory_name if subcategory_name is not None else getattr(tx, "subcategory", ""),
    )
    if not resolved_category or resolved_category == "Needs Review":
        return False
    if not getattr(tx, "date", None):
        return False
    if getattr(tx, "amount", None) is None:
        return False
    return True


def transaction_review_reason_list(tx):
    if not tx:
        return []

    reasons = []
    confidence_bucket = normalize_confidence_bucket(getattr(tx, "category_confidence", ""))
    category_name = canonical_transaction_category(getattr(tx, "category", ""))
    subtype = (getattr(tx, "transaction_subtype", "") or transaction_subtype_for(getattr(tx, "amount", 0), category_name, getattr(tx, "category_source", ""))).strip().lower()
    raw_description = (transaction_raw_description(tx) or "").strip()
    display_name = (transaction_display_name(tx) or "").strip()
    reference_text = raw_description or display_name

    if not getattr(tx, "date", None):
        reasons.append("Invalid Date")

    amount = getattr(tx, "amount", None)
    if amount is None:
        reasons.append("Invalid Amount")

    if category_name == "Needs Review":
        reasons.append("No Category Match")

    if confidence_bucket == "low":
        reasons.append("Low Confidence")
    elif confidence_bucket == "error" and "Invalid Amount" not in reasons and "Invalid Date" not in reasons:
        reasons.append("Low Confidence")

    if not getattr(tx, "matched_rule_id", None) and category_name == "Needs Review":
        reasons.append("Unknown Merchant")
    elif not getattr(tx, "matched_rule_id", None) and confidence_bucket in {"low", "uncategorized"} and "Unknown Merchant" not in reasons:
        reasons.append("No Rule Match")

    noisy_description = False
    if reference_text:
        alpha_words = re.findall(r"[A-Za-z]{3,}", reference_text)
        noisy_description = len(reference_text) > 90 or len(alpha_words) >= 10
    if noisy_description:
        reasons.append("Description Too Noisy")

    if subtype == "transfer" and "Possible Transfer" not in reasons:
        reasons.append("Possible Transfer")
    if subtype == "payment":
        reasons.append("Possible Credit Card Payment")

    ordered_reasons = []
    seen = set()
    for reason in reasons:
        if reason not in seen:
            ordered_reasons.append(reason)
            seen.add(reason)
    return ordered_reasons


def review_reason_display_label(reason):
    value = (reason or "").strip()
    if not value:
        return ""
    replacements = {
        "Unknown Merchant": "Unknown merchant",
        "No Category Match": "No category match",
        "No Rule Match": "No learned rule yet",
        "Low Confidence": "Low confidence",
        "Multiple Possible Matches": "Multiple possible matches",
        "Possible Transfer": "Possible transfer",
        "Possible Credit Card Payment": "Possible credit card payment",
        "Duplicate Candidate": "Duplicate candidate",
        "Duplicate In File": "Duplicate in file",
        "Invalid Date": "Invalid date",
        "Invalid Amount": "Invalid amount",
        "Description Too Noisy": "Description too noisy",
        "Already Imported": "Already imported",
        "Skipped by Rule": "Skipped by rule",
    }
    return replacements.get(value, value)


def transaction_learning_note(tx):
    source_value = normalize_text(getattr(tx, "category_source", "") or "")
    if source_value in {"merchant history", "merchant memory", "merchant consistency"}:
        return "Matched previous decision"
    if source_value.startswith("rule"):
        return "Learned rule"
    return ""


def transaction_suggested_category_pair(tx, category_lookup=None):
    lookup = category_lookup or category_lookup_by_id()
    suggested_category = ""
    suggested_subcategory = ""
    if getattr(tx, "suggested_category_id", None):
        suggested_category = ((lookup.get(tx.suggested_category_id) or {}).get("name") or "").strip()
    if getattr(tx, "suggested_subcategory_id", None):
        suggested_subcategory = ((lookup.get(tx.suggested_subcategory_id) or {}).get("name") or "").strip()
    return canonical_category_pair(suggested_category, suggested_subcategory)


def resolved_learnable_category_pair(category_name, subcategory_name=""):
    resolved_category, resolved_subcategory = canonical_category_pair(category_name, subcategory_name)
    if not resolved_category or resolved_category.lower() in GENERIC_CATEGORIES:
        return "", ""
    return resolved_category, resolved_subcategory


def amount_direction_for_transaction_like(amount, subtype=""):
    subtype_value = (subtype or "").strip().lower()
    if subtype_value == "income":
        return "credit"
    if subtype_value in {"expense", "payment"}:
        return "debit"
    try:
        amount_value = float(amount or 0)
    except Exception:
        return "any"
    if amount_value > 0:
        return "credit"
    if amount_value < 0:
        return "debit"
    return "any"


def merchant_consistency_confidence(similarity_score, same_direction=True, same_account=False):
    if similarity_score >= 0.94:
        return 0.96
    if similarity_score >= 0.9 and same_direction:
        return 0.93 if same_account else 0.91
    if similarity_score >= 0.84 and same_direction:
        return 0.88
    if similarity_score >= 0.78:
        return 0.8
    return 0.0


def apply_manual_transaction_review(
    tx,
    user_id,
    category_name="",
    subcategory_name="",
    subtype="",
    review_status="reviewed",
    apply_to_similar=False,
):
    if not tx or tx.user_id != user_id:
        return {"ok": False, "similar_count": 0}

    resolved_category, resolved_subcategory = canonical_category_pair(
        category_name or getattr(tx, "category", ""),
        subcategory_name or getattr(tx, "subcategory", ""),
    )
    if not resolved_category:
        resolved_category = "Needs Review"
    resolved_subtype = (subtype or getattr(tx, "transaction_subtype", "") or "").strip().lower()
    if resolved_subtype not in VALID_TRANSACTION_SUBTYPES:
        resolved_subtype = transaction_subtype_for(tx.amount, resolved_category, "Manual Review", getattr(tx, "transaction_subtype", ""))

    tx.category = resolved_category
    tx.subcategory = resolved_subcategory
    tx.transaction_subtype = resolved_subtype
    tx.category_source = "Manual Review"
    tx.normalized_description = derive_normalized_description(transaction_reference_description(tx))
    tx.merchant_guess = derive_merchant_guess(transaction_reference_description(tx))
    tx.suggested_category_id, tx.suggested_subcategory_id = resolve_category_ids(resolved_category, resolved_subcategory)

    if (review_status or "").strip().lower() == "needs_attention" or resolved_category == "Needs Review":
        tx.needs_review = True
        tx.category_confidence = "uncategorized" if resolved_category == "Needs Review" else normalize_confidence_bucket(getattr(tx, "category_confidence", "")) or "low"
        similar_count = 0
    else:
        tx.needs_review = False
        tx.category_confidence = "high"
        remember_merchant_category(
            user_id,
            transaction_reference_description(tx),
            resolved_category,
            subcategory=resolved_subcategory,
            display_name=transaction_display_name(tx),
            subtype=resolved_subtype,
        )
        learned_rule = upsert_learned_category_rule(
            user_id,
            transaction_reference_description(tx),
            resolved_category,
            subcategory=resolved_subcategory,
            subtype=resolved_subtype,
            matched_rule_id=getattr(tx, "matched_rule_id", None),
        )
        tx.matched_rule_id = learned_rule.id if learned_rule else getattr(tx, "matched_rule_id", None)
        similar_count = apply_category_to_similar_transactions(
            source_tx=tx,
            user_id=user_id,
            category_name=resolved_category,
            subcategory_name=resolved_subcategory,
            subtype=resolved_subtype,
            matched_rule_id=tx.matched_rule_id,
            apply_to_similar=apply_to_similar,
        )
    return {"ok": True, "similar_count": similar_count}


def apply_category_to_similar_transactions(
    source_tx,
    user_id,
    category_name,
    subcategory_name="",
    subtype="",
    matched_rule_id=None,
    apply_to_similar=False,
):
    if not apply_to_similar or not source_tx or source_tx.user_id != user_id:
        return 0

    reference_keys = merchant_lookup_keys(transaction_reference_description(source_tx))
    if not reference_keys:
        return 0

    resolved_category, resolved_subcategory = resolved_learnable_category_pair(category_name, subcategory_name)
    if not resolved_category:
        return 0

    resolved_subtype = (subtype or getattr(source_tx, "transaction_subtype", "") or "").strip().lower()
    if resolved_subtype not in VALID_TRANSACTION_SUBTYPES:
        resolved_subtype = transaction_subtype_for(source_tx.amount, resolved_category, "Manual Review", getattr(source_tx, "transaction_subtype", ""))
    source_direction = amount_direction_for_transaction_like(getattr(source_tx, "amount", 0), resolved_subtype)

    candidate_transactions = Transaction.query.filter(
        Transaction.user_id == user_id,
        Transaction.id != source_tx.id,
    ).all()

    updated_count = 0
    for candidate in candidate_transactions:
        candidate_needs_consistency = (
            getattr(candidate, "needs_review", False)
            or canonical_transaction_category(getattr(candidate, "category", "")) == "Needs Review"
            or normalize_confidence_bucket(getattr(candidate, "category_confidence", "")) in {"low", "uncategorized", "medium"}
        )
        if not candidate_needs_consistency:
            continue

        candidate_keys = merchant_lookup_keys(transaction_reference_description(candidate))
        if not candidate_keys:
            continue
        similarity_score = max(
            merchant_match_strength(reference_key, candidate_key)
            for reference_key in reference_keys
            for candidate_key in candidate_keys
        )
        if similarity_score < 0.72:
            continue
        candidate_direction = amount_direction_for_transaction_like(
            getattr(candidate, "amount", 0),
            getattr(candidate, "transaction_subtype", ""),
        )
        same_direction = (
            source_direction == "any"
            or candidate_direction == "any"
            or candidate_direction == source_direction
        )
        consistency_confidence = merchant_consistency_confidence(
            similarity_score,
            same_direction=same_direction,
            same_account=getattr(candidate, "account_id", None) == getattr(source_tx, "account_id", None),
        )
        if consistency_confidence <= 0:
            continue

        candidate.category = resolved_category
        candidate.subcategory = resolved_subcategory
        candidate.transaction_subtype = resolved_subtype
        candidate.category_source = "Merchant Consistency"
        candidate.category_confidence = "high" if consistency_confidence >= 0.9 else "medium" if consistency_confidence >= 0.8 else "low"
        candidate.needs_review = consistency_confidence < 0.9
        candidate.normalized_description = derive_normalized_description(transaction_reference_description(candidate))
        candidate.merchant_guess = derive_merchant_guess(transaction_reference_description(candidate))
        candidate.suggested_category_id, candidate.suggested_subcategory_id = resolve_category_ids(resolved_category, resolved_subcategory)
        candidate.matched_rule_id = matched_rule_id or candidate.matched_rule_id
        updated_count += 1
    return updated_count


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
            "redirect_url": redirect_url if (redirect_url or "").startswith("/") else "/goals-wealth",
        }


def clear_allocation_undo():
    session.pop("_allocation_undo", None)


@app.context_processor
def inject_shared_ui_state():
    user_id = session.get("user_id")
    try:
        import_jobs = recent_import_jobs_for_user(user_id, limit=3) if user_id else []
    except Exception:
        import_jobs = []
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
        "csrf_token": get_or_create_csrf_token(),
        "tx_display_name": transaction_display_name,
        "tx_raw_description": transaction_raw_description,
        "tx_category_label": transaction_category_label,
        "canonical_category_name": canonical_transaction_category,
        "tx_type_label": transaction_type_label,
        "tx_learning_note": transaction_learning_note,
        "display_tag": display_tag,
        "review_reason_display_label": review_reason_display_label,
        "format_local_datetime": format_local_datetime,
        "transactions_filter_url": transactions_filter_url,
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
    PlaidAccountLink.query.filter_by(user_id=account.user_id, account_id=account.id).delete()
    Transaction.query.filter_by(account_id=account.id).delete()
    db.session.delete(account)


def delete_user_and_related_data(user):
    if not user:
        return
    delete_user_financial_data(user.id)
    db.session.delete(user)


def delete_user_financial_data(user_id):
    if not user_id:
        return {}

    delete_counts = {}

    goal_ids = []
    if model_table_exists(FinancialGoal):
        try:
            goal_ids = [
                goal_id
                for (goal_id,) in db.session.query(FinancialGoal.id).filter_by(user_id=user_id).all()
            ]
        except Exception as exc:
            app.logger.exception("Delete-all prefetch for financial goals failed for user_id=%s: %s", user_id, exc)
            raise
    else:
        app.logger.warning("Skipping goal prefetch for delete-all because financial_goal table is unavailable.")

    account_ids = []
    if model_table_exists(Account):
        try:
            account_ids = [
                account_id
                for (account_id,) in db.session.query(Account.id).filter_by(user_id=user_id).all()
            ]
        except Exception as exc:
            app.logger.exception("Delete-all prefetch for accounts failed for user_id=%s: %s", user_id, exc)
            raise
    else:
        app.logger.warning("Skipping account prefetch for delete-all because account table is unavailable.")

    deletion_plan = [
        ("transactions", Transaction, lambda: Transaction.query.filter_by(user_id=user_id)),
        ("upcoming_payments", UpcomingPayment, lambda: UpcomingPayment.query.filter_by(user_id=user_id)),
        ("import_jobs", ImportJob, lambda: ImportJob.query.filter_by(user_id=user_id)),
        ("import_batches", ImportBatch, lambda: ImportBatch.query.filter_by(user_id=user_id)),
        ("goal_allocations", GoalAllocation, lambda: GoalAllocation.query.filter(or_(
            GoalAllocation.goal_id.in_(goal_ids) if goal_ids else GoalAllocation.goal_id == -1,
            GoalAllocation.account_id.in_(account_ids) if account_ids else GoalAllocation.account_id == -1,
        ))),
        ("budgets", Budget, lambda: Budget.query.filter_by(user_id=user_id)),
        ("debts", Debt, lambda: Debt.query.filter_by(user_id=user_id)),
        ("rules", CategoryRule, lambda: CategoryRule.query.filter_by(user_id=user_id)),
        ("merchant_memory", MerchantMemory, lambda: MerchantMemory.query.filter_by(user_id=user_id)),
        ("plaid_account_links", PlaidAccountLink, lambda: PlaidAccountLink.query.filter_by(user_id=user_id)),
        ("plaid_items", PlaidItem, lambda: PlaidItem.query.filter_by(user_id=user_id)),
        ("activity_log", ActivityLog, lambda: ActivityLog.query.filter_by(user_id=user_id)),
    ]

    for label, model, query_factory in deletion_plan:
        safe_user_delete_step(user_id, label, model, query_factory, delete_counts, optional=True)

    if model_table_exists(FinancialGoal):
        try:
            updated_goals = FinancialGoal.query.filter_by(user_id=user_id).update({
                FinancialGoal.linked_account_id: None,
                FinancialGoal.allocated_amount: 0,
            }, synchronize_session=False)
            delete_counts["financial_goal_links_cleared"] = updated_goals
            app.logger.info(
                "Delete-all step 'financial_goal_links_cleared' updated %s row(s) for user_id=%s.",
                updated_goals,
                user_id,
            )
        except Exception as exc:
            app.logger.exception(
                "Delete-all step 'financial_goal_links_cleared' failed for user_id=%s: %s",
                user_id,
                exc,
            )
            raise
    else:
        delete_counts["financial_goal_links_cleared"] = 0
        app.logger.warning("Skipping financial goal unlink step for delete-all because financial_goal table is unavailable.")

    safe_user_delete_step(
        user_id,
        "financial_goals",
        FinancialGoal,
        lambda: FinancialGoal.query.filter_by(user_id=user_id),
        delete_counts,
        optional=True,
    )
    safe_user_delete_step(
        user_id,
        "accounts",
        Account,
        lambda: Account.query.filter_by(user_id=user_id),
        delete_counts,
        optional=False,
    )

    db.session.flush()
    return delete_counts


def verify_user_financial_data_cleared(user_id):
    verification = {}
    verification_models = [
        ("accounts", Account),
        ("transactions", Transaction),
        ("budgets", Budget),
        ("rules", CategoryRule),
        ("merchant_memory", MerchantMemory),
        ("financial_goals", FinancialGoal),
        ("upcoming_payments", UpcomingPayment),
        ("plaid_items", PlaidItem),
        ("plaid_account_links", PlaidAccountLink),
        ("import_jobs", ImportJob),
        ("import_batches", ImportBatch),
        ("debts", Debt),
        ("activity_log", ActivityLog),
    ]

    for label, model in verification_models:
        if not model_table_exists(model):
            verification[label] = 0
            app.logger.warning(
                "Skipping delete-all verification for '%s' because table '%s' is unavailable.",
                label,
                model_table_name(model),
            )
            continue
        try:
            verification[label] = model.query.filter_by(user_id=user_id).count()
        except Exception as exc:
            app.logger.exception(
                "Delete-all verification failed for '%s' and user_id=%s: %s",
                label,
                user_id,
                exc,
            )
            raise

    if model_table_exists(GoalAllocation) and model_table_exists(FinancialGoal) and model_table_exists(Account):
        try:
            remaining_goal_ids = [
                goal_id
                for (goal_id,) in db.session.query(FinancialGoal.id).filter_by(user_id=user_id).all()
            ]
            remaining_account_ids = [
                account_id
                for (account_id,) in db.session.query(Account.id).filter_by(user_id=user_id).all()
            ]
            verification["goal_allocations"] = GoalAllocation.query.filter(or_(
                GoalAllocation.goal_id.in_(remaining_goal_ids) if remaining_goal_ids else GoalAllocation.goal_id == -1,
                GoalAllocation.account_id.in_(remaining_account_ids) if remaining_account_ids else GoalAllocation.account_id == -1,
            )).count()
        except Exception as exc:
            app.logger.exception(
                "Delete-all verification failed for goal allocations and user_id=%s: %s",
                user_id,
                exc,
            )
            raise
    else:
        verification["goal_allocations"] = 0

    return verification


def merge_account_references(source_account, target_account):
    if not source_account or not target_account or source_account.id == target_account.id:
        return
    user_id = target_account.user_id
    Transaction.query.filter_by(account_id=source_account.id).update({
        Transaction.account_id: target_account.id,
    }, synchronize_session=False)
    ImportBatch.query.filter_by(user_id=user_id, account_id=source_account.id).update({
        ImportBatch.account_id: target_account.id,
    }, synchronize_session=False)
    ImportJob.query.filter_by(user_id=user_id, account_id=source_account.id).update({
        ImportJob.account_id: target_account.id,
    }, synchronize_session=False)
    FinancialGoal.query.filter_by(user_id=user_id, linked_account_id=source_account.id).update({
        FinancialGoal.linked_account_id: target_account.id,
    }, synchronize_session=False)
    for allocation in GoalAllocation.query.filter_by(account_id=source_account.id).all():
        existing = GoalAllocation.query.filter_by(goal_id=allocation.goal_id, account_id=target_account.id).first()
        if existing:
            existing.allocated_amount = round(float(existing.allocated_amount or 0) + float(allocation.allocated_amount or 0), 2)
            db.session.delete(allocation)
        else:
            allocation.account_id = target_account.id
    PlaidAccountLink.query.filter_by(user_id=user_id, account_id=source_account.id).update({
        PlaidAccountLink.account_id: target_account.id,
    }, synchronize_session=False)
    if source_account.plaid_account_id and not target_account.plaid_account_id:
        target_account.plaid_account_id = source_account.plaid_account_id
    db.session.delete(source_account)


def deduplicate_plaid_accounts_for_user(user_id):
    links = PlaidAccountLink.query.filter_by(user_id=user_id).order_by(PlaidAccountLink.id.asc()).all()
    grouped_links = defaultdict(list)
    for link in links:
        plaid_account_id = (link.plaid_account_id or "").strip()
        if plaid_account_id:
            grouped_links[plaid_account_id].append(link)
    for account in Account.query.filter_by(user_id=user_id).order_by(Account.id.asc()).all():
        plaid_account_id = (account.plaid_account_id or "").strip()
        if plaid_account_id and plaid_account_id not in grouped_links:
            grouped_links[plaid_account_id] = []

    for plaid_account_id, group_links in grouped_links.items():
        linked_accounts = []
        seen_account_ids = set()
        for link in group_links:
            account = Account.query.get(link.account_id)
            if account and account.user_id == user_id and account.id not in seen_account_ids:
                linked_accounts.append(account)
                seen_account_ids.add(account.id)
        linked_accounts.extend(
            account for account in Account.query.filter_by(user_id=user_id, plaid_account_id=plaid_account_id).order_by(Account.id.asc()).all()
            if account.id not in seen_account_ids
        )
        if not linked_accounts:
            continue

        canonical_account = sorted(linked_accounts, key=lambda account: (
            -Transaction.query.filter_by(account_id=account.id).count(),
            account.id,
        ))[0]
        canonical_account.plaid_account_id = plaid_account_id
        for account in linked_accounts:
            if account.id != canonical_account.id:
                merge_account_references(account, canonical_account)

        refreshed_links = PlaidAccountLink.query.filter_by(user_id=user_id, plaid_account_id=plaid_account_id).order_by(
            PlaidAccountLink.updated_at.desc(),
            PlaidAccountLink.id.asc(),
        ).all()
        if not refreshed_links:
            continue
        canonical_link = refreshed_links[0]
        canonical_link.account_id = canonical_account.id
        latest_link = refreshed_links[0]
        canonical_link.plaid_item_id = latest_link.plaid_item_id
        canonical_link.name = latest_link.name
        canonical_link.official_name = latest_link.official_name
        canonical_link.mask = latest_link.mask
        canonical_link.plaid_type = latest_link.plaid_type
        canonical_link.plaid_subtype = latest_link.plaid_subtype
        canonical_link.current_balance = latest_link.current_balance
        canonical_link.available_balance = latest_link.available_balance
        canonical_link.currency_code = latest_link.currency_code
        canonical_link.status = latest_link.status
        canonical_link.updated_at = datetime.utcnow()
        for extra_link in refreshed_links[1:]:
            if extra_link.account_id != canonical_account.id:
                extra_link.account_id = canonical_account.id
            db.session.delete(extra_link)


def deduplicate_plaid_items_for_user(user_id):
    items = PlaidItem.query.filter_by(user_id=user_id).order_by(PlaidItem.updated_at.desc(), PlaidItem.id.desc()).all()
    groups = defaultdict(list)
    for item in items:
        institution_id = (item.institution_id or "").strip().lower()
        institution_name = normalize_text(item.institution_name)
        dedupe_key = institution_id or institution_name
        if dedupe_key:
            groups[dedupe_key].append(item)

    for item_group in groups.values():
        if len(item_group) < 2:
            continue
        canonical_item = item_group[0]
        for extra_item in item_group[1:]:
            PlaidAccountLink.query.filter_by(user_id=user_id, plaid_item_id=extra_item.id).update({
                PlaidAccountLink.plaid_item_id: canonical_item.id,
            }, synchronize_session=False)
            db.session.delete(extra_item)


def deduplicate_all_plaid_connections():
    user_ids = {
        user_id for (user_id,) in db.session.query(PlaidAccountLink.user_id).distinct().all() if user_id
    }
    user_ids.update(
        user_id for (user_id,) in db.session.query(Account.user_id).filter(Account.plaid_account_id.isnot(None)).distinct().all() if user_id
    )
    user_ids.update(
        user_id for (user_id,) in db.session.query(PlaidItem.user_id).distinct().all() if user_id
    )
    for user_id in sorted(user_ids):
        deduplicate_plaid_accounts_for_user(user_id)
        deduplicate_plaid_items_for_user(user_id)


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
    recurring_index = build_recurring_index(transactions)
    for tx in transactions or []:
        current_category, current_subcategory = canonical_category_pair(
            (tx.category or "").strip() or "Needs Review",
            (getattr(tx, "subcategory", "") or "").strip(),
        )
        normalized_current = current_category.lower()
        persisted_source = (getattr(tx, "category_source", "") or "").strip()
        persisted_confidence = normalize_confidence_bucket(getattr(tx, "category_confidence", ""))
        suggestion = categorize_transaction_detailed(
            user_id,
            transaction_reference_description(tx),
            float(tx.amount or 0),
            tx_date=getattr(tx, "date", None),
            recurring_index=recurring_index,
        )
        suggested_category = (suggestion.get("category") or "").strip() or "Needs Review"
        suggested_subcategory = (suggestion.get("subcategory") or "").strip()
        suggested_source = suggestion.get("category_source") or "Needs Review"

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
            "current_subcategory": current_subcategory,
            "current_category_label": transaction_category_label(current_category, current_subcategory),
            "suggested_category": suggested_category,
            "suggested_subcategory": suggested_subcategory,
            "suggested_category_label": category_label(suggested_category, suggested_subcategory),
            "suggested_source": persisted_source or suggested_source,
            "show_suggestion": (suggested_category, suggested_subcategory) != (current_category, current_subcategory),
            "is_uncategorized": is_uncategorized,
            "is_low_confidence": is_low_confidence,
            "confidence_label": confidence_label,
            "confidence_tone": confidence_tone,
            "confidence_detail": confidence_detail,
            "amount_display": f"${abs(amount_value):,.2f}",
        })

    return rows


def ensure_db_schema(force=False):
    if app.config.get("_SCHEMA_READY") and not force:
        return
    with DB_INIT_LOCK:
        if app.config.get("_SCHEMA_READY") and not force:
            return

        db.create_all()
        UpcomingPayment.__table__.create(bind=db.engine, checkfirst=True)
        inspector = inspect(db.engine)
        if "account" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("account")}
            with db.engine.begin() as conn:
                if "savings_preference" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE account ADD COLUMN savings_preference VARCHAR(20) NOT NULL DEFAULT 'auto'")
                if "subtype" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE account ADD COLUMN subtype VARCHAR(40) NOT NULL DEFAULT ''")
                if "plaid_account_id" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE account ADD COLUMN plaid_account_id VARCHAR(120)")
        if "user" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("user")}
            with db.engine.begin() as conn:
                if "is_admin" not in columns:
                    safe_schema_alter(conn, f'ALTER TABLE "user" ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT {sql_boolean_literal(False)}')
                if "created_at" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "user" ADD COLUMN created_at TIMESTAMP')
                if "last_login_at" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "user" ADD COLUMN last_login_at TIMESTAMP')
                if "reset_token" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "user" ADD COLUMN reset_token VARCHAR(120)')
                if "reset_token_expires_at" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "user" ADD COLUMN reset_token_expires_at TIMESTAMP')
        if "category_rule" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("category_rule")}
            with db.engine.begin() as conn:
                if "priority" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN priority INTEGER NOT NULL DEFAULT 100")
                if "match_type" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN match_type VARCHAR(20) NOT NULL DEFAULT 'contains'")
                if "amount_direction" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN amount_direction VARCHAR(20) NOT NULL DEFAULT 'any'")
                if "rule_type" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN rule_type VARCHAR(20) NOT NULL DEFAULT 'contains'")
                if "pattern" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN pattern VARCHAR(255) NOT NULL DEFAULT ''")
                if "category_id" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN category_id INTEGER")
                if "subcategory_id" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN subcategory_id INTEGER")
                if "confidence" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN confidence FLOAT NOT NULL DEFAULT 0.8")
                if "is_system_rule" not in columns:
                    safe_schema_alter(conn, f"ALTER TABLE category_rule ADD COLUMN is_system_rule BOOLEAN NOT NULL DEFAULT {sql_boolean_literal(False)}")
                if "is_active" not in columns:
                    safe_schema_alter(conn, f"ALTER TABLE category_rule ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT {sql_boolean_literal(True)}")
                if "subtype" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN subtype VARCHAR(20) NOT NULL DEFAULT ''")
                if "display_name_override" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN display_name_override VARCHAR(255) NOT NULL DEFAULT ''")
                if "tag_rules" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE category_rule ADD COLUMN tag_rules VARCHAR(255) NOT NULL DEFAULT ''")
                if "skip_transaction" not in columns:
                    safe_schema_alter(conn, f"ALTER TABLE category_rule ADD COLUMN skip_transaction BOOLEAN NOT NULL DEFAULT {sql_boolean_literal(False)}")
        if "transaction" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("transaction")}
            with db.engine.begin() as conn:
                if "tags" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN tags VARCHAR(255) NOT NULL DEFAULT \'\'')
                if "import_batch_id" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN import_batch_id VARCHAR(32)')
                if "raw_description" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN raw_description VARCHAR(255) NOT NULL DEFAULT \'\'')
                if "display_name" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN display_name VARCHAR(255) NOT NULL DEFAULT \'\'')
                if "normalized_description" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN normalized_description VARCHAR(255) NOT NULL DEFAULT \'\'')
                if "merchant_guess" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN merchant_guess VARCHAR(255) NOT NULL DEFAULT \'\'')
                if "category_source" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN category_source VARCHAR(80) NOT NULL DEFAULT \'\'')
                if "category_confidence" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN category_confidence VARCHAR(20) NOT NULL DEFAULT \'\'')
                if "subcategory" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN subcategory VARCHAR(100) NOT NULL DEFAULT \'\'')
                if "suggested_category_id" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN suggested_category_id INTEGER')
                if "suggested_subcategory_id" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN suggested_subcategory_id INTEGER')
                if "matched_rule_id" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN matched_rule_id INTEGER')
                if "needs_review" not in columns:
                    safe_schema_alter(conn, f'ALTER TABLE "transaction" ADD COLUMN needs_review BOOLEAN NOT NULL DEFAULT {sql_boolean_literal(False)}')
                if "transaction_subtype" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN transaction_subtype VARCHAR(20) NOT NULL DEFAULT \'\'')
                if "import_source" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN import_source VARCHAR(20) NOT NULL DEFAULT \'\'')
                if "fingerprint" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN fingerprint VARCHAR(255) NOT NULL DEFAULT \'\'')
                if "plaid_transaction_id" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN plaid_transaction_id VARCHAR(120)')
                if "plaid_pending_transaction_id" not in columns:
                    safe_schema_alter(conn, 'ALTER TABLE "transaction" ADD COLUMN plaid_pending_transaction_id VARCHAR(120)')
        if "import_batch" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("import_batch")}
            with db.engine.begin() as conn:
                if "duplicate_candidate_count" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_batch ADD COLUMN duplicate_candidate_count INTEGER NOT NULL DEFAULT 0")
                if "start_date" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_batch ADD COLUMN start_date DATE")
                if "end_date" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_batch ADD COLUMN end_date DATE")
        if "merchant_memory" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("merchant_memory")}
            with db.engine.begin() as conn:
                if "subcategory" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE merchant_memory ADD COLUMN subcategory VARCHAR(100) NOT NULL DEFAULT ''")
                if "display_name" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE merchant_memory ADD COLUMN display_name VARCHAR(255) NOT NULL DEFAULT ''")
                if "subtype" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE merchant_memory ADD COLUMN subtype VARCHAR(20) NOT NULL DEFAULT ''")
                if "is_disabled" not in columns:
                    safe_schema_alter(conn, f"ALTER TABLE merchant_memory ADD COLUMN is_disabled BOOLEAN NOT NULL DEFAULT {sql_boolean_literal(False)}")
        if "import_job" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("import_job")}
            with db.engine.begin() as conn:
                if "current_stage" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN current_stage VARCHAR(40) NOT NULL DEFAULT 'uploaded'")
                if "progress_percent" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN progress_percent INTEGER NOT NULL DEFAULT 5")
                if "balance_mode" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN balance_mode VARCHAR(20) NOT NULL DEFAULT 'add'")
                if "source_files" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN source_files TEXT NOT NULL DEFAULT '[]'")
                if "file_count" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN file_count INTEGER NOT NULL DEFAULT 0")
                if "preview_id" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN preview_id VARCHAR(64)")
                if "review_payload_json" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN review_payload_json TEXT NOT NULL DEFAULT '{}'")
                if "summary_json" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN summary_json TEXT NOT NULL DEFAULT '{}'")
                if "error_message" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN error_message VARCHAR(255)")
                if "start_date" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN start_date DATE")
                if "end_date" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN end_date DATE")
                if "started_at" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN started_at TIMESTAMP")
                if "completed_at" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE import_job ADD COLUMN completed_at TIMESTAMP")
        if "financial_goal" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("financial_goal")}
            with db.engine.begin() as conn:
                if "linked_account_id" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE financial_goal ADD COLUMN linked_account_id INTEGER")
                if "allocated_amount" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE financial_goal ADD COLUMN allocated_amount FLOAT NOT NULL DEFAULT 0")
        if "goal_allocation" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("goal_allocation")}
            with db.engine.begin() as conn:
                if "allocated_amount" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE goal_allocation ADD COLUMN allocated_amount FLOAT NOT NULL DEFAULT 0")
        if "upcoming_payment" in inspector.get_table_names():
            columns = {col["name"] for col in inspector.get_columns("upcoming_payment")}
            with db.engine.begin() as conn:
                if "account_id" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE upcoming_payment ADD COLUMN account_id INTEGER")
                if "category" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE upcoming_payment ADD COLUMN category VARCHAR(100) NOT NULL DEFAULT ''")
                if "is_recurring" not in columns:
                    safe_schema_alter(conn, f"ALTER TABLE upcoming_payment ADD COLUMN is_recurring BOOLEAN NOT NULL DEFAULT {sql_boolean_literal(False)}")
                if "frequency" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE upcoming_payment ADD COLUMN frequency VARCHAR(40) NOT NULL DEFAULT 'Monthly'")
                if "is_active" not in columns:
                    safe_schema_alter(conn, f"ALTER TABLE upcoming_payment ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT {sql_boolean_literal(True)}")
                if "created_at" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE upcoming_payment ADD COLUMN created_at TIMESTAMP")
                if "updated_at" not in columns:
                    safe_schema_alter(conn, "ALTER TABLE upcoming_payment ADD COLUMN updated_at TIMESTAMP")

        with db.engine.begin() as conn:
            if DATABASE_URI.startswith("sqlite"):
                conn.execute(text("PRAGMA journal_mode=WAL"))
                conn.execute(text("PRAGMA synchronous=NORMAL"))
            conn.execute(text('UPDATE "user" SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL'))
            conn.execute(text(f'UPDATE "user" SET is_admin = {sql_boolean_literal(True)} WHERE id = (SELECT id FROM "user" ORDER BY id ASC LIMIT 1) AND NOT EXISTS (SELECT 1 FROM "user" WHERE is_admin = {sql_boolean_literal(True)})'))
            conn.execute(text('UPDATE "transaction" SET raw_description = description WHERE COALESCE(raw_description, \'\') = \'\''))
            conn.execute(text('UPDATE "transaction" SET display_name = description WHERE COALESCE(display_name, \'\') = \'\''))
            conn.execute(text('UPDATE "transaction" SET normalized_description = display_name WHERE COALESCE(normalized_description, \'\') = \'\''))
            conn.execute(text('UPDATE "transaction" SET merchant_guess = display_name WHERE COALESCE(merchant_guess, \'\') = \'\''))
            conn.execute(text('UPDATE "transaction" SET category_source = COALESCE(category_source, \'\')'))
            conn.execute(text('UPDATE "transaction" SET category_confidence = COALESCE(category_confidence, \'\')'))
            conn.execute(text('UPDATE "transaction" SET subcategory = COALESCE(subcategory, \'\')'))
            conn.execute(text(f'UPDATE "transaction" SET needs_review = {sql_boolean_literal(False)} WHERE needs_review IS NULL'))
            conn.execute(text('UPDATE "transaction" SET transaction_subtype = CASE WHEN COALESCE(transaction_subtype, \'\') <> \'\' THEN transaction_subtype WHEN amount > 0 THEN \'income\' WHEN LOWER(COALESCE(category, \'\')) IN (\'transfer\', \'transfer / payment\') THEN \'transfer\' WHEN LOWER(COALESCE(category, \'\')) = \'credit card payment\' THEN \'payment\' WHEN amount < 0 THEN \'expense\' ELSE \'neutral\' END'))
            conn.execute(text("UPDATE \"transaction\" SET import_source = 'rule_based' WHERE COALESCE(import_source, '') = ''"))
            conn.execute(text('UPDATE "transaction" SET fingerprint = \'\' WHERE fingerprint IS NULL'))
            conn.execute(text('UPDATE "transaction" SET plaid_transaction_id = NULL WHERE COALESCE(plaid_transaction_id, \'\') = \'\''))
            conn.execute(text('UPDATE "transaction" SET plaid_pending_transaction_id = NULL WHERE COALESCE(plaid_pending_transaction_id, \'\') = \'\''))
            conn.execute(text("UPDATE category_rule SET pattern = keyword WHERE COALESCE(pattern, '') = ''"))
            conn.execute(text("UPDATE category_rule SET rule_type = COALESCE(rule_type, match_type, 'contains')"))
            conn.execute(text(f"UPDATE category_rule SET is_system_rule = {sql_boolean_literal(False)} WHERE is_system_rule IS NULL"))
            conn.execute(text(f"UPDATE category_rule SET is_active = {sql_boolean_literal(True)} WHERE is_active IS NULL"))
            conn.execute(text("UPDATE category_rule SET subtype = '' WHERE subtype IS NULL"))
            conn.execute(text("UPDATE merchant_memory SET subcategory = '' WHERE subcategory IS NULL"))
            conn.execute(text("UPDATE merchant_memory SET display_name = '' WHERE display_name IS NULL"))
            conn.execute(text("UPDATE merchant_memory SET subtype = '' WHERE subtype IS NULL"))
            conn.execute(text(f"UPDATE merchant_memory SET is_disabled = {sql_boolean_literal(False)} WHERE is_disabled IS NULL"))
            if "upcoming_payment" in inspector.get_table_names():
                conn.execute(text("UPDATE upcoming_payment SET category = '' WHERE category IS NULL"))
                conn.execute(text("UPDATE upcoming_payment SET frequency = 'Monthly' WHERE COALESCE(frequency, '') = ''"))
                conn.execute(text(f"UPDATE upcoming_payment SET is_recurring = {sql_boolean_literal(False)} WHERE is_recurring IS NULL"))
                conn.execute(text(f"UPDATE upcoming_payment SET is_active = {sql_boolean_literal(True)} WHERE is_active IS NULL"))
                conn.execute(text("UPDATE upcoming_payment SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"))
                conn.execute(text("UPDATE upcoming_payment SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"))
            conn.execute(text("""
                UPDATE account
                SET plaid_account_id = (
                    SELECT pal.plaid_account_id
                    FROM plaid_account_link pal
                    WHERE pal.account_id = account.id
                      AND COALESCE(pal.plaid_account_id, '') <> ''
                    ORDER BY pal.id ASC
                    LIMIT 1
                )
                WHERE COALESCE(account.plaid_account_id, '') = ''
            """))
            conn.execute(text("UPDATE account SET plaid_account_id = NULL WHERE COALESCE(plaid_account_id, '') = ''"))
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

        with db.engine.begin() as conn:
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_date ON "transaction" (user_id, date)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_account ON "transaction" (user_id, account_id)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_account_fingerprint ON "transaction" (account_id, fingerprint)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_category ON "transaction" (user_id, category)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_review ON "transaction" (user_id, needs_review)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_subtype ON "transaction" (user_id, transaction_subtype)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_user_confidence ON "transaction" (user_id, category_confidence)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_import_batch ON "transaction" (import_batch_id)'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_transaction_plaid_tx ON "transaction" (plaid_transaction_id)'))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_activity_log_user_created_at ON activity_log (user_id, created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_import_batch_user_created_at ON import_batch (user_id, created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_import_job_user_created_at ON import_job (user_id, created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_import_job_user_status ON import_job (user_id, status)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_merchant_memory_user_merchant ON merchant_memory (user_id, merchant)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_category_rule_user_active ON category_rule (user_id, is_active)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_category_slug ON category (slug)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_financial_goal_user_account ON financial_goal (user_id, linked_account_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_goal_allocation_goal ON goal_allocation (goal_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_goal_allocation_account ON goal_allocation (account_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_upcoming_payment_user ON upcoming_payment (user_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_upcoming_payment_due_date ON upcoming_payment (due_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_upcoming_payment_is_active ON upcoming_payment (is_active)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_upcoming_payment_user_due_date ON upcoming_payment (user_id, due_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_upcoming_payment_user_active ON upcoming_payment (user_id, is_active)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_plaid_item_user_item ON plaid_item (user_id, item_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_plaid_account_link_user_account ON plaid_account_link (user_id, account_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_plaid_account_link_plaid_account ON plaid_account_link (plaid_account_id)"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_account_user_plaid_account ON account (user_id, plaid_account_id) WHERE plaid_account_id IS NOT NULL AND plaid_account_id <> ''"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_plaid_account_link_user_plaid_account ON plaid_account_link (user_id, plaid_account_id) WHERE plaid_account_id IS NOT NULL AND plaid_account_id <> ''"))

        seed_default_categories()
        db.session.flush()
        seed_default_category_rules()
        encrypt_existing_plaid_tokens_if_needed()
        db.session.commit()

        app.config["_SCHEMA_READY"] = True


def initialize_schema_once():
    if app.config.get("_SCHEMA_READY") or app.config.get("_SCHEMA_INIT_ATTEMPTED"):
        return
    app.config["_SCHEMA_INIT_ATTEMPTED"] = True
    try:
        ensure_db_schema()
    except Exception as exc:
        app.config["_SCHEMA_INIT_ATTEMPTED"] = False
        log_safe_exception("Schema initialization failed during maintenance.", exc=exc)
        raise


def run_plaid_deduplication_maintenance():
    with DB_INIT_LOCK:
        deduplicate_all_plaid_connections()
        db.session.commit()


@app.route("/admin/run-maintenance", methods=["POST"])
def run_manual_maintenance():
    if not require_login():
        if request_wants_json():
            return jsonify({"ok": False, "error": "Authentication required."}), 401
        return "Authentication required.", 401
    if not require_admin():
        if request_wants_json():
            return jsonify({"ok": False, "error": "Admin access required."}), 403
        return "Admin access required.", 403

    rate_limited = rate_limit_response(
        "manual_maintenance",
        limit=3,
        window_seconds=3600,
        html_fallback="/settings",
        message="Maintenance was run recently. Please wait before trying again.",
    )
    if rate_limited:
        return rate_limited

    try:
        with timed_route_section("run_manual_maintenance", "ensure_db_schema", warning_ms=500):
            ensure_db_schema(force=True)
        with timed_route_section("run_manual_maintenance", "deduplicate_plaid", warning_ms=500):
            run_plaid_deduplication_maintenance()
    except Exception as exc:
        db.session.rollback()
        log_safe_exception("Manual maintenance failed.", exc=exc)
        message = "Maintenance failed. Check server logs for details."
        if request_wants_json():
            return jsonify({"ok": False, "error": message}), 500
        return message, 500

    message = "Maintenance completed successfully."
    if request_wants_json():
        return jsonify({"ok": True, "message": message})
    return message


@app.before_request
def start_request_timer():
    g._request_started_at = time.perf_counter()


@app.before_request
def prepare_request_context():
    if "user_id" in session:
        if not User.query.get(session.get("user_id")):
            csrf_token = session.get("_csrf_token")
            session.clear()
            if csrf_token:
                session["_csrf_token"] = csrf_token
        else:
            session.permanent = True
    csrf_result = validate_csrf_request()
    if csrf_result:
        return csrf_result


@app.after_request
def log_slow_request(response):
    started_at = getattr(g, "_request_started_at", None)
    if started_at is None:
        return response
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 1)
    if elapsed_ms >= SLOW_REQUEST_WARNING_MS:
        app.logger.warning(
            "Slow request method=%s path=%s endpoint=%s status=%s elapsed_ms=%.1f",
            request.method,
            request.path,
            request.endpoint,
            response.status_code,
            elapsed_ms,
        )
    return response

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
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, date):
        return s
    s = str(s or "").strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%m-%d-%y",
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        pass
    try:
        parts = s.replace("-", "/").split("/")
        if len(parts) == 3:
            m, d, y = parts
            y = int(y)
            if y < 100:
                y += 2000
            return date(int(y), int(m), int(d))
    except Exception:
        return None
    return None

def auto_category_for_user(user_id, description, amount):
    return auto_categorize(user_id, description, amount)


def sorted_user_rules(user_id):
    rules = (
        CategoryRule.query
        .filter(
            or_(
                CategoryRule.user_id == user_id,
                CategoryRule.is_system_rule == True,  # noqa: E712
            )
        )
        .filter_by(is_active=True)
        .all()
    )
    categories = category_lookup_by_id()
    for rule in rules:
        if getattr(rule, "category_id", None) and rule.category_id in categories:
            rule.category_name = categories[rule.category_id]["name"]
        else:
            rule.category_name = canonical_transaction_category(getattr(rule, "category", ""))
        if getattr(rule, "subcategory_id", None) and rule.subcategory_id in categories:
            rule.subcategory_name = categories[rule.subcategory_id]["name"]
        else:
            rule.subcategory_name = ""
        actions = transaction_rule_actions(rule)
        rule.display_name_override_value = actions["display_name_override"]
        rule.tag_rules_value = actions["tag_rules"]
        rule.skip_transaction_value = actions["skip_transaction"]
    return sorted(
        rules,
        key=lambda rule: (
            int(getattr(rule, "priority", 100) or 100),
            len((getattr(rule, "pattern", "") or getattr(rule, "keyword", "") or "").strip()),
        ),
        reverse=True,
    )


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
                "subcategory": (getattr(tx, "subcategory", "") or "").strip(),
                "display_name": transaction_display_name(tx),
                "subtype": (getattr(tx, "transaction_subtype", "") or "").strip(),
            }

    for merchant, payload in learned.items():
        remember_merchant_category(
            user_id,
            merchant,
            payload["category"],
            subcategory=payload.get("subcategory"),
            display_name=payload.get("display_name"),
            subtype=payload.get("subtype"),
        )


def active_merchant_memories_for_user(user_id):
    return MerchantMemory.query.filter_by(user_id=user_id, is_disabled=False).all()


def find_best_merchant_memory(user_id, description, memories=None):
    lookup_keys = merchant_lookup_keys(description)
    if not lookup_keys:
        return None

    memories = memories if memories is not None else active_merchant_memories_for_user(user_id)
    best_memory = None
    best_score = 0
    for memory in memories:
        merchant = (memory.merchant or "").strip()
        if not merchant:
            continue
        if merchant in lookup_keys or any(merchant in key or key in merchant for key in lookup_keys):
            return memory
        score = max(merchant_match_strength(merchant, key) for key in lookup_keys)
        if score > best_score:
            best_score = score
            best_memory = memory
    if best_memory and best_score >= 0.72:
        return best_memory
    return None


def remember_merchant_category(user_id, description, category, subcategory=None, display_name=None, subtype=None):
    lookup_keys = merchant_lookup_keys(description)
    normalized = lookup_keys[0] if lookup_keys else ""
    cleaned_category, cleaned_subcategory = resolved_learnable_category_pair(category, subcategory)
    cleaned_display_name = (display_name or "").strip()
    cleaned_subtype = (subtype or "").strip().lower()
    if cleaned_subtype not in VALID_TRANSACTION_SUBTYPES:
        cleaned_subtype = ""
    if not normalized or not cleaned_category:
        return

    memory = MerchantMemory.query.filter_by(user_id=user_id, merchant=normalized).first()
    if memory:
        memory.category = cleaned_category
        memory.subcategory = cleaned_subcategory
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
            subcategory=cleaned_subcategory,
            display_name=cleaned_display_name,
            subtype=cleaned_subtype,
            is_disabled=False,
        ))


def learned_rule_amount_direction(subtype):
    subtype = (subtype or "").strip().lower()
    if subtype == "income":
        return "credit"
    if subtype in {"expense", "payment"}:
        return "debit"
    return "any"


def learned_rule_pattern(description):
    merchant_pattern = (derive_merchant_key(description) or "").strip()
    normalized_pattern = (derive_normalized_description(description) or "").strip()
    generic_patterns = {
        "payment",
        "purchase",
        "deposit",
        "transfer",
        "withdrawal",
        "credit card payment",
        "online payment",
    }
    if merchant_pattern and merchant_pattern not in generic_patterns and len(merchant_pattern) >= 3:
        return merchant_pattern
    return normalized_pattern


def normalize_rule_display_name(value):
    cleaned = clean_transaction_description((value or "").strip())
    return cleaned[:255] if cleaned else ""


def normalize_rule_tags_value(value):
    return serialize_tags(value or "")


def normalize_rule_skip(value):
    return value in (True, 1, "1", "true", "True", "on", "yes")


def transaction_rule_actions(rule):
    if not rule:
        return {
            "display_name_override": "",
            "tag_rules": "",
            "skip_transaction": False,
        }
    return {
        "display_name_override": normalize_rule_display_name(getattr(rule, "display_name_override", "") or ""),
        "tag_rules": normalize_rule_tags_value(getattr(rule, "tag_rules", "") or ""),
        "skip_transaction": bool(getattr(rule, "skip_transaction", False)),
    }


def merge_transaction_tags(*values):
    merged = []
    seen = set()
    for value in values:
        for tag in parse_tags(value or ""):
            if tag not in seen:
                merged.append(tag)
                seen.add(tag)
    return serialize_tags(",".join(merged))


def rule_match_type_options():
    return [
        ("exact", "Exact"),
        ("startswith", "Starts With"),
        ("contains", "Contains"),
        ("regex", "Regex"),
    ]


def direction_label_for_subtype(subtype, fallback_amount=None):
    subtype = (subtype or "").strip().lower()
    if subtype == "income":
        return "credit"
    if subtype in {"expense", "payment"}:
        return "debit"
    if fallback_amount is not None:
        try:
            return "credit" if float(fallback_amount or 0) > 0 else "debit" if float(fallback_amount or 0) < 0 else "any"
        except Exception:
            return "any"
    return "any"


def upsert_transaction_rule(
    user_id,
    description,
    category,
    subcategory=None,
    subtype=None,
    display_name=None,
    tags=None,
    skip_transaction=False,
    matched_rule_id=None,
    match_type="exact",
    priority=1000,
    amount_direction=None,
    pattern=None,
    confidence_override=None,
):
    cleaned_category, cleaned_subcategory = canonical_category_pair(category, subcategory)
    cleaned_subtype = (subtype or "").strip().lower()
    if cleaned_subtype not in VALID_TRANSACTION_SUBTYPES:
        cleaned_subtype = ""
    cleaned_display_name = normalize_rule_display_name(display_name)
    cleaned_tags = normalize_rule_tags_value(tags)
    should_skip = normalize_rule_skip(skip_transaction)
    normalized_match_type = (match_type or "exact").strip().lower()
    if normalized_match_type not in {"exact", "startswith", "contains", "regex"}:
        normalized_match_type = "exact"
    try:
        priority = int(priority)
    except Exception:
        priority = 1000
    pattern = (pattern or learned_rule_pattern(description) or "").strip()
    if not cleaned_category:
        cleaned_category = "Needs Review"
    if not pattern:
        pattern = (description or "").strip()
    if not pattern:
        return None
    if normalized_match_type != "regex":
        pattern = (derive_merchant_key(pattern) or derive_normalized_description(pattern) or pattern).strip()
    if not pattern:
        return None
    category_id, subcategory_id = resolve_category_ids(cleaned_category, cleaned_subcategory)
    amount_direction = (amount_direction or direction_label_for_subtype(cleaned_subtype)).strip().lower()
    if amount_direction not in {"credit", "debit", "any"}:
        amount_direction = "any"

    matching_rules = []
    if matched_rule_id:
        matched_rule = CategoryRule.query.filter_by(
            id=matched_rule_id,
            user_id=user_id,
            is_system_rule=False,
        ).first()
        if matched_rule:
            matching_rules.append(matched_rule)

    duplicate_matches = (
        CategoryRule.query
        .filter(
            CategoryRule.user_id == user_id,
            CategoryRule.is_system_rule == False,  # noqa: E712
            or_(
                func.lower(CategoryRule.pattern) == pattern,
                func.lower(CategoryRule.keyword) == pattern,
            ),
        )
        .order_by(CategoryRule.priority.desc(), CategoryRule.id.asc())
        .all()
    )
    for rule in duplicate_matches:
        if rule not in matching_rules:
            matching_rules.append(rule)

    existing_rule = matching_rules[0] if matching_rules else None
    duplicate_rules = matching_rules[1:] if len(matching_rules) > 1 else []
    confidence = (
        float(confidence_override)
        if confidence_override is not None
        else 0.96 if normalized_match_type == "exact" else 0.9 if normalized_match_type == "startswith" else 0.85 if normalized_match_type == "contains" else 0.82
    )

    if not existing_rule:
        existing_rule = CategoryRule(
            user_id=user_id,
            keyword=pattern,
            category=cleaned_category,
            priority=priority,
            match_type=normalized_match_type,
            amount_direction=amount_direction,
            rule_type=normalized_match_type,
            pattern=pattern,
            category_id=category_id,
            subcategory_id=subcategory_id,
            confidence=confidence,
            is_system_rule=False,
            is_active=True,
            subtype=cleaned_subtype,
            display_name_override=cleaned_display_name,
            tag_rules=cleaned_tags,
            skip_transaction=should_skip,
        )
        db.session.add(existing_rule)
    else:
        existing_rule.keyword = pattern
        existing_rule.category = cleaned_category
        existing_rule.priority = priority
        existing_rule.match_type = normalized_match_type
        existing_rule.amount_direction = amount_direction
        existing_rule.rule_type = normalized_match_type
        existing_rule.pattern = pattern
        existing_rule.category_id = category_id
        existing_rule.subcategory_id = subcategory_id
        existing_rule.confidence = max(float(existing_rule.confidence or 0), confidence)
        existing_rule.is_system_rule = False
        existing_rule.is_active = True
        existing_rule.subtype = cleaned_subtype
        existing_rule.display_name_override = cleaned_display_name
        existing_rule.tag_rules = cleaned_tags
        existing_rule.skip_transaction = should_skip

    for duplicate_rule in duplicate_rules:
        db.session.delete(duplicate_rule)

    return existing_rule


def upsert_learned_category_rule(user_id, description, category, subcategory=None, subtype=None, matched_rule_id=None):
    """Persist one reusable user rule from a manual category correction."""
    cleaned_category, cleaned_subcategory = resolved_learnable_category_pair(category, subcategory)
    if not cleaned_category:
        return None
    return upsert_transaction_rule(
        user_id,
        description,
        cleaned_category,
        subcategory=cleaned_subcategory,
        subtype=subtype,
        matched_rule_id=matched_rule_id,
        match_type="contains",
        priority=1100,
        amount_direction=learned_rule_amount_direction(subtype),
        confidence_override=0.93,
    )


def merchant_lookup_keys(description):
    raw_description = (description or "").strip()
    lookup_keys = []
    for candidate in (
        derive_merchant_key(raw_description),
        normalize_text(clean_transaction_description(raw_description)),
        normalize_text(raw_description),
    ):
        candidate = (candidate or "").strip()
        if candidate and candidate not in lookup_keys:
            lookup_keys.append(candidate)
    return lookup_keys


def preferred_display_name_for_user(user_id, description, fallback=None):
    memory = find_best_merchant_memory(user_id, description)
    if memory and (memory.display_name or "").strip():
        return memory.display_name.strip()
    normalized = normalize_text(description)
    if not normalized:
        return (fallback or "").strip()
    return (fallback or "").strip()


def transaction_is_consistent_history_candidate(tx):
    if not tx:
        return False
    category_name = canonical_transaction_category(getattr(tx, "category", ""))
    if category_name == "Needs Review" or getattr(tx, "needs_review", False):
        return False
    confidence_bucket = normalize_confidence_bucket(getattr(tx, "category_confidence", ""))
    source_label = normalize_text(getattr(tx, "category_source", ""))
    return confidence_bucket in {"high", "medium"} or "manual" in source_label or "merchant memory" in source_label or "rule" in source_label


def build_merchant_history_index(transactions):
    grouped_transactions = defaultdict(list)
    for tx in transactions or []:
        if not transaction_is_consistent_history_candidate(tx):
            continue
        lookup_keys = merchant_lookup_keys(transaction_reference_description(tx))
        merchant_key_value = lookup_keys[0] if lookup_keys else ""
        if not merchant_key_value:
            continue
        grouped_transactions[merchant_key_value].append(tx)

    history_index = {}
    for merchant_key_value, grouped_rows in grouped_transactions.items():
        if len(grouped_rows) < 2:
            continue
        dominant_counter = Counter(
            (
                canonical_transaction_category(getattr(tx, "category", "")),
                canonical_subcategory_name(getattr(tx, "subcategory", "")),
                (getattr(tx, "transaction_subtype", "") or "").strip().lower(),
            )
            for tx in grouped_rows
        )
        (category_name, subcategory_name, subtype_name), dominant_count = dominant_counter.most_common(1)[0]
        dominant_ratio = dominant_count / len(grouped_rows)
        if category_name == "Needs Review" or dominant_ratio < 0.7:
            continue
        manual_like_count = sum(
            1
            for tx in grouped_rows
            if "manual" in normalize_text(getattr(tx, "category_source", "")) or normalize_text(getattr(tx, "category_source", "")) == "merchant memory"
        )
        strong_count = sum(
            1
            for tx in grouped_rows
            if normalize_confidence_bucket(getattr(tx, "category_confidence", "")) in {"high", "medium"}
        )
        matched_rule_ids = [
            getattr(tx, "matched_rule_id", None)
            for tx in grouped_rows
            if getattr(tx, "matched_rule_id", None)
        ]
        history_index[merchant_key_value] = {
            "merchant_key": merchant_key_value,
            "category": category_name,
            "subcategory": subcategory_name,
            "transaction_subtype": subtype_name,
            "dominant_ratio": round(dominant_ratio, 3),
            "manual_like_count": manual_like_count,
            "strong_count": strong_count,
            "matched_rule_id": Counter(matched_rule_ids).most_common(1)[0][0] if matched_rule_ids else None,
            "sample_count": len(grouped_rows),
        }
    return history_index


def find_merchant_history_match(description, merchant_history_index):
    if not merchant_history_index:
        return None
    lookup_keys = merchant_lookup_keys(description)
    if not lookup_keys:
        return None
    for lookup_key in lookup_keys:
        if lookup_key in merchant_history_index:
            return {**merchant_history_index[lookup_key], "match_score": 1.0}
    best_match = None
    best_score = 0.0
    for lookup_key in lookup_keys:
        for merchant_key_value, history_row in merchant_history_index.items():
            score = merchant_match_strength(lookup_key, merchant_key_value)
            if score > best_score:
                best_score = score
                best_match = history_row
    if best_match and best_score >= 0.72:
        return {**best_match, "match_score": round(best_score, 3)}
    return None


def attach_rule_actions_to_categorization(result, user_rules):
    result = dict(result or {})
    matched_rule_id = result.get("matched_rule_id")
    rule_by_id = {
        getattr(rule, "id", None): rule
        for rule in (user_rules or [])
        if getattr(rule, "id", None) is not None
    }
    matched_rule = rule_by_id.get(matched_rule_id)
    actions = transaction_rule_actions(matched_rule)
    result["rule_display_name"] = actions["display_name_override"]
    result["rule_tags"] = actions["tag_rules"]
    result["skip_transaction"] = actions["skip_transaction"]
    return result


def categorize_transaction_detailed(user_id, description, amount, tx_date=None, recurring_index=None):
    user_rules = sorted_user_rules(user_id)
    memories = active_merchant_memories_for_user(user_id)
    merchant_history_index = {}
    if recurring_index is None:
        historical_transactions = (
            Transaction.query
            .filter_by(user_id=user_id)
            .order_by(Transaction.date.desc(), Transaction.id.desc())
            .limit(500)
            .all()
        )
        recurring_index = build_recurring_index(historical_transactions)
        merchant_history_index = build_merchant_history_index(historical_transactions)
    result = categorize_transaction_record(
        description,
        amount,
        tx_date=tx_date,
        user_rules=user_rules,
        merchant_memories=memories,
        category_lookup=category_lookup_by_id(),
        recurring_index=recurring_index,
    )
    result = attach_rule_actions_to_categorization(result, user_rules)
    merchant_history_match = find_merchant_history_match(description, merchant_history_index)
    if merchant_history_match:
        resolved_history_category, resolved_history_subcategory = canonical_category_pair(
            merchant_history_match.get("category"),
            merchant_history_match.get("subcategory"),
        )
        result_confidence_bucket = normalize_confidence_bucket(result.get("category_confidence") or result.get("confidence_bucket"))
        should_apply_history = (
            result.get("category") == "Needs Review"
            or result.get("needs_review")
            or result_confidence_bucket in {"low", "uncategorized", "medium"}
            or (
                canonical_transaction_category(result.get("category")) == resolved_history_category
                and canonical_subcategory_name(result.get("subcategory")) == resolved_history_subcategory
            )
        )
        if should_apply_history and resolved_history_category and resolved_history_category != "Needs Review":
            match_score = float(merchant_history_match.get("match_score") or 0)
            current_confidence = float(result.get("confidence_score") or 0)
            boosted_confidence = (
                0.96
                if merchant_history_match.get("manual_like_count") and match_score >= 0.92
                else 0.91
                if int(merchant_history_match.get("strong_count") or 0) >= 3 and match_score >= 0.86
                else 0.84
            )
            merged_confidence = max(current_confidence, boosted_confidence)
            result.update({
                "category": resolved_history_category,
                "subcategory": resolved_history_subcategory,
                "confidence_score": merged_confidence,
                "confidence_bucket": confidence_bucket(merged_confidence),
                "category_source": "Merchant History",
                "matched_rule_id": result.get("matched_rule_id") or merchant_history_match.get("matched_rule_id"),
                "transaction_subtype": (merchant_history_match.get("transaction_subtype") or result.get("transaction_subtype") or "").strip().lower() or result.get("transaction_subtype"),
                "needs_review": merged_confidence < 0.9,
            })
    suggested_category_id, suggested_subcategory_id = resolve_category_ids(
        result.get("category"),
        result.get("subcategory"),
    )
    result["suggested_category_id"] = suggested_category_id
    result["suggested_subcategory_id"] = suggested_subcategory_id
    result["category_confidence"] = categorization_confidence_bucket(result.get("confidence_score"))
    return result


def categorize_transaction(user_id, description, amount, tx_date=None, recurring_index=None):
    result = categorize_transaction_detailed(
        user_id,
        description,
        amount,
        tx_date=tx_date,
        recurring_index=recurring_index,
    )
    return result["category"], result["category_source"]


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


IMPORT_REVIEW_BASE_CATEGORIES = TOP_LEVEL_CATEGORY_ORDER[:]


def import_category_choices(user_id):
    categories = set(IMPORT_REVIEW_BASE_CATEGORIES)
    categories.update(
        canonical_category_name(r.category)
        for r in sorted_user_rules(user_id)
        if r.category and canonical_category_name(r.category).lower() not in GENERIC_CATEGORIES
    )
    categories.update(
        canonical_category_name(category)
        for category, in db.session.query(Budget.category).filter(Budget.user_id == user_id).distinct().all()
        if category and canonical_category_name(category).lower() not in GENERIC_CATEGORIES
    )
    categories.update(
        canonical_category_name(category)
        for category, in db.session.query(MerchantMemory.category).filter(MerchantMemory.user_id == user_id).distinct().all()
        if category and canonical_category_name(category).lower() not in GENERIC_CATEGORIES
    )
    categories.update(
        canonical_category_name(category)
        for category, in db.session.query(Transaction.category).filter(Transaction.user_id == user_id).distinct().all()
        if category and canonical_category_name(category).lower() not in GENERIC_CATEGORIES
    )
    ordered = []
    seen = set()
    for category in TOP_LEVEL_CATEGORY_ORDER:
        if category in categories and category not in seen:
            ordered.append(category)
            seen.add(category)
    for category in sorted(categories):
        if category not in seen:
            ordered.append(category)
            seen.add(category)
    return ordered

def transaction_ui_category(category):
    return canonical_category_name(category or "")


def transaction_ui_category_choices(user_id):
    categories = {
        transaction_ui_category(category)
        for category in import_category_choices(user_id)
        if transaction_ui_category(category) and transaction_ui_category(category).lower() not in GENERIC_CATEGORIES
    }
    ordered = []
    seen = set()
    for category in TOP_LEVEL_CATEGORY_ORDER:
        if category in categories and category not in seen:
            ordered.append(category)
            seen.add(category)
    for category in sorted(categories):
        if category not in seen:
            ordered.append(category)
            seen.add(category)
    return ordered


def dedupe_filter_display_values(values, consolidate_other=False):
    deduped = {}
    for raw_value in values or []:
        cleaned_value = " ".join(str(raw_value or "").split()).strip()
        if not cleaned_value:
            continue
        display_value = "Other" if consolidate_other and normalize_text(cleaned_value).startswith("other") else cleaned_value
        deduped.setdefault(normalize_text(display_value), display_value)
    return sorted(deduped.values(), key=lambda value: normalize_text(value))


def transaction_filter_category_options(user_id):
    grouped_matches = defaultdict(set)
    for category in transaction_ui_category_choices(user_id):
        cleaned_category = transaction_ui_category(category)
        if not cleaned_category:
            continue
        display_label = "Other" if normalize_text(cleaned_category).startswith("other") else cleaned_category
        grouped_matches[display_label].add(cleaned_category)

    top_level_positions = {
        normalize_text(category_name): index
        for index, category_name in enumerate(TOP_LEVEL_CATEGORY_ORDER)
    }

    options = [
        {
            "value": display_label,
            "label": display_label,
            "match_values": sorted(match_values, key=lambda value: normalize_text(value)),
        }
        for display_label, match_values in grouped_matches.items()
    ]
    options.sort(
        key=lambda option: (
            top_level_positions.get(normalize_text(option["label"]), len(TOP_LEVEL_CATEGORY_ORDER) + 1),
            normalize_text(option["label"]),
        )
    )
    return options


TRANSACTION_STATUS_OPTIONS = [
    ("needs_attention", "Needs attention"),
    ("reviewed", "Reviewed"),
    ("errors", "Errors"),
]

TRANSACTION_SORT_OPTIONS = [
    ("newest", "Newest first"),
    ("oldest", "Oldest first"),
    ("highest_amount", "Highest amount first"),
    ("lowest_amount", "Lowest amount first"),
]

TRANSACTION_DATE_PRESET_OPTIONS = [
    ("this_month", "This month"),
    ("last_month", "Last month"),
    ("last_7_days", "Last 7 days"),
    ("last_30_days", "Last 30 days"),
]

TRANSACTION_PAGE_SIZE_OPTIONS = [25, 50, 100, 200]


def canonical_transaction_category(category):
    normalized = transaction_ui_category(category or "")
    return normalized or "Needs Review"


def month_date_range(month=None, year=None):
    try:
        month = int(month or 0)
        year = int(year or 0)
    except (TypeError, ValueError):
        return "", ""
    if month < 1 or month > 12 or year < 1:
        return "", ""
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return start.isoformat(), end.isoformat()


def parse_month_filter_key(value):
    raw_value = (value or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", raw_value):
        return "", None, None
    year_value, month_value = raw_value.split("-", 1)
    start_iso, end_iso = month_date_range(month_value, year_value)
    start_date = parse_date_any(start_iso)
    end_date = parse_date_any(end_iso)
    if not start_date or not end_date:
        return "", None, None
    return raw_value, start_date, end_date


def transaction_date_preset_range(preset_key, today_value=None):
    today_value = today_value or date.today()
    preset_key = (preset_key or "").strip().lower()
    if preset_key == "this_month":
        return date(today_value.year, today_value.month, 1), today_value
    if preset_key == "last_month":
        if today_value.month == 1:
            target_year = today_value.year - 1
            target_month = 12
        else:
            target_year = today_value.year
            target_month = today_value.month - 1
        start_iso, end_iso = month_date_range(target_month, target_year)
        return parse_date_any(start_iso), parse_date_any(end_iso)
    if preset_key == "last_7_days":
        return today_value - timedelta(days=6), today_value
    if preset_key == "last_30_days":
        return today_value - timedelta(days=29), today_value
    return None, None


def format_transaction_range_label(start_date_value, end_date_value):
    start_date = parse_date_any(start_date_value)
    end_date = parse_date_any(end_date_value)
    if not start_date and not end_date:
        return "All dates"
    if start_date and end_date:
        if start_date == end_date:
            return start_date.strftime("%b %d, %Y").replace(" 0", " ")
        if start_date.year == end_date.year:
            if start_date.month == end_date.month:
                return f"{start_date.strftime('%b %d').replace(' 0', ' ')} - {end_date.strftime('%d, %Y').replace(' 0', ' ')}"
            return f"{start_date.strftime('%b %d').replace(' 0', ' ')} - {end_date.strftime('%b %d, %Y').replace(' 0', ' ')}"
        return f"{start_date.strftime('%b %d, %Y').replace(' 0', ' ')} - {end_date.strftime('%b %d, %Y').replace(' 0', ' ')}"
    if start_date:
        return f"From {start_date.strftime('%b %d, %Y').replace(' 0', ' ')}"
    return f"Through {end_date.strftime('%b %d, %Y').replace(' 0', ' ')}"


def transactions_filter_url(category=None, subtype=None, start_date=None, end_date=None, month=None, year=None, preserve_current=True, **extra):
    params = {}
    if preserve_current and has_request_context():
        for key in ("q", "tag", "account_id", "source", "status", "sort", "date_preset", "month", "start_date", "end_date", "page_size"):
            value = request.args.get(key, "").strip()
            if value:
                params[key] = value
    if month and year and not start_date and not end_date:
        start_date, end_date = month_date_range(month, year)
    if category:
        params["category"] = canonical_transaction_category(category)
    if subtype:
        params["type"] = (subtype or "").strip().lower()
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    for key, value in (extra or {}).items():
        if value not in (None, "", []):
            params[key] = value
    return url_for("transactions_page", **params)


def transaction_matches_filter_category(tx, category):
    selected_category = (category or "").strip()
    normalized_category = transaction_ui_category(getattr(tx, "category", "") or "")
    if not selected_category:
        return True
    if selected_category == "Other":
        return not normalized_category or normalized_category in {"Needs Review", "Other"}
    return normalized_category == canonical_transaction_category(selected_category)


def serialize_transaction_drilldown_row(tx, account_name_map=None):
    account_name_map = account_name_map or {}
    amount = float(getattr(tx, "amount", 0) or 0)
    subtype = (getattr(tx, "transaction_subtype", "") or transaction_subtype_for(amount, getattr(tx, "category", ""), getattr(tx, "category_source", ""))).strip().lower()
    return {
        "id": tx.id,
        "date": tx.date.isoformat() if getattr(tx, "date", None) else "",
        "date_label": tx.date.strftime("%b %d, %Y") if getattr(tx, "date", None) else "No date",
        "display_name": transaction_display_name(tx) or "Transaction",
        "raw_description": transaction_raw_description(tx) or "",
        "category": transaction_ui_category(getattr(tx, "category", "") or "") or "Other",
        "category_label": transaction_category_label(tx) or "Other",
        "amount": round(amount, 2),
        "amount_abs": round(abs(amount), 2),
        "amount_label": f"{'+' if amount > 0 else '-' if amount < 0 else ''}${abs(amount):,.2f}",
        "amount_tone": "income" if amount > 0 else "expense" if amount < 0 else "neutral",
        "subtype": subtype,
        "subtype_label": transaction_type_label(tx),
        "account_name": account_name_map.get(tx.account_id, "Unassigned Account"),
    }


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
    if os.path.exists(preview_path):
        with open(preview_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        job = ImportJob.query.filter_by(preview_id=preview_id).first()
        if job and payload and not parse_import_review_payload(job.review_payload_json):
            job.review_payload_json = json.dumps(payload)
            db.session.commit()
        return payload
    job = ImportJob.query.filter_by(preview_id=preview_id).first()
    if not job:
        return None
    payload = parse_import_review_payload(job.review_payload_json)
    if payload:
        payload["import_job_id"] = job.id
    return payload or None


def delete_import_preview_by_id(preview_id):
    if not preview_id:
        return
    preview_path = os.path.join(get_import_preview_dir(), f"{preview_id}.json")
    if os.path.exists(preview_path):
        os.remove(preview_path)


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


def clear_import_preview(remove_saved_preview=None):
    preview_id = session.pop("import_preview_id", None)
    if not preview_id:
        return
    if remove_saved_preview is None:
        remove_saved_preview = ImportJob.query.filter_by(preview_id=preview_id).first() is None
    if remove_saved_preview:
        delete_import_preview_by_id(preview_id)


def import_job_is_staged(job):
    return bool(job and (job.status or "").lower() == "completed" and job.preview_id)


def preview_row_default_review_state(row):
    if not row or row.get("is_duplicate"):
        return "reviewed"
    if row.get("review_required") or row.get("is_low_confidence") or row.get("is_uncategorized") or (row.get("confidence_bucket") == "error"):
        return "needs_review"
    return "reviewed"


def preview_row_needs_attention(row):
    if not row or preview_row_is_skipped(row):
        return False
    review_state = (row.get("review_state") or "").strip().lower()
    if review_state == "reviewed":
        return False
    if review_state == "needs_review":
        return True
    return preview_row_default_review_state(row) == "needs_review"


def import_job_for_preview(preview_id, user_id=None):
    if not preview_id:
        return None
    query = ImportJob.query.filter_by(preview_id=preview_id)
    if user_id is not None:
        query = query.filter_by(user_id=user_id)
    return query.first()


def import_job_summary_from_preview_payload(job, payload):
    summary = dict(payload.get("summary") or {})
    parser_debug = payload.get("parser_debug") or []
    return {
        "transaction_count": summary.get("transaction_count", len(payload.get("rows", []))),
        "new_transaction_count": summary.get("new_transaction_count", 0),
        "already_imported_count": summary.get("already_imported_count", 0),
        "duplicate_candidate_count": summary.get("duplicate_candidate_count", 0),
        "ignored_row_count": summary.get("ignored_row_count", 0),
        "needs_review_count": summary.get("needs_review_count", 0),
        "error_count": summary.get("error_count", 0),
        "ready_count": summary.get("ready_count", 0),
        "auto_approved_count": summary.get("auto_approved_count", 0),
        "duplicate_count": summary.get("duplicate_existing_count", 0) + summary.get("duplicate_file_count", 0),
        "net_impact": summary.get("net_impact", 0),
        "date_range_start": summary.get("date_range_start", ""),
        "date_range_end": summary.get("date_range_end", ""),
        "date_range_label": summary.get("date_range_label", "Date range unavailable"),
        "file_count": job.file_count if job else len(payload.get("file_summaries") or []),
        "parser_debug": parser_debug,
        "warnings": [warning for debug in parser_debug for warning in debug.get("warnings", [])][:12],
    }


def sync_import_job_review_payload(job, payload, persist_preview_file=True, set_active_session=False):
    if not job or not payload:
        return payload
    payload["import_job_id"] = job.id
    preview_id = job.preview_id or f"job_{job.id}"
    job.preview_id = preview_id
    payload = refresh_preview_payload(payload)
    job.review_payload_json = json.dumps(payload)
    job.summary_json = json.dumps(import_job_summary_from_preview_payload(job, payload))
    summary = payload.get("summary") or {}
    job.start_date = parse_date_any(summary.get("date_range_start")) if summary.get("date_range_start") else None
    job.end_date = parse_date_any(summary.get("date_range_end")) if summary.get("date_range_end") else None
    if persist_preview_file:
        save_import_preview(job.user_id, payload, preview_id=preview_id, store_in_session=set_active_session)
    return payload


def apply_review_form_to_preview(preview, form):
    if not preview:
        return preview
    rows = list(preview.get("rows") or [])
    for row in rows:
        row_id = row.get("row_id")
        if row_id is None:
            continue
        display_name = clean_transaction_description(
            (form.get(f"display_name_{row_id}") or row.get("display_name") or row.get("description") or "").strip()
        )
        if display_name:
            row["display_name"] = display_name
        category_value = canonical_transaction_category((form.get(f"category_{row_id}") or row.get("category") or "").strip())
        subcategory_value = canonical_subcategory_name((form.get(f"subcategory_{row_id}") or row.get("subcategory") or "").strip())
        category_value, subcategory_value = canonical_category_pair(category_value, subcategory_value)
        row["category"] = category_value or "Needs Review"
        row["subcategory"] = subcategory_value or ""
        row["is_uncategorized"] = row["category"] == "Needs Review"
        action_value = (form.get(f"row_action_{row_id}") or row.get("default_row_action") or "import").strip().lower()
        if action_value not in {"import", "skip", "not_transaction"}:
            action_value = "import"
        row["default_row_action"] = action_value
        review_state = (form.get(f"review_state_{row_id}") or row.get("review_state") or preview_row_default_review_state(row)).strip().lower()
        row["review_state"] = "reviewed" if review_state == "reviewed" else "needs_review"
    preview["rows"] = rows
    return refresh_preview_payload(preview)


def delete_staged_import_job(job, user_id):
    if not job or job.user_id != user_id or not import_job_is_staged(job):
        return False
    if has_request_context() and session.get("import_preview_id") == job.preview_id:
        session.pop("import_preview_id", None)
    delete_import_preview_by_id(job.preview_id)
    remove_import_job_files(job.id)
    db.session.delete(job)
    return True


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


def parse_import_review_payload(raw_payload):
    parsed = parse_import_job_summary(raw_payload)
    return parsed if isinstance(parsed, dict) else {}


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
    account_ids = sorted({job.account_id for job in jobs if getattr(job, "account_id", None)})
    if account_ids:
        account_map = {
            account_id: account_name
            for account_id, account_name in (
                db.session.query(Account.id, Account.name)
                .filter(Account.user_id == user_id, Account.id.in_(account_ids))
                .all()
            )
        }
    else:
        account_map = {}
    rows = []
    for job in jobs:
        summary = parse_import_job_summary(job.summary_json)
        if not summary.get("date_range_label"):
            summary["date_range_label"] = format_import_date_range(job.start_date, job.end_date)
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


def next_pending_import_job():
    return (
        ImportJob.query
        .filter(
            ImportJob.status.in_(("queued", "processing")),
            ImportJob.preview_id.is_(None),
        )
        .order_by(ImportJob.created_at.asc(), ImportJob.id.asc())
        .first()
    )


def import_worker_loop():
    global IMPORT_WORKER_THREAD
    try:
        while True:
            with app.app_context():
                db.session.remove()
                job = next_pending_import_job()
                if not job:
                    db.session.remove()
                    break
                job_id = job.id
            process_import_job(job_id)
            time.sleep(0.05)
    finally:
        with IMPORT_WORKER_LOCK:
            IMPORT_WORKER_THREAD = None
        with app.app_context():
            db.session.remove()


def start_import_worker_if_needed():
    global IMPORT_WORKER_THREAD
    with IMPORT_WORKER_LOCK:
        if IMPORT_WORKER_THREAD and IMPORT_WORKER_THREAD.is_alive():
            return
        with app.app_context():
            pending_job = next_pending_import_job()
        if not pending_job:
            return
        IMPORT_WORKER_THREAD = threading.Thread(
            target=import_worker_loop,
            name="akuos-import-worker",
            daemon=True,
        )
        IMPORT_WORKER_THREAD.start()


def queue_import_job(user_id, account_id, file_storages=None, pasted_text=""):
    job_id = uuid.uuid4().hex[:32]
    stored_files = []
    file_names = []
    file_storages = file_storages or []
    for index, file_storage in enumerate(file_storages, start=1):
        original_name = file_storage.filename or f"statement-{index}.csv"
        destination_name = f"{index:02d}_{secure_filename(original_name) or f'statement-{index}.dat'}"
        destination_path = import_job_file_path(job_id, destination_name)
        file_storage.save(destination_path)
        stored_files.append({
            "path": destination_path,
            "filename": original_name,
            "source_type": "file",
        })
        file_names.append(original_name)
    pasted_text = (pasted_text or "").strip()
    if pasted_text:
        destination_name = f"{len(stored_files) + 1:02d}_pasted-statement.txt"
        destination_path = import_job_file_path(job_id, destination_name)
        with open(destination_path, "w", encoding="utf-8") as handle:
            handle.write(pasted_text)
        stored_files.append({
            "path": destination_path,
            "filename": "pasted-statement.txt",
            "label": "Pasted Statement Text",
            "source_type": "manual_text",
        })
        file_names.append("Pasted Statement Text")

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

    start_import_worker_if_needed()
    return job


def process_import_job(job_id):
    with app.app_context():
        db.session.remove()
        job = ImportJob.query.get(job_id)
        if not job:
            return

        parser_debug = []
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

                payload, error, parser_debug = build_import_preview(
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
            sync_import_job_review_payload(job, payload, persist_preview_file=True, set_active_session=False)
            summary = payload.get("summary", {})
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
            app.logger.exception("Import job %s failed", job_id, extra={"parser_debug": parser_debug})
            job = ImportJob.query.get(job_id)
            if job:
                job.status = "failed"
                job.current_stage = "failed"
                job.progress_percent = 100
                job.error_message = str(exc)[:255]
                existing_summary = parse_import_job_summary(job.summary_json)
                if parser_debug:
                    existing_summary["parser_debug"] = parser_debug
                job.summary_json = json.dumps(existing_summary)
                job.completed_at = datetime.utcnow()
                db.session.commit()
        finally:
            remove_import_job_files(job_id)
            db.session.remove()


def transaction_fingerprint(tx_date, description, amount, merchant_guess=""):
    if hasattr(tx_date, "isoformat"):
        date_key = tx_date.isoformat()
    else:
        date_key = str(tx_date)
    amount_key = round(float(amount or 0), 2)
    merchant_key = normalize_text(merchant_guess or description)
    return f"{date_key}|{amount_key:.2f}|{merchant_key}"


def existing_transaction_fingerprints(user_id, account_id):
    fingerprints = set()
    account_transactions = Transaction.query.filter_by(user_id=user_id, account_id=account_id).all()
    for tx in account_transactions:
        stored_fingerprint = (getattr(tx, "fingerprint", "") or "").strip()
        fingerprints.add(
            stored_fingerprint
            or transaction_fingerprint(
                tx.date,
                transaction_reference_description(tx),
                tx.amount,
                merchant_guess=getattr(tx, "merchant_guess", "") or transaction_reference_description(tx),
            )
        )
    return fingerprints


def duplicate_reference_description(tx):
    return (
        (getattr(tx, "merchant_guess", "") or "").strip()
        or (getattr(tx, "normalized_description", "") or "").strip()
        or transaction_reference_description(tx)
    )


def import_date_range(rows, parser_debug=None):
    parsed_dates = []
    missing_date_count = 0
    invalid_date_values = []
    for row in rows or []:
        raw_date = row.get("date")
        if raw_date in (None, ""):
            missing_date_count += 1
            continue
        parsed = parse_date_any(raw_date)
        if parsed:
            parsed_dates.append(parsed)
        else:
            invalid_date_values.append(str(raw_date))
    fallback_start = min(parsed_dates).isoformat() if parsed_dates else ""
    fallback_end = max(parsed_dates).isoformat() if parsed_dates else ""
    for debug in parser_debug or []:
        start = parse_date_any(debug.get("statement_period_start"))
        end = parse_date_any(debug.get("statement_period_end"))
        if start and end:
            app.logger.info(
                "Import date range detected from statement period: %s to %s",
                start.isoformat(),
                end.isoformat(),
            )
            return start.isoformat(), end.isoformat(), "statement_period"
    if fallback_start and fallback_end:
        if invalid_date_values or missing_date_count:
            app.logger.info(
                "Import date range detected from parsed transaction dates: %s to %s (%s valid, %s missing, %s invalid)",
                fallback_start,
                fallback_end,
                len(parsed_dates),
                missing_date_count,
                len(invalid_date_values),
            )
        return fallback_start, fallback_end, "transaction_dates"
    if missing_date_count or invalid_date_values:
        app.logger.warning(
            "Import date range unavailable: %s valid dates, %s missing dates, %s invalid dates. Sample invalid values: %s",
            len(parsed_dates),
            missing_date_count,
            len(invalid_date_values),
            ", ".join(invalid_date_values[:5]) if invalid_date_values else "none",
        )
    return "", "", ""


def format_import_date_range(start_date_value, end_date_value):
    start = parse_date_any(start_date_value)
    end = parse_date_any(end_date_value)
    if not start or not end:
        return "Date range unavailable"
    if start == end:
        return start.strftime("%b %d, %Y").replace(" 0", " ")
    if start.year == end.year:
        if start.month == end.month:
            return f"{start.strftime('%b %d').replace(' 0', ' ')} – {end.strftime('%b %d').replace(' 0', ' ')}"
        return f"{start.strftime('%b %d').replace(' 0', ' ')} – {end.strftime('%b %d').replace(' 0', ' ')}"
    return f"{start.strftime('%b %d, %Y').replace(' 0', ' ')} – {end.strftime('%b %d, %Y').replace(' 0', ' ')}"


def existing_transactions_for_duplicate_matching(user_id, account_id):
    transactions = Transaction.query.filter_by(user_id=user_id, account_id=account_id).all()
    exact_matches = {}
    by_amount = defaultdict(list)
    for tx in transactions:
        tx_fingerprint = (getattr(tx, "fingerprint", "") or "").strip() or transaction_fingerprint(
            tx.date,
            transaction_reference_description(tx),
            tx.amount,
            merchant_guess=getattr(tx, "merchant_guess", "") or transaction_reference_description(tx),
        )
        metadata = {
            "id": tx.id,
            "date": tx.date.isoformat() if tx.date else "",
            "display_name": transaction_display_name(tx),
            "raw_description": transaction_raw_description(tx),
            "amount": round(float(tx.amount or 0), 2),
            "category": getattr(tx, "category", "") or "",
            "subcategory": getattr(tx, "subcategory", "") or "",
            "fingerprint": tx_fingerprint,
            "normalized_reference": normalize_text(duplicate_reference_description(tx)),
        }
        exact_matches[tx_fingerprint] = metadata
        by_amount[round(float(tx.amount or 0), 2)].append(metadata)
    return exact_matches, by_amount


def probable_duplicate_match(candidate_date, amount, normalized_reference, amount_matches):
    if not candidate_date or amount is None or not normalized_reference:
        return None
    rounded_amount = round(float(amount or 0), 2)
    for existing in amount_matches.get(rounded_amount, []):
        existing_date = parse_date_any(existing.get("date"))
        if not existing_date or abs((existing_date - candidate_date).days) > 3:
            continue
        similarity = merchant_similarity(normalized_reference, existing.get("normalized_reference", ""))
        if similarity >= 0.9 or normalized_reference == existing.get("normalized_reference", ""):
            return {
                **existing,
                "similarity": round(similarity, 2),
            }
    return None


PDF_DATE_PATTERN = re.compile(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b")
PDF_DATE_AT_START_PATTERN = re.compile(r"^\s*\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?(?:\s+\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)?")
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
PDF_NON_TRANSACTION_PHRASES = (
    "years previous adjusted",
    "previous adjusted",
    "payment information",
    "minimum payment due",
    "new balance",
    "previous balance",
    "credit line",
    "available credit",
    "cash advance line",
    "rewards summary",
    "rewards earned",
    "earnings summary",
    "interest charge calculation",
    "interest charge explanation",
    "legal notice",
    "customer service",
    "billing rights",
    "payment mailing address",
    "detached and mail",
    "important account information",
    "contact us",
    "call customer service",
    "year to date",
    "fees charged",
    "interest charged",
    "account summary",
)
PDF_TRANSACTION_TYPE_PATTERNS = [
    (re.compile(r"\bach\s+deposit\b|\bdirect\s+deposit\b", re.I), "Income", "Income"),
    (re.compile(r"\batm\b", re.I), "Cash Withdrawal", "Other"),
    (re.compile(r"\belectronic\s+pmt\b|\bpayment thank you\b|\bautopay payment\b|\bcredit card payment\b|\bcapital one(?:\s+online)? payment\b|\bpayment received\b", re.I), "Bills/Payments", "Subscriptions / Bills"),
    (re.compile(r"\bdbcrd\b|\bpurchase\b|\bpur\b", re.I), "Expense", None),
]
PDF_ACTIVE_SECTION_PATTERNS = [
    (re.compile(r"^\s*transactions(?:\s*\(continued\))?(?:\s+.*)?$", re.I), "transactions"),
    (re.compile(r"^\s*payments,\s*credits?\s+and\s+adjustments(?:\s+.*)?$", re.I), "payments_credits_adjustments"),
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
    re.compile(r"^\s*payment information.*$", re.I),
    re.compile(r"^\s*(?:previous|new)\s+balance.*$", re.I),
    re.compile(r"^\s*(?:credit line|available credit|cash advance line).*$", re.I),
    re.compile(r"^\s*interest charge.*$", re.I),
    re.compile(r"^\s*legal.*$", re.I),
    re.compile(r"^\s*customer service.*$", re.I),
    re.compile(r"^\s*(?:rewards?|earnings?)\s+.*summary.*$", re.I),
    re.compile(r"^\s*years?\s+previous\s+adjusted.*$", re.I),
]
PDF_SECTION_END_PATTERNS = [
    re.compile(r"^\s*totals?.*$", re.I),
    re.compile(r"^\s*fees?\s*(?:charged|summary)?.*$", re.I),
    re.compile(r"^\s*interest\s*(?:charged|summary|details?)?.*$", re.I),
    re.compile(r"^\s*(?:year to date|years previous adjusted).*$", re.I),
    re.compile(r"^\s*(?:payment information|customer service|legal|billing rights).*$", re.I),
    re.compile(r"^\s*(?:previous balance|new balance|minimum payment due|payment due).*$", re.I),
    re.compile(r"^\s*(?:rewards?|earnings?)\s+.*$", re.I),
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


def init_pdf_parser_debug(filename):
    return {
        "filename": filename or "statement.pdf",
        "text_extracted": False,
        "readable_pages": 0,
        "sections_found": [],
        "candidate_rows_found": 0,
        "rows_parsed": 0,
        "rows_rejected": 0,
        "rows_filtered_out": 0,
        "ignored_followups": 0,
        "rejection_reasons": {},
        "sample_rejections": [],
    }


def increment_pdf_debug_reason(debug_info, reason):
    if not debug_info or not reason:
        return
    rejection_reasons = debug_info.setdefault("rejection_reasons", {})
    rejection_reasons[reason] = int(rejection_reasons.get(reason, 0)) + 1


def add_pdf_debug_rejection(debug_info, reason, raw_line, section_name=None, page_index=None):
    if not debug_info:
        return
    debug_info["rows_rejected"] = int(debug_info.get("rows_rejected", 0)) + 1
    increment_pdf_debug_reason(debug_info, reason)
    samples = debug_info.setdefault("sample_rejections", [])
    if len(samples) >= 10:
        return
    sample = {
        "reason": reason,
        "line": normalize_pdf_cell(raw_line)[:220],
    }
    if section_name:
        sample["section"] = section_name
    if page_index:
        sample["page"] = page_index
    samples.append(sample)


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


def is_pdf_section_end_line(line):
    cleaned = normalize_pdf_cell(line)
    if not cleaned:
        return False
    return any(pattern.match(cleaned) for pattern in PDF_SECTION_END_PATTERNS)


def is_obviously_non_transaction_text(text):
    cleaned = normalize_pdf_cell(text).lower()
    if not cleaned:
        return True
    if any(phrase in cleaned for phrase in PDF_NON_TRANSACTION_PHRASES):
        return True
    if len(cleaned) > 150:
        return True
    if len(cleaned.split()) > 18:
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


def has_pdf_date_token(line):
    return bool(pdf_date_matches_with_values(line))


def starts_with_pdf_date(line):
    return bool(PDF_DATE_AT_START_PATTERN.match(normalize_pdf_cell(line)))


def has_pdf_amount_token(line):
    cleaned = normalize_pdf_cell(line)
    if not cleaned:
        return False
    return choose_pdf_amount_match(cleaned, pdf_date_matches_with_values(cleaned)) is not None


def is_pdf_amount_only_line(line):
    cleaned = normalize_pdf_cell(line)
    if not cleaned or has_pdf_date_token(cleaned):
        return False
    amount_match = choose_pdf_amount_match(cleaned, [])
    if not amount_match:
        return False
    stripped = cleaned.replace(amount_match.group(0), " ").strip(" -|")
    return not bool(re.search(r"[A-Za-z]", stripped))


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
    if not starts_with_pdf_date(cleaned):
        return False
    if is_obviously_non_transaction_text(cleaned):
        return False
    date_matches = pdf_date_matches_with_values(cleaned)
    amount_match = choose_pdf_amount_match(cleaned, date_matches)
    if not date_matches or not amount_match:
        return False
    description = pdf_description_between_dates_and_amount(cleaned, date_matches, amount_match)
    if not bool(re.search(r"[A-Za-z]", description or cleaned)):
        return False
    return not is_obviously_non_transaction_text(description or cleaned)


def is_pdf_continuation_line(line):
    cleaned = normalize_pdf_cell(line)
    if not cleaned or is_pdf_noise_line(cleaned) or is_foreign_currency_followup(cleaned):
        return False
    if is_obviously_non_transaction_text(cleaned):
        return False
    if pdf_date_matches_with_values(cleaned):
        return False
    amount_matches = list(PDF_AMOUNT_PATTERN.finditer(cleaned))
    if amount_matches and not re.search(r"[A-Za-z]", cleaned):
        return False
    if len(cleaned.split()) > 8:
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


def parse_pdf_candidate_block(block_lines, source_document, row_index, section_name=None):
    if section_name not in {"transactions", "payments_credits_adjustments"}:
        return None, "inactive_section"

    normalized_lines = [normalize_pdf_cell(line) for line in (block_lines or []) if normalize_pdf_cell(line)]
    if not normalized_lines:
        return None, "empty_block"

    starter_line = normalized_lines[0]
    if not starts_with_pdf_date(starter_line):
        return None, "no_transaction_table_row_detected"
    if is_obviously_non_transaction_text(starter_line):
        return None, "summary_help_block_rejected"
    if len(normalized_lines) > 3:
        return None, "invalid_description_block"

    non_fx_lines = [line for line in normalized_lines if not is_foreign_currency_followup(line)]
    if not non_fx_lines:
        return None, "foreign_currency_only"

    combined = " ".join(non_fx_lines).strip()
    if is_obviously_non_transaction_text(combined):
        return None, "summary_help_block_rejected"
    date_matches = pdf_date_matches_with_values(combined)
    if not date_matches:
        return None, "missing_date"
    if len(date_matches) > 2:
        return None, "too_many_dates"

    chosen_amount_match = choose_pdf_amount_match(combined, date_matches)
    if not chosen_amount_match:
        return None, "missing_amount"

    parsed_date = date_matches[0]["parsed"]
    post_date = date_matches[1]["parsed"] if len(date_matches) > 1 else None
    sign_hint = infer_statement_sign(combined, chosen_amount_match.group(0))
    amount = parse_statement_amount(chosen_amount_match.group(0), force_sign=sign_hint)
    if amount is None:
        return None, "invalid_amount"

    description = pdf_description_between_dates_and_amount(combined, date_matches, chosen_amount_match)
    if not description:
        return None, "missing_description"
    if is_obviously_non_transaction_text(description):
        return None, "invalid_description_block"

    transaction_type, default_category = classify_pdf_transaction_type(description or combined, section_name=section_name)
    cleaned_description = build_pdf_transaction_description(description, combined, transaction_type)
    if not cleaned_description or not re.search(r"[A-Za-z]", cleaned_description):
        return None, "invalid_description"
    if is_obviously_non_transaction_text(cleaned_description):
        return None, "invalid_description_block"

    return {
        "source_document": source_document,
        "raw_source": combined,
        "date": parsed_date.isoformat() if parsed_date else "",
        "post_date": post_date.isoformat() if post_date else "",
        "description": cleaned_description,
        "raw_description": description or combined,
        "amount": round(amount, 2),
        "source_category": default_category or "",
        "raw_category": "",
        "category": "",
        "category_source": "",
        "fingerprint": f"pdfblock|{source_document}|{row_index}|{normalize_text(combined)}",
        "requires_manual_fields": False,
        "manual_reason": "",
        "transaction_type": transaction_type or "",
        "parser_label": f"PDF block parser · {section_name.replace('_', ' ')}",
    }, None


def classify_pdf_transaction_type(raw_text, section_name=None):
    text = normalize_pdf_cell(raw_text)
    if section_name == "payments_credits_adjustments":
        return "Bills/Payments", "Subscriptions / Bills"
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
        "Bills/Payments": "Credit Card Payment",
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
    if is_obviously_non_transaction_text(raw_line):
        return None

    date_indexes = []
    parsed_date = None
    for idx, cell in enumerate(normalized_cells):
        parsed = parse_statement_date_with_fallback(cell)
        if parsed:
            date_indexes.append(idx)
            if parsed_date is None:
                parsed_date = parsed
    if not date_indexes or date_indexes[0] > 1:
        return None

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
    if not description or is_obviously_non_transaction_text(description):
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
    parsed, error, diagnostics = parse_statement_input(file_storage)
    if parsed and diagnostics:
        parsed.setdefault("diagnostics", diagnostics)
    return parsed, error


def extract_pdf_statement_data(file_storage, debug_info=None):
    parsed, error, diagnostics = parse_statement_input(file_storage)
    if debug_info is not None and diagnostics:
        debug_info.update(diagnostics)
    if parsed and diagnostics:
        parsed.setdefault("diagnostics", diagnostics)
    return parsed, error


def extract_text_statement_data(file_storage, debug_info=None):
    parsed, error, diagnostics = parse_statement_input(file_storage)
    if debug_info is not None and diagnostics:
        debug_info.update(diagnostics)
    if parsed and diagnostics:
        parsed.setdefault("diagnostics", diagnostics)
    return parsed, error


def detect_statement_file_type(file_storage):
    return parser_detect_statement_file_type(file_storage.filename or "")


def import_review_priority(row):
    reasons = set(row.get("review_reasons") or [])
    if "Invalid Date" in reasons or "Invalid Amount" in reasons:
        return 0
    if "No Rule Match" in reasons:
        return 1
    if "Multiple Possible Matches" in reasons:
        return 2
    if "Low Confidence" in reasons:
        return 3
    if "Unknown Merchant" in reasons:
        return 4
    if "Description Too Noisy" in reasons:
        return 5
    if "Possible Credit Card Payment" in reasons or "Possible Transfer" in reasons:
        return 6
    bucket = row.get("confidence_bucket")
    if bucket == "high" and not row.get("auto_approved"):
        return 7
    if row.get("is_duplicate") or row.get("default_row_action") == "skip":
        return 8
    if row.get("auto_approved"):
        return 9
    return 10


def preview_row_is_skipped(row):
    return bool(row.get("is_duplicate") or (row.get("default_row_action") or "").strip().lower() == "skip")


def summarize_preview_rows(rows, parser_debug=None, parser_filtered_count=0):
    summary = {
        "transaction_count": len(rows or []),
        "total_rows_found": 0,
        "parsed_candidate_count": len(rows or []),
        "new_transaction_count": 0,
        "already_imported_count": 0,
        "duplicate_candidate_count": 0,
        "needs_review_count": 0,
        "ignored_row_count": 0,
        "skipped_count": 0,
        "ready_count": 0,
        "manual_fix_count": 0,
        "error_count": 0,
        "low_confidence_count": 0,
        "medium_confidence_count": 0,
        "high_confidence_count": 0,
        "uncategorized_count": 0,
        "auto_approved_count": 0,
        "duplicate_existing_count": 0,
        "duplicate_file_count": 0,
        "duplicates_skipped_count": 0,
        "net_impact": 0.0,
        "income_total": 0.0,
        "expense_total": 0.0,
        "transfer_payment_total": 0.0,
        "transfer_count": 0,
        "expense_impact": 0.0,
        "payment_impact": 0.0,
        "importable_count": 0,
        "source_breakdown": {},
        "reason_counts": {},
        "category_totals": {},
        "parser_used": "rule_based",
        "parser_confidence_score": 0.0,
        "date_range_start": "",
        "date_range_end": "",
        "date_range_label": "Date range unavailable",
        "date_range_source": "",
    }
    source_breakdown = defaultdict(int)
    reason_counts = Counter()
    category_totals = defaultdict(float)
    for row in rows or []:
        amount = safe_float(row.get("amount")) or 0.0
        skipped = preview_row_is_skipped(row)
        current_category = canonical_category_name((row.get("category") or "").strip())
        source_breakdown[(row.get("category_source") or "Unknown").strip() or "Unknown"] += 1
        for reason in (row.get("review_reasons") or []):
            if reason in {"Duplicate", "Duplicate in File"}:
                reason_counts["Duplicate Candidate"] += 1
            else:
                reason_counts[reason] += 1
        if row.get("requires_manual_fields"):
            summary["manual_fix_count"] += 1
        if row.get("is_uncategorized"):
            summary["uncategorized_count"] += 1
        confidence_bucket = (row.get("confidence_bucket") or "").strip().lower()
        if confidence_bucket == "error":
            summary["error_count"] += 1
        if confidence_bucket == "low":
            summary["low_confidence_count"] += 1
        elif confidence_bucket == "medium":
            summary["medium_confidence_count"] += 1
        elif confidence_bucket == "high":
            summary["high_confidence_count"] += 1
        if row.get("auto_approved") and not skipped:
            summary["auto_approved_count"] += 1
        if row.get("is_possible_duplicate"):
            summary["duplicate_candidate_count"] += 1
        if row.get("is_duplicate"):
            summary["ignored_row_count"] += 1
            duplicate_reason = (row.get("duplicate_reason") or "").strip().lower()
            if duplicate_reason == "already_imported":
                summary["already_imported_count"] += 1
                summary["duplicate_existing_count"] += 1
            else:
                summary["duplicate_file_count"] += 1
            continue
        if skipped:
            summary["ignored_row_count"] += 1
            continue
        summary["importable_count"] += 1
        summary["new_transaction_count"] += 1
        summary["net_impact"] += amount
        if amount > 0:
            summary["income_total"] += amount
        if preview_row_needs_attention(row):
            summary["needs_review_count"] += 1
        else:
            summary["ready_count"] += 1
        row_kind = (row.get("row_kind") or "").strip().lower()
        if row_kind in {"payment", "transfer"}:
            summary["transfer_count"] += 1
            summary["transfer_payment_total"] += abs(amount)
            category_totals[current_category or "Other"] += amount
            if amount < 0 and row_kind == "payment":
                summary["payment_impact"] += abs(amount)
        elif amount < 0:
            summary["expense_total"] += abs(amount)
            category_totals[current_category or "Needs Review"] += amount
            if row_kind == "expense":
                summary["expense_impact"] += abs(amount)
        else:
            category_totals[current_category or "Needs Review"] += amount

    summary["net_impact"] = round(summary["net_impact"], 2)
    summary["income_total"] = round(summary["income_total"], 2)
    summary["expense_total"] = round(summary["expense_total"], 2)
    summary["transfer_payment_total"] = round(summary["transfer_payment_total"], 2)
    summary["expense_impact"] = round(summary["expense_impact"], 2)
    summary["payment_impact"] = round(summary["payment_impact"], 2)
    summary["duplicates_skipped_count"] = summary["duplicate_existing_count"] + summary["duplicate_file_count"]
    summary["skipped_count"] = int(parser_filtered_count or 0) + summary["ignored_row_count"]
    summary["source_breakdown"] = dict(source_breakdown)
    summary["reason_counts"] = dict(reason_counts)
    summary["category_totals"] = dict(
        sorted(
            ((name, round(total, 2)) for name, total in category_totals.items() if abs(total) > 0.0001),
            key=lambda item: abs(item[1]),
            reverse=True,
        )
    )

    parser_entries = list(parser_debug or [])
    if parser_entries:
        parser_labels = []
        confidence_scores = []
        total_rows_found = 0
        parsed_candidate_count = 0
        for entry in parser_entries:
            parser_label = (entry.get("parser_name") or entry.get("parser_used") or "rule_based").strip()
            if parser_label and parser_label not in parser_labels:
                parser_labels.append(parser_label)
            score = entry.get("confidence_score")
            if score is not None:
                confidence_scores.append(float(score or 0))
            total_rows_found += int(entry.get("candidate_rows_found") or 0)
            parsed_candidate_count += int(entry.get("rows_parsed") or 0)
        if parser_labels:
            summary["parser_used"] = ", ".join(parser_labels)
        if confidence_scores:
            summary["parser_confidence_score"] = round(sum(confidence_scores) / len(confidence_scores), 2)
        if total_rows_found:
            summary["total_rows_found"] = total_rows_found
        if parsed_candidate_count:
            summary["parsed_candidate_count"] = parsed_candidate_count

    if not summary["total_rows_found"]:
        summary["total_rows_found"] = summary["parsed_candidate_count"]
    range_start, range_end, range_source = import_date_range(rows or [], parser_debug=parser_debug)
    summary["date_range_start"] = range_start
    summary["date_range_end"] = range_end
    summary["date_range_source"] = range_source
    summary["date_range_label"] = format_import_date_range(range_start, range_end)
    return summary


def refresh_preview_payload(preview):
    if not preview:
        return preview
    rows = list(preview.get("rows") or [])
    preview["rows"] = sorted(
        rows,
        key=lambda row: (
            import_review_priority(row),
            row.get("is_duplicate", False),
            row.get("date", ""),
            str(row.get("display_name") or row.get("description") or "").lower(),
        )
    )
    preview["summary"] = summarize_preview_rows(
        preview["rows"],
        parser_debug=preview.get("parser_debug"),
        parser_filtered_count=preview.get("skipped_rows", 0),
    )
    return preview


def build_import_preview(user_id, file_storages, account_id, progress_callback=None):
    if not isinstance(file_storages, list):
        file_storages = [file_storages]

    account_id = int(account_id)
    existing_fingerprints, existing_amount_matches = existing_transactions_for_duplicate_matching(user_id, account_id)
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
    active_user_rules = sorted_categorization_rules(sorted_user_rules(user_id))
    recurring_index = build_recurring_index(
        Transaction.query
        .filter_by(user_id=user_id)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .limit(500)
        .all()
    )
    parser_debug = []

    if progress_callback:
        progress_callback("extracting", 18)

    total_files = max(1, len(file_storages))
    for file_index, file_storage in enumerate(file_storages, start=1):
        file_type = detect_statement_file_type(file_storage)
        file_parser_debug = init_pdf_parser_debug(file_storage.filename or "statement.pdf") if file_type == "pdf" else None
        extracted, error = (
            extract_pdf_statement_data(file_storage, debug_info=file_parser_debug)
            if file_type == "pdf"
            else extract_text_statement_data(file_storage, debug_info=file_parser_debug)
            if file_type == "text"
            else extract_csv_statement_data(file_storage)
        )
        diagnostics = (extracted or {}).get("diagnostics", {})
        if diagnostics:
            parser_debug.append(diagnostics)
        elif file_parser_debug:
            parser_debug.append(file_parser_debug)
        if error:
            return None, error, parser_debug

        file_rows = extracted["rows"]
        skipped_rows += extracted.get("skipped_rows", 0)
        detected_columns.append({
            "document": file_storage.filename or ("statement.pdf" if file_type == "pdf" else "statement.csv"),
            **extracted.get("detected_columns", {})
        })
        file_summaries.append({
            "name": file_storage.filename or ("statement.pdf" if file_type == "pdf" else "statement.csv"),
            "file_type": "TEXT" if file_type == "text" else file_type.upper(),
            "row_count": len(file_rows),
            "parser_used": diagnostics.get("parser_used", "rule_based"),
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
            normalized_desc = derive_normalized_description(raw_description or description)
            guessed_merchant = derive_merchant_guess(raw_description or description)
            matching_rules = [
                rule for rule in active_user_rules
                if normalized_desc and amount is not None and categorization_rule_matches(normalized_desc, amount, rule)
            ]
            matched_action_rule = matching_rules[0] if matching_rules else None

            fingerprint = row.get("fingerprint") or (
                transaction_fingerprint(parsed_date, description, amount, merchant_guess=guessed_merchant or description)
                if parsed_date and description and amount is not None
                else f"manual|{row_counter}|{normalize_text(row.get('raw_source'))}"
            )
            exact_duplicate_match = existing_fingerprints.get(fingerprint) if parsed_date and description and amount is not None else None
            is_existing_duplicate = bool(exact_duplicate_match)
            is_file_duplicate = fingerprint in preview_fingerprints
            preview_fingerprints.add(fingerprint)
            possible_duplicate_match = None
            if not is_existing_duplicate and not is_file_duplicate and parsed_date and amount is not None:
                possible_duplicate_match = probable_duplicate_match(
                    parsed_date,
                    amount,
                    normalize_text(guessed_merchant or normalized_desc or description),
                    existing_amount_matches,
                )

            source_category = (row.get("source_category") or "").strip()
            source_subcategory = (row.get("source_subcategory") or "").strip()
            if parsed_date and description and amount is not None and not source_category:
                categorization = categorize_transaction_detailed(
                    user_id,
                    raw_description or description,
                    amount,
                    tx_date=parsed_date,
                    recurring_index=recurring_index,
                )
                detected_category = categorization["category"]
                detected_subcategory = categorization.get("subcategory", "")
                category_source = categorization["category_source"]
            else:
                categorization = None
                detected_category, detected_subcategory, category_source = ("Needs Review", "", "Needs Review")
            detected_category, detected_subcategory = canonical_category_pair(source_category or detected_category, source_subcategory or detected_subcategory)
            matched_rule_id = (categorization or {}).get("matched_rule_id") or (getattr(matched_action_rule, "id", None) if matched_action_rule else None)
            action_source = transaction_rule_actions(matched_action_rule) if matched_action_rule else {}
            rule_applied_display_name = normalize_rule_display_name((categorization or {}).get("rule_display_name", "") or action_source.get("display_name_override", ""))
            if rule_applied_display_name:
                description = rule_applied_display_name
            applied_rule_tags = normalize_rule_tags_value((categorization or {}).get("rule_tags", "") or action_source.get("tag_rules", ""))
            skip_by_rule = bool((categorization or {}).get("skip_transaction") or action_source.get("skip_transaction"))

            if source_category:
                detected_category, detected_subcategory = canonical_category_pair(source_category, source_subcategory)
                category_source = "PDF Type" if file_type == "pdf" else "CSV"
            elif category_source == "Fallback":
                detected_category = "Needs Review"
                detected_subcategory = ""
                category_source = "Needs Review"

            requires_manual_fields = row.get("requires_manual_fields", False) or parsed_date is None or not description or amount is None
            review_required = requires_manual_fields or (detected_category or "").strip().lower() in GENERIC_CATEGORIES or category_source == "Needs Review"
            normalized_detected_category = (detected_category or "").strip().lower()
            auto_approved = False
            review_reasons = []

            if parsed_date is None:
                review_reasons.append("Invalid Date")
            if amount is None:
                review_reasons.append("Invalid Amount")
            if not description:
                review_reasons.append("Description Too Noisy")

            if source_category and file_type == "pdf":
                confidence_label = "High confidence"
                confidence_tone = "positive"
                confidence_detail = "Detected directly from the statement transaction row."
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
            elif categorization:
                confidence_bucket = categorization.get("category_confidence") or "low"
                if confidence_bucket == "high":
                    confidence_label = "High confidence"
                    confidence_tone = "positive"
                    confidence_detail = f"Matched using {category_source.lower()}."
                    auto_approved = True
                elif confidence_bucket == "medium":
                    confidence_label = "Moderate confidence"
                    confidence_tone = "info"
                    confidence_detail = f"Suggested using {category_source.lower()}."
                elif confidence_bucket == "low":
                    confidence_label = "Low confidence"
                    confidence_tone = "warning"
                    confidence_detail = f"{category_source} found a possible match, but this row should still be reviewed."
                else:
                    confidence_label = "Uncategorized"
                    confidence_tone = "warning"
                    confidence_detail = "No rule or saved merchant match was strong enough yet."
            elif normalized_detected_category in GENERIC_CATEGORIES or category_source == "Needs Review":
                confidence_label = "Uncategorized"
                confidence_tone = "warning"
                confidence_detail = "No rule or saved merchant match was strong enough yet."
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

            if len(matching_rules) > 1:
                review_reasons.append("Multiple Possible Matches")
            if normalized_detected_category in GENERIC_CATEGORIES:
                review_reasons.append("No Rule Match")
            if confidence_bucket in {"low", "medium"} and not requires_manual_fields:
                review_reasons.append("Low Confidence")
            if (
                confidence_bucket in {"low", "uncategorized"}
                and not matched_memory
                and not guessed_merchant
                and not row.get("source_category")
            ):
                review_reasons.append("Unknown Merchant")
            elif (
                confidence_bucket in {"low", "uncategorized"}
                and not matched_memory
                and category_source == "Needs Review"
            ):
                review_reasons.append("Unknown Merchant")
            if (
                len((raw_description or "").split()) >= 7
                and len((description or "").split()) <= 3
                and not requires_manual_fields
            ):
                review_reasons.append("Description Too Noisy")
            if (categorization.get("transaction_subtype") or "").strip().lower() == "transfer" and review_required:
                review_reasons.append("Possible Transfer")
            if (categorization.get("transaction_subtype") or "").strip().lower() == "payment" and review_required:
                review_reasons.append("Possible Credit Card Payment")
            if possible_duplicate_match:
                review_reasons.append("Duplicate Candidate")
                review_required = True

            if is_existing_duplicate:
                duplicate_existing_count += 1
                row_status = "Duplicate"
                status_tone = "warning"
                default_row_action = "skip"
                review_reasons = ["Already Imported"]
            elif is_file_duplicate:
                duplicate_file_count += 1
                row_status = "Duplicate"
                status_tone = "warning"
                default_row_action = "skip"
                review_reasons = ["Duplicate In File"]
            elif skip_by_rule:
                row_status = "Skipped"
                status_tone = "info"
                default_row_action = "skip"
                review_required = False
                auto_approved = True
                review_reasons = ["Skipped by Rule"]
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
                row_status = "Auto-Approved" if auto_approved else "Ready"
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
            if categorization and (categorization.get("transaction_subtype") or "").strip().lower() in VALID_TRANSACTION_SUBTYPES:
                row_kind = categorization["transaction_subtype"].strip().lower()
            elif matched_memory and (matched_memory.subtype or "").strip().lower() in VALID_TRANSACTION_SUBTYPES:
                row_kind = matched_memory.subtype.strip().lower()
            elif detected_category == "Subscriptions / Bills" and canonical_subcategory_name(detected_subcategory) == "Credit Card Payment":
                row_kind = "payment"
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
            suggestion_confidence = None
            if categorization and categorization.get("confidence_score") is not None:
                suggestion_confidence = round(float(categorization.get("confidence_score") or 0) * 100)
            elif row.get("parser_confidence") is not None:
                suggestion_confidence = round(float(row.get("parser_confidence") or 0) * 100)
            suggestion_reason = confidence_detail
            if category_source == "Merchant Memory":
                suggestion_reason = f"Matched saved merchant {guessed_merchant or description}."
            elif category_source.startswith("Rule"):
                rule_pattern = (categorization or {}).get("matched_rule_pattern") or (guessed_merchant or normalized_desc or description)
                rule_type = ((categorization or {}).get("matched_rule_type") or "").replace("_", " ")
                suggestion_reason = f"Matched {rule_type + ' ' if rule_type else ''}rule {rule_pattern}."
            elif category_source == "Recurring Pattern":
                suggestion_reason = "Matched a recurring pattern from prior transactions."
            elif category_source.startswith("Heuristic"):
                suggestion_reason = f"Suggested from {category_source.lower()}."
            elif category_source == "PDF Type":
                suggestion_reason = "Derived from the statement transaction type."
            elif category_source == "CSV":
                suggestion_reason = "Taken from the imported file."
            preview_rows.append({
                "row_id": row_counter,
                "source_document": row.get("source_document") or (file_storage.filename or ""),
                "parser_label": row.get("parser_label") or file_type.upper(),
                "raw_source": row.get("raw_source") or "",
                "raw_description": raw_description,
                "date": date_value,
                "description": description,
                "display_name": description,
                "normalized_description": normalized_desc,
                "merchant_guess": guessed_merchant,
                "amount": amount_value,
                "tags": applied_rule_tags,
                "category": detected_category,
                "subcategory": detected_subcategory,
                "source_category": source_category,
                "category_source": category_source,
                "suggested_category_id": categorization.get("suggested_category_id") if categorization else None,
                "suggested_subcategory_id": categorization.get("suggested_subcategory_id") if categorization else None,
                "matched_rule_id": matched_rule_id,
                "row_status": row_status,
                "status_tone": status_tone,
                "status_label": row_status,
                "is_duplicate": is_existing_duplicate or is_file_duplicate,
                "duplicate_reason": "already_imported" if is_existing_duplicate else "duplicate_in_file" if is_file_duplicate else "",
                "duplicate_status_label": "Already Imported" if is_existing_duplicate else "Duplicate In File" if is_file_duplicate else "",
                "is_possible_duplicate": bool(possible_duplicate_match),
                "duplicate_match": exact_duplicate_match or possible_duplicate_match,
                "review_required": review_required,
                "needs_review": review_required,
                "review_state": "needs_review" if review_required else "reviewed",
                "requires_manual_fields": requires_manual_fields,
                "manual_reason": row.get("manual_reason", ""),
                "confidence_label": confidence_label,
                "confidence_tone": confidence_tone,
                "confidence_detail": confidence_detail,
                "confidence_bucket": confidence_bucket,
                "suggested_category": detected_category,
                "suggested_subcategory": detected_subcategory,
                "suggestion_confidence_percent": suggestion_confidence,
                "suggestion_reason": suggestion_reason,
                "review_reasons": list(dict.fromkeys(reason for reason in review_reasons if reason)),
                "auto_approved": auto_approved and not review_required and not (is_existing_duplicate or is_file_duplicate),
                "is_uncategorized": normalized_detected_category in GENERIC_CATEGORIES,
                "is_low_confidence": confidence_bucket == "low",
                "default_row_action": default_row_action,
                "fingerprint": fingerprint,
                "row_kind": row_kind,
                "parser_source": row.get("parser_source") or ("manual" if file_type == "text" else "rule_based"),
                "parser_confidence": row.get("parser_confidence", 0),
                "parser_warnings": row.get("parser_warnings", []),
                "post_date": row.get("post_date", ""),
            })
            row_counter += 1
            if progress_callback and processed_rows == total_rows:
                progress_callback("saving", 86)

    if not preview_rows:
        return None, "No valid transactions were detected in the uploaded files.", parser_debug

    preview_rows = sorted(
        preview_rows,
        key=lambda row: (
            import_review_priority(row),
            row.get("is_duplicate", False),
            row.get("date", ""),
            str(row.get("display_name") or row.get("description") or "").lower(),
        )
    )

    summary = summarize_preview_rows(
        preview_rows,
        parser_debug=parser_debug,
        parser_filtered_count=skipped_rows,
    )
    payload = {
        "account_id": account_id,
        "filenames": [summary["name"] for summary in file_summaries],
        "rows": preview_rows,
        "detected_columns": detected_columns,
        "file_summaries": file_summaries,
        "parser_debug": parser_debug,
        "skipped_rows": skipped_rows,
        "summary": summary,
    }
    return payload, None, parser_debug


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

    remaining_recurring_bills = estimate_remaining_recurring_charges(
        subscriptions,
        selected_month,
        selected_year,
        current_day,
        confirmed_only=True,
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


ACCOUNT_GROUP_DEFINITIONS = [
    {
        "key": "checking",
        "label": "Checking",
        "subtypes": {"checking"},
        "types": {"asset"},
        "default_open": False,
        "tone": "asset",
    },
    {
        "key": "savings",
        "label": "Savings",
        "subtypes": {"savings"},
        "types": {"asset"},
        "default_open": False,
        "tone": "asset",
    },
    {
        "key": "investments",
        "label": "Investments",
        "subtypes": {"investment"},
        "types": {"asset"},
        "default_open": False,
        "tone": "asset",
    },
    {
        "key": "credit_cards",
        "label": "Credit Cards",
        "subtypes": {"credit_card"},
        "types": {"liability"},
        "default_open": False,
        "tone": "liability",
    },
    {
        "key": "loans",
        "label": "Loans / Liabilities",
        "subtypes": {"loan"},
        "types": {"liability"},
        "default_open": False,
        "tone": "liability",
    },
    {
        "key": "other_assets",
        "label": "Other Assets",
        "subtypes": {"cash", "other_asset"},
        "types": {"asset"},
        "default_open": False,
        "tone": "asset",
    },
    {
        "key": "other",
        "label": "Other",
        "subtypes": {"other_liability"},
        "types": {"liability"},
        "default_open": False,
        "tone": "liability",
    },
]


def account_group_key(account):
    subtype = infer_account_subtype(account)
    account_type = (getattr(account, "type", "") or "").strip().lower()
    if account_type == "asset":
        if subtype == "checking":
            return "checking"
        if subtype == "savings":
            return "savings"
        if subtype == "investment":
            return "investments"
        return "other_assets"
    if subtype == "credit_card":
        return "credit_cards"
    if subtype == "loan":
        return "loans"
    return "other"


def build_account_groups(accounts):
    grouped = defaultdict(list)
    for account in accounts or []:
        grouped[account_group_key(account)].append(account)

    groups = []
    for definition in ACCOUNT_GROUP_DEFINITIONS:
        group_accounts = sorted(
            grouped.get(definition["key"], []),
            key=lambda account: (-abs(float(account.balance or 0)), (account.name or "").lower()),
        )
        if not group_accounts:
            continue
        total_balance = round(sum(abs(float(account.balance or 0)) for account in group_accounts), 2)
        groups.append({
            "key": definition["key"],
            "label": definition["label"],
            "accounts": group_accounts,
            "count": len(group_accounts),
            "total_balance": total_balance,
            "default_open": definition["default_open"],
            "tone": definition["tone"],
        })
    return groups


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
    today = date.today()
    for goal in goals:
        allocation_rows = goal_allocation_rows(goal, wealth_context)
        current_amount, source_label = resolve_goal_current_amount(goal, wealth_context)
        target_amount = max(float(goal.target_amount or 0), 0)
        progress_pct = min(100.0, (current_amount / target_amount) * 100) if target_amount > 0 else 0
        gap_remaining = max(target_amount - current_amount, 0)
        target_date = goal.target_date
        months_remaining = None
        weeks_remaining = None
        required_monthly_savings = None
        required_weekly_savings = None
        timeline_warning = ""
        if target_date:
            days_remaining = (target_date - today).days
            if days_remaining < 0 and gap_remaining > 0:
                timeline_warning = "Target date passed"
            elif gap_remaining <= 0:
                timeline_warning = "Goal funded"
            else:
                months_remaining = max(1, math.ceil((days_remaining + 1) / 30.44))
                weeks_remaining = max(1, math.ceil((days_remaining + 1) / 7))
                required_monthly_savings = round(gap_remaining / months_remaining, 2)
                required_weekly_savings = round(gap_remaining / weeks_remaining, 2)
        goal_rows.append({
            "id": goal.id,
            "name": goal.name,
            "goal_type": goal.goal_type,
            "target_amount": round(target_amount, 2),
            "current_amount": round(current_amount, 2),
            "target_date": target_date,
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
            "months_remaining": months_remaining,
            "weeks_remaining": weeks_remaining,
            "required_monthly_savings": required_monthly_savings,
            "required_weekly_savings": required_weekly_savings,
            "timeline_warning": timeline_warning,
        })
        if target_amount > 0:
            total_progress_ratio += min(current_amount / target_amount, 1.0)

    average_progress = (total_progress_ratio / len(goal_rows)) if goal_rows else None
    return goal_rows, average_progress


def goal_focus_summary(goal_rows):
    active_goals = [goal for goal in (goal_rows or []) if float(goal.get("target_amount") or 0) > 0]
    funded_goals = [goal for goal in active_goals if float(goal.get("gap_remaining") or 0) <= 0]
    total_remaining = round(sum(float(goal.get("gap_remaining") or 0) for goal in active_goals), 2)
    total_required_monthly = round(
        sum(float(goal.get("required_monthly_savings") or 0) for goal in active_goals if goal.get("required_monthly_savings") is not None),
        2,
    )
    return {
        "active_goal_count": len(active_goals),
        "funded_goal_count": len(funded_goals),
        "total_remaining": total_remaining,
        "total_required_monthly": total_required_monthly,
    }


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
    ordered_labels = [
        "Checking",
        "Savings",
        "Cash",
        "Investments",
        "Credit Cards",
        "Loans",
        "Other Assets",
        "Other Liabilities",
    ]
    grouped_balances = {label: 0.0 for label in ordered_labels}

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

        grouped_balances[label] = grouped_balances.get(label, 0.0) + amount

    labels = [label for label in ordered_labels if grouped_balances.get(label, 0) > 0]
    values = [round(grouped_balances[label], 2) for label in labels]
    return labels, values


def account_type_bucket_label(account):
    subtype = infer_account_subtype(account)
    if account.type == "liability":
        if subtype == "credit_card":
            return "Credit Cards"
        if subtype == "loan":
            return "Loans"
        return "Other Liabilities"
    if subtype == "checking":
        return "Checking"
    if subtype == "cash":
        return "Cash"
    if subtype == "savings":
        return "Savings"
    if subtype == "investment":
        return "Investments"
    return "Other Assets"


def wealth_breakdown_drilldown(accounts):
    ordered_labels = [
        "Checking",
        "Savings",
        "Cash",
        "Investments",
        "Credit Cards",
        "Loans",
        "Other Assets",
        "Other Liabilities",
    ]
    grouped = defaultdict(lambda: {"total": 0.0, "accounts": []})
    for account in accounts or []:
        amount = abs(float(account.balance or 0))
        if amount <= 0:
            continue
        label = account_type_bucket_label(account)
        grouped[label]["total"] += amount
        grouped[label]["accounts"].append({
            "account_id": account.id,
            "name": account.name,
            "balance": round(float(account.balance or 0), 2),
            "display_balance": round(amount, 2),
            "type": account.type,
            "subtype": infer_account_subtype(account),
            "detail_url": url_for("account_detail", account_id=account.id),
        })

    payload = {}
    for label in ordered_labels:
        row = grouped.get(label)
        if not row:
            continue
        accounts_sorted = sorted(row["accounts"], key=lambda item: abs(float(item["balance"] or 0)), reverse=True)
        payload[label] = {
            "label": label,
            "total": round(row["total"], 2),
            "account_count": len(accounts_sorted),
            "accounts": accounts_sorted,
        }
    return payload


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


def current_goal_funding_total(goal):
    allocation_rows = GoalAllocation.query.filter_by(goal_id=goal.id).order_by(GoalAllocation.id.asc()).all()
    if allocation_rows:
        return round(sum(float(row.allocated_amount or 0) for row in allocation_rows), 2), allocation_rows

    legacy_allocated = float(getattr(goal, "allocated_amount", 0) or 0)
    if getattr(goal, "linked_account_id", None) and legacy_allocated > 0:
        return round(legacy_allocated, 2), []

    return round(float(goal.current_amount or 0), 2), []


def set_goal_funding_total(goal, new_total):
    normalized_total = round(max(float(new_total or 0), 0), 2)
    allocation_rows = GoalAllocation.query.filter_by(goal_id=goal.id).order_by(GoalAllocation.id.asc()).all()

    if allocation_rows:
        existing_total = sum(float(row.allocated_amount or 0) for row in allocation_rows)
        if normalized_total <= 0:
            for row in allocation_rows:
                db.session.delete(row)
            goal.linked_account_id = None
            goal.allocated_amount = 0
            goal.current_amount = 0
            return

        if existing_total <= 0 or len(allocation_rows) == 1:
            allocation_rows[0].allocated_amount = normalized_total
            for extra_row in allocation_rows[1:]:
                db.session.delete(extra_row)
            goal.linked_account_id = allocation_rows[0].account_id
            goal.allocated_amount = normalized_total
            goal.current_amount = normalized_total
            return

        remaining_total = normalized_total
        for index, row in enumerate(allocation_rows):
            if index == len(allocation_rows) - 1:
                updated_amount = round(max(remaining_total, 0), 2)
            else:
                share_ratio = float(row.allocated_amount or 0) / existing_total if existing_total > 0 else 0
                updated_amount = round(normalized_total * share_ratio, 2)
                remaining_total = round(remaining_total - updated_amount, 2)
            row.allocated_amount = updated_amount

        goal.linked_account_id = allocation_rows[0].account_id if len(allocation_rows) == 1 else None
        goal.allocated_amount = normalized_total if len(allocation_rows) == 1 else 0
        goal.current_amount = normalized_total
        return

    if getattr(goal, "linked_account_id", None):
        goal.allocated_amount = normalized_total
        goal.current_amount = normalized_total
        if normalized_total <= 0:
            goal.linked_account_id = None
            goal.allocated_amount = 0
        return

    goal.current_amount = normalized_total


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


def monthly_overview_drilldowns(transactions, accounts, limit=6):
    account_name_map = {account.id: account.name for account in (accounts or [])}
    bucket_map = defaultdict(lambda: {
        "year": None,
        "month": None,
        "income_total": 0.0,
        "expense_total": 0.0,
        "income_categories": defaultdict(float),
        "expense_categories": defaultdict(float),
        "income_accounts": defaultdict(float),
        "expense_accounts": defaultdict(float),
        "top_income_transactions": [],
        "top_expense_transactions": [],
    })

    for tx in transactions or []:
        if not getattr(tx, "date", None):
            continue
        key = (tx.date.year, tx.date.month)
        bucket = bucket_map[key]
        bucket["year"] = tx.date.year
        bucket["month"] = tx.date.month
        tx_subtype = (getattr(tx, "transaction_subtype", "") or transaction_subtype_for(tx.amount, tx.category, getattr(tx, "category_source", ""))).strip().lower()
        amount = float(tx.amount or 0)
        category_name = transaction_ui_category(getattr(tx, "category", "") or "")
        if not category_name or category_name == "Needs Review":
            category_name = "Other"
        account_name = account_name_map.get(tx.account_id, "Unassigned Account")
        transaction_row = {
            "id": tx.id,
            "display_name": transaction_display_name(tx) or "Transaction",
            "date": tx.date.isoformat() if getattr(tx, "date", None) else "",
            "amount": round(abs(amount), 2),
            "account_name": account_name,
            "category": category_name,
        }

        if tx_subtype == "income" and amount > 0:
            bucket["income_total"] += amount
            bucket["income_categories"][category_name] += amount
            bucket["income_accounts"][account_name] += amount
            bucket["top_income_transactions"].append(transaction_row)
        elif tx_subtype == "expense" and amount < 0:
            normalized_amount = abs(amount)
            bucket["expense_total"] += normalized_amount
            bucket["expense_categories"][category_name] += normalized_amount
            bucket["expense_accounts"][account_name] += normalized_amount
            bucket["top_expense_transactions"].append(transaction_row)

    if not bucket_map:
        return {}

    ordered_keys = sorted(bucket_map.keys())[-limit:]
    payload = {}
    for year, month in ordered_keys:
        label = f"{calendar.month_abbr[month]} {str(year)[-2:]}"
        bucket = bucket_map[(year, month)]
        start_date, end_date = month_date_range(month, year)
        payload[label] = {
            "label": label,
            "month_label": f"{calendar.month_name[month]} {year}",
            "start_date": start_date,
            "end_date": end_date,
            "income_total": round(bucket["income_total"], 2),
            "expense_total": round(bucket["expense_total"], 2),
            "income_categories": [
                {
                    "label": name,
                    "amount": round(total, 2),
                    "url": transactions_filter_url(
                        category=name,
                        subtype="income",
                        start_date=start_date,
                        end_date=end_date,
                        preserve_current=True,
                    ),
                }
                for name, total in sorted(bucket["income_categories"].items(), key=lambda item: item[1], reverse=True)
            ],
            "expense_categories": [
                {
                    "label": name,
                    "amount": round(total, 2),
                    "url": transactions_filter_url(
                        category=name,
                        subtype="expense",
                        start_date=start_date,
                        end_date=end_date,
                        preserve_current=True,
                    ),
                }
                for name, total in sorted(bucket["expense_categories"].items(), key=lambda item: item[1], reverse=True)
            ],
            "income_accounts": [
                {"label": name, "amount": round(total, 2)}
                for name, total in sorted(bucket["income_accounts"].items(), key=lambda item: item[1], reverse=True)
            ],
            "expense_accounts": [
                {"label": name, "amount": round(total, 2)}
                for name, total in sorted(bucket["expense_accounts"].items(), key=lambda item: item[1], reverse=True)
            ],
            "top_income_transactions": sorted(bucket["top_income_transactions"], key=lambda item: item["amount"], reverse=True)[:5],
            "top_expense_transactions": sorted(bucket["top_expense_transactions"], key=lambda item: item["amount"], reverse=True)[:5],
        }
    return payload


def build_spending_chart_state(transactions, selected_month, selected_year, requested_month=None, requested_year=None, limit=12):
    month_buckets = defaultdict(lambda: {
        "category_totals": defaultdict(float),
        "expense_count": 0,
        "uncategorized_count": 0,
        "uncategorized_total": 0.0,
    })

    for tx in transactions or []:
        if not getattr(tx, "date", None):
            continue
        amount = float(tx.amount or 0)
        if amount >= 0:
            continue
        explicit_subtype = (getattr(tx, "transaction_subtype", "") or "").strip().lower()
        category_name = transaction_ui_category(getattr(tx, "category", "") or "")
        source_name = normalize_text(getattr(tx, "category_source", "") or "")

        if explicit_subtype in {"income", "transfer", "payment"}:
            continue

        include_as_expense = (
            explicit_subtype == "expense"
            or is_spending_category(category_name)
            or ("transfer" not in source_name and "payment" not in source_name)
        )
        if not include_as_expense:
            continue

        key = (tx.date.year, tx.date.month)
        bucket = month_buckets[key]
        normalized_amount = abs(amount)
        bucket["expense_count"] += 1
        if not category_name or category_name == "Needs Review":
            bucket["uncategorized_count"] += 1
            bucket["uncategorized_total"] += normalized_amount
            category_name = "Other"
        bucket["category_totals"][category_name] += normalized_amount

    requested_key = (
        int(requested_year or selected_year),
        int(requested_month or selected_month),
    )
    chart_key = requested_key
    chart_bucket = month_buckets.get(chart_key, {
        "category_totals": defaultdict(float),
        "expense_count": 0,
        "uncategorized_count": 0,
        "uncategorized_total": 0.0,
    })

    month_options = []
    cursor_year, cursor_month = selected_year, selected_month
    for _ in range(limit):
        month_options.append({
            "value_month": cursor_month,
            "value_year": cursor_year,
            "label": f"{calendar.month_name[cursor_month]} {cursor_year}",
            "has_data": (cursor_year, cursor_month) in month_buckets,
        })
        if cursor_month == 1:
            cursor_month = 12
            cursor_year -= 1
        else:
            cursor_month -= 1

    chart_labels = list(chart_bucket["category_totals"].keys())
    chart_values = [round(chart_bucket["category_totals"][label], 2) for label in chart_labels]
    chart_month_label = f"{calendar.month_name[chart_key[1]]} {chart_key[0]}"

    if chart_bucket["expense_count"] <= 0:
        empty_message = "No categorized spending for this month"
        notice = f"Choose another month to see recent spending history."
    elif chart_bucket["uncategorized_count"] > 0 and chart_bucket["uncategorized_count"] == chart_bucket["expense_count"]:
        empty_message = ""
        notice = f"Showing {chart_month_label} spending grouped under Other until categories are cleaned up."
    elif chart_bucket["uncategorized_count"] > 0:
        empty_message = ""
        notice = f"{chart_bucket['uncategorized_count']} expense transaction{'s' if chart_bucket['uncategorized_count'] != 1 else ''} are grouped under Other in {chart_month_label}."
    else:
        empty_message = ""
        notice = f"Showing spending for {chart_month_label}."

    return {
        "labels": chart_labels,
        "values": chart_values,
        "month_label": chart_month_label,
        "month": chart_key[1],
        "year": chart_key[0],
        "expense_count": chart_bucket["expense_count"],
        "uncategorized_count": chart_bucket["uncategorized_count"],
        "uncategorized_total": round(chart_bucket["uncategorized_total"], 2),
        "empty_message": empty_message or "No categorized spending yet",
        "notice": notice,
        "month_options": month_options,
        "used_fallback": False,
    }


def build_spending_category_drilldown(transactions, accounts, category_name, month, year, limit=12):
    requested_category = (category_name or "").strip()
    try:
        month = int(month or 0)
        year = int(year or 0)
    except (TypeError, ValueError):
        return {
            "category": requested_category or "Category",
            "month": 0,
            "year": 0,
            "month_label": "Selected month",
            "total_amount": 0.0,
            "transaction_count": 0,
            "average_amount": 0.0,
            "top_merchants": [],
            "account_breakdown": [],
            "transactions": [],
        }
    account_name_map = {account.id: account.name for account in (accounts or [])}
    total_amount = 0.0
    transaction_count = 0
    merchant_totals = defaultdict(float)
    merchant_counts = defaultdict(int)
    account_totals = defaultdict(float)
    account_counts = defaultdict(int)
    matching_transactions = []

    for tx in transactions or []:
        if not getattr(tx, "date", None) or tx.date.month != month or tx.date.year != year:
            continue
        amount = float(tx.amount or 0)
        if amount >= 0:
            continue
        explicit_subtype = (getattr(tx, "transaction_subtype", "") or "").strip().lower()
        tx_category = transaction_ui_category(getattr(tx, "category", "") or "")
        source_name = normalize_text(getattr(tx, "category_source", "") or "")
        if explicit_subtype in {"income", "transfer", "payment"}:
            continue
        include_as_expense = (
            explicit_subtype == "expense"
            or is_spending_category(tx_category)
            or ("transfer" not in source_name and "payment" not in source_name)
        )
        if not include_as_expense:
            continue
        if not tx_category or tx_category == "Needs Review":
            tx_category = "Other"
        if tx_category != requested_category:
            continue

        normalized_amount = abs(amount)
        total_amount += normalized_amount
        transaction_count += 1
        merchant_name = (
            (getattr(tx, "merchant_guess", "") or "").strip()
            or (transaction_display_name(tx) or "").strip()
            or "Transaction"
        )
        display_name = (transaction_display_name(tx) or merchant_name or "Transaction").strip()
        account_name = account_name_map.get(tx.account_id, "Unassigned Account")
        merchant_totals[merchant_name] += normalized_amount
        merchant_counts[merchant_name] += 1
        account_totals[account_name] += normalized_amount
        account_counts[account_name] += 1
        matching_transactions.append({
            "id": tx.id,
            "date": tx.date.isoformat() if tx.date else "",
            "display_name": display_name,
            "raw_description": transaction_raw_description(tx) or "",
            "account_name": account_name,
            "amount": round(normalized_amount, 2),
            "category": tx_category,
        })

    top_merchants = [
        {
            "label": label,
            "amount": round(amount, 2),
            "count": merchant_counts[label],
        }
        for label, amount in sorted(merchant_totals.items(), key=lambda item: item[1], reverse=True)[:5]
    ]
    account_breakdown = [
        {
            "label": label,
            "amount": round(amount, 2),
            "count": account_counts[label],
        }
        for label, amount in sorted(account_totals.items(), key=lambda item: item[1], reverse=True)
    ]
    sorted_transactions = sorted(
        matching_transactions,
        key=lambda item: (item.get("date") or "", item.get("id") or 0),
        reverse=True,
    )[:limit]
    return {
        "category": requested_category or "Category",
        "month": month,
        "year": year,
        "month_label": f"{calendar.month_name[month]} {year}" if month and year else "Selected month",
        "total_amount": round(total_amount, 2),
        "transaction_count": transaction_count,
        "average_amount": round((total_amount / transaction_count), 2) if transaction_count else 0.0,
        "top_merchants": top_merchants,
        "account_breakdown": account_breakdown,
        "transactions": sorted_transactions,
    }


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
            "cadence_hit_ratio": 0,
            "cadence_target_days": 0,
            "frequency_label": "Irregular",
            "monthly_factor": 0,
            "timing_stability_days": 0,
            "interval_score": 0
        }

    avg_interval = sum(intervals) / len(intervals)
    median_interval = median_value(intervals)
    cadence_targets = (7, 14, 15, 30, 45, 60, 90)
    cadence_target = min(cadence_targets, key=lambda target: abs(median_interval - target))
    cadence_tolerance = max(2, cadence_target * 0.22)
    cadence_hits = [gap for gap in intervals if abs(gap - cadence_target) <= cadence_tolerance]
    cadence_hit_ratio = len(cadence_hits) / len(intervals)
    timing_stability_days = sum(abs(gap - median_interval) for gap in intervals) / len(intervals)
    closeness_penalty = min(abs(median_interval - cadence_target) / max(cadence_target * 0.45, 4), 1)
    variability_penalty = min(timing_stability_days / max(cadence_target * 0.35, 4), 1)
    frequency_label, monthly_factor = recurring_frequency_profile(median_interval or avg_interval or cadence_target)
    interval_score = max(
        0,
        (cadence_hit_ratio * 0.62) + ((1 - closeness_penalty) * 0.18) + ((1 - variability_penalty) * 0.2)
    )

    return {
        "avg_interval": avg_interval,
        "median_interval": median_interval,
        "monthly_hit_ratio": cadence_hit_ratio,
        "cadence_hit_ratio": cadence_hit_ratio,
        "cadence_target_days": cadence_target,
        "frequency_label": frequency_label,
        "monthly_factor": monthly_factor,
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


def recurring_reference_text(tx):
    return (
        (getattr(tx, "merchant_guess", "") or "").strip()
        or (getattr(tx, "display_name", "") or "").strip()
        or transaction_reference_description(tx)
    )


def recurring_signature(tx):
    reference = recurring_reference_text(tx)
    return normalize_merchant(reference) or normalize_text(clean_transaction_description(reference)) or normalize_text(reference)


def recurring_source_label(tx_list):
    if any((getattr(tx, "import_source", "") or "").strip().lower() == "plaid" for tx in (tx_list or [])):
        return "Bank synced"
    return "Imported history"


def analyze_subscriptions(transactions):
    merchant_groups = defaultdict(list)

    for tx in transactions:
        if not getattr(tx, "date", None):
            continue
        if not is_spending_transaction(tx):
            continue
        key = recurring_signature(tx)
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
        latest_reference = recurring_reference_text(tx_list[-1]) or merchant
        dominant_category = Counter(canonical_transaction_category(getattr(tx, "category", "")) for tx in tx_list).most_common(1)[0][0]
        reference_hint = normalize_text(latest_reference)
        subscription_hint = dominant_category == "Subscriptions / Bills" or any(
            keyword in reference_hint
            for keyword in ("subscription", "membership", "netflix", "spotify", "prime", "hulu", "icloud", "apple.com/bill")
        )
        cadence_target = interval_metrics["cadence_target_days"] or interval_metrics["median_interval"]
        monthlyish_cadence = 21 <= float(cadence_target or 0) <= 40
        count_score = min(len(tx_list) / 4, 1)
        confidence_score = (
            interval_metrics["interval_score"] * 0.5
            + amount_metrics["amount_score"] * 0.35
            + count_score * 0.15
        )

        if confidence_score < 0.55:
            continue
        if not monthlyish_cadence and not subscription_hint:
            continue
        if monthlyish_cadence and interval_metrics["monthly_hit_ratio"] < 0.55 and not subscription_hint:
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
            "name": clean_transaction_description(latest_reference).title() or merchant.title(),
            "average_amount": round(avg_amount, 2),
            "monthly_equivalent": round(avg_amount * float(interval_metrics["monthly_factor"] or 1), 2),
            "occurrences": len(tx_list),
            "estimated_yearly_cost": round(avg_amount * 12, 2),
            "next_expected_charge": next_charge,
            "last_charge": last_charge,
            "frequency": interval_metrics["frequency_label"],
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
            "flags": flags,
            "is_bank_synced": any((getattr(tx, "import_source", "") or "").strip().lower() == "plaid" for tx in tx_list),
            "source_label": recurring_source_label(tx_list),
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


RECURRING_EXPENSE_KEYWORDS = (
    "rent",
    "mortgage",
    "lease",
    "insurance",
    "internet",
    "phone",
    "wireless",
    "electric",
    "water",
    "gas bill",
    "utility",
    "gym",
    "membership",
    "subscription",
    "netflix",
    "spotify",
    "hulu",
    "youtube premium",
    "icloud",
    "apple.com/bill",
    "loan",
)

RECURRING_BILL_CATEGORY_HINTS = {
    "housing",
    "utilities",
    "health",
    "subscriptions",
}


def recurring_frequency_profile(avg_interval_days):
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
    if avg_interval_days <= 75:
        return "Every 2 months", 0.5
    if avg_interval_days <= 110:
        return "Quarterly", 1 / 3
    return "Irregular", 0


def recurring_income_frequency(avg_interval_days):
    return recurring_frequency_profile(avg_interval_days)


def is_candidate_recurring_expense(tx):
    if not is_spending_transaction(tx):
        return False

    amount = float(getattr(tx, "amount", 0) or 0)
    if amount >= 0:
        return False

    subtype = (getattr(tx, "transaction_subtype", "") or "").strip().lower()
    if subtype and subtype != "expense":
        return False

    category = normalize_text(getattr(tx, "category", ""))
    if category in {"needs review", "transfer", "credit card payment", "income", "cash withdrawal", "savings"}:
        return False

    raw_description = normalize_text(recurring_reference_text(tx))
    if not raw_description:
        return False
    if any(keyword in raw_description for keyword in INTERNAL_TRANSFER_EXCLUDE_KEYWORDS):
        return False
    if "payment thank you" in raw_description or "autopay payment" in raw_description:
        return False
    return True


def recurring_expense_hint_score(reference_text, category_name):
    normalized_reference = normalize_text(reference_text)
    normalized_category = normalize_text(category_name)
    score = 0.0
    if normalized_category in RECURRING_BILL_CATEGORY_HINTS:
        score += 0.16
    if any(keyword in normalized_reference for keyword in RECURRING_EXPENSE_KEYWORDS):
        score += 0.18
    if normalized_category == "subscriptions":
        score += 0.08
    return min(score, 0.3)


def recurring_expense_kind_label(category_name, reference_text):
    normalized_category = normalize_text(category_name)
    normalized_reference = normalize_text(reference_text)
    if normalized_category == "subscriptions / bills" or any(keyword in normalized_reference for keyword in ("subscription", "membership", "netflix", "spotify", "prime")):
        return "Subscription"
    if normalized_category in RECURRING_BILL_CATEGORY_HINTS or any(keyword in normalized_reference for keyword in ("rent", "mortgage", "insurance", "internet", "phone", "utility", "loan")):
        return "Bill"
    return "Recurring expense"


def analyze_recurring_expenses(transactions):
    merchant_groups = defaultdict(list)

    for tx in transactions or []:
        if not getattr(tx, "date", None):
            continue
        if not is_candidate_recurring_expense(tx):
            continue
        key = recurring_signature(tx)
        if not key:
            continue
        merchant_groups[key].append(tx)

    recurring_expenses = []

    for merchant, tx_list in merchant_groups.items():
        if len(tx_list) < 2:
            continue

        tx_list.sort(key=lambda x: x.date)
        intervals = [(tx_list[i].date - tx_list[i - 1].date).days for i in range(1, len(tx_list))]
        amounts = [abs(float(t.amount or 0)) for t in tx_list if t.date]
        if not amounts:
            continue

        interval_metrics = subscription_interval_metrics(intervals)
        amount_metrics = subscription_amount_metrics(amounts)
        median_interval = interval_metrics["median_interval"] or interval_metrics["avg_interval"] or 30
        frequency_label, monthly_factor = recurring_frequency_profile(median_interval)
        if monthly_factor <= 0:
            continue

        category_counter = Counter(
            canonical_category_pair(getattr(tx, "category", ""), getattr(tx, "subcategory", ""))
            for tx in tx_list
        )
        (category_name, subcategory_name), _ = category_counter.most_common(1)[0]
        latest_reference = recurring_reference_text(tx_list[-1]) or merchant
        avg_amount = round(amount_metrics["average_amount"], 2)
        count_score = min(len(tx_list) / 5, 1)
        hint_score = recurring_expense_hint_score(latest_reference, category_name)
        confidence_score = (
            interval_metrics["interval_score"] * 0.42
            + amount_metrics["amount_score"] * 0.28
            + count_score * 0.15
            + hint_score
        )

        strong_category_hint = normalize_text(category_name) in RECURRING_BILL_CATEGORY_HINTS
        strong_keyword_hint = recurring_expense_hint_score(latest_reference, category_name) >= 0.18
        monthly_or_slower = float(median_interval or 0) >= 21
        if interval_metrics["cadence_hit_ratio"] < 0.45 or confidence_score < 0.56:
            continue
        if avg_amount < 15 and not strong_category_hint and not strong_keyword_hint and confidence_score < 0.8:
            continue
        if not strong_category_hint and not strong_keyword_hint and not monthly_or_slower and avg_amount < 40:
            continue

        last_charge = tx_list[-1].date
        next_expected = last_charge + timedelta(days=max(1, round(median_interval)))
        monthly_equivalent = round(avg_amount * monthly_factor, 2)
        kind_label = recurring_expense_kind_label(category_name, latest_reference)
        is_confirmed = confidence_score >= 0.78 or ((strong_category_hint or strong_keyword_hint) and confidence_score >= 0.72)
        status_label = "Confirmed recurring bill" if is_confirmed else "Likely recurring bill"
        confidence_label = "High confidence" if confidence_score >= 0.82 else "Moderate confidence" if confidence_score >= 0.68 else "Emerging pattern"

        recurring_expenses.append({
            "name": clean_transaction_description(latest_reference).title() or merchant.title(),
            "category": category_name,
            "subcategory": subcategory_name,
            "kind_label": kind_label,
            "average_amount": avg_amount,
            "monthly_equivalent": monthly_equivalent,
            "occurrences": len(tx_list),
            "frequency": frequency_label,
            "last_charge": last_charge,
            "next_expected_date": next_expected,
            "avg_interval_days": round(interval_metrics["avg_interval"], 1),
            "median_interval_days": round(median_interval, 1),
            "cadence_target_days": round(interval_metrics["cadence_target_days"] or 0, 1),
            "cadence_hit_ratio": round(interval_metrics["cadence_hit_ratio"] * 100, 1),
            "timing_stability_days": round(interval_metrics["timing_stability_days"], 1),
            "amount_tolerance_pct": round(amount_metrics["amount_tolerance_pct"], 1),
            "confidence_score": round(confidence_score * 100, 1),
            "confidence_label": confidence_label,
            "status_label": status_label,
            "is_confirmed": is_confirmed,
            "latest_account_id": tx_list[-1].account_id,
            "is_bank_synced": any((getattr(tx, "import_source", "") or "").strip().lower() == "plaid" for tx in tx_list),
            "source_label": recurring_source_label(tx_list),
        })

    recurring_expenses.sort(
        key=lambda item: (
            item["is_confirmed"],
            item["monthly_equivalent"],
            item["average_amount"],
        ),
        reverse=True,
    )
    return recurring_expenses


def recurring_expense_monthly_estimate(recurring_expenses, confirmed_only=True):
    eligible = [
        item for item in (recurring_expenses or [])
        if not confirmed_only or item.get("is_confirmed")
    ]
    return round(sum(float(item.get("monthly_equivalent") or item.get("average_amount") or 0) for item in eligible), 2)


MANUAL_UPCOMING_FREQUENCY_MONTHS = {
    "Weekly": 0,
    "Biweekly": 0,
    "Semimonthly": 0,
    "Monthly": 1,
    "Quarterly": 3,
    "Yearly": 12,
}


def serialize_optional_date(value):
    return value.isoformat() if value else ""


def add_months(base_date, months):
    if not base_date or months <= 0:
        return base_date
    target_year = base_date.year + ((base_date.month - 1 + months) // 12)
    target_month = ((base_date.month - 1 + months) % 12) + 1
    target_day = min(base_date.day, calendar.monthrange(target_year, target_month)[1])
    return date(target_year, target_month, target_day)


def next_manual_payment_due(payment, today=None):
    due_date = getattr(payment, "due_date", None)
    if not due_date:
        return None
    today = today or date.today()
    if not getattr(payment, "is_recurring", False):
        return due_date if due_date >= today else None

    frequency = (getattr(payment, "frequency", "") or "Monthly").strip() or "Monthly"
    if frequency == "Weekly":
        while due_date < today:
            due_date += timedelta(days=7)
        return due_date
    if frequency == "Biweekly":
        while due_date < today:
            due_date += timedelta(days=14)
        return due_date
    if frequency == "Semimonthly":
        while due_date < today:
            due_date += timedelta(days=15)
        return due_date

    month_interval = MANUAL_UPCOMING_FREQUENCY_MONTHS.get(frequency, 1)
    if month_interval <= 0:
        month_interval = 1
    while due_date < today:
        due_date = add_months(due_date, month_interval)
    return due_date


def previous_manual_payment_due(payment, next_due_date=None):
    due_date = getattr(payment, "due_date", None)
    if not due_date or not getattr(payment, "is_recurring", False):
        return None

    next_due_date = next_due_date or next_manual_payment_due(payment)
    if not next_due_date or next_due_date == due_date:
        return None

    frequency = (getattr(payment, "frequency", "") or "Monthly").strip() or "Monthly"
    if frequency == "Weekly":
        return next_due_date - timedelta(days=7)
    if frequency == "Biweekly":
        return next_due_date - timedelta(days=14)
    if frequency == "Semimonthly":
        return next_due_date - timedelta(days=15)

    month_interval = MANUAL_UPCOMING_FREQUENCY_MONTHS.get(frequency, 1)
    if month_interval <= 0:
        month_interval = 1
    return add_months(next_due_date, -month_interval)


def manual_upcoming_payments_for_user(user_id):
    if not table_exists("upcoming_payment"):
        return []
    try:
        return (
            UpcomingPayment.query
            .filter_by(user_id=user_id, is_active=True)
            .order_by(UpcomingPayment.due_date.asc(), UpcomingPayment.id.asc())
            .all()
        )
    except (ProgrammingError, OperationalError) as exc:
        db.session.rollback()
        log_safe_exception("Upcoming payment table is not ready yet.", exc=exc)
        return []


def manual_upcoming_event_rows(user_id, account_name_map=None, today=None):
    today = today or date.today()
    account_name_map = account_name_map or {}
    rows = []
    for payment in manual_upcoming_payments_for_user(user_id):
        next_due = next_manual_payment_due(payment, today=today)
        if not next_due:
            continue
        days_until_due = (next_due - today).days
        if not getattr(payment, "is_recurring", False) and days_until_due < 0:
            continue
        rows.append({
            "id": payment.id,
            "name": (payment.name or "Upcoming payment").strip(),
            "expected_date": next_due,
            "average_amount": round(abs(float(payment.amount or 0)), 2),
            "source_label": "Manual",
            "type_label": "Recurring" if payment.is_recurring else "Payment",
            "status_label": "Scheduled recurring payment" if payment.is_recurring else "Scheduled payment",
            "account_name": account_name_map.get(payment.account_id),
            "category": canonical_transaction_category(payment.category) if (payment.category or "").strip() else "",
            "days_until_due": days_until_due,
            "is_due_soon": 0 <= days_until_due <= 3,
            "is_overdue": days_until_due < 0,
            "is_recurring": bool(payment.is_recurring),
            "frequency": (payment.frequency or "Monthly").strip() or "Monthly",
        })
    rows.sort(key=lambda item: (item["expected_date"], item["days_until_due"], item["name"].lower()))
    return rows


def manual_recurring_subscription_rows(user_id, account_name_map=None, today=None):
    today = today or date.today()
    account_name_map = account_name_map or {}
    rows = []
    for payment in manual_upcoming_payments_for_user(user_id):
        if not getattr(payment, "is_recurring", False):
            continue
        next_due = next_manual_payment_due(payment, today=today)
        if not next_due:
            continue
        monthly_factor = MANUAL_UPCOMING_FREQUENCY_MONTHS.get((payment.frequency or "Monthly").strip() or "Monthly", 1)
        if (payment.frequency or "").strip() == "Weekly":
            monthly_factor = 52 / 12
        elif (payment.frequency or "").strip() == "Biweekly":
            monthly_factor = 26 / 12
        elif (payment.frequency or "").strip() == "Semimonthly":
            monthly_factor = 2
        elif monthly_factor > 0:
            monthly_factor = 1 / monthly_factor
        else:
            monthly_factor = 1
        amount_value = round(abs(float(payment.amount or 0)), 2)
        rows.append({
            "name": (payment.name or "Recurring payment").strip(),
            "average_amount": amount_value,
            "monthly_equivalent": round(amount_value * monthly_factor, 2),
            "occurrences": 1,
            "estimated_yearly_cost": round(amount_value * monthly_factor * 12, 2),
            "next_expected_charge": next_due,
            "last_charge": previous_manual_payment_due(payment, next_due),
            "frequency": (payment.frequency or "Monthly").strip() or "Monthly",
            "frequency_label": (payment.frequency or "Monthly").strip() or "Monthly",
            "avg_interval_days": 0,
            "median_interval_days": 0,
            "monthly_hit_ratio": 100,
            "timing_stability_days": 0,
            "latest_amount": amount_value,
            "baseline_amount": amount_value,
            "price_increase_pct": 0,
            "has_price_increase": False,
            "cancel_candidate": False,
            "overdue_days": 0,
            "amount_tolerance_pct": 0,
            "stable_amount_ratio": 100,
            "confidence_score": 100,
            "confidence_label": "Manual recurring",
            "flags": [],
            "is_bank_synced": False,
            "source_label": "Manual",
            "status_label": "Manual recurring payment",
            "account_name": account_name_map.get(payment.account_id),
            "category": canonical_transaction_category(payment.category) if (payment.category or "").strip() else "",
        })
    return rows


def build_dashboard_upcoming_payments(recurring_expenses, manual_upcoming_events, account_name_map=None, today=None, limit=6):
    today = today or date.today()
    account_name_map = account_name_map or {}
    derived_rows = []
    for item in recurring_expenses or []:
        expected_date = item.get("next_expected_date")
        if not expected_date:
            continue
        days_until_due = (expected_date - today).days
        derived_rows.append({
            "name": item.get("name") or "Upcoming payment",
            "expected_date": expected_date,
            "average_amount": round(abs(float(item.get("average_amount") or 0)), 2),
            "source_label": "Recurring",
            "type_label": item.get("kind_label") or "Bill",
            "status_label": item.get("status_label") or "Expected payment",
            "account_name": account_name_map.get(item.get("latest_account_id")),
            "category": item.get("category") or "",
            "days_until_due": days_until_due,
            "is_due_soon": 0 <= days_until_due <= 3,
            "is_overdue": days_until_due < 0,
            "is_recurring": True,
            "frequency": item.get("frequency") or "Recurring",
        })

    combined = derived_rows + list(manual_upcoming_events or [])
    combined.sort(key=lambda item: (item.get("expected_date") or date.max, item.get("days_until_due") or 9999, -float(item.get("average_amount") or 0)))
    return combined[:limit]


def estimate_remaining_recurring_charges(recurring_items, selected_month, selected_year, current_day, confirmed_only=True):
    total = 0.0
    month_end = date(selected_year, selected_month, calendar.monthrange(selected_year, selected_month)[1])
    for item in recurring_items or []:
        if confirmed_only and not item.get("is_confirmed", True):
            continue
        next_date = item.get("next_expected_date") or item.get("next_expected_charge")
        if not next_date:
            continue
        interval_days = max(1, round(float(item.get("median_interval_days") or item.get("avg_interval_days") or 30)))
        amount = max(float(item.get("average_amount") or 0), 0)
        cursor = next_date
        while cursor and cursor <= month_end:
            if cursor.year == selected_year and cursor.month == selected_month and cursor.day >= current_day:
                total += amount
            cursor = cursor + timedelta(days=interval_days)
    return round(total, 2)


def is_candidate_recurring_income(tx):
    if float(getattr(tx, "amount", 0) or 0) <= 0:
        return False

    subtype = (getattr(tx, "transaction_subtype", "") or "").strip().lower()
    if subtype and subtype != "income":
        return False

    raw_description = normalize_text(recurring_reference_text(tx))
    category = normalize_text(getattr(tx, "category", ""))
    cleaned = normalize_text(clean_transaction_description(recurring_reference_text(tx)))

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
        if not getattr(tx, "date", None):
            continue
        if not is_candidate_recurring_income(tx):
            continue
        source_key = recurring_signature(tx)
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
        source_name = clean_transaction_description(recurring_reference_text(tx_list[-1]) or source_key).title()
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
            "is_bank_synced": any((getattr(tx, "import_source", "") or "").strip().lower() == "plaid" for tx in tx_list),
            "source_label": recurring_source_label(tx_list),
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
                    previous_category = canonical_transaction_category(tx.category)
                    previous_subcategory = (getattr(tx, "subcategory", "") or "").strip()
                    tx.category = canonical_transaction_category(bulk_category)
                    tx.subcategory = ""
                    tx.category_source = "Manual Review"
                    tx.category_confidence = "high"
                    tx.needs_review = False
                    tx.transaction_subtype = transaction_subtype_for(tx.amount, tx.category, "Manual Review")
                    remember_merchant_category(
                        user_id,
                        transaction_reference_description(tx),
                        tx.category,
                        display_name=transaction_display_name(tx),
                        subtype=tx.transaction_subtype,
                    )
                    if (tx.category, tx.subcategory) != canonical_category_pair(previous_category, previous_subcategory):
                        learned_rule = upsert_learned_category_rule(
                            user_id,
                            transaction_reference_description(tx),
                            tx.category,
                            subtype=tx.transaction_subtype,
                            matched_rule_id=tx.matched_rule_id,
                        )
                        if learned_rule:
                            tx.matched_rule_id = learned_rule.id
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
            tx = scoped_record(Transaction, tx_id, user_id)
            chosen_category = (request.form.get(f"category_{tx_id}") or "").strip()
            chosen_subcategory = (request.form.get(f"subcategory_{tx_id}") or "").strip()
            if not tx:
                push_ui_feedback("That transaction is no longer available.", "danger")
            elif not chosen_category:
                push_ui_feedback("Choose a category before saving the change.", "danger")
            else:
                previous_category = canonical_transaction_category(tx.category)
                previous_subcategory = (getattr(tx, "subcategory", "") or "").strip()
                tx.category, tx.subcategory = canonical_category_pair(chosen_category, chosen_subcategory)
                tx.category_source = "Manual Review"
                tx.category_confidence = "high"
                tx.needs_review = False
                tx.transaction_subtype = transaction_subtype_for(tx.amount, tx.category, "Manual Review")
                remember_merchant_category(
                    user_id,
                    transaction_reference_description(tx),
                    tx.category,
                    subcategory=tx.subcategory,
                    display_name=transaction_display_name(tx),
                    subtype=tx.transaction_subtype,
                )
                if (tx.category, tx.subcategory) != canonical_category_pair(previous_category, previous_subcategory):
                    learned_rule = upsert_learned_category_rule(
                        user_id,
                        transaction_reference_description(tx),
                        tx.category,
                        subcategory=tx.subcategory,
                        subtype=tx.transaction_subtype,
                        matched_rule_id=tx.matched_rule_id,
                    )
                    if learned_rule:
                        tx.matched_rule_id = learned_rule.id
                log_activity(
                    user_id,
                    f"Updated category for {transaction_display_name(tx)}",
                    f"Saved as {transaction_category_label(tx)}.",
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
        category_groups=category_grouped_choices(user_id),
        subcategory_map=category_subcategory_map(),
    )


@app.route("/subscriptions")
def subscriptions():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    bootstrap_merchant_memory(user_id)
    transactions = Transaction.query.filter_by(user_id=user_id).order_by(Transaction.date.asc()).all()
    account_name_map = {
        account.id: account.name
        for account in Account.query.filter_by(user_id=user_id).all()
    }
    subscriptions = analyze_subscriptions(transactions)
    subscriptions.extend(manual_recurring_subscription_rows(user_id, account_name_map=account_name_map))
    subscriptions.sort(key=lambda sub: ((sub.get("next_expected_charge") or date.max), -float(sub.get("monthly_equivalent") or sub.get("average_amount") or 0)))
    total_monthly = sum(float(s.get("monthly_equivalent") or s.get("average_amount") or 0) for s in subscriptions)
    total_yearly = sum(s["estimated_yearly_cost"] for s in subscriptions)
    price_increase_count = sum(1 for s in subscriptions if s["has_price_increase"])
    cancel_candidate_count = sum(1 for s in subscriptions if s["cancel_candidate"])
    today = date.today()
    upcoming_subscriptions = sorted(
        [s for s in subscriptions if s.get("next_expected_charge")],
        key=lambda item: (item.get("next_expected_charge") or date.max, -float(item.get("average_amount") or 0)),
    )[:5]
    due_soon_count = sum(
        1
        for item in subscriptions
        if item.get("next_expected_charge")
        and 0 <= (item["next_expected_charge"] - today).days <= 30
    )
    due_soon_total = round(
        sum(
            float(item.get("average_amount") or 0)
            for item in subscriptions
            if item.get("next_expected_charge")
            and 0 <= (item["next_expected_charge"] - today).days <= 30
        ),
        2,
    )

    return render_template(
        "subscriptions.html",
        subs=subscriptions,
        total_monthly=round(total_monthly, 2),
        total_yearly=round(total_yearly, 2),
        price_increase_count=price_increase_count,
        cancel_candidate_count=cancel_candidate_count,
        upcoming_subscriptions=upcoming_subscriptions,
        due_soon_count=due_soon_count,
        due_soon_total=due_soon_total,
    )


@app.route("/dashboard/upcoming-payments", methods=["POST"])
def add_dashboard_upcoming_payment():
    if not require_login():
        if request_wants_json():
            return jsonify({"ok": False, "error": "Login required."}), 401
        return redirect("/login")

    limited_response = rate_limit_response(
        "dashboard_upcoming_payment_add",
        limit=12,
        window_seconds=3600,
        html_fallback="/",
        message="You've added several upcoming payments recently. Please pause for a moment before adding another.",
    )
    if limited_response:
        return limited_response

    user_id = get_user_id()
    name = (request.form.get("name") or "").strip()
    amount = safe_float(request.form.get("amount"))
    due_date = parse_date_any(request.form.get("due_date"))
    category = canonical_transaction_category((request.form.get("category") or "").strip()) if (request.form.get("category") or "").strip() else ""
    frequency = ((request.form.get("frequency") or "Monthly").strip() or "Monthly")[:40]
    is_recurring = (request.form.get("is_recurring") or "").strip().lower() in {"1", "true", "yes", "on"}
    account_id = safe_int(request.form.get("account_id"))
    account = scoped_record(Account, account_id, user_id) if account_id else None

    if not name:
        message = "Add a payment name so AkuOS can show it clearly in your timeline."
        if request_wants_json():
            return jsonify({"ok": False, "error": message}), 400
        push_ui_feedback(message, "danger")
        return redirect("/")
    if amount is None or abs(float(amount or 0)) <= 0:
        message = "Enter a payment amount greater than zero."
        if request_wants_json():
            return jsonify({"ok": False, "error": message}), 400
        push_ui_feedback(message, "danger")
        return redirect("/")
    if not due_date:
        message = "Choose a valid due date."
        if request_wants_json():
            return jsonify({"ok": False, "error": message}), 400
        push_ui_feedback(message, "danger")
        return redirect("/")
    if account_id and not account:
        message = "That account is no longer available."
        if request_wants_json():
            return jsonify({"ok": False, "error": message}), 404
        push_ui_feedback(message, "danger")
        return redirect("/")
    if not table_exists("upcoming_payment"):
        message = "Upcoming payments are not ready yet. Run maintenance or initialize the database first."
        if request_wants_json():
            return jsonify({"ok": False, "error": message}), 503
        push_ui_feedback(message, "danger")
        return redirect("/")

    payment = UpcomingPayment(
        user_id=user_id,
        name=name[:140],
        amount=round(abs(float(amount or 0)), 2),
        due_date=due_date,
        account_id=account.id if account else None,
        category=category,
        is_recurring=is_recurring,
        frequency=frequency if is_recurring else "Monthly",
        is_active=True,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    try:
        db.session.add(payment)
        db.session.commit()
    except (ProgrammingError, OperationalError) as exc:
        db.session.rollback()
        log_safe_exception("Could not save upcoming payment because the table is not ready.", exc=exc)
        message = "Upcoming payments are not ready yet. Run maintenance or initialize the database first."
        if request_wants_json():
            return jsonify({"ok": False, "error": message}), 503
        push_ui_feedback(message, "danger")
        return redirect("/")

    source_message = "Recurring payment added." if is_recurring else "Upcoming payment added."
    log_activity(
        user_id,
        f"Added {payment.name}",
        source_message,
        kind="upcoming_payment_added",
        icon="bi-calendar-plus",
        target_url="/",
    )

    payload = {
        "ok": True,
        "message": source_message,
        "payment": {
            "id": payment.id,
            "name": payment.name,
            "amount": round(float(payment.amount or 0), 2),
            "due_date": serialize_optional_date(payment.due_date),
            "account_name": account.name if account else "",
            "category": payment.category,
            "is_recurring": bool(payment.is_recurring),
            "frequency": payment.frequency,
        },
    }
    if request_wants_json():
        return jsonify(payload)
    push_ui_feedback(source_message, "success")
    return redirect("/")


@app.route("/login", methods=["GET", "POST"])
def login():
    login_error = None
    login_notice = None
    username_value = ""
    if request.method == "GET" and request.args.get("reset_status") == "updated":
        login_notice = "Password updated successfully. Sign in with your new password."
        username_value = normalize_username(request.args.get("username", ""))
    if request.method == "POST":
        limited_response = rate_limit_response(
            "login",
            limit=6,
            window_seconds=300,
            html_fallback=url_for("login"),
            message="Too many sign-in attempts. Please wait a few minutes and try again.",
        )
        if limited_response:
            return limited_response
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


@app.route("/settings/delete-all-data", methods=["POST"])
def delete_all_data():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    confirmation_text = (request.form.get("confirmation_text") or "").strip()
    if confirmation_text != "DELETE":
        push_ui_feedback("Type DELETE exactly to confirm removing all financial data.", "danger")
        return redirect(url_for("settings"))

    try:
        delete_counts = delete_user_financial_data(user_id)
        session.pop("_allocation_undo", None)
        session.pop("import_preview_id", None)
        session.pop("reopen_import_summary_job_id", None)
        verification_counts = verify_user_financial_data_cleared(user_id)
        uncleared = {label: count for label, count in verification_counts.items() if int(count or 0) > 0}
        if uncleared:
            app.logger.error(
                "Delete all data verification failed for user_id=%s. Remaining rows=%s delete_counts=%s",
                user_id,
                uncleared,
                delete_counts,
            )
            raise RuntimeError(f"Delete verification failed: {uncleared}")
        db.session.commit()
        app.logger.info(
            "Deleted all financial data for user_id=%s counts=%s verification=%s",
            user_id,
            delete_counts,
            verification_counts,
        )
    except Exception as exc:
        db.session.rollback()
        app.logger.exception("Delete all data failed for user_id=%s: %s", user_id, exc)
        push_ui_feedback("AkuOS could not delete your financial data right now. Please try again.", "danger")
        return redirect(url_for("settings"))

    push_ui_feedback("All financial data has been cleared.", "success")
    return redirect(url_for("home"))


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
    return goals_view_redirect("#planning")


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
    return goals_view_redirect("#planning")


@app.route("/logout", methods=["POST"])
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
# PLAID
# ---------------------

@app.route("/plaid/link-token", methods=["POST"])
def create_plaid_link_token():
    if not require_login():
        return jsonify({"error": "Login required."}), 401
    if not plaid_is_configured():
        return jsonify({"error": "Plaid is not configured for this environment."}), 503
    limited_response = rate_limit_response(
        "plaid-link-token",
        limit=10,
        window_seconds=600,
        message="Too many bank connection attempts. Please wait and try again.",
    )
    if limited_response:
        return limited_response
    user = current_user()
    payload = request.get_json(silent=True) or {}
    plaid_item = None
    requested_item_id = safe_int(payload.get("item_id"))
    if requested_item_id:
        plaid_item = scoped_record(PlaidItem, requested_item_id, user.id)
        if not plaid_item:
            return jsonify({"error": "That bank connection is no longer available."}), 404
    try:
        link_token = plaid_link_token(user, plaid_item=plaid_item)
    except Exception as exc:
        log_safe_exception("Plaid link token creation failed", exc)
        return jsonify({"error": "Could not start bank connection right now. Please try again."}), 500
    return jsonify({"link_token": link_token, "update_mode": bool(plaid_item)})


@app.route("/plaid/exchange-public-token", methods=["POST"])
def exchange_plaid_public_token():
    if not require_login():
        return jsonify({"error": "Login required."}), 401
    if not plaid_is_configured():
        return jsonify({"error": "Plaid is not configured for this environment."}), 503
    limited_response = rate_limit_response(
        "plaid-exchange-public-token",
        limit=10,
        window_seconds=600,
        message="Too many bank connection attempts. Please wait and try again.",
    )
    if limited_response:
        return limited_response

    payload = request.get_json(silent=True) or {}
    public_token = (payload.get("public_token") or "").strip()
    metadata = payload.get("metadata") or {}
    institution = metadata.get("institution") or {}
    if not public_token:
        return jsonify({"error": "Missing public token."}), 400

    user_id = get_user_id()
    try:
        exchange = plaid_exchange_public_token(public_token)
        item_id = (exchange.get("item_id") or "").strip()
        access_token = (exchange.get("access_token") or "").strip()
        if not item_id or not access_token:
            return jsonify({"error": "Plaid did not return a usable item token."}), 502

        plaid_item = PlaidItem.query.filter_by(item_id=item_id).first()
        created_item = False
        encrypted_access_token = encrypt_sensitive_value(access_token)
        if plaid_item and plaid_item.user_id != user_id:
            return jsonify({"error": "This bank connection is already linked to another AkuOS profile."}), 409
        if not plaid_item:
            existing_item = existing_plaid_item_for_institution(
                user_id,
                institution_id=(institution.get("institution_id") or "").strip(),
                institution_name=(institution.get("name") or "").strip(),
            )
            if existing_item:
                plaid_item = existing_item
        if not plaid_item:
            plaid_item = PlaidItem(
                user_id=user_id,
                item_id=item_id,
                access_token=encrypted_access_token,
                institution_id=(institution.get("institution_id") or "").strip(),
                institution_name=(institution.get("name") or "").strip(),
                sync_cursor="",
                status="active",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.session.add(plaid_item)
            db.session.flush()
            created_item = True
        else:
            item_id_changed = (plaid_item.item_id or "").strip() != item_id
            plaid_item.user_id = user_id
            plaid_item.item_id = item_id
            plaid_item.access_token = encrypted_access_token
            plaid_item.institution_id = (institution.get("institution_id") or plaid_item.institution_id or "").strip()
            plaid_item.institution_name = (institution.get("name") or plaid_item.institution_name or "").strip()
            if item_id_changed:
                plaid_item.sync_cursor = ""
            plaid_item.status = "active"
            plaid_item.last_sync_error = None
            plaid_item.updated_at = datetime.utcnow()

        sync_summary = sync_plaid_item_transactions(plaid_item, user_id=user_id)
        log_activity(
            user_id,
            f"Connected {plaid_item.institution_name or 'bank account'}",
            f"{sync_summary['accounts_linked']} account(s) linked and {sync_summary['transactions_added']} transaction(s) synced.",
            kind="bank_connected",
            icon="bi-bank",
            target_url="/accounts",
        )
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        log_safe_exception("Plaid public token exchange failed", exc)
        return jsonify({"error": "Bank connection failed. Please try again."}), 500

    return jsonify({
        "ok": True,
        "created_item": created_item,
        "institution_name": plaid_item.institution_name,
        "accounts_found": sync_summary["accounts_found"],
        "accounts_linked": sync_summary["accounts_linked"],
        "accounts_created": sync_summary["accounts_created"],
        "accounts_skipped": sync_summary["accounts_skipped"],
        "accounts_added_names": sync_summary["accounts_added_names"],
        "accounts_skipped_names": sync_summary["accounts_skipped_names"],
        "transactions_added": sync_summary["transactions_added"],
        "transactions_modified": sync_summary["transactions_modified"],
        "transactions_removed": sync_summary["transactions_removed"],
    })


@app.route("/plaid/sync", methods=["POST"])
def sync_plaid_connections():
    wants_json = request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    if not require_login():
        if wants_json:
            return jsonify({"ok": False, "error": "Login required."}), 401
        return redirect("/login")
    user_id = get_user_id()
    if not plaid_is_configured():
        if wants_json:
            return jsonify({"ok": False, "error": "Plaid is not configured in this environment yet."}), 503
        push_ui_feedback("Plaid is not configured in this environment yet.", "danger")
        return redirect("/accounts")
    limited_response = rate_limit_response(
        "plaid-sync",
        limit=6,
        window_seconds=300,
        html_fallback="/accounts",
        message="Too many refresh requests. Please wait a minute and try again.",
    )
    if limited_response:
        return limited_response

    items = PlaidItem.query.filter_by(user_id=user_id).order_by(PlaidItem.created_at.asc()).all()
    if not items:
        if wants_json:
            return jsonify({"ok": False, "error": "No connected banks were found yet."}), 400
        push_ui_feedback("No connected banks were found yet.", "danger")
        return redirect("/accounts")

    total_accounts_created = 0
    total_accounts_linked = 0
    total_added = 0
    total_modified = 0
    total_removed = 0
    failed_items = []
    for item in items:
        try:
            summary = sync_plaid_item_transactions(item, user_id=user_id)
            total_accounts_created += summary["accounts_created"]
            total_accounts_linked += summary["accounts_linked"]
            total_added += summary["transactions_added"]
            total_modified += summary["transactions_modified"]
            total_removed += summary["transactions_removed"]
        except Exception as exc:
            item.status = "error"
            item.last_sync_error = "AkuOS could not refresh this bank connection right now."
            item.updated_at = datetime.utcnow()
            failed_items.append({
                "institution_name": (item.institution_name or "Connected bank").strip(),
                "error": "Refresh failed. Please reconnect or try again.",
            })
            log_safe_exception(f"Plaid sync failed for stored item {item.id}", exc)
    db.session.commit()
    last_synced_at = max((item.last_synced_at for item in items if item.last_synced_at), default=None)
    success_message = (
        f"Refresh finished. {total_added} new, {total_modified} updated, {total_removed} removed across "
        f"{total_accounts_linked} linked account(s)."
    )
    if failed_items and (total_added or total_modified or total_removed or total_accounts_linked):
        success_message = (
            f"{success_message} {len(failed_items)} bank connection"
            f"{'' if len(failed_items) == 1 else 's'} still need attention."
        )
    if wants_json:
        if failed_items and not (total_added or total_modified or total_removed or total_accounts_linked):
            return jsonify({
                "ok": False,
                "error": failed_items[0]["error"] if len(failed_items) == 1 else "One or more connected banks could not be refreshed.",
                "failed_items": failed_items,
                "last_synced_at": last_synced_at.isoformat() if last_synced_at else None,
            }), 500
        return jsonify({
            "ok": True,
            "message": success_message,
            "accounts_created": total_accounts_created,
            "accounts_linked": total_accounts_linked,
            "transactions_added": total_added,
            "transactions_modified": total_modified,
            "transactions_removed": total_removed,
            "failed_items": failed_items,
            "last_synced_at": last_synced_at.isoformat() if last_synced_at else None,
        })
    push_ui_feedback(success_message, "success")
    return redirect("/accounts")


@app.route("/plaid/items/<int:item_id>/reconnect-complete", methods=["POST"])
def plaid_reconnect_complete(item_id):
    if not require_login():
        return jsonify({"error": "Login required."}), 401

    user_id = get_user_id()
    limited_response = rate_limit_response(
        "plaid-reconnect-complete",
        limit=6,
        window_seconds=300,
        message="Too many reconnect attempts. Please wait a minute and try again.",
    )
    if limited_response:
        return limited_response
    plaid_item = scoped_record(PlaidItem, item_id, user_id)
    if not plaid_item:
        return jsonify({"error": "That connected bank is no longer available."}), 404

    try:
        sync_summary = sync_plaid_item_transactions(plaid_item, user_id=user_id)
        plaid_item.status = "active"
        plaid_item.last_sync_error = None
        plaid_item.updated_at = datetime.utcnow()
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        plaid_item = scoped_record(PlaidItem, item_id, user_id)
        if plaid_item:
            plaid_item.status = "reconnect_required"
            plaid_item.last_sync_error = "Reconnect completed, but AkuOS could not refresh transactions yet."
            plaid_item.updated_at = datetime.utcnow()
            db.session.commit()
        log_safe_exception(f"Plaid reconnect sync failed for stored item {item_id}", exc)
        return jsonify({"error": "Reconnect finished, but sync failed. Please try again."}), 500

    return jsonify({
        "ok": True,
        "accounts_linked": sync_summary.get("accounts_linked", 0),
        "transactions_added": sync_summary.get("transactions_added", 0),
    })


@app.route("/plaid/webhook", methods=["POST"])
def plaid_webhook():
    payload = request.get_json(silent=True) or {}
    item_id = (payload.get("item_id") or "").strip()
    webhook_type = (payload.get("webhook_type") or "").strip().upper()
    webhook_code = (payload.get("webhook_code") or "").strip().upper()
    if not item_id:
        return "", 200

    plaid_item = PlaidItem.query.filter_by(item_id=item_id).first()
    if not plaid_item:
        return "", 200

    try:
        if webhook_type == "TRANSACTIONS" and webhook_code in {
            "SYNC_UPDATES_AVAILABLE",
            "INITIAL_UPDATE",
            "HISTORICAL_UPDATE",
            "DEFAULT_UPDATE",
            "TRANSACTIONS_REMOVED",
        }:
            sync_plaid_item_transactions(plaid_item, user_id=plaid_item.user_id)
            db.session.commit()
        elif webhook_type == "ITEM":
            if webhook_code == "ERROR":
                error = payload.get("error") or {}
                error_code = (error.get("error_code") or "").strip()
                message = plaid_user_error_message(
                    error.get("error_message"),
                    "This bank connection needs to be refreshed.",
                )
                plaid_item.status = "reconnect_required" if error_code == "ITEM_LOGIN_REQUIRED" else "error"
                plaid_item.last_sync_error = message[:255]
                plaid_item.updated_at = datetime.utcnow()
                db.session.commit()
            elif webhook_code in {"PENDING_DISCONNECT", "PENDING_EXPIRATION"}:
                plaid_item.status = "reconnect_required"
                plaid_item.last_sync_error = "Your bank connection needs to be refreshed soon."
                plaid_item.updated_at = datetime.utcnow()
                db.session.commit()
            elif webhook_code == "LOGIN_REPAIRED":
                plaid_item.status = "active"
                plaid_item.last_sync_error = None
                plaid_item.updated_at = datetime.utcnow()
                sync_plaid_item_transactions(plaid_item, user_id=plaid_item.user_id)
                db.session.commit()
            elif webhook_code == "NEW_ACCOUNTS_AVAILABLE":
                plaid_item.status = "needs_update"
                plaid_item.last_sync_error = "New accounts are available from this bank connection."
                plaid_item.updated_at = datetime.utcnow()
                db.session.commit()
    except Exception as exc:
        db.session.rollback()
        log_safe_exception(f"Plaid webhook handling failed ({webhook_type}:{webhook_code})", exc)
    return "", 200


# ---------------------
# ACCOUNTS
# ---------------------

@app.route("/accounts")
def accounts():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    with timed_route_section("accounts", "accounts_query"):
        accounts = Account.query.filter_by(user_id=user_id).all()
    with timed_route_section("accounts", "plaid_links_query"):
        plaid_links = PlaidAccountLink.query.filter_by(user_id=user_id).all()
    plaid_link_by_account_id = {link.account_id: link for link in plaid_links}
    with timed_route_section("accounts", "plaid_summary"):
        plaid_summary = plaid_connected_summary(user_id)
    linked_account_summary_by_account_id = {
        row.get("account_id"): row
        for row in (plaid_summary.get("linked_accounts") or [])
        if row.get("account_id")
    }
    account_groups = build_account_groups(accounts)
    account_type_labels, account_type_values = account_type_breakdown_series(accounts)
    max_group_balance = max((abs(float(group.get("total_balance") or 0)) for group in account_groups), default=0)
    for group in account_groups:
        balance_value = abs(float(group.get("total_balance") or 0))
        group["visual_share"] = round((balance_value / max_group_balance) * 100, 2) if max_group_balance else 0
    net_worth_breakdown = build_net_worth_breakdown(accounts)
    total_assets = net_worth_breakdown["total_assets"]
    total_liabilities = net_worth_breakdown["total_liabilities"]
    liability_only_nudge = ""
    if total_liabilities > 0 and total_assets <= 0:
        liability_only_nudge = "Add a checking or savings account to see your full net worth."
    return render_template(
        "accounts.html",
        accounts=accounts,
        has_accounts=bool(accounts),
        total_assets=total_assets,
        total_liabilities=total_liabilities,
        net_worth=round(total_assets - total_liabilities, 2),
        liability_only_nudge=liability_only_nudge,
        plaid_summary=plaid_summary,
        plaid_link_by_account_id=plaid_link_by_account_id,
        linked_account_summary_by_account_id=linked_account_summary_by_account_id,
        account_groups=account_groups,
        account_type_labels=account_type_labels,
        account_type_values=account_type_values,
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
    account = scoped_record(Account, account_id, user_id)
    if not account:
        return "Account not found"

    transactions = (
        Transaction.query
        .filter_by(user_id=user_id, account_id=account_id)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .limit(100)
        .all()
    )
    plaid_link = PlaidAccountLink.query.filter_by(user_id=user_id, account_id=account_id).first()
    goal_allocations = account_goal_allocation_summary(user_id, account)
    return render_template(
        "account_detail.html",
        account=account,
        transactions=transactions,
        goal_allocations=goal_allocations,
        plaid_link=plaid_link,
        subtype_label=subtype_label,
        transaction_count=len(transactions),
    )


def plaid_link_for_account(user_id, account_id):
    return PlaidAccountLink.query.filter_by(user_id=user_id, account_id=account_id).first()


@app.route("/edit_account/<int:account_id>", methods=["GET", "POST"])
def edit_account(account_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    acct = scoped_record(Account, account_id, user_id)
    if not acct:
        return "Account not found"
    plaid_link = plaid_link_for_account(user_id, acct.id)
    if plaid_link:
        push_ui_feedback(
            "Plaid-linked accounts are synced and read-only. Disconnect the bank link if you need to stop syncing this account.",
            "info",
        )
        return redirect(url_for("account_detail", account_id=acct.id))

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
    acct = scoped_record(Account, account_id, user_id)
    if not acct:
        return "Account not found"
    if plaid_link_for_account(user_id, acct.id):
        push_ui_feedback(
            "Savings tracking for Plaid-linked accounts is managed by synced account data and can't be edited directly.",
            "info",
        )
        return redirect(url_for("account_detail", account_id=acct.id))

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
    acct = scoped_record(Account, account_id, user_id)

    if acct:
        if plaid_link_for_account(user_id, acct.id):
            push_ui_feedback(
                "Plaid-linked accounts can't be deleted directly. Use Disconnect to remove the bank connection while keeping your account history.",
                "info",
            )
            return redirect("/accounts")
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


@app.route("/accounts/<int:account_id>/disconnect", methods=["POST"])
def disconnect_plaid_account(account_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    account = scoped_record(Account, account_id, user_id)
    if not account:
        push_ui_feedback("That account is no longer available.", "danger")
        return redirect("/accounts")

    plaid_link = plaid_link_for_account(user_id, account.id)
    if not plaid_link:
        push_ui_feedback("That account is not currently linked through Plaid.", "danger")
        return redirect("/accounts")

    plaid_item = scoped_record(PlaidItem, plaid_link.plaid_item_id, user_id)
    institution_name = (
        (plaid_item.institution_name if plaid_item else "") or plaid_link.official_name or plaid_link.name or "your bank"
    ).strip()
    plaid_item_id = plaid_link.plaid_item_id
    db.session.delete(plaid_link)
    db.session.flush()

    remaining_links = PlaidAccountLink.query.filter_by(user_id=user_id, plaid_item_id=plaid_item_id).count()
    if plaid_item and remaining_links == 0:
        db.session.delete(plaid_item)

    log_activity(
        user_id,
        f"Disconnected {account.name}",
        f"Removed the Plaid connection for {account.name} from {institution_name}. Account history stays in AkuOS.",
        kind="account_updated",
        icon="bi-plug",
        target_url="/accounts",
    )
    db.session.commit()
    push_ui_feedback(
        f"Disconnected {account.name} from {institution_name}. The account and existing transactions are still available in AkuOS.",
        "success",
    )
    return redirect("/accounts")


@app.route("/goals-wealth", methods=["GET", "POST"])
def goals_wealth():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    selected_month, selected_year = month_year_from_request()
    accounts = Account.query.filter_by(user_id=user_id).all()
    transactions = Transaction.query.filter_by(user_id=user_id).order_by(Transaction.date.asc()).all()
    goals = FinancialGoal.query.filter_by(user_id=user_id).all()
    debts = Debt.query.filter_by(user_id=user_id).all()
    goals.sort(key=lambda goal: (goal.target_date is None, goal.target_date or date.max, -goal.id))

    debt_budget_override = None
    purchase_payload = None
    if request.method == "POST":
        form_name = (request.form.get("form_name") or "").strip()
        if form_name == "add_debt":
            name = request.form.get("name", "").strip()
            balance = safe_float(request.form.get("balance"))
            rate = safe_float(request.form.get("rate"))
            if name and balance is not None and rate is not None:
                db.session.add(Debt(user_id=user_id, name=name, balance=balance, rate=rate))
                db.session.commit()
                push_ui_feedback(f"Added debt for {name}.", "success")
            else:
                push_ui_feedback("Add a debt name, balance, and rate.", "danger")
            return goals_view_redirect("#planning")
        if form_name == "debt_plan":
            submitted_budget = safe_float(request.form.get("monthly_budget"))
            if submitted_budget is not None:
                debt_budget_override = submitted_budget
        if form_name == "purchase_plan":
            purchase_payload = {
                "name": request.form.get("name", "").strip(),
                "price": request.form.get("price"),
                "down": request.form.get("down"),
                "rate": request.form.get("rate"),
                "years": request.form.get("years"),
            }

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
    goal_focus = goal_focus_summary(wealth_snapshot["goal_rows"])
    account_groups = build_account_groups(accounts)
    planning_context = build_planning_context(
        accounts,
        transactions,
        goals,
        debts,
        debt_budget_override=debt_budget_override,
        purchase_payload=purchase_payload,
    )
    transaction_years = sorted({tx.date.year for tx in transactions} | {selected_year, datetime.now().year}, reverse=True)
    month_labels = {month: calendar.month_name[month] for month in range(1, 13)}
    page_context = {
        "selected_month": selected_month,
        "selected_year": selected_year,
        "transaction_years": transaction_years,
        "month_labels": month_labels,
        "nw_labels": nw_labels,
        "nw_values": nw_values,
        "selected_month_income": monthly_income,
        "selected_month_expenses": monthly_expenses,
        "savings_snapshot": savings_snapshot,
        "wealth_snapshot": wealth_snapshot,
        "goal_focus": goal_focus,
        "account_allocation_summary": account_allocation_summary,
        "account_groups": account_groups,
        "has_goals": bool(goals),
        "goal_linkable_accounts": linked_goalable_accounts(accounts),
        "subtype_label": subtype_label,
    }
    overlapping_context_keys = sorted(set(page_context).intersection(planning_context))
    if overlapping_context_keys:
        app.logger.warning(
            "goals_wealth planning context overlaps page context keys: %s",
            ", ".join(overlapping_context_keys),
        )
    final_context = {**page_context, **planning_context}
    return render_template("goals_wealth.html", **final_context)


def goals_view_redirect(anchor=""):
    return redirect(f"/goals-wealth{anchor}")


def build_planning_context(accounts, transactions, goals, debts, debt_budget_override=None, purchase_payload=None):
    current_month = date.today().month
    current_year = date.today().year

    monthly_income = 0.0
    monthly_expenses = 0.0
    for tx in transactions or []:
        if tx.date and tx.date.month == current_month and tx.date.year == current_year:
            if float(tx.amount or 0) > 0:
                monthly_income += float(tx.amount or 0)
            else:
                monthly_expenses += abs(float(tx.amount or 0))

    total_debt_balance = sum(float(d.balance or 0) for d in debts or [])
    monthly_debt_budget = sum(
        estimated_minimum_payment(float(d.balance or 0), float(d.rate or 0))
        for d in debts or []
    )
    if debts:
        monthly_debt_budget += 250.0
    if debt_budget_override is not None:
        monthly_debt_budget = debt_budget_override

    purchase_result = None
    if purchase_payload:
        purchase_name = (purchase_payload.get("name") or "").strip()
        price = safe_float(purchase_payload.get("price"))
        down = safe_float(purchase_payload.get("down"))
        rate = safe_float(purchase_payload.get("rate"))
        years = safe_float(purchase_payload.get("years"))
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
                "budget_pressure": round((monthly_payment / monthly_income) * 100, 2) if monthly_income > 0 else None,
            }

    recurring_income_sources = analyze_recurring_income(transactions)
    recurring_income_estimate = recurring_income_monthly_estimate(recurring_income_sources)
    effective_monthly_income = max(monthly_income, recurring_income_estimate)
    recurring_expenses = analyze_recurring_expenses(transactions)
    recurring_bills = recurring_expenses[:4]
    recurring_bill_total = recurring_expense_monthly_estimate(recurring_expenses, confirmed_only=False)
    category_totals = defaultdict(float)
    for tx in transactions or []:
        if tx.date and tx.date.month == current_month and tx.date.year == current_year and is_spending_transaction(tx):
            category_totals[tx.category] += abs(float(tx.amount or 0))

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
    recurring_obligations = recurring_expense_monthly_estimate(recurring_expenses, confirmed_only=True)
    safe_to_spend = calculate_safe_to_spend(
        accounts,
        recurring_expenses,
        [],
        effective_monthly_income,
        monthly_expenses,
        recurring_obligations,
        savings_snapshot.get("recommended_amount"),
        current_month,
        current_year,
        actual_monthly_income=monthly_income,
        goal_set_aside_amount=goal_budget.get("suggested_goal_set_aside"),
    )

    snowball = simulate_debt_payoff(debts, "snowball", monthly_debt_budget)
    avalanche = simulate_debt_payoff(debts, "avalanche", monthly_debt_budget)
    interest_saved = round(max(snowball["interest_paid"] - avalanche["interest_paid"], 0), 2)
    planning_result = {
        "monthly_budget": round(monthly_debt_budget, 2),
        "total_balance": round(total_debt_balance, 2),
        "snowball": snowball,
        "avalanche": avalanche,
        "interest_saved": interest_saved,
    }

    return {
        "debts": debts,
        "planning_result": planning_result,
        "purchase_result": purchase_result,
        "monthly_income": round(monthly_income, 2),
        "monthly_expenses": round(monthly_expenses, 2),
        "recurring_income_sources": recurring_income_sources[:4],
        "recurring_income_estimate": round(recurring_income_estimate, 2),
        "recurring_bills": recurring_bills,
        "recurring_bill_total": round(recurring_bill_total, 2),
        "effective_monthly_income": round(effective_monthly_income, 2),
        "recurring_obligations": round(recurring_obligations, 2),
        "safe_to_spend": safe_to_spend,
        "planning_goal_budget": goal_budget,
    }


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
        return goals_view_redirect()

    allocation_value = allocated_amount
    if linked_account_id:
        if allocation_value is None:
            allocation_value = current_amount if current_amount is not None else 0
        if allocation_value is None or allocation_value < 0:
            push_ui_feedback("Add an allocated amount of zero or more for the linked account.", "danger")
            return goals_view_redirect()
        linked_account, allocation_error = validate_account_allocation(user_id, linked_account_id, allocation_value)
        if allocation_error:
            push_ui_feedback(allocation_error, "danger")
            return goals_view_redirect()
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
    return goals_view_redirect()


@app.route("/goals-wealth/update-goal/<int:goal_id>", methods=["POST"])
def update_financial_goal(goal_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    goal = FinancialGoal.query.get(goal_id)
    if not goal or goal.user_id != user_id:
        return goals_view_redirect()

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
            return goals_view_redirect()
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
    return goals_view_redirect()


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
        return goals_view_redirect()

    if allocated_amount is None or allocated_amount < 0:
        push_ui_feedback("Enter an allocation amount of zero or more.", "danger")
        return goals_view_redirect()

    existing_allocation = GoalAllocation.query.filter_by(goal_id=goal.id, account_id=account_id).first() if account_id else None
    linked_account, allocation_error = validate_account_allocation(
        user_id,
        account_id,
        allocated_amount,
        exclude_allocation_id=existing_allocation.id if existing_allocation else None,
    )
    if allocation_error:
        push_ui_feedback(allocation_error, "danger")
        return goals_view_redirect()

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
        return goals_view_redirect()

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
    return goals_view_redirect()


@app.route("/goals-wealth/auto-allocate/<int:account_id>", methods=["POST"])
def auto_allocate_account(account_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    account = scoped_record(Account, account_id, user_id)
    if not account or account.type != "asset":
        push_ui_feedback("Choose a valid asset account to auto-allocate.", "danger")
        return goals_view_redirect()

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
        return goals_view_redirect()

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
        "/goals-wealth",
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
    return goals_view_redirect()


@app.route("/goals-wealth/goal-action/<int:goal_id>", methods=["POST"])
def goal_quick_action(goal_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    goal = FinancialGoal.query.get(goal_id)
    if not goal or goal.user_id != user_id:
        push_ui_feedback("Choose a valid goal first.", "danger")
        return goals_view_redirect()

    action = (request.form.get("action") or "").strip().lower()
    if action not in {"fully_fund", "add_remaining"}:
        push_ui_feedback("Choose a valid goal action.", "danger")
        return goals_view_redirect()

    result = quick_allocate_goal(user_id, goal, "full" if action == "fully_fund" else "remaining")
    added_total = float(result.get("added_total") or 0)
    if added_total <= 0:
        push_ui_feedback(f"No unallocated funds were available to update {goal.name}.", "warning")
        return goals_view_redirect()

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
    return goals_view_redirect()


@app.route("/goals-wealth/manage-funds/<int:goal_id>", methods=["POST"])
def manage_goal_funds(goal_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    goal = FinancialGoal.query.get(goal_id)
    if not goal or goal.user_id != user_id:
        push_ui_feedback("Choose a valid goal first.", "danger")
        return goals_view_redirect()

    action = (request.form.get("action") or "").strip().lower()
    exact_amount = safe_float(request.form.get("exact_amount"))
    adjustment_amount = safe_float(request.form.get("adjustment_amount"))
    current_total, _ = current_goal_funding_total(goal)
    target_total = max(float(goal.target_amount or 0), 0)

    if action == "set_exact":
        if exact_amount is None or exact_amount < 0:
            push_ui_feedback("Enter an exact goal amount of zero or more.", "danger")
            return goals_view_redirect()
        new_total = exact_amount
        feedback_message = f"{goal.name} is now set to ${new_total:,.2f}."
        activity_message = f"Funding is now ${new_total:,.2f}."
    elif action == "add":
        if adjustment_amount is None or adjustment_amount <= 0:
            push_ui_feedback("Enter an amount greater than zero to add funds.", "danger")
            return goals_view_redirect()
        new_total = current_total + adjustment_amount
        feedback_message = f"Added ${adjustment_amount:,.2f} to {goal.name}."
        activity_message = f"Increased funding by ${adjustment_amount:,.2f}."
    elif action == "reduce":
        if adjustment_amount is None or adjustment_amount <= 0:
            push_ui_feedback("Enter an amount greater than zero to reduce funds.", "danger")
            return goals_view_redirect()
        new_total = max(current_total - adjustment_amount, 0)
        feedback_message = f"Reduced {goal.name} by ${min(adjustment_amount, current_total):,.2f}."
        activity_message = f"Reduced funding to ${new_total:,.2f}."
    elif action == "fully_fund":
        new_total = target_total
        feedback_message = f"{goal.name} is now fully funded."
        activity_message = f"Funding moved to the full goal amount of ${new_total:,.2f}."
    elif action == "remove_all":
        new_total = 0
        feedback_message = f"Removed all saved funds from {goal.name}."
        activity_message = "Cleared all saved funds from the goal."
    else:
        push_ui_feedback("Choose a valid funding action.", "danger")
        return goals_view_redirect()

    set_goal_funding_total(goal, new_total)
    log_activity(
        user_id,
        f"Updated funding for {goal.name}",
        activity_message,
        kind="goal_updated",
        icon="bi-wallet2",
        target_url="/goals-wealth",
    )
    db.session.commit()
    push_ui_feedback(feedback_message, "success")
    return goals_view_redirect()


@app.route("/income-allocation/apply", methods=["POST"])
def apply_income_allocation_suggestion():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    account_id = safe_int(request.form.get("account_id"))
    source_name = (request.form.get("source_name") or "Income source").strip()
    account = scoped_record(Account, account_id, user_id) if account_id else None
    if not account or account.type != "asset":
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
        return goals_view_redirect()

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

    redirect_url = payload.get("redirect_url") or "/goals-wealth"
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
    return goals_view_redirect()


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
            result = categorize_transaction_detailed(user_id, description, amount)
            rule_test_result = {
                "description": description,
                "amount": round(amount, 2),
                "normalized_merchant": result.get("normalized_description") or normalize_text(description),
                "merchant_guess": result.get("merchant_guess") or clean_transaction_description(description),
                "category": result.get("category"),
                "subcategory": result.get("subcategory"),
                "source": result.get("category_source"),
                "confidence": result.get("category_confidence"),
                "display_name": result.get("rule_display_name") or preferred_display_name_for_user(user_id, description, fallback=clean_transaction_description(description)),
                "tags": result.get("rule_tags") or "",
                "skip_transaction": bool(result.get("skip_transaction")),
            }
        else:
            rule_test_result = {
                "error": "Enter both a description and an amount to test categorization."
            }

    return render_template(
        "rules.html",
        rules=rules,
        rule_test_result=rule_test_result,
        category_groups=category_grouped_choices(user_id),
        subcategory_map=category_subcategory_map(),
        subtype_choices=[("income", "Income"), ("expense", "Expense"), ("transfer", "Transfer"), ("payment", "Payment")],
        rule_match_type_choices=rule_match_type_options(),
    )


@app.route("/add_rule", methods=["POST"])
def add_rule():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    keyword = request.form["keyword"].strip()
    category = request.form["category"].strip()
    subcategory = request.form.get("subcategory", "").strip()
    display_name_override = request.form.get("display_name_override", "").strip()
    subtype = (request.form.get("subtype", "") or "").strip().lower()
    tag_rules = request.form.get("tag_rules", "").strip()
    skip_transaction = normalize_rule_skip(request.form.get("skip_transaction"))
    priority = request.form.get("priority", "100").strip()
    match_type = request.form.get("match_type", "contains").strip()
    amount_direction = request.form.get("amount_direction", "any").strip()
    try:
        priority = int(priority)
    except:
        priority = 100
    if match_type not in ("exact", "contains", "startswith", "regex"):
        match_type = "contains"
    if amount_direction not in ("debit", "credit", "any"):
        amount_direction = "any"
    if not keyword or not category:
        return "Keyword and category required"
    r = upsert_transaction_rule(
        user_id,
        keyword,
        category,
        subcategory=subcategory,
        subtype=subtype,
        display_name=display_name_override,
        tags=tag_rules,
        skip_transaction=skip_transaction,
        match_type=match_type,
        priority=priority,
        amount_direction=amount_direction,
        pattern=keyword,
    )
    if not r:
        return redirect("/rules")
    db.session.commit()
    return redirect("/rules")


@app.route("/rules/<int:rule_id>/update", methods=["POST"])
def update_rule(rule_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    rule = scoped_record(CategoryRule, rule_id, user_id)
    if not rule or rule.is_system_rule:
        return redirect("/rules")

    keyword = request.form.get("keyword", "").strip()
    category = request.form.get("category", "").strip()
    subcategory = request.form.get("subcategory", "").strip()
    subtype = (request.form.get("subtype", "") or "").strip().lower()
    display_name_override = request.form.get("display_name_override", "").strip()
    tag_rules = request.form.get("tag_rules", "").strip()
    skip_transaction = normalize_rule_skip(request.form.get("skip_transaction"))
    match_type = (request.form.get("match_type", "contains") or "contains").strip().lower()
    amount_direction = (request.form.get("amount_direction", "any") or "any").strip().lower()
    priority = request.form.get("priority", "100").strip()
    is_active = normalize_rule_skip(request.form.get("is_active"))
    updated_rule = upsert_transaction_rule(
        user_id,
        keyword or rule.pattern or rule.keyword,
        category or rule.category or "Needs Review",
        subcategory=subcategory or getattr(rule, "subcategory_name", "") or "",
        subtype=subtype or rule.subtype,
        display_name=display_name_override,
        tags=tag_rules,
        skip_transaction=skip_transaction,
        matched_rule_id=rule.id,
        match_type=match_type or rule.match_type,
        priority=priority,
        amount_direction=amount_direction or rule.amount_direction,
        pattern=keyword or rule.pattern or rule.keyword,
    )
    if updated_rule:
        updated_rule.is_active = is_active
    db.session.commit()
    return redirect("/rules")


@app.route("/delete_rule/<int:rule_id>", methods=["POST"])
def delete_rule(rule_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    r = scoped_record(CategoryRule, rule_id, user_id)
    if r:
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
        category_groups=category_grouped_choices(user_id),
        subcategory_map=category_subcategory_map(),
        subtype_choices=[("income", "Income"), ("expense", "Expense"), ("transfer", "Transfer"), ("payment", "Payment")],
    )


@app.route("/merchant-memory/add", methods=["POST"])
def add_merchant_memory():
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    merchant = request.form.get("merchant", "").strip()
    category = canonical_transaction_category(request.form.get("category", "").strip())
    subcategory = canonical_subcategory_name(request.form.get("subcategory", "").strip())
    category, subcategory = canonical_category_pair(category, subcategory)
    display_name = clean_transaction_description(request.form.get("display_name", "").strip() or merchant)
    subtype = (request.form.get("subtype", "") or "").strip().lower()
    is_disabled = (request.form.get("is_disabled") or "").strip() == "1"
    if merchant and category:
        remember_merchant_category(user_id, merchant, category, subcategory=subcategory, display_name=display_name, subtype=subtype)
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
    memory = scoped_record(MerchantMemory, memory_id, user_id)
    if not memory:
        return redirect("/merchant-memory")

    merchant = request.form.get("merchant", "").strip()
    category = canonical_transaction_category(request.form.get("category", "").strip())
    subcategory = canonical_subcategory_name(request.form.get("subcategory", "").strip())
    category, subcategory = canonical_category_pair(category, subcategory)
    display_name = clean_transaction_description(request.form.get("display_name", "").strip() or merchant)
    subtype = (request.form.get("subtype", "") or "").strip().lower()
    is_disabled = (request.form.get("is_disabled") or "").strip() == "1"
    normalized = normalize_text(merchant)
    if normalized and category and category.lower() not in GENERIC_CATEGORIES:
        memory.merchant = normalized
        memory.category = category
        memory.subcategory = subcategory
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
    memory = scoped_record(MerchantMemory, memory_id, user_id)
    if memory:
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
    current_month = date.today().month
    current_year = date.today().year
    monthly_spend_by_category = defaultdict(float)
    for tx in transactions:
        if not tx.date or tx.date.month != current_month or tx.date.year != current_year:
            continue
        if not is_spending_transaction(tx):
            continue
        category_name = transaction_ui_category(getattr(tx, "category", "") or "") or "Other"
        monthly_spend_by_category[category_name] += abs(float(tx.amount or 0))

    budget_rows = []
    for budget in budgets:
        budget_category = transaction_ui_category(getattr(budget, "category", "") or "") or (budget.category or "Other")
        monthly_spent = round(monthly_spend_by_category.get(budget_category, 0), 2)
        monthly_limit = round(float(budget.monthly_limit or 0), 2)
        remaining_amount = round(monthly_limit - monthly_spent, 2)
        progress_ratio = (monthly_spent / monthly_limit) if monthly_limit > 0 else 0
        if progress_ratio >= 1:
            status_tone = "over"
            status_label = "Over budget"
        elif progress_ratio >= 0.8:
            status_tone = "warning"
            status_label = "Close to limit"
        else:
            status_tone = "good"
            status_label = "On track"
        budget_rows.append({
            "id": budget.id,
            "category": budget_category,
            "monthly_limit": monthly_limit,
            "monthly_spent": monthly_spent,
            "remaining_amount": remaining_amount,
            "progress_pct": round(min(progress_ratio * 100, 100), 1),
            "progress_visual_pct": round(min(progress_ratio * 100, 132), 1),
            "status_tone": status_tone,
            "status_label": status_label,
        })

    budget_rows.sort(key=lambda row: (row["status_tone"] != "over", row["status_tone"] != "warning", -row["progress_visual_pct"], row["category"]))
    total_budget_limit = round(sum(row["monthly_limit"] for row in budget_rows), 2)
    total_budget_spent = round(sum(row["monthly_spent"] for row in budget_rows), 2)
    total_budget_remaining = round(total_budget_limit - total_budget_spent, 2)
    budget_overview = {
        "count": len(budget_rows),
        "on_track_count": sum(1 for row in budget_rows if row["status_tone"] == "good"),
        "warning_count": sum(1 for row in budget_rows if row["status_tone"] == "warning"),
        "over_count": sum(1 for row in budget_rows if row["status_tone"] == "over"),
        "total_limit": total_budget_limit,
        "total_spent": total_budget_spent,
        "total_remaining": total_budget_remaining,
        "progress_pct": round(min((total_budget_spent / total_budget_limit) * 100, 100), 1) if total_budget_limit > 0 else 0,
        "visual_progress_pct": round(min((total_budget_spent / total_budget_limit) * 100, 132), 1) if total_budget_limit > 0 else 0,
        "current_month_label": f"{calendar.month_name[current_month]} {current_year}",
    }
    return render_template(
        "budgets.html",
        budgets=budgets,
        budget_rows=budget_rows,
        budget_overview=budget_overview,
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
    start_import_worker_if_needed()
    bootstrap_merchant_memory(user_id)
    accounts = Account.query.filter_by(user_id=user_id).all()
    plaid_summary = plaid_connected_summary(user_id)
    transaction_count = Transaction.query.filter_by(user_id=user_id).count()
    preview = load_import_preview()
    last_import_batch = latest_import_batch_for_user(user_id)
    import_error = None
    import_success = None
    import_summary = None
    reopen_summary_job_id = (session.pop("reopen_import_summary_job_id", "") or "").strip()
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
            limited_response = rate_limit_response(
                "import-upload",
                limit=10,
                window_seconds=600,
                html_fallback="/imports",
                message="Too many import attempts. Please wait a few minutes and try again.",
            )
            if limited_response:
                return limited_response
            account_id = request.form.get("account_id")
            files = [file for file in request.files.getlist("files") if file and file.filename]
            if not files:
                files = [file for file in request.files.getlist("files[]") if file and file.filename]
            pasted_text = (request.form.get("pasted_statement_text") or "").strip()
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
            elif not files and not pasted_text:
                uploaded_keys = list(request.files.keys())
                app.logger.warning(
                    "Import Center submit received no usable files for user %s. request.files keys=%s content_length=%s",
                    user_id,
                    uploaded_keys,
                    request.content_length,
                )
                import_error = "Choose one or more CSV or PDF statements, or paste statement text to preview."
                selected_account_id = int(account_id)
            else:
                try:
                    set_last_import_account(account_id)
                    queued_job = queue_import_job(user_id, account_id, files, pasted_text=pasted_text)
                except Exception:
                    app.logger.exception(
                        "Import Center failed to queue import for user %s account %s with %s files",
                        user_id,
                        account_id,
                        len(files),
                    )
                    import_error = "AkuOS could not start that import. Please try again, or use a different file if the problem continues."
                else:
                    push_ui_feedback(
                        f"Import queued for background processing. AkuOS is preparing your transaction review for {len(files) + (1 if pasted_text else 0)} source{'s' if (len(files) + (1 if pasted_text else 0)) != 1 else ''}.",
                        "info",
                    )
                    return redirect(url_for("review_import_job", job_id=queued_job.id))

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
                    import_success = f"Added {new_account.name}. Your previous import review is still saved in Recent Import Jobs if you want to reopen it later."
                else:
                    import_success = f"Added {new_account.name}. It is now selected for your next import."

        elif form_name == "commit_import":
            active_preview_id = session.get("import_preview_id")
            preview = load_import_preview()
            if not preview:
                import_error = "Import preview expired. Upload the file again."
            else:
                current_review_job = import_job_for_preview(active_preview_id, user_id)
                preview = apply_review_form_to_preview(preview, request.form)
                if current_review_job:
                    sync_import_job_review_payload(current_review_job, preview, persist_preview_file=True, set_active_session=True)
                    db.session.commit()
                elif active_preview_id:
                    save_import_preview(user_id, preview, preview_id=active_preview_id)
                account_id = int(preview["account_id"])
                import_job_id = (preview.get("import_job_id") or "").strip()
                acct = scoped_record(Account, account_id, user_id)
                if not acct:
                    import_error = "Selected account is no longer available."
                else:
                    set_last_import_account(account_id)
                    preview_summary = preview.get("summary", {})
                    imported_count = 0
                    duplicate_count = 0
                    skipped_count = 0
                    not_transaction_count = 0
                    needs_review_count = 0
                    corrected_count = 0
                    auto_detected_count = 0
                    merchant_memory_updated_count = 0
                    pending_manual_count = 0
                    existing_fingerprints, _ = existing_transactions_for_duplicate_matching(user_id, account_id)
                    commit_fingerprints = set()
                    prepared_transactions = []
                    for row in preview["rows"]:
                        row_fingerprint = row.get("fingerprint") or transaction_fingerprint(
                            row["date"],
                            row["description"],
                            row["amount"],
                            merchant_guess=row.get("merchant_guess") or row.get("normalized_description") or row["description"],
                        )
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
                        chosen_subcategory = canonical_subcategory_name(request.form.get(f"subcategory_{row['row_id']}", "").strip() or row.get("subcategory", ""))
                        review_state = (request.form.get(f"review_state_{row['row_id']}") or ("needs_review" if row.get("review_required") else "reviewed")).strip().lower()
                        chosen_category, chosen_subcategory = canonical_category_pair(chosen_category, chosen_subcategory)
                        original_category = canonical_transaction_category((row.get("category") or "").strip())
                        if not chosen_category or chosen_category.lower() in GENERIC_CATEGORIES:
                            chosen_category = "Needs Review"
                            chosen_subcategory = ""
                        parsed_date = parse_date_any(chosen_date_raw)
                        amount = safe_float(chosen_amount_raw)
                        if parsed_date is None or not chosen_display_name or amount is None:
                            pending_manual_count += 1
                            continue

                        final_fingerprint = transaction_fingerprint(
                            parsed_date,
                            raw_description_value or chosen_display_name,
                            amount,
                            merchant_guess=derive_merchant_guess(raw_description_value or chosen_display_name),
                        )
                        category_source = row.get("category_source") or "Manual Review"
                        category_confidence = normalize_confidence_bucket(row.get("confidence_bucket") or row.get("confidence_label"))
                        if (
                            chosen_category != original_category
                            or (chosen_subcategory or "").strip() != (row.get("subcategory") or "").strip()
                        ) and chosen_category.lower() not in GENERIC_CATEGORIES:
                            category_source = "Manual Review"
                            category_confidence = "high"
                        elif review_state == "reviewed" and chosen_category.lower() not in GENERIC_CATEGORIES:
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
                            "normalized_description": derive_normalized_description(raw_description_value or chosen_display_name),
                            "merchant_guess": derive_merchant_guess(raw_description_value or chosen_display_name),
                            "amount": amount,
                            "category": chosen_category,
                            "subcategory": chosen_subcategory,
                            "category_source": category_source,
                            "category_confidence": category_confidence,
                            "matched_rule_id": row.get("matched_rule_id"),
                            "suggested_category_id": row.get("suggested_category_id"),
                            "suggested_subcategory_id": row.get("suggested_subcategory_id"),
                            "needs_review": chosen_category == "Needs Review" or category_confidence in {"error", "uncategorized", "low"},
                            "transaction_subtype": final_subtype,
                            "import_source": (row.get("parser_source") or "rule_based").strip() or "rule_based",
                            "fingerprint": final_fingerprint,
                            "tags": normalize_rule_tags_value(row.get("tags", "")),
                        })
                        commit_fingerprints.add(final_fingerprint)
                        existing_fingerprints[final_fingerprint] = {"fingerprint": final_fingerprint}
                        if (
                            original_category and chosen_category != original_category
                        ) or (
                            row.get("date", "") != chosen_date_raw
                        ) or (
                            (row.get("display_name") or row.get("description") or "").strip() != chosen_display_name
                        ) or (
                            (row.get("subcategory") or "").strip() != chosen_subcategory
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
                                normalized_description=prepared_row["normalized_description"],
                                merchant_guess=prepared_row["merchant_guess"],
                                amount=prepared_row["amount"],
                                category=prepared_row["category"],
                                subcategory=prepared_row["subcategory"],
                                suggested_category_id=prepared_row["suggested_category_id"],
                                suggested_subcategory_id=prepared_row["suggested_subcategory_id"],
                                category_source=prepared_row["category_source"],
                                category_confidence=prepared_row["category_confidence"] or "high",
                                matched_rule_id=prepared_row["matched_rule_id"],
                                needs_review=prepared_row["needs_review"],
                                transaction_subtype=prepared_row["transaction_subtype"],
                                import_source=prepared_row["import_source"],
                                fingerprint=prepared_row["fingerprint"],
                                tags=prepared_row["tags"],
                                import_batch_id=import_batch_id,
                            )
                            db.session.add(tx)
                            remember_merchant_category(
                                user_id,
                                prepared_row["raw_description"] or prepared_row["display_name"],
                                prepared_row["category"],
                                subcategory=prepared_row["subcategory"],
                                display_name=prepared_row["display_name"],
                                subtype=prepared_row["transaction_subtype"],
                            )
                            if prepared_row["category_source"] == "Manual Review":
                                learned_rule = upsert_learned_category_rule(
                                    user_id,
                                    prepared_row["raw_description"] or prepared_row["display_name"],
                                    prepared_row["category"],
                                    subcategory=prepared_row["subcategory"],
                                    subtype=prepared_row["transaction_subtype"],
                                    matched_rule_id=prepared_row["matched_rule_id"],
                                )
                                if learned_rule:
                                    tx.matched_rule_id = learned_rule.id
                            merchant_memory_updated_count += 1
                        if not prepared_transactions:
                            ending_balance = starting_balance
                        else:
                            acct.balance = round(starting_balance + net_change, 2)
                            ending_balance = round(float(acct.balance or 0), 2)
                        if import_batch_id:
                            batch_start = parse_date_any(preview_summary.get("date_range_start")) if preview_summary.get("date_range_start") else None
                            batch_end = parse_date_any(preview_summary.get("date_range_end")) if preview_summary.get("date_range_end") else None
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
                                duplicate_candidate_count=preview_summary.get("duplicate_candidate_count", 0),
                                skipped_count=skipped_count,
                                not_transaction_count=not_transaction_count,
                                needs_review_count=needs_review_count,
                                start_date=batch_start,
                                end_date=batch_end,
                            ))
                        if import_job_id:
                            import_job = scoped_record(ImportJob, import_job_id, user_id)
                            if import_job:
                                existing_job_summary = parse_import_job_summary(import_job.summary_json)
                                import_job.status = "imported"
                                import_job.current_stage = "complete"
                                import_job.progress_percent = 100
                                import_job.completed_at = datetime.utcnow()
                                import_job.summary_json = json.dumps({
                                    "transaction_count": imported_count,
                                    "imported_count": imported_count,
                                    "new_transaction_count": imported_count,
                                    "already_imported_count": duplicate_count,
                                    "duplicate_candidate_count": preview_summary.get("duplicate_candidate_count", 0),
                                    "auto_approved_count": preview_summary.get("auto_approved_count", 0),
                                    "needs_review_count": needs_review_count,
                                    "ignored_row_count": skipped_count,
                                    "duplicate_count": duplicate_count,
                                    "skipped_count": skipped_count,
                                    "not_transaction_count": not_transaction_count,
                                    "merchant_memory_updated_count": merchant_memory_updated_count,
                                    "net_impact": net_change,
                                    "date_range_start": preview_summary.get("date_range_start", ""),
                                    "date_range_end": preview_summary.get("date_range_end", ""),
                                    "date_range_label": preview_summary.get("date_range_label", "Date range unavailable"),
                                    "parser_debug": existing_job_summary.get("parser_debug", []),
                                    "warnings": existing_job_summary.get("warnings", []),
                                })
                                import_job.start_date = parse_date_any(preview_summary.get("date_range_start")) if preview_summary.get("date_range_start") else None
                                import_job.end_date = parse_date_any(preview_summary.get("date_range_end")) if preview_summary.get("date_range_end") else None
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
                        clear_import_preview(remove_saved_preview=True)
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
                            "new_transaction_count": imported_count,
                            "already_imported_count": duplicate_count,
                            "duplicate_candidate_count": preview_summary.get("duplicate_candidate_count", 0),
                            "auto_approved_count": preview_summary.get("auto_approved_count", 0),
                            "auto_detected_count": auto_detected_count,
                            "corrected_count": corrected_count,
                            "duplicate_count": duplicate_count,
                            "skipped_count": skipped_count,
                            "not_transaction_count": not_transaction_count,
                            "needs_review_count": needs_review_count,
                            "merchant_memory_updated_count": merchant_memory_updated_count,
                            "net_change": net_change,
                            "date_range_start": preview_summary.get("date_range_start", ""),
                            "date_range_end": preview_summary.get("date_range_end", ""),
                            "date_range_label": preview_summary.get("date_range_label", "Date range unavailable"),
                            "starting_balance": starting_balance,
                            "ending_balance": ending_balance,
                            "import_batch_id": import_batch_id,
                        }

        elif form_name == "clear_preview":
            clear_import_preview()
            preview = None
        elif form_name == "delete_staged_rows":
            preview = load_import_preview()
            selected_ids = {raw_id.strip() for raw_id in request.form.getlist("selected_row_ids") if raw_id.strip()}
            if not preview:
                import_error = "Import preview expired. Upload the file again."
            elif not selected_ids:
                import_error = "Select one or more staged rows to delete."
            else:
                existing_rows = list(preview.get("rows") or [])
                remaining_rows = [row for row in existing_rows if str(row.get("row_id")) not in selected_ids]
                removed_count = len(existing_rows) - len(remaining_rows)
                if removed_count <= 0:
                    import_error = "Selected rows were not found in this staged import."
                else:
                    preview["rows"] = remaining_rows
                    preview = refresh_preview_payload(preview)
                    active_preview_id = session.get("import_preview_id")
                    if remaining_rows and active_preview_id:
                        current_job = ImportJob.query.filter_by(user_id=user_id, preview_id=active_preview_id).first()
                        if current_job:
                            sync_import_job_review_payload(current_job, preview, persist_preview_file=True, set_active_session=True)
                            db.session.commit()
                        else:
                            save_import_preview(user_id, preview, preview_id=active_preview_id)
                        import_success = f"Deleted {removed_count} staged row{'s' if removed_count != 1 else ''} from this review."
                    else:
                        preview = None
                        current_job = ImportJob.query.filter_by(user_id=user_id, preview_id=active_preview_id).first() if active_preview_id else None
                        if current_job and import_job_is_staged(current_job):
                            delete_staged_import_job(current_job, user_id)
                            db.session.commit()
                        else:
                            clear_import_preview()
                        import_success = "Deleted all staged rows. The import review was removed."

    import_jobs = recent_import_jobs_for_user(user_id, limit=8)
    grouped_import_jobs = group_import_jobs(import_jobs)
    latest_active_job = next((job for job in import_jobs if job["status"] in {"queued", "processing"}), None)
    latest_ready_job = next((job for job in import_jobs if job["is_ready_for_review"]), None)
    if not preview and not import_summary and reopen_summary_job_id:
        reopened_job = scoped_record(ImportJob, reopen_summary_job_id, user_id)
        if reopened_job and (reopened_job.status or "").lower() == "imported":
            import_summary = parse_import_job_summary(reopened_job.summary_json)
            selected_account_id = reopened_job.account_id
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
    current_preview_job = None
    active_preview_id = session.get("import_preview_id")
    if active_preview_id:
        current_preview_job = ImportJob.query.filter_by(user_id=user_id, preview_id=active_preview_id).first()
    selected_account_name = next((account.name for account in accounts if account.id == selected_account_id), None)
    return render_template(
        "imports.html",
        accounts=accounts,
        plaid_summary=plaid_summary,
        preview=preview,
        import_error=import_error,
        import_success=import_success,
        import_summary=import_summary,
        category_choices=category_choices,
        category_groups=category_grouped_choices(user_id),
        subcategory_map=category_subcategory_map(),
        current_preview_job=current_preview_job,
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

    account = scoped_record(Account, latest_batch.account_id, user_id)
    if account:
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
    job = scoped_record(ImportJob, job_id, user_id)
    if not job:
        push_ui_feedback("That import job is no longer available.", "danger")
        return redirect("/imports")

    job_status = (job.status or "").lower()
    if job_status == "imported":
        session["reopen_import_summary_job_id"] = job.id
        push_ui_feedback("That import was already completed. AkuOS reopened the completion summary instead.", "info")
        return redirect("/imports")

    if job_status != "completed" or not job.preview_id:
        push_ui_feedback("That import job is still processing. AkuOS opened the job details so you can track progress.", "info")
        return redirect(url_for("import_job_detail", job_id=job.id))

    review_payload = load_import_preview_by_id(job.preview_id)
    if not review_payload:
        push_ui_feedback("AkuOS no longer has the saved review rows for that import job. Please re-upload the original files to rebuild the review.", "danger")
        return redirect("/imports")

    save_import_preview(user_id, review_payload, preview_id=job.preview_id)
    if not activate_import_preview(job.preview_id):
        push_ui_feedback("AkuOS found the import job but could not reactivate the saved review. Please try reopening it again.", "danger")
        return redirect("/imports")

    session["last_seen_completed_import_job_id"] = job.id
    return redirect("/imports")


@app.route("/imports/review/save", methods=["POST"])
def save_import_review():
    if not require_login():
        return jsonify({"error": "Login required."}), 401

    user_id = get_user_id()
    active_preview_id = session.get("import_preview_id")
    preview = load_import_preview()
    current_job = import_job_for_preview(active_preview_id, user_id)
    if not preview or not active_preview_id or not current_job or not import_job_is_staged(current_job):
        return jsonify({"error": "No staged import review is currently active."}), 409

    preview = apply_review_form_to_preview(preview, request.form)
    sync_import_job_review_payload(current_job, preview, persist_preview_file=True, set_active_session=True)
    db.session.commit()
    return jsonify({
        "ok": True,
        "saved_at": datetime.utcnow().isoformat(),
        "needs_review_count": (preview.get("summary") or {}).get("needs_review_count", 0),
    })


@app.route("/imports/jobs/<job_id>")
def import_job_detail(job_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    job = scoped_record(ImportJob, job_id, user_id)
    if not job:
        push_ui_feedback("That import job is no longer available.", "danger")
        return redirect("/imports")

    account = scoped_record(Account, job.account_id, user_id)
    summary = parse_import_job_summary(job.summary_json)
    if not summary.get("date_range_label"):
        summary["date_range_label"] = format_import_date_range(job.start_date, job.end_date)
    preview_payload = load_import_preview_by_id(job.preview_id) if job.preview_id else None
    parser_debug = summary.get("parser_debug") or (preview_payload or {}).get("parser_debug") or []
    preview_rows = (preview_payload or {}).get("rows", [])
    matched_rows = [row for row in preview_rows if not row.get("is_duplicate")]
    failed_rows = []
    for debug in parser_debug:
        for sample in debug.get("sample_rejections", []):
            failed_rows.append(sample)

    return render_template(
        "import_job_detail.html",
        job=job,
        account=account,
        status_label=import_job_status_label(job.status),
        summary=summary,
        parser_debug=parser_debug,
        matched_rows=matched_rows[:50],
        failed_rows=failed_rows[:50],
        preview_payload=preview_payload,
    )


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

    jobs = (
        ImportJob.query
        .filter(
            ImportJob.user_id == user_id,
            ImportJob.id.in_(job_ids),
        )
        .all()
    )
    deleted_count = 0
    for job in jobs:
        if session.get("import_preview_id") == job.preview_id:
            session.pop("import_preview_id", None)
        delete_import_preview_by_id(job.preview_id)
        remove_import_job_files(job.id)
        db.session.delete(job)
        deleted_count += 1
    db.session.commit()
    push_ui_feedback(
        f"Cleared {deleted_count} import job{'s' if deleted_count != 1 else ''}.",
        "success",
    )
    return redirect("/imports")


@app.route("/imports/jobs/<job_id>/delete", methods=["POST"])
def delete_staged_import(job_id):
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    job = scoped_record(ImportJob, job_id, user_id)
    redirect_to = (request.form.get("redirect_to") or "/imports").strip()
    if not redirect_to.startswith("/"):
        redirect_to = "/imports"

    if not job:
        push_ui_feedback("That staged import is no longer available.", "danger")
        return redirect(redirect_to)

    if not import_job_is_staged(job):
        push_ui_feedback("Only uncommitted imports that are ready for review can be deleted.", "danger")
        return redirect(redirect_to)

    account = scoped_record(Account, job.account_id, user_id)
    account_name = account.name if account else "your account"
    deleted = delete_staged_import_job(job, user_id)
    if deleted:
        log_activity(
            user_id,
            "Deleted staged import",
            f"Removed the uncommitted import review for {account_name}. No committed transactions were changed.",
            kind="import_deleted",
            icon="bi-x-circle",
            target_url="/imports",
        )
        db.session.commit()
        push_ui_feedback(
            "Staged import deleted. You can upload the same file again and start a fresh review.",
            "success",
        )
    else:
        push_ui_feedback("That import could not be deleted.", "danger")

    return redirect(redirect_to)


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
    subcategory = request.form.get("subcategory", "").strip()
    tags = serialize_tags(request.form.get("tags", ""))

    if dt is None or not display_name or amount is None:
        push_ui_feedback("Enter a date, description, and valid amount to save the transaction.", "danger")
        return redirect("/")

    categorization = None
    category_source = "Manual"
    category_confidence = "high"
    if not category:
        categorization = categorize_transaction_detailed(user_id, raw_description, amount, tx_date=dt)
        category = (categorization.get("category") or "Needs Review").strip()
        subcategory = (categorization.get("subcategory") or "").strip()
        category_source = (categorization.get("category_source") or "Auto").strip()
        category_confidence = categorization.get("category_confidence") or "uncategorized"
        if normalize_rule_display_name((categorization or {}).get("rule_display_name", "")):
            display_name = normalize_rule_display_name(categorization.get("rule_display_name"))
        tags = merge_transaction_tags(tags, (categorization or {}).get("rule_tags", ""))
    else:
        category, subcategory = canonical_category_pair(category, subcategory)
        remember_merchant_category(
            user_id,
            raw_description,
            category,
            subcategory=subcategory,
            display_name=display_name,
            subtype=transaction_subtype_for(amount, category, "Manual"),
        )
        upsert_learned_category_rule(
            user_id,
            raw_description,
            category,
            subcategory=subcategory,
            subtype=transaction_subtype_for(amount, category, "Manual"),
        )

    account_id = int(account_id)

    tx = Transaction(
        user_id=user_id,
        account_id=account_id,
        date=dt,
        description=display_name,
        raw_description=raw_description,
        display_name=display_name,
        normalized_description=derive_normalized_description(raw_description or display_name),
        merchant_guess=derive_merchant_guess(raw_description or display_name),
        amount=amount,
        category=category,
        subcategory=subcategory,
        suggested_category_id=(categorization or {}).get("suggested_category_id"),
        suggested_subcategory_id=(categorization or {}).get("suggested_subcategory_id"),
        category_source=category_source,
        category_confidence=category_confidence,
        matched_rule_id=(categorization or {}).get("matched_rule_id"),
        needs_review=category.lower() in GENERIC_CATEGORIES or category_confidence in {"low", "uncategorized", "error"},
        transaction_subtype=((categorization or {}).get("transaction_subtype") or transaction_subtype_for(amount, category, category_source)),
        import_source="manual",
        tags=tags,
    )

    db.session.add(tx)

    acct = scoped_record(Account, account_id, user_id)
    if acct:
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
    new_subcategory = request.form.get("subcategory", "").strip()
    redirect_to = request.form.get("redirect_to", "/").strip()
    if not redirect_to.startswith("/"):
        redirect_to = "/"

    if not tx_id or not new_category:
        return redirect(redirect_to)

    transaction = scoped_record(Transaction, tx_id, user_id)
    if not transaction:
        return redirect(redirect_to)

    # update transaction category
    previous_rule_id = transaction.matched_rule_id
    new_category, new_subcategory = canonical_category_pair(new_category, new_subcategory)
    transaction.category = new_category
    transaction.subcategory = new_subcategory
    transaction.category_source = "Manual Review"
    transaction.category_confidence = "high"
    transaction.needs_review = False
    transaction.matched_rule_id = None
    transaction.transaction_subtype = transaction_subtype_for(transaction.amount, new_category, "Manual Review")
    transaction.normalized_description = derive_normalized_description(transaction_reference_description(transaction))
    transaction.merchant_guess = derive_merchant_guess(transaction_reference_description(transaction))

    remember_merchant_category(
        user_id,
        transaction_reference_description(transaction),
        new_category,
        subcategory=new_subcategory,
        display_name=transaction_display_name(transaction),
        subtype=transaction.transaction_subtype,
    )
    learned_rule = upsert_learned_category_rule(
        user_id,
        transaction_reference_description(transaction),
        new_category,
        subcategory=new_subcategory,
        subtype=transaction.transaction_subtype,
        matched_rule_id=previous_rule_id,
    )
    if learned_rule:
        transaction.matched_rule_id = learned_rule.id
    similar_count = apply_category_to_similar_transactions(
        source_tx=transaction,
        user_id=user_id,
        category_name=new_category,
        subcategory_name=new_subcategory,
        subtype=transaction.transaction_subtype,
        matched_rule_id=transaction.matched_rule_id,
        apply_to_similar=True,
    )
    log_activity(
        user_id,
        f"Updated category for {transaction_display_name(transaction)}",
        f"Saved as {transaction_category_label(transaction)}.",
        kind="category_updated",
        icon="bi-tags",
        target_url=redirect_to,
    )

    db.session.commit()
    similar_feedback = (
        f" Updated {similar_count} similar transaction{'s' if similar_count != 1 else ''} too."
        if similar_count
        else ""
    )
    push_ui_feedback(f"Category correction saved.{similar_feedback}", "success")

    return redirect(redirect_to)


@app.route("/edit_tx/<int:tx_id>", methods=["GET", "POST"])
def edit_tx(tx_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    redirect_to = request.values.get("redirect_to", url_for("transactions_page")).strip()
    if not redirect_to.startswith("/"):
        redirect_to = url_for("transactions_page")
    tx = scoped_record(Transaction, tx_id, user_id)
    if not tx:
        return redirect(redirect_to)

    accounts = Account.query.filter_by(user_id=user_id).all()
    category_choices = import_category_choices(user_id)
    category_groups = category_grouped_choices(user_id)
    subcategory_map = category_subcategory_map()
    current_tx_category, current_tx_subcategory = canonical_category_pair(tx.category, getattr(tx, "subcategory", ""))

    if request.method == "POST":
        new_date = parse_date_any(request.form.get("date"))
        new_raw_desc = request.form.get("raw_description", "").strip() or transaction_raw_description(tx)
        new_display_name = clean_transaction_description(request.form.get("display_name", "").strip() or request.form.get("description", "").strip())
        new_amount = safe_float(request.form.get("amount"))
        new_category = request.form.get("category", "").strip()
        new_subcategory = request.form.get("subcategory", "").strip()
        new_tags = serialize_tags(request.form.get("tags", ""))
        new_account_id = int(request.form.get("account_id"))
        requested_subtype = (request.form.get("transaction_subtype") or "").strip().lower()
        create_rule = normalize_rule_skip(request.form.get("create_rule"))
        rule_pattern = (request.form.get("rule_pattern") or "").strip()
        rule_match_type = (request.form.get("rule_match_type") or "exact").strip().lower()
        rule_priority = request.form.get("rule_priority", "1000").strip()
        rule_skip_transaction = normalize_rule_skip(request.form.get("rule_skip_transaction"))
        previous_category = canonical_transaction_category(tx.category)
        previous_subcategory = (getattr(tx, "subcategory", "") or "").strip()

        if new_date is None or not new_display_name or new_amount is None:
            return "Invalid input"

        # reverse old impact
        old_acct = scoped_record(Account, tx.account_id, user_id)
        if old_acct:
            old_acct.balance -= tx.amount

        # apply new data
        tx.date = new_date
        tx.raw_description = new_raw_desc
        tx.display_name = new_display_name
        tx.description = new_display_name
        tx.amount = new_amount
        previous_rule_id = tx.matched_rule_id
        if new_category:
            resolved_category, resolved_subcategory = canonical_category_pair(new_category, new_subcategory)
            resolved_source = "Manual Edit"
            resolved_confidence = "high"
            resolved_rule_id = None
            suggested_category_id, suggested_subcategory_id = resolve_category_ids(resolved_category, resolved_subcategory)
            needs_review_flag = resolved_category == "Needs Review"
        else:
            categorization = categorize_transaction_detailed(user_id, new_raw_desc, new_amount, tx_date=new_date)
            resolved_category = (categorization.get("category") or "Needs Review").strip()
            resolved_subcategory = (categorization.get("subcategory") or "").strip()
            resolved_source = categorization.get("category_source") or "Auto"
            resolved_confidence = categorization.get("category_confidence") or "uncategorized"
            resolved_rule_id = categorization.get("matched_rule_id")
            suggested_category_id = categorization.get("suggested_category_id")
            suggested_subcategory_id = categorization.get("suggested_subcategory_id")
            needs_review_flag = categorization.get("needs_review", False)
        tx.category = resolved_category
        tx.subcategory = resolved_subcategory
        tx.category_source = resolved_source
        tx.category_confidence = resolved_confidence
        tx.normalized_description = derive_normalized_description(new_raw_desc or new_display_name)
        tx.merchant_guess = derive_merchant_guess(new_raw_desc or new_display_name)
        tx.matched_rule_id = resolved_rule_id
        tx.suggested_category_id = suggested_category_id
        tx.suggested_subcategory_id = suggested_subcategory_id
        tx.needs_review = needs_review_flag
        tx.transaction_subtype = requested_subtype if requested_subtype in VALID_TRANSACTION_SUBTYPES else transaction_subtype_for(new_amount, resolved_category, resolved_source)
        tx.account_id = new_account_id
        tx.tags = new_tags

        if new_category:
            remember_merchant_category(user_id, new_raw_desc, resolved_category, subcategory=resolved_subcategory, display_name=new_display_name, subtype=tx.transaction_subtype)
            if create_rule:
                learned_rule = upsert_transaction_rule(
                    user_id,
                    new_raw_desc or new_display_name,
                    resolved_category,
                    subcategory=resolved_subcategory,
                    subtype=tx.transaction_subtype,
                    display_name=new_display_name,
                    tags=new_tags,
                    skip_transaction=rule_skip_transaction,
                    matched_rule_id=previous_rule_id,
                    match_type=rule_match_type,
                    priority=rule_priority,
                    amount_direction=direction_label_for_subtype(tx.transaction_subtype, new_amount),
                    pattern=rule_pattern,
                )
                if learned_rule:
                    tx.matched_rule_id = learned_rule.id
            elif (resolved_category, resolved_subcategory) != canonical_category_pair(previous_category, previous_subcategory):
                learned_rule = upsert_learned_category_rule(
                    user_id,
                    new_raw_desc,
                    resolved_category,
                    subcategory=resolved_subcategory,
                    subtype=tx.transaction_subtype,
                    matched_rule_id=previous_rule_id,
                )
                if learned_rule:
                    tx.matched_rule_id = learned_rule.id
            apply_category_to_similar_transactions(
                source_tx=tx,
                user_id=user_id,
                category_name=resolved_category,
                subcategory_name=resolved_subcategory,
                subtype=tx.transaction_subtype,
                matched_rule_id=tx.matched_rule_id,
                apply_to_similar=True,
            )

        new_acct = scoped_record(Account, new_account_id, user_id)
        if new_acct:
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
        current_tx_category=current_tx_category,
        current_tx_subcategory=current_tx_subcategory,
        accounts=accounts,
        redirect_to=redirect_to,
        category_choices=category_choices,
        category_groups=category_groups,
        subcategory_map=subcategory_map,
        suggested_rule_pattern=learned_rule_pattern(transaction_raw_description(tx) or transaction_display_name(tx) or ""),
        rule_match_type_choices=rule_match_type_options(),
        tx_tags=", ".join(display_tag(tag) for tag in parse_tags(getattr(tx, "tags", ""))),
    )


@app.route("/delete_tx/<int:tx_id>", methods=["POST"])
def delete_tx(tx_id):
    if not require_login():
        return redirect("/login")
    user_id = get_user_id()
    tx = scoped_record(Transaction, tx_id, user_id)
    if tx:
        description = transaction_display_name(tx)
        acct = scoped_record(Account, tx.account_id, user_id)
        if acct:
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
    limited_response = rate_limit_response(
        "import-upload",
        limit=10,
        window_seconds=600,
        html_fallback="/imports",
        message="Too many import attempts. Please wait a few minutes and try again.",
    )
    if limited_response:
        return limited_response
    account_id = request.form.get("account_id")
    files = [file for file in request.files.getlist("files") if file and file.filename]
    if not files:
        single_file = request.files.get("file")
        if single_file and single_file.filename:
            files = [single_file]

    if not account_id or not files:
        return redirect("/imports")

    payload, error, _ = build_import_preview(user_id, files, account_id)
    if error:
        return redirect("/imports")

    save_import_preview(user_id, payload)
    return redirect("/imports")


@app.route("/transactions", methods=["GET", "POST"])
def transactions_page():
    if not require_login():
        return redirect("/login")

    user_id = get_user_id()
    return_to = safe_local_redirect(request.form.get("return_to"), url_for("transactions_page"))
    if request.method == "POST":
        row_action = (request.form.get("row_action") or "").strip().lower()
        if row_action:
            tx_id = safe_int(request.form.get("tx_id"))
            tx = scoped_record(Transaction, tx_id, user_id) if tx_id else None
            if not tx:
                push_ui_feedback("That transaction is no longer available.", "danger")
                return redirect(return_to)

            quick_category = (request.form.get("quick_category") or "").strip()
            quick_subcategory = (request.form.get("quick_subcategory") or "").strip()
            quick_subtype = (request.form.get("quick_subtype") or "").strip().lower()
            quick_status = (request.form.get("quick_status") or "").strip().lower() or "reviewed"
            apply_to_similar = normalize_rule_skip(request.form.get("apply_to_similar"))
            suggested_category, suggested_subcategory = transaction_suggested_category_pair(tx)

            if row_action == "approve":
                approval_category, approval_subcategory = canonical_category_pair(
                    quick_category or tx.category,
                    quick_subcategory or getattr(tx, "subcategory", ""),
                )
                if not transaction_can_be_approved(
                    tx,
                    category_name=approval_category,
                    subcategory_name=approval_subcategory,
                ):
                    push_ui_feedback("Categorize this transaction before approving it.", "danger")
                    return redirect(return_to)
                review_result = apply_manual_transaction_review(
                    tx,
                    user_id,
                    category_name=approval_category,
                    subcategory_name=approval_subcategory,
                    subtype=quick_subtype or getattr(tx, "transaction_subtype", ""),
                    review_status="reviewed",
                    apply_to_similar=apply_to_similar,
                )
                similar_count = int((review_result or {}).get("similar_count") or 0)
                similar_feedback = (
                    f" Updated {similar_count} similar transaction{'s' if similar_count != 1 else ''} too."
                    if similar_count
                    else ""
                )
                push_ui_feedback(f"{transaction_display_name(tx)} is marked reviewed.{similar_feedback}", "success")
            elif row_action == "use_suggestion":
                if not suggested_category:
                    push_ui_feedback("No stronger suggestion is available for that transaction yet.", "danger")
                    return redirect(return_to)
                review_result = apply_manual_transaction_review(
                    tx,
                    user_id,
                    category_name=suggested_category,
                    subcategory_name=suggested_subcategory,
                    subtype=quick_subtype or getattr(tx, "transaction_subtype", ""),
                    review_status="reviewed",
                    apply_to_similar=apply_to_similar,
                )
                similar_count = int((review_result or {}).get("similar_count") or 0)
                similar_feedback = (
                    f" Updated {similar_count} similar transaction{'s' if similar_count != 1 else ''} too."
                    if similar_count
                    else ""
                )
                push_ui_feedback(f"Applied {suggested_category} to {transaction_display_name(tx)}.{similar_feedback}", "success")
            elif row_action == "save_quick":
                resolved_quick_category, _resolved_quick_subcategory = canonical_category_pair(
                    quick_category or tx.category,
                    quick_subcategory or getattr(tx, "subcategory", ""),
                )
                effective_quick_status = (
                    "reviewed"
                    if resolved_quick_category and resolved_quick_category != "Needs Review"
                    else quick_status
                )
                review_result = apply_manual_transaction_review(
                    tx,
                    user_id,
                    category_name=quick_category or tx.category,
                    subcategory_name=quick_subcategory or getattr(tx, "subcategory", ""),
                    subtype=quick_subtype or getattr(tx, "transaction_subtype", ""),
                    review_status=effective_quick_status,
                    apply_to_similar=apply_to_similar and effective_quick_status != "needs_attention",
                )
                similar_count = int((review_result or {}).get("similar_count") or 0)
                similar_feedback = (
                    f" Updated {similar_count} similar transaction{'s' if similar_count != 1 else ''} too."
                    if similar_count
                    else ""
                )
                if effective_quick_status == "reviewed":
                    push_ui_feedback(f"Transaction reviewed for {transaction_display_name(tx)}.{similar_feedback}", "success")
                else:
                    push_ui_feedback(f"Saved quick review changes for {transaction_display_name(tx)}.{similar_feedback}", "success")
            elif row_action == "mark_needs_attention":
                apply_manual_transaction_review(
                    tx,
                    user_id,
                    category_name=quick_category or tx.category,
                    subcategory_name=quick_subcategory or getattr(tx, "subcategory", ""),
                    subtype=quick_subtype or getattr(tx, "transaction_subtype", ""),
                    review_status="needs_attention",
                    apply_to_similar=False,
                )
                push_ui_feedback(f"{transaction_display_name(tx)} is back in Needs Attention.", "success")
            else:
                push_ui_feedback("That review action is not available.", "danger")
                return redirect(return_to)

            log_activity(
                user_id,
                f"Reviewed {transaction_display_name(tx)}",
                f"Quick review updated {transaction_category_label(tx)} as {transaction_type_label(tx)}.",
                kind="transaction_edited",
                icon="bi-lightning-charge",
                target_url=return_to,
            )
            db.session.commit()
            return redirect(return_to)

        selected_ids = [safe_int(value) for value in request.form.getlist("selected_tx")]
        selected_ids = [value for value in selected_ids if value]
        action = (request.form.get("bulk_action") or "").strip().lower()
        bulk_category = (request.form.get("bulk_category") or "").strip()
        bulk_tags = serialize_tags(request.form.get("bulk_tags", ""))
        bulk_subtype = (request.form.get("bulk_subtype") or "").strip().lower()

        if not selected_ids:
            push_ui_feedback("Select at least one transaction first.", "danger")
            return redirect(return_to)

        transactions_to_update = (
            Transaction.query
            .filter(Transaction.user_id == user_id, Transaction.id.in_(selected_ids))
            .all()
        )
        if not transactions_to_update:
            push_ui_feedback("Those transactions are no longer available.", "danger")
            return redirect(return_to)

        updated_count = 0
        if action == "set_category" and bulk_category:
            for tx in transactions_to_update:
                apply_manual_transaction_review(
                    tx,
                    user_id,
                    category_name=bulk_category,
                    subtype=bulk_subtype or getattr(tx, "transaction_subtype", ""),
                    review_status="reviewed",
                    apply_to_similar=False,
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
        elif action == "mark_reviewed":
            for tx in transactions_to_update:
                apply_manual_transaction_review(
                    tx,
                    user_id,
                    category_name=tx.category,
                    subcategory_name=getattr(tx, "subcategory", ""),
                    subtype=getattr(tx, "transaction_subtype", ""),
                    review_status="reviewed",
                    apply_to_similar=False,
                )
                updated_count += 1
            push_ui_feedback(f"Marked {updated_count} transaction{'s' if updated_count != 1 else ''} as reviewed.", "success")
        elif action == "mark_needs_attention":
            for tx in transactions_to_update:
                apply_manual_transaction_review(
                    tx,
                    user_id,
                    category_name=tx.category,
                    subcategory_name=getattr(tx, "subcategory", ""),
                    subtype=getattr(tx, "transaction_subtype", ""),
                    review_status="needs_attention",
                    apply_to_similar=False,
                )
                updated_count += 1
            push_ui_feedback(f"Moved {updated_count} transaction{'s' if updated_count != 1 else ''} back to Needs Attention.", "success")
        else:
            push_ui_feedback("Choose a valid bulk action and value to continue.", "danger")
            return redirect(return_to)

        log_activity(
            user_id,
            "Bulk updated transactions",
            f"{updated_count} transactions were updated from the transactions command center.",
            kind="transaction_edited",
            icon="bi-sliders",
            target_url=return_to,
        )
        db.session.commit()
        return redirect(return_to)

    query_text = request.args.get("q", "").strip()
    category_filter = request.args.get("category", "").strip()
    type_filter = request.args.get("type", "").strip().lower()
    account_filter = safe_int(request.args.get("account_id", ""))
    tag_filter = normalize_tag_label(request.args.get("tag", ""))
    source_filter = request.args.get("source", "").strip()
    status_filter = (request.args.get("status", "") or "").strip().lower()
    sort_filter = (request.args.get("sort", "newest") or "newest").strip().lower()
    page_size = safe_int(request.args.get("page_size", "")) or 50
    current_page = safe_int(request.args.get("page", "")) or 1
    date_preset_filter = (request.args.get("date_preset", "") or "").strip().lower()
    selected_month_filter = (request.args.get("month", "") or "").strip()
    requested_start_date = parse_date_any(request.args.get("start_date", ""))
    requested_end_date = parse_date_any(request.args.get("end_date", ""))
    valid_preset_values = {value for value, _label in TRANSACTION_DATE_PRESET_OPTIONS}
    if date_preset_filter not in valid_preset_values:
        date_preset_filter = ""
    normalized_month_filter, month_start_date, month_end_date = parse_month_filter_key(selected_month_filter)
    selected_month_filter = normalized_month_filter
    if month_start_date and month_end_date:
        start_date_filter = month_start_date
        end_date_filter = month_end_date
        date_preset_filter = ""
    else:
        preset_start_date, preset_end_date = transaction_date_preset_range(date_preset_filter)
        if preset_start_date and preset_end_date:
            start_date_filter = preset_start_date
            end_date_filter = preset_end_date
            selected_month_filter = ""
        else:
            start_date_filter = requested_start_date
            end_date_filter = requested_end_date
    if start_date_filter and end_date_filter and start_date_filter > end_date_filter:
        start_date_filter, end_date_filter = end_date_filter, start_date_filter
    valid_sort_values = {value for value, _label in TRANSACTION_SORT_OPTIONS}
    if sort_filter not in valid_sort_values:
        sort_filter = "newest"
    if page_size not in TRANSACTION_PAGE_SIZE_OPTIONS:
        page_size = 50
    if current_page < 1:
        current_page = 1

    with timed_route_section("transactions", "filtered_query"):
        category_filter_options = transaction_filter_category_options(user_id)
        category_filter_lookup = {
            normalize_text(option["value"]): option["match_values"]
            for option in category_filter_options
        }
        needs_attention_clause = or_(
            Transaction.needs_review == True,  # noqa: E712
            func.lower(func.coalesce(Transaction.category, "")) == "needs review",
            func.lower(func.coalesce(Transaction.category_confidence, "")).in_(["error", "uncategorized", "low"]),
        )
        filtered_query = (
            Transaction.query
            .filter(Transaction.user_id == user_id)
            .options(*transaction_minimal_load_options())
        )

        if query_text:
            lowered = query_text.lower()
            parsed_date = parse_date_any(query_text)
            search_like = f"%{lowered}%"
            search_clauses = [
                func.lower(func.coalesce(Transaction.display_name, "")).like(search_like),
                func.lower(func.coalesce(Transaction.raw_description, "")).like(search_like),
                func.lower(func.coalesce(Transaction.description, "")).like(search_like),
                func.lower(func.coalesce(Transaction.category, "")).like(search_like),
                func.lower(func.coalesce(Transaction.subcategory, "")).like(search_like),
            ]
            if parsed_date is not None:
                search_clauses.append(Transaction.date == parsed_date)
            filtered_query = filtered_query.filter(or_(*search_clauses))

        if category_filter:
            category_matches = category_filter_lookup.get(normalize_text(category_filter)) or [category_filter]
            filtered_query = filtered_query.filter(
                func.lower(func.coalesce(Transaction.category, "")).in_(
                    [match_value.lower() for match_value in category_matches if match_value]
                )
            )

        if account_filter:
            filtered_query = filtered_query.filter(Transaction.account_id == account_filter)

        if tag_filter:
            filtered_query = filtered_query.filter(or_(*tag_filter_clauses(tag_filter)))

        if type_filter:
            filtered_query = filtered_query.filter(
                func.lower(func.coalesce(Transaction.transaction_subtype, "")) == type_filter
            )

        if source_filter:
            filtered_query = filtered_query.filter(Transaction.category_source == source_filter)

        if status_filter == "needs_attention":
            filtered_query = filtered_query.filter(needs_attention_clause)
        elif status_filter == "errors":
            filtered_query = filtered_query.filter(
                func.lower(func.coalesce(Transaction.category_confidence, "")) == "error"
            )
        elif status_filter == "reviewed":
            filtered_query = filtered_query.filter(~needs_attention_clause)

        if start_date_filter:
            filtered_query = filtered_query.filter(Transaction.date >= start_date_filter)
        if end_date_filter:
            filtered_query = filtered_query.filter(Transaction.date <= end_date_filter)

    with timed_route_section("transactions", "counts"):
        total_results = filtered_query.order_by(None).count()
        overall_transaction_count = (
            Transaction.query
            .filter(Transaction.user_id == user_id)
            .order_by(None)
            .count()
        )
        all_needs_attention_count = (
            Transaction.query
            .filter(Transaction.user_id == user_id)
            .filter(needs_attention_clause)
            .order_by(None)
            .count()
        )
        total_pages = max(1, math.ceil(total_results / page_size)) if total_results else 1
        if current_page > total_pages:
            current_page = total_pages

    ordered_query = filtered_query
    if sort_filter == "oldest":
        ordered_query = ordered_query.order_by(Transaction.date.asc(), Transaction.id.asc())
    elif sort_filter == "highest_amount":
        ordered_query = ordered_query.order_by(Transaction.amount.desc(), Transaction.date.desc(), Transaction.id.desc())
    elif sort_filter == "lowest_amount":
        ordered_query = ordered_query.order_by(Transaction.amount.asc(), Transaction.date.asc(), Transaction.id.asc())
    else:
        ordered_query = ordered_query.order_by(Transaction.date.desc(), Transaction.id.desc())

    with timed_route_section("transactions", "page_rows"):
        page_offset = max((current_page - 1) * page_size, 0)
        transactions = ordered_query.offset(page_offset).limit(page_size).all()

    page_start_index = page_offset + 1 if total_results else 0
    page_end_index = min(page_offset + len(transactions), total_results) if total_results else 0

    def build_transactions_page_url(**overrides):
        params = {}
        for key, value in request.args.items():
            if value not in (None, ""):
                params[key] = value
        for key, value in overrides.items():
            if value in (None, "", []):
                params.pop(key, None)
            else:
                params[key] = str(value)
        return url_for("transactions_page", **params)

    page_size_options = [
        {
            "value": size,
            "label": f"{size} / page",
            "url": build_transactions_page_url(page=1, page_size=size),
            "selected": size == page_size,
        }
        for size in TRANSACTION_PAGE_SIZE_OPTIONS
    ]

    pagination_items = []
    if total_pages <= 7:
        page_numbers = list(range(1, total_pages + 1))
    else:
        page_numbers = sorted({
            1,
            total_pages,
            max(1, current_page - 1),
            current_page,
            min(total_pages, current_page + 1),
        })
    previous_number = None
    for page_number in page_numbers:
        if previous_number is not None and page_number - previous_number > 1:
            pagination_items.append({"kind": "ellipsis"})
        pagination_items.append({
            "kind": "page",
            "number": page_number,
            "current": page_number == current_page,
            "url": build_transactions_page_url(page=page_number, page_size=page_size),
        })
        previous_number = page_number
    previous_page_url = build_transactions_page_url(page=current_page - 1, page_size=page_size) if current_page > 1 else ""
    next_page_url = build_transactions_page_url(page=current_page + 1, page_size=page_size) if current_page < total_pages else ""

    range_expense_total = 0.0
    range_income_total = 0.0
    range_net_total = 0.0
    range_category_totals = defaultdict(float)
    range_transaction_count = total_results
    if start_date_filter or end_date_filter:
        with timed_route_section("transactions", "range_summary"):
            summary_transactions = filtered_query.all()
            range_expense_total = round(
                sum(
                    abs(float(tx.amount or 0))
                    for tx in summary_transactions
                    if (getattr(tx, "transaction_subtype", "") or transaction_subtype_for(tx.amount, tx.category, getattr(tx, "category_source", ""))).lower() == "expense"
                    and float(tx.amount or 0) < 0
                ),
                2,
            )
            range_income_total = round(
                sum(
                    float(tx.amount or 0)
                    for tx in summary_transactions
                    if (getattr(tx, "transaction_subtype", "") or transaction_subtype_for(tx.amount, tx.category, getattr(tx, "category_source", ""))).lower() == "income"
                    and float(tx.amount or 0) > 0
                ),
                2,
            )
            range_net_total = round(range_income_total - range_expense_total, 2)
            for tx in summary_transactions:
                tx_subtype = (getattr(tx, "transaction_subtype", "") or transaction_subtype_for(tx.amount, tx.category, getattr(tx, "category_source", ""))).lower()
                if tx_subtype != "expense" or float(tx.amount or 0) >= 0:
                    continue
                category_name = transaction_ui_category(tx.category) or "Uncategorized"
                range_category_totals[category_name] += abs(float(tx.amount or 0))

    range_days = None
    if start_date_filter and end_date_filter:
        range_days = max((end_date_filter - start_date_filter).days + 1, 1)
    average_spend_per_day = round(range_expense_total / range_days, 2) if range_days else None

    with timed_route_section("transactions", "filter_metadata"):
        categories = category_filter_options
        user_accounts = Account.query.filter_by(user_id=user_id).all()
        source_choices = dedupe_filter_display_values(
            source_name
            for source_name, in (
                db.session.query(Transaction.category_source)
                .filter(
                    Transaction.user_id == user_id,
                    Transaction.category_source.isnot(None),
                    Transaction.category_source != "",
                )
                .distinct()
                .all()
            )
            if source_name
        )
        known_tags = sorted({
            tag
            for raw_tags, in (
                db.session.query(Transaction.tags)
                .filter(
                    Transaction.user_id == user_id,
                    Transaction.tags.isnot(None),
                    Transaction.tags != "",
                )
                .distinct()
                .all()
            )
            for tag in parse_tags(raw_tags or "")
        })
        recent_dates = [
            tx_date
            for tx_date, in (
                db.session.query(Transaction.date)
                .filter(Transaction.user_id == user_id, Transaction.date.isnot(None))
                .order_by(Transaction.date.desc())
                .limit(400)
                .all()
            )
            if tx_date
        ]
    account_name_map = {account.id: account.name for account in user_accounts}
    month_option_pairs = {
        (
            (date.today().year * 12 + date.today().month - 1 - offset) // 12,
            (date.today().year * 12 + date.today().month - 1 - offset) % 12 + 1,
        )
        for offset in range(18)
    }
    month_option_pairs.update(
        (tx_date.year, tx_date.month)
        for tx_date in recent_dates
    )
    month_options = [
        {
            "value": f"{year_value:04d}-{month_value:02d}",
            "label": f"{calendar.month_name[month_value]} {year_value}",
        }
        for year_value, month_value in sorted(month_option_pairs, reverse=True)
    ]
    has_transactions = bool(
        db.session.query(Transaction.id).filter(Transaction.user_id == user_id).limit(1).first()
    )
    has_active_filters = any([
        query_text,
        category_filter,
        account_filter,
        type_filter,
        tag_filter,
        source_filter,
        status_filter,
        start_date_filter,
        end_date_filter,
        date_preset_filter,
        selected_month_filter,
    ])
    current_transactions_url = build_transactions_page_url(page=current_page, page_size=page_size)
    review_queue_mode = status_filter in {"needs_attention", "errors"}
    queue_reason_counts = Counter()
    category_lookup = category_lookup_by_id()
    transaction_rows = []
    for tx in transactions:
        confidence_bucket = normalize_confidence_bucket(getattr(tx, "category_confidence", ""))
        needs_attention = transaction_needs_attention(tx)
        review_reasons = transaction_review_reason_list(tx)
        if needs_attention:
            queue_reason_counts.update(review_reasons)
        suggested_category, suggested_subcategory = transaction_suggested_category_pair(tx, category_lookup)
        current_category, current_subcategory = canonical_category_pair(tx.category, getattr(tx, "subcategory", ""))
        category_label_value = (
            f"{current_category} > {current_subcategory}"
            if current_category and current_category != "Needs Review" and current_subcategory
            else current_category
            if current_category and current_category != "Needs Review"
            else "Needs category"
        )
        transaction_rows.append({
            "tx": tx,
            "confidence_bucket": confidence_bucket,
            "needs_attention": needs_attention,
            "status_label": transaction_review_status_label(tx),
            "review_reasons": review_reasons,
            "primary_review_reason": review_reasons[0] if review_reasons else "",
            "secondary_review_reason_count": max(0, len(review_reasons) - 1),
            "review_reason_summary": ", ".join(
                review_reason_display_label(reason)
                for reason in review_reasons
                if review_reason_display_label(reason)
            ),
            "current_category": current_category,
            "current_subcategory": current_subcategory,
            "category_label": category_label_value,
            "has_category": bool(current_category and current_category != "Needs Review"),
            "suggested_category": suggested_category,
            "suggested_subcategory": suggested_subcategory,
            "has_suggestion": bool(suggested_category),
            "can_quick_approve": transaction_can_be_approved(
                tx,
                category_name=current_category,
                subcategory_name=current_subcategory,
            ),
        })
    visible_needs_attention_count = sum(1 for row in transaction_rows if row["needs_attention"])
    preset_label_map = {value: label for value, label in TRANSACTION_DATE_PRESET_OPTIONS}
    if selected_month_filter and month_start_date:
        selected_range_label = f"{calendar.month_name[month_start_date.month]} {month_start_date.year}"
    elif date_preset_filter:
        selected_range_label = preset_label_map.get(date_preset_filter, "Selected range")
    else:
        selected_range_label = "Custom range" if (start_date_filter or end_date_filter) else "All dates"
    selected_range_dates_label = format_transaction_range_label(start_date_filter, end_date_filter)

    return render_template(
        "transactions.html",
        transactions=transaction_rows,
        total_results=total_results,
        has_transactions=has_transactions,
        categories=categories,
        account_name_map=account_name_map,
        query_text=query_text,
        category_filter=category_filter,
        account_filter=account_filter,
        type_filter=type_filter,
        tag_filter=tag_filter,
        source_filter=source_filter,
        status_filter=status_filter,
        sort_filter=sort_filter,
        page_size=page_size,
        page_size_choices=page_size_options,
        current_page=current_page,
        total_pages=total_pages,
        page_start_index=page_start_index,
        page_end_index=page_end_index,
        previous_page_url=previous_page_url,
        next_page_url=next_page_url,
        pagination_items=pagination_items,
        date_preset_filter=date_preset_filter,
        date_preset_choices=TRANSACTION_DATE_PRESET_OPTIONS,
        selected_month_filter=selected_month_filter,
        start_date_filter=start_date_filter.isoformat() if start_date_filter else "",
        end_date_filter=end_date_filter.isoformat() if end_date_filter else "",
        account_choices=user_accounts,
        month_options=month_options,
        source_choices=source_choices,
        status_choices=TRANSACTION_STATUS_OPTIONS,
        sort_choices=TRANSACTION_SORT_OPTIONS,
        bulk_subtype_choices=[("income", "Income"), ("expense", "Expense"), ("transfer", "Transfer"), ("payment", "Payment")],
        known_tags=known_tags,
        category_choices=transaction_ui_category_choices(user_id),
        category_groups=category_grouped_choices(user_id),
        subcategory_map=category_subcategory_map(),
        has_active_filters=has_active_filters,
        show_range_summary=bool(start_date_filter or end_date_filter),
        range_expense_total=range_expense_total,
        range_income_total=range_income_total,
        range_net_total=range_net_total,
        range_transaction_count=range_transaction_count,
        range_days=range_days,
        average_spend_per_day=average_spend_per_day,
        selected_range_label=selected_range_label,
        selected_range_dates_label=selected_range_dates_label,
        current_transactions_url=current_transactions_url,
        range_category_totals=[
            {"category": category_name, "amount": round(total, 2)}
            for category_name, total in sorted(range_category_totals.items(), key=lambda item: item[1], reverse=True)
        ],
        overall_transaction_count=overall_transaction_count,
        all_needs_attention_count=all_needs_attention_count,
        review_queue_mode=review_queue_mode,
        queue_reason_counts=queue_reason_counts,
        visible_needs_attention_count=visible_needs_attention_count,
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
    if request.method == "HEAD":
        return Response(status=200)
    user_id = get_user_id()

    selected_month, selected_year = month_year_from_request()
    spending_month = request.args.get("spending_month", "").strip()
    spending_year = request.args.get("spending_year", "").strip()
    transaction_q = request.args.get("transaction_q", "").strip()
    tag_filter = normalize_tag_label(request.args.get("tag", ""))
    with timed_route_section("dashboard", "core_queries"):
        accounts = Account.query.filter_by(user_id=user_id).all()
        budgets = Budget.query.filter_by(user_id=user_id).all()
        goals = FinancialGoal.query.filter_by(user_id=user_id).all()
        dashboard_has_transactions = bool(
            db.session.query(Transaction.id).filter(Transaction.user_id == user_id).limit(1).first()
        )
    account_name_map = {a.id: a.name for a in accounts}
    dashboard_empty_state = not dashboard_has_transactions
    onboarding_state = build_onboarding_state(accounts, [], budgets, goals) if dashboard_empty_state else None

    # -------------------------
    # NET WORTH
    # -------------------------
    net_worth_breakdown = build_net_worth_breakdown(accounts)
    total_assets = float(net_worth_breakdown["total_assets"] or 0)
    total_liabilities = float(net_worth_breakdown["total_liabilities"] or 0)
    net_worth = float(net_worth_breakdown["net_worth"] or 0)
    net_worth_explainer = ""
    if dashboard_empty_state and total_liabilities > 0 and total_assets <= 0:
        net_worth_explainer = (
            f"Net worth is negative because liabilities total ${total_liabilities:,.2f} and no asset accounts are connected yet."
        )
    elif dashboard_empty_state and total_liabilities > 0 and total_assets > 0:
        net_worth_explainer = (
            f"Net worth reflects ${total_assets:,.2f} in assets and ${total_liabilities:,.2f} in liabilities."
        )
    if dashboard_empty_state:
        return render_template(
            "home.html",
            accounts=accounts,
            today_iso=date.today().isoformat(),
            transactions=[],
            transaction_q=transaction_q,
            tag_filter=tag_filter,
            account_name_map=account_name_map,
            onboarding_state=onboarding_state,
            dashboard_empty_state=True,
            net_worth_explainer=net_worth_explainer,
            subscriptions=[],
            recurring_income_sources=[],
            recurring_income_estimate=0,
            recurring_bills=[],
            income_allocation_alerts=[],
            effective_monthly_income=0,
            goal_allocation_budget={},
            selected_month=selected_month,
            selected_year=selected_year,
            safe_to_spend={
                "base_safe_to_spend": 0,
                "used_amount": 0,
                "remaining_amount": 0,
                "income_basis": 0,
                "recurring_expenses": 0,
                "savings_target_amount": 0,
                "goal_set_aside_amount": 0,
                "explanation": "Safe-to-spend will appear after transactions are imported.",
            },
            savings_snapshot={"current_savings": 0},
            dashboard_metric_changes={},
            wealth_snapshot={"primary_goal": None, "secondary_goals": []},
            net_worth=net_worth,
            total_assets=total_assets,
            total_liabilities=total_liabilities,
            monthly_income=0,
            monthly_expenses=0,
            category_labels=[],
            category_values=[],
            spending_chart_month=selected_month,
            spending_chart_year=selected_year,
            spending_chart_month_label=f"{calendar.month_name[selected_month]} {selected_year}",
            spending_chart_month_options=[],
            upcoming_payment_account_options=accounts,
            upcoming_payment_category_options=transaction_ui_category_choices(user_id),
            expense_transaction_count=0,
            uncategorized_expense_count=0,
            uncategorized_expense_total=0,
            spending_chart_empty_message="No categorized spending yet",
            spending_chart_notice="Import transactions to start tracking spending.",
            spending_chart_used_fallback=False,
            monthly_overview_labels=[],
            monthly_overview_income=[],
            monthly_overview_expenses=[],
        )

    previous_month = 12 if selected_month == 1 else selected_month - 1
    previous_year = selected_year - 1 if selected_month == 1 else selected_year

    with timed_route_section("dashboard", "monthly_metrics"):
        monthly_income, monthly_expenses = dashboard_month_totals_aggregate(user_id, selected_month, selected_year)
        prev_monthly_income, prev_monthly_expenses = dashboard_month_totals_aggregate(user_id, previous_month, previous_year)
        savings_rate = round(((monthly_income - monthly_expenses) / monthly_income) * 100, 2) if monthly_income > 0 else 0
        current_month_transactions = (
            Transaction.query
            .filter(
                Transaction.user_id == user_id,
                Transaction.date >= date(selected_year, selected_month, 1),
                Transaction.date <= date(selected_year, selected_month, calendar.monthrange(selected_year, selected_month)[1]),
            )
            .options(load_only(Transaction.account_id, Transaction.amount, Transaction.date))
            .all()
        )
        previous_net_worth = compute_previous_net_worth(
            accounts,
            current_month_transactions,
            selected_month,
            selected_year,
        )

    with timed_route_section("dashboard", "goals_and_savings"):
        effective_monthly_income = float(monthly_income or 0)
        savings_snapshot = dashboard_savings_snapshot_light(accounts, effective_monthly_income)
        previous_savings_snapshot = dashboard_savings_snapshot_light(accounts, float(prev_monthly_income or 0))
        wealth_snapshot = build_dashboard_goal_snapshot_light(goals)
        goal_allocation_budget = suggested_goal_allocation_budget(wealth_snapshot["goal_rows"])

    with timed_route_section("dashboard", "safe_to_spend"):
        safe_to_spend = dashboard_safe_to_spend_light(
            monthly_income=monthly_income,
            monthly_expenses=monthly_expenses,
            savings_target_amount=savings_snapshot.get("recommended_amount") or 0,
        )
        previous_safe_to_spend = dashboard_safe_to_spend_light(
            monthly_income=prev_monthly_income,
            monthly_expenses=prev_monthly_expenses,
            savings_target_amount=previous_savings_snapshot.get("recommended_amount") or 0,
        )
        dashboard_metric_changes = {
            "net_worth": build_metric_change(net_worth, previous_net_worth, "up"),
            "income": build_metric_change(monthly_income, prev_monthly_income, "up"),
            "expenses": build_metric_change(monthly_expenses, prev_monthly_expenses, "down"),
            "savings": build_metric_change(savings_snapshot["current_savings"], previous_savings_snapshot["current_savings"], "up"),
            "safe_to_spend": build_metric_change(safe_to_spend["safe_to_spend"], previous_safe_to_spend["safe_to_spend"], "up"),
        }

    with timed_route_section("dashboard", "charts"):
        monthly_overview_labels, monthly_overview_income, monthly_overview_expenses = dashboard_monthly_overview_series_aggregate(user_id)
        spending_chart = dashboard_spending_chart_state_aggregate(
            user_id,
            selected_month,
            selected_year,
            requested_month=spending_month or None,
            requested_year=spending_year or None,
        )

    subscriptions = []
    recurring_income_sources = []
    recurring_income_estimate = 0
    recurring_bills = []
    income_allocation_alerts = []
    budget_rows = []

    recent_transactions_query = (
        Transaction.query
        .filter_by(user_id=user_id)
        .options(*transaction_minimal_load_options())
    )
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

    with timed_route_section("dashboard", "recent_transactions"):
        recent_transactions = (
            recent_transactions_query
            .order_by(Transaction.date.desc(), Transaction.id.desc())
            .limit(8)
            .all()
        )
    for tx in recent_transactions:
        tx.tag_list = parse_tags(getattr(tx, "tags", ""))
        tx.tag_display_list = [display_tag(tag) for tag in tx.tag_list]

    return render_template(
        "home.html",
        accounts=accounts,
        today_iso=date.today().isoformat(),
        transactions=recent_transactions,
        transaction_q=transaction_q,
        tag_filter=tag_filter,
        account_name_map=account_name_map,
        onboarding_state=onboarding_state,
        dashboard_empty_state=dashboard_empty_state,
        net_worth_explainer=net_worth_explainer,
        subscriptions=subscriptions,
        recurring_income_sources=recurring_income_sources,
        recurring_income_estimate=recurring_income_estimate,
        recurring_bills=recurring_bills,
        income_allocation_alerts=income_allocation_alerts,
        effective_monthly_income=effective_monthly_income,
        goal_allocation_budget=goal_allocation_budget,
        selected_month=selected_month,
        selected_year=selected_year,
        safe_to_spend=safe_to_spend,
        savings_snapshot=savings_snapshot,
        dashboard_metric_changes=dashboard_metric_changes,
        wealth_snapshot=wealth_snapshot,
        net_worth=net_worth,
        total_assets=total_assets,
        total_liabilities=total_liabilities,
        monthly_income=monthly_income,
        monthly_expenses=monthly_expenses,
        category_labels=spending_chart["labels"],
        category_values=spending_chart["values"],
        spending_chart_month=spending_chart["month"],
        spending_chart_year=spending_chart["year"],
        spending_chart_month_label=spending_chart["month_label"],
        spending_chart_month_options=spending_chart["month_options"],
        upcoming_payment_account_options=accounts,
        upcoming_payment_category_options=transaction_ui_category_choices(user_id),
        expense_transaction_count=spending_chart["expense_count"],
        uncategorized_expense_count=spending_chart["uncategorized_count"],
        uncategorized_expense_total=spending_chart["uncategorized_total"],
        spending_chart_empty_message=spending_chart["empty_message"],
        spending_chart_notice=spending_chart["notice"],
        spending_chart_used_fallback=spending_chart["used_fallback"],
        monthly_overview_labels=monthly_overview_labels,
        monthly_overview_income=monthly_overview_income,
        monthly_overview_expenses=monthly_overview_expenses,
        budget_rows=budget_rows,
    )


@app.route("/api/dashboard/spending-category-detail")
def dashboard_spending_category_detail():
    if not require_login():
        return jsonify({"error": "Login required."}), 401

    user_id = get_user_id()
    category_name = (request.args.get("category") or "").strip()
    try:
        month = int(request.args.get("month") or 0)
        year = int(request.args.get("year") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "Choose a valid month and year."}), 400

    if not category_name:
        return jsonify({"error": "Choose a category first."}), 400
    if month < 1 or month > 12 or year < 1:
        return jsonify({"error": "Choose a valid month and year."}), 400

    with timed_route_section("dashboard_spending_category_detail", "query"):
        transactions = load_dashboard_transactions(user_id, newest_first=True)
        accounts = Account.query.filter_by(user_id=user_id).all()
    detail = build_spending_category_drilldown(transactions, accounts, category_name, month, year)
    return jsonify({"ok": True, "detail": detail})


@app.route("/api/dashboard/monthly-overview-detail")
def dashboard_monthly_overview_detail():
    if not require_login():
        return jsonify({"error": "Login required."}), 401

    user_id = get_user_id()
    month_label = (request.args.get("label") or "").strip()
    metric = (request.args.get("metric") or "expense").strip().lower()
    if metric not in {"income", "expense"}:
        return jsonify({"error": "Choose a valid metric."}), 400

    with timed_route_section("dashboard_monthly_overview_detail", "query"):
        transactions = load_dashboard_transactions(user_id, newest_first=False)
        accounts = Account.query.filter_by(user_id=user_id).all()
    details = monthly_overview_drilldowns(transactions, accounts)
    if not details:
        return jsonify({"ok": True, "detail": None})
    if not month_label or month_label not in details:
        month_label = list(details.keys())[-1]
    return jsonify({
        "ok": True,
        "detail": details.get(month_label),
        "metric": metric,
    })


@app.route("/api/dashboard/wealth-breakdown-detail")
def dashboard_wealth_breakdown_detail():
    if not require_login():
        return jsonify({"error": "Login required."}), 401

    user_id = get_user_id()
    label = (request.args.get("label") or "").strip()
    with timed_route_section("dashboard_wealth_breakdown_detail", "query"):
        accounts = Account.query.filter_by(user_id=user_id).all()
    details = wealth_breakdown_drilldown(accounts)
    if not details:
        return jsonify({"ok": True, "detail": None})
    if not label or label not in details:
        label = next(iter(details))
    return jsonify({"ok": True, "detail": details.get(label)})


@app.route("/api/dashboard/recurring-summary")
def dashboard_recurring_summary():
    if not require_login():
        return jsonify({"error": "Login required."}), 401

    user_id = get_user_id()
    selected_month, selected_year = month_year_from_request()
    with timed_route_section("dashboard_recurring_summary", "query"):
        transactions = load_dashboard_transactions(user_id, newest_first=False)[-800:]
        accounts = Account.query.filter_by(user_id=user_id).all()
        goals = FinancialGoal.query.filter_by(user_id=user_id).all()

    with timed_route_section("dashboard_recurring_summary", "analysis"):
        recurring_income_sources = analyze_recurring_income(transactions)
        recurring_income_estimate = recurring_income_monthly_estimate(recurring_income_sources)
        recurring_expenses = analyze_recurring_expenses(transactions)
        wealth_snapshot = build_dashboard_goal_snapshot_light(goals)
        account_allocation_summary = goals_account_allocation_summary(user_id, accounts, wealth_snapshot["goal_rows"])
        income_allocation_alerts = build_income_allocation_alerts(
            recurring_income_sources=recurring_income_sources,
            goal_rows=wealth_snapshot["goal_rows"],
            account_allocation_rows=account_allocation_summary,
            selected_month=selected_month,
            selected_year=selected_year,
        )

    account_name_map = {account.id: account.name for account in accounts}
    manual_upcoming_events = manual_upcoming_event_rows(user_id, account_name_map=account_name_map)
    upcoming_payments = build_dashboard_upcoming_payments(
        recurring_expenses,
        manual_upcoming_events,
        account_name_map=account_name_map,
    )

    return jsonify({
        "ok": True,
        "recurring_income_estimate": round(float(recurring_income_estimate or 0), 2),
        "recurring_income_sources": [
            {
                "source_name": item.get("source_name"),
                "status_label": item.get("status_label"),
                "frequency": item.get("frequency"),
                "last_received_date": serialize_optional_date(item.get("last_received_date")),
                "next_expected_date": serialize_optional_date(item.get("next_expected_date")),
                "average_amount": round(float(item.get("average_amount") or 0), 2),
                "monthly_equivalent": round(float(item.get("monthly_equivalent") or 0), 2),
                "is_confirmed": bool(item.get("is_confirmed")),
            }
            for item in recurring_income_sources
        ],
        "upcoming_payments": [
            {
                "name": item.get("name"),
                "expected_date": serialize_optional_date(item.get("expected_date")),
                "average_amount": round(float(item.get("average_amount") or 0), 2),
                "source_label": item.get("source_label"),
                "type_label": item.get("type_label"),
                "status_label": item.get("status_label"),
                "account_name": item.get("account_name"),
                "category": item.get("category"),
                "frequency": item.get("frequency"),
                "days_until_due": int(item.get("days_until_due") or 0),
                "is_due_soon": bool(item.get("is_due_soon")),
                "is_overdue": bool(item.get("is_overdue")),
                "is_recurring": bool(item.get("is_recurring")),
            }
            for item in upcoming_payments
        ],
        "income_allocation_alerts": [
            {
                "source_name": alert.get("source_name"),
                "status_label": alert.get("status_label"),
                "amount_received": round(float(alert.get("amount_received") or 0), 2),
                "account_name": alert.get("account_name"),
                "suggested_pool": round(float(alert.get("suggested_pool") or 0), 2),
                "last_received_date": serialize_optional_date(alert.get("last_received_date")),
                "suggestions": [
                    {
                        "goal_name": suggestion.get("goal_name"),
                        "goal_id": suggestion.get("goal_id"),
                        "suggested_amount": round(float(suggestion.get("suggested_amount") or 0), 2),
                    }
                    for suggestion in alert.get("suggestions", [])
                ],
            }
            for alert in income_allocation_alerts
        ],
    })

@app.route("/init_db")
def init_db():
    with app.app_context():
        initialize_schema_once()
        run_plaid_deduplication_maintenance()
    return "DB initialized"

@app.route("/simulator", methods=["GET", "POST"])
def simulator():
    # Compatibility alias for the older Purchase Simulator URL.
    return goals_view_redirect("#planning")


if __name__ == "__main__":
    app.run(
        host=os.getenv("FLASK_RUN_HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", os.getenv("FLASK_RUN_PORT", 5000))),
        debug=app.config["DEBUG"],
    )
