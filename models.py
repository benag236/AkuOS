"""SQLAlchemy model definitions.

Extracted from app.py during the architecture refactor pass. `db` comes from
the shared `extensions` module so models can be imported anywhere without
pulling in the rest of app.py.
"""
from datetime import datetime

from extensions import db


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    is_admin = db.Column(db.Boolean, nullable=False, default=False)
    is_pro = db.Column(db.Boolean, nullable=False, default=False)
    pro_since = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    last_login_at = db.Column(db.DateTime, nullable=True)
    reset_token = db.Column(db.String(120), nullable=True)
    reset_token_expires_at = db.Column(db.DateTime, nullable=True)


class UserPreference(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, unique=True, nullable=False, index=True)
    currency = db.Column(db.String(12), nullable=False, default="USD")
    date_format = db.Column(db.String(24), nullable=False, default="MMM D, YYYY")
    ui_density = db.Column(db.String(20), nullable=False, default="comfortable")
    auto_categorization_enabled = db.Column(db.Boolean, nullable=False, default=True)
    apply_memory_automatically = db.Column(db.Boolean, nullable=False, default=True)
    ai_insights_enabled = db.Column(db.Boolean, nullable=False, default=True)
    calendar_subscription_auto_sync = db.Column(db.Boolean, nullable=False, default=False)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


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
    original_amount = db.Column(db.Float, nullable=True)
    original_currency = db.Column(db.String(12), nullable=False, default="USD")
    exchange_rate = db.Column(db.Float, nullable=True)
    exchange_rate_provider = db.Column(db.String(80), nullable=False, default="")
    exchange_rate_fetched_at = db.Column(db.DateTime, nullable=True)
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


class ExchangeRateCache(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    base_currency = db.Column(db.String(12), nullable=False, index=True)
    target_currency = db.Column(db.String(12), nullable=False, index=True)
    rate = db.Column(db.Float, nullable=False)
    provider = db.Column(db.String(80), nullable=False, default="")
    fetched_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    raw_payload = db.Column(db.Text, nullable=False, default="")


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


class TransactionEmbedding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    transaction_id = db.Column(db.Integer, nullable=False, index=True)
    model_name = db.Column(db.String(120), nullable=False, default="all-MiniLM-L6-v2")
    text_hash = db.Column(db.String(64), nullable=False)
    category = db.Column(db.String(100), nullable=False)
    subcategory = db.Column(db.String(100), nullable=False, default="")
    embedding_json = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    __table_args__ = (
        db.UniqueConstraint("user_id", "transaction_id", "model_name", name="uq_transaction_embedding_user_tx_model"),
    )


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


class GoogleCalendarConnection(db.Model):
    __tablename__ = "google_calendar_connection"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, unique=True, nullable=False, index=True)
    access_token = db.Column(db.Text, nullable=False, default="")
    refresh_token = db.Column(db.Text, nullable=False, default="")
    token_type = db.Column(db.String(40), nullable=False, default="Bearer")
    scope = db.Column(db.String(255), nullable=False, default="")
    expires_at = db.Column(db.DateTime, nullable=True)
    calendar_id = db.Column(db.String(255), nullable=False, default="primary")
    connected_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class SubscriptionCalendarSync(db.Model):
    __tablename__ = "subscription_calendar_sync"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    pattern_id = db.Column(db.String(120), nullable=False, index=True)
    merchant_name = db.Column(db.String(255), nullable=False, default="")
    google_event_id = db.Column(db.String(255), nullable=False, default="")
    last_synced_at = db.Column(db.DateTime, nullable=True)
    sync_status = db.Column(db.String(40), nullable=False, default="pending")
    is_ignored = db.Column(db.Boolean, nullable=False, default=False)
    last_error = db.Column(db.String(255), nullable=False, default="")
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    __table_args__ = (
        db.UniqueConstraint("user_id", "pattern_id", name="uq_subscription_calendar_sync_user_pattern"),
    )
