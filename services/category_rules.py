"""Category taxonomy helpers for rule-based transaction categorization."""

import re

from seed.default_category_rules import DEFAULT_CATEGORY_TAXONOMY


TOP_LEVEL_CATEGORY_ORDER = [node["name"] for node in DEFAULT_CATEGORY_TAXONOMY]

LEGACY_CATEGORY_ALIASES = {
    "food & drink": "Food",
    "food": "Food",
    "eating out": "Food",
    "dining": "Food",
    "restaurants": "Food",
    "coffee": "Food",
    "fast food": "Food",
    "takeout": "Food",
    "transport": "Transportation",
    "subscription": "Subscriptions / Bills",
    "subscriptions": "Subscriptions / Bills",
    "bills/payments": "Subscriptions / Bills",
    "bills": "Subscriptions / Bills",
    "phone/cable": "Subscriptions / Bills",
    "credit card payment": "Subscriptions / Bills",
    "gas": "Car- Related",
    "fees": "Other",
    "fees & charges": "Other",
    "cash withdrawal": "Other",
    "transfer": "Other",
    "transfer / payment": "Other",
    "internal transfer": "Other",
    "merchandise": "Shopping",
    "other services": "Other",
    "other travel": "Travel",
    "unknown": "Other",
    "misc": "Other",
    "uncategorized": "Needs Review",
}

LEGACY_SUBCATEGORY_ALIASES = {
    "restaurants": "Dining",
    "coffee": "Dining",
    "bars": "Dining",
    "fast food": "Takeout",
    "delivery": "Takeout",
    "coffee shops": "Dining",
    "rideshare": "Uber / Rideshare",
    "uber/taxi": "Uber / Rideshare",
    "hotels": "Hotel",
    "movie": "Movies",
}

LEGACY_CATEGORY_PAIR_ALIASES = {
    ("dining", ""): ("Food", "Dining"),
    ("dining", "restaurants"): ("Food", "Dining"),
    ("dining", "coffee"): ("Food", "Dining"),
    ("dining", "bars"): ("Food", "Dining"),
    ("food", ""): ("Food", "Dining"),
    ("food", "restaurants"): ("Food", "Dining"),
    ("food", "coffee"): ("Food", "Dining"),
    ("food", "fast food"): ("Food", "Takeout"),
    ("food", "takeout"): ("Food", "Takeout"),
    ("transportation", "uber/taxi"): ("Transportation", "Uber / Rideshare"),
    ("transportation", "rideshare"): ("Transportation", "Uber / Rideshare"),
    ("transportation", "parking & tolls"): ("Transportation", "Parking & Tolls"),
    ("transportation", "public transit"): ("Transportation", ""),
    ("transportation", "car maintenance"): ("Car- Related", "Maintenance"),
    ("gas", ""): ("Car- Related", "Gas"),
    ("shopping", "general shopping"): ("Shopping", ""),
    ("shopping", "electronics"): ("Shopping", ""),
    ("shopping", "clothing"): ("Shopping", ""),
    ("utilities", "phone"): ("Subscriptions / Bills", "Phone"),
    ("utilities", "internet"): ("Subscriptions / Bills", "Internet"),
    ("utilities", "electric"): ("Utilities", "Electric"),
    ("utilities", "water"): ("Utilities", "Water"),
    ("utilities", "gas"): ("Utilities", "Gas (Home)"),
    ("health", "doctor"): ("Health", ""),
    ("health", "pharmacy"): ("Health", ""),
    ("health", "insurance"): ("Health", ""),
    ("subscriptions", ""): ("Subscriptions / Bills", "Other Bills"),
    ("subscriptions", "streaming"): ("Subscriptions / Bills", "Streaming"),
    ("subscriptions", "memberships"): ("Subscriptions / Bills", "Memberships"),
    ("credit card payment", ""): ("Subscriptions / Bills", "Credit Card Payment"),
    ("transfer", ""): ("Other", ""),
    ("cash withdrawal", ""): ("Other", ""),
    ("fees", ""): ("Other", ""),
    ("travel", "flights"): ("Travel", "Flights"),
    ("travel", "hotels"): ("Travel", "Hotel"),
    ("travel", "hotel"): ("Travel", "Hotel"),
    ("travel", "vacation"): ("Travel", "Hotel"),
    ("travel", "travel fees"): ("Travel", "Travel Fees"),
    ("entertainment", "events"): ("Entertainment", "Events"),
    ("entertainment", "movies"): ("Entertainment", "Movies"),
    ("entertainment", "games"): ("Entertainment", "Games"),
    ("income", "bonus"): ("Income", "Salary"),
    ("income", "salary"): ("Income", "Salary"),
    ("income", "refund"): ("Income", "Refund"),
    ("income", "investment income"): ("Income", "Investment Income"),
    ("other travel", ""): ("Travel", ""),
    ("other services", ""): ("Other", ""),
    ("merchandise", ""): ("Shopping", ""),
}


def slugify(value):
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def taxonomy_index():
    by_name = {}
    children_by_parent = {}
    for node in DEFAULT_CATEGORY_TAXONOMY:
        by_name[node["name"]] = node
        children_by_parent[node["name"]] = [child["name"] for child in node.get("children", [])]
    return by_name, children_by_parent


def canonical_category_name(name):
    cleaned = (name or "").strip()
    if not cleaned:
        return "Needs Review"
    return LEGACY_CATEGORY_ALIASES.get(cleaned.lower(), cleaned)


def canonical_subcategory_name(name):
    cleaned = (name or "").strip()
    if not cleaned:
        return ""
    return LEGACY_SUBCATEGORY_ALIASES.get(cleaned.lower(), cleaned)


def valid_subcategory(parent_name, subcategory_name):
    parent_name = canonical_category_name(parent_name)
    subcategory_name = canonical_subcategory_name(subcategory_name)
    if not subcategory_name:
        return ""
    _, children_by_parent = taxonomy_index()
    return subcategory_name if subcategory_name in children_by_parent.get(parent_name, []) else ""


def canonical_category_pair(category_name, subcategory_name=""):
    raw_category = (category_name or "").strip()
    raw_subcategory = (subcategory_name or "").strip()
    legacy_key = (raw_category.lower(), raw_subcategory.lower())
    if legacy_key in LEGACY_CATEGORY_PAIR_ALIASES:
        mapped_category, mapped_subcategory = LEGACY_CATEGORY_PAIR_ALIASES[legacy_key]
        top_level = canonical_category_name(mapped_category)
        return top_level, valid_subcategory(top_level, mapped_subcategory)

    top_level = canonical_category_name(raw_category)
    normalized_subcategory = canonical_subcategory_name(raw_subcategory)
    return top_level, valid_subcategory(top_level, normalized_subcategory)


def category_label(category_name, subcategory_name=""):
    category_name, subcategory_name = canonical_category_pair(category_name, subcategory_name)
    return f"{category_name} / {subcategory_name}" if subcategory_name else category_name


def flattened_category_labels():
    labels = []
    for top_level in DEFAULT_CATEGORY_TAXONOMY:
        labels.append(top_level["name"])
        for child in top_level.get("children", []):
            labels.append(category_label(top_level["name"], child["name"]))
    return labels
