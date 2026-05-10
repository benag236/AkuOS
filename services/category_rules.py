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
    "gas": "Car Related",
    "fees": "Needs Review",
    "fees & charges": "Needs Review",
    "cash withdrawal": "Cash",
    "transfer": "Transfers",
    "transfers": "Transfers",
    "transfer / payment": "Transfers",
    "internal transfer": "Transfers",
    "money transfer": "Transfers",
    "merchandise": "Shopping",
    "other services": "Needs Review",
    "other travel": "Travel",
    "unknown": "Needs Review",
    "misc": "Needs Review",
    "uncategorized": "Needs Review",
    "car- related": "Car Related",
    "car related": "Car Related",
}

LEGACY_SUBCATEGORY_ALIASES = {
    "restaurants": "Restaurants",
    "coffee": "Dining",
    "bars": "Dining",
    "fast food": "Takeout",
    "delivery": "Takeout",
    "coffee shops": "Dining",
    "rideshare": "Uber / Rideshare",
    "uber/taxi": "Uber / Rideshare",
    "charging": "Charging",
    "ev charging": "Charging",
    "supercharger": "Charging",
    "gas and charging": "Gas & Charging",
    "gas & charging": "Gas & Charging",
    "hotels": "Hotel",
    "movie": "Movies",
    "atm": "ATM Withdrawal",
    "atm withdrawal": "ATM Withdrawal",
    "withdrawal": "ATM Withdrawal",
    "cash withdrawal": "ATM Withdrawal",
    "cash deposit": "Cash Deposit",
    "money transfer": "Money Transfer",
    "internal transfer": "Internal Transfer",
    "brokerage transfer": "Money Transfer",
    "savings transfer": "Savings Transfer",
    "credit card payment": "Credit Card Payment",
    "family support": "Family Support",
    "partner support": "Girlfriend / Partner Support",
    "girlfriend support": "Girlfriend / Partner Support",
    "girlfriend / partner support": "Girlfriend / Partner Support",
    "friends payment": "Friends Payment",
    "friend payment": "Friends Payment",
    "rent shared bills transfer": "Rent / Shared Bills Transfer",
    "rent / shared bills transfer": "Rent / Shared Bills Transfer",
}

LEGACY_CATEGORY_PAIR_ALIASES = {
    ("dining", ""): ("Food", "Dining"),
    ("dining", "restaurants"): ("Food", "Restaurants"),
    ("dining", "coffee"): ("Food", "Dining"),
    ("dining", "bars"): ("Food", "Dining"),
    ("food", ""): ("Food", "Dining"),
    ("food", "restaurants"): ("Food", "Restaurants"),
    ("food", "coffee"): ("Food", "Dining"),
    ("food", "fast food"): ("Food", "Takeout"),
    ("food", "takeout"): ("Food", "Takeout"),
    ("transportation", "uber/taxi"): ("Transportation", "Uber / Rideshare"),
    ("transportation", "rideshare"): ("Transportation", "Uber / Rideshare"),
    ("transportation", "parking & tolls"): ("Transportation", "Parking & Tolls"),
    ("transportation", "public transit"): ("Transportation", ""),
    ("transportation", "gas and charging"): ("Transportation", "Gas & Charging"),
    ("transportation", "gas & charging"): ("Transportation", "Gas & Charging"),
    ("transportation", "car maintenance"): ("Car Related", "Maintenance"),
    ("gas", ""): ("Car Related", "Gas"),
    ("charging", ""): ("Car Related", "Charging"),
    ("ev charging", ""): ("Car Related", "Charging"),
    ("supercharger", ""): ("Car Related", "Charging"),
    ("shopping", "general shopping"): ("Shopping", ""),
    ("shopping", "electronics"): ("Shopping", ""),
    ("shopping", "clothing"): ("Shopping", ""),
    ("utilities", "phone"): ("Subscriptions / Bills", "Phone"),
    ("utilities", "internet"): ("Subscriptions / Bills", "Internet"),
    ("utilities", "electric"): ("Utilities", "Electric"),
    ("utilities", "water"): ("Utilities", "Water"),
    ("utilities", "gas"): ("Utilities", "Gas (home)"),
    ("utilities", "gas (home)"): ("Utilities", "Gas (home)"),
    ("utilities", "gas (home )"): ("Utilities", "Gas (home)"),
    ("health", "doctor"): ("Health", ""),
    ("health", "pharmacy"): ("Health", ""),
    ("health", "insurance"): ("Health", ""),
    ("subscriptions", ""): ("Subscriptions / Bills", "Other Bills"),
    ("subscriptions", "streaming"): ("Subscriptions / Bills", "Streaming"),
    ("subscriptions", "memberships"): ("Subscriptions / Bills", "Memberships"),
    ("credit card payment", ""): ("Subscriptions / Bills", "Credit Card Payment"),
    ("transfer", ""): ("Transfers", "Money Transfer"),
    ("transfers", ""): ("Transfers", "Money Transfer"),
    ("transfer", "money transfer"): ("Transfers", "Money Transfer"),
    ("transfer / payment", ""): ("Transfers", "Money Transfer"),
    ("internal transfer", ""): ("Transfers", "Internal Transfer"),
    ("transfers", "internal transfer"): ("Transfers", "Internal Transfer"),
    ("savings transfer", ""): ("Transfers", "Savings Transfer"),
    ("transfers", "credit card payment"): ("Transfers", "Credit Card Payment"),
    ("loan payments", "credit card payment"): ("Transfers", "Credit Card Payment"),
    ("brokerage transfer", ""): ("Transfers", "Money Transfer"),
    ("transfers", "brokerage transfer"): ("Transfers", "Money Transfer"),
    ("transfers", "family support"): ("Transfers", "Family Support"),
    ("transfers", "girlfriend support"): ("Transfers", "Girlfriend / Partner Support"),
    ("transfers", "partner support"): ("Transfers", "Girlfriend / Partner Support"),
    ("transfers", "friends payment"): ("Transfers", "Friends Payment"),
    ("transfers", "rent shared bills transfer"): ("Transfers", "Rent / Shared Bills Transfer"),
    ("cash withdrawal", ""): ("Cash", "ATM Withdrawal"),
    ("cash", "atm withdrawal"): ("Cash", "ATM Withdrawal"),
    ("cash", "cash deposit"): ("Cash", "Cash Deposit"),
    ("fees", ""): ("Needs Review", ""),
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
    ("other services", ""): ("Needs Review", ""),
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
    mapped = LEGACY_CATEGORY_ALIASES.get(cleaned.lower(), cleaned)
    taxonomy_names, _children_by_parent = taxonomy_index()
    if mapped in taxonomy_names or mapped == "Needs Review":
        return mapped
    # Other is manual-only: unclear automatic/category-import values stay in review.
    return "Needs Review"


def canonical_subcategory_name(name):
    cleaned = (name or "").strip()
    if not cleaned:
        return ""
    mapped = LEGACY_SUBCATEGORY_ALIASES.get(cleaned.lower(), cleaned)
    valid_names = {
        child["name"]
        for node in DEFAULT_CATEGORY_TAXONOMY
        for child in node.get("children", [])
    }
    return mapped if mapped in valid_names else ""


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
