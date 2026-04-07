"""Category taxonomy helpers for rule-based transaction categorization."""

import re

from seed.default_category_rules import DEFAULT_CATEGORY_TAXONOMY


TOP_LEVEL_CATEGORY_ORDER = [node["name"] for node in DEFAULT_CATEGORY_TAXONOMY]

LEGACY_CATEGORY_ALIASES = {
    "food & drink": "Dining",
    "eating out": "Dining",
    "food": "Dining",
    "transport": "Transportation",
    "transfer / payment": "Transfer",
    "internal transfer": "Transfer",
    "subscription": "Subscriptions",
    "fees & charges": "Fees",
    "uncategorized": "Needs Review",
}

LEGACY_SUBCATEGORY_ALIASES = {
    "coffee shops": "Coffee",
    "rideshare": "Uber/Taxi",
}


def slugify(value):
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


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


def taxonomy_index():
    by_name = {}
    children_by_parent = {}
    for node in DEFAULT_CATEGORY_TAXONOMY:
        by_name[node["name"]] = node
        children_by_parent[node["name"]] = [child["name"] for child in node.get("children", [])]
    return by_name, children_by_parent


def valid_subcategory(parent_name, subcategory_name):
    parent_name = canonical_category_name(parent_name)
    subcategory_name = canonical_subcategory_name(subcategory_name)
    if not subcategory_name:
        return ""
    _, children_by_parent = taxonomy_index()
    return subcategory_name if subcategory_name in children_by_parent.get(parent_name, []) else ""


def canonical_category_pair(category_name, subcategory_name=""):
    top_level = canonical_category_name(category_name)
    return top_level, valid_subcategory(top_level, subcategory_name)


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
