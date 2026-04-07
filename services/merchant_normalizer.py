"""Reusable merchant cleanup helpers for categorization and review flows."""

from finance_engine import clean_transaction_description, normalize_text


def normalized_description(description):
    return normalize_text(description)


def merchant_guess(description):
    return clean_transaction_description(description or "").strip()


def merchant_key(description):
    guess = merchant_guess(description)
    return normalize_text(guess or description)
