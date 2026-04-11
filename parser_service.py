from __future__ import annotations

import csv
import logging
import re
from collections import Counter
from io import BytesIO, StringIO

from import_confidence import score_rule_parse
from transaction_normalizer import (
    AMOUNT_PATTERN,
    NON_TRANSACTION_PHRASES,
    build_transaction_fingerprint,
    choose_amount_match,
    cleaned_display_name,
    description_between_dates_and_amount,
    extract_date_matches,
    infer_sign_from_text,
    is_obviously_non_transaction_text,
    normalize_row_payload,
    normalize_whitespace,
    parse_date_any,
    parse_statement_amount,
    parse_statement_date_with_fallback,
    safe_float,
)

try:
    import pdfplumber
except ImportError:
    pdfplumber = None


logger = logging.getLogger(__name__)


POSITIVE_HINTS = (
    "deposit", "refund", "interest", "credit", "payment received",
    "payroll", "salary", "direct dep", "reversal", "cashback",
)
NEGATIVE_HINTS = (
    "purchase", "withdrawal", "debit", "pos", "check", "fee",
    "autopay", "card", "payment thank you", "zelle", "venmo",
    "online transfer", "transfer", "rent",
)
FOREIGN_CURRENCY_PATTERNS = [
    re.compile(r"\bforeign currency\b", re.I),
    re.compile(r"\bexchange rate\b", re.I),
    re.compile(r"\bcurrency conversion\b", re.I),
    re.compile(r"\bmerchant amount\b", re.I),
    re.compile(r"\bconverted from\b", re.I),
]
DATE_AT_START_PATTERN = re.compile(r"^\s*\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?(?:\s+\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)?")
STOP_LINE_PATTERNS = [
    re.compile(r"^\s*totals?.*$", re.I),
    re.compile(r"^\s*fees?\s*(?:charged|summary)?.*$", re.I),
    re.compile(r"^\s*interest\s*(?:charged|summary|details?)?.*$", re.I),
    re.compile(r"^\s*(?:year to date|years previous adjusted).*$", re.I),
    re.compile(r"^\s*(?:payment information|customer service|legal|billing rights).*$", re.I),
    re.compile(r"^\s*(?:previous balance|new balance|minimum payment due|payment due).*$", re.I),
    re.compile(r"^\s*(?:rewards?|earnings?)\s+.*$", re.I),
]
PAGE_NOISE_PATTERNS = [
    re.compile(r"^\s*page\s+\d+(?:\s+of\s+\d+)?\s*$", re.I),
    re.compile(r"^\s*\d+\s+of\s+\d+\s*$", re.I),
    re.compile(r"^\s*(?:member fdic|equal housing lender)\s*$", re.I),
    re.compile(r"^\s*(?:please see reverse|continued on next page).*$", re.I),
    re.compile(r"^\s*(?:www\.[^\s]+|[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})\s*$", re.I),
]
COLUMN_HEADER_PATTERNS = [
    re.compile(r"^\s*(?:trans(?:action)? date|post date|date)\s+(?:post date\s+)?(?:description|merchant|details).*(?:amount|debit|credit)\s*$", re.I),
    re.compile(r"^\s*(?:date|posted|posting date)\s+(?:description|details).*(?:amount|debit|credit|balance)\s*$", re.I),
]
CSV_COLUMN_CANDIDATES = {
    "date": ("date", "transaction date", "posted date", "posting date", "trans date", "posted", "transactiondate"),
    "description": ("description", "merchant", "details", "memo", "name", "transaction details", "transaction description"),
    "amount": ("amount", "transaction amount", "amt"),
    "credit": ("credit", "credits", "deposit", "deposits", "money in"),
    "debit": ("debit", "debits", "withdrawal", "withdrawals", "money out", "charge"),
    "category": ("category", "type"),
    "account": ("account", "account name"),
    "tags": ("tags", "labels"),
    "notes": ("notes", "note"),
}
STATEMENT_PERIOD_PATTERNS = [
    re.compile(
        r"\bstatement\s+period[:\s]+(?P<start>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:to|-|through)\s*(?P<end>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        re.I,
    ),
    re.compile(
        r"\bfrom\s+(?P<start>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(?:to|through|-)\s+(?P<end>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        re.I,
    ),
]


def detect_statement_file_type(filename):
    lowered = (filename or "").lower()
    if lowered.endswith(".pdf"):
        return "pdf"
    if lowered.endswith(".txt"):
        return "text"
    return "csv"


def detect_csv_columns(headers):
    header_map = {}
    cleaned_headers = [normalize_whitespace(header).lower() for header in headers]
    for logical_name, candidates in CSV_COLUMN_CANDIDATES.items():
        for idx, header in enumerate(cleaned_headers):
            normalized_header = header.replace("_", " ")
            if header in candidates or normalized_header in candidates:
                header_map[logical_name] = idx
                break
    return header_map


def init_diagnostics(source_document, parser_name):
    return {
        "source_document": source_document,
        "parser_name": parser_name,
        "text_extracted": False,
        "readable_pages": 0,
        "sections_found": [],
        "candidate_rows_found": 0,
        "rows_parsed": 0,
        "rows_rejected": 0,
        "rows_filtered_out": 0,
        "ignored_followups": 0,
        "warnings": [],
        "matched_columns": {},
        "malformed_rows": 0,
        "rejection_reasons": {},
        "sample_rejections": [],
        "raw_text_preview": "",
        "cleaned_lines_preview": [],
        "candidate_row_preview": [],
        "ignored_rows_preview": [],
        "parsed_row_preview": [],
        "line_sources": [],
        "statement_period_start": "",
        "statement_period_end": "",
        "parser_used": "rule_based",
        "confidence_score": 0.0,
        "confidence_label": "low",
        "ai_ready": False,
    }


def append_preview_item(diagnostics, key, value, limit=12):
    if not diagnostics or not value:
        return
    items = diagnostics.setdefault(key, [])
    if len(items) < limit:
        items.append(value)


def detect_statement_period(raw_text):
    cleaned_text = normalize_whitespace(raw_text)
    if not cleaned_text:
        return "", ""
    for pattern in STATEMENT_PERIOD_PATTERNS:
        match = pattern.search(cleaned_text)
        if not match:
            continue
        start = parse_statement_date_with_fallback(match.group("start"))
        end = parse_statement_date_with_fallback(match.group("end"))
        if start and end:
            return start.isoformat(), end.isoformat()
    return "", ""


def add_rejection(diagnostics, reason, raw_line, section_name=None, page_index=None):
    diagnostics["rows_rejected"] = int(diagnostics.get("rows_rejected", 0)) + 1
    rejection_reasons = diagnostics.setdefault("rejection_reasons", {})
    rejection_reasons[reason] = int(rejection_reasons.get(reason, 0)) + 1
    samples = diagnostics.setdefault("sample_rejections", [])
    if len(samples) < 12:
        sample = {
            "reason": reason,
            "line": normalize_whitespace(raw_line)[:220],
        }
        if section_name:
            sample["section"] = section_name
        if page_index:
            sample["page"] = page_index
        samples.append(sample)


def add_ignored_line(diagnostics, reason, raw_line, section_name=None, page_index=None):
    append_preview_item(
        diagnostics,
        "ignored_rows_preview",
        {
            "reason": reason,
            "line": normalize_whitespace(raw_line)[:220],
            "section": section_name or "",
            "page": page_index or "",
        },
        limit=16,
    )


def is_foreign_currency_followup(line):
    cleaned = normalize_whitespace(line)
    if not cleaned:
        return False
    if any(pattern.search(cleaned) for pattern in FOREIGN_CURRENCY_PATTERNS):
        return True
    amount_tokens = AMOUNT_PATTERN.findall(cleaned)
    return len(amount_tokens) >= 2 and bool(re.search(r"\b(?:rate|currency|converted)\b", cleaned, re.I))


def has_amount_token(line):
    return bool(AMOUNT_PATTERN.search(normalize_whitespace(line)))


def is_amount_only_line(line):
    cleaned = normalize_whitespace(line)
    if not cleaned or not has_amount_token(cleaned):
        return False
    stripped = (
        cleaned
        .replace("$", "")
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .replace("CR", "")
        .replace("DR", "")
        .strip()
    )
    return not bool(re.search(r"[A-Za-z]", stripped))


def looks_like_page_noise(line):
    cleaned = normalize_whitespace(line)
    if not cleaned:
        return True
    return any(pattern.match(cleaned) for pattern in PAGE_NOISE_PATTERNS)


def looks_like_column_header(line):
    cleaned = normalize_whitespace(line)
    if not cleaned:
        return False
    return any(pattern.match(cleaned) for pattern in COLUMN_HEADER_PATTERNS)


def clean_pdf_line(line):
    cleaned = str(line or "").replace("\u00a0", " ").replace("\t", " ")
    cleaned = re.sub(r"[ ]{2,}", " ", cleaned)
    cleaned = re.sub(r"\s+\|\s+", " | ", cleaned)
    return normalize_whitespace(cleaned)


def cleaned_text_lines(raw_text):
    return [clean_pdf_line(line) for line in str(raw_text or "").splitlines() if clean_pdf_line(line)]


def build_lines_from_words(words, tolerance=3):
    if not words:
        return []
    sorted_words = sorted(
        words,
        key=lambda word: (
            round(float(word.get("top", 0.0)) / tolerance),
            float(word.get("x0", 0.0)),
        ),
    )
    line_groups = []
    current_key = None
    current_words = []
    for word in sorted_words:
        line_key = round(float(word.get("top", 0.0)) / tolerance)
        if current_key is None or line_key == current_key:
            current_key = line_key
            current_words.append(word)
            continue
        line_groups.append(current_words)
        current_key = line_key
        current_words = [word]
    if current_words:
        line_groups.append(current_words)

    lines = []
    for group in line_groups:
        ordered = sorted(group, key=lambda word: float(word.get("x0", 0.0)))
        line = clean_pdf_line(" ".join(str(word.get("text") or "") for word in ordered))
        if line:
            lines.append(line)
    return lines


def score_line_source(lines):
    score = 0
    for line in lines or []:
        if DATE_AT_START_PATTERN.match(line):
            score += 4
            if has_amount_token(line):
                score += 4
        elif has_amount_token(line) and re.search(r"[A-Za-z]", line):
            score += 1
        if looks_like_column_header(line):
            score += 2
        if is_obviously_non_transaction_text(line):
            score -= 2
        if looks_like_page_noise(line):
            score -= 1
    return score


def choose_best_page_lines(raw_lines, layout_lines, word_lines):
    candidates = [
        ("text", raw_lines or []),
        ("layout", layout_lines or []),
        ("words", word_lines or []),
    ]
    best_name, best_lines = max(candidates, key=lambda item: (score_line_source(item[1]), len(item[1])))
    return best_name, list(best_lines)


def strip_repeated_pdf_noise(page_payloads):
    line_counts = Counter()
    for page in page_payloads:
        seen_on_page = set()
        for line in page.get("lines") or []:
            cleaned = clean_pdf_line(line)
            if cleaned and looks_like_page_noise(cleaned) and cleaned not in seen_on_page:
                line_counts[cleaned] += 1
                seen_on_page.add(cleaned)

    for page in page_payloads:
        cleaned_lines = []
        ignored_lines = list(page.get("ignored_lines") or [])
        for line in page.get("lines") or []:
            cleaned = clean_pdf_line(line)
            if line_counts.get(cleaned, 0) >= 2 and looks_like_page_noise(cleaned):
                ignored_lines.append({"reason": "repeated_page_noise", "line": cleaned})
                continue
            cleaned_lines.append(cleaned)
        page["lines"] = cleaned_lines
        page["ignored_lines"] = ignored_lines
    return page_payloads


def classify_transaction_type(raw_text, section_name=None):
    text = normalize_whitespace(raw_text)
    if section_name == "payments_credits_adjustments":
        return "Bills/Payments", "Transfer"
    if re.search(r"\bach\s+deposit\b|\bdirect\s+deposit\b", text, re.I):
        return "Income", "Income"
    if re.search(r"\batm\b", text, re.I):
        return "Cash Withdrawal", "Cash Withdrawal"
    if re.search(r"\belectronic\s+pmt\b|\bpayment thank you\b|\bautopay payment\b|\bcredit card payment\b|\bcapital one(?:\s+online)? payment\b|\bpayment received\b", text, re.I):
        return "Bills/Payments", "Transfer"
    if re.search(r"\bdbcrd\b|\bpurchase\b|\bpur\b", text, re.I):
        return "Expense", ""
    return "", ""


class BasePdfParser:
    name = "generic"
    bank_hint = "generic"
    active_sections = []
    blocked_headers = []
    extra_stop_phrases = ()

    def matches(self, full_text):
        return False

    def active_section_for_line(self, line):
        cleaned = normalize_whitespace(line)
        if not cleaned:
            return None
        for pattern, section_name in self.active_sections:
            if pattern.match(cleaned):
                return section_name
        for pattern in self.blocked_headers:
            if pattern.match(cleaned):
                return "__blocked__"
        return None

    def is_stop_line(self, line):
        cleaned = normalize_whitespace(line)
        if not cleaned:
            return False
        if looks_like_page_noise(cleaned) or looks_like_column_header(cleaned):
            return True
        if any(pattern.match(cleaned) for pattern in STOP_LINE_PATTERNS):
            return True
        return any(phrase in cleaned.lower() for phrase in self.extra_stop_phrases)

    def is_starter_line(self, line):
        cleaned = normalize_whitespace(line)
        if not cleaned or is_foreign_currency_followup(cleaned):
            return False
        if looks_like_page_noise(cleaned) or looks_like_column_header(cleaned):
            return False
        if not DATE_AT_START_PATTERN.match(cleaned):
            return False
        if is_obviously_non_transaction_text(cleaned, extra_stop_phrases=self.extra_stop_phrases):
            return False
        date_matches = extract_date_matches(cleaned)
        if not date_matches:
            return False
        amount_match = choose_amount_match(cleaned, date_matches)
        if amount_match:
            description = description_between_dates_and_amount(cleaned, date_matches, amount_match)
            return bool(description) and not is_obviously_non_transaction_text(description, extra_stop_phrases=self.extra_stop_phrases)
        return len(cleaned.split()) <= 14 and bool(re.search(r"[A-Za-z]", cleaned))

    def is_continuation_line(self, line):
        cleaned = normalize_whitespace(line)
        if not cleaned or is_foreign_currency_followup(cleaned):
            return False
        if looks_like_page_noise(cleaned) or looks_like_column_header(cleaned):
            return False
        if is_obviously_non_transaction_text(cleaned, extra_stop_phrases=self.extra_stop_phrases):
            return False
        if DATE_AT_START_PATTERN.match(cleaned):
            return False
        if len(cleaned) > 120:
            return False
        word_count = len(cleaned.split())
        if word_count > 14 and not is_amount_only_line(cleaned):
            return False
        if re.search(r"[.!?;:]", cleaned) and word_count > 8:
            return False
        return bool(re.search(r"[A-Za-z]", cleaned) or has_amount_token(cleaned))

    def parse_candidate_block(self, block_lines, source_document, row_index, section_name, parser_source):
        normalized_lines = [normalize_whitespace(line) for line in (block_lines or []) if normalize_whitespace(line)]
        if not normalized_lines:
            return None, "empty_block"
        starter = normalized_lines[0]
        if not DATE_AT_START_PATTERN.match(starter):
            return None, "no_transaction_table_row_detected"
        if is_obviously_non_transaction_text(starter, extra_stop_phrases=self.extra_stop_phrases):
            return None, "summary_help_block_rejected"
        if len(normalized_lines) > 4:
            return None, "invalid_description_block"

        non_fx_lines = [line for line in normalized_lines if not is_foreign_currency_followup(line)]
        if not non_fx_lines:
            return None, "foreign_currency_only"
        combined = " ".join(non_fx_lines).strip()
        if is_obviously_non_transaction_text(combined, extra_stop_phrases=self.extra_stop_phrases):
            return None, "summary_help_block_rejected"

        date_matches = extract_date_matches(combined)
        if not date_matches:
            return None, "missing_date"
        if len(date_matches) > 2:
            return None, "too_many_dates"

        amount_match = choose_amount_match(combined, date_matches)
        if not amount_match:
            return None, "missing_valid_amount"

        parsed_date = date_matches[0]["parsed"]
        post_date = date_matches[1]["parsed"] if len(date_matches) > 1 else None
        amount = parse_statement_amount(
            amount_match.group(0),
            force_sign=infer_sign_from_text(combined, amount_match.group(0), POSITIVE_HINTS, NEGATIVE_HINTS),
        )
        if amount is None or abs(amount) > 1_000_000:
            return None, "missing_valid_amount"

        description = description_between_dates_and_amount(combined, date_matches, amount_match)
        if not description:
            return None, "missing_description"
        if is_obviously_non_transaction_text(description, extra_stop_phrases=self.extra_stop_phrases):
            return None, "invalid_description_block"

        transaction_type, source_category = classify_transaction_type(description or combined, section_name=section_name)
        cleaned_name = cleaned_display_name(description, transaction_type=transaction_type, fallback=combined)
        if not cleaned_name or is_obviously_non_transaction_text(cleaned_name, extra_stop_phrases=self.extra_stop_phrases):
            return None, "invalid_description_block"

        return normalize_row_payload(
            source_document=source_document,
            raw_source=combined,
            parsed_date=parsed_date,
            post_date=post_date,
            raw_description=description or combined,
            amount=amount,
            source_category=source_category,
            raw_category=source_category,
            transaction_type=transaction_type,
            parser_label=f"{self.name} rule parser · {section_name.replace('_', ' ')}",
            parser_source=parser_source,
            parser_confidence=0.76,
            display_name=cleaned_name,
        ), ""

    def parse_table_row(self, cells, source_document, row_index, section_name, parser_source):
        normalized_cells = [normalize_whitespace(cell) for cell in cells if normalize_whitespace(cell)]
        if len(normalized_cells) < 2:
            return None, "table_row_too_short"
        raw_row = " | ".join(normalized_cells)
        if is_obviously_non_transaction_text(raw_row, extra_stop_phrases=self.extra_stop_phrases):
            return None, "summary_help_block_rejected"

        date_indexes = []
        parsed_date = None
        for idx, cell in enumerate(normalized_cells):
            parsed = parse_statement_date_with_fallback(cell)
            if parsed:
                date_indexes.append(idx)
                if parsed_date is None:
                    parsed_date = parsed
        if not date_indexes or date_indexes[0] > 1:
            return None, "no_transaction_table_row_detected"

        amount_idx = None
        amount = None
        for idx in range(len(normalized_cells) - 1, -1, -1):
            token_match = AMOUNT_PATTERN.search(normalized_cells[idx])
            if token_match:
                amount_idx = idx
                amount = parse_statement_amount(
                    token_match.group(0),
                    force_sign=infer_sign_from_text(raw_row, token_match.group(0), POSITIVE_HINTS, NEGATIVE_HINTS),
                )
                break
        if amount is None:
            return None, "missing_valid_amount"

        description_parts = [cell for idx, cell in enumerate(normalized_cells) if idx not in date_indexes and idx != amount_idx]
        description = " ".join(description_parts).strip()
        if not description or is_obviously_non_transaction_text(description, extra_stop_phrases=self.extra_stop_phrases):
            return None, "invalid_description_block"

        transaction_type, source_category = classify_transaction_type(description or raw_row, section_name=section_name)
        cleaned_name = cleaned_display_name(description, transaction_type=transaction_type, fallback=raw_row)
        if not cleaned_name:
            return None, "invalid_description_block"

        return normalize_row_payload(
            source_document=source_document,
            raw_source=raw_row,
            parsed_date=parsed_date,
            post_date=extract_date_matches(raw_row)[1]["parsed"] if len(extract_date_matches(raw_row)) > 1 else None,
            raw_description=description or raw_row,
            amount=amount,
            source_category=source_category,
            raw_category=source_category,
            transaction_type=transaction_type,
            parser_label=f"{self.name} table parser · {section_name.replace('_', ' ')}",
            parser_source=parser_source,
            parser_confidence=0.8,
            display_name=cleaned_name,
        ), ""

    def parse_pages(self, pages, source_document, parser_source="rule_based"):
        diagnostics = init_diagnostics(source_document, self.name)
        rows = []
        seen_fingerprints = set()
        sections_found = set()

        for page_index, page in enumerate(pages, start=1):
            page_text = page.get("text") or ""
            if page_text.strip():
                diagnostics["text_extracted"] = True
                diagnostics["readable_pages"] = int(diagnostics["readable_pages"]) + 1
                if not diagnostics["raw_text_preview"]:
                    diagnostics["raw_text_preview"] = page_text[:4000]
            line_source = (page.get("line_source") or "").strip()
            if line_source and line_source not in diagnostics["line_sources"]:
                diagnostics["line_sources"].append(line_source)
            for ignored in page.get("ignored_lines") or []:
                add_ignored_line(
                    diagnostics,
                    ignored.get("reason") or "ignored_page_noise",
                    ignored.get("line") or "",
                    page_index=page_index,
                )
            for cleaned_line in page.get("lines") or []:
                append_preview_item(diagnostics, "cleaned_lines_preview", cleaned_line, limit=30)

            current_section = None
            page_active_sections = []
            block = None
            block_index = 0

            def flush_block():
                nonlocal block, block_index
                if not block or current_section not in {"transactions", "payments_credits_adjustments"}:
                    block = None
                    return
                diagnostics["candidate_rows_found"] = int(diagnostics["candidate_rows_found"]) + 1
                append_preview_item(
                    diagnostics,
                    "candidate_row_preview",
                    {
                        "page": page_index,
                        "section": current_section,
                        "line": normalize_whitespace(" ".join(block))[:220],
                    },
                    limit=16,
                )
                record, reason = self.parse_candidate_block(block, source_document, f"{page_index}_{block_index}", current_section, parser_source)
                if not record:
                    add_rejection(diagnostics, reason or "rejected", " ".join(block), current_section, page_index)
                else:
                    fingerprint = record.get("fingerprint") or build_transaction_fingerprint(record["date"], record["raw_description"], record["amount"])
                    if fingerprint in seen_fingerprints:
                        diagnostics["rows_filtered_out"] = int(diagnostics["rows_filtered_out"]) + 1
                    else:
                        seen_fingerprints.add(fingerprint)
                        diagnostics["rows_parsed"] = int(diagnostics["rows_parsed"]) + 1
                        rows.append(record)
                        append_preview_item(
                            diagnostics,
                            "parsed_row_preview",
                            {
                                "page": page_index,
                                "section": current_section,
                                "date": record.get("date") or "",
                                "description": record.get("description") or "",
                                "amount": record.get("amount"),
                            },
                            limit=16,
                        )
                block = None

            page_lines = list(page.get("lines") or cleaned_text_lines(page_text))
            for line_index, cleaned_line in enumerate(page_lines, start=1):
                section_marker = self.active_section_for_line(cleaned_line)
                if section_marker == "__blocked__":
                    flush_block()
                    current_section = None
                    add_ignored_line(diagnostics, "blocked_section_header", cleaned_line, page_index=page_index)
                    continue
                if section_marker:
                    flush_block()
                    current_section = section_marker
                    sections_found.add(section_marker)
                    if section_marker not in page_active_sections:
                        page_active_sections.append(section_marker)
                    continue
                if current_section in {"transactions", "payments_credits_adjustments"}:
                    if self.is_stop_line(cleaned_line):
                        flush_block()
                        stopped_section = current_section
                        current_section = None
                        add_ignored_line(diagnostics, "summary_help_block_rejected", cleaned_line, section_name=stopped_section, page_index=page_index)
                        continue
                    if not cleaned_line or is_foreign_currency_followup(cleaned_line):
                        if is_foreign_currency_followup(cleaned_line):
                            diagnostics["ignored_followups"] = int(diagnostics["ignored_followups"]) + 1
                            add_ignored_line(diagnostics, "foreign_currency_followup", cleaned_line, section_name=current_section, page_index=page_index)
                        continue
                    if self.is_starter_line(cleaned_line):
                        flush_block()
                        block_index = line_index
                        block = [cleaned_line]
                        continue
                    if block and self.is_continuation_line(cleaned_line):
                        block.append(cleaned_line)
                        continue
                    add_ignored_line(diagnostics, "no_transaction_table_row_detected", cleaned_line, section_name=current_section, page_index=page_index)
            flush_block()

            if page_active_sections:
                default_section = page_active_sections[0]
                for table_index, table in enumerate(page.get("tables") or [], start=1):
                    table_section = default_section
                    for row_index, row in enumerate(table or [], start=1):
                        raw_row = " | ".join(normalize_whitespace(cell) for cell in (row or []) if normalize_whitespace(cell))
                        if len(page_active_sections) > 1:
                            if classify_transaction_type(raw_row, section_name="payments_credits_adjustments")[1] == "Transfer":
                                table_section = "payments_credits_adjustments"
                            else:
                                table_section = "transactions"
                        if raw_row:
                            diagnostics["candidate_rows_found"] = int(diagnostics["candidate_rows_found"]) + 1
                            append_preview_item(
                                diagnostics,
                                "candidate_row_preview",
                                {
                                    "page": page_index,
                                    "section": table_section,
                                    "line": raw_row[:220],
                                },
                                limit=16,
                            )
                        record, reason = self.parse_table_row(row or [], source_document, f"{page_index}_{table_index}_{row_index}", table_section, parser_source)
                        if not record:
                            if raw_row:
                                add_rejection(diagnostics, reason or "table_row_rejected", raw_row, table_section, page_index)
                            continue
                        fingerprint = record.get("fingerprint") or build_transaction_fingerprint(record["date"], record["raw_description"], record["amount"])
                        if fingerprint in seen_fingerprints:
                            diagnostics["rows_filtered_out"] = int(diagnostics["rows_filtered_out"]) + 1
                            continue
                        seen_fingerprints.add(fingerprint)
                        diagnostics["rows_parsed"] = int(diagnostics["rows_parsed"]) + 1
                        rows.append(record)
                        append_preview_item(
                            diagnostics,
                            "parsed_row_preview",
                            {
                                "page": page_index,
                                "section": table_section,
                                "date": record.get("date") or "",
                                "description": record.get("description") or "",
                                "amount": record.get("amount"),
                            },
                            limit=16,
                        )

        diagnostics["sections_found"] = sorted(section.replace("_", " ") for section in sections_found)
        return rows, diagnostics


class CapitalOnePdfParser(BasePdfParser):
    name = "Capital One"
    bank_hint = "capital one"
    active_sections = [
        (re.compile(r"^\s*transactions(?:\s*\(continued\))?(?:\s+.*)?$", re.I), "transactions"),
        (re.compile(r"^\s*payments,\s*credits?\s+and\s+adjustments(?:\s+.*)?$", re.I), "payments_credits_adjustments"),
    ]
    blocked_headers = [
        re.compile(r"^\s*account summary.*$", re.I),
        re.compile(r"^\s*(?:payment information|rewards?\s+summary|interest charge.*|fees?.*|legal.*|customer service.*|years?\s+previous\s+adjusted.*)$", re.I),
        re.compile(r"^\s*(?:previous balance|new balance|minimum payment due|credit line|available credit).*$", re.I),
    ]
    extra_stop_phrases = tuple(NON_TRANSACTION_PHRASES)

    def matches(self, full_text):
        lowered = (full_text or "").lower()
        return "capital one" in lowered


class TdBankPdfParser(BasePdfParser):
    name = "TD Bank"
    bank_hint = "td bank"
    active_sections = [
        (re.compile(r"^\s*deposits?(?:\s+and\s+credits?)?\s*$", re.I), "transactions"),
        (re.compile(r"^\s*deposits?\s+and\s+additions\s*$", re.I), "transactions"),
        (re.compile(r"^\s*electronic\s+payments?\s*$", re.I), "payments_credits_adjustments"),
        (re.compile(r"^\s*payments?\s*$", re.I), "payments_credits_adjustments"),
        (re.compile(r"^\s*other\s+withdrawals?\s*$", re.I), "transactions"),
        (re.compile(r"^\s*checks?\s+paid\s*$", re.I), "transactions"),
        (re.compile(r"^\s*debit\s+card\s+purchases?\s*$", re.I), "transactions"),
    ]
    blocked_headers = [
        re.compile(r"^\s*account summary.*$", re.I),
        re.compile(r"^\s*daily balance summary.*$", re.I),
        re.compile(r"^\s*(?:beginning|ending) balance.*$", re.I),
        re.compile(r"^\s*subtotals?.*$", re.I),
    ]
    extra_stop_phrases = tuple(list(NON_TRANSACTION_PHRASES) + ["daily balance summary", "beginning balance", "ending balance"])

    def matches(self, full_text):
        lowered = (full_text or "").lower()
        return "td bank" in lowered or "toronto-dominion" in lowered


class GenericPdfParser(BasePdfParser):
    name = "Generic"
    bank_hint = "generic"
    active_sections = [
        (re.compile(r"^\s*transactions(?:\s*\(continued\))?(?:\s+.*)?$", re.I), "transactions"),
        (re.compile(r"^\s*payments,\s*credits?\s+and\s+adjustments(?:\s+.*)?$", re.I), "payments_credits_adjustments"),
        (re.compile(r"^\s*deposits?(?:\s+and\s+credits?)?\s*$", re.I), "transactions"),
    ]
    blocked_headers = [
        re.compile(r"^\s*account summary.*$", re.I),
        re.compile(r"^\s*daily balance summary.*$", re.I),
    ]
    extra_stop_phrases = tuple(NON_TRANSACTION_PHRASES)

    def matches(self, full_text):
        return True


PARSERS = [CapitalOnePdfParser(), TdBankPdfParser(), GenericPdfParser()]


def choose_pdf_parser(full_text):
    for parser in PARSERS:
        if parser.matches(full_text):
            return parser
    return GenericPdfParser()


def extract_pdf_pages(file_storage):
    if pdfplumber is None:
        raise RuntimeError("PDF import support requires pdfplumber.")
    pdf_bytes = file_storage.read()
    pages = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            raw_text = page.extract_text() or ""
            layout_text = page.extract_text(layout=True) or ""
            try:
                words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
            except TypeError:
                words = page.extract_words() or []
            word_lines = build_lines_from_words(words)
            raw_lines = cleaned_text_lines(raw_text)
            layout_lines = cleaned_text_lines(layout_text)
            line_source, chosen_lines = choose_best_page_lines(raw_lines, layout_lines, word_lines)
            pages.append({
                "page_number": page_number,
                "text": raw_text or layout_text or "\n".join(word_lines),
                "layout_text": layout_text,
                "line_source": line_source,
                "lines": chosen_lines,
                "ignored_lines": [],
                "tables": page.extract_tables() or [],
            })
    return pdf_bytes, strip_repeated_pdf_noise(pages)


def parse_csv_statement(file_storage):
    content = file_storage.read().decode("utf-8-sig", errors="ignore")
    reader = csv.reader(StringIO(content))
    rows = list(reader)
    if not rows:
        return None, "CSV is empty."

    header_probe = [normalize_whitespace(cell).lower() for cell in rows[0]]
    has_header = rows and any(
        any(token in header for token in ("date", "description", "amount", "debit", "credit", "posted", "merchant"))
        for header in header_probe
    )
    data_rows = rows[1:] if has_header else rows
    column_map = detect_csv_columns(rows[0] if has_header else [])

    date_idx = column_map.get("date")
    desc_idx = column_map.get("description")
    amount_idx = column_map.get("amount")
    credit_idx = column_map.get("credit")
    debit_idx = column_map.get("debit")
    category_idx = column_map.get("category")

    if date_idx is None or desc_idx is None or (amount_idx is None and (credit_idx is None or debit_idx is None)):
        widths = {len(row) for row in data_rows[:8] if row}
        if widths == {3}:
            date_idx, desc_idx, amount_idx = 0, 1, 2
        elif widths == {4}:
            date_idx, desc_idx, credit_idx, debit_idx = 0, 1, 2, 3
        else:
            return None, "Could not confidently map the CSV columns. A mapping step is needed for this file."

    extracted_rows = []
    skipped_rows = 0
    source_document = file_storage.filename or "statement.csv"
    diagnostics = init_diagnostics(source_document, "CSV detector")
    diagnostics["text_extracted"] = True
    diagnostics["raw_text_preview"] = content[:3000]
    diagnostics["matched_columns"] = {
        "date": date_idx,
        "description": desc_idx,
        "amount": amount_idx,
        "credit": credit_idx,
        "debit": debit_idx,
        "category": category_idx,
    }
    for row_index, row in enumerate(data_rows, start=1):
        if not row:
            continue
        parsed_date = parse_statement_date_with_fallback(row[date_idx] if date_idx is not None and date_idx < len(row) else "")
        raw_description = normalize_whitespace(row[desc_idx] if desc_idx is not None and desc_idx < len(row) else "")
        if amount_idx is not None and amount_idx < len(row):
            amount = safe_float(row[amount_idx])
        else:
            credit = safe_float(row[credit_idx]) if credit_idx is not None and credit_idx < len(row) else None
            debit = safe_float(row[debit_idx]) if debit_idx is not None and debit_idx < len(row) else None
            amount = credit if credit not in (None, 0) else (-abs(debit) if debit not in (None, 0) else None)
        if parsed_date is None or not raw_description or amount is None:
            skipped_rows += 1
            diagnostics["malformed_rows"] = int(diagnostics.get("malformed_rows", 0)) + 1
            reason = "missing_valid_amount" if amount is None else "missing_date" if parsed_date is None else "invalid_description_block"
            add_rejection(diagnostics, reason, " | ".join(normalize_whitespace(cell) for cell in row if normalize_whitespace(cell)))
            continue
        source_category = normalize_whitespace(row[category_idx]) if category_idx is not None and category_idx < len(row) else ""
        transaction_type, default_category = classify_transaction_type(raw_description)
        extracted_rows.append(
            normalize_row_payload(
                source_document=source_document,
                raw_source=" | ".join(normalize_whitespace(cell) for cell in row if normalize_whitespace(cell)),
                parsed_date=parsed_date,
                raw_description=raw_description,
                amount=amount,
                source_category=source_category or default_category,
                raw_category=source_category,
                transaction_type=transaction_type,
                parser_label="CSV detector",
                parser_source="rule_based",
                parser_confidence=0.92,
            )
        )
        diagnostics["rows_parsed"] = int(diagnostics.get("rows_parsed", 0)) + 1

    if not extracted_rows:
        return None, "No valid transactions were detected in the uploaded CSV."

    diagnostics["rows_rejected"] = skipped_rows
    diagnostics["confidence_score"] = 0.92 if skipped_rows == 0 else 0.84
    diagnostics["confidence_label"] = "high" if skipped_rows <= max(1, len(extracted_rows) // 5) else "medium"
    if skipped_rows:
        diagnostics["warnings"].append(f"{skipped_rows} malformed row{'s' if skipped_rows != 1 else ''} were skipped.")

    return {
        "rows": extracted_rows,
        "skipped_rows": skipped_rows,
        "detected_columns": {
            "date": rows[0][date_idx] if has_header and date_idx is not None else "column 1",
            "description": rows[0][desc_idx] if has_header and desc_idx is not None else "column 2",
            "amount": rows[0][amount_idx] if has_header and amount_idx is not None else "credit/debit columns" if has_header else "column 3/4",
            "source_category": rows[0][category_idx] if has_header and category_idx is not None else "Not provided",
        },
        "diagnostics": diagnostics,
    }, ""


def parse_text_statement(source_document, raw_text, parser_source="manual"):
    pages = [{
        "text": raw_text or "",
        "tables": [],
        "line_source": "text",
        "lines": cleaned_text_lines(raw_text or ""),
        "ignored_lines": [],
    }]
    parser = choose_pdf_parser(raw_text)
    rows, diagnostics = parser.parse_pages(pages, source_document, parser_source=parser_source)
    period_start, period_end = detect_statement_period(raw_text)
    diagnostics["statement_period_start"] = period_start
    diagnostics["statement_period_end"] = period_end
    confidence = score_rule_parse(diagnostics)
    diagnostics["confidence_score"] = confidence["score"]
    diagnostics["confidence_label"] = confidence["label"]
    diagnostics["parser_used"] = "rule_based"
    warnings = list(confidence.get("reasons", []))
    diagnostics["ai_ready"] = True

    diagnostics["warnings"] = warnings[:10]
    logger.info(
        "Parsed text statement %s via %s: %s rows, %s candidates, %s rejected, confidence %.2f",
        source_document,
        parser.name,
        len(rows),
        diagnostics.get("candidate_rows_found", 0),
        diagnostics.get("rows_rejected", 0),
        diagnostics.get("confidence_score", 0.0),
    )
    if not rows:
        if not diagnostics.get("text_extracted"):
            return None, f"No readable text was found in {source_document}.", diagnostics
        if not diagnostics.get("sections_found"):
            return None, f"AkuOS could not find a Transactions section in {source_document}.", diagnostics
        if diagnostics.get("candidate_rows_found", 0) == 0:
            return None, f"A transactions section was found in {source_document}, but no transaction rows could be detected.", diagnostics
        if diagnostics.get("rows_rejected", 0) >= diagnostics.get("candidate_rows_found", 0):
            return None, f"AkuOS found candidate transaction rows in {source_document}, but all of them were rejected during parsing.", diagnostics
        return None, f"No valid transactions were detected in {source_document}.", diagnostics

    return {
        "rows": rows,
        "skipped_rows": int(diagnostics.get("rows_rejected", 0)),
        "detected_columns": {
            "date": "Statement detection",
            "description": "Statement detection",
            "amount": "Statement detection",
            "source_category": "Not provided",
            "parser": f"{diagnostics.get('parser_used', 'rule_based')} · {parser.name}",
            "sections": ", ".join(diagnostics.get("sections_found", [])) or "Not detected",
        },
        "diagnostics": diagnostics,
    }, "", diagnostics


def parse_pdf_statement(file_storage):
    if pdfplumber is None:
        return None, "PDF import support requires `pdfplumber`. Add it to your environment and try again.", {}
    _, pages = extract_pdf_pages(file_storage)
    full_text = "\n".join(page.get("text") or "" for page in pages)
    parser = choose_pdf_parser(full_text)
    rows, diagnostics = parser.parse_pages(pages, file_storage.filename or "statement.pdf", parser_source="rule_based")
    period_start, period_end = detect_statement_period(full_text)
    diagnostics["statement_period_start"] = period_start
    diagnostics["statement_period_end"] = period_end
    confidence = score_rule_parse(diagnostics)
    diagnostics["confidence_score"] = confidence["score"]
    diagnostics["confidence_label"] = confidence["label"]
    diagnostics["parser_used"] = "rule_based"
    diagnostics["warnings"] = list(confidence.get("reasons", []))
    diagnostics["ai_ready"] = True
    logger.info(
        "Parsed PDF %s via %s: %s rows, %s candidates, %s rejected, confidence %.2f",
        file_storage.filename or "statement.pdf",
        parser.name,
        len(rows),
        diagnostics.get("candidate_rows_found", 0),
        diagnostics.get("rows_rejected", 0),
        diagnostics.get("confidence_score", 0.0),
    )

    if not rows:
        filename = file_storage.filename or "the PDF"
        logger.warning(
            "PDF parsing failed for %s: readable=%s sections=%s candidates=%s rejected=%s filtered=%s",
            filename,
            diagnostics.get("text_extracted"),
            diagnostics.get("sections_found"),
            diagnostics.get("candidate_rows_found", 0),
            diagnostics.get("rows_rejected", 0),
            diagnostics.get("rows_filtered_out", 0),
        )
        if not diagnostics.get("text_extracted"):
            return None, f"No readable text was found in {filename}. The PDF may be image-only or protected.", diagnostics
        if not diagnostics.get("sections_found"):
            return None, f"AkuOS could not find a Transactions section in {filename}. Try a full statement export instead of a summary PDF.", diagnostics
        if diagnostics.get("candidate_rows_found", 0) == 0:
            return None, f"A transactions section was found in {filename}, but no transaction table rows could be detected.", diagnostics
        if diagnostics.get("rows_rejected", 0) >= diagnostics.get("candidate_rows_found", 0):
            return None, f"AkuOS found candidate transaction rows in {filename}, but all of them were rejected during parsing.", diagnostics
        return None, f"No valid transactions were detected in {filename}.", diagnostics

    return {
        "rows": rows,
        "skipped_rows": int(diagnostics.get("rows_rejected", 0)),
        "detected_columns": {
            "date": "PDF statement detection",
            "description": "PDF statement detection",
            "amount": "PDF statement detection",
            "source_category": "Not provided",
            "parser": f"{diagnostics.get('parser_used', 'rule_based')} · {parser.name}",
            "sections": ", ".join(diagnostics.get("sections_found", [])) or "Not detected",
        },
        "diagnostics": diagnostics,
    }, "", diagnostics


def parse_statement_input(file_storage):
    file_type = detect_statement_file_type(file_storage.filename or "")
    if file_type == "csv":
        result, error = parse_csv_statement(file_storage)
        return result, error, (result or {}).get("diagnostics", {})
    if file_type == "pdf":
        return parse_pdf_statement(file_storage)
    raw_text = file_storage.read().decode("utf-8", errors="ignore")
    result, error, diagnostics = parse_text_statement(file_storage.filename or "pasted statement text", raw_text, parser_source="manual")
    return result, error, diagnostics
