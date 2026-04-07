from __future__ import annotations


def bounded_score(value):
    return max(0.0, min(1.0, float(value or 0.0)))


def confidence_label(score):
    score = bounded_score(score)
    if score >= 0.85:
        return "high"
    if score >= 0.62:
        return "medium"
    return "low"


def score_rule_parse(diagnostics):
    diagnostics = diagnostics or {}
    candidate_rows = int(diagnostics.get("candidate_rows_found") or 0)
    rows_parsed = int(diagnostics.get("rows_parsed") or 0)
    rows_rejected = int(diagnostics.get("rows_rejected") or 0)
    rows_filtered = int(diagnostics.get("rows_filtered_out") or 0)
    text_extracted = bool(diagnostics.get("text_extracted"))
    sections_found = bool(diagnostics.get("sections_found"))

    score = 0.0
    reasons = []
    if text_extracted:
        score += 0.20
    else:
        reasons.append("no readable text extracted")
    if sections_found:
        score += 0.18
    else:
        reasons.append("no target transaction section detected")
    if candidate_rows:
        score += 0.18
    else:
        reasons.append("no transaction candidates found")

    acceptance_ratio = (rows_parsed / candidate_rows) if candidate_rows else 0.0
    rejection_ratio = (rows_rejected / candidate_rows) if candidate_rows else 1.0
    filtered_ratio = (rows_filtered / max(rows_parsed, 1)) if rows_parsed else 1.0

    score += min(0.28, acceptance_ratio * 0.28)
    score -= min(0.14, rejection_ratio * 0.14)
    score -= min(0.08, filtered_ratio * 0.08)

    if rows_parsed >= 8:
        score += 0.10
    elif rows_parsed >= 3:
        score += 0.06
    elif rows_parsed == 1:
        reasons.append("only one row parsed")

    final_score = bounded_score(score)
    label = confidence_label(final_score)
    return {
        "score": final_score,
        "label": label,
        "should_try_ai": final_score < 0.62,
        "reasons": reasons[:5],
    }


def score_ai_parse(rows, warnings=None):
    rows = rows or []
    warning_count = len(warnings or [])
    base = 0.72 if rows else 0.0
    if len(rows) >= 5:
        base += 0.08
    if warning_count:
        base -= min(0.18, warning_count * 0.04)
    final_score = bounded_score(base)
    return {
        "score": final_score,
        "label": confidence_label(final_score),
        "should_accept": bool(rows) and final_score >= 0.55,
    }
