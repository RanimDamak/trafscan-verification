"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A | Layer 4: Alerting + HTML Report
────────────────────────────────────────────────────────────────────────────
Two responsibilities:

1. Generate a self-contained HTML daily report (for archiving / NOC display).
2. Send an HTML email alert when CRITICAL or WARNING anomalies are found.

Email configuration
────────────────────
⚠ EMAIL NOT YET CONFIGURED ⚠
The SMTP settings in config.yaml are currently left as placeholders.
To enable email alerts, fill in the `alerting` section in config.yaml:

  alerting:
    smtp_host:     smtp.example.com       # ← fill in
    smtp_port:     587
    smtp_user:     trafscan@example.com   # ← fill in
    smtp_password: your_app_password      # ← fill in
    smtp_tls:      true
    from_email:    trafscan@example.com   # ← fill in
    to_emails:
      - ops@example.com                  # ← fill in
    alert_on: [CRITICAL, WARNING]

Until configured, the send_email_alert() function will log a warning and
return False without attempting any connection. No emails will be sent.

NOC / Frontend integration
───────────────────────────
The batch produces two integration artifacts after every run:
  - reports/report_YYYYMMDD_HHMMSS.html  → full HTML report for browser/NOC
  - reports/noc_latest.json              → JSON payload for frontend polling

The Trafscan frontend team can:
  Option A (polling): GET /reports/noc_latest.json every N minutes
  Option B (webhook): Configure alerting.noc_webhook_url in config.yaml
                      to receive a POST after every batch run

See build_noc_payload() and send_noc_notification() for payload format.

This module is called by batch.py — do not run it directly.
For standalone testing: python alerting.py --config config.yaml --test
"""

from __future__ import annotations

import logging
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HTML Report Generator
# ─────────────────────────────────────────────────────────────────────────────

def build_html_report(results: dict, config: dict) -> str:
    """
    Build a fully self-contained HTML report from batch results.
    No external CSS or JS — renders in any email client or browser.

    The results dict from batch.py has this structure:
      {
        "stuck":             [...],
        "mismatches":        [...],
        "divergences":       {          ← dict keyed by dimension
            "minutes":  [...],
            "voice":    [...],
            "sms":      [...],
            "data":     [...],
            "recharge": [...],
        },
        "missing_records":   [...],
        "count_anomalies":   [...],     ← Q5a: daily file count vs baseline
        "funnel_anomalies":  [...],     ← Q5b: pipeline funnel gaps
        "unregistered":      [...],     ← Q5c: files on disk not in DB
        "summary":           [...],
      }
    """
    now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = datetime.now().strftime("%Y-%m-%d")

    stuck        = results.get("stuck", [])
    mismatches   = results.get("mismatches", [])
    divergences      = results.get("divergences", {})
    missing_rec      = results.get("missing_records", [])
    count_anomalies  = results.get("count_anomalies", [])
    funnel_anomalies = results.get("funnel_anomalies", [])
    unregistered     = results.get("unregistered", [])
    summary          = results.get("summary", [])

    all_div_flat = [item for lst in divergences.values() for item in lst]
    all_issues   = (stuck + mismatches + all_div_flat + missing_rec +
                    count_anomalies + funnel_anomalies + unregistered)

    total_anomalies = len(all_issues)
    critical        = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    warnings        = total_anomalies - critical

    # Banner
    if critical > 0:
        banner_color = "#c0392b"
        banner_text  = f"⚠ ACTION REQUIRED — {critical} CRITICAL anomaly(ies) detected"
    elif warnings > 0:
        banner_color = "#e67e22"
        banner_text  = f"⚠ {warnings} WARNING(s) detected — review recommended"
    else:
        banner_color = "#27ae60"
        banner_text  = "✓ All checks passed — system healthy"

    def severity_badge(severity: str) -> str:
        color = "#c0392b" if severity == "CRITICAL" else "#e67e22"
        return (f'<span style="background:{color};color:white;'
                f'padding:2px 8px;border-radius:3px;font-size:12px;'
                f'font-weight:bold;">{severity}</span>')

    def section_header(title: str, count: int, ok: bool) -> str:
        icon  = "✓" if ok else "✗"
        color = "#27ae60" if ok else "#c0392b"
        badge = (f'<span style="float:right;background:#c0392b;color:white;'
                 f'padding:1px 10px;border-radius:10px;">{count} issue(s)</span>'
                 if not ok else "")
        return (f'<tr><td colspan="99" style="background:#2c3e50;color:white;'
                f'padding:10px 16px;font-weight:bold;font-size:14px;">'
                f'<span style="color:{color};">{icon}</span>&nbsp; {title}{badge}'
                f'</td></tr>')

    def ok_row(msg: str) -> str:
        return f'<tr><td colspan="99" style="padding:10px 16px;color:#27ae60;">✓ {msg}</td></tr>'

    # ── Q1: Stuck files ───────────────────────────────────────────────────────
    q1_ok   = len(stuck) == 0
    q1_rows = ok_row("No stuck files.") if q1_ok else ""
    if not q1_ok:
        q1_rows = (
            '<tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">'
            '<td style="padding:8px 12px;">File</td>'
            '<td style="padding:8px 12px;">Operator</td>'
            '<td style="padding:8px 12px;">Type</td>'
            '<td style="padding:8px 12px;">Status</td>'
            '<td style="padding:8px 12px;">Waiting</td>'
            '<td style="padding:8px 12px;">Severity</td></tr>'
        )
        for r in stuck:
            q1_rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r["file_name"]}</td>'
                f'<td style="padding:8px 12px;">{r["operator_id"]}</td>'
                f'<td style="padding:8px 12px;">{r["cdr_type"]}</td>'
                f'<td style="padding:8px 12px;">{r["status"]}</td>'
                f'<td style="padding:8px 12px;">{r["hours_waiting"]}h</td>'
                f'<td style="padding:8px 12px;">{severity_badge(r["severity"])}</td></tr>'
            )

    # ── Q2: Checksum mismatches ───────────────────────────────────────────────
    q2_ok   = len(mismatches) == 0
    q2_rows = ok_row("All transferred files have matching checksums.") if q2_ok else ""
    if not q2_ok:
        q2_rows = (
            '<tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">'
            '<td style="padding:8px 12px;">File</td>'
            '<td style="padding:8px 12px;">Operator</td>'
            '<td style="padding:8px 12px;">Type</td>'
            '<td style="padding:8px 12px;">Expected (16 chars)</td>'
            '<td style="padding:8px 12px;">Received (16 chars)</td>'
            '<td style="padding:8px 12px;">Severity</td></tr>'
        )
        for r in mismatches:
            q2_rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r["file_name"]}</td>'
                f'<td style="padding:8px 12px;">{r["operator_id"]}</td>'
                f'<td style="padding:8px 12px;">{r["cdr_type"]}</td>'
                f'<td style="padding:8px 12px;font-family:monospace;font-size:11px;">{r["expected"][:16]}...</td>'
                f'<td style="padding:8px 12px;font-family:monospace;font-size:11px;color:#c0392b;">{r["transferred"][:16]}...</td>'
                f'<td style="padding:8px 12px;">{severity_badge(r["severity"])}</td></tr>'
            )

    # ── Q3: Divergence sections (5 dimensions) ────────────────────────────────
    threshold = config.get("batch", {}).get("divergence_pct", 1.0)
    dim_defs  = [
        ("minutes",  "Q3a · Minutes divergence (DB vs ES)",                     "Minutes"),
        ("voice",    "Q3b · Voice traffic divergence (DB vs ES)",                "Voice"),
        ("sms",      "Q3c · SMS traffic divergence (DB vs ES)",                  "SMS"),
        ("data",     "Q3d · Data traffic divergence (DB vs ES)",                 "Data"),
        ("recharge", "Q3e · Recharge divergence (DB vs ES) — cdr_type=in only", "Recharge"),
    ]
    q3_sections = ""
    for dim_key, dim_title, dim_label in dim_defs:
        dim_list = divergences.get(dim_key, [])
        dim_ok   = len(dim_list) == 0
        dim_rows = ok_row(f"No {dim_label.lower()} divergence above {threshold}% threshold.") if dim_ok else ""
        if not dim_ok:
            dim_rows = (
                '<tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">'
                '<td style="padding:8px 12px;">File</td>'
                '<td style="padding:8px 12px;">Operator</td>'
                '<td style="padding:8px 12px;">Type</td>'
                f'<td style="padding:8px 12px;">DB {dim_label}</td>'
                f'<td style="padding:8px 12px;">ES {dim_label}</td>'
                '<td style="padding:8px 12px;">Delta %</td>'
                '<td style="padding:8px 12px;">Severity</td></tr>'
            )
            for r in dim_list:
                dim_rows += (
                    f'<tr style="border-bottom:1px solid #eee;">'
                    f'<td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r["file_name"]}</td>'
                    f'<td style="padding:8px 12px;">{r["operator_id"]}</td>'
                    f'<td style="padding:8px 12px;">{r["cdr_type"]}</td>'
                    f'<td style="padding:8px 12px;">{r["db_value"]:.3f}</td>'
                    f'<td style="padding:8px 12px;">{r["es_value"]:.3f}</td>'
                    f'<td style="padding:8px 12px;color:#c0392b;font-weight:bold;">{r["delta_pct"]:.2f}%</td>'
                    f'<td style="padding:8px 12px;">{severity_badge(r["severity"])}</td></tr>'
                )
        q3_sections += section_header(dim_title, len(dim_list), dim_ok) + dim_rows

    # TODO FORFAITS: add Q3f section here once forfait table names confirmed

    # ── Q4: Missing records ───────────────────────────────────────────────────
    q4_ok   = len(missing_rec) == 0
    q4_rows = ok_row("No missing records detected.") if q4_ok else ""
    if not q4_ok:
        q4_rows = (
            '<tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">'
            '<td style="padding:8px 12px;">File</td>'
            '<td style="padding:8px 12px;">Operator</td>'
            '<td style="padding:8px 12px;">Expected</td>'
            '<td style="padding:8px 12px;">Processed</td>'
            '<td style="padding:8px 12px;">Missing</td>'
            '<td style="padding:8px 12px;">Missing %</td>'
            '<td style="padding:8px 12px;">Severity</td></tr>'
        )
        for r in missing_rec:
            q4_rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r["file_name"]}</td>'
                f'<td style="padding:8px 12px;">{r["operator_id"]}</td>'
                f'<td style="padding:8px 12px;">{r["expected"]:,}</td>'
                f'<td style="padding:8px 12px;">{r["processed"]:,}</td>'
                f'<td style="padding:8px 12px;color:#c0392b;font-weight:bold;">{r["missing"]:,}</td>'
                f'<td style="padding:8px 12px;">{r["missing_pct"]:.2f}%</td>'
                f'<td style="padding:8px 12px;">{severity_badge(r["severity"])}</td></tr>'
            )

    # ── Q5a: Daily file count vs baseline ────────────────────────────────────
    tol_pct = config.get("baselines", {}).get("tolerance_pct", 3.0)
    q5a_ok   = len(count_anomalies) == 0
    q5a_rows = ok_row(f"All operator/type file counts within ±{tol_pct}% baseline.") if q5a_ok else ""
    if not q5a_ok:
        q5a_rows = (
            '<tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">'
            '<td style="padding:8px 12px;">Operator</td>'
            '<td style="padding:8px 12px;">Type</td>'
            '<td style="padding:8px 12px;">Received</td>'
            '<td style="padding:8px 12px;">Baseline</td>'
            '<td style="padding:8px 12px;">Delta %</td>'
            '<td style="padding:8px 12px;">Detail</td>'
            '<td style="padding:8px 12px;">Severity</td></tr>'
        )
        for r in count_anomalies:
            q5a_rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:8px 12px;">{r["operator_id"]}</td>'
                f'<td style="padding:8px 12px;">{r["cdr_type"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;">{r["received"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;">{r["baseline"]}</td>'
                f'<td style="padding:8px 12px;color:#c0392b;font-weight:bold;">{r["delta_pct"]:+.1f}%</td>'
                f'<td style="padding:8px 12px;font-size:11px;">{r["detail"]}</td>'
                f'<td style="padding:8px 12px;">{severity_badge(r["severity"])}</td></tr>'
            )

    # ── Q5b: Pipeline funnel ──────────────────────────────────────────────────
    q5b_ok   = len(funnel_anomalies) == 0
    q5b_rows = ok_row("Pipeline funnel healthy for all operator/type combinations.") if q5b_ok else ""
    if not q5b_ok:
        q5b_rows = (
            '<tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">'
            '<td style="padding:8px 12px;">Operator</td>'
            '<td style="padding:8px 12px;">Type</td>'
            '<td style="padding:8px 12px;text-align:right;">Received</td>'
            '<td style="padding:8px 12px;text-align:right;">Completed</td>'
            '<td style="padding:8px 12px;text-align:right;">Gap %</td>'
            '<td style="padding:8px 12px;">Detail</td>'
            '<td style="padding:8px 12px;">Severity</td></tr>'
        )
        for r in funnel_anomalies:
            q5b_rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:8px 12px;">{r["operator_id"]}</td>'
                f'<td style="padding:8px 12px;">{r["cdr_type"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;">{r["received"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;">{r["completed"]}</td>'
                f'<td style="padding:8px 12px;color:#c0392b;font-weight:bold;">{r["gap_pct"]}%</td>'
                f'<td style="padding:8px 12px;font-size:11px;">{r["detail"]}</td>'
                f'<td style="padding:8px 12px;">{severity_badge(r["severity"])}</td></tr>'
            )

    # ── Q5c: Files on disk not registered in DB ───────────────────────────────
    q5c_ok   = len(unregistered) == 0
    q5c_rows = ok_row("All files on disk are registered in the DB.") if q5c_ok else ""
    if not q5c_ok:
        q5c_rows = (
            '<tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">'
            '<td style="padding:8px 12px;">File</td>'
            '<td style="padding:8px 12px;">Operator</td>'
            '<td style="padding:8px 12px;">Type</td>'
            '<td style="padding:8px 12px;">Directory</td>'
            '<td style="padding:8px 12px;">Severity</td></tr>'
        )
        for r in unregistered:
            q5c_rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r["file_name"]}</td>'
                f'<td style="padding:8px 12px;">{r["operator_id"]}</td>'
                f'<td style="padding:8px 12px;">{r["cdr_type"]}</td>'
                f'<td style="padding:8px 12px;font-family:monospace;font-size:11px;">{r["directory"]}</td>'
                f'<td style="padding:8px 12px;">{severity_badge(r["severity"])}</td></tr>'
            )

    # ── Q6: Daily summary ─────────────────────────────────────────────────────
    if not summary:
        q6_rows = ok_row("No files received today.")
    else:
        q6_rows = (
            '<tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">'
            '<td style="padding:8px 12px;">Operator</td>'
            '<td style="padding:8px 12px;">Type</td>'
            '<td style="padding:8px 12px;text-align:right;">Total</td>'
            '<td style="padding:8px 12px;text-align:right;">Done</td>'
            '<td style="padding:8px 12px;text-align:right;">Pending</td>'
            '<td style="padding:8px 12px;text-align:right;">Mismatch</td>'
            '<td style="padding:8px 12px;text-align:right;">Error</td></tr>'
        )
        for r in summary:
            mm = 'color:#c0392b;font-weight:bold;' if r["mismatch"] > 0 else ''
            em = 'color:#c0392b;font-weight:bold;' if r["error"]    > 0 else ''
            q6_rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:8px 12px;">{r["operator_id"]}</td>'
                f'<td style="padding:8px 12px;">{r["cdr_type"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;">{r["total"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;color:#27ae60;">{r["done"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;">{r["pending"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;{mm}">{r["mismatch"]}</td>'
                f'<td style="padding:8px 12px;text-align:right;{em}">{r["error"]}</td></tr>'
            )

    files_done = sum(r["done"] for r in summary) if summary else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trafscan CDR Report — {date_str}</title>
</head>
<body style="margin:0;padding:0;font-family:Arial,sans-serif;background:#f0f2f5;color:#2c3e50;">
<div style="max-width:960px;margin:30px auto;background:white;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.1);">

  <!-- Header -->
  <div style="background:#2c3e50;padding:24px 30px;">
    <h1 style="margin:0;color:white;font-size:20px;">TRAFSCAN — CDR Registry</h1>
    <p style="margin:6px 0 0;color:#95a5a6;font-size:13px;">Daily Reconciliation Report &nbsp;|&nbsp; {now}</p>
  </div>

  <!-- Status Banner -->
  <div style="background:{banner_color};padding:14px 30px;">
    <p style="margin:0;color:white;font-size:15px;font-weight:bold;">{banner_text}</p>
  </div>

  <!-- Summary Cards -->
  <div style="display:flex;gap:0;border-bottom:2px solid #eee;">
    <div style="flex:1;padding:20px;text-align:center;border-right:1px solid #eee;">
      <div style="font-size:32px;font-weight:bold;color:#2c3e50;">{total_anomalies}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Total Anomalies</div>
    </div>
    <div style="flex:1;padding:20px;text-align:center;border-right:1px solid #eee;">
      <div style="font-size:32px;font-weight:bold;color:#c0392b;">{critical}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Critical</div>
    </div>
    <div style="flex:1;padding:20px;text-align:center;border-right:1px solid #eee;">
      <div style="font-size:32px;font-weight:bold;color:#e67e22;">{warnings}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Warnings</div>
    </div>
    <div style="flex:1;padding:20px;text-align:center;">
      <div style="font-size:32px;font-weight:bold;color:#27ae60;">{files_done}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Files Done Today</div>
    </div>
  </div>

  <!-- Check sections -->
  <div style="padding:0;">
    <table style="width:100%;border-collapse:collapse;">
      {section_header("Q1 — Stuck files (PENDING / PROCESSING too long)", len(stuck), q1_ok)}
      {q1_rows}
      {section_header("Q2 — Checksum mismatches (transfer corruption)", len(mismatches), q2_ok)}
      {q2_rows}
      {q3_sections}
      {section_header("Q4 — Missing records per file", len(missing_rec), q4_ok)}
      {q4_rows}
      {section_header(f"Q5a — Daily file count vs baseline (±{tol_pct}%)", len(count_anomalies), q5a_ok)}
      {q5a_rows}
      {section_header("Q5b — Pipeline funnel (received → completed)", len(funnel_anomalies), q5b_ok)}
      {q5b_rows}
      {section_header("Q5c — Files on disk not registered in DB", len(unregistered), q5c_ok)}
      {q5c_rows}
      {section_header("Q6 — Daily summary", 0, True)}
      {q6_rows}
    </table>
  </div>

  <!-- Footer -->
  <div style="background:#f8f9fa;border-top:2px solid #eee;padding:16px 24px;">
    <p style="margin:0;font-size:11px;color:#95a5a6;">
      Generated by Trafscan Solution A &nbsp;|&nbsp; {now} &nbsp;|&nbsp;
      <strong style="color:{'#c0392b' if critical > 0 else '#27ae60'};">
        Exit code: {'1 (CRITICAL)' if critical > 0 else '0 (OK)'}
      </strong>
    </p>
  </div>
</div>
</body>
</html>"""

    return html


# ─────────────────────────────────────────────────────────────────────────────
# Email sender
# ─────────────────────────────────────────────────────────────────────────────

def send_email_alert(results: dict, config: dict, html_report: str) -> bool:
    """
    Send an HTML email alert with the report embedded.
    Returns True on success, False on failure (or if not configured).

    ⚠ NOT YET CONFIGURED — fill in config.yaml alerting section first.

    config.yaml alerting section:
      alerting:
        smtp_host:     smtp.gmail.com
        smtp_port:     587
        smtp_user:     trafscan@example.com
        smtp_password: your_app_password
        smtp_tls:      true
        from_email:    trafscan@example.com
        to_emails:
          - ops-team@example.com
          - noc@example.com
        alert_on: [CRITICAL, WARNING]   # which severities trigger an email
    """
    alert_cfg     = config.get("alerting", {})
    smtp_host     = alert_cfg.get("smtp_host")
    smtp_port     = int(alert_cfg.get("smtp_port", 587))
    smtp_user     = alert_cfg.get("smtp_user")
    smtp_password = alert_cfg.get("smtp_password")
    smtp_tls      = alert_cfg.get("smtp_tls", True)
    from_email    = alert_cfg.get("from_email", smtp_user)
    to_emails     = alert_cfg.get("to_emails", [])
    alert_on      = alert_cfg.get("alert_on", ["CRITICAL"])

    # Guard: abort gracefully if not configured
    if not smtp_host or not to_emails:
        log.warning(
            "[alerting] ⚠ Email not configured. "
            "Set smtp_host and to_emails in config.yaml alerting section."
        )
        return False

    # Decide whether to send based on what was found
    stuck        = results.get("stuck", [])
    mismatches   = results.get("mismatches", [])
    divergences  = results.get("divergences", {})
    missing_rec  = results.get("missing_records", [])
    unregistered = results.get("unregistered", [])
    all_div_flat = [item for lst in divergences.values() for item in lst]
    all_issues   = stuck + mismatches + all_div_flat + missing_rec + unregistered

    has_critical = any(r.get("severity") == "CRITICAL" for r in all_issues)
    has_warning  = any(r.get("severity") == "WARNING"  for r in all_issues)
    should_send  = (
        ("CRITICAL" in alert_on and has_critical) or
        ("WARNING"  in alert_on and has_warning)
    )

    if not should_send:
        log.info("[alerting] No anomalies matching alert_on=%s — email not sent.", alert_on)
        return True

    total    = len(all_issues)
    critical = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    date_str = datetime.now().strftime("%Y-%m-%d")

    subject = (
        f"[TRAFSCAN] ⚠ CRITICAL — {critical} anomaly(ies) — {date_str}"
        if has_critical else
        f"[TRAFSCAN] WARNING — {total} anomaly(ies) — {date_str}"
    )

    msg           = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_email
    msg["To"]      = ", ".join(to_emails)

    plain = (
        f"TRAFSCAN CDR Registry — Daily Report\n"
        f"Date: {date_str}\n\n"
        f"Total anomalies : {total}\n"
        f"Critical        : {critical}\n"
        f"Warnings        : {total - critical}\n\n"
        f"Please view the HTML version for full details.\n"
        f"Or run manually:\n"
        f"  SELECT * FROM anomaly_log WHERE detected_at >= CURRENT_DATE ORDER BY severity DESC;\n"
    )
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html_report, "html"))

    try:
        if smtp_tls:
            context = ssl.create_default_context()
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.ehlo()
                server.starttls(context=context)
                server.login(smtp_user, smtp_password)
                server.sendmail(from_email, to_emails, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.sendmail(from_email, to_emails, msg.as_string())

        log.info("[alerting] Email sent to: %s", to_emails)
        return True

    except Exception as exc:
        log.error("[alerting] Failed to send email: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# NOC / Frontend notification
# ─────────────────────────────────────────────────────────────────────────────

def build_noc_payload(results: dict) -> dict:
    """
    Build a JSON-serializable payload for the NOC / frontend notification system.

    This is the integration contract between Trafscan Solution A and
    the Trafscan administration frontend (NOC panel).

    Integration options for the frontend team
    ──────────────────────────────────────────
    Option A — Polling (simplest):
      The batch saves noc_latest.json after every run.
      Frontend polls GET /reports/noc_latest.json every N minutes.
      No configuration needed beyond a static file server.

    Option B — Webhook (real-time):
      Configure alerting.noc_webhook_url in config.yaml.
      The batch will POST this payload to that URL after every run.
      Frontend exposes a POST /api/noc/alerts endpoint.

    Payload structure
    ─────────────────
    {
      "generated_at": "2025-04-23T02:00:00",
      "status":       "CRITICAL" | "WARNING" | "OK",
      "summary": {
        "total_anomalies":  3,
        "critical":         2,
        "warnings":         1,
        "files_done_today": 42
      },
      "anomalies": [
        {
          "type":        "MINUTES_DIVERGENCE",
          "severity":    "CRITICAL",
          "file_name":   "CDR_20250423.gz",
          "operator_id": "atel",
          "cdr_type":    "in",
          "dimension":   "minutes",   ← present for divergence anomalies
          "detail":      "db=1000.25 es=1050.10 delta=4.97%"
        },
        ...
      ]
    }
    """
    stuck            = results.get("stuck", [])
    mismatches       = results.get("mismatches", [])
    divergences      = results.get("divergences", {})
    missing_rec      = results.get("missing_records", [])
    count_anomalies  = results.get("count_anomalies", [])
    funnel_anomalies = results.get("funnel_anomalies", [])
    unregistered     = results.get("unregistered", [])
    summary          = results.get("summary", [])

    all_div_flat = [item for lst in divergences.values() for item in lst]
    all_issues   = (stuck + mismatches + all_div_flat + missing_rec +
                    count_anomalies + funnel_anomalies + unregistered)

    critical = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    warnings = len(all_issues) - critical

    overall_status = ("CRITICAL" if critical > 0 else
                      "WARNING"  if warnings  > 0 else "OK")

    anomalies = []

    for r in stuck:
        anomalies.append({
            "type":        "TIMEOUT",
            "severity":    r["severity"],
            "file_name":   r["file_name"],
            "operator_id": r["operator_id"],
            "cdr_type":    str(r["cdr_type"]),
            "detail":      f"Stuck {r['status']} for {r['hours_waiting']}h",
        })

    for r in mismatches:
        anomalies.append({
            "type":        "CHECKSUM_MISMATCH",
            "severity":    "CRITICAL",
            "file_name":   r["file_name"],
            "operator_id": r["operator_id"],
            "cdr_type":    str(r["cdr_type"]),
            "detail":      "Transfer corruption detected",
        })

    # All 5 divergence dimensions
    for dim_key, dim_list in divergences.items():
        for r in dim_list:
            # Map dimension key to anomaly_type ENUM value
            type_map = {
                "minutes":  "MINUTES_DIVERGENCE",
                "voice":    "VOICE_DIVERGENCE",
                "sms":      "SMS_DIVERGENCE",
                "data":     "DATA_DIVERGENCE",
                "recharge": "RECHARGE_DIVERGENCE",
            }
            anomalies.append({
                "type":        type_map.get(dim_key, "MINUTES_DIVERGENCE"),
                "severity":    r["severity"],
                "file_name":   r["file_name"],
                "operator_id": r["operator_id"],
                "cdr_type":    str(r["cdr_type"]),
                "dimension":   r["dimension"],
                "detail":      (f"db={r['db_value']:.3f} "
                                f"es={r['es_value']:.3f} "
                                f"delta={r['delta_pct']:.2f}%"),
            })

    for r in missing_rec:
        anomalies.append({
            "type":        "MISSING_RECORDS",
            "severity":    r["severity"],
            "file_name":   r["file_name"],
            "operator_id": r["operator_id"],
            "cdr_type":    str(r["cdr_type"]),
            "detail":      f"{r['missing']:,} records missing ({r['missing_pct']:.2f}%)",
        })

    # Q5a — daily file count vs baseline (operator-level anomaly, no file_id)
    for r in count_anomalies:
        anomalies.append({
            "type":        "FILE_MISSING",
            "severity":    r["severity"],
            "file_name":   None,
            "operator_id": r["operator_id"],
            "cdr_type":    r["cdr_type"],
            "detail":      (f"Daily count: {r['detail']} "
                            f"(delta={r['delta_pct']:+.1f}%)"),
        })

    # Q5b — pipeline funnel gaps (operator-level anomaly, no file_id)
    for r in funnel_anomalies:
        anomalies.append({
            "type":        "FILE_MISSING",
            "severity":    r["severity"],
            "file_name":   None,
            "operator_id": r["operator_id"],
            "cdr_type":    r["cdr_type"],
            "detail":      (f"Pipeline gap: received={r['received']} "
                            f"completed={r['completed']} gap={r['gap_pct']}% "
                            f"| {r['detail']}"),
        })

    # Q5c — files on disk not in DB
    for r in unregistered:
        anomalies.append({
            "type":        "FILE_MISSING",
            "severity":    "CRITICAL",
            "file_name":   r["file_name"],
            "operator_id": r["operator_id"],
            "cdr_type":    str(r["cdr_type"]),
            "detail":      f"File on disk but not in DB: {r['directory']}",
        })

    # CRITICAL first
    anomalies.sort(key=lambda x: 0 if x["severity"] == "CRITICAL" else 1)

    return {
        "generated_at": datetime.now().isoformat(),
        "status": overall_status,
        "summary": {
            "total_anomalies":  len(all_issues),
            "critical":         critical,
            "warnings":         warnings,
            "files_done_today": sum(r["done"] for r in summary) if summary else 0,
        },
        "anomalies": anomalies,
    }


def send_noc_notification(payload: dict, config: dict) -> bool:
    """
    Save noc_latest.json and optionally POST to the frontend webhook.

    noc_latest.json is always written — no configuration required.
    The webhook POST only fires if alerting.noc_webhook_url is set.
    """
    import json

    alert_cfg   = config.get("alerting", {})
    webhook_url = alert_cfg.get("noc_webhook_url", None)
    report_dir  = config.get("batch", {}).get("report_dir", "reports")

    json_path = Path(report_dir) / "noc_latest.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    log.info("[alerting] NOC payload saved: %s", json_path)

    if webhook_url:
        try:
            import urllib.request
            data = json.dumps(payload).encode("utf-8")
            req  = urllib.request.Request(
                webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                log.info("[alerting] NOC webhook responded: %s", resp.status)
            return True
        except Exception as exc:
            log.error("[alerting] NOC webhook failed: %s", exc)
            return False

    log.info("[alerting] No noc_webhook_url configured — JSON file only.")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# CLI test mode
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import json
    import yaml

    parser = argparse.ArgumentParser(description="Trafscan Alerting — test mode")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--test",   action="store_true",
                        help="Send a test email with fake data (requires email config)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    if args.test:
        fake_results = {
            "stuck": [{
                "file_id": "abc-123", "file_name": "TEST_CDR_20250423.gz",
                "operator_id": "atel", "cdr_type": "in",
                "status": "PENDING", "hours_waiting": 5.0, "severity": "CRITICAL",
            }],
            "mismatches":     [],
            "divergences": {
                "minutes":  [{
                    "file_id": "def-456", "file_name": "TEST_CDR_20250423_B.gz",
                    "operator_id": "orange", "cdr_type": "in", "dimension": "minutes",
                    "db_value": 1000.25, "es_value": 1050.10,
                    "delta_pct": 4.97, "severity": "CRITICAL",
                }],
                "voice":    [],
                "sms":      [],
                "data":     [],
                "recharge": [],
            },
            "missing_records": [],
            "unregistered":    [],
            "summary": [{
                "operator_id": "atel",  "cdr_type": "in",
                "total": 10, "done": 8, "pending": 1,
                "processing": 0, "mismatch": 1, "error": 0,
            }],
        }

        html = build_html_report(fake_results, config)
        with open("test_report.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("[test] HTML report preview saved to: test_report.html")

        noc = build_noc_payload(fake_results)
        print("[test] NOC payload:")
        print(json.dumps(noc, indent=2))

        print("\n[test] Attempting test email (requires smtp config in config.yaml)...")
        send_email_alert(fake_results, config, html)
        