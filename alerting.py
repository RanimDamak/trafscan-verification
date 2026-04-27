"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A  |  Layer 4: Alerting + HTML Report
────────────────────────────────────────────────────────────────────────────
Two responsibilities:
  1. Send an HTML email alert when CRITICAL or WARNING anomalies are found.
  2. Generate a self-contained HTML daily report (for archiving / NOC display).

Email configuration is in config.yaml under the `alerting` section.
The HTML report is saved to the path configured under `batch.report_dir`.

This module is called by batch.py : we do not run it directly.
It can also be imported and called standalone for testing:

    python alerting.py --config config.yaml --test
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
    No external CSS or JS dependencies — renders in any email client or browser.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = datetime.now().strftime("%Y-%m-%d")

    stuck        = results.get("stuck", [])
    mismatches   = results.get("mismatches", [])
    divergences  = results.get("divergences", [])
    missing_rec  = results.get("missing_records", [])
    unregistered = results.get("unregistered", [])
    summary      = results.get("summary", [])

    total_anomalies = len(stuck) + len(mismatches) + len(divergences) + len(missing_rec) + len(unregistered)
    critical = sum(1 for r in (stuck + mismatches + divergences + missing_rec + unregistered)
                   if r.get("severity") == "CRITICAL")
    warnings = total_anomalies - critical

    # ── Status banner color ───────────────────────────────────────────────────
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
        return f'<span style="background:{color};color:white;padding:2px 8px;border-radius:3px;font-size:12px;font-weight:bold;">{severity}</span>'

    def section_header(title: str, count: int, ok: bool) -> str:
        icon  = "✓" if ok else "✗"
        color = "#27ae60" if ok else ("#c0392b" if not ok else "#e67e22")
        return f"""
        <tr>
            <td colspan="99" style="background:#2c3e50;color:white;padding:10px 16px;font-weight:bold;font-size:14px;">
                <span style="color:{color};">{icon}</span>&nbsp; {title}
                {"" if ok else f'<span style="float:right;background:#c0392b;color:white;padding:1px 10px;border-radius:10px;">{count} issue(s)</span>'}
            </td>
        </tr>"""

    def ok_row(msg: str) -> str:
        return f'<tr><td colspan="99" style="padding:10px 16px;color:#27ae60;">✓ {msg}</td></tr>'

    # ── Q1: Stuck files ───────────────────────────────────────────────────────
    q1_ok = len(stuck) == 0
    q1_rows = ok_row("No stuck files.") if q1_ok else ""
    if not q1_ok:
        q1_rows = """
        <tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">
            <td style="padding:8px 12px;">File</td>
            <td style="padding:8px 12px;">Operator</td>
            <td style="padding:8px 12px;">Type</td>
            <td style="padding:8px 12px;">Status</td>
            <td style="padding:8px 12px;">Waiting</td>
            <td style="padding:8px 12px;">Severity</td>
        </tr>"""
        for r in stuck:
            q1_rows += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r['file_name']}</td>
            <td style="padding:8px 12px;">{r['operator_id']}</td>
            <td style="padding:8px 12px;">{r['cdr_type']}</td>
            <td style="padding:8px 12px;">{r['status']}</td>
            <td style="padding:8px 12px;">{r['hours_waiting']}h</td>
            <td style="padding:8px 12px;">{severity_badge(r['severity'])}</td>
        </tr>"""

    # ── Q2: Checksum mismatches ───────────────────────────────────────────────
    q2_ok = len(mismatches) == 0
    q2_rows = ok_row("All transferred files have matching checksums.") if q2_ok else ""
    if not q2_ok:
        q2_rows = """
        <tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">
            <td style="padding:8px 12px;">File</td>
            <td style="padding:8px 12px;">Operator</td>
            <td style="padding:8px 12px;">Type</td>
            <td style="padding:8px 12px;">Expected (first 16)</td>
            <td style="padding:8px 12px;">Received (first 16)</td>
            <td style="padding:8px 12px;">Severity</td>
        </tr>"""
        for r in mismatches:
            q2_rows += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r['file_name']}</td>
            <td style="padding:8px 12px;">{r['operator_id']}</td>
            <td style="padding:8px 12px;">{r['cdr_type']}</td>
            <td style="padding:8px 12px;font-family:monospace;font-size:11px;">{r['expected'][:16]}...</td>
            <td style="padding:8px 12px;font-family:monospace;font-size:11px;color:#c0392b;">{r['transferred'][:16]}...</td>
            <td style="padding:8px 12px;">{severity_badge(r['severity'])}</td>
        </tr>"""

    # ── Q3: Minutes divergence ────────────────────────────────────────────────
    threshold = config.get("batch", {}).get("minutes_divergence_pct", 0.5)
    q3_ok = len(divergences) == 0
    q3_rows = ok_row(f"No divergence above {threshold}% threshold.") if q3_ok else ""
    if not q3_ok:
        q3_rows = """
        <tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">
            <td style="padding:8px 12px;">File</td>
            <td style="padding:8px 12px;">Operator</td>
            <td style="padding:8px 12px;">DB minutes</td>
            <td style="padding:8px 12px;">ES minutes</td>
            <td style="padding:8px 12px;">Delta %</td>
            <td style="padding:8px 12px;">Severity</td>
        </tr>"""
        for r in divergences:
            q3_rows += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r['file_name']}</td>
            <td style="padding:8px 12px;">{r['operator_id']}</td>
            <td style="padding:8px 12px;">{r['db_minutes']:.3f}</td>
            <td style="padding:8px 12px;">{r['es_minutes']:.3f}</td>
            <td style="padding:8px 12px;color:#c0392b;font-weight:bold;">{r['delta_pct']:.2f}%</td>
            <td style="padding:8px 12px;">{severity_badge(r['severity'])}</td>
        </tr>"""

    # ── Q4: Missing records ───────────────────────────────────────────────────
    q4_ok = len(missing_rec) == 0
    q4_rows = ok_row("No missing records detected.") if q4_ok else ""
    if not q4_ok:
        q4_rows = """
        <tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">
            <td style="padding:8px 12px;">File</td>
            <td style="padding:8px 12px;">Operator</td>
            <td style="padding:8px 12px;">Expected</td>
            <td style="padding:8px 12px;">Processed</td>
            <td style="padding:8px 12px;">Missing</td>
            <td style="padding:8px 12px;">Missing %</td>
            <td style="padding:8px 12px;">Severity</td>
        </tr>"""
        for r in missing_rec:
            q4_rows += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r['file_name']}</td>
            <td style="padding:8px 12px;">{r['operator_id']}</td>
            <td style="padding:8px 12px;">{r['expected']:,}</td>
            <td style="padding:8px 12px;">{r['processed']:,}</td>
            <td style="padding:8px 12px;color:#c0392b;font-weight:bold;">{r['missing']:,}</td>
            <td style="padding:8px 12px;">{r['missing_pct']:.2f}%</td>
            <td style="padding:8px 12px;">{severity_badge(r['severity'])}</td>
        </tr>"""

    # ── Q5: Unregistered files ────────────────────────────────────────────────
    q5_ok = len(unregistered) == 0
    q5_rows = ok_row("All files on disk are registered in the DB.") if q5_ok else ""
    if not q5_ok:
        q5_rows = """
        <tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">
            <td style="padding:8px 12px;">File</td>
            <td style="padding:8px 12px;">Operator</td>
            <td style="padding:8px 12px;">Type</td>
            <td style="padding:8px 12px;">Directory</td>
            <td style="padding:8px 12px;">Severity</td>
        </tr>"""
        for r in unregistered:
            q5_rows += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:8px 12px;font-family:monospace;font-size:12px;">{r['file_name']}</td>
            <td style="padding:8px 12px;">{r['operator_id']}</td>
            <td style="padding:8px 12px;">{r['cdr_type']}</td>
            <td style="padding:8px 12px;font-family:monospace;font-size:11px;">{r['directory']}</td>
            <td style="padding:8px 12px;">{severity_badge(r['severity'])}</td>
        </tr>"""

    # ── Q6: Daily summary ─────────────────────────────────────────────────────
    if not summary:
        q6_rows = ok_row("No files received today.")
    else:
        q6_rows = """
        <tr style="background:#f5f5f5;font-weight:bold;font-size:12px;">
            <td style="padding:8px 12px;">Operator</td>
            <td style="padding:8px 12px;">Type</td>
            <td style="padding:8px 12px;text-align:right;">Total</td>
            <td style="padding:8px 12px;text-align:right;">Done</td>
            <td style="padding:8px 12px;text-align:right;">Pending</td>
            <td style="padding:8px 12px;text-align:right;">Mismatch</td>
            <td style="padding:8px 12px;text-align:right;">Error</td>
        </tr>"""
        for r in summary:
            mismatch_style = 'color:#c0392b;font-weight:bold;' if r['mismatch'] > 0 else ''
            error_style    = 'color:#c0392b;font-weight:bold;' if r['error'] > 0 else ''
            q6_rows += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:8px 12px;">{r['operator_id']}</td>
            <td style="padding:8px 12px;">{r['cdr_type']}</td>
            <td style="padding:8px 12px;text-align:right;">{r['total']}</td>
            <td style="padding:8px 12px;text-align:right;color:#27ae60;">{r['done']}</td>
            <td style="padding:8px 12px;text-align:right;">{r['pending']}</td>
            <td style="padding:8px 12px;text-align:right;{mismatch_style}">{r['mismatch']}</td>
            <td style="padding:8px 12px;text-align:right;{error_style}">{r['error']}</td>
        </tr>"""

    # ── Assemble full HTML ────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trafscan CDR Report — {date_str}</title>
</head>
<body style="margin:0;padding:0;font-family:Arial,sans-serif;background:#f0f2f5;color:#2c3e50;">

<div style="max-width:900px;margin:30px auto;background:white;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.1);">

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
      <div style="font-size:32px;font-weight:bold;color:#27ae60;">{sum(r['done'] for r in summary) if summary else 0}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Files Done Today</div>
    </div>
  </div>

  <!-- Check sections -->
  <div style="padding:0;">
    <table style="width:100%;border-collapse:collapse;">
      {section_header("Q1 - Stuck files (PENDING / PROCESSING too long)", len(stuck), q1_ok)}
      {q1_rows}
      {section_header("Q2 - Checksum mismatches (transfer corruption)", len(mismatches), q2_ok)}
      {q2_rows}
      {section_header("Q3 - Minutes divergence DB vs ElasticSearch", len(divergences), q3_ok)}
      {q3_rows}
      {section_header("Q4 - Missing records", len(missing_rec), q4_ok)}
      {q4_rows}
      {section_header("Q5 - Files on disk not registered in DB", len(unregistered), q5_ok)}
      {q5_rows}
      {section_header("Q6 - Daily summary", 0, True)}
      {q6_rows}
    </table>
  </div>

  <!-- NOC Integration Hook -->
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
    Returns True on success, False on failure.

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
          alert_on:      [CRITICAL, WARNING]   # which severities trigger an email
    """
    alert_cfg = config.get("alerting", {})

    smtp_host     = alert_cfg.get("smtp_host")
    smtp_port     = int(alert_cfg.get("smtp_port", 587))
    smtp_user     = alert_cfg.get("smtp_user")
    smtp_password = alert_cfg.get("smtp_password")
    smtp_tls      = alert_cfg.get("smtp_tls", True)
    from_email    = alert_cfg.get("from_email", smtp_user)
    to_emails     = alert_cfg.get("to_emails", [])
    alert_on      = alert_cfg.get("alert_on", ["CRITICAL"])

    if not smtp_host or not to_emails:
        log.warning("[alerting] Email not configured. Set smtp_host and to_emails in config.yaml.")
        return False

    # Decide whether to send based on what was found
    stuck        = results.get("stuck", [])
    mismatches   = results.get("mismatches", [])
    divergences  = results.get("divergences", [])
    missing_rec  = results.get("missing_records", [])
    unregistered = results.get("unregistered", [])
    all_issues   = stuck + mismatches + divergences + missing_rec + unregistered

    has_critical = any(r.get("severity") == "CRITICAL" for r in all_issues)
    has_warning  = any(r.get("severity") == "WARNING"  for r in all_issues)

    should_send = (
        ("CRITICAL" in alert_on and has_critical) or
        ("WARNING"  in alert_on and has_warning)
    )

    if not should_send:
        log.info("[alerting] No anomalies matching alert_on=%s — email not sent.", alert_on)
        return True

    # Build subject line
    total    = len(all_issues)
    critical = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    date_str = datetime.now().strftime("%Y-%m-%d")

    if has_critical:
        subject = f"[TRAFSCAN] ⚠ CRITICAL — {critical} anomaly(ies) detected — {date_str}"
    else:
        subject = f"[TRAFSCAN] WARNING — {total} anomaly(ies) detected — {date_str}"

    # Build email
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_email
    msg["To"]      = ", ".join(to_emails)

    # Plain text fallback (for clients that don't render HTML)
    plain = (
        f"TRAFSCAN CDR Registry — Daily Report\n"
        f"Date: {date_str}\n\n"
        f"Total anomalies: {total}\n"
        f"Critical: {critical}\n"
        f"Warnings: {total - critical}\n\n"
        f"Please view the HTML version of this email for full details.\n"
        f"Or check the registry directly:\n"
        f"  SELECT * FROM anomaly_log WHERE detected_at >= CURRENT_DATE ORDER BY severity DESC;\n"
    )

    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html_report, "html"))

    # Send
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
# NOC notification template
# ─────────────────────────────────────────────────────────────────────────────

def build_noc_payload(results: dict) -> dict:
    """
    Build a JSON-serializable payload for the NOC/frontend notification system.

    This is the integration contract between Trafscan Solution A and
    the Trafscan administration frontend (NOC panel).

    The frontend team should:
      1. Expose a POST endpoint (e.g. POST /api/noc/alerts)
      2. Configure its URL in config.yaml under alerting.noc_webhook_url
      3. The batch will POST this payload after every run

    The payload is also saved as noc_latest.json in the report directory
    so the frontend can poll it via a simple GET instead of receiving POSTs.

    Payload structure:
    {
      "generated_at": "2025-04-23T02:00:00",
      "status": "CRITICAL" | "WARNING" | "OK",
      "summary": {
        "total_anomalies": 3,
        "critical": 2,
        "warnings": 1,
        "files_done_today": 42
      },
      "anomalies": [
        {
          "type": "TIMEOUT",
          "severity": "CRITICAL",
          "file_name": "CDR_20250423.gz",
          "operator_id": "atel",
          "cdr_type": "msc",
          "detail": "Stuck PENDING for 17.0h"
        },
        ...
      ]
    }
    """
    stuck        = results.get("stuck", [])
    mismatches   = results.get("mismatches", [])
    divergences  = results.get("divergences", [])
    missing_rec  = results.get("missing_records", [])
    unregistered = results.get("unregistered", [])
    summary      = results.get("summary", [])

    all_issues = stuck + mismatches + divergences + missing_rec + unregistered
    critical   = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    warnings   = len(all_issues) - critical

    if critical > 0:
        overall_status = "CRITICAL"
    elif warnings > 0:
        overall_status = "WARNING"
    else:
        overall_status = "OK"

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
            "detail":      f"Transfer corruption detected",
        })

    for r in divergences:
        anomalies.append({
            "type":        "MINUTES_DIVERGENCE",
            "severity":    r["severity"],
            "file_name":   r["file_name"],
            "operator_id": r["operator_id"],
            "cdr_type":    str(r["cdr_type"]),
            "detail":      f"DB={r['db_minutes']:.2f} ES={r['es_minutes']:.2f} delta={r['delta_pct']:.2f}%",
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

    for r in unregistered:
        anomalies.append({
            "type":        "FILE_MISSING",
            "severity":    "CRITICAL",
            "file_name":   r["file_name"],
            "operator_id": r["operator_id"],
            "cdr_type":    str(r["cdr_type"]),
            "detail":      f"File on disk but not in DB: {r['directory']}",
        })

    return {
        "generated_at": datetime.now().isoformat(),
        "status":        overall_status,
        "summary": {
            "total_anomalies":  len(all_issues),
            "critical":         critical,
            "warnings":         warnings,
            "files_done_today": sum(r["done"] for r in summary) if summary else 0,
        },
        "anomalies": sorted(anomalies, key=lambda x: 0 if x["severity"] == "CRITICAL" else 1),
    }


def send_noc_notification(payload: dict, config: dict) -> bool:
    """
    POST the NOC payload to the frontend webhook URL (if configured).
    Also saves noc_latest.json to report_dir for polling-based frontends.
    Falls back gracefully if not configured.
    """
    import json

    alert_cfg   = config.get("alerting", {})
    webhook_url = alert_cfg.get("noc_webhook_url", None)
    report_dir  = config.get("batch", {}).get("report_dir", "reports")

    # Always save the JSON file for polling
    json_path = Path(report_dir) / "noc_latest.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    log.info("[alerting] NOC payload saved: %s", json_path)

    # POST to webhook if configured
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
    import yaml

    parser = argparse.ArgumentParser(description="Trafscan Alerting — test mode")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--test",   action="store_true",
                        help="Send a test email with fake data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    if args.test:
        # Fake results to test the email template
        fake_results = {
            "stuck": [{
                "file_id": "abc-123", "file_name": "TEST_CDR_20250423.gz",
                "operator_id": "atel", "cdr_type": "in",
                "status": "PENDING", "hours_waiting": 5.0, "severity": "CRITICAL"
            }],
            "mismatches":      [],
            "divergences":     [],
            "missing_records": [],
            "unregistered":    [],
            "summary": [{"operator_id": "atel", "cdr_type": "in",
                          "total": 10, "done": 9, "pending": 1,
                          "processing": 0, "mismatch": 0, "error": 0}],
        }
        html  = build_html_report(fake_results, config)
        noc   = build_noc_payload(fake_results)

        print("[test] HTML report preview saved to: test_report.html")
        with open("test_report.html", "w", encoding="utf-8") as f:
            f.write(html)

        import json
        print("[test] NOC payload:")
        print(json.dumps(noc, indent=2))

        print("\n[test] Sending test email...")
        send_email_alert(fake_results, config, html)
