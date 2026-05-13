"""
────────────────────────────────────────────────────────────────────────────
TRAFSCAN - Solution A | Layer 4: Alerting + HTML Report
────────────────────────────────────────────────────────────────────────────

Two responsibilities:
  1. Send an HTML email alert when CRITICAL or WARNING anomalies are found.
  2. Generate a self-contained HTML daily report (for archiving / NOC display).

New in v3.1:
  - Q3b–Q3e sections (voice / SMS / data / recharge divergence)
  - Q5a section (file count baseline)
  - Q5b section (pipeline funnel)
  - Q_PT section (processing time anomalies)
  - Q7  section (forfait daily count)
  - Updated NOC payload with all new anomaly types
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
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _severity_badge(severity: str) -> str:
    color = "#c0392b" if severity == "CRITICAL" else "#e67e22"
    return (f'<span style="background:{color};color:white;padding:2px 8px;'
            f'border-radius:3px;font-size:12px;font-weight:bold;">{severity}</span>')


def _section_header(title: str, count: int, ok: bool) -> str:
    icon = "✓" if ok else "✗"
    color = "#27ae60" if ok else "#c0392b"
    badge = (f'<span style="float:right;background:#c0392b;color:white;'
             f'padding:1px 10px;border-radius:10px;">{count} issue(s)</span>'
             if not ok else "")
    return f"""
      <tr>
        <td colspan="99" style="background:#2c3e50;color:white;padding:10px 16px;
                                font-weight:bold;font-size:14px;">
          <span style="color:{color};">{icon}</span>&nbsp; {title}{badge}
        </td>
      </tr>"""


def _ok_row(msg: str) -> str:
    return f'<tr><td colspan="99" style="padding:10px 16px;color:#27ae60;">✓ {msg}</td></tr>'


def _header_row(*cols) -> str:
    cells = "".join(
        f'<td style="padding:8px 12px;font-weight:bold;font-size:12px;">{c}</td>'
        for c in cols
    )
    return f'<tr style="background:#f5f5f5;">{cells}</tr>'


def _data_row(*cells_html) -> str:
    cells = "".join(
        f'<td style="padding:8px 12px;">{c}</td>' for c in cells_html
    )
    return f'<tr style="border-bottom:1px solid #eee;">{cells}</tr>'


def _mono(val) -> str:
    return f'<span style="font-family:monospace;font-size:12px;">{val}</span>'


# ─────────────────────────────────────────────────────────────────────────────
# HTML Report Generator
# ─────────────────────────────────────────────────────────────────────────────

def build_html_report(results: dict, config: dict) -> str:
    """
    Build a fully self-contained HTML report from batch results.
    No external CSS or JS dependencies — renders in any email client or browser.
    """
    now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = datetime.now().strftime("%Y-%m-%d")

    stuck          = results.get("stuck", [])
    mismatches     = results.get("mismatches", [])
    div_minutes    = results.get("divergences_minutes", [])
    div_voice      = results.get("divergences_voice", [])
    div_sms        = results.get("divergences_sms", [])
    div_data       = results.get("divergences_data", [])
    div_recharge   = results.get("divergences_recharge", [])
    missing_rec    = results.get("missing_records", [])
    baseline_iss   = results.get("baseline_issues", [])
    funnel_iss     = results.get("funnel_issues", [])
    unregistered   = results.get("unregistered", [])
    summary        = results.get("summary", [])
    pt_issues      = results.get("pt_issues", [])
    forfait_issues = results.get("forfait_issues", [])

    all_issues = (stuck + mismatches + div_minutes + div_voice + div_sms
                  + div_data + div_recharge + missing_rec + baseline_iss
                  + funnel_iss + unregistered + pt_issues + forfait_issues)

    total_anomalies = len(all_issues)
    critical  = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    warnings_ = total_anomalies - critical

    threshold = config.get("batch", {}).get("minutes_divergence_pct", 0.5)

    if critical > 0:
        banner_color = "#c0392b"
        banner_text  = f"⚠ ACTION REQUIRED — {critical} CRITICAL anomaly(ies) detected"
    elif warnings_ > 0:
        banner_color = "#e67e22"
        banner_text  = f"⚠ {warnings_} WARNING(s) detected — review recommended"
    else:
        banner_color = "#27ae60"
        banner_text  = "✓ All checks passed — system healthy"

    # ── Q1: Stuck files ───────────────────────────────────────────────────────
    q1_ok = not stuck
    if q1_ok:
        q1_rows = _ok_row("No stuck files.")
    else:
        q1_rows = _header_row("File", "Operator", "Type", "Status", "Waiting", "Severity")
        for r in stuck:
            q1_rows += _data_row(
                _mono(r['file_name']), r['operator_id'], r['cdr_type'],
                r['status'], f"{r['hours_waiting']}h",
                _severity_badge(r['severity']),
            )

    # ── Q2: Checksum mismatches ───────────────────────────────────────────────
    q2_ok = not mismatches
    if q2_ok:
        q2_rows = _ok_row("All transferred files have matching checksums.")
    else:
        q2_rows = _header_row("File", "Operator", "Type", "Expected (first 16)", "Received (first 16)", "Severity")
        for r in mismatches:
            q2_rows += _data_row(
                _mono(r['file_name']), r['operator_id'], r['cdr_type'],
                _mono(r['expected'][:16] + "..."),
                f'<span style="color:#c0392b;">{_mono(r["transferred"][:16]+"...")}</span>',
                _severity_badge(r['severity']),
            )

    # ── Q3a–Q3e: Divergence sections ─────────────────────────────────────────
    def _divergence_rows(divs: list, label: str) -> str:
        if not divs:
            return _ok_row(f"No {label} divergence above {threshold}% threshold.")
        rows = _header_row("File", "Operator", "Type", f"DB {label}", f"ES {label}", "Delta %", "Severity")
        for r in divs:
            rows += _data_row(
                _mono(r['file_name']), r['operator_id'], r['cdr_type'],
                f"{r['db_value']:.3f}", f"{r['es_value']:.3f}",
                f'<span style="color:#c0392b;font-weight:bold;">{r["delta_pct"]:.2f}%</span>',
                _severity_badge(r['severity']),
            )
        return rows

    q3a_rows = _divergence_rows(div_minutes,  "minutes")
    q3b_rows = _divergence_rows(div_voice,    "voice")
    q3c_rows = _divergence_rows(div_sms,      "SMS")
    q3d_rows = _divergence_rows(div_data,     "data")
    q3e_rows = _divergence_rows(div_recharge, "recharge")

    # ── Q4: Missing records ───────────────────────────────────────────────────
    q4_ok = not missing_rec
    if q4_ok:
        q4_rows = _ok_row("No missing records detected.")
    else:
        q4_rows = _header_row("File", "Operator", "Type", "Expected", "Processed", "Missing", "Missing %", "Severity")
        for r in missing_rec:
            q4_rows += _data_row(
                _mono(r['file_name']), r['operator_id'], r['cdr_type'],
                f"{r['expected']:,}", f"{r['processed']:,}",
                f'<span style="color:#c0392b;font-weight:bold;">{r["missing"]:,}</span>',
                f"{r['missing_pct']:.2f}%",
                _severity_badge(r['severity']),
            )

    # ── Q5a: Baseline ─────────────────────────────────────────────────────────
    q5a_ok = not baseline_iss
    if q5a_ok:
        q5a_rows = _ok_row("All operator/type file counts within baseline tolerance.")
    else:
        q5a_rows = _header_row("Operator", "Type", "Today", "Baseline", "Detail", "Severity")
        for r in baseline_iss:
            q5a_rows += _data_row(
                r['operator_id'], r['cdr_type'],
                str(r['actual']), str(r['baseline']),
                r['detail'], _severity_badge(r['severity']),
            )

    # ── Q5b: Pipeline funnel ──────────────────────────────────────────────────
    q5b_ok = not funnel_iss
    if q5b_ok:
        q5b_rows = _ok_row("Pipeline funnel healthy — all files progressing normally.")
    else:
        q5b_rows = _header_row("Operator", "Type", "Total", "Completed", "Still Pending", "Pending %", "Severity")
        for r in funnel_iss:
            q5b_rows += _data_row(
                r['operator_id'], r['cdr_type'],
                str(r['total']), str(r['completed']),
                f'<span style="color:#c0392b;">{r["still_pending"]}</span>',
                f"{r['pending_pct']:.1f}%",
                _severity_badge(r['severity']),
            )

    # ── Q5c: Unregistered files ───────────────────────────────────────────────
    q5c_ok = not unregistered
    if q5c_ok:
        q5c_rows = _ok_row("All files on disk are registered in the DB.")
    else:
        q5c_rows = _header_row("File", "Operator", "Type", "Directory", "Severity")
        for r in unregistered:
            q5c_rows += _data_row(
                _mono(r['file_name']), r['operator_id'], r['cdr_type'],
                _mono(r['directory']), _severity_badge(r['severity']),
            )

    # ── Q6: Daily summary ─────────────────────────────────────────────────────
    if not summary:
        q6_rows = _ok_row("No files received today.")
    else:
        q6_rows = _header_row("Operator", "Type", "Total", "Done", "Pending", "Mismatch", "Error")
        for r in summary:
            mm_style = 'color:#c0392b;font-weight:bold;' if r['mismatch'] > 0 else 'color:#27ae60;'
            er_style = 'color:#c0392b;font-weight:bold;' if r['error'] > 0 else 'color:#27ae60;'
            q6_rows += _data_row(
                r['operator_id'], str(r['cdr_type']),
                str(r['total']),
                f'<span style="color:#27ae60;">{r["done"]}</span>',
                str(r['pending']),
                f'<span style="{mm_style}">{r["mismatch"]}</span>',
                f'<span style="{er_style}">{r["error"]}</span>',
            )

    # ── Q_PT: Processing time ─────────────────────────────────────────────────
    qpt_ok = not pt_issues
    if qpt_ok:
        qpt_rows = _ok_row("All processing times within expected range.")
    else:
        qpt_rows = _header_row("File", "Operator", "Type", "Duration", "Median (7d)", "Ratio", "Mode", "Severity")
        for r in pt_issues:
            dur_min  = f"{r['duration_s']/60:.1f}min"
            med_min  = f"{r['median_s']/60:.1f}min"
            mode_lbl = "Java hooks" if r.get("ts_mode") == "java_hooks" else "fallback"
            mode_col = ("#888" if mode_lbl == "fallback" else "#2c3e50")
            qpt_rows += _data_row(
                _mono(r['file_name']), r['operator_id'], r['cdr_type'],
                dur_min, med_min,
                f'<span style="color:#c0392b;font-weight:bold;">{r["ratio"]:.1f}×</span>',
                f'<span style="color:{mode_col};font-size:11px;">{mode_lbl}</span>',
                _severity_badge(r['severity']),
            )

    # ── Q7: Forfait ───────────────────────────────────────────────────────────
    q7_ok = not forfait_issues
    if q7_ok:
        q7_rows = _ok_row("Forfait daily count within expected range.")
    else:
        q7_rows = _header_row("Today (known)", "Today (unknown)", "Today (total)",
                               "7d Median", "Drop %", "ES total", "Severity")
        for r in forfait_issues:
            es_cell = (f'<span style="color:#888;font-size:11px;">TODO (confirm with team)</span>'
                       if r.get("es_total") is None
                       else f"{r['es_total']:,.0f}")
            q7_rows += _data_row(
                f"{r['today_known']:,.0f}",
                f"{r['today_unknown']:,.0f}",
                f'<span style="color:#c0392b;font-weight:bold;">{r["today_total"]:,.0f}</span>',
                f"{r['median_total']:,.0f}",
                f'<span style="color:#c0392b;">{r["drop_pct"]:.1f}%</span>',
                es_cell,
                _severity_badge(r['severity']),
            )

    # ── Assemble ──────────────────────────────────────────────────────────────
    files_done_today = sum(r['done'] for r in summary) if summary else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Trafscan CDR Report — {date_str}</title>
</head>
<body style="margin:0;padding:0;font-family:Arial,sans-serif;background:#f0f2f5;color:#2c3e50;">
<div style="max-width:960px;margin:30px auto;background:white;border-radius:8px;
            overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.1);">

  <!-- Header -->
  <div style="background:#2c3e50;padding:24px 30px;">
    <h1 style="margin:0;color:white;font-size:20px;">TRAFSCAN — CDR Registry</h1>
    <p style="margin:6px 0 0;color:#95a5a6;font-size:13px;">
      Daily Reconciliation Report v3.1 &nbsp;|&nbsp; {now}
    </p>
  </div>

  <!-- Status Banner -->
  <div style="background:{banner_color};padding:14px 30px;">
    <p style="margin:0;color:white;font-size:15px;font-weight:bold;">{banner_text}</p>
  </div>

  <!-- Summary Cards -->
  <div style="display:flex;border-bottom:2px solid #eee;">
    <div style="flex:1;padding:20px;text-align:center;border-right:1px solid #eee;">
      <div style="font-size:32px;font-weight:bold;color:#2c3e50;">{total_anomalies}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Total Anomalies</div>
    </div>
    <div style="flex:1;padding:20px;text-align:center;border-right:1px solid #eee;">
      <div style="font-size:32px;font-weight:bold;color:#c0392b;">{critical}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Critical</div>
    </div>
    <div style="flex:1;padding:20px;text-align:center;border-right:1px solid #eee;">
      <div style="font-size:32px;font-weight:bold;color:#e67e22;">{warnings_}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Warnings</div>
    </div>
    <div style="flex:1;padding:20px;text-align:center;">
      <div style="font-size:32px;font-weight:bold;color:#27ae60;">{files_done_today}</div>
      <div style="font-size:12px;color:#7f8c8d;margin-top:4px;">Files Done Today</div>
    </div>
  </div>

  <!-- Check sections -->
  <div style="padding:0;">
    <table style="width:100%;border-collapse:collapse;">

      {_section_header("Q1 — Stuck files (PENDING / PROCESSING too long)", len(stuck), q1_ok)}
      {q1_rows}

      {_section_header("Q2 — Checksum mismatches (transfer corruption)", len(mismatches), q2_ok)}
      {q2_rows}

      {_section_header("Q3a — Minutes divergence DB vs ElasticSearch", len(div_minutes), not div_minutes)}
      {q3a_rows}

      {_section_header("Q3b — Voice traffic divergence DB vs ElasticSearch", len(div_voice), not div_voice)}
      {q3b_rows}

      {_section_header("Q3c — SMS traffic divergence DB vs ElasticSearch", len(div_sms), not div_sms)}
      {q3c_rows}

      {_section_header("Q3d — Data traffic divergence DB vs ElasticSearch", len(div_data), not div_data)}
      {q3d_rows}

      {_section_header("Q3e — Recharge divergence DB vs ElasticSearch", len(div_recharge), not div_recharge)}
      {q3e_rows}

      {_section_header("Q4 — Missing records", len(missing_rec), q4_ok)}
      {q4_rows}

      {_section_header("Q5a — File count baseline check", len(baseline_iss), q5a_ok)}
      {q5a_rows}

      {_section_header("Q5b — Pipeline funnel check", len(funnel_iss), q5b_ok)}
      {q5b_rows}

      {_section_header("Q5c — Files on disk not registered in DB", len(unregistered), q5c_ok)}
      {q5c_rows}

      {_section_header("Q6 — Daily summary", 0, True)}
      {q6_rows}

      {_section_header("Q_PT — Processing time anomalies (vs 7-day median)", len(pt_issues), qpt_ok)}
      {qpt_rows}

      {_section_header("Q7 — Forfait daily count (daily_state_forfait + daily_state_unknown_forfait)", len(forfait_issues), q7_ok)}
      {q7_rows}

    </table>
  </div>

  <!-- Footer -->
  <div style="background:#f8f9fa;border-top:2px solid #eee;padding:16px 24px;">
    <p style="margin:0;font-size:11px;color:#95a5a6;">
      Generated by Trafscan Solution A v3.1 &nbsp;|&nbsp; {now} &nbsp;|&nbsp;
      <strong style="color:{'#c0392b' if critical > 0 else '#27ae60'};">
        Exit code: {'1 (CRITICAL)' if critical > 0 else '0 (OK)'}
      </strong>
      &nbsp;|&nbsp;
      <span style="color:#888;">Q7 Part 2 ES comparison: pending team confirmation</span>
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
    Send an HTML email alert with the full report embedded.
    Returns True on success, False on failure (never raises).

    config.yaml alerting section:
      alerting:
        smtp_host:     smtp.example.com
        smtp_port:     587
        smtp_user:     trafscan@example.com
        smtp_password: your_app_password
        smtp_tls:      true
        from_email:    trafscan@example.com
        to_emails:
          - ops@example.com
          - noc@example.com
        alert_on: [CRITICAL, WARNING]
    """
    alert_cfg    = config.get("alerting", {})
    smtp_host    = alert_cfg.get("smtp_host")
    smtp_port    = int(alert_cfg.get("smtp_port", 587))
    smtp_user    = alert_cfg.get("smtp_user")
    smtp_password = alert_cfg.get("smtp_password")
    smtp_tls     = alert_cfg.get("smtp_tls", True)
    from_email   = alert_cfg.get("from_email", smtp_user)
    to_emails    = alert_cfg.get("to_emails", [])
    alert_on     = alert_cfg.get("alert_on", ["CRITICAL"])

    if not smtp_host or not to_emails:
        log.warning("[alerting] Email not configured. Set smtp_host and to_emails in config.yaml.")
        return False

    all_issues = []
    for key in ("stuck", "mismatches", "divergences_minutes", "divergences_voice",
                "divergences_sms", "divergences_data", "divergences_recharge",
                "missing_records", "baseline_issues", "funnel_issues",
                "unregistered", "pt_issues", "forfait_issues"):
        all_issues.extend(results.get(key, []))

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

    if has_critical:
        subject = f"[TRAFSCAN] ⚠ CRITICAL — {critical} anomaly(ies) detected — {date_str}"
    else:
        subject = f"[TRAFSCAN] WARNING — {total} anomaly(ies) detected — {date_str}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_email
    msg["To"]      = ", ".join(to_emails)

    plain = (
        f"TRAFSCAN CDR Registry — Daily Report v3.1\n"
        f"Date: {date_str}\n\n"
        f"Total anomalies: {total}\n"
        f"Critical: {critical}\n"
        f"Warnings: {total - critical}\n\n"
        f"Please view the HTML version for full details.\n"
        f"Direct DB query:\n"
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
# NOC notification payload
# ─────────────────────────────────────────────────────────────────────────────

def build_noc_payload(results: dict) -> dict:
    """
    Build a JSON-serializable payload for the NOC / frontend notification.

    The frontend team should POST to this endpoint after each batch run, or
    poll noc_latest.json from the report directory.

    Payload structure (v3.1):
    {
      "generated_at": "2025-05-01T02:00:00",
      "version": "3.1",
      "status": "CRITICAL" | "WARNING" | "OK",
      "summary": {
        "total_anomalies": N,
        "critical": N,
        "warnings": N,
        "files_done_today": N
      },
      "anomalies": [
        {
          "type": "TIMEOUT" | "CHECKSUM_MISMATCH" | "MINUTES_DIVERGENCE" |
                  "VOICE_DIVERGENCE" | "SMS_DIVERGENCE" | "DATA_DIVERGENCE" |
                  "RECHARGE_DIVERGENCE" | "MISSING_RECORDS" | "FILE_MISSING" |
                  "PROCESSING_TIME_ANOMALY" | "FORFAIT_COUNT_DROP",
          "severity": "CRITICAL" | "WARNING",
          "file_name": "...",   (null for Q5a/Q5b/Q7)
          "operator_id": "...",
          "cdr_type": "...",
          "detail": "human-readable description"
        }, ...
      ],
      "forfait": {                   (null if Q7 had no issue)
        "today_known": N,
        "today_unknown": N,
        "today_total": N,
        "median_total": N,
        "drop_pct": N,
        "es_total": null             (TODO: pending team confirmation)
      }
    }
    """
    stuck          = results.get("stuck", [])
    mismatches     = results.get("mismatches", [])
    div_minutes    = results.get("divergences_minutes", [])
    div_voice      = results.get("divergences_voice", [])
    div_sms        = results.get("divergences_sms", [])
    div_data       = results.get("divergences_data", [])
    div_recharge   = results.get("divergences_recharge", [])
    missing_rec    = results.get("missing_records", [])
    baseline_iss   = results.get("baseline_issues", [])
    funnel_iss     = results.get("funnel_issues", [])
    unregistered   = results.get("unregistered", [])
    summary        = results.get("summary", [])
    pt_issues      = results.get("pt_issues", [])
    forfait_issues = results.get("forfait_issues", [])

    all_issues = (stuck + mismatches + div_minutes + div_voice + div_sms
                  + div_data + div_recharge + missing_rec + baseline_iss
                  + funnel_iss + unregistered + pt_issues + forfait_issues)

    critical  = sum(1 for r in all_issues if r.get("severity") == "CRITICAL")
    warnings_ = len(all_issues) - critical
    overall   = "CRITICAL" if critical > 0 else ("WARNING" if warnings_ > 0 else "OK")

    anomalies = []

    for r in stuck:
        anomalies.append({"type": "TIMEOUT", "severity": r["severity"],
                           "file_name": r["file_name"], "operator_id": r["operator_id"],
                           "cdr_type": str(r["cdr_type"]),
                           "detail": f"Stuck {r['status']} for {r['hours_waiting']}h"})

    for r in mismatches:
        anomalies.append({"type": "CHECKSUM_MISMATCH", "severity": "CRITICAL",
                           "file_name": r["file_name"], "operator_id": r["operator_id"],
                           "cdr_type": str(r["cdr_type"]),
                           "detail": "Transfer corruption detected"})

    for label, divs in [
        ("MINUTES_DIVERGENCE",  div_minutes),
        ("VOICE_DIVERGENCE",    div_voice),
        ("SMS_DIVERGENCE",      div_sms),
        ("DATA_DIVERGENCE",     div_data),
        ("RECHARGE_DIVERGENCE", div_recharge),
    ]:
        for r in divs:
            anomalies.append({"type": label, "severity": r["severity"],
                               "file_name": r["file_name"], "operator_id": r["operator_id"],
                               "cdr_type": str(r["cdr_type"]),
                               "detail": f"db={r['db_value']:.3f} es={r['es_value']:.3f} delta={r['delta_pct']:.2f}%"})

    for r in missing_rec:
        anomalies.append({"type": "MISSING_RECORDS", "severity": r["severity"],
                           "file_name": r["file_name"], "operator_id": r["operator_id"],
                           "cdr_type": str(r["cdr_type"]),
                           "detail": f"{r['missing']:,} records missing ({r['missing_pct']:.2f}%)"})

    for r in baseline_iss:
        anomalies.append({"type": "FILE_MISSING", "severity": r["severity"],
                           "file_name": None, "operator_id": r["operator_id"],
                           "cdr_type": r["cdr_type"], "detail": r["detail"]})

    for r in funnel_iss:
        anomalies.append({"type": "TIMEOUT", "severity": r["severity"],
                           "file_name": None, "operator_id": r["operator_id"],
                           "cdr_type": r["cdr_type"], "detail": r["detail"]})

    for r in unregistered:
        anomalies.append({"type": "FILE_MISSING", "severity": "CRITICAL",
                           "file_name": r["file_name"], "operator_id": r["operator_id"],
                           "cdr_type": r["cdr_type"],
                           "detail": f"File on disk but not in DB: {r['directory']}"})

    for r in pt_issues:
        anomalies.append({"type": "PROCESSING_TIME_ANOMALY", "severity": r["severity"],
                           "file_name": r["file_name"], "operator_id": r["operator_id"],
                           "cdr_type": r["cdr_type"],
                           "detail": r["detail"]})

    for r in forfait_issues:
        anomalies.append({"type": "FORFAIT_COUNT_DROP", "severity": r["severity"],
                           "file_name": None, "operator_id": "all", "cdr_type": "forfait",
                           "detail": r["detail"]})

    forfait_summary = None
    if forfait_issues:
        r = forfait_issues[0]
        forfait_summary = {
            "today_known":    r["today_known"],
            "today_unknown":  r["today_unknown"],
            "today_total":    r["today_total"],
            "median_total":   r["median_total"],
            "drop_pct":       r["drop_pct"],
            "es_total":       r.get("es_total"),  # None until team confirms ES field
        }

    return {
        "generated_at": datetime.now().isoformat(),
        "version": "3.1",
        "status": overall,
        "summary": {
            "total_anomalies": len(all_issues),
            "critical": critical,
            "warnings": warnings_,
            "files_done_today": sum(r["done"] for r in summary) if summary else 0,
        },
        "anomalies": sorted(anomalies, key=lambda x: 0 if x["severity"] == "CRITICAL" else 1),
        "forfait": forfait_summary,
    }


def send_noc_notification(payload: dict, config: dict) -> bool:
    """
    Save noc_latest.json and optionally POST to webhook URL.
    Falls back gracefully if not configured.
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
                webhook_url, data=data,
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

    parser = argparse.ArgumentParser(description="Trafscan Alerting v3.1 — test mode")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--test", action="store_true",
                        help="Generate a test report with fake data (does not send email)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    if args.test:
        fake_results = {
            "stuck": [{"file_id": "aaa-1", "file_name": "CDR_20250501_orange_in.gz",
                        "operator_id": "orange", "cdr_type": "in",
                        "status": "PENDING", "hours_waiting": 4.5, "severity": "CRITICAL"}],
            "mismatches": [],
            "divergences_minutes":  [],
            "divergences_voice":    [{"file_id": "bbb-2", "file_name": "CDR_20250501_atel_msc.gz",
                                       "operator_id": "atel", "cdr_type": "msc",
                                       "db_value": 1000.0, "es_value": 850.0, "delta_pct": 15.0,
                                       "dimension": "VOICE_DIVERGENCE", "severity": "CRITICAL"}],
            "divergences_sms":      [],
            "divergences_data":     [],
            "divergences_recharge": [],
            "missing_records": [],
            "baseline_issues": [],
            "funnel_issues": [],
            "unregistered": [],
            "summary": [{"operator_id": "orange", "cdr_type": "in",
                          "total": 12, "done": 11, "pending": 1,
                          "processing": 0, "mismatch": 0, "error": 0}],
            "pt_issues": [{"file_id": "ccc-3", "file_name": "CDR_20250501_moov_pgw.gz",
                            "operator_id": "moov", "cdr_type": "pgw",
                            "duration_s": 7200, "median_s": 600, "ratio": 12.0,
                            "ts_mode": "fallback_updated_at", "severity": "CRITICAL",
                            "detail": "Duration 120.0min = 12.0× median (10.0min over 45 samples, 7d)"}],
            "forfait_issues": [{"today_total": 0.0, "today_known": 0.0, "today_unknown": 0.0,
                                  "median_total": 85000.0, "days_with_data": 7,
                                  "drop_pct": 100.0, "es_total": None,
                                  "detail": "Today: 0 forfaits — median over 7 days is 85,000.",
                                  "severity": "CRITICAL"}],
        }

        html = build_html_report(fake_results, config)
        noc  = build_noc_payload(fake_results)

        report_dir  = config.get("batch", {}).get("report_dir", "reports")
        report_path = Path(report_dir) / "test_report_v31.html"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[test] HTML report saved to: {report_path.resolve()}")
        print(f"[test] Open in browser: start {report_path.resolve()}")
        print("[test] NOC payload:")
        print(json.dumps(noc, indent=2))