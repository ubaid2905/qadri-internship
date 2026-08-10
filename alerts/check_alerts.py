"""
Runs on a schedule via GitHub Actions (see .github/workflows/check_alerts.yml).
Queries InfluxDB Cloud Serverless via SQL for the latest vibration, deformation,
and temperature readings, compares each against its danger threshold, and emails
everyone on the dashboard's recipient list (read from the "alert_config" table)
via SendGrid — naming exactly which sensor(s) triggered it.

Because GitHub Actions runs are stateless (nothing persists between runs),
alert state (was this sensor already in danger last run?) is stored back into
InfluxDB itself, in an "alert_state" table. This is what lets the script only
email on ok->danger transitions instead of every single run, plus send a
periodic reminder if a danger condition persists.

Secrets (set in GitHub repo Settings -> Secrets and variables -> Actions):
  INFLUXDB_HOST     e.g. us-east-1-1.aws.cloud2.influxdata.com  (NO https://, NO trailing slash)
  INFLUXDB_DATABASE e.g. Beam
  INFLUXDB_TOKEN    Read+write token for that database (write is needed now, for alert_state)
  SENDGRID_API_KEY  Your SendGrid API key (Mail Send permission)
  FROM_EMAIL        Your verified SendGrid sender address
"""

import os
import sys
from datetime import datetime, timezone

import requests
from influxdb_client_3 import InfluxDBClient3, Point

# ---- Fallback thresholds: used ONLY if the dashboard has never synced a
# value for that sensor yet. Once synced, live InfluxDB values take over. ----
DEFAULT_THRESHOLDS = {
    "vibration":   {"danger_max": 1.0,  "unit": "g RMS", "abs": False},
    "deformation": {"danger_max": 5.0,  "unit": "mm",    "abs": True},   # deformation can go negative too
    "temperature": {"danger_max": 80.0, "unit": "°C",    "abs": False},
}

THRESHOLD_FIELD_MAP = {
    "vibration": "danger_vibration",
    "deformation": "danger_deformation",
    "temperature": "danger_temperature",
}

DEVICE_TAG = "esp32_01"

STALE_AFTER_MINUTES = 5   # ESP32 silent this long -> treat as offline, not "still in danger"
REMINDER_AFTER_MINUTES = 30  # if still in danger, remind again after this long (0 = never remind again)


def get_env(name):
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: missing required environment variable {name}")
        sys.exit(1)
    return val


def fetch_latest_field(client, table, field):
    """Latest FRESH value of one field from one table, or None if no row
    exists in the last STALE_AFTER_MINUTES (sensor likely offline, so its
    old last reading shouldn't keep re-triggering alerts).

    Filters WHERE {field} IS NOT NULL because InfluxDB 3 stores each write as
    its own row rather than merging into one "current state" row — a write
    that only touched OTHER fields would otherwise become "latest" here, with
    this field coming back NULL even though an older row had a real value.
    """
    query = f"""
        SELECT {field}
        FROM {table}
        WHERE device = '{DEVICE_TAG}'
          AND {field} IS NOT NULL
          AND time >= now() - INTERVAL '{STALE_AFTER_MINUTES} minutes'
        ORDER BY time DESC
        LIMIT 1
    """
    try:
        rows = client.query(query=query, language="sql").to_pylist()
        return rows[0][field] if rows else None
    except Exception as e:
        print(f"Query failed for {table}.{field}: {e}")
        return None


def fetch_threshold(client, field, default):
    """Latest synced value for one danger threshold field, or the default if never set."""
    query = f"""
        SELECT {field}
        FROM alert_config
        WHERE {field} IS NOT NULL
        ORDER BY time DESC
        LIMIT 1
    """
    try:
        rows = client.query(query=query, language="sql").to_pylist()
        if not rows or rows[0].get(field) is None:
            return default
        return float(rows[0][field])
    except Exception as e:
        print(f"Threshold query failed for {field}, using default {default}: {e}")
        return default


def fetch_recipient_list(client):
    """Latest non-null 'emails' row (avoids being shadowed by threshold-only writes)."""
    query = """
        SELECT emails
        FROM alert_config
        WHERE emails IS NOT NULL
        ORDER BY time DESC
        LIMIT 1
    """
    try:
        rows = client.query(query=query, language="sql").to_pylist()
        if not rows or not rows[0].get("emails"):
            return []
        return [e.strip() for e in rows[0]["emails"].split(",") if e.strip()]
    except Exception as e:
        print(f"Failed to fetch recipient list: {e}")
        return []


def fetch_alert_state(client, field):
    """Last known state for one sensor's alerting: was it in danger, and when
    did we last email about it. Returns (was_in_danger: bool, last_alert: datetime|None).
    Defaults to (False, None) if no state row exists yet (first run ever).
    """
    query = f"""
        SELECT is_danger, last_alert_time
        FROM alert_state
        WHERE field = '{field}'
        ORDER BY time DESC
        LIMIT 1
    """
    try:
        rows = client.query(query=query, language="sql").to_pylist()
        if not rows:
            return False, None
        was_in_danger = bool(rows[0].get("is_danger"))
        last_alert_raw = rows[0].get("last_alert_time")
        last_alert = datetime.fromisoformat(last_alert_raw) if last_alert_raw else None
        return was_in_danger, last_alert
    except Exception as e:
        print(f"Failed to fetch alert_state for {field}, assuming not-in-danger: {e}")
        return False, None


def write_alert_state(client, field, is_danger, last_alert_time):
    """Persist current state so the next (stateless) run can compare against it."""
    try:
        point = (
            Point("alert_state")
            .tag("field", field)
            .field("is_danger", is_danger)
            .field("last_alert_time", last_alert_time.isoformat() if last_alert_time else "")
        )
        client.write(record=point)
    except Exception as e:
        print(f"Failed to write alert_state for {field}: {e}")


def send_email(sendgrid_key, from_email, recipients, subject, body):
    if not recipients:
        print("No recipients configured on the dashboard — skipping send.")
        return
    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {sendgrid_key}",
            "Content-Type": "application/json",
        },
        json={
            "personalizations": [{"to": [{"email": e} for e in recipients]}],
            "from": {"email": from_email},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        },
        timeout=15,
    )
    if resp.status_code >= 300:
        print(f"SendGrid error {resp.status_code}: {resp.text}")
    else:
        print(f"Sent: {subject} -> {recipients}")


def main():
    host = get_env("INFLUXDB_HOST")
    database = get_env("INFLUXDB_DATABASE")
    token = get_env("INFLUXDB_TOKEN")
    sendgrid_key = get_env("SENDGRID_API_KEY")
    from_email = get_env("FROM_EMAIL")

    try:
        client = InfluxDBClient3(host=host, token=token, database=database)
    except Exception as e:
        print(f"FATAL: could not connect to InfluxDB ({host}/{database}): {e}")
        sys.exit(1)

    recipients = fetch_recipient_list(client)
    print(f"Recipients: {recipients}")

    now = datetime.now(timezone.utc)
    triggered = []  # collects lines for a single combined email this run

    for field, defaults in DEFAULT_THRESHOLDS.items():
        cfg = {**defaults, "danger_max": fetch_threshold(client, THRESHOLD_FIELD_MAP[field], defaults["danger_max"])}
        print(f"{field} threshold: {cfg['danger_max']} {defaults['unit']} (dashboard-synced or default)")

        value = fetch_latest_field(client, "sensor_reading", field)
        if value is None:
            print(f"{field}: no fresh data in the last {STALE_AFTER_MINUTES} min (offline or no data yet), skipping.")
            continue

        in_danger = (abs(value) >= cfg["danger_max"]) if cfg["abs"] else (value >= cfg["danger_max"])
        print(f"{field}: {value} {cfg['unit']} (danger >= {cfg['danger_max']}) -> {'DANGER' if in_danger else 'ok'}")

        was_in_danger, last_alert = fetch_alert_state(client, field)

        should_alert = False
        if in_danger and not was_in_danger:
            should_alert = True  # just entered danger zone
        elif in_danger and was_in_danger and REMINDER_AFTER_MINUTES > 0 and last_alert:
            minutes_since = (now - last_alert).total_seconds() / 60
            if minutes_since >= REMINDER_AFTER_MINUTES:
                should_alert = True  # still in danger, reminder is due

        if should_alert:
            triggered.append(
                f"- {field.capitalize()}: {value} {cfg['unit']} (danger threshold: {cfg['danger_max']} {cfg['unit']})"
            )
            write_alert_state(client, field, is_danger=True, last_alert_time=now)
        elif in_danger:
            # still in danger but not due for a reminder yet -> keep state, don't reset last_alert
            write_alert_state(client, field, is_danger=True, last_alert_time=last_alert)
        else:
            # back to normal -> reset so next danger reading alerts immediately
            write_alert_state(client, field, is_danger=False, last_alert_time=None)

    if triggered:
        send_email(
            sendgrid_key,
            from_email,
            recipients,
            subject=f"DANGER: {len(triggered)} sensor(s) exceeded threshold",
            body=(
                f"Device: {DEVICE_TAG}\n\n"
                + "\n".join(triggered)
                + "\n\nThis is either a new danger condition or a periodic reminder "
                  f"(every {REMINDER_AFTER_MINUTES} min while it persists)."
            ),
        )
    else:
        print("Nothing to alert on this run.")


if __name__ == "__main__":
    main()
