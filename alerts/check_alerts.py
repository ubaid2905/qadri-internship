"""
Runs on a schedule via GitHub Actions (see .github/workflows/check_alerts.yml).
Queries InfluxDB Cloud Serverless via SQL for the latest vibration, deformation,
and temperature readings, compares each against its danger threshold, and emails
everyone on the dashboard's recipient list (read from the "alert_config" table)
via SendGrid — naming exactly which sensor triggered it.

Secrets (set in GitHub repo Settings -> Secrets and variables -> Actions):
  INFLUXDB_HOST     e.g. us-east-1-1.aws.cloud2.influxdata.com  (NO https://, NO trailing slash)
  INFLUXDB_DATABASE e.g. Beam
  INFLUXDB_TOKEN    Read-permission token for that database
  SENDGRID_API_KEY  Your SendGrid API key (Mail Send permission)
  FROM_EMAIL        Your verified SendGrid sender address
"""

import os
import sys
import requests
from influxdb_client_3 import InfluxDBClient3

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

STALE_AFTER_MINUTES = 5  # if the ESP32 hasn't posted in this long, treat it as offline, not "still in danger"


def get_env(name):
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: missing required environment variable {name}")
        sys.exit(1)
    return val


def fetch_latest_field(client, table, field):
    """Latest value of one field from one table (measurement), or None if no
    FRESH row exists (no rows at all, or the newest one is older than
    STALE_AFTER_MINUTES — meaning the ESP32 is likely powered off/disconnected,
    so its last reading shouldn't keep re-triggering alerts forever).

    Also filters WHERE {field} IS NOT NULL. InfluxDB 3 stores each write as
    its own independent row rather than merging into one "current state"
    row, so a write that only touched OTHER fields would otherwise become the
    "latest" row here, with this field coming back NULL even though an older
    row actually set a real value for it.
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
        table_result = client.query(query=query, language="sql")
        rows = table_result.to_pylist()
        if not rows:
            return None
        return rows[0][field]
    except Exception as e:
        print(f"Query failed for {table}.{field}: {e}")
        return None


def fetch_threshold(client, field, default):
    """Latest synced value for one danger threshold field, or the default if never set.

    Same "latest non-null row for THIS field" fix as fetch_latest_field — a
    write to the emails field alone would otherwise be able to shadow a real,
    older threshold value.
    """
    query = f"""
        SELECT {field}
        FROM alert_config
        WHERE {field} IS NOT NULL
        ORDER BY time DESC
        LIMIT 1
    """
    try:
        table_result = client.query(query=query, language="sql")
        rows = table_result.to_pylist()
        if not rows or rows[0].get(field) is None:
            return default
        return float(rows[0][field])
    except Exception as e:
        print(f"Threshold query failed for {field}, using default {default}: {e}")
        return default


def fetch_recipient_list(client):
    """Latest non-null 'emails' row.

    This is the fix for the earlier bug: a threshold-sync write (which only
    sets danger_vibration/danger_deformation/danger_temperature, never
    emails) was becoming the "latest" alert_config row under a plain
    `ORDER BY time DESC LIMIT 1`, so emails came back NULL even though an
    older row still had the real recipient list. Filtering to rows where
    emails IS NOT NULL fixes that.
    """
    query = """
        SELECT emails
        FROM alert_config
        WHERE emails IS NOT NULL
        ORDER BY time DESC
        LIMIT 1
    """
    try:
        table_result = client.query(query=query, language="sql")
        rows = table_result.to_pylist()
        if not rows or not rows[0].get("emails"):
            return []
        return [e.strip() for e in rows[0]["emails"].split(",") if e.strip()]
    except Exception as e:
        print(f"Failed to fetch recipient list: {e}")
        return []


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

    client = InfluxDBClient3(host=host, token=token, database=database)

    recipients = fetch_recipient_list(client)
    print(f"Recipients: {recipients}")

    # Pull live thresholds (dashboard-synced if available, else defaults),
    # then check every sensor against ITS live value, not a fixed constant.
    thresholds = {}
    for field, defaults in DEFAULT_THRESHOLDS.items():
        live_danger_max = fetch_threshold(client, THRESHOLD_FIELD_MAP[field], defaults["danger_max"])
        thresholds[field] = {**defaults, "danger_max": live_danger_max}
        print(f"{field} threshold: {live_danger_max} {defaults['unit']} (dashboard-synced or default)")

    for field, cfg in thresholds.items():
        value = fetch_latest_field(client, "sensor_reading", field)
        if value is None:
            print(f"{field}: no fresh data in the last {STALE_AFTER_MINUTES} min (offline or no data yet), skipping.")
            continue

        in_danger = (abs(value) >= cfg["danger_max"]) if cfg["abs"] else (value >= cfg["danger_max"])
        print(f"{field}: {value} {cfg['unit']} (danger >= {cfg['danger_max']}) -> {'DANGER' if in_danger else 'ok'}")

        if in_danger:
            send_email(
                sendgrid_key,
                from_email,
                recipients,
                subject=f"DANGER: {field.capitalize()} threshold exceeded",
                body=(
                    f"{field.capitalize()} reading {value} {cfg['unit']} has crossed the danger "
                    f"threshold ({cfg['danger_max']} {cfg['unit']}). Device: {DEVICE_TAG}."
                ),
            )


if __name__ == "__main__":
    main()
