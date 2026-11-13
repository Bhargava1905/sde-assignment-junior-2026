import csv
import json
from pathlib import Path

from cryptography.fernet import Fernet

from solution.pipeline import Pipeline, canonical_phone, parse_date, parse_decimal


ROOT = Path(__file__).parents[1]
SCHEMAS = ROOT / "assignment-a-data" / "schemas"


def make_pipeline(tmp_path):
    return Pipeline(tmp_path / "state" / "ledger.sqlite", SCHEMAS, Fernet.generate_key().decode())


def make_drop(tmp_path, leads, events):
    drop = tmp_path / "drop"
    lead_dir = drop / "leads" / "motor_renewal"
    event_dir = drop / "events"
    lead_dir.mkdir(parents=True)
    event_dir.mkdir()
    with (lead_dir / "batch_001.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_name", "phone", "policy_number"])
        writer.writeheader()
        writer.writerows(leads)
    (event_dir / "events_0001.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n")
    return drop


def event(event_id, kind, call_id, at, phone=None, payload=None):
    value = {"event_id": event_id, "event_type": kind, "workflow_id": "motor_renewal", "call_id": call_id,
             "occurred_at": at, "payload": payload or {}}
    if phone is not None:
        value["phone"] = phone
    return value


def test_phone_dates_and_amounts_normalise():
    assert canonical_phone("91-77187-50036") == "+917718750036"
    assert canonical_phone("07718750036") == "+917718750036"
    assert canonical_phone("522-123-4567") is None
    assert parse_date("01-Dec-2026") == "2026-12-01"
    assert parse_date("31/02/2026") is None
    assert parse_decimal("Rs. 12,345.678") == "12345.68"
    assert parse_decimal("-1") is None


def test_ingest_is_idempotent_and_overrides_win(tmp_path):
    phone = "+91 98765 43210"
    drop = make_drop(tmp_path, [{"customer_name": "Asha", "phone": phone, "policy_number": "P-1"}], [
        event("e1", "call.initiated", "c1", "2026-10-01T10:00:00Z", phone),
        event("e2", "call.connected", "c1", "2026-10-01T10:01:00Z", phone),
        event("e3", "disposition.set", "c1", "2026-10-01T10:02:00Z", phone, {"disposition_code": "payment_link_sent"}),
        event("e4", "disposition.override", "c1", "2026-10-01T10:03:00Z", None, {"disposition_code": "renewed"}),
    ])
    pipeline = make_pipeline(tmp_path)
    pipeline.ingest(drop)
    pipeline.ingest(drop)
    out = tmp_path / "reports"
    pipeline.report(out)
    rows = list(csv.DictReader((out / "lead_report.csv").open()))
    quality = json.loads((out / "data_quality.json").read_text())
    assert rows[0]["current_disposition"] == "renewed"
    assert rows[0]["bucket"] == "completed"
    assert rows[0]["ever_success"] == "true"
    assert quality["events_total"] == 4
    assert quality["duplicate_events_dropped"] == 0
    pipeline.close()


def test_telephony_failed_bucket_and_orphan_relinks_when_lead_arrives(tmp_path):
    phone = "+919876543210"
    drop_events = make_drop(tmp_path / "one", [], [
        event("e1", "call.initiated", "c1", "2026-10-01 10:00:00", phone),
        event("e2", "call.telephony_failed", "c1", "2026-10-01 10:00:01", phone),
    ])
    drop_lead = make_drop(tmp_path / "two", [{"customer_name": "Dev", "phone": phone, "policy_number": "P-2"}], [])
    pipeline = make_pipeline(tmp_path)
    pipeline.ingest(drop_events)
    pipeline.ingest(drop_lead)
    out = tmp_path / "reports"
    pipeline.report(out)
    rows = list(csv.DictReader((out / "lead_report.csv").open()))
    quality = json.loads((out / "data_quality.json").read_text())
    assert rows[0]["bucket"] == "in_progress_telephony_failed"
    assert quality["orphan_events"] == 0
    pipeline.close()


def test_parent_directory_ingests_drops_in_filename_order(tmp_path):
    root = tmp_path / "deliveries"
    first = make_drop(root / "drop_1", [{"customer_name": "Asha", "phone": "+919876543210", "policy_number": "P-1"}], [])
    second = make_drop(root / "drop_2", [{"customer_name": "Asha Updated", "phone": "9876543210", "policy_number": "P-2"}], [])
    # make_drop creates a child named "drop"; rename it to the generator's shape.
    first.rename(root / "drop_1_final")
    second.rename(root / "drop_2_final")
    pipeline = make_pipeline(tmp_path)
    pipeline.ingest(root)
    out = tmp_path / "reports"
    pipeline.report(out)
    rows = list(csv.DictReader((out / "lead_report.csv").open()))
    assert len(rows) == 1
    assert rows[0]["external_ref"] == "P-2"
    pipeline.close()


def test_unparseable_timestamp_is_retained_but_not_counted_as_malformed(tmp_path):
    phone = "+919876543210"
    drop = make_drop(tmp_path, [{"customer_name": "Asha", "phone": phone, "policy_number": "P-1"}], [
        event("e1", "call.initiated", "c1", "not-a-timestamp", phone),
    ])
    pipeline = make_pipeline(tmp_path)
    pipeline.ingest(drop)
    out = tmp_path / "reports"
    pipeline.report(out)
    quality = json.loads((out / "data_quality.json").read_text())
    assert quality["events_total"] == 1
    assert quality["malformed_events"] == 0
    assert list(csv.DictReader((out / "daily_overview.csv").open())) == []
    pipeline.close()
