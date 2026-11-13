"""Assignment A's small, deliberately boring data pipeline.

The database is the source of truth.  Input-file receipts make ingest idempotent;
the event table is an append-only ledger; reports are rebuilt from those facts.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import hmac
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
QUALITY_KEYS = ("lead_rows_total", "lead_rows_rejected", "duplicate_lead_rows_merged",
                "field_warnings", "events_total", "malformed_events", "duplicate_events_dropped")


def norm_header(value: str) -> str:
    return re.sub(r"[_\s]+", " ", value.strip().lower())


def canonical_phone(value: str | None) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) != 10 or digits[0:1] not in {"6", "7", "8", "9"}:
        return None
    return "+91" + digits


def parse_date(value: str) -> str | None:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            pass
    # strptime's month names are platform-dependent, so normalise them explicitly.
    match = re.fullmatch(r"(\d{2})-([A-Za-z]{3})-(\d{4})", value)
    if match:
        months = {m.lower(): i for i, m in enumerate(
            ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 1)}
        try:
            return datetime(int(match[3]), months[match[2].lower()], int(match[1])).date().isoformat()
        except (KeyError, ValueError):
            return None
    return None


def parse_decimal(value: str) -> str | None:
    cleaned = re.sub(r"^\s*(?:₹|rs\.?|inr)\s*", "", value, flags=re.I).replace(",", "").strip()
    try:
        amount = Decimal(cleaned)
    except InvalidOperation:
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return str(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def parse_timestamp(value: str) -> datetime:
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=IST)
    return parsed.astimezone(UTC)


def utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def read_schemas(schema_dir: Path) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, bool]]]:
    schemas = {}
    for file in schema_dir.glob("*.json"):
        schema = json.loads(file.read_text(encoding="utf-8"))
        schemas[schema["workflow_id"]] = schema
    dispositions: dict[tuple[str, str], dict[str, bool]] = {}
    with (schema_dir / "dispositions.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            dispositions[(row["workflow_id"], row["code"].lower())] = {
                "terminal": row["is_terminal"].lower() == "true",
                "success": row["is_success"].lower() == "true",
            }
    return schemas, dispositions


class Pipeline:
    def __init__(self, db_path: Path, schema_dir: Path, pii_key: str):
        try:
            self.fernet = Fernet(pii_key.encode())
        except Exception as exc:
            raise ValueError("PII key must be a urlsafe Fernet key (generate one with Fernet.generate_key()).") from exc
        self.phone_secret = hashlib.sha256(pii_key.encode()).digest()
        self.schemas, self.dispositions = read_schemas(schema_dir)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self._create_schema()

    def close(self) -> None:
        self.db.close()

    def _create_schema(self) -> None:
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS receipts (
          source_path TEXT PRIMARY KEY, content_sha256 TEXT NOT NULL, kind TEXT NOT NULL,
          imported_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS counters (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS rejection_reasons (reason TEXT PRIMARY KEY, value INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS leads (
          lead_id INTEGER PRIMARY KEY, workflow_id TEXT NOT NULL, phone_hash TEXT NOT NULL,
          batch_id TEXT NOT NULL, pii_ciphertext BLOB NOT NULL, data_json TEXT NOT NULL,
          created_at TEXT NOT NULL, UNIQUE(workflow_id, phone_hash)
        );
        CREATE TABLE IF NOT EXISTS rejections (
          rejection_id INTEGER PRIMARY KEY, workflow_id TEXT NOT NULL, batch_id TEXT NOT NULL,
          row_number INTEGER NOT NULL, reason TEXT NOT NULL, source_path TEXT NOT NULL,
          UNIQUE(source_path, row_number)
        );
        CREATE TABLE IF NOT EXISTS events (
          event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, workflow_id TEXT NOT NULL,
          call_id TEXT, occurred_utc TEXT NOT NULL, phone_hash TEXT, payload_json TEXT NOT NULL,
          invalid_disposition INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS events_call_idx ON events(workflow_id, call_id);
        CREATE INDEX IF NOT EXISTS events_phone_idx ON events(workflow_id, phone_hash);
        """)
        for key in QUALITY_KEYS:
            self.db.execute("INSERT OR IGNORE INTO counters(key, value) VALUES (?, 0)", (key,))
        self.db.commit()

    def _phone_hash(self, phone: str) -> str:
        return hmac.new(self.phone_secret, phone.encode(), hashlib.sha256).hexdigest()

    def _seal(self, data: dict[str, Any]) -> bytes:
        return self.fernet.encrypt(json.dumps(data, sort_keys=True).encode())

    def _open(self, value: bytes) -> dict[str, Any]:
        return json.loads(self.fernet.decrypt(value).decode())

    def _counter(self, key: str, increment: int = 1) -> None:
        self.db.execute("UPDATE counters SET value=value+? WHERE key=?", (increment, key))

    def _reason(self, reason: str) -> None:
        self.db.execute("""INSERT INTO rejection_reasons(reason,value) VALUES (?,1)
                         ON CONFLICT(reason) DO UPDATE SET value=value+1""", (reason,))

    def _receipt(self, file: Path, kind: str) -> bool:
        path = str(file.resolve())
        digest = hashlib.sha256(file.read_bytes()).hexdigest()
        old = self.db.execute("SELECT content_sha256 FROM receipts WHERE source_path=?", (path,)).fetchone()
        if old:
            if old["content_sha256"] != digest:
                raise ValueError(f"Refusing changed file already ingested: {file}")
            return True
        self.db.execute("INSERT INTO receipts VALUES (?, ?, ?, ?)", (path, digest, kind, utc_text(datetime.now(UTC))))
        return False

    def ingest(self, drop: Path) -> None:
        # Accept either one delivery (``data/drop_1``) or a directory of
        # deliveries (``data``).  Sorting keeps duplicate-merge semantics stable.
        roots = [drop] if (drop / "leads").exists() or (drop / "events").exists() else sorted(
            child for child in drop.iterdir()
            if child.is_dir() and ((child / "leads").exists() or (child / "events").exists())
        )
        if not roots:
            raise ValueError(f"No ingestion drop found at {drop}")
        for root in roots:
            for file in sorted((root / "leads").glob("*/*.csv")):
                self._ingest_leads(file)
            for file in sorted((root / "events").glob("*.jsonl")):
                self._ingest_events(file)
        self._apply_defaults()

    def _apply_defaults(self) -> None:
        """Apply defaults only after the drop's duplicate rows have merged."""
        with self.db:
            for row in self.db.execute("SELECT lead_id, workflow_id, data_json FROM leads"):
                values = json.loads(row["data_json"])
                changed = False
                for field in self.schemas[row["workflow_id"]]["fields"]:
                    if "default" in field and values.get(field["name"]) is None:
                        values[field["name"]] = field["default"]
                        changed = True
                if changed:
                    self.db.execute("UPDATE leads SET data_json=? WHERE lead_id=?",
                                    (json.dumps(values, sort_keys=True), row["lead_id"]))

    def _header_map(self, headers: list[str], schema: dict[str, Any]) -> dict[str, str]:
        candidates = {}
        for field in schema["fields"]:
            for name in [field["name"], *field.get("aliases", [])]:
                candidates[norm_header(name)] = field["name"]
        return {field: candidates[norm_header(field)] for field in headers if norm_header(field) in candidates}

    def _normalise_value(self, field: dict[str, Any], raw: str) -> tuple[Any, bool]:
        value = raw.strip()
        if not value:
            return None, False
        if field["type"] == "string":
            return value, False
        if field["type"] == "phone":
            phone = canonical_phone(value)
            return phone, phone is None
        if field["type"] == "date":
            parsed = parse_date(value)
            return parsed, parsed is None
        if field["type"] == "decimal":
            parsed = parse_decimal(value)
            return parsed, parsed is None
        if field["type"] == "enum":
            allowed = {x.lower() for x in field["values"]}
            return (value.lower(), False) if value.lower() in allowed else (None, True)
        raise ValueError(f"Unknown field type {field['type']}")

    def _ingest_leads(self, file: Path) -> None:
        workflow = file.parent.name
        if workflow not in self.schemas:
            raise ValueError(f"No schema for workflow {workflow}")
        schema = self.schemas[workflow]
        batch_id = file.stem
        with self.db:
            if self._receipt(file, "lead"):
                return
            with file.open(newline="", encoding="utf-8-sig") as stream:
                reader = csv.DictReader(stream)
                mapping = self._header_map(reader.fieldnames or [], schema)
                row_number = 0
                for row in reader:
                    if all(not (v or "").strip() for v in row.values()):
                        continue
                    row_number += 1
                    self._counter("lead_rows_total")
                    values: dict[str, Any] = {}
                    invalid: dict[str, bool] = {}
                    for field in schema["fields"]:
                        source = next((header for header, name in mapping.items() if name == field["name"]), None)
                        values[field["name"]], invalid[field["name"]] = self._normalise_value(field, row.get(source, "") or "")
                    reason = None
                    for field in schema["fields"]:
                        name = field["name"]
                        if field.get("required") and values[name] is None:
                            reason = "invalid_phone" if name == "phone" and invalid[name] else f"missing_required:{name}"
                            break
                        if name == "phone" and invalid[name]:
                            reason = "invalid_phone"
                            break
                    if reason:
                        self._counter("lead_rows_rejected")
                        self._reason(reason)
                        self.db.execute("INSERT INTO rejections(workflow_id,batch_id,row_number,reason,source_path) VALUES (?,?,?,?,?)",
                                        (workflow, batch_id, row_number, reason, str(file.resolve())))
                        continue
                    for field in schema["fields"]:
                        if not field.get("required") and invalid[field["name"]]:
                            self._counter("field_warnings")
                            values[field["name"]] = None
                    phone = values["phone"]
                    phone_hash = self._phone_hash(phone)
                    pii = {f["name"]: values[f["name"]] for f in schema["fields"] if f.get("pii")}
                    public = {f["name"]: values[f["name"]] for f in schema["fields"] if not f.get("pii")}
                    old = self.db.execute("SELECT * FROM leads WHERE workflow_id=? AND phone_hash=?", (workflow, phone_hash)).fetchone()
                    if old:
                        self._counter("duplicate_lead_rows_merged")
                        old_pii, old_public = self._open(old["pii_ciphertext"]), json.loads(old["data_json"])
                        for name, value in pii.items():
                            if value is not None:
                                old_pii[name] = value
                        for name, value in public.items():
                            if value is not None:
                                old_public[name] = value
                        self.db.execute("UPDATE leads SET pii_ciphertext=?, data_json=? WHERE lead_id=?",
                                        (self._seal(old_pii), json.dumps(old_public, sort_keys=True), old["lead_id"]))
                    else:
                        self.db.execute("""INSERT INTO leads(workflow_id,phone_hash,batch_id,pii_ciphertext,data_json,created_at)
                                           VALUES (?,?,?,?,?,?)""",
                                        (workflow, phone_hash, batch_id, self._seal(pii), json.dumps(public, sort_keys=True), utc_text(datetime.now(UTC))))

    def _ingest_events(self, file: Path) -> None:
        with self.db:
            if self._receipt(file, "event"):
                return
            for line in file.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                self._counter("events_total")
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict) or not event.get("event_id"):
                        raise ValueError("event id")
                except (json.JSONDecodeError, ValueError):
                    self._counter("malformed_events")
                    continue
                if self.db.execute("SELECT 1 FROM events WHERE event_id=?", (event["event_id"],)).fetchone():
                    self._counter("duplicate_events_dropped")
                    continue
                try:
                    occurred = utc_text(parse_timestamp(event["occurred_at"]))
                except (KeyError, TypeError, ValueError):
                    # The contract reserves "malformed" for invalid JSON / missing event_id.
                    # Retain an unparseable but otherwise valid event in the ledger; it cannot
                    # form a derived call until a valid initiation timestamp is available.
                    occurred = ""
                phone = canonical_phone(event.get("phone")) if event.get("phone") else None
                payload = event.get("payload") or {}
                typ, workflow = event.get("event_type", ""), event.get("workflow_id", "")
                code = str(payload.get("disposition_code", "")).lower()
                invalid_disposition = int(typ.startswith("disposition.") and (workflow, code) not in self.dispositions)
                self.db.execute("""INSERT INTO events(event_id,event_type,workflow_id,call_id,occurred_utc,phone_hash,payload_json,invalid_disposition)
                                   VALUES (?,?,?,?,?,?,?,?)""",
                                (event["event_id"], typ, workflow, event.get("call_id"), occurred,
                                 self._phone_hash(phone) if phone else None, json.dumps(payload, sort_keys=True), invalid_disposition))

    def _facts(self) -> tuple[list[dict[str, Any]], dict[int, list[dict[str, Any]]], list[sqlite3.Row], set[str]]:
        leads = []
        by_phone: dict[tuple[str, str], int] = {}
        for row in self.db.execute("SELECT * FROM leads"):
            public, pii = json.loads(row["data_json"]), self._open(row["pii_ciphertext"])
            lead = {"id": row["lead_id"], "workflow": row["workflow_id"], "batch": row["batch_id"], "phone": pii["phone"],
                    "public": public, "pii": pii}
            leads.append(lead)
            by_phone[(row["workflow_id"], row["phone_hash"])] = row["lead_id"]
        events = list(self.db.execute("SELECT * FROM events ORDER BY occurred_utc,event_id"))
        event_leads: dict[str, int] = {}
        call_leads: dict[tuple[str, str], int] = {}
        for event in events:
            if event["event_type"] != "disposition.override" and event["phone_hash"]:
                lead_id = by_phone.get((event["workflow_id"], event["phone_hash"]))
                if lead_id is not None:
                    event_leads[event["event_id"]] = lead_id
                    if event["call_id"]:
                        call_leads[(event["workflow_id"], event["call_id"])] = lead_id
        for event in events:
            if event["event_type"] == "disposition.override" and event["call_id"]:
                lead_id = call_leads.get((event["workflow_id"], event["call_id"]))
                if lead_id is not None:
                    event_leads[event["event_id"]] = lead_id
        calls_by_lead: dict[int, list[dict[str, Any]]] = defaultdict(list)
        grouped: dict[tuple[str, str], list[sqlite3.Row]] = defaultdict(list)
        for event in events:
            if event["event_id"] in event_leads and event["call_id"]:
                grouped[(event["workflow_id"], event["call_id"])].append(event)
        for (_workflow, _call_id), call_events in grouped.items():
            # A timestampless event remains retained/auditable, but cannot contribute to a
            # time-based report or determine an effective disposition.
            call_events = [e for e in call_events if e["occurred_utc"]]
            initiations = [e for e in call_events if e["event_type"] == "call.initiated"]
            if not initiations:
                continue
            initiated = min(initiations, key=lambda e: (e["occurred_utc"], e["event_id"]))
            lead_id = event_leads[initiated["event_id"]]
            valid = [e for e in call_events if not e["invalid_disposition"]]
            overrides = [e for e in valid if e["event_type"] == "disposition.override"]
            sets = [e for e in valid if e["event_type"] == "disposition.set"]
            choice = max(overrides or sets, key=lambda e: (e["occurred_utc"], e["event_id"]), default=None)
            code = json.loads(choice["payload_json"]).get("disposition_code", "").lower() if choice else None
            calls_by_lead[lead_id].append({"initiated": initiated["occurred_utc"], "connected": any(e["event_type"] == "call.connected" for e in valid),
                                           "telephony_failed": any(e["event_type"] == "call.telephony_failed" for e in valid), "disposition": code})
        orphan_ids = {e["event_id"] for e in events if e["event_id"] not in event_leads}
        return leads, calls_by_lead, events, orphan_ids

    def report(self, out: Path) -> None:
        out.mkdir(parents=True, exist_ok=True)
        leads, calls, events, orphan_ids = self._facts()
        lead_by_id = {lead["id"]: lead for lead in leads}
        buckets: dict[int, str] = {}
        for lead in leads:
            lead_calls = calls[lead["id"]]
            if not lead_calls:
                buckets[lead["id"]] = "yet_to_reach_out"
            elif any(self.dispositions.get((lead["workflow"], c["disposition"]), {}).get("terminal") for c in lead_calls):
                buckets[lead["id"]] = "completed"
            elif all(c["telephony_failed"] for c in lead_calls):
                buckets[lead["id"]] = "in_progress_telephony_failed"
            else:
                buckets[lead["id"]] = "in_progress_dialed"
        self._write_batch_summary(out, leads, buckets)
        self._write_daily(out, leads, calls)
        self._write_leads(out, leads, calls, buckets)
        self._write_rejections(out)
        counter = {r["key"]: r["value"] for r in self.db.execute("SELECT * FROM counters")}
        reasons = {r["reason"]: r["value"] for r in self.db.execute("SELECT * FROM rejection_reasons ORDER BY reason")}
        unknown = sum(e["invalid_disposition"] for e in events if e["event_id"] not in orphan_ids)
        data_quality = {
            "lead_rows_total": counter["lead_rows_total"],
            "lead_rows_rejected": counter["lead_rows_rejected"],
            "rejections_by_reason": reasons,
            "duplicate_lead_rows_merged": counter["duplicate_lead_rows_merged"],
            "field_warnings": counter["field_warnings"],
            "events_total": counter["events_total"],
            "malformed_events": counter["malformed_events"],
            "duplicate_events_dropped": counter["duplicate_events_dropped"],
            "orphan_events": len(orphan_ids),
            "unknown_disposition_events": unknown,
        }
        (out / "data_quality.json").write_text(json.dumps(data_quality, indent=2) + "\n", encoding="utf-8")

    @staticmethod
    def _csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)

    def _write_batch_summary(self, out: Path, leads: list[dict[str, Any]], buckets: dict[int, str]) -> None:
        data: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
        for lead in leads:
            data[(lead["workflow"], lead["batch"])][buckets[lead["id"]]] += 1
        rows = [[wf, batch, sum(c.values()), c["yet_to_reach_out"], c["in_progress_telephony_failed"], c["in_progress_dialed"], c["completed"]]
                for (wf, batch), c in sorted(data.items())]
        self._csv(out / "batch_summary.csv", ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed"], rows)

    def _write_daily(self, out: Path, leads: list[dict[str, Any]], calls: dict[int, list[dict[str, Any]]]) -> None:
        days: dict[tuple[str, str], list[tuple[int, dict[str, Any]]]] = defaultdict(list)
        for lead in leads:
            for call in calls[lead["id"]]:
                day = datetime.fromisoformat(call["initiated"]).astimezone(IST).date().isoformat()
                days[(lead["workflow"], day)].append((lead["id"], call))
        rows = []
        for (workflow, day), entries in sorted(days.items()):
            total = len(entries)
            connected = sum(c["connected"] for _, c in entries)
            successes = len({lid for lid, c in entries if self.dispositions.get((workflow, c["disposition"]), {}).get("success")})
            rows.append([workflow, day, total, connected, f"{connected / total:.4f}", successes])
        self._csv(out / "daily_overview.csv", ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"], rows)

    def _write_leads(self, out: Path, leads: list[dict[str, Any]], calls: dict[int, list[dict[str, Any]]], buckets: dict[int, str]) -> None:
        rows = []
        ref_name = {wf: next(f["name"] for f in s["fields"] if f.get("reference")) for wf, s in self.schemas.items()}
        for lead in sorted(leads, key=lambda l: (l["workflow"], l["public"].get(ref_name[l["workflow"]]) or "")):
            lead_calls = calls[lead["id"]]
            latest = max(lead_calls, key=lambda c: c["initiated"], default=None)
            with_disposition = [c for c in lead_calls if c["disposition"]]
            current = max(with_disposition, key=lambda c: c["initiated"])["disposition"] if with_disposition else ""
            last_at = ""
            if latest:
                last_at = datetime.fromisoformat(latest["initiated"]).astimezone(IST).isoformat(timespec="seconds")
            success = any(self.dispositions.get((lead["workflow"], c["disposition"]), {}).get("success") for c in lead_calls)
            rows.append([lead["workflow"], lead["public"].get(ref_name[lead["workflow"]], ""), lead["batch"],
                         "+91******" + lead["phone"][-4:], buckets[lead["id"]], len(lead_calls), sum(c["connected"] for c in lead_calls),
                         last_at, current, str(success).lower()])
        self._csv(out / "lead_report.csv", ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls", "connected_calls", "last_call_at", "current_disposition", "ever_success"], rows)

    def _write_rejections(self, out: Path) -> None:
        rows = [[r["workflow_id"], r["batch_id"], r["row_number"], r["reason"]]
                for r in self.db.execute("SELECT * FROM rejections ORDER BY workflow_id,batch_id,row_number")]
        self._csv(out / "rejections.csv", ["workflow_id", "batch_id", "row_number", "reason"], rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Assignment A interaction ledger pipeline")
    parser.add_argument("--db", type=Path, default=Path("ledger.sqlite"))
    parser.add_argument("--schemas", type=Path, default=Path("assignment-a-data/schemas"))
    parser.add_argument("--pii-key", default=os.environ.get("AU_PII_KEY"), help="Fernet key; defaults to AU_PII_KEY")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("ingest").add_argument("drop", type=Path)
    commands.add_parser("report").add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if not args.pii_key:
        parser.error("provide --pii-key or set AU_PII_KEY")
    pipeline = Pipeline(args.db, args.schemas, args.pii_key)
    try:
        if args.command == "ingest":
            pipeline.ingest(args.drop)
        else:
            pipeline.report(args.out)
    finally:
        pipeline.close()
