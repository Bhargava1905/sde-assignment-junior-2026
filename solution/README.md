# Assignment A solution: Interaction Ledger

## Run it

From the repository root, on Python 3.11+:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
$env:AU_PII_KEY = .\.venv\Scripts\python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
.\.venv\Scripts\python -m solution --db state/ledger.sqlite ingest data/drop_1
.\.venv\Scripts\python -m solution --db state/ledger.sqlite ingest data/drop_2
.\.venv\Scripts\python -m solution --db state/ledger.sqlite report --out reports
.\.venv\Scripts\python -m pytest
```

Keep `AU_PII_KEY` secret and use the **same value** for every command that reads
or writes a particular database. It is intentionally not stored in SQLite.

## CLI contract

This is a batch pipeline, so its public API is its CLI rather than HTTP endpoints.

```text
python -m solution [--db PATH] [--schemas PATH] [--pii-key KEY] ingest DROP_DIRECTORY
python -m solution [--db PATH] [--schemas PATH] [--pii-key KEY] report --out OUTPUT_DIRECTORY
```

`ingest` accepts either a generator-style drop containing `leads/<workflow>/*.csv`
and `events/*.jsonl`, or a parent directory containing multiple such drops (for
example the generated `data/` directory). `report` emits the five files required
by the assignment.
It is safe to call `ingest` again for an unchanged file: the source receipt is
recognised and no rows or counters change. Reusing an already-ingested path with
different bytes fails explicitly instead of silently corrupting the ledger.

## Architecture

```text
lead CSV files  -> schema validation -> leads / rejections
                                         |       (SQLite)
event JSONL    -> event checks      -> append-only events ledger
                                         |
                                         +--> derived calls, dispositions and buckets --> CSV/JSON reports
```

The application code is intentionally a small, single-process CLI. SQLite gives
each file import a transaction, which means a process failure either commits the
file and its receipt together or rolls both back. This makes a retry safe. The
report is recomputed from durable facts rather than maintained as mutable totals;
therefore an event which arrived before its lead automatically starts counting
when the lead is later ingested.

### Example workflow

For `motor_renewal`, a CSV row with `Mobile No=91-77187-50036` becomes the
canonical `+917718750036`. Its HMAC hash identifies one lead in that workflow;
the source file stem (for example `batch_001`) remains its permanent batch.

An incoming `call.initiated` event with that phone starts a derived call. A later
`disposition.set` is the provisional outcome, and a later
`disposition.override` is linked by `call_id` and becomes the effective outcome.
Neither event is edited or deleted. If it is terminal, the lead is `completed`;
otherwise it is `in_progress_dialed`, unless every call failed at telephony.

## Data model

| Table | Purpose and key constraints |
|---|---|
| `receipts` | Source path, content digest, and import timestamp. One receipt per input file gives file-level idempotency. |
| `leads` | One current merged lead per `(workflow_id, phone_hash)`. Its first `batch_id` is immutable. |
| `events` | Append-only interaction ledger keyed by `event_id`; indexed by `(workflow_id, call_id)` and by phone hash. |
| `rejections` | Non-PII rejected-row audit keyed by source file and row number. |
| `counters`, `rejection_reasons` | Durable quality counters, updated in the same transaction as their source file. |

PII (`phone` and names) is JSON encrypted with Fernet before writing to SQLite.
The canonical phone also has a keyed HMAC-SHA256 token. Matching uses that token;
the clear phone is decrypted only to generate the masked report value. The key is
provided out-of-band in `AU_PII_KEY`, so a copied database alone does not reveal
the PII.

## Deliberate decisions

- A changed file at the same absolute path is rejected. A delivery should be
  immutable; accepting a rewrite makes idempotency ambiguous.
- Valid-but-unlinked events stay in the ledger. Orphan and unknown-disposition
  counts are derived at report time, so an event can become linked when a later
  lead arrives.
- Defaults are not stored during a partial merge. This prevents a default from
  masking a real non-null value in a later duplicate row.
- Report aggregation is in Python for clarity and to keep the SQLite schema
  focused on durable source facts. SQL aggregation is an appropriate stretch.
