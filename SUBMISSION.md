# Submission

> Replace everything in _italics_. Be concise: bullet points are fine. We read every word.

## Candidate
- **Name:** Bhargava Sai Pasumarthy
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** 5 hours

## How to run
```bash
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
$env:AU_PII_KEY = .\.venv\Scripts\python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
.\.venv\Scripts\python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
.\.venv\Scripts\python -m solution --db state/ledger.sqlite ingest data/drop_1
.\.venv\Scripts\python -m solution --db state/ledger.sqlite ingest data/drop_2
.\.venv\Scripts\python -m solution --db state/ledger.sqlite report --out reports
.\.venv\Scripts\python -m pytest
```

## Design
The pipeline is a deterministic Python CLI backed by SQLite. Each CSV or JSONL
file is imported inside one transaction. A `receipts` row (absolute source path
and SHA-256 digest) is committed with its data, so a successful file is skipped on
re-ingest and a process crash rolls back both the data and receipt.

`leads` has one current row per `(workflow_id, phone_hash)` and preserves the
first batch ID. New non-null duplicate values update the merged lead. `events`
is an append-only ledger keyed by `event_id`. Reports are derived from it every
time, including effective dispositions and buckets; this makes previously orphaned
events count once a later lead can link them.

PII is Fernet-encrypted before it reaches SQLite. A keyed HMAC-SHA256 token of
the canonical phone supports matching without storing a searchable raw phone.
The Fernet key is supplied as `AU_PII_KEY` and is not stored with the database.

## Decisions & assumptions
| Decision | Why | Alternative considered |
|---|---|---|
| Reject an already-ingested path whose content changed | Input deliveries should be immutable; silently applying a rewrite makes idempotency unsafe. | Reprocess by content hash. |
| Derive orphan / unknown-disposition counts at report time | An event can precede its lead and needs to transition from orphaned to linked without mutation. | Persist a mutable `linked_lead_id`. |
| Apply schema defaults after a drop merges | A default must not prevent a later duplicate from supplying a real value. | Apply defaults as each row is read. |
| Use Python aggregation | Keeps the source tables and tricky ledger linkage easy to inspect in a time-boxed solution. | SQL reporting queries. |

## Failure modes considered
- Dirty lead values are normalised; rejected rows are non-PII and have one stable
  reason. Invalid optional fields become null and increment warnings.
- A transaction covers each source file and its receipt, making a crash at a file
  boundary recoverable by re-running the command.
- Duplicate event IDs are rejected by the ledger primary key; duplicate source
  files are skipped through receipts.
- Events with no current lead stay in the ledger rather than being discarded.
- This SQLite implementation is single-writer. I would add a work queue / database
  locking strategy for concurrent ingestion workers in production.

## Testing
`pytest` covers phone/date/currency normalisation, idempotent re-ingest, override
precedence, terminal bucket selection, telephony-failure buckets, and an event
that is orphaned before its lead arrives. I also generated seed 42, ingested both
drops, generated all reports, then re-ingested both drops and compared the quality
output. I did not benchmark the large fixture in this time-boxed pass.

## Results
On the supplied small seed (`42`), the pipeline produced all five reports from
2,057 non-blank lead rows and 8,745 non-empty event lines. It recorded 108
rejections, 57 merged duplicate leads, 237 duplicate event IDs, 462 final
orphans, and 18 final unknown-disposition events.

## If I had another day, I would
1. Add a migration mechanism and strict database-level check constraints.
2. Move the report aggregation to SQL and benchmark the large fixture.
3. Add metrics, structured logs, retryable batch orchestration and a key-management
   service for production deployment.
