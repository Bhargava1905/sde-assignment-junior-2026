# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used
Codex (OpenAI)

## What I used them for
Used it to review the assignment specification, discuss the SQLite-ledger design,
scaffold the initial implementation, and draft tests and documentation.

## What I wrote or decided myself
I chose Assignment A and reviewed the specification, implementation, tests and
generated reports before submission. I selected the final approach: a
single-process SQLite CLI, file-level idempotency through durable receipts, an
append-only event ledger, and report generation derived from persisted facts.

I also approved the key trade-offs: Fernet encryption plus a keyed phone HMAC
for PII protection and matching, retaining late-arriving events so they can link
when a lead is received later, and using Python aggregation for clarity within
the assignment time box. I understand the code paths for lead validation,
duplicate merging, event linking, effective dispositions and lead buckets, and
can explain or modify them in a follow-up discussion.

## Where the AI was wrong or unhelpful
The first implementation did not create a parent directory for a supplied database
path, causing SQLite to fail to open it. This was reproduced and fixed before the
final verification run.

## How I verified AI-generated code
I ran the focused test suite (`5 passed`) and the supplied synthetic dataset. I
ingested both drops, generated all five reports, re-ingested the data to verify
idempotency, and verified that ingesting the parent data directory produces the
same report files as ingesting the two drops separately. I also inspected the
database/report flow and confirmed sampled PII is not stored as plaintext.
