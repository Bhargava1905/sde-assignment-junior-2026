# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used
Codex (OpenAI)

## What I used them for
Used it to review the assignment specification, discuss the SQLite-ledger design,
scaffold the initial implementation, and draft tests and documentation.

## What I wrote or decided myself
Before submitting, replace this section with an accurate description of what you
personally reviewed, wrote, and decided. You should only submit code you can
explain and modify live.

## Where the AI was wrong or unhelpful
The first implementation did not create a parent directory for a supplied database
path, causing SQLite to fail to open it. This was reproduced and fixed before the
final verification run.

## How I verified AI-generated code
The implementation was exercised with the focused test suite and the supplied
synthetic dataset, including a re-ingest to verify idempotency. Before submitting,
also read the code line by line and perform your own verification.
