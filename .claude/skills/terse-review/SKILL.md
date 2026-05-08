---
name: terse-review
description: Autonomous long-running cleanup pass — trims verbose comments, deduplicates logic, removes unused APIs, tightens verbose tests, then runs the full test suite and commits. Adds nothing. Invoke with `/terse-review [path]`.
---

# Terse Review

Cleanup-only autonomous agent. Walks `src/` and `test/` (or the supplied path) and applies the checklist below. Do not propose. Do not ask. Apply.

CLAUDE.md `## Code Style` and `## Unit Test Style` are the source of truth. This skill is the application checklist.

## Mandate

- **Only simplify, remove, rename, or extract.** Never add features, public APIs, abstractions, options, or "scaffolding for later."
- **Never add a doc comment that wasn't there before** unless a careful reader would still miss something.
- **Never modify behavior.** Tests should still describe the same invariants. Builds should still pass with the same artifacts.
- If you find a real bug, stop and surface it; do not fix it inside the cleanup pass.

## Comments — default to none

Delete or trim if any apply:

- **Restates the code.** `// Pause so wall clock advances` above `sleep_for(50ms)` — no.
- **Narrates the body.** Step-by-step prose mirroring each statement — no.
- **Names APIs the field's name already implies.** `last_advertised_ts_` does NOT need `// Updated by WaitUntilSafeToRead` — the name implies an advertising API; readers can grep. State only what the name does NOT convey (e.g., `// monotonically increasing`).
- **Uses internal jargon callers don't think in.** `// Returns the stored bound` — `stored bound` is the implementation, not the contract.
- **Mentions change history.** `// Replaces X`, `// Used to do Y`, `// Added for issue #42` — git knows.
- **Documents the obvious.** `// Returns true on success` on `IsValid()` — no.
- **Enumerates callers or implementation paragraphs in class headers.** A class doc is one or two sentences naming the role. No bulleted caller list. No "Cleanup is lazy: …" paragraph.

When a comment is warranted:
- **Field/member**: one short clause for what the name doesn't convey.
- **Public API**: up to three short sentences — what it does, what the caller can rely on, what `false`/error means. Skip parts obvious from the rest.
- **Class**: one or two sentences for the role.

EXPECT/ASSERT messages state the rule, not the outcome. Yes: `<< "reader at write_ts must not pass while in-flight"`. No: `<< "failed to read at write_ts"`.

## Tests

- Test name 1–4 words. Long noun-phrases are sentences, not names.
- Lead comment: one sentence stating the invariant. Not "this test does X then asserts Y."
- Body uses numbered steps (`// 1.`, `// 2.`). Step comments describe the phase, not the next line.
- No inline arg comments (`Register(/*txn_id=*/777, …)`). IDE shows them.
- Small literal values when only ordering matters: `2`, not `1'000'000'000'000`.
- Variables named by role (`writer`, `concurrent_reader`), not index.
- Extract a helper when a check pattern repeats.
- **No test-only APIs on production classes.** No `ClearForTest`, no `friend class FooTest`. Use a separate test binary if needed.

## Logic

- **Duplicated scans/loops** → extract one helper, call from each.
- **Unused public APIs** → delete. No "for future use."
- **Test-only branches in production code** → delete.
- **Half-finished implementations / TODO scaffolds** → finish or remove.
- **Sentinel constants with one user** → fold inline.
- **State that exists only to feed a comment** → delete the comment, then the state.

## Workflow

1. **Default scope**: `src/` and `test/unit/`. If `<path>` is supplied, use that instead.
2. Walk file by file. For each file:
   - Read it.
   - Apply the checklist.
   - Apply fixes via Edit (not Write — surgical).
3. After each file (or small batch), build the affected target. On build failure, revert the offending edit and try a smaller change. Don't move on with a broken build.
4. Run the unit test that touches the changed code.
5. Continue until the entire scope is processed. Don't stop at the first finding. Don't ask between files.

## Pre-commit gate

Before any commit, every step must pass. If any fails, fix and re-run; never `--no-verify`.

1. **Format/lint**: `./scripts/format/lint.sh`
2. **Build**: `./scripts/setup/build.sh`
3. **Unit tests**: every binary under `./build/debug/test/unit/`
4. **Integration tests**: `./scripts/test/test.sh`
5. **Jepsen** (heavyweight; preflight first):
   - `uv run ./scripts/setup/check-env.py` — fix any ✗
   - `cd small-db-jepsen && lein run test-all --node america --node europe --node asia --ssh-private-key ~/.vagrant.d/insecure_private_key --username vagrant`

Then commit. Use the project's `chore(cleanup): …` style; one short subject summarizing what was trimmed.

## Out of scope

- Don't refactor working code for taste if it adds risk to a public API.
- Don't change function signatures without checking call sites.
- Don't introduce new abstractions, options, or features.
- Don't rewrite a file when an Edit suffices.
- Don't fix bugs you discover; surface them and stop.
