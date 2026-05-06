# Anomaly-Driven Page Format

The chapters in [Distributed Database](../SUMMARY.md) are organized around concrete failures discovered while running the [Jepsen bank test](../distributed_database/bank_test.md) against the system: one chapter per anomaly, each chapter walking from "what we observed in the test" to "what the fix has to guarantee" to "how we close it." This appendix documents the structural template these chapters share, so future chapters of the same kind stay consistent.

The reference examples are [Read Skew](../distributed_database/read_skew.md), [Lost Updates](../distributed_database/lost_update.md), [Multi-Statement Transactions](../distributed_database/multi_statement_transactions.md), and [Shadowed Writes](../distributed_database/shadowed_writes.md). New pages should mirror their structure.

## Why This Structure

The pages are written backwards from a failing test. That dictates a specific narrative arc:

1. **Frame the anomaly abstractly first.** A reader who's never seen this failure shouldn't have to reverse-engineer what it is from the test output. Open with the textbook framing.
2. **Then ground it in the actual evidence.** Once the reader knows the *kind* of failure they're looking at, walk them through the specific instance from the test history -- timestamps, on-disk versions, code paths.
3. **Pin down the invariant before discussing fixes.** Many fixes share one underlying invariant. Naming the invariant separates "what the fix has to enforce" from "how we choose to enforce it."
4. **Survey the solution space; don't prescribe.** Different systems make different trade-offs for the same invariant. Show the menu before pointing at one item.
5. **Compare, then commit.** A matrix forces honest accounting of cost vs. correctness. The implementation section is the small last step where one option becomes code.

This shape is borrowed from how database papers (Berenson et al., Bailis, Adya) frame anomalies, with the small-db twist of putting concrete log/disk evidence before the architectural discussion.

## The Six Sections

Every anomaly-driven page has these top-level sections, in this order:

```
# <Page Title>

## The Anomaly
## The Problem (in this system)
## What "Fixing It" Has to Guarantee
## The Solution Space
## Comparison
## Implementing <chosen solution>   (or: ## What's Currently in the Code)
```

The title should name the anomaly in standard literature terms when one exists (read skew, lost update) and in plain English otherwise (shadowed writes). Avoid clever names; the page title is also the index entry.

### 1. The Anomaly

The general theory section. Four subsections, each opening with a bolded phrase:

- **Behavior.** What an outside observer (the application, the bank checker, a human reading a SELECT result) sees go wrong. One paragraph. Do not dive into mechanism here -- just describe the symptom in user-visible terms.
- **Root cause.** One paragraph naming the underlying mechanic in general terms. This is where you say things like "MVCC selects by lex-largest version_ts" or "read-modify-write is non-atomic." Stay general; resist the urge to mention small-db internals here.
- **Does this happen in single-server databases?** A direct yes/no, then one paragraph explaining why. This question is load-bearing because it tells the reader whether the problem is fundamental to concurrency (yes -- e.g., read skew, lost update) or specific to distribution (no -- e.g., shadowed writes from cross-coordinator clock skew). Cite the relevant Berenson 1995 anomaly number when applicable.
- **Typical solutions.** Three families of approach used in practice, each with one or more real-system pointers. Each family is a bullet with a bolded name and a paragraph; do not number them at this stage (numbered enumeration is for §4). Cite production systems by name (Spanner, CockroachDB, Postgres, MySQL InnoDB, Cassandra, DynamoDB) -- they ground the abstract families in something the reader can look up.

This section is the abstract-first framing. A reader who already knows this anomaly can skim it. A reader who doesn't gets the textbook view in 200-400 words.

### 2. The Problem (in this system)

The concrete-evidence section. Open with what the failing test actually reported -- the totals, the operation, the values. Then walk through the on-disk evidence:

- Identify the operations involved by their op-index in the Jepsen history.
- Show the wall-clock timeline of writes (or commits, or whatever's relevant) on each affected node, with `version_ts` values.
- Where helpful, include an ASCII diagram of the timeline. Diagrams are not optional for failures with non-obvious orderings -- shadowed writes, read skew, sequential dispatch races. See the diagram in [Shadowed Writes](../distributed_database/shadowed_writes.md) for the canonical example.
- End the section with a short rundown of "what every layer beneath the failure delivered correctly" vs "what specifically went wrong." This separates the anomaly from collateral correctness.

The point of this section is to ground the rest of the page in evidence the reader can verify in `small-db-jepsen/store/latest/`. Conjecture is forbidden here; everything in this section should be backed by a log line, a disk scan, or a history entry.

### 3. What "Fixing It" Has to Guarantee

One paragraph, often one sentence in **bold**, naming the invariant the fix must enforce. Then an optional second paragraph clarifying scope (per-row vs. global, per-statement vs. per-transaction).

This section is short. It's the bridge between "here's what's broken" and "here's the menu of fixes" -- a clean restatement of the property we need, without specifying *how* to get it.

Examples:

- Read skew: "A SELECT must observe rows as of one consistent point in time."
- Lost update: "For every committed transaction T that wrote row R, the value T wrote was computed from a pre-image that was still the latest committed version at the moment T's write took effect."
- Shadowed writes: "For any row R, the lex order of version_ts values written to R must match the chronological order in which their commits took effect on R."

### 4. The Solution Space

Numbered (`### 1. <name>`, `### 2. <name>`, ...), one section per option. The order should reflect ascending complexity / cost, so the simplest answer comes first.

Each option's body has two parts:

1. A one-paragraph prose description of the mechanism.
2. A standardized table:

```markdown
| | |
|---|---|
| **Implementation** | Code-change estimate (e.g., "~10 lines in update.cc"; "New module"; "Wire-format change") |
| **Granularity** | Per-table / per-row / per-key / cluster-wide / etc. |
| **What it fixes** | The specific anomaly + any incidentally-fixed siblings |
| **Cross-node** | Does it require cross-coordinator coordination? Does it solve cross-node variants of the anomaly? |
| **Concurrency cost** | How much it serializes; what's blocked by what |
| **Client visible** | Aborts vs. waiting; latency floor; retry obligations |
```

After the table, a paragraph or two of commentary -- known production users, gotchas, when this option is right, when it's wrong. Cite real systems consistently; the more concrete this section is, the more useful the page is.

Aim for 5-10 options. Including an option that's clearly overkill (Spanner-grade 2PC + DLM, full SSI) is fine and even useful -- it gives the reader the upper-bound design and lets them see why we don't need it.

### 5. Comparison

A single matrix table summarizing the options:

```markdown
| Approach | Correctness | Code change | Concurrency cost | Aborts to client? | Granularity | Cross-node? |
```

Followed by 3-5 bullet points titled "Reading the matrix:" that pull out the key takeaways. This is where the reader sees the optimization landscape at a glance and the page's recommendation crystallizes.

The bullets should be axis-of-comparison observations: "pessimistic vs. optimistic is the first axis," "in-process vs. in-storage is the second axis," "options X and Y solve more than asked, which is fine if we'll need that later but expensive today." Avoid restating individual rows.

### 6. Implementing <chosen solution>

Where the page commits to one option and walks through its design. Sub-headings:

- **The new module / type / data structure** -- show the public API (struct, class, function signatures) with brief commentary. Real header excerpts work well; sketch them in the page if the actual code isn't checked in yet.
- **The flow change** -- a numbered list or pseudocode showing how the existing request path changes. Reference the exact files (e.g., "`src/execution/update.cc`'s `dispatch=false` branch becomes:").
- **Edge cases / scope decisions** -- bulleted list of things explicitly out of scope, with one-sentence rationale each. Empty schema, single-pk WHERE, lock map GC, etc.
- **What This Buys (and What It Doesn't)** -- the closing subsection. Two halves:
  - **Buys.** What new failure modes go away after this lands.
  - **Doesn't.** What remains broken; ideally with a forward-pointer to the next page.

If the page has *not* committed to an implementation -- e.g., the fix was deferred or removed -- replace this section with **What's Currently in the Code**: a short paragraph saying which option is in or out of the codebase, what the consequence is (the failure documented in §2 will continue to occur), and that re-enabling the chosen fix requires explicit user direction. See [Shadowed Writes](../distributed_database/shadowed_writes.md) for an example.

## Style Notes

- **Diagrams.** ASCII art only. The book is plain Markdown via mdBook with no diagram plugin. Use box-drawing characters (`┌─┐│└─┘├─┤├──►`) sparingly and only where they add clarity. A wall-clock timeline with arrows is the most common shape.
- **Tables.** Keep narrow enough to fit a typical browser column without horizontal scrolling. Two-column tables for option details; six- or seven-column matrices for comparisons.
- **Code blocks.** When showing code, prefer real excerpts with file paths in the surrounding prose: `src/server/stmt_handler.cc`'s `commit_txn` does X. Do not paste hundreds of lines. A 5-15 line excerpt is usually enough; the reader can follow links to the full file.
- **Tone.** Direct, evidence-grounded, no marketing. Avoid hedges ("perhaps," "might be") when an evidence trace settles the question. Conversely, distinguish observed facts from inferred conclusions clearly.
- **Length.** Target 250-400 lines per page. Anomaly section ~50, problem section ~50, invariant section ~10, solution space ~80-150 (depending on option count), comparison ~30, implementation ~50-100.
- **Citations.** Cite real-world systems by name when describing a solution family. Cite the original literature once per page if it's a named anomaly (Berenson 1995 + DDIA chapter 7 is the standard pair).
- **Cross-references.** Link to sibling anomaly pages when one's fix is the prerequisite of another, or when an anomaly observed here is closed by a fix on another page. The Distributed Database section's chapters compose -- make the dependencies explicit in prose.

## Anti-Patterns

These ruin the structure:

- **Skipping §2 (Problem in this system).** Don't write a page that's all theory. The whole point is that the failure was *observed*, not predicted. If there's no concrete trace, the page is premature.
- **Conflating §3 (invariant) with §4 (solutions).** The invariant must be solution-agnostic. If you find yourself writing "the invariant is: take a row lock," back up -- that's a solution.
- **Numbered solutions that aren't comparable.** Every option in §4 must close the anomaly in §1. Options that "almost work" or "partially mitigate" belong in commentary, not as numbered choices.
- **A comparison matrix that's all "yes."** If every option scores the same on every axis, you've picked bad axes. Find ones that distinguish.
- **An implementation section that includes options not from §4.** §6 must commit to one of the numbered options -- not invent a new one. If the implementation needed a hybrid, that hybrid is itself a numbered option in §4 and gets a row in the matrix.
- **TLDR boxes at the top.** The "Anomaly" section is the TL;DR; it's the first 4 paragraphs. A separate TL;DR is redundant. (Read Skew briefly had one and it was removed for exactly this reason.)

## When To Use This Format

This template fits pages where:

- The page is centered on a specific failure mode the test surfaced.
- There's a well-defined invariant the fix must enforce.
- There's a non-trivial solution space (at least 3-4 distinct options worth comparing).

It does *not* fit:

- Architectural overviews (use [Architecture](../architecture.md)'s style instead).
- Tutorials or runbooks (use a flat how-to style).
- Narrative essays about a journey or trade-off (use the appendix's [AI-Coding-Era Narrative Styles](./ai_coding_narrative.md)).
- Single-fix pages where there's only one viable approach (compress §4 and §5 into one section, or skip them entirely).

When in doubt: if the page can't honestly fill all six sections, it isn't an anomaly-driven page. Pick a different structure.
