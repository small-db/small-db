# Testing Techniques: Property-Based Testing vs. Fuzzing

This page explains two testing techniques that are easy to confuse — *property-based
testing (PBT)* and *fuzzing* — and shows where each fits in small-db's surface
area. They are not substitutes; they find different bugs.

The one-line distinction:

> **Property-based testing needs an oracle. Fuzzing doesn't.**

An *oracle* is something that tells you "this output is wrong." PBT requires
you to write one (the property). Fuzzing uses an implicit one (the process
crashed, a sanitizer fired, an assertion tripped). That single difference
cascades into everything else.

## Side-by-side

| Dimension | Property-based testing | Fuzzing |
|---|---|---|
| **Oracle** | Explicit predicate you write | Crash / sanitizer / assertion |
| **Input shape** | Typed, structured (you control generators) | Raw bytes, mutated by the fuzzer |
| **Guidance** | Random, sometimes shrink-driven | Coverage feedback (which branches got hit) |
| **Per-run cost** | Seconds; runs in CI on every commit | Hours to days; runs continuously |
| **Failure artifact** | Shrunk minimal counterexample | Reproducer input (often messy bytes) |
| **Best for** | Logic correctness, invariants, refactor safety | Memory safety, parsers, crash resistance |
| **Tools (C++)** | rapidcheck | libFuzzer, AFL++, honggfuzz |
| **Tools (Rust)** | proptest, quickcheck | cargo-fuzz, AFL.rs |

## Same surface, different technique — small-db key encoding

Take the small-db key encoder: `encode(schema, table, pk, ts) → bytes` and its
inverse `decode(bytes) → (schema, table, pk, ts)`. Both PBT and fuzzing apply
here, and they find *different* bugs.

### Property-based test

```rust
// Property: decode is the inverse of encode for any valid tuple.
proptest! {
    #[test]
    fn encode_decode_roundtrip(
        schema in "[a-z]{1,16}",
        table  in "[a-z]{1,16}",
        pk     in any::<u64>(),
        ts     in any::<u64>(),
    ) {
        let bytes = encode(&schema, &table, pk, ts);
        let (s, t, p, t2) = decode(&bytes).unwrap();
        prop_assert_eq!((schema, table, pk, ts), (s, t, p, t2));
    }
}
```

- **Inputs are structured.** The generator produces only well-formed tuples.
- **Oracle is explicit.** "Round-trip equals input."
- **Bug it finds:** if `encode` writes the timestamp little-endian but `decode`
  reads big-endian, the property fails immediately. proptest *shrinks* the
  failing input to something minimal like `("a", "a", 0, 1)` so you see the
  bug, not the noise.
- **Bug it does not find:** `decode(b"\xff\xff\xff\xff\xff\xff\xff\xff")`
  segfaulting. The generator never produces that input because it only
  generates *encoded* outputs.

### Fuzz test

```rust
// fuzz/fuzz_targets/decode.rs
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = small_db::decode(data);  // must not crash, panic, or UBsan-fire
});
```

- **Inputs are arbitrary bytes.** Most are malformed; the fuzzer mutates the
  ones that reach deeper code paths (coverage feedback).
- **Oracle is implicit.** Process crashed → bug. Process exited cleanly with
  `Err` → fine.
- **Bug it finds:** a maliciously crafted length prefix saying "schema is
  4 GB long" causes `decode` to call `Vec::with_capacity(4_000_000_000)` and
  abort. Or a UTF-8 boundary bug in schema-name parsing trips an out-of-bounds
  read that ASan catches. These are bugs the round-trip property would never
  see, because the round-trip property never produces malformed input in the
  first place.
- **Bug it does not find:** "decode returns `("foo", "foo", 1, 2)` when the
  bytes encode `("foo", "foo", 1, 3)`." There's no oracle for "wrong value" —
  decode returned successfully, the process didn't crash, fuzzer is happy.

### What you learn

- **PBT defended a correctness contract on valid inputs.**
- **Fuzzing defended crash-resistance on hostile inputs.**

You want both. They are not substitutes.

## Where each technique is the only sensible choice

### Pure PBT territory: MVCC snapshot reads

```rust
proptest! {
    #[test]
    fn snapshot_read_returns_latest_visible_version(
        // a write history: list of (commit_ts, key, value)
        history in prop::collection::vec(
            (any::<u64>(), any::<u8>(), any::<Vec<u8>>()), 0..100),
        read_ts   in any::<u64>(),
        probe_key in any::<u8>(),
    ) {
        let mut engine = Engine::new();
        for (ts, k, v) in &history {
            engine.put_at(*ts, *k, v.clone());
        }

        // Reference oracle: compute the expected answer in plain Rust.
        let expected = history.iter()
            .filter(|(ts, k, _)| *ts <= read_ts && *k == probe_key)
            .max_by_key(|(ts, _, _)| *ts)
            .map(|(_, _, v)| v.clone());

        prop_assert_eq!(engine.get_at(read_ts, probe_key), expected);
    }
}
```

Here PBT is uniquely powerful because **the spec has an obvious reference
implementation** (filter and max in plain Rust), and the engine's job is to
match it under any history. Fuzzing this surface would find crashes but not
"returns the version with `commit_ts = 7` when it should return the one with
`commit_ts = 8`," because the fuzzer has no idea what the right answer is.

### Pure fuzzing territory: pg_wire protocol decoder

The Postgres wire protocol parser ingests untrusted bytes from clients. The
relevant property is "any input bytes → either a valid parsed message or a
clean error, never a crash, OOB read, or panic." That's an implicit oracle
(crash = bug), exactly what fuzzing is built for.

```rust
// fuzz/fuzz_targets/pg_wire_decode.rs
fuzz_target!(|data: &[u8]| {
    let mut buf = data;
    let _ = small_db::pg_wire::decode_message(&mut buf);
});
```

Trying to PBT this is awkward — what's the oracle? You'd end up writing a
generator that produces only valid wire frames, which means you'd only test
the happy path. The interesting bugs are in the *malformed* inputs, and a
mutation-driven fuzzer with coverage feedback explores that space far more
efficiently than `prop_oneof![valid_frame(), random_bytes()]`.

## The hybrid: differential fuzzing

The two techniques converge in **differential fuzzing** — coverage-guided
fuzzing with an explicit oracle, usually "two implementations agree."

```rust
fuzz_target!(|ops: Vec<Op>| {  // Vec<Op> via the `arbitrary` crate
    let mut engine = small_db::Engine::new();
    let mut reference = std::collections::BTreeMap::<(Key, Ts), Value>::new();

    for op in ops {
        apply(&mut engine, &op);
        apply_reference(&mut reference, &op);
    }

    // Cross-check: every key/timestamp pair must match.
    for ((k, ts), v) in &reference {
        assert_eq!(engine.get_at(*ts, *k).as_ref(), Some(v));
    }
});
```

This is morally a property test (oracle = reference implementation) wearing
fuzzing's clothes (coverage-guided, mutation-driven, runs for hours, finds
weird operation sequences a `proptest` generator wouldn't). RocksDB, sqlite,
and BoltDB all use variants of this pattern. For small-db the storage engine
and the MVCC engine are both natural fits.

## Allocation guide for small-db

| Subsystem | Best fit | Why |
|---|---|---|
| Key encoder/decoder | PBT (round-trip) + fuzz the decoder | Round-trip property + crash resistance on malformed bytes |
| `pg_wire` decoder | Fuzz | Untrusted input, implicit oracle |
| Catalog protobuf decoder | Fuzz | Same |
| MVCC snapshot read | PBT | Reference implementation is trivial |
| Compaction | PBT or differential fuzz | Pre/post equivalence is the property |
| Memtable freeze race | PBT with `loom` or stress tests | Concurrency, not bytes |
| Gossip convergence | PBT (model-based) | Stateful, with a convergence predicate |
| SQL planner | Differential fuzz vs. SQLite/Postgres on a SQL subset | Oracle = "another DB agrees" |

## The mental rule

When designing a test:

1. **Can you write down what "correct output" means as a function of input?**
   → PBT. Use the function as the oracle.
2. **Is the only thing you care about that the code doesn't crash or trip a
   sanitizer on hostile bytes?** → Fuzz.
3. **Both?** → Differential fuzz: PBT-style oracle, fuzzer-style input
   exploration.

## Cross-reference

The narrative-style appendix's [§2 "Spec-First — and What 'Property' Means"](./ai_coding_narrative.md#2-spec-first--and-what-property-means)
defines *property* as the universal claim a property-based test checks. This
page goes one level lower and shows how that definition cashes out against
fuzzing in practice.
