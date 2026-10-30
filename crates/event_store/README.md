# nautilus-event-store

[![build](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml)
[![Documentation](https://img.shields.io/docsrs/nautilus-event-store)](https://docs.rs/nautilus-event-store/latest/nautilus_event_store/)
[![crates.io version](https://img.shields.io/crates/v/nautilus-event-store.svg)](https://crates.io/crates/nautilus-event-store)
![license](https://img.shields.io/github/license/nautechsystems/nautilus_trader?color=blue)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/NautilusTrader)

Embedded event store and authoritative log of state-affecting messages for
[NautilusTrader](https://nautilustrader.io).

> [!WARNING]
> **Early alpha**. The API is not stable and may change between versions. Event-store capture,
> replay, and verification workflows are the current focus.

The `nautilus-event-store` crate captures commands, generated events, raw venue reports,
reconciliation outputs, and request/response traffic at the message bus boundary. It persists
those messages as a durable per-run log and exposes read-only verification and replay surfaces.

For the design model, capture boundaries, replay modes, and operational examples, see the
[Event Sourcing concept guide](../../docs/concepts/event_sourcing.md).

## Design contract

The event store is the durable boundary for deterministic engine history.

- The synchronous core is deterministic.
- The cache is a write-through projection, not the source of truth.
- Replay derives state from captured history and named deterministic replay rules.
- The event store records ordered inputs and generated state-affecting messages.
- Anything not recorded is non-state-affecting or named as a deterministic replay rule.
- External I/O becomes replayable when captured as raw reports or commands.

## What this crate provides

`nautilus-event-store` is a single-node, embedded event store for one trading instance. It defines:

- `BusCaptureAdapter`: the seam between message bus dispatch and the writer.
- `EventStoreLifecycleOptions`: runtime-only lifecycle policy for custom encoder registries and
  backend openers.
- `EventStoreWriter`: the append path, batching, high-watermark advancement, and fail-stop signaling.
- `EventStoreReader`: the read-only range scan, point lookup, and replay-facing surface.
- `RedbBackend`: the default on-disk backend, with one `redb` file per run.
- `MemoryBackend`: the in-process backend used by focused tests and simulation-style capture.
- `Verifier`: the library surface for integrity checks over a single run.
- `verify`: the standalone binary for process-isolated verification of sealed run files.
- `plan_redb_retention`: a non-destructive planner for sealed run-file reclaim candidates.
- `ReplayInputPlan`: planned event-store entries in `seq` order, optionally with typed catalog
  slice plans.
- `ReplayInputs`: loaded replay entries in `seq` order, optionally with catalog records grouped
  under selected slices.

The crate does not replace the data catalog, provide OLAP queries, or aggregate multiple trader
instances into a consensus log.

## Operational use today

Verify a sealed run file:

```bash
cargo run -p nautilus-event-store --bin verify -- /path/to/run.redb
```

Verifier exit codes:

- `0`: the run is clean.
- `1`: the run has corrupt findings, or the verifier worker aborted or timed out.
- `2`: the verifier could not open or run against the requested file.

The verifier opens the run through a read-only `redb` handle and reports
`quarantine=not-performed`. A supervisor or operator process owns quarantine policy.

Read a sealed run from Rust:

```rust
use nautilus_event_store::{EventStoreReader, RedbBackend, ScanDirection};

fn inspect_run() -> Result<(), Box<dyn std::error::Error>> {
    let backend =
        RedbBackend::open_sealed_file("./event_store/trader-001/1700000000-cafe0001.redb")?;
    let reader = EventStoreReader::new(backend);
    let high_watermark = reader.high_watermark()?;

    for entry in reader.scan_range(1, high_watermark, ScanDirection::Forward) {
        let entry = entry?;
        println!("{} {}", entry.seq, entry.topic);
    }

    Ok(())
}
```

## Lifecycle options

`EventStoreLifecycle::boot(...)` keeps the default behavior: it opens `RedbBackend` and installs
`default_registry()`. Advanced callers can pass runtime-only options through
`EventStoreLifecycle::boot_with_options(...)` without changing serialized `EventStoreConfig`.

Register a custom bus payload before the lifecycle opens a run:

```rust
use bytes::Bytes;
use nautilus_event_store::{
    EncodedPayload, EncoderRegistry, EventStoreLifecycleOptions,
};
use ustr::Ustr;

#[derive(Debug)]
struct AuditRecord {
    payload: Bytes,
}

let mut registry = EncoderRegistry::new();
registry.register::<AuditRecord, _>(Ustr::from("AuditRecord"), |record| {
    Ok(EncodedPayload::without_indices(record.payload.clone()))
});

let options = EventStoreLifecycleOptions::new().with_encoder_registry(registry);
// Pass `options` to EventStoreLifecycle::boot_with_options(...).
```

The same options type accepts a backend opener. The default opener remains `RedbBackend`; tests or
simulation harnesses can supply an opener that returns `MemoryBackend` or another `EventStore`
implementation.

## Storage model

The default backend is [`redb`](https://crates.io/crates/redb), a pure-Rust ACID key-value
store. The backend uses one file per run:

```text
<base>/<instance_id>/<run_id>.redb
```

Each file stores:

- Entries keyed by monotonic `seq`.
- Secondary indices for `client_order_id` and `venue_order_id`.
- A manifest written at run start and sealed at run end.
- An optional snapshot anchor for cache restore.

Every commit uses `Durability::Immediate`. The high-watermark advances only after the backend
acknowledges durability.

## Immutable prefixes of an active run

`EventStoreLifecycle::start_prefix_checkpoint` (or `EventStoreSession::start_prefix_checkpoint`
for a directly owned session) admits one optional export job through the existing
bounded writer channel. The writer flushes earlier entries, records the durable high-watermark
and current manifest, and hands off one native `redb::ReadTransaction`. The worker copies native
encoded entry rows and their original secondary index rows in bounded transactions while the
source continues appending. It does not copy an open database file or seal the source.
The lifecycle rejects disabled, unopened or sealed capture handles. Writer ownership remains
private; lower-level writer users can call `EventStoreWriter::start_prefix_checkpoint` directly.

The destination uses a `.checkpoint` extension and explicit `PrefixCheckpoint` metadata. Its
source manifest remains `Running`; no `RunEnded` event is synthesized. A private staging artifact
is published with an atomic, no-overwrite hard link only after all rows and completion metadata
have committed. The destination parent directory must support hard links and directory sync.
If directory sync fails after publication, `CheckpointError::PublicationUncertain` identifies
the completed artifact which exists at the destination; inspect that path before retrying.
Ordinary run openers reject
checkpoint metadata, even if the file is renamed to `.redb`.

`PrefixCheckpointReader` explicitly opens completed prefix artifacts. Its verifier combines the
existing native entry/hash/index checks with digests of the original encoded entry and index
tables. This proves structural identity through the frozen boundary, not capture completeness,
availability of external dependencies, or exact replay. The cache snapshot anchor is an opaque
reference, not full initial kernel state; exporting its referenced blob and proving replay remain
separate work. Existing verification may allocate memory proportional to the prefix.

The existing native verifier isolates checkpoint reads in its worker subprocess:

```bash
cargo run -p nautilus-event-store --bin verify -- --prefix-checkpoint /path/to/prefix.checkpoint
```

The explicit mode prevents treating a running source as a sealed run or a completed checkpoint.
It reuses the verifier's timeout and abnormal-worker-exit handling, and never repairs, seals or
quarantines its input. Entry/index table digests participate in the clean result. Marker sidecars
and cache snapshot blobs are not exported; the report retains those limitations. The library
reader and its `verify` method remain in-process APIs, so use the binary for potentially damaged
files.

Per-job limits must be supplied explicitly: transaction rows and bytes, destination cache and
file bytes, source-file growth while the read view is pinned, and a cooperative deadline.
Dropping or cancelling the job requests release of its native read view and cleans private
partial output. Bounds and cancellation are checked between native reads/commits; operating
system I/O cannot be forcibly interrupted. One active job prevents unbounded accumulation of
pinned views. Actual MVCC page growth and shared-device latency still require workload
qualification; these limits are operational export bounds, not trading capacity guarantees.
Optional admission or destination failures do not trigger capture's required halt callback;
a failed native writer flush retains the existing fail-stop behavior. Other backends default to
explicit unsupported behavior, and deterministic simulation does not spawn export workers.

The ignored `checkpoint_workload_qualification` test is an explicit release diagnostic. It pairs
writer-only and concurrent-export cases over a fixed synthetic dataset, reporting submit latency,
backlog, export completion time, source-file growth and native allocated-page samples. Its native
write-transaction stats observer runs in both cases; observation overhead is included. Run it
separately with `--ignored --nocapture --test-threads=1` and retain its JSON output with the tested
platform, filesystem, source and build identity. The measurements qualify only that workload,
not a deployment capacity, full initial kernel state or same-strategy replay.

## Failure and verification model

The verifier is intentionally process-isolated. Some corrupted `redb` files can panic on open or
first read, and the release profile builds with `panic = "abort"`. The `verify` binary delegates
the scan to a worker subprocess so a bad run file aborts the worker, not the caller.

Integrity checks include:

- Recomputing every entry hash.
- Detecting gaps inside the high-watermark.
- Checking table-key and embedded-`seq` agreement.
- Cross-checking secondary indices against entries.
- Validating manifest status and high-watermark fields.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `defi`: Enables DeFi (Decentralized Finance) support.
- `live`: Enables live-runtime support through `nautilus-common`.
- `persistence`: Enables Parquet catalog replay support through `nautilus-persistence`.

## Documentation

- [Event Sourcing concept guide](../../docs/concepts/event_sourcing.md): design, replay, recovery,
  and operational examples.
- [docs.rs](https://docs.rs/nautilus-event-store): generated Rust API reference.

## NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## License

The source code for NautilusTrader is available on GitHub under the
[GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

<img src="https://github.com/nautechsystems/nautilus_trader/raw/develop/assets/nautilus-logo-white.png" alt="logo" width="300" height="auto"/>

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
