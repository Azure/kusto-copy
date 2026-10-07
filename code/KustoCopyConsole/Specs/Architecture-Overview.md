# Architecture Overview

Start here.  These specs record **design decisions and non-obvious behavior**, not what the
code already says.  Public how-to docs are not repeated:  see the
[root README](../../../README.md) and [documentation](../../../documentation/README.md)
(setup, parameters, YAML, progress).  Snapshot:  version 1.8.1, .NET 10.  Code wins on conflict.

## What it is

`kc` is a .NET console app that copies Kusto tables (ADX / Fabric Eventhouse) to Kusto tables
at petabyte scale, exactly-once, resumable, preserving extent creation time.  It only
**orchestrates**:

```
Source --.export parquet--> ADLS Gen2 staging --queued ingest--> temp table --.move--> Destination
```

All progress lives in an embedded DB (**track-db**, author's own library,
https://github.com/vplauzon/track-db/) whose log is stored in the staging container
(`<first staging dir>/tracking`).  Kill and restart with the same parameters:  it resumes.

## Design principles

1. **Persisted state machines, no in-memory state.**  The unit of work is a *block* (bounded
   `ingestion_time()` range, at most 16 M rows) whose `State` advances in transactions.
2. **Exactly-once via a destination temp table.**  Ingest into `kc-<table>-<guid>`, verify by
   tag + row count, then metadata-only `.move extents`.  Update policies fire on move only.
3. **Independent runners talking only through the DB.**  One loop per stage; a runner queries
   blocks in its input state and writes them back in its output state.
4. **Async Kusto commands tracked by persisted operation IDs**; lost / failed operations send
   the block back one step.
5. **Backpressure from capacity**:  export bounded by source `DataExport` capacity, Kusto call
   concurrency by cluster query capacity, planning by number of in-flight blocks.
6. **Fail fast on permanent errors**:  first runner failure cancels all and exits non-zero.

## Code map (`code/KustoCopyConsole`, all `internal`)

* `Program.cs`, `CommandLineOptions.cs`:  CLI entry
* `JobParameter/`:  CLI + YAML merge, validation, credentials
* `Entity/`:  track-db records, keys, state enums, `TrackDatabase` (schema + triggers)
* `Runner/`:  all pipeline logic, one class per stage (`Source/`, `Destination/` subfolders)
* `Kusto/`:  SDK wrappers, **all KQL / control-command text**, per-cluster priority queues
* `Concurrency/`:  `PriorityExecutionQueue`, `AsyncCache`
* `code/KustoCopyTest`:  xUnit project, empty.  `deployment/`:  CI version stamp, test infra
* `design/`:  historical drafts (e.g. multiple destinations per activity, not implemented)

## Glossary

* **Activity**:  one source-table to destination-table copy (+ optional KQL query).
* **Iteration**:  one pass over a Kusto cursor window `(CursorStart, CursorEnd]` of an activity.
* **Block**, **planning partition** (day / minute slices used to discover blocks),
  **temp table**, **block tag** (`drop-by:kusto-copy|block-<id>;<guid>`).
* **Copy mode**:  BackfillOnly / BackfillAndNew / NewOnly.  **Copy flow**:  All / ExportOnly /
  IngestOnly (multi-tenant:  export and ingest run as separate processes and identities).

## Known gaps and quirks

* No automated tests.
* `NewOnly` doesn't skip history:  iteration 1 always starts with an empty cursor.
* Public docs say export parallelism is max(capacity, 20); code uses min(capacity, `--export`).
* `TempTableCreatingRunner`:  the leftover-table drop guard tests `IsNullOrWhiteSpace(
  TempTableName)`, which looks inverted.
* `DmCommandClient` and `ShowMoveDetailsAsync` are unused / unimplemented.

## Other specs

* [Runtime-Architecture.md](Runtime-Architecture.md)
* [Data-Model-And-State-Machines.md](Data-Model-And-State-Machines.md)
* [Copy-Pipeline.md](Copy-Pipeline.md)
* [Kusto-Storage-Infrastructure.md](Kusto-Storage-Infrastructure.md)
* [CodingStandard.md](CodingStandard.md)