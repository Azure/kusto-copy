# Data Model and State Machines

Tracking DB = **track-db** (typed tables from records, primary keys, transactions, triggers,
log persisted to blob storage).  `TrackDatabase` defines the schema; read it for columns.
Update = delete + append.  `await tx.CompleteAsync()` forces the log to be persisted.

## Entities (in `Entity/`)

* `Activity` -> `Iteration` (cursor window, `NextBlockId`) -> `Block` (the work unit).
* Per block:  `BlobUrl` (exported parquet), `IngestionBatch` (serialized ingest operations),
  `Extent` (extents in the temp table).
* Per iteration:  `TempTable`, `PlanningPartition` (planning scratch), `BlockMetric`.
* Keys are records:  `IterationKey(activity, iterationId)`, `BlockKey(iterationKey, blockId)`.
* `Block.Validate()` encodes invariants:  `ExportOperationId` only in `Exporting`;  `BlockTag`
  only in `Queued` / `Ingested`.

## Block state machine

```
Planned -> Exporting -> Exported -> Queued -> Ingested -> ExtentMoving -> ExtentMoved
```

* Back edges:  lost / failed export -> `Planned`;  ingestion failed or **over-ingested**
  (rows for the tag exceed expected) -> `Planned` (re-export);  lost / failed move ->
  `Ingested`.  Returning to `Planned` clears operation ID, tag, `BlobUrl`, `IngestionBatch`.
* Empty export (no blobs) jumps `Exporting` -> `ExtentMoved` with 0 rows.
* `Queued` -> `Ingested` only when temp-table row count for the tag equals the exported count.
* `ExtentMoved` blocks are deleted by `BlockCompletingRunner` (with staging blobs).
* **Enum order matters**:  `BlockMetric` mirrors `BlockState` by int value, and code uses
  `metric < ExtentMoved` for "in flight".  Add states to both enums in the same position.

## Iteration / activity / temp table

* Iteration:  `Starting` -> `Planning` (cursor captured, blocks being created) -> `Planned`
  (block count final) -> `Completed` (no in-flight block; temp table dropped; staging folder
  deleted).  Completed iterations older than the first active one are deleted.
* Activity:  `Active` -> `Completed` when all iterations complete and not in repeat mode.
* TempTable:  `Required` (created JIT at first exported block) -> `Creating` (name persisted
  **before** the Kusto command) -> `Created`.

## Block metrics (derived counters)

Progress and flow control need "blocks in state X per iteration" constantly, so triggers keep
`BlockMetric` rows (`+1` new state, `-1` old state on every block change).

* Deleted `ExtentMoved` blocks are **not** decremented:  that metric counts blocks completed so
  far even after cleanup.  `MovedRowCount` and `TotalPlannedRowCount` are also tracked.
* `BlockMetricMaintenanceRunner` compacts duplicate `(iteration, metric)` rows.
* `QueryAggregatedBlockMetrics` returns every metric (0 if missing).
* Consumers:  progress, planning throttle, iteration completion, ExportOnly stop condition.

## Crash-safety patterns

* **Persist before act** for identifiers (temp table name, operation IDs, tags).
* **Completion is read from Kusto**, not assumed:  tag row counts, `.show operations`.
* Replace whole batches of block records in one transaction so triggers stay correct.
* Changing a record constructor changes the persisted schema;  old tracking folders may break.