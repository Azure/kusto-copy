# Copy Pipeline

Why each stage works the way it does.  Class names are in `Runner/`.

## Iterations (`IterationManagementRunner`)

* Cursors give a consistent, non-overlapping window of ingested data even while the source
  keeps ingesting:  `cursor_current()` at start, then `cursor_after(start)` and
  `cursor_before_or_at(end)`.  Needs a strongly consistent source workload group.
* A new iteration starts from the previous `CursorEnd`.  Created when none exists, or (not
  BackfillOnly) at process start with no active iteration, or when `IterationPeriod` elapsed
  and fewer than 2 iterations are open.
* On completion:  delete staging folder, drop temp table, mark `Completed`.

## Planning (`PlanningRunner`)

Turns a cursor window into blocks (at most 16 M rows) without one giant query or millions of
blocks at once.

* Throttle with hysteresis:  start planning below 600 in-flight blocks, keep going until 1000
  (counted across all activities on the same source cluster).
* Partition hierarchy persisted in `PlanningPartition` (resumable):  root -> 1-day bins ->
  1-minute bins (merged up to 4 G rows) -> blocks.  Partitions of at most 4 G rows go to block
  loading.
* Block loading query lists the window's extents with the `execute_show_command` plugin (must
  not be disabled) to capture each block's **extent creation time**, then cuts the ordered
  rows into groups of at most 16 M with `row_cumsum`.
* The user's `KqlQuery` is spliced into planning **and** export, so counts reflect the query.
  It must not aggregate (breaks `ingestion_time()`).

## Export (`ExportingRunner`, `AwaitExportedRunner`)

* Capacity = source `DataExport` capacity, capped by `--export`.
* `.export async` to parquet into a per-block folder via write-only SAS URIs (one per staging
  directory, so Kusto spreads blobs across storage accounts to dodge throttling), with
  `persistDetails=true` so blobs are listed later from the operation details.
* On completion:  `BlobUrl` rows, `ExportedRowCount`, and a `TempTable(Required)` row if the
  iteration has none.

## Temp table (`TempTableCreatingRunner`)

Created from the destination table's schema (the destination table must exist) with:  tag
retention policy removed, fast ingestion batching, **merge disabled** (extents must stay stable
until moved), partitioning removed, `restricted_view_access`.  Folder `kc`.

## Ingestion (`QueueIngestRunner`, `AwaitIngestRunner`)

* Queued ingestion through the destination's `ingest-<host>` endpoint, using read SAS URLs,
  `CreationTime` = block creation time (preserves retention), extent tag = block tag.
* Success = rows for the tag in the temp table equal the expected count.  Greater means
  duplicates -> re-export.  `FailureDetection` checks only the **oldest** queued block's
  ingestion operations (`Failed` / `Cancelled` / `PartialSuccess` -> re-export).

## Move (`MovingExtentRunner`, `AwaitMovedRunner`)

* `.move async extents` with `setNewIngestionTime=true`;  capacity = destination node count.
* After completion `.drop table ... extent tags` removes our tags from destination extents.
* Update policies run here (temp table has none), so destination policies drive this stage's
  speed.

## Copy modes and flows

* BackfillOnly:  one iteration.  BackfillAndNew:  more iterations at start / every
  `IterationPeriod`;  without a period the process exits after the iteration.
* ExportOnly:  source half only.  IngestOnly:  destination half only, from the tracking folder
  written by ExportOnly (same container, possibly another identity / tenant).
  The client factory only connects to the clusters each flow needs;  touching another
  cluster yields `CopyException("Can't find cluster")`.

## Failure matrix

* Transient Kusto error:  retried in place.  Process killed:  resume from block states.
* Source rows deleted after planning:  block completes with its actual exported count.