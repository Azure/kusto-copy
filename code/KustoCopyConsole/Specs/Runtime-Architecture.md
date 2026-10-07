# Runtime Architecture

## Startup

`Program.Main` -> `MainRunner.CreateAsync` -> `MainRunner.RunAsync`.  Culture is forced to
en-US (KQL is built with string interpolation).  Logging is `System.Diagnostics.Trace` only
(warnings always, information with `-v`).  `ProcessExit` cancels and waits for graceful stop.

`RunAsync`:  `SyncActivities` (reconcile config vs. tracked activities), `ReactivateActivities`
(non-BackfillOnly:  completed activities restart), start every runner with `Task.Run`, and a
monitor that cancels all runners on the first fault.

`SyncActivities` throws when a tracked activity is missing from config or its source /
destination differs.  So **activities can't be renamed or removed in an existing tracking
folder**;  use a new staging folder to start over.

## Runner model

A runner is a loop:  `while (ShouldRunnersContinue()) { work from DB queries; Sleep }`.

* Runners share nothing but `RunnerParameters` (database, Kusto client factory, staging URI
  provider).  Eligibility is always a DB query, which makes restarts free.
* State changes are transactions that typically **delete the old block record and append the
  new one** (track-db records are immutable).
* Flow-restricted runners no-op via `ShouldExportRun` / `ShouldIngestionRun`.

Base classes worth knowing (`Runner/`):

* `ActivityRunnerBase`:  per non-completed activity, sequentially.
* `StartCommandRunnerBase`:  start an async Kusto command per block (export, move).  Groups by
  cluster, caches capacity for 2 min, picks blocks of the **oldest iteration** only, batches
  up to 20, limited to `capacity - blocks already in the destination state`.
* `AwaitCommandRunnerBase`:  poll `.show operations` for blocks in a "running" state.  Lost
  operation ID or retriable failure -> block reset to `ResetState`;  non-retriable failure ->
  permanent `CopyException`;  completed -> `ProcessOperationAsync`.

## When does the process stop?  (`ShouldRunnersContinue`)

1. `IterationPeriod` set:  never (daemon).
2. Flow is not ExportOnly:  when all activities are `Completed`.
3. ExportOnly:  when every iteration is `Planned` and no block is below `Exported`.

`SleepAsync` wakes early once all activities are complete.

## Concurrency

* One task per runner; per-cluster grouping runs clusters in parallel.
* Every Kusto call goes through a `PriorityExecutionQueue` (see
  [Kusto-Storage-Infrastructure.md](Kusto-Storage-Infrastructure.md)); priority is
  (activity, iteration, block) ascending, so older work always wins and iterations finish in
  order.
* `AsyncCache<T>`:  single-flight expiring cache (blob user-delegation key).

## Errors

* `CopyException(message, isTransient)` for config / permanent errors.
* `Validate()` on records throws `InvalidDataException` (programming errors).
* DEBUG-only invariant checks (`ValidateUrls`, negative metrics) compile out in Release.
* Progress table (Spectre.Console, every 20 s) folds 7 block states into the 4 public columns.