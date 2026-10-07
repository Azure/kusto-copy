# Kusto, Storage and Infrastructure

## Kusto access (`Kusto/`)

* **All KQL and control-command text lives in `DbQueryClient` / `DbCommandClient`** (plus
  two capacity queries in `DbClientFactory`).  Runners never build KQL.
* `ProviderFactory` creates per-cluster SDK providers with `DefaultAzureCredential` and tags
  requests `KUSTO-COPY;<version>` (visible in `.show queries`).  The ingest client uses the
  `ingest-<host>` endpoint.
* `DbClientFactory` owns per-cluster `PriorityExecutionQueue<KustoPriority>`:  query and
  command queues sized at 10 % of `.show capacity queries` (min 1), ingest queue 75.
* `KustoPriority` = (activity, iteration, block), nulls first, ascending.
  `HighestPriority` (all null) is used for housekeeping calls so they are never starved.
* `KustoClientBase.RequestRunAsync`:  queue + Polly retry **inside the queue slot** (10 tries,
  backoff `min(120, 2^n)` s, about 10 min).  All exceptions are retried, so permission errors
  show as repeated warnings before the process fails.
* Dependencies on Kusto behavior:  `cursor_*` consistency, `execute_show_command` plugin,
  `.export async` + `persistDetails`, extent tags, `.move async ... setNewIngestionTime`,
  operations remaining visible in `.show operations` (missing = lost).

## Storage (`Runner/AzureBlobUriProvider.cs`)

* Staging = ADLS Gen2 container (or subfolder), validated (no query string, at least a
  container).  The **first** directory also holds `tracking/`.
* No keys and no stored SAS:  SAS tokens are generated on demand from a cached **user
  delegation key**:  write SAS 90 min (export), read SAS 5 days (ingestion).  Needs *Storage
  Blob Delegator* + *Data Contributor*.
* Layout:  `activities/<activity>/iterations/<id:D20>/blocks/<id:D20>/` (parquet).  Deleted per
  block on completion and per iteration at the end.  Storage soft-delete is unsupported.

## Configuration (`JobParameter/`)

YAML loaded first, then **CLI overrides**;  `-s` replaces the YAML activities with one
`default` activity.  Destination table defaults to the source table name.  Cluster URIs are
normalized (trim, lower case).  Validation runs last.

## Build and release

* `net10.0`, warnings as errors;  trimming warnings suppressed on purpose (Kusto assemblies
  rooted in `rd.xml` / `TrimmerRootAssembly`).  Assembly name `kc`.
* Version = csproj `<Version>` + CI run number (`deployment/patch-version.py`).
* `continuous-build.yaml`:  build + `dotnet test`.  `release.yaml`:  single-file, trimmed,
  ReadyToRun publish for linux / windows / macos.

## Testing

`KustoCopyTest` (xUnit) is empty.  Good seams:  `PriorityExecutionQueue`, `AsyncCache`,
`KustoPriority`, `MainJobParameterization.FromOptions`, record `Validate()`, track-db triggers.
`deployment/integration-test` has Bicep infra only.  End-to-end needs a real cluster.

## Guidance for agents

* Follow [CodingStandard.md](CodingStandard.md).
* New stage = new runner:  query input state, work, write output state in a transaction, add
  to `MainRunner.RunAsync`, extend `BlockState` **and** `BlockMetric` identically.
* Re-query the DB each loop;  never keep work queues in memory.
* Route Kusto calls through the clients with a meaningful `KustoPriority`.
* Persist identifiers before depending on them.
* Update public `documentation/` when changing CLI, YAML or progress output.