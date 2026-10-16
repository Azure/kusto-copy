# Advanced topics

[Documentation home](README.md) | [Common scenarios](scenarios.md) |
[Troubleshooting & performance](troubleshooting.md)

## Contents

- [How the pipeline works](#how-the-pipeline-works)
- [Resume and restart](#resume-and-restart)
- [Authentication and security](#authentication-and-security)
- [Split flows and tenant boundaries](#split-flows-and-tenant-boundaries)
- [Production checklist](#production-checklist)

## How the pipeline works

Kusto Copy orchestrates data movement; the console host does not download and upload the
table's rows itself:

```text
Source table -> Parquet in ADLS Gen2 -> Destination temporary table -> Destination table
```

### Cursor windows and blocks

Each activity copies one source table to one destination table. An iteration captures a
source database cursor and plans the rows in that window into blocks using `ingestion_time()`.
Later iterations start after the preceding cursor, even if the source keeps ingesting.
This requires a strongly consistent source workload group.

Blocks contain up to roughly 16 million rows. Planning and copying overlap, with backpressure
to avoid planning the entire dataset into memory or overwhelming downstream stages.
The cursor bounds ingestion, not the event timestamp in a user column.

An optional `-q` fragment is applied during planning and export. Keep it stable across the
copy and any restart. Kusto Copy does not capture a separate immutable snapshot: rows
deleted or expired from the source before export cannot be recovered by resuming.

### Why ingestion uses a temporary table

The destination table must already exist. Kusto Copy creates a temporary table named like
`kc-<table>-<guid>` from its schema and queues ingestion of the exported Parquet files there.
It tags blocks and verifies ingested row counts before moving extents into the final table.
Persisted operation state and these checks support exactly-once copying within the tracked job.

Temporary tables have merge disabled so that extents remain stable until moved. Do not alter
or drop these tables while a job is active or awaiting resume.

Destination update policies execute when extents are **moved** into the destination, not when
files are ingested into the temporary table. Include those policies in capacity planning and
validation; they can make the move stage a bottleneck.

### Time and retention semantics

Ingestion uses the block's source extent creation time, preserving that age for retention
purposes. Moving extents into the destination sets a new ingestion time.
Do not treat preserved extent creation time as a promise to preserve source
`ingestion_time()` values.

Review destination retention before copying old data: a successful copy is not a way to
bypass retention. See the [progress guide](progress.md) for the visible pipeline states.

## Resume and restart

### The staging folder is part of the job

The **first** configured staging directory holds the persisted tracking database under
`tracking/`. Exported data lives under:

```text
activities/<activity>/iterations/<iteration-id>/blocks/<block-id>/
```

With multiple staging directories, exported files can be spread across them, but tracking
remains in the first one. Keep the directory list and its order unchanged when resuming.

Resume an interrupted copy with the same command or YAML, credentials with the required
permissions, and the same staging directories. The process recovers persisted states and
checks outstanding Kusto operations rather than intentionally starting the copy over.
Use one active process per tracking folder; separate export and ingestion phases sequentially.

Keep activity names, source and destination identities, and queries unchanged. The runtime
rejects removed activities or changed table identities, but that check is not a substitute
for preserving the rest of the configuration.

### Resume is not a new copy

A completed `BackfillOnly` job does not recopy history when run against the same tracking
folder. Use `BackfillAndNew` to copy subsequent arrivals; see
[the scheduled-copy scenario](scenarios.md#backfill-then-copy-new-arrivals).

To deliberately start an independent job, use a **new staging folder**. That resets tracking,
not the destination table. Recopying overlapping data into an already populated table can
create duplicates: exactly-once behavior is scoped to the tracked job, not all jobs targeting
that table.

### Cleanup and recovery boundaries

Kusto Copy deletes completed block staging data and drops temporary tables as iterations
complete. Tracking remains available for later runs. Do not apply a storage lifecycle rule
that deletes active exports or tracking logs, and do not manually delete state to clear an
error.

Staging is not a backup: completed exports are cleaned up, and no destination rollback is
provided. Plan data protection and recovery separately. If state has been lost, investigate
what already reached the destination before deciding how to rebuild the job.

## Authentication and security

### Choose an identity for the host

Kusto Copy uses
[DefaultAzureCredential][credential].
The same selected credential is used for Kusto and Storage in a process. Visual Studio
credentials are excluded by this implementation; an Azure CLI login can be used for local
trials.

For unattended copies, prefer a managed identity on an Azure host that supports it:

- System-assigned identity: no client ID is needed.
- User-assigned identity: pass `--clientId <managed-identity-client-id>`, or set
  `managedIdentityClientId` in YAML.

`--clientId` configures managed identity selection within `DefaultAzureCredential`; it is
not a service-principal login option and does not force that credential to win over other
available credentials. Check the host's credential configuration before starting the job.

For a service principal, configure the standard `EnvironmentCredential` variables:
`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`. Supply the secret through the
host's secret management, not a command argument or checked-in YAML. Ensure the principal
has the [required data-plane permissions](setup.md#permissions).

### Storage access

Supply plain container or directory URLs, not account keys or SAS-bearing URLs. The CLI
requests a user delegation key and generates SAS tokens on demand: write access for source
exports and read access for destination ingestion.

The current implementation issues export SAS tokens for 90 minutes and ingestion SAS tokens
for five days. Treat any SAS-bearing diagnostics as credentials. Restrict staging access,
avoid publishing its contents, and keep tracking and data available until the job completes.

See [storage role scopes](setup.md#adls-gen2-container-permissions). An identity that can
list blobs is not necessarily authorized to request a delegation key.

### Network paths to allow

Validate all paths, not just connectivity from the console host:

| Component | Needs access to |
|-----------|-----------------|
| Console host | Entra authentication, required Kusto endpoints, Blob and DFS storage endpoints |
| Source cluster | Staging storage for server-side exports |
| Destination ingestion service | Staging storage to read exported files |

The destination also needs its ingestion endpoint, not only its query endpoint. Storage
firewalls, private endpoints, and DNS must allow the relevant services to reach staging.
A successful host-side storage check does not prove the source can export or the destination
can ingest. Verify the full path with a small copy in the intended network environment.

## Split flows and tenant boundaries

A managed identity belongs to one Entra tenant. The intended split-flow model is:

1. Run `ExportOnly` using an identity authorized on the source and staging storage.
2. After export finishes, run `IngestOnly` using an identity authorized on the destination
   and the **same** staging storage.

Keep activity names, endpoints, queries, and staging directories identical across phases;
change only the flow and authentication as needed. Both identities need appropriate
storage access, including delegation-key generation. Cross-tenant storage authorization
must be arranged explicitly; switching the flow does not grant it.

**Current implementation limitation:** `ExportOnly` skips activity synchronization at
startup. On a fresh tracking folder, it does not initialize the activities needed to start
exporting. Do not assume that a successful process exit means data was exported. Verify
support in the release you deploy before relying on this workflow; this guide does not
recommend manually editing tracking state as a workaround.

See [copy flow values](parameters.md#copy-flow). Split flows are not a general-purpose
standalone Parquet import/export interface: ingestion depends on the job's tracking state.

## Production checklist

### Before the copy

- Create destination tables with schemas compatible with the exported columns and types.
- Check source consistency, retention, permissions, and planning-plugin availability.
- Review destination retention, update policies, and existing data.
- Use a dedicated staging folder with soft delete disabled, as required by
  [setup](setup.md#prerequisites). Keep lifecycle cleanup away from active job state.
- Test a representative subset, including wide rows and policy effects. Use a separate
  destination or non-overlapping data for a trial so the production run does not duplicate it.
- Place the console host on a reliable runtime and configure unattended authentication.

### While it runs

- Capture console output and process exit status through the hosting platform. Alert on
  failures and prolonged lack of progress; the CLI does not configure alerts itself.
- Monitor source export capacity, destination ingestion and move throughput, and storage
  throttling. Tune from the [observed bottleneck](troubleshooting.md#the-copy-is-slow).
- Budget staging capacity for backlog, especially if ingestion is slower than export or
  export and ingestion are separated.
- Keep source data available until it has been copied and validated. This is batch
  migration, not a deletion/update replication service or an automatic failover system.

### Validate and cut over

- Compare counts for the same selected dataset, not an ever-growing unbounded source.
- Compare deterministic aggregates such as counts by a fixed event-time bucket and sums
  of important numeric fields. Check representative records and transformed columns.
- Validate update-policy outputs separately from the directly copied destination table.
- For a final cutover, coordinate a source ingestion pause or an agreed ingestion boundary,
  copy the remaining arrivals, and reconcile results before redirecting consumers.
- Retain the job configuration, version, and validation results. Remove remaining staging
  state only after deciding that resume or later incremental iterations are no longer needed.

[credential]: https://learn.microsoft.com/dotnet/api/azure.identity.defaultazurecredential
