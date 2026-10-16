# 📋 CLI parameters

[Documentation home](README.md) | [Common scenarios](scenarios.md) | [YAML schema](yaml.md)

Look up an option here, or run `kc --help` for the options in your installed version.

> [!TIP]
> A minimal copy needs a **source** (`-s`), **destination** (`-d`), and **staging storage**
> (`-t`). Supply them through CLI options or YAML. CLI values take precedence when combined.

<a id="quick-reference"></a>

## 🧭 Quick reference

[Connection](#connection) · [Storage](#storage) · [Copy behavior](#copy-behavior) ·
[Authentication](#authentication) · [Performance](#performance) · [Configuration](#configuration)

### Connection

| Parameter | Description |
|-----------|-------------|
| `-s`, `--source` | Source [table URI](#database--table-uri) |
| `-d`, `--destination` | Destination [database or table URI](#database--table-uri) |

### Storage

| Parameter | Description |
|-----------|-------------|
| `-t`, `--staging-storage` | One or more [ADLS Gen2 directories](#adls-gen-2-containers) |

### Copy Behavior

| Parameter | Description | Example |
|-----------|-------------|---------|
| `-q`, `--query` | Row-level [query](#query) | `"\| where Level == 'error'"` |
| `--aggregation-query` | Optional [aggregation](#aggregation-query) | `"\| summarize count()"` |
| `--copy-mode` | [Data selection mode](#copy-mode) | `BackfillOnly` |
| `--copy-flow` | [Pipeline stages](#copy-flow) | `ExportOnly` |
| `--iteration-period` | [New-data polling interval](#iteration-period) | `00:15:00` |

### Authentication

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--clientId` | Managed identity [client ID](#client-id) | GUID |

### Performance

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--export` | Maximum [parallel exports](#parallel-export) | `20` |

### Configuration

| Parameter | Description | Example |
|-----------|-------------|---------|
| `-y`, `--yaml` | [YAML file path](#yaml-file) | `my-activities.yaml` |
| `-v`, `--verbose` | Informational diagnostics | `kc -y my-activities.yaml -v` |

<a id="database--table-uri"></a>

## 🔗 Database & table URI

A table URI combines the cluster, database, and table:

```text
https://mycluster.eastus.kusto.windows.net/mydb/mytable
```

- **Source:** always points to a table.
- **Destination:** can point to a database or a table. If the table is omitted, its name
  defaults to the source table name.

<a id="adls-gen-2-containers"></a>

## ☁️ ADLS Gen2 containers

Use a container URL, optionally including a subfolder:

```text
https://mystorageaccount.blob.core.windows.net/mycontainer/myfolder
```

An account-only URL is not sufficient. Storage serves two purposes:

| Subfolder | Purpose |
|-----------|---------|
| `tracking/` | Persist copy state in the first configured staging directory |
| `activities/` | Stage exported data for destination ingestion |

> [!IMPORTANT]
> Reuse the same staging directories to **resume**. Choose a new dedicated folder to
> **start over**; this does not clear the destination table.
> Read [resume and restart](advanced.md#resume-and-restart) before resetting a job.

**When should you use multiple accounts?** Large clusters can hit Azure Storage throttling.
If storage is the measured bottleneck, spreading exports across different storage accounts
can help. More containers within the **same account** do not avoid account-level throttling.

<a id="query"></a>

## 🔍 Query

Use optional `-q` to filter rows or transform columns, for example:

```text
-q "| where Level == 'error' | project Timestamp, Message, Level"
```

Keep `-q` for row-level filtering and transformations. To summarize the selected rows, use
[aggregation query](#aggregation-query) instead.
See the [filtered-copy recipe](scenarios.md#filter-or-project-source-data).

<a id="aggregation-query"></a>

## 🧮 Aggregation query

Use optional `--aggregation-query` to aggregate the rows selected by the regular query.
It runs **after** the filters and transformations in `-q` and **before** export.

For example, summarize temperature readings by hour and widget:

```text
--aggregation-query "| summarize TotalTemp=sum(Temperature) by bin(Timestamp, 1h), WidgetId"
```

In YAML, set `kqlAggregationQuery` on the activity. The destination table's schema must match
the aggregation output, not the original source rows.

See the [aggregation recipe](scenarios.md#aggregate-before-copying) for a complete command.

<a id="client-id"></a>

## 🔐 Client ID

Kusto Copy uses [DefaultAzureCredential][credential] to authenticate to both Kusto and Storage.
For long-running copies, prefer an [Azure managed identity][mi].

- **System-assigned identity:** for example, use the identity of the Azure VM running the CLI.
- **User-assigned identity:** select it with `--clientId <managed-identity-client-id>`.

For credential selection details and service principals, see
[authentication and security](advanced.md#authentication-and-security).

<a id="copy-mode"></a>

## 🔄 Copy mode

`--copy-mode` controls which data is copied:

| Mode | Behavior |
|------|----------|
| `BackfillOnly` | **Default.** Copy history up to the initial copy boundary. |
| `BackfillAndNew` | Copy history, then iteratively copy new arrivals. |
| `NewOnly` | Intended to skip history; see the current limitation below. |

> [!WARNING]
> On a fresh tracking folder, `NewOnly` starts with an empty cursor and **includes history**,
> just like `BackfillAndNew`. Do not rely on it to skip historical data.

**Next:** [Iteration period](#iteration-period) controls polling.
See the [incremental-copy recipe](scenarios.md#backfill-then-copy-new-arrivals).

<a id="copy-flow"></a>

## 🌐 Copy flow

`--copy-flow` controls which pipeline stages run:

| Flow | Behavior |
|------|----------|
| `All` | **Default.** Export to staging storage, then ingest into the destination. |
| `ExportOnly` | Export to staging storage without destination ingestion. |
| `IngestOnly` | Ingest previously exported data using the same tracking state; no export. |

Split flows are intended for cross-tenant migrations with separate source and destination
managed identities. A managed identity cannot authenticate outside its own tenant.

> [!WARNING]
> `ExportOnly` does not initialize activities on a fresh tracking folder. Review
> [split flows](advanced.md#split-flows-and-tenant-boundaries) before relying on this workflow.

<a id="iteration-period"></a>

## 🕒 Iteration period

`--iteration-period` is relevant to `BackfillAndNew` and `NewOnly`, not `BackfillOnly`.

| Setting | Process behavior |
|---------|------------------|
| Omitted | Complete the current iteration for each activity, then exit. |
| Specified, e.g. `00:15:00` | Stay running and try to start iterations at that interval. |

See [copy mode](#copy-mode) to choose which data each iteration selects.

<a id="parallel-export"></a>

## ⚡ Parallel export

`--export` defaults to **20** and caps parallel exports per source cluster, across activities.

```text
Effective parallelism = min(--export, source cluster data export capacity)
```

Raising the setting does not override cluster capacity. Tune from the
[observed bottleneck](troubleshooting.md#the-copy-is-slow): extra exports will not fix slow
destination ingestion.

<a id="yaml-file"></a>

## ⚙️ YAML file

Use `-y my-activities.yaml` to configure multiple table copies in one job.
**CLI parameters take precedence:** a CLI export count overrides the YAML export count.

See the [YAML schema](yaml.md) and [multi-table recipe](scenarios.md#copy-multiple-tables).

[credential]: https://learn.microsoft.com/dotnet/api/azure.identity.defaultazurecredential
[mi]: https://learn.microsoft.com/entra/identity/managed-identities-azure-resources/overview
