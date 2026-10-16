# 🎯 Common scenarios

[Documentation home](README.md) | [CLI reference](parameters.md) | [Advanced topics](advanced.md)

Start with the [single-table example](README.md#start-here) and complete the
[setup prerequisites](setup.md#prerequisites) before trying these scenarios.
Commands below use Bash line continuations.

**Pick your recipe:** [Rename](#copy-into-a-differently-named-table) ·
[Multiple tables](#copy-multiple-tables) · [Filter](#filter-or-project-source-data) ·
[Aggregate](#aggregate-before-copying) ·
[New arrivals](#backfill-then-copy-new-arrivals) · [Cross-tenant](#copy-across-tenants)

<a id="copy-into-a-differently-named-table"></a>

## 📦 Copy into a differently named table

Include the table name in the destination URI:

```bash
kc -s https://source.kusto.windows.net/db/Events \
   -d https://dest.kusto.windows.net/db/EventsArchive \
   -t https://storage.blob.core.windows.net/container/events-archive
```

Create `EventsArchive` first with a compatible schema. Kusto Copy appends data; it does not
replace existing rows.

<a id="copy-multiple-tables"></a>

## 🗂️ Copy multiple tables

Save this as `migration.yaml`, replacing the example endpoints and names:

```yaml
activities:
- activityName: events
  source:
    clusterUri: https://source.kusto.windows.net
    databaseName: sourcedb
    tableName: Events
  destination:
    clusterUri: https://dest.kusto.windows.net
    databaseName: destdb
- activityName: metrics
  source:
    clusterUri: https://source.kusto.windows.net
    databaseName: sourcedb
    tableName: Metrics
  destination:
    clusterUri: https://dest.kusto.windows.net
    databaseName: destdb
stagingStorageDirectories:
- https://storage.blob.core.windows.net/container/migration
```

Run `kc -y migration.yaml`. Both destination tables must already exist. Activities share
cluster capacity; adding activities does not multiply a cluster's export allowance.
Keep activity names stable when resuming.

See the [YAML schema](yaml.md) for all fields, including entity groups.

<a id="filter-or-project-source-data"></a>

## 🔍 Filter or project source data

Pass a **KQL pipeline fragment** with `-q`, not as part of the source URI:

```bash
kc -s https://source.kusto.windows.net/db/Events \
   -d https://dest.kusto.windows.net/db/Errors \
   -t https://storage.blob.core.windows.net/container/errors \
   -q "| where Level == 'Error' | project Timestamp, Message, Level"
```

The destination schema must match the query output. The fragment runs during both planning
and export, so use a stable predicate for reproducible results. For example, use a fixed
datetime boundary rather than `ago(30d)` during a long-running migration.

> [!IMPORTANT]
> Keep `-q` for row-level filtering and transformations. Put aggregations in
> `--aggregation-query`, as shown below.

<a id="aggregate-before-copying"></a>

## 🧮 Aggregate before copying

Use `-q` to select the input rows and `--aggregation-query` to summarize them before export:

```bash
kc -s https://source.kusto.windows.net/db/Readings \
   -d https://dest.kusto.windows.net/db/HourlyTemperature \
   -t https://storage.blob.core.windows.net/container/hourly-temperature \
   -q "| where isnotnull(Temperature)" \
   --aggregation-query "| summarize TotalTemp=sum(Temperature) by bin(Timestamp, 1h), WidgetId"
```

Create `HourlyTemperature` with columns and types matching the aggregation output:
`Timestamp`, `WidgetId`, and `TotalTemp`. Validate the resulting summaries rather
than comparing destination row counts with raw source row counts.

For YAML activities, use `kqlQuery` and `kqlAggregationQuery` respectively.
See [aggregation query](parameters.md#aggregation-query) for the option details.

<a id="backfill-then-copy-new-arrivals"></a>

## 🔄 Backfill, then copy new arrivals

Use `BackfillAndNew` with a polling period:

```bash
kc -s https://source.kusto.windows.net/db/Events \
   -d https://dest.kusto.windows.net/db/Events \
   -t https://storage.blob.core.windows.net/container/events-sync \
   --copy-mode BackfillAndNew --iteration-period 00:15:00
```

**What to expect:**

- **First iteration:** copies history.
- **Later iterations:** copy newly ingested rows using database cursors.
- **Process lifetime:** stays running. The period is a scheduling interval, not a guarantee
  that each copy finishes within 15 minutes.

Without `--iteration-period`, the process finishes its iteration and exits. Restart the same
command with the same staging folder and `BackfillAndNew` to copy the next cursor window
after the previous job has completed.

> [!NOTE]
> This is **batch copying**, not change-data capture: source deletions and updates are not
> reconciled at the destination. In the current implementation, `NewOnly` also starts with
> history on a fresh tracking folder; do not use it to skip the backfill.

See [copy modes](parameters.md#copy-mode) and [resume guidance](advanced.md#resume-and-restart).

<a id="copy-across-tenants"></a>

## 🌐 Copy across tenants

Managed identities cannot authenticate outside their own Entra tenant. Kusto Copy exposes
separate export and ingestion flows for this use case, but the current implementation has
a fresh-job limitation.
Read [split flows and tenant boundaries](advanced.md#split-flows-and-tenant-boundaries)
before planning a migration around them.
