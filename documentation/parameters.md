# Parameters

The parameters of the CLI are the following:

> **Note**: You need either CLI parameters OR a YAML file. A minimal command requires at least source (-s), destination (-d), and staging storage (-t) which can be specified in the command line or a YAML file.

## Quick Reference

**[Connection](#connection)** • **[Storage](#storage)** • **[Copy Behavior](#copy-behavior)** • **[Authentication](#authentication)** • **[Performance](#performance)** • **[Configuration](#configuration)**

### Connection
Parameter|Description|Example
-|-|-
Source (-s)|Source [table](#database--table-uri)|https://mycluster.eastus.kusto.windows.net/mydb/mytable
Destination (-d)|Destination [database or table](#database--table-uri)|https://yourcluster.eastus.kusto.windows.net/yourdb

### Storage  
Parameter|Description|Example
-|-|-
Staging Storage (-t)|One or many [ADLS gen 2 containers](#adls-gen-2-containers) (can be a sub folder)|https://mystorageaccount.blob.core.windows.net/mycontainer/myfolder

### Copy Behavior
Parameter|Description|Example
-|-|-
Query (-q)|Optional [query](#query)|"\| where Level == 'error'"
Aggregation Query (--aggregation-query)|[Not applied by current export](#aggregation-query)|N/A
Copy Mode (--copy-mode)|[Copy mode](#copy-mode) behavior|BackfillOnly
Copy Flow (--copy-flow)|[Copy flow](#copy-flow) stages|ExportOnly
Iteration Period (--iteration-period)|[Iteration period](#iteration-period) for new data|0:15:00

### Authentication
Parameter|Description|Example
-|-|-
Managed Identity Client ID (--clientId)|[Client ID](#client-id)|GUID

### Performance
Parameter|Description|Example
-|-|-
Export Count (--export)|Maximum [parallel export](#parallel-export) (default is 20)|10

### Configuration
Parameter|Description|Example
-|-|-
YAML (-y)|[Path of YAML file](#yaml-file)|my-activities.yaml

##  Database & Table URI

Kusto Copy uses a table URI notation, for example, https://mycluster.eastus.kusto.windows.net/mydb/mytable refers to the table `mytable` in database `mydb` on the cluster `https://mycluster.eastus.kusto.windows.net`.

The source **always** points to a table while a destination can either point to a database (i.e. the table is ommitted) or a table.  If the destination specifies a database, the table name is infered (as the source's table name).

##  ADLS gen 2 containers

The staging storage points to a container and can even be a sub folder in that container.  It cannot be the storage account URL (i.e. above a container).

**Why would you use more than one storage account**?  Azure Storage API can throttle when usage is high.  If you are copying data between two big clusters (more than 500 cores), it is possible they will exceed the storage account capacity and get throttled, slowing down the process.  In those cases, adding different storage accounts could help.

**Specifying multiple containers within the same storage account is useless**:  throttling is at the storage account level.

Kusto Copy uses storage for two purposes:

1. Track copy (in `tracking` sub folder), by persisting tracking information
1. Stage data (in `activities` sub folder), source cluster exports data there for the destination cluster to ingest

Because tracking is done in the specified container / folder, when you want to "start over", it is important to either whip out the folder or specify a new one.  Otherwise, Kusto Copy will try to resume the same copy activities.

##  Query

A query is optional.  It allows you to filter and transform the source table's data (e.g. projecting only some columns).  Do not put aggregations in this query because Kusto Copy uses `ingestion_time()` to track the copy.  Use [Aggregation Query](#aggregation-query) for aggregation instead.

##  Aggregation Query

The CLI accepts `--aggregation-query` and YAML accepts `kqlAggregationQuery`, but the current
implementation does not apply this value during export. Do not rely on it to produce
aggregated output. Aggregation in `-q` is not a supported workaround because planning needs
row-level ingestion metadata.

##  Client ID

Kusto copy needs an Azure Entra identity to authenticate against source and destination Kusto clusters as well as Azure Storage.

By default it uses the [default Azure Authentication](https://learn.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential?view=azure-dotnet) which tries different authentication, most of them about the currently logged in user.

For long running copies (i.e. more than 10 minutes), we recommend using an [Azure Managed Identity](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview) to authenticate Kusto Copy.

If you are running Kusto Copy on an Azure VM, you could, for instance, use the VM system identity.

##  Copy Mode

The --copy-mode parameter controls the copy behavior and can take the following values:

Mode|Description
-|-
BackfillOnly|The default:  copy historical data and will not copy data after the point where the copy started.
BackfillAndNew|Copy historical data and will iteratively copy new data.
NewOnly|Intended to copy only new data; currently includes history on a fresh job (see below).

**Current implementation limitation:** on a fresh tracking folder, `NewOnly` starts with an
empty cursor and includes history, just like `BackfillAndNew`. Do not rely on it to skip
historical data. See [incremental copying](scenarios.md#backfill-then-copy-new-arrivals).

**See also**: [Iteration Period](#iteration-period) - controls polling frequency for BackfillAndNew and NewOnly modes.

##  Copy Flow

The --copy-flow parameter controls which stages of the copy are run and can take the following values:

Flow|Description
-|-
All|The default: export data to staging storage and ingest it into the destination.
ExportOnly|Export data to staging storage without ingesting it into the destination.
IngestOnly|Ingest previously exported data from staging storage (no export).

This parameter is meant to support a multi-tenant migration where the export portion would be done with a Managed Identity (MI) from the source tenant whereas the ingestion would be done with a MI from the destination tenant (Entra ID Managed Identity does not support multiple tenants).

**Current implementation limitation:** `ExportOnly` does not initialize activities on a fresh
tracking folder. Review [split flows](advanced.md#split-flows-and-tenant-boundaries) before
depending on this workflow.

##  Iteration Period

This parameter is relevant only for [Copy Mode](#copy-mode) `BackfillAndNew` & `NewOnly` (not `BackfillOnly`).

When you don't specify an iteration period, Kusto Copy will copy the data for an iteration and exit once the iteration is completed (for each activity).  Specifying an iteration period has Kusto Copy run indefinitely and tries to start a new copy recurrently.

**See also**: [Copy Mode](#copy-mode) - determines when iteration period is used.

##  Parallel export

`--export` defaults to 20 and caps parallel exports per source cluster, across its activities.
Effective parallelism is the **minimum** of that setting and the source cluster's data export
capacity. Raising the setting does not override cluster capacity.

Tune it based on [observed bottlenecks](troubleshooting.md#the-copy-is-slow); extra exports
will not fix slow destination ingestion.

##  YAML file

A YAML file can be used instead of CLI parameters.

CLI parameters have precedence:  for example, if an export count is specified both in the YAML file and as a CLI parameter, the CLI parameter will be used.

The big advantage of using YAML file is that it allows to run multiple table copies in parallel.

See [YAML file schema](yaml.md) for details about the schema of the YAML file.

For operational guidance, see [common scenarios](scenarios.md) and
[advanced topics](advanced.md).