# Parameters

The parameters of the CLI are the following:

> **Note**: You need either CLI parameters OR a YAML file. A minimal command requires at least source (-s), destination (-d), and staging storage (-t) which can be specified in the command line or a YAML file.

## Quick Reference

**[Connection](#connection)** • **[Storage](#storage)** • **[Data Control](#data-control)** • **[Authentication](#authentication)** • **[Performance](#performance)** • **[Configuration](#configuration)**

### Connection
Parameter|Description|Example
-|-|-
Source (-s)|Source [table](#database--table-uri)|https://mycluster.eastus.kusto.windows.net/mydb/mytable
Destination (-d)|Destination [database or table](#database--table-uri)|https://yourcluster.eastus.kusto.windows.net/yourdb

### Storage  
Parameter|Description|Example
-|-|-
Staging Storage (-t)|One or many [ADLS gen 2 containers](#adls-gen-2-containers) (can be a sub folder)|https://mystorageaccount.blob.core.windows.net/mycontainer/myfolder

### Data Control
Parameter|Description|Example
-|-|-
Query (-q)|Optional [query](#query)|"\| where Level == 'error'"
Copy Mode (--copy-mode)|[Copy mode](#copy-mode) behavior|BackfillOnly
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

A query is optional.  It allows you to filter and transform the source table's data (e.g. projecting only some columns).  The query shouldn't contain aggregation as those make `ingestion_time()` function unusable (Kusto Copy uses that function).

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
NewOnly|Copy only new data.

**See also**: [Iteration Period](#iteration-period) - controls polling frequency for BackfillAndNew and NewOnly modes.

##  Iteration Period

This parameter is relevant only for [Copy Mode](#copy-mode) `BackfillAndNew` & `NewOnly` (not `BackfillOnly`).

When you don't specify an iteration period, Kusto Copy will copy the data for an iteration and exit once the iteration is completed (for each activity).  Specifying an iteration period has Kusto Copy run indefinitely and tries to start a new copy recurrently.

**See also**: [Copy Mode](#copy-mode) - determines when iteration period is used.

##  Parallel export

By default Kusto Copy will use the maximum value between the data export capacity of the source cluster and 20.

Exporting more or less could improve the performance.  You could try varying that as fine tuning.

##  YAML file

A YAML file can be used instead of CLI parameters.

CLI parameters have precedence:  for example, if an export count is specified both in the YAML file and as a CLI parameter, the CLI parameter will be used.

The big advantage of using YAML file is that it allows to run multiple table copies in parallel.

See [YAML file schema](yaml.md) for details about the schema of the YAML file.