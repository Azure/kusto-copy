#   Kusto Copy

This project aims at building a tool to provide copying capabilities between Azure Data Explorer (Kusto) databases.  This enables scenarios such as table copy, cluster migration, replication across region (BCDR) & others.

The solution is based on exporting data on one side and ingesting it on the other.  The tool takes care of orchestrating the data movement.  It is fault tolerant (i.e. the tool can be interupted and restarted) and guarantees exact copies.  It also tries to give a consistant replica at one point in time using database cursors and ingestion-time windows.

## Limitations

The tool has the following known limitations:

*   Is bound by the capacity of the source cluster to export data and the destination clusters to ingest the data
*   Number of databases:  the tool isn't optimized to work with lots of small databases totalling a huge amount (1000+) of tables
*   The tool doesn't track purges, row deletions or extent drops
*   Update policies aren't taken into account ; tables are replicated assuming it is all "original content"
*   If some records have no ingestion-time (e.g. ingestion time policy was disabled), records won't be copied over

## Getting started

Kusto Copy is a command line interface (CLI) tool.

You can find the binary executable [here](https://github.com/Azure/kusto-copy/releases) for Linux, Windows & Mac.

Here is an example of usage:

```
kusto-copy -l https://vpdeltalake.blob.core.windows.net/copy/laptop/d16 -s https://help.kusto.windows.net/ -d https://vpdeltalake.eastus.kusto.windows.net --db Samples --tables-include StormEvents demo_make_series1 ConferenceSessions US_States
```

That CLI invocation is configured as followed:

* The data lake checkpoint folder is located at https://vpdeltalake.blob.core.windows.net/copy/laptop/d16
* The source cluster is https://help.kusto.windows.net/
* The destination cluster is https://vpdeltalake.eastus.kusto.windows.net
* The database that will be copied is `Samples`
* The tables inside that database that will be copied are:  `StormEvents`, `demo_make_series1`, `ConferenceSessions` & `US_States`
* It will use the "current user" authentication to run queries / commands on both clusters

The CLI will start copying the data from the source to destination cluster.  You can monitor the process by querying the `KC_Status` view in the destination database.  That view is surfacing the checkpoint file in the data lake folder.

Here are the command line options:

Short name|Long name|Is required|Description
-|-|-|-
v|verbose|No|Set output to verbose messages.
-|continuous|No|Continuous run.  If set, runs continuously. Otherwise, stop after first batch.
l|lake|Yes|Data Lake (ADLS gen 2) folder URL or Kusto-style connection string
s|source|Yes|Source Cluster Query Connection String
d|destination|Yes|Destination Cluster Query Connection String
-|db|Yes|Database to copy
-|tables-include|No|Tables to include (default:  copies all tables)
-|tables-exclude|No|Tables to exclude (default:  copies all tables)
-|query-slots|No|Number of concurrent queries / commands on the clusters (default:  %10 of query capacity)
-|export-slots|No|# export slots to use on source cluster (default:  %100 of export capacity)
-|ingestion-slots|No|Number of concurrent ingestions on the clusters (default:  %100 of ingestion capacity)
r|rpo|No|Recovery Point Objectives:  the target timespan between 2 iterations (default:  5 minutes)
b|backfillHorizon|No|Backfill horizon:  how long in the past should we start? (default:  no horizon, i.e. backfill everything)

## Authentication

Kusto Copy leverages [Kusto Connection strings](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/kusto).

This can be used to specify the authentication mechanism to connect to the source cluster, the destination cluster and even the data lake.

If no authentication mechanism is specified, the `Azure Default` will be used, which go through the followings:

* Azure.Identity.EnvironmentCredential
* Azure.Identity.ManagedIdentityCredential
* Azure.Identity.SharedTokenCacheCredential
* Azure.Identity.VisualStudioCredential
* Azure.Identity.VisualStudioCodeCredential
* Azure.Identity.AzureCliCredential
* Azure.Identity.InteractiveBrowserCredential

A popular authentication alternative is to use a service principal.  The connection string would then look as followed:

```
Data Source={serviceUri};Database=NetDefaultDB;Fed=True;AppClientId={applicationClientId};AppKey={applicationKey};Authority Id={authority}
```

Look at [Kusto Connection strings](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/kusto) for more details / alternatives.
