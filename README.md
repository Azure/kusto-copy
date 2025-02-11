#   Kusto Copy

*Kusto Copy* is a command-line utility providing copying capabilities between Kusto databases (either Azure Data Explorer or Fabric Eventhouse).  This enables scenarios such as table copy, cluster migration, replication across region (BCDR) & others.

*Kusto Copy* is an orchestration tool exporting data on the source database and ingesting it on the other.  It is fault tolerant (i.e. the tool can be interupted and restarted) and with exact copy semantic (no data loss nor data duplication).

## Limitations

Known limitations:

*   Is bound by the capacity of the source cluster to export data and the destination clusters to ingest the data
*   The tool doesn't track purges, row deletions, row updates or extent drops
*   If some records have no ingestion-time (e.g. ingestion time policy was disabled), records won't be copied over

## Getting started

Kusto Copy is a command line interface (CLI) tool.

You can find the binary executable [here](https://github.com/Azure/kusto-copy/releases) for Linux, Windows & Mac.

Here is an example of usage:

```
kc -s https://mycluster.eastus.kusto.windows.net/mydb/mytable -d https://yourcluster.eastus.kusto.windows.net/yourdb/ -t https://mystorageaccount.blob.core.windows.net/mycontainer/myfolder
```

That CLI invocation is configured as followed:

* The data lake checkpoint blobs are staging blob folders are located at https://mystorageaccount.blob.core.windows.net/mycontainer/myfolder
* The source cluster is https://mycluster.eastus.kusto.windows.net
* The source database is mydb
* The source table is mytable
* The destination cluster is https://yourcluster.eastus.kusto.windows.net
* The destination database is yourdb
* The destination table is mytable (implicitly takes the same value as the source if not specified)
* It will use the "current user" authentication to run queries / commands on both clusters

# Documentation

See the [full documentation here](documentation)