# Reading progress

Kusto Copy reports progress every 20 seconds in the console as follow:

```
┌──────────┬─────────────────┬───────┬──────┬─────────┬──────────┬──────────┬───────┐
│ Activity │ Iteration State │ Total │ %    │ Planned │ Exported │ Ingested │ Moved │
├──────────┼─────────────────┼───────┼──────┼─────────┼──────────┼──────────┼───────┤
│ default  │ Starting        │ 210   │ % 28 │ 10      │ 50       │ 90       │ 60    │
└──────────┴─────────────────┴───────┴──────┴─────────┴──────────┴──────────┴───────┘
```

In the following section, you'll understand what each number means.

But if you want the **quick way**, just look at the **percentage column**.

## Blocks

Kusto Copy splits the data to copy into *blocks*.

A block is a subset of the data to copy with a min & max value of [ingestion_time](https://learn.microsoft.com/en-us/kusto/query/ingestion-time-function).  Dealing with subset of data guarantees export and ingestion does not time out (exporting many billion rows with one `.export` command would likely time out).

## Iteration State

The **iteration state** column takes one of the following values:

State|Description
-|-
Starting|Initial state, planning hasn't started yet.  Iteration doesn't stay in that state for long hence often you won't see progress with that state.
Planning|Queries are done on the source table to `plan` the blocks.  **You should see the total number of blocks increasing while in that state**.
Planned|Blocks have been planned and are copied over.  **Total number of blocks should not move in that state**.
Completed|Blocks have all been copied.

Block copying starts as soon as blocks are planned:  it doesn't wait for all blocks of one activity / iteration to be planned, i.e. copying usually starts while the iteration is still in `planning` state.

## Block State

Once a block is planned, it goes through a state machine to get copied to the destination table.

Here are the different state values:

State|Description
-|-
Planned|Block is planned (i.e. just created) and ready to be copied.
Exported|Block has been exported to blob storage.
Ingested|All exported blobs have been ingested (in one or more extents / data shards in Kusto table).
Moved|Extents have been moved to destination table.

Each state correspond to a column in the progress report.

To understand the difference between `Ingested` & `Moved`, it is important to note that Kusto Copy uses a staging table on the destination cluster.  That table's name looks like `kc-<Table Name>-f0fbaa23b8ad4cef9a9969539fa2b87d` (where the last bit is a GUID without hyphens).  The reason Kusto Copy uses a staging table is to guarantee once-and-only-once data copy.

If update policies are connected to the destination tables, those are executed when blocks are *moved*, not when they are ingested.  This is because blocks are ingested in that staging table which doesn't have update policy defined to it.

In theory, each block should go from one state to the next but there are a few exceptions.  For instance, if an error is detected during ingestion, a block will be sent back to `Exported`.

## How do I measure progress?

You should look at the block count in each state.  A copy is completed when all blocks (i.e. the total number of blocks) is in state `moved`.

The ratio of `Moved` blocks (over `Total`) block should give you the percentage of completion.

This should end with %100 unless data got deleted between the planning phase and the copy ending.

## Bottlenecks

If the copy doesn't go as fast as you would like, how can you interpret the progress report to understand where the bottlenecks are?

As discussed above, blocks go through 4 states (`planned`, `exported`, `ingested` & `moved`).  Look at where blocks are accumulating:

State|Bottleneck|Workarounds
-|-|-
Planned|Source cluster takes time to export data|Consider tuning the [Export Count](parameters.md#parallel-export) parameter, scaling out your source cluster & increasing the cache on your source table.
Exported|Destination cluster takes time to ingest data|Consider scaling out the destination cluster. 
Ingested|Destination cluster takes time to move extents (from staging table to destination table) ; this is likely due to update policies.|Consider scaling out destination cluster.

All that being said, it is normal for blocks to take time to export, ingest and move.  Each block is 16 million rows and if the table is large (many columns) or contains many long strings / dynamics, it's just a lot of data to deal with.
