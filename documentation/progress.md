# Reading progress

Kusto Copy reports progress every 5 seconds or so.  Progress report looks like this:

```
Progress [Planned]:  Total=210, Planned=10, Exporting=20, Exported=30, Queued=40, Ingested=50, Moved=60
(500,000,000 planned rows / 250,000,000 exported rows)
```

In the following section, you'll understand what each number means.

## Blocks

Kusto Copy splits the data to copy into *blocks*.

A block is a subset of the data to copy with a min & max value of `ingestion_time`.  Dealing with subset of data makes export and ingestion manageable (e.g. exporting many billion of rows in one go would likely time out).

Kusto Copy emits a bunch of queries to your source table to figure out where the blocks are, i.e. determining the min & max `ingestion_time` for each so that each block is about 16 million rows.  This is called the *planning* phase.

The total number of blocks is shown in the progress:

<pre>
Progress [Planned]:  Total=<mark>210</mark>, Planned=10, Exporting=20, Exported=30, Queued=40, Ingested=50, Moved=60
(500,000,000 planned rows / 250,000,000 exported rows)
</pre>


## Iteration State

The iteration state is displayed within the brackets:

<pre>
Progress [<mark>Planned</mark>]:  Total=210, Planned=10, Exporting=20, Exported=30, Queued=40, Ingested=50, Moved=60
(500,000,000 planned rows / 250,000,000 exported rows)
</pre>

The state takes one of the following values:

State|Description
-|-
Starting|Initial state, planning hasn't started yet.  Iteration doesn't stay in that state for long hence often you won't see progress with that state.
Planning|Queries are done on the source table to `plan` the blocks.  **You should see the total number of blocks increasing in that state**.
Planned|Blocks have been planned and are copied over.  **Total number of blocks should stay fix in that state**.
Completed|Blocks have all been copied.  Iteration doesn't spent much time in this state and the application can often exit before it is reported.

The main states you should see are `planning` and `planned`.

Block copying starts as soon as blocks are planned:  it doesn't wait for all blocks to be planned, i.e. copying usually starts while the iteration is still in `planning` state.

## Block State

Once a block is planned, it goes through a state machine to get copied to the destination table.

Progress displays the number of blocks in each state.  Those should sum up to the total number of blocks.

<pre>
Progress [Planned]:  Total=210, Planned=<mark>10</mark>, Exporting=<mark>20</mark>, Exported=<mark>30</mark>, Queued=<mark>40</mark>, Ingested=<mark>50</mark>, Moved=<mark>60</mark>
(500,000,000 planned rows / 250,000,000 exported rows)
</pre>

Here are the different state values:

State|Description
-|-
Planned|Block is planned (i.e. just created) and ready to be copied.
Exporting|An `.export` command has been issued on the source cluster to export the data of the block to blobs.  The command is currently running.
Exported|The `.export` command has completed and exported blobs are available.
Queued|All exported blobs have been queued for ingestion in the destination cluster.
Ingested|All exported blobs have been ingested (in one or more extents / data shards in Kusto table).
Moved|Extents have been moved to destination table.

To understand the different between `Ingested` & `Moved`, it is important to note that Kusto Copy uses a staging table on the destination cluster.  That table's name looks like `kc-<Table Name>-f0fbaa23b8ad4cef9a9969539fa2b87d` (where the last bit is a GUID without hyphens).  The reason Kusto Copy uses a staging table is to guarantee once-and-only-once data copy.

In theory, each block should go from one state to the next but there are a few exceptions.  For instance, if an error occured during `Exporting`, a block will be sent back to `Planned`.

## Row count

Progress displays two row counts:

<pre>
Progress [Planned]:  Total=210, Planned=10, Exporting=20, Exported=30, Queued=40, Ingested=50, Moved=60
(<mark>500,000,000</mark> planned rows / <mark>250,000,000</mark> exported rows)
</pre>

`Planned rows` is the count returned by the query planning the block.

`Exported rows` is the number of rows exported to blobs.

Those two numbers should be equal once every block is exported.  The only way they could differ is if rows were deleted in between.

## How do I measure progress?

You should look at the block count in each state.  A copy is completed when all blocks (i.e. the total number of blocks) is in state `moved`.

The ratio of `Moved` blocks (over `Total`) block should give you a percentage of completion.