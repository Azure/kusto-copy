# Planning

Planning is the process that turns a cursor window into a set of exportable blocks whose
boundaries are chosen to keep the source-side work predictable and the destination-side
aggregation cost under control.

This is a target design document: the intent is to make planning efficient even when the
`postPlanningKqlQuery` is non-empty and materially shrinks the data before the destination sees it.
The key problem to avoid is this:

* the raw source rows might still be very large
* the post-planning aggregate may collapse them to a small result
* if planning is done only against raw row counts, we create too many tiny blocks
* the destination then has to re-aggregate a lot of data into a materialized view (MV)
* the total cost becomes dominated by churn and unnecessary block count, not by useful work

The design therefore tries to keep the source cluster doing the expensive aggregation work and to
make each planned block as large as possible while still bounded by the final output size.

## Core idea

Planning is recursive over the source ingestion-time timeline.  We drill down by time levels until
we reach the smallest useful partition size, but we do not blindly stop at "raw row count <= 16M".
When a post-planning query is present, the stop condition is based on the projected output of that
query, not just the raw source rows.

The levels are:

* 1 day
* 1 hour
* 1 minute
* 1 second
* 100 milliseconds
* 10 milliseconds
* 1 millisecond

At each level we create partitions over `ingestion_time()` ranges.  We work depth-first:  first
refine the current partition, then continue until we either reach a leaf partition or we decide it
is already a good block boundary.

## Why this matters for aggregate-heavy exports

The source and destination have different roles:

* the source is good at filtering and aggregating large data sets quickly
* the destination is the place where the final materialized view is built and maintained
* if we create one block per tiny raw partition, the destination sees a lot of tiny fragments
  that must be re-aggregated later

The target design avoids that by intentionally generating larger blocks when the output of
`postPlanningKqlQuery` is small, so the source does more useful work per block and the destination
has fewer, larger aggregates to reconcile.

## Planning behavior

Planning differs depending on whether `postPlanningKqlQuery` is empty, but the hierarchy and the
recursive subdivision are the same.

We define a planning timeout as roughly 2.5 minutes.  This leaves headroom before the cluster's
normal query timeout, because the planning query and the export query share the same operational
conditions and we do not want planning to become the trigger for a cluster-wide timeout cascade.

### First level

The first level does not use `kqlQuery` for row counting.  It simply breaks the cursor window into
1-day partitions.  This is cheap and makes it possible to plan an entire history in a resumable,
progressive way, even if the first iteration covers a long time range and a very large number of
extents.

The tradeoff is that a cheap predicate in `kqlQuery` (for example "last 30 days" or a small subset)
may not reduce the work of the initial partitioning pass.  That is acceptable for the first level,
which is intentionally broad and cheap, because the real pruning happens as we drill deeper.

### No post-planning query

For the second level onward, we use `kqlQuery` to estimate how much data falls in each partition.
If a query times out or runs out of memory, we retry without the `kqlQuery` fragment as a fallback;
if it still fails, planning fails permanently with a clear error.

We recursively sub-partition a partition until one of these is true:

* the partition is small enough to be a single block
* the partition is as granular as the chosen time resolution allows
* the projected output is within the target block size and can be exported as-is

Small sub-partitions are merged at the KQL level with `row_cumsum()` to avoid generating a large
number of near-empty partitions.

### With a post-planning query

This is the important target behavior.

For a partition, we run a query including both `kqlQuery` and `postPlanningKqlQuery`, and we use the
resulting row estimate as the planning budget.

The target algorithm is:

* estimate the output size of the partition after the post-planning query
* if the estimate is below the target block threshold, keep the partition large and create a block
* if the estimate is above the threshold, split the partition into smaller time bins and re-estimate
* continue until the projected output fits within a block budget, or until the partition reaches the
  smallest supported time resolution

This is intentionally different from "raw row count decides block size."  With aggregation-heavy
exports, raw source row counts are a poor proxy for useful work.

The goal is to keep the block count low and let the source cluster do the heavy lifting of the
aggregate before the data reaches the destination.  That means the size budget is driven more by the
post-planning output than by the pre-aggregation source cardinality.

## Creating blocks

Once a partition is considered terminal, we materialize it as a block.

For the non-post-planning path, block creation still follows the raw-source extent model:

* identify extents in the partition
* determine max extent creation time per extent
* align ingestion-time ranges with those creation times
* merge adjacent ranges with `row_cumsum()` so the final block stays within the target row count

We use the max creation time rather than the min, so the newest data wins and the destination does
not accidentally evict newer data before older source data is copied.

For the post-planning path, the block boundary is chosen by the projected aggregate output rather
than by the raw source footprint.  The source cluster is expected to aggregate the partition first;
the resulting block may therefore be much larger than a block built from raw rows alone, while still
staying within the destination's practical re-aggregation budget.

For both partitions and blocks, ingestion-time values are persisted as strings rather than .NET
`DateTime` values.  This avoids precision mismatches between Kusto's datetime resolution and .NET's
runtime representation.

## Design tradeoff

This target approach favors fewer, larger blocks when a post-planning aggregate substantially reduces
cardinality.  It accepts that a partition may have many raw source rows but still be a single block,
because the exported result is much smaller and should be treated as a single unit of work.

The tradeoff is straightforward:

* fewer blocks = less destination churn and less MV re-aggregation
* more source-side aggregate work = more efficient export planning for aggregate-heavy pipelines
* the source cluster must remain stable and query timeouts must remain controlled

That is the intended direction for planning in aggregate-oriented copies.

## Notes to implementer

The main engineering concern is detecting when a query is too expensive or too broad to be used as a
planning estimator.  We need a reliable way to distinguish:

* a valid but large aggregate that should be subdivided
* a query that is failing due to a timeout, memory pressure, or cluster instability

The target design assumes that we will rely on elapsed time, result-size heuristics, and explicit
failure signatures to decide whether to subdivide the partition or fail the planning pass.