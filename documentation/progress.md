# 📊 Reading progress

[Documentation home](README.md) | [Troubleshooting](troubleshooting.md) |
[Advanced topics](advanced.md)

Kusto Copy refreshes its console progress table approximately every **20 seconds**:

```text
┌──────────┬─────────────────┬───────┬──────┬─────────┬──────────┬──────────┬───────┐
│ Activity │ Iteration State │ Total │ %    │ Planned │ Exported │ Ingested │ Moved │
├──────────┼─────────────────┼───────┼──────┼─────────┼──────────┼──────────┼───────┤
│ default  │ Starting        │ 210   │ % 28 │ 10      │ 50       │ 90       │ 60    │
└──────────┴─────────────────┴───────┴──────┴─────────┴──────────┴──────────┴───────┘
```

> [!TIP]
> **Just checking in?** Start with the percentage column. To understand a slowdown, look
> at which state is accumulating blocks across several reports.

**Jump to:** [Blocks](#blocks) · [Iterations](#iteration-state) · [Block states](#block-state) ·
[Completion](#how-do-i-measure-progress) · [Bottlenecks](#bottlenecks)

<a id="blocks"></a>

## 🧱 Blocks

Kusto Copy splits data into **blocks**, each bounded by minimum and maximum
[`ingestion_time()`][ingestion-time] values. Smaller units avoid relying on a single export
or ingestion operation for billions of rows.

<a id="iteration-state"></a>

## 🔄 Iteration State

An iteration is one pass over a cursor window. Its state tells you whether block planning
is still in progress:

| State | What it means |
|-------|---------------|
| `Starting` | Planning has not started. This state is usually brief. |
| `Planning` | Source queries are discovering blocks; **Total can increase**. |
| `Planned` | Planning is finished; **Total is fixed** while blocks finish copying. |
| `Completed` | All blocks have been copied. |

**Planning and copying overlap.** Blocks start copying as soon as they are planned; they
do not wait for the entire iteration to finish planning.

<a id="block-state"></a>

## 📦 Block State

Each progress column represents a stage in the copy:

```text
Planned -> Exported -> Ingested -> Moved
```

| State | What it means |
|-------|---------------|
| `Planned` | Ready to export from the source. |
| `Exported` | Exported to staging storage. |
| `Ingested` | Exported files have been ingested into destination temporary-table extents. |
| `Moved` | Extents have been moved into the final destination table. |

### Why Ingested is not yet Moved

Kusto Copy first ingests into a temporary table named like `kc-<table>-<guid>`. It checks
the ingested data before moving extents into the destination, supporting exactly-once
copying within the tracked job.

> [!NOTE]
> Destination **update policies run on move**, not on ingestion into the temporary table.
> An `Ingested` backlog can therefore reflect the cost of those policies.

Blocks can return to an earlier stage during error recovery; progress is not always a
strictly one-way progression. See [pipeline internals](advanced.md#how-the-pipeline-works)
for the verification and recovery behavior.

<a id="how-do-i-measure-progress"></a>

## 🏁 How do I measure progress?

For a full copy, all blocks must reach **Moved**:

```text
Completion percentage = Moved blocks / Total blocks × 100
```

Planning can still change the total while the iteration is in `Planning`.
Completion describes pipeline work, not an independent data comparison; always
[validate the copy](setup.md#validating-the-copy), especially if source data expired or was
deleted during the run.

<a id="bottlenecks"></a>

## ⚡ Bottlenecks

Look for a **sustained backlog**, not a single slow-looking report:

| Blocks accumulating in | Likely bottleneck | Investigate |
|------------------------|-------------------|-------------|
| `Planned` | Source export | Export limit, source capacity, and cache |
| `Exported` | Destination ingestion | Ingestion health and destination capacity |
| `Ingested` | Extent moves | Destination capacity and update-policy cost |

Start with [export tuning](parameters.md#parallel-export) when blocks accumulate in
`Planned`. For a step-by-step diagnosis, use
[Troubleshooting & performance](troubleshooting.md#the-copy-is-slow).

**Allow for data shape.** A block can contain roughly 16 million rows; many columns or
large strings and dynamic values can make it substantially slower than a narrow block.

[ingestion-time]: https://learn.microsoft.com/kusto/query/ingestion-time-function
