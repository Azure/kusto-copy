# Troubleshooting & performance

[Documentation home](README.md) | [Progress guide](progress.md) | [Advanced topics](advanced.md)

## Collect useful evidence

Keep the version printed at startup, the command or YAML configuration, the error output,
and several progress reports taken over time. Add `-v` (or `--verbose`) to the same command
for informational diagnostics. Capture console output with your shell or job runner.

Remove credentials, SAS query strings, and sensitive data before sharing logs. Kusto requests
are tagged with `KUSTO-COPY;<version>`, which helps correlate them with cluster-side queries.

## Authentication or authorization errors

Check the identity selected by `DefaultAzureCredential`; an Azure CLI login on a developer
machine may not be the identity you intended for an unattended job.

- Source: database viewer permission.
- Destination: database admin permission, including access to temporary tables.
- Storage: blob data access plus permission to request a user delegation key. Role scope
  matters; see [storage permissions](setup.md#adls-gen2-container-permissions).
- Managed identity: run on a host where the identity is available. For a user-assigned
  identity, use its **client ID**, not its object ID, with `--clientId`.

Some Kusto failures are retried before the process exits. Repeated permission warnings are
not evidence that a permission problem will resolve by itself.

See [authentication and security](advanced.md#authentication-and-security) for credential
selection and network paths.

## Planning fails or never seems to finish

- Verify that the source workload group uses strong consistency.
- Ensure the `execute_show_command` plugin is available; planning uses it to obtain extent
  metadata.
- Check any `-q` fragment. It must remain suitable for row-level planning, not aggregate rows.
- Watch the total block count. It can grow during `Planning`, while export and ingestion
  are already running. Planning also pauses intentionally when enough blocks are in flight.

See [iteration states](progress.md#iteration-state) before treating planning as a stall.

## The copy is slow

Compare several progress reports, not a single snapshot. Blocks are not equal-sized byte
units: wide rows and large dynamic columns can take much longer than narrow rows.

| Blocks accumulating in | Likely area to investigate | First actions |
|------------------------|----------------------------|---------------|
| Planned | Source export | Check export capacity, load, and `--export` |
| Exported | Destination ingestion | Check ingestion health and destination capacity |
| Ingested | Extent moves and update policies | Check destination load and policy cost |

The [bottleneck reference](progress.md#bottlenecks) explains the columns in more detail.

Tune one factor at a time:

1. Establish throughput with a representative subset and record cluster and storage metrics.
2. If export is limiting progress, adjust `--export` within the source cluster's capacity.
   Its default is 20; effective parallelism is the smaller of that limit and cluster capacity.
3. If ingestion or moving is limiting progress, increasing exports can just build a larger
   staging backlog. Investigate destination capacity and update policies first.
4. Check storage throttling and network placement. If storage is the measured bottleneck,
   use multiple **storage accounts**, not more containers in the same account.

Do not disable destination policies just to improve a benchmark without understanding the
behavioral change. Update policies execute when copied extents move into the final table.

## Progress is missing or appears stuck

The progress table refreshes approximately every 20 seconds after initialization. Startup
authentication and connection initialization happen before normal progress reporting.

- Check the process is still running and look for warnings or errors in captured output.
- Check whether block counts advance across several reports.
- For scheduled copies, a quiet period can mean the process is waiting for the next iteration.
- In `ExportOnly`, blocks stop at export, not at `Moved`; this is not an ingestion failure.

The CLI provides console progress, not a built-in monitoring or alerting service. Use your
hosting environment to capture output, detect process failure, and alert on lack of progress.

## Restart reports a configuration mismatch

The staging folder contains persisted job state. A tracked activity cannot be removed,
renamed, or redirected to different source or destination tables and still be treated as
the same job.

Restore the original configuration to resume, or choose a new dedicated staging folder for
an independent copy. Do not delete tracking data as a routine error-recovery step.
See [resume and restart](advanced.md#resume-and-restart), including the duplicate-data risk.

## Counts do not match

Validate the same filtered data and a fixed boundary on both sides. A live source can keep
growing after the copy's cursor was captured, so comparing unbounded counts can be misleading.

Also check source retention or deletions during the copy, destination retention, query
projections, and update policies. Kusto Copy preserves extent creation time, so old data can
be subject to destination retention immediately.

Completion means the planned pipeline work finished, not that an independent data comparison
was performed. Follow the [validation checklist](advanced.md#production-checklist).
