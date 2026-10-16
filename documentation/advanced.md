# 🔬 Advanced topics

[Documentation home](README.md) | [Common scenarios](scenarios.md) |
[Troubleshooting & performance](troubleshooting.md)

Plan a larger copy, resume safely, and prepare for production. Start with the
[setup guide](setup.md) if you have not run your first copy yet.

**Jump to:** [Copy behavior](#how-the-pipeline-works) · [Resume](#resume-and-restart) ·
[Security](#authentication-and-security) · [Cross-tenant](#split-flows-and-tenant-boundaries) ·
[Production checklist](#production-checklist)

<a id="how-the-pipeline-works"></a>

## 🏗️ What to expect from a copy

Data travels from the source through your staging storage to the destination. The clusters
move the data directly; the machine running Kusto Copy coordinates the work.

### Prepare the destination

- **Create the table first.** Its schema must be compatible with the exported data, including
  any columns selected or transformed by your query.
- **Expect an append, not a replacement.** Existing destination rows are not cleared.
- **Review update policies.** They run when copied data reaches the final destination table.
  Include their outputs and processing cost in your validation and capacity planning.

### Keep the source available

Kusto Copy can run while the source continues ingesting. Choose whether to copy history once
or also copy new arrivals using [copy mode](parameters.md#copy-mode).
New arrivals are selected by ingestion, not by the event timestamp in a user column.

Keep source data available until the copy is validated. Rows deleted or expired before they
are copied cannot be recovered by restarting the job.

> [!IMPORTANT]
> Review **destination retention** before copying old data. Kusto Copy preserves source
> extent creation times, so copied data can already be old enough to expire under the
> destination's retention policy.

This is batch copying, not change-data capture: source deletions and updates are not
reconciled at the destination. See [common scenarios](scenarios.md) for supported workflows.

<a id="resume-and-restart"></a>

## 🔄 Resume and restart

Your staging storage holds both temporary data and the information needed to resume.
Treat it as part of the job, not as disposable scratch space while a copy is in progress.

### Resume an interrupted copy

1. Rerun the same command or YAML configuration.
2. Keep activity names, source and destination tables, queries, and staging directories
   unchanged. If you use several staging directories, preserve their order too.
3. Ensure the selected identity still has the required permissions.
4. Run only one process against the same job's staging folders at a time.

Kusto Copy uses the saved progress to resume rather than deliberately copying everything
again. Do not delete staging data or alter temporary resources created by the tool to try
to clear an error; use the [troubleshooting guide](troubleshooting.md) first.

### Choose between continuing and starting over

| Your goal | What to do |
|-----------|------------|
| Finish an interrupted copy | Rerun the original job with the same staging folders. |
| Copy subsequent arrivals | Reuse the job with `BackfillAndNew`. See [the recipe][new-data]. |
| Start an independent copy | Use a new dedicated staging folder and review destination data. |

A completed `BackfillOnly` job does not recopy history just because you run it again.

> [!WARNING]
> A **new staging folder** starts a new job; it does **not** clear the destination table.
> Copying overlapping data into a populated table can create duplicates. Resume protection
> applies within the same tracked job, not across independent copies.

### Plan cleanup separately

Kusto Copy cleans up temporary data as work completes, while retaining progress for later
runs. Keep storage lifecycle rules away from active job folders.

**Staging is not a backup**, and the tool does not roll back destination data. If you lose
the saved progress, check what already reached the destination before deciding how to
start again. Remove remaining job data only when you no longer need resume or later
incremental copies.

<a id="authentication-and-security"></a>

## 🔐 Authentication and security

### Choose credentials for where the job runs

Kusto Copy uses [DefaultAzureCredential][credential]. The selected identity needs access to
the required Kusto databases and staging storage.

| Where you run | Recommended approach |
|---------------|----------------------|
| Local trial | Use your Azure CLI login. |
| Unattended Azure host | Prefer a managed identity available to that host. |
| Automation using a service principal | Supply credentials through the host's secret management. |

For managed identities:

- **System-assigned:** no client ID is needed.
- **User-assigned:** use `--clientId <managed-identity-client-id>` or
  `managedIdentityClientId` in YAML.

`--clientId` selects a managed identity; it is not a service-principal login option.
Other credentials available on the host can take precedence, so check which identity
the job will use.

For a service principal, set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`
through your hosting environment. Never put secrets in command arguments or checked-in YAML.

### Protect staging storage

- Grant the [required permissions](setup.md#permissions), including the
  [storage role scopes](setup.md#adls-gen2-container-permissions).
- Supply plain container or directory URLs, not account keys or URLs containing SAS tokens.
- Restrict access to staging data and treat any SAS-bearing logs as credentials.

### Check the whole network path

A successful connection from your machine is not enough:

| Component | Needs access to |
|-----------|-----------------|
| Console host | Authentication, required Kusto endpoints, and Blob/DFS storage endpoints |
| Source cluster | Staging storage for exports |
| Destination ingestion service | Staging storage to read exported data |

Account for storage firewalls, private endpoints, and DNS. Allow the destination's ingestion
endpoint as well as its query endpoint. Test a small copy in the intended network environment
before scheduling a large migration.

<a id="split-flows-and-tenant-boundaries"></a>

## 🌐 Cross-tenant copies

A managed identity belongs to one Entra tenant. Separate export and ingestion flows are
intended to let each phase use an identity from the appropriate tenant:

1. `ExportOnly`: export using an identity authorized on the source and staging storage.
2. `IngestOnly`: after export finishes, ingest using an identity authorized on the destination
   and the **same** staging storage.

Keep the copy configuration and staging directories identical across phases; change the
flow and credentials as needed. Both identities need the required storage permissions.
Cross-tenant access must be arranged explicitly; selecting a flow does not grant access.

> [!WARNING]
> A new `ExportOnly` job cannot currently start exporting from a fresh staging folder.
> Verify support in the release you deploy before relying on this workflow. A successful
> process exit alone does **not** prove that data was exported.

See [copy flow](parameters.md#copy-flow). `IngestOnly` requires the saved job information
from the export phase; it is not an importer for arbitrary Parquet files.

<a id="production-checklist"></a>

## ✅ Production checklist

### Before the copy

- [ ] Complete the [setup prerequisites](setup.md#prerequisites) and create destination tables.
- [ ] Review retention, existing destination data, and update policies.
- [ ] Use dedicated staging folders protected from premature lifecycle deletion.
- [ ] Test representative data, including wide rows and policy outputs. Keep trial data
  separate from the production copy to avoid overlap.
- [ ] Configure reliable hosting, unattended authentication, and console-log capture.

### While it runs

- [ ] Monitor [progress](progress.md), process exit status, and prolonged lack of progress.
  Configure alerts through your hosting platform; the CLI does not create them.
- [ ] Tune from the [observed bottleneck](troubleshooting.md#the-copy-is-slow).
- [ ] Allow enough staging capacity if exports outpace ingestion.
- [ ] Keep source data available until copying and validation are complete.

### Validate and cut over

- [ ] Compare counts for the same selected dataset, not an ever-growing unbounded source.
- [ ] Compare deterministic aggregates, representative records, and transformed columns.
- [ ] Validate update-policy outputs separately from the directly copied table.
- [ ] Coordinate a source ingestion pause or agreed ingestion boundary for final cutover,
  copy the remaining arrivals, and reconcile results before redirecting consumers.
- [ ] Retain the job configuration, version, and validation results.
- [ ] Decide whether the job will continue incrementally before removing its staging folders.

[credential]: https://learn.microsoft.com/dotnet/api/azure.identity.defaultazurecredential
[new-data]: scenarios.md#backfill-then-copy-new-arrivals
